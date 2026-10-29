package udbtest_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

func TestLiveGoServiceExampleCommitsAndAcknowledges(t *testing.T) {
	if os.Getenv("UDB_LIVE_CDC_TESTS") != "1" {
		t.Skip("requires the CI served CDC lane (UDB_LIVE_CDC_TESTS=1)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	session := connectLive(t, ctx)
	defer session.Close()
	example, err := filepath.Abs("../../../examples/go_service")
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(example, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, line := range strings.Split(string(source), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	if lines > 30 {
		t.Fatalf("Go service has %d nonblank handwritten lines, maximum 30", lines)
	}
	binary := filepath.Join(t.TempDir(), "go-service")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, ".")
	build.Dir = example
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the Go service: %v\n%s", err, output)
	}
	fileID := conformance.NewID()
	name := "go-service-" + fileID
	control := &entityv1.CdcControlRequest{SlotName: "go-service-proof-" + fileID}
	if _, err := session.Data.Broker.PauseCdc(ctx, control); err != nil {
		t.Fatal(err)
	}
	defer session.Data.Broker.ResumeCdc(context.Background(), control)
	logPath := filepath.Join(t.TempDir(), "service.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	service := exec.CommandContext(ctx, binary)
	service.Stdout, service.Stderr = log, log
	service.Env = append(os.Environ(),
		"UDB_TARGET="+os.Getenv("UDB_GRPC_TARGET"),
		"UDB_AUTH_TARGET="+os.Getenv("UDB_AUTH_GRPC_TARGET"),
		"UDB_TENANT_ID="+session.CanonicalTenantID,
		"UDB_PROJECT_ID="+session.Udb.Meta.ProjectID,
		"UDB_PURPOSE=go.live.table-conformance",
		"UDB_API_KEY=", "UDB_BEARER="+strings.TrimPrefix(session.Bearer(), "Bearer "), "UDB_TLS=false",
		"EXAMPLE_FILE_ID="+fileID)
	if err := service.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- service.Wait() }()
	defer func() {
		_ = service.Process.Kill()
		<-stopped
	}()
	var handled string
	for handled == "" {
		contents, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(contents), "\n") {
			if strings.Contains(line, "handled "+fileID+" ") {
				parts := strings.Fields(line)
				handled = parts[len(parts)-1]
			}
		}
		if handled != "" {
			break
		}
		select {
		case err := <-stopped:
			stopped <- err
			// Re-read: the copy above predates the exit and misses its error.
			final, _ := os.ReadFile(logPath)
			t.Fatalf("Go service exited before handling its event: %v\n%s", err, final)
		case <-ctx.Done():
			t.Fatalf("Go service did not handle its event: %v\n%s", ctx.Err(), contents)
		case <-time.After(100 * time.Millisecond):
		}
	}
	for {
		status, err := session.Data.Broker.GetCdcStatus(ctx, control)
		if err != nil {
			t.Fatal(err)
		}
		for _, consumer := range status.GetConsumers() {
			if consumer.GetConsumerName() == name && consumer.GetLastEventId() == handled {
				files := udbclient.TableOf[*storagev1.File](session.Udb)
				row, err := files.Get(ctx, udbclient.RowKey{"file_id": fileID})
				if err != nil || row.GetFilename() != "after.txt" || row.GetTenantId() != session.CanonicalTenantID {
					t.Fatalf("CAS row was not committed in the authenticated tenant: %v, %v", row, err)
				}
				if consumer.GetTenantId() != session.CanonicalTenantID || consumer.GetOwnerMigrationRequired() {
					t.Fatalf("acknowledgment was not owned by the verified tenant: %v", consumer)
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("the handled event was not durably acknowledged")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
