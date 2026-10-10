package udbtest

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

// Start runs the installed real broker with its embedded PostgreSQL, logs in as
// the bootstrapped test administrator, and returns its tenant-scoped client.
// It stops the client, broker and PostgreSQL when t ends. UDB_TEST_BINARY selects
// a particular broker binary; otherwise Start resolves udb on PATH. The binary
// must support dev up --embedded. Start never builds a broker or runs Docker.
//
// protoRoot names the application's proto directory; an empty string creates
// an empty project using the broker's native models. policyFile, when supplied,
// is applied through the real `udb policy apply` command for the bootstrapped
// tenant, with the same JSON/YAML format and reconciliation rules as production.
// Each call owns a temporary project and separate loopback ports. PostgreSQL
// downloads use the ordinary shared cache or UDB_HOME, including configured
// UDB_POSTGRES_BINARIES_URL mirrors and UDB_EMBEDDED_PG_ARCHIVE offline archives.
func Start(t testing.TB, protoRoot, policyFile string) *udbclient.Udb {
	t.Helper()
	first, _ := start(t, protoRoot, policyFile, false)
	return first
}

// StartPair starts one isolated embedded broker with two independently
// provisioned tenant administrators, for the full shared isolation contract.
func StartPair(t testing.TB, protoRoot, policyFile string) (*udbclient.Udb, *udbclient.Udb) {
	t.Helper()
	return start(t, protoRoot, policyFile, true)
}

func start(t testing.TB, protoRoot, policyFile string, withPeer bool) (*udbclient.Udb, *udbclient.Udb) {
	t.Helper()
	binary := os.Getenv("UDB_TEST_BINARY")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("udb")
		if err != nil {
			t.Fatalf("udbtest.Start: install an embedded-capable udb or set UDB_TEST_BINARY: %v", err)
		}
	}
	abs := func(path string) string {
		resolved, err := filepath.Abs(path)
		if err != nil {
			t.Fatalf("udbtest.Start: resolve path: %v", err)
		}
		return resolved
	}
	binary = abs(binary)
	project := t.TempDir()
	if protoRoot == "" {
		protoRoot = filepath.Join(project, "proto")
		if err := os.Mkdir(protoRoot, 0700); err != nil {
			t.Fatalf("udbtest.Start: create proto directory: %v", err)
		}
	}
	protoRoot = abs(protoRoot)
	if policyFile != "" {
		policyFile = abs(policyFile)
	}
	// Avoid inheriting another project's listeners, database DSNs or credentials.
	// Acquisition settings alone may be shared between isolated test brokers.
	environment := make([]string, 0, len(os.Environ())+8)
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		upper := strings.ToUpper(key)
		if !strings.HasPrefix(upper, "UDB_") && upper != "DATABASE_URL" {
			environment = append(environment, value)
		}
	}
	for _, key := range []string{"UDB_HOME", "UDB_POSTGRES_BINARIES_URL", "UDB_EMBEDDED_PG_ARCHIVE"} {
		if value, ok := os.LookupEnv(key); ok {
			environment = append(environment, key+"="+value)
		}
	}
	environment = append(environment, "UDB_PROTO_ROOT="+protoRoot)
	// Hold reservations until every port is selected, so listeners in the same
	// child cannot accidentally receive the same ephemeral port.
	var reservations []net.Listener
	defer func() {
		for _, reservation := range reservations {
			_ = reservation.Close()
		}
	}()
	port := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("udbtest.Start: reserve loopback port: %v", err)
		}
		reservations = append(reservations, listener)
		return listener.Addr().String()
	}
	pgAddress := port()
	_, pgPort, _ := net.SplitHostPort(pgAddress)
	addresses := make(map[string]string)
	for _, key := range []string{"UDB_GRPC_ADDR", "UDB_AUTH_GRPC_ADDR", "UDB_WEBRTC_GRPC_ADDR", "UDB_HTTP_ADDR", "UDB_METRICS_ADDR"} {
		addresses[key] = port()
		environment = append(environment, key+"="+addresses[key])
	}
	logPath := filepath.Join(project, "broker.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("udbtest.Start: open private broker log: %v", err)
	}
	t.Cleanup(func() { _ = log.Close() })
	command := exec.Command(binary, "dev", "up", "--embedded", "--pg-port", pgPort)
	command.Dir, command.Env, command.Stdout, command.Stderr = project, environment, log, log
	for _, reservation := range reservations {
		_ = reservation.Close()
	}
	reservations = nil
	if err := command.Start(); err != nil {
		t.Fatalf("udbtest.Start: launch broker: %v", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = command.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		stop := exec.CommandContext(ctx, binary, "dev", "down", "--embedded")
		stop.Dir, stop.Env, stop.Stdout, stop.Stderr = project, environment, log, log
		if err := stop.Run(); err != nil {
			t.Errorf("udbtest.Start: stop embedded broker: %v (private log: %s)", err, logPath)
		}
		select {
		case <-done:
			if waitErr != nil {
				t.Errorf("udbtest.Start: broker exit: %v (private log: %s)", waitErr, logPath)
			}
		case <-ctx.Done():
			_ = command.Process.Kill()
			<-done
			t.Errorf("udbtest.Start: embedded broker did not stop within 45 seconds")
		}
		for _, address := range []string{pgAddress, addresses["UDB_GRPC_ADDR"], addresses["UDB_AUTH_GRPC_ADDR"]} {
			if connection, err := net.DialTimeout("tcp", address, time.Second); err == nil {
				_ = connection.Close()
				t.Errorf("udbtest.Start: shutdown left %s listening", address)
			}
		}
	})
	var credentials struct {
		TenantID string `json:"tenant_id"`
		Username string `json:"username"`
		Password string `json:"password"`
		DSN      string `json:"dsn"`
	}
	deadline := time.NewTimer(10 * time.Minute)
	defer deadline.Stop()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	marker := filepath.Join(project, ".udb", "dev", "bootstrap.json")
	for {
		data, err := os.ReadFile(marker)
		if err == nil && json.Unmarshal(data, &credentials) == nil && credentials.Password != "" && credentials.TenantID != "" {
			break
		}
		select {
		case <-done:
			t.Fatalf("udbtest.Start: broker exited during startup: %v (private log: %s)", waitErr, logPath)
		case <-deadline.C:
			t.Fatalf("udbtest.Start: startup exceeded 10 minutes (private log: %s)", logPath)
		case <-poll.C:
		}
	}
	var peerUsername, peerPassword, peerTenant string
	if withPeer {
		peerUsername = "udbtest-peer-" + conformance.NewID()
		peerPassword = "UdbTestPeer#" + conformance.NewID()
		peerTenant = conformance.NewID()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		bootstrap := exec.CommandContext(ctx, binary, "auth", "bootstrap", "user", "--username", peerUsername, "--email", peerUsername+"@udbtest.invalid", "--password", peerPassword, "--tenant", peerTenant, "--project", "default")
		bootstrap.Dir, bootstrap.Env = project, append(append([]string{}, environment...), "UDB_PG_DSN="+credentials.DSN)
		bootstrap.Stdout, bootstrap.Stderr = log, log
		err := bootstrap.Run()
		cancel()
		if err != nil {
			t.Fatalf("udbtest.StartPair: provision peer tenant: %v (private log: %s)", err, logPath)
		}
	}
	if policyFile != "" {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		apply := exec.CommandContext(ctx, binary, "policy", "apply", "-f", policyFile, "--tenant", credentials.TenantID)
		apply.Dir, apply.Env = project, append(append([]string{}, environment...), "UDB_PG_DSN="+credentials.DSN)
		apply.Stdout, apply.Stderr = log, log
		err := apply.Run()
		cancel()
		if err != nil {
			t.Fatalf("udbtest.Start: apply policy file: %v (private log: %s)", err, logPath)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	session, err := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
		Target: addresses["UDB_GRPC_ADDR"], AuthTarget: addresses["UDB_AUTH_GRPC_ADDR"],
		Username: credentials.Username, Password: credentials.Password,
		TenantCode: credentials.TenantID, ProjectID: "default", Purpose: "udbtest.embedded",
		Deadline: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("udbtest.Start: authenticated client: %v (private log: %s)", err, logPath)
	}
	t.Cleanup(func() { _ = session.Close() })
	// Do not print bootstrap credentials or the DSN in test output.
	t.Logf("udbtest.Start: isolated embedded broker on %s (PostgreSQL port %s)", addresses["UDB_GRPC_ADDR"], pgPort)
	var peer *udbclient.Udb
	if withPeer {
		second, err := udbclient.ConnectEnterprise(ctx, udbclient.EnterpriseConfig{
			Target: addresses["UDB_GRPC_ADDR"], AuthTarget: addresses["UDB_AUTH_GRPC_ADDR"],
			Username: peerUsername, Password: peerPassword, TenantCode: peerTenant, ProjectID: "default", Purpose: "udbtest.embedded", Deadline: 30 * time.Second,
		})
		if err != nil {
			t.Fatalf("udbtest.StartPair: peer client: %v (private log: %s)", err, logPath)
		}
		t.Cleanup(func() { _ = second.Close() })
		peer = second.Udb
	}
	return session.Udb, peer
}
