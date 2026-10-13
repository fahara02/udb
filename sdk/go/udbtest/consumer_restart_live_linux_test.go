package udbtest_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	storageeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/events/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kill only the isolated CI process whose executable, directory and complete
// command match launch-broker. The restarted process survives this test so the
// following SDK tests use it; the workflow's final Stop broker owns cleanup.
func restartOwnedCIBroker(t *testing.T, ctx context.Context) {
	t.Helper()
	if os.Getenv("CI") != "true" || os.Getenv("UDB_LIVE_CDC_RESTART_TESTS") != "1" {
		t.Fatal("broker process control is restricted to the explicit CI restart lane")
	}
	pid, err := strconv.Atoi(os.Getenv("UDB_BROKER_PID"))
	if err != nil || pid <= 1 || pid == os.Getpid() {
		t.Fatal("invalid isolated broker PID")
	}
	binary, err := filepath.EvalSymlinks(os.Getenv("UDB_SERVED_BIN"))
	if err != nil {
		t.Fatalf("resolve served binary: %v", err)
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatalf("absolute served binary: %v", err)
	}
	workspace, err := filepath.EvalSymlinks(os.Getenv("GITHUB_WORKSPACE"))
	if err != nil {
		t.Fatalf("resolve CI workspace: %v", err)
	}
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || executable != binary {
		t.Fatalf("refuse to stop a process that is not the served CI binary: %v", err)
	}
	directory, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
	if err != nil || directory != workspace {
		t.Fatalf("refuse to stop a broker outside the CI workspace: %v", err)
	}
	target := os.Getenv("UDB_GRPC_TARGET")
	if target != "127.0.0.1:50051" || os.Getenv("UDB_AUTH_GRPC_TARGET") != "127.0.0.1:50061" {
		t.Fatal("restart proof requires the isolated CI loopback listeners")
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		t.Fatalf("read broker command: %v", err)
	}
	args := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	if len(args) != 5 || args[1] != "serve" || args[2] != "proto" || args[3] != "" || args[4] != target {
		t.Fatal("refuse to stop a process with a different launch command")
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("find owned broker: %v", err)
	}
	if err := process.Kill(); err != nil {
		t.Fatalf("crash isolated broker: %v", err)
	}
	// A crashed leader may leave a durable lease. The real broker must recover
	// it; no test SQL clears leases, cursors or authentication state.
	deadline := time.Now().Add(180 * time.Second)
	for {
		_, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("broker crash did not complete")
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("broker crash did not release its executable")
		}
	}
	logPath := filepath.Join(os.Getenv("RUNNER_TEMP"), "udb-go-live-restart.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("open restart diagnostics: %v", err)
	}
	defer log.Close()
	command := exec.Command(binary, "serve", "proto", "", target)
	command.Dir = workspace
	command.Stdout, command.Stderr = log, log
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatalf("restart served CI binary: %v", err)
	}
	newPID := command.Process.Pid
	env, err := os.OpenFile(os.Getenv("GITHUB_ENV"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		_ = command.Process.Kill()
		t.Fatalf("persist restarted broker cleanup PID: %v", err)
	}
	_, writeErr := fmt.Fprintf(env, "UDB_BROKER_PID=%d\n", newPID)
	closeErr := env.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = command.Process.Kill()
		t.Fatalf("persist restarted broker cleanup PID: %v", err)
	}
	if err := os.Setenv("UDB_BROKER_PID", strconv.Itoa(newPID)); err != nil {
		_ = command.Process.Kill()
		t.Fatalf("adopt restarted broker PID: %v", err)
	}
	_ = command.Process.Release()
	for time.Now().Before(deadline) {
		ready := true
		for _, address := range []string{target, os.Getenv("UDB_AUTH_GRPC_TARGET")} {
			connection, err := net.DialTimeout("tcp", address, 250*time.Millisecond)
			if err != nil {
				ready = false
				break
			}
			_ = connection.Close()
		}
		if ready {
			return
		}
		if _, err := os.Stat(fmt.Sprintf("/proc/%d/exe", newPID)); errors.Is(err, os.ErrNotExist) {
			t.Fatal("restarted broker exited; inspect restart diagnostics")
		}
		select {
		case <-ctx.Done():
			t.Fatal("restarted broker did not become ready")
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatal("restarted broker did not become ready within 180 seconds")
}

func TestLiveConsumerResumesAfterBrokerCrash(t *testing.T) {
	if os.Getenv("UDB_LIVE_CDC_RESTART_TESTS") != "1" {
		t.Skip("requires isolated CI broker restart lane")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sess := connectLive(t, ctx)
	defer sess.Close()
	broker := &observedConsumerBroker{DataBrokerClient: sess.Udb.Data.Broker, acknowledged: make(chan string, 4)}
	sess.Udb.Data.Broker = broker
	name := fmt.Sprintf("go-restart-%d", time.Now().UnixNano())
	prefix := name + "/"
	topic := udbclient.Topic(&storageeventsv1.FileUploaded{})
	counts := map[string]int{}
	running, stop := context.WithCancel(ctx)
	defer stop()
	finished := make(chan error, 1)
	reconnect := make(chan struct{}, 1)
	handler := func(_ context.Context, event udbclient.ConsumedEvent[*storageeventsv1.FileUploaded]) error {
		file := event.Payload.GetFileId()
		if !strings.HasPrefix(file, prefix) {
			return nil
		}
		counts[file]++
		broker.watched.Store(event.ID, true)
		return nil
	}
	options := udbclient.ConsumeOptions{Backoff: 10 * time.Millisecond,
		OnError: func(err error) {
			if status.Code(err) == codes.Unavailable {
				select {
				case reconnect <- struct{}{}:
				default:
				}
			}
		},
	}
	go func() { finished <- udbclient.Consume(running, sess.Udb, name, topic, handler, options) }()
	emit := func(file string) {
		t.Helper()
		if err := sess.Udb.Tx(ctx, func(tx *udbclient.TxScope) error {
			return tx.Emit(&storageeventsv1.FileUploaded{FileId: prefix + file, TenantId: sess.Udb.Meta.TenantID, ObjectKey: "consumer/" + prefix + file})
		}); err != nil {
			t.Fatalf("emit %s: %v", file, err)
		}
	}
	waitAck := func(file string) string {
		t.Helper()
		select {
		case id := <-broker.acknowledged:
			return id
		case err := <-finished:
			t.Fatalf("consumer stopped before %s: %v", file, err)
		case <-ctx.Done():
			t.Fatalf("consumer did not acknowledge %s", file)
		}
		return ""
	}
	emit("before")
	before := waitAck("before")
	restartOwnedCIBroker(t, ctx)
	select {
	case <-reconnect:
	case err := <-finished:
		t.Fatalf("broker crash stopped the consumer: %v", err)
	case <-ctx.Done():
		t.Fatal("consumer did not observe the crashed connection")
	}
	stop()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stop original consumer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("original consumer did not stop")
	}
	// A new Consume has an EMPTY in-process dedup set. The broker's durable
	// watermark must prevent re-handling before, independent of SDK memory.
	freshContext, stopFresh := context.WithCancel(ctx)
	defer stopFresh()
	go func() { finished <- udbclient.Consume(freshContext, sess.Udb, name, topic, handler, options) }()
	emit("after")
	after := waitAck("after")
	stopFresh()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer stop: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not stop")
	}
	if before == after || counts[prefix+"before"] != 1 || counts[prefix+"after"] != 1 {
		t.Fatalf("broker restart lost or repeated handler calls: before=%s after=%s counts=%v", before, after, counts)
	}
}
