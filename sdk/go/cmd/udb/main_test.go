package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

// The launcher must never probe itself, including through a symlink to itself.
func TestIsSelfCandidateSkipsLauncherAndSymlink(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	self := canonicalPath(exe)
	if !isSelfCandidate(exe, self) {
		t.Fatalf("launcher executable %q not recognised as itself", exe)
	}

	other := filepath.Join(t.TempDir(), exeName("udb"))
	writeExecutable(t, other, "not the launcher")
	if isSelfCandidate(other, self) {
		t.Fatalf("unrelated binary %q treated as the launcher", other)
	}

	link := filepath.Join(t.TempDir(), exeName("udb"))
	if err := os.Symlink(exe, link); err != nil {
		t.Logf("symlink unsupported here (%v); skipping symlink case", err)
		return
	}
	if !isSelfCandidate(link, self) {
		t.Fatalf("symlink %q to the launcher not recognised as itself", link)
	}
}

// Every PATH match is returned in PATH order, de-duplicated.
func TestPathCandidatesReturnsAllMatches(t *testing.T) {
	d1, d2, empty := t.TempDir(), t.TempDir(), t.TempDir()
	writeExecutable(t, filepath.Join(d1, exeName("udb")), "one")
	writeExecutable(t, filepath.Join(d2, exeName("udb")), "two")
	sep := string(os.PathListSeparator)
	t.Setenv("PATH", d1+sep+empty+sep+d2+sep+d1)

	got := pathCandidates()
	if len(got) != 2 {
		t.Fatalf("want 2 candidates, got %v", got)
	}
	if filepath.Dir(got[0]) != d1 || filepath.Dir(got[1]) != d2 {
		t.Fatalf("candidates out of PATH order: %v", got)
	}
}

// A nested launcher (UDB_LAUNCHER_ACTIVE set) must not run any PATH candidate,
// and every probe a top-level launcher runs carries the guard variable.
func TestNestedLauncherNeverProbesPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake udb is a shell script")
	}
	dir := t.TempDir()
	marker := filepath.Join(dir, "invocations")
	fake := filepath.Join(dir, "udb")
	writeExecutable(t, fake, "#!/bin/sh\necho \"guard=$UDB_LAUNCHER_ACTIVE\" >> '"+marker+"'\necho \"udb "+udbVersion+"\"\n")
	t.Setenv("PATH", dir)

	t.Setenv(launcherEnv, "1")
	if got := lookPathMatching(); got != "" {
		t.Fatalf("nested launcher probed PATH and returned %q", got)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("nested launcher ran a PATH candidate")
	}

	t.Setenv(launcherEnv, "")
	if got := lookPathMatching(); got != fake {
		t.Fatalf("want %q, got %q", fake, got)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "guard=1\n" {
		t.Fatalf("probe did not carry the guard variable exactly once: %q", data)
	}
}
