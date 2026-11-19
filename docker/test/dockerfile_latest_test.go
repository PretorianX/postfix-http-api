package docker_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Postfix makedefs rejects shlib_directory=no ("must specify an absolute path
// name") and requires shared=no to disable dynamically-linked libraries.
func TestDockerfileLatestDisablesSharedLibsViaSharedNo(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	content, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "Dockerfile.latest"))
	if err != nil {
		t.Fatalf("read Dockerfile.latest: %v", err)
	}
	src := string(content)

	if strings.Contains(src, "shlib_directory=no") {
		t.Fatal("Dockerfile.latest must not use shlib_directory=no; Postfix 3.11+ requires shared=no instead")
	}
	if !strings.Contains(src, "shared=no") {
		t.Fatal("Dockerfile.latest makefiles step must pass shared=no to disable Postfix shared libraries")
	}
}

// Debian ships /usr/local/man as a symlink to share/man. Postfix's default
// manpage_directory=/usr/local/man creates a real directory in the install
// root, and COPY --from=postfix-builder then fails with "cannot copy to
// non-directory". Install man pages under /usr/local/share/man instead.
// Postfix 3.11 install also rejects manpage_directory=no.
func TestDockerfileLatestInstallsManPagesUnderShareMan(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	content, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "Dockerfile.latest"))
	if err != nil {
		t.Fatalf("read Dockerfile.latest: %v", err)
	}
	src := string(content)
	if strings.Contains(src, "manpage_directory=no") {
		t.Fatal("Dockerfile.latest must not use manpage_directory=no; Postfix 3.11 install rejects it")
	}
	if !strings.Contains(src, "manpage_directory=/usr/local/share/man") {
		t.Fatal("Dockerfile.latest must set manpage_directory=/usr/local/share/man to match Debian's /usr/local/man symlink")
	}
}
