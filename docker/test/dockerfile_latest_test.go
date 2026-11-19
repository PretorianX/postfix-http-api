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
