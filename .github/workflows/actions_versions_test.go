package workflows_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// GitHub runners deprecate the Node.js 20 action runtime. Keep workflow
// actions on Node 24-compatible majors.
var minActionMajors = map[string]int{
	"actions/checkout":           7,
	"actions/setup-go":           7,
	"docker/setup-buildx-action": 4,
	"docker/setup-qemu-action":   4,
	"docker/login-action":        4,
	"docker/metadata-action":     6,
	"docker/build-push-action":   7,
}

func TestWorkflowActionsUseNode24Runtimes(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read workflows dir: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".yml") {
			continue
		}
		path := filepath.Join(dir, name)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "uses:") {
				continue
			}
			ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "uses:"))
			action, version, cut := strings.Cut(ref, "@")
			if !cut {
				t.Errorf("%s: action ref %q missing @version", name, ref)
				continue
			}
			minMajor, tracked := minActionMajors[action]
			if !tracked {
				continue
			}
			major := actionMajor(version)
			if major < minMajor {
				t.Errorf("%s: %s@%s uses deprecated Node 20 runtime; require @v%d+", name, action, version, minMajor)
			}
		}
	}
}

func actionMajor(version string) int {
	version = strings.TrimPrefix(version, "v")
	major := 0
	for _, r := range version {
		if r < '0' || r > '9' {
			break
		}
		major = major*10 + int(r-'0')
	}
	return major
}
