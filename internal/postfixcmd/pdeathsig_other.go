//go:build !linux

package postfixcmd

import "os/exec"

// setPdeathsig is a no-op on non-Linux platforms where PR_SET_PDEATHSIG is unavailable.
func setPdeathsig(_ *exec.Cmd) {}
