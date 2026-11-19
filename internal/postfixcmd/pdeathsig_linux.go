//go:build linux

package postfixcmd

import (
	"os/exec"
	"syscall"
)

// setPdeathsig configures the child process to receive SIGTERM if its parent dies.
func setPdeathsig(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGTERM
}
