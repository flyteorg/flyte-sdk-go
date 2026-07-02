package externalprocess

import (
	"fmt"
	"os/exec"
)

func Execute(command []string) ([]byte, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("external auth command is empty")
	}
	cmd := exec.Command(command[0], command[1:]...) //nolint
	return cmd.Output()
}
