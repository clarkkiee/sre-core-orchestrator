package multipass

import "fmt"

// CommandError is returned when a multipass CLI command exits with non-zero.
type CommandError struct {
	Command    string
	ReturnCode int
	Stderr     string
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("multipass command '%s' failed (rc=%d): %s", e.Command, e.ReturnCode, e.Stderr)
}

// VMNotFoundError is returned when a VM does not exist.
type VMNotFoundError struct {
	VMName string
}

func (e *VMNotFoundError) Error() string {
	return fmt.Sprintf("VM %q not found", e.VMName)
}
