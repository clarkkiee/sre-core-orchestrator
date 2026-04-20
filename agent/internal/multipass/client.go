package multipass

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

// LaunchOpts holds optional parameters for LaunchVM.
type LaunchOpts struct {
	CPUs          int
	Memory        string
	Disk          string
	CloudInitFile string
	Image         string
}

// VMInfo represents a Multipass VM entry from `multipass list`.
type VMInfo struct {
	Name  string `json:"name"`
	State string `json:"state"`
	IPv4  string `json:"ipv4"`
}

// Client is a wrapper around the multipass CLI binary.
type Client struct {
	binary string
}

// NewClient creates a Client pointing to the given multipass binary path.
func NewClient(binary string) *Client {
	return &Client{binary: binary}
}

// run executes a multipass command and returns stdout.
// If check is true, a non-zero exit code returns a CommandError.
func (c *Client) run(ctx context.Context, check bool, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, c.binary, args...)
	slog.Info("running multipass", "cmd", fmt.Sprintf("%s %s", c.binary, strings.Join(args, " ")))

	out, err := cmd.Output()
	stdout := strings.TrimSpace(string(out))

	if err != nil && check {
		stderr := ""
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(exitErr.Stderr))
			return "", &CommandError{
				Command:    c.binary + " " + strings.Join(args, " "),
				ReturnCode: exitErr.ExitCode(),
				Stderr:     stderr,
			}
		}
		return "", fmt.Errorf("multipass exec error: %w", err)
	}

	return stdout, nil
}

// runRC executes a multipass command and returns (returncode, stdout, stderr).
func (c *Client) runRC(ctx context.Context, args ...string) (int, string, string) {
	cmd := exec.CommandContext(ctx, c.binary, args...)
	out, err := cmd.Output()
	stdout := strings.TrimSpace(string(out))

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode(), stdout, strings.TrimSpace(string(exitErr.Stderr))
		}
		return 1, "", err.Error()
	}
	return 0, stdout, ""
}

// LaunchVM launches a new Multipass VM.
func (c *Client) LaunchVM(ctx context.Context, name string, opts LaunchOpts) error {
	args := []string{
		"launch",
		"--name", name,
		"--cpus", fmt.Sprintf("%d", opts.CPUs),
		"--memory", opts.Memory,
		"--disk", opts.Disk,
	}
	if opts.CloudInitFile != "" {
		args = append(args, "--cloud-init", opts.CloudInitFile)
	}
	image := opts.Image
	if image == "" {
		image = "22.04"
	}
	args = append(args, image)

	_, err := c.run(ctx, true, args...)
	if err != nil {
		return err
	}
	slog.Info("VM launched", "name", name)
	return nil
}

// DeleteVM stops (best-effort) then deletes and purges a VM.
func (c *Client) DeleteVM(ctx context.Context, name string) error {
	c.runRC(ctx, "stop", name) // best-effort stop
	_, err := c.run(ctx, true, "delete", name, "--purge")
	if err != nil {
		return err
	}
	slog.Info("VM deleted", "name", name)
	return nil
}

// VMExists returns true if a VM with the given name exists.
func (c *Client) VMExists(ctx context.Context, name string) bool {
	rc, _, _ := c.runRC(ctx, "info", name)
	return rc == 0
}

// GetVMIP returns the primary IPv4 address of a running VM.
func (c *Client) GetVMIP(ctx context.Context, name string) (string, error) {
	if !c.VMExists(ctx, name) {
		return "", &VMNotFoundError{VMName: name}
	}

	stdout, err := c.run(ctx, true, "info", name, "--format", "json")
	if err != nil {
		return "", err
	}

	var data struct {
		Info map[string]struct {
			IPv4 []string `json:"ipv4"`
		} `json:"info"`
	}
	if err := json.Unmarshal([]byte(stdout), &data); err != nil {
		return "", fmt.Errorf("failed to parse multipass info for %q: %w", name, err)
	}

	vmInfo, ok := data.Info[name]
	if !ok || len(vmInfo.IPv4) == 0 {
		return "", fmt.Errorf("VM %q has no IPv4 addresses", name)
	}
	return vmInfo.IPv4[0], nil
}

// ListVMs lists all VMs with name, state, and IP.
func (c *Client) ListVMs(ctx context.Context) ([]VMInfo, error) {
	stdout, err := c.run(ctx, true, "list", "--format", "json")
	if err != nil {
		return nil, err
	}

	var data struct {
		List []struct {
			Name  string   `json:"name"`
			State string   `json:"state"`
			IPv4  []string `json:"ipv4"`
		} `json:"list"`
	}
	if err := json.Unmarshal([]byte(stdout), &data); err != nil {
		return nil, fmt.Errorf("failed to parse multipass list: %w", err)
	}

	vms := make([]VMInfo, 0, len(data.List))
	for _, vm := range data.List {
		ip := ""
		if len(vm.IPv4) > 0 {
			ip = vm.IPv4[0]
		}
		vms = append(vms, VMInfo{Name: vm.Name, State: vm.State, IPv4: ip})
	}
	return vms, nil
}

// ExecInVM runs a command inside a VM via `multipass exec`.
func (c *Client) ExecInVM(ctx context.Context, vmName string, cmd ...string) (string, error) {
	args := append([]string{"exec", vmName, "--"}, cmd...)
	return c.run(ctx, true, args...)
}

// ExecInVMRC runs a command inside a VM, returning (rc, stdout, stderr).
func (c *Client) ExecInVMRC(ctx context.Context, vmName string, cmd ...string) (int, string, string) {
	args := append([]string{"exec", vmName, "--"}, cmd...)
	return c.runRC(ctx, args...)
}

// WaitForCloudInit polls until cloud-init finishes inside the VM.
func (c *Client) WaitForCloudInit(ctx context.Context, name string, timeout time.Duration) error {
	poll := 5 * time.Second
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("cloud-init on VM %q not done after %s", name, timeout)
		default:
		}

		rc, stdout, _ := c.ExecInVMRC(ctx, name, "cloud-init", "status")
		if rc == 0 && strings.Contains(strings.ToLower(stdout), "done") {
			slog.Info("cloud-init finished", "vm", name)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}
