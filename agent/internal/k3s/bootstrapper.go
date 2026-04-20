package k3s

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
	"gopkg.in/yaml.v3"
)

const (
	k3sInstallURL   = "https://get.k3s.io"
	nodeReadyPoll   = 5 * time.Second
	k3sReadyTimeout = 120 * time.Second
)

// Bootstrapper installs k3s server and joins agents on Multipass VMs.
type Bootstrapper struct {
	mp *multipass.Client
}

// NewBootstrapper creates a Bootstrapper using the given multipass Client.
func NewBootstrapper(mp *multipass.Client) *Bootstrapper {
	return &Bootstrapper{mp: mp}
}

// ServerOpts configures the k3s server installation.
type ServerOpts struct {
	DisableTraefik bool
	TLSSAN         string
	NodeLabels     map[string]string
}

// InstallServer installs k3s server on a VM and returns (node_token, kubeconfig).
func (b *Bootstrapper) InstallServer(ctx context.Context, serverVM string, opts ServerOpts) (string, string, error) {
	execArgs := "server --write-kubeconfig-mode 644"
	if opts.DisableTraefik {
		execArgs += " --disable traefik"
	}
	if opts.TLSSAN != "" {
		execArgs += " --tls-san " + opts.TLSSAN
	}
	for k, v := range opts.NodeLabels {
		execArgs += fmt.Sprintf(" --node-label %s=%s", k, v)
	}

	installCmd := fmt.Sprintf(
		`curl -sfL %s | INSTALL_K3S_EXEC="%s" sh -`,
		k3sInstallURL, execArgs,
	)

	slog.Info("installing k3s server", "vm", serverVM)
	if _, err := b.mp.ExecInVM(ctx, serverVM, "bash", "-c", installCmd); err != nil {
		return "", "", fmt.Errorf("k3s server install failed on %s: %w", serverVM, err)
	}

	// Wait for k3s API server to be ready.
	if err := b.waitForK3sReady(ctx, serverVM, k3sReadyTimeout); err != nil {
		return "", "", err
	}

	// Retrieve node token.
	token, err := b.mp.ExecInVM(ctx, serverVM, "sudo", "cat", "/var/lib/rancher/k3s/server/node-token")
	if err != nil {
		return "", "", fmt.Errorf("read node-token: %w", err)
	}
	token = strings.TrimSpace(token)

	// Retrieve kubeconfig.
	kubeconfig, err := b.mp.ExecInVM(ctx, serverVM, "sudo", "cat", "/etc/rancher/k3s/k3s.yaml")
	if err != nil {
		return "", "", fmt.Errorf("read kubeconfig: %w", err)
	}

	slog.Info("k3s server installed", "vm", serverVM)
	return token, kubeconfig, nil
}

// JoinAgent joins a k3s agent (worker) to the server.
func (b *Bootstrapper) JoinAgent(ctx context.Context, agentVM, serverIP, token string, labels map[string]string) error {
	execArgs := "agent"
	for k, v := range labels {
		execArgs += fmt.Sprintf(" --node-label %s=%s", k, v)
	}

	installCmd := fmt.Sprintf(
		`curl -sfL %s | K3S_URL=https://%s:6443 K3S_TOKEN=%s INSTALL_K3S_EXEC="%s" sh -`,
		k3sInstallURL, serverIP, token, execArgs,
	)

	slog.Info("joining agent to cluster", "vm", agentVM, "server", serverIP)
	if _, err := b.mp.ExecInVM(ctx, agentVM, "bash", "-c", installCmd); err != nil {
		return fmt.Errorf("k3s agent join failed on %s: %w", agentVM, err)
	}
	slog.Info("agent joined", "vm", agentVM)
	return nil
}

// GetKubeconfig retrieves kubeconfig from the server VM and rewrites the server URL.
func (b *Bootstrapper) GetKubeconfig(ctx context.Context, serverVM, serverIP string) (string, error) {
	raw, err := b.mp.ExecInVM(ctx, serverVM, "sudo", "cat", "/etc/rancher/k3s/k3s.yaml")
	if err != nil {
		return "", fmt.Errorf("read kubeconfig: %w", err)
	}

	var kc map[string]any
	if err := yaml.Unmarshal([]byte(raw), &kc); err != nil {
		return "", fmt.Errorf("parse kubeconfig: %w", err)
	}

	newServer := fmt.Sprintf("https://%s:6443", serverIP)
	if clusters, ok := kc["clusters"].([]any); ok {
		for _, c := range clusters {
			if entry, ok := c.(map[string]any); ok {
				if cluster, ok := entry["cluster"].(map[string]any); ok {
					oldServer, _ := cluster["server"].(string)
					cluster["server"] = newServer
					cluster["insecure-skip-tls-verify"] = true
					delete(cluster, "certificate-authority-data")
					slog.Info("rewrote kubeconfig server", "old", oldServer, "new", newServer)
				}
			}
		}
	}

	out, err := yaml.Marshal(kc)
	if err != nil {
		return "", fmt.Errorf("marshal kubeconfig: %w", err)
	}
	return string(out), nil
}

// WaitForNodesReady waits until all k3s nodes report Ready.
func (b *Bootstrapper) WaitForNodesReady(ctx context.Context, serverVM string, expectedCount int, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("expected %d Ready nodes on %s but timed out after %s", expectedCount, serverVM, timeout)
		default:
		}

		rc, stdout, _ := b.mp.ExecInVMRC(ctx, serverVM, "sudo", "k3s", "kubectl", "get", "nodes", "--no-headers")
		if rc == 0 {
			readyCount := 0
			for _, line := range strings.Split(stdout, "\n") {
				if strings.TrimSpace(line) != "" && strings.Contains(line, " Ready ") {
					readyCount++
				}
			}
			if readyCount >= expectedCount {
				slog.Info("all nodes ready", "count", expectedCount, "vm", serverVM)
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nodeReadyPoll):
		}
	}
}

// EnsureNetemModule ensures the sch_netem kernel module is loaded on a VM.
func (b *Bootstrapper) EnsureNetemModule(ctx context.Context, vmName string) error {
	if _, err := b.mp.ExecInVM(ctx, vmName, "sudo", "modprobe", "sch_netem"); err != nil {
		return fmt.Errorf("modprobe sch_netem on %s: %w", vmName, err)
	}
	stdout, err := b.mp.ExecInVM(ctx, vmName, "lsmod")
	if err != nil {
		return fmt.Errorf("lsmod on %s: %w", vmName, err)
	}
	if !strings.Contains(stdout, "sch_netem") {
		return fmt.Errorf("sch_netem module not loaded on VM %q", vmName)
	}
	slog.Info("sch_netem module loaded", "vm", vmName)
	return nil
}

func (b *Bootstrapper) waitForK3sReady(ctx context.Context, vmName string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("k3s not ready on VM %q after %s", vmName, timeout)
		default:
		}

		rc, _, _ := b.mp.ExecInVMRC(ctx, vmName, "sudo", "k3s", "kubectl", "get", "nodes")
		if rc == 0 {
			slog.Info("k3s API server ready", "vm", vmName)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nodeReadyPoll):
		}
	}
}
