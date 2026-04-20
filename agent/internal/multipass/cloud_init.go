package multipass

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

var basePackages = []string{
	"curl",
	"iproute2",   // tc (traffic control) for network chaos
	"iptables",   // LitmusChaos network-level rules
	"conntrack",  // connection tracking for kube-proxy
	"open-iscsi", // optional: for persistent volumes
}

var chaosKernelModules = []string{
	"sch_netem", // network emulation (delay, loss, corruption)
	"sch_tbf",   // token bucket filter (rate limiting)
	"sch_sfq",   // stochastic fairness queueing
}

// cloudInitConfig is the structure serialized to YAML.
type cloudInitConfig struct {
	PackageUpdate bool     `yaml:"package_update"`
	Packages      []string `yaml:"packages"`
	RunCmd        []string `yaml:"runcmd"`
}

// BuildServerCloudInit generates cloud-init YAML for the k3s server node.
func BuildServerCloudInit() (string, error) {
	return buildCloudInit()
}

// BuildAgentCloudInit generates cloud-init YAML for k3s agent (worker) nodes.
func BuildAgentCloudInit() (string, error) {
	return buildCloudInit()
}

func buildCloudInit() (string, error) {
	var runcmd []string
	for _, mod := range chaosKernelModules {
		runcmd = append(runcmd, fmt.Sprintf("modprobe %s", mod))
	}
	for _, mod := range chaosKernelModules {
		runcmd = append(runcmd, fmt.Sprintf("echo \"%s\" >> /etc/modules-load.d/chaos.conf", mod))
	}

	cfg := cloudInitConfig{
		PackageUpdate: false,
		Packages:      append([]string{}, basePackages...),
		RunCmd:        runcmd,
	}

	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return "", fmt.Errorf("failed to generate cloud-init YAML: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("#cloud-config\n")
	sb.Write(data)
	return sb.String(), nil
}

// WriteCloudInitFile writes cloud-init content to a temp file in the given
// directory and returns the file path. Caller is responsible for cleanup.
func WriteCloudInitFile(dir, prefix, content string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create cloud-init dir: %w", err)
	}

	f, err := os.CreateTemp(dir, prefix+"*-cloud-init.yaml")
	if err != nil {
		return "", fmt.Errorf("create cloud-init temp file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(content); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("write cloud-init: %w", err)
	}

	// Ensure snap daemon can read the file
	if err := os.Chmod(f.Name(), 0o644); err != nil {
		os.Remove(f.Name())
		return "", fmt.Errorf("chmod cloud-init: %w", err)
	}

	abs, _ := filepath.Abs(f.Name())
	return abs, nil
}
