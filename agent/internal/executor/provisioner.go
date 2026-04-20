package executor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/k3s"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/models"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
)

// runProvision executes the full provisioning flow for a cluster.
func (e *TaskExecutor) runProvision(ctx context.Context, req models.ProvisionRequest) {
	taskID := req.TaskID

	// Limit to one concurrent provision.
	select {
	case e.provisionSem <- struct{}{}:
		defer func() { <-e.provisionSem }()
	default:
		e.finishErr(taskID, fmt.Errorf("another provisioning task is already running"))
		return
	}

	result, err := e.doProvision(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return // cancelled — status already set
		}
		e.finishErr(taskID, err)
		return
	}

	e.finishOK(taskID, result)
}

func (e *TaskExecutor) doProvision(ctx context.Context, req models.ProvisionRequest) (*models.TaskResult, error) {
	taskID := req.TaskID
	clusterName := req.ClusterName
	workerCount := req.WorkerCount

	if workerCount <= 0 {
		workerCount = 2
	}

	cpus := req.VMCPUs
	if cpus <= 0 {
		cpus = 2
	}
	memory := req.VMMemory
	if memory == "" {
		memory = "2G"
	}
	disk := req.VMDisk
	if disk == "" {
		disk = "10G"
	}
	image := req.VMImage
	if image == "" {
		image = "22.04"
	}

	serverVM := clusterName + "-server"

	// Write cloud-init files to a non-hidden directory under $HOME.
	// Multipass runs in a snap sandbox — the `home` interface grants
	// access to $HOME but NOT hidden directories (~/.*) and NOT /tmp.
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}
	cloudInitDir := homeDir + "/orchestrator-agent-cloud-init"

	// Phase: BUILDING_CONFIG (5%)
	e.progress(taskID, "BUILDING_CONFIG", 5)

	serverCI, err := multipass.BuildServerCloudInit()
	if err != nil {
		return nil, fmt.Errorf("build server cloud-init: %w", err)
	}
	agentCI, err := multipass.BuildAgentCloudInit()
	if err != nil {
		return nil, fmt.Errorf("build agent cloud-init: %w", err)
	}

	serverCIPath, err := multipass.WriteCloudInitFile(cloudInitDir, "server-", serverCI)
	if err != nil {
		return nil, fmt.Errorf("write server cloud-init: %w", err)
	}
	defer os.Remove(serverCIPath)

	agentCIPath, err := multipass.WriteCloudInitFile(cloudInitDir, "agent-", agentCI)
	if err != nil {
		return nil, fmt.Errorf("write agent cloud-init: %w", err)
	}
	defer os.Remove(agentCIPath)

	// Phase: CREATING_SERVER_VM (15%)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.progress(taskID, "CREATING_SERVER_VM", 15)

	err = e.mp.LaunchVM(ctx, serverVM, multipass.LaunchOpts{
		CPUs:          cpus,
		Memory:        memory,
		Disk:          disk,
		CloudInitFile: serverCIPath,
		Image:         image,
	})
	if err != nil {
		return nil, fmt.Errorf("launch server VM: %w", err)
	}

	if err := e.mp.WaitForCloudInit(ctx, serverVM, 5*time.Minute); err != nil {
		return nil, fmt.Errorf("server cloud-init: %w", err)
	}

	serverIP, err := e.mp.GetVMIP(ctx, serverVM)
	if err != nil {
		return nil, fmt.Errorf("get server IP: %w", err)
	}

	// Phase: BOOTSTRAPPING_K3S (30%)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.progress(taskID, "BOOTSTRAPPING_K3S", 30)

	token, _, err := e.k3s.InstallServer(ctx, serverVM, k3s.ServerOpts{
		DisableTraefik: req.DisableTraefik,
		TLSSAN:         serverIP,
		NodeLabels:     map[string]string{"ingress-ready": "true", "master": "true"},
	})
	if err != nil {
		return nil, fmt.Errorf("bootstrap k3s server: %w", err)
	}

	// Phase: JOINING_WORKERS (45%)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.progress(taskID, "JOINING_WORKERS", 45)

	var wg sync.WaitGroup
	errCh := make(chan error, workerCount)

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			workerVM := fmt.Sprintf("%s-w%d", clusterName, idx)

			if err := e.mp.LaunchVM(ctx, workerVM, multipass.LaunchOpts{
				CPUs:          cpus,
				Memory:        memory,
				Disk:          disk,
				CloudInitFile: agentCIPath,
				Image:         image,
			}); err != nil {
				errCh <- fmt.Errorf("launch worker %s: %w", workerVM, err)
				return
			}

			if err := e.mp.WaitForCloudInit(ctx, workerVM, 5*time.Minute); err != nil {
				errCh <- fmt.Errorf("worker %s cloud-init: %w", workerVM, err)
				return
			}

			if err := e.k3s.JoinAgent(ctx, workerVM, serverIP, token, map[string]string{"worker": "true"}); err != nil {
				errCh <- fmt.Errorf("join worker %s: %w", workerVM, err)
				return
			}

			slog.Info("worker joined", "vm", workerVM)
		}(i)
	}

	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return nil, err
	}

	// Phase: EXPORTING_KUBECONFIG (55%)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.progress(taskID, "EXPORTING_KUBECONFIG", 55)

	expectedNodes := 1 + workerCount
	if err := e.k3s.WaitForNodesReady(ctx, serverVM, expectedNodes, 3*time.Minute); err != nil {
		return nil, fmt.Errorf("wait for nodes: %w", err)
	}

	kubeconfig, err := e.k3s.GetKubeconfig(ctx, serverVM, serverIP)
	if err != nil {
		return nil, fmt.Errorf("get kubeconfig: %w", err)
	}

	// Ensure sch_netem on all VMs.
	allVMs := []string{serverVM}
	for i := 1; i <= workerCount; i++ {
		allVMs = append(allVMs, fmt.Sprintf("%s-w%d", clusterName, i))
	}
	for _, vm := range allVMs {
		if err := e.k3s.EnsureNetemModule(ctx, vm); err != nil {
			slog.Warn("netem module warning", "vm", vm, "error", err)
		}
	}

	return &models.TaskResult{
		KubeconfigContent: kubeconfig,
		ControlPlaneIP:    serverIP,
	}, nil
}
