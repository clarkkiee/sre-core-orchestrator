package executor

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/models"
)

// runTeardown executes the teardown flow for a cluster.
func (e *TaskExecutor) runTeardown(ctx context.Context, req models.TeardownRequest) {
	taskID := req.TaskID

	e.progress(taskID, "DELETING_VMS", 10)

	if err := e.doTeardown(ctx, req); err != nil {
		if ctx.Err() != nil {
			return
		}
		e.finishErr(taskID, err)
		return
	}

	e.finishOK(taskID, &models.TaskResult{})
}

func (e *TaskExecutor) doTeardown(ctx context.Context, req models.TeardownRequest) error {
	clusterName := req.ClusterName
	workerCount := req.WorkerCount

	// Build list of all VM names.
	vms := []string{clusterName + "-server"}
	for i := 1; i <= workerCount; i++ {
		vms = append(vms, fmt.Sprintf("%s-w%d", clusterName, i))
	}

	for _, vm := range vms {
		if err := ctx.Err(); err != nil {
			return err
		}

		if e.mp.VMExists(ctx, vm) {
			if err := e.mp.DeleteVM(ctx, vm); err != nil {
				return fmt.Errorf("delete VM %s: %w", vm, err)
			}
			slog.Info("deleted VM", "name", vm)
		}
	}

	return nil
}
