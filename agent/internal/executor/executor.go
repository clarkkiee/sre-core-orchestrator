package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/k3s"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/models"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/store"
)

// TaskExecutor manages goroutines for async task execution.
type TaskExecutor struct {
	store   *store.TaskStore
	mp      *multipass.Client
	k3s     *k3s.Bootstrapper
	dataDir string

	mu      sync.Mutex
	cancels map[string]context.CancelFunc

	// provisionSem limits concurrent provisioning to 1.
	provisionSem chan struct{}
}

// NewTaskExecutor creates an executor wired to the given dependencies.
func NewTaskExecutor(s *store.TaskStore, mp *multipass.Client, k3sBoot *k3s.Bootstrapper, dataDir string) *TaskExecutor {
	return &TaskExecutor{
		store:        s,
		mp:           mp,
		k3s:          k3sBoot,
		dataDir:      dataDir,
		cancels:      make(map[string]context.CancelFunc),
		provisionSem: make(chan struct{}, 1),
	}
}

// SubmitProvision creates and starts a provision task.
func (e *TaskExecutor) SubmitProvision(req models.ProvisionRequest) error {
	task := &models.Task{
		TaskID:    req.TaskID,
		Type:      models.TaskTypeProvision,
		Status:    models.TaskStatusAccepted,
		Phase:     "",
		Progress:  0,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := e.store.Create(task); err != nil {
		return fmt.Errorf("task already exists: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.mu.Lock()
	e.cancels[req.TaskID] = cancel
	e.mu.Unlock()

	go e.runProvision(ctx, req)
	return nil
}

// SubmitTeardown creates and starts a teardown task.
func (e *TaskExecutor) SubmitTeardown(req models.TeardownRequest) error {
	task := &models.Task{
		TaskID:    req.TaskID,
		Type:      models.TaskTypeTeardown,
		Status:    models.TaskStatusAccepted,
		Phase:     "",
		Progress:  0,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := e.store.Create(task); err != nil {
		return fmt.Errorf("task already exists: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.mu.Lock()
	e.cancels[req.TaskID] = cancel
	e.mu.Unlock()

	go e.runTeardown(ctx, req)
	return nil
}

// Cancel requests cancellation of a running task.
func (e *TaskExecutor) Cancel(taskID string) error {
	t := e.store.Get(taskID)
	if t == nil {
		return fmt.Errorf("task not found")
	}

	if t.Status == models.TaskStatusCompleted || t.Status == models.TaskStatusFailed {
		return fmt.Errorf("task already finished")
	}

	e.mu.Lock()
	cancel, ok := e.cancels[taskID]
	e.mu.Unlock()

	if ok {
		cancel()
	}

	e.store.Update(taskID, func(t *models.Task) {
		t.Status = models.TaskStatusCancelled
		now := time.Now().UTC()
		t.CompletedAt = &now
	})
	return nil
}

// GetTask returns a task by ID from the store.
func (e *TaskExecutor) GetTask(taskID string) *models.Task {
	return e.store.Get(taskID)
}

// progress is a helper to update phase/progress on a task.
func (e *TaskExecutor) progress(taskID, phase string, pct int) {
	e.store.Update(taskID, func(t *models.Task) {
		t.Phase = phase
		t.Progress = pct
		t.Status = models.TaskStatusRunning
	})
	slog.Info("task progress", "task_id", taskID, "phase", phase, "progress", pct)
}

// finishOK marks a task as completed with a result.
func (e *TaskExecutor) finishOK(taskID string, result *models.TaskResult) {
	now := time.Now().UTC()
	e.store.Update(taskID, func(t *models.Task) {
		t.Status = models.TaskStatusCompleted
		t.Progress = 100
		t.Phase = "COMPLETE"
		t.Result = result
		t.CompletedAt = &now
	})

	e.mu.Lock()
	delete(e.cancels, taskID)
	e.mu.Unlock()
}

// finishErr marks a task as failed with an error message.
func (e *TaskExecutor) finishErr(taskID string, err error) {
	now := time.Now().UTC()
	e.store.Update(taskID, func(t *models.Task) {
		t.Status = models.TaskStatusFailed
		t.Error = err.Error()
		t.CompletedAt = &now
	})
	slog.Error("task failed", "task_id", taskID, "error", err)

	e.mu.Lock()
	delete(e.cancels, taskID)
	e.mu.Unlock()
}
