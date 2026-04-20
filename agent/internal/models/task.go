package models

import "time"

// TaskStatus represents the lifecycle state of a task.
type TaskStatus string

const (
	TaskStatusAccepted  TaskStatus = "accepted"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskType represents the kind of operation.
type TaskType string

const (
	TaskTypeProvision TaskType = "provision"
	TaskTypeTeardown  TaskType = "teardown"
)

// Task tracks the lifecycle of an async operation.
type Task struct {
	TaskID      string      `json:"task_id"`
	Type        TaskType    `json:"type"`
	Status      TaskStatus  `json:"status"`
	Phase       string      `json:"phase"`
	Progress    int         `json:"progress"`
	Result      *TaskResult `json:"result"`
	Error       string      `json:"error,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	CompletedAt *time.Time  `json:"completed_at,omitempty"`
}

// TaskResult holds the output of a completed provision task.
type TaskResult struct {
	KubeconfigContent string `json:"kubeconfig_content"`
	ControlPlaneIP    string `json:"control_plane_ip"`
}
