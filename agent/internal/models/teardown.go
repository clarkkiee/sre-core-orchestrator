package models

// TeardownRequest is the payload for POST /tasks/teardown.
type TeardownRequest struct {
	TaskID      string `json:"task_id"`
	ClusterName string `json:"cluster_name"`
	WorkerCount int    `json:"worker_count"`
}
