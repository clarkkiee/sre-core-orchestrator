package models

// ProvisionRequest is the payload for POST /tasks/provision.
type ProvisionRequest struct {
	TaskID         string `json:"task_id"`
	ClusterName    string `json:"cluster_name"`
	WorkerCount    int    `json:"worker_count"`
	VMCPUs         int    `json:"vm_cpus"`
	VMMemory       string `json:"vm_memory"`
	VMDisk         string `json:"vm_disk"`
	VMImage        string `json:"vm_image"`
	DisableTraefik bool   `json:"disable_traefik"`
}
