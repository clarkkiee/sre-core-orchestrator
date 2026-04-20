package api

import (
	"encoding/json"
	"net/http"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/executor"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/models"
)

// TaskHandlers holds HTTP handlers that depend on the TaskExecutor.
type TaskHandlers struct {
	exec *executor.TaskExecutor
}

// NewTaskHandlers creates handlers wired to the given executor.
func NewTaskHandlers(exec *executor.TaskExecutor) *TaskHandlers {
	return &TaskHandlers{exec: exec}
}

// Register adds task routes to the mux.
func (h *TaskHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /tasks/provision", h.handleProvision)
	mux.HandleFunc("POST /tasks/teardown", h.handleTeardown)
	mux.HandleFunc("GET /tasks/{id}", h.handleGetTask)
	mux.HandleFunc("DELETE /tasks/{id}/cancel", h.handleCancelTask)
}

func (h *TaskHandlers) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req models.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.TaskID == "" || req.ClusterName == "" {
		writeError(w, http.StatusBadRequest, "task_id and cluster_name are required")
		return
	}

	if err := h.exec.SubmitProvision(req); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"task_id": req.TaskID,
		"status":  "accepted",
	})
}

func (h *TaskHandlers) handleTeardown(w http.ResponseWriter, r *http.Request) {
	var req models.TeardownRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.TaskID == "" || req.ClusterName == "" {
		writeError(w, http.StatusBadRequest, "task_id and cluster_name are required")
		return
	}

	if err := h.exec.SubmitTeardown(req); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"task_id": req.TaskID,
		"status":  "accepted",
	})
}

func (h *TaskHandlers) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task id is required")
		return
	}

	t := h.exec.GetTask(taskID)
	if t == nil {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}

	writeJSON(w, http.StatusOK, t)
}

func (h *TaskHandlers) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task id is required")
		return
	}

	if err := h.exec.Cancel(taskID); err != nil {
		if err.Error() == "task not found" {
			writeError(w, http.StatusNotFound, err.Error())
		} else {
			writeError(w, http.StatusConflict, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"task_id": taskID,
		"status":  "cancelled",
	})
}
