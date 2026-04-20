package api

import (
	"net/http"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
)

// VMHandlers holds HTTP handlers for VM operations.
type VMHandlers struct {
	mp *multipass.Client
}

// NewVMHandlers creates handlers wired to the given multipass client.
func NewVMHandlers(mp *multipass.Client) *VMHandlers {
	return &VMHandlers{mp: mp}
}

// Register adds VM routes to the mux.
func (h *VMHandlers) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /vms", h.handleListVMs)
	mux.HandleFunc("GET /vms/{name}", h.handleGetVM)
}

func (h *VMHandlers) handleListVMs(w http.ResponseWriter, r *http.Request) {
	vms, err := h.mp.ListVMs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list VMs: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"vms": vms})
}

func (h *VMHandlers) handleGetVM(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}

	if !h.mp.VMExists(r.Context(), name) {
		writeError(w, http.StatusNotFound, "vm not found")
		return
	}

	ip, err := h.mp.GetVMIP(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get VM info: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"name":  name,
		"state": "Running",
		"ipv4":  ip,
	})
}
