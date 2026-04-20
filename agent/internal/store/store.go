package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/models"
)

// TaskStore manages task state in memory with JSON file persistence.
type TaskStore struct {
	mu      sync.RWMutex
	tasks   map[string]*models.Task
	dataDir string // directory for task JSON files
}

// NewTaskStore creates a store backed by the given directory.
// On creation it loads any existing task files and marks orphaned
// running tasks as failed (the goroutine is gone after a restart).
func NewTaskStore(dataDir string) (*TaskStore, error) {
	tasksDir := filepath.Join(dataDir, "tasks")
	if err := os.MkdirAll(tasksDir, 0o755); err != nil {
		return nil, fmt.Errorf("create tasks dir: %w", err)
	}

	s := &TaskStore{
		tasks:   make(map[string]*models.Task),
		dataDir: tasksDir,
	}

	if err := s.loadFromDisk(); err != nil {
		return nil, err
	}
	return s, nil
}

// Create adds a new task. Returns an error if the task ID already exists.
func (s *TaskStore) Create(task *models.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[task.TaskID]; exists {
		return fmt.Errorf("task %s already exists", task.TaskID)
	}

	s.tasks[task.TaskID] = task
	return s.persistLocked(task)
}

// Get returns a task by ID. Returns nil if not found.
func (s *TaskStore) Get(taskID string) *models.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[taskID]
}

// Update atomically modifies a task via the provided function.
// The function receives the task pointer and mutates it in place.
func (s *TaskStore) Update(taskID string, fn func(*models.Task)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	fn(t)
	t.UpdatedAt = time.Now().UTC()
	return s.persistLocked(t)
}

// List returns all tasks.
func (s *TaskStore) List() []*models.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*models.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		result = append(result, t)
	}
	return result
}

// persistLocked writes a task to disk as JSON. Caller must hold the lock.
func (s *TaskStore) persistLocked(t *models.Task) error {
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", t.TaskID, err)
	}

	path := filepath.Join(s.dataDir, t.TaskID+".json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write task %s: %w", t.TaskID, err)
	}
	return nil
}

// loadFromDisk reads all .json files in the tasks directory.
// Tasks with status "running" are moved to "failed" since their goroutines
// are gone after a restart.
func (s *TaskStore) loadFromDisk() error {
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return nil // directory empty or doesn't exist yet
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		data, err := os.ReadFile(filepath.Join(s.dataDir, entry.Name()))
		if err != nil {
			slog.Warn("skip unreadable task file", "file", entry.Name(), "error", err)
			continue
		}

		var t models.Task
		if err := json.Unmarshal(data, &t); err != nil {
			slog.Warn("skip malformed task file", "file", entry.Name(), "error", err)
			continue
		}

		// Orphaned running tasks become failed after restart.
		if t.Status == models.TaskStatusRunning || t.Status == models.TaskStatusAccepted {
			t.Status = models.TaskStatusFailed
			t.Error = "agent restarted while task was in progress"
			now := time.Now().UTC()
			t.CompletedAt = &now
			t.UpdatedAt = now
			s.persistLocked(&t)
		}

		s.tasks[t.TaskID] = &t
		slog.Debug("loaded task from disk", "task_id", t.TaskID, "status", t.Status)
	}
	return nil
}
