package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	Status  string `json:"status"` // "pending", "processing", "completed"
	Result  string `json:"result,omitempty"`
}

var (
	tasks       = make(map[string]Task)
	mu          sync.Mutex
	taskCounter = 0
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/api/tasks", handleTasks)
	http.HandleFunc("/api/node/pending", handleGetPendingTask)
	http.HandleFunc("/api/node/result", handlePostTaskResult)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "GeoChat Backend Coordinator is running securely.")
	})

	log.Printf("🚀 Backend coordinador activo en puerto %s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var t Task
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		taskCounter++
		t.ID = fmt.Sprintf("task-%d", taskCounter)
		t.Status = "pending"
		tasks[t.ID] = t
		mu.Unlock()

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(t)
		log.Printf("📥 Tarea creada: %s\n", t.ID)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	list := make([]Task, 0, len(tasks))
	for _, t := range tasks {
		list = append(list, t)
	}
	json.NewEncoder(w).Encode(list)
}

func handleGetPendingTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mu.Lock()
	defer mu.Unlock()

	for id, t := range tasks {
		if t.Status == "pending" {
			t.Status = "processing"
			tasks[id] = t
			json.NewEncoder(w).Encode(t)
			log.Printf("📤 Tarea %s enviada al nodo local para procesamiento\n", id)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func handlePostTaskResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var res Task
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if t, exists := tasks[res.ID]; exists {
		t.Status = "completed"
		t.Result = res.Result
		tasks[res.ID] = t
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"status":"success"}`)
		log.Printf("✅ Tarea %s completada por el nodo local\n", res.ID)
		return
	}
	http.Error(w, "Task not found", http.StatusNotFound)
}

