package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type Task struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	Status  string `json:"status"`
	Result  string `json:"result,omitempty"`
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

const cloudBackend = "https://geochat-backend-misx.onrender.com"
const ollamaURL = "http://localhost:11434/api/generate"
const modelName = "llama3" // O el modelo que tengas activo en tu Ollama local

func main() {
	log.Println("🟢 Worker local de GeoChat iniciado. Sintonizando con la Lattice en Render...")

	for {
		resp, err := http.Get(cloudBackend + "/api/node/pending")
		if err != nil {
			log.Printf("⚠️ Error conectando con la nube: %v. Reintentando en 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		var task Task
		if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
			resp.Body.Close()
			log.Printf("⚠️ Error decodificando tarea: %v", err)
			continue
		}
		resp.Body.Close()

		log.Printf("📥 Tarea recibida de la nube [%s]: %s", task.ID, task.Payload)

		localResult, err := consultarOllama(task.Payload)
		if err != nil {
			log.Printf("❌ Error procesando con Ollama local: %v", err)
			localResult = fmt.Sprintf("Error local: %v", err)
		} else {
			log.Printf("✨ Tarea procesada con éxito por Ollama local.")
		}

		err = enviarResultado(task.ID, localResult)
		if err != nil {
			log.Printf("⚠️ Error enviando resultado a la nube: %v", err)
		}
	}
}

func consultarOllama(prompt string) (string, error) {
	reqBody, _ := json.Marshal(OllamaRequest{
		Model:  modelName,
		Prompt: prompt,
		Stream: false,
	})

	resp, err := http.Post(ollamaURL, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var ollamaRes OllamaResponse
	if err := json.Unmarshal(body, &ollamaRes); err != nil {
		return "", err
	}

	return ollamaRes.Response, nil
}

func enviarResultado(id string, result string) error {
	resBody, _ := json.Marshal(Task{
		ID:     id,
		Result: result,
	})

	resp, err := http.Post(cloudBackend+"/api/node/result", "application/json", bytes.NewBuffer(resBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("servidor respondió con estado: %d", resp.StatusCode)
	}

	log.Printf("✅ Resultado de la tarea %s sincronizado con la nube.\n", id)
	return nil
}