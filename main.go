package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type DocumentacionAnalisis struct {
	FilePath             string    `json:"file_path"`
	NombreArchivo        string    `json:"nombre_archivo"`
	Estado               string    `json:"estado"`
	ContenidoOriginal    string    `json:"contenido_original"`
	ResumenCambios       string    `json:"resumen_cambios"`
	TiempoProcesamiento  string    `json:"tiempo_procesamiento,omitempty"`
	PesoAuditado         string    `json:"peso_auditado,omitempty"`
	Timestamp            time.Time `json:"timestamp"`
	TieneRecomendaciones bool      `json:"tiene_recomendaciones"`
}

type TaskPayload struct {
	IDPadre       string   `json:"id_padre"`
	ListaArchivos []string `json:"lista_archivos"`
	IndiceInicio  int      `json:"indice_inicio"`
}

type Task struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "pending", "processing", "completed", "error"
	Result string `json:"result,omitempty"`
}

// Esta es la estructura que envías de vuelta a Linux lista para Ollama
type EstructuraParaOllama struct {
	Nombre string `json:"nombre"`
	Codigo string `json:"codigo"`
	Prompt string `json:"prompt"`
}

// Estructura que viaja por el buzón hacia la Linux local con todo el estado del checkpoint
type MensajeCheckpointBuzon struct {
	IDPadre        string    `json:"id_padre"`
	Total          int       `json:"total"`
	Procesados     int       `json:"procesados"`
	UltimoAuditado string    `json:"ultimo_auditado"`
	EstadoForzado  string    `json:"estado_forzado"`
	Timestamp      time.Time `json:"timestamp"`
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

// Variable global o almacenamiento temporal en memoria en Render para el buzón de checkpoints
var ultimoCheckpointEnviado MensajeCheckpointBuzon
var hayCheckpointPendiente bool
var muBuzonSync sync.Mutex

// Función en Render que actualiza el estado y lo deja disponible en el buzón de la nube
func actualizarCheckpointProgreso(idPadre string, total int, procesados int, ultimoAuditado string, estadoForzado string) {
    muBuzonSync.Lock()
    defer muBuzonSync.Unlock()

    estadoGlobalCalculado := estadoForzado
    if estadoGlobalCalculado == "" {
        if procesados >= total && total > 0 {
            estadoGlobalCalculado = "COMPLETADO"
        } else if procesados > 0 {
            estadoGlobalCalculado = "AUDITANDO"
        } else {
            estadoGlobalCalculado = "INICIAL"
        }
    }

    ultimoCheckpointEnviado = MensajeCheckpointBuzon{
        IDPadre:        idPadre,
        Total:          total,
        Procesados:     procesados,
        UltimoAuditado: ultimoAuditado,
        EstadoForzado:  estadoGlobalCalculado,
        Timestamp:      time.Now(),
    }
    hayCheckpointPendiente = true

    log.Printf("☁️ [RENDER - BUZÓN SYNC]: Checkpoint empaquetado para el Worker local -> Procesados: %d/%d | Estado: %s\n",
        procesados, total, estadoGlobalCalculado)
}

// Handler HTTP en Render expuesto en la ruta /api/auditoria/checkpoint-status que consulta el Worker local
func HandleObtenerCheckpointBuzon(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    muBuzonSync.Lock()
    defer muBuzonSync.Unlock()

    if !hayCheckpointPendiente {
        json.NewEncoder(w).Encode(map[string]string{"status": "sin_checkpoint_pendiente"})
        return
    }

    json.NewEncoder(w).Encode(ultimoCheckpointEnviado)
    hayCheckpointPendiente = false // Se consume y se limpia el buzón
}

func handleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var payload TaskPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		taskCounter++
		taskID := fmt.Sprintf("task-%d", taskCounter)
		mu.Unlock()

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":     taskID,
			"status": "pending",
		})
		log.Printf("📥 Tarea de auditoría recibida: %s (Total archivos: %d, Inicio: %d)\n", taskID, len(payload.ListaArchivos), payload.IndiceInicio)

		// ☁️ Ejecución en segundo plano en la nube de Render
		go ejecutarAuditoriaEnNube(taskID, payload)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	list := make([]map[string]interface{}, 0, len(tasks))
	for _, t := range tasks {
		list = append(list, map[string]interface{}{
			"id":     t.ID,
			"status": t.Status,
			"result": t.Result,
		})
	}
	json.NewEncoder(w).Encode(list)
}

func enviarSintesisAOllamaRemoto(nombreArchivo string, hallazgosConsolidados string, totalChunks int) (string, error) {
    horaSalida := time.Now()

    fmt.Printf("🧠 [☁️ RENDER - SÍNTESIS EMISOR]: Despachando síntesis global -> Archivo: %s | Partes procesadas: %d | Salida: %s\n",
        nombreArchivo, totalChunks, horaSalida.Format("15:04:05.000"))

    urlOllamaNube := os.Getenv("OLLAMA_CLOUD_URL")
    if urlOllamaNube == "" {
        urlOllamaNube = "http://localhost:11434/api/generate"
    }

    promptSintesis := fmt.Sprintf(
        `[SÍNTESIS GLOBAL Y CONSOLIDACIÓN DE FRAGMENTOS] 
        Actúa como Arquitecto Master de GeoChat sintonizado a 432Hz. 
        Estás recibiendo las respuestas y dictámenes parciales que TÚ MISMO emitiste al auditar por separado las %d partes del archivo "%s". 

        Aquí tienes todos esos fragmentos analizados:
        %s

        Tu tarea es interpretar todas estas respuestas parciales y unificarlas en una sola respuesta coherente para todo el archivo completo. No uses JSON. Devuelve texto plano estructurado estrictamente en estas 3 secciones claras:

        1) DICTAMEN DE SALUD GLOBAL: Evaluación general unificada del archivo completo basándote en lo que analizaron tus partes.
        2) ARQUITECTURA CONSOLIDADA: Cómo queda estructurado y funcionando el archivo en su totalidad.
        3) CONCLUSIONES Y OBSERVACIONES FINALES: Notas técnicas definitivas para el registro del nodo.`,
        totalChunks,
        nombreArchivo,
        hallazgosConsolidados,
    )

    payload := map[string]interface{}{
        "model":  "gemma2:2b",
        "prompt": promptSintesis,
        "stream": false,
        "options": map[string]interface{}{
            "num_ctx": 8192,
        },
    }

    jsonBody, err := json.Marshal(payload)
    if err != nil {
        return "", err
    }

    client := &http.Client{Timeout: 900 * time.Second}
    resp, err := client.Post(urlOllamaNube, "application/json", bytes.NewBuffer(jsonBody))
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    var result map[string]interface{}
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return "", err
    }

    if responseText, ok := result["response"].(string); ok {
        return responseText, nil
    }

    return "Síntesis global completada sin respuesta de texto explícita.", nil
}

func ejecutarAuditoriaEnNube(taskID string, payload TaskPayload) {
	total := len(payload.ListaArchivos)
	if total == 0 {
		log.Println("⚠️ [RENDER]: La lista de archivos está vacía.")
		return
	}

	var documentosAuditados []DocumentacionAnalisis

	for i := payload.IndiceInicio; i < total; i++ {
		currentFilePath := payload.ListaArchivos[i]

		log.Printf("🔄 [☁️ RENDER - AUDITORÍA]: Iniciando chunking y auditoría profunda del archivo (%d/%d): %s\n", i+1, total, currentFilePath)

		// 1️⃣ LEEMOS EL CÓDIGO REAL DEL ARCHIVO
		contenidoBytes, errRead := os.ReadFile(currentFilePath)
		if errRead != nil {
			log.Printf("❌ [RENDER]: Error leyendo %s para chunks: %v\n", currentFilePath, errRead)
			continue
		}
		contenidoCodigo := string(contenidoBytes)

		// 2️⃣ Partimos el código real en líneas (bloques de 40 para aliviar al modelo)
		lineas := strings.Split(contenidoCodigo, "\n")
		tamanoChunk := 40
		var hallazgosConsolidados strings.Builder

		totalChunks := (len(lineas) + tamanoChunk - 1) / tamanoChunk
		if totalChunks == 0 {
			totalChunks = 1
		}

		hallazgosConsolidados.WriteString(fmt.Sprintf("=== REPORTE DE HALLAZGOS PARCIALES (RENDER): %s ===\n", currentFilePath))

		exitoTotal := true
		tiempoInicioTotal := time.Now()
		var pesoTotalBytes int64 = 0

		for j := 0; j < len(lineas); j += tamanoChunk {
			fin := j + tamanoChunk
			if fin > len(lineas) {
				fin = len(lineas)
			}

			// 3️⃣ CORTAMOS EL CHUNK DE CÓDIGO Y SE LO MANDAMOS A LA IA EN LA NUBE
			chunkCodigo := strings.Join(lineas[j:fin], "\n")
			numParte := (j / tamanoChunk) + 1

			pesoTotalBytes += int64(len(chunkCodigo))

			log.Printf("📦 [☁️ RENDER - CHUNKING]: Procesando parte %d de %d del archivo %s...\n", numParte, totalChunks, currentFilePath)

			// 🛡️ Mecanismo de reintento inteligente ante cortes abruptos
			var respuestaChunk string
			var errOllama error
			maxIntentos := 2

			for intento := 1; intento <= maxIntentos; intento++ {
				respuestaChunk, errOllama = enviarAOllamaRemoto(chunkCodigo)
				if errOllama == nil {
					break
				}
				log.Printf("⚠️ [AVISO IA RENDER]: Reintentando parte %d/%d (Intento %d/%d)...\n", numParte, totalChunks, intento, maxIntentos)
				time.Sleep(1 * time.Second)
			}

			if errOllama != nil {
				log.Printf("❌ [ERROR IA RENDER CRÍTICO]: Falló definitivamente la parte %d/%d: %v\n", numParte, totalChunks, errOllama)
				exitoTotal = false
				break
			}

			hallazgosConsolidados.WriteString(fmt.Sprintf("\n[BLOQUE %d / %d]\n%s\n----------------------------------------\n",
				numParte,
				totalChunks,
				respuestaChunk,
			))
		}

		tiempoTotalProceso := time.Since(tiempoInicioTotal)

		// Verificación y ejecución del paso de Síntesis Global
		if !exitoTotal || hallazgosConsolidados.Len() == 0 {
			log.Printf("⚠️ [ADVERTENCIA RENDER]: La auditoría por chunks se interrumpió en %s\n", currentFilePath)
			documentosAuditados = append(documentosAuditados, DocumentacionAnalisis{
				FilePath:          currentFilePath,
				NombreArchivo:     currentFilePath,
				Estado:            "ERROR_SINTESIS",
				ContenidoOriginal: hallazgosConsolidados.String(),
				ResumenCambios:    "⚠️ Auditoría incompleta por corte en fragmentos de IA en Render.",
				Timestamp:         time.Now(),
			})
		} else {
			log.Println("🧠 [☁️ RENDER - SÍNTESIS GLOBAL]: Alimentando al modelo con las respuestas parciales para obtener el veredicto final...")

			dictamenFinal, errSintesis := enviarSintesisAOllamaRemoto(
				currentFilePath,
				hallazgosConsolidados.String(),
				totalChunks,
			)

			if errSintesis != nil {
				log.Printf("⚠️ [RENDER - ERROR SÍNTESIS]: No se pudo consolidar la síntesis final: %v\n", errSintesis)
				documentosAuditados = append(documentosAuditados, DocumentacionAnalisis{
					FilePath:          currentFilePath,
					NombreArchivo:     currentFilePath,
					Estado:            "ERROR_SINTESIS",
					ContenidoOriginal: hallazgosConsolidados.String(),
					ResumenCambios:    hallazgosConsolidados.String(),
					Timestamp:         time.Now(),
				})
			} else {
				log.Println("☁️ - ✅ [RENDER - ÉXITO TOTAL]: Síntesis global completada en la nube.")

				nuevoDoc := DocumentacionAnalisis{
					FilePath:             currentFilePath,
					NombreArchivo:        currentFilePath,
					Estado:               "AUDITADO_CON_IA",
					ContenidoOriginal:    hallazgosConsolidados.String(),
					ResumenCambios:       dictamenFinal,
					TiempoProcesamiento:  tiempoTotalProceso.String(),
					PesoAuditado:         fmt.Sprintf("%d bytes", pesoTotalBytes),
					Timestamp:            time.Now(),
					TieneRecomendaciones: true,
				}
				documentosAuditados = append(documentosAuditados, nuevoDoc)
			}
		}

		// 💾 Guardado global y actualización del checkpoint local/remoto
		//errGlobal := ArchivarAuditoriaGlobal(payload.IDPadre, documentosAuditados)
		//if errGlobal != nil {
		//	log.Printf("❌ [RENDER]: Error al archivar auditoría global: %v\n", errGlobal)
		//}

		actualizarCheckpointProgreso(payload.IDPadre, total, i+1, currentFilePath, "AUDITANDO")
	}

	actualizarCheckpointProgreso(payload.IDPadre, total, total, "", "COMPLETADO")
	log.Printf("✨ [RENDER]: Tarea %s completada al 100%% en la nube.\n", taskID)
}

func enviarAOllamaRemoto(codigo string) (string, error) {
	// URL o endpoint del servicio de IA / Ollama configurado en la nube de Render
	urlOllamaNube := os.Getenv("OLLAMA_CLOUD_URL")
	if urlOllamaNube == "" {
		urlOllamaNube = "http://localhost:11434/api/generate" // O la URL de tu servicio de IA remoto
	}

	payload := map[string]interface{}{
		"model":  "llama3", // O el modelo que utilices
		"prompt": "Realiza la auditoría y análisis de este bloque de código:\n" + codigo,
		"stream": false,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	resp, err := http.Post(urlOllamaNube, "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error en IA remota, status: %d", resp.StatusCode)
	}

	var resultado struct {
		Response string `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resultado); err != nil {
		return "", err
	}

	return resultado.Response, nil
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
		return
	}
	http.Error(w, "Task not found", http.StatusNotFound)
}
