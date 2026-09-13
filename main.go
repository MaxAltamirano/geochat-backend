package main

import (
	//"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
    "bytes"
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



type Task struct {
	ID     string `json:"id"`
	Status string `json:"status"` // "pending", "processing", "completed", "error"
	Result string `json:"result,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// Esta es la estructura que envías de vuelta a Linux lista para Ollama
type EstructuraParaOllama struct {
	Nombre string `json:"nombre"`
	Codigo string `json:"codigo"`
	Prompt string `json:"prompt"`
}

// Estructura que viaja por el buzón hacia la Linux local con todo el estado del checkpoint
type MensajeCheckpointBuzon struct {
    TipoAccion         string    `json:"tipo_accion"`          // "CHECKPOINT" o "AUDITAR_CHUNK"
    IDPadre            string    `json:"id_padre"`             // Identificador de la tarea global
    FilePath           string    `json:"file_path,omitempty"`           // Ruta o nombre del archivo actual
    Contenido          string    `json:"contenido,omitempty"`           // Contenido para guardar en Vault
    ContenidoCodigo    string    `json:"contenido_codigo,omitempty"`    // Fragmento de código puro para Ollama local
    Total              int       `json:"total"`                // Total de elementos de la tarea
    Procesados         int       `json:"procesados"`           // Conteo de elementos listos
    UltimoAuditado     string    `json:"ultimo_auditado"`      // Nombre del último archivo procesado
    EstadoForzado      string    `json:"estado_forzado"`       // Estado global forzado (ej. "COMPLETADO")
    TamanioBytes       int64     `json:"tamanio_bytes"`        // Peso para telemetría de red
    TimestampInyeccion time.Time `json:"timestamp_inyeccion"`  // Hora de salida desde la nube
    TimestampRespuesta time.Time `json:"timestamp_respuesta"`  // Hora de llegada / respuesta
    Timestamp          time.Time `json:"timestamp"`            // Sello de tiempo general
	Documentos           []DocumentacionAnalisis   `json:"documentos,omitempty"`
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

var (
    muBuzonResultados     sync.Mutex
    ultimaTaskEntrada     Task
    hayResultadoPendiente bool
    //muAuditoria        sync.Mutex
    //ultimoPaqueteListo []byte
)

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
		TipoAccion:     "CHECKPOINT", // 👈 Acá le avisas explícitamente al worker que esto es un checkpoint
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

    // Devolvemos el estado actual de forma persistente para que el frontend lo lea con fluidez
    json.NewEncoder(w).Encode(ultimoCheckpointEnviado)
}

type ArchivoFisicoPayload struct {
    Ruta      string `json:"ruta"`
    Contenido string `json:"contenido"`
}

type TaskPayload struct {
    IDPadre         string                 `json:"id_proceso"`
    ArchivosFisicos []ArchivoFisicoPayload `json:"archivos_fisicos"` // 👈 Coincide exactamente con el JSON enviado
    IndiceInicio    int                    `json:"indice_inicio"`
    Total           int                    `json:"total"`
}
// 📦 Declaración global a nivel de paquete (fuera de cualquier función)
var colaTareasGlobal TaskPayload

func handleTasks(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    
    if r.Method == http.MethodPost {
        var payload TaskPayload
        if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
            http.Error(w, err.Error(), http.StatusBadRequest)
            return
        }

        // Asignación a la variable global declarada arriba
        colaTareasGlobal = payload

        mu.Lock()
        taskCounter++
        taskID := fmt.Sprintf("task-%d", taskCounter)
        mu.Unlock()

        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]interface{}{
            "id":      taskID,
            "status":  "pending",
            "mensaje": "Tareas recibidas en la nube",
        })
        
        log.Printf("📥 [RENDER COORDINATOR]: Tarea de auditoría recibida: %s (ID Padre: %s, Total archivos: %d, Inicio: %d)\n", taskID, payload.IDPadre, len(payload.ArchivosFisicos), payload.IndiceInicio)
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

func enviarSintesisAOllamaRemoto(taskID string, nombreArchivo string, hallazgosConsolidados string, totalChunks int) (string, error) {
    horaSalida := time.Now()

    fmt.Printf("🧠 [☁️ RENDER - SÍNTESIS EMISOR]: Empaquetando síntesis global en el buzón -> Archivo: %s | Partes: %d | Salida: %s\n",
        nombreArchivo, totalChunks, horaSalida.Format("15:04:05.000"))

    // 1. Render arma el prompt maestro con toda la sintergia
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

    // 2. Depositamos la orden en el buzón de salida
    muBuzonSync.Lock()
    ultimoCheckpointEnviado = MensajeCheckpointBuzon{
        TipoAccion:         "SINTESIS_GLOBAL",
        IDPadre:            taskID,
        FilePath:           nombreArchivo,
        ContenidoCodigo:    promptSintesis,
        Total:              totalChunks,
        TimestampInyeccion: time.Now(),
        Timestamp:          time.Now(),
    }
    hayCheckpointPendiente = true
    muBuzonSync.Unlock()

    log.Printf("☁️ [RENDER - BUZÓN PULL]: Orden de Síntesis depositada para el taskID [%s]. Esperando que el Worker local la procese y devuelva...\n", taskID)

    // 3️⃣ BUCLE DE ESPERA INTELIGENTE (POLLING) CON VALIDACIÓN ESTRICTA DE taskID
    timeout := time.After(900 * time.Second) // 15 minutos para que Ollama procese la síntesis global
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-timeout:
            return "", fmt.Errorf("timeout: la Linux local no devolvió el dictamen de síntesis global para el taskID [%s] a tiempo", taskID)
        case <-ticker.C:
            muBuzonResultados.Lock()
            if hayResultadoPendiente {
                // 🔍 Validación estricta: nos aseguramos de que el resultado pertenezca exactamente a esta tarea de síntesis
                if ultimaTaskEntrada.ID == taskID {
                    dictamenFinal := ultimaTaskEntrada.Result
                    
                    // Limpiamos el buzón y liberamos el mutex
                    hayResultadoPendiente = false
                    muBuzonResultados.Unlock()

                    log.Printf("✨ [RENDER - BUZÓN]: ¡Dictamen de síntesis global capturado y validado para el taskID [%s]!\n", taskID)
                    return dictamenFinal, nil
                }
            }
            muBuzonResultados.Unlock()
        }
    }
}

func ArchivarAuditoriaGlobalRemoto(idSesion string, documentos []DocumentacionAnalisis) error {
    muBuzonSync.Lock()
    defer muBuzonSync.Unlock()

    // Extraemos de forma segura el FilePath del primer documento si existe
    var pathArchivo string
    if len(documentos) > 0 {
        pathArchivo = documentos[0].FilePath
    }

    // Empaquetamos la acción para que el worker local la retire en su próximo Pull/Ticker
    ultimoCheckpointEnviado = MensajeCheckpointBuzon{
        TipoAccion:  "ARCHIVAR_GLOBAL",
        IDPadre:     idSesion,
        FilePath:    pathArchivo, // 👈 Usamos el path extraído del documento
        Documentos:  documentos,        
        Timestamp:   time.Now(),
    }
    hayCheckpointPendiente = true

    log.Printf("☁️ [RENDER - BUZÓN SYNC]: Paquete de Archivo Global empaquetado para el Worker local -> Sesión: %s (%d documentos)\n",
        idSesion, len(documentos))

    return nil
}

func ejecutarAuditoriaEnNube(taskID string, payload TaskPayload) {
    total := len(payload.ArchivosFisicos)
    if total == 0 {
        log.Println("⚠️ [RENDER]: La lista de archivos físicos está vacía.")
        return
    }

    var documentosAuditados []DocumentacionAnalisis

    for i := payload.IndiceInicio; i < total; i++ {
        archivoActual := payload.ArchivosFisicos[i]
        currentFilePath := archivoActual.Ruta
        contenidoCodigo := archivoActual.Contenido

        log.Printf("🔄 [☁️ RENDER - AUDITORÍA]: Iniciando chunking y auditoría profunda del archivo (%d/%d): %s\n", i+1, total, currentFilePath)

        // 1️⃣ Notificar inicio de archivo en el buzón de inmediato
        actualizarCheckpointProgreso(
            payload.IDPadre, 
            total, 
            i, 
            fmt.Sprintf("%s (Iniciando...)", currentFilePath), 
            "AUDITANDO",
        )

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

            // 🚀 NOTIFICAR AL BUZÓN EN TIEMPO REAL POR CADA CHUNK
            actualizarCheckpointProgreso(
                payload.IDPadre, 
                total, 
                i, 
                fmt.Sprintf("%s [Parte %d/%d]", currentFilePath, numParte, totalChunks), 
                "AUDITANDO",
            )

            // 🔍 LOG 1: Sabiendo exactamente qué chunk sale y su tamaño
            log.Printf("📡 [RENDER -> OLLAMA]: Enviando parte %d/%d de '%s' (Tamaño: %d bytes)...", numParte, totalChunks, currentFilePath, len(chunkCodigo))
            log.Printf("[🔵 AUDITORÍA]: Procesando parte %d de %d del archivo %s...\n", numParte, totalChunks, currentFilePath)
            tiempoInicioChunk := time.Now()
            
            // 🛡️ Mecanismo de reintento inteligente ante cortes abruptos
            var respuestaChunk string
            var errOllama error
            maxIntentos := 2

            for intento := 1; intento <= maxIntentos; intento++ {
                respuestaChunk, errOllama = enviarAOllamaRemoto(taskID, chunkCodigo)
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

            // 🔍 LOG 2: Confirmando que la IA respondió este chunk específico y cuánto demoró
            log.Printf("✅ [OLLAMA -> RENDER]: Parte %d/%d completada en %v (Recibidos: %d caracteres)\n", 
                numParte, totalChunks, time.Since(tiempoInicioChunk), len(respuestaChunk))

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
            log.Println("[🔵 AUDITORÍA]:- SÍNTESIS GLOBAL]: Alimentando al modelo con las respuestas parciales para obtener el veredicto final...")

            // Notificando fase de síntesis final en el buzón
            actualizarCheckpointProgreso(
                payload.IDPadre, 
                total, 
                i, 
                fmt.Sprintf("%s (Consolidando Síntesis...)", currentFilePath), 
                "AUDITANDO",
            )

            dictamenFinal, errSintesis := enviarSintesisAOllamaRemoto(
                taskID,
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
                log.Println("[🔵 AUDITORÍA]:☁️ - ✅ [RENDER - ÉXITO TOTAL]: Síntesis global completada en la nube.")

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

        // 💾 Guardado global y actualización del checkpoint con el archivo completado (i+1)
        errGlobal := ArchivarAuditoriaGlobalRemoto(payload.IDPadre, documentosAuditados)
        if errGlobal != nil {
            log.Printf("❌ [RENDER]: Error al archivar auditoría global: %v\n", errGlobal)
        }

        actualizarCheckpointProgreso(payload.IDPadre, total, i+1, currentFilePath, "AUDITANDO")
    }

    actualizarCheckpointProgreso(payload.IDPadre, total, total, "", "COMPLETADO")
    log.Printf("✨ [RENDER]: Tarea %s completada al 100%% en la nube.\n", taskID)
}

func enviarAOllamaRemoto(taskID string, chunkCodigo string) (string, error) {
    pesoBytes := int64(len(chunkCodigo))

    payload := MensajeCheckpointBuzon{
        TipoAccion:         "AUDITAR_CHUNK",
        IDPadre:            taskID,
        ContenidoCodigo:    chunkCodigo,
        TamanioBytes:       pesoBytes,
        TimestampInyeccion: time.Now(),
        Timestamp:          time.Now(),
    }

    bodyBytes, err := json.Marshal(payload)
    if err != nil {
        return "", err
    }

    // 1️⃣ Envíamos el chunk por HTTP POST al Córtex Buzón en Render para que el worker local lo tome
    urlBuzon := "https://geochat-buzon.onrender.com/api/auditoria/depositar-chunk"
    req, err := http.NewRequest("POST", urlBuzon, bytes.NewBuffer(bodyBytes))
    if err != nil {
        return "", err
    }
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-Soberano-Key", "Yo.Soy.Riqueza.Incalculable.Yo.Soy.Abundancia.Total")

    client := &http.Client{Timeout: 15 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return "", fmt.Errorf("error de red contactando al buzón en Render: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        cuerpoError, _ := io.ReadAll(resp.Body)
        return "", fmt.Errorf("el buzón rechazó el chunk (HTTP %d): %s", resp.StatusCode, string(cuerpoError))
    }

    log.Printf("------------>>☁️ [RENDER BACKEND -> BUZÓN]: Chunk depositado en la nube con éxito. Esperando respuesta de la Linux local...\n")

    // 2️⃣ BUCLE DE POLLING: Esperamos a que la Linux local procese y devuelva el resultado por el endpoint de resultados
    timeout := time.After(900 * time.Second)
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-timeout:
            return "", fmt.Errorf("timeout: la Linux local no devolvió el resultado del chunk a tiempo")
        case <-ticker.C:
            // Consultamos al buzón si ya hay un resultado listo de la Linux local
            reqCheck, err := http.NewRequest("GET", "https://geochat-buzon.onrender.com/api/auditoria/consultar-resultado-local", nil)
            if err != nil {
                continue
            }
            reqCheck.Header.Set("X-Soberano-Key", "Yo.Soy.Riqueza.Incalculable.Yo.Soy.Abundancia.Total")

            respCheck, err := client.Do(reqCheck)
            if err != nil {
                continue
            }
            
            cuerpoCheck, err := io.ReadAll(respCheck.Body)
            respCheck.Body.Close()
            if err != nil {
                continue
            }

            if respCheck.StatusCode == http.StatusOK && len(cuerpoCheck) > 0 {
                // Verificamos si no es el mensaje de espera en vacío
                var raw map[string]interface{}
                if json.Unmarshal(cuerpoCheck, &raw) == nil {
                    if status, ok := raw["status"].(string); ok && status == "sin_resultados_pendientes" {
                        continue
                    }
                }

                log.Printf("✨ [RENDER - BUZÓN]: ¡Respuesta del chunk rescatada del buzón con éxito!\n")
                return string(cuerpoCheck), nil
            }
        }
    }
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
        http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
        return
    }

    // 1️⃣ Estructura exacta que mandás desde la Linux local
    var payloadRetorno struct {
        TaskID    string `json:"task_id"`
        Archivo   string `json:"archivo"`
        Resultado string `json:"resultado"`
    }

    if err := json.NewDecoder(r.Body).Decode(&payloadRetorno); err != nil {
        log.Printf("❌ [ERROR DECODIFICACIÓN RETORNO]: %v\n", err)
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    // 2️⃣ Actualizamos el mapa general de tareas usando el TaskID recibido
    mu.Lock()
    if t, exists := tasks[payloadRetorno.TaskID]; exists {
        t.Status = "completed"
        t.Result = payloadRetorno.Resultado
        tasks[payloadRetorno.TaskID] = t
    } else {
        log.Printf("⚠️ [AVISO TAREA]: La tarea ID [%s] llegó al resultado pero no estaba en el mapa de tasks.\n", payloadRetorno.TaskID)
    }
    mu.Unlock()

    // 3️⃣ Alimentamos el buzón de resultados para despertar al hilo en espera
    muBuzonResultados.Lock()
    ultimaTaskEntrada = Task{
        ID:     payloadRetorno.TaskID,
        Status: "completed",
        Result: payloadRetorno.Resultado,
    }
    hayResultadoPendiente = true
    muBuzonResultados.Unlock()

    log.Printf("📥 [RENDER - BUZÓN ENTRADA]: Resultado sincronizado y registrado para la tarea ID [%s] (Archivo: %s)\n", payloadRetorno.TaskID, payloadRetorno.Archivo)

    w.WriteHeader(http.StatusOK)
    io.WriteString(w, `{"status":"success"}`)
}