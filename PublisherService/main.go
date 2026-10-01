package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"event_publisher/esdb"
	kafkaproducer "event_publisher/kafka"
)

const (
	positionStream = "publisher-position"
	schemaVersion  = 1
)

// Envelope es el contrato V2 en Kafka: identidad del evento, stream de origen
// y revision dentro del mismo, para que los consumidores puedan deduplicar.
type Envelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	AccountID     string          `json:"accountId"`
	StreamID      string          `json:"streamId"`
	Revision      int64           `json:"revision"`
	SchemaVersion int             `json:"schemaVersion"`
	Timestamp     string          `json:"timestamp"`
	Data          json.RawMessage `json:"data"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	esdbURL := getenv("ESDB_URL", "http://localhost:2113")
	category := getenv("ESDB_CATEGORY", "account")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "account-events")
	pollInterval := envDurationMS("POLL_INTERVAL_MS", 500)

	client := esdb.NewClient(esdbURL)

	// El topic puede tardar: Kafka recien arrancado. Reintentar hasta el ctx.
	// Si ya existe (lo creo antes alguna proyeccion), contar como exito.
	for ctx.Err() == nil {
		if err := kafkaproducer.EnsureTopic(brokers[0], topic); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				break
			}
			log.Printf("warn: asegurar topic %q: %v (reintento en 2s)", topic, err)
			if !sleepCtx(ctx, 2*time.Second) {
				return
			}
			continue
		}
		break
	}

	writer := kafkaproducer.NewWriter(brokers, topic)
	defer writer.Close()

	// Posicion = numero de evento procesado en $ce-<categoria> (-1 = ninguno).
	// Revision = numero de evento del ultimo checkpoint en publisher-position.
	position, revision, err := client.ReadPosition(ctx, positionStream)
	if err != nil {
		log.Fatalf("leer posicion inicial: %v", err)
	}
	log.Printf("publisher arranca: categoria=%q topic=%q posicion=$ce-%s hasta %d",
		category, topic, category, position)

	for ctx.Err() == nil {
		ev, found, err := client.ReadCategoryEvent(ctx, category, position+1)
		if err != nil {
			log.Printf("error leyendo $ce-%s: %v (reintento en 2s)", category, err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}
		if !found {
			// Catch-up terminado: modo live, esperando eventos nuevos.
			sleepCtx(ctx, pollInterval)
			continue
		}

		if err := publish(ctx, writer, category, ev); err != nil {
			// At-least-once: la posicion no avanzo, se re-intenta el mismo evento;
			// los consumidores idempotentes toleran la duplicacion.
			log.Printf("error publicando %s/%d: %v", ev.EventStreamID, ev.EventNumber, err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}

		ok, err := client.AppendPosition(ctx, positionStream, revision, position+1)
		if err != nil {
			log.Printf("error guardando posicion %d: %v", position+1, err)
			sleepCtx(ctx, 2*time.Second)
			continue
		}
		if !ok {
			// Otro escritor toco el checkpoint (WrongExpectedVersion): recargar.
			position, revision, err = client.ReadPosition(ctx, positionStream)
			if err != nil {
				log.Printf("recargar checkpoint: %v", err)
				return
			}
			continue
		}

		position++
		revision++
		log.Printf("publicado: stream=%s rev=%d type=%s -> %s (posicion $ce=%d)",
			ev.EventStreamID, ev.EventNumber, ev.EventType, topic, position)
	}
}

func publish(ctx context.Context, writer *kafkaproducer.Writer, category string, ev esdb.ResolvedEvent) error {
	accountID := accountIDFromStream(category, ev.EventStreamID)
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	// metadata puede venir como objeto (lo escribio la API de comandos) o vacio.
	var meta struct {
		Timestamp string `json:"timestamp"`
		AccountID string `json:"accountId"`
	}
	if err := json.Unmarshal(ev.Metadata, &meta); err == nil {
		if meta.Timestamp != "" {
			timestamp = meta.Timestamp
		}
		if meta.AccountID != "" {
			accountID = meta.AccountID
		}
	}

	envelope := Envelope{
		EventID:       ev.EventID,
		EventType:     ev.EventType,
		AccountID:     accountID,
		StreamID:      ev.EventStreamID,
		Revision:      ev.EventNumber,
		SchemaVersion: schemaVersion,
		Timestamp:     timestamp,
		Data:          ev.Data,
	}

	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("serializar envelope: %w", err)
	}
	return writer.Publish(ctx, accountID, value)
}

func accountIDFromStream(category, streamID string) string {
	return strings.TrimPrefix(streamID, category+"-")
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationMS(key string, defMs int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Duration(defMs) * time.Millisecond
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
