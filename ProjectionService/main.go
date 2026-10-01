package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"event_projection/api"
	"event_projection/consumer"
	"event_projection/domain"
	"event_projection/store"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kind := getenv("PROJECTION_KIND", store.KindBalances)

	db, err := store.Connect(getenv("DB_PATH", "data.db"), kind)
	if err != nil {
		log.Fatalf("conectar sqlite: %v", err)
	}
	defer db.Close()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "account-events")
	dlqTopic := getenv("KAFKA_DLQ_TOPIC", topic+".dlq")
	groupID := getenv("KAFKA_GROUP", kind+"-projection-group")

	startOffset := kafka.LastOffset
	if strings.EqualFold(getenv("KAFKA_START_OFFSET", "last"), "first") {
		startOffset = kafka.FirstOffset
	}

	// Asegurar los topics antes de armar el reader: sin topic, el grupo
	// recibe asignacion vacia.
	for _, t := range []string{topic, dlqTopic} {
		if err := consumer.EnsureTopic(brokers[0], t); err != nil {
			log.Printf("warn: asegurar topic %q: %v", t, err)
		}
	}

	reader := consumer.NewReader(brokers, topic, groupID, startOffset)
	defer reader.Close()

	dlq := consumer.NewDLQ(brokers, dlqTopic)
	defer dlq.Close()

	handle := func(event domain.Event) error {
		applied, err := store.ApplyEvent(ctx, db, kind, event)
		if err != nil {
			return err
		}
		if !applied {
			log.Printf("evento ya procesado, se ignora: stream=%s rev=%d",
				event.StreamID, event.Revision)
		}
		return nil
	}

	log.Printf("proyeccion %q: topic=%s group=%s dlq=%s", kind, topic, groupID, dlqTopic)

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(ctx, reader, handle, dlq)
	}()

	httpDone := make(chan error, 1)
	go func() {
		httpDone <- api.Run(ctx, getenv("HTTP_ADDR", ":8090"), db, kind)
	}()

	fatal := func(err error, msg string) {
		if err != nil {
			stop()
			log.Fatalf("%s: %v", msg, err)
		}
	}

	// La caida de cualquiera de los dos procesos tira al otro.
	select {
	case err := <-consumerDone:
		fatal(err, "consumer terminó con error")
		<-httpDone
	case err := <-httpDone:
		fatal(err, "HTTP de lecturas terminó con error")
		<-consumerDone
	}
	stop()
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
