package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"event_projection/domain"

	"github.com/segmentio/kafka-go"
)

type Handler func(domain.Event) error

func NewReader(brokers []string, topic, groupID string, startOffset int64) *kafka.Reader {
	// El consumer se subscribe a topics al unirse al group. Si el topic todavía
	// no existe, kafka-go recibe asignación vacía de particiones y queda
	// escuchando "nada" para siempre. Forzamos la creación del topic acá
	// para que la asignación incluya al menos la partición 0.
	if err := ensureTopic(brokers, topic, 1, 1); err != nil {
		log.Printf("warn: no se pudo asegurar el topic %q (continúa igual): %v", topic, err)
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: startOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Println("[kafka-go] " + fmt.Sprintf(msg, args...))
		}),
	})
}

func ensureTopic(brokers []string, topic string, partitions, replicationFactor int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
}

func Run(ctx context.Context, reader *kafka.Reader, handle Handler) error {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// Errores transitorios (ej. "Group Coordinator Not Available" mientras
			// Kafka inicializa __consumer_offsets en KRaft) se reintentan con backoff
			// en vez de matar el consumer.
			log.Printf("kafka fetch error (reintentando en 2s): %v", err)
			select {
			case <-time.After(2 * time.Second):
				continue
			case <-ctx.Done():
				return nil
			}
		}

		var event domain.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("mensaje inválido, se commitea y descarta (offset=%d): %v", msg.Offset, err)
			if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil {
				log.Printf("error commiteando mensaje inválido: %v", commitErr)
			}
			continue
		}

		if err := handle(event); err != nil {
			log.Printf("error procesando evento (account=%s, offset=%d): %v", event.AccountID, msg.Offset, err)
			continue
		}

		log.Printf("evento procesado: account=%s type=%s amount=%d offset=%d", event.AccountID, event.EventType, event.Amount, msg.Offset)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("error commiteando offset %d: %v", msg.Offset, err)
		}
	}
}
