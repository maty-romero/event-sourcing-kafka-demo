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

// EnsureTopic crea el topic si todavia no existe (1 particion, RF 1).
// Sin topic, los grupos reciben asignacion vacia y el consumer queda
// escuchando "nada" para siempre.
func EnsureTopic(broker, topic string) error {
	conn, err := kafka.Dial("tcp", broker)
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
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}

type Handler func(domain.Event) error

// DLQ publica eventos que la proyeccion no puede aplicar en un topic aparte,
// para inspeccionarlos en vez de perderlos (limitacion 2c de la V1).
type DLQ struct {
	w     *kafka.Writer
	topic string
}

func NewDLQ(brokers []string, topic string) *DLQ {
	return &DLQ{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
		},
		topic: topic,
	}
}

func (d *DLQ) Send(ctx context.Context, key, raw []byte, reason string) error {
	dead := map[string]string{"reason": reason, "original": string(raw)}
	encoded, err := json.Marshal(dead)
	if err != nil {
		return err
	}
	return d.w.WriteMessages(ctx, kafka.Message{Key: key, Value: encoded})
}

func (d *DLQ) Close() error { return d.w.Close() }

func NewReader(brokers []string, topic, groupID string, startOffset int64) *kafka.Reader {
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

func Run(ctx context.Context, reader *kafka.Reader, handle Handler, dlq *DLQ) error {
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
			log.Printf("mensaje invalido -> DLQ (offset=%d): %v", msg.Offset, err)
			if err := dlq.Send(ctx, msg.Key, msg.Value, "json invalido: "+err.Error()); err != nil {
				return fmt.Errorf("enviar a DLQ: %w", err)
			}
		} else if err := handle(event); err != nil {
			log.Printf("evento no aplicable -> DLQ (account=%s, offset=%d): %v",
				event.AccountID, msg.Offset, err)
			if err := dlq.Send(ctx, msg.Key, msg.Value, err.Error()); err != nil {
				return fmt.Errorf("enviar a DLQ: %w", err)
			}
		} else {
			log.Printf("evento procesado: account=%s type=%s amount=%d rev=%d offset=%d",
				event.AccountID, event.EventType, event.Amount(), event.Revision, msg.Offset)
		}

		// Todo mensaje que llega se commitea: o se aplico, o se fue al DLQ.
		// El checkpoint propio (tabla checkpoints) es el que garantiza la
		// idempotencia ante redeliveries, no el offset de Kafka.
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("error commiteando offset %d: %v", msg.Offset, err)
		}
	}
}
