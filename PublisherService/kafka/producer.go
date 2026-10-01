package kafkaproducer

import (
	"context"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic crea el topic si todavia no existe (1 particion, RF 1), igual
// que hacia el consumer de la V1: sin topic, los grupos no reciben asignacion.
func EnsureTopic(broker string, topic string) error {
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

type Writer struct {
	w *kafka.Writer
}

func NewWriter(brokers []string, topic string) *Writer {
	return &Writer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

// Publish escribe el mensaje con key = accountId, conservando el orden
// por agregado (mismo contrato que la V1).
func (w *Writer) Publish(ctx context.Context, key string, value []byte) error {
	return w.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}

func (w *Writer) Close() error {
	return w.w.Close()
}
