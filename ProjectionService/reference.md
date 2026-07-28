# Guía de Implementación — Balance Projection (Go)

Referencia rápida para encarar el consumer de Kafka en Go. No es código completo, son
lineamientos + snippets para no arrancar en blanco.

---

## 1. Cliente de Kafka a usar

| Opción | Cuándo conviene |
|---|---|
| `segmentio/kafka-go` | ✅ Recomendado para este proyecto. Puro Go, sin cgo, API simple y explícita (Reader, offsets, partition). |
| `confluent-kafka-go` | Wrapper de librdkafka (C). Más robusto para producción, pero requiere cgo y es más pesado de setupear para un TP. |

```bash
go get github.com/segmentio/kafka-go
```

---

## 2. Estructura del proyecto

```
balance-projection/
├── main.go              # wiring: arma consumer, DB, arranca el loop, shutdown gracioso
├── consumer/
│   └── consumer.go       # solo sabe leer mensajes de Kafka (Reader, offsets, consumer group)
├── domain/
│   └── events.go         # structs de eventos + lógica pura (evento -> nuevo balance)
└── store/
    └── sqlite.go         # solo lectura/escritura del balance en SQLite
```

**Principio clave:** `domain` no conoce Kafka ni SQLite. Es lógica pura, testeable sin
infraestructura. Esto es lo que separa "hice funcionar un tutorial" de "diseñé algo prolijo".

---

## 3. Orden sugerido de implementación

1. **Domain primero** (sin I/O) — definir eventos y la función `Apply`, con tests unitarios.
2. **Store (SQLite)** — leer/escribir balance, sin lógica de negocio acá.
3. **Consumer** — loop de lectura de Kafka usando consumer group.
4. **main.go** — wiring, config por env vars, shutdown gracioso con `context`.

---

## 4. Code samples de referencia

### 4.1 Domain — eventos y lógica pura

```go
// domain/events.go

type EventType string

const (
    AccountCreated  EventType = "AccountCreated"
    MoneyDeposited  EventType = "MoneyDeposited"
    MoneyWithdrawn  EventType = "MoneyWithdrawn"
)

type Event struct {
    EventType EventType `json:"eventType"`
    AccountID string    `json:"accountId"`
    Amount    int       `json:"amount,omitempty"`
}

// Apply es lógica PURA: sin DB, sin Kafka. Fácil de testear.
func Apply(currentBalance int, event Event) (int, error) {
    switch event.EventType {
    case AccountCreated:
        return 0, nil
    case MoneyDeposited:
        return currentBalance + event.Amount, nil
    case MoneyWithdrawn:
        if currentBalance-event.Amount < 0 {
            return 0, fmt.Errorf("balance insuficiente para cuenta")
        }
        return currentBalance - event.Amount, nil
    default:
        return currentBalance, fmt.Errorf("evento desconocido: %s", event.EventType)
    }
}
```

```go
// domain/events_test.go — ejemplo de test unitario sin infraestructura

func TestApply_Deposit(t *testing.T) {
    balance, err := Apply(1000, Event{EventType: MoneyDeposited, Amount: 500})
    assert.NoError(t, err)
    assert.Equal(t, 1500, balance)
}
```

### 4.2 Consumer — leer de Kafka con consumer group

```go
// consumer/consumer.go

func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:  brokers,
        Topic:    topic,
        GroupID:  groupID, // <- esto habilita consumer group + offset management automático
        MinBytes: 1,
        MaxBytes: 10e6,
    })
}

func Run(ctx context.Context, reader *kafka.Reader, handle func(domain.Event) error) error {
    for {
        msg, err := reader.ReadMessage(ctx)
        if err != nil {
            return err // ctx cancelado o error de conexión
        }

        var event domain.Event
        if err := json.Unmarshal(msg.Value, &event); err != nil {
            log.Printf("mensaje inválido, se descarta: %v", err)
            continue
        }

        if err := handle(event); err != nil {
            log.Printf("error procesando evento: %v", err)
            // decisión de diseño: ¿reintentar, mandar a DLQ, o continuar?
        }
    }
}
```

### 4.3 Store — SQLite simple

Diferencias clave con Postgres: placeholders `?` (no numerados), `ON CONFLICT(col) DO UPDATE
SET col = excluded.col`, y la columna `account_id` se define como `TEXT PRIMARY KEY`. El
driver elegido es [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) (puro Go,
sin cgo, encaja con el `golang:alpine` del Dockerfile).

```go
// store/sqlite.go

func Connect(dsn string) (*sql.DB, error) {
    db, err := sql.Open("sqlite", dsn)
    if err != nil {
        return nil, err
    }
    if _, err := db.ExecContext(context.Background(), `
        CREATE TABLE IF NOT EXISTS account_balances (
            account_id TEXT PRIMARY KEY,
            balance    INTEGER NOT NULL
        )
    `); err != nil {
        return nil, err
    }
    return db, nil
}

func GetBalance(ctx context.Context, db *sql.DB, accountID string) (int, error) {
    var balance int
    err := db.QueryRowContext(ctx,
        `SELECT balance FROM account_balances WHERE account_id = ?`, accountID,
    ).Scan(&balance)
    if err == sql.ErrNoRows {
        return 0, nil // cuenta nueva
    }
    return balance, err
}

func SaveBalance(ctx context.Context, db *sql.DB, accountID string, balance int) error {
    _, err := db.ExecContext(ctx, `
        INSERT INTO account_balances (account_id, balance)
        VALUES (?, ?)
        ON CONFLICT(account_id) DO UPDATE SET balance = excluded.balance
    `, accountID, balance)
    return err
}
```

DSN recomendado para un servicio de larga duración (WAL habilita lectores concurrentes;
`busy_timeout` evita fallos por `SQLITE_BUSY` ante escrituras contenciosas):

```
data.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)
```

> ⚠️ SQLite serializa escritores: si en el futuro hay más de un proceso escribiendo la
> misma DB, considerar migrar a Postgres. Para un único consumer projection es más que
> suficiente.

### 4.4 main.go — wiring + shutdown gracioso

```go
import _ "modernc.org/sqlite"

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    db, err := store.Connect(os.Getenv("DB_PATH")) // ej: data.db o la DSN con _pragma
    if err != nil {
        log.Fatalf("conectar sqlite: %v", err)
    }
    defer db.Close()

    reader := consumer.NewReader(
        strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
        "account-events",
        "balance-projection-group",
    )
    defer reader.Close()

    handle := func(event domain.Event) error {
        current, err := store.GetBalance(ctx, db, event.AccountID)
        if err != nil {
            return err
        }
        newBalance, err := domain.Apply(current, event)
        if err != nil {
            return err
        }
        return store.SaveBalance(ctx, db, event.AccountID, newBalance)
    }

    if err := consumer.Run(ctx, reader, handle); err != nil && ctx.Err() == nil {
        log.Fatalf("consumer terminó con error: %v", err)
    }
}
```

---

## 5. Decisiones de diseño a resolver conscientemente

### 5.1 Idempotencia
Si un evento se reprocesa (ej. tras un restart antes de que se commitee el offset), `Apply` con
sumas/restas simples **no es idempotente**. Opciones a evaluar:
- Guardar el último offset procesado en la misma fila/transacción que el balance, y chequear
  "¿ya procesé este offset?" antes de aplicar.
- Aceptar el riesgo para el alcance del TP, pero **documentarlo explícitamente** en el informe
  como limitación conocida.

```sql
-- SQLite: ejemplo de columna extra para llevar control de offset procesado
ALTER TABLE account_balances ADD COLUMN last_offset INTEGER;
```

### 5.2 Orden: ¿commitear offset antes o después de persistir?

| Estrategia | Riesgo |
|---|---|
| Commitear offset **antes** de guardar en Postgres | Si falla el guardado, se pierde el evento (at-most-once). |
| Commitear offset **después** de guardar en Postgres | Si falla el commit tras guardar, se reprocesa el evento (at-least-once) → requiere idempotencia. |

Con `kafka-go`, si usás `reader.FetchMessage` + `reader.CommitMessages` en vez de
`ReadMessage` (que auto-commitea), tenés control explícito de este orden — vale la pena
mencionarlo en el informe como decisión consciente de garantía de entrega.

### 5.3 Modo replay

Para el escenario de demo (reconstruir desde offset 0), la forma más simple es usar un
`GroupID` nuevo y arrancar con `KAFKA_START_OFFSET=first`, que el wiring traduce a
`kafka.FirstOffset` en el reader:

```bash
rm -f data.db data.db-wal data.db-shm

DB_PATH='data.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)' \
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPIC=account-events \
KAFKA_GROUP=balance-projection-replay \
KAFKA_START_OFFSET=first \
go run .
```

Equivale a este `ReaderConfig` (lo importante es el `StartOffset`, ya wireado vía env var):

```go
kafka.ReaderConfig{
    Brokers:     brokers,
    Topic:       "account-events",
    GroupID:     "balance-projection-replay", // grupo nuevo = sin offsets previos
    StartOffset: kafka.FirstOffset,
}
```

Si en cambio querés resetear un grupo existente por CLI (útil para re-correr la demo con el
mismo group id), la variante manual es:

```bash
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group balance-projection-group \
  --topic account-events \
  --reset-offsets --to-earliest --execute
```

---

## 6. Checklist antes de dar por terminado el módulo

- [ ] `domain.Apply` tiene tests unitarios sin dependencias externas.
- [ ] El consumer usa consumer group (`GroupID`) — no lectura manual de particiones.
- [ ] Se documentó la decisión de orden commit-offset vs. persistencia (sección 5.2).
- [ ] Existe un modo/script de replay reproducible para la demo.
- [ ] Shutdown gracioso con `context` (no matar el proceso en medio de un mensaje).