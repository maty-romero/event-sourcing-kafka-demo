# Event-Driven Banking con Apache Kafka

Investigación aplicada de **Apache Kafka** en Sistemas Distribuidos, usando como caso de uso un
sistema bancario simplificado inspirado en Event Sourcing / CQRS.

Foco de investigación: **80% Apache Kafka / 20% Event Sourcing**. Informe completo en
[`/docs/informe.pdf`](./docs/informe.pdf).

## Arquitectura

```
                 ┌────────────────────┐
                 │  Transaction API   │   (Command side)
                 │       (.NET)       │
                 └──────────┬─────────┘
                            │ produce eventos
                            ▼
                 ┌──────────────────────────┐
                 │  Kafka: account-events    │
                 │  (particionado por        │
                 │   accountId)              │
                 └───────────┬──────────────┘
                             │ consume eventos
                             ▼
                 ┌──────────────────────────┐
                 │  Balance Projection       │   (Query side)
                 │       (Go)                │
                 └───────────┬──────────────┘
                             │ persiste
                             ▼
                 ┌──────────────────────────┐
                 │  Account State            │
                 │  (read model - SQLite)    │
                 └──────────────────────────┘
```

- **Transaction API**: recibe comandos HTTP y produce eventos. No calcula balances.
- **Kafka**: log de eventos, particionado por `accountId` (garantiza orden por cuenta).
- **Balance Projection**: consume eventos y reconstruye el balance (read model en SQLite).

> Kafka se usa como backbone de eventos (EDA/CQRS con consistencia eventual), no como Event
> Store transaccional estricto. Detalle y alternativa (EventStoreDB) en la sección de
> Limitaciones del informe.

## Requisitos

- Docker + Docker Compose
- `curl`
- `jq` (los scripts de demo lo usan para parsear el JSON)

## Cómo correrlo

```bash
git clone <URL_DEL_REPO>
cd <nombre-del-repo>
docker compose up -d --build
./scripts/demo.sh          # corre el flujo y muestra el balance inicial
./scripts/replay.sh        # borra la proyección, resetea el offset y la reconstruye
```

Transaction API queda expuesta en `http://localhost:5087`. Kafka UI en `http://localhost:8080`.

## Uso manual

```bash
# Crear cuenta
curl -X POST localhost:5087/accounts -d '{"accountId": "123"}'

# Depositar / retirar
curl -X POST localhost:5087/accounts/123/deposit -d '{"amount": 1000}'
curl -X POST localhost:5087/accounts/123/withdraw -d '{"amount": 200}'

# Consultar balance
curl localhost:5087/accounts/123/balance
```

## Demo de replay

Escenario central del proyecto: demuestra que el balance es completamente reconstruible desde
el log de eventos.

```bash
./scripts/demo.sh          # corre el flujo completo y muestra el balance inicial
./scripts/replay.sh        # borra la proyección, resetea el offset y la reconstruye
```

El balance reconstruido debe coincidir exactamente con el original. El script `replay.sh`:
1. Lee el balance actual.
2. Detiene solo el consumer (deja Kafka y la API vivos).
3. Borra `./data/data.db*` (la proyección).
4. Re-arranca el consumer con un `KAFKA_GROUP` nuevo y `KAFKA_START_OFFSET=first` para
   reprocesar el topic desde el primer evento.
5. Hace polling hasta que el `GET /balance` vuelve a 1300.
6. Imprime los últimos 30 logs del consumer.

## Endpoints

| Método | Ruta | Evento |
|---|---|---|
| POST | `/accounts` | `AccountCreated` |
| POST | `/accounts/{id}/deposit` | `MoneyDeposited` |
| POST | `/accounts/{id}/withdraw` | `MoneyWithdrawn` |
| GET | `/accounts/{id}/balance` | — |

## Logs en vivo

```bash
docker compose logs -f projection transactions-api   # ambos
docker compose logs -f projection                    # solo el consumer
docker compose logs -f transactions-api              # solo la API
```

## Limitaciones

Sin control de concurrencia optimista (OCC): operaciones concurrentes sobre la misma cuenta
pueden generar inconsistencias. Es intencional — analizado en el informe como evidencia de los
límites de Kafka como Event Store estricto.

## Apagar

```bash
docker compose down -v      # baja servicios y borra ./data/
```
