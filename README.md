# Event Sourcing con EventStoreDB + Apache Kafka

> **Implementacion actual: V2.** EventStoreDB es el event store (system of
> record) y Kafka la plataforma de distribucion. La V1 —Kafka forzado a ser
> event store— queda preservada en el branch `kafka-initial-v1`, con sus
> limitaciones documentadas en [`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md)
> y su guia en [`.docs/docs_v1.md`](./.docs/docs_v1.md). El diseno de la
> transicion esta en [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md) y la
> guia de esta version en [`.docs/docs_v2.md`](./.docs/docs_v2.md).

Investigacion aplicada de **Event Sourcing / CQRS** sobre **EventStoreDB +
Apache Kafka**, con un sistema bancario simplificado como caso de uso.

## La historia en dos actos

1. **V1** puso a Kafka de event store y documento donde eso se rompe: doble
   gasto posible, estado en memoria, proyeccion no idempotente, eventos
   descartados en silencio, sin agregados. Nada de eso era un bug de
   implementacion: eran consecuencias del storage
   ([`scripts/show_limitations.sh`](./.docs/limitaciones_v1.md), branch `kafka-initial-v1`).
2. **V2** (esta implementacion) resuelve esas limitaciones aplicando event
   sourcing como corresponde: **EventStoreDB** con streams por agregado y
   append condicional, **Kafka** solo con fan-out/distribucion, y
   **[`scripts/show_fixes.sh`](./scripts/show_fixes.sh)** que demuestra, con
   las mismas situaciones de la V1, que ahora no pasan.

## Arquitectura

```
                        comandos HTTP (:5087)
                               │  load stream → fold → validar invariante
                               │  append con expected revision (OCC)
                               ▼
                  ┌─────────────────────────┐
                  │      EventStoreDB       │  ◄── SYSTEM OF RECORD
                  │  account-901: [0]..[N]  │      UI: http://localhost:2113
                  │  $ce-account / publisher│
                  └───────────┬─────────────┘
                              │  publisher (Go): catch-up con posicion persistida
                              ▼
                  ┌─────────────────────────┐
                  │          Kafka          │  ◄── DISTRIBUCION (fan-out)
                  │  account-events (Go key=│       UI: http://localhost:8080
                  │  accountId) + .dlq      │
                  └──────┬──────────┬───────┘
                         ▼          ▼
              ┌──────────────┐ ┌──────────────┐
              │  balances    │ │  statement   │  Go: idempotentes, checkpoint
              │  (:8090)     │ │  (:8091)     │  en la misma transaccion, DLQ
              └──────────────┘ └──────────────┘
```

- **TransactionsAPI** (.NET, :5087): lado de comandos. Agregado `Account` que
  pliega su stream de EventStoreDB, valida las invariantes **antes** de aceptar
  y appendea con *expected revision* (concurrencia optimista). Habla con
  EventStoreDB por su API HTTP/JSON — sin gRPC a proposito.
- **EventStoreDB** (:2113): event store real. Streams por agregado
  (`account-901`), numeracion por evento, append condicional, UI web embebida.
- **publisher** (Go): puente ESDB→Kafka. Sigue la proyeccion de categoria
  `$ce-account`, publica envelopes con `key = accountId` y persiste su posicion
  en el propio EventStoreDB (at-least-once; los consumidores son idempotentes).
- **projection-balances** (Go, :8090) y **projection-statement** (Go, :8091):
  dos consumer groups sobre el mismo topic — el fan-out que Kafka hace bien.
  Cada una es unica duena de su SQLite (read model + checkpoint) y de su DLQ.

## Requisitos ejecucion

- Docker + Docker Compose
- `curl`
- `jq` (los scripts de demo lo usan para parsear el JSON)

## Como correrlo

```bash
git clone <URL_DEL_REPO>
cd <nombre-del-repo>
docker compose up -d --build
./scripts/demo.sh          # happy path + el stream final en EventStoreDB
./scripts/replay.sh        # reconstruye el read model desde el log
./scripts/show_fixes.sh    # las limitaciones de la V1, ahora resueltas
```

Cada script es independiente (los podes correr en cualquier orden).
`show_fixes.sh` es destructivo con el estado de demo y deja el sistema limpio
al terminar.

Interfaces: comandos `http://localhost:5087`, balances `:8090`, extracto
`:8091`, EventStoreDB UI `http://localhost:2113`, Kafka UI `http://localhost:8080`.

## Criterios de aceptacion (lo que verifica `show_fixes.sh`)

1. **Doble gasto**: 5 retiros concurrentes con saldo para 1 → exactamente un
   `200` y cuatro `409`; el stream contiene un solo `MoneyWithdrawn`.
2. **Restart**: reiniciar la API no pierde cuentas (la cuenta vive en su stream).
3. **Redelivery**: re-entregar un evento ya procesado no altera el read model
   (checkpoint + deduplicacion por `streamId`+`revision`).
4. **Proyecciones**: una segunda proyeccion (extracto), reconstruible de forma
   independiente, y los eventos invalidos van a un DLQ en vez de descartarse.

## Uso manual

```bash
# Crear cuenta (409 si el stream ya existe)
curl -X POST localhost:5087/accounts -d '{"accountId": "123"}'

# Depositar / retirar (409 si no hay saldo, 404 si la cuenta no existe)
curl -X POST localhost:5087/accounts/123/deposit -d '{"amount": 1000}'
curl -X POST localhost:5087/accounts/123/withdraw -d '{"amount": 200}'

# Lecturas: las sirven las proyecciones, no la API de comandos
curl localhost:8090/accounts/123/balance
curl localhost:8091/accounts/123/movements

# La fuente de verdad: el stream en EventStoreDB (API HTTP)
curl -s -H 'Accept: application/vnd.eventstore.events+json' \
  'http://localhost:2113/streams/account-123/head/1000?embed=content' | jq '.entries[].content'
```

## Endpoints

Transaction API (comandos, `:5087`):

| Metodo | Ruta | Evento | Códigos |
|---|---|---|---|
| POST | `/accounts` | `AccountCreated` | 201 / 400 / 409 |
| POST | `/accounts/{id}/deposit` | `MoneyDeposited` | 200 / 400 / 404 / 409 |
| POST | `/accounts/{id}/withdraw` | `MoneyWithdrawn` | 200 / 400 / 404 / 409 |

Proyecciones (lecturas):

| Servicio | Ruta | Devuelve |
|---|---|---|
| balances `:8090` | `GET /accounts/{id}/balance` | balance desde el read model |
| statement `:8091` | `GET /accounts/{id}/movements` | extracto de movimientos |
| ambos | `GET /healthz` | `ok` |

## Tests

```bash
dotnet test TransactionsAPI.sln   # agregado Account + OCC (xUnit)
cd ProjectionService && go test ./...   # Apply + checkpoint (SQLite real)
```

## Logs en vivo

```bash
docker compose logs -f transactions-api publisher projection-balances projection-statement
```

## Versiones y transicion V1 → V2

- **V1** (branch `kafka-initial-v1`): Kafka como event store. Su codigo, docs y
  `show_limitations.sh` quedan como registro del experimento.
- **V2** (esta): EventStoreDB como event store + Kafka como distribucion.
  Diseno en [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md), guia en
  [`.docs/docs_v2.md`](./.docs/docs_v2.md).

## Apagar

```bash
docker compose down        # los datos del demo son efimeros (ESDB en memoria,
                           # ./data/ con los read models)
```
