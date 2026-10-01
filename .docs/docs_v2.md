# Iteracion V2 — EventStoreDB como event store + Kafka como plataforma de distribucion

Guia de la version actual. El diagnostico del que nace:
[limitaciones_v1.md](./limitaciones_v1.md). El registro de diseno:
[propuesta_v2.md](./propuesta_v2.md). La V1 queda preservada en el branch
`kafka-initial-v1` (y su guia en [docs_v1.md](./docs_v1.md)).

## 1. El sistema y que se esta probando

El mismo banco minimo de la V1 — cuentas, depositos, retiros, consultas de
saldo — pero ahora con **event sourcing implementado como corresponde**:

- **EventStoreDB** es el *system of record*: la unica fuente de verdad. All
  viven los streams por agregado (`account-901`), la numeracion por evento
  (`eventNumber`) y el **append condicional** con *expected revision*.
- **Kafka** conserva solo el rol que hace bien: **plataforma de distribucion**
  (fan-out hacia N proyecciones, cada consumer group a su ritmo).

La pregunta que prueba la V2 ya no es "¿aguanta Kafka como event store?" (se
respondio en la V1: no), sino: **"¿resuelve un event store real las
limitaciones que la V1 documentaba?"**. La evidencia es
`scripts/show_fixes.sh`: las mismas situaciones que antes fallaban, ahora no.

## 2. Vocabulario minimo

Terminos nuevos respecto de la V1 (los basicos —evento, fold, proyeccion,
consumer group— siguen en [docs_v1.md](./docs_v1.md) y en el glosario de
[limitaciones_v1.md](./limitaciones_v1.md)):

| Termino | Definicion en este codigo |
|---|---|
| **Stream** | Secuencia ordenada e inmutable de los eventos de UN agregado: `account-901`. La posicion dentro del stream (`eventNumber`, 0, 1, 2…) es la **revision** del agregado. |
| **Agregado** | Objeto de dominio (`Account`, en `TransactionsAPI/Domain/Account.cs`) que se reconstruye plegando su stream, valida las invariantes y produce eventos nuevos. Estado = `fold(eventos del stream)`. |
| **Append condicional / expected revision** | El append declara `ES-ExpectedVersion: N`; si el stream ya no esta en N, EventStoreDB rechaza con 400 (`Wrong expected EventNumber`). Es lo que transforma la validacion en garantia: **el evento invalido nunca nace**. |
| **OCC** | Concurrencia optimista: "confio y verifico". Ante `WrongExpectedVersion` el comando re-le el estado real, re-valida y reintenta (`AccountStore.ExecuteAsync`). Sin locks. |
| **Proyeccion de categoria (`$ce-account`)** | Stream especial que mantiene la proyeccion estandar `$CE` de EventStoreDB: enlaza todos los eventos de la categoria `account` (todo stream que empieza con `account-`) en una sola secuencia global. Es la "catch-up subscription" que usa el puente a Kafka. |
| **Catch-up** | Leer la historia desde una posicion guardada y despues seguir en vivo: el publisher reanuda donde quedo (`publisher-position`) y luego hace polling. |
| **Checkpoint (proyeccion)** | Tabla `checkpoints(stream_id, last_revision)` en la MISMA transaccion SQLite que el read model. "Ya procese la revision N de este stream": redelivery no duplica. |
| **DLQ** | Topic `account-events.dlq`: eventos que la proyeccion no puede aplicar, guardados para inspeccion. Nunca descarte silencioso. |
| **System of record vs distribucion** | EventStoreDB decide que es verdad (append con version); Kafka propaga esa verdad (fan-out). Los dos roles separados, cada uno en la herramienta que los cumple. |

## 3. Arquitectura

```
                        comandos HTTP (:5087)
                               │
                               ▼
                  ┌─────────────────────────┐
                  │     TransactionsAPI     │  .NET: agregado Account
                  │  load stream → fold →   │  valida invariante
                  │  validar → append con   │  ANTES de aceptar
                  │  expected revision (OCC)│
                  └───────────┬─────────────┘
                              ▼
                  ┌─────────────────────────┐
                  │      EventStoreDB       │  ◄── SYSTEM OF RECORD
                  │  account-901: [0]Created│      UI: http://localhost:2113
                  │               [1]Dep    │
                  │               [2]Wd     │
                  │  $ce-account (catch-up) │
                  │  publisher-position     │
                  └───────────┬─────────────┘
                              │  publisher (Go): lee $ce-account
                              │  desde su posicion, publica, avanza
                              ▼
                  ┌─────────────────────────┐
                  │          Kafka          │  ◄── DISTRIBUCION
                  │  account-events         │  key = accountId
                  │  account-events.dlq     │
                  └──────┬──────────┬───────┘
                         ▼          ▼
              ┌──────────────┐ ┌──────────────┐
              │  balances    │ │  statement   │   Go: idempotentes,
              │  (Go, :8090) │ │  (Go, :8091) │   checkpoint propio,
              │  +checkpoint │ │  +checkpoint │   DLQ propio
              │  SQLite      │ │  SQLite      │
              └──────────────┘ └──────────────┘
```

- **`TransactionsAPI` (.NET, :5087)** — lado de comandos. Habla con EventStoreDB
  por su **API HTTP/JSON** (sin gRPC a proposito: menos protocolos, y el
  contrato —header `ES-ExpectedVersion`, `eventNumber`, `?embed=content`— queda
  a la vista en el codigo).
- **`EventStoreDB` (:2113)** — event store. Single node, modo dev sin auth. Su
  UI web embebida (misma URL) muestra streams, eventos, revisiones y metadata.
- **`publisher` (Go)** — puente ESDB→Kafka. Catch-up sobre `$ce-account`,
  publica el **envelope V2** con `key = accountId`, persiste su posicion en el
  stream `publisher-position` de la propia base (append condicional otra vez).
- **`projection-balances` / `projection-statement` (Go, :8090/:8091)** — dos
  consumer groups sobre el mismo topic (fan-out). Cada una es unica duena de su
  SQLite (`data/balances.db`, `data/statement.db`) con su tabla de checkpoints.

## 4. Contenedores y puertos (docker-compose)

| Servicio | Imagen / build | Puerto host | Rol |
|---|---|---|---|
| `eventstore` | `eventstore/eventstore:23.10.8-bookworm-slim` | 2113 | System of record + UI web |
| `kafka` | `apache/kafka:latest` (KRaft, sin Zookeeper) | 9092 (red) / 9094 (host) | Distribucion |
| `transactions-api` | build `TransactionsAPI/Dockerfile` | 5087 | Comandos |
| `publisher` | build `PublisherService/Dockerfile` | — | Puente ESDB→Kafka |
| `projection-balances` | build `ProjectionService/Dockerfile` | 8090 | Read model de saldos |
| `projection-statement` | build `ProjectionService/Dockerfile` (otra env!) | 8091 | Read model de extracto |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:latest` | 8080 | UI de topics |

Notas de infra:

- **Un solo binario Go para dos proyecciones**: `PROJECTION_KIND` elige el read
  model (`balances` | `statement`). Mismo codigo, dos despliegues.
- **EventStoreDB con `EVENTSTORE_MEM_DB=true`**: los eventos viven en memoria.
  Sobrevive `docker compose restart transactions-api` (que es lo que importa al
  demo) pero no un `restart eventstore`. Para un demo que se levanta y se tira,
  es el trade-off elegido: cero administracion de volumenes.
- `EVENTSTORE_RUN_PROJECTIONS=All` + `EVENTSTORE_START_STANDARD_PROJECTIONS=true`
  habilitan `$CE`, que genera `$ce-account`.
- `EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true`: en 23.10 es el interruptor que
  activa la API HTTP de `/streams` (la V2 habla HTTP, no gRPC).
- Healthchecks: `kafka` con `nc`, `eventstore` con `curl /health/live`; los
  demas arrancan con `depends_on: condition: service_healthy`.
- El **topic `account-events` lo crea el publisher** al arrancar (antes lo
  creaba la proyeccion); cada proyeccion asegura ademas su DLQ. Con un topic
  inexistente la asignacion del grupo queda vacia para siempre.

## 5. Configuracion (env vars por servicio)

**transactions-api** (config de .NET; `appsettings.json` trae defaults para
correr fuera de Docker):

| Variable | Default | Efecto |
|---|---|---|
| `EventStore__Url` | `http://localhost:2113` | API HTTP de EventStoreDB |

**publisher**:

| Variable | Default | Efecto |
|---|---|---|
| `ESDB_URL` | `http://localhost:2113` | EventStoreDB |
| `ESDB_CATEGORY` | `account` | Categoria que puentea (`$ce-account`) |
| `KAFKA_BROKERS` | `localhost:9092` | Bootstrap Kafka (compose: `kafka:9092`) |
| `KAFKA_TOPIC` | `account-events` | Topic destino |
| `POLL_INTERVAL_MS` | `500` | Frecuencia del catch-up/live |

**proyecciones** (mismas para las dos, cambian los valores):

| Variable | balances | statement | Efecto |
|---|---|---|---|
| `PROJECTION_KIND` | `balances` | `statement` | Read model a construir |
| `KAFKA_GROUP` | `${KAFKA_GROUP:-balance-projection-group}` | `${STATEMENT_KAFKA_GROUP:-statement-projection-group}` | Interpolable para replay |
| `KAFKA_START_OFFSET` | `${KAFKA_START_OFFSET:-first}` | `${STATEMENT_KAFKA_START_OFFSET:-first}` | `first` = reconstruible desde el offset 0 |
| `KAFKA_DLQ_TOPIC` | `account-events.dlq` | `account-events.dlq` | Dead-letter |
| `DB_PATH` | `/data/balances.db?_pragma=...` | `/data/statement.db?_pragma=...` | SQLite propio por proyeccion |
| `HTTP_ADDR` | `:8090` | `:8091` | Servidor de lecturas |

## 6. Contrato de eventos V2

En EventStoreDB el stream `account-901` guarda los eventos con su
`eventNumber`; el payload `data` es `{"amount": N}` y `metadata` lleva
`{"timestamp": "...", "accountId": "..."}` (lo escribe la API de comandos).

El **envelope** que publica el puente a Kafka agrega la identidad que la V1 no
tenia (limitacion 2b: sin identidad no hay dedupe posible):

```json
{
  "eventId": "6ac2b17c-...",
  "eventType": "MoneyDeposited",
  "accountId": "901",
  "streamId": "account-901",
  "revision": 1,
  "schemaVersion": 1,
  "timestamp": "2026-10-01T00:24:26.7084011Z",
  "data": { "amount": 100 }
}
```

- `revision` = `eventNumber` dentro del stream: con `streamId` forma la
  identidad que la proyeccion usa para deduplicar.
- `eventId` = UUID generado por el agregado al producir el evento.
- Transporte: `key = accountId` (orden por cuenta, igual que en la V1).
- Los tres tipos y el efecto del fold (`domain.Apply`) son los mismos eventos
  semanticos de la V1; lo que cambio es *donde se validan* (ahora aguas arriba).

## 7. El recorrido de un deposito

`POST localhost:5087/accounts/901/deposit {"amount": 100}`:

1. **Cargar el agregado**: `GET /streams/account-901/head/1000?embed=content`
   → fold → `Account{Balance=100, Version=2}`.
2. **Validar la invariante** en el agregado (`Account.Deposit`): monto > 0.
   Para `Withdraw`: `Balance - amount >= 0` — si falla, **HTTP 409 y no se
   escribe nada**. (En la V1 esto pasaba en la proyeccion, tarde.)
3. **Append condicional**: `POST /streams/account-901` con header
   `ES-ExpectedVersion: 2`. Si otro escritor gano la carrera, EventStoreDB
   responde 400 `Wrong expected EventNumber` y el paso 1-3 se repite (max. 3
   reintentos, despues 409). El `200` ahora significa "el evento fue
   appendeado al stream", no "alguien lo aplicara".
4. **El publisher** (≤1s, por el tracking interval de `$CE`): lee
   `$ce-account` en su posicion + 1 — la API HTTP **resuelve el enlace** y
   devuelve el evento original con su `eventNumber` — y lo publica como
   envelope V2.
5. **Checkpoint del publisher**: append a `publisher-position` con
   `ES-ExpectedVersion: N` (el propio mecanismo OCC otra vez). Si el proceso se
   cae entre 4 y 5, al reiniciar re-publica el mismo evento: at-least-once, y
   los consumidores lo toleran por idempotentes.
6. **La proyeccion de balances** consume, abre una transaccion SQLite, lee su
   checkpoint (`streamId=account-901, last_revision=2`), ve `revision=3 > 2`,
   aplica `Apply`, escribe `account_balances` Y `checkpoints` en la misma
   transaccion, commitea. `GET :8090/accounts/901/balance` → `200`.
7. **La proyeccion de extracto** hace lo propio con su grupo, su base y su
   checkpoint: el mismo evento, dos vistas, sin enterarse una de la otra.

## 8. Idempotencia y DLQ (limitaciones 2b y 2c resueltas)

- **Redelivery**: Kafka entrega at-least-once; el checkpoint por stream hace
  que re-procesar la revision N sea un no-op (`applied=false`, log
  "evento ya procesado"). Verificado en el demo 3 de `show_fixes.sh`.
- **Evento no aplicable** (JSON invalido, tipo desconocido, invariante que no
  debio pasar): va a `account-events.dlq` con el motivo y el offset se
  commitea. El read model y su checkpoint avanzan juntos o no avanzan (una sola
  transaccion): nunca divergencia silenciosa.
- **Reconstruccion**: borrar `data/<proyeccion>.db` y levantar con
  `KAFKA_GROUP` nuevo + `KAFKA_START_OFFSET=first` (eso hace `replay.sh`). Con
  checkpoint, ademas, se puede releer el topic desde el offset 0 SIN borrar la
  base: todo se ignora y el estado queda igual.

## 9. Ejecutarlo

```bash
docker compose up -d --build
./scripts/demo.sh          # happy path + stream final en EventStoreDB
./scripts/replay.sh        # reconstruye balances desde el log (:8090)
./scripts/show_fixes.sh    # los 4 criterios de aceptacion de propuesta_v2.md
```

Interfaces web mientras corre:

- **EventStoreDB UI**: `http://localhost:2113` — navegar streams
  (`account-901`, `$ce-account`, `publisher-position`), ver revisiones y
  metadata. Es la misma API HTTP que usan las apps.
- **Kafka UI**: `http://localhost:8080` — topic `account-events` (envelopes) y
  `account-events.dlq`.

Inspeccion por HTTP (lo que hacen los scripts):

```bash
# el stream de una cuenta, como system of record
curl -s -H 'Accept: application/vnd.eventstore.events+json' \
  'http://localhost:2113/streams/account-901/head/1000?embed=content' | jq '.entries[].content'

# la posicion del puente
curl -s -H 'Accept: application/vnd.eventstore.events+json' \
  'http://localhost:2113/streams/publisher-position/head/1?embed=content' | jq '.entries[0].content.data'
```

## 10. Mapa limitacion → solucion

| Limitacion V1 | Donde se resuelve en la V2 |
|---|---|
| 1. Sin OCC (doble gasto) | `AccountStore.ExecuteAsync` + `ES-ExpectedVersion` (`TransactionsAPI/Services/`) — demo 1 de `show_fixes.sh` |
| 2a. Estado en memoria | La cuenta = su stream. `Program.cs` ya no tiene lista en memoria — demo 2 |
| 2b. Proyeccion no idempotente | Tabla `checkpoints` en la misma transaccion (`ProjectionService/store/sqlite.go`) — demo 3 |
| 2c. Descarte silencioso | DLQ `account-events.dlq` (`ProjectionService/consumer/consumer.go`) — extra |
| 3. Sin agregados | `Account` en `TransactionsAPI/Domain/Account.cs` (fold + invariantes + eventos) |
| 4. Una sola proyeccion | `projection-statement` (fan-out real, reconstruible independiente) — demo 4 |
| 5. Kafka como event store | EventStoreDB system of record; Kafka solo distribuye |

## 11. Limitaciones conocidas de esta V2

Son un demo, no produccion. Lo que a proposito no esta (o esta simple):

- **EventStoreDB single node con `MEM_DB`**: en produccion, cluster de 3 nodos
  y disco. La API HTTP se deprecia en versiones nuevas (por eso el compose fija
  `23.10.8`); el cliente "serio" seria gRPC, y este proyecto eligio HTTP para
  no sumar protocolos y mostrar el contrato crudo.
- **El puente es polling de `$ce-account`**, no una catch-up subscription
  nativa (gRPC). El efecto es el mismo (at-least-once con posicion persistida);
  el costo es latencia ~1s por el tracking interval de `$CE`.
- **Sin snapshots**: el agregado pliega el stream completo en cada comando.
  Con streams chicos es gratis; con miles de eventos por stream conviene
  cachear/snapshotear (fuera de alcance).
- **Sin versionado de esquema con upcasting**: `schemaVersion` viaja en el
  envelope pero no hay migracion de streams antiguos.
- **Sin auth** en EventStoreDB (`INSECURE=true`) ni en Kafka.
- **1 particion en Kafka**: el orden por clave es trivialmente cierto; con N
  particiones seguiria garantizado el orden por `accountId`.
