# Event-Driven Banking con Apache Kafka

> **Implementacion actual: V1.** Esta version usa Kafka como log de eventos.
> Sus limitaciones conocidas estan documentadas en
> [`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md) (con un script
> para reproducirlas), y la siguiente iteracion — **V2**, con EventStoreDB
> como event store y Kafka como plataforma de distribucion — esta
> especificada en [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md).

Investigacion aplicada de **Apache Kafka** en Sistemas Distribuidos, usando como caso de uso un
sistema bancario simplificado inspirado en Event Sourcing / CQRS.

## Los dos propositos de la V1

Esta V1 es un experimento con dos hipotesis:

1. **Kafka como log distribuido**: ¿hace Kafka lo que dice hacer? Se prueba en
   `scripts/demo.sh` y `scripts/replay.sh` (orden por clave, consumer groups,
   replay del log para reconstruir estado). Resultado: **si**.
2. **Kafka como event store**: ¿que pasa si lo obligas a ser la fuente de
   verdad de un sistema inspirado en event sourcing? Se prueba en
   `scripts/show_limitations.sh`. Resultado: **falla en puntos especificos**,
   y cada falla esta clasificada por nivel (arquitectura / posible-pero-con-
   costo / scaffolding) en [`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md).

## Arquitectura

```
   comandos HTTP (5087)                    lecturas HTTP (8090)
        │                                        │
        ▼                                        ▼
┌────────────────┐  produce   ┌──────────────┐   consume  ┌──────────────────┐
│ Transaction API│ ─────────▶ │    Kafka     │ ─────────▶ │ Balance Projection│
│     (.NET)     │  eventos   │account-events│            │       (Go)        │
│ (Command side) │            │(orden por    │            │   (Query side)    │
└────────────────┘            │ accountId)   │            └─────────┬─────────┘
                              └──────────────┘                      │ persiste
                                                                    ▼
                                                         ┌──────────────────┐
                                                         │  Account State    │
                                                         │ (read model,      │
                                                         │  SQLite - unico   │
                                                         │  dueno: la        │
                                                         │  proyeccion)      │
                                                         └──────────────────┘
```

- **Transaction API** (puerto 5087): recibe comandos HTTP y produce eventos. No calcula balances
  y no lee el read model: es solo el lado de escritura.
- **Kafka**: log de eventos, particionado por `accountId` (garantiza orden por cuenta).
- **Balance Projection** (puerto 8090): consume eventos, reconstruye el balance (read model en
  SQLite del que es unica duena) y **sirve las lecturas por HTTP** (`GET /accounts/{id}/balance`).

> Kafka se usa como backbone de eventos (EDA/CQRS con consistencia eventual), no como Event
> Store transaccional estricto. Las consecuencias de esto y la propuesta de usar EventStoreDB
> como event store estan analizadas en [`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md)
> y [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md).

---

## Requisitos ejecucion

- Docker + Docker Compose
- `curl`
- `jq` (los scripts de demo lo usan para parsear el JSON)

## Como correrlo

```bash
git clone <URL_DEL_REPO>
cd <nombre-del-repo>
docker compose up -d --build
./scripts/demo.sh               # corre el flujo y muestra el balance inicial
./scripts/replay.sh             # borra la proyeccion, resetea el offset y la reconstruye
./scripts/show_limitations.sh   # demuestra las limitaciones de la V1 (doble gasto,
                                # estado perdido tras restart, redelivery sin idempotencia)
```

Cada script es independiente de los demas (los podes correr en cualquier orden).
Detalle de las limitaciones en
[`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md) y propuesta de V2
(EventStoreDB + Kafka) en [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md).

La Transaction API (comandos) queda expuesta en `http://localhost:5087`, la Projection (lecturas)
en `http://localhost:8090`. Kafka UI en `http://localhost:8080`.

## Uso manual

```bash
# Crear cuenta
curl -X POST localhost:5087/accounts -d '{"accountId": "123"}'

# Depositar / retirar
curl -X POST localhost:5087/accounts/123/deposit -d '{"amount": 1000}'
curl -X POST localhost:5087/accounts/123/withdraw -d '{"amount": 200}'

# Consultar balance (lo sirve la proyeccion, no la API de comandos)
curl localhost:8090/accounts/123/balance
```

## Demo de replay

El escenario central del proyecto: demuestra que el balance se puede reconstruir completo
desde el log de eventos.

```bash
./scripts/demo.sh          # corre el flujo completo y muestra el balance inicial
./scripts/replay.sh        # borra la proyeccion, resetea el offset y la reconstruye
```

El balance reconstruido tiene que coincidir exacto con el original. El script `replay.sh`:
1. Lee el balance actual.
2. Detiene solo el consumer (deja Kafka y la API vivos).
3. Borra `./data/data.db*` (la proyeccion).
4. Re-arranca el consumer con un `KAFKA_GROUP` nuevo y `KAFKA_START_OFFSET=first` para
   reprocesar el topic desde el primer evento.
5. Hace polling hasta que el `GET /balance` de la proyeccion (:8090) vuelve a 1300.
6. Imprime los ultimos 30 logs del consumer.

## Endpoints

Transaction API (comandos, `:5087`):

| Metodo | Ruta | Evento |
|---|---|---|
| POST | `/accounts` | `AccountCreated` |
| POST | `/accounts/{id}/deposit` | `MoneyDeposited` |
| POST | `/accounts/{id}/withdraw` | `MoneyWithdrawn` |

Balance Projection (lecturas, `:8090`):

| Metodo | Ruta | Devuelve |
|---|---|---|
| GET | `/accounts/{id}/balance` | balance desde el read model |
| GET | `/healthz` | `ok` |

## Logs en vivo

```bash
docker compose logs -f projection transactions-api   # ambos
docker compose logs -f projection                    # solo el consumer
docker compose logs -f transactions-api              # solo la API
```

## Limitaciones

- Sin control de concurrencia optimista (OCC): operaciones concurrentes sobre la misma cuenta
pueden generar inconsistencias. Es intencional — analizado en el informe como evidencia de los
limites de Kafka como Event Store estricto.

Analisis completo de esta y otras limitaciones (consistencia de estado, falta de agregados),
**clasificadas por nivel de evidencia** (arquitectura / posible-pero-con-costo / scaffolding),
con ejemplos y un script para reproducirlas (`./scripts/show_limitations.sh`):
[`.docs/limitaciones_v1.md`](./.docs/limitaciones_v1.md). Tambien incluye lo que Kafka **si**
hace bien (replay, fan-out): la otra mitad del experimento.

## Versiones y transicion V1 → V2

- **V1 (esta implementacion)**: Kafka como backbone de eventos, sin OCC ni agregados.
  El codigo de la V1 se preserva en un branch del repo como registro de la transicion.
- **V2 (proxima iteracion)**: EventStoreDB como event store (streams por agregado,
  concurrencia optimista, agregado `Account`) + Kafka como plataforma de distribucion
  para las proyecciones. Diseño completo en [`.docs/propuesta_v2.md`](./.docs/propuesta_v2.md).

## Apagar

```bash
docker compose down -v      # baja servicios y borra ./data/
```
