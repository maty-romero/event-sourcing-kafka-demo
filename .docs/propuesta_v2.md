# Propuesta V2 — EventStoreDB como event store + Kafka como plataforma de distribucion

> **Estado: implementada.** La guia de la V2 ya construida (arquitectura,
> config, contratos, scripts) esta en [docs_v2.md](./docs_v2.md); la
> verificacion de los criterios de aceptacion es `scripts/show_fixes.sh`.
>
> Este documento es el **registro de diseño antes de arrancar la V2**:
> contexto, motivacion, decisiones de arquitectura y plan de implementacion.
> La V1 queda preservada como branch en el repo para documentar la
> transicion; sus limitaciones estan analizadas en
> [limitaciones_v1.md](./limitaciones_v1.md).

---

## Contexto y motivacion

La V1 implementó un flujo event-driven basico: API .NET → Kafka → proyeccion
Go → SQLite. Sirvio para validar el happy path y el replay del read model,
pero dejo cinco limitaciones estructurales (documentadas con ejemplos y
script de reproduccion en [limitaciones_v1.md](./limitaciones_v1.md)):

1. Sin concurrencia optimista → el doble gasto es posible.
2. Estado inconsistente: registro de cuentas en memoria, proyeccion no
   idempotente, eventos descartados en silencio.
3. Sin agregados: las reglas de negocio se validan "despues", en la proyeccion.
4. Proyeccion sin checkpoints junto al read model y sin suscripcion
   catch-up+live (sumar proyecciones, en cambio, es trivial en Kafka:
   otro consumer group — eso es fortaleza, no limitacion).
5. Kafka usado como event store, rol para el que no esta diseñado.

**Conclusion de la V1:** algunas limitaciones se podrian parchear sobre Kafka
(idempotencia, reconstruir el estado del lado de comandos), pero cada parche
consiste en reconstruir a mano lo que un event store da nativo — y las
estructurales (append condicional, streams por agregado) no tienen parche
posible. No son bugs de implementacion: son consecuencias del storage. De ahi
sale esta propuesta: para hacer event sourcing en serio hace falta un event
store de verdad (streams por agregado, append condicional, subscriptions).

## Objetivo

Resolver las limitaciones de la V1 aplicando event sourcing correctamente:

| Limitacion V1                        | Solucion V2                                    |
|--------------------------------------|------------------------------------------------|
| 1. Sin concurrencia optimista        | Append condicional con *expected revision*     |
| 2. Consistencia de estado            | Estado reconstruido desde streams + proyecciones idempotentes con checkpoints |
| 3. Sin agregados                     | Agregado `Account` en el lado de comandos      |
| 4. Una sola proyeccion               | Multiples proyecciones Go independientes       |
| 5. Kafka como event store            | EventStoreDB = system of record; Kafka = distribucion |

## Alcance

**Incluye**: agregado `Account` con OCC en .NET, EventStoreDB como system of
record, puente EventStoreDB → Kafka, proyecciones Go idempotentes con
checkpoint, una segunda proyeccion de ejemplo, y scripts que demuestren que
lo que hoy falla queda resuelto.

**No incluye** (capaz en otra iteracion): autenticacion y cluster multi nodo
de EventStoreDB, snapshots automatizados, versionado de esquemas con
upcasting, y cualquier preocupacion de despliegue productivo.

---

## Arquitectura propuesta

```
                          ┌─────────────────────────┐
   comandos HTTP ────────►│     TransactionsAPI     │
   (crear, depositar,     │   (.NET + agregado      │
    retirar)              │    Account)             │
                          └───────────┬─────────────┘
                                      │ 1. cargar stream → fold → validar invariantes
                                      │ 2. append eventos con expected revision (OCC)
                                      ▼
                          ┌─────────────────────────┐
                          │      EventStoreDB       │  ◄── SYSTEM OF RECORD
                          │  stream "account-901":  │      (eventos inmutables,
                          │   0 AccountCreated      │       versionados por stream)
                          │   1 MoneyDeposited      │
                          │   2 MoneyWithdrawn      │
                          └───────────┬─────────────┘
                                      │ catch-up subscription
                                      ▼
                          ┌─────────────────────────┐
                          │   Publisher service     │  (Go o .NET)
                          │   ESDB → Kafka          │  idempotente: persiste la
                          └───────────┬─────────────┘  ultima posicion publicada
                                      ▼
                          ┌─────────────────────────┐
                          │         Kafka           │  ◄── PLATAFORMA DE DISTRIBUCIÓN
                          │ key = accountId         │      (fan-out, desacople)
                          └──────┬───────────┬──────┘
                                 ▼           ▼
                      ┌─────────────────┐ ┌─────────────────┐
                      │ Proyeccion Go   │ │ Proyeccion Go   │
                      │ balances        │ │ extracto/otros  │
                      │ + checkpoint    │ │ + checkpoint    │
                      └─────────────────┘ └─────────────────┘
```

---

## Decisiones y justificacion

### 1. EventStoreDB como event store (system of record)

Es una base de datos diseñada exactamente para esto:

- **Streams por agregado**: `account-901`, `account-902`, … cada cuenta tiene
  su secuencia ordenada e inmutable de eventos.
- **Optimistic concurrency nativo**: cada append declara una *expected
  revision*. Si otro escritor appendeo antes, EventStoreDB rechaza el append
  (`WrongExpectedVersion`) y el comando reintenta. Esto elimina el doble
  gasto de la V1 **en el lugar correcto**: el lado de escritura.
- **Catch-up subscriptions**: leen la historia y siguen en vivo; son el
  mecanismo natural para alimentar proyecciones y para el puente a Kafka.
- **Numeracion por evento dentro del stream**: permite checkpoints precisos y
  proyecciones idempotentes ("ya procese hasta la revision N de este stream").

### 2. Kafka solo como plataforma de distribucion

Kafka sigue en el sistema, pero con el rol que si hace bien:

- **Fan-out**: N proyecciones/servicios consumen los mismos eventos de forma
  independiente, cada uno a su ritmo (consumer groups).
- **Desacople**: los consumidores no dependen de EventStoreDB ni de su
  esquema de despliegue; pueden vivir en otros equipos/servicios.
- **Buffer**: absorbe picos si una proyeccion se atrasa.

> ¿Por que no las proyecciones directo desde EventStoreDB? Tambien es valido
> (y mas simple). Pasar por Kafka se justifica cuando hay varios consumidores
> heterogeneos o cuando queres mantener Kafka como backbone de integracion.
> Es la direccion que elegimos para esta iteracion; conectar proyecciones
> directo a EventStoreDB queda como simplificacion posible mas adelante.

### 3. Mantener el split .NET (comandos) + Go (proyecciones)

- Cada lenguaje queda donde ya funciona y es idiomatico: .NET para el lado de
  comandos (modelado de dominio, agregados) y Go para consumidores livianos.
- EventStoreDB tiene clientes oficiales para ambos (.NET y Go), y los
  clientes de Kafka ya los usamos (Confluent.Kafka y segmentio/kafka-go).
- No reescribimos lo que ya anda: la logica de `domain.Apply` de la V1 se
  reutiliza casi igual en las proyecciones Go.

### 4. Agregado `Account` en el lado de comandos (.NET)

El flujo correcto de un comando:

```
POST /accounts/901/withdraw {amount: 100}
  1. Leer el stream "account-901" desde EventStoreDB
  2. Fold de eventos → estado del agregado (balance=100, revision=2)
  3. Validar invariantes: balance - 100 >= 0  → OK
  4. Append MoneyWithdrawn con expectedRevision=2
     ├─ si EventStoreDB devuelve WrongExpectedVersion → reintentar desde 1
     └─ si la invariante falla → HTTP 400/409 ANTES de publicar nada
```

Consecuencias:

- Las reglas de negocio se validan **antes** de que exista el evento (no
  "despues" en la proyeccion como en la V1).
- El registro de cuentas deja de ser una lista en memoria: una cuenta existe
  si existe su stream. Los restarts ya no pierden nada.
- Respuestas HTTP honestas: `409 Conflict` si no hay saldo, en vez de `200`
  seguido de un descarte silencioso.

### 5. Proyecciones idempotentes con checkpoint

Cada proyeccion Go guarda, **en la misma transaccion SQLite** que el read
model, la ultima posicion procesada (stream + revision, u offset de Kafka).
Al re-consumir, detecta eventos ya aplicados y los saltea: reprocesar ya no
duplica el estado, y reconstruir = borrar read model + checkpoint y re-leer.

### 6. Publisher ESDB → Kafka idempotente

El puente guarda la ultima posicion de EventStoreDB publicada en Kafka (en el
propio EventStoreDB, en un stream `$published-position`, o en SQLite). Ante un
reinicio reanuda desde ahi sin duplicar (o publicando dos veces, que los
consumidores idempotentes toleran). Los mensajes a Kafka usan
`key = accountId` para conservar el orden por agregado, igual que en la V1.

---

## Alternativas consideradas

- **Parchear la V1 sobre Kafka** (validar en la API leyendo SQLite, locks,
  idempotencia por offset): mitiga sintomas pero no resuelve el append
  condicional ni los streams por agregado, y la API seguiria sin derivar su
  estado de los eventos. La descartamos: el problema es arquitectural.
- **Proyecciones directo desde EventStoreDB** (sin Kafka en el medio): mas
  simple y totalmente valido. La postergamos para mantener Kafka como
  plataforma de distribucion/fan-out, que es el foco de investigacion del
  proyecto.
- **Postgres como event store** (tabla de eventos con constraint unico por
  stream+version): viable, pero EventStoreDB ya trae streams, subscriptions
  catch-up y tooling especificos; para el demo su imagen oficial simplifica
  el despliegue.

---

## Cambios por componente

| Componente          | V1                                   | V2                                                        |
|---------------------|--------------------------------------|-----------------------------------------------------------|
| TransactionsAPI     | Publica a Kafka, lista en memoria    | Agregado `Account`; lee/escribe EventStoreDB con OCC      |
| Event store         | Kafka topic unico                    | EventStoreDB, stream por cuenta                           |
| Puente a Kafka      | —                                    | Publisher con catch-up subscription + posicion persistida |
| ProjectionService   | 1 proyeccion, sin checkpoint         | N proyecciones, idempotentes, con checkpoint              |
| Read model          | SQLite compartido entre procesos     | SQLite por proyeccion (cada una el suyo)                  |
| docker-compose      | kafka, api, projection, kafka-ui     | + eventstore (imagen `eventstore/eventstore`)             |

---

## Plan de implementacion

**Fase 1 — EventStoreDB + agregado (fix limitaciones 1, 2a, 3)**
- Agregar EventStoreDB a docker-compose (single node, sin auth para dev).
- Implementar el agregado `Account` en .NET: fold, invariantes, append con
  expected revision y reintento ante `WrongExpectedVersion`.
- Endpoints que devuelven errores correctos (409 sin saldo, 404 stream
  inexistente).
- Script de demo: el doble gasto de la V1 ahora devuelve un solo `200`.

**Fase 2 — Puente ESDB → Kafka (fix limitacion 5)**
- Publisher con catch-up subscription; posicion publicada persistida.
- Mantener el topic `account-events` con key=accountId.

**Fase 3 — Proyecciones idempotentes + segunda proyeccion (fix 2b, 2c, 4)**
- Checkpoint en la misma transaccion que el read model.
- Eventos invalidos → DLQ/topic de errores, nunca descarte silencioso.
- Segunda proyeccion de ejemplo (extracto de movimientos).
- Script de demo: reprocesar el topic ya no duplica balances.

**Fase 4 — Endurecimiento**
- Tests unitarios del agregado y de `Apply` (la V1 no tiene tests).
- Snapshots si los streams crecen mucho (opcional).
- Documentar runbook de reconstruccion de proyecciones.

---

## Criterios de aceptacion

La V2 esta lista cuando las mismas situaciones que
`scripts/show_limitations.sh` demuestra en la V1 se comportan asi:

1. **Doble gasto**: 5 retiros concurrentes con saldo para 1 → exactamente un
   `200` y cuatro `409`; el stream de la cuenta contiene un solo
   `MoneyWithdrawn`.
2. **Restart**: reiniciar la API no pierde cuentas; los comandos sobre
   cuentas existentes siguen funcionando.
3. **Redelivery**: re-entregar un evento ya procesado no altera el read model
   (checkpoint + deduplicacion).
4. **Proyecciones**: existe al menos una segunda proyeccion, reconstruible de
   forma independiente.

---

## Riesgos y notas

- **Consistencia ESDB → Kafka**: el puente puede caer entre append y publish;
  la catch-up subscription + posicion persistida lo resuelve (at-least-once
  hacia Kafka, consumidores idempotentes).
- **EventStoreDB single node** alcanza para el demo; en produccion pide
  cluster de 3 nodos.
- **Esquema de eventos**: la V2 tiene que incluir `eventId`, `accountId`,
  `revision` y version de esquema en el payload (la V1 ni siquiera numera los
  eventos).
