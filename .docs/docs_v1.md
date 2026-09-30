# Iteracion V1 

Guia breve de la version actual. Las fallas en detalle:
[limitaciones_v1.md](./limitaciones_v1.md). El diseno correctivo (V2):
[propuesta_v2.md](./propuesta_v2.md).

## 1. El sistema y que se esta probando

Un banco minimo: se crean cuentas, se depositan y retiran montos enteros, y se
consulta el saldo. Lo interesante es *como* esta construido: el estado no se guarda
como resultado de las operaciones. Cada operacion aceptada se registra como un
**evento** — un hecho inmutable del pasado ("se depositaron 100 en la cuenta 123") —
y el saldo actual se *deriva* de leer los eventos en orden. Eso es **Event Sourcing**
y su operacion central es el **fold**:

```
estado_actual = fold(eventos)   // aplicar los eventos uno a uno, en orden
```

Analogia contable: el libro de una cuenta no es el saldo, es la lista de movimientos;
el saldo es una consecuencia de reproducirla.

Dos hipotesis en juego: **¿Kafka funciona como log distribuido?** Si, lo demuestran
`demo.sh` y `replay.sh`. **¿Kafka aguanta como event store?** No, lo refuta
`show_limitations.sh`. Para que la evidencia sea limpia la implementacion es simple
pero correcta como sistema distribuido: la API de comandos **nunca lee el estado**,
asi cuando algo falla se puede atribuir a Kafka, a la arquitectura o al codigo.

## 2. Vocabulario minimo

| Terminos | Definicion |
|---|---|
| **Evento** | Hecho inmutable que ya ocurrio; el estado se deriva de el. Aqui `AccountCreated`, `MoneyDeposited`, `MoneyWithdrawn`. Un **event store** es la base append-only que los guarda; en la V1 ese rol lo cumple Kafka. |
| **Topic / Log** | Secuencia ordenada e inmutable de mensajes bajo un nombre: aqui `account-events`. Se divide en **particiones**, sub-secuencias dentro de las cuales si esta garantizado el orden: aqui hay 1. |
| **Offset** | Posicion de un mensaje dentro de su particion (0, 1, 2, ...): marcapaginas. |
| **Clave (key)** | Etiqueta del mensaje; Kafka enruta la misma clave a la misma particion y preserva su orden relativo. Aqui `key = accountId`. |
| **Consumer group** | Consumidores que comparten el progreso de lectura: Kafka recuerda por grupo el ultimo offset procesado, y dos grupos leen el mismo log independientemente. Decirle "hasta aqui procese" es **commitear**; si el commit va *despues* de aplicar el evento la entrega es **at-least-once** (tras una caida un evento puede llegar duplicado). |
| **Proyeccion / read model** | Vista derivada de los eventos para la lectura (la tabla de saldos). Descartable: se borra y se reconstruye desde el log (**replay** = reconstruccion desde el offset 0). |
| **CQRS** | Separar escritura (comandos) y lectura (consultas), incluso en procesos distintos. Consecuencia: **consistencia eventual** — hasta que la proyeccion procesa el comando, las lecturas muestran datos viejos. |

Terminos de Event Sourcing "duro" (agregado, expected version, append condicional,
OCC) no estan en el codigo porque la V1 **no** los implementa; se definen en el
glosario de [limitaciones_v1.md](./limitaciones_v1.md).

## 3. Arquitectura

```
comandos HTTP (:5087)                          lecturas HTTP (:8090)
     │                                              │
     ▼                                              ▼
┌─────────────────┐   produce    ┌──────────────┐   consume   ┌────────────────────┐
│ TransactionsAPI │ ───────────► │    Kafka     │ ──────────► │ ProjectionService  │
│     (.NET)      │   eventos    │account-events│             │        (Go)        │
│ command side    │              │ 1 particion, │             │ query side         │
│ sin balances    │              │  key=accountId│            │ duena del read model│
└─────────────────┘              └──────────────┘             └─────────┬──────────┘
                                                                        │ persiste
                                                                        ▼
                                                          ┌────────────────────────┐
                                                          │ SQLite account_balances │
                                                          └────────────────────────┘
```

- **`TransactionsAPI` (.NET, :5087)** — lado de comandos: recibe HTTP, valida minimo,
  publica eventos y responde. No conoce saldos ni expone lecturas.
- **Kafka** — el log: un unico topic con los tres tipos de evento. Ejerce de event
  store por decision del experimento.
- **`ProjectionService` (Go, :8090)** — lado de lecturas: consume, pliega los eventos
  contra una tabla SQLite y sirve el saldo por HTTP. Unica duena de esa base.

Que escritura y lectura sean procesos separados sin comunicacion directa es la esencia
de CQRS sobre Event Sourcing: el comando no "actualiza una fila", "propone un evento".
Las lecturas desactualizadas por milisegundos no son un defecto: es el precio del
desacople.

## 4. El recorrido de un deposito

`POST localhost:5087/accounts/123/deposit` con body `{"amount": 1000}`.

1. **La API valida poco**: que la cuenta exista y el monto > 0. **No valida saldo**: no
   lo conoce; el registro de cuentas es una lista en memoria (`Program.cs:19-22`).
2. **Publica el evento** con `AccountId` como clave, esperando la confirmacion del
   broker (por eso el log imprime `partition: 0, offset: 1`). Payload real, PascalCase:

   ```json
   {"EventType":"MoneyDeposited","AccountId":"123","Timestamp":"2026-09-26T15:01:22.1234567Z","Amount":1000}
   ```

3. **El consumer lee** el siguiente mensaje no procesado y lo deserializa contra un
   struct Go en camelCase (`domain/events.go`): `encoding/json` ignora mayusculas, y
   `Timestamp` no esta en el struct, asi que se descarta. Usa `FetchMessage` + commit
   explicito (no `ReadMessage`, que auto-commitea): eso define el at-least-once.
4. **La proyeccion aplica**: *read-modify-write* — leer el saldo, calcular el nuevo con
   `domain.Apply` (logica pura del fold), upsert en la unica tabla del read model:
   `account_balances(account_id TEXT PRIMARY KEY, balance INTEGER NOT NULL)`.
5. **Se commitea el offset**, solo si el paso 4 tuvo exito. Desde ahi
   `GET :8090/accounts/123/balance` responde `{"accountId":"123","balance":1000}`.

El `200` del paso 2 significa "el evento fue aceptado por el log", **no** "la operacion
fue aplicada al estado". Con la secuencia de `demo.sh` (crear, +1000, +500, -200) el log
queda con cuatro eventos (offsets 0 a 3) y el read model con `1300`: el `fold` literal
de esos cuatro eventos.

## 5. Donde vive la regla de negocio, y porque importa

La unica regla del sistema es "una cuenta no puede quedar en descubierto". En Event
Sourcing clasico se verifica en el **lado de escritura**, antes de aceptar el comando.
En la V1 se verifica en el **lado de lectura**, dentro de `Apply` (`domain/events.go`).

Consecuencia: un retiro invalido **ya fue aceptado por la API (HTTP 200) y ya existe en
el log** cuando la proyeccion lo detecta. El handler devuelve error y el consumer
**descarta el evento**: lo loguea y sigue sin commitearlo. Como el offset del lector ya
avanzo, en cuanto el siguiente evento exitoso se commitea el offset commiteado salta por
encima: el evento rechazado queda perdido para siempre y **log y read model divergen**.

No es un descuido: Kafka no puede rechazar un evento al escribirlo (no ofrece *append
condicional* ni *expected version*), asi que la invariante no tiene donde vivir aguas
arriba y queda empujada aguas abajo (`limitaciones_v1.md` §2c).

## 6. Ejecutarlo

`docker compose up -d --build` levanta `kafka` (KRaft de un nodo, sin Zookeeper:
`kafka:9092` en la red Docker, `localhost:9094` desde el host), `transactions-api`
(:5087), `projection` (:8090) y `kafka-ui` (:8080). Curls de ejemplo: README.

- El **topic no se declara en el compose**: lo crea la proyeccion al arrancar con 1
  particion y replicacion 1 (con un topic inexistente la asignacion del grupo quedaria
  vacia y el reader escucharia "nada" para siempre).
- El **SQLite vive en el repo** (`./data:/data`), sobrevive a un `restart`;
  `KAFKA_GROUP` y `KAFKA_START_OFFSET` vienen del entorno para que `replay.sh` use un
  grupo nuevo sin tocar el compose.
- Con 1 particion la clave `accountId` no altera el enrutamiento: el orden total que ve
  la proyeccion es un subproducto de la configuracion (con N particiones se seguiria
  garantizando el orden *por cuenta*).

Scripts: **`demo.sh`** (camino feliz: crea `123`, +1000, +500, -200, con *polling* del
balance tras cada operacion — consistencia eventual en accion; resultado `1300`).
**`replay.sh`** (detiene *solo* la proyeccion, borra la base y la levanta con un grupo
nuevo y `KAFKA_START_OFFSET=first`: reprocesa todo el log y el balance vuelve a `1300`;
el estado vive en el log, no en SQLite). **`show_limitations.sh`** (contraexperimento:
doble gasto, amnesia tras reinicio y re-inyeccion de un evento ya aplicado; deja el
sistema limpio al terminar).

## 7. Sintesis: por que se rompe donde se rompe

Niveles segun [limitaciones_v1.md](./limitaciones_v1.md): **1** = ningun codigo lo
arregla (arquitectura); **2** = Kafka permite el arreglo pero obliga a reconstruir a
mano lo que un event store da nativo; **0** = atajo de demo.

| Falla | Nivel | Origen |
|---|---|---|
| Doble gasto: dos retiros concurrentes se aceptan aunque solo uno puede aplicarse | 1 | Sin append condicional ni expected version la API no puede validar contra una version del estado (§5) |
| Amnesia: tras reiniciar, las cuentas creadas dejan de existir para la API | 2 | Registro en una `List<string>` en memoria; el arreglo (que la API pliegue el log) exige incrustar un consumer en el lado de comandos |
| No idempotencia: un evento re-entregado duplica su efecto | 2 | Sin `eventId` ni checkpoint; la deduplicacion es del consumidor y hay que inventarla |
| Descarte silencioso de eventos invalidos | consecuencia de 1 | La invariante no se valida aguas arriba y aguas abajo solo produce divergencia log ↔ estado (§5) |
| Sin agregados de dominio | 1 | No hay objeto `Account` que pliegue su stream y valide; sin append condicional la validacion queda "sin dientes" |

Lo que Kafka **si** hace bien: replay con offsets estables, orden por clave (de
*entrega*, no de *version*: no cierra la carrera del doble gasto), fan-out con grupos
propios a ritmo independiente, y consumidores que se caen y se reconstruyen sin afectar
a los demas.

Conclusion: Kafka es excelente como plataforma de distribucion de eventos y deficiente
como event store. De ahi la V2: **EventStoreDB como event store y Kafka solo como
plataforma de distribucion** ([propuesta_v2.md](./propuesta_v2.md)).

## 8. Contrato de eventos

Payload unico para los tres eventos (`EventType`, `AccountId`, `Timestamp`, `Amount`),
montos enteros sin moneda. Efecto en `Apply`: `AccountCreated` → `balance := 0`;
`MoneyDeposited` → `balance += Amount`; `MoneyWithdrawn` → `balance -= Amount` (error si
daria < 0). Transporte: clave = `AccountId`, value = JSON PascalCase, sin headers, sin
`eventId`, sin numero de version, sin Schema Registry; `Timestamp` viaja en ISO-8601 pero
Go lo ignora. Mapeo endpoint ↔ evento: README ("Endpoints").
