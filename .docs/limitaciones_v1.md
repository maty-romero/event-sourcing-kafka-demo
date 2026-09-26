# Limitaciones de la V1 — event-sourcing-kafka-demo

Este documento cuenta las limitaciones de la V1 del proyecto. Por cada una
hay una explicacion, un ejemplo concreto, donde esta en el codigo
y como reproducirla con `scripts/show_limitations.sh`.

> **Contexto — que hace la V1**: una API .NET recibe comandos HTTP (crear
> cuenta, depositar, retirar) y los publica como eventos JSON a un topic de
> Kafka. Un servicio Go consume esos eventos y mantiene un *read model* en
> SQLite con el balance de cada cuenta. Kafka se usa como si fuera el event
> store del sistema.
>
> Para el happy path la V1 anda (`scripts/demo.sh`) y tambien demuestra que
> el balance se puede reconstruir desde el log (`scripts/replay.sh`). Pero
> con concurrencia o fallas parciales no resiste, que es justamente lo que
> documentamos aca => consecuencias de la arquitectura elegida.

---

## 1. Sin control de concurrencia optimista (doble gasto)

**El problema**: si 2 comandos llegan a la vez para la misma cuenta, la API
acepta **los dos** sin fijarse contra que version del estado esta operando.
Ni siquiera sabe el balance: publica el evento  simplemente.

**Ejemplo concreto**:

1. Cuenta `901` con balance **100**.
2. Dos clientes mandan `withdraw 100` **al mismo tiempo**.
3. La API les responde `200 OK` a los dos (solo valida que la cuenta exista y
   que el monto sea > 0).
4. Se publican dos eventos `MoneyWithdrawn` a Kafka.
5. La proyeccion aplica el primero (balance → 0); el segundo **falla**
   ("balance insuficiente") y se **descarta en silencio**.

**Resultado**: el cliente recibio "OK" por un retiro que nunca se aplico. El
log de eventos dice una cosa (2 retiros) y el read model otra (solo 1
aplicado). El banco "presto" plata que no existe.

**En criollo**: la API es un cajero que acepta retiros sin mirar la caja:
anota el pedido y lo manda para atras (Kafka). Recien al final, cuando la
proyeccion procesa los eventos, alguien se da cuenta de que no alcanzaba la
plata — y en vez de rechazar la operacion, la tira en silencio. Deberia ser
al reves: validar *antes* de aceptar el comando y rechazarlo con un error
claro.

**Por que pasa**: no hay ningun mecanismo de *expected version* (ver
glosario). Kafka no ofrece append condicional: no le podes decir "aceptame
este evento solo si el stream esta en la version N".

**Codigo**: `TransactionsAPI/Program.cs` (el endpoint
`/withdraw` no chequea ni balance ni version) y
`ProjectionService/consumer/consumer.go:96-99` (el evento que falla se loguea
y se descarta).

### ¿Y si "preguntamos" antes de publicar? (por que esto es de arquitectura, no de codigo)

La objecion natural: *"basta con que la API lea el balance antes de aceptar el
retiro y lo rechace si no alcanza"*. Suena obvio, y sin embargo no resuelve el
problema. Tres razones, en orden de maldad:

**1. Preguntar y publicar son dos pasos separados.** Los 5 clientes preguntan
a la vez, los 5 leen "balance 100", los 5 se convencen de que pueden retirar y
los 5 publican. Nadie dijo "alto, alguien acaba de retirar". Para cerrar esa
ventana hace falta que la *escritura misma* sea condicional: "grabate este
evento **solo si** el stream sigue en la version que yo lei". Eso lo decide el
almacen de datos en el momento del append, y Kafka no ofrece ese "solo si" —
no importa cuantas preguntas le agregues arriba. El hueco no esta en nuestro
codigo: esta en la API del storage. Por eso la limitacion es de arquitectura.

**2. La proyeccion siempre contesta con datos viejos.** Aunque no haya
concurrencia: publicas un retiro, la proyeccion tarda unos milisegundos en
consumirlo, llega el segundo retiro, pregunta, y le dicen "100" cuando el log
ya decia "0". Un espejo que se actualiza tarde no puede ser el juez de una
regla de negocio.

**3. Y aunque la API lea el log de eventos directamente** (en vez de la
proyeccion), la carrera sigue: dos lectores ven la misma version, los dos
validan OK, los dos appendean. Sin el "solo si" del punto 1, la validacion es
decorativa.

**La prueba empirica**: el API ya tiene inyectado un lector de balance
(`Program.cs:11`, un `IBalanceReader` que lee el SQLite de la proyeccion) y
ningun comando lo usa. Si lo usaras para rechazar cuando no hay saldo y corres
los 5 retiros paralelos del script, **los 5 pasan el chequeo igual** (los 5
leen 100 antes de que la proyeccion aplique el primero) y el doble gasto
ocurre igual. Que el problema sobreviva al "parche obvio" es justamente la
señal de que no es un bug de implementacion.

**El unico parche que funcionaria sobre Kafka** confirma que es arquitectura:
hacer que *un solo* consumidor reciba todos los comandos de una cuenta, los
procese de a uno (validando contra el estado que el mismo lleva), y publique
los eventos. Eso funciona porque elimina la concurrencia en vez de controlarla
— y en el fondo estas reconstruyendo a mano las reglas de un event store
encima de Kafka, con mas piezas y sin ganar la version esperada.

---

## 2. Consistencia de estado

Aca hay tres problemas distintos:

### 2a. El registro de cuentas vive en memoria

La API guarda las cuentas creadas en una `List<string>` en memoria
(`Program.cs:20-23`), con "111" y "222" hardcodeados.

**Ejemplo**: creas la cuenta `902`, depositas 250, reinicias el contenedor de
la API… y te responde `404 not found` para la cuenta `902`, aunque sus
eventos siguen en Kafka y su balance sigue en SQLite. La API nunca
reconstruye su estado a partir de los eventos, que es justamente la idea de
event sourcing.

**En criollo**: hay tres fuentes que deberian decir lo mismo (la lista en
memoria de la API, el log de eventos y el read model) pero cada una dice lo
suyo: despues del restart, para la API la cuenta no existe, para Kafka existe
(tiene eventos) y para SQLite existe (tiene balance). Encima la lista arranca
con "111" y "222" hardcodeadas, que capaz no tienen ni un evento en el log.
El estado del lado de comandos deberia *salir* de los eventos, no vivir en
una estructura aparte que se puede perder.

### 2b. Proyeccion no idempotente

El handler hace *read-modify-write*: lee el balance, aplica el evento y
guarda (`ProjectionService/main.go:42-52`). Si un evento se procesa dos
veces, el balance se altera dos veces. Como los eventos **no tienen
eventId**, la proyeccion no puede detectar duplicados, y como **no guarda
checkpoints**, no sabe que ya proceso.

**Ejemplo**: cuenta `900` con un deposito de 500 (balance 500). Con entrega
*at-least-once* y commit manual, si el consumer se muere entre procesar el
evento y commitear el offset, al reiniciar Kafka **re-entrega** ese evento y
el deposito se aplica de nuevo: el balance queda en **1000**. El script lo
demuestra re-inyectando en el topic un evento ya aplicado: la proyeccion lo
procesa sin darse cuenta de que es duplicado.

> **Detalle fino**: re-consumir el topic *completo* desde el offset 0 no
> muestra el problema, porque `AccountCreated` resetea el balance a 0 y la
> historia se vuelve a plegar igual. Por eso `scripts/replay.sh` "funciona"
> solo si borras la base antes, y por eso en la vida real el problema aparece
> con redeliveries **parciales** (algunos eventos re-entregados), que es lo
> que simula `scripts/show_limitations.sh`.

### 2c. Eventos descartados en silencio

Cuando `domain.Apply` falla (ej. un retiro sin saldo), el consumer loguea el
error y **descarta el evento**: sin reintento, sin dead-letter queue, sin
alerta. El log y la proyeccion quedan divergentes para siempre.

**En criollo**: en la V1 este descarte es, de hecho, el *unico* control de
descubierto que hay — pero esta en el lugar equivocado. Una proyeccion
deberia ser un espejo del log: si el log dice que algo paso, la proyeccion lo
refleja o falla a los gritos. Descartar en silencio convierte un problema de
validacion (que habia que rechazar al recibir el comando) en una
inconsistencia de datos permanente e invisible.

### Bonus: sin atomicidad publicar/estado

En `POST /accounts`, la cuenta se agrega a la lista en memoria **antes** de
publicar. Si publicar a Kafka falla, te queda una cuenta registrada sin
evento `AccountCreated` (y si el evento se publica pero la respuesta HTTP
falla, no hay idempotencia del lado del comando).

---

## 3. Sin agregados de dominio

No existe un agregado `Account`. Consecuencias:

- Las reglas de negocio (ej. "no girar en descubierto") se verifican **solo
  en la proyeccion**, o sea *despues* de que el evento ya fue aceptado y
  publicado. En event sourcing las invariantes se validan en el lado de
  escritura, antes de appendear el evento.
- El lado de comandos no reconstruye el estado de la cuenta desde su stream
  de eventos; directamente no conoce el estado.
- No hay streams por agregado: los eventos de todas las cuentas van a un
  unico topic, sin numeracion por agregado.

**Como se veria con un agregado**: un objeto `Account` que se construye
leyendo el stream de la cuenta (aplicando cada evento en orden, o *fold*),
expone operaciones como `Withdraw(amount)` que validan invariantes ("hay
saldo suficiente") y, si pasan, devuelven el evento `MoneyWithdrawn` listo
para appendear. El agregado es el unico lugar donde se decide si un comando
es valido, y su estado sale siempre de los eventos.

---

## 4. Una sola proyeccion

Solo existe la proyeccion de balances. Ademas:

- No guarda **checkpoints** (que offset/posicion ya proceso) junto al read
  model, asi que reconstruir o versionar proyecciones es a mano (borrar la
  base y re-consumir desde cero).
- No hay estructura para sumar una segunda proyeccion (ej. extracto de
  movimientos, totales por dia) sin duplicar codigo de consumo.

**En criollo**: una de las gracias de event sourcing es que del log podes
derivar *muchas* vistas distintas de los mismos eventos (balances, extractos,
metricas). En la V1 el log alimenta una sola vista, y sumar otra implicaria
copiar el consumer y rezar para que las dos procesen igual. Sin checkpoints,
cada proyeccion nueva tendria que re-consumir todo desde cero y no podria
retomar despues de una caida sin riesgo de duplicar.

---

## 5. La causa raiz: Kafka como event store

Kafka es barbaro como plataforma de distribucion, pero como *event store* le
falta lo esencial:

| Necesidad de event sourcing          | Kafka | EventStoreDB |
|--------------------------------------|-------|--------------|
| Streams por agregado                 | ✗ (topics/particiones) | ✓ (`account-901`) |
| Append condicional (expected version)| ✗ | ✓ |
| Numeracion por evento en el stream   | ✗ (offset global) | ✓ |
| Subscripciones catch-up              | ✗ (consumer groups) | ✓ |

Por eso la V2 (ver [propuesta_v2.md](./propuesta_v2.md)) usa **EventStoreDB
como event store** y **Kafka solo como plataforma de distribucion**.

---

## Como reproducir todo esto

```bash
docker compose up -d --build
./scripts/show_limitations.sh
```

El script es **independiente** de `demo.sh` y `replay.sh` (lo podes correr en
cualquier orden) y al final deja el sistema en estado limpio (borra topic y
read model, reinicia la API).
Demuestra: doble gasto (1), amnesia tras restart (2a), proyeccion no
idempotente (2b) y descarte de eventos (2c).

---

## Glosario

- **Evento**: un hecho inmutable que ya paso ("se depositaron 100"). Es la
  fuente de verdad; el estado actual se deriva de los eventos.
- **Event store**: base de datos especializada en guardar eventos de forma
  append-only (solo se agregan, nunca se modifican ni se borran).
- **Stream**: secuencia ordenada de eventos de una entidad, ej. todos los
  eventos de la cuenta `901`.
- **Fold**: armar el estado aplicando los eventos uno a uno en orden (como un
  `reduce`): `estado = fold(eventos)`. Es la operacion central de event
  sourcing, tanto para reconstruir un agregado como para proyectar.
- **Agregado (aggregate)**: entidad de dominio que junta estado y reglas de
  negocio. En event sourcing se reconstruye leyendo sus eventos, valida cada
  comando y produce eventos nuevos.
- **Control de concurrencia optimista (OCC)**: estrategia "confio y
  verifico": al escribir declaras que version del stream leiste (*expected
  version*). Si otro escribio antes, la escritura se rechaza y el cliente
  reintenta. No usa locks.
- **Expected version / expected revision**: el numero de version que esperas
  que tenga el stream al momento de appendear. Es el mecanismo que hace
  posible el OCC en event stores.
- **Append condicional**: escribir un evento con una condicion de version
  ("grabalo solo si el stream esta en la version N"); si la condicion no se
  cumple, el almacen rechaza la escritura de forma atomica. Es lo que
  transforma una validacion "decorativa" (preguntar antes) en una garantia
  real. Kafka no lo ofrece; EventStoreDB si.
- **Proyeccion / read model**: vista derivada de los eventos, optimizada para
  leer (ej. la tabla de balances). Se puede borrar y reconstruir.
- **Idempotencia**: procesar el mismo evento 2 veces da el mismo resultado
  que procesarlo 1 vez.
- **At-least-once**: garantia de entrega "al menos una vez": los mensajes
  pueden llegar duplicados; el consumidor tiene que tolerarlo (→ idempotencia).
- **Offset**: posicion numerica de un mensaje dentro de una particion de Kafka.
- **Consumer group**: conjunto de consumidores que comparten el progreso de
  lectura de un topic; Kafka les recuerda el offset commiteado.
- **Checkpoint**: registro persistente de "hasta donde procese", guardado
  junto al read model para poder retomar/reconstruir tranquilo.
- **Dead-letter queue (DLQ)**: cola donde dejas los eventos que no pudiste
  procesar, para inspeccionarlos en vez de perderlos.
- **Catch-up subscription**: subscripcion de EventStoreDB que lee eventos
  historicos y despues sigue en vivo; ideal para alimentar proyecciones.
