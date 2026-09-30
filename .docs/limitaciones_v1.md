# Limitaciones de la V1 — event-sourcing-kafka-demo

Este documento cuenta las limitaciones de la V1 del proyecto. Por cada una
hay una explicacion, un ejemplo concreto, donde esta en el codigo
y como reproducirla con `scripts/show_limitations.sh`.

## Los dos propositos de la V1

La V1 no es "un sistema bancario roto": es un experimento con dos hipotesis.

1. **Kafka como log distribuido** (proposito 1). ¿Hace Kafka lo que dice
   hacer? La V1 lo pone a prueba y **aprueba**: producir y consumir eventos con
   orden por clave, consumer groups con commit manual, y reconstruir el estado
   desde el log (`scripts/demo.sh` y `scripts/replay.sh`).
2. **Kafka como event store** (proposito 2). ¿Que pasa si lo obligas a ser la
   fuente de verdad de un sistema inspirado en event sourcing? Se rompe en
   puntos *especificos y identificables*, y de eso trata este documento.

La implementacion es deliberadamente **simple pero correcta como sistema
distribuido**: la API solo acepta comandos y publica; la proyeccion es la unica
duena de su base y sirve las lecturas por HTTP (`:8090`). Nada de trampas que
ensucien el experimento. Esto importa por honestidad argumental: una
limitacion solo cuenta como evidencia *contra Kafka* si no se puede arreglar
con mejor codigo. Por eso cada limitacion lleva su nivel puesto.

## Tres niveles de evidencia

| Nivel | Significado | Que prueba |
|---|---|---|
| **1 — Arquitectura** | Ningun codigo lo arregla: la operacion no existe en la API de Kafka | Kafka no puede ser event store |
| **2 — Posible, pero con costo** | Kafka permite el arreglo, pero exige reconstruir a mano lo que un event store da nativo (identidad de eventos, version de stream, escritura condicional) | Kafka *no deberia* forzarse a ser event store |
| **0 — Scaffolding** | Atajo de demo; no es evidencia de nada | — |

La reclasificacion importa: si todo se presenta como "culpa de Kafka", un
lector atento encuentra tres de esas cosas arreglables con un consumer mejor
escrito, y ahi el argumento pierde fuerza. Presentadas por nivel, las
conclusiones del informe son inatacables.

> **Contexto — que hace la V1**: una API .NET recibe comandos HTTP (crear
> cuenta, depositar, retirar) y los publica como eventos JSON a un topic de
> Kafka. Un servicio Go consume esos eventos y mantiene un *read model* en
> SQLite con el balance de cada cuenta, que el mismo sirve por HTTP. Kafka se
> usa como si fuera el event store del sistema.
>
> Para el happy path la V1 anda (`scripts/demo.sh`) y tambien demuestra que
> el balance se puede reconstruir desde el log (`scripts/replay.sh`). Pero
> con concurrencia o fallas parciales no resiste, y aca documentamos *que*
> falla, *por que*, y sobre todo *de quien* es la falla.

---

## 1. Sin control de concurrencia optimista (doble gasto) — nivel 1

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
codigo: esta en lo que Kafka acepta como instruccion. Por eso la limitacion es
de arquitectura.

**2. La proyeccion siempre contesta con datos viejos.** Aunque no haya
concurrencia: publicas un retiro, la proyeccion tarda unos milisegundos en
consumirlo, llega el segundo retiro, pregunta, y le dicen "100" cuando el log
ya decia "0". Un espejo que se actualiza tarde no puede ser el juez de una
regla de negocio.

**3. Y aunque la API lea el log de eventos directamente** (en vez de la
proyeccion), la carrera sigue: dos lectores ven la misma version, los dos
validan OK, los dos appendean. Sin el "solo si" del punto 1, la validacion es
decorativa.

**La prueba empirica**: el demo 1 de `scripts/show_limitations.sh` ya hace
exactamente esto. Antes de lanzar los 5 retiros lee el balance
(`GET :8090/accounts/901/balance` → 100): la informacion estaba ahi,
disponible para cualquiera que quisiera "preguntar antes". Los 5 retiros
entran en paralelo, los 5 ven un saldo que todavia no se toco, los 5 son
aceptados y el log queda con 5 eventos que el read model no puede reflejar.
Que el problema sobreviva a la "pregunta previa" es justamente la senal de
que no es un bug de implementacion.

**El unico parche que funcionaria sobre Kafka** confirma que es arquitectura:
hacer que *un solo* consumidor reciba todos los comandos de una cuenta, los
procese de a uno (validando contra el estado que el mismo lleva), y publique
los eventos. Eso funciona porque elimina la concurrencia en vez de controlarla
— y en el fondo estas reconstruyendo a mano las reglas de un event store
encima de Kafka, con mas piezas y sin ganar la version esperada.

---

## 2. Consistencia de estado

Aca hay tres problemas, de niveles distintos (y ese matiz es parte del
argumento):

### 2a. El registro de cuentas vive en memoria — nivel 2

La API guarda las cuentas creadas en una `List<string>` en memoria
(`Program.cs:19-22`), con "111" y "222" hardcodeados (esto ultimo es
scaffolding de demo, nivel 0: no demuestra nada sobre Kafka).

**Ejemplo**: creas la cuenta `902`, depositas 250, reinicias el contenedor de
la API… y te responde `404 not found` para la cuenta `902`, aunque sus
eventos siguen en Kafka y su balance sigue en el read model. La API nunca
reconstruye su estado a partir de los eventos, que es justamente la idea de
event sourcing.

**En criollo**: hay tres fuentes que deberian decir lo mismo (la lista en
memoria de la API, el log de eventos y el read model) pero cada una dice lo
suyo: despues del restart, para la API la cuenta no existe, para Kafka existe
(tiene eventos) y para el read model existe (tiene balance). El estado del
lado de comandos deberia *salir* de los eventos, no vivir en una estructura
aparte que se puede perder.

**Como se arregla con Kafka (y cuanto cuesta)**: la API, al arrancar, consume
el topic desde el inicio, filtra los `AccountCreated` y reconstruye su lista —
y para no quedarse atras, mantiene una suscripcion viva ademas. O sea: un
consumer group escondido dentro de la API de comandos. Funciona, Kafka lo
permite, y cada paso te aleja mas de "usar Kafka" y mas cerca de "escribir tu
propio event store encima". Ese impuesto es el argumento para no forzarlo.

### 2b. Proyeccion no idempotente — nivel 2

El handler hace *read-modify-write*: lee el balance, aplica el evento y
guarda (`ProjectionService/main.go:43-53`). Si un evento se procesa dos
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

**Como se arregla con Kafka (y cuanto cuesta)**: agregar un `eventId` (UUID)
al payload, y en el mismo transaction SQLite donde se aplica el evento,
guardar el offset/posicion procesada (checkpoint). Doble aplicacion se vuelve
imposible. Ojo con el matiz: **la at-least-once es el contrato de Kafka**, y
la idempotencia del consumidor es obligacion tuya en *cualquier* arquitectura
de eventos — no es una falla de Kafka. Lo que Kafka no te da gratis es la
*identidad* del evento: un event store real ya sabe que ese evento es la
revision 4 del stream `account-900`, sin que vos inventes el UUID ni la tabla
de dedupe. De nuevo: se puede, y se paga.

**Objeciones anticipadas**:

- *"¿Y por qué no exactly-once?"* Las transacciones de Kafka (KIP-98) hacen
  atomicos consume→produce→commit, pero **solo dentro de Kafka**. La
  proyeccion escribe en SQLite, afuera de esa transaccion: la ventana de
  crash sigue ahi, con otra forma. La entrega exactly-once con efectos
  externos es irresoluble en general; lo resoluble es el *efecto*
  exactly-once, y eso se llama idempotencia (el arreglo de arriba).
- *"¿Y por qué no usan Postgres, que maneja concurrencia?"* No es un problema
  de SQLite: tiene transacciones ACID completas, y el arreglo
  checkpoint+aplicar en una sola transaccion funciona ahi perfecto. Postgres
  tampoco puede invitar a Kafka a su transaccion: el doble-write cruza
  sistemas, sea cual sea el motor. Prueba de fuego: cambias SQLite por
  Postgres y corres el demo 3 — el deposito se aplica dos veces igual. Lo que
  Postgres aporta es concurrencia operativa (muchos escritores, throughput),
  ortogonal a la deduplicacion.

### 2c. Eventos descartados en silencio — consecuencia del nivel 1

Cuando `domain.Apply` falla (ej. un retiro sin saldo), el consumer loguea el
error y **descarta el evento**: sin reintento, sin dead-letter queue, sin
alerta. El log y la proyeccion quedan divergentes para siempre.

**En criollo**: en la V1 este descarte es, de hecho, el *unico* control de
descubierto que hay — pero esta en el lugar equivocado. Una proyeccion
deberia ser un espejo del log: si el log dice que algo paso, la proyeccion lo
refleja o falla a los gritos. Descartar en silencio convierte un problema de
validacion (que habia que rechazar al recibir el comando) en una
inconsistencia de datos permanente e invisible.

**Por que es consecuencia del nivel 1 y no un olvido**: como Kafka no puede
rechazar un append, la invariante "no girar en descubierto" no tiene donde
vivir aguas arriba y queda **forzada aguas abajo**, en la proyeccion. Y ahi
no hay opciones buenas: descartar (divergencia, lo que hace la V1), bloquear
el consumer (un evento malo frena todo el stream), o armar la maquinaria de
DLQ + reintentos + alertas. Con un event store con append condicional el
dilema ni siquiera existe: el evento invalido **nunca nace**.

### Bonus: sin atomicidad publicar/estado — nivel 0/2

En `POST /accounts`, la cuenta se agrega a la lista en memoria **antes** de
publicar. Si publicar a Kafka falla, te queda una cuenta registrada sin
evento `AccountCreated` (y si el evento se publica pero la respuesta HTTP
falla, no hay idempotencia del lado del comando).

---

## 3. Sin agregados de dominio — nivel 1 (y 2 para llegar al 1)

No existe un agregado `Account`. Consecuencias:

- Las reglas de negocio (ej. "no girar en descubierto") se verifican **solo
  en la proyeccion**, o sea *despues* de que el evento ya fue aceptado y
  publicado. En event sourcing las invariantes se validan en el lado de
  escritura, antes de appendear el evento.
- El lado de comandos no reconstruye el estado de la cuenta desde su stream
  de eventos; directamente no conoce el estado.
- No hay streams por agregado: los eventos de todas las cuentas van a un
  unico topic, sin numeracion por agregado.

**El matiz por niveles**: construir el agregado *es posible* sobre Kafka —
la API podria foldear el stream de la cuenta y validar contra ese estado
(nivel 2, con el impuesto de 2a: leer el log desde el arranque, sostener una
suscripcion viva, y scannear el topic entero para aislar "el stream de la
901" porque Kafka no tiene lectura por agregado). Lo que **no es posible a
nivel ninguno** es el paso final: que el append del evento validado sea
condicional. Sin eso, el agregado valida contra un estado que puede dejar de
ser verdadero entre la validacion y el append: la invariante queda sin
dientes. Por eso la V1 no tiene agregados: el esfuerzo de construirlos no
compra la garantia que los hace tener sentido.

**Como se veria con un agregado**: un objeto `Account` que se construye
leyendo el stream de la cuenta (aplicando cada evento en orden, o *fold*),
expone operaciones como `Withdraw(amount)` que validan invariantes ("hay
saldo suficiente") y, si pasan, devuelven el evento `MoneyWithdrawn` listo
para appendear **contra la version leida**. El agregado es el unico lugar
donde se decide si un comando es valido, y su estado sale siempre de los
eventos.

---

## 4. La causa raiz: Kafka como event store — nivel 1

Kafka es barbaro como plataforma de distribucion, pero como *event store* le
falta lo esencial:

| Necesidad de event sourcing          | Kafka | EventStoreDB |
|--------------------------------------|-------|--------------|
| Streams por agregado                 | ✗ (topics/particiones) | ✓ (`account-901`) |
| Append condicional (expected version)| ✗ | ✓ |
| Numeracion por evento en el stream   | ✗ (offset global) | ✓ |
| Subscripciones catch-up              | ✗ (consumer groups) | ✓ |
| Identidad de evento (para dedupe)    | ✗ (la inventa el productor) | ✓ (revision del stream) |

Por eso la V2 (ver [propuesta_v2.md](./propuesta_v2.md)) usa **EventStoreDB
como event store** y **Kafka solo como plataforma de distribucion**.

---

## 5. Lo que Kafka si hace bien (y la V1 muestra)

Este documento seria media verdad si solo contara fallos. La otra mitad del
experimento (proposito 1) tambien tiene resultados, y son positivos:

- **Replay**: el log se puede releer completo y reconstruir el read model
  desde cero (`scripts/replay.sh`). Esta es la idea fundante de event
  sourcing, y Kafka la habilita de verdad.
- **Orden por clave**: particionando por `accountId`, los eventos de una
  cuenta llegan en orden. (Ojo: orden *de entrega*, no *version* — el orden
  no cierra la carrera del §1.)
- **Fan-out**: del mismo log pueden comer N consumidores con grupos propios.
  Una limitacion que se listaba antes, "solo hay una proyeccion", era falsa:
  sumar una segunda proyeccion (extracto, totales por dia) es levantar otro
  consumer group sobre el mismo topic, trivial y sin duplicar el log. La V1
  tiene una sola proyeccion por **scope**, no por limitacion — y eso no es
  evidencia contra Kafka, es Kafka haciendo exactamente lo que mejor hace.
- **Consumidores independientes**: cada proyeccion avanza a su ritmo y puede
  reconstruirse sin tocar a las demas.

Lo unico del apartado proyecciones que *si* es nivel 1: no existe la
suscripcion "catch-up + live" en una sola operacion (leer el historico y
seguir en vivo, con la posicion guardada atomicamente junto al read model).
`replay.sh` lo simula a mano con un grupo nuevo; un event store lo da nativo.

---

## Como reproducir todo esto

```bash
docker compose up -d --build
./scripts/show_limitations.sh
```

El script es **independiente** de `demo.sh` y `replay.sh` (lo podes correr en
cualquier orden) y al final deja el sistema en estado limpio (borra topic y
read model, reinicia la API).
Demuestra, con su nivel: doble gasto (1, nivel 1), amnesia tras restart (2a,
nivel 2), proyeccion no idempotente (2b, nivel 2) y el descarte silencioso
como consecuencia del §1 (2c).

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
  leer (ej. la tabla de balances). Se puede borrar y reconstruir. En la V1 el
  ProjectionService es su unico dueno y la sirve por HTTP.
- **Fan-out**: que varios consumidores lean el mismo log con grupos propios,
  cada uno a su ritmo. Fortaleza nativa de Kafka.
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
- **Scaffolding**: atajo de implementacion que solo existe para que el demo
  sea chico (listas hardcodeadas, datos de ejemplo). No es evidencia ni a
  favor ni en contra de nada.
