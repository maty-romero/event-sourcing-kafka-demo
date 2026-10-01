#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# show_fixes.sh — demuestra que las limitaciones de la V1
# (documentadas en .docs/limitaciones_v1.md) quedan resueltas en la V2.
#
# Es el espejo de scripts/show_limitations.sh de la V1 (preservada en el
# branch kafka-initial-v1): las mismas situaciones, ahora con EventStoreDB
# como system of record + Kafka como plataforma de distribucion.
#
# Criterios de aceptacion de .docs/propuesta_v2.md:
#   1. Doble gasto: 5 retiros concurrentes con saldo para 1 -> un 200, cuatro 409,
#      y el stream de la cuenta contiene UN solo MoneyWithdrawn.
#   2. Restart: reiniciar la API no pierde cuentas.
#   3. Redelivery: re-entregar un evento ya procesado no altera el read model.
#   4. Proyecciones: una segunda proyeccion, reconstruible de forma independiente.
#   Extra: el evento invalido no se descarta en silencio -> DLQ.
#
# Al final deja el sistema en estado limpio (topic + read models + ESDB).
# Las cuentas usan los mismos IDs de la V1 (901/902/900) para que el espejo
# sea directo. Atencion: si una corrida se interrumpe a mitad quedan creadas
# y la siguiente falla con "no se pudo crear" (409) — la limpieza del final es
# lo que lo previene; tras un corte, "docker compose restart eventstore" y ya.
# ============================================================================

API="${API:-http://localhost:5087}"
PROJ="${PROJ:-http://localhost:8090}"
STMT="${STMT:-http://localhost:8091}"
ESDB="${ESDB:-http://localhost:2113}"

S="$(date +%H%M%S)"          # solo para nombres de grupo de replay
ACC_OCC="901"                # demo 1: concurrencia / doble gasto
ACC_AMNESIA="902"            # demo 2: estado tras restart
ACC_IDEMP="900"              # demo 3: redelivery
ACC_DLQ="903"                # extra: evento invalido -> DLQ

say()  { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓ %s\033[0m\n' "$*"; }
bad()  { printf '  \033[31m✗ %s\033[0m\n' "$*"; exit 1; }
note() { printf '  %s\n' "$*"; }

command -v jq >/dev/null || { echo "se necesita jq instalado"; exit 1; }

post_status() { # path body -> HTTP code
  curl -s -o /dev/null -w '%{http_code}' -X POST "$API$1" \
    -H 'Content-Type: application/json' -d "$2"
}

get_balance() { # account -> balance | ""
  curl -fsS "$PROJ/accounts/$1/balance" 2>/dev/null | jq -r .balance 2>/dev/null || echo ""
}

poll_balance() { # account expected
  local got="" i
  for i in $(seq 1 40); do
    got=$(get_balance "$1")
    [[ "$got" == "$2" ]] && { ok "balance cuenta $1 = $got"; return 0; }
    sleep 0.5
  done
  bad "timeout: balance cuenta $1 = '$got', esperaba $2"
}

wait_api() {
  local i code
  for i in $(seq 1 40); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$API/accounts" || true)
    [[ -n "$code" && "$code" != "000" ]] && return 0
    sleep 0.5
  done
  bad "API no esta lista en $API"
}

esdb_stream() { # account -> JSON del stream account-<id> (API HTTP de ESDB)
  curl -fsS -H 'Accept: application/vnd.eventstore.events+json' \
    "$ESDB/streams/account-$1/head/1000?embed=content" 2>/dev/null || echo '{"entries":[]}'
}

esdb_count() { # account eventType -> cantidad en el STREAM de EventStoreDB
  esdb_stream "$1" | jq --arg t "$2" '[.entries[]? | select(.content.eventType == $t)] | length'
}

say "0) Setup: stack arriba"
docker compose up -d >/dev/null
wait_api
ok "API lista en $API"

# ----------------------------------------------------------------------------
say "Demo 1 — Doble gasto: ahora hay append condicional con expected revision"
# ----------------------------------------------------------------------------
info "cuenta $ACC_OCC: se crea y se depositan 100"
[[ "$(post_status /accounts "{\"accountId\":\"$ACC_OCC\"}")" == "201" ]] || bad "no se pudo crear la cuenta"
[[ "$(post_status "/accounts/$ACC_OCC/deposit" '{"amount":100}')" == "200" ]] || bad "no se pudo depositar"
poll_balance "$ACC_OCC" 100

info "se lanzan 5 retiros de 100 EN PARALELO (solo hay saldo para 1)"
tmp=$(mktemp)
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST "$API/accounts/$ACC_OCC/withdraw" \
    -H 'Content-Type: application/json' -d '{"amount":100}' >> "$tmp" &
done
wait
codes=$(sort "$tmp" | uniq -c | tr '\n' ' ')
ok200=$(grep -c '^200$' "$tmp" || true)
n409=$(grep -c '^409$' "$tmp" || true)
rm -f "$tmp"
info "respuestas HTTP: $codes"
[[ "${ok200:-0}" == "1" && "${n409:-0}" == "4" ]] || bad "esperaba exactamente un 200 y cuatro 409"
ok "FIXED: un solo 200 y cuatro 409 — el descubierto se rechaza ANTES de publicar"

poll_balance "$ACC_OCC" 0
n=$(esdb_count "$ACC_OCC" MoneyWithdrawn)
[[ "$n" == "1" ]] || bad "esperaba 1 MoneyWithdrawn en el stream, hay $n"
ok "FIXED: el stream account-$ACC_OCC contiene exactamente 1 MoneyWithdrawn:"
esdb_stream "$ACC_OCC" | jq -r '.entries | reverse | .[] | .content | "    [\(.eventNumber)] \(.eventType) amount=\(.data.amount)"'
note "V1: los 5 eran aceptados y 4 eventos quedaban en el log sin aplicarse."

# ----------------------------------------------------------------------------
say "Demo 2 — Restart de la API: la cuenta vive en su stream, no en memoria"
# ----------------------------------------------------------------------------
info "cuenta $ACC_AMNESIA: se crea y se depositan 250"
[[ "$(post_status /accounts "{\"accountId\":\"$ACC_AMNESIA\"}")" == "201" ]] || bad "no se pudo crear la cuenta"
[[ "$(post_status "/accounts/$ACC_AMNESIA/deposit" '{"amount":250}')" == "200" ]] || bad "no se pudo depositar"
poll_balance "$ACC_AMNESIA" 250

info "se reinicia transactions-api"
docker compose restart transactions-api >/dev/null
wait_api

code=$(post_status "/accounts/$ACC_AMNESIA/deposit" '{"amount":10}')
[[ "$code" == "200" ]] || bad "tras el restart el deposit dio HTTP $code (esperaba 200)"
ok "FIXED: tras reiniciar la API el deposit sigue dando 200 — la cuenta existe porque su stream existe"
poll_balance "$ACC_AMNESIA" 260
note "V1: el deposit tras el restart daba 404 (registro en memoria perdido)."

# ----------------------------------------------------------------------------
say "Demo 3 — Redelivery: checkpoint junto al read model, proyeccion idempotente"
# ----------------------------------------------------------------------------
info "cuenta $ACC_IDEMP: se crea y se depositan 500"
[[ "$(post_status /accounts "{\"accountId\":\"$ACC_IDEMP\"}")" == "201" ]] || bad "no se pudo crear la cuenta"
[[ "$(post_status "/accounts/$ACC_IDEMP/deposit" '{"amount":500}')" == "200" ]] || bad "no se pudo depositar"
poll_balance "$ACC_IDEMP" 500

info "se re-inyecta en Kafka el MISMO evento MoneyDeposited (revision 1, duplicado de verdad)"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic account-events \
  --property parse.key=true --property key.separator='|' >/dev/null <<EOF
$ACC_IDEMP|{"eventId":"dup-$S","eventType":"MoneyDeposited","accountId":"$ACC_IDEMP","streamId":"account-$ACC_IDEMP","revision":1,"schemaVersion":1,"timestamp":"2026-10-01T00:00:00Z","data":{"amount":500}}
EOF
sleep 3
bal=$(get_balance "$ACC_IDEMP")
[[ "$bal" == "500" ]] || bad "el readelivery duplico el balance: $bal (esperaba 500)"
ok "FIXED: el duplicado llego y el balance sigue en 500 (checkpoint: revision 1 ya procesada)"
docker compose logs projection-balances --since 15s 2>&1 | grep "ya procesado" | tail -1 | sed 's/^/    /' || true
note "V1: el mismo truco dejaba el balance en 1000."

# ----------------------------------------------------------------------------
say "Extra — Evento invalido: DLQ, no descarte silencioso"
# ----------------------------------------------------------------------------
info "se inyecta un mensaje que no es JSON valido"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic account-events \
  --property parse.key=true --property key.separator='|' >/dev/null <<EOF
$ACC_DLQ|esto no es json
EOF
sleep 3
dlq=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic account-events.dlq \
  --from-beginning --timeout-ms 4000 2>/dev/null | tail -1 || true)
[[ -n "$dlq" ]] || bad "el mensaje invalido no llego al topic DLQ"
ok "FIXED: el mensaje invalido quedo registrado en account-events.dlq:"
printf '    %s\n' "$dlq"
note "V1: los eventos que fallaban se descartaban en silencio y log/read model divergian."

# ----------------------------------------------------------------------------
say "Demo 4 — Segunda proyeccion (extracto) reconstruida sin tocar la primera"
# ----------------------------------------------------------------------------
info "el extracto de $ACC_OCC (segunda proyeccion, consumer group propio) tiene su historia"
m=$(curl -fsS "$STMT/accounts/$ACC_OCC/movements" | jq '.movements | length')
[[ "$m" == "3" ]] || bad "esperaba 3 movimientos (create/deposit/withdraw), hay $m"
ok "fan-out: balances y statement comen del mismo topic, cada uno a su ritmo"

info "se borra el read model del extracto y se reconstruye con un grupo nuevo"
docker compose stop projection-statement >/dev/null
docker compose run --rm --no-deps -T --entrypoint sh projection-statement \
  -c "rm -f /data/statement.db /data/statement.db-wal /data/statement.db-shm"
STATEMENT_KAFKA_GROUP=statement-replay-$S STATEMENT_KAFKA_START_OFFSET=first \
  docker compose up -d projection-statement >/dev/null

for i in $(seq 1 40); do
  got=$(curl -fsS "$STMT/accounts/$ACC_OCC/movements" 2>/dev/null | jq -r '.movements | length' 2>/dev/null || echo "")
  [[ "$got" == "3" ]] && break
  sleep 0.5
done
[[ "$got" == "3" ]] || bad "el extracto reconstruido dio $got movimientos"
ok "FIXED: extracto reconstruido identico (3 movimientos), reconstruible independiente"

bal=$(get_balance "$ACC_OCC")
[[ "$bal" == "0" ]] || bad "la reconstruccion del extracto toco balances: $bal"
ok "FIXED: la proyeccion de balances no fue tocada durante la reconstruccion"

# ----------------------------------------------------------------------------
say "Restaurar estado inicial (como instalacion limpia)"
# ----------------------------------------------------------------------------
info "se borran read models, topics y EventStoreDB (mem db), y se reinicia todo"
docker compose stop publisher projection-balances projection-statement >/dev/null
docker compose run --rm --no-deps -T --entrypoint sh projection-balances \
  -c "rm -f /data/balances.db /data/balances.db-wal /data/balances.db-shm"
docker compose run --rm --no-deps -T --entrypoint sh projection-statement \
  -c "rm -f /data/statement.db /data/statement.db-wal /data/statement.db-shm"
for t in account-events account-events.dlq; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
done
docker compose restart eventstore >/dev/null   # mem db: nace vacia
docker compose up -d publisher projection-balances projection-statement >/dev/null
wait_api
ok "sistema en estado limpio (ESDB vacio, topics borrados, read models vacios)"

say "Resumen: las 5 limitaciones de la V1"
info "1. Doble gasto        -> append condicional con expected revision (demo 1)"
info "2a. Estado en memoria -> la cuenta vive en su stream de ESDB (demo 2)"
info "2b. No idempotencia   -> eventId + checkpoint en la misma transaccion (demo 3)"
info "2c. Descarte silencioso -> DLQ; y el evento invalido ya no nace (extra)"
info "3.  Sin agregados     -> Account valida invariantes antes del append (demo 1)"
info "4.  Una proyeccion    -> fan-out con statement reconstruible (demo 4)"
info "5.  Kafka como store  -> EventStoreDB es el system of record; Kafka distribuye"
info "Detalle: .docs/limitaciones_v1.md · Diseno V2: .docs/propuesta_v2.md · Guia V2: .docs/docs_v2.md"
