#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# show_limitations.sh — demuestra las limitaciones de la V1
# (documentadas en .docs/limitaciones_v1.md)
#
# Independiente de demo.sh y replay.sh: puede correrse en cualquier orden.
# Es destructivo: reinicia contenedores, re-consume el topic y borra el
# read model. Al final deja el sistema en estado LIMPIO (como instalacion
# fresca): borra topic y read model, y reinicia la API.
#
# Las lecturas de balance van siempre al ProjectionService (:8090), que es el
# dueno del read model; la API (:5087) solo recibe comandos.
#
# Cada demo clasifica su limitacion: la 1 es de ARQUITECTURA (irremediable con
# Kafka); las 2 y 3 son implementables con Kafka pero pagando el costo de
# reconstruir a mano lo que un event store da nativo.
#
# Uso: ./scripts/show_limitations.sh
# ============================================================================

API="${API:-http://localhost:5087}"
PROJ="${PROJ:-http://localhost:8090}"

ACC_OCC="901"      # demo 1: concurrencia / doble gasto
ACC_AMNESIA="902"  # demo 2: estado perdido tras restart
ACC_IDEMP="900"    # demo 3: proyeccion no idempotente

say()  { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓ %s\033[0m\n' "$*"; }
bad()  { printf '  \033[31m✗ %s\033[0m\n' "$*"; }

command -v jq >/dev/null || { bad "se necesita jq instalado"; exit 1; }

post_status() { # path body -> HTTP code
  curl -s -o /dev/null -w '%{http_code}' -X POST "$API$1" \
    -H 'Content-Type: application/json' -d "$2"
}

get_balance() { # account -> balance | "" (leido al ProjectionService, dueno del read model)
  curl -fsS "$PROJ/accounts/$1/balance" 2>/dev/null | jq -r .balance 2>/dev/null || echo ""
}

poll_balance() { # account expected
  local got="" i
  for i in $(seq 1 30); do
    got=$(get_balance "$1")
    [[ "$got" == "$2" ]] && { ok "balance cuenta $1 = $got"; return 0; }
    sleep 0.5
  done
  bad "timeout: balance cuenta $1 = '$got', esperaba $2"
  return 1
}

wait_api() {
  local i code
  for i in $(seq 1 40); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$API/accounts" || true)
    [[ -n "$code" && "$code" != "000" ]] && return 0
    sleep 0.5
  done
  bad "API no esta lista en $API"
  exit 1
}

count_events() { # account eventType -> cantidad en el log de Kafka
  local out
  out=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic account-events \
      --from-beginning --timeout-ms 4000 2>/dev/null || true)
  printf '%s\n' "$out" | grep "\"AccountId\": *\"$1\"" | grep -c "$2" || true
}

say "0) Setup: stack arriba + API con lista en memoria limpia"
docker compose up -d >/dev/null
docker compose restart transactions-api >/dev/null
wait_api
ok "API lista en $API"

# ----------------------------------------------------------------------------
say "Demo 1 — Sin concurrencia optimista: doble gasto"
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
info "respuestas HTTP de los 5 retiros: $(tr '\n' ' ' < "$tmp")(todos 200 = todos aceptados)"
rm -f "$tmp"

poll_balance "$ACC_OCC" 0
n=$(count_events "$ACC_OCC" MoneyWithdrawn)
info "eventos MoneyWithdrawn en el log: $n | aplicados en el read model: 1"
info "la proyeccion descarta en silencio ('balance insuficiente' en su log):"
docker compose logs projection 2>&1 | grep "balance insuficiente" | tail -4 | sed 's/^/    /' || true
bad "LIMITACIÓN (nivel 1, arquitectura): sin append condicional ni expected version"
bad "nadie puede rechazar el segundo append, así que 'preguntar antes' no cierra la carrera."

# ----------------------------------------------------------------------------
say "Demo 2 — El estado del lado de comandos se pierde al reiniciar"
# ----------------------------------------------------------------------------
info "cuenta $ACC_AMNESIA: se crea y se depositan 250"
[[ "$(post_status /accounts "{\"accountId\":\"$ACC_AMNESIA\"}")" == "201" ]] || bad "no se pudo crear la cuenta"
[[ "$(post_status "/accounts/$ACC_AMNESIA/deposit" '{"amount":250}')" == "200" ]] || bad "no se pudo depositar"
poll_balance "$ACC_AMNESIA" 250

info "se reinicia transactions-api"
docker compose restart transactions-api >/dev/null
wait_api

code=$(post_status "/accounts/$ACC_AMNESIA/deposit" '{"amount":10}')
info "POST deposit a la cuenta $ACC_AMNESIA tras el restart → HTTP $code"
bal=$(get_balance "$ACC_AMNESIA")
n=$(count_events "$ACC_AMNESIA" AccountCreated)
info "pero el read model (ProjectionService) sigue devolviendo $bal y hay $n evento(s) AccountCreated en Kafka"
bad "LIMITACIÓN (nivel 2, implementable con Kafka pero con costo): la API guarda las"
bad "cuentas en memoria y 'olvida' cuentas cuyos eventos siguen en el log. El arreglo"
bad "(que la API pliegue el log al arrancar) exige reconstruir el store a mano."

# ----------------------------------------------------------------------------
say "Demo 3 — Proyeccion no idempotente (redelivery)"
# ----------------------------------------------------------------------------
info "cuenta $ACC_IDEMP: se crea y se depositan 500"
[[ "$(post_status /accounts "{\"accountId\":\"$ACC_IDEMP\"}")" == "201" ]] || bad "no se pudo crear la cuenta"
[[ "$(post_status "/accounts/$ACC_IDEMP/deposit" '{"amount":500}')" == "200" ]] || bad "no se pudo depositar"
poll_balance "$ACC_IDEMP" 500

info "se re-inyecta en Kafka el MISMO evento MoneyDeposited ya aplicado (simula un redelivery de at-least-once)"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic account-events \
  --property parse.key=true --property key.separator='|' >/dev/null <<EOF
$ACC_IDEMP|{"EventType":"MoneyDeposited","AccountId":"$ACC_IDEMP","Timestamp":"2026-08-23T00:00:00Z","Amount":500}
EOF
poll_balance "$ACC_IDEMP" 1000
info "el deposito quedo aplicado DOS veces: balance 500 → 1000"
bad "LIMITACIÓN (nivel 2): sin eventId ni checkpoint, cualquier redelivery duplica el"
bad "estado. El arreglo (dedupe + checkpoint en la misma transaccion) lo pagas vos con"
bad "Kafka; con un event store viene dado por la posicion del evento."

# ----------------------------------------------------------------------------
say "Restaurar estado inicial (como instalacion limpia)"
# ----------------------------------------------------------------------------
info "se borran read model y topic, y se reinicia la API"
docker compose stop projection >/dev/null
docker compose run --rm --no-deps -T --entrypoint sh projection \
  -c "rm -f /data/data.db /data/data.db-wal /data/data.db-shm"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic account-events >/dev/null
for i in $(seq 1 20); do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q account-events || break
  sleep 1
done
docker compose up -d projection >/dev/null
docker compose restart transactions-api >/dev/null
wait_api
ok "sistema en estado limpio (topic recreado vacio, read model vacio, API reiniciada)"

say "Resumen"
info "1. Doble gasto (nivel 1, arquitectura): 5 retiros aceptados, 1 aplicado"
info "2. Amnesia (nivel 2): tras reiniciar, la API desconoce cuentas que estan en el log"
info "3. No idempotencia (nivel 2): un evento re-entregado se aplica dos veces"
info "Detalle: .docs/limitaciones_v1.md · Propuesta V2: .docs/propuesta_v2.md"
