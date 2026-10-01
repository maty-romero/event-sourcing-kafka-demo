#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# demo.sh (V2) — happy path sobre la nueva arquitectura:
# comandos -> EventStoreDB (agregado + OCC) -> puente ESDB->Kafka ->
# proyecciones idempotentes (balances :8090, extracto :8091).
#
# A diferencia de la V1 no necesita "reiniciar la API para limpiar memoria":
# el estado vive en el stream. Si la cuenta ya existe (409), sigue sumando
# sobre el balance actual (lecturas por delta).
#
# Al final muestra el stream de la cuenta tal como quedo en EventStoreDB,
# usando su API HTTP (la misma que usa la app; sin gRPC).
# ============================================================================

API="${API:-http://localhost:5087}"
PROJ="${PROJ:-http://localhost:8090}"
STMT="${STMT:-http://localhost:8091}"
ESDB="${ESDB:-http://localhost:2113}"
ACCOUNT="${ACCOUNT:-123}"

post() {
  local path="$1" body="$2"
  echo "→ POST $path  $body"
  curl -fsS -X POST "$API$path" -H 'Content-Type: application/json' -d "$body"
  echo
}

get_balance() {
  curl -fsS "$PROJ/accounts/$ACCOUNT/balance" 2>/dev/null | jq -r .balance 2>/dev/null || echo ""
}

get_movements() {
  curl -fsS "$STMT/accounts/$ACCOUNT/movements" 2>/dev/null | jq -r '.movements | length' 2>/dev/null || echo ""
}

poll_balance() {
  local expected="$1" got="" i
  for i in $(seq 1 40); do
    got=$(get_balance)
    if [[ "$got" == "$expected" ]]; then
      echo "  balance = $got ✓ (intento $i)"
      return 0
    fi
    sleep 0.5
  done
  echo "  ✗ timeout: balance=$got, esperaba $expected"
  return 1
}

poll_movements() {
  local expected="$1" got="" i
  for i in $(seq 1 40); do
    got=$(get_movements)
    if [[ "$got" == "$expected" ]]; then
      echo "  movimientos en el extracto = $got ✓ (intento $i)"
      return 0
    fi
    sleep 0.5
  done
  echo "  ✗ timeout: movimientos=$got, esperaba $expected"
  return 1
}

echo "=== 0) stack arriba ==="
docker compose up -d >/dev/null

echo "=== 1) crear cuenta $ACCOUNT ==="
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/accounts" \
  -H 'Content-Type: application/json' -d "{\"accountId\":\"$ACCOUNT\"}")
if [[ "$code" == "201" ]]; then
  echo "→ POST /accounts  (creada, 201)"
elif [[ "$code" == "409" ]]; then
  echo "→ POST /accounts  (ya existia, 409 — la cuenta vive en su stream, no en memoria)"
else
  echo "  ✗ HTTP $code"; exit 1
fi

base=$(get_balance); base="${base:-0}"
base_movements=$(get_movements); base_movements="${base_movements:-0}"
echo "  balance actual: $base | movimientos: $base_movements"

echo "=== 2) depositar 1000 ==="
post "/accounts/$ACCOUNT/deposit" '{"amount":1000}'
poll_balance $((base + 1000))

echo "=== 3) depositar 500 ==="
post "/accounts/$ACCOUNT/deposit" '{"amount":500}'
poll_balance $((base + 1500))

echo "=== 4) retirar 200 ==="
post "/accounts/$ACCOUNT/withdraw" '{"amount":200}'
poll_balance $((base + 1300))

echo "=== 5) extracto (segunda proyeccion, :8091) ==="
poll_movements $((base_movements + 4))
curl -fsS "$STMT/accounts/$ACCOUNT/movements" | jq -c '.movements[-4:][] | {revision, eventType, amount}' | sed 's/^/    /'

echo
echo "=== 6) el stream account-$ACCOUNT en EventStoreDB (system of record) ==="
curl -fsS -H 'Accept: application/vnd.eventstore.events+json' \
  "$ESDB/streams/account-$ACCOUNT/head/1000?embed=content" \
  | jq -r '.entries | reverse | .[] | .content | "    [\(.eventNumber)] \(.eventType) amount=\(.data.amount)"'

echo
echo "✓ demo OK: balance final = $((base + 1300)) en ambas proyecciones, historia completa en EventStoreDB"
