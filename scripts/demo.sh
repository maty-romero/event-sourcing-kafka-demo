#!/usr/bin/env bash
set -euo pipefail

API="${API:-http://localhost:5087}"
ACCOUNT="${ACCOUNT:-123}"

post() {
  local path="$1" body="$2"
  echo "→ POST $path  $body"
  curl -fsS -X POST "$API$path" -H 'Content-Type: application/json' -d "$body"
  echo
}

get_balance() {
  curl -fsS "$API/accounts/$ACCOUNT/balance" | jq -r .balance
}

poll_balance() {
  local expected="$1" got="" i
  for i in $(seq 1 20); do
    got=$(get_balance 2>/dev/null || echo "")
    if [[ "$got" == "$expected" ]]; then
      echo "  balance = $got ✓ (intento $i)"
      return 0
    fi
    sleep 0.5
  done
  echo "  ✗ timeout: balance=$got, esperaba $expected"
  return 1
}

echo "=== 0) reiniciar transactions-api (limpia lista en memoria) ==="
docker compose restart transactions-api
# esperar a que la API esté lista (el restart pierde ~1-2s)
for i in $(seq 1 20); do
  curl -fsS "$API/accounts/$ACCOUNT/balance" >/dev/null 2>&1 && break
  sleep 0.5
done

echo "=== 1) crear cuenta $ACCOUNT ==="
post /accounts "{\"accountId\":\"$ACCOUNT\"}"

echo "=== 2) depositar 1000 ==="
post "/accounts/$ACCOUNT/deposit" '{"amount":1000}'
poll_balance 1000

echo "=== 3) depositar 500 ==="
post "/accounts/$ACCOUNT/deposit" '{"amount":500}'
poll_balance 1500

echo "=== 4) retirar 200 ==="
post "/accounts/$ACCOUNT/withdraw" '{"amount":200}'
poll_balance 1300

echo
echo "=== 5) logs del consumer (últimos 20) ==="
docker compose logs --tail=20 projection

echo
echo "✓ demo OK: balance final = 1300"
