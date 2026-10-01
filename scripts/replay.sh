#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# replay.sh (V2) — reconstruccion del read model de balances desde el log.
#
# Con checkpoint la reconstruccion es segura: se borra read model + checkpoints
# y se re-consume el topic completo con un grupo nuevo desde el offset 0.
# El balance final debe ser identico al original.
# ============================================================================

PROJ="${PROJ:-http://localhost:8090}"
ACCOUNT="${ACCOUNT:-demo-123}"

echo "=== 1) balance ANTES de borrar (leido a la proyeccion de balances) ==="
expected=$(curl -fsS "$PROJ/accounts/$ACCOUNT/balance" | jq -r .balance)
echo "  balance actual = $expected"
if [[ "$expected" == "null" || -z "$expected" ]]; then
  echo "  ✗ la cuenta $ACCOUNT no existe en el read model; corra antes ./scripts/demo.sh"
  exit 1
fi

echo "=== 2) detener proyeccion de balances ==="
docker compose stop projection-balances

echo "=== 3) borrar read model + checkpoints (dentro del container para evitar problemas de permisos) ==="
docker compose run --rm --no-deps -T --entrypoint sh projection-balances \
  -c "rm -f /data/balances.db /data/balances.db-wal /data/balances.db-shm"

echo "=== 4) re-arrancar con grupo nuevo y offset=0 ==="
KAFKA_GROUP=balance-projection-replay KAFKA_START_OFFSET=first \
  docker compose up -d projection-balances

echo "=== 5) esperar a que reconstruya ==="
got=""
for i in $(seq 1 40); do
  got=$(curl -fsS "$PROJ/accounts/$ACCOUNT/balance" 2>/dev/null | jq -r .balance 2>/dev/null || echo "")
  if [[ "$got" == "$expected" ]]; then
    echo "  balance reconstruido = $got ✓ (intento $i)"
    break
  fi
  sleep 0.5
done

if [[ "$got" != "$expected" ]]; then
  echo "  ✗ timeout: balance=$got, esperaba $expected"
  docker compose logs --tail=30 projection-balances
  exit 1
fi

echo
echo "=== 6) la proyeccion de extracto NO fue tocada (consumidores independientes) ==="
curl -fsS http://localhost:8091/accounts/$ACCOUNT/movements | jq '.movements | length' | sed 's/^/  movimientos en el extracto: /'

echo
echo "✓ replay OK: $expected reconstruido idéntico al original"
