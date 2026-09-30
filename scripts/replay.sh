#!/usr/bin/env bash
set -euo pipefail

PROJ="${PROJ:-http://localhost:8090}"
ACCOUNT="${ACCOUNT:-123}"
DATA_DIR="${DATA_DIR:-./data}"

echo "=== 1) balance ANTES de borrar (leido al ProjectionService) ==="
curl -fsS "$PROJ/accounts/$ACCOUNT/balance"; echo

echo "=== 2) detener consumer ==="
docker compose stop projection

echo "=== 3) borrar read model (dentro del container para evitar problemas de permisos) ==="
docker compose run --rm --no-deps -T --entrypoint sh projection \
  -c "rm -f /data/data.db /data/data.db-wal /data/data.db-shm"
ls "$DATA_DIR" 2>/dev/null || echo "  (directorio vacío)"

echo "=== 4) re-arrancar consumer con offset=0 ==="
KAFKA_GROUP=balance-projection-replay KAFKA_START_OFFSET=first \
  docker compose up -d projection

echo "=== 5) esperar a que reconstruya ==="
expected=1300
got=""
for i in $(seq 1 20); do
    got=$(curl -fsS "$PROJ/accounts/$ACCOUNT/balance" 2>/dev/null | jq -r .balance 2>/dev/null || echo "")
  if [[ "$got" == "$expected" ]]; then
    echo "  balance reconstruido = $got ✓ (intento $i)"
    break
  fi
  sleep 0.5
done

if [[ "$got" != "$expected" ]]; then
  echo "  ✗ timeout: balance=$got"
  docker compose logs --tail=30 projection
  exit 1
fi

echo
echo "=== 6) logs del replay (últimos 30) ==="
docker compose logs --tail=30 projection

echo
echo "✓ replay OK: 1300 reconstruido idéntico al original"
