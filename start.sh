#!/usr/bin/env bash
set -e

python - <<'PY'
import os, json

val = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON", "")
if not val.strip():
    raise SystemExit("Missing env: GSPREAD_SERVICE_ACCOUNT_JSON")

obj = json.loads(val)  # validate JSON
with open("service_account.json", "w", encoding="utf-8") as f:
    json.dump(obj, f)

print("service_account.json written OK")
PY

python -m flask --app app run --host 0.0.0.0 --port 8080
