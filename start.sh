#!/usr/bin/env bash
set -e

echo "$GSPREAD_SERVICE_ACCOUNT_JSON" > service_account.json

python -m flask --app app run --host 0.0.0.0 --port 8080
