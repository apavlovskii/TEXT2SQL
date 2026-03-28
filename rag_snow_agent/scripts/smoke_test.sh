#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 1) Basic import checks
uv run python -c "import chromadb; print('chromadb import: OK')"
uv run python -c "import snowflake.connector; print('snowflake connector import: OK')"

# 2) Snowflake connectivity check using credentials JSON
uv run python - <<'PY'
import json
import os
from pathlib import Path
import pandas as pd
import snowflake.connector

candidate_paths = [
    os.environ.get("SNOWFLAKE_CREDENTIALS_JSON"),
    "snowflake_credential.json",
    "snowflake_credentials.json",
]

credential_path = None
for path in candidate_paths:
    if path and Path(path).exists():
        credential_path = path
        break

if credential_path is None:
    raise FileNotFoundError("Snowflake credentials file not found. Tried: SNOWFLAKE_CREDENTIALS_JSON, snowflake_credential.json, snowflake_credentials.json")

snowflake_credential = json.load(open(credential_path))

conn = snowflake.connector.connect(
    **snowflake_credential
)
cursor = conn.cursor()
sql_query = os.environ.get("SNOWFLAKE_SMOKE_SQL", "SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER()")
cursor.execute(sql_query)
results = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(results, columns=columns)

print("Snowflake connectivity: OK")
print(df.head(5).to_string(index=False))

cursor.close()
conn.close()
PY

echo "Smoke test: PASSED"
