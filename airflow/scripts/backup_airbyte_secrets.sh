#!/bin/bash
# Скрипта за backup на Airbyte Kubernetes secrets
# Зачувува ги credentials во airbyte_secrets.env (gitignored)
# Покренување: bash /mnt/d/IWConnect/smart-city-iw/airflow/scripts/backup_airbyte_secrets.sh

set -e

SCRIPT_DIR="/mnt/d/IWConnect/smart-city-iw/airflow/scripts"
OUTPUT_FILE="$SCRIPT_DIR/airbyte_secrets.env"
NAMESPACE="airbyte-abctl"
SECRET_NAME="airbyte-abctl-airbyte-secrets"

export KUBECONFIG=/home/irina/.airbyte/abctl/abctl.kubeconfig

echo "=== Airbyte Secrets Backup ==="
echo "Зачувувам во: $OUTPUT_FILE"
echo ""

# Читај ги secrets и зачувај во .env фајл
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o json | python3 -c "
import sys, json, base64

d = json.load(sys.stdin)
lines = []
for k, v in sorted(d['data'].items()):
    decoded = base64.b64decode(v).decode()
    lines.append(f'{k}={decoded}')

with open('$OUTPUT_FILE', 'w') as f:
    f.write('# Airbyte Kubernetes Secrets Backup\n')
    f.write('# Kreiran: $(date)\n')
    f.write('# NE go commituvaj ovoj fajl - e vo .gitignore\n\n')
    for line in lines:
        f.write(line + '\n')

print(f'Zacuvani {len(lines)} secrets.')
"

# Додај и workspace info од базата
echo "" >> "$OUTPUT_FILE"
echo "# Workspace Info" >> "$OUTPUT_FILE"
kubectl exec -it airbyte-db-0 -n "$NAMESPACE" -- psql -U airbyte -d db-airbyte -tAc \
    "SELECT '# WORKSPACE_ID=' || id || ' (' || name || ')' FROM workspace;" >> "$OUTPUT_FILE" 2>/dev/null || true

echo ""
echo "Backup zavrsен: $OUTPUT_FILE"
echo "VAZNO: Ovoj fajl sodrzi credentials - ne go spodeluvaj!"
