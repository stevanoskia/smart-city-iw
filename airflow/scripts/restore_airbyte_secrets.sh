#!/bin/bash
# Скрипта за restore на Airbyte Kubernetes secrets
# Ги враќа credentials од airbyte_secrets.env по reset/reinstall
# Покренување: bash /mnt/d/IWConnect/smart-city-iw/airflow/scripts/restore_airbyte_secrets.sh

set -e

SCRIPT_DIR="/mnt/d/IWConnect/smart-city-iw/airflow/scripts"
INPUT_FILE="$SCRIPT_DIR/airbyte_secrets.env"
NAMESPACE="airbyte-abctl"
SECRET_NAME="airbyte-abctl-airbyte-secrets"

export KUBECONFIG=/home/irina/.airbyte/abctl/abctl.kubeconfig

echo "=== Airbyte Secrets Restore ==="

# Провери дали backup фајлот постои
if [ ! -f "$INPUT_FILE" ]; then
    echo "GRESHKA: Ne najden backup fajl: $INPUT_FILE"
    echo "Prvo pokreni backup_airbyte_secrets.sh"
    exit 1
fi

echo "Citam od: $INPUT_FILE"
echo ""

# Читај ги вредностите и patch-ај го secret-от
python3 -c "
import subprocess, base64, json

input_file = '$INPUT_FILE'
namespace = '$NAMESPACE'
secret_name = '$SECRET_NAME'

# Читај credentials
secrets = {}
with open(input_file) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            if '=' in line:
                k, v = line.split('=', 1)
                secrets[k.strip()] = v.strip()

if not secrets:
    print('GRESHKA: Nema secrets vo fajlot!')
    exit(1)

# Креирај JSON patch
patches = []
for k, v in secrets.items():
    encoded = base64.b64encode(v.encode()).decode()
    patches.append({'op': 'replace', 'path': f'/data/{k}', 'value': encoded})

patch_json = json.dumps(patches)

# Примени patch
result = subprocess.run(
    ['kubectl', 'patch', 'secret', secret_name, '-n', namespace,
     '--type=json', f'-p={patch_json}'],
    capture_output=True, text=True
)

if result.returncode == 0:
    print(f'Uspesno patchani {len(secrets)} secrets.')
else:
    print(f'GRESHKA: {result.stderr}')
    exit(1)
"

echo ""
echo "Restartuvam workload-launcher..."
kubectl rollout restart deployment airbyte-abctl-workload-launcher -n "$NAMESPACE"

echo ""
echo "Cekam 30 sekundi..."
sleep 30

echo ""
echo "=== Status na podovi ==="
kubectl get pods -n "$NAMESPACE"

echo ""
echo "Restore zavrsен! Proveri http://localhost:8000"
