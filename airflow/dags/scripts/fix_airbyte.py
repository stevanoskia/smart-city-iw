# Library za da moze Python da povikuva komandi vo terminal
import subprocess
# Library za vreme / sleep
import time
# Library za izlez so greshka (sys.exit)
import sys


# Funkcija koja gi izvrsuva chmod komandite za Airbyte po restart
def fix_airbyte_permissions():
    print("Fixiram Airbyte permissions...")

    # Lista so dvete komandi koi gi davaat dozvoli na Airbyte volumi
    commands = [
        "docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-volume-db",
        "docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-local-pv",
    ]

    # Loop — za sekoja komanda od listata se izvrsuva vo terminal
    for cmd in commands:
        print(f"Izvrsuvam: {cmd}")
        result = subprocess.run(cmd, shell=True)
        # Ako komandата ne uspea, pokazi greshka i stopi
        if result.returncode != 0:
            print(f"GRESHKA: komandата ne uspea (exit code {result.returncode})")
            print("Provjeri dali Docker raboti i dali containerot 'airbyte-abctl-control-plane' e up.")
            sys.exit(1)
        print("  OK")

    # Se ceka 3 minuti — vo toa vreme Airbyte treba da se startira
    print("\nCekam 3 minuti za Airbyte da se startira...")
    for i in range(3, 0, -1):
        print(f"  {i} min ostanati...")
        time.sleep(60)

    print("\nGotovo! Otvori http://localhost:8000")


# Povikuvanje na funkцijata — se izvrsuva samo koga skriptata se pokrenuva direktno
if __name__ == "__main__":
    fix_airbyte_permissions()
