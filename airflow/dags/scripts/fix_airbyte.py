# Library for running terminal commands from Python
import subprocess
# Library for time / sleep
import time
# Library for exiting with error (sys.exit)
import sys


# Function that runs the chmod commands for Airbyte after restart
def fix_airbyte_permissions():
    print("Fixing Airbyte permissions...")

    # List of two commands that grant permissions on Airbyte volumes
    commands = [
        "docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-volume-db",
        "docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-local-pv",
    ]

    # Loop — each command from the list is executed in the terminal
    for cmd in commands:
        print(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True)
        # If the command failed, show error and stop
        if result.returncode != 0:
            print(f"ERROR: command failed (exit code {result.returncode})")
            print("Check if Docker is running and if the 'airbyte-abctl-control-plane' container is up.")
            sys.exit(1)
        print("  OK")

    # Wait 3 minutes — Airbyte should start up during this time
    print("\nWaiting 3 minutes for Airbyte to start...")
    for i in range(3, 0, -1):
        print(f"  {i} min remaining...")
        time.sleep(60)

    print("\nDone! Open http://localhost:8000")


# Call the function — only runs when the script is executed directly
if __name__ == "__main__":
    fix_airbyte_permissions()
