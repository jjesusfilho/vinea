"""
Script de teste para FabricJobClient

Dispara e acompanha a execução de um notebook do Fabric via Job Scheduler
API. Requer as variáveis de ambiente FABRIC_TENANT_ID, FABRIC_CLIENT_ID,
FABRIC_CLIENT_SECRET (service principal com papel suficiente no workspace)
e FABRIC_WORKSPACE_ID, FABRIC_NOTEBOOK_ID (workspace e notebook a testar).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

from vinea import FabricJobClient, FabricJobError

load_dotenv()


def main():
    print("=" * 70)
    print("TESTE DE EXECUÇÃO REMOTA DE NOTEBOOK (FABRIC JOB SCHEDULER API)")
    print("=" * 70)

    tenant_id = os.getenv("FABRIC_TENANT_ID")
    client_id = os.getenv("FABRIC_CLIENT_ID")
    client_secret = os.getenv("FABRIC_CLIENT_SECRET")
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    notebook_id = os.getenv("FABRIC_NOTEBOOK_ID")

    faltando = [
        nome
        for nome, valor in [
            ("FABRIC_TENANT_ID", tenant_id),
            ("FABRIC_CLIENT_ID", client_id),
            ("FABRIC_CLIENT_SECRET", client_secret),
            ("FABRIC_WORKSPACE_ID", workspace_id),
            ("FABRIC_NOTEBOOK_ID", notebook_id),
        ]
        if not valor
    ]
    if faltando:
        print(f"\nERRO: configure no .env: {', '.join(faltando)}")
        return

    client = FabricJobClient(tenant_id, client_id, client_secret)

    print(f"\nWorkspace: {workspace_id}")
    print(f"Notebook: {notebook_id}")
    print("\nDisparando execução...")

    try:
        job_id = client.disparar_notebook(workspace_id, notebook_id)
        print(f"Job disparado: {job_id}")

        print("Aguardando conclusão (consulta a cada 10s)...")
        resultado = client.aguardar_job(workspace_id, notebook_id, job_id, intervalo_segundos=10)

        print("\n" + "-" * 70)
        print(f"Status final: {resultado.get('status')}")
        print(f"Início: {resultado.get('startTimeUtc')}")
        print(f"Fim: {resultado.get('endTimeUtc')}")
        if resultado.get("failureReason"):
            print(f"Motivo da falha: {resultado.get('failureReason')}")

        print("\n" + "=" * 70)
        print("TESTE CONCLUÍDO COM SUCESSO!" if resultado.get("status") == "Completed" else "TESTE CONCLUÍDO COM FALHA")
        print("=" * 70)

    except FabricJobError as e:
        print(f"\nERRO ao executar o notebook: {e}")
    except TimeoutError as e:
        print(f"\nERRO: tempo esgotado aguardando o job: {e}")


if __name__ == "__main__":
    main()
