import time
from typing import Any, Optional

import requests
from azure.identity import ClientSecretCredential

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"

STATUS_FINAIS = ("Completed", "Failed", "Cancelled", "Deduped")


def _tipo_parametro(valor: Any) -> str:
    """Infere o ItemJobParameterType da API a partir do tipo Python do valor."""
    if isinstance(valor, bool):
        return "Boolean"
    if isinstance(valor, (int, float)):
        return "Number"
    return "Text"


class FabricJobError(Exception):
    """Erro ao disparar ou executar um job de item do Fabric."""


class FabricJobClient:
    """
    Cliente para disparar e acompanhar execuções sob demanda de itens do
    Microsoft Fabric (ex.: notebooks) via Job Scheduler API.

    Referência: https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api

    Requer um service principal com papel suficiente (Contributor ou
    superior) no workspace do item a ser executado.
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """
        Inicializa o cliente com um service principal do Azure AD.

        Args:
            tenant_id: ID do tenant do Azure AD
            client_id: ID do aplicativo (service principal)
            client_secret: Segredo do aplicativo
        """
        self._credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    def _headers(self) -> dict:
        token = self._credential.get_token(FABRIC_API_SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def disparar_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        parameters: Optional[dict[str, Any]] = None,
        configuration: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Dispara a execução sob demanda de um notebook do Fabric.

        Args:
            workspace_id: ID do workspace onde está o notebook
            notebook_id: ID do item Notebook
            parameters: Valores para a célula "parameters" do notebook, no
                formato {"nome": valor} — o tipo (Text/Number/Boolean) é
                inferido automaticamente do tipo Python de cada valor.
            configuration: Configuração de execução (ex.: {"defaultLakehouse": {...}}),
                enviada em executionData

        Returns:
            job_id da instância de execução disparada

        Raises:
            FabricJobError: se a API não aceitar o disparo (status != 202)
                ou não retornar o header Location para acompanhamento
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

        body: dict[str, Any] = {}
        if configuration:
            body["executionData"] = configuration
        if parameters:
            body["parameters"] = [
                {"name": nome, "type": _tipo_parametro(valor), "value": valor}
                for nome, valor in parameters.items()
            ]

        resposta = requests.post(url, headers=self._headers(), json=body, timeout=30)
        if resposta.status_code != 202:
            raise FabricJobError(
                f"Falha ao disparar o notebook {notebook_id}: {resposta.status_code} {resposta.text}"
            )

        location = resposta.headers.get("Location")
        if not location:
            raise FabricJobError("Resposta sem header Location - não é possível acompanhar o job")

        return location.rstrip("/").rsplit("/", 1)[-1]

    def status_job(self, workspace_id: str, notebook_id: str, job_id: str) -> dict:
        """
        Consulta o status atual de uma instância de job.

        Usa `?beta=true` para incluir o campo `exitValue` (definido via
        `mssparkutils.notebook.exit(...)` dentro do notebook), hoje em beta
        na API.
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_id}?beta=true"
        resposta = requests.get(url, headers=self._headers(), timeout=30)
        resposta.raise_for_status()
        return resposta.json()

    def aguardar_job(
        self,
        workspace_id: str,
        notebook_id: str,
        job_id: str,
        intervalo_segundos: int = 10,
        timeout_segundos: int = 1800,
    ) -> dict:
        """
        Aguarda a conclusão de uma instância de job, consultando o status
        periodicamente até chegar num status final.

        Args:
            workspace_id: ID do workspace
            notebook_id: ID do item Notebook
            job_id: ID da instância de execução (retornado por disparar_notebook)
            intervalo_segundos: Intervalo entre consultas de status
            timeout_segundos: Tempo máximo de espera antes de desistir

        Returns:
            Corpo da resposta final da API (com "status", "failureReason" etc.)

        Raises:
            TimeoutError: se o job não chegar a um status final dentro de timeout_segundos
        """
        decorrido = 0
        while True:
            dados = self.status_job(workspace_id, notebook_id, job_id)
            if dados.get("status") in STATUS_FINAIS:
                return dados

            if decorrido >= timeout_segundos:
                raise TimeoutError(
                    f"Job {job_id} do notebook {notebook_id} não concluiu em {timeout_segundos}s "
                    f"(último status: {dados.get('status')})"
                )

            time.sleep(intervalo_segundos)
            decorrido += intervalo_segundos

    def executar_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        parameters: Optional[dict[str, Any]] = None,
        configuration: Optional[dict[str, Any]] = None,
        intervalo_segundos: int = 10,
        timeout_segundos: int = 1800,
    ) -> dict:
        """
        Dispara um notebook e aguarda a conclusão, numa única chamada.

        Returns:
            Corpo da resposta final da API

        Raises:
            FabricJobError: se o job terminar com status diferente de "Completed"
        """
        job_id = self.disparar_notebook(workspace_id, notebook_id, parameters, configuration)
        resultado = self.aguardar_job(
            workspace_id, notebook_id, job_id, intervalo_segundos, timeout_segundos
        )
        if resultado.get("status") != "Completed":
            raise FabricJobError(
                f"Notebook {notebook_id} terminou com status {resultado.get('status')}: "
                f"{resultado.get('failureReason')}"
            )
        return resultado
