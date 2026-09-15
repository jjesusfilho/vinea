import time
from typing import Any, Optional

import requests
from azure.identity import ClientSecretCredential

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_API_SCOPE = "https://api.fabric.microsoft.com/.default"
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
POWERBI_API_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

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
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    def _headers(self) -> dict:
        token = self._credential.get_token(FABRIC_API_SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _headers_powerbi(self) -> dict:
        token = self._credential.get_token(POWERBI_API_SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def atualizar_modelo_semantico(
        self, workspace_id: str, dataset_id: str, aguardar: bool = True, timeout_segundos: int = 600
    ) -> Optional[dict]:
        """
        Dispara um refresh do modelo semântico (API clássica do Power BI, não
        a do Fabric) — para modelos Direct Lake, isso é o "reenquadramento"
        (framing) que faz o motor reconhecer tabelas/dados novos direto no
        Delta Lake. Sem isso, uma tabela recém-adicionada ao modelo (via TOM,
        ex. `ntb_infancia_cria_modelo_semantico`) pode falhar em visuais com
        um erro genérico (`QueryUserError` / "capacity or license issue" na
        UI) até o primeiro refresh automático acontecer sozinho.

        Args:
            workspace_id: ID do workspace onde está o modelo semântico
            dataset_id: ID do item SemanticModel (não confundir com o
                notebookId/logicalId — pegue via GET .../items filtrando
                type == "SemanticModel")
            aguardar: se True, espera o refresh terminar antes de retornar
            timeout_segundos: tempo máximo de espera se aguardar=True

        Returns:
            O último status consultado (dict), ou None se aguardar=False
            (nesse caso já retorna depois do disparo, sem esperar)

        Raises:
            FabricJobError: se o refresh terminar com status diferente de
                "Completed" (só quando aguardar=True)
            TimeoutError: se não terminar dentro de timeout_segundos
        """
        url = f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        resposta = requests.post(url, headers=self._headers_powerbi(), json={"type": "full"}, timeout=30)
        if resposta.status_code != 202:
            raise FabricJobError(f"Falha ao disparar refresh do modelo {dataset_id}: {resposta.status_code} {resposta.text}")

        if not aguardar:
            return None

        url_status = f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
        decorrido = 0
        intervalo_segundos = 5
        while True:
            estado = requests.get(url_status, headers=self._headers_powerbi(), timeout=30).json()["value"][0]
            status = estado.get("status")
            if status == "Completed":
                return estado
            if status == "Failed":
                raise FabricJobError(f"Refresh do modelo {dataset_id} falhou: {estado}")

            if decorrido >= timeout_segundos:
                raise TimeoutError(f"Refresh do modelo {dataset_id} não terminou em {timeout_segundos}s (último status: {status})")

            time.sleep(intervalo_segundos)
            decorrido += intervalo_segundos

    def disparar_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        parameters: Optional[dict[str, Any]] = None,
        configuration: Optional[dict[str, Any]] = None,
        habilitar_pip_install: bool = False,
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
            habilitar_pip_install: `%pip install` vem desabilitado por padrão em
                execuções não-interativas (via esta API ou via pipeline) — só
                funciona rodando manual na UI do Fabric. Se o notebook tiver uma
                célula `%pip install`, passe True aqui para evitar
                `MagicUsageError`. Ver
                https://learn.microsoft.com/en-us/fabric/data-engineering/library-management#inline-installation

        Returns:
            job_id da instância de execução disparada

        Raises:
            FabricJobError: se a API não aceitar o disparo (status != 202)
                ou não retornar o header Location para acompanhamento
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

        todos_parametros = dict(parameters) if parameters else {}
        if habilitar_pip_install:
            todos_parametros["_inlineInstallationEnabled"] = True

        body: dict[str, Any] = {}
        if configuration:
            body["executionData"] = configuration
        if todos_parametros:
            body["parameters"] = [
                {"name": nome, "type": _tipo_parametro(valor), "value": valor}
                for nome, valor in todos_parametros.items()
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
        habilitar_pip_install: bool = False,
        intervalo_segundos: int = 10,
        timeout_segundos: int = 1800,
    ) -> dict:
        """
        Dispara um notebook e aguarda a conclusão, numa única chamada.

        Args:
            habilitar_pip_install: ver `disparar_notebook`.

        Returns:
            Corpo da resposta final da API

        Raises:
            FabricJobError: se o job terminar com status diferente de "Completed"
        """
        job_id = self.disparar_notebook(
            workspace_id, notebook_id, parameters, configuration, habilitar_pip_install
        )
        resultado = self.aguardar_job(
            workspace_id, notebook_id, job_id, intervalo_segundos, timeout_segundos
        )
        if resultado.get("status") != "Completed":
            raise FabricJobError(
                f"Notebook {notebook_id} terminou com status {resultado.get('status')}: "
                f"{resultado.get('failureReason')}"
            )
        return resultado

    def _aguardar_operacao_lro(
        self, operation_id: str, intervalo_segundos: int = 5, timeout_segundos: int = 300
    ) -> dict:
        """Aguarda uma long running operation genérica do Fabric (não a de job de notebook)."""
        url = f"{FABRIC_API_BASE}/operations/{operation_id}"
        decorrido = 0
        while True:
            resposta = requests.get(url, headers=self._headers(), timeout=30)
            resposta.raise_for_status()
            estado = resposta.json()
            if estado.get("status") == "Succeeded":
                return estado
            if estado.get("status") == "Failed":
                raise FabricJobError(f"Operação {operation_id} falhou: {estado.get('error')}")

            if decorrido >= timeout_segundos:
                raise TimeoutError(f"Operação {operation_id} não terminou em {timeout_segundos}s")

            time.sleep(intervalo_segundos)
            decorrido += intervalo_segundos

    def criar_conexao_ado_service_principal(self, organizacao: str, projeto: str, repositorio: str) -> str:
        """
        Cria, no Fabric, uma conexão "Azure DevOps - Source Control" autenticada
        com as próprias credenciais deste service principal (sem PAT). Passo
        único de configuração, não precisa rodar de novo a cada sincronização.

        Args:
            organizacao: nome da organização do Azure DevOps (ex.: "mpsp")
            projeto: nome do projeto no Azure DevOps
            repositorio: nome do repositório git

        Returns:
            connection_id, usado depois em `configurar_credencial_git`
        """
        payload: dict[str, Any] = {
            "displayName": f"ADO SP - {projeto}/{repositorio}",
            "connectivityType": "ShareableCloud",
            "connectionDetails": {
                "creationMethod": "AzureDevOpsSourceControl.Contents",
                "type": "AzureDevOpsSourceControl",
                "parameters": [
                    {
                        "dataType": "Text",
                        "name": "url",
                        "value": f"https://dev.azure.com/{organizacao}/{projeto}/_git/{repositorio}/",
                    }
                ],
            },
            "credentialDetails": {
                "credentials": {
                    "credentialType": "ServicePrincipal",
                    "tenantId": self._tenant_id,
                    "servicePrincipalClientId": self._client_id,
                    "servicePrincipalSecret": self._client_secret,
                }
            },
        }
        resposta = requests.post(f"{FABRIC_API_BASE}/connections", headers=self._headers(), json=payload, timeout=30)
        resposta.raise_for_status()
        return resposta.json()["id"]

    def configurar_credencial_git(self, workspace_id: str, connection_id: str) -> None:
        """
        Aponta a credencial git deste service principal, para o workspace
        informado, para a conexão criada por `criar_conexao_ado_service_principal`.
        Passo único de configuração.
        """
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/git/myGitCredentials"
        payload = {"source": "ConfiguredConnection", "connectionId": connection_id}
        resposta = requests.patch(url, headers=self._headers(), json=payload, timeout=30)
        resposta.raise_for_status()

    def status_git(self, workspace_id: str) -> dict:
        """Consulta o status git do workspace (workspaceHead, remoteCommitHash, changes)."""
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/git/status"
        resposta = requests.get(url, headers=self._headers(), timeout=60)
        resposta.raise_for_status()
        return resposta.json()

    def sincronizar_do_git(self, workspace_id: str, allow_override_items: bool = True) -> dict:
        """
        Aplica ao workspace os commits mais recentes do branch git conectado
        (equivalente ao botão "Update all" do portal). Prefere sempre o
        conteúdo do git em caso de conflito.

        Requer `configurar_credencial_git` já ter sido chamado (uma vez) para
        este workspace com este service principal.

        Args:
            allow_override_items: a API do Fabric exige esse consentimento
                sempre que há qualquer item modificado vindo do git, não só
                em conflito de verdade — sem ele, a chamada falha com
                `OverrideItemsNotAllowed` mesmo sem conflito real.

        Returns:
            O status git consultado antes de disparar a atualização.
        """
        status = self.status_git(workspace_id)
        if status["workspaceHead"] == status["remoteCommitHash"] and not status.get("changes"):
            return status

        payload = {
            "workspaceHead": status["workspaceHead"],
            "remoteCommitHash": status["remoteCommitHash"],
            "conflictResolution": {
                "conflictResolutionType": "Workspace",
                "conflictResolutionPolicy": "PreferRemote",
            },
            "options": {"allowOverrideItems": allow_override_items},
        }
        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/git/updateFromGit"
        resposta = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        resposta.raise_for_status()
        if resposta.status_code == 202:
            self._aguardar_operacao_lro(resposta.headers["x-ms-operation-id"])
        return status

    def commitar_para_git(self, workspace_id: str, comentario: Optional[str] = None) -> dict:
        """
        Commita as mudanças do workspace para o repositório git conectado
        (equivalente ao botão "Commit" do portal) — direção oposta de
        `sincronizar_do_git`. Ainda não confirmado em produção (só a direção
        git->Fabric foi validada em outro projeto); se falhar, o fallback é
        clicar "Commit" manualmente na UI do Fabric.

        Args:
            comentario: mensagem do commit (máx. 300 caracteres); se omitido,
                usa o comentário padrão do provedor git.

        Returns:
            O status git consultado antes de disparar o commit.
        """
        status = self.status_git(workspace_id)
        payload: dict[str, Any] = {"mode": "All", "workspaceHead": status["workspaceHead"]}
        if comentario:
            payload["comment"] = comentario[:300]

        url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/git/commitToGit"
        resposta = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        resposta.raise_for_status()
        if resposta.status_code == 202:
            self._aguardar_operacao_lro(resposta.headers["x-ms-operation-id"])
        return status
