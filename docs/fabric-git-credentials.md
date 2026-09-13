# Sincronizar workspace do Fabric a partir do git via service principal

**Implementado em `FabricJobClient` (v0.9.0)**: `criar_conexao_ado_service_principal`,
`configurar_credencial_git`, `status_git`, `sincronizar_do_git` (git→Fabric,
"Update all") e `commitar_para_git` (Fabric→git, "Commit") — as duas direções
confirmadas funcionando em produção no projeto `infancia` (2026-09-13), sem
PAT do Azure DevOps, usando as credenciais do próprio service principal.
`commitar_para_git` preserva o conteúdo corretamente no ciclo (só reordena
chaves JSON e canonicaliza `notebookId`/`workspaceId` de referências
cross-item pro formato `logicalId` + workspaceId zerado — que o Fabric
aceita de volta normalmente na próxima sincronização, então não precisa
imitar esse formato ao editar manualmente).

O texto abaixo é o registro histórico de por que isso não tinha sido
implementado antes — a solução real (SP direto na credencial da conexão do
Azure DevOps) acabou não precisando do PAT que motivou adiar.

## Por que não funcionava (achado original, antes da solução com SP)

`POST /v1/workspaces/{workspaceId}/git/updateFromGit` (e `GET .../git/status`)
falhavam para o service principal com `GitCredentialsNotConfigured`. A
integração git de um workspace é por identidade chamadora: cada
usuário/principal precisa ter suas próprias credenciais git configuradas
nesse workspace.

## O que a API de credenciais git suporta

`PATCH /v1/workspaces/{workspaceId}/git/myGitCredentials`
([docs](https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-my-git-credentials))
aceita três fontes (`source`):

- `Automatic` — **bloqueado para service principal** (só funciona pra usuário).
- `ConfiguredConnection` — usa uma Fabric Connection já criada, via `connectionId`. **É a única opção viável para service principal.**
- `None` — remove a configuração.

## Como configurar (passo único por workspace)

```python
from vinea import FabricJobClient

client = FabricJobClient(tenant_id, client_id, client_secret)
connection_id = client.criar_conexao_ado_service_principal("org", "projeto", "repositorio")
client.configurar_credencial_git(workspace_id, connection_id)
```

A conexão criada usa `credentialType: "ServicePrincipal"` (as próprias
credenciais do SP, sem PAT) — diferente do que o achado original abaixo
supunha ser necessário.

A partir daí, `client.sincronizar_do_git(workspace_id)` e
`client.commitar_para_git(workspace_id, comentario=...)` funcionam
normalmente, análogos a `executar_notebook` (aguardam a long running
operation do Fabric internamente).

## Achado original (supunha precisar de PAT — não é mais o caminho usado)

1. Gerar um PAT do Azure DevOps com acesso ao repositório do projeto (escopo Code: Read & Write).
2. Criar uma Connection no Fabric apontando para o Azure DevOps, usando esse PAT como credencial.
3. Dar ao service principal permissão de uso nessa Connection.
4. Chamar `PATCH /v1/workspaces/{workspaceId}/git/myGitCredentials` com `{"source": "ConfiguredConnection", "connectionId": "<id da connection>"}`, autenticado como o service principal.

Isso introduziria mais um segredo de longa duração (o PAT, com escrita no
repositório) — motivo original de adiar. A solução implementada evita isso
completamente, reaproveitando o `client_secret` do SP que já existe.

Referências:

- https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-my-git-credentials
- https://learn.microsoft.com/en-us/rest/api/fabric/core/git/commit-to-git
- https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation
