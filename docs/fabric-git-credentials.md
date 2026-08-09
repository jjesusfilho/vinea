# Sincronizar workspace do Fabric a partir do git via service principal

Anotação para implementação futura no `FabricJobClient` (ou classe nova):
permitir que o service principal dispare "Update from Git" no workspace,
sem depender de alguém clicar em Sync na UI do Fabric.

## Por que não funciona hoje

`POST /v1/workspaces/{workspaceId}/git/updateFromGit` (e `GET .../git/status`)
falham para o service principal com `GitCredentialsNotConfigured`. A
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

## Passos para configurar (quando for implementar)

1. Gerar um PAT do Azure DevOps com acesso ao repositório do projeto (escopo Code: Read & Write).
2. Criar uma Connection no Fabric apontando para o Azure DevOps, usando esse PAT como credencial (via UI: Configurações > Manage connections and gateways > New connection; ou via API `POST /v1/connections`).
3. Dar ao service principal permissão de uso nessa Connection.
4. Chamar `PATCH /v1/workspaces/{workspaceId}/git/myGitCredentials` com `{"source": "ConfiguredConnection", "connectionId": "<id da connection>"}`, autenticado como o service principal.
5. A partir daí, `GET .../git/status` e `POST .../git/updateFromGit` devem funcionar para esse SP.

## Trade-off (por que ainda não implementamos)

Isso introduz mais um segredo de longa duração (o PAT, com escrita no
repositório) só para automatizar uma ação que hoje é pouco frequente (clicar
Sync quando um notebook/arquivo novo é adicionado via git). Vale a pena
implementar quando esse clique manual virar fricção real em algum projeto —
por exemplo, se o fluxo de trabalho passar a exigir sincronizações
frequentes e não supervisionadas.

## Onde isso entraria no vinea

Provavelmente métodos novos em `FabricJobClient` (`src/vinea/fabric.py`):

- `configurar_git_credentials(workspace_id, connection_id)`
- `status_git(workspace_id)`
- `sincronizar_do_git(workspace_id)` (chama `updateFromGit` e espera concluir, análogo a `executar_notebook`)

Referências:

- https://learn.microsoft.com/en-us/rest/api/fabric/core/git/update-my-git-credentials
- https://learn.microsoft.com/en-us/fabric/cicd/git-integration/git-automation
