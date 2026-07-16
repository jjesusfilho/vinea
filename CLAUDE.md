# Vinea — instruções de commit e push

## Commits automáticos por tarefa lógica

Ao concluir cada tarefa lógica (uma mudança coesa e funcional, ex.: corrigir um bug, adicionar uma função, ajustar uma dependência), faça commit e push automaticamente, sem pedir confirmação antes.

Não agrupe mudanças não relacionadas no mesmo commit. Se uma solicitação do usuário envolver várias tarefas independentes, separe em múltiplos commits.

## Formato das mensagens (Conventional Commits)

Use o padrão de commits convencionais:

```
tipo: descrição curta no imperativo
```

Tipos:

- feat: nova funcionalidade
- fix: correção de bug
- docs: mudanças em documentação
- refactor: mudança de código sem alterar comportamento
- test: adição ou ajuste de testes
- chore: tarefas de manutenção (dependências, configuração, build)
- style: formatação, sem mudança de lógica
- perf: melhoria de performance
- ci: mudanças em integração contínua

Exemplos:

- `fix: corrigir parsing de datas em distributed.py`
- `feat: adicionar suporte a consulta por CNPJ`
- `chore: remover dependência xmlsec do zeep`

Use o corpo do commit apenas quando o porquê da mudança não for óbvio a partir do diff.

## Push

Após cada commit, faça push para a branch atual do remoto (`origin`). Se a branch ainda não tiver upstream configurado, defina-o no push (`git push -u origin <branch>`).

Não force push, não use `--no-verify` e não faça squash ou amend de commits já enviados ao remoto, a menos que explicitamente solicitado.
