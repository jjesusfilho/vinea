# Instruções para Claude Code

## Git: commits e push

Ao concluir cada tarefa lógica (uma alteração coesa e testada), fazer commit e push antes de seguir para a próxima tarefa. Não acumular mudanças não relacionadas em um único commit.

Seguir a convenção: `Tipo: descrição`, com o tipo em inglês e inicial maiúscula, seguido de descrição em português iniciando com verbo no presente (terceira pessoa).

Tipos:

- `Feat`: nova funcionalidade
- `Fix`: correção de bug
- `Docs`: documentação (README, CLAUDE.md etc.)
- `Chore`: manutenção, limpeza, configuração
- `Refactor`: refatoração sem mudança de comportamento
- `Test`: testes

## Release: gerar o wheel automaticamente

O workflow [.github/workflows/release.yml](.github/workflows/release.yml) builda o pacote com `uv build` e publica wheel + sdist como assets de uma release no GitHub sempre que uma tag `v*.*.*` é enviada. Ninguém cria essa tag manualmente: após um commit `Feat` ou `Fix` (e só esses dois tipos) ser enviado com sucesso, faça isso automaticamente, sem perguntar:

1. Incremente a versão em `pyproject.toml` (`version = "x.y.z"`): `Fix` incrementa o patch (x.y.z → x.y.z+1); `Feat` incrementa o minor e zera o patch (x.y.z → x.(y+1).0). Incrementos de major só acontecem se o usuário pedir explicitamente.
2. Faça commit dessa alteração isolada com o tipo `Chore` (ex.: `Chore: eleva versão para 0.2.0`).
3. Crie a tag correspondente (`git tag v0.2.0`) e envie tanto o commit quanto a tag (`git push && git push origin v0.2.0`).

Commits `Docs`, `Chore`, `Refactor` e `Test` não disparam bump de versão nem tag.