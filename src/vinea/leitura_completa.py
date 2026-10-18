"""Leitura completa da resposta de consultarProcesso (MNI 2.2.2), e-SAJ e eproc.

Devolve uma tabela por estrutura do XML, com todos os campos que o tribunal envia, sem
interpretação: os valores ficam como texto, e as tabelas filhas apontam para a mãe pela
posição no XML (``parte_seq``, ``mov_seq``...). Quem consome decide tipos e chaves.

Os elementos são lidos pelo nome local, ignorando namespaces (e-SAJ e eproc usam
prefixos diferentes).

Tabelas:
- processo: atributos de dadosBasicos, órgão julgador, valor da causa, magistrado atuante;
- parametros: todos os outroParametro (nome, valor);
- assuntos: todos os assuntos (principal, código nacional, código local e descrição);
- prioridades, vinculos (processoVinculado);
- partes: uma linha por parte, com os atributos da parte e da pessoa;
- representantes: pessoaProcessualRelacionada de cada parte;
- advogados: advogados de cada parte;
- documentos, enderecos, outros_nomes: de partes, representantes e advogados
  (``dono_tipo`` = parte, representante ou advogado; ``dono_seq`` = posição do dono
  dentro da parte; para a própria parte, ``dono_seq`` = 0);
- movimentos e movimentos_complementos.
"""

from typing import Iterable, Optional

import pandas as pd
from lxml import etree

from .consulta import _separar_mtom


def _local(el) -> str:
    return etree.QName(el).localname if isinstance(el.tag, str) else ""


def _filhos(el, nome: str) -> list:
    return [f for f in el if _local(f) == nome]


def _filho(el, nome: str):
    return next((f for f in el if _local(f) == nome), None)


def _texto(el, nome: str) -> Optional[str]:
    f = _filho(el, nome)
    return f.text.strip() if f is not None and f.text and f.text.strip() else None


def _atributos(el, prefixo: str = "") -> dict:
    return {prefixo + _snake(etree.QName(k).localname): v for k, v in el.attrib.items() if etree.QName(k).localname != "nil"}


def _snake(nome: str) -> str:
    saida = []
    for i, c in enumerate(nome):
        if c.isupper() and i and (nome[i - 1].islower() or (i + 1 < len(nome) and nome[i + 1].islower())):
            saida.append("_")
        saida.append(c.lower())
    return "".join(saida)


COLUNAS = {
    "processo": ["numero", "competencia", "classe_processual", "codigo_localidade", "nivel_sigilo", "intervencao_mp",
                 "data_ajuizamento", "tamanho_processo", "orgao_codigo_orgao", "orgao_nome_orgao", "orgao_instancia",
                 "orgao_codigo_municipio_ibge", "valor_causa", "magistrado_atuante"],
    "parametros": ["numero", "seq", "nome", "valor"],
    "assuntos": ["numero", "seq", "principal", "codigo_nacional", "codigo_local", "codigo_pai_nacional", "descricao"],
    "prioridades": ["numero", "seq", "prioridade"],
    "vinculos": ["numero", "seq", "numero_vinculado", "vinculo"],
    "partes": ["numero", "parte_seq", "polo", "assistencia_judiciaria", "intimacao_pendente", "relacionamento_processual",
               "nome", "tipo_pessoa", "sexo", "data_nascimento", "nacionalidade", "cidade_natural", "estado_natural",
               "nome_genitor", "nome_genitora", "numero_documento_principal"],
    "representantes": ["numero", "parte_seq", "dono_seq", "relacionamento_processual", "assistencia_judiciaria",
                       "intimacao_pendente", "nome", "tipo_pessoa", "sexo", "data_nascimento", "nacionalidade",
                       "cidade_natural", "estado_natural", "nome_genitor", "nome_genitora", "numero_documento_principal"],
    "advogados": ["numero", "parte_seq", "dono_seq", "nome", "inscricao", "numero_documento_principal",
                  "tipo_representante", "intimacao"],
    "documentos": ["numero", "parte_seq", "dono_tipo", "dono_seq", "seq", "tipo_documento", "codigo_documento",
                   "emissor_documento"],
    "enderecos": ["numero", "parte_seq", "dono_tipo", "dono_seq", "seq", "cep", "logradouro", "numero_endereco",
                  "complemento", "bairro", "cidade", "estado", "pais"],
    "outros_nomes": ["numero", "parte_seq", "dono_tipo", "dono_seq", "seq", "outro_nome"],
    "movimentos": ["numero", "mov_seq", "identificador_movimento", "data_hora", "nivel_sigilo", "codigo_movimento_local",
                   "codigo_pai_nacional_local", "descricao_movimento_local", "codigo_movimento_local_pai",
                   "codigo_pai_nacional_local_pai", "descricao_movimento_local_pai", "codigo_movimento_nacional"],
    "movimentos_complementos": ["numero", "mov_seq", "seq", "complemento"],
}
CAMPOS_PESSOA = ["nome", "tipo_pessoa", "sexo", "data_nascimento", "nacionalidade", "cidade_natural", "estado_natural",
                 "nome_genitor", "nome_genitora", "numero_documento_principal"]


def _pessoa(pessoa, base: dict, dono_tipo: str, dono_seq: int, saida: dict) -> dict:
    """Atributos da pessoa e, em ``saida``, seus documentos, endereços e outros nomes."""
    dados = {c: None for c in CAMPOS_PESSOA}
    if pessoa is None:
        return dados
    dados.update({k: v for k, v in _atributos(pessoa).items() if k in dados})
    chave = {**base, "dono_tipo": dono_tipo, "dono_seq": dono_seq}
    for i, d in enumerate(_filhos(pessoa, "documento"), 1):
        saida["documentos"].append({**chave, "seq": i, **_atributos(d)})
    _enderecos(pessoa, chave, saida)
    for i, o in enumerate(_filhos(pessoa, "outroNome"), 1):
        if o.text and o.text.strip():
            saida["outros_nomes"].append({**chave, "seq": i, "outro_nome": o.text.strip()})
    return dados


def _enderecos(el, chave: dict, saida: dict) -> None:
    for i, e in enumerate(_filhos(el, "endereco"), 1):
        saida["enderecos"].append({
            **chave, "seq": i, "cep": e.get("cep"), "logradouro": _texto(e, "logradouro"),
            "numero_endereco": _texto(e, "numero"), "complemento": _texto(e, "complemento"),
            "bairro": _texto(e, "bairro"), "cidade": _texto(e, "cidade"), "estado": _texto(e, "estado"),
            "pais": _texto(e, "pais"),
        })


def _tabelas(linhas: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    return {nome: pd.DataFrame(linhas.get(nome, []), columns=colunas).astype(object) for nome, colunas in COLUNAS.items()}


def ler_resposta_completa(xml: str | bytes) -> dict[str, pd.DataFrame]:
    """Todas as tabelas de uma resposta, como DataFrames (vazios se não houver processo)."""
    return _tabelas(extrair_registros(xml))


def extrair_registros(xml: str | bytes) -> dict[str, list[dict]]:
    """Como ler_resposta_completa, mas em listas de dicionários, sem montar DataFrames:
    bem mais rápido para processar muitas respostas e juntar tudo de uma vez."""
    if isinstance(xml, str):
        xml = xml.encode("utf-8")
    xml, _ = _separar_mtom(xml)
    root = etree.fromstring(xml)
    saida: dict[str, list[dict]] = {nome: [] for nome in COLUNAS}
    db = next((el for el in root.iter() if _local(el) == "dadosBasicos"), None)
    if db is None:
        return saida

    numero = db.get("numero")
    base = {"numero": numero}
    processo = {**base, **{k: v for k, v in _atributos(db).items() if k != "numero"}}
    orgao = _filho(db, "orgaoJulgador")
    if orgao is not None:
        processo.update(_atributos(orgao, "orgao_"))
    processo["valor_causa"] = _texto(db, "valorCausa")
    processo["magistrado_atuante"] = _texto(db, "magistradoAtuante")
    saida["processo"].append(processo)

    for i, p in enumerate(_filhos(db, "outroParametro"), 1):
        saida["parametros"].append({**base, "seq": i, "nome": p.get("nome"), "valor": p.get("valor")})
    for i, a in enumerate(_filhos(db, "assunto"), 1):
        local = _filho(a, "assuntoLocal")
        saida["assuntos"].append({
            **base, "seq": i, "principal": a.get("principal"), "codigo_nacional": _texto(a, "codigoNacional"),
            "codigo_local": local.get("codigoAssunto") if local is not None else None,
            "codigo_pai_nacional": local.get("codigoPaiNacional") if local is not None else None,
            "descricao": local.get("descricao") if local is not None else None,
        })
    for i, p in enumerate(_filhos(db, "prioridade"), 1):
        if p.text and p.text.strip():
            saida["prioridades"].append({**base, "seq": i, "prioridade": p.text.strip()})
    for i, v in enumerate(_filhos(db, "processoVinculado"), 1):
        saida["vinculos"].append({**base, "seq": i, "numero_vinculado": v.get("numeroProcesso"), "vinculo": v.get("vinculo")})

    parte_seq = 0
    for polo in _filhos(db, "polo"):
        for parte in _filhos(polo, "parte"):
            parte_seq += 1
            chave = {**base, "parte_seq": parte_seq}
            dados = _pessoa(_filho(parte, "pessoa"), chave, "parte", 0, saida)
            saida["partes"].append({**chave, "polo": polo.get("polo"), **_atributos(parte), **dados})
            for j, rel in enumerate(_filhos(parte, "pessoaProcessualRelacionada"), 1):
                dados_rel = _pessoa(_filho(rel, "pessoa"), chave, "representante", j, saida)
                saida["representantes"].append({**chave, "dono_seq": j, **_atributos(rel), **dados_rel})
            for j, adv in enumerate(_filhos(parte, "advogado"), 1):
                saida["advogados"].append({**chave, "dono_seq": j, **_atributos(adv)})
                _enderecos(adv, {**chave, "dono_tipo": "advogado", "dono_seq": j}, saida)

    for i, m in enumerate((el for el in root.iter() if _local(el) == "movimento"), 1):
        local = _filho(m, "movimentoLocal")
        pai = _filho(local, "movimentoLocalPai") if local is not None else None
        nacional = _filho(m, "movimentoNacional")
        saida["movimentos"].append({
            **base, "mov_seq": i, "identificador_movimento": m.get("identificadorMovimento"),
            "data_hora": m.get("dataHora"), "nivel_sigilo": m.get("nivelSigilo"),
            "codigo_movimento_local": local.get("codigoMovimento") if local is not None else None,
            "codigo_pai_nacional_local": local.get("codigoPaiNacional") if local is not None else None,
            "descricao_movimento_local": local.get("descricao") if local is not None else None,
            "codigo_movimento_local_pai": pai.get("codigoMovimento") if pai is not None else None,
            "codigo_pai_nacional_local_pai": pai.get("codigoPaiNacional") if pai is not None else None,
            "descricao_movimento_local_pai": pai.get("descricao") if pai is not None else None,
            "codigo_movimento_nacional": nacional.get("codigoNacional") if nacional is not None else None,
        })
        for j, c in enumerate(_filhos(m, "complemento"), 1):
            if c.text and c.text.strip():
                saida["movimentos_complementos"].append({**base, "mov_seq": i, "seq": j, "complemento": c.text.strip()})

    # Só as colunas conhecidas; atributos novos do tribunal não quebram a leitura.
    for nome, colunas in COLUNAS.items():
        saida[nome] = [{c: linha.get(c) for c in colunas} for linha in saida[nome]]
    return saida


def colunas_desconhecidas(xmls: Iterable[str | bytes]) -> set[str]:
    """Atributos de dadosBasicos/parte/pessoa vistos nos XMLs que não viram coluna (para
    revisar quando o tribunal passar a enviar campos novos)."""
    conhecidas = {c for cs in COLUNAS.values() for c in cs}
    novas = set()
    for xml in xmls:
        xml = xml.encode("utf-8") if isinstance(xml, str) else xml
        root = etree.fromstring(_separar_mtom(xml)[0])
        for el in root.iter():
            if _local(el) in ("dadosBasicos", "parte", "pessoa", "advogado", "pessoaProcessualRelacionada", "movimento"):
                novas |= {k for k in _atributos(el) if k not in conhecidas}
    return novas
