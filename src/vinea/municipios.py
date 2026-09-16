"""
Padronização de nomes de municípios contra a lista oficial do IBGE.

Texto de município extraído de documento (BO, petição etc.) chega com grafias
muito variadas para o mesmo lugar — caixa, acento, sufixo de UF e abreviação:
"São Paulo", "SÃO PAULO - SP", "S.PAULO", "São Paulo/SP". Isso fragmenta
contagem e filtro em qualquer análise. Diferente da classificação de natureza
(que precisa de LLM porque o mapeamento é semântico), aqui existe lista
autoritativa, então a normalização é determinística: normaliza a grafia e
casa contra os municípios do IBGE.
"""

import gzip
import json
import re
import unicodedata
from importlib import resources
from typing import Optional

URL_MUNICIPIOS_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
ARQUIVO_MUNICIPIOS = "municipios_ibge.json.gz"

_PREFIXOS_ABREVIAVEIS = ("sao", "santa", "santo")
_CONECTORES = {"de", "do", "da", "dos", "das", "d"}
_SIGLAS_UF = {
    "ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma", "mt", "ms",
    "mg", "pa", "pb", "pr", "pe", "pi", "rj", "rn", "rs", "ro", "rr", "sc",
    "sp", "se", "to",
}


def _normalizar(valor: str) -> str:
    """Reduz a grafia ao essencial: sem acento, minúscula, sem pontuação, sem sufixo de UF."""
    texto = unicodedata.normalize("NFKD", str(valor)).encode("ascii", "ignore").decode()
    texto = texto.lower().strip()
    texto = re.sub(
        r"[\s/,.-]*\b([a-z]{2})\b\.?$",
        lambda m: "" if m.group(1) in _SIGLAS_UF else m.group(0),
        texto,
    )
    return re.sub(r"[^a-z0-9]+", " ", texto).strip()


def _sem_conectores(chave: str) -> str:
    return " ".join(t for t in chave.split() if t not in _CONECTORES)


def _variantes_abreviacao(chave: str) -> list[str]:
    """
    Expande tokens "s" isolados para são/santa/santo, em qualquer posição.

    Cobre tanto "S.PAULO" quanto "ESPIRITO S. DO TURVO" — como a expansão certa
    depende do município, gera todas as combinações e deixa o índice oficial
    decidir qual existe de fato.
    """
    tokens = chave.split()
    posicoes = [i for i, t in enumerate(tokens) if t == "s"]
    if not posicoes:
        return []
    combinacoes = [tokens]
    for i in posicoes:
        combinacoes = [
            variante[:i] + [prefixo] + variante[i + 1:]
            for variante in combinacoes
            for prefixo in _PREFIXOS_ABREVIAVEIS
        ]
    return [" ".join(c) for c in combinacoes]


class NormalizadorMunicipios:
    """
    Casa nomes de município em texto livre com a grafia oficial do IBGE.

    A busca é feita primeiro dentro da UF preferencial (padrão SP) e só depois
    no país inteiro: sem isso, nomes homônimos entre estados ("Bom Jesus",
    "Bonito") cairiam no município errado.

    Exemplo:
        >>> n = NormalizadorMunicipios()
        >>> n.canonico("S.PAULO - SP")
        'São Paulo'
        >>> n.canonico("ESPIRITO S. DO TURVO")
        'Espírito Santo do Turvo'
    """

    def __init__(self, uf_preferencial: str = "SP", online: bool = False, timeout: int = 60):
        """
        Args:
            uf_preferencial: sigla da UF consultada antes do restante do país.
                Use `None`/"" para não priorizar nenhuma.
            online: se True, busca a lista na API do IBGE em vez de usar a cópia
                embutida no pacote. O padrão é offline de propósito: dentro de
                notebook do Fabric, depender de host externo em tempo de execução
                deixa o pipeline sujeito a bloqueio de rede e a indisponibilidade
                do IBGE, e a lista de municípios muda muito raramente.
            timeout: timeout (s) da chamada à API, quando `online=True`.
        """
        self.uf_preferencial = (uf_preferencial or "").upper()
        municipios = self._buscar_online(timeout) if online else self._carregar_embutidos()
        preferenciais = [m for m in municipios if m["uf"] == self.uf_preferencial]
        self._indices = [self._construir_indice(m) for m in (preferenciais, municipios) if m]

    @staticmethod
    def _carregar_embutidos() -> list[dict]:
        arquivo = resources.files("vinea.dados").joinpath(ARQUIVO_MUNICIPIOS)
        with gzip.open(arquivo.open("rb"), "rt", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _buscar_online(timeout: int) -> list[dict]:
        import requests

        resposta = requests.get(URL_MUNICIPIOS_IBGE, timeout=timeout)
        resposta.raise_for_status()
        # A UF vem do prefixo do código IBGE (2 primeiros dígitos, 35 = SP). É mais
        # confiável que navegar `microrregiao`/`regiao-imediata`, cuja estrutura varia
        # entre registros (municípios criados recentemente não têm `microrregiao`).
        codigos_uf = {
            "12": "AC", "27": "AL", "16": "AP", "13": "AM", "29": "BA", "23": "CE",
            "53": "DF", "32": "ES", "52": "GO", "21": "MA", "51": "MT", "50": "MS",
            "31": "MG", "15": "PA", "25": "PB", "41": "PR", "26": "PE", "22": "PI",
            "33": "RJ", "24": "RN", "43": "RS", "11": "RO", "14": "RR", "42": "SC",
            "35": "SP", "28": "SE", "17": "TO",
        }
        return [
            {"nome": m["nome"], "uf": codigos_uf.get(str(m["id"])[:2], "")}
            for m in resposta.json()
        ]

    @staticmethod
    def _construir_indice(municipios: list[dict]) -> tuple[dict, dict]:
        exato: dict[str, str] = {}
        sem_conectores: dict[str, str] = {}
        for m in municipios:
            chave = _normalizar(m["nome"])
            exato.setdefault(chave, m["nome"])
            sem_conectores.setdefault(_sem_conectores(chave), m["nome"])
        return exato, sem_conectores

    def canonico(self, valor: Optional[str]) -> Optional[str]:
        """
        Devolve a grafia oficial do município, ou `None` se nada casar.

        `None` é resposta legítima (município inexistente, texto truncado, lixo de
        extração) — quem chama decide se mantém o valor original ou descarta.
        """
        if valor is None or not str(valor).strip():
            return None
        chave = _normalizar(valor)
        sem_con = _sem_conectores(chave)
        for exato, indice_sem_con in self._indices:
            for candidato in [chave, *_variantes_abreviacao(chave)]:
                if candidato in exato:
                    return exato[candidato]
            for candidato in [sem_con, *_variantes_abreviacao(sem_con)]:
                if candidato in indice_sem_con:
                    return indice_sem_con[candidato]
        return None

    def mapa(self, valores) -> dict[str, Optional[str]]:
        """De/para pronto para join, com os valores distintos já deduplicados."""
        return {v: self.canonico(v) for v in {x for x in valores if x is not None}}
