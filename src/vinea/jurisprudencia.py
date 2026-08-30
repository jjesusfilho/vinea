"""
Cliente para a busca pública de jurisprudência do e-Proc do TJSP.

A busca (`externo_controlador.php?acao=jurisprudencia@jurisprudencia/...`) é
pública, sem autenticação, e cobre 1º e 2º grau simultaneamente: sentenças,
acórdãos e decisões monocráticas, incluindo os órgãos recursais do Juizado
Especial (Turma/Colégio Recursal), além das Câmaras de Direito Privado/Público
do 2º grau "comum". Os parâmetros, a paginação e o formato de resposta foram
identificados a partir de uma captura HAR de uma sessão real de busca, e a
lógica de paginação foi confirmada lendo `modulos/jurisprudencia/js/jurisprudencia.js`
(função `paginar()`, que reenvia o formulário inteiro para
`ajax_paginar_resultado`, só alterando `hdnPaginaAtual`).

Como o CJPG/CJSG do e-SAJ, essa busca não deixa navegar além de 1000
resultados (100 páginas de 10) por consulta; pesquisas amplas precisam ser
fatiadas por período de julgamento ou de publicação.
"""

import time
from dataclasses import dataclass, field
from typing import Iterator, Optional, Sequence

import requests
from lxml import html

BASE_URL = "https://eproc1g.tjsp.jus.br/eproc/externo_controlador.php"

ACAO_PESQUISAR = "jurisprudencia@jurisprudencia/pesquisar"
ACAO_LISTAR_RESULTADOS = "jurisprudencia@jurisprudencia/listar_resultados"
ACAO_PAGINAR_RESULTADO = "jurisprudencia@jurisprudencia/ajax_paginar_resultado"
ACAO_CARREGAR_LISTAS = "jurisprudencia@jurisprudencia/ajax_carregar_listas_pesquisa"
ACAO_LISTAR_TIPO_DOCUMENTO = "jurisprudencia@jurisprudencia/ajax_listar_tipo_documento"

# id -> descrição, conforme retornado por `ajax_listar_tipo_documento`.
TIPOS_DOCUMENTO = {1: "Acórdão", 2: "Decisão monocrática", 5: "Sentença"}

_ROTULOS_RESULTADO = {
    "orgao_julgador": "ÓRGÃO JULGADOR",
    "relator": "RELATOR",
    "data_julgamento": "DATA DO JULGAMENTO",
    "data_publicacao": "DATA DA PUBLICAÇÃO",
    "decisao": "DECISÃO",
    "ementa": "EMENTA",
}


@dataclass
class PaginaResultadosJurisprudencia:
    """Uma página de resultados da busca de jurisprudência."""

    resultados: list[dict] = field(default_factory=list)
    pagina_atual: int = 1
    total_paginas: int = 0
    total_resultados: int = 0


class EprocJurisprudenciaClient:
    """Cliente para a busca pública de jurisprudência do e-Proc do TJSP."""

    def __init__(
        self,
        timeout: float = 30.0,
        max_tentativas: int = 3,
        espera_entre_tentativas: float = 1.5,
    ):
        """
        Args:
            timeout: tempo máximo (em segundos) para cada requisição.
            max_tentativas: tentativas por requisição antes de desistir.
                Necessário porque o portal responde 503 de forma
                intermitente a requisições automatizadas (confirmado
                empiricamente; uma sessão de navegador real não recebeu
                nenhum 503 em ~200 requisições na mesma busca).
            espera_entre_tentativas: base (em segundos) do backoff
                exponencial entre tentativas.
        """
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9",
            }
        )
        self.timeout = timeout
        self.max_tentativas = max_tentativas
        self.espera_entre_tentativas = espera_entre_tentativas
        self._sessao_aquecida = False

    def _requisitar(self, metodo: str, url: str, **kwargs) -> requests.Response:
        ultimo_erro: Optional[Exception] = None
        for tentativa in range(1, self.max_tentativas + 1):
            try:
                resposta = self.session.request(
                    metodo, url, timeout=self.timeout, **kwargs
                )
                resposta.raise_for_status()
                return resposta
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as erro:
                ultimo_erro = erro
                if tentativa == self.max_tentativas:
                    break
                time.sleep(self.espera_entre_tentativas * (2 ** (tentativa - 1)))
        raise ultimo_erro

    def _aquecer_sessao(self) -> None:
        """
        Visita a página de busca antes do primeiro POST.

        Sem isso (e sem um User-Agent de navegador), o portal responde
        503 a requisições feitas "a frio" — comportamento confirmado
        empiricamente, não documentado. Roda uma única vez por cliente.
        """
        if self._sessao_aquecida:
            return
        resposta = self._requisitar("GET", f"{BASE_URL}?acao={ACAO_PESQUISAR}")
        self.session.headers["Referer"] = resposta.url
        self.session.headers["Origin"] = "https://eproc1g.tjsp.jus.br"
        self._sessao_aquecida = True

    def _post(self, acao: str, dados) -> requests.Response:
        # `@` e `/` em `acao` precisam ir literais na query string: o portal
        # rejeita (503) a forma percent-encoded que `params=` produziria.
        self._aquecer_sessao()
        return self._requisitar("POST", f"{BASE_URL}?acao={acao}", data=dados)

    def carregar_listas_pesquisa(self, origem: Sequence[int]) -> dict:
        """
        Carrega as opções de classe, relator e órgão julgador disponíveis
        para as origens informadas.

        Args:
            origem: códigos numéricos de origem (os mesmos aceitos por `buscar`).

        Returns:
            dict com as chaves `arrClasse`, `arrRelator` e `arrOrgao`, cada uma
            um mapeamento {rótulo: rótulo} das opções disponíveis.
        """
        dados = [("arrOrigem[]", str(codigo)) for codigo in origem]
        return self._post(ACAO_CARREGAR_LISTAS, dados).json()

    def listar_tipo_documento(self, origem: Sequence[int]) -> list[dict]:
        """
        Lista os tipos de documento (Acórdão, Decisão monocrática, Sentença)
        disponíveis para as origens informadas.

        Args:
            origem: códigos numéricos de origem.

        Returns:
            lista de dicts `{"id": int, "descricao": str}`.
        """
        dados = {"origem": ",".join(str(codigo) for codigo in origem)}
        return self._post(ACAO_LISTAR_TIPO_DOCUMENTO, dados).json()

    def _montar_formulario(
        self,
        *,
        termo: Optional[str],
        origem: Sequence[int],
        tipo_documento: Sequence[int],
        campo: str,
        classe: Sequence[str],
        relator: Sequence[str],
        orgao: Sequence[str],
        processo: Optional[str],
        data_julgamento_inicio: Optional[str],
        data_julgamento_fim: Optional[str],
        data_publicacao_inicio: Optional[str],
        data_publicacao_fim: Optional[str],
        agrupar_resultados: bool,
        pagina: int,
    ) -> list[tuple[str, str]]:
        if not origem:
            raise ValueError("Informe ao menos uma origem em `origem`.")
        if campo not in ("I", "E"):
            raise ValueError("`campo` deve ser 'I' (inteiro teor) ou 'E' (ementa).")
        if pagina < 1:
            raise ValueError("`pagina` deve ser >= 1.")

        termo_formatado = f'"{termo}"' if termo else ""

        dados: list[tuple[str, str]] = [
            ("txtPesquisa", termo_formatado),
            ("hdnExibirPesquisaAvancada", ""),
            (
                "hdnUrlCarregarListasCombobox",
                f"externo_controlador.php?acao={ACAO_CARREGAR_LISTAS}",
            ),
            ("rdoCampo", campo),
            ("txtProcesso", processo or ""),
            ("dtDecisaoInicio", data_julgamento_inicio or ""),
            ("dtDecisaoFim", data_julgamento_fim or ""),
            ("hdnDecisaoInicio", ""),
            ("hdnDecisaoFim", ""),
            ("dtPublicacaoInicio", data_publicacao_inicio or ""),
            ("dtPublicacaoFim", data_publicacao_fim or ""),
            ("hdnPublicacaoInicio", ""),
            ("hdnPublicacaoFim", ""),
            ("hdnPaginaAtual", str(pagina)),
        ]
        if agrupar_resultados:
            dados.append(("chkAgruparResultados", "on"))
        dados.extend(("selOrigem[]", str(codigo)) for codigo in origem)
        dados.extend(("selTipoDocumento[]", str(codigo)) for codigo in tipo_documento)
        dados.extend(("selClasse[]", valor) for valor in classe)
        dados.extend(("selRelator[]", valor) for valor in relator)
        dados.extend(("selOrgao[]", valor) for valor in orgao)
        return dados

    def buscar(
        self,
        termo: Optional[str] = None,
        *,
        origem: Sequence[int] = (3, 4, 5),
        tipo_documento: Sequence[int] = (1, 2, 5),
        campo: str = "I",
        classe: Sequence[str] = (),
        relator: Sequence[str] = (),
        orgao: Sequence[str] = (),
        processo: Optional[str] = None,
        data_julgamento_inicio: Optional[str] = None,
        data_julgamento_fim: Optional[str] = None,
        data_publicacao_inicio: Optional[str] = None,
        data_publicacao_fim: Optional[str] = None,
        agrupar_resultados: bool = True,
        pagina: int = 1,
    ) -> PaginaResultadosJurisprudencia:
        """
        Busca uma página de resultados na jurisprudência do e-Proc.

        Datas no formato "dd/mm/aaaa". `origem` e `tipo_documento` aceitam os
        códigos numéricos do portal (ver `carregar_listas_pesquisa` e
        `listar_tipo_documento`); os padrões (3, 4, 5) e (1, 2, 5) reproduzem
        a busca "1º e 2º grau, todos os tipos de documento" observada na
        captura original.

        O portal não deixa navegar além de 1000 resultados (100 páginas) por
        busca; para universos maiores, estreite o período de julgamento ou
        publicação e faça buscas em janelas sucessivas (ver
        `buscar_todas_paginas`).

        Args:
            termo: termo(s) de busca; é enviado entre aspas (busca por frase
                exata), como a interface faz por padrão.
            campo: "I" (inteiro teor, padrão) ou "E" (só ementa).
            pagina: página a buscar (1-based).

        Returns:
            `PaginaResultadosJurisprudencia` com os resultados da página e os
            totais informados pelo portal.
        """
        dados = self._montar_formulario(
            termo=termo,
            origem=origem,
            tipo_documento=tipo_documento,
            campo=campo,
            classe=classe,
            relator=relator,
            orgao=orgao,
            processo=processo,
            data_julgamento_inicio=data_julgamento_inicio,
            data_julgamento_fim=data_julgamento_fim,
            data_publicacao_inicio=data_publicacao_inicio,
            data_publicacao_fim=data_publicacao_fim,
            agrupar_resultados=agrupar_resultados,
            pagina=pagina,
        )
        acao = ACAO_LISTAR_RESULTADOS if pagina == 1 else ACAO_PAGINAR_RESULTADO
        resposta = self._post(acao, dados)
        return self._parsear_pagina(resposta.content)

    def buscar_todas_paginas(
        self,
        termo: Optional[str] = None,
        *,
        pagina_maxima: Optional[int] = None,
        **kwargs,
    ) -> Iterator[dict]:
        """
        Percorre todas as páginas de uma busca, um resultado por vez.

        Para quando a página atual atinge `total_paginas` (informado pelo
        próprio portal) ou `pagina_maxima`, o que vier primeiro. Como o
        portal não devolve mais de 100 páginas por busca, use
        `pagina_maxima=100` como trava se não tiver certeza de que o período
        já está estreito o bastante.

        Args:
            termo: ver `buscar`.
            pagina_maxima: limite de segurança de páginas a percorrer.
            **kwargs: demais argumentos de `buscar` (origem, tipo_documento,
                datas etc.).

        Yields:
            um dict por resultado (mesmo formato de `PaginaResultadosJurisprudencia.resultados`).
        """
        pagina = 1
        while True:
            resultado = self.buscar(termo, pagina=pagina, **kwargs)
            yield from resultado.resultados

            limite = resultado.total_paginas
            if pagina_maxima is not None:
                limite = min(limite, pagina_maxima)
            if pagina >= limite or not resultado.resultados:
                break
            pagina += 1

    def _parsear_pagina(self, conteudo_html: bytes) -> PaginaResultadosJurisprudencia:
        arvore = html.fromstring(conteudo_html)

        def _valor_hidden(id_campo: str) -> str:
            elementos = arvore.xpath(f"//input[@id='{id_campo}']/@value")
            return elementos[0] if elementos else "0"

        resultados = [
            self._parsear_resultado(cartao)
            for cartao in arvore.xpath("//div[div[@class='card-body py-2 px-3']]")
        ]

        return PaginaResultadosJurisprudencia(
            resultados=resultados,
            pagina_atual=int(_valor_hidden("hdnPaginaAtual") or "1"),
            total_paginas=int(_valor_hidden("hdnTotalPaginas") or "0"),
            total_resultados=int(_valor_hidden("hdnTotalResultado") or "0"),
        )

    def _parsear_resultado(self, cartao) -> dict:
        def _texto_label(rotulo: str) -> Optional[str]:
            valores = cartao.xpath(
                f".//div[@class='resLabel' and normalize-space(text())='{rotulo}']"
                "/following-sibling::div[contains(@class,'resValue')][1]"
            )
            if not valores:
                return None
            texto = valores[0].text_content().strip()
            return texto or None

        link_processo = cartao.xpath(".//a[contains(@class,'numero-processo')]")
        numero_processo = None
        link_consulta_publica = None
        if link_processo:
            link_consulta_publica = link_processo[0].get("href")
            numero_processo = link_processo[0].text_content().strip()
            if link_consulta_publica:
                match = link_consulta_publica.split("num_processo=")
                if len(match) > 1:
                    numero_processo = match[1].split("&")[0]

        tipo_documento = cartao.xpath(".//div[@class='resValueTipoJurisprudencia']/text()")
        citacao = cartao.xpath(".//a[contains(@class,'copiarCitacao')]/@data-citacao")
        id_documento = cartao.get("id")

        resultado = {
            "id_documento": id_documento,
            "numero_processo": numero_processo,
            "link_consulta_publica": link_consulta_publica,
            "tipo_documento": tipo_documento[0].strip() if tipo_documento else None,
            "citacao": citacao[0] if citacao else None,
        }
        for chave, rotulo in _ROTULOS_RESULTADO.items():
            resultado[chave] = _texto_label(rotulo)
        return resultado
