from zeep import Client
from lxml import etree

NS_SGT = "https://www.cnj.jus.br/sgt/sgt_ws.php"


class TPUClient:
    """
    Cliente SOAP para o sistema de tabelas unificadas (SGT) do CNJ.

    Utiliza o WSDL disponível em https://www.cnj.jus.br/sgt/sgt_ws.php?wsdl
    para invocar operações de tabelas públicas.
    """

    WSDL_URL = "https://www.cnj.jus.br/sgt/sgt_ws.php?wsdl"

    def __init__(self, timeout: float = 30.0) -> None:
        """
        Inicializa o cliente SOAP com timeout para as chamadas.

        :param timeout: tempo máximo (em segundos) para chamadas SOAP.
        """
        self.client = Client(wsdl=self.WSDL_URL)
        self.client.transport.session.timeout = timeout

    def pesquisar_item_publico_ws(self, tipo_tabela: str, tipo_pesquisa: str, valor_pesquisa: str):
        """
        Pesquisa as tabelas públicas de acordo com os parâmetros passados.

        :param tipo_tabela: Tipo da tabela (A, M ou C) - Assuntos, Movimentos, Classes.
        :param tipo_pesquisa: Tipo de pesquisa (G, N ou C) - Glossário, Nome ou Código.
        :param valor_pesquisa: Valor a ser pesquisado.
        :return: lista de itens encontrados.
        """
        return self.client.service.pesquisarItemPublicoWS(tipo_tabela, tipo_pesquisa, valor_pesquisa)

    def get_array_detalhes_item_publico_ws(self, seq_item: str, tipo_item: str):
        """
        Retorna detalhes do item requisitado.

        :param seq_item: Sequencial do item requisitado (código do item).
        :param tipo_item: Tipo do item (A, M ou C) - Assuntos, Movimentos, Classes.
        :return: detalhes do item.
        """
        return self.client.service.getArrayDetalhesItemPublicoWS(seq_item, tipo_item)

    def get_array_filhos_item_publico_ws(self, seq_item: str, tipo_item: str) -> list[dict]:
        """
        Retorna lista de classes/assuntos filhos de um item.

        O parser automático do zeep quebra na resposta desse método
        específico (`AttributeError: 'NoneType' object has no attribute
        'elements'` — schema com elemento `xsd:any` sem tipo resolvível,
        problema conhecido do zeep com esse WSDL). Por isso usa
        `raw_response=True` e faz o parse manual com `lxml`.

        :param seq_item: Sequencial do item requisitado (código do item).
        :param tipo_item: Tipo do item (A, M ou C) - Assuntos, Movimentos, Classes.
        :return: lista de dicts com seq_elemento, dsc_elemento, seq_elemento_pai, tem_filhos, situacao.
        """
        with self.client.settings(raw_response=True):
            resposta = self.client.service.getArrayFilhosItemPublicoWS(seq_item, tipo_item)

        raiz = etree.fromstring(resposta.content)
        nos = raiz.findall(f".//{{{NS_SGT}}}ArvoreGenerica")

        return [
            {
                "seq_elemento": no.findtext("seq_elemento"),
                "dsc_elemento": no.findtext("dsc_elemento"),
                "seq_elemento_pai": no.findtext("seq_elemento_pai"),
                "tem_filhos": bool(no.findtext("temFilhos")),
                "situacao": no.findtext("situacao"),
            }
            for no in nos
        ]

    def get_arvore_completa(self, seq_item: str, tipo_item: str = "A") -> list[dict]:
        """
        Busca recursivamente toda a subárvore abaixo de um item (todos os
        descendentes, não só os filhos diretos), usando
        `get_array_filhos_item_publico_ws`.

        :param seq_item: Sequencial do item raiz (ex.: "9634" para "Ato Infracional").
        :param tipo_item: Tipo do item (A, M ou C) - Assuntos, Movimentos, Classes.
        :return: lista de dicts (mesmo formato de `get_array_filhos_item_publico_ws`),
            achatada, com todos os descendentes em qualquer nível.
        """
        filhos = self.get_array_filhos_item_publico_ws(seq_item, tipo_item)
        todos = list(filhos)
        for filho in filhos:
            if filho["tem_filhos"]:
                todos.extend(self.get_arvore_completa(filho["seq_elemento"], tipo_item))
        return todos

    def get_string_pais_item_publico_ws(self, seq_item: str, tipo_item: str):
        """
        Retorna encadeamento de pais de um item como string.

        :param seq_item: Sequencial do item requisitado (código do item).
        :param tipo_item: Tipo do item (A, M ou C) - Assuntos, Movimentos, Classes.
        :return: string de pais encadeados.
        """
        return self.client.service.getStringPaisItemPublicoWS(seq_item, tipo_item)

    def get_complemento_movimento_ws(self, cod_movimento: str):
        """
        Retorna complementos tabelados para um movimento.

        :param cod_movimento: Sequencial do movimento (pode ser vazio para todos).
        :return: lista de complementos.
        """
        return self.client.service.getComplementoMovimentoWS(cod_movimento)

    def get_data_ultima_versao_ws(self):
        """
        Retorna a data da última versão do sistema de tabelas.

        :return: string com a data da versão.
        """
        return self.client.service.getDataUltimaVersao()
