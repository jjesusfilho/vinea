"""
Módulo para extração de informações de processos de ato infracional a
partir do Boletim de Ocorrência e dos documentos de qualificação das
partes, usando Azure OpenAI.
"""

import base64
import io
import json
import os
import re
from pathlib import Path
from typing import Optional, Union

from openai import AzureOpenAI, OpenAI

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

from .infracional_models import ProcessoInfracionalData, IdentificacaoProcesso

PADRAO_CID = re.compile(r"\(cid:\d+\)")


class InfracionalExtractor:
    """
    Extrator de informações de processos de ato infracional, combinando o
    texto do Boletim de Ocorrência com o(s) documento(s) de qualificação
    das partes — a qualificação normalmente não está no BO desde 2022, só
    nos documentos seguintes (ver CLAUDE.md do projeto infancia).
    """

    def __init__(
        self,
        azure_openai_endpoint: Optional[str] = None,
        azure_openai_key: Optional[str] = None,
        azure_openai_deployment: Optional[str] = None,
        azure_openai_api_version: Optional[str] = None,
    ):
        """
        Inicializa o extrator.

        Args:
            azure_openai_endpoint: Endpoint do Azure OpenAI
            azure_openai_key: Chave de acesso do Azure OpenAI
            azure_openai_deployment: Nome do deployment do modelo
            azure_openai_api_version: Versão da API do Azure OpenAI
        """
        if not PDFPLUMBER_AVAILABLE:
            raise ImportError(
                "pdfplumber não está disponível. "
                "Instale com: pip install pdfplumber"
            )

        self.azure_openai_endpoint = azure_openai_endpoint or os.getenv("AZURE_OPENAI_RESOURCE")
        self.azure_openai_key = azure_openai_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.azure_openai_deployment = azure_openai_deployment or os.getenv("AZURE_OPENAI_IMPLEMENTACAO")
        self.azure_openai_api_version = azure_openai_api_version or os.getenv("AZURE_OPENAI_VERSAO_API", "2024-02-01")

        if self.azure_openai_endpoint and self.azure_openai_key:
            if self.azure_openai_endpoint.startswith("https://"):
                # URL completa (ex.: endpoint "v1" do Azure AI Foundry,
                # .../openai/v1) — client OpenAI padrão, compatível com essa
                # API mais nova, sem api_version por chamada.
                self.openai_client = OpenAI(
                    base_url=self.azure_openai_endpoint,
                    api_key=self.azure_openai_key,
                )
            else:
                # Nome "nu" do resource — endpoint clássico do Azure OpenAI.
                classic_endpoint = f"https://{self.azure_openai_endpoint}.openai.azure.com"
                self.openai_client = AzureOpenAI(
                    api_key=self.azure_openai_key,
                    api_version=self.azure_openai_api_version,
                    azure_endpoint=classic_endpoint,
                )
        else:
            self.openai_client = None

    def extract_text_from_pdf(self, pdf_path: Union[str, Path]) -> str:
        """Extrai texto de um PDF usando pdfplumber."""
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {pdf_path}")

        text_content = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text)

        return "\n\n".join(text_content)

    def transcrever_paginas_por_visao(
        self,
        pdf_path: Union[str, Path],
        resolution: int = 200,
        max_completion_tokens: int = 4000,
    ) -> str:
        """
        Transcreve um PDF (provavelmente digitalizado/sem camada de texto)
        renderizando cada página como imagem e enviando pro modelo de
        visão do Azure OpenAI/AI Foundry — sem depender de Document
        Intelligence nem de binário de OCR local (usa `pypdfium2`, que já
        vem com o `pdfplumber`, sem dependência de sistema).

        Args:
            pdf_path: Caminho do PDF
            resolution: DPI da renderização da página (200 costuma bastar)
            max_completion_tokens: Tokens máximos por página transcrita.
                Modelos de raciocínio (ex.: gpt-5.6-luna) gastam parte
                desse orçamento em "pensamento" interno antes da resposta
                visível — em teste real, uma página densa consumiu ~1600
                tokens só de raciocínio; com 2000 no total a resposta veio
                vazia, com 4000 veio completa. Não reduza sem testar.

        Returns:
            Texto transcrito, concatenado por página
        """
        if not self.openai_client:
            raise ValueError(
                "Azure OpenAI não foi configurado. "
                "Forneça azure_openai_endpoint, azure_openai_key e azure_openai_deployment."
            )

        pdf_path = Path(pdf_path)
        textos_paginas = []

        with pdfplumber.open(pdf_path) as pdf:
            for pagina in pdf.pages:
                imagem = pagina.to_image(resolution=resolution)
                buffer = io.BytesIO()
                imagem.original.save(buffer, format="PNG")
                b64 = base64.b64encode(buffer.getvalue()).decode("utf-8")

                resposta = self.openai_client.chat.completions.create(
                    model=self.azure_openai_deployment,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        "Transcreva todo o texto legível desta imagem de um "
                                        "documento judicial/policial, preservando a estrutura "
                                        "(campos, seções, tabelas de pessoas). Se não conseguir "
                                        "ler nada, responda apenas 'ILEGÍVEL'."
                                    ),
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {"url": f"data:image/png;base64,{b64}"},
                                },
                            ],
                        }
                    ],
                    max_completion_tokens=max_completion_tokens,
                )
                textos_paginas.append(resposta.choices[0].message.content or "")

        return "\n\n".join(textos_paginas)

    def extract_text_com_fallback_visao(
        self,
        pdf_path: Union[str, Path],
        min_chars: int = 50,
    ) -> tuple[str, bool]:
        """
        Extrai texto do PDF; se vier insuficiente (provável documento
        digitalizado sem camada de texto, ou fonte com encoding quebrado),
        cai pra transcrição por visão (`transcrever_paginas_por_visao`).

        Args:
            pdf_path: Caminho do PDF
            min_chars: Tamanho mínimo (após limpar ruído `(cid:N)`) do
                texto extraído por `pdfplumber` pra considerar suficiente

        Returns:
            Tupla (texto, usou_visao) — `usou_visao` indica se caiu no
            fallback, útil pra registrar isso na tabela de destino.
        """
        texto = self.extract_text_from_pdf(pdf_path)
        texto_limpo = PADRAO_CID.sub("", texto).strip()

        if len(texto_limpo) >= min_chars:
            return texto, False

        return self.transcrever_paginas_por_visao(pdf_path), True

    def _montar_texto_combinado(self, texto_bo: str, textos_qualificacao: Optional[list[str]]) -> str:
        """Combina o texto do BO com os textos de qualificação, com seções rotuladas."""
        partes = [f"=== BOLETIM DE OCORRÊNCIA ===\n{texto_bo[:40000]}"]
        for i, texto_q in enumerate(textos_qualificacao or [], start=1):
            partes.append(f"=== DOCUMENTO DE QUALIFICAÇÃO {i} ===\n{texto_q[:20000]}")
        return "\n\n".join(partes)

    def extract_infracional_data_with_llm(
        self,
        texto_bo: str,
        numero_processo: str,
        textos_qualificacao: Optional[list[str]] = None,
        max_completion_tokens: int = 4000,
        temperature: Optional[float] = None,
    ) -> ProcessoInfracionalData:
        """
        Extrai dados estruturados de um processo de ato infracional usando LLM.

        Args:
            texto_bo: Texto extraído do(s) boletim(ns) de ocorrência
            numero_processo: Número do processo
            textos_qualificacao: Textos dos documentos de qualificação das
                partes (Auto de Qualificação, Petição (Outras) etc.), se
                houver — é onde normalmente está a qualificação completa
                das pessoas, não no BO
            max_completion_tokens: Número máximo de tokens na resposta
            temperature: Temperatura para geração (menor = mais determinístico).
                None (padrão) não envia o parâmetro — modelos de raciocínio
                (ex.: GPT-5, o1/o3) só aceitam o valor default e retornam erro
                se `temperature` for informado.

        Returns:
            Objeto ProcessoInfracionalData com os dados extraídos
        """
        if not self.openai_client:
            raise ValueError(
                "Azure OpenAI não foi configurado. "
                "Forneça azure_openai_endpoint, azure_openai_key e azure_openai_deployment."
            )

        schema = ProcessoInfracionalData.model_json_schema()
        texto_completo = self._montar_texto_combinado(texto_bo, textos_qualificacao)

        prompt = f"""Você é um assistente especializado em análise de processos judiciais de apuração de ato infracional (infância e juventude).

O texto abaixo pode conter mais de um documento: o Boletim de Ocorrência (que costuma trazer poucos dados de qualificação das partes) e, quando houver, documento(s) de qualificação das partes (Auto de Qualificação, Petição (Outras), Representação, Informações sobre Antecedentes do Adolescente), que é onde normalmente está a qualificação completa (filiação, endereço, documentos).

Extraia todas as informações relevantes conforme o schema JSON fornecido.

INSTRUÇÕES:
1. Extraia TODAS as informações presentes no texto que correspondam aos campos do schema
2. Combine informações do BO e dos documentos de qualificação sobre a MESMA pessoa num único registro em "pessoas" — não duplique a pessoa
3. Pode haver mais de um boletim de ocorrência (mais de uma delegacia envolvida) — inclua todos em "boletins_ocorrencia"
4. Pode haver mais de um crime/natureza por boletim — inclua todos em "crimes"
5. Para campos não encontrados, use null
6. Para datas, use o formato ISO (YYYY-MM-DD) ou (YYYY-MM-DDTHH:MM:SS) quando houver hora
7. Para campos booleanos, use true/false baseado no que está explícito ou implícito no texto
8. Para enums, use exatamente os valores definidos no schema
9. Seja preciso e fiel ao conteúdo do documento — não infira dados que não estão no texto
10. Retorne APENAS o JSON válido, sem texto adicional

SCHEMA JSON:
{json.dumps(schema, indent=2, ensure_ascii=False)}

TEXTO DO PROCESSO:
{texto_completo}

Retorne o JSON estruturado com os dados extraídos:"""

        kwargs = {}
        if temperature is not None:
            kwargs["temperature"] = temperature

        response = self.openai_client.chat.completions.create(
            model=self.azure_openai_deployment,
            messages=[
                {
                    "role": "system",
                    "content": "Você é um assistente especializado em extração de dados de processos judiciais. Retorne apenas JSON válido.",
                },
                {"role": "user", "content": prompt},
            ],
            max_completion_tokens=max_completion_tokens,
            response_format={"type": "json_object"},
            **kwargs,
        )

        json_str = response.choices[0].message.content

        try:
            data_dict = json.loads(json_str)
            if "identificacao_processo" not in data_dict:
                data_dict["identificacao_processo"] = {}
            data_dict["identificacao_processo"]["numero_processo"] = numero_processo

            return ProcessoInfracionalData(**data_dict)
        except Exception as e:
            print(f"Erro ao parsear resposta do LLM: {e}")
            print(f"Resposta: {json_str}")
            return ProcessoInfracionalData(
                identificacao_processo=IdentificacaoProcesso(numero_processo=numero_processo)
            )

    def extract_from_textos(
        self,
        texto_bo: str,
        numero_processo: str,
        textos_qualificacao: Optional[list[str]] = None,
        geocode: bool = True,
    ) -> ProcessoInfracionalData:
        """
        Extrai dados de um processo a partir de textos já extraídos (ex.:
        vindos do MNI via `vinea.MNIClient`, sem precisar salvar em PDF).
        """
        print(f"Extraindo dados estruturados com LLM para o processo {numero_processo}...")
        dados = self.extract_infracional_data_with_llm(texto_bo, numero_processo, textos_qualificacao)

        if geocode:
            print("Geocodificando endereços...")
            from .geocoding import ProcessoInfracionalGeocoder
            ProcessoInfracionalGeocoder().geocode_processo_addresses(dados)

        return dados

    def extract_from_pdfs(
        self,
        pdf_bo: Union[str, Path],
        pdfs_qualificacao: Optional[list[Union[str, Path]]] = None,
        numero_processo: Optional[str] = None,
        geocode: bool = True,
    ) -> ProcessoInfracionalData:
        """
        Extrai dados de um processo diretamente de PDFs (um BO e, opcionalmente,
        um ou mais documentos de qualificação).
        """
        pdf_bo = Path(pdf_bo)

        if not numero_processo:
            numero_processo = pdf_bo.stem.replace("-", "").replace(".", "")[:20]

        print(f"Extraindo texto do BO: {pdf_bo.name}")
        texto_bo = self.extract_text_from_pdf(pdf_bo)

        textos_qualificacao = []
        for pdf_q in pdfs_qualificacao or []:
            pdf_q = Path(pdf_q)
            print(f"Extraindo texto do documento de qualificação: {pdf_q.name}")
            textos_qualificacao.append(self.extract_text_from_pdf(pdf_q))

        dados = self.extract_from_textos(texto_bo, numero_processo, textos_qualificacao, geocode=geocode)
        dados.arquivo_fonte = str(pdf_bo)
        return dados

    def save_processo_data(
        self,
        processo_data: ProcessoInfracionalData,
        output_path: Union[str, Path],
        format: str = "json",
    ):
        """Salva os dados extraídos do processo."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(processo_data.model_dump_json(indent=2, exclude_none=False))
        elif format == "dict":
            data_dict = processo_data.model_dump(exclude_none=False)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data_dict, f, indent=2, ensure_ascii=False, default=str)
        else:
            raise ValueError(f"Formato não suportado: {format}")

        print(f"Dados do processo salvos em: {output_path}")

    def classificar_naturezas(
        self,
        naturezas: list[str],
        taxonomia: list[str],
        tamanho_lote: int = 40,
        max_completion_tokens: int = 4000,
    ) -> dict[str, Optional[str]]:
        """
        Classifica valores de `natureza` (texto livre, extraído dos BOs)
        contra uma taxonomia de referência (ex.: árvore "Ato Infracional"
        da Tabela Processual Unificada do CNJ, via `vinea.TPUClient`) —
        pensado pra reduzir o número de variações de texto que descrevem o
        mesmo tipo de ato infracional (ver CLAUDE.md do projeto infancia).

        Classifica os valores DISTINTOS (ex.: os de `dim_natureza`), não
        cada ocorrência — é uma tabela de/para pequena, não algo por linha
        de fato.

        Args:
            naturezas: Lista de valores distintos de `natureza` a classificar
            taxonomia: Lista de categorias de referência (ex.: nomes dos nós
                da árvore retornada por `TPUClient.get_arvore_completa`)
            tamanho_lote: Quantas naturezas mandar por chamada ao LLM
            max_completion_tokens: Tokens máximos por chamada

        Returns:
            Dict mapeando cada natureza original para a categoria da
            taxonomia mais próxima, ou `None` se nenhuma categoria for uma
            correspondência razoável (fica pra revisão manual depois).
        """
        if not self.openai_client:
            raise ValueError(
                "Azure OpenAI não foi configurado. "
                "Forneça azure_openai_endpoint, azure_openai_key e azure_openai_deployment."
            )

        lista_taxonomia = "\n".join(f"- {c}" for c in taxonomia)
        resultado: dict[str, Optional[str]] = {}

        for inicio in range(0, len(naturezas), tamanho_lote):
            lote = naturezas[inicio : inicio + tamanho_lote]
            lista_naturezas = "\n".join(f"{i}. {n}" for i, n in enumerate(lote))

            prompt = f"""Você é um assistente especializado em classificação de atos infracionais (infância e juventude) conforme a Tabela Processual Unificada do CNJ.

CATEGORIAS DE REFERÊNCIA (escolha sempre uma destas, exatamente como escrita, ou null se nenhuma for uma correspondência razoável):
{lista_taxonomia}

Para cada item da lista abaixo (textos livres extraídos de boletins de ocorrência, descrevendo o ato infracional), identifique a categoria de referência que melhor corresponde.

ITENS A CLASSIFICAR:
{lista_naturezas}

Retorne APENAS um JSON no formato {{"0": "categoria ou null", "1": "categoria ou null", ...}}, usando o índice de cada item como chave."""

            resposta = self.openai_client.chat.completions.create(
                model=self.azure_openai_deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "Você é um assistente especializado em classificação de atos infracionais. Retorne apenas JSON válido.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_completion_tokens=max_completion_tokens,
                response_format={"type": "json_object"},
            )

            try:
                mapa_indices = json.loads(resposta.choices[0].message.content)
            except Exception as e:
                print(f"Erro ao parsear classificação do lote {inicio}: {e}")
                mapa_indices = {}

            for i, natureza in enumerate(lote):
                categoria = mapa_indices.get(str(i))
                resultado[natureza] = categoria if categoria and categoria != "null" else None

        return resultado
