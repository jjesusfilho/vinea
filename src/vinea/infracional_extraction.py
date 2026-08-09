"""
Módulo para extração de informações de processos de ato infracional a
partir do Boletim de Ocorrência e dos documentos de qualificação das
partes, usando Azure OpenAI.
"""

import json
import os
from pathlib import Path
from typing import Optional, Union

from openai import AzureOpenAI

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

from .infracional_models import ProcessoInfracionalData, IdentificacaoProcesso


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
            if not self.azure_openai_endpoint.startswith("https://"):
                self.azure_openai_endpoint = f"https://{self.azure_openai_endpoint}.openai.azure.com"

            self.openai_client = AzureOpenAI(
                api_key=self.azure_openai_key,
                api_version=self.azure_openai_api_version,
                azure_endpoint=self.azure_openai_endpoint,
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
        temperature: float = 0.1,
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
            temperature: Temperatura para geração (menor = mais determinístico)

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

        response = self.openai_client.chat.completions.create(
            model=self.azure_openai_deployment,
            messages=[
                {
                    "role": "system",
                    "content": "Você é um assistente especializado em extração de dados de processos judiciais. Retorne apenas JSON válido.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=temperature,
            max_completion_tokens=max_completion_tokens,
            response_format={"type": "json_object"},
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
