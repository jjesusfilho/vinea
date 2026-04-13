"""
Módulo para extração de informações de MPUs (Medidas Protetivas de Urgência).

Este módulo utiliza pdfplumber para processar PDFs e Azure OpenAI para
extrair informações estruturadas de processos de medidas protetivas.
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

from .mpu_models import MPUData, IdentificacaoProcesso


class MPUExtractor:
    """
    Extrator de informações de MPUs a partir de documentos PDF.

    Utiliza pdfplumber para extração de texto e Azure OpenAI para
    estruturação dos dados conforme o modelo MPUData.
    """

    def __init__(
        self,
        azure_openai_endpoint: Optional[str] = None,
        azure_openai_key: Optional[str] = None,
        azure_openai_deployment: Optional[str] = None,
        azure_openai_api_version: Optional[str] = None,
    ):
        """
        Inicializa o extrator de MPUs.

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

        # Azure OpenAI
        self.azure_openai_endpoint = azure_openai_endpoint or os.getenv("AZURE_OPENAI_RESOURCE")
        self.azure_openai_key = azure_openai_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.azure_openai_deployment = azure_openai_deployment or os.getenv("AZURE_OPENAI_IMPLEMENTACAO")
        self.azure_openai_api_version = azure_openai_api_version or os.getenv("AZURE_OPENAI_VERSAO_API", "2024-02-01")

        if self.azure_openai_endpoint and self.azure_openai_key:
            # Ensure endpoint has proper format
            if not self.azure_openai_endpoint.startswith("https://"):
                self.azure_openai_endpoint = f"https://{self.azure_openai_endpoint}.openai.azure.com"

            self.openai_client = AzureOpenAI(
                api_key=self.azure_openai_key,
                api_version=self.azure_openai_api_version,
                azure_endpoint=self.azure_openai_endpoint
            )
        else:
            self.openai_client = None

    def extract_text_from_pdf(self, pdf_path: Union[str, Path]) -> str:
        """
        Extrai texto de um PDF usando pdfplumber.

        Args:
            pdf_path: Caminho para o arquivo PDF

        Returns:
            Texto extraído do documento
        """
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

    def extract_mpu_data_with_llm(
        self,
        text_content: str,
        numero_processo: str,
        max_completion_tokens: int = 4000,
        temperature: float = 0.1
    ) -> MPUData:
        """
        Extrai dados estruturados de MPU usando LLM.

        Args:
            text_content: Texto extraído do documento
            numero_processo: Número do processo
            max_completion_tokens: Número máximo de tokens na resposta
            temperature: Temperatura para geração (menor = mais determinístico)

        Returns:
            Objeto MPUData com os dados extraídos
        """
        if not self.openai_client:
            raise ValueError(
                "Azure OpenAI não foi configurado. "
                "Forneça azure_openai_endpoint, azure_openai_key e azure_openai_deployment."
            )

        # Get the JSON schema for MPUData
        schema = MPUData.model_json_schema()

        prompt = f"""Você é um assistente especializado em análise de processos judiciais de Medidas Protetivas de Urgência (MPU).

Analise o texto do processo judicial abaixo e extraia todas as informações relevantes conforme o schema JSON fornecido.

INSTRUÇÕES:
1. Extraia TODAS as informações presentes no texto que correspondam aos campos do schema
2. Para campos não encontrados, use null
3. Para datas, use o formato ISO (YYYY-MM-DD)
4. Para campos booleanos, use true/false baseado no que está explícito ou implícito no texto
5. Para enums, use exatamente os valores definidos no schema
6. Seja preciso e fiel ao conteúdo do documento
7. Retorne APENAS o JSON válido, sem texto adicional

SCHEMA JSON:
{json.dumps(schema, indent=2, ensure_ascii=False)}

TEXTO DO PROCESSO:
{text_content[:50000]}  # Limita o texto para não exceder tokens

Retorne o JSON estruturado com os dados extraídos:"""

        response = self.openai_client.chat.completions.create(
            model=self.azure_openai_deployment,
            messages=[
                {
                    "role": "system",
                    "content": "Você é um assistente especializado em extração de dados de processos judiciais. Retorne apenas JSON válido."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=temperature,
            max_completion_tokens=max_completion_tokens,  # Updated parameter for newer models
            response_format={"type": "json_object"}
        )

        # Parse the JSON response
        json_str = response.choices[0].message.content

        try:
            data_dict = json.loads(json_str)
            # Ensure numero_processo is set
            if "identificacao_processo" not in data_dict:
                data_dict["identificacao_processo"] = {}
            data_dict["identificacao_processo"]["numero_processo"] = numero_processo

            return MPUData(**data_dict)
        except Exception as e:
            # If parsing fails, create minimal valid object
            print(f"Erro ao parsear resposta do LLM: {e}")
            print(f"Resposta: {json_str}")
            return MPUData(
                identificacao_processo=IdentificacaoProcesso(numero_processo=numero_processo)
            )

    def extract_from_pdf(
        self,
        pdf_path: Union[str, Path],
        numero_processo: Optional[str] = None,
        save_text: bool = False,
        text_output_path: Optional[Union[str, Path]] = None,
        geocode: bool = True
    ) -> MPUData:
        """
        Extrai dados de MPU diretamente de um PDF.

        Args:
            pdf_path: Caminho para o arquivo PDF
            numero_processo: Número do processo (se None, tenta extrair do nome do arquivo)
            save_text: Se True, salva o texto extraído
            text_output_path: Caminho para salvar o texto extraído
            geocode: Se True, geocodifica os endereços

        Returns:
            Objeto MPUData com os dados extraídos
        """
        pdf_path = Path(pdf_path)

        # Extract numero_processo from filename if not provided
        if not numero_processo:
            # Try to extract from filename (e.g., "0000399-66.2018.8.26.0594.pdf")
            numero_processo = pdf_path.stem.replace("-", "").replace(".", "")
            if len(numero_processo) > 20:
                # Keep only first 20 digits
                numero_processo = numero_processo[:20]

        print(f"Extraindo texto do PDF: {pdf_path.name}")
        text_content = self.extract_text_from_pdf(pdf_path)

        if save_text:
            if text_output_path is None:
                text_output_path = pdf_path.with_suffix(".txt")
            else:
                text_output_path = Path(text_output_path)

            with open(text_output_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            print(f"Texto salvo em: {text_output_path}")

        print(f"Extraindo dados estruturados com LLM...")
        mpu_data = self.extract_mpu_data_with_llm(text_content, numero_processo)
        mpu_data.arquivo_fonte = str(pdf_path)

        # Geocodifica endereços se solicitado
        if geocode and mpu_data.identificacao_fato:
            print("Geocodificando endereços...")
            from .geocoding import MPUGeocoder
            geocoder = MPUGeocoder()
            geocoder.geocode_mpu_addresses(mpu_data)

        return mpu_data

    def extract_from_text(
        self,
        text_path: Union[str, Path],
        numero_processo: str
    ) -> MPUData:
        """
        Extrai dados de MPU de um arquivo de texto já processado.

        Args:
            text_path: Caminho para o arquivo de texto
            numero_processo: Número do processo

        Returns:
            Objeto MPUData com os dados extraídos
        """
        text_path = Path(text_path)
        with open(text_path, "r", encoding="utf-8") as f:
            text_content = f.read()

        print(f"Extraindo dados estruturados de texto: {text_path.name}")
        mpu_data = self.extract_mpu_data_with_llm(text_content, numero_processo)
        mpu_data.arquivo_fonte = str(text_path)

        return mpu_data

    def save_mpu_data(
        self,
        mpu_data: MPUData,
        output_path: Union[str, Path],
        format: str = "json"
    ):
        """
        Salva os dados extraídos da MPU.

        Args:
            mpu_data: Dados da MPU
            output_path: Caminho para salvar
            format: Formato de saída ("json" ou "dict")
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(mpu_data.model_dump_json(indent=2, exclude_none=False))
        elif format == "dict":
            data_dict = mpu_data.model_dump(exclude_none=False)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data_dict, f, indent=2, ensure_ascii=False, default=str)
        else:
            raise ValueError(f"Formato não suportado: {format}")

        print(f"Dados da MPU salvos em: {output_path}")


class MPUBatchProcessor:
    """Processador em lote de múltiplos documentos de MPU."""

    def __init__(self, extractor: MPUExtractor):
        """
        Inicializa o processador em lote.

        Args:
            extractor: Instância de MPUExtractor
        """
        self.extractor = extractor

    def process_directory(
        self,
        input_dir: Union[str, Path],
        output_dir: Union[str, Path],
        file_pattern: str = "*.pdf",
        save_text: bool = False,
        geocode: bool = True
    ) -> list[MPUData]:
        """
        Processa todos os PDFs em um diretório.

        Args:
            input_dir: Diretório com os PDFs
            output_dir: Diretório para salvar os resultados
            file_pattern: Padrão de arquivos (e.g., "*.pdf")
            save_text: Se True, salva também o texto extraído
            geocode: Se True, geocodifica os endereços

        Returns:
            Lista de objetos MPUData extraídos
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        pdf_files = list(input_dir.glob(file_pattern))
        print(f"Encontrados {len(pdf_files)} arquivos para processar")

        results = []
        for i, pdf_path in enumerate(pdf_files, 1):
            print(f"\n[{i}/{len(pdf_files)}] Processando: {pdf_path.name}")

            try:
                mpu_data = self.extractor.extract_from_pdf(
                    pdf_path=pdf_path,
                    save_text=save_text,
                    text_output_path=output_dir / f"{pdf_path.stem}.txt" if save_text else None,
                    geocode=geocode
                )

                # Save extracted data
                json_output = output_dir / f"{pdf_path.stem}.json"
                self.extractor.save_mpu_data(mpu_data, json_output)

                results.append(mpu_data)
                print(f"✓ Processado com sucesso: {pdf_path.name}")

            except Exception as e:
                print(f"✗ Erro ao processar {pdf_path.name}: {e}")
                continue

        print(f"\n✓ Processamento concluído: {len(results)}/{len(pdf_files)} arquivos")
        return results
