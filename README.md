# Vinea – Ponte MNI do TJSP e Análise de MPUs

`vinea` é um conjunto de ferramentas Python para análise de processos judiciais do TJSP, incluindo:

## 🔹 MNI Client (Consulta de Processos)

Wrapper e parser Python sobre a interface SOAP MNI do TJSP para você:

- baixar cabeçalhos de processo, listas de movimentações e metadados de documentos via `consultarProcesso`
- obter arquivos binários de documentos (PDF/OCR) pelos seus identificadores
- persistir as respostas em disco para ETL ou análises posteriores
- analisar os XMLs salvos (como arquivos `.xml` isolados, caminhos ABFSS ou diretórios de shards de texto do Spark) em DataFrames pandas (`dados básicos`, `partes`, `movimentos`, `documentos`)

## 🔹 MPU Extractor (Medidas Protetivas de Urgência)

Sistema completo de extração e análise de dados de processos de MPUs (Lei Maria da Penha):

- **Extração automática** de texto de PDFs com `pdfplumber`
- **Estruturação de dados** usando Azure OpenAI (GPT-4/3.5)
- **Geocodificação automática** de endereços (local dos fatos, residências)
- **100+ campos estruturados** seguindo questionário especializado
- **7 DataFrames pandas** para análise (principal, vítima, autor, relacionamento, episódio, histórico, localização)
- **Análise espacial** com coordenadas geográficas

**📚 [Documentação completa do MPU Extractor →](docs/mpu-extractor.md)**

### Início Rápido - MPU

```bash
# Testar extração de um PDF
uv run python scripts/teste_extracao_mpu.py

# Processar todos os PDFs
uv run python scripts/extrair_mpu.py
```

```python
from vinea.mpu_extraction import MPUExtractor
from vinea.mpu_parser import MPUParser

# Extrair dados de um PDF (com geocodificação automática)
extractor = MPUExtractor()
mpu_data = extractor.extract_from_pdf("mpus/file.pdf")

# Converter para DataFrames
parser = MPUParser()
df_principal = parser.mpu_para_df_principal(mpu_data)
df_localizacao = parser.mpu_para_df_localizacao(mpu_data)  # Endereços + coordenadas
```

---

## Pré‑requisitos

1. **Python 3.11+** com `uv`:
   ```bash
   # uv já instala e gerencia o ambiente virtual automaticamente
   uv sync
   ```

2. Configurar `.env` com credenciais:
   ```env
   # MNI TJSP (para consulta de processos)
   TJSPMNIUSUARIO=seu_usuario
   TJSPMNISENHA=sua_senha

   # Azure OpenAI (para extração de MPUs)
   AZURE_OPENAI_API_KEY=sua_chave
   AZURE_OPENAI_RESOURCE=seu_resource
   AZURE_OPENAI_IMPLEMENTACAO=nome_deployment
   AZURE_OPENAI_VERSAO_API=2024-02-01

   # Opcionais: Azure AI, SQL Server, etc.
   ```

3. **PySpark** (opcional): para salvar XMLs em shards de texto
4. **ABFSS** (opcional): para ler arquivos em Azure Data Lake Storage

## Exemplo de uso - MNI Client

```python
from vinea import MNIClient, MNIParser
from config import config

cfg = config["development"]()
cfg.create_directories()

# Cliente: pode receber uma SparkSession opcional; cria uma internamente se não passar.
client = MNIClient(
    usuario=cfg.TJSP_MNI_USUARIO,
    senha=cfg.TJSP_MNI_SENHA,
    # spark=my_spark_session  # opcional
)

# Parser leve: use use_spark=True para habilitar leitura Spark, shards TXT e caminhos ABFSS
parser = MNIParser(use_spark=True)
processo = "00000023120238260631"

# Baixa cabeçalho e movimentos
header_path = client.consultar_processo(processo, save_dir=str(cfg.DATA_BRONZE_DIR))
movimentos_path = client.baixar_movimentos(processo, save_dir=str(cfg.DATA_BRONZE_DIR))

# Lista documentos e extrai IDs válidos
lista_xml = client.listar_documentos(processo, save_dir=str(cfg.DATA_BRONZE_DIR))
doc_ids = parser.ler_lista_documentos(lista_xml).id_documento.dropna().tolist()

# Baixa documentos PDF/OCR pelos IDs extraídos
pdf_paths = client.baixar_documentos(
    numero_processo=processo,
    documentos_ids=doc_ids,
    save_dir=str(cfg.DATA_BRONZE_DIR / "pdfs"),
)
```

- O `MNIClient` salva XMLs usando Spark (`.coalesce(1).write.text`) quando uma sessão Spark estiver disponível.
- **Atenção ao formato de ID**: passe exatamente os valores de `id_documento` (incluindo o sufixo `-1`), caso contrário nenhum binário será retornado.

## Análise dos XMLs salvos

O `MNIParser` é um parser leve que pode usar Spark para ler arquivos e shards em local ou ABFSS. Ele aceita caminhos para:

- arquivo `.xml` isolado (local ou ABFSS)
- diretório de shards de texto Spark (`part-*.txt`, local ou ABFSS)

```python
# Parser leve com Spark (shards local/ABFSS); em ABFSS testa head, depois concatena shards em caso de diretório
parser = MNIParser(use_spark=True)
processo_df, partes_df = parser.extrair_dados_basicos_xml(header_path)
documentos_df = parser.ler_lista_documentos(lista_xml)
movimentos_df = parser.ler_movimentos(movimentos_path)
```

## Exemplo de uso - MPU Extractor

```python
from vinea.mpu_extraction import MPUExtractor, MPUBatchProcessor
from vinea.mpu_parser import MPUParser
from vinea.geocoding import MPUGeocoder

# === EXTRAÇÃO DE UM PDF ===
extractor = MPUExtractor()

# Extrai dados estruturados + geocodifica endereços automaticamente
mpu_data = extractor.extract_from_pdf(
    pdf_path="mpus/1500147-95.2024.8.26.0232.pdf",
    save_text=True,    # Salva texto extraído
    geocode=True       # Geocodifica endereços (padrão: True)
)

# Salvar em JSON
extractor.save_mpu_data(mpu_data, "output.json")

# Acessar dados
print(f"Vítima: {mpu_data.perfil_vitima.idade} anos, {mpu_data.perfil_vitima.raca_cor}")
print(f"Local: {mpu_data.identificacao_fato.local_fatos_latitude}, "
      f"{mpu_data.identificacao_fato.local_fatos_longitude}")

# === CONVERSÃO PARA DATAFRAMES ===
parser = MPUParser()

# 7 DataFrames especializados
df_principal = parser.mpu_para_df_principal(mpu_data)      # Dados principais + coordenadas
df_vitima = parser.mpu_para_df_vitima(mpu_data)            # Perfil da vítima
df_autor = parser.mpu_para_df_autor(mpu_data)              # Perfil do autor
df_relacionamento = parser.mpu_para_df_relacionamento(mpu_data)  # Dinâmica
df_episodio = parser.mpu_para_df_episodio(mpu_data)        # Episódio de violência
df_historico = parser.mpu_para_df_historico(mpu_data)      # Histórico
df_localizacao = parser.mpu_para_df_localizacao(mpu_data)  # Todos os endereços + lat/long

# === PROCESSAMENTO EM LOTE ===
processor = MPUBatchProcessor(extractor)
results = processor.process_directory(
    input_dir="mpus",
    output_dir="data/mpu_extracted",
    save_text=True,
    geocode=True
)

# Processar múltiplos JSONs
dfs = parser.processar_lote_json("data/mpu_extracted")
# Retorna dicionário com 7 DataFrames: principal, vitima, autor, relacionamento,
# episodio, historico, localizacao

# === GEOCODIFICAÇÃO MANUAL ===
geocoder = MPUGeocoder()
geocoder.geocode_mpu_addresses(mpu_data)  # Geocodifica todos os endereços
```

## Scripts

### MNI Client
- `scripts/rotina1_listar_documentos.py`: itera sobre resultado de consulta SQL, chama `listar_documentos` e armazena XMLs em `data/bronze`.
- `scripts/rotina_2_ler_lista_documentos.py`: lê cada arquivo em `data/bronze`, executa `MNIParser.ler_lista_documentos` e concatena DataFrames.
- `scripts/rotina_3_ler_mni.py`: exemplo simples de chamada a `consultar_processo` seguido de parsing do cabeçalho.

### MPU Extractor
- `scripts/teste_azure_openai.py`: testa conexão com Azure OpenAI
- `scripts/teste_extracao_mpu.py`: testa extração completa de um PDF (texto + LLM + geocoding)
- `scripts/extrair_mpu.py`: processamento em lote de todos os PDFs

## Módulos Principais

### MNI Client
- `src/vinea/consulta.py` - Cliente SOAP MNI (`MNIClient`)
- `src/vinea/leitura.py` - Parser de XMLs (`MNIParser`)

### MPU Extractor
- `src/vinea/mpu_models.py` - Modelos Pydantic (100+ campos estruturados)
- `src/vinea/mpu_extraction.py` - Extração de PDFs com Azure OpenAI (`MPUExtractor`, `MPUBatchProcessor`)
- `src/vinea/mpu_parser.py` - Conversão para DataFrames (`MPUParser`)
- `src/vinea/geocoding.py` - Geocodificação de endereços (`Geocoder`, `MPUGeocoder`)

### Configuração
- `config.py` - Centraliza caminhos, credenciais e configurações

## Estrutura de Diretórios

```
vinea/
├── src/vinea/           # Código fonte
│   ├── consulta.py      # MNI Client
│   ├── leitura.py       # MNI Parser
│   ├── mpu_models.py    # Modelos MPU
│   ├── mpu_extraction.py # Extrator MPU
│   ├── mpu_parser.py    # Parser MPU
│   └── geocoding.py     # Geocodificação
├── scripts/             # Scripts de exemplo
├── mpus/                # PDFs de MPUs + README detalhado
├── data/                # Dados extraídos
│   ├── bronze/          # XMLs do MNI
│   └── mpu_extracted/   # JSONs/CSVs das MPUs
├── config.py            # Configurações
└── README.md            # Este arquivo
```

## Configuração

`config.py` centraliza caminhos de diretórios, strings de conexão e as variáveis `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON`. Também expõe `config['development']()` etc. e o helper `create_directories()`.

## Desenvolvimento

1. Instalar dependências de desenvolvimento:
   ```bash
   uv sync
   ```

2. Rodar testes:
   ```bash
   # Testar MNI
   uv run python scripts/rotina_3_ler_mni.py

   # Testar MPU
   uv run python scripts/teste_azure_openai.py
   uv run python scripts/teste_extracao_mpu.py
   ```

3. Verificar sintaxe:
   ```bash
   python -m py_compile src/vinea/*.py
   ```

4. Manter credenciais no `.env` fora do controle de versão.

5. Se precisar de Spark, garanta que cada worker aponte para o mesmo binário Python (o `.venv/bin/python3` usado acima).

## Documentação

- **[MPU Extractor - Documentação Completa](docs/mpu-extractor.md)** - Guia detalhado do sistema de extração de MPUs
  - Modelos de dados
  - API completa
  - Exemplos de análise espacial
  - Geocodificação
  - Performance e manutenção

## Contribuição

1. Abra uma issue descrevendo o problema ou feature.
2. Envie um pull request seguindo padrões de importação/estilo e adicione testes simples para helpers do parser se possível.
3. Mantemos dependências mínimas (pandas, zeep, lxml; pyspark opcional); considere isso ao adicionar pacotes.

## Dependências

### Core
- `pandas>=2.3.3` - Manipulação de dados
- `zeep[xmlsec]>=4.3.1` - Cliente SOAP para MNI
- `lxml>=6.0.0` - Parsing de XML
- `python-dotenv>=1.1.1` - Variáveis de ambiente

### MPU Extractor
- `openai>=1.0.0` - Cliente Azure OpenAI
- `pydantic>=2.0.0` - Validação de dados
- `pdfplumber>=0.11.0` - Extração de texto de PDFs
- `requests>=2.32.4` - Requisições HTTP (geocodificação)

### Opcionais
- `pyspark>=4.0.1` - Processamento distribuído
- `pyodbc>=5.2.0` - Conexão SQL Server
- `sqlalchemy>=2.0.43` - ORM
- `duckdb>=1.3.2` - Banco de dados analítico

## Licença

[Adicionar licença conforme necessário]

---

**Desenvolvido para análise de processos judiciais do TJSP**
