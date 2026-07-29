# Vinea – Ponte MNI do TJSP

`vinea` é um conjunto de ferramentas Python para análise de processos judiciais do TJSP, incluindo:

## 🔹 MNI Client (Consulta de Processos)

Wrapper e parser Python sobre a interface SOAP MNI do TJSP para você:

- **Suporta múltiplos sistemas**: E-SAJ, E-Proc 1G (1ª Instância) e E-Proc 2G (2ª Instância)
- baixar cabeçalhos de processo, listas de movimentações e metadados de documentos via `consultarProcesso`
- obter arquivos binários de documentos (PDF/OCR) pelos seus identificadores
- **Geração automática de senha E-Proc** usando SHA-256 com data atual
- persistir as respostas em disco para ETL ou análises posteriores
- analisar os XMLs salvos (como arquivos `.xml` isolados, caminhos ABFSS ou diretórios de shards de texto do Spark) em DataFrames pandas (`dados básicos`, `partes`, `movimentos`, `documentos`)
- funciona com ou sem PySpark (modo simplificado disponível)

---

## Pré‑requisitos

1. **Python 3.11+** com `uv`:
   ```bash
   # uv já instala e gerencia o ambiente virtual automaticamente
   uv sync
   ```

2. Configurar `.env` com credenciais (veja [.env.example](.env.example)):
   ```env
   # MNI TJSP E-SAJ (sistema tradicional)
   TJSPMNIUSUARIO=seu_usuario
   TJSPMNISENHA=sua_senha

   # E-Proc (1G e 2G)
   EPROC_USUARIO=seu_usuario
   EPROC_PASSWORD_SECRET=seu_segredo  # Segredo para geração de senha SHA-256

   # Opcionais: Azure AI, SQL Server, etc.
   ```

3. **PySpark** (opcional): para salvar XMLs em shards de texto
4. **ABFSS** (opcional): para ler arquivos em Azure Data Lake Storage

## Exemplo de uso - MNI Client

### E-SAJ (Sistema Tradicional)

```python
from vinea import MNIClient, MNIParser
from config import config

cfg = config["development"]()
cfg.create_directories()

# Cliente E-SAJ: pode receber uma SparkSession opcional; cria uma internamente se não passar.
client = MNIClient(
    usuario=cfg.TJSP_MNI_USUARIO,
    senha=cfg.TJSP_MNI_SENHA,
    system="esaj",  # Sistema padrão
    use_spark=True  # Se False, salva arquivos sem Spark
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

### E-Proc (1ª e 2ª Instâncias)

```python
from vinea import create_eproc1g_client, create_eproc2g_client, generate_eproc_password
from config import config

cfg = config["development"]()

# === E-Proc 1G (1ª Instância) ===
# A senha é gerada automaticamente usando SHA-256(DD-MM-AAAA + secret)
client_1g = create_eproc1g_client(
    usuario=cfg.EPROC_USUARIO,  # CAO_CAEx_Consulta_MP
    version="2.2",  # ou "3.0"
    use_spark=False  # Modo simplificado sem Spark
    # senha será gerada automaticamente do .env
)

# === E-Proc 2G (2ª Instância) ===
client_2g = create_eproc2g_client(
    usuario=cfg.EPROC_USUARIO,
    use_spark=False
)

# === Geração manual de senha (se necessário) ===
from datetime import datetime
senha_hoje = generate_eproc_password()  # Usa secret do .env
senha_data = generate_eproc_password(date=datetime(2026, 4, 13))

# Uso é idêntico ao E-SAJ
processo = "4000634-60.2025.8.26.0483"
header_path = client_1g.consultar_processo(processo, save_dir="data/bronze/eproc1g")
movimentos_path = client_1g.baixar_movimentos(processo, save_dir="data/bronze/eproc1g")
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

## Scripts

### MNI Client
- `scripts/rotina1_listar_documentos.py`: itera sobre resultado de consulta SQL, chama `listar_documentos` e armazena XMLs em `data/bronze`.
- `scripts/rotina_2_ler_lista_documentos.py`: lê cada arquivo em `data/bronze`, executa `MNIParser.ler_lista_documentos` e concatena DataFrames.
- `scripts/rotina_3_ler_mni.py`: exemplo simples de chamada a `consultar_processo` seguido de parsing do cabeçalho.

## Módulos Principais

### MNI Client
- `src/vinea/consulta.py` - Cliente SOAP MNI (`MNIClient`)
- `src/vinea/leitura.py` - Parser de XMLs (`MNIParser`)

### Configuração
- `config.py` - Centraliza caminhos, credenciais e configurações

## Estrutura de Diretórios

```
vinea/
├── src/vinea/           # Código fonte
│   ├── consulta.py      # MNI Client
│   └── leitura.py       # MNI Parser
├── scripts/             # Scripts de exemplo
├── data/                # Dados extraídos
│   └── bronze/          # XMLs do MNI
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
   ```

3. Verificar sintaxe:
   ```bash
   python -m py_compile src/vinea/*.py
   ```

4. Manter credenciais no `.env` fora do controle de versão.

5. Se precisar de Spark, garanta que cada worker aponte para o mesmo binário Python (o `.venv/bin/python3` usado acima).

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

### Opcionais
- `pyspark>=4.0.1` - Processamento distribuído
- `pyodbc>=5.2.0` - Conexão SQL Server
- `sqlalchemy>=2.0.43` - ORM
- `duckdb>=1.3.2` - Banco de dados analítico

## Licença

[Adicionar licença conforme necessário]

---

**Desenvolvido para análise de processos judiciais do TJSP**
