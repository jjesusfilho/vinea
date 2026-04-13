# Extração de Dados de MPUs (Medidas Protetivas de Urgência)

Este diretório contém PDFs de processos de Medidas Protetivas de Urgência (Lei Maria da Penha) e ferramentas para extrair dados estruturados desses documentos usando Azure OpenAI.

## 📋 Visão Geral

O sistema extrai automaticamente informações de processos judiciais de MPUs, estruturando os dados conforme o questionário definido em [Perguntas pedido MPU.md](Perguntas%20pedido%20MPU.md).

### Dados Extraídos

O sistema extrai mais de 100 campos organizados em categorias:

1. **Identificação do Processo**
   - Número do processo, comarca, vara, classe processual

2. **Identificação do Fato**
   - Local dos fatos, residências, data e período
   - **🗺️ Coordenadas geográficas (latitude/longitude)** de todos os endereços

3. **Perfil Sociodemográfico da Vítima**
   - Idade, raça/cor, escolaridade, orientação sexual, identidade de gênero
   - Informações complementares (gravidez, deficiência, autonomia financeira, etc.)

4. **Perfil Sociodemográfico do Autor da Violência**
   - Dados demográficos similares à vítima
   - Situação de emprego, dependência química, acesso a armas

5. **Dinâmica do Relacionamento**
   - Tipo e tempo de relacionamento, filhos, conflitos
   - Comportamentos de controle (ciúmes, monitoramento digital, isolamento social)

6. **Caracterização do Episódio**
   - Naturezas da violência (física, psicológica, sexual, etc.)
   - Gravidade objetiva (armas, estrangulamento, lesões)
   - Contexto (local, testemunhas, prisão em flagrante)

7. **Histórico de Escalada e Reincidência**
   - Agressões anteriores, descumprimento de protetivas
   - Ameaças de morte, violência contra familiares

8. **Informações do Boletim de Ocorrência**
   - Tipo de delegacia, relatório de risco

9. **Trâmite Processual**
   - Peça inaugural, manifestações do MP, decisões

10. **Linha do Tempo**
    - Datas do fato, BO, abertura, deferimento, intimações

11. **Outras Demandas**
    - Representação criminal, processos de família

## 🗺️ Geocodificação Automática

O sistema inclui **geocodificação automática** de endereços usando OpenStreetMap Nominatim:

- **Bounding box baseado no município** - Restringe a busca ao município do processo
- **Geocodifica 3 endereços**:
  - Local dos fatos (onde ocorreu a violência)
  - Residência da vítima
  - Residência do autor da violência
- **Estratégia inteligente de fallback**:
  1. Tenta endereço completo (rua + bairro + município)
  2. Se falhar, tenta CEP
  3. Se falhar, usa centro do município
- **Sem necessidade de API key** - Usa serviço gratuito do OpenStreetMap
- **Respeita rate limits** - 1 requisição por segundo

### Exemplo de Coordenadas

```
Local dos fatos:      -23.2232976, -47.9544546 (Cesário Lange, SP)
Residência vítima:    -23.2232976, -47.9544546 (mesma rua)
Residência autor:     -23.2205861, -47.9640196 (bairro próximo)
```

## 🚀 Como Usar

### 1. Testar com um único PDF

```bash
uv run python scripts/teste_extracao_mpu.py
```

Este script:
- Processa o arquivo `1500147-95.2024.8.26.0232.pdf`
- Extrai texto com `pdfplumber`
- Envia para Azure OpenAI para estruturação
- **Geocodifica automaticamente os endereços**
- Mostra os dados extraídos na tela

**Saída:** `data/teste_mpu.json` (com coordenadas incluídas)

### 2. Processar todos os PDFs em lote

```bash
uv run python scripts/extrair_mpu.py
```

Este script:
- Processa todos os PDFs no diretório `mpus/`
- Geocodifica todos os endereços encontrados
- Salva os dados extraídos em JSON em `data/mpu_extracted/`
- Gera DataFrames pandas e exporta para CSV (incluindo CSV de localização)
- Salva também o texto extraído (`.txt`)

**Saídas:**
- `data/mpu_extracted/*.json` - Dados estruturados de cada processo
- `data/mpu_extracted/*.txt` - Texto extraído dos PDFs
- `data/mpu_extracted/csv/*.csv` - DataFrames em CSV para análise
  - `principal.csv` - Dados principais (inclui coordenadas do local dos fatos)
  - `vitima.csv` - Perfil das vítimas
  - `autor.csv` - Perfil dos autores
  - `relacionamento.csv` - Dinâmica do relacionamento
  - `episodio.csv` - Caracterização dos episódios
  - `historico.csv` - Histórico de escalada
  - **`localizacao.csv`** - Todos os endereços e coordenadas geográficas

### 3. Usar programaticamente

#### Extração com Geocodificação

```python
from vinea.mpu_extraction import MPUExtractor
from vinea.mpu_parser import MPUParser

# Inicializar extrator (credenciais do .env)
extractor = MPUExtractor()

# Extrair dados de um PDF (geocodificação automática)
mpu_data = extractor.extract_from_pdf(
    pdf_path="mpus/1500147-95.2024.8.26.0232.pdf",
    save_text=True,
    geocode=True  # Default: True
)

# Acessar coordenadas
if mpu_data.identificacao_fato:
    print(f"Local: {mpu_data.identificacao_fato.local_fatos_latitude}, "
          f"{mpu_data.identificacao_fato.local_fatos_longitude}")

# Salvar resultado
extractor.save_mpu_data(mpu_data, "output.json")
```

#### Geocodificação Manual

```python
from vinea.geocoding import MPUGeocoder

# Geocodificar endereços manualmente
geocoder = MPUGeocoder()
geocoder.geocode_mpu_addresses(mpu_data)
```

#### Desabilitar Geocodificação

```python
# Se você não quiser geocodificar
mpu_data = extractor.extract_from_pdf(
    pdf_path="mpus/file.pdf",
    geocode=False
)
```

#### Converter para DataFrames

```python
# Converter para DataFrame
parser = MPUParser()

# DataFrame principal (inclui coordenadas do local dos fatos)
df_principal = parser.mpu_para_df_principal(mpu_data)
print(df_principal[['numero_processo', 'local_fatos_latitude', 'local_fatos_longitude']])

# DataFrame específico de localização (todos os endereços e coordenadas)
df_localizacao = parser.mpu_para_df_localizacao(mpu_data)
print(df_localizacao.columns)
# ['numero_processo', 'local_fatos_rua', 'local_fatos_bairro',
#  'local_fatos_cidade', 'local_fatos_cep',
#  'local_fatos_latitude', 'local_fatos_longitude',
#  'residencia_vitima_rua', ..., 'residencia_vitima_latitude', 'residencia_vitima_longitude',
#  'residencia_autor_rua', ..., 'residencia_autor_latitude', 'residencia_autor_longitude',
#  'dia_mes', 'dia_semana', 'periodo_dia', 'data_bo']

# Outros DataFrames
df_vitima = parser.mpu_para_df_vitima(mpu_data)
df_autor = parser.mpu_para_df_autor(mpu_data)
df_relacionamento = parser.mpu_para_df_relacionamento(mpu_data)
df_episodio = parser.mpu_para_df_episodio(mpu_data)
df_historico = parser.mpu_para_df_historico(mpu_data)
```

#### Processamento em Lote

```python
# Processar múltiplos JSONs e gerar DataFrames
dfs = parser.processar_lote_json("data/mpu_extracted")

# Acessar DataFrames
df_principal = dfs["principal"]      # Dados principais + coordenadas
df_vitima = dfs["vitima"]            # Perfil das vítimas
df_autor = dfs["autor"]              # Perfil dos autores
df_relacionamento = dfs["relacionamento"]  # Dinâmica do relacionamento
df_episodio = dfs["episodio"]        # Caracterização dos episódios
df_historico = dfs["historico"]      # Histórico de escalada
df_localizacao = dfs["localizacao"]  # Todos os endereços e coordenadas

# Salvar em CSV
for nome, df in dfs.items():
    df.to_csv(f"{nome}.csv", index=False)
```

## 📊 Estrutura dos Dados

### Modelos Pydantic

Os dados são estruturados usando modelos Pydantic definidos em `src/vinea/mpu_models.py`:

- `MPUData` - Modelo principal que contém todos os dados
- `IdentificacaoProcesso` - Dados do processo
- `IdentificacaoFato` - Dados do fato (inclui lat/long de 3 endereços)
- `PerfilSociodemograficoVitima` - Perfil da vítima
- `PerfilSociodemograficoAutor` - Perfil do autor
- `DinamicaRelacionamento` - Dados do relacionamento
- `CaracterizacaoEpisodio` - Dados do episódio de violência
- `HistoricoEscalada` - Histórico de violência
- `InformacoesBO` - Dados do boletim de ocorrência
- `TramiteProcessual` - Trâmite judicial
- `LinhaDoTempo` - Datas importantes
- `OutrasDemandas` - Outras demandas judiciais

### Formato JSON

Exemplo de estrutura do JSON gerado (com geocodificação):

```json
{
  "identificacao_processo": {
    "numero_processo": "15001479520248260232",
    "comarca": "Cesário Lange",
    "vara": "Vara única",
    "classe_processual": "Medidas Protetivas de Urgência"
  },
  "identificacao_fato": {
    "local_fatos_rua": "Rua Vereador Francisco Mendes Castanho",
    "local_fatos_bairro": "Don Lázaro",
    "local_fatos_cidade": "Cesário Lange",
    "local_fatos_cep": "18285000",
    "local_fatos_latitude": -23.2232976,
    "local_fatos_longitude": -47.9544546,

    "residencia_vitima_rua": "Rua Vereador Francisco Mendes Castanho",
    "residencia_vitima_latitude": -23.2232976,
    "residencia_vitima_longitude": -47.9544546,

    "residencia_autor_rua": "Rua Benedito Miranda da Silva",
    "residencia_autor_latitude": -23.2205861,
    "residencia_autor_longitude": -47.9640196
  },
  "perfil_vitima": {
    "idade": 27,
    "raca_cor": "branca",
    "escolaridade": "médio completo",
    "informacoes_complementares": {
      "dependencia_economica": false,
      "rede_apoio": true
    }
  },
  "dinamica_relacionamento": {
    "tipo_relacionamento": "ex-cônjuge/ex-companheiro(a)",
    "filhos_comum": true,
    "conflitos_guarda": true
  },
  "linha_tempo": {
    "data_fato": "2024-08-06",
    "data_bo": "2024-08-07"
  }
}
```

## 🔧 Configuração

### Pré-requisitos

1. **Python 3.11+** com `uv` instalado
2. **Azure OpenAI** com deployment configurado

### Variáveis de Ambiente

Configure as seguintes variáveis no arquivo `.env`:

```env
# Azure OpenAI
AZURE_OPENAI_API_KEY=sua_chave_aqui
AZURE_OPENAI_RESOURCE=seu_resource_name
AZURE_OPENAI_IMPLEMENTACAO=nome_do_deployment
AZURE_OPENAI_VERSAO_API=2024-02-01
```

### Dependências

As dependências já estão no `pyproject.toml`:

- `openai>=1.0.0` - Cliente Azure OpenAI
- `pydantic>=2.0.0` - Validação de dados
- `pdfplumber>=0.11.0` - Extração de texto de PDFs
- `pandas>=2.3.3` - Análise de dados
- `requests>=2.32.4` - Requisições HTTP (para geocodificação)

## 📈 Análise de Dados

### Análise Espacial com Coordenadas

```python
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Carregar dados com coordenadas
df_loc = pd.read_csv("data/mpu_extracted/csv/localizacao.csv")

# Converter para GeoDataFrame
geometry_fatos = [Point(xy) for xy in zip(df_loc.local_fatos_longitude,
                                            df_loc.local_fatos_latitude)]
gdf = gpd.GeoDataFrame(df_loc, geometry=geometry_fatos, crs="EPSG:4326")

# Análise de distância entre vítima e autor
from geopy.distance import geodesic

def calcular_distancia(row):
    if pd.notna(row['residencia_vitima_latitude']) and pd.notna(row['residencia_autor_latitude']):
        vitima = (row['residencia_vitima_latitude'], row['residencia_vitima_longitude'])
        autor = (row['residencia_autor_latitude'], row['residencia_autor_longitude'])
        return geodesic(vitima, autor).kilometers
    return None

df_loc['distancia_residencias_km'] = df_loc.apply(calcular_distancia, axis=1)
print(df_loc['distancia_residencias_km'].describe())
```

### Análise Estatística

```python
# Processar lote de JSONs
parser = MPUParser()
dfs = parser.processar_lote_json("data/mpu_extracted")

# Acessar DataFrames
df_vitima = dfs["vitima"]
df_episodio = dfs["episodio"]
df_loc = dfs["localizacao"]

# Análises demográficas
print("Idade média das vítimas:", df_vitima["idade"].mean())
print("\nDistribuição por raça/cor:")
print(df_vitima["raca_cor"].value_counts())

# Análise de violência
print("\nTipos de violência mais comuns:")
print(df_episodio["naturezas_violencia"].value_counts())

# Análise espacial
print("\nMunicípios com mais casos:")
print(df_loc["local_fatos_cidade"].value_counts())

# Casos com geocodificação bem-sucedida
casos_geocodificados = df_loc['local_fatos_latitude'].notna().sum()
print(f"\nCasos geocodificados: {casos_geocodificados}/{len(df_loc)}")
```

## 📍 Módulos do Sistema

### 1. `src/vinea/mpu_models.py`
Modelos de dados Pydantic para validação e estruturação.

**Classes principais:**
- `MPUData` - Modelo raiz
- `IdentificacaoFato` - Inclui campos de latitude/longitude
- `PerfilSociodemograficoVitima`
- `PerfilSociodemograficoAutor`
- `DinamicaRelacionamento`
- `CaracterizacaoEpisodio`
- Diversos Enums para valores padronizados

### 2. `src/vinea/mpu_extraction.py`
Extração de dados de PDFs usando Azure OpenAI.

**Classes:**
- `MPUExtractor` - Extrator principal
  - `extract_text_from_pdf()` - Extrai texto com pdfplumber
  - `extract_mpu_data_with_llm()` - Estrutura dados com LLM
  - `extract_from_pdf()` - Pipeline completo (texto + LLM + geocoding)
  - `save_mpu_data()` - Salva em JSON

- `MPUBatchProcessor` - Processamento em lote
  - `process_directory()` - Processa múltiplos PDFs

### 3. `src/vinea/mpu_parser.py`
Conversão de dados extraídos para DataFrames pandas.

**Classe:**
- `MPUParser`
  - `ler_mpu_json()` - Carrega JSON para objeto MPUData
  - `mpu_para_df_principal()` - DataFrame principal (inclui coordenadas do local)
  - `mpu_para_df_vitima()` - DataFrame de vítimas
  - `mpu_para_df_autor()` - DataFrame de autores
  - `mpu_para_df_relacionamento()` - DataFrame de relacionamento
  - `mpu_para_df_episodio()` - DataFrame de episódios
  - `mpu_para_df_historico()` - DataFrame de histórico
  - **`mpu_para_df_localizacao()`** - DataFrame de localização (todos os endereços + coordenadas)
  - `processar_lote_json()` - Processa múltiplos JSONs e retorna 7 DataFrames

### 4. `src/vinea/geocoding.py` ⭐ NOVO
Geocodificação de endereços usando OpenStreetMap Nominatim.

**Classes:**
- `Geocoder` - Geocodificador genérico
  - `geocode_address()` - Geocodifica endereço completo
  - `geocode_cep()` - Geocodifica por CEP
  - `_get_municipality_bbox()` - Obtém bounding box do município

- `MPUGeocoder` - Geocodificador especializado para MPUs
  - `geocode_mpu_addresses()` - Geocodifica todos os endereços de um MPUData
  - Usa bounding box do município para maior precisão
  - Fallback inteligente: endereço → CEP → município

**Características:**
- Respeita rate limit de 1 req/segundo
- Sem necessidade de API key
- Bounding box baseado no município
- Tratamento de erros robusto

### 5. Scripts

**`scripts/teste_azure_openai.py`**
- Testa conexão com Azure OpenAI
- Verifica credenciais do `.env`

**`scripts/teste_extracao_mpu.py`**
- Testa extração completa em um PDF
- Mostra todas as etapas (texto, LLM, geocoding)
- Exibe dados extraídos na tela

**`scripts/extrair_mpu.py`**
- Processamento em lote de todos os PDFs
- Gera JSONs, TXTs e CSVs
- Inclui geocodificação automática

## 🧪 Testes

### Testar Conexão Azure OpenAI

```bash
uv run python scripts/teste_azure_openai.py
```

**Saída esperada:**
```
✅ SUCESSO: Conexão com Azure OpenAI está funcionando!
```

### Testar Extração Completa

```bash
uv run python scripts/teste_extracao_mpu.py
```

Mostra o processo completo:
1. Extração de texto (403,490 caracteres)
2. Estruturação com LLM
3. Geocodificação (3 endereços)
4. Dados extraídos formatados

### Testar Geocodificação

```python
from vinea.geocoding import Geocoder

geocoder = Geocoder()
result = geocoder.geocode_address(
    street="Rua Vereador Francisco Mendes Castanho",
    neighborhood="Don Lázaro",
    municipality="Cesário Lange",
    state="São Paulo"
)

if result.success:
    print(f"Coordenadas: {result.latitude}, {result.longitude}")
```

## 🔍 Detalhes Técnicos

### Extração de Texto

Utiliza `pdfplumber` para extrair texto de PDFs, preservando a estrutura do documento.

### Estruturação com LLM

O texto extraído é enviado ao Azure OpenAI (GPT-4/GPT-3.5) com:
- **Prompt especializado** em processos judiciais
- **JSON Schema** dos modelos Pydantic
- **Temperatura baixa (0.1)** para maior precisão
- **Validação automática** via Pydantic
- **Parâmetro `max_completion_tokens`** para modelos mais recentes

### Geocodificação

- **API:** OpenStreetMap Nominatim (gratuita)
- **Bounding box:** Restrito ao município do processo
- **Rate limit:** 1 requisição por segundo (respeitado automaticamente)
- **Fallback:** Endereço → CEP → Município
- **User-Agent:** `vinea-mpu-extractor/1.0`
- **Coordenadas:** WGS84 (EPSG:4326)

### Performance

- **Tempo médio por PDF:** 30-60 segundos
  - Extração de texto: ~5s
  - LLM: ~15-30s
  - Geocodificação: ~10-20s (3 endereços)
- **Tokens por processo:** ~5.000-15.000 tokens
- **Acurácia:** Alta para dados estruturados presentes no texto
- **Taxa de geocodificação bem-sucedida:** ~80-90%

## 🛠️ Manutenção

### Adicionar Novos Campos

1. Edite os modelos em `src/vinea/mpu_models.py`
2. Atualize o parser em `src/vinea/mpu_parser.py` se necessário
3. O LLM automaticamente tentará extrair os novos campos

### Melhorar Extração

- Ajuste o prompt em `mpu_extraction.py` → `extract_mpu_data_with_llm()`
- Aumente `max_completion_tokens` para respostas maiores
- Experimente diferentes valores de `temperature`

### Melhorar Geocodificação

- Ajuste a lógica de fallback em `geocoding.py` → `MPUGeocoder._geocode_location()`
- Considere adicionar cache de coordenadas para endereços comuns
- Use serviços de geocodificação pagos para maior precisão (Google, Mapbox)

## 📚 Referências

- [Lei Maria da Penha (11.340/2006)](http://www.planalto.gov.br/ccivil_03/_ato2004-2006/2006/lei/l11340.htm)
- [CNJ - Medidas Protetivas de Urgência](https://www.cnj.jus.br/programas-e-acoes/violencia-contra-a-mulher/)
- [OpenStreetMap Nominatim API](https://nominatim.org/release-docs/latest/api/Search/)
- [Azure OpenAI Documentation](https://learn.microsoft.com/azure/ai-services/openai/)

## ✅ Status

- ✅ Modelos de dados implementados
- ✅ Extração de PDF com pdfplumber
- ✅ Integração com Azure OpenAI
- ✅ Parser para DataFrames pandas
- ✅ Scripts de teste e processamento em lote
- ✅ **Geocodificação automática de endereços**
- ✅ **DataFrame dedicado de localização**
- ✅ Testado com sucesso em arquivos reais

## 🎯 Recursos Principais

| Recurso | Status | Descrição |
|---------|--------|-----------|
| Extração de texto | ✅ | pdfplumber para PDFs |
| Estruturação com LLM | ✅ | Azure OpenAI GPT-4/3.5 |
| Validação de dados | ✅ | Pydantic models |
| Geocodificação | ✅ | OpenStreetMap Nominatim |
| DataFrames pandas | ✅ | 7 DataFrames especializados |
| Exportação CSV | ✅ | Incluindo localização |
| Processamento em lote | ✅ | Múltiplos PDFs |
| Análise espacial | ✅ | Coordenadas geográficas |

---

**Desenvolvido para o projeto Vinea - Análise de Processos Judiciais**

*Versão 1.1 - Com suporte a geocodificação automática*
