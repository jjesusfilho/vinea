import os
import pandas as pd
from lxml import etree
from typing import Optional
from pathlib import Path
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    SPARK_AVAILABLE = False

try:
    from notebookutils import mssparkutils
    MSSPARKUTILS_AVAILABLE = True
except ImportError:
    MSSPARKUTILS_AVAILABLE = False


class MNIParser:
    """
    Parser leve para MNI que opera sobre arquivos XML.
    Pode opcionalmente usar SparkSession para salvar ou ler shards de texto.
    """
    def __init__(self, use_spark: bool = False, spark_session: Optional[SparkSession] = None):
        """
        Inicializa o parser.

        Args:
            use_spark: se True, habilita leitura via Spark.
            spark_session: SparkSession opcional; se use_spark=True e não fornecida, cria uma sessão.
        """
        self.use_spark = use_spark
        if use_spark:
            if not SPARK_AVAILABLE:
                raise ImportError("PySpark não está disponível. Instale com: pip install pyspark")
            self.spark = spark_session or SparkSession.builder \
                .appName("MNIParser") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        else:
            self.spark = None

    def _is_abfss_path(self, path: str) -> bool:
        """Retorna True se for caminho ABFSS (Azure Data Lake Storage)."""
        return path.startswith("abfss://") or path.startswith("abfs://")

    def _get_file_mtime(self, file_path: str) -> float:
        """
        Obtém o timestamp de modificação de um arquivo local ou ABFSS (via mssparkutils).
        """
        if self.use_spark and self._is_abfss_path(file_path):
            if not MSSPARKUTILS_AVAILABLE:
                raise ImportError("mssparkutils não está disponível para caminhos ABFSS")
            try:
                info = mssparkutils.fs.ls(file_path)
                return info[0].modifyTime / 1000.0 if info else datetime.now().timestamp()
            except Exception:
                return datetime.now().timestamp()
        arquivo_path = Path(file_path)
        return arquivo_path.stat().st_mtime if arquivo_path.exists() else datetime.now().timestamp()

    def criar_registro_carga(self, caminho_arquivo: str) -> pd.DataFrame:
        """
        Cria um registro de carga (DataFrame) a partir de um XML local ou ABFSS.
        """
        if self.use_spark and self._is_abfss_path(caminho_arquivo):
            nome_arquivo = caminho_arquivo.split("/")[-1]
            diretorio = caminho_arquivo.split("/")[-2]
        else:
            arquivo_path = Path(caminho_arquivo)
            nome_arquivo = arquivo_path.name
            diretorio = arquivo_path.parent.name

        nome_sem_ext = nome_arquivo.replace(".xml", "")
        if nome_sem_ext.startswith("cabecalho_"):
            numero_processo_raw = nome_sem_ext.replace("cabecalho_", "")
            tipo_consulta = "cabecalho"
        else:
            numero_processo_raw = nome_sem_ext
            tipo_consulta = "documentos"

        mtime = self._get_file_mtime(caminho_arquivo)
        data_carga = datetime.fromtimestamp(mtime)
        timestamp = int(mtime)

        if len(numero_processo_raw) == 20:
            numero_processo_formatado = (
                f"{numero_processo_raw[:7]}-{numero_processo_raw[7:9]}."
                f"{numero_processo_raw[9:13]}.{numero_processo_raw[13:14]}."
                f"{numero_processo_raw[14:16]}.{numero_processo_raw[16:]}"
            )
        else:
            numero_processo_formatado = numero_processo_raw

        if self.use_spark and self._is_abfss_path(caminho_arquivo) and MSSPARKUTILS_AVAILABLE:
            try:
                info = mssparkutils.fs.ls(caminho_arquivo)
                tamanho = info[0].size if info else 0
            except Exception:
                tamanho = 0
        else:
            arquivo_path = Path(caminho_arquivo)
            tamanho = arquivo_path.stat().st_size if arquivo_path.exists() else 0

        registro = {
            "id_carga": f"{numero_processo_raw}_{timestamp}",
            "nome_arquivo": nome_arquivo,
            "caminho_completo": caminho_arquivo
            if self.use_spark and self._is_abfss_path(caminho_arquivo)
            else str(Path(caminho_arquivo).absolute()),
            "diretorio": diretorio,
            "numero_processo_raw": numero_processo_raw,
            "numero_processo_formatado": numero_processo_formatado,
            "tipo_consulta": tipo_consulta,
            "timestamp_carga": timestamp,
            "data_carga": data_carga,
            "data_carga_str": data_carga.strftime("%Y-%m-%d %H:%M:%S"),
            "tamanho_arquivo_bytes": tamanho,
            "extensao": ".xml"
            if self.use_spark and self._is_abfss_path(caminho_arquivo)
            else Path(caminho_arquivo).suffix,
            "status_processamento": "pendente",
            "data_registro": datetime.now(),
            "observacoes": f"Arquivo {tipo_consulta} carregado do diretório {diretorio}",
        }
        return pd.DataFrame([registro])

    def _read_xml_file(self, xml_path: str) -> str:
        """
        Lê o conteúdo XML de um arquivo ou de um diretório de shards de texto Spark.
        """
        path = Path(xml_path)
        # leitura direta em ABFSS (arquivo ou diretório) via mssparkutils ou Spark
        if self.use_spark and self._is_abfss_path(xml_path):
            if MSSPARKUTILS_AVAILABLE:
                try:
                    # tenta ler o início do arquivo (head)
                    return mssparkutils.fs.head(xml_path)
                except Exception:
                    # provavelmente é diretório: concatena shards via Spark
                    df = self.spark.read.text(xml_path)
                    return "".join(row.value for row in df.collect())
            # sem mssparkutils disponível, lê arquivo único via Spark wholetext
            df = self.spark.read.text(xml_path, wholetext=True)
            return df.collect()[0]["value"]

        if path.is_dir():
            # usa Spark se disponível
            if self.spark:
                df = self.spark.read.text(str(path))
                return "".join(row.value for row in df.collect())

            # fallback: concatena shards .txt (ignora .crc)
            parts: list[str] = []
            for shard in sorted(path.iterdir()):
                if shard.is_file() and shard.suffix == ".txt":
                    parts.append(shard.read_text(encoding="utf-8"))
            return "".join(parts)

        # leitura de um único arquivo XML ou via Spark wholetext
        if self.use_spark:
            df = self.spark.read.text(str(path), wholetext=True)
            return df.collect()[0]["value"]

        with open(xml_path, "r", encoding="utf-8") as file:
            return file.read()

    def extrair_numero_tempo(self, xml_file: str) -> list[str]:
        arquivo = Path(xml_file).name.replace('.xml', '')
        if arquivo.startswith('cabecalho_'):
            numero_processo = arquivo.replace('cabecalho_', '')
        else:
            numero_processo = arquivo

        timestamp = int(self._get_file_mtime(xml_file))

        return [numero_processo, str(timestamp)]

    def extrair_dados_basicos_xml(self, xml_path: str):
        xml_content = self._read_xml_file(xml_path)
        root = etree.fromstring(xml_content)

        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }

        dados_basicos = root.find('.//ns2:dadosBasicos', namespaces)
        if dados_basicos is None:
            return pd.DataFrame(), pd.DataFrame()

        processo_data = {
            'numero_processo': dados_basicos.get('numero'),
            'competencia': dados_basicos.get('competencia'),
            'classe_processual': dados_basicos.get('classeProcessual'),
            'codigo_localidade': dados_basicos.get('codigoLocalidade'),
            'nivel_sigilo': dados_basicos.get('nivelSigilo'),
            'intervencao_mp': dados_basicos.get('intervencaoMP'),
            'data_ajuizamento': dados_basicos.get('dataAjuizamento')
        }

        principal_assunto = dados_basicos.find('.//ns2:assunto[@principal="true"]', namespaces)
        if principal_assunto is not None:
            assunto_local = principal_assunto.find('.//ns2:assuntoLocal', namespaces)
            if assunto_local is not None:
                processo_data.update({
                    'codigo_assunto': assunto_local.get('codigoAssunto'),
                    'codigo_pai_nacional': assunto_local.get('codigoPaiNacional'),
                    'descricao_assunto': assunto_local.get('descricao')
                })
            else:
                codigo_nacional = principal_assunto.find('.//ns2:codigoNacional', namespaces)
                if codigo_nacional is not None:
                    processo_data['codigo_assunto'] = codigo_nacional.text

                codigo_pai_nacional = principal_assunto.find('.//ns2:codigoPaiNacional', namespaces)
                if codigo_pai_nacional is not None:
                    processo_data['codigo_pai_nacional'] = codigo_pai_nacional.text

                descricao_assunto = principal_assunto.find('.//ns2:descricao', namespaces)
                if descricao_assunto is not None:
                    processo_data['descricao_assunto'] = descricao_assunto.text

        valor_causa = dados_basicos.find('.//ns2:valorCausa', namespaces)
        if valor_causa is not None:
            processo_data['valor_causa'] = valor_causa.text

        situacao = dados_basicos.find('.//ns2:outroParametro[@nome="situacaoProcesso"]', namespaces)
        if situacao is not None:
            processo_data['situacao_processo'] = situacao.get('valor')

        orgao = dados_basicos.find('.//ns2:orgaoJulgador', namespaces)
        if orgao is not None:
            processo_data.update({
                'codigo_orgao': orgao.get('codigoOrgao'),
                'nome_orgao': orgao.get('nomeOrgao'),
                'instancia': orgao.get('instancia'),
                'codigo_municipio_ibge': orgao.get('codigoMunicipioIBGE')
            })

        polos = dados_basicos.findall('.//ns2:polo', namespaces)
        partes_data = []

        for polo in polos:
            polo_tipo = polo.get('polo')
            for parte in polo.findall('.//ns2:parte', namespaces):
                pessoa = parte.find('.//ns2:pessoa', namespaces)
                if pessoa is None:
                    continue

                parte_info = {
                    'numero_processo': processo_data.get('numero_processo'),
                    'polo': polo_tipo,
                    'nome': pessoa.get('nome'),
                    'sexo': pessoa.get('sexo'),
                    'tipo_pessoa': pessoa.get('tipoPessoa'),
                    'nacionalidade': pessoa.get('nacionalidade'),
                    'cidade_natural': pessoa.get('cidadeNatural'),
                    'nome_genitor': pessoa.get('nomeGenitor'),
                    'nome_genitora': pessoa.get('nomeGenitora'),
                    'data_nascimento': pessoa.get('dataNascimento'),
                    'assistencia_judiciaria': parte.get('assistenciaJudiciaria'),
                    'intimacao_pendente': parte.get('intimacaoPendente')
                }

                endereco = pessoa.find('.//ns2:endereco', namespaces)
                if endereco is not None:
                    parte_info.update({
                        'cep': endereco.get('cep'),
                        'logradouro': endereco.find('.//ns2:logradouro', namespaces).text if endereco.find('.//ns2:logradouro', namespaces) is not None else None,
                        'numero_endereco': endereco.find('.//ns2:numero', namespaces).text if endereco.find('.//ns2:numero', namespaces) is not None else None,
                        'bairro': endereco.find('.//ns2:bairro', namespaces).text if endereco.find('.//ns2:bairro', namespaces) is not None else None,
                        'cidade': endereco.find('.//ns2:cidade', namespaces).text if endereco.find('.//ns2:cidade', namespaces) is not None else None,
                        'pais': endereco.find('.//ns2:pais', namespaces).text if endereco.find('.//ns2:pais', namespaces) is not None else None
                    })

                partes_data.append(parte_info)

        df_processo = pd.DataFrame([processo_data])
        df_partes = pd.DataFrame(partes_data) if partes_data else pd.DataFrame()

        if not df_processo.empty and 'data_ajuizamento' in df_processo.columns:
            df_processo['data_ajuizamento'] = pd.to_datetime(
                df_processo['data_ajuizamento'].astype(str).str[:8],
                format='%Y%m%d',
                errors='coerce'
            ).dt.date

        if not df_processo.empty and 'valor_causa' in df_processo.columns:
            df_processo['valor_causa'] = pd.to_numeric(df_processo['valor_causa'], errors='coerce').round(2)

        if not df_partes.empty and 'data_nascimento' in df_partes.columns:
            df_partes['data_nascimento'] = pd.to_datetime(
                df_partes['data_nascimento'],
                format='%Y%m%d',
                errors='coerce'
            ).dt.date

        return df_processo, df_partes

    def ler_movimentos(self, xml_file: str) -> pd.DataFrame:
        xml_content = self._read_xml_file(xml_file)
        root = etree.fromstring(xml_content)

        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }

        movimentos = []
        numero_processo, hora_coleta = self.extrair_numero_tempo(xml_file)

        for mov in root.xpath('.//ns2:movimento', namespaces=namespaces):
            local = mov.find('.//ns2:movimentoLocal', namespaces)
            local_pai = local.find('.//ns2:movimentoLocalPai', namespaces) if local is not None else None
            movimentonacional = mov.find('.//ns2:movimentoNacional', namespaces)

            movimentos.append({
                'numero_processo': numero_processo,
                'hora_coleta': hora_coleta,
                'data_hora': mov.get('dataHora'),
                'nivel_sigilo': mov.get('nivelSigilo'),
                'identificador_movimento': mov.get('identificadorMovimento'),
                'complemento': mov.find('.//ns2:complemento', namespaces).text if mov.find('.//ns2:complemento', namespaces) is not None else None,
                'codigo_movimento_local': local.get('codigoMovimento') if local is not None else None,
                'codigo_pai_nacional_local': local.get('codigoPaiNacional') if local is not None else None,
                'descricao_movimento_local': local.get('descricao') if local is not None else None,
                'codigo_mp_local': local_pai.get('codigoMovimento') if local_pai is not None else None,
                'codigo_pai_nacional_local_pai': local_pai.get('codigoPaiNacional') if local_pai is not None else None,
                'descricao_movimento_local_pai': local_pai.get('descricao') if local_pai is not None else None,
                'codigo_movimento_nacional': movimentonacional.get('codigoNacional') if movimentonacional is not None else None
            })

        df = pd.DataFrame(movimentos)
        if not df.empty:
            df['hora_coleta'] = pd.to_datetime(df['hora_coleta'].astype(int), unit='s')
            df['data_hora'] = pd.to_datetime(
                df['data_hora'],
                format='%Y%m%d%H%M%S',
                errors='coerce'
            ).dt.tz_localize('America/Sao_Paulo')
            numeric_columns = [
                'nivel_sigilo',
                'codigo_movimento_local',
                'codigo_pai_nacional_local',
                'codigo_mp_local',
                'codigo_pai_nacional_local_pai',
                'codigo_movimento_nacional'
            ]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # ensure specific columns are nullable integers
            df['nivel_sigilo'] = df['nivel_sigilo'].astype('Int64')
            df['identificador_movimento'] = df['identificador_movimento'].astype('Int64')
            codigo_cols = [col for col in df.columns if col.startswith('codigo')]
            df[codigo_cols] = df[codigo_cols].astype('Int64')

            # normalize numero_processo to digits-only string
            df['numero_processo'] = (
                df['numero_processo']
                .astype(str)
                .str.replace(r'\D', '', regex=True)
            )

        return df

    def ler_lista_documentos(self, xml_file: str) -> pd.DataFrame:
        xml_content = self._read_xml_file(xml_file)
        root = etree.fromstring(xml_content)

        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }

        documentos = []
        numero_processo, hora_coleta = self.extrair_numero_tempo(xml_file)

        for doc in root.xpath('.//ns2:documento', namespaces=namespaces):
            documentos.append({
                'numero_processo': numero_processo,
                'hora_coleta': hora_coleta,
                'id_documento': doc.get('idDocumento'),
                'tipo_documento': doc.get('tipoDocumento'),
                'data_hora': doc.get('dataHora'),
                'descricao': doc.get('descricao'),
                'mimetype': doc.get('mimetype'),
                'nivel_sigilo': doc.get('nivelSigilo'),
                'movimento': doc.get('movimento'),
                'tipo_documento_local': doc.get('tipoDocumentoLocal')
            })

        df = pd.DataFrame(documentos)
        if df.empty:
            return df

        df['hora_coleta'] = pd.to_datetime(df['hora_coleta'].astype(int), unit='s')
        df['data_hora'] = pd.to_datetime(
            df['data_hora'],
            format='%Y%m%d%H%M%S',
            errors='coerce'
        ).dt.tz_localize('America/Sao_Paulo')
        df['descricao'] = df['descricao'].str.strip('"')
        numeric_columns = ['tipo_documento', 'nivel_sigilo', 'tipo_documento_local', 'movimento']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        dados_basicos = root.xpath('.//ns2:dadosBasicos', namespaces=namespaces)
        if dados_basicos:
            df['numero_processo'] = dados_basicos[0].get('numero')
        # normalize numero_processo to digits-only string
        df['numero_processo'] = (
            df['numero_processo']
            .astype(str)
            .str.replace(r'\D', '', regex=True)
        )

        return df
