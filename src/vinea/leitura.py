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
    SPARK_AVAILABLE = False

try:
    from notebookutils import mssparkutils
    MSSPARKUTILS_AVAILABLE = True
except ImportError:
    MSSPARKUTILS_AVAILABLE = False

class MNIParser():
    
    def __init__(self, use_spark: bool = False, spark_session: Optional[SparkSession] = None):
        """
        Inicializa o parser

        Args:
            use_spark: Se True, usa Spark para ler arquivos. Se False, usa Python padrão
            spark_session: Sessão Spark opcional. Se não fornecida e use_spark=True, cria uma nova
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
        """
        Verifica se o caminho é um caminho ABFSS (Azure Blob File System)

        Args:
            path: Caminho do arquivo

        Returns:
            bool: True se for caminho ABFSS, False caso contrário
        """
        return path.startswith('abfss://') or path.startswith('abfs://')

    def _get_file_mtime(self, file_path: str) -> float:
        """
        Obtém o timestamp de modificação do arquivo, suportando tanto caminhos locais quanto ABFSS

        Args:
            file_path: Caminho do arquivo (local ou ABFSS)

        Returns:
            float: Timestamp de modificação do arquivo em segundos desde epoch
        """
        if self.use_spark and self._is_abfss_path(file_path):
            # Usar mssparkutils para caminhos ABFSS
            if not MSSPARKUTILS_AVAILABLE:
                raise ImportError(
                    "mssparkutils não está disponível. "
                    "Este módulo é necessário para trabalhar com caminhos ABFSS em ambientes Azure."
                )

            try:
                # Obter informações do arquivo usando mssparkutils
                file_info = mssparkutils.fs.ls(file_path)
                if file_info:
                    # mssparkutils retorna modifyTime em milissegundos
                    return file_info[0].modifyTime / 1000.0
                else:
                    # Se não conseguir obter informações, usar timestamp atual
                    return datetime.now().timestamp()
            except Exception as e:
                # Fallback para timestamp atual em caso de erro
                print(f"Aviso: Não foi possível obter timestamp do arquivo {file_path}: {e}")
                return datetime.now().timestamp()
        else:
            # Usar pathlib para caminhos locais
            arquivo_path = Path(file_path)
            if arquivo_path.exists():
                return arquivo_path.stat().st_mtime
            else:
                return datetime.now().timestamp()
    
    
    def criar_registro_carga(self, caminho_arquivo: str) -> pd.DataFrame:
        """
        Cria um DataFrame com registro de carga a partir do nome do arquivo XML.

        Args:
            caminho_arquivo: Caminho completo para o arquivo XML

        Returns:
            pd.DataFrame: DataFrame com informações do registro de carga
        """
        # Extrair informações do caminho
        if self._is_abfss_path(caminho_arquivo):
            # Para caminhos ABFSS, extrair nome do arquivo manualmente
            nome_arquivo = caminho_arquivo.split('/')[-1]
            diretorio = caminho_arquivo.split('/')[-2]
        else:
            arquivo_path = Path(caminho_arquivo)
            nome_arquivo = arquivo_path.name
            diretorio = arquivo_path.parent.name

        # Extrair informações do nome do arquivo
        nome_sem_ext = nome_arquivo.replace('.xml', '')

        # Separar as partes
        if nome_sem_ext.startswith('cabecalho_'):
            numero_processo_raw = nome_sem_ext.replace('cabecalho_', '')
            tipo_consulta = 'cabecalho'
        else:
            numero_processo_raw = nome_sem_ext
            tipo_consulta = 'documentos'

        # Usar a data de modificação do arquivo como data de carga
        mtime = self._get_file_mtime(caminho_arquivo)
        data_carga = datetime.fromtimestamp(mtime)
        timestamp = int(mtime)
        
        # Formatar número do processo
        if len(numero_processo_raw) == 20:
            numero_processo_formatado = f"{numero_processo_raw[:7]}-{numero_processo_raw[7:9]}.{numero_processo_raw[9:13]}.{numero_processo_raw[13:14]}.{numero_processo_raw[14:16]}.{numero_processo_raw[16:]}"
        else:
            numero_processo_formatado = numero_processo_raw

        # Obter tamanho do arquivo
        if self._is_abfss_path(caminho_arquivo):
            if self.use_spark and MSSPARKUTILS_AVAILABLE:
                try:
                    file_info = mssparkutils.fs.ls(caminho_arquivo)
                    tamanho_arquivo = file_info[0].size if file_info else 0
                except Exception:
                    tamanho_arquivo = 0
            else:
                tamanho_arquivo = 0
        else:
            arquivo_path = Path(caminho_arquivo)
            tamanho_arquivo = arquivo_path.stat().st_size if arquivo_path.exists() else 0
        
        # Criar registro de carga
        registro = {
            'id_carga': f"{numero_processo_raw}_{timestamp}",
            'nome_arquivo': nome_arquivo,
            'caminho_completo': caminho_arquivo if self._is_abfss_path(caminho_arquivo) else str(Path(caminho_arquivo).absolute()),
            'diretorio': diretorio,
            'numero_processo_raw': numero_processo_raw,
            'numero_processo_formatado': numero_processo_formatado,
            'tipo_consulta': tipo_consulta,
            'timestamp_carga': timestamp,
            'data_carga': data_carga,
            'data_carga_str': data_carga.strftime('%Y-%m-%d %H:%M:%S'),
            'tamanho_arquivo_bytes': tamanho_arquivo,
            'extensao': '.xml' if self._is_abfss_path(caminho_arquivo) else Path(caminho_arquivo).suffix,
            'status_processamento': 'pendente',
            'data_registro': datetime.now(),
            'observacoes': f'Arquivo {tipo_consulta} carregado do diretório {diretorio}'
        }
        
        return pd.DataFrame([registro])
     
    def _read_xml_file(self, xml_path: str) -> str:
        """
        Lê o arquivo XML usando Spark ou Python padrão
        
        Args:
            xml_path: Caminho para o arquivo XML
            
        Returns:
            str: Conteúdo do arquivo XML
        """
        if self.use_spark:
            # Usar Spark para ler o arquivo
            df = self.spark.read.text(xml_path, wholetext=True)
            xml_content = df.collect()[0]['value']
            return xml_content
        else:
            # Usar Python padrão
            with open(xml_path, 'r', encoding='utf-8') as file:
                return file.read()

    def extrair_numero_tempo(self, xml_file):
        """
        Extrai o número do processo do nome do arquivo e a data de modificação.

        Args:
          xml_file (str): Nome do arquivo no formato xml

        Returns:
            list: [Número do processo extraído, timestamp de modificação do arquivo]
        """
        # Exemplo de nome de arquivo: "0000000000000000000.xml" ou "cabecalho_0000000000000000000.xml"
        if self._is_abfss_path(xml_file):
            arquivo = xml_file.split('/')[-1].replace('.xml', '')
        else:
            arquivo = os.path.basename(xml_file).replace('.xml', '')

        # Remover o prefixo 'cabecalho_' se existir
        if arquivo.startswith('cabecalho_'):
            numero_processo = arquivo.replace('cabecalho_', '')
        else:
            numero_processo = arquivo

        # Obter timestamp de modificação do arquivo
        timestamp = int(self._get_file_mtime(xml_file))

        return [numero_processo, str(timestamp)]

    def extrair_dados_basicos_xml(self, xml_path: str):
        """
        Extrai dados básicos do processo de um XML do MNI
        
        Args:
            xml_path: Caminho para o arquivo XML
        
        Returns:
            tuple: (DataFrame do processo, DataFrame das partes)
        """
        # Lê o XML usando Spark ou Python padrão
        xml_content = self._read_xml_file(xml_path)
        
        root = etree.fromstring(xml_content)
        
        # Definir namespaces
        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }
        
        # Encontrar elemento dadosBasicos
        dados_basicos = root.find('.//ns2:dadosBasicos', namespaces)
        
        if dados_basicos is None:
            return pd.DataFrame(), pd.DataFrame()
        
        # Extrair dados básicos do processo
        processo_data = {
            'numero_processo': dados_basicos.get('numero'),
            'competencia': dados_basicos.get('competencia'),
            'classe_processual': dados_basicos.get('classeProcessual'),
            'codigo_localidade': dados_basicos.get('codigoLocalidade'),
            'nivel_sigilo': dados_basicos.get('nivelSigilo'),
            'intervencao_mp': dados_basicos.get('intervencaoMP'),
            'data_ajuizamento': dados_basicos.get('dataAjuizamento')
        }
        
        # Extrair assunto principal
        assunto = dados_basicos.find('.//ns2:assunto[@principal="true"]/ns2:assuntoLocal', namespaces)
        if assunto is not None:
            processo_data.update({
                'codigo_assunto': assunto.get('codigoAssunto'),
                'codigo_pai_nacional': assunto.get('codigoPaiNacional'),
                'descricao_assunto': assunto.get('descricao')
            })
        
        # Extrair valor da causa
        valor_causa = dados_basicos.find('.//ns2:valorCausa', namespaces)
        if valor_causa is not None:
            processo_data['valor_causa'] = valor_causa.text
        
        # Extrair situação do processo
        situacao = dados_basicos.find('.//ns2:outroParametro[@nome="situacaoProcesso"]', namespaces)
        if situacao is not None:
            processo_data['situacao_processo'] = situacao.get('valor')
        
        # Extrair orgão julgador
        orgao = dados_basicos.find('.//ns2:orgaoJulgador', namespaces)
        if orgao is not None:
            processo_data.update({
                'codigo_orgao': orgao.get('codigoOrgao'),
                'nome_orgao': orgao.get('nomeOrgao'),
                'instancia': orgao.get('instancia'),
                'codigo_municipio_ibge': orgao.get('codigoMunicipioIBGE')
            })
        
        # Extrair dados das partes
        polos = dados_basicos.findall('.//ns2:polo', namespaces)
        partes_data = []
        
        for polo in polos:
            polo_tipo = polo.get('polo')
            partes = polo.findall('.//ns2:parte', namespaces)
            
            for parte in partes:
                pessoa = parte.find('.//ns2:pessoa', namespaces)
                if pessoa is not None:
                    parte_info = {
                        'numero_processo': processo_data['numero_processo'],
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
                    
                    # Extrair endereço se existir
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
        
        # Criar DataFrames com Pandas
        df_processo = pd.DataFrame([processo_data])
        df_partes = pd.DataFrame(partes_data) if partes_data else pd.DataFrame()

        # Converter data_nascimento para tipo date
        if not df_partes.empty and 'data_nascimento' in df_partes.columns:
            df_partes['data_nascimento'] = pd.to_datetime(
                df_partes['data_nascimento'], 
                format='%Y%m%d', 
                errors='coerce'
            ).dt.date
            
        return df_processo, df_partes

    def ler_lista_documentos(self, xml_file):
        """
        Lê um arquivo XML de lista de documentos e retorna um DataFrame Pandas com os detalhes dos documentos.

        Args:
            xml_file (str): Caminho para o arquivo XML.
            
        Returns:
            pd.DataFrame: DataFrame do Pandas contendo os detalhes dos documentos.
        """
        # Usar _read_xml_file para consistência
        xml_content = self._read_xml_file(xml_file)
        root = etree.fromstring(xml_content)

        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }
        documentos = []

        for doc in root.xpath('.//ns2:documento', namespaces=namespaces):
            documento_info = {
                'processo': self.extrair_numero_tempo(xml_file)[0],
                'hora_coleta': self.extrair_numero_tempo(xml_file)[1],
                'id_documento': doc.get('idDocumento'),
                'tipo_documento': doc.get('tipoDocumento'),
                'data_hora': doc.get('dataHora'),
                'descricao': doc.get('descricao'),
                'mimetype': doc.get('mimetype'),
                'nivel_sigilo': doc.get('nivelSigilo'),
                'movimento': doc.get('movimento'),
                'tipo_documento_local': doc.get('tipoDocumentoLocal')
            }
            documentos.append(documento_info)

        # Criar DataFrame e remover linhas com id_documento nulo
        df = pd.DataFrame(documentos)
        df = df.dropna(subset=['id_documento'])

        if not df.empty:
            # Atualizar coluna processo com número do processo real do XML
            dados_basicos = root.xpath('.//ns2:dadosBasicos', namespaces=namespaces)
            if dados_basicos:
                df['processo'] = dados_basicos[0].get("numero")

            # Conversões de tipo com Pandas
            # Converter hora_coleta de timestamp Unix para datetime
            df['hora_coleta'] = pd.to_datetime(df['hora_coleta'].astype(int), unit='s')
            
            # Converter data_hora para datetime com timezone
            df['data_hora'] = pd.to_datetime(
                df['data_hora'], 
                format='%Y%m%d%H%M%S', 
                errors='coerce'
            ).dt.tz_localize('America/Sao_Paulo')
            
            # Limpar descrição removendo aspas
            df['descricao'] = df['descricao'].str.strip('"')
            
            # Converter colunas numéricas
            numeric_columns = ['tipo_documento', 'nivel_sigilo', 'tipo_documento_local', 'movimento']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df