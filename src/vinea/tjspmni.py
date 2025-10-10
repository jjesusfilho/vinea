import os
from typing import Any
from zeep import Client
from time import time
from lxml import etree
import polars as pl

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class MNIClient():
    """
    Cliente para interagir com o MNI do TJSP
    """
     
    def __init__(self, usuario: str, senha: str):
        """
        Inicializa o cliente TJSP com Zeep
        
        Args:
            usuario: Usuário para autenticação (se necessário)
            senha: Senha para autenticação (se necessário)
        """
        self.wsdl = 'http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl'
        self.usuario = usuario
        self.senha = senha
        self.client = Client(wsdl=self.wsdl)

    def save_to_xml_file(self, data: Any, save_dir: str, filename: str):
        filepath = os.path.join(save_dir, filename)
        
        # Assegura que o diretório existe
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        try:
            with open(filepath, 'wb') as f:  # Usando 'wb' para bytes
                f.write(data)
        except Exception as e:
            raise ValueError(f"Não pode salvar arquivo XML: {e}")
        
        return filepath

    def validar_numero_processo(self, numero_processo: str):
        numero_limpo = numero_processo.replace(".", "").replace("-", "")
        
        if len(numero_limpo) != 20:
            raise ValueError(f"Número do processo deve ter 20 dígitos após limpeza. "
                            f"Recebido: '{numero_limpo}' com {len(numero_limpo)} dígitos")
        
        if not numero_limpo.isdigit():
            raise ValueError(f"Número do processo deve conter apenas dígitos após limpeza. "
                            f"Recebido: '{numero_limpo}'")
        
        return numero_limpo

    def consultar_processo(self, numero_processo: str, save_dir: str = "."):
        
        numero_limpo = self.validar_numero_processo(numero_processo)

        try:
            with self.client.settings(raw_response=True, strict=False):
                resposta = self.client.service.consultarProcesso(
                    idConsultante=self.usuario,
                    senhaConsultante=self.senha,
                    numeroProcesso=numero_limpo,
                    incluirCabecalho=True,
                    movimentos=None,
                    dataReferencia=None,
                    incluirDocumentos=None
                )

            filename = f"cabecalho_{''.join(filter(str.isalnum, numero_processo))}_time_{int(time())}.xml"
            saved_path = self.save_to_xml_file(resposta.content, save_dir, filename)
            print(f"Response saved to: {saved_path}")
            return saved_path

        except Exception as e:
            print(f"Erro ao processar o processo {numero_processo}: {e}")
            return None


    def baixar_lista_documentos(self, numero_processo: str, save_dir: str = "."):
       
        numero_limpo = self.validar_numero_processo(numero_processo)

        try:
            with self.client.settings(raw_response=True, strict=False):
                resposta = self.client.service.consultarProcesso(
                    idConsultante=self.usuario,
                    senhaConsultante=self.senha,
                    numeroProcesso=numero_limpo,
                    incluirCabecalho=None,
                    movimentos=None,
                    dataReferencia=None,
                    incluirDocumentos=True
                )

            filename = f"{''.join(filter(str.isalnum, numero_processo))}_time_{int(time())}.xml"
            saved_path = self.save_to_xml_file(resposta.content, save_dir, filename)
            print(f"Response saved to: {saved_path}")
            return saved_path

        except Exception as e:
            print(f"Erro ao processar o processo {numero_processo}: {e}")
            return None

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
        Extrai o número do processo e o tempo do nome do arquivo.
    
        Args:
          xml_file (str): Nome do arquivo no formato xml
        
        Returns:
            list: [Número do processo extraído, hora_coleta]
        """
        # Exemplo de nome de arquivo: "0000000-00.0000.0.00.0000_time_1700000000.xml"
        arquivo = os.path.basename(xml_file).replace('.xml', '')

        partes = arquivo.split('_time_')
        
        if len(partes) != 2:
            raise ValueError("Nome do arquivo não está no formato esperado.")
        
        return partes

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
            return pl.DataFrame(), pl.DataFrame()
        
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
        
        # Criar DataFrames
        df_processo = pl.DataFrame([processo_data])
        df_partes = pl.DataFrame(partes_data) if partes_data else pl.DataFrame()

            # Converter data_nascimento para tipo date
        if not df_partes.is_empty() and 'data_nascimento' in df_partes.columns:
            df_partes = df_partes.with_columns(
                pl.col('data_nascimento').str.strptime(pl.Date, format='%Y%m%d', strict=False).alias('data_nascimento')
            )
            
        return df_processo, df_partes

    def ler_lista_documentos(self, xml_file):
        """
        Lê um arquivo XML de lista de documentos e retorna uma lista de dicionários com os detalhes dos documentos.

        Args:
            xml_file (str): Caminho para o arquivo XML.
            
        Returns:
            pl.DataFrame: DataFrame do Polars contendo os detalhes dos documentos.
        """
        tree = etree.parse(xml_file)
        root = tree.getroot()

        namespaces = {
            'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
            'ns4': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
        }
        documentos = []

        for doc in root.xpath('.//ns2:documento', namespaces=namespaces):
            documento_info = {
                'processo' : self.extrair_numero_tempo(xml_file)[0],
                'hora_coleta' : self.extrair_numero_tempo(xml_file)[1],
                'id_documento': doc.get('idDocumento'),  # get() para atributos
                'tipo_documento': doc.get('tipoDocumento'),
                'data_hora': doc.get('dataHora'),
                'descricao': doc.get('descricao'),
                'mimetype': doc.get('mimetype'),
                'nivel_sigilo': doc.get('nivelSigilo'),
                'movimento': doc.get('movimento'),
                'tipo_documento_local': doc.get('tipoDocumentoLocal')
            }
            documentos.append(documento_info)

        df = pl.DataFrame(documentos) \
            .drop_nulls(subset = ['id_documento']) 

        df = df \
            .with_columns(
                    pl.lit(root.xpath('.//ns2:dadosBasicos', namespaces=namespaces)[0].get("numero")).alias("processo")
            )

        df = df \
            .with_columns(
                pl.from_epoch(pl.col("hora_coleta").str.to_integer(), time_unit = "s").alias("hora_coleta"),
                pl.col("data_hora").str.to_datetime(format="%Y%m%d%H%M%S", strict = False, time_zone= 'America/Sao_Paulo').alias("data_hora"),
                pl.col("descricao").str.strip_chars('"').alias("descricao"),
                pl.col("tipo_documento").str.to_integer().alias("tipo_documento"),
                pl.col("nivel_sigilo").str.to_integer().alias("nivel_sigilo"),
                pl.col("tipo_documento_local").str.to_integer().alias("tipo_documento_local"),
                pl.col("movimento").str.to_integer().alias("movimento")
            ) 

        return df
