import os
from typing import Any
from zeep import Client
from time import time
from lxml import etree
import polars as pl



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

    def baixar_lista_documentos(self, numero_processo: str, save_dir: str = "."):
        # Limpa o número do processo
        numero_limpo = numero_processo.replace(".", "").replace("-", "")

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
    """
    Parser para arquivos XML do MNI do TJSP
    """
    def __init__(self):
        pass

    def extrair_numero_tempo(self, xml_file):
        """
        Extrai o número do processo  e o tempo do nome do arquivo.
    
        Args:
          xmo_file (str): Nome do arquivo no formato xml
        
        Returns:
            list: [Número do processo extraído, hora_coleta]
        """
        # Exemplo de nome de arquivo: "0000000-00.0000.0.00.0000_time_1700000000.xml"
        arquivo = os.path.basename(xml_file).replace('.xml', '')

        partes = arquivo.split('_time_')
        
        if len(partes) != 2:
            raise ValueError("Nome do arquivo não está no formato esperado.")
        
        return partes

    def ler_lista_documentos(self, xml_file):
        """
        Lê um arquivo XML de lista de documentos e retorna uma lista de dicionários com os detalhes dos documentos.

        Args:
            xml_file (str): Caminho para o arquivo XML.
            
        Returns:
            list: Lista de dicionários contendo os detalhes dos documentos.
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



    