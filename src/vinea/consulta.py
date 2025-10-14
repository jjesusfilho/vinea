import os
from typing import Any
from zeep import Client

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

            filename = f"cabecalho_{''.join(filter(str.isalnum, numero_processo))}.xml"
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

            filename = f"{''.join(filter(str.isalnum, numero_processo))}.xml"
            saved_path = self.save_to_xml_file(resposta.content, save_dir, filename)
            print(f"Response saved to: {saved_path}")
            return saved_path

        except Exception as e:
            print(f"Erro ao processar o processo {numero_processo}: {e}")
            return None
