"""
Módulo para comunicação com o serviço de intercomunicação do TJSP usando Zeep
WSDL: http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from zeep import Client, Transport, Settings
from zeep.exceptions import Fault, TransportError
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class MNIClient:
    """
    Cliente para o serviço de intercomunicação do TJSP usando Zeep
    """
    
    def __init__(self, username: str = None, password: str = None, timeout: int = 30):
        """
        Inicializa o cliente TJSP com Zeep
        
        Args:
            username: Usuário para autenticação (se necessário)
            password: Senha para autenticação (se necessário)
            timeout: Timeout para as requisições em segundos
        """
        self.wsdl_url = "http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl"
        self.username = username
        self.password = password
        self.timeout = timeout
        
        # Configuração de logging
        self.logger = logging.getLogger(__name__)
        
        # Configuração da sessão HTTP
        self.session = Session()
        
        # Configuração de retry
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Headers padrão
        self.session.headers.update({
            'User-Agent': 'MPSP'
        })
        
        # Autenticação se fornecida
        if self.username and self.password:
            self.session.auth = HTTPBasicAuth(self.username, self.password)
        
        # Configuração do transport do Zeep
        self.transport = Transport(session=self.session, timeout=self.timeout)
        
        # Configurações do Zeep
        self.settings = Settings(
            strict=False,  # Não falhar em elementos desconhecidos
            xml_huge_tree=True,  # Para documentos XML grandes
            forbid_dtd=False,  # Permitir DTDs se necessário
            forbid_entities=False  # Permitir entidades se necessário
        )
        
        # Cliente Zeep (será inicializado quando necessário)
        self._client = None
    
    @property
    def client(self) -> Client:
        """
        Lazy initialization do cliente Zeep
        
        Returns:
            Instância do cliente Zeep
        """
        if self._client is None:
            try:
                self._client = Client(
                    wsdl=self.wsdl_url,
                    transport=self.transport,
                    settings=self.settings
                )
                self.logger.info("Cliente Zeep inicializado com sucesso")
            except Exception as e:
                self.logger.error(f"Erro ao inicializar cliente Zeep: {e}")
                raise
        
        return self._client
    
    def get_services(self) -> Dict[str, Any]:
        """
        Retorna informações sobre os serviços disponíveis no WSDL
        
        Returns:
            Dicionário com informações dos serviços
        """
        try:
            services = {}
            for service_name, service in self.client.services.items():
                services[service_name] = {
                    'name': service_name,
                    'binding': str(service.binding.name),
                    'operations': []
                }
                
                for operation_name, operation in service.binding._operations.items():
                    services[service_name]['operations'].append({
                        'name': operation_name,
                        'input': str(operation.input.signature()) if hasattr(operation.input, 'signature') else str(operation.input),
                        'output': str(operation.output.signature()) if hasattr(operation.output, 'signature') else str(operation.output)
                    })
            
            return services
        
        except Exception as e:
            self.logger.error(f"Erro ao obter serviços: {e}")
            return {'error': str(e)}
    
    def print_services(self):
        """
        Imprime informações sobre os serviços disponíveis
        """
        try:
            print("=== Serviços WSDL TJSP ===")
            print(self.client.wsdl.dump())
        except Exception as e:
            print(f"Erro ao imprimir serviços: {e}")
    
    def consultar_teor_comunicacao(self, id_comunicacao: str, incluir_documento: bool = True) -> Dict:
        """
        Consulta o teor de uma comunicação
        
        Args:
            id_comunicacao: ID da comunicação
            incluir_documento: Se deve incluir o documento na resposta
            
        Returns:
            Dicionário com a resposta da consulta
        """
        try:
            # Tenta diferentes nomes de operação possíveis
            operation_names = [
                'consultarTeorComunicacao',
                'ConsultarTeorComunicacao',
                'consultar_teor_comunicacao'
            ]
            
            service = self.client.service
            
            for op_name in operation_names:
                if hasattr(service, op_name):
                    operation = getattr(service, op_name)
                    result = operation(
                        idComunicacao=id_comunicacao,
                        incluirDocumento=incluir_documento
                    )
                    
                    # Converte o resultado para dict
                    return self._zeep_to_dict(result)
            
            # Se não encontrou a operação, lista as disponíveis
            available_ops = [name for name in dir(service) if not name.startswith('_')]
            return {
                'error': 'Operação não encontrada',
                'available_operations': available_ops
            }
            
        except Fault as e:
            self.logger.error(f"SOAP Fault: {e}")
            return {'error': f'SOAP Fault: {str(e)}', 'fault_code': e.code}
        
        except TransportError as e:
            self.logger.error(f"Erro de transporte: {e}")
            return {'error': f'Erro de transporte: {str(e)}'}
        
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return {'error': f'Erro inesperado: {str(e)}'}
    
    def consultar_comunicacoes(self, 
                              numero_processo: str = None,
                              data_inicio: str = None,
                              data_fim: str = None,
                              codigo_orgao: str = None) -> Dict:
        """
        Consulta comunicações por filtros
        
        Args:
            numero_processo: Número do processo
            data_inicio: Data de início (formato YYYY-MM-DD)
            data_fim: Data de fim (formato YYYY-MM-DD)
            codigo_orgao: Código do órgão
            
        Returns:
            Dicionário com a resposta da consulta
        """
        try:
            # Tenta diferentes nomes de operação possíveis
            operation_names = [
                'consultarComunicacoes',
                'ConsultarComunicacoes',
                'consultar_comunicacoes'
            ]
            
            service = self.client.service
            
            # Prepara os parâmetros
            params = {}
            if numero_processo:
                params['numeroProcesso'] = numero_processo
            if data_inicio:
                params['dataInicio'] = data_inicio
            if data_fim:
                params['dataFim'] = data_fim
            if codigo_orgao:
                params['codigoOrgao'] = codigo_orgao
            
            for op_name in operation_names:
                if hasattr(service, op_name):
                    operation = getattr(service, op_name)
                    result = operation(**params)
                    
                    # Converte o resultado para dict
                    return self._zeep_to_dict(result)
            
            # Se não encontrou a operação, lista as disponíveis
            available_ops = [name for name in dir(service) if not name.startswith('_')]
            return {
                'error': 'Operação não encontrada',
                'available_operations': available_ops
            }
            
        except Fault as e:
            self.logger.error(f"SOAP Fault: {e}")
            return {'error': f'SOAP Fault: {str(e)}', 'fault_code': e.code}
        
        except TransportError as e:
            self.logger.error(f"Erro de transporte: {e}")
            return {'error': f'Erro de transporte: {str(e)}'}
        
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return {'error': f'Erro inesperado: {str(e)}'}
    
    def confirmar_recebimento(self, id_comunicacao: str) -> Dict:
        """
        Confirma o recebimento de uma comunicação
        
        Args:
            id_comunicacao: ID da comunicação
            
        Returns:
            Dicionário com a resposta
        """
        try:
            # Tenta diferentes nomes de operação possíveis
            operation_names = [
                'confirmarRecebimento',
                'ConfirmarRecebimento',
                'confirmar_recebimento'
            ]
            
            service = self.client.service
            
            for op_name in operation_names:
                if hasattr(service, op_name):
                    operation = getattr(service, op_name)
                    result = operation(
                        idComunicacao=id_comunicacao,
                        dataRecebimento=datetime.now()
                    )
                    
                    # Converte o resultado para dict
                    return self._zeep_to_dict(result)
            
            # Se não encontrou a operação, lista as disponíveis
            available_ops = [name for name in dir(service) if not name.startswith('_')]
            return {
                'error': 'Operação não encontrada',
                'available_operations': available_ops
            }
            
        except Fault as e:
            self.logger.error(f"SOAP Fault: {e}")
            return {'error': f'SOAP Fault: {str(e)}', 'fault_code': e.code}
        
        except TransportError as e:
            self.logger.error(f"Erro de transporte: {e}")
            return {'error': f'Erro de transporte: {str(e)}'}
        
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return {'error': f'Erro inesperado: {str(e)}'}
    
    def call_operation(self, operation_name: str, **kwargs) -> Dict:
        """
        Chama uma operação genérica do serviço
        
        Args:
            operation_name: Nome da operação
            **kwargs: Parâmetros da operação
            
        Returns:
            Dicionário com a resposta
        """
        try:
            service = self.client.service
            
            if hasattr(service, operation_name):
                operation = getattr(service, operation_name)
                result = operation(**kwargs)
                
                return self._zeep_to_dict(result)
            else:
                available_ops = [name for name in dir(service) if not name.startswith('_')]
                return {
                    'error': f'Operação {operation_name} não encontrada',
                    'available_operations': available_ops
                }
                
        except Fault as e:
            self.logger.error(f"SOAP Fault: {e}")
            return {'error': f'SOAP Fault: {str(e)}', 'fault_code': e.code}
        
        except TransportError as e:
            self.logger.error(f"Erro de transporte: {e}")
            return {'error': f'Erro de transporte: {str(e)}'}
        
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            return {'error': f'Erro inesperado: {str(e)}'}
    
    def _zeep_to_dict(self, obj) -> Any:
        """
        Converte objetos Zeep para dicionários Python
        
        Args:
            obj: Objeto Zeep
            
        Returns:
            Dicionário ou valor convertido
        """
        if obj is None:
            return None
        
        # Se é um objeto complexo do Zeep
        if hasattr(obj, '__dict__'):
            result = {}
            for key, value in obj.__dict__.items():
                if not key.startswith('_'):
                    result[key] = self._zeep_to_dict(value)
            return result
        
        # Se é uma lista
        elif isinstance(obj, list):
            return [self._zeep_to_dict(item) for item in obj]
        
        # Se é um dicionário
        elif isinstance(obj, dict):
            return {key: self._zeep_to_dict(value) for key, value in obj.items()}
        
        # Se é datetime, converte para string ISO
        elif isinstance(obj, datetime):
            return obj.isoformat()
        
        # Outros tipos básicos
        else:
            return obj
    
    def get_wsdl_content(self) -> str:
        """
        Retorna o conteúdo do WSDL
        
        Returns:
            String com o conteúdo do WSDL
        """
        try:
            response = self.session.get(self.wsdl_url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Erro ao baixar WSDL: {e}")
            return f"Erro ao baixar WSDL: {str(e)}"
    
    def close(self):
        """
        Fecha a sessão HTTP
        """
        if self.session:
            self.session.close()


# Context manager para o cliente TJSP
class TJSPClientContext:
    """Context manager para o cliente TJSP com Zeep"""
    
    def __init__(self, username: str = None, password: str = None, timeout: int = 30):
        self.client = TJSPIntercomunicacaoClient(username, password, timeout)
    
    def __enter__(self):
        return self.client
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


# Funções utilitárias (mantidas da versão anterior)
def validar_numero_processo(numero: str) -> bool:
    """
    Valida se o número do processo está no formato correto
    
    Args:
        numero: Número do processo
        
    Returns:
        True se válido, False caso contrário
    """
    # Remove caracteres não numéricos
    numero_limpo = ''.join(filter(str.isdigit, numero))
    
    # Verifica se tem 20 dígitos (padrão CNJ)
    if len(numero_limpo) != 20:
        return False
    
    return True


def formatar_numero_processo(numero: str) -> str:
    """
    Formata o número do processo no padrão CNJ
    
    Args:
        numero: Número do processo
        
    Returns:
        Número formatado
    """
    numero_limpo = ''.join(filter(str.isdigit, numero))
    
    if len(numero_limpo) == 20:
        return f"{numero_limpo[:7]}-{numero_limpo[7:9]}.{numero_limpo[9:13]}.{numero_limpo[13:14]}.{numero_limpo[14:16]}.{numero_limpo[16:20]}"
    
    return numero


# Exemplo de uso com Zeep
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Criar cliente
    try:
        client = TJSPIntercomunicacaoClient()
        
        # Exemplo 1: Ver serviços disponíveis
        print("=== Serviços Disponíveis ===")
        client.print_services()
        
        # Exemplo 2: Obter informações estruturadas dos serviços
        print("\n=== Informações dos Serviços ===")
        services = client.get_services()
        print(f"Serviços: {services}")
        
        # Exemplo 3: Consultar comunicações
        print("\n=== Consultando Comunicações ===")
        resultado = client.consultar_comunicacoes(
            data_inicio="2024-01-01",
            data_fim="2024-01-31"
        )
        print(f"Resultado: {resultado}")
        
        # Exemplo 4: Usar context manager
        print("\n=== Usando Context Manager ===")
        with TJSPClientContext() as client_ctx:
            resultado_ctx = client_ctx.consultar_comunicacoes(
                numero_processo="1234567-89.2024.8.26.0001"
            )
            print(f"Resultado com context: {resultado_ctx}")
    
    except Exception as e:
        print(f"Erro na execução: {e}")