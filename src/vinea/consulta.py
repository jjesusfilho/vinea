import base64
import hashlib
import os
import warnings
from datetime import datetime
from typing import Any, Optional, Sequence, Literal
import xml.etree.ElementTree as ET

from zeep import Client
from pyspark.sql import SparkSession

CNJ_NAMESPACE = "http://www.cnj.jus.br/intercomunicacao-2.2.2"

# Tipos de sistema disponíveis
SystemType = Literal["esaj", "eproc1g_2.2", "eproc1g_3.0", "eproc2g"]

# Mapeamento de WSDLs
SYSTEM_WSDLS = {
    "esaj": "http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl",
    "eproc1g_2.2": "https://eproc1gws.tjsp.jus.br/eproc/wsdl.php?srv=intercomunicacaoUnificada2.2",
    "eproc1g_3.0": "https://eproc1gws.tjsp.jus.br/eproc/wsdl.php?srv=intercomunicacaoUnificada3.0",
    "eproc2g": "https://eproc2g.tjsp.jus.br/eproc/ws/intercomunicacao2.2/wsdl/servico-intercomunicacao-2.2.2.wsdl",
}

def generate_eproc_password(secret: Optional[str] = None, date: Optional[datetime] = None) -> str:
    """
    Gera a senha dinâmica para E-Proc usando SHA-256

    A senha é gerada através do hash SHA-256 da concatenação:
    dia-mês-ano + segredo

    Args:
        secret: Segredo fixo para geração da senha (se None, lê de EPROC_PASSWORD_SECRET no .env)
        date: Data para geração da senha (padrão: data atual)

    Returns:
        Hash SHA-256 em formato hexadecimal

    Raises:
        ValueError: Se o segredo não for fornecido e não estiver no .env

    Example:
        >>> # Para hoje (com segredo do .env)
        >>> senha = generate_eproc_password()
        >>> # Para uma data específica
        >>> from datetime import datetime
        >>> senha = generate_eproc_password(date=datetime(2026, 4, 13))
        >>> # Com segredo customizado
        >>> senha = generate_eproc_password(secret="meu_segredo")
    """
    # Se o segredo não foi fornecido, tenta ler do ambiente
    if secret is None:
        secret = os.getenv('EPROC_PASSWORD_SECRET', '')
        if not secret:
            raise ValueError(
                "Segredo não fornecido. Configure EPROC_PASSWORD_SECRET no arquivo .env "
                "ou passe o parâmetro 'secret' para a função."
            )

    if date is None:
        date = datetime.now()

    # Formata a data como DD-MM-AAAA
    date_str = date.strftime("%d-%m-%Y")

    # Concatena data + segredo
    password_input = date_str + secret

    # Gera o hash SHA-256
    password_hash = hashlib.sha256(password_input.encode('utf-8')).hexdigest()

    return password_hash

class MNIClient():
    """
    Cliente para interagir com o MNI do TJSP (ESAJ e E-Proc)

    Suporta os seguintes sistemas:
    - esaj: Sistema E-SAJ tradicional
    - eproc1g_2.2: E-Proc 1ª Instância versão 2.2
    - eproc1g_3.0: E-Proc 1ª Instância versão 3.0
    - eproc2g: E-Proc 2ª Instância
    """

    def __init__(
        self,
        usuario: str,
        senha: str,
        spark: Optional[SparkSession] = None,
        system: SystemType = "esaj",
        wsdl: Optional[str] = None,
        use_spark: bool = True
    ):
        """
        Inicializa o cliente TJSP com Zeep

        Args:
            usuario: Usuário para autenticação
            senha: Senha para autenticação
            spark: Sessão Spark (opcional, será criada se use_spark=True e não fornecida)
            system: Sistema a ser usado ("esaj", "eproc1g_2.2", "eproc1g_3.0", "eproc2g")
            wsdl: WSDL customizado (sobrescreve o padrão do sistema)
            use_spark: Se True, cria/usa Spark; se False, não usa Spark (padrão: True)
        """
        self.system = system
        self.use_spark = use_spark

        # Define o WSDL baseado no sistema ou usa o customizado
        if wsdl:
            self.wsdl = wsdl
        elif system in SYSTEM_WSDLS:
            self.wsdl = SYSTEM_WSDLS[system]
        else:
            raise ValueError(
                f"Sistema '{system}' não reconhecido. "
                f"Opções válidas: {', '.join(SYSTEM_WSDLS.keys())}"
            )

        self.usuario = usuario
        self.senha = senha
        self.client = Client(wsdl=self.wsdl)

        # Inicializa ou usa a sessão Spark fornecida
        if use_spark:
            if spark is None:
                self.spark = SparkSession.builder \
                    .appName(f"MNIClient-{system}") \
                    .getOrCreate()
            else:
                self.spark = spark
        else:
            self.spark = None
    def save_to_xml_file(self, data: Any, save_dir: str, filename: str):
        # Assegura que o diretório existe
        os.makedirs(save_dir, exist_ok=True)

        filepath = os.path.join(save_dir, filename)

        try:
            # Converte bytes para string se necessário
            if isinstance(data, bytes):
                xml_content = data.decode('utf-8')
            else:
                xml_content = str(data)

            # Se Spark está disponível, usa Spark
            if self.use_spark and self.spark is not None:
                # Cria um DataFrame com o conteúdo XML
                df = self.spark.createDataFrame([(xml_content,)], ["value"])

                # Salva usando Spark em formato texto
                # Usa coalesce(1) para gerar um único arquivo de saída
                output_path = filepath
                df.coalesce(1).write.mode("overwrite").text(output_path)

                print(f"Arquivo salvo em: {output_path}")
                return output_path
            else:
                # Salva diretamente como arquivo de texto sem Spark
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(xml_content)

                print(f"Arquivo salvo em: {filepath}")
                return filepath

        except Exception as e:
            raise ValueError(f"Não pode salvar arquivo XML: {e}")

    def validar_numero_processo(self, numero_processo: str):
        numero_limpo = numero_processo.replace(".", "").replace("-", "")
        
        if len(numero_limpo) != 20:
            raise ValueError(f"Número do processo deve ter 20 dígitos após limpeza. "
                            f"Recebido: '{numero_limpo}' com {len(numero_limpo)} dígitos")
        
        if not numero_limpo.isdigit():
            raise ValueError(f"Número do processo deve conter apenas dígitos após limpeza. "
                            f"Recebido: '{numero_limpo}'")
        
        return numero_limpo

    def _consultar_processo_e_salvar(
        self,
        numero_processo: str,
        save_dir: str,
        *,
        filename_prefix: str = "",
        incluir_cabecalho: Optional[bool] = None,
        movimentos: Optional[bool] = None,
        incluir_documentos: Optional[bool] = None,
        documentos: Optional[Sequence[str]] = None,
    ):
        """
        Centraliza a chamada ao serviço MNI e salva o XML retornado.
        """
        numero_limpo = self.validar_numero_processo(numero_processo)

        choice_args = [
            incluir_cabecalho is not None,
            incluir_documentos is not None,
            documentos is not None,
        ]
        if sum(choice_args) == 0:
            raise ValueError("É necessário informar incluir_cabecalho, incluir_documentos ou documento(s) na consulta")
        if sum(choice_args) > 1:
            raise ValueError("Somente um tipo de assinatura (cabeçalho, documentos ou identificadores) pode ser solicitado por vez.")

        try:
            with self.client.settings(raw_response=True, strict=False):
                resposta = self.client.service.consultarProcesso(
                    idConsultante=self.usuario,
                    senhaConsultante=self.senha,
                    numeroProcesso=numero_limpo,
                    movimentos=movimentos,
                    dataReferencia=None,
                    incluirCabecalho=incluir_cabecalho,
                    incluirDocumentos=incluir_documentos,
                    documento=list(documentos) if documentos else None,
                )

            filename = f"{filename_prefix}{numero_limpo}.xml"
            saved_path = self.save_to_xml_file(resposta.content, save_dir, filename)
            print(f"Response saved to: {saved_path}")
            return saved_path

        except Exception as e:
            print(f"Erro ao processar o processo {numero_processo}: {e}")
            return None

    def _read_saved_xml_file(self, saved_path: str) -> str:
        """
        Lê o XML salvo (pode ser arquivo ou diretório gerado pelo Spark) e retorna o texto.
        """
        if os.path.isdir(saved_path):
            for entry in sorted(os.listdir(saved_path)):
                if entry.startswith("_"):
                    continue
                candidate = os.path.join(saved_path, entry)
                if os.path.isfile(candidate):
                    with open(candidate, 'r', encoding='utf-8') as file:
                        return file.read()
            raise FileNotFoundError(f"Nenhum arquivo válido encontrado em {saved_path}")

        with open(saved_path, 'r', encoding='utf-8') as file:
            return file.read()

    def _iter_document_elements(self, xml_content: str | bytes) -> list[ET.Element]:
        if isinstance(xml_content, str):
            xml_content = xml_content.encode('utf-8')
        root = ET.fromstring(xml_content)
        return root.findall(f".//{{{CNJ_NAMESPACE}}}documento")

    def _extract_document_ids(self, xml_content: str | bytes) -> list[str]:
        ids = []
        for document in self._iter_document_elements(xml_content):
            doc_id = document.get("idDocumento")
            if doc_id:
                ids.append(doc_id)
        return ids

    def _sanitize_filename(self, value: str) -> str:
        sanitized = ''.join(c if c.isalnum() else '_' for c in value)
        sanitized = sanitized.strip('_')
        return sanitized or "documento"

    def _mimetype_to_extension(self, mimetype: Optional[str]) -> str:
        if not mimetype:
            return '.bin'
        mimetype = mimetype.lower()
        mapping = {
            'application/pdf': '.pdf',
            'text/plain': '.txt',
            'application/xml': '.xml'
        }
        for prefix, extension in mapping.items():
            if mimetype.startswith(prefix):
                return extension

        if '/' in mimetype:
            return f".{mimetype.split('/')[-1]}"

        return '.bin'

    def _consultar_documentos_com_ids(
        self,
        numero_processo: str,
        documentos_ids: Sequence[str]
    ) -> bytes | None:
        try:
            numero_limpo = self.validar_numero_processo(numero_processo)

            with self.client.settings(raw_response=True, strict=False):
                resposta = self.client.service.consultarProcesso(
                    idConsultante=self.usuario,
                    senhaConsultante=self.senha,
                    numeroProcesso=numero_limpo,
                    documento=list(documentos_ids),
                )

            return resposta.content

        except Exception as e:
            print(f"Erro ao baixar documentos binários do processo {numero_processo}: {e}")
            return None

    def _save_documentos_binarios(
        self,
        xml_content: bytes,
        save_dir: str,
        numero_limpo: str
    ) -> list[str]:
        arquivos_salvos = []
        os.makedirs(save_dir, exist_ok=True)

        for document in self._iter_document_elements(xml_content):
            conteudo = document.find(f"{{{CNJ_NAMESPACE}}}conteudo")
            if conteudo is None or not conteudo.text:
                continue

            try:
                conteudo_bytes = base64.b64decode(conteudo.text)
            except Exception as e:
                print(f"Não foi possível decodificar o documento {document.get('idDocumento')}: {e}")
                continue

            doc_id = document.get("idDocumento", "documento")
            mimetype = document.get("mimetype")
            extension = self._mimetype_to_extension(mimetype)
            filename = f"{numero_limpo}_{self._sanitize_filename(doc_id)}{extension}"
            file_path = os.path.join(save_dir, filename)

            with open(file_path, 'wb') as file_obj:
                file_obj.write(conteudo_bytes)

            arquivos_salvos.append(file_path)

        return arquivos_salvos

    def consultar_processo(self, numero_processo: str, save_dir: str = "."):
        return self._consultar_processo_e_salvar(
            numero_processo,
            save_dir,
            filename_prefix="cabecalho_",
            incluir_cabecalho=True,
        )


    def baixar_movimentos(self, numero_processo: str, save_dir: str = "."):
        return self._consultar_processo_e_salvar(
            numero_processo,
            save_dir,
            filename_prefix="movimentos_",
            movimentos=True,
            incluir_cabecalho=True,
        )

    def listar_documentos(self, numero_processo: str, save_dir: str = "."):
        return self._consultar_processo_e_salvar(
            numero_processo,
            save_dir,
            filename_prefix="lista_documentos_",
            incluir_documentos=True,
        )

    def baixar_documentos(
        self,
        numero_processo: str,
        save_dir: str = ".",
        documentos_ids: Optional[Sequence[str]] = None,
    ):
        numero_limpo = self.validar_numero_processo(numero_processo)
        documentos = list(documentos_ids) if documentos_ids else None

        if not documentos:
            lista_path = self.listar_documentos(numero_processo, save_dir)
            if not lista_path:
                return []

            try:
                lista_xml = self._read_saved_xml_file(lista_path)
            except Exception as e:
                print(f"Erro ao ler a lista de documentos do processo {numero_processo}: {e}")
                return []

            documentos = self._extract_document_ids(lista_xml)

        if not documentos:
            print(f"Nenhum documento foi encontrado para o processo {numero_processo}")
            return []

        documento_binario = self._consultar_documentos_com_ids(numero_processo, documentos)
        if not documento_binario:
            return []

        arquivos = self._save_documentos_binarios(documento_binario, save_dir, numero_limpo)

        if not arquivos:
            print(f"Nenhum conteúdo binário foi retornado para o processo {numero_processo}")
        else:
            print(f"Documentos baixados: {arquivos}")

        return arquivos

    def baixar_lista_documentos(self, numero_processo: str, save_dir: str = "."):
        warnings.warn(
            "Use listar_documentos() em vez de baixar_lista_documentos() para obter apenas a lista.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.listar_documentos(numero_processo, save_dir)


# Factory methods para facilitar a criação de clientes
def create_esaj_client(usuario: str, senha: str, spark: Optional[SparkSession] = None) -> MNIClient:
    """
    Cria um cliente para o sistema E-SAJ

    Args:
        usuario: Usuário para autenticação
        senha: Senha para autenticação
        spark: Sessão Spark opcional

    Returns:
        MNIClient configurado para E-SAJ
    """
    return MNIClient(usuario=usuario, senha=senha, spark=spark, system="esaj")


def create_eproc1g_client(
    usuario: str,
    senha: Optional[str] = None,
    spark: Optional[SparkSession] = None,
    version: Literal["2.2", "3.0"] = "2.2",
    auto_password: bool = True
) -> MNIClient:
    """
    Cria um cliente para o sistema E-Proc 1ª Instância

    Args:
        usuario: Usuário para autenticação
        senha: Senha para autenticação (se None e auto_password=True, será gerada automaticamente)
        spark: Sessão Spark opcional
        version: Versão da API ("2.2" ou "3.0")
        auto_password: Se True, gera a senha automaticamente usando a data atual

    Returns:
        MNIClient configurado para E-Proc 1G
    """
    if senha is None and auto_password:
        senha = generate_eproc_password()
    elif senha is None:
        raise ValueError("Senha deve ser fornecida ou auto_password deve ser True")

    system = f"eproc1g_{version}"
    return MNIClient(usuario=usuario, senha=senha, spark=spark, system=system)


def create_eproc2g_client(
    usuario: str,
    senha: Optional[str] = None,
    spark: Optional[SparkSession] = None,
    auto_password: bool = True
) -> MNIClient:
    """
    Cria um cliente para o sistema E-Proc 2ª Instância

    Args:
        usuario: Usuário para autenticação
        senha: Senha para autenticação (se None e auto_password=True, será gerada automaticamente)
        spark: Sessão Spark opcional
        auto_password: Se True, gera a senha automaticamente usando a data atual

    Returns:
        MNIClient configurado para E-Proc 2G
    """
    if senha is None and auto_password:
        senha = generate_eproc_password()
    elif senha is None:
        raise ValueError("Senha deve ser fornecida ou auto_password deve ser True")

    return MNIClient(usuario=usuario, senha=senha, spark=spark, system="eproc2g")
