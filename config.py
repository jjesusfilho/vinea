import os
from dotenv import load_dotenv
from pathlib import Path

# Carrega variáveis do arquivo .env
load_dotenv()

_BASE_DIR = Path(__file__).resolve().parent
_VENV_PYTHON = _BASE_DIR / '.venv' / 'bin' / 'python3'
_PYSPARK_PYTHON = os.getenv('PYSPARK_PYTHON', str(_VENV_PYTHON))
_PYSPARK_DRIVER_PYTHON = os.getenv('PYSPARK_DRIVER_PYTHON', str(_VENV_PYTHON))
os.environ.setdefault('PYSPARK_PYTHON', _PYSPARK_PYTHON)
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', _PYSPARK_DRIVER_PYTHON)

class Config:
    """Configuração base da aplicação"""
    
    # Caminhos - BASE_DIR agora aponta para a raiz do projeto (2 níveis acima)
    BASE_DIR = Path(__file__).resolve().parent  # Vai para a raiz do projeto
    
    _VENV_PYTHON = BASE_DIR / '.venv' / 'bin' / 'python3'
    PYSPARK_PYTHON = os.getenv('PYSPARK_PYTHON', str(_VENV_PYTHON))
    PYSPARK_DRIVER_PYTHON = os.getenv('PYSPARK_DRIVER_PYTHON', str(_VENV_PYTHON))
    
    # Diretórios fora de src/vinea
    LOGS_DIR = BASE_DIR / 'logs'
    OUTPUT_DIR = BASE_DIR / 'output'
    DATA_DIR = BASE_DIR / 'data'
    DATA_BRONZE_DIR = DATA_DIR / 'bronze'
    DATA_SILVER_DIR = DATA_DIR / 'silver'
    DATA_GOLD_DIR = DATA_DIR / 'gold'
    
    LOG_FILE = LOGS_DIR / 'app.log'
    ERROR_LOG_FILE = LOGS_DIR / 'errors.log'

    # MNI TJSP - usando as variáveis do seu .env
    TJSP_MNI_USUARIO = os.getenv('TJSPMNIUSUARIO', '')
    TJSP_MNI_SENHA = os.getenv('TJSPMNISENHA', '')
    TJSP_MNI_WSDL = os.getenv('TJSP_MNI_WSDL', 'http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl')

    # E-Proc - usando as variáveis do seu .env
    EPROC_USUARIO = os.getenv('EPROC_USUARIO', 'CAO_CAEx_Consulta_MP')
    EPROC_PASSWORD_SECRET = os.getenv('EPROC_PASSWORD_SECRET', '')  # Segredo para geração de senha SHA-256

    # E-Proc 1G WSDLs
    EPROC_1G_WSDL_2_2 = os.getenv('EPROC_1G_WSDL_2_2', 'https://eproc1gws.tjsp.jus.br/eproc/wsdl.php?srv=intercomunicacaoUnificada2.2')
    EPROC_1G_WSDL_3_0 = os.getenv('EPROC_1G_WSDL_3_0', 'https://eproc1gws.tjsp.jus.br/eproc/wsdl.php?srv=intercomunicacaoUnificada3.0')

    # E-Proc 2G WSDL
    EPROC_2G_WSDL = os.getenv('EPROC_2G_WSDL', 'https://eproc2g.tjsp.jus.br/eproc/ws/intercomunicacao2.2/wsdl/servico-intercomunicacao-2.2.2.wsdl')
    
    # Azure OpenAI - usando as variáveis do seu .env
    AZURE_OPENAI_API_KEY = os.getenv('AZURE_OPENAI_API_KEY', '')
    AZURE_OPENAI_RESOURCE = os.getenv('AZURE_OPENAI_RESOURCE', '')
    AZURE_OPENAI_VERSAO_API = os.getenv('AZURE_OPENAI_VERSAO_API', '2024-02-01')
    AZURE_OPENAI_IMPLEMENTACAO = os.getenv('AZURE_OPENAI_IMPLEMENTACAO', '')
    
    # SQL Server Database
    SQL_SERVER_HOST = os.getenv('SQL_SERVER_HOST', 'localhost')
    SQL_SERVER_PORT = os.getenv('SQL_SERVER_PORT', '1433')
    SQL_SERVER_DATABASE = os.getenv('SQL_SERVER_DATABASE', '')
    SQL_SERVER_USERNAME = os.getenv('SQL_SERVER_USERNAME', '')
    SQL_SERVER_PASSWORD = os.getenv('SQL_SERVER_PASSWORD', '')
    SQL_SERVER_DRIVER = os.getenv('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server')
    
    # Connection string do SQL Server
    @property
    def SQL_SERVER_CONNECTION_STRING(self):
        return (
            f"DRIVER={{{self.SQL_SERVER_DRIVER}}};"
            f"SERVER={self.SQL_SERVER_HOST},{self.SQL_SERVER_PORT};"
            f"DATABASE={self.SQL_SERVER_DATABASE};"
            f"UID={self.SQL_SERVER_USERNAME};"
            f"PWD={self.SQL_SERVER_PASSWORD};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
    
    # SQLAlchemy connection string
    @property
    def SQLALCHEMY_DATABASE_URI(self):
        return (
            f"mssql+pyodbc://{self.SQL_SERVER_USERNAME}:{self.SQL_SERVER_PASSWORD}"
            f"@{self.SQL_SERVER_HOST}:{self.SQL_SERVER_PORT}/{self.SQL_SERVER_DATABASE}"
            f"?driver={self.SQL_SERVER_DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
        )
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # API
    API_TIMEOUT = int(os.getenv('API_TIMEOUT', '30'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

    @classmethod
    def create_directories(cls):
        """Cria os diretórios necessários se não existirem"""
        for attr_name in dir(cls):
            attr_value = getattr(cls, attr_name)
            if isinstance(attr_value, Path) and '_DIR' in attr_name:
                attr_value.mkdir(parents=True, exist_ok=True)


class DevelopmentConfig(Config):
    """Configuração para desenvolvimento"""
    DEBUG = True

class ProductionConfig(Config):
    """Configuração para produção"""
    DEBUG = False

class TestingConfig(Config):
    """Configuração para testes"""
    TESTING = True


# Configurações por ambiente
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
