processo = '00000024420248260449'

from vinea import MNIClient
from config import config


env = "development"

configClass = config[env]
cfg = configClass()

cliente = MNIClient(cfg.TJSP_MNI_USUARIO, cfg.TJSP_MNI_SENHA)

cliente.consultar_processo(processo, save_dir = cfg.DATA_BRONZE_DIR)


from vinea import MNIParser

parser = MNIParser()

dados_basicos, partes = parser.extrair_dados_basicos_xml("/dados_app/usuarios/jose/projetos/vinea/data/bronze/10171177420238260068_time_1758575370.xml")
