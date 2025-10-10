processo = '10171177420238260068'

from vinea import MNIClient
from config import config


env = "development"

configClass = config[env]
cfg = configClass()

cliente = MNIClient(cfg.TJSP_MNI_USUARIO, cfg.TJSP_MNI_SENHA)

cliente.consultar_processo(processo, save_dir = cfg.DATA_BRONZE_DIR)
