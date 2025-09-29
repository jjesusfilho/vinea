from vinea import MNIClient
from vinea import config
import pyodbc
import polars as pl

#### Esta rotina está prevista para rodar via bash_operator no Airflow. ####

env = "development"

configClass = config[env]
cfg = configClass()

conn = pyodbc.connect(cfg.SQL_SERVER_CONNECTION_STRING)


query = """select  processo from tjsp.mni_dados_basicos p1 
        where competencia = '26'
        and  data_ajuizamento > '2022-04-30'"""

df = pl.read_database(query = query , connection = conn)


cliente = MNIClient(cfg.TJSP_MNI_USUARIO, cfg.TJSP_MNI_SENHA)

for processo in df["processo"]:
        cliente.baixar_lista_documentos(processo, cfg.DATA_BRONZE_DIR)

