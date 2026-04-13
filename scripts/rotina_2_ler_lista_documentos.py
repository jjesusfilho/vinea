from vinea import MNIParser
from pathlib import Path
from config import config 

cfg = config["development"]()

#### Esta rotina está prevista para rodar via bash_operator no Airflow. ####

diretorio  = Path(cfg.DATA_BRONZE_DIR)

parseador = MNIParser()

lista = []

for arquivo in diretorio.iterdir():
    df = parseador.ler_lista_documentos(arquivo)
    lista.append(df)

dfs = pl.concat(lista)
