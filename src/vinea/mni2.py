import os
import json
import getpass
import xmltodict
from zeep import Client
from dotenv import load_dotenv


class MNIClient():
    def __self__(usuario: str, senha: str):
        self.wsdl = 'http://esaj.tjsp.jus.br/mniws/servico-intercomunicacao-2.2.2/intercomunicacao?wsdl'
        


