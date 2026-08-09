"""
Modelos de dados para extração de informações de processos de apuração de
ato infracional (infância e juventude) a partir do Boletim de Ocorrência e
dos documentos de qualificação das partes (Auto de Qualificação, Petição
(Outras), Representação, Informações sobre Antecedentes do Adolescente).
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

from .mpu_models import RacaCor


# ==================== Enums ====================

class CategoriaPessoa(str, Enum):
    """Categoria da pessoa envolvida no processo"""
    ADOLESCENTE_REPRESENTADO = "adolescente representado"
    VITIMA = "vítima"
    REPRESENTANTE_LEGAL = "representante legal"
    TESTEMUNHA = "testemunha"
    COMUNICANTE = "comunicante"
    AUTORIDADE_POLICIAL = "autoridade policial"
    CONDUTOR = "condutor"
    OUTRO = "outro"


class Sexo(str, Enum):
    """Sexo, conforme registrado no BO"""
    MASCULINO = "masculino"
    FEMININO = "feminino"
    NAO_INFORMADO = "não informado"


class ConsumadoTentado(str, Enum):
    """Se o ato infracional/crime foi consumado ou tentado"""
    CONSUMADO = "consumado"
    TENTADO = "tentado"


# ==================== Modelos de Dados ====================

class IdentificacaoProcesso(BaseModel):
    """Identificação do processo judicial"""
    numero_processo: str = Field(..., description="Número do processo")
    comarca: Optional[str] = Field(None, description="Comarca do processo")
    vara: Optional[str] = Field(None, description="Vara (ex.: Vara da Infância e Juventude)")
    classe_processual: Optional[str] = Field(None, description="Classe processual")
    competencia: Optional[str] = Field(None, description="Código de competência do MNI")


class Crime(BaseModel):
    """Um crime/natureza dentro de um boletim de ocorrência (pode haver mais de um por BO)"""
    especie: Optional[str] = Field(None, description="Espécie da natureza (ex.: Crimes contra o patrimônio)")
    natureza: Optional[str] = Field(None, description="Natureza/tipificação (ex.: Furto qualificado)")
    objeto_material: Optional[str] = Field(
        None, description="Objeto material da conduta, quando aplicável (ex.: tipo de droga apreendida)"
    )
    consumado_ou_tentado: Optional[ConsumadoTentado] = Field(None, description="Consumado ou tentado")


class BoletimOcorrencia(BaseModel):
    """
    Um boletim de ocorrência do processo. Um mesmo processo pode ter mais de
    um BO (mais de uma delegacia/dependência envolvida).
    """
    numero_boletim: Optional[str] = Field(None, description="Número do boletim de ocorrência")
    dependencia: Optional[str] = Field(None, description="Delegacia/dependência policial que lavrou o BO")
    circunscricao: Optional[str] = Field(None, description="Circunscrição policial")
    tipo_local: Optional[str] = Field(None, description="Tipo do local do fato (via pública, residência etc.)")

    endereco_fato_rua: Optional[str] = Field(None, description="Logradouro do local do fato")
    endereco_fato_bairro: Optional[str] = Field(None, description="Bairro do local do fato")
    endereco_fato_cidade: Optional[str] = Field(None, description="Cidade/município do local do fato")
    endereco_fato_cep: Optional[str] = Field(None, description="CEP do local do fato")
    endereco_fato_latitude: Optional[float] = Field(None, description="Latitude do local do fato")
    endereco_fato_longitude: Optional[float] = Field(None, description="Longitude do local do fato")

    data_ocorrencia: Optional[datetime] = Field(None, description="Data/hora do fato")
    data_comunicacao: Optional[datetime] = Field(None, description="Data/hora da comunicação à polícia")
    data_elaboracao: Optional[datetime] = Field(None, description="Data/hora de elaboração do BO")

    flagrante: Optional[bool] = Field(None, description="Se houve flagrante")
    autoria_conhecida: Optional[bool] = Field(None, description="Se a autoria era conhecida no momento do BO")

    crimes: Optional[list[Crime]] = Field(None, description="Crimes/naturezas registrados neste BO")


class PessoaEnvolvida(BaseModel):
    """
    Uma pessoa envolvida no processo (adolescente representado, vítima,
    representante legal, testemunha etc.). A qualificação completa vem
    principalmente dos documentos de qualificação, não do BO em si.
    """
    categoria: Optional[CategoriaPessoa] = Field(None, description="Papel da pessoa no processo")
    nome: Optional[str] = Field(None, description="Nome completo")
    data_nascimento: Optional[date] = Field(None, description="Data de nascimento")
    sexo: Optional[Sexo] = Field(None, description="Sexo")
    cor_raca: Optional[RacaCor] = Field(None, description="Raça/cor/etnia")

    # Para adolescentes é comum não haver RG ainda — CPF costuma ser o
    # identificador mais confiável quando presente.
    rg: Optional[str] = Field(None, description="Número do RG, se houver")
    cpf: Optional[str] = Field(None, description="Número do CPF — identificador mais confiável quando presente")

    naturalidade: Optional[str] = Field(None, description="Naturalidade")
    estado_civil: Optional[str] = Field(None, description="Estado civil")
    profissao: Optional[str] = Field(None, description="Profissão/ocupação")
    nome_pai: Optional[str] = Field(None, description="Nome do pai")
    nome_mae: Optional[str] = Field(None, description="Nome da mãe")

    endereco_rua: Optional[str] = Field(None, description="Logradouro da residência")
    endereco_bairro: Optional[str] = Field(None, description="Bairro da residência")
    endereco_cidade: Optional[str] = Field(None, description="Cidade da residência")
    endereco_cep: Optional[str] = Field(None, description="CEP da residência")
    endereco_latitude: Optional[float] = Field(None, description="Latitude da residência")
    endereco_longitude: Optional[float] = Field(None, description="Longitude da residência")

    presente_ao_plantao: Optional[bool] = Field(None, description="Se esteve presente ao plantão policial")
    presente_advogado: Optional[bool] = Field(None, description="Se havia advogado presente")


class ProcessoInfracionalData(BaseModel):
    """Modelo completo de dados extraídos de um processo de ato infracional"""

    identificacao_processo: IdentificacaoProcesso = Field(..., description="Identificação do processo")
    boletins_ocorrencia: Optional[list[BoletimOcorrencia]] = Field(
        None, description="Boletins de ocorrência do processo (pode haver mais de um)"
    )
    pessoas: Optional[list[PessoaEnvolvida]] = Field(
        None, description="Pessoas envolvidas no processo"
    )

    # Metadados
    arquivo_fonte: Optional[str] = Field(None, description="Caminho/identificador do(s) documento(s) fonte")
    data_extracao: datetime = Field(default_factory=datetime.now, description="Data da extração")
