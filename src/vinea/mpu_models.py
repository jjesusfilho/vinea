"""
Modelos de dados para extração de informações de MPUs (Medidas Protetivas de Urgência).

Baseado no questionário de coleta de dados para MPUs em casos de violência doméstica.
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


# ==================== Enums ====================

class TipoVara(str, Enum):
    """Tipos de vara judicial"""
    VIOLENCIA_DOMESTICA = "Especializada de Violência Doméstica e Familiar contra a Mulher"
    CRIMINAL_COMUM = "Criminal comum"
    CASA_MULHER_BRASILEIRA = "da Casa da Mulher Brasileira"
    FAMILIA = "de Família"
    CIVEL = "Cível"
    JECRIM = "Juizado Especial Criminal (JECRIM)"
    VARA_UNICA = "Vara única"
    OUTRA = "Outra"


class FaixaIdade(str, Enum):
    """Faixas etárias para classificação"""
    CRIANCA = "criança"
    ADOLESCENTE = "adolescente"
    MULHER = "mulher"
    IDOSA = "idosa"


class RacaCor(str, Enum):
    """Raça/cor/etnia"""
    BRANCA = "branca"
    PRETA = "preta"
    PARDA = "parda"
    AMARELA = "amarela"
    INDIGENA = "indígena"
    NAO_INFORMADO = "não informado"


class Escolaridade(str, Enum):
    """Níveis de escolaridade"""
    ANALFABETO = "analfabeto"
    FUNDAMENTAL_INCOMPLETO = "fundamental incompleto"
    FUNDAMENTAL_COMPLETO = "fundamental completo"
    MEDIO_INCOMPLETO = "médio incompleto"
    MEDIO_COMPLETO = "médio completo"
    SUPERIOR_INCOMPLETO = "superior incompleto"
    SUPERIOR_COMPLETO = "superior completo"
    POS_GRADUACAO = "pós-graduação"
    NAO_INFORMADO = "não informado"


class Sexualidade(str, Enum):
    """Orientação sexual"""
    HETEROSSEXUAL = "heterossexual"
    BISSEXUAL = "bissexual"
    HOMOSSEXUAL = "homossexual"
    ASSEXUAL = "assexual"
    OUTRO = "outro"
    NAO_INFORMADO = "não informado"


class IdentidadeGenero(str, Enum):
    """Identidade de gênero"""
    MULHER_CISGENERO = "mulher cisgênero"
    MULHER_TRANSGENERO = "mulher transgênero"
    MULHER_TRAVESTI = "mulher travesti"
    PESSOA_TRANSFEMININA_NAO_BINARIA = "pessoa transfeminina não binária"
    HOMEM_CISGENERO = "homem cisgênero"
    HOMEM_TRANSGENERO = "homem transgênero"
    TRAVESTI = "travesti"
    PESSOA_NAO_BINARIA = "pessoa não binária"
    OUTRO = "outro"
    NAO_INFORMADO = "não informado"


class TipoRelacionamento(str, Enum):
    """Tipos de relacionamento entre vítima e agressor"""
    CONJUGE_ATUAL = "cônjuge/companheiro(a) atual"
    NAMORADO_ATUAL = "namorado(a) atual"
    EX_CONJUGE = "ex-cônjuge/ex-companheiro(a)"
    EX_NAMORADO = "ex-namorado(a)"
    RELACAO_EVENTUAL = "relação eventual/ficante"
    PAI_FILHO = "pai do(s) filho(s)"
    ASCENDENTE = "ascendente (pai, padrasto)"
    DESCENDENTE = "descendente (filho)"
    OUTRO_PARENTE = "outro parente (irmão, tio, primo, etc.)"
    FAMILIAR_AFINIDADE = "outro familiar por afinidade (cunhado, etc.)"
    CONVIVENTE_DOMESTICO = "convivente/doméstico (ex.: enteado, agregado)"
    RELACAO_HOMOAFETIVA = "relação homoafetiva"
    SEM_RELACAO_AFETO = "sem relação íntima de afeto"
    NAO_INFORMADO = "não informado"


class NaturezaViolencia(str, Enum):
    """Naturezas de violência"""
    FISICA = "física"
    PSICOLOGICA = "psicológica"
    MORAL = "moral"
    PATRIMONIAL = "patrimonial"
    SEXUAL = "sexual"
    VIRTUAL = "virtual"
    PERSEGUICAO = "perseguição"
    SIMBOLICA_VICARIA = "simbólica e/ou vicária"


class TipoArma(str, Enum):
    """Tipos de arma"""
    ARMA_FOGO = "arma de fogo"
    ARMA_BRANCA = "arma branca"
    OBJETO_CONTUNDENTE = "objeto contundente"
    OUTRO = "outro"
    NAO_UTILIZADA = "não utilizada"


class TipoDelegacia(str, Enum):
    """Tipos de delegacia"""
    POLICIA_CIVIL = "Polícia Civil"
    ESPECIALIZADA = "especializada"
    DA_MULHER = "da mulher"
    ELETRONICA = "eletrônica"
    OUTRA = "outra"


class TipoBO(str, Enum):
    """Tipos de boletim de ocorrência"""
    PRESENCIAL = "presencial"
    ONLINE = "online"


class PecaInaugural(str, Enum):
    """Tipos de peça inaugural do processo"""
    ADVOGADO_PARTICULAR = "advogado particular"
    DPE = "DPE"
    BO = "BO"
    MP = "MP"
    OUTRA = "outra"


class ManifestacaoMP(str, Enum):
    """Manifestação do Ministério Público"""
    A_FAVOR = "a favor"
    CONTRA = "contra deferimento"


class TipoDecisao(str, Enum):
    """Tipos de decisão judicial"""
    DEFERE = "defere"
    INDEFERE = "indefere"
    ENCAMINHA_SETOR_TECNICO = "encaminha para o setor técnico"
    OUTRO = "outro"


# ==================== Modelos de Dados ====================

class IdentificacaoProcesso(BaseModel):
    """Identificação do processo judicial"""
    numero_processo: str = Field(..., description="Número do processo")
    comarca: Optional[str] = Field(None, description="Comarca do processo")
    vara: Optional[TipoVara] = Field(None, description="Tipo de vara")
    classe_processual: Optional[str] = Field(None, description="Classe processual")


class IdentificacaoFato(BaseModel):
    """Identificação do fato/local da violência"""
    local_fatos_rua: Optional[str] = Field(None, description="Rua onde ocorreram os fatos")
    local_fatos_bairro: Optional[str] = Field(None, description="Bairro onde ocorreram os fatos")
    local_fatos_cidade: Optional[str] = Field(None, description="Cidade onde ocorreram os fatos")
    local_fatos_cep: Optional[str] = Field(None, description="CEP onde ocorreram os fatos")
    local_fatos_latitude: Optional[float] = Field(None, description="Latitude do local dos fatos")
    local_fatos_longitude: Optional[float] = Field(None, description="Longitude do local dos fatos")

    residencia_vitima_rua: Optional[str] = Field(None, description="Rua da residência da vítima")
    residencia_vitima_bairro: Optional[str] = Field(None, description="Bairro da residência da vítima")
    residencia_vitima_cidade: Optional[str] = Field(None, description="Cidade da residência da vítima")
    residencia_vitima_cep: Optional[str] = Field(None, description="CEP da residência da vítima")
    residencia_vitima_latitude: Optional[float] = Field(None, description="Latitude da residência da vítima")
    residencia_vitima_longitude: Optional[float] = Field(None, description="Longitude da residência da vítima")

    residencia_autor_rua: Optional[str] = Field(None, description="Rua da residência do autor da violência")
    residencia_autor_bairro: Optional[str] = Field(None, description="Bairro da residência do autor")
    residencia_autor_cidade: Optional[str] = Field(None, description="Cidade da residência do autor")
    residencia_autor_cep: Optional[str] = Field(None, description="CEP da residência do autor")
    residencia_autor_latitude: Optional[float] = Field(None, description="Latitude da residência do autor")
    residencia_autor_longitude: Optional[float] = Field(None, description="Longitude da residência do autor")

    dia_mes: Optional[int] = Field(None, description="Dia do mês do fato", ge=1, le=31)
    dia_semana: Optional[str] = Field(None, description="Dia da semana do fato")
    periodo_dia: Optional[str] = Field(None, description="Período do dia (manhã, tarde, noite)")
    data_bo: Optional[date] = Field(None, description="Data do registro do BO")


class InformacoesComplementaresVitima(BaseModel):
    """Informações complementares sobre a vítima"""
    gravidez: Optional[bool] = Field(None, description="Está grávida")
    puerperio: Optional[bool] = Field(None, description="Está em puerpério")
    doenca: Optional[bool] = Field(None, description="Possui doença")
    deficiencia: Optional[bool] = Field(None, description="Possui deficiência")
    acompanhamento_saude_mental: Optional[bool] = Field(None, description="Faz acompanhamento em saúde mental")
    dependencia_economica: Optional[bool] = Field(None, description="Possui dependência econômica")
    autonomia_financeira: Optional[bool] = Field(None, description="Possui autonomia financeira")
    rede_apoio: Optional[bool] = Field(None, description="Possui rede de apoio")


class PerfilSociodemograficoVitima(BaseModel):
    """Perfil sociodemográfico da vítima"""
    idade: Optional[int] = Field(None, description="Idade da vítima", ge=0, le=150)
    faixa_idade: Optional[FaixaIdade] = Field(None, description="Faixa etária")
    nacionalidade: Optional[str] = Field(None, description="Nacionalidade")
    raca_cor: Optional[RacaCor] = Field(None, description="Raça/cor/etnia")
    comunidade_quilombola: Optional[bool] = Field(None, description="Pertence a comunidade quilombola")
    povo_indigena: Optional[bool] = Field(None, description="Pertence a povo indígena específico")
    povo_indigena_qual: Optional[str] = Field(None, description="Qual povo indígena")
    escolaridade: Optional[Escolaridade] = Field(None, description="Nível de escolaridade")
    sexualidade: Optional[Sexualidade] = Field(None, description="Orientação sexual")
    identidade_genero: Optional[IdentidadeGenero] = Field(None, description="Identidade de gênero")

    informacoes_complementares: Optional[InformacoesComplementaresVitima] = Field(
        None,
        description="Informações complementares"
    )


class InformacoesComplementaresAutor(BaseModel):
    """Informações complementares sobre o autor da violência"""
    emprego_situacao_financeira: Optional[str] = Field(None, description="Situação de emprego/financeira")
    dependencia_quimica: Optional[bool] = Field(None, description="Possui dependência química")
    doenca_mental: Optional[bool] = Field(None, description="Possui doença mental diagnosticada")
    cargo_poder: Optional[bool] = Field(None, description="Possui cargo de poder/agente público")
    cargo_poder_qual: Optional[str] = Field(None, description="Qual cargo de poder")
    acesso_arma_fogo: Optional[bool] = Field(None, description="Possui acesso a arma de fogo")


class PerfilSociodemograficoAutor(BaseModel):
    """Perfil sociodemográfico do autor da violência"""
    idade: Optional[int] = Field(None, description="Idade do autor", ge=0, le=150)
    nacionalidade: Optional[str] = Field(None, description="Nacionalidade")
    raca_cor: Optional[RacaCor] = Field(None, description="Raça/cor/etnia")
    comunidade_quilombola: Optional[bool] = Field(None, description="Pertence a comunidade quilombola")
    povo_indigena: Optional[bool] = Field(None, description="Pertence a povo indígena específico")
    povo_indigena_qual: Optional[str] = Field(None, description="Qual povo indígena")
    escolaridade: Optional[Escolaridade] = Field(None, description="Nível de escolaridade")
    sexualidade: Optional[Sexualidade] = Field(None, description="Orientação sexual")
    identidade_genero: Optional[IdentidadeGenero] = Field(None, description="Identidade de gênero")

    informacoes_complementares: Optional[InformacoesComplementaresAutor] = Field(
        None,
        description="Informações complementares"
    )


class ComportamentosControle(BaseModel):
    """Comportamentos de controle e isolamento"""
    ciumes_excessivo: Optional[bool] = Field(None, description="Relato de ciúmes excessivo")
    controle_financeiro: Optional[bool] = Field(None, description="Relato de controle financeiro")
    monitoramento_digital: Optional[bool] = Field(None, description="Relato de monitoramento digital")
    proibicao_trabalhar_estudar: Optional[bool] = Field(None, description="Relato de proibição de trabalhar/estudar")
    isolamento_social: Optional[bool] = Field(None, description="Relato de isolamento social")
    impedimento_acesso: Optional[bool] = Field(None, description="Relato de impedimento de acesso à saúde, trabalho, etc")


class DinamicaRelacionamento(BaseModel):
    """Dinâmica do relacionamento entre vítima e autor"""
    tipo_relacionamento: Optional[TipoRelacionamento] = Field(None, description="Tipo de relacionamento")
    tempo_relacionamento: Optional[str] = Field(None, description="Tempo de relacionamento")
    filhos_comum: Optional[bool] = Field(None, description="Possui filhos em comum")
    filhos_comum_quantidade: Optional[int] = Field(None, description="Quantidade de filhos em comum", ge=0)
    filhos_relacionamento_anterior: Optional[bool] = Field(None, description="Filhos de relacionamento anterior")
    conflitos_guarda: Optional[bool] = Field(None, description="Conflitos de guarda")
    conflitos_pensao: Optional[bool] = Field(None, description="Conflitos de pensão")
    conflitos_partilha: Optional[bool] = Field(None, description="Conflitos na partilha")
    residencia_comum: Optional[bool] = Field(None, description="Possuem residência comum")
    empresas_comum: Optional[bool] = Field(None, description="Empresas ou sociedade em comum")
    historico_bo_anterior: Optional[bool] = Field(None, description="Histórico de BO anterior")
    violencia_outras_parceiras: Optional[bool] = Field(None, description="Histórico de violência contra outras parceiras")

    comportamentos_controle: Optional[ComportamentosControle] = Field(
        None,
        description="Comportamentos de controle e isolamento"
    )


class GravidadeObjetiva(BaseModel):
    """Gravidade objetiva do episódio"""
    uso_arma: Optional[bool] = Field(None, description="Houve uso de arma")
    tipo_arma: Optional[TipoArma] = Field(None, description="Tipo de arma utilizada")
    tiro: Optional[bool] = Field(None, description="Houve disparo de arma de fogo")
    estrangulamento: Optional[bool] = Field(None, description="Houve estrangulamento")
    queimadura: Optional[bool] = Field(None, description="Houve queimadura")
    parte_corpo_atingida: Optional[str] = Field(None, description="Parte do corpo atingida")
    atendimento_medico: Optional[bool] = Field(None, description="Houve atendimento médico")
    internacao_medica: Optional[bool] = Field(None, description="Houve internação médica")


class ContextoEpisodio(BaseModel):
    """Contexto do episódio de violência"""
    local: Optional[str] = Field(None, description="Local do episódio (via pública, imóvel particular, etc)")
    presenca_criancas: Optional[bool] = Field(None, description="Havia presença de crianças")
    testemunhas: Optional[bool] = Field(None, description="Havia testemunhas")
    prisao_flagrante: Optional[bool] = Field(None, description="Houve prisão em flagrante")


class CaracterizacaoEpisodio(BaseModel):
    """Caracterização do episódio concreto de violência"""
    naturezas_violencia: Optional[list[NaturezaViolencia]] = Field(
        None,
        description="Naturezas da violência no episódio"
    )
    naturezas_ocorrencia: Optional[list[str]] = Field(
        None,
        description="Naturezas da ocorrência (crimes)"
    )

    gravidade_objetiva: Optional[GravidadeObjetiva] = Field(None, description="Gravidade objetiva")
    contexto: Optional[ContextoEpisodio] = Field(None, description="Contexto do episódio")


class HistoricoEscalada(BaseModel):
    """Histórico de escalada e reincidência"""
    agressoes_anteriores_graves: Optional[bool] = Field(None, description="Relato de agressões anteriores graves")
    frequencia_crescente: Optional[bool] = Field(
        None,
        description="Relato de frequência crescente de agressões nos últimos 6 meses"
    )
    descumprimento_protetivas: Optional[bool] = Field(None, description="Descumprimento anterior de protetivas")
    prisao_anterior: Optional[bool] = Field(None, description="Prisão anterior")
    prisao_anterior_tipo: Optional[str] = Field(
        None,
        description="Tipo de prisão anterior (por descumprimento de MPU, por outro crime)"
    )
    violencia_filhos_familiares_animais: Optional[bool] = Field(
        None,
        description="Relato de violência contra filhos/familiares/animais"
    )
    frases_ameaca: Optional[str] = Field(None, description="Frases de ameaça relatadas")
    ameaca_morte: Optional[bool] = Field(None, description="Ameaça de morte")


class InformacoesBO(BaseModel):
    """Informações do boletim de ocorrência"""
    delegacia: Optional[TipoDelegacia] = Field(None, description="Delegacia em que foi realizado o BO")
    tipo_bo: Optional[TipoBO] = Field(None, description="Tipo de BO (presencial, online)")
    palavras_chave_relato: Optional[list[str]] = Field(
        None,
        description="Palavras-chave encontradas no relato do BO"
    )
    termos_tecnicos: Optional[list[str]] = Field(
        None,
        description="Termos técnicos identificados conforme relatório de risco"
    )
    extensao_relato: Optional[int] = Field(None, description="Extensão do relato em caracteres", ge=0)
    relatorio_risco: Optional[bool] = Field(None, description="Consta relatório de risco junto ao pedido")
    relatorio_risco_completo: Optional[bool] = Field(
        None,
        description="Relatório de risco está completo (se presente)"
    )
    relatorio_violencia_fisica_sexual: Optional[bool] = Field(
        None,
        description="No relatório de risco constam informações sobre violências físicas e sexuais"
    )
    relato_violencia_fisica_sexual: Optional[bool] = Field(
        None,
        description="No relato do BO constam informações sobre violências físicas e sexuais"
    )


class TramiteProcessual(BaseModel):
    """Trâmite processual"""
    peca_inaugural: Optional[PecaInaugural] = Field(None, description="Peça inaugural do processo")
    tipologia: Optional[list[str]] = Field(None, description="Tipologia dos crimes")
    manifestacao_mp: Optional[ManifestacaoMP] = Field(None, description="Manifestação do MP")
    decisao_primeiro_pedido: Optional[TipoDecisao] = Field(None, description="Decisão após primeiro pedido")
    pedido_revogacao_vitima: Optional[bool] = Field(None, description="Pedido de revogação pela vítima")
    manifestacao_mp_revogacao: Optional[ManifestacaoMP] = Field(
        None,
        description="Manifestação do MP sobre a revogação"
    )
    decisao_revogacao: Optional[TipoDecisao] = Field(None, description="Decisão sobre pedido de revogação")
    revogacao_sem_pedido: Optional[bool] = Field(None, description="Revogação sem pedido da vítima")
    pedido_reinstaurar: Optional[bool] = Field(None, description="Pedido para reinstaurar MPU")
    fundamentacao_decisao: Optional[str] = Field(None, description="Fundamentação da decisão")


class LinhaDoTempo(BaseModel):
    """Linha do tempo do processo"""
    data_fato: Optional[date] = Field(None, description="Data do fato")
    data_bo: Optional[date] = Field(None, description="Data do BO")
    data_abertura_autos: Optional[date] = Field(None, description="Data de abertura dos autos")
    data_deferimento_indeferimento: Optional[date] = Field(
        None,
        description="Data do deferimento/indeferimento do pedido"
    )
    data_intimacao_agressor: Optional[date] = Field(None, description="Data da intimação do agressor")
    data_pedido_revogacao: Optional[date] = Field(None, description="Data de pedido de revogação")
    data_decisao_revogacao: Optional[date] = Field(None, description="Data da decisão revogando a MPU")
    data_pedido_renovacao: Optional[date] = Field(None, description="Data de pedido de renovação/restauração")
    data_decisao_renovacao: Optional[date] = Field(
        None,
        description="Data de decisão que renova/restaura MPUs"
    )


class OutrasDemandas(BaseModel):
    """Outras demandas relacionadas ao processo"""
    manifestacao_representar: Optional[bool] = Field(
        None,
        description="Manifestação de interesse em representar criminalmente"
    )
    inquerido_policial: Optional[bool] = Field(None, description="Notícia de instauração de inquérito policial")
    solicitacao_direito_familia: Optional[bool] = Field(
        None,
        description="Solicitação envolvendo direito de família no pedido de MPU"
    )
    restricao_suspensao_visitas: Optional[bool] = Field(
        None,
        description="Restrição ou suspensão de visitas"
    )
    prestacao_alimentos: Optional[bool] = Field(None, description="Prestação de alimentos")
    guarda_filhos: Optional[bool] = Field(None, description="Guarda do(s) filho(s)")
    acao_separacao: Optional[bool] = Field(
        None,
        description="No questionário de atendimento é informada ação de separação"
    )
    processos_varas_familia: Optional[bool] = Field(
        None,
        description="Notícia de distribuição de outro(s) processo(s) em varas de família"
    )


class MPUData(BaseModel):
    """Modelo completo de dados de uma MPU (Medida Protetiva de Urgência)"""

    # Identificação
    identificacao_processo: IdentificacaoProcesso = Field(..., description="Identificação do processo")
    identificacao_fato: Optional[IdentificacaoFato] = Field(None, description="Identificação do fato")

    # Perfis
    perfil_vitima: Optional[PerfilSociodemograficoVitima] = Field(
        None,
        description="Perfil sociodemográfico da vítima"
    )
    perfil_autor: Optional[PerfilSociodemograficoAutor] = Field(
        None,
        description="Perfil sociodemográfico do autor"
    )

    # Relacionamento e episódio
    dinamica_relacionamento: Optional[DinamicaRelacionamento] = Field(
        None,
        description="Dinâmica do relacionamento"
    )
    caracterizacao_episodio: Optional[CaracterizacaoEpisodio] = Field(
        None,
        description="Caracterização do episódio"
    )
    historico_escalada: Optional[HistoricoEscalada] = Field(None, description="Histórico de escalada")

    # Processo
    informacoes_bo: Optional[InformacoesBO] = Field(None, description="Informações do BO")
    tramite_processual: Optional[TramiteProcessual] = Field(None, description="Trâmite processual")
    linha_tempo: Optional[LinhaDoTempo] = Field(None, description="Linha do tempo")
    outras_demandas: Optional[OutrasDemandas] = Field(None, description="Outras demandas")

    # Metadados
    arquivo_fonte: Optional[str] = Field(None, description="Caminho do arquivo fonte")
    data_extracao: datetime = Field(default_factory=datetime.now, description="Data da extração")
