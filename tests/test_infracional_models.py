from vinea.infracional_models import (
    Crime,
    EntorpecenteApreendido,
    GrauInstrucao,
    PessoaEnvolvida,
    Substancia,
    UnidadeEmbalagem,
)


def test_pessoa_com_grau_de_instrucao():
    p = PessoaEnvolvida(grau_instrucao="2º grau incompleto", grau_instrucao_origem="2º grau incompleto, veio ao plantão")
    assert p.grau_instrucao is GrauInstrucao.MEDIO_INCOMPLETO
    assert PessoaEnvolvida().grau_instrucao is None


def test_crime_com_entorpecentes_contagem_e_massa():
    c = Crime(
        natureza="Tráfico de drogas",
        entorpecentes=[
            EntorpecenteApreendido(
                substancia="cocaína", contagem=25, unidade_embalagem="pino", massa_bruta_gramas=19.2,
                texto_origem="Cocaína: 19,2 g, acondicionada em 25 pinos",
            ),
            EntorpecenteApreendido(substancia="crack", contagem=17, unidade_embalagem="pedra"),
        ],
    )
    cocaina, crack = c.entorpecentes
    assert cocaina.substancia is Substancia.COCAINA and cocaina.unidade_embalagem is UnidadeEmbalagem.PINO
    assert cocaina.massa_liquida_gramas is None
    assert crack.massa_bruta_gramas is None  # só contagem: nunca estima gramas


def test_crime_sem_drogas_continua_valido():
    assert Crime(natureza="Furto").entorpecentes is None


def test_pessoa_preserva_texto_original_do_papel():
    p = PessoaEnvolvida(categoria="adolescente representado", categoria_origem="Autor do fato")
    assert p.categoria.value == "adolescente representado"
    assert p.categoria_origem == "Autor do fato"
