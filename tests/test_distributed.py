from vinea.distributed import _dv_cnj_valido, corrigir_numero, extract_observations


def test_dv_cnj_valido() -> None:
    assert _dv_cnj_valido("00332467020268260100")
    assert not _dv_cnj_valido("00332467120268260100")


def test_corrigir_numero_recoloca_zeros() -> None:
    assert corrigir_numero("3324670-20.2682.6.01.0100") == "0033246-70.2026.8.26.0100"


def test_corrigir_numero_mantem_numero_valido() -> None:
    assert corrigir_numero("1001098-46.2026.8.26.0081") == "1001098-46.2026.8.26.0081"


def test_corrigir_numero_mantem_outro_tribunal() -> None:
    assert corrigir_numero("5000001-23.2026.4.03.6100") == "5000001-23.2026.4.03.6100"


def test_extract_observations_junta_numero_quebrado_eproc() -> None:
    texto = (
        "Processos distribuídos em \n12/10/2026\nSistema eproc\nPrimeira Instância/Colégio Recursal\n"
        "Comarca\nVara\nNº processo\nClasse\n"
        "Adamantina\n2ª Vara da Comarca de Adamantina\n4003432-\n02.2026.8.26.0081\nProcedimento Comum Cível\n"
    )
    df = extract_observations(texto)
    assert df["processo"].tolist() == ["4003432-02.2026.8.26.0081"]
    assert df["vara"].tolist() == ["2ª Vara da Comarca de Adamantina"]


def test_extract_observations_com_foro_saj() -> None:
    texto = (
        "Processos distribuídos em \n12/10/2026\nSistema \nSAJ\nPrimeira Instância\nCível\n"
        "Comarca\nForo\nVara\nProcesso\nClasse\n"
        "Adamantina\nForo de Adamantina\n3ª Vara\n1001098-46.2026.8.26.0081\nReconhecimento e Extinção de \nUnião Estável\n"
    )
    df = extract_observations(texto)
    assert df.iloc[0]["foro"] == "Foro de Adamantina"
    assert df.iloc[0]["processo"] == "1001098-46.2026.8.26.0081"
