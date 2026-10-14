from vinea import MNIParser

XML = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<ns4:consultarProcessoResposta xmlns:ns2="http://www.cnj.jus.br/intercomunicacao-2.2.2"
  xmlns:ns4="http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/">
<sucesso>true</sucesso><mensagem>ok</mensagem>
<processo>
  <ns2:dadosBasicos numero="00001934120258260001" competencia="600" classeProcessual="417"
    codigoLocalidade="9061" nivelSigilo="0" intervencaoMP="false" dataAjuizamento="20250109000000">
    <ns2:polo polo="AT"><ns2:parte><ns2:pessoa nome="Fulano" tipoPessoa="fisica"/></ns2:parte></ns2:polo>
    <ns2:outroParametro nome="situacaoProcesso" valor="Suspenso"/>
    <ns2:outroParametro nome="numeroProcessoSuperior" valor="0013165-12.2018.8.26.0996"/>
    <ns2:orgaoJulgador codigoOrgao="27" nomeOrgao="Turma Recursal Criminal" instancia="2" codigoMunicipioIBGE="3550308"/>
  </ns2:dadosBasicos>
  <ns2:movimento dataHora="20250109115703" nivelSigilo="0" identificadorMovimento="1">
    <ns2:movimentoLocal codigoMovimento="60137" codigoPaiNacional="26" descricao="Recebidos os Autos"/>
  </ns2:movimento>
  <ns2:movimento dataHora="20250109151541" nivelSigilo="0" identificadorMovimento="2">
    <ns2:movimentoNacional codigoNacional="26"/>
  </ns2:movimento>
</processo>
</ns4:consultarProcessoResposta></soap:Body></soap:Envelope>"""


def test_ler_resposta_bytes() -> None:
    r = MNIParser().ler_resposta(XML.encode("utf-8"))
    processo = r["processo"].iloc[0]
    assert processo["numero_processo"] == "00001934120258260001"
    assert processo["instancia"] == "2"
    assert processo["situacao_processo"] == "Suspenso"
    assert processo["numero_processo_superior"] == "0013165-12.2018.8.26.0996"
    assert len(r["partes"]) == 1
    movimentos = r["movimentos"]
    assert movimentos["numero_processo"].unique().tolist() == ["00001934120258260001"]
    assert movimentos["codigo_movimento_local"].tolist()[0] == 60137
    assert movimentos["codigo_movimento_nacional"].tolist()[1] == 26
    assert str(movimentos["data_hora"].dt.tz) == "America/Sao_Paulo"


def test_ler_resposta_mtom() -> None:
    mtom = (
        b'--uuid:abc\r\nContent-Type: application/xop+xml; type="text/xml"\r\n\r\n'
        + XML.encode("utf-8")
        + b"\r\n--uuid:abc--\r\n"
    )
    assert MNIParser().ler_resposta(mtom)["processo"].iloc[0]["instancia"] == "2"


def test_extrair_numero_remove_prefixos(tmp_path) -> None:
    p = MNIParser()
    for prefixo in ("cabecalho_", "movimentos_", "lista_documentos_", ""):
        arquivo = tmp_path / f"{prefixo}00001934120258260001.xml"
        arquivo.write_text(XML, encoding="utf-8")
        assert p.extrair_numero_tempo(str(arquivo))[0] == "00001934120258260001"
