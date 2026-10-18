from vinea.leitura_completa import colunas_desconhecidas, ler_resposta_completa

ESAJ = """<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>
<ns4:consultarProcessoResposta xmlns:ns2="http://www.cnj.jus.br/intercomunicacao-2.2.2"
  xmlns:ns4="http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/">
<sucesso>true</sucesso><mensagem>ok</mensagem><processo>
<ns2:dadosBasicos numero="15004360820258260001" competencia="16" classeProcessual="282" codigoLocalidade="1"
  nivelSigilo="0" intervencaoMP="true" dataAjuizamento="20250109000000">
  <ns2:polo polo="VI"><ns2:parte assistenciaJudiciaria="false" intimacaoPendente="0">
    <ns2:pessoa nome="Vítima Um" sexo="F" tipoPessoa="fisica" dataNascimento="19900101" numeroDocumentoPrincipal="11111111111">
      <ns2:outroNome>Apelido</ns2:outroNome>
      <ns2:documento codigoDocumento="111.111.111-11" emissorDocumento="MF" tipoDocumento="CMF"/>
      <ns2:documento codigoDocumento="12.345.678" emissorDocumento="SSP" tipoDocumento="CI"/>
      <ns2:endereco cep="01000000"><ns2:logradouro>Rua A</ns2:logradouro><ns2:numero>1</ns2:numero>
        <ns2:bairro>Centro</ns2:bairro><ns2:cidade>São Paulo</ns2:cidade><ns2:pais>Brasil</ns2:pais></ns2:endereco>
      <ns2:endereco cep="02000000"><ns2:logradouro>Rua B</ns2:logradouro></ns2:endereco>
    </ns2:pessoa>
    <ns2:pessoaProcessualRelacionada relacionamentoProcessual="RP" assistenciaJudiciaria="false" intimacaoPendente="0">
      <ns2:pessoa nome="Representante" sexo="M" tipoPessoa="fisica"/></ns2:pessoaProcessualRelacionada>
  </ns2:parte></ns2:polo>
  <ns2:polo polo="PA"><ns2:parte assistenciaJudiciaria="true" intimacaoPendente="1">
    <ns2:pessoa nome="Réu" sexo="M" tipoPessoa="fisica"/>
    <ns2:advogado nome="Advogada" inscricao="SP123" numeroDocumentoPrincipal="22222222222" tipoRepresentante="A" intimacao="true">
      <ns2:endereco cep="03000000"><ns2:logradouro>Rua C</ns2:logradouro></ns2:endereco></ns2:advogado>
  </ns2:parte></ns2:polo>
  <ns2:assunto principal="true"><ns2:codigoNacional>12091</ns2:codigoNacional></ns2:assunto>
  <ns2:assunto principal="false"><ns2:assuntoLocal codigoAssunto="50224" codigoPaiNacional="3372" descricao="Prática"/></ns2:assunto>
  <ns2:outroParametro nome="situacaoProcesso" valor="Em andamento"/>
  <ns2:outroParametro nome="numeroProcessoSuperior" valor="0013165-12.2018.8.26.0996"/>
  <ns2:valorCausa>0.0</ns2:valorCausa>
  <ns2:orgaoJulgador codigoOrgao="1" nomeOrgao="1ª Vara do Júri" instancia="1" codigoMunicipioIBGE="3550308"/>
</ns2:dadosBasicos>
<ns2:movimento dataHora="20250109120000" nivelSigilo="0" identificadorMovimento="1">
  <ns2:complemento>primeiro</ns2:complemento><ns2:complemento>segundo</ns2:complemento>
  <ns2:movimentoLocal codigoMovimento="60137" codigoPaiNacional="26" descricao="Distribuído">
    <ns2:movimentoLocalPai codigoMovimento="26" codigoPaiNacional="26" descricao="Distribuição"/></ns2:movimentoLocal>
  <ns2:movimentoNacional codigoNacional="26"/>
</ns2:movimento>
<ns2:movimento dataHora="20250110120000" nivelSigilo="0" identificadorMovimento="2"><ns2:complemento/>
  <ns2:movimentoLocal codigoMovimento="11010" codigoPaiNacional="11010" descricao="Mero expediente"/></ns2:movimento>
</processo></ns4:consultarProcessoResposta></soap:Body></soap:Envelope>"""

EPROC = """<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://www.cnj.jus.br/intercomunicacao-2.2.2"><SOAP-ENV:Body>
<ns1:consultarProcessoResposta><sucesso>true</sucesso><mensagem>ok</mensagem><processo>
<ns1:dadosBasicos numero="61000117520268260081" classeProcessual="7" codigoLocalidade="81" competencia="1"
  nivelSigilo="0" intervencaoMP="false" dataAjuizamento="20261012101010" tamanhoProcesso="1">
  <ns1:polo polo="AT"><ns1:parte assistenciaJudiciaria="false"><ns1:pessoa nome="Autora" sexo="F" tipoPessoa="fisica"/></ns1:parte></ns1:polo>
  <ns1:assunto principal="true"><ns1:codigoNacional>7677</ns1:codigoNacional></ns1:assunto>
  <ns1:magistradoAtuante>Juiz X</ns1:magistradoAtuante>
  <ns1:outroParametro nome="rito" valor="Comum"/>
  <ns1:prioridade>Idoso</ns1:prioridade><ns1:prioridade>Justiça Gratuita - Deferida</ns1:prioridade>
  <ns1:processoVinculado numeroProcesso="00000011120248260081" vinculo="Originário"/>
  <ns1:valorCausa>1000</ns1:valorCausa>
  <ns1:orgaoJulgador codigoOrgao="2" nomeOrgao="2ª Vara" instancia="ORIG" codigoMunicipioIBGE="3500105"/>
</ns1:dadosBasicos>
<ns1:movimento dataHora="20261012101010" identificadorMovimento="9">
  <ns1:movimentoLocal codigoMovimento="1" codigoPaiNacional="26" descricao="Distribuição"/></ns1:movimento>
</processo></ns1:consultarProcessoResposta></SOAP-ENV:Body></SOAP-ENV:Envelope>"""


def test_esaj_completo() -> None:
    t = ler_resposta_completa(ESAJ)
    p = t["processo"].iloc[0]
    assert p["numero"] == "15004360820258260001" and p["classe_processual"] == "282"
    assert p["intervencao_mp"] == "true" and p["orgao_nome_orgao"] == "1ª Vara do Júri"
    assert p["orgao_codigo_municipio_ibge"] == "3550308" and p["valor_causa"] == "0.0"
    assert t["parametros"]["nome"].tolist() == ["situacaoProcesso", "numeroProcessoSuperior"]
    a = t["assuntos"]
    assert a["codigo_nacional"].tolist()[0] == "12091" and a["codigo_local"].tolist()[1] == "50224"
    assert a["principal"].tolist() == ["true", "false"]
    partes = t["partes"].set_index("parte_seq")
    assert partes.loc[1, "polo"] == "VI" and partes.loc[1, "sexo"] == "F"
    assert partes.loc[1, "numero_documento_principal"] == "11111111111"
    assert partes.loc[2, "assistencia_judiciaria"] == "true"
    docs = t["documentos"]
    assert docs[(docs.parte_seq == 1) & (docs.dono_tipo == "parte")]["tipo_documento"].tolist() == ["CMF", "CI"]
    end = t["enderecos"]
    assert len(end[(end.parte_seq == 1) & (end.dono_tipo == "parte")]) == 2
    assert end[end.dono_tipo == "advogado"]["logradouro"].tolist() == ["Rua C"]
    assert t["outros_nomes"]["outro_nome"].tolist() == ["Apelido"]
    rep = t["representantes"].iloc[0]
    assert rep["parte_seq"] == 1 and rep["relacionamento_processual"] == "RP" and rep["nome"] == "Representante"
    adv = t["advogados"].iloc[0]
    assert adv["parte_seq"] == 2 and adv["inscricao"] == "SP123" and adv["tipo_representante"] == "A"
    mov = t["movimentos"]
    assert mov["identificador_movimento"].tolist() == ["1", "2"]
    assert mov.iloc[0]["codigo_movimento_local_pai"] == "26" and mov.iloc[0]["codigo_movimento_nacional"] == "26"
    comp = t["movimentos_complementos"]
    assert comp["complemento"].tolist() == ["primeiro", "segundo"] and set(comp["mov_seq"]) == {1}


def test_eproc_mtom_e_campos_proprios() -> None:
    mtom = (b'--uuid:x\r\nContent-Type: application/xop+xml; type="text/xml"\r\n\r\n'
            + EPROC.encode() + b"\r\n--uuid:x--\r\n")
    t = ler_resposta_completa(mtom)
    p = t["processo"].iloc[0]
    assert p["orgao_instancia"] == "ORIG" and p["magistrado_atuante"] == "Juiz X" and p["tamanho_processo"] == "1"
    assert t["prioridades"]["prioridade"].tolist() == ["Idoso", "Justiça Gratuita - Deferida"]
    assert t["vinculos"].iloc[0]["vinculo"] == "Originário"
    assert t["partes"]["polo"].tolist() == ["AT"] and t["advogados"].empty


def test_sem_processo_devolve_tabelas_vazias() -> None:
    t = ler_resposta_completa("<r><sucesso>false</sucesso></r>")
    assert all(df.empty for df in t.values()) and "movimentos" in t


def test_colunas_desconhecidas() -> None:
    novo = EPROC.replace('tamanhoProcesso="1"', 'tamanhoProcesso="1" campoNovo="x"')
    assert colunas_desconhecidas([ESAJ, novo]) == {"campo_novo"}


def test_extrair_registros_igual_a_tabelas() -> None:
    from vinea.leitura_completa import extrair_registros

    r = extrair_registros(ESAJ)
    t = ler_resposta_completa(ESAJ)
    assert {k: len(v) for k, v in r.items()} == {k: len(v) for k, v in t.items()}
    assert r["partes"][0]["polo"] == "VI"
