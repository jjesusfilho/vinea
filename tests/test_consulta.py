from datetime import datetime

import pytest

from vinea.consulta import FUSO_TJSP, MNIErro, _verificar_sucesso, generate_eproc_password

SUCESSO = (
    b'<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>'
    b'<ns2:consultarProcessoResposta xmlns:ns2="http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/">'
    b"<sucesso>true</sucesso><mensagem>Consulta realizada com sucesso.</mensagem>"
    b"</ns2:consultarProcessoResposta></soap:Body></soap:Envelope>"
)
FALHA = SUCESSO.replace(b"<sucesso>true", b"<sucesso>false").replace(
    b"Consulta realizada com sucesso.", b"Processo n\xc3\xa3o encontrado"
)
FAULT = (
    b'<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault>'
    b"<faultcode>soap:Server</faultcode><faultstring>Senha inv\xc3\xa1lida</faultstring>"
    b"</soap:Fault></soap:Body></soap:Envelope>"
)


def test_verificar_sucesso_aceita_true() -> None:
    _verificar_sucesso(SUCESSO, "1")


def test_verificar_sucesso_lanca_com_mensagem() -> None:
    with pytest.raises(MNIErro, match="Processo não encontrado"):
        _verificar_sucesso(FALHA, "1")


def test_verificar_sucesso_lanca_com_fault() -> None:
    with pytest.raises(MNIErro, match="Senha inválida"):
        _verificar_sucesso(FAULT, "1")


def test_verificar_sucesso_mtom() -> None:
    mtom = (
        b"--uuid:abc\r\nContent-Type: application/xop+xml; type=\"text/xml\"\r\n\r\n"
        + SUCESSO
        + b"\r\n--uuid:abc--\r\n"
    )
    _verificar_sucesso(mtom, "1")


def test_senha_eproc_usa_data_informada() -> None:
    import hashlib

    esperado = hashlib.sha256(b"13-04-2026segredo").hexdigest()
    assert generate_eproc_password("segredo", datetime(2026, 4, 13)) == esperado


def test_senha_eproc_padrao_usa_fuso_de_sao_paulo() -> None:
    hoje = datetime.now(FUSO_TJSP)
    assert generate_eproc_password("segredo") == generate_eproc_password("segredo", hoje)
