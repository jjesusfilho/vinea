"""
Autenticação por certificado digital A1 (.pfx) no e-SAJ e no e-Proc do TJSP.

As funções deste módulo devolvem uma ``requests.Session`` autenticada
(cookies); elas não fazem nenhuma consulta de processo — isso fica a cargo
de quem consome a sessão (ex.: injetando os cookies num cliente do
``videre``).
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import utils as asym_utils
from cryptography.hazmat.primitives.serialization import Encoding, pkcs12

ESAJ_BASE_URL = "https://esaj.tjsp.jus.br"

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


class AutenticacaoCertificadoError(Exception):
    """Erro ao autenticar com certificado digital A1 no e-SAJ/e-Proc."""


def _resolver_credenciais(
    pfx_path: Optional[str], pfx_password: Optional[str]
) -> tuple[str, str]:
    pfx_path = pfx_path or os.getenv("CERTIFICADOTJSP")
    pfx_password = pfx_password or os.getenv("SENHACERTIFICADO")
    if not pfx_path:
        raise AutenticacaoCertificadoError(
            "Informe pfx_path ou configure a variável de ambiente CERTIFICADOTJSP."
        )
    if not pfx_password:
        raise AutenticacaoCertificadoError(
            "Informe pfx_password ou configure a variável de ambiente SENHACERTIFICADO."
        )
    return pfx_path, pfx_password


def _carregar_pfx(pfx_path: str, pfx_password: str):
    """Decifra o .pfx/.p12 e devolve (chave_privada, certificado)."""
    if not Path(pfx_path).exists():
        raise AutenticacaoCertificadoError(f"Arquivo de certificado não encontrado: {pfx_path}")

    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    try:
        key, cert, _chain = pkcs12.load_key_and_certificates(pfx_data, pfx_password.encode())
    except ValueError as e:
        raise AutenticacaoCertificadoError(f"Certificado .pfx inválido ou senha incorreta: {e}") from e

    if key is None or cert is None:
        raise AutenticacaoCertificadoError("Certificado .pfx sem chave privada ou certificado.")

    return key, cert


def autenticar_certificado_esaj(
    pfx_path: Optional[str] = None,
    pfx_password: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> requests.Session:
    """
    Autentica no e-SAJ do TJSP com certificado digital A1 (.pfx).

    Replica o desafio-resposta do CAS do e-SAJ: a página de login embute um
    ``hashDesafio`` (SHA-256, base64); a chave privada do certificado assina
    esse hash (RSA PKCS#1 v1.5) e o certificado + assinatura são enviados de
    volta — o mesmo fluxo que o WebSigner faz no navegador, sem handshake
    TLS mútuo. Porta de ``tjsp_autenticar_certificado`` (pacote R ``tjsp``,
    https://github.com/jjesusfilho/tjsp).

    Args:
        pfx_path: Caminho do arquivo .pfx/.p12. Se omitido, usa a variável
            de ambiente ``CERTIFICADOTJSP``.
        pfx_password: Senha do certificado. Se omitida, usa a variável de
            ambiente ``SENHACERTIFICADO``.
        session: Sessão `requests` a reaproveitar. Se omitida, cria uma nova.

    Returns:
        `requests.Session` autenticada (cookies de sessão do CAS do e-SAJ).

    Raises:
        AutenticacaoCertificadoError: certificado inválido/senha incorreta,
            `hashDesafio` não encontrado na página de login, ou assinatura
            recusada pelo CAS.

    Example:
        >>> session = autenticar_certificado_esaj()
        >>> resp = session.get(
        ...     "https://esaj.tjsp.jus.br/cpopg/search.do",
        ...     params={"conversationId": "", "cbPesquisa": "DOCPARTE"},
        ...     verify=False,
        ... )
    """
    pfx_path, pfx_password = _resolver_credenciais(pfx_path, pfx_password)
    key, cert = _carregar_pfx(pfx_path, pfx_password)
    cert_der = cert.public_bytes(Encoding.DER)

    session = session or requests.Session()
    session.headers.setdefault("User-Agent", _USER_AGENT)

    # Acesso inicial ao portal — necessário para obter os cookies de sessão
    # que o CAS espera já existirem antes do login.
    session.get(f"{ESAJ_BASE_URL}/esaj/portal.do?servico=740000", verify=False, timeout=30)

    login_url = (
        f"{ESAJ_BASE_URL}/sajcas/login?service="
        f"{quote(ESAJ_BASE_URL + '/esaj/j_spring_cas_security_check', safe='')}"
    )
    resp = session.get(login_url, verify=False, timeout=30)
    if resp.status_code != 200:
        raise AutenticacaoCertificadoError(f"Erro ao acessar página de login: {resp.status_code}")

    execution_match = re.search(r'name="execution"\s+value="([^"]*)"', resp.text)
    if not execution_match:
        raise AutenticacaoCertificadoError(
            "Parâmetro 'execution' não encontrado na página de login do CAS "
            "(o e-SAJ pode ter mudado)."
        )
    execution = execution_match.group(1)

    hash_match = re.search(r"hashDesafio\s*=\s*'([^']+)'", resp.text)
    if not hash_match:
        raise AutenticacaoCertificadoError(
            "hashDesafio não encontrado na página de login do CAS (o e-SAJ pode ter mudado)."
        )
    digest = base64.b64decode(hash_match.group(1))

    assinatura = key.sign(digest, padding.PKCS1v15(), asym_utils.Prehashed(hashes.SHA256()))

    # O action do form traz o jsessionid quando disponível — usar quando existir.
    action_match = re.search(r'id="formCertificado" action="([^"]+)"', resp.text)
    post_url = (
        f"{ESAJ_BASE_URL}{action_match.group(1).replace('&amp;', '&')}"
        if action_match
        else login_url
    )

    session.post(
        post_url,
        data={
            "lt": "",
            "execution": execution,
            "_eventId": "submit",
            "token": "",
            "certificadoSelecionado": base64.b64encode(cert_der).decode(),
            "signature": base64.b64encode(assinatura).decode(),
        },
        verify=False,
        timeout=30,
    )

    check = session.get(f"{ESAJ_BASE_URL}/sajcas/verificarLogin.js", verify=False, timeout=30)
    if "true" not in check.text.lower():
        raise AutenticacaoCertificadoError(
            "Login por certificado recusado pelo CAS do e-SAJ (verifique .pfx/senha "
            "e se o certificado está cadastrado no e-SAJ)."
        )

    return session
