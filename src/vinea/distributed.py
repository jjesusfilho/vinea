
"""Module for listing and downloading distributed PDF files from TJSP."""

import pandas as pd
import re
import requests
from lxml import html
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import numpy as np
from pypdf import PdfReader
from pdfminer.high_level import extract_text


def extract_observations(texto):
    regex_limpar = r"^[\w\W]+?Classe\n"

    header_match = re.search(regex_limpar, texto)
    if not header_match:
        return pd.DataFrame()

    has_foro = "Foro\n" in header_match.group()

    # Join process numbers split across lines (e.g. "4016311-\n90.2026.8.26.0000")
    texto = re.sub(r'(\d+)-\n(\d)', r'\1-\2', texto)

    limpa = re.sub(regex_limpar, '', texto)

    if has_foro:
        colunas = ["comarca", "foro", "vara", "processo", "classe"]
        regex_row = r"(\n?[\w\W]+?\w\n)(\w[\w\W]+?\S\n)(\w[\w\W]+?\S\n)(\w[\w\W]+?\S\n)(\w[\w\W]+?\S\n)"
    else:
        colunas = ["comarca", "vara", "processo", "classe"]
        regex_row = r"(\n?[\w\W]+?\w\n)(\w[\w\W]+?\S\n)(\w[\w\W]+?\S\n)(\w[\w\W]+?\S\n)"

    dados = re.findall(regex_row, limpa)

    df = pd.DataFrame(dados, columns=colunas)
    df[colunas] = df[colunas].apply(lambda col: col.str.strip().str.replace(r"\n", " ", regex=True))

    # Valida número do processo (formato CNJ: NNNNNNN-DD.AAAA.J.TR.OOOO)
    # Remove linhas onde 'processo' não tem o padrão correto
    processo_pattern = r'^\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4}$'
    if 'processo' in df.columns:
        df = df[df['processo'].str.match(processo_pattern, na=False)].copy()

    return df



def parse_distributed(file_path: str):
    regex_dt = r"\d{2}/\d{2}/\d{4}"
    regex_instancia = r"^[\w\W]+?(?=Comarca)"
    regex_meta2 = r"(.+\n)?(.+\n)?(Comarca)"

    reader = PdfReader(file_path)
    pages = [page.extract_text() for page in reader.pages]

    # Use pdfminer only for the first page header: it reconstructs text spacing correctly
    first_page_header = extract_text(file_path, page_numbers=[0])

    dt_match = re.search(regex_dt, first_page_header)
    dt_distribuicao = (
        datetime.strptime(dt_match.group(), "%d/%m/%Y").date() if dt_match else None
    )
    meta_match = re.search(regex_instancia, first_page_header, re.DOTALL)
    meta = meta_match.group() if meta_match else first_page_header
    sistema = re.search("(?<=Sistema ).+", meta).group()
    instancia_area = re.sub(r"[\w\W]+Sistema.+\n", "", meta).strip()

    instancia_match = re.search(".+", instancia_area)
    instancia = instancia_match.group() if instancia_match else np.nan

    area_match = re.search("\n.+", instancia_area)
    area = area_match.group().strip() if area_match else np.nan

    capa = {
        "dt_distribuicao": dt_distribuicao,
        "sistema": sistema,
        "instancia": instancia,
        "area": area,
        "arquivo": Path(file_path).stem,
    }

    df1 = pd.DataFrame(capa, index=[0])

    df2 = extract_observations(pages[0])
    df2["pagina_pdf"] = 1

    df = pd.concat([df1, df2], axis=1)

    for pagina in range(1, len(pages)):
        pagina_text = pages[pagina]
        meta = re.search(regex_meta2, pagina_text)

        df3 = extract_observations(pagina_text)
        df3["pagina_pdf"] = pagina + 1

        if meta and meta.group(2):
            df3["instancia"] = meta.group(1).strip()
            df3["area"] = meta.group(2).strip()
        elif meta and meta.group(1):
            df3["area"] = meta.group(1).strip()

        df = pd.concat([df, df3], axis=0)

    preencher = ["dt_distribuicao", "sistema", "instancia", "area", "arquivo"]
    df[preencher] = df[preencher].ffill()

    return df



BASE_LIST_URL = (
    "https://www.tjsp.jus.br/Processos/Comunicados/ListaDistribuicao"
)
BASE_DOWNLOAD_URL = (
    "https://api.tjsp.jus.br/Handlers/Handler/FileFetch.ashx"
)


def list_distributed_urls(all_pages: bool = False, tipo_destino: int = 3441) -> List[str]:
    """Return a list of PDF URLs for distributed communications.

    Args:
        all_pages: if True, fetch from all available pages.
        tipo_destino: numeric code for destination type in paginated requests.

    Returns:
        List of href strings pointing to PDF files.
    """
    session = requests.Session()
    response = session.get(BASE_LIST_URL)
    response.raise_for_status()
    dom = html.fromstring(response.content)

    link_xpath = "//div[@class='lista-comunicados']//a/@href"
    urls = dom.xpath(link_xpath)

    if all_pages:
        pages_text = dom.xpath("//span[@class='pages']/text()")[0]
        total_pages = int(pages_text.replace("Página 1 de ", ""))
        for page in range(2, total_pages + 1):
            page_url = f"{BASE_LIST_URL}?pagina={page}&tipoDestino={tipo_destino}"
            resp = session.get(page_url)
            resp.raise_for_status()
            dom = html.fromstring(resp.content)
            urls.extend(dom.xpath(link_xpath))

    return urls


def download_distributed(
    codigos: Optional[List[str]],
    diretorio: str = ".",
) -> None:
    """Download PDFs specified by their codes into a target directory.

    Args:
        codigos: list of string codes identifying the files to download.
        diretorio: path where PDF files will be saved (created if missing).
    """
    if not codigos:
        return

    dest_dir = Path(diretorio)
    dest_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    for codigo in codigos:
        download_url = f"{BASE_DOWNLOAD_URL}?codigo={codigo}"
        response = session.get(download_url)
        response.raise_for_status()
        output_path = dest_dir / f"{codigo}.pdf"
        output_path.write_bytes(response.content)