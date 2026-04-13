#!/usr/bin/env python3
"""
Script de exemplo: baixa e analisa dados MNI para um processo específico.
"""
from pathlib import Path

from vinea import MNIClient, MNIParser
from config import config


def main():
    # Carrega a configuração de desenvolvimento e garante existência dos diretórios de dados
    cfg = config["development"]()
    cfg.create_directories()

    # Inicializa o cliente MNI e o parser
    client = MNIClient(cfg.TJSP_MNI_USUARIO, cfg.TJSP_MNI_SENHA)
    parser = MNIParser()

    # Define o número do processo alvo
    processo = "15000385420248260435"

    # Baixa o cabeçalho e o XML de movimentos para o diretório bronze
    header_path = client.consultar_processo(
        numero_processo=processo,
        save_dir=str(cfg.DATA_BRONZE_DIR),
    )
    movimentos_path = client.baixar_movimentos(
        numero_processo=processo,
        save_dir=str(cfg.DATA_BRONZE_DIR),
    )

    # Lista metadados de documentos e baixa os binários (PDF/OCR)
    lista_xml_path = client.listar_documentos(
        numero_processo=processo,
        save_dir=str(cfg.DATA_BRONZE_DIR),
    )
    doc_ids = (
        parser.ler_lista_documentos(lista_xml_path)
        .id_documento.dropna()
        .tolist()
    )
    downloaded_pdfs = client.baixar_documentos(
        numero_processo=processo,
        documentos_ids=doc_ids,
        save_dir=str(cfg.DATA_BRONZE_DIR / "pdfs"),
    )

    

    # Analisa os XMLs salvos em DataFrames pandas
    processo_df, partes_df = parser.extrair_dados_basicos_xml(header_path)
    documentos_df = parser.ler_lista_documentos(lista_xml_path)
    movimentos_df = parser.ler_movimentos(movimentos_path)

    # Exibe resumo dos DataFrames e lista de arquivos baixados
    print("Header DataFrame:")
    print(processo_df)
    print("\nPartes DataFrame:")
    print(partes_df)
    print("\nDocumentos DataFrame:")
    print(documentos_df)
    print("\nMovimentos DataFrame:")
    print(movimentos_df)
    print("\nDownloaded PDFs:")
    print(downloaded_pdfs)


if __name__ == "__main__":
    main()
