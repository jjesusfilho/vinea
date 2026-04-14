"""
Script para baixar amostras de processos distribuídos hoje

Este script:
1. Baixa e processa o PDF de distribuição do dia atual do TJSP
2. Extrai os números dos processos de 1ª Instância
3. Baixa 100 amostras usando ESAJ (movimentos com cabeçalho) usando consulta.py
4. Salva os dados na pasta bronze
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import time

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from vinea.distributed import list_distributed_urls, download_distributed, parse_distributed
from vinea import MNIClient, generate_eproc_password
from config import config

# Configuração
cfg = config["development"]()
cfg.create_directories()


def separar_por_sistema(df: pd.DataFrame) -> tuple[list, list]:
    """
    Separa os processos por sistema usando a coluna 'sistema' do DataFrame

    Args:
        df: DataFrame com os processos distribuídos (deve conter coluna 'sistema')

    Returns:
        Tupla com (lista_esaj, lista_eproc)
    """
    if df.empty or 'processo' not in df.columns or 'sistema' not in df.columns:
        return [], []

    # Filtra por sistema usando a coluna do próprio CSV
    df_esaj = df[df['sistema'].str.lower() == 'esaj']
    df_eproc = df[df['sistema'].str.lower() == 'eproc']

    processos_esaj = df_esaj['processo'].dropna().unique().tolist()
    processos_eproc = df_eproc['processo'].dropna().unique().tolist()

    return processos_esaj, processos_eproc


def baixar_amostra_esaj(processos: list, limite: int, save_dir: str):
    """
    Baixa amostras de processos do ESAJ (movimentos com cabeçalho)

    Args:
        processos: Lista de números de processos
        limite: Número máximo de processos a baixar
        save_dir: Diretório onde salvar os dados
    """
    print(f"\n{'=' * 70}")
    print(f"BAIXANDO {min(len(processos), limite)} PROCESSOS DO ESAJ")
    print(f"{'=' * 70}\n")

    try:
        # Cria cliente ESAJ sem Spark
        client = MNIClient(
            usuario=cfg.TJSP_MNI_USUARIO,
            senha=cfg.TJSP_MNI_SENHA,
            system="esaj",
            use_spark=False
        )

        sucesso = 0
        erro = 0

        # Baixa até o limite especificado
        for i, processo in enumerate(processos[:limite], 1):
            print(f"[{i}/{min(len(processos), limite)}] Processando {processo}...")

            try:
                # Baixa movimentos (inclui cabeçalho)
                movimentos_path = client.baixar_movimentos(
                    numero_processo=processo,
                    save_dir=save_dir
                )

                if movimentos_path:
                    print("  ✓ Movimentos salvos")
                    sucesso += 1
                else:
                    print("  ✗ Erro ao baixar movimentos")
                    erro += 1

                # Pequeno delay para não sobrecarregar o servidor
                time.sleep(1)

            except Exception as e:
                print(f"  ✗ Erro: {e}")
                erro += 1
                continue

        print(f"\n{'=' * 70}")
        print(f"RESUMO ESAJ: {sucesso} sucessos, {erro} erros")
        print(f"{'=' * 70}\n")

    except Exception as e:
        print(f"❌ Erro ao criar cliente ESAJ: {e}")


def baixar_amostra_eproc(processos: list, limite: int, save_dir: str):
    """
    Baixa amostras de processos do E-Proc (movimentos com cabeçalho)

    Args:
        processos: Lista de números de processos
        limite: Número máximo de processos a baixar
        save_dir: Diretório onde salvar os dados
    """
    print(f"\n{'=' * 70}")
    print(f"BAIXANDO {min(len(processos), limite)} PROCESSOS DO E-PROC")
    print(f"{'=' * 70}\n")

    try:
        # Gera senha do E-Proc
        senha = generate_eproc_password()

        # Cria cliente E-Proc 1G sem Spark
        client = MNIClient(
            usuario=cfg.EPROC_USUARIO,
            senha=senha,
            system="eproc1g_2.2",
            use_spark=False
        )

        sucesso = 0
        erro = 0

        # Baixa até o limite especificado
        for i, processo in enumerate(processos[:limite], 1):
            print(f"[{i}/{min(len(processos), limite)}] Processando {processo}...")

            try:
                # Baixa movimentos (inclui cabeçalho)
                movimentos_path = client.baixar_movimentos(
                    numero_processo=processo,
                    save_dir=save_dir
                )

                if movimentos_path:
                    print("  ✓ Movimentos salvos")
                    sucesso += 1
                else:
                    print("  ✗ Erro ao baixar movimentos")
                    erro += 1

                # Pequeno delay para não sobrecarregar o servidor
                time.sleep(1)

            except Exception as e:
                print(f"  ✗ Erro: {e}")
                erro += 1
                continue

        print(f"\n{'=' * 70}")
        print(f"RESUMO E-PROC: {sucesso} sucessos, {erro} erros")
        print(f"{'=' * 70}\n")

    except Exception as e:
        print(f"❌ Erro ao criar cliente E-Proc: {e}")


def main():
    """Função principal do script"""
    print(f"{'=' * 70}")
    print(f"DOWNLOAD DE AMOSTRAS DE PROCESSOS DISTRIBUÍDOS")
    print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'=' * 70}\n")

    # Configurações
    LIMITE_POR_SISTEMA = 100  # Quantidade de processos por sistema

    # Diretórios
    temp_dir = cfg.DATA_BRONZE_DIR / "distributed_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    save_dir_esaj = str(cfg.DATA_BRONZE_DIR / "esaj")

    # Passo 1: Baixar PDFs de distribuição de hoje
    print("Passo 1: Listando URLs de distribuição de hoje...")
    try:
        urls = list_distributed_urls(all_pages=False)
        print(f"✓ Encontradas {len(urls)} URLs de distribuição")

        if not urls:
            print("❌ Nenhuma URL de distribuição encontrada para hoje")
            return

    except Exception as e:
        print(f"❌ Erro ao listar URLs: {e}")
        return

    # Passo 2: Baixar todos os PDFs
    print(f"\nPasso 2: Baixando {len(urls)} PDFs de distribuição...")
    try:
        codigos = []
        for url in urls:
            codigo = url.split("=")[-1] if "=" in url else None
            if codigo:
                codigos.append(codigo)

        if not codigos:
            print("❌ Não foi possível extrair códigos das URLs")
            return

        print(f"Códigos: {codigos}")
        download_distributed(codigos, diretorio=str(temp_dir))

        # Verifica se os arquivos foram baixados
        pdf_paths = [temp_dir / f"{codigo}.pdf" for codigo in codigos]
        pdfs_baixados = [p for p in pdf_paths if p.exists()]

        print(f"✓ {len(pdfs_baixados)} PDFs baixados com sucesso")

    except Exception as e:
        print(f"❌ Erro ao baixar PDFs: {e}")
        return

    # Passo 3: Processar todos os PDFs e extrair processos
    print(f"\nPasso 3: Processando {len(pdfs_baixados)} PDFs e extraindo processos...")
    try:
        dfs = []
        for pdf_path in pdfs_baixados:
            try:
                df = parse_distributed(str(pdf_path))
                dfs.append(df)
                print(f"  ✓ {pdf_path.name}: {len(df)} registros")
            except Exception as e:
                print(f"  ✗ Erro ao processar {pdf_path.name}: {e}")
                continue

        if not dfs:
            print("❌ Nenhum PDF foi processado com sucesso")
            return

        # Concatena todos os DataFrames
        df_distribuidos = pd.concat(dfs, ignore_index=True)
        print(f"\n✓ Total extraído: {len(df_distribuidos)} registros de {len(dfs)} PDFs")

        # Mostra informações sobre o DataFrame
        print(f"\nColunas disponíveis: {list(df_distribuidos.columns)}")
        print(f"\nInstâncias encontradas:")
        if 'instancia' in df_distribuidos.columns:
            print(df_distribuidos['instancia'].value_counts())

        print(f"\nSistemas encontrados:")
        if 'sistema' in df_distribuidos.columns:
            print(df_distribuidos['sistema'].value_counts())

    except Exception as e:
        print(f"❌ Erro ao processar PDFs: {e}")
        import traceback
        traceback.print_exc()
        return

    # Passo 4: Filtrar apenas 1ª Instância
    print("\nPasso 4: Filtrando processos de 1ª Instância...")
    try:
        if 'instancia' in df_distribuidos.columns:
            # Filtra por 1ª Instância
            mask_1inst = df_distribuidos['instancia'].str.contains('1', na=False) | \
                         df_distribuidos['instancia'].str.contains('Primeira', na=False, case=False)
            df_primeira_instancia = df_distribuidos[mask_1inst].copy()
        else:
            print("⚠ Coluna 'instancia' não encontrada. Usando todos os processos.")
            df_primeira_instancia = df_distribuidos.copy()

        print(f"✓ Encontrados {len(df_primeira_instancia)} processos de 1ª Instância")

        if df_primeira_instancia.empty:
            print("❌ Nenhum processo de 1ª Instância encontrado")
            return

    except Exception as e:
        print(f"❌ Erro ao filtrar instância: {e}")
        return

    # Passo 5: Filtrar apenas processos SAJ (ESAJ)
    print("\nPasso 5: Filtrando processos SAJ (ESAJ)...")
    try:
        df_saj = df_primeira_instancia[df_primeira_instancia['sistema'] == 'SAJ'].copy()
        processos = df_saj['processo'].dropna().unique().tolist()
        print(f"✓ Total de {len(processos)} processos SAJ encontrados")

        if not processos:
            print("⚠ Nenhum processo SAJ encontrado, tentando com todos os processos...")
            processos = df_primeira_instancia['processo'].dropna().unique().tolist()

    except Exception as e:
        print(f"❌ Erro ao filtrar processos SAJ: {e}")
        return

    # Passo 6: Baixar amostras usando ESAJ (tenta todos os processos via ESAJ)
    print(f"\nPasso 6: Baixando {min(len(processos), LIMITE_POR_SISTEMA)} amostras via ESAJ...")
    if processos:
        baixar_amostra_esaj(processos, LIMITE_POR_SISTEMA, save_dir_esaj)
    else:
        print("\n⚠ Nenhum processo para baixar")

    # Salva a lista de processos em CSV para referência
    print("\nSalvando lista de processos processados...")
    try:
        output_csv = cfg.DATA_BRONZE_DIR / f"processos_distribuidos_{datetime.now().strftime('%Y%m%d')}.csv"
        df_primeira_instancia.to_csv(output_csv, index=False)
        print(f"✓ Lista salva em: {output_csv}")
    except Exception as e:
        print(f"⚠ Erro ao salvar CSV: {e}")

    print(f"\n{'=' * 70}")
    print("SCRIPT CONCLUÍDO!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
