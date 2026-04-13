"""
Script para extrair informações de MPUs (Medidas Protetivas de Urgência) de PDFs.

Este script utiliza Azure OpenAI para extrair dados estruturados de processos judiciais.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vinea.mpu_extraction import MPUExtractor, MPUBatchProcessor
from vinea.mpu_parser import MPUParser


def main():
    """Função principal para extrair dados de MPUs."""

    print("=" * 80)
    print("EXTRAÇÃO DE DADOS DE MPUs (Medidas Protetivas de Urgência)")
    print("=" * 80)
    print()

    # Configuração
    input_dir = Path("mpus")  # Diretório com os PDFs
    output_dir = Path("data/mpu_extracted")  # Diretório para os resultados
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Diretório de entrada: {input_dir.absolute()}")
    print(f"Diretório de saída: {output_dir.absolute()}")
    print()

    # Inicializa o extrator (credenciais do .env)
    print("Inicializando extrator com credenciais do .env...")
    extractor = MPUExtractor()

    if not extractor.openai_client:
        print("\n❌ ERRO: Azure OpenAI não foi configurado!")
        print("Verifique se as seguintes variáveis estão no .env:")
        print("  - AZURE_OPENAI_API_KEY")
        print("  - AZURE_OPENAI_RESOURCE")
        print("  - AZURE_OPENAI_IMPLEMENTACAO")
        return 1

    print(f"✓ Azure OpenAI configurado")
    print(f"  - Resource: {extrator.azure_openai_endpoint}")
    print(f"  - Deployment: {extrator.azure_openai_deployment}")
    print(f"  - API Version: {extrator.azure_openai_api_version}")
    print()

    # Processamento em lote
    processor = MPUBatchProcessor(extractor)

    print("Iniciando processamento dos PDFs...")
    print("-" * 80)

    results = processor.process_directory(
        input_dir=input_dir,
        output_dir=output_dir,
        file_pattern="*.pdf",
        save_text=True  # Salva também o texto extraído
    )

    print()
    print("=" * 80)
    print(f"✓ PROCESSAMENTO CONCLUÍDO")
    print(f"  - Total de arquivos processados: {len(results)}")
    print(f"  - Resultados salvos em: {output_dir.absolute()}")
    print("=" * 80)
    print()

    # Análise dos resultados (opcional)
    if results:
        print("Gerando DataFrames para análise...")
        parser = MPUParser()

        dfs = parser.processar_lote_json(
            input_dir=output_dir,
            file_pattern="*.json"
        )

        print()
        print("DataFrames gerados:")
        for nome, df in dfs.items():
            if not df.empty:
                print(f"  - {nome}: {len(df)} registros, {len(df.columns)} colunas")

        # Salva DataFrames em CSV
        csv_dir = output_dir / "csv"
        csv_dir.mkdir(exist_ok=True)

        for nome, df in dfs.items():
            if not df.empty:
                csv_path = csv_dir / f"{nome}.csv"
                df.to_csv(csv_path, index=False, encoding="utf-8")
                print(f"  ✓ {csv_path.name} salvo")

        print()
        print(f"CSVs salvos em: {csv_dir.absolute()}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠ Processamento interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
