"""
Exemplo de processamento de dados do E-Proc

Demonstra como extrair e analisar informações dos XMLs retornados pelo E-Proc
usando o MNIParser.
"""

import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from vinea import MNIParser


def main():
    print("=" * 70)
    print("PROCESSANDO DADOS DO E-PROC")
    print("=" * 70)

    # Caminho do XML baixado
    xml_path = "data/bronze/eproc1g/cabecalho_40006346020258260483.xml"

    if not Path(xml_path).exists():
        print(f"\n❌ Arquivo não encontrado: {xml_path}")
        print("\nExecute primeiro: uv run python scripts/teste_eproc.py")
        return

    # Cria o parser
    parser = MNIParser(use_spark=False)

    print(f"\nLendo arquivo: {xml_path}")
    print("-" * 70)

    # Extrai dados básicos e partes do processo
    try:
        processo_df, partes_df = parser.extrair_dados_basicos_xml(xml_path)

        print("\n📊 DADOS BÁSICOS DO PROCESSO")
        print("=" * 70)
        print(processo_df.to_string())

        print("\n\n👥 PARTES DO PROCESSO")
        print("=" * 70)
        print(partes_df.to_string())

        # Informações resumidas
        print("\n\n📋 RESUMO")
        print("=" * 70)

        if not processo_df.empty:
            row = processo_df.iloc[0]
            print(f"Número do processo: {row['numero']}")
            print(f"Classe processual: {row.get('classe_processual', 'N/A')}")
            print(f"Data de ajuizamento: {row.get('data_ajuizamento', 'N/A')}")
            print(f"Valor da causa: R$ {row.get('valor_causa', 'N/A')}")
            print(f"Órgão julgador: {row.get('orgao_julgador_nome', 'N/A')}")

        if not partes_df.empty:
            print(f"\nTotal de partes: {len(partes_df)}")
            print("\nPartes por polo:")
            for polo in partes_df['polo'].unique():
                partes_polo = partes_df[partes_df['polo'] == polo]
                print(f"  - {polo}: {len(partes_polo)} parte(s)")
                for _, parte in partes_polo.iterrows():
                    print(f"    • {parte['nome']} (CPF: {parte.get('cpf', 'N/A')})")

        print("\n" + "=" * 70)
        print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Erro ao processar XML: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
