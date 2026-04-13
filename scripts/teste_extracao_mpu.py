"""
Script de teste para extrair informações de um único PDF de MPU.
"""

import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vinea.mpu_extraction import MPUExtractor


def main():
    """Testa extração de dados de um único PDF."""

    print("=" * 80)
    print("TESTE DE EXTRAÇÃO DE MPU - UM ARQUIVO")
    print("=" * 80)
    print()

    # Escolhe um PDF para testar (o menor)
    pdf_path = Path("mpus/1500147-95.2024.8.26.0232.pdf")

    if not pdf_path.exists():
        print(f"❌ Arquivo não encontrado: {pdf_path}")
        return 1

    print(f"Arquivo de teste: {pdf_path.name}")
    print(f"Tamanho: {pdf_path.stat().st_size / 1024 / 1024:.2f} MB")
    print()

    # Inicializa o extrator
    print("Inicializando extrator...")
    extractor = MPUExtractor()

    if not extractor.openai_client:
        print("\n❌ ERRO: Azure OpenAI não configurado!")
        return 1

    print(f"✓ Configurado")
    print(f"  - Deployment: {extractor.azure_openai_deployment}")
    print()

    # Extrai texto do PDF
    print("=" * 80)
    print("ETAPA 1: Extração de texto do PDF")
    print("=" * 80)
    try:
        text_content = extractor.extract_text_from_pdf(pdf_path)
        print(f"✓ Texto extraído: {len(text_content)} caracteres")
        print(f"\nPrimeiras 500 caracteres:")
        print("-" * 80)
        print(text_content[:500])
        print("-" * 80)
        print()
    except Exception as e:
        print(f"❌ Erro ao extrair texto: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Extrai dados estruturados
    print("=" * 80)
    print("ETAPA 2: Extração de dados estruturados com LLM")
    print("=" * 80)
    print("Enviando para Azure OpenAI...")
    print("(Isso pode levar alguns segundos...)")
    print()

    try:
        numero_processo = pdf_path.stem.replace("-", "").replace(".", "")[:20]
        mpu_data = extractor.extract_mpu_data_with_llm(
            text_content=text_content,
            numero_processo=numero_processo,
            max_completion_tokens=4000,
            temperature=0.1
        )

        print("✓ Dados extraídos com sucesso!")
        print()

        # Geocodifica endereços
        print("=" * 80)
        print("ETAPA 3: Geocodificação de Endereços")
        print("=" * 80)
        if mpu_data.identificacao_fato:
            from vinea.geocoding import MPUGeocoder
            geocoder = MPUGeocoder()
            geocoder.geocode_mpu_addresses(mpu_data)
            print()
        else:
            print("⚠ Sem endereços para geocodificar")
            print()

        # Mostra os dados extraídos
        print("=" * 80)
        print("DADOS EXTRAÍDOS")
        print("=" * 80)
        print()

        # Identificação do processo
        print("IDENTIFICAÇÃO DO PROCESSO:")
        print(f"  - Número: {mpu_data.identificacao_processo.numero_processo}")
        print(f"  - Comarca: {mpu_data.identificacao_processo.comarca}")
        print(f"  - Vara: {mpu_data.identificacao_processo.vara}")
        print(f"  - Classe processual: {mpu_data.identificacao_processo.classe_processual}")
        print()

        # Perfil da vítima
        if mpu_data.perfil_vitima:
            print("PERFIL DA VÍTIMA:")
            print(f"  - Idade: {mpu_data.perfil_vitima.idade}")
            print(f"  - Raça/cor: {mpu_data.perfil_vitima.raca_cor}")
            print(f"  - Escolaridade: {mpu_data.perfil_vitima.escolaridade}")
            print()

        # Perfil do autor
        if mpu_data.perfil_autor:
            print("PERFIL DO AUTOR:")
            print(f"  - Idade: {mpu_data.perfil_autor.idade}")
            print(f"  - Raça/cor: {mpu_data.perfil_autor.raca_cor}")
            print(f"  - Escolaridade: {mpu_data.perfil_autor.escolaridade}")
            print()

        # Relacionamento
        if mpu_data.dinamica_relacionamento:
            print("DINÂMICA DO RELACIONAMENTO:")
            print(f"  - Tipo: {mpu_data.dinamica_relacionamento.tipo_relacionamento}")
            print(f"  - Tempo: {mpu_data.dinamica_relacionamento.tempo_relacionamento}")
            print(f"  - Filhos em comum: {mpu_data.dinamica_relacionamento.filhos_comum}")
            print()

        # Episódio
        if mpu_data.caracterizacao_episodio:
            print("CARACTERIZAÇÃO DO EPISÓDIO:")
            if mpu_data.caracterizacao_episodio.naturezas_violencia:
                print(f"  - Naturezas de violência: {', '.join([n.value for n in mpu_data.caracterizacao_episodio.naturezas_violencia])}")
            if mpu_data.caracterizacao_episodio.gravidade_objetiva:
                print(f"  - Uso de arma: {mpu_data.caracterizacao_episodio.gravidade_objetiva.uso_arma}")
                print(f"  - Estrangulamento: {mpu_data.caracterizacao_episodio.gravidade_objetiva.estrangulamento}")
            print()

        # Linha do tempo
        if mpu_data.linha_tempo:
            print("LINHA DO TEMPO:")
            print(f"  - Data do fato: {mpu_data.linha_tempo.data_fato}")
            print(f"  - Data do BO: {mpu_data.linha_tempo.data_bo}")
            print(f"  - Data de abertura: {mpu_data.linha_tempo.data_abertura_autos}")
            print()

        # Coordenadas geográficas
        if mpu_data.identificacao_fato:
            fato = mpu_data.identificacao_fato
            print("COORDENADAS GEOGRÁFICAS:")
            if fato.local_fatos_latitude and fato.local_fatos_longitude:
                print(f"  - Local dos fatos: {fato.local_fatos_latitude}, {fato.local_fatos_longitude}")
            if fato.residencia_vitima_latitude and fato.residencia_vitima_longitude:
                print(f"  - Residência vítima: {fato.residencia_vitima_latitude}, {fato.residencia_vitima_longitude}")
            if fato.residencia_autor_latitude and fato.residencia_autor_longitude:
                print(f"  - Residência autor: {fato.residencia_autor_latitude}, {fato.residencia_autor_longitude}")
            print()

        # Salva resultado
        output_path = Path("data/teste_mpu.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        extractor.save_mpu_data(mpu_data, output_path)
        print(f"✓ Resultado salvo em: {output_path.absolute()}")
        print()

        # Mostra JSON completo
        print("=" * 80)
        print("JSON COMPLETO (primeiros 2000 caracteres):")
        print("=" * 80)
        json_str = mpu_data.model_dump_json(indent=2, exclude_none=True)
        print(json_str[:2000])
        if len(json_str) > 2000:
            print(f"\n... ({len(json_str) - 2000} caracteres restantes)")
        print()

        print("=" * 80)
        print("✅ TESTE CONCLUÍDO COM SUCESSO!")
        print("=" * 80)
        return 0

    except Exception as e:
        print(f"\n❌ Erro ao extrair dados estruturados: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
