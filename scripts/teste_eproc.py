"""
Script de teste para E-Proc

Testa a consulta de processo no sistema E-Proc usando o cliente MNI atualizado.
Processo de exemplo: 4000634-60.2025.8.26.0483
"""

import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from vinea import create_eproc1g_client, create_eproc2g_client, generate_eproc_password
from config import config

# Configuração
cfg = config["development"]()
cfg.create_directories()


def main():
    print("=" * 70)
    print("TESTE DE CONSULTA E-PROC")
    print("=" * 70)

    # Número do processo fornecido
    numero_processo = "4000634-60.2025.8.26.0483"
    usuario = cfg.EPROC_USUARIO

    print(f"\nProcesso: {numero_processo}")
    print(f"Usuário: {usuario}")

    # Gera e mostra a senha para hoje
    try:
        senha = generate_eproc_password()
        print(f"Senha gerada para hoje: {senha}")
    except ValueError as e:
        print(f"\nERRO: {e}")
        print("\nConfigure o .env com o segredo do E-Proc:")
        print("Adicione ao seu arquivo .env:")
        print("EPROC_PASSWORD_SECRET=seu_segredo")
        return

    print("\n" + "-" * 70)
    print("Testando E-Proc 1G (1ª Instância) - Versão 2.2")
    print("-" * 70)

    try:
        # Cria cliente E-Proc 1G sem Spark
        from vinea import MNIClient
        client = MNIClient(
            usuario=usuario,
            senha=senha,
            system="eproc1g_2.2",
            use_spark=False  # Não usar Spark para este teste
        )

        print(f"WSDL: {client.wsdl}")
        print(f"Sistema: {client.system}")

        # Diretório de saída
        save_dir = str(cfg.DATA_BRONZE_DIR / "eproc1g")
        print(f"Diretório de saída: {save_dir}\n")

        # Tenta consultar o processo
        print("Consultando cabeçalho do processo...")
        header_path = client.consultar_processo(
            numero_processo,
            save_dir=save_dir
        )

        if header_path:
            print(f"✓ Cabeçalho salvo em: {header_path}")

            # Tenta baixar movimentos
            print("\nConsultando movimentos do processo...")
            movimentos_path = client.baixar_movimentos(
                numero_processo,
                save_dir=save_dir
            )

            if movimentos_path:
                print(f"✓ Movimentos salvos em: {movimentos_path}")

            # Tenta listar documentos
            print("\nListando documentos do processo...")
            lista_path = client.listar_documentos(
                numero_processo,
                save_dir=save_dir
            )

            if lista_path:
                print(f"✓ Lista de documentos salva em: {lista_path}")

        print("\n" + "=" * 70)
        print("TESTE CONCLUÍDO COM SUCESSO!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ ERRO durante a consulta: {e}")
        print(f"\nTipo do erro: {type(e).__name__}")

        # Mostra mais detalhes do erro se disponível
        import traceback
        print("\nDetalhes do erro:")
        traceback.print_exc()

        print("\n" + "=" * 70)
        print("POSSÍVEIS CAUSAS:")
        print("=" * 70)
        print("1. Credenciais incorretas (usuário ou senha)")
        print("2. Processo não existe ou não está disponível no E-Proc 1G")
        print("3. Problema de conectividade com o servidor")
        print("4. WSDL incorreto ou serviço indisponível")
        print("\nVerifique:")
        print(f"- Usuário: {usuario}")
        print(f"- Senha gerada: {senha}")
        print(f"- WSDL: {client.wsdl if 'client' in locals() else 'N/A'}")


if __name__ == "__main__":
    main()
