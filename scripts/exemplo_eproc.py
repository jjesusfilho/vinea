"""
Exemplo de uso do MNIClient com E-Proc

Este script demonstra como usar o cliente MNI com os diferentes sistemas:
- E-SAJ (sistema tradicional)
- E-Proc 1G (1ª Instância) - versões 2.2 e 3.0
- E-Proc 2G (2ª Instância)
"""

from vinea import (
    create_esaj_client,
    create_eproc1g_client,
    create_eproc2g_client,
    generate_eproc_password,
    MNIClient,
)
from config import config

# Configuração
cfg = config["development"]()
cfg.create_directories()


def exemplo_esaj():
    """Exemplo usando E-SAJ (sistema tradicional)"""
    print("=" * 60)
    print("EXEMPLO 1: E-SAJ (Sistema Tradicional)")
    print("=" * 60)

    # Opção 1: Usando factory method
    client = create_esaj_client(
        usuario=cfg.TJSP_MNI_USUARIO,
        senha=cfg.TJSP_MNI_SENHA
    )

    # Opção 2: Instanciação direta
    # client = MNIClient(
    #     usuario=cfg.TJSP_MNI_USUARIO,
    #     senha=cfg.TJSP_MNI_SENHA,
    #     system="esaj"
    # )

    numero_processo = "00000023120238260631"

    # Consultar processo
    header_path = client.consultar_processo(
        numero_processo,
        save_dir=str(cfg.DATA_BRONZE_DIR)
    )
    print(f"Cabeçalho salvo em: {header_path}")

    # Baixar movimentos
    movimentos_path = client.baixar_movimentos(
        numero_processo,
        save_dir=str(cfg.DATA_BRONZE_DIR)
    )
    print(f"Movimentos salvos em: {movimentos_path}")


def exemplo_eproc1g():
    """Exemplo usando E-Proc 1ª Instância"""
    print("\n" + "=" * 60)
    print("EXEMPLO 2: E-Proc 1G (1ª Instância)")
    print("=" * 60)

    # Usuário E-Proc conforme fornecido
    usuario_eproc = "CAO_CAEx_Consulta_MP"

    # A senha será gerada automaticamente baseada na data atual
    # Formato: SHA256(DD-MM-AAAA + "***REMOVIDO-SEGREDO-EPROC***")
    print(f"Senha gerada para hoje: {generate_eproc_password()}")

    # Opção 1: Usando factory method com senha automática (recomendado)
    client_v22 = create_eproc1g_client(
        usuario=usuario_eproc,
        version="2.2"  # senha será gerada automaticamente
    )

    # Opção 2: Usando versão 3.0
    client_v30 = create_eproc1g_client(
        usuario=usuario_eproc,
        version="3.0"
    )

    # Opção 3: Fornecendo senha manualmente
    # client = create_eproc1g_client(
    #     usuario=usuario_eproc,
    #     senha="senha_customizada",
    #     version="2.2",
    #     auto_password=False
    # )

    # Opção 4: Instanciação direta
    # client = MNIClient(
    #     usuario=usuario_eproc,
    #     senha=generate_eproc_password(),
    #     system="eproc1g_2.2"
    # )

    numero_processo = "1000000-00.2024.8.26.0000"  # Exemplo de número

    print(f"Cliente criado para E-Proc 1G versão 2.2")
    print(f"WSDL: {client_v22.wsdl}")

    # Consultar processo
    # header_path = client_v22.consultar_processo(
    #     numero_processo,
    #     save_dir=str(cfg.DATA_BRONZE_DIR / "eproc1g")
    # )
    # print(f"Cabeçalho salvo em: {header_path}")


def exemplo_eproc2g():
    """Exemplo usando E-Proc 2ª Instância"""
    print("\n" + "=" * 60)
    print("EXEMPLO 3: E-Proc 2G (2ª Instância)")
    print("=" * 60)

    usuario_eproc = "CAO_CAEx_Consulta_MP"

    # Opção 1: Usando factory method com senha automática (recomendado)
    client = create_eproc2g_client(
        usuario=usuario_eproc
        # senha será gerada automaticamente
    )

    # Opção 2: Fornecendo senha manualmente
    # client = create_eproc2g_client(
    #     usuario=usuario_eproc,
    #     senha="senha_customizada",
    #     auto_password=False
    # )

    # Opção 3: Instanciação direta
    # client = MNIClient(
    #     usuario=usuario_eproc,
    #     senha=generate_eproc_password(),
    #     system="eproc2g"
    # )

    numero_processo = "2000000-00.2024.8.26.0000"  # Exemplo de número

    print(f"Cliente criado para E-Proc 2G")
    print(f"WSDL: {client.wsdl}")

    # Consultar processo
    # header_path = client.consultar_processo(
    #     numero_processo,
    #     save_dir=str(cfg.DATA_BRONZE_DIR / "eproc2g")
    # )
    # print(f"Cabeçalho salvo em: {header_path}")


def exemplo_geracao_senha():
    """Demonstra a geração de senha para E-Proc"""
    print("\n" + "=" * 60)
    print("EXEMPLO 4: Geração de Senha E-Proc")
    print("=" * 60)

    from datetime import datetime

    # Senha para hoje
    senha_hoje = generate_eproc_password()
    print(f"Senha para hoje: {senha_hoje}")

    # Senha para uma data específica
    data_especifica = datetime(2026, 4, 13)
    senha_data = generate_eproc_password(date=data_especifica)
    print(f"Senha para 13/04/2026: {senha_data}")

    # Mostrando o formato da entrada antes do hash
    date_str = datetime.now().strftime("%d-%m-%Y")
    secret = "***REMOVIDO-SEGREDO-EPROC***"
    print(f"\nFormato da entrada: {date_str}{secret}")


def exemplo_multiplos_sistemas():
    """Exemplo usando múltiplos sistemas simultaneamente"""
    print("\n" + "=" * 60)
    print("EXEMPLO 5: Usando Múltiplos Sistemas Simultaneamente")
    print("=" * 60)

    # Criar clientes para diferentes sistemas
    clientes = {
        "esaj": create_esaj_client(
            usuario=cfg.TJSP_MNI_USUARIO,
            senha=cfg.TJSP_MNI_SENHA
        ),
        "eproc1g_2.2": create_eproc1g_client(
            usuario="CAO_CAEx_Consulta_MP",
            version="2.2"
        ),
        "eproc1g_3.0": create_eproc1g_client(
            usuario="CAO_CAEx_Consulta_MP",
            version="3.0"
        ),
        "eproc2g": create_eproc2g_client(
            usuario="CAO_CAEx_Consulta_MP"
        ),
    }

    # Mostrar informações de cada cliente
    for sistema, client in clientes.items():
        print(f"\n{sistema}:")
        print(f"  WSDL: {client.wsdl}")
        print(f"  Usuário: {client.usuario}")


if __name__ == "__main__":
    # Executar exemplos
    # exemplo_esaj()  # Descomente para testar E-SAJ
    # exemplo_eproc1g()  # Descomente para testar E-Proc 1G
    # exemplo_eproc2g()  # Descomente para testar E-Proc 2G
    exemplo_geracao_senha()  # Demonstra geração de senha
    exemplo_multiplos_sistemas()  # Mostra configuração de múltiplos sistemas

    print("\n" + "=" * 60)
    print("Exemplos concluídos!")
    print("=" * 60)
