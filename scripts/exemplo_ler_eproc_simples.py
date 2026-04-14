"""
Exemplo simples de leitura de dados do E-Proc

Demonstra como extrair informações básicas dos XMLs retornados pelo E-Proc
usando lxml diretamente.
"""

import sys
from pathlib import Path
from lxml import etree

# Adiciona o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))


def extrair_dados_processo(xml_path):
    """Extrai dados básicos de um XML de processo E-Proc"""

    # Lê o arquivo XML
    with open(xml_path, 'rb') as f:  # Lê como bytes
        xml_content = f.read()

    # Parse do XML
    root = etree.fromstring(xml_content)

    # Namespaces do SOAP/CNJ
    namespaces = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns1': 'http://www.cnj.jus.br/tipos-servico-intercomunicacao-2.2.2',
        'ns2': 'http://www.cnj.jus.br/intercomunicacao-2.2.2',
        'ns3': 'http://www.cnj.jus.br/servico-intercomunicacao-2.2.2/'
    }

    # Verifica se a consulta foi bem-sucedida
    sucesso = root.find('.//ns1:sucesso', namespaces)
    mensagem = root.find('.//ns1:mensagem', namespaces)

    print(f"Sucesso: {sucesso.text if sucesso is not None else 'N/A'}")
    print(f"Mensagem: {mensagem.text if mensagem is not None else 'N/A'}")
    print()

    if sucesso is not None and sucesso.text == 'false':
        return None

    # Encontra os dados básicos do processo
    dados_basicos = root.find('.//ns2:dadosBasicos', namespaces)

    if dados_basicos is None:
        print("❌ Dados básicos não encontrados no XML")
        return None

    # Extrai atributos
    dados = {
        'numero': dados_basicos.get('numero'),
        'competencia': dados_basicos.get('competencia'),
        'classe_processual': dados_basicos.get('classeProcessual'),
        'localidade': dados_basicos.get('codigoLocalidade'),
        'nivel_sigilo': dados_basicos.get('nivelSigilo'),
        'intervencao_mp': dados_basicos.get('intervencaoMP'),
        'tamanho': dados_basicos.get('tamanhoProcesso'),
        'data_ajuizamento': dados_basicos.get('dataAjuizamento'),
    }

    # Valor da causa
    valor_causa = dados_basicos.find('.//ns2:valorCausa', namespaces)
    if valor_causa is not None:
        dados['valor_causa'] = float(valor_causa.text) if valor_causa.text else 0

    # Órgão julgador
    orgao = dados_basicos.find('.//ns2:orgaoJulgador', namespaces)
    if orgao is not None:
        dados['orgao_codigo'] = orgao.get('codigoOrgao')
        dados['orgao_nome'] = orgao.get('nomeOrgao')
        dados['instancia'] = orgao.get('instancia')

    # Assuntos
    assuntos = dados_basicos.findall('.//ns2:assunto', namespaces)
    dados['assuntos'] = [
        {
            'codigo': a.find('.//ns2:codigoNacional', namespaces).text,
            'principal': a.get('principal') == 'true'
        }
        for a in assuntos if a.find('.//ns2:codigoNacional', namespaces) is not None
    ]

    # Partes
    polos = dados_basicos.findall('.//ns2:polo', namespaces)
    dados['partes'] = []

    for polo in polos:
        polo_tipo = polo.get('polo')
        partes = polo.findall('.//ns2:parte', namespaces)

        for parte in partes:
            pessoa = parte.find('.//ns2:pessoa', namespaces)
            if pessoa is not None:
                parte_info = {
                    'polo': polo_tipo,
                    'nome': pessoa.get('nome'),
                    'sexo': pessoa.get('sexo'),
                    'data_nascimento': pessoa.get('dataNascimento'),
                    'cpf': pessoa.get('numeroDocumentoPrincipal'),
                    'tipo_pessoa': pessoa.get('tipoPessoa'),
                    'nacionalidade': pessoa.get('nacionalidade'),
                    'advogados': []
                }

                # Endereços
                enderecos = pessoa.findall('.//ns2:endereco', namespaces)
                parte_info['enderecos'] = []
                for end in enderecos:
                    endereco = {
                        'cep': end.get('cep'),
                        'logradouro': end.find('.//ns2:logradouro', namespaces).text if end.find('.//ns2:logradouro', namespaces) is not None else None,
                        'numero': end.find('.//ns2:numero', namespaces).text if end.find('.//ns2:numero', namespaces) is not None else None,
                        'bairro': end.find('.//ns2:bairro', namespaces).text if end.find('.//ns2:bairro', namespaces) is not None else None,
                        'cidade': end.find('.//ns2:cidade', namespaces).text if end.find('.//ns2:cidade', namespaces) is not None else None,
                        'estado': end.find('.//ns2:estado', namespaces).text if end.find('.//ns2:estado', namespaces) is not None else None,
                    }
                    parte_info['enderecos'].append(endereco)

                # Advogados
                advogados = parte.findall('.//ns2:advogado', namespaces)
                for adv in advogados:
                    advogado = {
                        'nome': adv.get('nome'),
                        'oab': adv.get('inscricao'),
                        'cpf': adv.get('numeroDocumentoPrincipal'),
                        'intimacao': adv.get('intimacao') == 'true',
                        'tipo': adv.get('tipoRepresentante')
                    }
                    parte_info['advogados'].append(advogado)

                dados['partes'].append(parte_info)

    return dados


def main():
    print("=" * 70)
    print("LEITURA SIMPLES DE DADOS DO E-PROC")
    print("=" * 70)

    # Caminho do XML baixado
    xml_path = "data/bronze/eproc1g/cabecalho_40006346020258260483.xml"

    if not Path(xml_path).exists():
        print(f"\n❌ Arquivo não encontrado: {xml_path}")
        print("\nExecute primeiro: uv run python scripts/teste_eproc.py")
        return

    print(f"\nLendo arquivo: {xml_path}\n")

    try:
        dados = extrair_dados_processo(xml_path)

        if dados is None:
            return

        # Exibe dados básicos
        print("📋 DADOS BÁSICOS DO PROCESSO")
        print("=" * 70)
        print(f"Número: {dados['numero']}")
        print(f"Competência: {dados['competencia']}")
        print(f"Classe processual: {dados['classe_processual']}")
        print(f"Localidade: {dados['localidade']}")
        print(f"Data de ajuizamento: {dados['data_ajuizamento']}")
        print(f"Valor da causa: R$ {dados.get('valor_causa', 0):,.2f}")
        print(f"Tamanho do processo: {int(dados['tamanho']):,} bytes")
        print(f"Intervenção MP: {dados['intervencao_mp']}")
        print(f"\nÓrgão julgador: {dados.get('orgao_nome', 'N/A')}")
        print(f"Instância: {dados.get('instancia', 'N/A')}")

        # Assuntos
        print(f"\n📚 ASSUNTOS ({len(dados['assuntos'])})")
        print("-" * 70)
        for assunto in dados['assuntos']:
            principal = " [PRINCIPAL]" if assunto['principal'] else ""
            print(f"  • Código: {assunto['codigo']}{principal}")

        # Partes
        print(f"\n👥 PARTES DO PROCESSO ({len(dados['partes'])})")
        print("=" * 70)

        for i, parte in enumerate(dados['partes'], 1):
            print(f"\n{i}. {parte['nome']} [{parte['polo']}]")
            print(f"   CPF: {parte['cpf']}")
            print(f"   Sexo: {parte['sexo']} | Nascimento: {parte['data_nascimento']}")
            print(f"   Tipo: {parte['tipo_pessoa']} | Nacionalidade: {parte['nacionalidade']}")

            # Endereços
            if parte['enderecos']:
                print(f"\n   📍 Endereços ({len(parte['enderecos'])}):")
                for end in parte['enderecos']:
                    print(f"      {end['logradouro']}, {end['numero']} - {end['bairro']}")
                    print(f"      {end['cidade']}/{end['estado']} - CEP: {end['cep']}")

            # Advogados
            if parte['advogados']:
                print(f"\n   ⚖️  Advogados ({len(parte['advogados'])}):")
                for adv in parte['advogados']:
                    intimacao = " [Recebe intimação]" if adv['intimacao'] else ""
                    print(f"      • {adv['nome']} - OAB: {adv['oab']}{intimacao}")

        print("\n" + "=" * 70)
        print("✅ LEITURA CONCLUÍDA COM SUCESSO!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Erro ao processar XML: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
