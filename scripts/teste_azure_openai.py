"""
Script de teste para verificar conexão com Azure OpenAI.
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
from openai import AzureOpenAI

# Carrega variáveis de ambiente
load_dotenv()

def test_azure_openai():
    """Testa a conexão com Azure OpenAI."""

    # Lê credenciais do .env
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    resource = os.getenv("AZURE_OPENAI_RESOURCE")
    deployment = os.getenv("AZURE_OPENAI_IMPLEMENTACAO")
    api_version = os.getenv("AZURE_OPENAI_VERSAO_API", "2024-02-01")

    print("=== Teste de Conexão Azure OpenAI ===\n")
    print(f"Resource: {resource}")
    print(f"Deployment: {deployment}")
    print(f"API Version: {api_version}")
    print(f"API Key: {'*' * 20}{api_key[-10:] if api_key else 'NOT SET'}\n")

    if not api_key or not resource or not deployment:
        print("❌ ERRO: Credenciais não configuradas no .env")
        print("Verifique se as seguintes variáveis estão definidas:")
        print("  - AZURE_OPENAI_API_KEY")
        print("  - AZURE_OPENAI_RESOURCE")
        print("  - AZURE_OPENAI_IMPLEMENTACAO")
        return False

    try:
        # Monta o endpoint
        if not resource.startswith("https://"):
            endpoint = f"https://{resource}.openai.azure.com"
        else:
            endpoint = resource

        print(f"Endpoint: {endpoint}\n")

        # Cria cliente
        print("Criando cliente Azure OpenAI...")
        client = AzureOpenAI(
            api_key=api_key,
            api_version=api_version,
            azure_endpoint=endpoint
        )

        print("✓ Cliente criado com sucesso\n")

        # Testa chamada simples
        print("Enviando mensagem de teste...")
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "Você é um assistente útil."
                },
                {
                    "role": "user",
                    "content": "Responda apenas: 'Conexão bem-sucedida!'"
                }
            ],
            temperature=0.0,
            max_completion_tokens=50  # Updated parameter name for newer models
        )

        resposta = response.choices[0].message.content
        print(f"\n✓ Resposta recebida: {resposta}")

        # Informações sobre o uso
        print(f"\nTokens usados:")
        print(f"  - Prompt: {response.usage.prompt_tokens}")
        print(f"  - Completion: {response.usage.completion_tokens}")
        print(f"  - Total: {response.usage.total_tokens}")

        print("\n✅ SUCESSO: Conexão com Azure OpenAI está funcionando!")
        return True

    except Exception as e:
        print(f"\n❌ ERRO ao conectar com Azure OpenAI:")
        print(f"   {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    sucesso = test_azure_openai()
    sys.exit(0 if sucesso else 1)
