# vinea/__init__.py
from .consulta import (
    MNIClient,
    SystemType,
    SYSTEM_WSDLS,
    create_esaj_client,
    create_eproc1g_client,
    create_eproc2g_client,
    generate_eproc_password,
)
from .leitura import MNIParser
from .inf_web_service import TPUClient

__all__ = [
    'MNIClient',
    'MNIParser',
    'TPUClient',
    'SystemType',
    'SYSTEM_WSDLS',
    'create_esaj_client',
    'create_eproc1g_client',
    'create_eproc2g_client',
    'generate_eproc_password',
]
