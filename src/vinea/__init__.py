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

# FabricJobClient depende de azure-identity, instalado só com o extra
# `vinea[fabric]` (ver pyproject.toml). Import condicional para que
# `import vinea` continue funcionando em ambientes sem esse extra (ex.:
# notebooks do Fabric que só usam MNIClient/MNIParser).
try:
    from .fabric import FabricJobClient, FabricJobError
    __all__ += ['FabricJobClient', 'FabricJobError']
except ImportError:
    pass
