from vinea import EprocJurisprudenciaClient


cliente = EprocJurisprudenciaClient()

pagina = cliente.buscar(
    '"cartao de credito consignado"',
    data_publicacao_inicio="01/08/2026",
    data_publicacao_fim="07/08/2026",
)

print(f"total_resultados={pagina.total_resultados} total_paginas={pagina.total_paginas}")

resultado = pagina.resultados[0]
print(resultado["numero_processo"], resultado["tipo_documento"], resultado["orgao_julgador"])
assert len(pagina.resultados) == 10
assert resultado["numero_processo"] and len(resultado["numero_processo"]) == 20
