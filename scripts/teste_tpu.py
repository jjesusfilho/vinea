
from vinea import TPUClient


cliente = TPUClient()

classe = cliente.pesquisar_item_publico_ws(tipo_tabela = "C", tipo_pesquisa = "C", valor_pesquisa = "1268")[0].nome

assunto = cliente.pesquisar_item_publico_ws(tipo_tabela = "A", tipo_pesquisa = "C", valor_pesquisa = "3385")[0].nome