"""
Parser para dados de MPU extraídos, convertendo para DataFrames pandas.

Este módulo complementa o mpu_extraction.py, fornecendo funcionalidades
para converter os dados estruturados em DataFrames para análise.
"""

import json
from pathlib import Path
from typing import Union, Optional
import pandas as pd

from .mpu_models import MPUData


class MPUParser:
    """
    Parser para converter dados de MPU em DataFrames pandas.

    Similar ao MNIParser, mas focado em dados de MPUs extraídos.
    """

    def __init__(self):
        """Inicializa o parser de MPUs."""
        pass

    def ler_mpu_json(self, json_path: Union[str, Path]) -> MPUData:
        """
        Lê um arquivo JSON com dados de MPU.

        Args:
            json_path: Caminho para o arquivo JSON

        Returns:
            Objeto MPUData
        """
        json_path = Path(json_path)
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return MPUData(**data)

    def mpu_para_df_principal(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados principais da MPU em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com dados principais (identificação, processo, datas)
        """
        data = {
            # Identificação do processo
            "numero_processo": mpu_data.identificacao_processo.numero_processo,
            "comarca": mpu_data.identificacao_processo.comarca,
            "vara": mpu_data.identificacao_processo.vara.value if mpu_data.identificacao_processo.vara else None,
            "classe_processual": mpu_data.identificacao_processo.classe_processual,

            # Linha do tempo
            "data_fato": mpu_data.linha_tempo.data_fato if mpu_data.linha_tempo else None,
            "data_bo": mpu_data.linha_tempo.data_bo if mpu_data.linha_tempo else None,
            "data_abertura_autos": mpu_data.linha_tempo.data_abertura_autos if mpu_data.linha_tempo else None,
            "data_deferimento_indeferimento": mpu_data.linha_tempo.data_deferimento_indeferimento if mpu_data.linha_tempo else None,
            "data_intimacao_agressor": mpu_data.linha_tempo.data_intimacao_agressor if mpu_data.linha_tempo else None,

            # Trâmite processual
            "peca_inaugural": mpu_data.tramite_processual.peca_inaugural.value if mpu_data.tramite_processual and mpu_data.tramite_processual.peca_inaugural else None,
            "manifestacao_mp": mpu_data.tramite_processual.manifestacao_mp.value if mpu_data.tramite_processual and mpu_data.tramite_processual.manifestacao_mp else None,
            "decisao_primeiro_pedido": mpu_data.tramite_processual.decisao_primeiro_pedido.value if mpu_data.tramite_processual and mpu_data.tramite_processual.decisao_primeiro_pedido else None,

            # Coordenadas geográficas (resumo)
            "local_fatos_latitude": mpu_data.identificacao_fato.local_fatos_latitude if mpu_data.identificacao_fato else None,
            "local_fatos_longitude": mpu_data.identificacao_fato.local_fatos_longitude if mpu_data.identificacao_fato else None,

            # Metadados
            "arquivo_fonte": mpu_data.arquivo_fonte,
            "data_extracao": mpu_data.data_extracao,
        }

        return pd.DataFrame([data])

    def mpu_para_df_vitima(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados da vítima em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com perfil sociodemográfico da vítima
        """
        if not mpu_data.perfil_vitima:
            return pd.DataFrame()

        perfil = mpu_data.perfil_vitima
        info_comp = perfil.informacoes_complementares

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,
            "idade": perfil.idade,
            "faixa_idade": perfil.faixa_idade.value if perfil.faixa_idade else None,
            "nacionalidade": perfil.nacionalidade,
            "raca_cor": perfil.raca_cor.value if perfil.raca_cor else None,
            "comunidade_quilombola": perfil.comunidade_quilombola,
            "povo_indigena": perfil.povo_indigena,
            "povo_indigena_qual": perfil.povo_indigena_qual,
            "escolaridade": perfil.escolaridade.value if perfil.escolaridade else None,
            "sexualidade": perfil.sexualidade.value if perfil.sexualidade else None,
            "identidade_genero": perfil.identidade_genero.value if perfil.identidade_genero else None,

            # Informações complementares
            "gravidez": info_comp.gravidez if info_comp else None,
            "puerperio": info_comp.puerperio if info_comp else None,
            "doenca": info_comp.doenca if info_comp else None,
            "deficiencia": info_comp.deficiencia if info_comp else None,
            "acompanhamento_saude_mental": info_comp.acompanhamento_saude_mental if info_comp else None,
            "dependencia_economica": info_comp.dependencia_economica if info_comp else None,
            "autonomia_financeira": info_comp.autonomia_financeira if info_comp else None,
            "rede_apoio": info_comp.rede_apoio if info_comp else None,
        }

        return pd.DataFrame([data])

    def mpu_para_df_autor(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados do autor da violência em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com perfil sociodemográfico do autor
        """
        if not mpu_data.perfil_autor:
            return pd.DataFrame()

        perfil = mpu_data.perfil_autor
        info_comp = perfil.informacoes_complementares

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,
            "idade": perfil.idade,
            "nacionalidade": perfil.nacionalidade,
            "raca_cor": perfil.raca_cor.value if perfil.raca_cor else None,
            "comunidade_quilombola": perfil.comunidade_quilombola,
            "povo_indigena": perfil.povo_indigena,
            "povo_indigena_qual": perfil.povo_indigena_qual,
            "escolaridade": perfil.escolaridade.value if perfil.escolaridade else None,
            "sexualidade": perfil.sexualidade.value if perfil.sexualidade else None,
            "identidade_genero": perfil.identidade_genero.value if perfil.identidade_genero else None,

            # Informações complementares
            "emprego_situacao_financeira": info_comp.emprego_situacao_financeira if info_comp else None,
            "dependencia_quimica": info_comp.dependencia_quimica if info_comp else None,
            "doenca_mental": info_comp.doenca_mental if info_comp else None,
            "cargo_poder": info_comp.cargo_poder if info_comp else None,
            "cargo_poder_qual": info_comp.cargo_poder_qual if info_comp else None,
            "acesso_arma_fogo": info_comp.acesso_arma_fogo if info_comp else None,
        }

        return pd.DataFrame([data])

    def mpu_para_df_relacionamento(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados de relacionamento em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com dinâmica do relacionamento
        """
        if not mpu_data.dinamica_relacionamento:
            return pd.DataFrame()

        din = mpu_data.dinamica_relacionamento
        comp = din.comportamentos_controle

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,
            "tipo_relacionamento": din.tipo_relacionamento.value if din.tipo_relacionamento else None,
            "tempo_relacionamento": din.tempo_relacionamento,
            "filhos_comum": din.filhos_comum,
            "filhos_comum_quantidade": din.filhos_comum_quantidade,
            "filhos_relacionamento_anterior": din.filhos_relacionamento_anterior,
            "conflitos_guarda": din.conflitos_guarda,
            "conflitos_pensao": din.conflitos_pensao,
            "conflitos_partilha": din.conflitos_partilha,
            "residencia_comum": din.residencia_comum,
            "empresas_comum": din.empresas_comum,
            "historico_bo_anterior": din.historico_bo_anterior,
            "violencia_outras_parceiras": din.violencia_outras_parceiras,

            # Comportamentos de controle
            "ciumes_excessivo": comp.ciumes_excessivo if comp else None,
            "controle_financeiro": comp.controle_financeiro if comp else None,
            "monitoramento_digital": comp.monitoramento_digital if comp else None,
            "proibicao_trabalhar_estudar": comp.proibicao_trabalhar_estudar if comp else None,
            "isolamento_social": comp.isolamento_social if comp else None,
            "impedimento_acesso": comp.impedimento_acesso if comp else None,
        }

        return pd.DataFrame([data])

    def mpu_para_df_episodio(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados do episódio de violência em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com caracterização do episódio
        """
        if not mpu_data.caracterizacao_episodio:
            return pd.DataFrame()

        ep = mpu_data.caracterizacao_episodio
        grav = ep.gravidade_objetiva
        ctx = ep.contexto

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,

            # Naturezas
            "naturezas_violencia": ", ".join([n.value for n in ep.naturezas_violencia]) if ep.naturezas_violencia else None,
            "naturezas_ocorrencia": ", ".join(ep.naturezas_ocorrencia) if ep.naturezas_ocorrencia else None,

            # Gravidade objetiva
            "uso_arma": grav.uso_arma if grav else None,
            "tipo_arma": grav.tipo_arma.value if grav and grav.tipo_arma else None,
            "tiro": grav.tiro if grav else None,
            "estrangulamento": grav.estrangulamento if grav else None,
            "queimadura": grav.queimadura if grav else None,
            "parte_corpo_atingida": grav.parte_corpo_atingida if grav else None,
            "atendimento_medico": grav.atendimento_medico if grav else None,
            "internacao_medica": grav.internacao_medica if grav else None,

            # Contexto
            "local": ctx.local if ctx else None,
            "presenca_criancas": ctx.presenca_criancas if ctx else None,
            "testemunhas": ctx.testemunhas if ctx else None,
            "prisao_flagrante": ctx.prisao_flagrante if ctx else None,
        }

        return pd.DataFrame([data])

    def mpu_para_df_historico(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados de histórico de escalada em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com histórico de escalada e reincidência
        """
        if not mpu_data.historico_escalada:
            return pd.DataFrame()

        hist = mpu_data.historico_escalada

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,
            "agressoes_anteriores_graves": hist.agressoes_anteriores_graves,
            "frequencia_crescente": hist.frequencia_crescente,
            "descumprimento_protetivas": hist.descumprimento_protetivas,
            "prisao_anterior": hist.prisao_anterior,
            "prisao_anterior_tipo": hist.prisao_anterior_tipo,
            "violencia_filhos_familiares_animais": hist.violencia_filhos_familiares_animais,
            "frases_ameaca": hist.frases_ameaca,
            "ameaca_morte": hist.ameaca_morte,
        }

        return pd.DataFrame([data])

    def mpu_para_df_localizacao(self, mpu_data: MPUData) -> pd.DataFrame:
        """
        Converte dados de localização/endereços em DataFrame.

        Args:
            mpu_data: Dados da MPU

        Returns:
            DataFrame com todos os endereços e coordenadas geográficas
        """
        if not mpu_data.identificacao_fato:
            return pd.DataFrame()

        fato = mpu_data.identificacao_fato

        data = {
            "numero_processo": mpu_data.identificacao_processo.numero_processo,

            # Local dos fatos
            "local_fatos_rua": fato.local_fatos_rua,
            "local_fatos_bairro": fato.local_fatos_bairro,
            "local_fatos_cidade": fato.local_fatos_cidade,
            "local_fatos_cep": fato.local_fatos_cep,
            "local_fatos_latitude": fato.local_fatos_latitude,
            "local_fatos_longitude": fato.local_fatos_longitude,

            # Residência da vítima
            "residencia_vitima_rua": fato.residencia_vitima_rua,
            "residencia_vitima_bairro": fato.residencia_vitima_bairro,
            "residencia_vitima_cidade": fato.residencia_vitima_cidade,
            "residencia_vitima_cep": fato.residencia_vitima_cep,
            "residencia_vitima_latitude": fato.residencia_vitima_latitude,
            "residencia_vitima_longitude": fato.residencia_vitima_longitude,

            # Residência do autor
            "residencia_autor_rua": fato.residencia_autor_rua,
            "residencia_autor_bairro": fato.residencia_autor_bairro,
            "residencia_autor_cidade": fato.residencia_autor_cidade,
            "residencia_autor_cep": fato.residencia_autor_cep,
            "residencia_autor_latitude": fato.residencia_autor_latitude,
            "residencia_autor_longitude": fato.residencia_autor_longitude,

            # Informações temporais
            "dia_mes": fato.dia_mes,
            "dia_semana": fato.dia_semana,
            "periodo_dia": fato.periodo_dia,
            "data_bo": fato.data_bo,
        }

        return pd.DataFrame([data])

    def processar_lote_json(
        self,
        input_dir: Union[str, Path],
        file_pattern: str = "*.json"
    ) -> dict[str, pd.DataFrame]:
        """
        Processa múltiplos arquivos JSON de MPUs e retorna DataFrames concatenados.

        Args:
            input_dir: Diretório com os arquivos JSON
            file_pattern: Padrão de arquivos

        Returns:
            Dicionário com DataFrames concatenados:
            - "principal": Dados principais
            - "vitima": Perfil da vítima
            - "autor": Perfil do autor
            - "relacionamento": Dinâmica do relacionamento
            - "episodio": Caracterização do episódio
            - "historico": Histórico de escalada
            - "localizacao": Endereços e coordenadas geográficas
        """
        input_dir = Path(input_dir)
        json_files = list(input_dir.glob(file_pattern))

        print(f"Processando {len(json_files)} arquivos JSON...")

        # Listas para acumular DataFrames
        dfs_principal = []
        dfs_vitima = []
        dfs_autor = []
        dfs_relacionamento = []
        dfs_episodio = []
        dfs_historico = []
        dfs_localizacao = []

        for json_file in json_files:
            try:
                mpu_data = self.ler_mpu_json(json_file)

                # Converte para DataFrames
                dfs_principal.append(self.mpu_para_df_principal(mpu_data))

                df_vitima = self.mpu_para_df_vitima(mpu_data)
                if not df_vitima.empty:
                    dfs_vitima.append(df_vitima)

                df_autor = self.mpu_para_df_autor(mpu_data)
                if not df_autor.empty:
                    dfs_autor.append(df_autor)

                df_rel = self.mpu_para_df_relacionamento(mpu_data)
                if not df_rel.empty:
                    dfs_relacionamento.append(df_rel)

                df_ep = self.mpu_para_df_episodio(mpu_data)
                if not df_ep.empty:
                    dfs_episodio.append(df_ep)

                df_hist = self.mpu_para_df_historico(mpu_data)
                if not df_hist.empty:
                    dfs_historico.append(df_hist)

                df_loc = self.mpu_para_df_localizacao(mpu_data)
                if not df_loc.empty:
                    dfs_localizacao.append(df_loc)

            except Exception as e:
                print(f"Erro ao processar {json_file.name}: {e}")
                continue

        # Concatena todos os DataFrames
        result = {
            "principal": pd.concat(dfs_principal, ignore_index=True) if dfs_principal else pd.DataFrame(),
            "vitima": pd.concat(dfs_vitima, ignore_index=True) if dfs_vitima else pd.DataFrame(),
            "autor": pd.concat(dfs_autor, ignore_index=True) if dfs_autor else pd.DataFrame(),
            "relacionamento": pd.concat(dfs_relacionamento, ignore_index=True) if dfs_relacionamento else pd.DataFrame(),
            "episodio": pd.concat(dfs_episodio, ignore_index=True) if dfs_episodio else pd.DataFrame(),
            "historico": pd.concat(dfs_historico, ignore_index=True) if dfs_historico else pd.DataFrame(),
            "localizacao": pd.concat(dfs_localizacao, ignore_index=True) if dfs_localizacao else pd.DataFrame(),
        }

        print(f"✓ Processamento concluído:")
        for name, df in result.items():
            print(f"  - {name}: {len(df)} registros")

        return result
