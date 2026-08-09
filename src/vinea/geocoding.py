"""
Módulo de geocodificação para endereços de MPUs.

Utiliza a API do Nominatim (OpenStreetMap) para geocodificar endereços,
com bounding box baseado no município.
"""

import time
from typing import Optional, Tuple
from dataclasses import dataclass
import requests
from urllib.parse import quote


@dataclass
class GeocodingResult:
    """Resultado de uma geocodificação."""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    display_name: Optional[str] = None
    success: bool = False
    error: Optional[str] = None


class Geocoder:
    """
    Geocodificador usando Nominatim (OpenStreetMap).

    Respeita os limites de uso da API (1 requisição por segundo).
    """

    def __init__(self, user_agent: str = "vinea-mpu-extractor/1.0"):
        """
        Inicializa o geocodificador.

        Args:
            user_agent: User agent para requisições (obrigatório pelo Nominatim)
        """
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.user_agent = user_agent
        self.last_request_time = 0
        self.min_interval = 1.0  # Mínimo de 1 segundo entre requisições

    def _wait_if_needed(self):
        """Garante intervalo mínimo entre requisições."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def _get_municipality_bbox(self, municipality: str, state: str = "São Paulo") -> Optional[Tuple[float, float, float, float]]:
        """
        Obtém o bounding box de um município.

        Args:
            municipality: Nome do município
            state: Estado (padrão: São Paulo)

        Returns:
            Tupla (min_lat, max_lat, min_lon, max_lon) ou None se não encontrado
        """
        self._wait_if_needed()

        query = f"{municipality}, {state}, Brasil"
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 1
        }

        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers={"User-Agent": self.user_agent},
                timeout=10
            )
            response.raise_for_status()

            results = response.json()
            if results:
                bbox = results[0].get("boundingbox")
                if bbox and len(bbox) == 4:
                    # Nominatim retorna [min_lat, max_lat, min_lon, max_lon]
                    return (
                        float(bbox[0]),  # min_lat
                        float(bbox[1]),  # max_lat
                        float(bbox[2]),  # min_lon
                        float(bbox[3])   # max_lon
                    )
        except Exception as e:
            print(f"Erro ao obter bbox de {municipality}: {e}")

        return None

    def geocode_address(
        self,
        street: Optional[str] = None,
        neighborhood: Optional[str] = None,
        municipality: Optional[str] = None,
        cep: Optional[str] = None,
        state: str = "São Paulo"
    ) -> GeocodingResult:
        """
        Geocodifica um endereço usando bounding box do município.

        Args:
            street: Nome da rua/logradouro
            neighborhood: Bairro
            municipality: Município
            cep: CEP
            state: Estado (padrão: São Paulo)

        Returns:
            GeocodingResult com latitude/longitude
        """
        # Verifica se há informação suficiente
        if not municipality:
            return GeocodingResult(
                success=False,
                error="Município não informado"
            )

        # Monta a query de busca
        query_parts = []

        if street:
            query_parts.append(street)
        if neighborhood:
            query_parts.append(neighborhood)
        if municipality:
            query_parts.append(municipality)
        if state:
            query_parts.append(state)

        query_parts.append("Brasil")

        if not query_parts:
            return GeocodingResult(
                success=False,
                error="Endereço vazio"
            )

        query = ", ".join(query_parts)

        # Obtém o bounding box do município
        bbox = self._get_municipality_bbox(municipality, state)

        # Aguarda intervalo mínimo
        self._wait_if_needed()

        # Parâmetros da requisição
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
            "countrycodes": "br"
        }

        # Adiciona bounding box se disponível
        if bbox:
            # viewbox formato: min_lon,min_lat,max_lon,max_lat
            params["viewbox"] = f"{bbox[2]},{bbox[0]},{bbox[3]},{bbox[1]}"
            params["bounded"] = 1  # Restringe resultados ao viewbox

        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers={"User-Agent": self.user_agent},
                timeout=10
            )
            response.raise_for_status()

            results = response.json()

            if results:
                result = results[0]
                return GeocodingResult(
                    latitude=float(result["lat"]),
                    longitude=float(result["lon"]),
                    display_name=result.get("display_name"),
                    success=True
                )
            else:
                return GeocodingResult(
                    success=False,
                    error=f"Nenhum resultado encontrado para: {query}"
                )

        except requests.RequestException as e:
            return GeocodingResult(
                success=False,
                error=f"Erro na requisição: {e}"
            )
        except (KeyError, ValueError) as e:
            return GeocodingResult(
                success=False,
                error=f"Erro ao processar resposta: {e}"
            )

    def geocode_cep(self, cep: str, municipality: Optional[str] = None) -> GeocodingResult:
        """
        Geocodifica usando CEP.

        Args:
            cep: CEP (com ou sem formatação)
            municipality: Município (opcional, para restringir busca)

        Returns:
            GeocodingResult com latitude/longitude
        """
        # Remove formatação do CEP
        cep_clean = cep.replace("-", "").replace(".", "").strip()

        if len(cep_clean) != 8:
            return GeocodingResult(
                success=False,
                error=f"CEP inválido: {cep}"
            )

        # Tenta buscar pelo CEP
        query = f"{cep_clean}, Brasil"

        bbox = None
        if municipality:
            bbox = self._get_municipality_bbox(municipality, "São Paulo")

        self._wait_if_needed()

        params = {
            "postalcode": cep_clean,
            "country": "br",
            "format": "json",
            "limit": 1
        }

        if bbox:
            params["viewbox"] = f"{bbox[2]},{bbox[0]},{bbox[3]},{bbox[1]}"
            params["bounded"] = 1

        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers={"User-Agent": self.user_agent},
                timeout=10
            )
            response.raise_for_status()

            results = response.json()

            if results:
                result = results[0]
                return GeocodingResult(
                    latitude=float(result["lat"]),
                    longitude=float(result["lon"]),
                    display_name=result.get("display_name"),
                    success=True
                )
            else:
                return GeocodingResult(
                    success=False,
                    error=f"Nenhum resultado para CEP: {cep}"
                )

        except Exception as e:
            return GeocodingResult(
                success=False,
                error=f"Erro ao geocodificar CEP: {e}"
            )

    def geocode_com_fallback(
        self,
        street: Optional[str],
        neighborhood: Optional[str],
        municipality: Optional[str],
        cep: Optional[str]
    ) -> GeocodingResult:
        """
        Geocodifica tentando primeiro o endereço completo, depois o CEP,
        depois só o município. Compartilhado entre os geocodificadores
        especializados (MPU, ato infracional etc.) para não duplicar essa
        lógica de fallback em cada um.
        """
        if street or neighborhood:
            result = self.geocode_address(
                street=street,
                neighborhood=neighborhood,
                municipality=municipality,
                cep=cep
            )
            if result.success:
                return result

        if cep and municipality:
            result = self.geocode_cep(cep, municipality)
            if result.success:
                return result

        if municipality:
            result = self.geocode_address(municipality=municipality, state="São Paulo")
            if result.success:
                return result

        return GeocodingResult(success=False, error="Não foi possível geocodificar")


class MPUGeocoder:
    """Geocodificador especializado para dados de MPUs."""

    def __init__(self, user_agent: str = "vinea-mpu-extractor/1.0"):
        """Inicializa o geocodificador de MPUs."""
        self.geocoder = Geocoder(user_agent=user_agent)

    def geocode_mpu_addresses(self, mpu_data) -> None:
        """
        Geocodifica todos os endereços de um objeto MPUData in-place.

        Args:
            mpu_data: Objeto MPUData a ser enriquecido com lat/long
        """
        if not mpu_data.identificacao_fato:
            return

        identificacao = mpu_data.identificacao_fato

        # Obtém o município (usado como bounding box)
        municipality = (
            identificacao.local_fatos_cidade or
            identificacao.residencia_vitima_cidade or
            identificacao.residencia_autor_cidade
        )

        if not municipality:
            print("⚠ Município não encontrado, geocodificação pode ser imprecisa")

        # Geocodifica local dos fatos
        if identificacao.local_fatos_rua or identificacao.local_fatos_cep:
            print(f"  Geocodificando local dos fatos...")
            result = self._geocode_location(
                street=identificacao.local_fatos_rua,
                neighborhood=identificacao.local_fatos_bairro,
                municipality=identificacao.local_fatos_cidade or municipality,
                cep=identificacao.local_fatos_cep
            )
            if result.success:
                identificacao.local_fatos_latitude = result.latitude
                identificacao.local_fatos_longitude = result.longitude
                print(f"    ✓ {result.latitude}, {result.longitude}")
            else:
                print(f"    ✗ {result.error}")

        # Geocodifica residência da vítima
        if identificacao.residencia_vitima_rua or identificacao.residencia_vitima_cep:
            print(f"  Geocodificando residência da vítima...")
            result = self._geocode_location(
                street=identificacao.residencia_vitima_rua,
                neighborhood=identificacao.residencia_vitima_bairro,
                municipality=identificacao.residencia_vitima_cidade or municipality,
                cep=identificacao.residencia_vitima_cep
            )
            if result.success:
                identificacao.residencia_vitima_latitude = result.latitude
                identificacao.residencia_vitima_longitude = result.longitude
                print(f"    ✓ {result.latitude}, {result.longitude}")
            else:
                print(f"    ✗ {result.error}")

        # Geocodifica residência do autor
        if identificacao.residencia_autor_rua or identificacao.residencia_autor_cep:
            print(f"  Geocodificando residência do autor...")
            result = self._geocode_location(
                street=identificacao.residencia_autor_rua,
                neighborhood=identificacao.residencia_autor_bairro,
                municipality=identificacao.residencia_autor_cidade or municipality,
                cep=identificacao.residencia_autor_cep
            )
            if result.success:
                identificacao.residencia_autor_latitude = result.latitude
                identificacao.residencia_autor_longitude = result.longitude
                print(f"    ✓ {result.latitude}, {result.longitude}")
            else:
                print(f"    ✗ {result.error}")

    def _geocode_location(
        self,
        street: Optional[str],
        neighborhood: Optional[str],
        municipality: Optional[str],
        cep: Optional[str]
    ) -> GeocodingResult:
        """Geocodifica uma localização com fallback endereço → CEP → município."""
        return self.geocoder.geocode_com_fallback(street, neighborhood, municipality, cep)


class ProcessoInfracionalGeocoder:
    """
    Geocodificador especializado para dados de processos de ato infracional
    (`ProcessoInfracionalData`). Diferente do `MPUGeocoder` (endereços fixos),
    aqui os endereços vêm em listas — um por boletim de ocorrência (local do
    fato) e um por pessoa envolvida (residência) —, então itera sobre elas.
    """

    def __init__(self, user_agent: str = "vinea-infracional-extractor/1.0"):
        self.geocoder = Geocoder(user_agent=user_agent)

    def geocode_processo_addresses(self, processo_data) -> None:
        """
        Geocodifica in-place os endereços de todos os boletins de ocorrência
        (local do fato) e de todas as pessoas envolvidas (residência) de um
        `ProcessoInfracionalData`.
        """
        municipality_padrao = None
        for bo in processo_data.boletins_ocorrencia or []:
            if bo.endereco_fato_cidade:
                municipality_padrao = bo.endereco_fato_cidade
                break

        for bo in processo_data.boletins_ocorrencia or []:
            if not (bo.endereco_fato_rua or bo.endereco_fato_cep):
                continue
            municipio = bo.endereco_fato_cidade or municipality_padrao
            print(f"  Geocodificando local do fato (boletim {bo.numero_boletim})...")
            resultado = self.geocoder.geocode_com_fallback(
                street=bo.endereco_fato_rua,
                neighborhood=bo.endereco_fato_bairro,
                municipality=municipio,
                cep=bo.endereco_fato_cep,
            )
            if resultado.success:
                bo.endereco_fato_latitude = resultado.latitude
                bo.endereco_fato_longitude = resultado.longitude
                print(f"    ✓ {resultado.latitude}, {resultado.longitude}")
            else:
                print(f"    ✗ {resultado.error}")

        for pessoa in processo_data.pessoas or []:
            if not (pessoa.endereco_rua or pessoa.endereco_cep):
                continue
            municipio = pessoa.endereco_cidade or municipality_padrao
            print(f"  Geocodificando residência ({pessoa.categoria}, {pessoa.nome})...")
            resultado = self.geocoder.geocode_com_fallback(
                street=pessoa.endereco_rua,
                neighborhood=pessoa.endereco_bairro,
                municipality=municipio,
                cep=pessoa.endereco_cep,
            )
            if resultado.success:
                pessoa.endereco_latitude = resultado.latitude
                pessoa.endereco_longitude = resultado.longitude
                print(f"    ✓ {resultado.latitude}, {resultado.longitude}")
            else:
                print(f"    ✗ {resultado.error}")
