"""
Camada de Extract: busca o snapshot atual das top 250 criptomoedas
na API do CoinGecko e devolve um DataFrame pronto para a carga na Bronze.
"""

import requests
import pandas as pd
from config import COINGECKO_API_KEY

# Endpoint da API que retorna a lista de moedas com dados de mercado
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Apenas as colunas que alimentam as 4 análises da camada Gold
COLUNAS_INTERESSE = [
    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
    "total_volume", "high_24h", "low_24h", "price_change_percentage_24h",
    "ath", "ath_change_percentage"
]


def extract_cripto() -> pd.DataFrame:
    """
    Chama a API do CoinGecko e retorna um DataFrame com o snapshot atual
    das top 250 moedas por valor de mercado, já com a coluna data_coleta
    (timestamp do momento da extração, usada para acumular histórico na Bronze).
    """
    parametros = {
        "vs_currency": "usd",
        "category": "layer-1", 
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "x_cg_demo_api_key": COINGECKO_API_KEY
    }

    response = requests.get(BASE_URL, params=parametros)

    # Interrompe a execução se a API não responder corretamente,
    # em vez de deixar o pipeline seguir com dado vazio ou incompleto
    if response.status_code != 200:
        raise Exception(f"Erro na requisição à API do CoinGecko: {response.status_code}")

    dados_json = response.json()

    df_cripto = pd.DataFrame(dados_json)
    df_cripto = df_cripto[COLUNAS_INTERESSE]

    # Timestamp do momento da coleta para o histórico na Bronze
    df_cripto["data_coleta"] = pd.Timestamp.now()

    return df_cripto
