"""
Camada de Transform: lê o dado bruto da Bronze, limpa e tipa corretamente,
e calcula as métricas usadas depois pelas 4 análises da camada Gold.
"""

import pandas as pd
from config import get_engine

# Colunas que precisam ser garantidamente numéricas (a API pode devolver como texto)
COLUNAS_FLOAT = [
    "current_price", "market_cap", "total_volume", "high_24h", "low_24h",
    "price_change_percentage_24h", "ath", "ath_change_percentage"
]


def transform_silver() -> pd.DataFrame:
    """
    Lê o snapshot mais recente da tabela bronze_cripto, aplica limpeza,
    tipagem e as métricas derivadas, e retorna o DataFrame pronto
    para a carga na camada Silver.
    """
    engine = get_engine()

    # Lê apenas o snapshot mais recente da Bronze (não o histórico acumulado inteiro)
    df_bronze = pd.read_sql(
        "SELECT * FROM bronze_cripto WHERE data_coleta = (SELECT MAX(data_coleta) FROM bronze_cripto)",
        engine
    )

    df_silver = df_bronze.copy()

    # Remove duplicatas por moeda, mantendo o registro mais recente
    df_silver = df_silver.drop_duplicates(subset="id", keep="last")

    # Remove linhas sem informação essencial (sem preço ou market cap não serve pra análise)
    df_silver = df_silver.dropna(subset=["id", "current_price", "market_cap"])

    # Garante que as colunas numéricas estão como float, não como texto
    df_silver[COLUNAS_FLOAT] = df_silver[COLUNAS_FLOAT].astype(float)
    df_silver["market_cap_rank"] = df_silver["market_cap_rank"].astype("Int64")

    # Padroniza texto (evita duplicidade por espaço extra ou diferença de maiúscula)
    df_silver["id"] = df_silver["id"].str.strip().str.lower()
    df_silver["symbol"] = df_silver["symbol"].str.strip().str.lower()
    df_silver["name"] = df_silver["name"].str.strip()

    # Amplitude percentual do dia (Análise 1: volatilidade)
    df_silver["amplitude_pct_24h"] = (
        (df_silver["high_24h"] - df_silver["low_24h"]) / df_silver["low_24h"]
    ) * 100

    # Onde o preço atual está dentro da faixa do dia (0 = mínima, 1 = máxima)
    df_silver["posicao_no_range_24h"] = (
        (df_silver["current_price"] - df_silver["low_24h"]) /
        (df_silver["high_24h"] - df_silver["low_24h"])
    )

    # Volume em milhões, só para facilitar leitura em tabelas e gráficos
    df_silver["volume_milhoes"] = df_silver["total_volume"] / 1_000_000

    # Categoriza a distância do all-time high (Análise 2)
    # O limite -100.01 (em vez de -100) evita que moedas com ath_change exatamente
    # -100 fiquem fora de todas as faixas e virem NaN
    df_silver["categoria_ath"] = pd.cut(
        df_silver["ath_change_percentage"],
        bins=[-100.01, -75, -50, -25, 0],
        labels=["Muito longe do topo", "Longe do topo", "Perto do topo", "Próximo do topo"]
    )

    return df_silver
