"""
Camada de Load: grava os DataFrames de cada etapa no PostgreSQL.
Também contém as funções que constroem as 4 tabelas finais da Gold
a partir do DataFrame já tratado pela Silver.
"""

import pandas as pd
from config import get_engine


def load_bronze(df_cripto: pd.DataFrame):
    """
    Grava o dado bruto na tabela bronze_cripto.
    Usa append para acumular um snapshot novo
    a cada execução, sem apagar o histórico anterior.
    """
    engine = get_engine()
    df_cripto.to_sql("bronze_cripto", engine, if_exists="append", index=False)
    print(f"Carga concluída na tabela bronze_cripto ({len(df_cripto)} linhas)")


def load_silver(df_silver: pd.DataFrame):
    """
    Grava o dado tratado na tabela silver_cripto.
    Também usa append, pelo mesmo motivo da Bronze, manter histórico.
    """
    engine = get_engine()
    df_silver.to_sql("silver_cripto", engine, if_exists="append", index=False)
    print(f"Carga concluída na tabela silver_cripto ({len(df_silver)} linhas)")


def _build_gold_volatilidade(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Ranking das moedas por volatilidade do dia (Análise 1)."""
    return df_silver[["id", "name", "data_coleta", "amplitude_pct_24h", "posicao_no_range_24h"]] \
        .sort_values("amplitude_pct_24h", ascending=False) \
        .reset_index(drop=True)


def _build_gold_distancia_ath(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Ranking das moedas por distância do all-time high (Análise 2)."""
    return df_silver[["id", "name", "data_coleta", "ath_change_percentage", "categoria_ath"]] \
        .sort_values("ath_change_percentage") \
        .reset_index(drop=True)


def _build_gold_sentimento_mercado(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Sentimento do mercado — quantas moedas subiram vs caíram (Análise 5)."""
    total = len(df_silver)
    subindo = len(df_silver[df_silver["price_change_percentage_24h"] > 0])
    caindo = len(df_silver[df_silver["price_change_percentage_24h"] < 0])
    neutro = total - subindo - caindo

    return pd.DataFrame({
        "data_coleta": [df_silver["data_coleta"].iloc[0]],
        "total_moedas": [total],
        "subindo": [subindo],
        "caindo": [caindo],
        "neutro": [neutro],
        "pct_subindo": [round((subindo / total) * 100, 2)],
        "pct_caindo": [round((caindo / total) * 100, 2)]
    })


def _build_gold_performance_24h(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Top 10 maiores altas e top 10 maiores quedas em 24h (Análise 6)."""
    df = df_silver[["id", "name", "data_coleta", "price_change_percentage_24h", "market_cap"]].copy()
    df = df.dropna(subset=["price_change_percentage_24h"])

    top_altas = df.nlargest(10, "price_change_percentage_24h").assign(tipo="Alta")
    top_quedas = df.nsmallest(10, "price_change_percentage_24h").assign(tipo="Queda")

    return pd.concat([top_altas, top_quedas]).reset_index(drop=True)


def load_gold(df_silver: pd.DataFrame):
    """
    Constrói as 4 tabelas Gold a partir do Silver e grava todas no banco.
    """
    engine = get_engine()

    gold_volatilidade = _build_gold_volatilidade(df_silver)
    gold_distancia_ath = _build_gold_distancia_ath(df_silver)
    gold_sentimento = _build_gold_sentimento_mercado(df_silver)
    gold_performance = _build_gold_performance_24h(df_silver)

    gold_volatilidade.to_sql("gold_volatilidade", engine, if_exists="append", index=False)
    gold_distancia_ath.to_sql("gold_distancia_ath", engine, if_exists="append", index=False)
    gold_sentimento.to_sql("gold_sentimento_mercado", engine, if_exists="append", index=False)
    gold_performance.to_sql("gold_performance_24h", engine, if_exists="append", index=False)

    print("Carga concluída nas 4 tabelas Gold")
