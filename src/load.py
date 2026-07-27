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
    Usa append (nunca replace) para acumular um snapshot novo
    a cada execução, sem apagar o histórico anterior.
    """
    engine = get_engine()
    df_cripto.to_sql("bronze_cripto", engine, if_exists="append", index=False)
    print(f"Carga concluída na tabela bronze_cripto ({len(df_cripto)} linhas)")


def load_silver(df_silver: pd.DataFrame):
    """
    Grava o dado tratado na tabela silver_cripto.
    Também usa append, pelo mesmo motivo da Bronze — manter histórico.
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


def _build_gold_concentracao(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Percentual do market cap total concentrado nas top 10 moedas (Análise 3)."""
    market_cap_total = df_silver["market_cap"].sum()
    top10_market_cap = df_silver.sort_values("market_cap", ascending=False).head(10)["market_cap"].sum()
    concentracao_pct = (top10_market_cap / market_cap_total) * 100

    return pd.DataFrame({
        "data_coleta": [df_silver["data_coleta"].iloc[0]],
        "market_cap_total_250": [market_cap_total],
        "market_cap_top10": [top10_market_cap],
        "concentracao_top10_pct": [concentracao_pct]
    })


def _build_gold_correlacao_bitcoin(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Diferença de variação de cada moeda em relação ao Bitcoin no mesmo snapshot (Análise 4).
    Nota: correlação estatística real (Pearson) só faz sentido depois de acumular
    vários snapshots ao longo do tempo — com um único snapshot, essa é a métrica possível.
    """
    preco_bitcoin_var = df_silver.loc[df_silver["id"] == "bitcoin", "price_change_percentage_24h"].values[0]

    df_silver = df_silver.copy()
    df_silver["diferenca_variacao_vs_bitcoin"] = df_silver["price_change_percentage_24h"] - preco_bitcoin_var

    return df_silver[["id", "name", "data_coleta", "price_change_percentage_24h", "diferenca_variacao_vs_bitcoin"]] \
        .sort_values("diferenca_variacao_vs_bitcoin", key=abs) \
        .reset_index(drop=True)


def load_gold(df_silver: pd.DataFrame):
    """
    Constrói as 4 tabelas Gold a partir do Silver e grava todas no banco.
    gold_concentracao_mercado usa append (queremos ver esse índice mudar com o tempo);
    as demais usam replace (são rankings do snapshot mais recente).
    """
    engine = get_engine()

    gold_volatilidade = _build_gold_volatilidade(df_silver)
    gold_distancia_ath = _build_gold_distancia_ath(df_silver)
    gold_concentracao = _build_gold_concentracao(df_silver)
    gold_correlacao_bitcoin = _build_gold_correlacao_bitcoin(df_silver)

    gold_volatilidade.to_sql("gold_volatilidade", engine, if_exists="append", index=False)
    gold_distancia_ath.to_sql("gold_distancia_ath", engine, if_exists="append", index=False)
    gold_concentracao.to_sql("gold_concentracao_mercado", engine, if_exists="append", index=False)
    gold_correlacao_bitcoin.to_sql("gold_correlacao_bitcoin", engine, if_exists="replace", index=False)

    print("Carga concluída nas 4 tabelas Gold")
