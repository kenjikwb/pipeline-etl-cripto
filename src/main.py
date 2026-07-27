"""
Executa o pipeline completo, na ordem: Extract -> Bronze -> Silver -> Gold.
Serve para testar tudo localmente, do mesmo jeito que você fazia célula por
célula no notebook. Mais adiante, a DAG do Airflow vai chamar essas mesmas
funções (extract_cripto, load_bronze, transform_silver, load_silver, load_gold)
como tasks separadas, em vez de rodar tudo num script só.
"""

from extract import extract_cripto
from transform import transform_silver
from load import load_bronze, load_silver, load_gold


def run_pipeline():
    # 1. Extract: busca o snapshot atual da API do CoinGecko
    df_cripto = extract_cripto()

    # 2. Load Bronze: grava o dado bruto, acumulando histórico
    load_bronze(df_cripto)

    # 3. Transform: lê o snapshot mais recente da Bronze e gera o Silver
    df_silver = transform_silver()

    # 4. Load Silver: grava o dado tratado, acumulando histórico
    load_silver(df_silver)

    # 5. Load Gold: constrói e grava as 4 tabelas finais de análise
    load_gold(df_silver)

    print("Pipeline executado com sucesso, do Extract até a Gold.")


if __name__ == "__main__":
    run_pipeline()
