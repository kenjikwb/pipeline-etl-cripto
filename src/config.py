"""
Configuração central do pipeline: variáveis de ambiente e conexão com o banco.
Todos os outros módulos (extract, transform, load) importam a engine daqui,
em vez de recriar a conexão em cada arquivo.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Carrega as variáveis do arquivo .env (chave da API, credenciais do banco)
load_dotenv()

# Credenciais do banco, lidas do .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Chave da API do CoinGecko
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


def get_engine():
    """
    Cria e retorna a engine de conexão com o PostgreSQL.
    Cada módulo chama essa função quando precisar falar com o banco.
    """
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
