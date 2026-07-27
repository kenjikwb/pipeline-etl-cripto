-- Roda automaticamente apenas na primeira vez que o container do Postgres é criado.
-- Cria o banco "airflow" (metadados do Airflow), separado do banco "estudos"
-- (onde ficam as tabelas bronze/silver/gold do projeto).
CREATE DATABASE airflow;
