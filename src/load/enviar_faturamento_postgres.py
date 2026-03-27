from dotenv import load_dotenv
import polars as pl
import psycopg2
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def enviar_faturamento():
    print("✅ DB Conectado.")
    df_gold = pl.read_parquet("data/gold/faturamento_por_zona.parquet")
    df_gold.write_database(table_name="faturamento_zonas", connection=DATABASE_URL, if_table_exists="replace", engine="adbc")
    print("✅ Dados Enviados Com Sucesso para o Banco de Dados")

if __name__ == "__main__":
    enviar_faturamento()

