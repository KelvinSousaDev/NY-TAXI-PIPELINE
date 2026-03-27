import polars as pl
from pathlib import Path

caminho_silver = Path("data/silver")
caminho_gold = Path("data/gold/")

def agregar_dados_gold():
    query_gold = (
        pl.scan_parquet(caminho_silver/"*.parquet")
        .group_by("PULocationID")
        .agg(
            pl.col("fare_amount").sum().alias("receita_total"),
            pl.len().alias("total_de_corridas")
            )
        .sort("receita_total", descending=True)
        )
    
    df_gold = query_gold.collect()
    df_gold.write_parquet(f"{caminho_gold}/faturamento_por_zona.parquet")
    print(df_gold.head(5))

if __name__ == "__main__":
    agregar_dados_gold()
