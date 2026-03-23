import polars as pl
import time

def refinar_para_silver():
    inicio = time.time()
    print("Iniciando a Forja da Camada Silver...")

    caminho_bronze = "data/bronze/yellow_tripdata_2025-01.parquet"
    caminho_silver = "data/silver/yellow_tripdata_2025-01_clean.parquet"

    query = (
        pl.scan_parquet(caminho_bronze)
        .filter(
            (pl.col("passenger_count") > 0) &      # Remove carros vazios
            (pl.col("fare_amount") >= 0) &         # Remove estornos e erros de cobrança
            (pl.col("trip_distance") > 0.0)        # Remove viagens fantasmas (0 milhas)
        )
    )

    print("A limpar milhões de linhas...")
    df_limpo = query.collect()

    df_limpo.write_parquet(caminho_silver)

    fim = time.time()
    linhas_originais = 3475226
    linhas_finais = df_limpo.height

    print(f"Processamento concluído em {fim - inicio:.2f} segundos!")
    print(f"Sujidade removida: {linhas_originais - linhas_finais:} linhas defeituosas.")
    print(f"Novo volume da Camada Silver: {linhas_finais:} linhas purificadas.")

if __name__ == "__main__":
    refinar_para_silver()