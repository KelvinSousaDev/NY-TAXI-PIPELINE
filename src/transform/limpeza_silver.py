import polars as pl
from pathlib import Path
import time

def refinar_para_silver():
    inicio = time.time()
    print("Iniciando a limpeza para a Camada Silver...")

    caminho_bronze = Path("data/bronze/")
    caminho_silver = Path("data/silver/")

    for arquivo in caminho_bronze.glob("*.parquet"):
    
        query = (
            pl.scan_parquet(arquivo)
            .filter(
                (pl.col("passenger_count") > 0) &      # Remove carros vazios
                (pl.col("fare_amount") >= 0) &         # Remove estornos e erros de cobrança
                (pl.col("trip_distance") > 0.0) &      # Remove viagens fantasmas (0 milhas)
                (pl.col("trip_distance") <= 100) &     # Remove Viagens Longas demais (Maior que 100 milhas)
                (pl.col("PULocationID") <= 263) &       # Remove Viagens que foram iniciadas fora das zonas de NY ou zonas desconhecidas
                (pl.col("DOLocationID") <= 263)         # Remove Viagens que foram finalizadas fora das zonas de NY ou zonas desconhecidas
            )
            .with_columns(
                (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime")).dt.total_minutes().alias("trip_duration_minutes")
            )
            .filter(
                (pl.col("trip_duration_minutes") > 0) # Remove Corridas com 0 minutos de duração
            )
        )

        print("A limpar linhas...")
        df_limpo = query.collect()

        name_and_silver_path = caminho_silver / f"clean_{arquivo.name}"
        df_limpo.write_parquet(name_and_silver_path)

    fim = time.time()
    print(f"Processamento concluído em {fim - inicio:.2f} segundos!")


if __name__ == "__main__":
    refinar_para_silver()