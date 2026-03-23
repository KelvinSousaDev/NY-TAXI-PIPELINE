import polars as pl

def explorar_dados_brutos():
    print("Iniciando Varredura do ficheiro Parquet...")
    caminho_ficheiro = "data/bronze/yellow_tripdata_2025-02.parquet"

    df = pl.read_parquet(caminho_ficheiro)
    linhas, colunas = df.shape
    print(f"[VOLUME] O Ficheiro Contém {linhas} linhas e {colunas} colunas")

    print("[AMOSTRRA] Primeiras 5 linhas:")
    print(df.head(5))

    print("[RAIO-X] Estatísticas para detecção de anomalias")
    colunas_criticas = ["passenger_count", "trip_distance", "fare_amount", "total_amount"]
    print(df.select(colunas_criticas).describe())

if __name__ == "__main__":
    explorar_dados_brutos()