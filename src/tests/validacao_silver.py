from pathlib import Path

import polars as pl
import pandera.polars as pa

def validar_silver():
    print("Iniciando a validação para a Camada Silver...")

    caminho_silver = Path("data/silver/")

    schema = pa.DataFrameSchema(
            columns={
                "trip_duration_minutes": pa.Column(
                    dtype="int64",
                    checks=[
                        pa.Check.greater_than_or_equal_to(min_value=1)
                    ],
                    nullable=False,
                    required=True,
                ),
                "fare_amount": pa.Column(
                    dtype="float64",
                    checks=[
                        pa.Check.greater_than_or_equal_to(min_value=0)
                    ],
                    nullable=False,
                    required=True,
                ),
                "passenger_count": pa.Column(
                    dtype="int64",
                    checks=[
                        pa.Check.greater_than_or_equal_to(min_value=1)
                    ],
                    nullable=False,
                    required=True,
                ),
                "trip_distance": pa.Column(
                    dtype="float64",
                    checks=[
                        pa.Check.in_range(min_value=0, max_value=100)
                    ],
                    nullable=False,
                    required=True,
                ),
                "PULocationID": pa.Column(
                    dtype="int32",
                    checks=[
                        pa.Check.in_range(min_value=1, max_value=263)
                    ],
                    nullable=False,
                    required=True,
                ),
                "DOLocationID": pa.Column(
                    dtype="int32",
                    checks=[
                        pa.Check.in_range(min_value=1, max_value=263)
                    ],
                    nullable=False,
                    required=True,
                ),
            }
        )

    for arquivo in caminho_silver.glob("*.parquet"):
        df = pl.read_parquet(arquivo)

        try:
            schema.validate(df)
            print(f"Tudo Correto no seu Schema {arquivo.name}")
        except pa.errors.SchemaError as pa_error:
            print(f"Seu Schema Contém um Erro {pa_error}")

if __name__ == "__main__":
    validar_silver()
