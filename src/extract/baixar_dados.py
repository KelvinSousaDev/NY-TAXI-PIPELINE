from urllib.error import HTTPError, URLError
import urllib.request
import argparse
import os

def baixar_parquet():
    # Definindo os Argumentos que irão iniciar junto com o programa (pensando na automação via Airflow)
    parser = argparse.ArgumentParser(description="Baixar Parquet dos Dados de Taxi Amarelos de NY")
    parser.add_argument("-y", "--year", type=int, help="Ano a Ser Baixado", required=True)
    parser.add_argument("-m", "--month", type=int, help="Mês a Ser Baixado", required=True)
    args = parser.parse_args()

    # Criando a URL de Download, o nome e o local a ser salvo
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{args.year}-{args.month:02d}.parquet"
    nome_arquivo = f"yellow_tripdata_{args.year}-{args.month:02d}.parquet"

    local_arquivo = "data/bronze/"
    os.makedirs(local_arquivo, exist_ok=True)

    nome_e_local = os.path.join(local_arquivo, nome_arquivo)

    try:
        urllib.request.urlretrieve(url, nome_e_local)
        print(f"{nome_arquivo} baixado com sucesso!")
    except HTTPError as erro_http:
        print(f"O Parquet deste mês ainda não foi publicado pelo governo! Erro: {erro_http}")
    except URLError as erro_url:
        print(f"Falha na Rede, Dá uma olhada na conexão de internet ai! Erro: {erro_url}")
    except Exception as erro:
        print(f"Falha Desconhecida! Erro: {erro}")

if __name__ == "__main__":
    baixar_parquet()