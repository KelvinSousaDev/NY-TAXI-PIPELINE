import urllib.request

def baixar_parquet():
    ano = int(input("Digite o Ano a ser baixado: "))
    mes = str(input("Digite o Mês a ser baixado em Alfa-númerico (01, 02): "))

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ano}-{mes}.parquet"
    nome_arquivo = f"yellow_tripdata_{ano}-{mes}.parquet"
    local_arquivo = "data/bronze/"


    urllib.request.urlretrieve(url, local_arquivo+nome_arquivo)

    print(f"{nome_arquivo} baixado com sucesso!")

if __name__ == "__main__":
    baixar_parquet()