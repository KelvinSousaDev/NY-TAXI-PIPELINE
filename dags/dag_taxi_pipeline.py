from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="nyc_taxi_pipeline_gold_v2",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly", # Roda uma vez por mês
    catchup=True,
    max_active_runs=1
) as dag:

    tarefa_ingestao = BashOperator(
        task_id="baixar_dados_bronze",
        bash_command="python src/extract/baixar_dados.py -y {{ logical_date.year }} -m {{ logical_date.month }}",
        cwd="/opt/airflow/"
    )

    tarefa_limpeza_silver = BashOperator(
        task_id="limpeza_silver",
        bash_command="python src/transform/limpeza_silver.py",
        cwd="/opt/airflow/"
    )

    agregacao_gold = BashOperator(
        task_id="agregacao_gold",
        bash_command="python src/transform/agregacao_gold.py",
        cwd="/opt/airflow/"
    )

    tarefa_carga_postgres = BashOperator(
        task_id="carga_db",
        bash_command="python src/load/enviar_faturamento_postgres.py", 
        cwd="/opt/airflow"
    )

    tarefa_ingestao >> tarefa_limpeza_silver >> agregacao_gold >> tarefa_carga_postgres
