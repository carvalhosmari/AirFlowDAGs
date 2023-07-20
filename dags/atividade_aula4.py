from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

with DAG(
    'atividade_aula4',
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:
    
    def imprime_cumprimento():
        print("Bem vindo ao AirFlow!")

    task = PythonOperator(
       task_id = 'boas_vindas',
       python_callable = imprime_cumprimento
    )