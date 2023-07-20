from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator

with DAG(
    'primeiro_dag',
    start_date = days_ago(3),
    schedule_interval='@daily'
) as dag: 
    
    task_1 = EmptyOperator(task_id = 'tarefa_1')
    task_2 = EmptyOperator(task_id = 'tarefa_2')
    task_3 = EmptyOperator(task_id = 'tarefa_3')
    task_4 = BashOperator(
        task_id = 'cria_pasta', 
        bash_command = 'mkdir -p "/home/carvalhosmari/Documents/airflowAlura/pasta:{{data_interval_end}}"'
    )

    task_1 >> [task_2, task_3]
    task_3 >> task_4