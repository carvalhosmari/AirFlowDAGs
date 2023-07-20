from airflow.models import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
from os.path import join
import pandas as pd

with DAG(
    'dados_climaticos',
    start_date = pendulum.datetime(2023, 6, 26, tz="UTC"),
    schedule_interval = "0 0 * * 1"
) as dag:
    
    task_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/carvalhosmari/Documents/airflowAlura/dados_climaticos/semana{{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):
        city = 'Itanhandu'
        access_key = 'FPE5WQFGRRLBX3W858VXYHXBA'

        URL = join(f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={access_key}&contentType=csv')
        
        extracted_data = pd.read_csv(URL)
        path = f"/home/carvalhosmari/Documents/airflowAlura/dados_climaticos/semana{data_interval_end}/"

        #salvando os dados
        extracted_data.to_csv(path + "dados_brutos.csv")
        extracted_data[['datetime', 'tempmin', 'tempmax', 'temp']].to_csv(path + "infos_temperatura.csv")

    task_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    task_1 >> task_2