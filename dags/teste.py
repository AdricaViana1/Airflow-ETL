from airflow import DAG

from datetime import datetime
#operador que vai executar a ação


from airflow.operators.python import PythonOperator, BranchPythonOperator

default_arguments={
    'owner':'Maria',
    'start_date': datetime(2023,3, 22)
}


def ler_csv():
    import pandas as pd
    teste = pd.read_csv('C://Users//adria//Documents//Trabalho - Central//Datasets//alunos.csv')
    
    return teste.head()


with DAG('teste', start_date= datetime(2023,3, 20), schedule='* */6 * * *', default_args=default_arguments,catchup=False) as dag:
    ler = PythonOperator(
        task_id='ler_csv',
        python_callable=ler_csv
    )
    
    ler
    