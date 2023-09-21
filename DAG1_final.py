from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Final1',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    comienza_proceso = DummyOperator(task_id='comienza_proceso')  

    ingest = BashOperator(
        task_id='ingest',
        bash_command='/usr/bin/sh /home/hadoop/scripts/ingestion.sh ',
    )

    with TaskGroup('transformacion', tooltip='transformacion') as Process:
        processing_table_1 = BashOperator(
            task_id='processing_table_1',
            bash_command='/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/aeropuerto_tabla.py '
        )
        processing_table_2 = BashOperator(
            task_id='processing_table_2',
            bash_command='/home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/aeropuerto_detalles_tabla.py '
        )

    finaliza_proceso = DummyOperator(task_id='finaliza_proceso')

    comienza_proceso >> ingest >> [processing_table_1, processing_table_2] >> finaliza_proceso

if __name__ == "__main__":
    dag.cli()

