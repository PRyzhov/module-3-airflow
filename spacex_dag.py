from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

rockets={'all' : '','falcon1' : 'falcon1', 'falcon9' : 'falcon9', 'falconheavy' : 'falconheavy'}
    

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2005, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")

for rocket in rockets:
    switch = ' -r %s' % rockets[rocket] if rockets[rocket] else ''
    t1 = BashOperator(
        task_id='get_data_for_%s' % rocket,
        bash_command='python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }}%s -o /var/data' % switch,
        dag=dag,
    )

    t2 = BashOperator(
        task_id="print_data_for_%s" % rocket, 
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket=%s/data.csv" % rocket, 
        dag=dag
    )

    t1 >> t2
