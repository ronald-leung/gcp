import airflow
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow import AirflowException

# Setup DAG and various variables before the tasks kick off
dag = DAG('BigQuery_to_pandas_test', description='Query data from bigquery and then use pandas library for verifying results',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dag_config = Variable.get('pandas_qa_test', deserialize_json=True)
startdate = str(airflow.utils.dates.days_ago(1))[:10]
bigquerySQL = 'SELECT [add your own columns]  From %s.%s.%s WHERE eventdate = "%s" and type is not null order by timestamp desc' % (dag_config['source_project'], dag_config['source_dataset'], dag_config['source_tablename'], startdate),

# Helper function and task for logging some info
def setup_info():
    print("dag_config: ", dag_config)
    print("bigquerySQL:", bigquerySQL)
    return
setup_task = [PythonOperator(task_id='setup', python_callable=setup_info, dag=dag)]

# Load data from Bigquery, store result in a temp table
load_test_data_task = BigQueryOperator(
    task_id='Load_test_data',
    dag=dag,
    sql=bigquerySQL,
    destination_dataset_table="%s.%s" % (dag_config['destination_dataset'], dag_config['destination_table']),
    use_legacy_sql=False,
    write_disposition='WRITE_TRUNCATE'
)

# Load Bigquery result data from temp table
load_query_results_task = BigQueryGetDataOperator(
                            task_id='bq_get_data',
                            dataset_id=dag_config['destination_dataset'],
                            table_id=dag_config['destination_table'],
                            dag=dag,
                            max_results=100000
                        )

# Load data into Pandas and apply test logic
def load_bq_result_intoPandas(**kwargs):
    print(kwargs)
    ti = kwargs['ti']
    resultData = ti.xcom_pull(task_ids='bq_get_data')

    df = pd.DataFrame(resultData)
    df.columns=['your columns', 'your columns']
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert('US/Pacific')
    print("Retrieved ", len(df), " rows")
    print(df.head())

    # Now the data is in a pandas dataframe, you can use perform whatever logic you like.
    testResult = True

    if (testResult):
        print("Test passes, awesome.")
    else:
        print("Test failed somehow.")
        raise ValueError('Failing task because test failed.')


    return "Done"

pandas_test_task = PythonOperator(task_id='pandas_test', python_callable=load_bq_result_intoPandas, provide_context=True, dag=dag)

setup_task >> load_test_data_task >> load_query_results_task >> pandas_test_task
