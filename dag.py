from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def process_logs(**kwargs):
    spark = SparkSession.builder.appName("Log Aggregation").getOrCreate()

    # Параметры
    input_dir = "/path/to/input"
    output_dir = "/path/to/output"
    target_date = kwargs['execution_date'].strftime('%Y-%m-%d')

    # Загрузка данных
    logs = []
    for i in range(7):
        date_str = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d')
        file_path = os.path.join(input_dir, f"{date_str}.csv")
        if os.path.exists(file_path):
            logs.append(spark.read.csv(file_path, header=True))

    if logs:
        # Объединяем данные
        all_logs = logs[0].union(*logs[1:])

        # Агрегация
        aggregated = all_logs.groupBy("email").agg(
            {'action': 'count'}
        ).withColumnRenamed("count(action)", "action_count")

        # Подсчет по типам действий
        result = aggregated.groupBy("email").pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]).agg(
            {"action_count": "sum"})

        # Запись результата
        output_file = os.path.join(output_dir, f"{target_date}.csv")
        result.write.csv(output_file, header=True)

    spark.stop()


with DAG('log_aggregation_dag', default_args=default_args, schedule_interval='0 7 * * *',
         start_date=datetime(2024, 9, 16), catchup=False) as dag:
    aggregate_task = PythonOperator(
        task_id='aggregate_logs',
        provide_context=True,
        python_callable=process_logs,
    )

    aggregate_task
