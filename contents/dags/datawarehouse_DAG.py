from airflow import DAG


from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


from airflow.utils.task_group import TaskGroup


from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


from update_job_salaries import update_job_salaries
from update_job_regions import update_job_regions


default_args = {
    'owner': 'Vũ Văn Nghĩa - 20206205',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,  # Số lần thử lại
    'retry_delay': timedelta(seconds=10),  # Thời gian chờ giữa các lần thử lại
    'email': ['lebaoxuan2005@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='workflow-datawarehouse',
    tags=['workflow-datawarehouse'],
    default_args=default_args,
    description='workflow-datawarehouse',
    # https://crontab.guru
    schedule_interval=None,
    # schedule_interval='0 0 * * *',
    # schedule_interval='@daily',
    catchup=False,
)


# Định nghĩa các task
start = DummyOperator(
    task_id='start',
    dag=dag,
)


requirements = BashOperator(
    task_id='requirements',
    bash_command="scripts/requirements.sh",
    dag=dag,
)


crawler = BashOperator(
    task_id='crawler',
    bash_command="scripts/crawler.sh",
    dag=dag,
)


with TaskGroup(group_id='etl', dag=dag) as etl:

    update_job_salaries_task = PythonOperator(
        task_id='update_job_salaries_task',
        python_callable=update_job_salaries,
        dag=dag,
    )
    update_job_regions_task = PythonOperator(
        task_id='update_job_regions_task',
        python_callable=update_job_regions,
        dag=dag,
    )

    [update_job_salaries_task, update_job_regions_task]


send_email = DummyOperator(
    task_id='send_email',
    dag=dag,
)


# send_email = EmailOperator(
#     task_id='send_email',
#     to='lebaoxuan2005@gmail.com',
#     subject='Data Warehouse',
#     html_content="""<h1>Chào bạn,</h1> <p>Đây là thông báo công việc từ Airflow.</p> <a style=" background-color: #04aa6d; color: white; padding: 10px; text-decoration: none; border-radius: 12px; "href="http://localhost:6205/index.php?route=/sql&db=crawler&table=jobs&pos=0" target="_blank"> &#128073; Truy cập MySQL </a> <p> <strong> Vũ Văn Nghĩa </strong> </p> <p> <strong> MSSV: 20206205 </strong> </p>""",
#     mime_charset='utf-8',
#     dag=dag,
# )


end = DummyOperator(
    task_id='end',
    dag=dag,
)


# Thiết lập thứ tự các task
# start >>   end
# start >>   send_email >> end
# start >> requirements>>  send_email >> end
# start >> requirements >> crawler >> send_email >> end
start >> requirements >> crawler >> etl >> send_email >> end
