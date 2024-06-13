from airflow import DAG


from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


from airflow.utils.task_group import TaskGroup


from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
# from airflow.providers.virtualenv.operators.virtualenv import PythonVirtualenvOperator
# from airflow.operators.python_virtualenv_operator  import PythonVirtualenvOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)




def my_python_function():
    pass



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

# requirements = DummyOperator(
#     task_id='requirements',
#     dag=dag,
# )




requirements = PythonVirtualenvOperator(
    task_id='requirements',
    python_callable=my_python_function,
    requirements=['scrapy'],  # Danh sách các packages cần cài đặt trong môi trường ảo
    python_version='3.8',     # Phiên bản Python sẽ sử dụng trong môi trường ảo
    dag=dag,
)


# crawler = DummyOperator(
#     task_id='crawler',
#     dag=dag,
# )




crawler = BashOperator(
    task_id='crawler',
    bash_command="scripts/crawler.sh",
    dag=dag,
)


# with TaskGroup(group_id='etl', dag=dag) as etl:
#     t0 = DummyOperator(task_id='task1', dag=dag)
#     t1 = DummyOperator(task_id='task2', dag=dag)
#     [t0, t1]


send_email = DummyOperator(
    task_id='send_email',
    dag=dag,
)


# send_email = EmailOperator(
#     task_id='send_email',
#     to='lebaoxuan2005@gmail.com',
#     subject='Data Warehouse',
#     html_content="""<h1>Chào bạn,</h1> <p>Đây là thông báo công việc từ Airflow.</p> <a style=" background-color: #04aa6d; color: white; padding: 10px; text-decoration: none; border-radius: 12px; "href="http://localhost:6205/index.php?route=/database/structure&db=crawler" target="_blank"> &#128073; Truy cập MySQL </a> <p> <strong> Vũ Văn Nghĩa </strong> </p> <p> <strong> MSSV: 20206205 </strong> </p>""",
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
start >> requirements>>crawler >> send_email >> end
# start >>requirements>> crawler >> send_email >> end
