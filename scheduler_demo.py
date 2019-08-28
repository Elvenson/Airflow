import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from configs import *


default_args = {
    'owner': 'Bob',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 22, 9, 30),
    'email': ['abc@gmail.com'],
    'email_on_failure': ['abc@gmail.com'],
    'email_on_retry': False,
    'retries': 0,
    'catchup_by_default': False,
    'params': build_params("/Users/bao.bui/airflow/dags/Demo/conf")
}


dag = DAG('Demo3', default_args=default_args, schedule_interval=timedelta(days=1))


def check_dir1():
    """
        Check dir1 directory
        :return: String, bash command line to get the current existing dir
    """
    return """
        if hdfs dfs -test -e {{ params.dir1_today }}/_SUCCESS; then
            echo {{ params.dir1_today }} # Get the current date
        else
            echo {{ params.dir1_yesterday }} # Get the yesterday date
        fi
        """


def check_dir2():
    """
        Check dir2 directory
        :return: String, bash command line to get the current existing dir
    """
    return """
        if hdfs dfs -test -e {{ params.dir2_today }}/_SUCCESS; then
            echo '{{ params.dir2_today }}' # Get the current date
        else
            echo '{{ params.dir2_yesterday }}' # Get the yesterday date
        fi
        """


def job1():
    """
    Run job1 bash job
    :return: String, bash command line to run job1 bash job
    """
    return """
        echo Got {{ ti.xcom_pull(task_ids='check_dir1') }}
        echo Got {{ ti.xcom_pull(task_ids='check_dir2') }}
    """


def job2():
    """
    Run job2 bash job
    :return: String, bash command line to run job2 bash job
    """
    return """
        echo run job2 after job1
    """


def job3():
    """
    Run job3 bash job
    :return: String, bash command line to run job3 bash job
    """
    return """
        echo run job3 after job1
    """


check_dir1_bash = BashOperator(
    task_id="check_dir1",
    bash_command=check_dir1(),
    trigger_rule="all_done",
    xcom_push=True,
    dag=dag
)

check_dir2_bash = BashOperator(
    task_id="check_dir2",
    bash_command=check_dir2(),
    trigger_rule="all_done",
    xcom_push=True,
    dag=dag
)

job1_bash = BashOperator(
    task_id="job1",
    bash_command=job1(),
    trigger_rule="all_success",
    dag=dag
)

job2_bash = BashOperator(
    task_id="job2",
    bash_command=job2(),
    trigger_rule="all_success",
    dag=dag
)

job3_bash = BashOperator(
    task_id="job3",
    bash_command=job3(),
    trigger_rule="all_success",
    dag=dag
)

[check_dir1_bash, check_dir2_bash] >> job1_bash
job1_bash.set_downstream(job2_bash)
job1_bash.set_downstream(job3_bash)



