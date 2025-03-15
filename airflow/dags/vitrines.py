from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('analytic_vitriina_dag',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False)

create_user_activity_vitrine = PostgresOperator(
    task_id='create_user_activity_vitrine',
    postgres_conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS user_activity_vitrine;
        CREATE TABLE user_activity_vitrine AS
        SELECT 
            user_id,
            COUNT(*) AS session_count,
            AVG(session_duration) AS avg_session_duration
        FROM user_sessions
        GROUP BY user_id;
    """,
    dag=dag
)

create_support_efficiency_vitrine = PostgresOperator(
    task_id='create_support_efficiency_vitrine',
    postgres_conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS support_efficiency_vitrine;
        CREATE TABLE support_efficiency_vitrine AS
        SELECT 
            issue_type,
            COUNT(*) AS ticket_count,
            AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 60) AS avg_resolution_time_minutes
        FROM support_tickets
        GROUP BY issue_type;
    """,
    dag=dag
)

create_user_activity_vitrine >> create_support_efficiency_vitrine
