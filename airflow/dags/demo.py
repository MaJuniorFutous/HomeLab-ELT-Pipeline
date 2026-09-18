import datetime, os
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable
from docker.types import Mount



# from elt.pull_script import main


def transfer_pg_data(**context):
    source_hook = PostgresHook(postgres_conn_id='source_pg_conn')
    dest_hook = PostgresHook(postgres_conn_id='dest_pg_conn')

    source_conn = source_hook.get_conn()
    dest_conn = dest_hook.get_conn()

    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    source_cursor.execute("SELECT * FROM public.events_sample")

    insert_sql = """
        INSERT INTO dev.bronze__dest_events_sample
        (user_id, device_id, referrer, host, url, event_time)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    batch_size = 1000

    while True:
        rows = source_cursor.fetchmany(batch_size)
        if not rows:
            break

        dest_cursor.executemany(insert_sql, rows)
        dest_conn.commit()

    source_conn.close()
    dest_conn.close()


dag = DAG(
    dag_id="events-data-transport",
    description="Orchestrator for events data transport",
    start_date=datetime.datetime(2026, 4, 1),
    schedule="@once",
    # schedule=timedelta(minutes=1),
    catchup=False
)

transformations_dir = Mount(
    target='/usr/app',
    source='/home/majunior/deployments/de/transformations/dbt/dbt_project',
    type='bind')
dbt_profiles_dir = Mount(
    target='/root/.dbt',
    source='/home/majunior/deployments/de/profiles',
    type='bind')

with dag:
    xfer = PythonOperator(
        task_id='demo-py-transfer-data',
        python_callable=transfer_pg_data
    )
    dbt_task = DockerOperator(
        task_id='dbt-run',
        auto_remove='success',
        mount_tmp_dir=False,
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.0',
        api_version='auto',
        command='run',
        docker_url='unix://var/run/docker.sock',
        environment={
            'DBT_PROFILES_DIR': '/root/.dbt',
            'DEST_PG_USER_PASSWORD': os.getenv('DEST_PG_USER_PASSWORD')
        },
        # env_file='/opt/airflow/.env',
        network_mode='de_net',
        working_dir='/usr/app',
        mounts=[transformations_dir, dbt_profiles_dir]
    )

    xfer >> dbt_task