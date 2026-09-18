import datetime, os, json, tomllib
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable
from docker.types import Mount
from airflow.sdk.exceptions import AirflowRuntimeError



SRC_SCHEMA, DEST_SCHEMA = 'firefly_db', 'main'
CONFIGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')
with open(os.path.join(CONFIGS_DIR, 'tables.toml'), "rb") as f:
    toml_config = tomllib.load(f)

with open(os.path.join(CONFIGS_DIR, 'config.json'), "r") as f:
    config = json.load(f)


#TODO: Get repeated code into function
def transfer_data(**context):
    #* tag_transaction_journal
    ts_cols = config['firefly_config']['timestamp_cols']

    source_hook = MySqlHook(schema=SRC_SCHEMA, mysql_conn_id='ff-mariadb')

    print(f"MySQL DB hook established.")

    dest_hook = PostgresHook(postgres_conn_id='ndw_ff')

    source_conn = source_hook.get_conn()
    dest_conn = dest_hook.get_conn()

    print("Connections successfully created")

    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()
    print("Cursors successfully created")
    for table in toml_config['tables']:
        tbl_name, wh_name = table['name'], table['wh_name']

        print(f"Processing table: {tbl_name}")

        #! -------------------------------------------------------------------
        #! Error in trying to get the tags table data
        #! -------------------------------------------------------------------

        # create the column tuple for insertion/upsertion
        cols, num_cols = f"({','.join(f'"{i}"' for i in table['columns'])})", len(table['columns'])
        if table['is_main']:
            wm_table = table['af_watermark']
            try:
                val = Variable.get(wm_table)
            except AirflowRuntimeError:
                print("No variable set yet.")
                #*Pull all data
                source_cursor.execute(f'SELECT * FROM {SRC_SCHEMA}.{tbl_name} WHERE deleted_at IS NULL')

                rows = source_cursor.fetchall()
                if not rows:
                    print(f"No data found for table {tbl_name}. !!! Check on this.")
                    continue

                #* Overwrite destination data (just in case)
                try:
                    dest_cursor.execute(f'TRUNCATE {DEST_SCHEMA}.{wh_name};')
                    dest_cursor.executemany(
                        f'INSERT INTO {DEST_SCHEMA}.{wh_name} ' + cols + ' VALUES ' + f"({', '.join([r'%s'] * num_cols)})",
                        rows)
                except Exception as ex:
                    dest_cursor.rollback()
                    print(f"Exception caught executing dest_cursor: {ex}")
                    raise(ex)
                else:
                    print(f"SUCCESS!! Full write to table: {tbl_name}")
                    dest_conn.commit()
            else:
                print(f"Getting all data after watermark timestamp: {val}")
                source_cursor.execute(f'''
                    SELECT * 
                    FROM {SRC_SCHEMA}.{tbl_name}
                    WHERE GREATEST(
                        CONVERT_TZ({ts_cols['updated']}, 'America/New_York', 'UTC'),
                        CONVERT_TZ({ts_cols['created']}, 'America/New_York', 'UTC')
                    ) > %s
                ''',
                (val,))
                rows = source_cursor.fetchall()

                if not rows:
                    print(f"No updated data found for table {tbl_name}")
                    continue

                try:
                    print("Updates detected...")
                    active_rows = [r for r in rows if r[3] is None]  # if deleted_at is Null
                    deleted_ids = [r[0] for r in rows if r[3] is not None]
                    # upsert
                    dest_cursor.executemany(
                        f'INSERT INTO {DEST_SCHEMA}.{wh_name} ' + cols + ' VALUES ' + f"({', '.join([r'%s'] * num_cols)})" + f' ON CONFLICT (id) DO UPDATE SET {",".join(f"\"{col}\" = EXCLUDED.\"{col}\"" for col in table['columns'] if col!='id')}',
                        active_rows)
                    if deleted_ids:
                        # delete soft-deleted items
                        dest_cursor.execute(
                            f"DELETE FROM {DEST_SCHEMA}.{wh_name} WHERE id=ANY(%s)",
                            (deleted_ids,)
                        )
                except Exception as ex:
                    print(f"Exception caught executing dest_cursor: {ex}")
                    dest_conn.rollback()
                    raise(ex)
                else:
                    print(f"SUCCESS!! Upsert to table: {tbl_name}")
                    dest_conn.commit()
            
            Variable.set(
                wm_table,
                (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            )
        else:
            print(f"Getting all data for junction table, {tbl_name}")
            source_cursor.execute(f'SELECT * FROM {SRC_SCHEMA}.{tbl_name}')
            rows = source_cursor.fetchall()
            if not rows:
                print(f"No data found for junction table {tbl_name}. !!! Check on this.")
                continue
            
            try:
                dest_cursor.execute(f'TRUNCATE {DEST_SCHEMA}.{wh_name};')
                dest_cursor.executemany(
                    f'INSERT INTO {DEST_SCHEMA}.{wh_name} ' + cols + ' VALUES ' + f"({', '.join([r'%s'] * num_cols)})",
                    rows)
            except Exception as ex:
                print(f"Exception caught executing dest_cursor: {ex}")
                dest_cursor.rollback()
                raise(ex)
            else:
                print(f"SUCCESS!! Full write to junction table: {tbl_name}")
                dest_conn.commit()


dag = DAG(
    dag_id="ff-transactions-data-transport",
    description="Orchestrator for FireFly III transaction transport.",
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
        task_id='ff-transfer-data',
        python_callable=transfer_data
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
            'DEST_FF_USER_PASSWORD': os.getenv('DEST_FF_USER_PASSWORD')
        },
        network_mode='de-net',
        working_dir='/usr/app',
        mounts=[transformations_dir, dbt_profiles_dir]
    )

    xfer# >> dbt_task

# if __name__ == '__main__':
#     dag.test()