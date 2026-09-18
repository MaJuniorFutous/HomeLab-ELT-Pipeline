import datetime
from typing import Optional
import pandas as pd, numpy as np
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
from airflow.sdk.exceptions import AirflowRuntimeError
import gspread, gspread_dataframe as dfspread

from kalman_filter import KalmanFilter
from kalman_filter.utils import np_ident, np_arr
from helpers.google import read_sheet, create_worksheet_obj
from helpers.google.utils import create_google_sa_obj
from helpers.google.creds.creds import SCOPES, CREDS



DEST_SCHEMA, TABLE_NAME = 'public', 'bodyweight'
def _pandas_to_tuple(df):
    return [tuple(i) for i in df.to_numpy()]

def _set_watermark(wm: Optional[str] = None):
    wm = wm if wm else datetime.datetime.today().strftime('%Y-%m-%d')
    Variable.set(
        'wm_bodyweight_ts',
        wm
    )

def xfer_sheet():
    print("Running task - xfer_sheet")
    BW_COLS = ['bodyweight', 'day']
    dest_hook = PostgresHook(postgres_conn_id='ndw_fitness')
    dest_conn = dest_hook.get_conn()
    dest_cursor = dest_conn.cursor()

    df = read_sheet(
        ws_obj=create_worksheet_obj(spreadsheet='gcpae_db', sheet1=True),
        columns=True,
        pandas=True
    )[['body_weight', 'datetime']]
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'])
        try:
            val = Variable.get('wm_bodyweight_ts')
        except AirflowRuntimeError:
            print("No variable set yet.")
            #* Overwrite destination data (just in case)
            try:
                # get most recent date and use that
                dest_cursor.execute(f'SELECT MAX(day) as latest_day FROM {DEST_SCHEMA}.{TABLE_NAME};')
                rows = dest_cursor.fetchall()

                if rows[0][0]:
                    new = df[df['datetime'].dt.date > rows[0][0]]
                    if not new.empty:
                        dest_hook.insert_rows(
                            table=TABLE_NAME,
                            rows=_pandas_to_tuple(df=new),
                            target_fields=BW_COLS,
                            executemany=True,
                            autocommit=True
                        )
                    else:
                        return False
                else:
                    # dest_cursor.execute(f'TRUNCATE {DEST_SCHEMA}.{TABLE_NAME};')  #* clear table
                    dest_hook.insert_rows(
                        table=TABLE_NAME,
                        rows=_pandas_to_tuple(df=df),
                        target_fields=BW_COLS,
                        executemany=True,
                        autocommit=True
                    )
                    print("No data in the dest DB. Adding entire df.")
                    
                wm = df['datetime'].max().strftime('%Y-%m-%d')
            except Exception as ex:
                print(f"Exception caught executing dest_cursor: {ex}")
                raise(ex)
            else:
                print(f"SUCCESS!! Full write to table.")
                _set_watermark(wm=wm)
                return True
        else:
            print(f"Getting all data after watermark timestamp: {val}")
            df = df[df['datetime'] > val].drop_duplicates('datetime', keep='last')  #* eg. val = '2025-06-05'
            if not df.empty:
                wm = df['datetime'].max().strftime('%Y-%m-%d')
                try:
                    # upsert
                    # dest_hook.upsert_rows(
                    #     table=TABLE_NAME,
                    #     rows=_pandas_to_tuple(df=df),
                    #     target_fields=BW_COLS,
                    #     conflict_fields=['day'],
                    #     update_fields=[],
                    #     commit_every=100,
                    #     autocommit=True
                    # )
                    # upsert (custom)
                    dest_cursor.executemany(
                        f"""
                        INSERT INTO {DEST_SCHEMA}.{TABLE_NAME} (bodyweight, day)
                        VALUES (%s, %s)
                        ON CONFLICT (day) DO NOTHING;
                        """,
                        _pandas_to_tuple(df=df)
                    )
                    dest_conn.commit()
                except Exception as ex:
                    raise ex
                else:
                    _set_watermark(wm=wm)
                    return True
            else:
                print(f"No new data after watermark: {val}")
                return False
    else:
        raise ValueError(
            "Google Sheet 'gcpae_db' returned no rows."
        )

def get_estimations():
    print("Running task - get_estimations")
    initial = True
    _gspread = gspread.authorize(create_google_sa_obj(creds=CREDS, scopes=SCOPES))
    bw_sheet = _gspread.open("gcpae_db").sheet1

    pg_wh_hook = PostgresHook(postgres_conn_id='ndw_fitness')
    df = pg_wh_hook.get_df(
        f'SELECT bodyweight AS body_weight, day AS datetime FROM {DEST_SCHEMA}.{TABLE_NAME} ORDER BY day DESC;', 
        df_type='pandas'
    )

    df['datetime'] = pd.to_datetime(df['datetime'])

    err_obs_pos = 0.015 #* default, standard bathroom scale error += 1% - 2% of current body weight
    n_state_var, n_measurement_var = 2, 1

    # For Q error matrix standard deviation value
    bw_perc = 0.0002

    if initial: batch_init_n = 1
    else: 
        df, batch_init_n = df[0:2], None

    df.sort_values('datetime', ascending=True, inplace=True)
    # get dynamic delta T
    df["delta_t"] = (
        (df["datetime"] - df["datetime"].shift(1)).dt.days
    )
    df['A'] = [np_arr([[1, dt if not np.isnan(dt) else 0],[0, 1]]) for dt in df['delta_t']]
    # create Q and R matrices
    df['R'] = [np_ident(n_measurement_var) * ((bw*err_obs_pos)**2) for bw in df['body_weight']]
    df['q'] = [((bw*bw_perc)**2)*np_arr([[d_t**4/4, d_t**3/2],[d_t**3/2, d_t**2]]) for d_t, bw in zip(df['delta_t'], df['body_weight'].shift(1))]

    if initial:
        filter = KalmanFilter(
            n_state_var=n_state_var,
            n_measurement_inputs=n_measurement_var,
            H=np_arr([[1, 0]])
        )
        #* pass dynamic Q (how “non-constant” your weight trend is) and R based on scale error as % bodyweight
        results = filter.forward(
            data=df['body_weight'],
            R=df['R'],
            q=df['q'],
            A=df['A'],
            return_history=True,
            batch_init_n=batch_init_n
        )
        # filter.save_state()
    else:
        filter = KalmanFilter.from_file(file='kfilter_save.npz')
        #* pass dynamic Q (how “non-constant” your weight trend is) and R based on scale error as % bodyweight
        results = filter.forward(
            data=df['body_weight'],
            R=df['R'],
            q=df['q'],
            A=df['A'],
            return_history=True,
            batch_init_n=batch_init_n
        )

    # remove all rows that dont have filter estimates
    df = df[batch_init_n:]
    df["est_bw"], df["est_vel"] = zip(*[(np.nan, np.nan)] * (len(df) - len(results)) + results)

    df.set_index('datetime', inplace=True)
    df['est_vel'] = (df['est_vel'] / df['est_bw']) * 100
    # df['7_day_rate'] = (df['est_vel'].pct_change()).round(2)
    df['7_day_cum_rate'] = (
        df['est_vel']
        .rolling('7D', min_periods=1)
        .sum()
        .round(2)
    )
    df['28_day_cum_rate'] = (
        df['est_vel']
        .rolling('28D', min_periods=1)
        .sum()
        .round(2)
    )
    df['est_vel'] = df['est_vel'].round(2)
    # df['est_bw'], df['est_vel'] = zip(*results)
    df = df.reset_index().sort_values('datetime', ascending=False)
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')
    df = df[['body_weight', 'datetime', 'est_bw', 'est_vel', '7_day_cum_rate', '28_day_cum_rate']]
    
    # Clear sheet and write the updated DataFrame back to it
    bw_sheet.clear()  # clears content only
    dfspread.set_with_dataframe(worksheet=bw_sheet, dataframe=df)


with DAG(
    dag_id="gsheet-bw",
    description="Orchestrator for body weight fitness tracker.",
    schedule='0 9-16 * * *',  # mornings
    start_date=datetime.datetime(2026, 6, 25),
    catchup=False
) as dag:
    xfer = ShortCircuitOperator(
        task_id="bw-py-transfer-data",
        python_callable=xfer_sheet,
    )
    
    kfilter_estimations = PythonOperator(
        task_id='bw_kfilter',
        python_callable=get_estimations
    )
    xfer >> kfilter_estimations


if __name__ == '__main__':
    print("Testing dag...")
    dag.test()
