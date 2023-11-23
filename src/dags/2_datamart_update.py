from datetime import datetime
from airflow import DAG
import pendulum 
import psycopg2
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
import io

# коннект к постгре
postgres_conn_id = 'postgres'
psql_hook = PostgresHook(postgres_conn_id)
psql = psql_hook.get_conn()

# запросы транзакций
query_get_transactions = """
            copy (
                select * 
                from public.transactions
                where transaction_dt::date = '{}'::date-1
                    ) 
                to stdout;
                """

query_delete_old_transactions = """
            delete from YASHMAOBLYANDEXRU__STAGING.transactions
            where transaction_dt::date = '{}'::date-1;
                """

query_save_data = f""" 
                copy YASHMAOBLYANDEXRU__STAGING.transactions
                from stdin 
                delimiter e'\t';
                """

# запросы курсов 
query_get_currencies = """
            copy (
                select * 
                from public.currencies
                where date_update::date = '{}'::date-1
                    ) 
                to stdout;
                """

query_delete_old_currencies = """
            delete from YASHMAOBLYANDEXRU__STAGING.currencies
            where date_update::date = '{}'::date-1;
                """

query_save_data_currencies = f""" 
                copy YASHMAOBLYANDEXRU__STAGING.currencies
                from stdin 
                delimiter e'\t';
                """

# коннект к вертике
vertica_conn_id = 'vertica'
vertica_hook = VerticaHook(vertica_conn_id)
vertica = vertica_hook.get_conn()

# функции транзакций 
def get_transaction_postgre(connection_get, query_get, date):
    with connection_get as conn:
        with conn.cursor() as cur:
            sql = (query_get.format(date))
            input = io.StringIO()
            cur.copy_expert(sql, input)
    return input

def save_data(connection_load, query_delete_old_transactions, query_save_data, date, data):
     with connection_load as conn:
        with conn.cursor() as cur:
            cur.execute(query_delete_old_transactions.format(date))
            cur.copy(query_save_data, data.getvalue())

def etl_transaction(connection_get, query_get_transactions, date, connection_load, query_delete_old_transactions, query_save_data):
    data = get_transaction_postgre(connection_get, query_get_transactions, date)
    save_data(connection_load, query_delete_old_transactions, query_save_data, date, data)

# функции курсов 
def get_currencies_postgre(connection_get, query_get_currencies, date):
    with connection_get as conn:
        with conn.cursor() as cur:
            sql = (query_get_currencies.format(date))
            input = io.StringIO()
            cur.copy_expert(sql, input)
    return input

def save_data_currencies(connection_load, query_delete_old_currencies, query_save_data_currencies, date, data):
     with connection_load as conn:
        with conn.cursor() as cur:
            cur.execute(query_delete_old_currencies.format(date))
            cur.copy(query_save_data_currencies, data.getvalue())

def etl_currencies(connection_get, query_get_currencies, date, connection_load, query_delete_old_currencies, query_save_data_currencies):
    data = get_transaction_postgre(connection_get, query_get_currencies, date)
    save_data(connection_load, query_delete_old_currencies, query_save_data_currencies, date, data)

dag = DAG('stg', 
        description='get data from postgre and safe to Vertica, twice', 
        schedule_interval='@daily',
        start_date=pendulum.parse('2022-10-01'), 
        end_date=pendulum.parse('2022-10-31'),
        catchup=True)


etl_transactions = PythonOperator(task_id='etl_transactions', 
                            python_callable=etl_transaction, 
                            op_kwargs={
                                'connection_get': psql,
                                'query_get_transactions': query_get_transactions, 
                                'connection_load' : vertica,
                                'query_delete_old_transactions': query_delete_old_transactions,
                                'query_save_data':query_save_data,
                                'date': '{{ ds }}',
                            },
                            dag=dag)

etl_currencies = PythonOperator(task_id='etl_currencies', 
                            python_callable=etl_currencies, 
                            op_kwargs={
                                'connection_get': psql,
                                'query_get_currencies': query_get_currencies, 
                                'connection_load' : vertica,
                                'query_delete_old_currencies': query_delete_old_currencies,
                                'query_save_data_currencies':query_save_data_currencies,
                                'date': '{{ ds }}',
                            },
                            dag=dag)


[etl_transactions, etl_currencies]

