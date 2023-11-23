from datetime import datetime
from airflow import DAG
import pendulum 
import psycopg2
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
import io

# коннект к вертике
vertica_conn_id = 'vertica'
vertica_hook = VerticaHook(vertica_conn_id)
vertica = vertica_hook.get_conn()

# запрос на получение агрегированных данных и запись
query_insert_data = """
 INSERT INTO YASHMAOBLYANDEXRU__DWH.global_metrics 
 SELECT t.transaction_dt::DATE,
		t.currency_code,
		SUM(t.amount) / COALESCE (MAX(c.currency_with_div), 1) AS amount_total,
		COUNT(DISTINCT t.operation_id) AS cnt_transaction,
		SUM(t.amount) / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
		COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
   FROM YASHMAOBLYANDEXRU__STAGING.transactions t
		LEFT JOIN YASHMAOBLYANDEXRU__STAGING.currencies c
			ON t.currency_code = c.currency_code
				AND t.transaction_dt::date = c.date_update::date 
				AND c.currency_code_with = 420
  WHERE (t.account_number_from > 0
  		OR t.account_number_to  > 0)
  		AND t.transaction_dt::date = '{}'::DATE - 1 
  		AND t.status IN ('done', 'chargeback')
  GROUP BY t.transaction_dt::DATE,
        t.currency_code;
                """

# запрос на удаление возможных старых данных
query_delete_old_data = """ 
            DELETE FROM YASHMAOBLYANDEXRU__DWH.global_metrics 
            WHERE  date_update::DATE =  '{}'::DATE - 1   
                """

# функция удаления и записи 
def get_save_data(vertica, query_delete_old_data, query_insert_data, date):
     with vertica as conn:
        with conn.cursor() as cur:
            cur.execute(query_delete_old_data.format(date))
            print(query_insert_data.format(date))
            cur.execute(query_insert_data.format(date))
            conn.commit()

dag = DAG('dwh', 
        description='delete old and save new mart', 
        schedule_interval='0 1 * * *',
        start_date=pendulum.parse('2022-10-01'), 
        end_date=pendulum.parse('2022-10-31'),
        catchup=True)

etl_dwh = PythonOperator(task_id='dwh', 
                            python_callable=get_save_data, 
                            op_kwargs={
                                'vertica': vertica,
                                'query_delete_old_data': query_delete_old_data, 
                                'query_insert_data' : query_insert_data,
                                'date': '{{ ds }}',
                            },
                            dag=dag)

etl_dwh

