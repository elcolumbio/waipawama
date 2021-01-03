from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
import datetime
from waipawama.models.bankkonto import BankkontoMeta


def get_timespan() -> str:
    """This is our main parameter in our monthly pipeline."""
    context = get_current_context()
    year_month = '-'.join(context['ds'].split('-')[:2])  # e.g. '2019-09'
    return year_month


@dag(default_args={'owner': 'florian'},
     schedule_interval='@monthly',
     start_date=datetime.datetime(2018, 12, 1),
     tags=['VAT'],
     max_active_runs=1)
def bankkonto_dag():
    """Ingestion for Bankkonto."""
    @task()
    def bankkonto_external_file() -> str:
        timespan = get_timespan()  # e.g '2021-01'
        meta = BankkontoMeta(timespan=timespan)
        meta.DataFileExists  # throws error if not
        return timespan

    @task()
    def bankkonto_write_parquet(timespan) -> str:
        meta = BankkontoMeta(timespan=timespan)
        meta.save_as_parquet()
        return timespan

    @task()
    def bankkonto_load_to_bigquery(timespan):
        meta = BankkontoMeta(timespan=timespan)
        if not meta.TableExists:
            meta.create_table()
        meta.update_table()  # relaxation and add columns possible
        meta.append_data()
        return timespan

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'source ~/dbt-env/bin/activate && '
            'cd ~/projects/accountant/ && '
            'dbt run --models bankkonto_monthly --vars '
            "'timespan: {{task_instance.xcom_pull(key='return_value')}}'"))

    @task()
    def bankkonto_write_lexware():
        timespan = get_timespan()
        meta = BankkontoMeta(timespan=timespan)
        meta.write_target()

    timespan = bankkonto_external_file()
    timespan = bankkonto_write_parquet(timespan)
    bankkonto_load_to_bigquery(timespan) >> dbt_run >> bankkonto_write_lexware()


bankkonto_etl_dag = bankkonto_dag()
