from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
import datetime
from waipawama.models.taxdoo import TaxdooMeta


def get_timespan() -> str:
    """This is our main parameter in our monthly pipeline."""
    context = get_current_context()
    year_month = '-'.join(context['ds'].split('-')[:2])  # e.g. '2019-09'
    year_month = '2018-02'
    return year_month


@dag(default_args={'owner': 'florian'},
     schedule_interval='@monthly',
     start_date=datetime.datetime(2018, 12, 1),
     tags=['VAT'])
def taxdoo_dag():
    """Ingestion for Taxdoo."""
    @task()
    def taxdoo_external_file() -> str:
        timespan = get_timespan()  # e.g '2021-01'
        meta = TaxdooMeta(timespan=timespan)
        meta.DataFileExists  # throws error if not
        return timespan

    @task()
    def taxdoo_write_parquet(timespan) -> str:
        meta = TaxdooMeta(timespan=timespan)
        meta.save_as_parquet()
        return timespan

    @task()
    def taxdoo_load_to_bigquery(timespan):
        meta = TaxdooMeta(timespan=timespan)
        if not meta.TableExists:
            meta.create_table()
        meta.update_table()  # relaxation and add columns possible
        meta.append_data()
        return timespan

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=('source ~/dbt-env/bin/activate && '
                      'cd ~/projects/accountant/ && dbt test'))

    timespan = taxdoo_external_file()
    timespan = taxdoo_write_parquet(timespan)
    timespan = taxdoo_load_to_bigquery(timespan)

    dbt_test.set_upstream(timespan)


taxdoo_etl_dag = taxdoo_dag()
