from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
import datetime
from waipawama.models.jtl_invoice import JtlMeta
from waipawama.models.paypal import PaypalMeta


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
def jtl_dag():
    """Ingestion for Jtl and Paypal."""
    @task()
    def jtl_external_file() -> str:
        timespan = get_timespan()  # e.g '2021-01'
        meta = JtlMeta(timespan=timespan)
        meta.DataFileExists  # throws error if not
        return timespan

    @task()
    def jtl_write_parquet(timespan) -> str:
        meta = JtlMeta(timespan=timespan)
        meta.save_as_parquet()
        return timespan

    @task()
    def jtl_load_to_bigquery(timespan):
        meta = JtlMeta(timespan=timespan)
        if not meta.TableExists:
            meta.create_table()
        meta.append_data()
        return timespan

    # same for Paypal
    @task()
    def paypal_external_file() -> str:
        timespan = get_timespan()  # e.g '2021-01'
        meta = PaypalMeta(timespan=timespan)
        meta.DataFileExists  # throws error if not
        return timespan

    @task()
    def paypal_write_parquet(timespan) -> str:
        meta = PaypalMeta(timespan=timespan)
        meta.save_as_parquet()
        return timespan

    @task()
    def paypal_load_to_bigquery(timespan):
        meta = PaypalMeta(timespan=timespan)
        if not meta.TableExists:
            meta.create_table()
        meta.append_data()
        return timespan

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=('source ~/dbt-env/bin/activate && '
                      'cd ~/projects/accountant/ && dbt test'))

    timespan = jtl_external_file()
    timespan = jtl_write_parquet(timespan)
    timespan = jtl_load_to_bigquery(timespan)

    timespan2 = paypal_external_file()
    timespan2 = paypal_write_parquet(timespan2)
    timespan2 = paypal_load_to_bigquery(timespan2)

    dbt_test.set_upstream([timespan, timespan2])


jtl_etl_dag = jtl_dag()
