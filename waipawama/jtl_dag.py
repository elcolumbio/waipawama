from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
import datetime
import pandas as pd
import pathlib
import numpy as np
from waipawama.jtl_invoice import JtlMeta

def get_timespan() -> str:
    """This is our main parameter in our monthly pipeline."""
    context = get_current_context()
    year_month = '-'.join(context['ds'].split('-')[:2])  # e.g. '2019-09'
    year_month = '2018-02'
    return year_month


@dag(default_args={'owner': 'florian'},
    schedule_interval='@monthly',
    start_date=datetime.datetime(2018,12,1),
    tags=['VAT'])
def jtl_dag():
    @task()
    def external_file() -> str:
        timespan = get_timespan()  # e.g '2021-01'
        meta = JtlMeta(timespan=timespan)
        meta.DataFileExists  # throws error if not
        return timespan


    @task()
    def write_parquet(timespan) -> str:
        meta = JtlMeta(timespan=timespan)
        meta.save_as_parquet()
        return timespan


    @task()
    def load_to_bigquery(timespan):
        meta = JtlMeta(timespan=timespan)
        if not meta.TableExists:
            meta.create_table()
        meta.append_data()

    timespan = external_file()
    timespan = write_parquet(timespan)
    load_to_bigquery(timespan)


jtl_etl_dag = jtl_dag()