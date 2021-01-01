from pydantic import Field
import datetime
import numpy as np
import pandas as pd
import pathlib
from typing import List, Optional
from airflow.exceptions import AirflowFailException
from .ingestion_template import (
    BaseModel,
    Meta)


class JtlInvoice(BaseModel):
    """Input model after parsing used for testing, aliasing and dtypes."""

    Rechnungsnummer: str = Field(description='Auto generated invoice number by our inventory management system.')
    BetragBrutto: float = Field(alias='Gesamtbetrag Brutto (alle Ust.)', description='For all positions in the invoice together.')
    DatumRechnung: datetime.datetime = Field(alias='Erstelldatum Rechnung', description='Relevant date for period of taxation.')
    Steuerschluessel: Optional[int] = Field(alias='Steuerschlüssel', description='For each VAT case there is a number.')
    # resolve in transformation just addd new fields here
    Steuerschluessel2: Optional[int] = Field(alias='Steuerschlüsselnummer', description='Another entrypoint for Steuerschluessel.')
    ExterneId: Optional[str] = Field(alias='Externe Transaktion-ID', description='For example the transaction id from another platform like ebay.')

    # Meta Columns
    Timespan: str = Field(description='The key parameter for every run of the complete datapipeline, represents a VAT period.')
    TimeInsert: datetime.datetime = Field(description='Timestamp when we insert the data, time is UTC and not local Time.')


class JtlMeta(Meta):
    """Wrapper for all the python functions for JtlData Model."""
    timespan: str = ''

    data: Optional[List[JtlInvoice]] = None  # for  second step validating
    table_id: str = 'waipa-1.test.JtlInvoice'
    model = JtlInvoice  # We don't instantiate the Model with data

    #  somehow duplicate but it's explicit and easy to read
    dtypes: dict = {
        'Rechnungsnummer': object,
        'Gesamtbetrag Brutto (alle Ust.)': np.float64,
        'Erstelldatum Rechnung': object,
        'Steuerschlüssel': int,
        'Steuerschlüsselnummer': int,
        'Externe Transaktion-ID': object}
    parse_dates = ['Erstelldatum Rechnung']

    @property
    def data_file(self):
        """Set first part in env."""
        return pathlib.Path('/home/flo/Nextcloud/data/finance/', self.timespan, 'jtl_ebay.csv')

    @property
    def DataFileExists(self):
        if self.data_file.exists():
            return True
        else:
            raise AirflowFailException(f'No such file available: {str(self.data_file)}.')

    @property
    def tmp_file(self):
        base_folder = '/home/flo/tests/'
        pathlib.Path(base_folder, self.timespan).mkdir(parents=True, exist_ok=True)
        return pathlib.Path('/home/flo/tests/', self.timespan, 'jtl_ebay.csv')

    def read_csv(self):
        return pd.read_csv(
            self.data_file,
            dtype=self.dtypes,
            encoding='latin-1',
            sep=';',
            decimal=',',
            thousands=None,
            dayfirst=True,
            parse_dates=self.parse_dates)
