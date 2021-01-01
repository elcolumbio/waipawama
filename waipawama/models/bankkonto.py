from pydantic import Field
import datetime
import numpy as np
import pandas as pd
import pathlib
from typing import List, Optional
from ..ingestion_template import (
    BaseModel,
    Meta)


class Bankkonto(BaseModel):
    """Input model after parsing used for testing, aliasing and dtypes."""
    Buchungstag: datetime.datetime
    Wertstellung: datetime.datetime
    Umsatzart: str
    Buchungstext: str
    Betrag: float
    Waehrung: str = Field(alias='Währung')
    Auftraggeberkonto: int
    Bankleitzahl: int = Field(alias='Bankleitzahl Auftraggeberkonto')
    IBAN: str = Field(alias='IBAN Auftraggeberkonto')
    Kategorie: str

    # Meta Columns
    Timespan: str = Field(description='The key parameter for every run of the complete datapipeline, represents a VAT period.')
    TimeInsert: datetime.datetime = Field(description='Timestamp when we insert the data, time is UTC and not local Time.')


class BankkontoMeta(Meta):
    """Wrapper for all the python functions for Bankkonto Model."""
    timespan: str = ''

    data: Optional[List[Bankkonto]] = None  # for  second step validating
    table_id: str = 'waipa-1.test.Bankkonto'
    model = Bankkonto  # We don't instantiate the Model with data

    #  somehow duplicate but it's explicit and easy to read
    dtypes: dict = {}
    parse_dates = ['Buchungstag', 'Wertstellung']

    @property
    def data_file(self):
        """Set first part in env."""
        return pathlib.Path('/home/flo/Nextcloud/data/finance/', self.timespan, 'bankkonto.CSV')

    @property
    def tmp_file(self):
        base_folder = '/home/flo/tests/'
        pathlib.Path(base_folder, self.timespan).mkdir(parents=True, exist_ok=True)
        return pathlib.Path('/home/flo/tests/', self.timespan, 'bankkonto.CSV')

    def read_csv(self):
        return pd.read_csv(
            self.data_file,
            dtype=self.dtypes,
            encoding='utf-8',
            sep=';',
            decimal=',',
            thousands=None,
            dayfirst=True,
            parse_dates=self.parse_dates)
