from pydantic import Field
import datetime
import numpy as np
import pandas as pd
import pathlib
from typing import List, Optional
from ..ingestion_template import (
    BaseModel,
    Meta)


class Paypal(BaseModel):
    """Optional and Required is here quite good."""
    Datum: datetime.datetime
    Uhrzeit: str
    Zeitzone: str
    Name: Optional[str]
    Typ: str
    Status: str = Field(description='What are possible values?')
    Waehrung: str = Field(alias='Währung')
    Brutto: float
    Gebuehr: float = Field(alias='Gebühr')
    Netto: float
    Transaktionscode: str
    Artikelbezeichnung: Optional[str]
    Artikelnummer: Optional[str]
    ZweiterTransaktionscode: Optional[str] = Field(alias='Zugehöriger Transaktionscode')
    AbsenderEmail: Optional[str] = Field(alias='Absender E-Mail-Adresse')
    EmpfaengerEmail: Optional[str] = Field(alias='Empfänger E-Mail-Adresse')
    Versandgebuehr: Optional[float] = Field(alias='Versand- und Bearbeitungsgebühr')
    Versicherungsbetrag: Optional[float]
    Umsatzsteuer: Optional[float]
    Zollnummer: Optional[str]
    Anzahl: Optional[float]
    Guthaben: float
    Land: Optional[str]
    Laendervorwahl: Optional[str] = Field(alias='Ländervorwahl')
    AuswirkungAufGuthaben: str = Field(alias='Auswirkung auf Guthaben')

    # Meta Columns
    Timespan: str = Field(description='The key parameter for every run of the complete datapipeline, represents a VAT period.')
    TimeInsert: datetime.datetime = Field(description='Timestamp when we insert the data, time is UTC and not local Time.')


class PaypalMeta(Meta):
    """Wrapper for all the python functions for JtlData Model."""
    timespan: str = ''

    # When you call it the 2nd time after you wrote a file
    # you can validate it by passing a data paramter: df.to_dict('records')
    data: Optional[List[Paypal]] = None
    table_id: str = 'waipa-1.test.Paypal'
    model = Paypal  # We don't instantiate the Model with data

    #  somehow duplicate but it's explicit and easy to read
    dtypes: dict = {}
    parse_dates = ['Datum']

    @property
    def data_file(self):
        """Set first part in env."""
        return pathlib.Path('/home/flo/Nextcloud/data/finance/', self.timespan, 'paypal.CSV')

    @property
    def tmp_file(self):
        base_folder = '/home/flo/tests/'
        pathlib.Path(base_folder, self.timespan).mkdir(parents=True, exist_ok=True)
        return pathlib.Path('/home/flo/tests/', self.timespan, 'paypal.CSV')

    def read_csv(self):
        return pd.read_csv(
            self.data_file,
            dtype=self.dtypes,
            encoding='utf-8-sig',
            sep=',',
            decimal=',',
            thousands='.',
            dayfirst=True,
            parse_dates=self.parse_dates)
