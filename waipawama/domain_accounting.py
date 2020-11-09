import datetime
from enum import Enum
import numpy as np
from pydantic import (
    BaseModel as PydanticBaseModel,
    Field,
    validator)
from typing import Optional


class BaseModel(PydanticBaseModel):
    """Unclear if this hjacking is helpful."""
    @validator('*')
    def change_nan_to_none(cls, v, values, field):
        if isinstance(v, float):
            if np.isnan(v):
                return None
        return v


class AccountingStatus(Enum):
    """Status wich are used by my specific accounting software."""
    normal = None
    reversed_transaction = 'S', 'Stornierte Buchungen'
    reverse_transaction = 's', 'Stornobuchung'
    corrected = 'U', 'Umbuchungen'
    voucher_reduction = 'AG', 'OP Abgleich Minderungen mit AG'
    voucher_creditor = 'OPk', 'OP Kreditor mit OPk'
    voucher_debitor = 'OPd', 'OP Debitor mit OPd'
    clearing_creditor = 'ABk', 'Ausbuchung mit Kreditor ABk'
    clearing_debitor = 'ABd', 'Ausbuchung mit Debitor ABd'
    opening_statement_new = 'EBa', 'Eröffnungsbuchung aktualisiert EBa'
    opening_statement_final = 'EBf', 'Eröffnungsbuchung final EBf'

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, description: str = None):
        self._description_ = description

    def __str__(self):
        return str(self.value)

    # this makes sure that the description is read-only
    @property
    def description(self):
        return self._description_


class Accounting(BaseModel):
    """Important features with types and alias function, see pydantic."""
    document_date: datetime.date = Field(alias='Belegdat.')
    vat_text: Optional[str] = Field(alias='USt Text')
    amount: float = Field(alias='BetragEUR')
    document_id: str = Field(alias='Belegnr.')
    booking_date: datetime.date = Field(alias='Jour. Dat.')
    debit_account: int = Field(alias='Sollkto')
    credit_account: int = Field(alias='Habenkto')
    status: Optional[AccountingStatus] = Field(alias='Status')
    posting_text: str = Field(alias='Buchungstext')
    contra_account: int = Field(alias='Gegenkto')
    vat_rate: Optional[int] = Field(alias='USt %')
    vat_account: Optional[int] = Field(alias='USt Kto')
