from pydantic import Field
import datetime
import numpy as np
import pandas as pd
import pathlib
from typing import List, Optional

from ..ingestion_template import (
    BaseModel,
    Meta)


class Taxdoo(BaseModel):
    """Input model after parsing used for testing, aliasing and dtypes."""

    Channel: Optional[str] = Field(alias='channel', description='Verkaufskanal')
    TransactionNumber: Optional[str] = Field(alias='transaction_number', description='Bestellnummer oder Verbringungsnummer')
    InvoiceNumber: Optional[str] = Field(alias='invoice_number', description='Rechnungsnummer')
    InvoiceDate: Optional[datetime.datetime] = Field(alias='invoice_date', description='Rechnungsdatum')
    TransactionStartDate: Optional[datetime.datetime] = Field(alias='transaction_start_date', description='Versendungsdatum')
    TransactionArrivalDate: Optional[datetime.datetime] = Field(alias='transaction_arrival_date', description='Ankunftsdatum')
    TransactionPaymentDate: Optional[datetime.datetime] = Field(alias='transaction_payment_date', description='Bezahldatum')
    TransactionType: Optional[str] = Field(alias='transaction_type', description='Transaktionstyp')
    SentFromCountry: Optional[str] = Field(alias='sent_from_country', description='Herkunftsland')
    SentToCountry: Optional[str] = Field(alias='sent_to_country', description='Zielland')
    BuyerVatNumber: Optional[str] = Field(alias='buyer_vat_number', description='Umsatzsteuer-ID des Käufers')
    BuyerVatNumberStatus: Optional[str] = Field(alias='buyer_vat_number_status', description='Validität Umsatzsteuer-ID des Käufers')
    BuyerName: Optional[str] = Field(alias='buyer_name', description='Name des Käufers')
    Quantity: Optional[float] = Field(alias='quantity', description='Menge')
    ProductIdentifier: Optional[str] = Field(alias='product_identifier', description='SKU des Produkts')
    Description: Optional[str] = Field(alias='description', description='Beschreibung des Produkts')
    CommodityCode: Optional[str] = Field(alias='commodity_code', description='Zolltarifnummer')
    Currency: Optional[str] = Field(alias='currency', description='Währung')
    TotalPriceOfItems: Optional[float] = Field(alias='total_price_of_items', description='Bruttopreis aller Artikel')
    TotalShipping: Optional[float] = Field(alias='total_shipping', description='Brutto Versandkosten aller Artikel')
    TotalGiftWrap: Optional[float] = Field(alias='total_gift_wrap', description='Brutto Geschenkverpackungskosten aller Artikel')
    TotalGoodwill: Optional[float] = Field(alias='total_goodwill', description='Goodwill der erstatten Bestellung')
    TotalTransactionValue: Optional[float] = Field(alias='total_transaction_value', description='Summe von TotalPriceOfItems + TotalShipping + TotalGiftWrap + TotalGoodwill in Währung von Currency. Verbringung: Quantity * PurchasePrice in Currency')
    PurchasePrice: Optional[float] = Field(alias='purchase_price', description='Verbringung: Netto Einkaufspreis einer Einheit')
    Weight: Optional[float] = Field(alias='weight', description='Verbringung: Summe Gewicht aller Einheiten, in kg')
    TaxdooVatRate: Optional[float] = Field(alias='taxdoo_vat_rate', description='Angewendeter Steuersatz.')
    TaxdooVatAmount: Optional[float] = Field(alias='taxdoo_vat_amount', description='Steuerzahlung in TaxdooCurrency: TotalTransactionValue/(100+TaxdooVatRate)*TaxdooVatRate')
    TaxdoCountryOfTaxation: Optional[str] = Field(alias='taxdoo_country_of_taxation', description='Land in dem die Transaktion steuerpflichtig ist.')
    TaxdooCurrency: Optional[str] = Field(alias='taxdoo_currency', description='Währung in der die Steuerzahlung abzuführen ist.')
    TaxdooNetAmount: Optional[float] = Field(alias='taxdoo_net_amount', description='Nettoerlös in Währung TaxdooCurrency: TotalTransactionValue - TaxdooVatAmount.')
    TransferOutboundFx: Optional[float] = Field(alias='transfer_outbound_fx', description='Verbringung: Angewendeter Wechselkurs für TaxdooCurrency to Currency based on TransactionStartDate')
    TranferOutboundCurrency: Optional[str] = Field(alias='transfer_outbound_currency', description='Währung des Landes, in dem die Verbringung beginnt: SentFromCountry.')
    TransferOutboundValue: Optional[float] = Field(alias='transfer_outbound_value', description='Wert der Verbringung (ohne Steuer) in der Währung von SentFromCountry.')
    TransferInboundFx: Optional[float] = Field(alias='transfer_inbound_fx', description='Angewendeter Wechselkurs für TransferInboundFx to Currcency basierend auf TransactionEndDate.')
    TransferInboundCurrency: Optional[str] = Field(alias='transfer_inbound_currency', description='Währung des Landes im dem die Verbrinung endet.')
    TransferInboundValue: Optional[float] = Field(alias='transfer_inbound_value', description='Wert der Verbringung ohne Steuer in der Währung des Ziellandes SentToCountry.')
    SellerName: Optional[str] = Field(alias='seller_name', description='Name des Verkäufers')
    SellerVatNumber: Optional[str] = Field(alias='seller_vat_number', description='Umsatzsteuer-ID des Verkäufers')
    CreatedFromTransfer: Optional[str] = Field(alias='created_from_transfer', description='Transaktion wurde aus dem Transfer kreiert, weil die Umsatzsteuer-ID des Käufers invalide ist (Yes/No).')

    # Meta Columns
    Timespan: str = Field(description='The key parameter for every run of the complete datapipeline, represents a VAT period.')
    TimeInsert: datetime.datetime = Field(description='Timestamp when we insert the data, time is UTC and not local Time.')


class TaxdooMeta(Meta):
    """Wrapper for all the python functions for Taxdoo Model."""
    timespan: str = ''

    data: Optional[List[Taxdoo]] = None  # for  second step validating
    table_id: str = 'waipa-1.test.Taxdoo'
    model = Taxdoo  # We don't instantiate the Model with data

    #  somehow duplicate but it's explicit and easy to read
    dtypes: dict = {
        'quantity': np.float64,
        'total_price_of_items': np.float64,
        'total_shipping': np.float64,
        'total_gift_wrap': np.float64,
        'total_goodwill': np.float64,
        'total_transaction_value': np.float64,
        'purchase_price': np.float64,
        'weight': np.float64,
        'taxdoo_vat_rate': np.float64,
        'taxdoo_vat_amount': np.float64,
        'taxdoo_net_amount': np.float64,
        'transfer_outbound_fx': np.float64,
        'transfer_outbound_value': np.float64,
        'transfer_inbound_fx': np.float64,
        'transfer_inbound_value': np.float64,
        'channel': str,
        'transaction_number': str,
        'invoice_date': str,
        'transaction_start_date': str,
        'transaction_arrival_date': str,
        'transaction_payment_date': str,
        'transaction_type': str,
        'sent_from_country': str,
        'sent_to_country': str,
        'buyer_vat_number': str,
        'buyer_vat_number_status': str,
        'buyer_name': str,
        'product_identifier': str,
        'description': str,
        'commodity_code': str,
        'currency': str,
        'taxdoo_country_of_taxation': str,
        'taxdoo_currency': str,
        'transfer_outbound_currency': str,
        'transfer_inbound_currency': str,
        'seller_name': str,
        'seller_vat_number': str,
        'created_from_transfer': str,
        'invoice_number': str}
    parse_dates = [
        'invoice_date',
        'transaction_start_date',
        'transaction_arrival_date',
        'transaction_payment_date']

    @property
    def data_file(self):
        """Set first part in env."""
        return pathlib.Path('/home/flo/Nextcloud/data/finance/', self.timespan, 'taxdoo.csv')

    @property
    def tmp_file(self):
        base_folder = '/home/flo/tests/'
        pathlib.Path(base_folder, self.timespan).mkdir(parents=True, exist_ok=True)
        return pathlib.Path('/home/flo/tests/', self.timespan, 'taxdoo.csv')

    def read_csv(self):
        return pd.read_csv(
            self.data_file,
            dtype=self.dtypes,
            encoding='latin-1',
            sep=';',
            decimal='.',
            thousands=None,
            dayfirst=False,
            parse_dates=self.parse_dates)
