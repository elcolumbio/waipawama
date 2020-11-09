"""Easy FastApi example to expose our pydantic models."""
from fastapi import FastAPI
from enum import Enum
from pydantic import BaseModel

app = FastAPI()


class PaypalStatus(str, Enum):
    done = 'Abgeschlossen'
    pending = 'Ausstehend'
    declined = 'Abgelehnt'


class PaypalTransactions(BaseModel):
    status: PaypalStatus


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post('/paypal/transactions/')
async def post_paypal_transactions(
    status: PaypalTransactions
):
    return {'hey there'}


@app.get('/paypal/transactions?{status}')
async def get_paypal_transactions(status: PaypalStatus):
    if status == 'Abgeschlossen':
        return {'all which are done'}
