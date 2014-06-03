# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from trytond.model import ModelSQL, ModelView, fields


class TestBabiModel(ModelSQL, ModelView):
    'Test BABI Model'
    __name__ = 'babi.test'

    date = fields.Date('Date')
    category = fields.Char('Category')
    amount = fields.Numeric('Amount')
