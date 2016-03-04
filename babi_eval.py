# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from decimal import Decimal
import datetime
import math
from dateutil.relativedelta import relativedelta
from trytond.pool import Pool
from trytond.transaction import Transaction


def year(text):
    if not text:
        return None
    text = str(text)
    return text[0:4]


def year_month(text):
    if not text:
        return None
    text = str(text)
    return text[0:4] + '-' + text[5:7]


def year_month_day(text):
    if not text:
        return None
    text = str(text)
    return text[0:10]


def month(text):
    if not text:
        return None
    text = str(text)
    return text[5:7]


def day(text):
    if not text:
        return None
    text = str(text)
    return text[8:10]


def week(text):
    if not text:
        return None
    return datetime.datetime.strptime(year_month_day(text),
        '%Y-%m-%d').strftime('%W')


def date(text):
    if not text:
        return None
    return datetime.datetime.strptime(year_month_day(text), '%Y-%m-%d').date()


def babi_eval(expression, obj, convert_none='empty'):
    objects = {
        'o': obj,
        'Pool': Pool,
        'Transaction': Transaction,
        'y': year,
        'm': month,
        'd': day,
        'w': week,
        'ym': year_month,
        'ymd': year_month_day,
        'date': date,
        'int': int,
        'float': float,
        'sum': sum,
        'min': min,
        'max': max,
        'now': datetime.datetime.now,
        'today': datetime.date.today,
        'relativedelta': relativedelta,
        'math': math,
        'Decimal': Decimal,
        'str': str,
        }
    value = eval(expression, objects)
    if (value is False or value is None):
        if convert_none == 'empty':
            # TODO: Make translatable
            value = '(empty)'
        elif convert_none == 'zero':
            value = '0'
        else:
            value = convert_none
    return value
