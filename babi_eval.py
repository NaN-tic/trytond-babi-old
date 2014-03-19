# -*- encoding: utf-8 -*-
##############################################################################
#
# Copyright (c) 2012 - NaN  (http://www.nan-tic.com) All Rights Reserved.
#
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsability of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial#
# garantees and support are strongly adviced to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
##############################################################################

import datetime
from trytond.tools import safe_eval
from dateutil.relativedelta import relativedelta


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
        '%Y-%M-%d').strftime('%W')


def date(text):
    if not text:
        return None
    return datetime.datetime.strptime(year_month_day(text), '%Y-%m-%d').date()


def babi_eval(expression, obj, convert_none='empty'):
    objects = {
        'o': obj,
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
        }
    value = safe_eval(expression, objects)
    if (value is False or value is None):
        if convert_none == 'empty':
            # TODO: Make translatable
            value = '(empty)'
        elif convert_none == 'zero':
            value = '0'
        else:
            value = convert_none
    return value
