#This file is part of Tryton.  The COPYRIGHT file at the top level of this
#repository contains the full copyright notices and license terms.

from trytond.pool import Pool
from .babi import *


def register():
    Pool.register(
        Filter,
        Expression,
        Report,
        ReportGroup,
        Dimension,
        DimensionColumn,
        Measure,
        InternalMeasure,
        Order,
        ActWindow,
        Menu,
        Keyword,
        Model,
        OpenChartStart,
        module='babi', type_='model')
    Pool.register(
        OpenChart,
        module='babi', type_='wizard')
