#This file is part of Tryton.  The COPYRIGHT file at the top level of this
#repository contains the full copyright notices and license terms.

from trytond.pool import Pool
from .configuration import *
from .babi import *
from .test_model import *


def register():
    Pool.register(
        Configuration,
        Filter,
        FilterParameter,
        Expression,
        Report,
        ReportExecution,
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
        OpenExecutionSelect,
        UpdateDataWizardStart,
        UpdateDataWizardUpdated,
        TestBabiModel,
        module='babi', type_='model')
    Pool.register(
        OpenChart,
        OpenExecution,
        UpdateDataWizard,
        module='babi', type_='wizard')
