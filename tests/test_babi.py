#!/usr/bin/env python
# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import datetime
import random
import unittest
from decimal import Decimal

from trytond.pool import Pool
import trytond.tests.test_tryton
from trytond.tests.test_tryton import ModuleTestCase, with_transaction
from trytond.transaction import Transaction
from trytond.exceptions import UserError
from trytond.modules.babi.babi_eval import babi_eval
from trytond.pyson import PYSONEncoder
from dateutil.relativedelta import relativedelta


class BaBITestCase(ModuleTestCase):
    '''
    Test BaBI module.
    '''
    module = 'babi'

    def create_data(self):
        pool = Pool()
        TestModel = pool.get('babi.test')
        Model = pool.get('ir.model')
        Expression = pool.get('babi.expression')
        Filter = pool.get('babi.filter')
        to_create = []
        year = datetime.date.today().year
        for month in range(1, 13):
            # Create at least one record for each category in each month
            num_records = int(round(random.random() * 10)) + 2
            for x in range(0, num_records):
                category = 'odd' if x % 2 == 0 else 'even'
                day = int(random.random() * 28) + 1
                amount = Decimal(str(round(random.random() * 10000, 2)))
                to_create.append({
                        'date': datetime.date(year, month, day),
                        'category': category,
                        'amount': amount,
                        })

        TestModel.create(to_create)
        model, = Model.search([('model', '=', 'babi.test')])
        Model.write([model], {
                'babi_enabled': True
                })

        Expression.create([{
                    'name': 'Id',
                    'model': model.id,
                    'ttype': 'int',
                    'expression': 'o.id',
                    }, {
                    'name': 'Year',
                    'model': model.id,
                    'ttype': 'char',
                    'expression': 'y(o.date)',
                    }, {
                    'name': 'Month',
                    'model': model.id,
                    'ttype': 'char',
                    'expression': 'm(o.date)',
                    }, {
                    'name': 'Category',
                    'model': model.id,
                    'ttype': 'char',
                    'expression': 'o.category',
                    }, {
                    'name': 'Amount',
                    'model': model.id,
                    'ttype': 'numeric',
                    'expression': 'o.amount',
                    }, {
                    'name': 'Amount this month',
                    'model': model.id,
                    'ttype': 'numeric',
                    'expression': ('o.amount if o.date >= '
                        'today() - relativedelta(days=today().day - 1) '
                        'else 0.0'),
                    }])

        Filter.create([{
                    'name': 'Odd',
                    'model': model.id,
                    'domain': "[('category', '=', 'odd')]",
                     }, {
                    'name': 'Even',
                    'model': model.id,
                    'domain': "[('category', '=', 'even')]",
                     }, {
                    'name': 'Date',
                    'model': model.id,
                    'domain': PYSONEncoder().encode([
                            ('date', '>=', datetime.date(year, 6, 1)),
                            ]),
                     }])
        Transaction().commit()

    @with_transaction()
    def test0010_basic_reports(self):
        'Test basic reports'
        pool = Pool()
        Model = pool.get('ir.model')
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Expression = pool.get('babi.expression')
        Dimension = pool.get('babi.dimension')
        Expression = pool.get('babi.expression')
        Measure = pool.get('babi.measure')
        self.create_data()
        model, = Model.search([('model', '=', 'babi.test')])
        menu, = Menu.search([('name', '=', 'Business Intelligence')])
        report, = Report.create([{
                    'name': 'Simple Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'timeout': 30,
                    }])
        self.assertEqual(len(report.order), 0)
        self.assertRaises(UserError, Report.calculate, [report])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        self.assertRaises(UserError, Report.calculate, [report])

        amount, = Expression.search([('name', '=', 'Amount')])
        amount, = Measure.create([{
                    'report': report.id,
                    'expression': amount.id,
                    'name': 'Amount',
                    'aggregate': 'sum',
                    }])
        amount_this_month, = Expression.search([
                ('name', '=', 'Amount this month'),
                ])
        amount_this_month, = Measure.create([{
                    'report': report.id,
                    'expression': amount_this_month.id,
                    'name': 'Amount this month',
                    'aggregate': 'sum',
                    }])
        report, = Report.search([])
        (category_order, amount_order,
            amount_this_month_order) = report.order
        self.assertIsNotNone(category_order.dimension)
        self.assertIsNone(category_order.measure)
        self.assertIsNone(amount_order.dimension)
        self.assertIsNotNone(amount_order.measure)
        self.assertIsNone(amount_this_month_order.dimension)
        self.assertIsNotNone(amount_this_month_order.measure)

        Report.calculate([report])
        report, = Report.search([])

        execution, = report.executions

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        total_amount = 0
        odd_amount = 0
        even_amount = 0
        total_amount_this_month = 0
        odd_amount_this_month = 0
        even_amount_this_month = 0
        today = datetime.date.today()
        for record in DataModel.search([]):
            total_amount += record.amount
            total_amount_this_month += (record.amount
                if record.date >= today - relativedelta(days=today.day - 1)
                else Decimal(0.0))
            if record.category == 'odd':
                odd_amount += record.amount
                odd_amount_this_month += (record.amount
                    if record.date >= today - relativedelta(days=today.day
                        - 1)
                    else Decimal(0.0))
            elif record.category == 'even':
                even_amount += record.amount
                even_amount_this_month += (record.amount
                    if record.date >= today - relativedelta(days=today.day
                        - 1)
                    else Decimal(0.0))

        self.assertEqual(len(ReportModel.search([])), 3)
        root, = ReportModel.search([('parent', '=', None)])

        self.assertEqual(getattr(root, category.internal_name), '(all)')
        self.assertEqual(getattr(root, amount.internal_name),
            total_amount)
        self.assertEqual(getattr(root, amount_this_month.internal_name),
            total_amount_this_month)
        odd, = ReportModel.search([(category.internal_name, '=', 'odd')])
        self.assertEqual(getattr(odd, amount.internal_name),
            odd_amount)
        self.assertEqual(getattr(odd, amount_this_month.internal_name),
            odd_amount_this_month)
        even, = ReportModel.search([(category.internal_name, '=', 'even')])
        self.assertEqual(getattr(even, amount.internal_name),
            even_amount)
        self.assertEqual(getattr(even, amount_this_month.internal_name),
            even_amount_this_month)

        month, = Expression.search([('name', '=', 'Month')])
        month, = Dimension.create([{
                    'report': report.id,
                    'name': 'Month',
                    'expression': month.id,
                    }])

        Report.calculate([report])
        report, = Report.search([])

        self.assertEqual(len(report.executions), 2)

        old_execution, execution = sorted(report.executions,
            key=lambda x: x.internal_name)
        self.assertEqual(old_execution.babi_model.model,
            ReportModel.__name__)
        old_fields = ReportModel.fields_view_get()['fields']
        self.assertFalse(month.internal_name in old_fields)

        ReportModel = pool.get(execution.babi_model.model)
        new_tree_view = ReportModel.fields_view_get(view_type='tree')
        new_fields = new_tree_view['fields']
        self.assertTrue(month.internal_name in new_fields)

        # (2x12 months) + 2 categories + 1 root = 15
        self.assertEqual(len(ReportModel.search([])), 27)
        root, = ReportModel.search([('parent', '=', None)])

        self.assertEqual(getattr(root, category.internal_name), '(all)')
        self.assertEqual(getattr(root, amount.internal_name),
            total_amount)
        odd, = ReportModel.search([
                (category.internal_name, '=', 'odd'),
                ('parent', '=', root.id),
                ])
        self.assertEqual(getattr(odd, amount.internal_name),
            odd_amount)
        even, = ReportModel.search([
                (category.internal_name, '=', 'even'),
                ('parent', '=', root.id),
                ])
        self.assertEqual(getattr(even, amount.internal_name),
            even_amount)

        odd_amount = 0
        even_amount = 0
        year = datetime.date.today().year
        for record in DataModel.search([
                    ('date', '>=', datetime.date(year, 1, 1)),
                    ('date', '<', datetime.date(year, 2, 1)),
                    ]):
            if record.category == 'odd':
                odd_amount += record.amount
            elif record.category == 'even':
                even_amount += record.amount

        january_odd, = ReportModel.search([
                (month.internal_name, '=', '01'),
                (category.internal_name, '=', 'odd'),
                ])
        self.assertEqual(getattr(january_odd, amount.internal_name),
            odd_amount)
        january_even, = ReportModel.search([
                (month.internal_name, '=', '01'),
                (category.internal_name, '=', 'even'),
                ])
        self.assertEqual(getattr(january_even, amount.internal_name),
            even_amount)

    @with_transaction()
    def test_count(self):
        'Test count reports'
        pool = Pool()
        Model = pool.get('ir.model')
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Expression = pool.get('babi.expression')
        Dimension = pool.get('babi.dimension')
        Measure = pool.get('babi.measure')
        model, = Model.search([('model', '=', 'babi.test')])
        menu, = Menu.search([('name', '=', 'Business Intelligence')])
        report, = Report.create([{
                    'name': 'Simple Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'timeout': 30,
                    }])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        id_expr, = Expression.search([('name', '=', 'Id')])
        id_measure, = Measure.create([{
                    'report': report.id,
                    'expression': id_expr.id,
                    'name': 'Id',
                    'aggregate': 'count',
                    }])

        Report.calculate([report])
        report = Report(report.id)

        execution, = report.executions

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        total_count = 0
        odd_count = 0
        even_count = 0
        for record in DataModel.search([]):
            total_count += 1
            if record.category == 'odd':
                odd_count += 1
            elif record.category == 'even':
                even_count += 1

        self.assertEqual(len(ReportModel.search([])), 3)
        root, = ReportModel.search([('parent', '=', None)])

        self.assertEqual(getattr(root, category.internal_name), '(all)')
        self.assertEqual(getattr(root, id_measure.internal_name),
            total_count)
        odd, = ReportModel.search([(category.internal_name, '=', 'odd')])
        self.assertEqual(getattr(odd, id_measure.internal_name),
            odd_count)
        even, = ReportModel.search([(category.internal_name, '=', 'even')])
        self.assertEqual(getattr(even, id_measure.internal_name),
            even_count)

    @with_transaction()
    def test_average(self):
        'Test average reports'
        pool = Pool()
        Model = pool.get('ir.model')
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Expression = pool.get('babi.expression')
        Dimension = pool.get('babi.dimension')
        Measure = pool.get('babi.measure')
        model, = Model.search([('model', '=', 'babi.test')])
        menu, = Menu.search([('name', '=', 'Business Intelligence')])
        report, = Report.create([{
                    'name': 'Simple Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'timeout': 30,
                    }])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        amount, = Expression.search([('name', '=', 'Amount')])
        amount, = Measure.create([{
                    'report': report.id,
                    'expression': amount.id,
                    'name': 'Amount',
                    'aggregate': 'avg',
                    }])

        Report.calculate([report])
        report = Report(report.id)

        execution, = report.executions

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        total = []
        odd = []
        even = []
        for record in DataModel.search([]):
            total.append(record.amount)
            if record.category == 'odd':
                odd.append(record.amount)
            elif record.category == 'even':
                even.append(record.amount)
        total_average = sum(total) / Decimal(str(len(total)))
        odd_average = sum(odd) / Decimal(str(len(odd)))
        even_average = sum(even) / Decimal(str(len(even)))

        self.assertEqual(len(ReportModel.search([])), 3)
        root, = ReportModel.search([('parent', '=', None)])

        decimals = Decimal('.0001')
        self.assertEqual(getattr(root, category.internal_name), '(all)')
        self.assertEqual(getattr(root, amount.internal_name).quantize(
                decimals), total_average.quantize(decimals))
        odd, = ReportModel.search([(category.internal_name, '=', 'odd')])
        self.assertEqual(getattr(odd, amount.internal_name).quantize(
                decimals), odd_average.quantize(decimals))
        even, = ReportModel.search([(category.internal_name, '=', 'even')])
        self.assertEqual(getattr(even, amount.internal_name).quantize(
                decimals), even_average.quantize(decimals))

    @with_transaction()
    def test_filtered_report(self):
        'Test filtered reports'
        pool = Pool()
        Model = pool.get('ir.model')
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Filter = pool.get('babi.filter')
        Expression = pool.get('babi.expression')
        Dimension = pool.get('babi.dimension')
        Measure = pool.get('babi.measure')
        model, = Model.search([('model', '=', 'babi.test')])
        menu, = Menu.search([('name', '=', 'Business Intelligence')])
        filter, = Filter.search([('name', '=', 'Odd')])
        report, = Report.create([{
                    'name': 'Simple Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'filter': filter.id,
                    'timeout': 30,
                    }])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        amount, = Expression.search([('name', '=', 'Amount')])
        amount, = Measure.create([{
                    'report': report.id,
                    'expression': amount.id,
                    'name': 'Amount',
                    'aggregate': 'sum',
                    }])

        Report.calculate([report])
        report = Report(report.id)

        execution, = report.executions

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        total_amount = 0
        for record in DataModel.search([]):
            if record.category == 'odd':
                total_amount += record.amount

        self.assertEqual(len(ReportModel.search([])), 2)
        root, = ReportModel.search([('parent', '=', None)])

        self.assertEqual(getattr(root, category.internal_name), '(all)')
        self.assertEqual(getattr(root, amount.internal_name),
            total_amount)
        odd, = ReportModel.search([(category.internal_name, '=', 'odd')])
        self.assertEqual(getattr(odd, amount.internal_name),
            total_amount)
        evens = ReportModel.search([(category.internal_name, '=', 'even')])
        self.assertEqual(len(evens), 0)

        #Test with datetime fields as they are JSONEncoded on saved
        #searches
        date_filter, = Filter.search([('name', '=', 'Date')])
        report, = Report.create([{
                    'name': 'Date filter Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'filter': date_filter.id,
                    'timeout': 30,
                    }])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        amount, = Expression.search([('name', '=', 'Amount')])
        amount, = Measure.create([{
                    'report': report.id,
                    'expression': amount.id,
                    'name': 'Amount',
                    'aggregate': 'sum',
                    }])

        Report.calculate([report])
        report = Report(report.id)

        execution, = report.executions

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        year = datetime.date.today().year
        total_amount = 0
        for record in DataModel.search([]):
            if record.date >= datetime.date(year, 6, 1):
                total_amount += record.amount

        root, = ReportModel.search([('parent', '=', None)])
        self.assertEqual(getattr(root, amount.internal_name), total_amount)

    @with_transaction()
    def test_dimensions_on_columns(self):
        'Test reports with dimensions on columns'
        pool = Pool()
        Model = pool.get('ir.model')
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Expression = pool.get('babi.expression')
        Dimension = pool.get('babi.dimension')
        Column = pool.get('babi.dimension.column')
        Measure = pool.get('babi.measure')
        model, = Model.search([('model', '=', 'babi.test')])
        menu, = Menu.search([('name', '=', 'Business Intelligence')])
        report, = Report.create([{
                    'name': 'Column Report',
                    'model': model.id,
                    'parent_menu': menu.id,
                    'timeout': 30,
                    }])

        category, = Expression.search([('name', '=', 'Category')])
        category, = Dimension.create([{
                    'report': report.id,
                    'name': 'Category',
                    'expression': category.id,
                    }])

        month, = Expression.search([('name', '=', 'Month')])
        month, = Column.create([{
                    'report': report.id,
                    'name': 'Month',
                    'expression': month.id,
                    }])

        amount, = Expression.search([('name', '=', 'Amount')])
        amount, = Measure.create([{
                    'report': report.id,
                    'expression': amount.id,
                    'name': 'Amount',
                    'aggregate': 'sum',
                    }])

        Report.calculate([report])
        report = Report(report.id)

        execution, = report.executions
        self.assertEqual(len(execution.internal_measures), 13)

        ReportModel = pool.get(execution.babi_model.model)
        DataModel = pool.get(model.model)

        keys = [x.internal_name for x in execution.internal_measures]
        total_amount = dict.fromkeys(keys, Decimal('0.0'))
        odd_amount = dict.fromkeys(keys, Decimal('0.0'))
        even_amount = dict.fromkeys(keys, Decimal('0.0'))
        for record in DataModel.search([]):
            all_key = '%s__all__%s' % (month.internal_name,
                amount.internal_name)
            val = babi_eval(month.expression.expression, record)
            month_key = '%s_%s_%s' % (month.internal_name, val,
                amount.internal_name)
            total_amount[all_key] += record.amount
            total_amount[month_key] += record.amount
            if record.category == 'odd':
                odd_amount[all_key] += record.amount
                odd_amount[month_key] += record.amount
            elif record.category == 'even':
                even_amount[all_key] += record.amount
                even_amount[month_key] += record.amount

        self.assertEqual(len(ReportModel.search([])), 3)
        root, = ReportModel.search([('parent', '=', None)])

        self.assertEqual(getattr(root, category.internal_name), '(all)')
        for key, value in total_amount.iteritems():
            self.assertEqual(getattr(root, key), value)

        odd, = ReportModel.search([(category.internal_name, '=', 'odd')])
        for key, value in odd_amount.iteritems():
            self.assertEqual(getattr(odd, key), value)

        even, = ReportModel.search([(category.internal_name, '=', 'even')])
        for key, value in even_amount.iteritems():
            self.assertEqual(getattr(even, key), value)

    @with_transaction()
    def test_eval(self):
        'Test babi_eval'
        pool = Pool()
        Model = pool.get('ir.model')
        date = datetime.date(2014, 10, 10)
        other_date = datetime.date(2014, 1, 1)
        tests = [
            ('o', None, '(empty)'),
            ('y(o)', date, str(date.year)),
            ('m(o)', date, str(date.month)),
            ('m(o)', other_date, '0' + str(other_date.month)),
            ('d(o)', date, str(date.day)),
            ('d(o)', other_date, '0' + str(other_date.day)),
            ('w(o)', other_date, '00'),
            ('ym(o)', date, '2014-10'),
            ('ym(o)', other_date, '2014-01'),
            ('ymd(o)', date, '2014-10-10'),
            ('ymd(o)', other_date, '2014-01-01'),
            ('date(o)', date, date),
            ('date(o).year', date, 2014),
            ('int(o)', 1.0, 1),
            ('float(o)', 1, 1.0),
            ('max(o[0], o[1])', (date, other_date,), date),
            ('min(o[0], o[1])', (date, other_date,), other_date),
            ('today()', None, datetime.date.today()),
            ('o - relativedelta(days=1)', date, datetime.date(2014, 10, 9)),
            ('o - relativedelta(months=1)', date, datetime.date(2014, 9, 10)),
            ('str(o)', 3.14, '3.14'),
            ('Decimal(o)', 3.14, Decimal(3.14)),
            ('Decimal(0)', None, Decimal(0)),
        ]
        models = Model.search([('model', '=', 'babi.test')])
        tests.append(
            ('Pool().get(\'ir.model\').search(['
                '(\'model\', \'=\', \'babi.test\')])', None, models),
            )
        for expression, obj, result in tests:
            self.assertEqual(babi_eval(expression, obj), result)
        with Transaction().set_context(date=date):
            self.assertEqual(babi_eval(
                    'Transaction().context.get(\'date\')', None), date)

        self.assertEqual(babi_eval('o', None, convert_none='zero'), '0')
        self.assertEqual(babi_eval('o', None, convert_none=''), '')
        self.assertEqual(babi_eval('o', None, convert_none=None), None)

    @with_transaction()
    def test_basic_operations(self):
        'Test basic operations'
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Execution = pool.get('babi.report.execution')
        Dimension = pool.get('babi.dimension')
        Measure = pool.get('babi.measure')
        report, = Report.search([], limit=1)
        # Delete one dimension as SQlite test fails with two dimensions.
        dimension, _ = report.dimensions
        Dimension.delete([dimension])
        measure, _ = report.measures
        Measure.delete([measure])
        new_report, = Report.copy([report])
        self.assertEqual(new_report.name, '%s (2)' % (report.name))
        menus = Menu.search([
                ('name', '=', new_report.name),
                ('parent', '=', new_report.parent_menu),
                ])
        self.assertEqual(len(menus), 0)
        Report.create_menus([new_report])
        menu, = Menu.search([
                ('name', '=', new_report.name),
                ('parent', '=', new_report.parent_menu),
                ])
        self.assertEqual(len(menu.childs), 3)
        report_name = new_report.name
        Report.delete([new_report])
        menus = Menu.search([
                ('name', '=', report_name),
                ('parent', '=', report.parent_menu),
                ])
        self.assertEqual(len(menus), 0)
        executions = Execution.search([])
        self.assertGreater(len(executions), 0)
        date = datetime.date.today() + datetime.timedelta(days=1)
        Execution.clean(date=date)
        executions = Execution.search([])
        self.assertEqual(len(executions), 0)


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(BaBITestCase))
    return suite
