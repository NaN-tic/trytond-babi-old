#!/usr/bin/env python
# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import datetime
import random
import unittest
from decimal import Decimal

import trytond.tests.test_tryton
from trytond.tests.test_tryton import ModuleTestCase
from trytond.tests.test_tryton import POOL, DB_NAME, USER, CONTEXT
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

    def setUp(self):
        super(BaBITestCase, self).setUp()
        self.test_model = POOL.get('babi.test')
        self.report = POOL.get('babi.report')
        self.expression = POOL.get('babi.expression')
        self.dimension = POOL.get('babi.dimension')
        self.column = POOL.get('babi.dimension.column')
        self.measure = POOL.get('babi.measure')
        self.filter = POOL.get('babi.filter')
        self.model = POOL.get('ir.model')
        self.menu = POOL.get('ir.ui.menu')

    def test0009_create_data(self):
        with Transaction().start(DB_NAME, USER, context=CONTEXT) as trans:
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

            self.test_model.create(to_create)
            model, = self.model.search([('model', '=', 'babi.test')])
            self.model.write([model], {
                    'babi_enabled': True
                    })

            self.expression.create([{
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

            self.filter.create([{
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
            trans.cursor.commit()

    def test0010_basic_reports(self):
        'Test basic reports'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            model, = self.model.search([('model', '=', 'babi.test')])
            menu, = self.menu.search([('name', '=', 'Business Intelligence')])
            report, = self.report.create([{
                        'name': 'Simple Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'timeout': 30,
                        }])
            self.assertEqual(len(report.order), 0)
            self.assertRaises(UserError, self.report.calculate, [report])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            self.assertRaises(UserError, self.report.calculate, [report])

            amount, = self.expression.search([('name', '=', 'Amount')])
            amount, = self.measure.create([{
                        'report': report.id,
                        'expression': amount.id,
                        'name': 'Amount',
                        'aggregate': 'sum',
                        }])
            amount_this_month, = self.expression.search([
                    ('name', '=', 'Amount this month'),
                    ])
            amount_this_month, = self.measure.create([{
                        'report': report.id,
                        'expression': amount_this_month.id,
                        'name': 'Amount this month',
                        'aggregate': 'sum',
                        }])
            report, = self.report.search([])
            (category_order, amount_order,
                amount_this_month_order) = report.order
            self.assertIsNotNone(category_order.dimension)
            self.assertIsNone(category_order.measure)
            self.assertIsNone(amount_order.dimension)
            self.assertIsNotNone(amount_order.measure)
            self.assertIsNone(amount_this_month_order.dimension)
            self.assertIsNotNone(amount_this_month_order.measure)

            self.report.calculate([report])
            report, = self.report.search([])

            execution, = report.executions

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

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

            month, = self.expression.search([('name', '=', 'Month')])
            month, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Month',
                        'expression': month.id,
                        }])

            self.report.calculate([report])
            report, = self.report.search([])

            self.assertEqual(len(report.executions), 2)

            old_execution, execution = sorted(report.executions,
                key=lambda x: x.internal_name)
            self.assertEqual(old_execution.babi_model.model,
                ReportModel.__name__)
            old_fields = ReportModel.fields_view_get()['fields']
            self.assertFalse(month.internal_name in old_fields)

            ReportModel = POOL.get(execution.babi_model.model)
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

    def test0020_count(self):
        'Test count reports'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            model, = self.model.search([('model', '=', 'babi.test')])
            menu, = self.menu.search([('name', '=', 'Business Intelligence')])
            report, = self.report.create([{
                        'name': 'Simple Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'timeout': 30,
                        }])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            id_expr, = self.expression.search([('name', '=', 'Id')])
            id_measure, = self.measure.create([{
                        'report': report.id,
                        'expression': id_expr.id,
                        'name': 'Id',
                        'aggregate': 'count',
                        }])

            self.report.calculate([report])
            report = self.report(report.id)

            execution, = report.executions

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

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

    def test0030_average(self):
        'Test average reports'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            model, = self.model.search([('model', '=', 'babi.test')])
            menu, = self.menu.search([('name', '=', 'Business Intelligence')])
            report, = self.report.create([{
                        'name': 'Simple Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'timeout': 30,
                        }])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            amount, = self.expression.search([('name', '=', 'Amount')])
            amount, = self.measure.create([{
                        'report': report.id,
                        'expression': amount.id,
                        'name': 'Amount',
                        'aggregate': 'avg',
                        }])

            self.report.calculate([report])
            report = self.report(report.id)

            execution, = report.executions

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

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

    def test0040_filtered_report(self):
        'Test filtered reports'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            model, = self.model.search([('model', '=', 'babi.test')])
            menu, = self.menu.search([('name', '=', 'Business Intelligence')])
            filter, = self.filter.search([('name', '=', 'Odd')])
            report, = self.report.create([{
                        'name': 'Simple Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'filter': filter.id,
                        'timeout': 30,
                        }])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            amount, = self.expression.search([('name', '=', 'Amount')])
            amount, = self.measure.create([{
                        'report': report.id,
                        'expression': amount.id,
                        'name': 'Amount',
                        'aggregate': 'sum',
                        }])

            self.report.calculate([report])
            report = self.report(report.id)

            execution, = report.executions

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

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
            date_filter, = self.filter.search([('name', '=', 'Date')])
            report, = self.report.create([{
                        'name': 'Date filter Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'filter': date_filter.id,
                        'timeout': 30,
                        }])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            amount, = self.expression.search([('name', '=', 'Amount')])
            amount, = self.measure.create([{
                        'report': report.id,
                        'expression': amount.id,
                        'name': 'Amount',
                        'aggregate': 'sum',
                        }])

            self.report.calculate([report])
            report = self.report(report.id)

            execution, = report.executions

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

            year = datetime.date.today().year
            total_amount = 0
            for record in DataModel.search([]):
                if record.date >= datetime.date(year, 6, 1):
                    total_amount += record.amount

            root, = ReportModel.search([('parent', '=', None)])
            self.assertEqual(getattr(root, amount.internal_name), total_amount)

    def test0050_dimensions_on_columns(self):
        'Test reports with dimensions on columns'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            model, = self.model.search([('model', '=', 'babi.test')])
            menu, = self.menu.search([('name', '=', 'Business Intelligence')])
            report, = self.report.create([{
                        'name': 'Column Report',
                        'model': model.id,
                        'parent_menu': menu.id,
                        'timeout': 30,
                        }])

            category, = self.expression.search([('name', '=', 'Category')])
            category, = self.dimension.create([{
                        'report': report.id,
                        'name': 'Category',
                        'expression': category.id,
                        }])

            month, = self.expression.search([('name', '=', 'Month')])
            month, = self.column.create([{
                        'report': report.id,
                        'name': 'Month',
                        'expression': month.id,
                        }])

            amount, = self.expression.search([('name', '=', 'Amount')])
            amount, = self.measure.create([{
                        'report': report.id,
                        'expression': amount.id,
                        'name': 'Amount',
                        'aggregate': 'sum',
                        }])

            self.report.calculate([report])
            report = self.report(report.id)

            execution, = report.executions
            self.assertEqual(len(execution.internal_measures), 13)

            ReportModel = POOL.get(execution.babi_model.model)
            DataModel = POOL.get(model.model)

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

    def test0060_eval(self):
        'Test babi_eval'
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
        ]
        with Transaction().start(DB_NAME, USER, context=CONTEXT) as trans:
            models = self.model.search([('model', '=', 'babi.test')])
            tests.append(
                ('Pool().get(\'ir.model\').search(['
                    '(\'model\', \'=\', \'babi.test\')])', None, models),
                )
            for expression, obj, result in tests:
                self.assertEqual(babi_eval(expression, obj), result)
            with trans.set_context(date=date):
                self.assertEqual(babi_eval(
                        'Transaction().context.get(\'date\')', None), date)

        self.assertEqual(babi_eval('o', None, convert_none='zero'), '0')
        self.assertEqual(babi_eval('o', None, convert_none=''), '')
        self.assertEqual(babi_eval('o', None, convert_none=None), None)

    def test0070_basic_operations(self):
        'Test basic operations'
        with Transaction().start(DB_NAME, USER, context=CONTEXT):
            report, = self.report.search([], limit=1)
            # Delete one dimension as SQlite test fails with two dimensions.
            dimension, _ = report.dimensions
            self.dimension.delete([dimension])
            measure, _ = report.measures
            self.measure.delete([measure])
            new_report, = self.report.copy([report])
            self.assertEqual(new_report.name, '%s (2)' % (report.name))
            menus = self.menu.search([
                    ('name', '=', new_report.name),
                    ('parent', '=', new_report.parent_menu),
                    ])
            self.assertEqual(len(menus), 0)
            self.report.create_menus([new_report])
            menu, = self.menu.search([
                    ('name', '=', new_report.name),
                    ('parent', '=', new_report.parent_menu),
                    ])
            self.assertEqual(len(menu.childs), 3)
            report_name = new_report.name
            self.report.delete([new_report])
            menus = self.menu.search([
                    ('name', '=', report_name),
                    ('parent', '=', report.parent_menu),
                    ])
            self.assertEqual(len(menus), 0)


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(BaBITestCase))
    return suite
