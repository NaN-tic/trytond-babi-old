# encoding: utf-8
# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import datetime as mdatetime
from datetime import datetime
from StringIO import StringIO
from collections import defaultdict
import logging
import os
import subprocess
import tempfile
import time
import unicodedata
try:
    import simplejson as json
except ImportError:
    import json

from trytond.wizard import Wizard, StateView, StateAction, StateTransition, \
    Button
from trytond.model import ModelSQL, ModelView, fields
from trytond.model.fields import depends
from trytond.pyson import Eval, Bool, PYSONEncoder, Id, In, Not, PYSONDecoder
from trytond.pool import Pool, PoolMeta
from trytond.transaction import Transaction
from trytond.tools import safe_eval
from trytond.config import config
from trytond import backend
from trytond.protocols.jsonrpc import JSONDecoder, JSONEncoder

from .babi_eval import babi_eval


__all__ = ['Filter', 'Expression', 'Report', 'ReportGroup', 'Dimension',
    'DimensionColumn', 'Measure', 'InternalMeasure', 'Order', 'ActWindow',
    'Menu', 'Keyword', 'Model', 'OpenChartStart', 'OpenChart',
    'ReportExecution', 'OpenExecutionSelect', 'OpenExecution',
    'UpdateDataWizardStart', 'UpdateDataWizardUpdated', 'UpdateDataWizard',
    'FilterParameter']
__metaclass__ = PoolMeta


FIELD_TYPES = [
    ('char', 'Char'),
    ('int', 'Integer'),
    ('float', 'Float'),
    ('numeric', 'Numeric'),
    ('bool', 'Boolean'),
    ('many2one', 'Many To One'),
    ]

AGGREGATE_TYPES = [
    ('avg', 'Average'),
    ('sum', 'Sum'),
    ('count', 'Count'),
    ('max', 'Max'),
    ('min', 'Min'),
    ]

SRC_CHARS = u""" .'"()/*-+?¿!&$[]{}@#`'^:;<>=~%,|\\"""
DST_CHARS = u"""__________________________________"""
CELERY_AVAILABLE = False
try:
    import celery
    CELERY_AVAILABLE = True
except ImportError:
    pass
except AttributeError:
    # If run from within frepple we will get
    # AttributeError: 'module' object has no attribute 'argv'
    pass


def unaccent(text):
    if not (isinstance(text, str) or isinstance(text, unicode)):
        return str(text)
    if isinstance(text, str):
        text = unicode(text, 'utf-8')
    text = text.lower()
    for c in xrange(len(SRC_CHARS)):
        if c >= len(DST_CHARS):
            break
        text = text.replace(SRC_CHARS[c], DST_CHARS[c])
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore')


def start_celery():
    celery_start = config.getboolean('celery', 'auto_start', True)
    if not CELERY_AVAILABLE or not celery_start:
        return
    db = Transaction().cursor.database_name
    _, config_path = tempfile.mkstemp(prefix='trytond-celery-')
    with open(config_path, 'w') as f:
        config.write(f)
    env = {
        'TRYTON_DATABASE': db,
        'TRYTON_CONFIG': config_path
    }
    # Copy environment variables in order to get virtualenvs working
    for key, value in os.environ.iteritems():
        env[key] = value
    call = ['celery', 'worker', '--app=tasks', '--loglevel=info',
        '--workdir=./modules/babi', '--queues=' + db,
        '--time-limit=7400',
        '--concurrency=1',
        '--hostname=' + db + '.%h',
        '--pidfile=' + os.path.join(tempfile.gettempdir(), 'trytond_celery_' +
            db + '.pid')]
    subprocess.Popen(call, env=env)


class DynamicModel(ModelSQL, ModelView):
    @classmethod
    def __setup__(cls):
        super(DynamicModel, cls).__setup__()
        cls._error_messages.update({
                'report_not_exists': ('Report "%s" no longer exists or you do '
                    'not have the rights to access it.'),
                })
        pool = Pool()
        Execution = pool.get('babi.report.execution')
        executions = Execution.search([
                ('babi_model.model', '=', cls.__name__),
                ])
        if not executions or len(executions) > 1:
            return
        execution, = executions
        try:
            cls._order = execution.get_orders()
        except AssertionError:
            # An exception error is raisen on tests where Execution is not
            # properly loaded in the pool
            pass

    @classmethod
    def fields_view_get(cls, view_id=None, view_type='form'):
        pool = Pool()
        Execution = pool.get('babi.report.execution')
        Dimension = pool.get('babi.dimension')
        InternalMeasure = pool.get('babi.internal.measure')

        model_name = '_'.join(cls.__name__.split('_')[0:2])
        executions = Execution.search([
                ('babi_model.model', '=', cls.__name__),
                ], limit=1)
        if not executions:
            cls.raise_user_error('report_not_exists', cls.__name__)
        context = Transaction().context
        execution, = executions
        with Transaction().set_context(_datetime=execution.create_date):
            report = execution.report
            view_type = context.get('view_type', view_type)

            result = {}
            result['type'] = view_type
            result['view_id'] = view_id
            result['field_childs'] = None
            fields = []
            if view_type == 'tree' or view_type == 'form':
                keyword = ''
                if view_type == 'tree':
                    keyword = 'keyword_open="1"'
                    fields.append('children')
                xml = '<%s string="%s" %s>\n' % (view_type, report.model.name,
                    keyword)
                for field in report.dimensions + execution.internal_measures:
                    # Avoid duplicated fields
                    if field.internal_name in fields:
                        continue
                    if view_type == 'form':
                        xml += '<label name="%s"/>\n' % (field.internal_name)
                    xml += '<field name="%s"/>\n' % (field.internal_name)
                    fields.append(field.internal_name)
                xml += '</%s>\n' % (view_type)
                result['arch'] = xml
                if view_type == 'tree' and context.get('babi_tree_view'):
                    result['field_childs'] = 'children'
            elif view_type == 'graph':
                # TODO: Remove it on 3.6 as client autogenerates it
                colors = ['#FF0000', '#0000FF', '#008000', '#FFFF00',
                    '#800080', '#FF00FF', '#FFA500', '#C0C0C0', '#000000']
                model_name = context.get('model_name')
                graph_type = context.get('graph_type')
                measure_ids = context.get('measures')
                legend = context.get('legend') and 1 or 0
                interpolation = context.get('interpolation', 'linear')
                dimension = Dimension(context.get('dimension'))

                x_xml = '<field name="%s"/>\n' % dimension.internal_name
                fields.append(dimension.internal_name)

                y_xml = ''
                for i, measure in enumerate(InternalMeasure.browse(
                            measure_ids)):
                    color = colors[i % len(colors)]
                    y_xml += ('<field name="%s" interpolation="%s" '
                        'color="%s"/> \n') % (measure.internal_name,
                            interpolation, color)
                    fields.append(measure.internal_name)

                xml = '''<?xml version="1.0"?>
                    <graph string="%(graph_name)s" type="%(graph_type)s"
                        legend="%(legend)s" background="#FFFFFF">
                        <x>
                            %(x_fields)s
                        </x>
                        <y>
                            %(y_fields)s
                        </y>
                    </graph>''' % {
                        'graph_type': graph_type,
                        'graph_name': model_name,
                        'legend': legend,
                        'x_fields': x_xml,
                        'y_fields': y_xml,
                        }
                result['arch'] = xml
            else:
                assert False
        result['fields'] = cls.fields_get(fields)
        return result

    def get_rec_name(self, name):
        result = []
        for field in self._babi_dimensions:
            value = getattr(self, field)
            if not value:
                result.append('-')
            elif isinstance(value, ModelSQL):
                result.append(value.rec_name)
            elif not isinstance(value, unicode):
                result.append(unicode(value))
            else:
                result.append(value)
        return ' / '.join(result)


def create_columns(name, ffields):
    "Create fields of new model"
    columns = {}
    for field in ffields:
        fname = field['name']
        field_name = field['internal_name']
        ttype = field['ttype']
        if ttype == 'int':
            columns[field_name] = fields.Integer(fname, select=1)
        elif ttype == 'float':
            columns[field_name] = fields.Float(fname, digits=(16, 2),
                select=1)
        elif ttype == 'numeric':
            columns[field_name] = fields.Numeric(fname, digits=(16, 2),
                select=1)
        elif ttype == 'char':
            columns[field_name] = fields.Char(fname, select=1)
        elif ttype == 'bool':
            columns[field_name] = fields.Boolean(fname, select=1)
        elif ttype == 'many2one':
            columns[field_name] = fields.Many2One(field['related_model'],
                fname, ondelete='SET NULL', select=1)

    columns['babi_group'] = fields.Char('Group', size=500)
    columns['parent'] = fields.Many2One(name, 'Parent', ondelete='CASCADE',
        select=True, left='parent_left', right='parent_right')
    columns['children'] = fields.One2Many(name, 'parent', 'Children')
    columns['parent_left'] = fields.Integer('Parent Left', select=True)
    columns['parent_right'] = fields.Integer('Parent Right', select=True)
    return columns


def create_class(name, description, dimensions, measures):
    "Create class, and make instance"
    body = {
        '__doc__': description,
        '__name__': name,
        # Used in get_rec_name()
        '_defaults': {},
        '_babi_dimensions': [x['internal_name'] for x in dimensions],
        }
    body.update(create_columns(name, dimensions + measures))
    return type(name, (DynamicModel, ), body)


def register_class(internal_name, name, dimensions, measures):
    "Register class an return model"
    pool = Pool()
    Model = pool.get('ir.model')

    Class = create_class(internal_name, name, dimensions, measures)
    Pool.register(Class, module='babi', type_='model')
    Class.__setup__()
    pool.add(Class, type='model')
    Class.__post_setup__()
    Class.__register__('babi')
    model, = Model.search([
            ('model', '=', internal_name),
            ])
    return model


def create_groups_access(model, groups):
    "Creates group access for a given model"
    pool = Pool()
    ModelAccess = pool.get('ir.model.access')
    to_create = []
    for group in groups:
        exists = ModelAccess.search([
                ('model', '=', model.id),
                ('group', '=', group.id),
                ])
        if not exists:
            to_create.append({
                    'model': model.id,
                    'group': group.id,
                    'perm_read': True,
                    'perm_create': True,
                    'perm_write': True,
                    'perm_delete': True,
                    })
    if to_create:
        ModelAccess.create(to_create)


class TimeoutException(Exception):
    pass


class TimeoutChecker:
    def __init__(self, timeout, callback):
        self._timeout = timeout
        self._callback = callback
        self._start = datetime.now()

    def check(self):
        elapsed = (datetime.now() - self._start).seconds
        if elapsed > self._timeout:
            self._callback()


class DimensionIterator:
    def __init__(self, values):
        """
        values should be a dictionary where its values are
        non-empty lists.
        """
        self.values = values
        self.keys = sorted(values.keys())
        self.keys.reverse()
        self.current = dict.fromkeys(values.keys(), 0)
        self.current[self.keys[0]] = -1

    def __iter__(self):
        return self

    def next(self):
        for x in xrange(len(self.keys)):
            key = self.keys[x]
            if self.current[key] >= len(self.values[key]) - 1:
                if x == len(self.keys) - 1:
                    raise StopIteration
                self.current[key] = 0
            else:
                self.current[key] += 1
                break
        return self.current


class Filter(ModelSQL, ModelView):
    "Filter"
    __name__ = 'babi.filter'
    _history = True

    name = fields.Char('Name', required=True, translate=True)
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)])
    model_name = fields.Function(fields.Char('Model Name'),
        'on_change_with_model_name')
    view_search = fields.Many2One('ir.ui.view_search', 'Search',
        domain=[('model', '=', Eval('model_name'))],
        depends=['model_name'])
    domain = fields.Char('Domain')
    python_expression = fields.Char('Python Expression',
        help='The python expression introduced will be evaluated. If the '
        'result is True the record will be included, it will be discarded '
        'otherwise.')
    parameters = fields.One2Many('babi.filter.parameter', 'filter',
        'Parameters',
        states={
            'invisible': Not(Eval('context', {}).get('groups', []).contains(
                Id('babi', 'group_babi_admin'))),
            })
    fields = fields.Function(fields.Many2Many('ir.model.field', None, None,
            'Model Fields', depends=['model']),
        'on_change_with_fields')

    @classmethod
    def __setup__(cls):
        super(Filter, cls).__setup__()
        cls._error_messages.update({
                'parameter_not_found': ('Parameter "%s" not found in Domain '
                    'nor in Python Expression.'),
                })

    @classmethod
    def validate(cls, filters):
        for filter in filters:
            filter.check_dinamic_filters()

    def check_dinamic_filters(self):
        for filter in self.parameters:
            placeholder = '{%s}' % filter.name
            if placeholder not in self.domain and \
                    placeholder not in self.python_expression:
                self.raise_user_error('parameter_not_found', filter.name)

    @depends('model')
    def on_change_with_model_name(self, name=None):
        return self.model.model if self.model else None

    @depends('model')
    def on_change_with_fields(self, name=None):
        if not self.model:
            return []
        return [x.id for x in self.model.fields]

    @depends('view_search')
    def on_change_with_domain(self):
        return self.view_search.domain if self.view_search else None

    @depends('model_name', 'domain')
    def on_change_with_view_search(self):
        ViewSearch = Pool().get('ir.ui.view_search')
        searches = ViewSearch.search([
                ('model', '=', self.model_name),
                ('domain', '=', self.domain),
                ])
        if not searches:
            return None
        return searches[0].id


class FilterParameter(ModelSQL, ModelView):
    "Filter Parameter"
    __name__ = 'babi.filter.parameter'
    _history = True

    filter = fields.Many2One('babi.filter', 'Filter', required=True)
    name = fields.Char('Name', required=True, translate=True, help='Name used '
        'on the domain substitution')
    ttype = fields.Selection(FIELD_TYPES + [('many2many', 'Many To Many')],
        'Field Type', required=True)
    related_model = fields.Many2One('ir.model', 'Related Model', states={
            'required': Eval('ttype').in_(['many2one', 'many2many']),
            'readonly': Not(Eval('ttype').in_(['many2one', 'many2many'])),
            }, depends=['ttype'])

    def create_keyword(self):
        pool = Pool()
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        Keyword = pool.get('ir.action.keyword')

        if self.ttype in ['many2one', 'many2many']:
            action = Action(ModelData.get_id('babi', 'open_execution_wizard'))
            keyword = Keyword()
            keyword.keyword = 'form_relate'
            keyword.model = '%s,-1' % self.related_model.model
            keyword.action = action.action
            keyword.babi_filter_parameter = self
            keyword.save()

    @classmethod
    def create(cls, vlist):
        filters = super(FilterParameter, cls).create(vlist)
        for filter in filters:
            filter.create_keyword()
        return filters

    @classmethod
    def write(cls, *args):
        pool = Pool()
        Keyword = pool.get('ir.action.keyword')
        super(FilterParameter, cls).write(*args)
        actions = iter(args)
        for filters, values in zip(actions, actions):
            if 'related_model' in values:
                filter_ids = [f.id for f in filters]
                Keyword.delete(Keyword.search([
                            ('babi_filter_parameter', 'in', filter_ids),
                        ]))
                for filter in filters:
                    filter.create_keyword()

    @classmethod
    def delete(cls, filters):
        pool = Pool()
        Keyword = pool.get('ir.action.keyword')
        Keyword.delete(Keyword.search([
                    ('babi_filter_parameter', 'in', [f.id for f in filters]),
                ]))
        super(FilterParameter, cls).delete(filters)


class Expression(ModelSQL, ModelView):
    "Expression"
    __name__ = 'babi.expression'
    _history = True

    name = fields.Char('Name', required=True, translate=True)
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)])
    expression = fields.Char('Expression', required=True,
        help='Python expression that will return the value to be used.\n'
            'The expression can include the following variables:\n\n'
            '- "o": A reference to the current record being processed. For '
            ' example: "o.party.name"\n'
            '\nAnd the following functions apply to dates and timestamps:\n\n'
            '- "y()": Returns the year (as a string)\n'
            '- "m()": Returns the month (as a string)\n'
            '- "w()": Returns the week (as a string)\n'
            '- "d()": Returns the day (as a string)\n'
            '- "ym()": Returns the year-month (as a string)\n'
            '- "ymd()": Returns the year-month-day (as a string).\n')
    ttype = fields.Selection(FIELD_TYPES, 'Field Type', required=True)
    related_model = fields.Many2One('ir.model', 'Related Model', states={
            'required': Eval('ttype') == 'many2one',
            'readonly': Eval('ttype') != 'many2one',
            }, depends=['ttype'])
    fields = fields.Function(fields.Many2Many('ir.model.field', None, None,
            'Model Fields'), 'on_change_with_fields')

    @depends('model')
    def on_change_with_fields(self, name=None):
        if not self.model:
            return []
        return [x.id for x in self.model.fields]


class Report(ModelSQL, ModelView):
    "Report"
    __name__ = 'babi.report'
    _history = True

    name = fields.Char('Name', required=True, translate=True,
        help='New virtual model name.')
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)], help='Model for data extraction')
    model_name = fields.Function(fields.Char('Model Name'),
        'on_change_with_model_name')
    internal_name = fields.Function(fields.Char('Internal Name', states={
                'invisible': Not(Eval('context', {}).get('groups', []
                        ).contains(Id('babi', 'group_babi_admin'))),
                }),
        'get_internal_name')
    filter = fields.Many2One('babi.filter', 'Filter',
        domain=[('model', '=', Eval('model'))], depends=['model'])
    dimensions = fields.One2Many('babi.dimension', 'report',
        'Dimensions')
    columns = fields.One2Many('babi.dimension.column', 'report',
        'Dimensions on Columns')
    measures = fields.One2Many('babi.measure', 'report', 'Measures')
    order = fields.One2Many('babi.order', 'report', 'Order', order=[
            ('sequence', 'ASC')
            ])
    groups = fields.Many2Many('babi.report-res.group', 'report', 'group',
        'Groups', help='User groups that will be able to see use this report.')
    parent_menu = fields.Many2One('ir.ui.menu', 'Parent Menu',
        required=True)
    menus = fields.One2Many('ir.ui.menu', 'babi_report', 'Menus',
        readonly=True,
        states={
            'invisible': Not(Eval('context', {}).get('groups', []).contains(
                Id('babi', 'group_babi_admin'))),
            })
    actions = fields.One2Many('ir.action.act_window', 'babi_report',
        'Actions', readonly=True,
        states={
            'invisible': Not(Eval('context', {}).get('groups', []).contains(
                Id('babi', 'group_babi_admin'))),
            })
    keywords = fields.One2Many('ir.action.keyword', 'babi_report', 'Keywords',
        readonly=True,
        states={
            'invisible': Not(Eval('context', {}).get('groups', []).contains(
                Id('babi', 'group_babi_admin'))),
            })
    timeout = fields.Integer('Timeout', required=True, help='If report '
        'calculation should take more than the specified timeout (in seconds) '
        'the process will be stopped automatically.')
    executions = fields.One2Many('babi.report.execution', 'report',
        'Executions', readonly=True, order=[('date', 'DESC')],
        states={
            'invisible': Not(Eval('context', {}).get('groups', []).contains(
                Id('babi', 'group_babi_admin'))),
            })
    last_execution = fields.Function(fields.Many2One('babi.report.execution',
        'Last Executions', readonly=True), 'get_last_execution')
    crons = fields.One2Many('ir.cron', 'babi_report', 'Schedulers',
        context={'babi_report': Eval('id')})

    @classmethod
    def __setup__(cls):
        super(Report, cls).__setup__()
        cls._error_messages.update({
                'no_dimensions': ('Report "%s" has no dimensions. At least '
                    'one is needed.'),
                'no_measures': ('Report "%s" has no measures. At least one '
                    'is needed.'),
                'timeout_exception': ('Report calculation exceeded timeout '
                    'limit.')
                })
        cls._buttons.update({
                'calculate': {},
                'create_menus': {},
                'remove_menus': {},
                })

        start_celery()

    @staticmethod
    def default_timeout():
        Config = Pool().get('babi.configuration')
        config = Config(1)
        return config.default_timeout

    @depends('model')
    def on_change_with_model_name(self, name=None):
        return self.model.model if self.model else None

    def get_internal_name(self, name):
        return 'babi_report_%d' % self.id

    def get_last_execution(self, name):
        if self.executions:
            for execution in self.executions:
                if execution.state == 'calculated' and not execution.filtered:
                    return execution.id

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        to_update = []
        for reports, values in zip(actions, actions):
            if 'name' in values:
                for report in reports:
                    if report.name != values['name']:
                        to_update.append(report)
        if to_update:
            cls.remove_menus(to_update)
        return super(Report, cls).write(*args)

    @classmethod
    def delete(cls, reports):
        cls.remove_menus(reports)
        cls.remove_crons(reports)
        with Transaction().set_context(babi_order_force=True):
            return super(Report, cls).delete(reports)

    @classmethod
    def copy(cls, reports, default=None):
        if default is None:
            default = {}
        default = default.copy()
        if 'order' not in default:
            default['order'] = None
        default['actions'] = None
        default['keywords'] = None
        default['menus'] = None
        default['executions'] = None
        if 'name' not in default:
            result = []
            for report in reports:
                default['name'] = '%s (2)' % report.name
                result.extend(super(Report, cls).copy([report], default))
            return result
        return super(Report, cls).copy(reports, default)

    @classmethod
    def remove_crons(cls, reports):
        pool = Pool()
        Cron = pool.get('ir.cron')
        Cron.delete([c for r in reports for c in r.crons])

    @classmethod
    def remove_menus(cls, reports):
        "Remove all menus and actions created"
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Menu = pool.get('ir.ui.menu')
        actions = []
        menus = []
        for report in reports:
            actions += report.actions
            menus += report.menus
        ActWindow.delete(actions)
        Menu.delete(menus)

    def create_tree_view_menu(self, langs):
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Action = pool.get('ir.action.wizard')
        Menu = pool.get('ir.ui.menu')
        ModelData = pool.get('ir.model.data')
        # This action is needed for the wizard to open the data
        action = ActWindow()
        action.name = self.name
        action.res_model = 'babi.report'
        action.domain = "[('parent', '=', None)]"
        action.babi_report = self
        action.groups = self.groups
        action.context = "{'babi_tree_view': True}"
        action.save()
        wizard = Action(ModelData.get_id('babi', 'open_execution_wizard'))
        menu = Menu()
        menu.name = self.name
        menu.parent = self.parent_menu
        menu.babi_report = self
        menu.action = str(wizard)
        menu.icon = 'tryton-tree'
        menu.groups = self.groups
        menu.babi_type = 'tree'
        menu.save()
        if langs:
            for lang in langs:
                with Transaction().set_context(language=lang.code,
                        fuzzy_translation=False):
                    data, = self.read([self], fields_names=['name'])
                    Menu.write([menu], data)
        return menu.id

    def create_list_view_menu(self, parent, langs):
        "Create list view and action to open"
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        Menu = pool.get('ir.ui.menu')
        # This action is needed for the wizard to open the data
        action = ActWindow()
        action.name = self.name
        action.res_model = 'babi.report'
        action.babi_report = self
        action.groups = self.groups
        action.save()
        wizard = Action(ModelData.get_id('babi', 'open_execution_wizard'))
        menu = Menu()
        menu.name = self.name
        menu.parent = parent
        menu.babi_report = self
        menu.action = str(wizard)
        menu.icon = 'tryton-list'
        menu.groups = self.groups
        menu.babi_type = 'list'
        menu.save()
        if langs:
            for lang in langs:
                with Transaction().set_context(language=lang.code,
                        fuzzy_translation=False):
                    data, = self.read([self], fields_names=['name'])
                    Menu.write([menu], data)
        return menu.id

    def create_update_wizard_menu(self, parent):
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        action = Action(ModelData.get_id('babi', 'open_execution_wizard'))
        menu = Menu(ModelData.get_id('babi', 'menu_update_data'))
        menu, = Menu.copy([menu], {
                'parent': parent,
                'babi_report': self.id,
                'icon': 'tryton-executable',
                'groups': [
                    ('remove', [g.id for g in menu.groups]),
                    ('add', [x.id for x in self.groups]),
                    ],
                'babi_type': 'wizard',
                'active': True,
                })
        menu.action = str(action)
        menu.save()

    def create_history_menu(self, parent):
        pool = Pool()
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        Menu = pool.get('ir.ui.menu')
        action = Action(ModelData.get_id('babi', 'open_execution_wizard'))
        menu = Menu(ModelData.get_id('babi', 'menu_historical_data'))
        menu, = Menu.copy([menu], {
                'parent': parent,
                'babi_report': self.id,
                'icon': 'tryton-executable',
                'groups': [
                    ('remove', [g.id for g in menu.groups]),
                    ('add', [x.id for x in self.groups]),
                    ],
                'babi_type': 'history',
                'active': True,
                })
        menu.action = str(action)
        menu.save()

    @classmethod
    def create_menus(cls, reports):
        """Regenerates all actions and menu entries"""
        pool = Pool()
        Lang = pool.get('ir.lang')
        langs = Lang.search([
            ('translatable', '=', True),
            ])
        cls.remove_menus(reports)
        for report in reports:
            menu = report.create_tree_view_menu(langs)
            report.create_list_view_menu(menu, langs)
            report.create_update_wizard_menu(menu)
            report.create_history_menu(menu)
        return 'reload menu'

    def get_dimensions(self, with_columns=False):
        dimensions = []
        for dimension in self.dimensions:
            dimensions.append(dimension.get_dimension_data())
        if with_columns:
            for dimension in self.columns:
                dimensions.append(dimension.get_dimension_data())

        return dimensions

    def get_execution_data(self):
        return {
            'report': self.id,
            'timeout': self.timeout,
            }

    @classmethod
    def calculate_babi_report(cls, args=None):
        """This method is intended to be called from ir.cron"""
        if not args:
            args = []
        reports = cls.search([('id', '=', args)])
        return cls.calculate(reports)

    @classmethod
    def calculate(cls, reports):
        pool = Pool()
        transaction = Transaction()
        cursor = transaction.cursor
        Execution = pool.get('babi.report.execution')
        celery_start = config.getboolean('celery', 'auto_start', True)
        for report in reports:
            if not report.measures:
                cls.raise_user_error('no_measures', report.rec_name)
            if not report.dimensions:
                cls.raise_user_error('no_dimensions', report.rec_name)
            execution, = Execution.create([report.get_execution_data()])
            cursor.commit()
            if CELERY_AVAILABLE and celery_start:
                os.system('celery call tasks.calculate_execution '
                    '--args=[%d,%d] '
                    '--config="trytond.modules.babi.celeryconfig" '
                    '--queue=%s' % (execution.id, transaction.user,
                        cursor.database_name))
            else:
                # Fallback to synchronous mode if celery is not available
                Execution.calculate([execution])


class ReportExecution(ModelSQL, ModelView):
    "Report Execution"
    __name__ = 'babi.report.execution'

    report = fields.Many2One('babi.report', 'Report', required=True,
        readonly=True, ondelete='CASCADE')
    date = fields.DateTime('Execution Date', required=True, readonly=True)
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    report_model = fields.Function(fields.Many2One('ir.model', 'Report Model'),
        'on_change_with_report_model')
    babi_model = fields.Many2One('ir.model', 'BI Model', readonly=True,
            help='Link to new model instance')
    state = fields.Selection([
            ('pending', 'Pending'),
            ('in_progress', 'In progress'),
            ('calculated', 'Calculated'),
            ('timeout', 'Timeout'),
            ('failed', 'Failed'),
            ('canceled', 'Canceled'),
            ], 'State', required=True, readonly=True)
    timeout = fields.Integer('Timeout', required=True, readonly=True,
        help='If report calculation should take more than the specified '
        'timeout (in seconds) the process will be stopped automatically.')
    duration = fields.Float('Duration', readonly=True,
        help='Number of seconds the calculation took.')
    filtered = fields.Boolean('Filtered', help='Used to mark executions with '
        'parameter filters evaluated', readonly=True)
    filter_values = fields.Text('Filter Values', readonly=True)
    internal_measures = fields.One2Many('babi.internal.measure',
        'execution', 'Internal Measures', readonly=True)
    pid = fields.Integer('Pid', readonly=True)

    @classmethod
    def __setup__(cls):
        super(ReportExecution, cls).__setup__()
        cls._order.insert(0, ('date', 'DESC'))
        cls._error_messages.update({
                'filter_parameters': ('Execution "%s" has filter parameters '
                    ' and you did not provide any of them. Please execute it '
                    ' from the menu.'),
                'no_dimensions': ('Execution "%s" has no dimensions. At least '
                    'one is needed.'),
                'no_measures': ('Execution "%s" has no measures. At least one '
                    'is needed.'),
                })
        cls._buttons.update({
                'open': {
                    'invisible': Eval('state') != 'calculated',
                    },
                'cancel': {
                    'invisible': ((Eval('state') != 'in_progress') &
                        ~Eval('pid', False)),
                    },
                })

    @staticmethod
    def default_date():
        return datetime.now()

    @staticmethod
    def default_state():
        return 'pending'

    @staticmethod
    def default_filtered():
        return False

    @staticmethod
    def default_timeout():
        Config = Pool().get('babi.configuration')
        config = Config(1)
        return config.default_timeout

    def get_rec_name(self, name):
        return '%s (%s)' % (self.report.rec_name, self.date)

    def get_internal_name(self, name):
        return 'babi_execution_%d' % self.id

    def get_measures(self):
        measures = []
        for measure in self.internal_measures:
            measures.append(measure.get_measure_data())
        return measures

    def get_orders(self):
        order = []
        with Transaction().set_context(_datetime=self.date):
            for record in self.report.order:
                if record.dimension:
                    field = record.dimension.internal_name
                    order.append((field, record.order))
                else:
                    for measure in record.measure.internal_measures:
                        if measure.execution == self:
                            field = measure.internal_name
                            order.append((field, record.order))
        return order

    @depends('report')
    def on_change_with_report_model(self, name=None):
        if self.report:
            return self.report.model.id

    @classmethod
    @ModelView.button_action('babi.open_execution_wizard')
    def open(cls, executions):
        pass

    @classmethod
    @ModelView.button
    def cancel(cls, executions):
        for execution in executions:
            if execution.state != 'in_progress':
                continue
            if not execution.pid:
                continue
            os.kill(execution.pid, 15)
            execution.state = 'canceled'
            execution.save()

    @classmethod
    def delete(cls, executions):
        cls.remove_data(executions)
        cls.remove_keywords(executions)
        to_delete = set([e.internal_name for e in executions])
        super(ReportExecution, cls).delete(executions)
        # We should remove the classes from the pool so when removing realted
        # records it doesn't fail checking unexisting models
        pool = Pool()
        with pool.lock:
            for name in to_delete:
                del pool._pool[pool.database_name]['model'][name]

    @classmethod
    def remove_keywords(cls, executions):
        pool = Pool()
        Keyword = pool.get('ir.action.keyword')

        models = ['%s,-1' % e.babi_model.model for e in executions]
        keywords = Keyword.search([('model', 'in', models)])
        Keyword.delete(keywords)

    @classmethod
    def remove_data(cls, executions):
        pool = Pool()
        cursor = Transaction().cursor
        for execution in executions:
            execution.validate_model()
            Model = pool.get(execution.babi_model.model)
            if Model:
                cursor.execute("DROP TABLE IF EXISTS %s " % Model._table)
                try:
                    # SQLite doesn't have sequences
                    cursor.execute("DROP SEQUENCE IF EXISTS %s_id_seq" %
                        Model._table)
                except:
                    pass

    def validate_model(self, with_columns=False):
        "makes model available on Tryton and pool instance"

        dimensions = self.report.get_dimensions(with_columns)
        measures = self.get_measures()

        model = register_class(self.internal_name, self.report.name,
            dimensions, measures)

        if not self.babi_model:
            self.babi_model = model
            self.save()

        create_groups_access(model, self.report.groups)
        # Commit transaction to avoid locks
        Transaction().cursor.commit()

    def timeout_exception(self):
        raise TimeoutException

    @staticmethod
    def save_state(execution_id, state, exception=False):
        " Save state in a new transaction"
        DatabaseOperationalError = backend.get('DatabaseOperationalError')
        Transaction().cursor.rollback()
        with Transaction().new_cursor() as new_transaction:
            try:
                pool = Pool()
                Execution = pool.get('babi.report.execution')
                Model = pool.get('ir.model')
                new_instances = Execution.browse([execution_id])
                to_write = {'state': state}
                if state == 'in_progress':
                    to_write['pid'] = os.getpid()
                Execution.write(new_instances, to_write)
                if exception:
                    Execution.remove_data(new_instances)
                    Model.delete([e.babi_model for e in new_instances])
                new_transaction.cursor.commit()
            except DatabaseOperationalError:
                new_transaction.cursor.rollback()

    @classmethod
    def calculate(cls, executions):
        transaction = Transaction()
        for execution in executions:
            execution.save_state(execution.id, 'in_progress')
            date = execution.create_date
            with transaction.set_context(_datetime=date):
                execution.validate_model()
                with transaction.set_user(0):
                    execution.create_keywords()
                try:
                    execution.create_data()
                except TimeoutException:
                    execution.save_state(execution.id, 'timeout',
                        exception=True)
                    cls.raise_user_error('timeout_exception')
                except Exception:
                    execution.save_state(execution.id, 'failed',
                        exception=True)
                    execution.save()
                    raise

    def get_python_filter(self):
        if self.report.filter and self.report.filter.python_expression:
            return self.report.filter.python_expression

    def create_keywords(self):
        pool = Pool()
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        Keyword = pool.get('ir.action.keyword')

        action = Action(ModelData.get_id('babi', 'open_chart_wizard'))
        keyword = Keyword()
        keyword.keyword = 'tree_open'
        keyword.model = '%s,-1' % self.babi_model.model
        keyword.action = action.action
        keyword.babi_report = self.report
        keyword.groups = self.report.groups
        keyword.save()

    def create_data(self):
        "Creates data for this execution"
        pool = Pool()
        Model = pool.get(self.report.model.model)
        transaction = Transaction()
        cursor = transaction.cursor

        BIModel = pool.get(self.babi_model.model)
        checker = TimeoutChecker(self.timeout, self.timeout_exception)

        logger = logging.getLogger()

        logger.info('Updating Data of report: %s' % self.rec_name)
        update_start = time.time()
        model = self.report.model.model
        if not self.report.measures:
            self.raise_user_error('no_measures', self.rec_name)
        if not self.report.dimensions:
            self.raise_user_error('no_dimensions', self.rec_name)

        domain = '[]'
        if self.report.filter and self.report.filter.domain:
            domain = self.report.filter.domain
            if '__' in domain:
                domain = str(PYSONDecoder().decode(domain))
        if domain and self.report.filter and (
                len(self.report.filter.parameters) > 0):
            if not self.filter_values:
                self.raise_user_error('filter_parameters', self.rec_name)
            filter_data = json.loads(self.filter_values.encode('utf-8'),
                object_hook=JSONDecoder())
            values = {}
            for key, value in filter_data.iteritems():
                key = '_'.join(key.split('_')[:-1])
                if not value or key not in domain:
                    continue
                values[key] = value
            if domain:
                domain = domain.format(**values)
        domain = safe_eval(domain, {
                'datetime': mdatetime,
                'false': False,
                'true': True,
                })
        start = datetime.today()
        self.update_internal_measures()
        with_columns = len(self.report.columns) > 0
        self.validate_model(with_columns=with_columns)

        dimension_names = [x.internal_name for x in self.report.dimensions]
        dimension_expressions = [(x.expression.expression,
                        '' if x.expression.ttype == 'many2one'
                        else 'empty') for x in
            self.report.dimensions]
        measure_names = [x.internal_name for x in
            self.internal_measures]
        measure_expressions = [x.expression for x in
            self.internal_measures]
        if self.report.columns:
            dimension_names.extend([x.internal_name for x in
                    self.report.columns])
            dimension_expressions.extend([(x.expression.expression,
                        '' if x.expression.ttype == 'many2one'
                        else 'empty') for x in self.report.columns])

        columns = (['create_date', 'create_uid'] + dimension_names +
            measure_names)
        columns = ['"%s"' % x for x in columns]
        # Some older versions of psycopg do not allow column names
        # to be of type unicode
        columns = [str(x) for x in columns]

        uid = transaction.user
        python_filter = self.get_python_filter()

        table = BIModel._table
        if self.report.columns:
            table = BIModel._table + '_tmp'
            # Save data to a temporally table:
            cursor.execute('CREATE TEMP TABLE %s AS SELECT * FROM %s WHERE '
                ' 0 = 1' % (table, BIModel._table))

        # Process records
        offset = 2000
        index = 0

        def sanitanize(x):
            if (isinstance(x, basestring) or isinstance(x, str)
                    or isinstance(x, unicode)):
                x = x.replace('|', '-')
            if not isinstance(x, unicode):
                return unicode(x)
            else:
                return unicode(x)

        with transaction.set_context(_datetime=None):
            records = Model.search(domain, offset=index*offset, limit=offset)
        while records:
            checker.check()
            logger.info('Calculated %s,  %s records in %s seconds'
                % (model, index * offset, datetime.today() - start))

            to_create = ''
            # var o it's used on expression!!
            # Don't rename var
            # chunk = records[index * offset:(index + 1) * offset]
            for record in records:
                if python_filter:
                    if not babi_eval(python_filter, record,
                            convert_none=False):
                        continue
                vals = ['now()', str(uid)]
                vals += [sanitanize(babi_eval(x[0], record, convert_none=x[1]))
                    for x in dimension_expressions]
                vals += [sanitanize(babi_eval(x, record, convert_none='zero'))
                    for x in measure_expressions]
                record = u'|'.join(vals).replace('\n', ' ')
                to_create += record.replace('\\', '').encode('utf-8') + '\n'

            if to_create:
                if hasattr(cursor, 'copy_from'):
                    data = StringIO(to_create)
                    cursor.copy_from(data, table, sep='|', null='',
                        columns=columns)
                else:
                    base_query = 'INSERT INTO %s (' % table
                    base_query += ','.join([unicode(x) for x in columns])
                    base_query += ' ) VALUES '
                    for line in to_create.split('\n'):
                        if len(line) == 0:
                            continue
                        query = base_query + '(now(),'
                        query += ','.join(["'%s'" % unicode(x)
                                for x in line.split('|')[1:]])
                        query += ')'
                        cursor.execute(query)

            index += 1
            with transaction.set_context(_datetime=None):
                records = Model.search(domain, offset=index * offset,
                    limit=offset)

        if self.report.columns:
            distincts = self.distinct_dimension_columns(cursor, table)
            self.update_internal_measures(distincts)
            self.validate_model()
            query = 'INSERT INTO %s ('
            query += ','.join([unicode(x) for x in columns])
            query += ',' + ','.join([unicode(x.internal_name) for x in
                    self.internal_measures])
            query += ') SELECT '
            query += ','.join([unicode(x) for x in columns])
            query += ',' + ','.join([unicode(x.expression) for x in
                    self.internal_measures])
            query += ' FROM %s '
            cursor.execute(query % (BIModel._table, table))
            cursor.execute('DROP TABLE %s ' % (table))

        self.update_measures(checker)

        logger.info('Calc all %s records in %s seconds'
            % (model, datetime.today() - start))

        self.state = 'calculated'
        self.duration = time.time() - update_start
        self.save()
        logger.info('End Update Data of report: %s' % self.rec_name)

    def distinct_dimension_columns(self, cursor, tablename):
        distincts = {}
        for dimension in self.report.columns:
            cursor.execute('SELECT %s from %s group by 1 order by 1' % (
                    dimension.internal_name, tablename))
            distincts[dimension.id] = [unicode(x[0]) for x in
                cursor.fetchall()]
        return distincts

    def update_internal_measures(self, distincts=None):
        InternalMeasure = Pool().get('babi.internal.measure')

        to_create = []
        if distincts is None:
            distincts = {}

        for key in distincts.keys():
            # TODO: Make translatable
            distincts[key] = ['(all)'] + sorted(list(distincts[key]))

        columns = {}
        for column in self.report.columns:
            columns[column.id] = column

        InternalMeasure.delete(self.internal_measures)
        sequence = 0
        for measure in self.report.measures:
            sequence += 1
            related_model_id = None
            if measure.expression.ttype == 'many2one':
                related_model_id = measure.expression.relate_model.id
            if distincts:
                iterator = DimensionIterator(distincts)
            else:
                iterator = [None]
            for combination in iterator:
                name = []
                internal_name = []
                expression = measure.expression.expression
                if combination:
                    for key, index in combination.iteritems():
                        dimension = columns[key]
                        value = distincts[key][index]
                        name.append(dimension.name + ' ' + value)
                        internal_name.append(dimension.internal_name + '_' +
                            unaccent(value))
                        # Zero will always be the '(all)' entry added above
                        if index > 0:
                            expression = ('CASE WHEN "%s" = \'%s\' THEN "%s"'
                                'END') % (dimension.internal_name,
                                    value, measure.internal_name)
                        else:
                            expression = "%s" % (measure.internal_name)

                name.append(measure.name)
                internal_name.append(measure.internal_name)
                name = '/'.join(name)
                internal_name = '_'.join(internal_name)
                to_create.append({
                        'execution': self.id,
                        'measure': measure.id,
                        'sequence': sequence,
                        'name': name,
                        'internal_name': internal_name,
                        'aggregate': measure.aggregate,
                        'expression': expression,
                        'ttype': measure.expression.ttype,
                        'related_model': related_model_id,
                        })
        if to_create:
            InternalMeasure.create(to_create)

    def update_measures(self, checker):
        logger = logging.getLogger(self.__name__)
        # Mapping from types to their null values
        types_null = defaultdict(int)
        types_null['bool'] = False
        types_null['char'] = "''"

        def query_inserts(table_name, measures, select_group, group,
                extra=None):
            """Inserts a group record"""
            cursor = Transaction().cursor

            babi_group = ""

            if group:
                babi_group = ",MAX('%s') as babi_group" % group
            local_measures = measures + babi_group

            if extra_data:
                local_measures += ", %s" % extra_data

            select_query = "SELECT %s FROM %s where babi_group IS NULL" % (
                local_measures, table_name)

            if select_group:
                select_query += " GROUP BY %s" % select_group

            if extra_data:
                select_query += ", %s" % extra_data

            fields = []
            for measure in local_measures.split(','):
                if ' as ' in measure:
                    measure = measure.split(' as ')[-1]
                measure = measure.replace('"', '').strip()
                fields.append(unaccent(measure))

            query = "INSERT INTO %s(%s)" % (table_name, ','.join(fields))

            if not cursor.has_returning():
                previous_id = 0
                cursor.execute('SELECT MAX(id) FROM %s' % table_name)
                row = cursor.fetchone()
                if row:
                    previous_id = row[0]
                query += select_query
                cursor.execute(query)
                cursor.execute('SELECT id from %s WHERE id > %s ' % (
                        table_name, previous_id))
            else:
                query += " %s RETURNING id" % select_query
                cursor.execute(query)
            return [x[0] for x in cursor.fetchall()]

        def update_parent(table_name, parent_id, group, group_by,
                group_by_types):
            sql_query = []
            for group_item in group_by:
                values = {
                    'item': group_item,
                    'def': types_null[group_by_types[group_item]],
                    'table': table_name,
                    'parent_id': parent_id,
                    }
                # Values should be coalesce to avoid parent errors when null
                group_query = ('Coalesce("%(item)s", %(def)s)=(select '
                    'Coalesce("%(item)s", %(def)s) from %(table)s '
                    'where id = %(parent_id)d)') % values
                sql_query.append(group_query)

            sql_query = u' AND '.join(sql_query)
            sql_query[:-5]

            query = """
                UPDATE """ + table_name + """ set parent=%s
                WHERE
                    parent IS NULL AND
                    id != %s AND
                    babi_group = '%s'
                    """ % (parent_id, parent_id, group)
            if sql_query:
                query += 'AND %s' % sql_query
            cursor.execute(query)

        pool = Pool()
        BIModel = pool.get(self.babi_model.model)

        if not self.internal_measures:
            return

        group_by_types = dict([(x.internal_name, x.expression.ttype)
                for x in self.report.dimensions if x.group_by])
        group_by = [x.internal_name for x in self.report.dimensions
            if x.group_by]

        extra_data = ",".join([x.internal_name for x in self.report.dimensions
            if not x.group_by])

        table_name = Pool().get(self.babi_model.model)._table
        cursor = Transaction().cursor

        group_by_iterator = group_by[:]

        aggregate = None
        current_group = None

        while group_by_iterator:
            checker.check()

            group = ['"%s"' % x for x in group_by_iterator]
            measures = ['%s("%s") as %s' % (
                        x.aggregate == 'count' and aggregate or x.aggregate,
                        x.internal_name,
                        x.internal_name,
                        ) for x in self.internal_measures] + group
            measures = ','.join(measures)
            group = ','.join(group)

            logger.info('SELECT table_name %s, measures %s, groups %s' % (
                    table_name, measures, group))

            child_group = current_group
            current_group = group_by[len(group_by_iterator) - 1]
            parent_ids = query_inserts(table_name, measures, group,
                current_group, extra_data)

            if group_by != group_by_iterator:
                for parent_id in parent_ids:
                    update_parent(table_name, parent_id, child_group,
                        group_by_iterator, group_by_types)

            child_group = current_group
            group_by_iterator.pop()
            extra_data = None

        # ROOT
        measures = ",".join(['%s("%s") as %s' % (
                    x.aggregate == 'count' and aggregate or x.aggregate,
                    x.internal_name, x.internal_name) for x in
                    self.internal_measures])
        group = None
        parent_id = query_inserts(table_name, measures, None, None)[0]
        # TODO: Translate '(all)'
        if group_by_types[group_by[0]] != 'many2one':
            cursor.execute("UPDATE " + table_name + " SET \"" + group_by[0] +
                "\"='" + '(all)' + "' WHERE id=%s" % parent_id)
        update_parent(table_name, parent_id, child_group, group_by_iterator,
            group_by_types)
        delete = 'DELETE FROM %s WHERE babi_group IS NULL' % (table_name)
        cursor.execute(delete + ' and id != %s ' % parent_id)
        # Update parent_left, parent_right
        BIModel._rebuild_tree('parent', None, 0)


class OpenExecutionSelect(ModelView):
    "Open Report Execution - Select Values"
    __name__ = 'babi.report.execution.open.select'

    # TODO: Add domain for validating report permisions
    report = fields.Many2One('babi.report', 'Report', required=True,
        states={
            'readonly': Bool(Eval('report_readonly')),
            }, depends=['report_readonly'])
    execution = fields.Many2One('babi.report.execution', 'Execution',
        required=True, domain=[
            ('report', '=', Eval('report')),
            ('state', '=', 'calculated'),
            ],
        states={
            'readonly': Bool(Eval('execution_readonly')),
            }, depends=['report', 'execution_readonly'])
    view_type = fields.Selection([
            ('tree', 'Tree'),
            ('list', 'List'),
            ], 'View type', required=True)

    report_readonly = fields.Boolean('Report Readonly')
    execution_readonly = fields.Boolean('Execution Readonly')

    @classmethod
    def default_get(cls, fields, with_rec_name=True):
        pool = Pool()
        Execution = pool.get('babi.report.execution')
        Menu = pool.get('ir.ui.menu')

        result = super(OpenExecutionSelect, cls).default_get(fields,
            with_rec_name)

        active_id = Transaction().context.get('active_id')
        model_name = Transaction().context.get('active_model')

        if model_name == 'babi.report.execution':
            execution = Execution(active_id)
            result.update({
                    'execution': execution.id,
                    'report': execution.report.id,
                    'view_type': 'tree',
                    'report_readonly': True,
                    'execution_readonly': True,
                    })
        elif model_name == 'ir.ui.menu':
            menu = Menu(active_id)
            result.update({
                    'report': menu.babi_report.id,
                    'view_type': 'tree',
                    'report_readonly': True,
                    })
            if menu.babi_type == 'filtered':
                result.update({
                        'execution_readonly': True,
                        })

        return result

    @depends('report')
    def on_change_report(self):
        if not self.report:
            return {'execution': None}
        return {}


class OpenExecutionFiltered(StateView):

    def __init__(self):
        buttons = [
                Button('Cancel', 'end', 'tryton-cancel'),
                Button('Open', 'create_execution', 'tryton-ok', True),
                ]
        super(OpenExecutionFiltered, self).__init__('babi.report', 0, buttons)

    def get_view(self):
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')
        Execution = pool.get('babi.report.execution')
        Parameter = pool.get('babi.filter.parameter')

        context = Transaction().context
        model = context.get('active_model')

        execution_definitions = Execution.fields_get(fields_names=[
                'report', 'report_model'])
        report_definition = execution_definitions['report']
        report_definition['required'] = True

        result = {}
        result['type'] = 'form'
        result['view_id'] = None
        result['model'] = 'babi.report.execution'
        result['field_childs'] = None
        fields = {}
        parameter2report = {}

        if model == 'ir.ui.menu':
            menu = Menu(context.get('active_id'))
            filter = menu.babi_report.filter
            report_definition['readonly'] = True
            parameters = Parameter.search([('filter', '=', filter)])
        else:
            # TODO: Report definition add domain for groups
            parameters = Parameter.search([
                        ('related_model.model', '=',
                            context.get('active_model'))],
                )
            report_definition['readonly'] = False
            reports = Report.search([('filter', 'in',
                        [p.filter for p in parameters])])

            for report in reports:
                key = report.filter.id
                if key in parameter2report:
                    parameter2report[key].append(report.id)
                else:
                    parameter2report[key] = [report.id]
            report_definition['domain'] = ['id', 'in', [r.id for r in reports]]

        if not parameters:
            self.raise_user_error('no_filter_parameter', model.model)

        xml = '<form string="Generate Filtered Report">\n'
        xml += '<label name="report"/>\n'
        xml += '<field name="report" colspan="3"/>\n'
        fields['report'] = report_definition
        encoder = PYSONEncoder()
        xml += '<group id="filters" string="Filters" colspan="4">\n'
        for parameter in parameters:
            name = parameter.name
            field_definition = {
                'loading': 'eager',
                'name': name,
                'string': name,
                'searchable': True,
                'create': True,
                'help': '',
                'context': {},
                'delete': True,
                'type': parameter.ttype,
                'select': False,
                'readonly': False,
                'required': True,
            }
            if parameter.ttype in['many2one', 'many2many']:
                field_definition['relation'] = parameter.related_model.model
            if parameter2report:
                field_definition['states'] = {
                    'invisible': Not(In(Eval('report', 0),
                            parameter2report[parameter.filter.id])),
                    'required': In(Eval('report', 0),
                        parameter2report[parameter.filter.id]),
                    }
            else:
                field_definition['states'] = {}
            # Copied from Model.fields_get
            for attr in ('states', 'domain', 'context', 'digits', 'size',
                    'add_remove', 'format'):
                if attr in field_definition:
                    field_definition[attr] = encoder.encode(
                        field_definition[attr])

            name = '%s_%d' % (name, parameter.id)
            if parameter.ttype == 'many2many':
                xml += '<field name="%s" colspan="4"/>\n' % (name)
            else:
                xml += '<label name="%s"/>\n' % (name)
                xml += '<field name="%s" colspan="3"/>\n' % (name)
            fields[name] = field_definition

        xml += '</group>\n'
        xml += '</form>\n'
        result['arch'] = xml
        result['fields'] = fields
        return result

    def get_defaults(self, wizard, state_name, fields):
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        Parameter = pool.get('babi.filter.parameter')
        context = Transaction().context
        model = context.get('active_model')

        defaults = {}
        if model == 'ir.ui.menu':
            menu = Menu(context.get('active_id'))
            defaults['report'] = menu.babi_report.id
        else:
            parameters = Parameter.search([
                        ('related_model.model', '=', model)]
                )
            for parameter in parameters:
                name = '%s_%d' % (parameter.name, parameter.id)
                defaults[name] = context.get('active_id')
        return defaults


class CustomDict(dict):

    def __getattr__(self, name):
        return {}

    def __setattr__(self, name, value):
        self[name] = value


class UpdateDataWizardStart(ModelView):
    "Update Data Wizard Start"
    __name__ = 'babi.update_data.wizard.start'


class UpdateDataWizardUpdated(ModelView):
    "Update Data Wizard Done"
    __name__ = 'babi.update_data.wizard.done'


class UpdateDataWizard(Wizard):
    "Update Data Wizard"
    __name__ = 'babi.update_data.wizard'


class OpenExecution(Wizard):
    'Open Report Execution'
    __name__ = 'babi.report.execution.open'

    start = StateTransition()
    update_start = StateView('babi.update_data.wizard.start',
        'babi.update_data_wizard_start_form_view', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Ok', 'update', 'tryton-ok', default=True),
            ])
    filtered = OpenExecutionFiltered()
    create_execution = StateTransition()
    select = StateView('babi.report.execution.open.select',
        'babi.open_execution_select_view_form', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Open', 'open_view', 'tryton-ok', True),
            ])
    open_view = StateAction('babi.open_execution_wizard')
    update = StateTransition()
    update_done = StateView('babi.update_data.wizard.done',
        'babi.update_data_wizard_done_form_view', [
            Button('Ok', 'end', 'tryton-ok', default=True),
            ])

    @classmethod
    def __setup__(cls):
        super(OpenExecution, cls).__setup__()
        cls._error_messages.update({
                'no_menus': ('No menus found for report %s. In order to view '
                    'it\'s data you must create menu entries.'),
                'no_report': ('No report found for current execution'),
                'no_execution': ('No execution found for current record. '
                    'Execute the update data wizard in order to create one.'),
                'no_filter_parameter': ('No parameter found for model %s.'
                    'In order to view filtered data, parameter should be'
                    ' defined on the report filter.'),
                })

    def __getattribute__(self, name):
        if name == 'filtered':
            if not hasattr(self, 'filter_values'):
                self.filter_values = CustomDict()
            name = 'filter_values'
        return super(OpenExecution, self).__getattribute__(name)

    def transition_start(self):
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        context = Transaction().context
        model_name = context.get('active_model')
        if model_name == 'babi.report.execution':
            return 'select'
        elif model_name == 'ir.ui.menu':
            menu = Menu(context.get('active_id'))
            if menu.babi_report.filter and \
                    len(menu.babi_report.filter.parameters) > 0:
                return 'filtered'
            if menu.babi_type == 'history':
                return 'select'
            if menu.babi_type == 'wizard':
                return 'update_start'
            return 'open_view'
        else:
            return 'filtered'

    def transition_create_execution(self):
        pool = Pool()
        Report = pool.get('babi.report')
        Execution = pool.get('babi.report.execution')

        report = self.filter_values.pop('report', None)
        if not report:
            self.raise_user_error('no_report_found')

        data = {}
        for key, value in self.filter_values.iteritems():
            # Fields has id of the field appendend, so it must be removed.
            new_key = '_'.join(key.split('_')[:-1])
            data[new_key] = value
        report = Report(report)
        execution = report.get_execution_data()
        data = json.dumps(self.filter_values, cls=JSONEncoder)
        execution['filter_values'] = data
        execution['filtered'] = True
        execution, = Execution.create([execution])
        Transaction().cursor.commit()
        Execution.calculate([execution])

        context = Transaction().context
        context.update({
                'filtered_execution': execution.id,
                })
        return 'open_view'

    def transition_update(self):
        pool = Pool()
        Menu = pool.get('ir.ui.menu')
        Report = pool.get('babi.report')

        menu = Menu(Transaction().context['active_id'])
        Report.calculate([menu.babi_report])
        return 'update_done'

    def do_open_view(self, action):
        pool = Pool()
        Action = pool.get('ir.action')
        ActionWindow = pool.get('ir.action.act_window')
        Menu = pool.get('ir.ui.menu')
        Execution = pool.get('babi.report.execution')

        transaction = Transaction()
        context = transaction.context

        model_name = context.get('active_model')
        if model_name == 'ir.ui.menu':
            menu = Menu(context.get('active_id'))
            if menu.babi_type == 'history':
                report = self.select.report
                execution = self.select.execution
                view_type = self.select.view_type
            else:
                report = menu.babi_report
                if 'filtered_execution' in context:
                    execution = Execution(context.get('filtered_execution'))
                else:
                    execution = report.last_execution
                view_type = menu.babi_type
        else:
            report = self.select.report
            execution = self.select.execution
            view_type = self.select.view_type

        if not execution:
            self.raise_user_error('no_execution', report.rec_name)

        with transaction.set_context(_datetime=execution.date):
            execution.validate_model()
        domain = [
            ('babi_report', '=', report.id),
            ]
        if view_type == 'tree':
            domain.append(('context', 'ilike', "%%babi_tree_view%%"))
        else:
            domain.append(('context', 'not ilike', "%%babi_tree_view%%"))
        try:
            action, = ActionWindow.search(domain, limit=1)
            action = Action.get_action_values(action.type, [action.id])[0]
        except ValueError:
            self.raise_user_error('no_menus', report.rec_name)
        action['res_model'] = execution.babi_model.model
        action['name'] = execution.rec_name
        return action, {}


class ReportGroup(ModelSQL):
    "Report - Group"
    __name__ = 'babi.report-res.group'

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    group = fields.Many2One('res.group', 'Group', required=True)

    @classmethod
    def __setup__(cls):
        super(ReportGroup, cls).__setup__()
        cls._sql_constraints += [
            ('report_group_uniq', 'UNIQUE (report,"group")',
                'Report and Group must be unique.'),
            ]


class DimensionMixin:

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence')
    name = fields.Char('Name', required=True, translate=True)
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    expression = fields.Many2One('babi.expression', 'Expression',
        required=True, domain=[
            ('model', '=', Eval('_parent_report', {}).get('model', 0)),
            ])
    group_by = fields.Boolean('Group By This Dimension')

    def get_internal_name(self, name):
        return 'babi_dimension_%d' % self.id

    @staticmethod
    def order_sequence(tables):
        table, _ = tables[None]
        return [table.sequence == None, table.sequence]

    @staticmethod
    def default_group_by():
        return True

    @depends('expression')
    def on_change_with_name(self):
        return self.expression.name if self.expression else None

    def get_dimension_data(self):
        return {
                    'name': self.name,
                    'internal_name': self.internal_name,
                    'expression': self.expression.expression,
                    'ttype': self.expression.ttype,
                    'related_model': (self.expression.related_model
                        and self.expression.related_model.model),
                }


class Dimension(ModelSQL, ModelView, DimensionMixin):
    "Dimension"
    __name__ = 'babi.dimension'
    _history = True

    @classmethod
    def __setup__(cls):
        super(Dimension, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))
        cls._sql_constraints += [
            ('report_and_name_unique', 'unique(report, name)',
                'Dimension name must be unique per report.'),
            ]

    @classmethod
    def update_order(cls, dimensions):
        Order = Pool().get('babi.order')
        cursor = Transaction().cursor
        dimension_ids = [x.id for x in dimensions if x.group_by]
        orders = Order.search([
                ('dimension', 'in', dimension_ids),
                ])
        existing = [x.dimension.id for x in orders]
        missing = set(dimension_ids) - set(existing)
        to_create = []
        for dimension in cls.browse(list(missing)):
            cursor.execute('SELECT MAX(sequence) FROM babi_order WHERE '
                'report=%s' % dimension.report.id)
            sequence = cursor.fetchone()[0] or 0
            to_create.append({
                    'report': dimension.report.id,
                    'sequence': sequence + 10,
                    'dimension': dimension.id,
                    })
        with Transaction().set_context({'babi_order_force': True}):
            Order.create(to_create)

    @classmethod
    def create(cls, values):
        dimensions = super(Dimension, cls).create(values)
        cls.update_order(dimensions)
        return dimensions

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        to_update = []
        for dimensions, _ in zip(actions, actions):
            to_update += dimensions
        cls.update_order(to_update)
        return super(Dimension, cls).write(*args)

    @classmethod
    def delete(cls, dimensions):
        Order = Pool().get('babi.order')
        orders = Order.search([
                ('dimension', 'in', [x.id for x in dimensions]),
                ])
        if orders:
            with Transaction().set_context({'babi_order_force': True}):
                Order.delete(orders)
        return super(Dimension, cls).delete(dimensions)


class DimensionColumn(ModelSQL, ModelView, DimensionMixin):
    "Column Dimension"
    __name__ = 'babi.dimension.column'
    _history = True

    @classmethod
    def __setup__(cls):
        super(DimensionColumn, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))


class Measure(ModelSQL, ModelView):
    "Measure"
    __name__ = 'babi.measure'
    _history = True

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence')
    name = fields.Char('Name', required=True, translate=True)
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    expression = fields.Many2One('babi.expression', 'Expression',
        required=True, domain=[
            ('model', '=', Eval('_parent_report', {}).get('model', 0)),
            ])
    aggregate = fields.Selection(AGGREGATE_TYPES, 'Aggregate', required=True)
    internal_measures = fields.One2Many('babi.internal.measure',
        'measure', 'Internal Measures')

    @classmethod
    def __setup__(cls):
        super(Measure, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))
        cls._sql_constraints += [
            ('report_and_name_unique', 'unique(report, name)',
                'Measure name must be unique per report.'),
            ]

    @staticmethod
    def order_sequence(tables):
        table, _ = tables[None]
        return [table.sequence == None, table.sequence]

    @staticmethod
    def default_aggregate():
        return 'sum'

    @depends('expression')
    def on_change_with_name(self):
        return self.expression.name if self.expression else None

    def get_internal_name(self, name):
        return 'babi_measure_%d' % (self.id)

    def get_measure_data(self):
        return {
                'name': self.name,
                'internal_name': self.internal_name,
                'expression': self.expression,
                'ttype': self.ttype,
                'related_model': (self.related_model and
                    self.related_model.model),
                }

    @classmethod
    def update_order(cls, measures):
        Order = Pool().get('babi.order')
        cursor = Transaction().cursor

        measure_ids = [x.id for x in measures]
        orders = Order.search([
                ('measure', 'in', measure_ids),
                ])
        existing_ids = [x.measure.id for x in orders]

        missing_ids = set(measure_ids) - set(existing_ids)
        to_create = []
        for measure in cls.browse(list(missing_ids)):
            cursor.execute('SELECT MAX(sequence) FROM babi_order WHERE '
                'report=%s' % measure.report.id)
            sequence = cursor.fetchone()[0] or 0
            to_create.append({
                    'report': measure.report.id,
                    'sequence': sequence + 1,
                    'measure': measure.id,
                    })
        with Transaction().set_context({'babi_order_force': True}):
            Order.create(to_create)

    @classmethod
    def create(cls, values):
        measures = super(Measure, cls).create(values)
        cls.update_order(measures)
        return measures

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        to_update = []
        for measures, _ in zip(actions, actions):
            to_update += measures
        cls.update_order(to_update)
        return super(Measure, cls).write(*args)

    @classmethod
    def delete(cls, measures):
        Order = Pool().get('babi.order')
        to_remove = []
        for measure in measures:
            orders = Order.search([
                    ('measure', '=', measure.id),
                    ])
            to_remove += orders
        if to_remove:
            with Transaction().set_context({'babi_order_force': True}):
                Order.delete(to_remove)
        return super(Measure, cls).delete(measures)


class InternalMeasure(ModelSQL, ModelView):
    "Internal Measure"
    __name__ = 'babi.internal.measure'

    execution = fields.Many2One('babi.report.execution', 'Report Execution',
        required=True, ondelete='CASCADE')
    measure = fields.Many2One('babi.measure', 'Measure', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence', required=True)
    name = fields.Char('Name', required=True)
    internal_name = fields.Char('Internal Name', required=True)
    expression = fields.Char('Expression')
    aggregate = fields.Selection(AGGREGATE_TYPES, 'Aggregate', required=True)
    ttype = fields.Selection(FIELD_TYPES, 'Field Type',
        required=True)
    related_model = fields.Many2One('ir.model', 'Related Model')

    @classmethod
    def __setup__(cls):
        super(InternalMeasure, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))

    @classmethod
    def __register__(cls, module_name):
        TableHandler = backend.get('TableHandler')
        cursor = Transaction().cursor
        super(InternalMeasure, cls).__register__(module_name)

        # Migration from 3.0: no more relation with reports.
        table = TableHandler(cursor, cls, module_name)
        if table.column_exist('report'):
            table.not_null_action('report', action='remove')

    def get_measure_data(self):
        return {
                'name': self.name,
                'internal_name': self.internal_name,
                'expression': self.expression,
                'ttype': self.ttype,
                'related_model': (self.related_model and
                    self.related_model.model),
                }


class Order(ModelSQL, ModelView):
    "Order"
    __name__ = 'babi.order'
    _history = True

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence', required=True)
    dimension = fields.Many2One('babi.dimension', 'Dimension', readonly=True)
    measure = fields.Many2One('babi.measure', 'Measure', readonly=True)
    order = fields.Selection([
            ('ASC', 'Ascending'),
            ('DESC', 'Descending'),
            ], 'Order', required=True)

    @staticmethod
    def default_order():
        return 'ASC'

    @classmethod
    def __setup__(cls):
        super(Order, cls).__setup__()
        cls._order.insert(0, ('sequence', 'ASC'))
        cls._error_messages.update({
                'cannot_create_order_entry': ('Order entries are created '
                    'automatically'),
                'cannot_remove_order_entry': ('Order entries are deleted '
                    'automatically'),
                })
        cls._sql_constraints += [
            ('report_and_dimension_unique', 'UNIQUE(report, dimension)',
                'Dimension must be unique per report.'),
            ('report_and_measure_unique', 'UNIQUE(report, measure)',
                'Measure must be unique per report.'),
            ('dimension_or_measure', 'CHECK((dimension IS NULL AND measure '
                'IS NOT NULL) OR (dimension IS NOT NULL AND measure IS NULL))',
                'Only dimension or measure can be set.'),
            ]

    @classmethod
    def create(cls, values):
        if not Transaction().context.get('babi_order_force'):
            cls.raise_user_error('cannot_create_order_entry')
        return super(Order, cls).create(values)

    @classmethod
    def delete(cls, orders):
        if not Transaction().context.get('babi_order_force'):
            cls.raise_user_error('cannot_remove_order_entry')
        return super(Order, cls).delete(orders)


class ActWindow:
    __name__ = 'ir.action.act_window'

    babi_report = fields.Many2One('babi.report', 'BABI Report')


class Menu:
    __name__ = 'ir.ui.menu'

    babi_report = fields.Many2One('babi.report', 'BABI Report')
    babi_type = fields.Selection([
            (None, ''),
            ('tree', 'Tree'),
            ('list', 'List'),
            ('history', 'History'),
            ('wizard', 'Wizard'),
            ], 'BABI Type', readonly=True)


class Keyword:
    __name__ = 'ir.action.keyword'

    babi_report = fields.Many2One('babi.report', 'BABI Report')
    babi_filter_parameter = fields.Many2One('babi.filter.parameter',
        'BABI Filter Parameter')


class Model(ModelSQL, ModelView):
    __name__ = 'ir.model'

    babi_enabled = fields.Boolean('BI Enabled', help='Check if you want '
        'this model to be available in Business Intelligence reports.')


class OpenChartStart(ModelView):
    "Open Chart Start"
    __name__ = 'babi.open_chart.start'

    graph_type = fields.Selection([
            ('vbar', 'Vertical Bars'),
            ('hbar', 'Horizontal Bars'),
            ('line', 'Line'),
            ('pie', 'Pie'),
            ], 'Graph', required=True, sort=False)
    interpolation = fields.Selection([
            ('linear', 'Linear'),
            ('constant-center', 'Constant Center'),
            ('constant-left', 'Constant Left'),
            ('constant-right', 'Constant Right'),
            ], 'Interpolation', states={
                'required': Eval('graph_type') == 'line',
                'invisible': Eval('graph_type') != 'line',
            }, sort=False)
    show_legend = fields.Boolean('Show Legend')
    report = fields.Many2One('babi.report', 'Report')
    execution = fields.Many2One('babi.report.execution', 'Execution')
    execution_date = fields.DateTime('Execution Time')
    dimension = fields.Many2One('babi.dimension', 'Dimension',
        required=True,
        domain=[
            ('report', '=', Eval('report')),
            ],
        context={
            '_datetime': Eval('execution_date'),
            },
        depends=['report', 'execution_date'])
    measures = fields.Many2Many('babi.internal.measure', None, None,
        'Measures', required=True,
        domain=[
            ('execution', '=', Eval('execution')),
            ],
        depends=['execution'])

    @classmethod
    def default_get(cls, fields, with_rec_name=True):
        pool = Pool()
        Execution = pool.get('babi.report.execution')
        model_name = Transaction().context.get('active_model')
        executions = Execution.search([
                ('babi_model.model', '=', model_name),
                ], limit=1)

        result = super(OpenChartStart, cls).default_get(fields, with_rec_name)
        if len(executions) != 1:
            return result
        execution, = executions
        report = execution.report
        Model = pool.get(model_name)
        active_id, = Transaction().context.get('active_ids')
        record = Model(active_id)

        with Transaction().set_context(_datetime=execution.create_date):
            fields = []
            found = False
            for x in report.dimensions:
                if found:
                    fields.append(x.id)
                    continue
                if x.internal_name == str(record.babi_group):
                    found = True

            if not fields:
                # If it was not found it means user clicked on 'root'
                # babi_group
                fields = [x.id for x in report.dimensions]

        return {
            'report': execution.report.id,
            'execution': execution.id,
            'execution_date': execution.date,
            'model': execution.babi_model.id,
            'dimension': fields[0] if fields else None,
            'measures': [x.id for x in execution.internal_measures],
            'graph_type': 'vbar',
            'show_legend': True,
            'interpolation': 'linear',
            }


class EmptyStateAction(StateAction):
    def __init__(self):
        super(EmptyStateAction, self).__init__(None)

    def get_action(self):
        return {}


class OpenChart(Wizard):
    "Open Chart"
    __name__ = 'babi.open_chart'
    start = StateView('babi.open_chart.start',
        'babi.open_chart_start_form_view', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Open', 'open_', 'tryton-ok', default=True),
            ])
    open_ = EmptyStateAction()

    @classmethod
    def __setup__(cls):
        super(OpenChart, cls).__setup__()
        cls._error_messages.update({
                'one_measure_in_pie_charts': ('Only one measure can be used '
                    'in pie charts.'),
                })

    def do_open_(self, action):
        pool = Pool()
        model_name = Transaction().context.get('active_model')
        Model = pool.get(model_name)

        active_ids = Transaction().context.get('active_ids')

        if len(self.start.measures) > 1 and self.start.graph_type == 'pie':
            self.raise_user_error('one_measure_in_pie_charts')

        group_name = self.start.dimension.internal_name
        records = Model.search([
                ('babi_group', '=', group_name),
                ('parent', 'child_of', active_ids),
                ])
        domain = [('id', 'in', [x.id for x in records])]
        domain = json.dumps(domain)
        context = {}
        context['view_type'] = 'graph'
        context['graph_type'] = self.start.graph_type
        context['dimension'] = self.start.dimension.id
        context['measures'] = [x.id for x in self.start.measures]
        context['legend'] = self.start.show_legend
        context['interpolation'] = self.start.interpolation
        context['model_name'] = model_name
        context = json.dumps(context)
        return {
            'id': -1,
            'name': '%s - %s Chart' % (self.start.execution.rec_name,
                self.start.dimension.rec_name),
            'model': model_name,
            'res_model': model_name,
            'type': 'ir.action.act_window',
            'pyson_domain': domain,
            'pyson_context': context,
            'pyson_order': '[]',
            'pyson_search_value': '[]',
            'domains': [],
            }, {}
