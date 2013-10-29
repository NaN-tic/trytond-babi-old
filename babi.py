# encoding: utf-8
from datetime import datetime, timedelta
from StringIO import StringIO
import logging
import threading
import time
import unicodedata
import json

from trytond.wizard import Wizard, StateView, StateAction, Button
from trytond.model import ModelSQL, ModelView, fields
from trytond.pyson import Eval
from trytond.pool import Pool, PoolMeta
from trytond.transaction import Transaction
from trytond.tools import safe_eval
from trytond import backend
from babi_eval import babi_eval


__all__ = ['Filter', 'Expression', 'Report', 'ReportGroup', 'Dimension',
    'DimensionColumn', 'Measure', 'InternalMeasure', 'Order', 'ActWindow',
    'Menu', 'Keyword', 'Model', 'OpenChartStart', 'OpenChart']
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
    ]

SRC_CHARS = u""" .'"()/*-+?¿!&$[]{}@#`'^:;<>=~%,|\\"""
DST_CHARS = u"""__________________________________"""


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
            if self.current[key] >= len(self.values[key])-1:
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
    name = fields.Char('Name', required=True)
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)])
    model_name = fields.Function(fields.Char('Model Name',
            on_change_with=['model']), 'on_change_with_model_name')
    view_search = fields.Many2One('ir.ui.view_search', 'Search',
        domain=[('model', '=', Eval('model_name'))],
        depends=['model_name'], on_change_with=['model_name', 'domain'])
    domain = fields.Char('Domain', on_change_with=['view_search'])
    python_expression = fields.Char('Python Expression',
        help='The python expression introduced will be evaluated. If the '
        'result is True the record will be included, it will be discarded '
        'otherwise.')
    fields = fields.Function(fields.Many2Many('ir.model.field', None, None,
            'Model Fields', on_change_with=['model'], depends=['model']),
        'on_change_with_fields')

    def on_change_with_model_name(self, name=None):
        return self.model.model if self.model else None

    def on_change_with_fields(self, name=None):
        if not self.model:
            return []
        return [x.id for x in self.model.fields]

    def on_change_with_domain(self):
        return self.view_search.domain if self.view_search else None

    def on_change_with_view_search(self):
        ViewSearch = Pool().get('ir.ui.view_search')
        searches = ViewSearch.search([
                ('model', '=', self.model_name),
                ('domain', '=', self.domain),
                ])
        view_search = None
        if not searches:
            return None
        return searches[0].id


class Expression(ModelSQL, ModelView):
    "Expression"
    __name__ = 'babi.expression'

    name = fields.Char('Name', required=True)
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)])
    expression = fields.Char('Expression', required=True,
        help='Python expression that will return the value to be used.\n'
        'The expression can include the following variables:\n\n'
        '- "o": A reference to the current record being processed. For '
        ' example: "o.partner.name"\n'
        '\nAnd the following functions apply to dates and timestamps:\n\n'
        '- "y()": Returns the year (as a string)\n'
        '- "m()": Returns the month (as a string)\n'
        '- "w()": Returns the week (as a string)\n'
        '- "d()": Returns the day (as a string)\n'
        '- "ym()": Returns the year-month (as a string)\n'
        '- "ymd()": Returns the year-mont-day (as a string).\n'
        )
    ttype = fields.Selection(FIELD_TYPES, 'Field Type', required=True)
    related_model = fields.Many2One('ir.model', 'Related Model', states={
            'required': Eval('ttype') == 'many2one',
            'readonly': Eval('ttype') != 'many2one',
            }, depends=['ttype'])
    fields = fields.Function(fields.Many2Many('ir.model.field', None, None,
            'Model Fields', on_change_with=['model']), 'on_change_with_fields')

    def on_change_with_fields(self, name=None):
        if not self.model:
            return []
        return [x.id for x in self.model.fields]


class DynamicModel(ModelSQL, ModelView):
    @classmethod
    def __setup__(cls):
        super(DynamicModel, cls).__setup__()
        cls._error_messages.update({
                'report_not_exists': ('Report "%s" no longer exists or you do '
                    'not have the rights to access it.'),
                })

    @classmethod
    def fields_view_get(cls, view_id=None, view_type='form'):
        pool = Pool()
        Report = pool.get('babi.report')
        Dimension = pool.get('babi.dimension')
        InternalMeasure = pool.get('babi.internal.measure')

        reports = Report.search([
                ('babi_model.model', '=', cls.__name__),
                ])
        if not reports:
            cls.raise_user_error('report_not_exists', cls.__name__)
        context = Transaction().context
        report = reports[0]

        view_type = context.get('view_type', view_type)

        result = {}
        result['type'] = view_type
        result['view_id'] = view_id
        result['field_childs'] = None
        fields = []
        if view_type == 'tree':
            fields = [x.internal_name for x in report.dimensions +
                       report.internal_measures]
            fields.append('children')
            xml = '<tree string="%s" keyword_open="1">\n' % report.model.name
            for field in report.dimensions + report.internal_measures:
                widget = ''
                if hasattr(field, 'progressbar') and field.progressbar:
                    widget = 'widget="progressbar"'
                xml += '<field name="%s" %s/>\n' % (field.internal_name, widget)
            xml += '</tree>\n'
            result['arch'] = xml
            if context.get('babi_tree_view'):
                result['field_childs'] = 'children'
        elif view_type == 'form':
            fields = [x.internal_name for x in report.dimensions +
                       report.internal_measures]
            xml = '<form string="%s">\n' % report.model.name
            for field in report.dimensions + report.internal_measures:
                widget = ''
                if 'progressbar' in field and field.progressbar:
                    widget = 'widget="progressbar"'
                xml += '<field name="%s" %s/>\n' % (field.internal_name, widget)
            xml += '</form>\n'
            result['arch'] = xml
        elif view_type == 'graph':
            model_name = context.get('model_name')
            graph_type = context.get('graph_type')
            measure_ids = context.get('measures')
            legend = context.get('legend') and 1 or 0
            interpolation = context.get('interpolation', 'linear')
            dimension = Dimension(context.get('dimension'))

            x_xml = '<field name="%s"/>\n' % dimension.internal_name
            fields.append(dimension.internal_name)

            y_xml = ''
            for measure in InternalMeasure.browse(measure_ids):
                y_xml += '<field name="%s" interpolation="%s"/>\n' % (
                    measure.internal_name, interpolation)
                fields.append(measure.internal_name)

            xml = '''<?xml version="1.0"?>
                <graph string="%(graph_name)s" type="%(graph_type)s"
                    legend="%(legend)s">
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


class Report(ModelSQL, ModelView):
    "Report"
    __name__ = 'babi.report'

    name = fields.Char('Name', required=True, help='New virtual model name.')
    model = fields.Many2One('ir.model', 'Model', required=True,
        domain=[('babi_enabled', '=', True)], help='Model for data extraction')
    model_name = fields.Function(fields.Char('Model Name',
            on_change_with=['model']), 'on_change_with_model_name')
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    babi_model = fields.Many2One('ir.model', 'BI Model', readonly=True,
            help='Link to new model instance')
    filter = fields.Many2One('babi.filter', 'Filter',
        domain=[('model', '=', Eval('model'))], depends=['model'])
    dimensions = fields.One2Many('babi.dimension', 'report',
        'Dimensions', required=True)
    columns = fields.One2Many('babi.dimension.column', 'report',
        'Dimensions on Columns')
    measures = fields.One2Many('babi.measure', 'report', 'Measures',
        required=True)
    internal_measures = fields.One2Many('babi.internal.measure',
        'report', 'Internal Measures', readonly=True)
    order = fields.One2Many('babi.order', 'report', 'Order')
    groups = fields.Many2Many('babi.report-res.group', 'report', 'group',
        'Groups', help='User groups that will be able to see use this report.')
    parent_menu = fields.Many2One('ir.ui.menu', 'Parent Menu',
        required=True)
    menus = fields.One2Many('ir.ui.menu', 'babi_report', 'Menus',
        readonly=True)
    actions = fields.One2Many('ir.action.act_window', 'babi_report',
        'Actions', readonly=True)
    keywords = fields.One2Many('ir.action.keyword', 'babi_report', 'Keywords',
        readonly=True)
    last_update = fields.DateTime('Last Update', readonly=True,
        help='Date & time of the last update.')
    last_update_seconds = fields.Float('Last Update Duration (s)',
        readonly=True, help='Number of seconds the last update took.')
    timeout = fields.Integer('Timeout (s)', required=True, help='If report '
        'calculation should take more than the specified timeout (in seconds) '
        'the process will be stopped automatically.')

    @classmethod
    def __setup__(cls):
        super(Report, cls).__setup__()
        cls._error_messages.update({
                'no_dimensions': ('Report "%s" has no dimensions. At least one '
                    'is needed.'),
                'timeout_exception': ('Report calculation exceeded timeout '
                    'limit.')
                })
        cls._buttons.update({
                'calculate': {},
                'create_menus': {},
                'remove_menus': {},
                })

    @staticmethod
    def default_timeout():
        Config = Pool().get('babi.configuration')
        config = Config(1)
        return config.default_timeout

    def on_change_with_model_name(self, name=None):
        return self.model.model if self.model else None

    def get_internal_name(self, name):
        return '%s_%d' % (unaccent(self.name)[:10], self.id)

    def create_class(self, name, description, dimensions, measures, order):
        "Create class, and make instance"
        # TODO: Implement parent_left / parent_right
        body = {
            '__doc__': description,
            '__name__': name,
            # Used in get_rec_name()
            '_defaults': {},
            '_babi_dimensions': [x['internal_name'] for x in dimensions],
            }
        body.update(self.create_columns(name, dimensions + measures))
        return type(name, (DynamicModel, ), body)

    def create_columns(self, name, ffields):
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
            select=True)
        columns['children'] = fields.One2Many(name, 'parent', 'Children')
        columns['parent_left'] = fields.Integer('Parent Left', select=1)
        columns['parent_right'] = fields.Integer('Parent Right', select=1)
        return columns

    @classmethod
    def write(cls, reports, values):
        if 'name' in values:
            to_update = []
            for report in reports:
                if report.name != values['name']:
                    to_update.append(report)
            if to_update:
                cls.remove_menus(to_update)
                cls.remove_data(to_update)
        return super(Report, cls).write(reports, values)

    @classmethod
    def delete(cls, reports):
        cls.remove_menus(reports)
        cls.remove_data(reports)
        return super(Report, cls).delete(reports)

    @classmethod
    def copy(cls, reports, default=None):
        if default is None:
            default = {}
        default = default.copy()
        if not 'order' in default:
            default['order'] = None
        defaults['actions'] = None
        defaults['menus'] = None
        defaults['internal_measures'] = None
        defaults['babi_model'] = None
        if not 'name' in defaults:
            result = []
            for report in reports:
                default['name'] = '%s (2)' % report.name
                super(Report, cls).copy([report], default)
            return result
        return super(Report, cls).copy(reports, default)

    @classmethod
    def remove_data(cls, reports):
        pool = Pool()
        cursor = Transaction().cursor
        for report in reports:
            Model = pool.get(report.babi_model.model)
            if Model:
                cursor.execute("DROP TABLE IF EXISTS %s " % Model._table)

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

    def create_tree_view_menu(self):
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Menu = pool.get('ir.ui.menu')
        ActView = pool.get('ir.action.act_window.view')
        action = ActWindow()
        action.name = self.name
        action.res_model = self.babi_model.model
        action.domain = "[('parent', '=', None)]"
        action.babi_report = self
        action.groups = self.groups
        action.context = "{'babi_tree_view': True}"
        action.save()
        menu = Menu()
        menu.name = self.name
        menu.parent = self.parent_menu
        menu.babi_report = self
        menu.action = str(action)
        menu.icon = 'tryton-tree'
        menu.groups = self.groups
        menu.save()
        return menu.id

    def create_tree_view_action(self):
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Action = pool.get('ir.action.wizard')
        ModelData = pool.get('ir.model.data')
        Keyword = pool.get('ir.action.keyword')
        action = ActWindow()
        action.name = self.name
        action.res_model = self.babi_model.model
        action.babi_report = self
        action.groups = self.groups
        action.save()
        action = Action(ModelData.get_id('babi', 'open_chart_wizard'))
        keyword = Keyword()
        keyword.keyword = 'tree_open'
        keyword.model = '%s,-1' % self.babi_model.model
        keyword.action = action.action
        keyword.babi_report = self
        keyword.groups = self.groups
        keyword.save()

    def create_list_view_menu(self, parent):
        "Create list view and action to open"
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Menu = pool.get('ir.ui.menu')
        action = ActWindow()
        action.name = self.name
        action.res_model = self.babi_model.model
        action.babi_report = self
        action.groups = self.groups
        action.save()
        menu = Menu()
        menu.name = self.name
        menu.parent = parent
        menu.babi_report = self
        menu.action = str(action)
        menu.icon = 'tryton-list'
        menu.groups = self.groups
        menu.save()

    def create_update_wizard_menu(self, parent):
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        Menu = pool.get('ir.ui.menu')
        action = ActWindow()
        # TODO: Translate
        action.name = '%s Wizard' % self.name
        action.res_model = 'babi.update_data.wizard'
        action.context = "{'model': '%s'}" % self.babi_model.model
        action.groups = self.groups
        action.babi_report = self
        action.save()
        menu = Menu()
        # TODO: Translate
        menu.name = 'Update data %s' % self.name
        menu.parent = parent
        menu.babi_report = self
        #menu.action = ('ir.act.act_window', action.id)
        menu.action = str(action)
        menu.icon = 'tryton-executable'
        menu.groups = self.groups

    @classmethod
    def create_menus(cls, reports):
        """Regenerates all actions and menu entries"""
        for report in reports:
            report.validate_model()
            report.__class__.remove_menus([report])
            menu = report.create_tree_view_menu()
            report.create_tree_view_action()
            report.create_list_view_menu(menu)
            report.create_update_wizard_menu(menu)

    def validate_model(self):
        "makes model available on OpenERP and pool instance"
        pool = Pool()
        Model = pool.get('ir.model')
        ModelAccess = pool.get('ir.model.access')
        dimensions = []
        for dimension in self.dimensions:
            dimensions.append({
                    'name': dimension.name,
                    'internal_name': dimension.internal_name,
                    'expression': dimension.expression.expression,
                    'ttype': dimension.expression.ttype,
                    'related_model': (dimension.expression.related_model
                        and dimension.expression.related_model.model),
                    })
        measures = []
        for measure in self.internal_measures:
            measures.append({
                    'name': measure.name,
                    'internal_name': measure.internal_name,
                    'expression': measure.expression,
                    'ttype': measure.ttype,
                    'related_model': (measure.related_model and
                        measure.related_model.model),
                    })
        order = []
        for record in self.order:
            if record.dimension:
                field = record.dimension.internal_name
                order.append('"%s" %s' % (field, record.order))
            else:
                for measure in record.measure.internal_measures:
                    field = measure.internal_name
                    order.append('%s %s' % (field, record.order))

        Class = self.create_class(self.internal_name, self.name,
            dimensions, measures, order)
        Pool.register(Class, module='babi', type_='model')
        Class.__setup__()
        pool.add(Class, type='model')
        Class.__post_setup__()
        Class.__register__('babi')
        pool = Pool()
        model, = Model.search([
                ('model', '=', self.internal_name),
                ])
        self.babi_model = model
        self.save()
        to_create = []
        for group in self.groups:
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

    def update_data_background(self):
        Menu = Pool().get('ir.ui.menu')
        menus = Menu.search([
                ('report', 'in', ids),
                ])
        for menu in menus:
            if '[' in menu.name:
                name = menu.name.rpartition('[')[0].strip()
            else:
                name = menu.name
            # TODO: Make translatable
            menu.name = '%s [%s]' % (name, 'Updating')
            menu.save()
        self.update_data_inthread()

    def distinct_dimension_columns(self, records):
        if not self.columns:
            return []
        python_filter = None
        if report.filter and report.filter.python_expression:
            python_filter = report.filter.python_expression
        distincts = {}
        for record in records:
            if python_filter:
                if not babi_eval(python_filter, record, convert_none=False):
                    continue
            for dimension in report.columns:
                value = babi_eval(dimension.expression.expression, record)
                if not dimension.id in distincts:
                    distincts[dimension.id] = set()
                distincts[dimension.id].add(unicode(value))
        return distincts

    def update_internal_measures(self, records):
        InternalMeasure = Pool().get('babi.internal.measure')

        distincts = self.distinct_dimension_columns(records)
        if not distincts:
            distincts = {}
        for key in distincts.keys():
            # TODO: Make translatable
            distincts[key] = ['(all)'] + sorted(list(distincts[key]))

        columns = {}
        for column in self.columns:
            columns[column.id] = column

        InternalMeasure.delete(self.internal_measures)
        to_create = []
        sequence = 0
        for measure in self.measures:
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
                            expression = '(%s) if (%s) == """%s""" else 0' % (
                                expression, dimension.expression.expression,
                                value)

                name.append(measure.name)
                internal_name.append(measure.internal_name)
                name = '/'.join(name)
                internal_name = '_'.join(internal_name)
                to_create.append({
                        'report': measure.report.id,
                        'measure': measure.id,
                        'sequence': sequence,
                        'name': name,
                        'internal_name': internal_name,
                        'aggregate': measure.aggregate,
                        'expression': expression,
                        'ttype': measure.expression.ttype,
                        'related_model': related_model_id,
                        'progressbar': measure.progressbar,
                        })
        if to_create:
            InternalMeasure.create(to_create)

    def update_data_inthread(self, ids):
        netsvc.Logger().notifyChannel(self.__name__, netsvc.LOG_DEBUG,
                "Calling 'update_data()' in a thread")
        update_data_thread = threading.Thread(target=self.update_data_wrapper,
                args=(cr.dbname, uid, ids, context))
        update_data_thread.start()
        return True

    def update_data_wrapper(self, dbname, uid, ids, context):
        db, unused = pooler.get_db_and_pool(dbname)
        cr = db.cursor()
        try:
            self.update_data(cr, uid, ids, context)
            cr.commit()
        finally:
            cr.rollback()
            cr.close()

    @classmethod
    def calculate(cls, reports):
        for report in reports:
            report.single_update_data()

    def timeout_exception(self):
        self.raise_user_error('timeout_exception')

    def single_update_data(self):
        pool = Pool()
        Model = pool.get(self.model.model)
        cursor = Transaction().cursor

        self.validate_model()
        BIModel = pool.get(self.babi_model.model)
        Menu = pool.get('ir.ui.menu')

        checker = TimeoutChecker(self.timeout, self.timeout_exception)

        logger = logging.getLogger()
        logger.info('Updating Data of report: %s' % self.rec_name)
        update_start = time.time()
        model = 'ANY REPORT'
        if not self.dimensions:
            self.raise_user_error('no_dimensions', self.rec_name)

        # Drop table
        table_name = BIModel._table
        cursor.execute("DROP TABLE IF EXISTS %s " % table_name)

        domain = safe_eval(self.filter.domain if self.filter else '[]')
        start = datetime.today()
        logger.info('Starting search on %s' % model)
        records = Model.search(domain)
        logger.info('Search %s records (%s) in %s seconds'
           % (model, str(len(records)), datetime.today() - start))

        self.update_internal_measures(records)

        dimension_names = [x.internal_name for x in self.dimensions]
        dimension_expressions = [x.expression.expression for x in
            self.dimensions]
        measure_names = [x.internal_name for x in self.internal_measures]
        measure_expressions = [x.expression for x in self.internal_measures]

        self.validate_model()
        logger.info('Table Deleted')
        python_filter = None
        if self.filter and self.filter.python_expression:
            python_filter = self.filter.python_expression

        columns = (['create_date', 'create_uid'] + dimension_names +
            measure_names)
        columns = ['"%s"' % x for x in columns]
        # Some older versions of psycopg do not allow column names
        # to be of type unicode
        columns = [str(x) for x in columns]

        uid = Transaction().user

        #Process records
        offset = 2000
        index = 0
        while index * offset < len(records):
            checker.check()
            logger.info('Calculated %s,  %s records in %s seconds'
                % (model, index * offset, datetime.today() - start))

            to_create = ''
            # var o it's used on expression!!
            # Don't rename var
            chunk = records[index * offset:(index + 1) * offset]
            for record in chunk:
                if python_filter:
                    if not babi_eval(python_filter, record, convert_none=False):
                        continue
                vals = ['now()', str(uid)]
                vals += [unicode(babi_eval(x, record))
                    for x in dimension_expressions]
                vals += [unicode(babi_eval(x, record, convert_none='zero'))
                    for x in measure_expressions]
                record = u'|'.join(vals).replace('\n', ' ')
                to_create += record.encode('utf-8') + '\n'

            if to_create:
                data = StringIO(to_create)
                cursor.copy_from(data, table_name, sep='|', null='',
                    columns=columns)
            index += 1

        self.update_measures(checker)

        logger.info('Calc all %s records in %s seconds'
            % (model, datetime.today() - start))

        now = datetime.now()
        menus = Menu.search([
                ('babi_report', '=', self.id),
                ])
        for menu in menus:
            if '[' in menu.name:
                name = menu.name.rpartition('[')[0].strip()
            else:
                name = menu.name
            menu.name = '%s [%s]' % (name, now.strftime('%d/%m/%Y %H:%M:%S'))
            menu.save()
        self.last_update = now
        self.last_update_seconds = time.time() - update_start
        self.save()
        logger.info('End Update Data of report: %s' % self.rec_name)

    def update_measures(self, checker):
        logger = logging.getLogger(self.__name__)

        def query_select(table_name, measures, group):
            """Calculate data measures (group by)"""
            cursor = Transaction().cursor

            query = "SELECT %s FROM %s where parent IS NULL" % (
                measures, table_name)
            if group:
                query += " GROUP BY %s" % group
            cursor.execute(query)
            return cursor.dictfetchall()

        def query_insert(table_name, record, group):
            """Inserts a group record"""
            cursor = Transaction().cursor

            fields = [unaccent(x) for x in record.keys() + ['babi_group']]
            query = "INSERT INTO %s(%s)" % (table_name, ','.join(fields))
            query += "VALUES %s RETURNING id"
            mogrify_query = cursor.mogrify(query, (tuple(record.values() +
                        [group]),))
            cursor.execute(mogrify_query)
            return cursor.fetchone()[0]

        def query_inserts(table_name, records, group):
            """Inserts a group record"""
            cursor = Transaction().cursor

            if not records:
                return
            record = records[0]
            rfields = record.keys()
            fields = ['"%s"' % unaccent(x) for x in rfields + ['babi_group']]

            sql_values = ('%s,' * (len(rfields)+1))[:-1]
            sql_values = '(%s),' % sql_values
            sql_values = (sql_values * len(records))[:-1]

            query = "INSERT INTO %s(%s)" % (table_name, ','.join(fields))
            query += "VALUES %s RETURNING id" % sql_values

            # Ensure all record fields are in the same order
            values = []
            for record in records:
                for field in rfields:
                    values.append(record[field])
                values.append(group)
            cursor.execute(query, values)
            return [x[0] for x in cursor.fetchall()]


        def update_parent(table_name, parent_id, row, group, group_by):
            sql_query = []
            sql_values =  []
            for group_item in group_by:
                sql_query.append('"%s"=%%s' % group_item)
                sql_values.append(row[group_item])

            sql_query = u' AND '.join(sql_query) or 'TRUE'

            parameters = [parent_id, parent_id, group]
            if sql_values:
                parameters += sql_values
            cursor.execute(u"""
                UPDATE %s set parent=%%s
                WHERE
                    parent IS NULL AND
                    id != %%s AND
                    babi_group = %%s AND
                    %s""" % (table_name, sql_query),
                tuple(parameters))

        def delete_rows(table_name, parent_ids):
            cursor.execute("DELETE FROM %s WHERE id NOT IN %%s" % table_name, (
                    tuple(parent_ids),))

        if not self.internal_measures:
            return

        group_by_types = dict([(x.internal_name, x.expression.ttype)
                for x in self.dimensions if x.group_by])
        group_by = [x.internal_name for x in self.dimensions
            if x.group_by]

        table_name = Pool().get(self.babi_model.model)._table
        cursor = Transaction().cursor

        group_by_iterator = group_by[:]

        # TODO: Use on Parent levels on Count AGGREGATE!
        #       Find better solution
        #       substitude after first level computation COUNT-> SUM
        #       Because rows with data it's deleted and calculated rows
        #       contain data.
        aggregate = None
        current_group = None

        while group_by_iterator:
            checker.check()

            #Select
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

            rows = query_select(table_name, measures, group)

            child_group = current_group
            current_group = group_by[len(group_by_iterator) - 1]
            parent_ids = query_inserts(table_name, rows, current_group)

            if group_by == group_by_iterator:
                # TODO: Use on Parent levels on Count AGGREGATE!
                if parent_ids:
                    aggregate = 'SUM'
                    delete_rows(table_name, parent_ids)
            else:
                for x in xrange(len(rows)):
                    row = rows[x]
                    parent_id = parent_ids[x]
                    if group_by != group_by_iterator:
                        update_parent(table_name, parent_id, row,
                            child_group, group_by_iterator)

            child_group = current_group
            group_by_iterator.pop()

        #ROOT
        measures = ",".join(['%s("%s") as %s' % (
                    x.aggregate == 'count' and aggregate or x.aggregate,
                    x.internal_name, x.internal_name) for x in
                    self.internal_measures])
        group = None
        rows = query_select(table_name, measures, group)
        parent_id = query_inserts(table_name, [rows[0]], "root")[0]
        # TODO: Translate '(all)'
        if group_by_types[group_by[0]] != 'many2one':
            cursor.execute("UPDATE " + table_name + " SET \"" + group_by[0] +
                "\"='" + '(all)' + "' WHERE id=%s", (parent_id,))
        update_parent(table_name, parent_id, rows[0], child_group,
            group_by_iterator)
        # Update parent_left, parent_right


class ReportGroup(ModelSQL):
    "Report - Group"
    __name__ = 'babi.report-res.group'
    report = fields.Many2One('babi.report', 'Report', required=True)
    group = fields.Many2One('res.group', 'Group', required=True)

    @classmethod
    def __setup__(cls):
        super(ReportGroup, cls).__setup__()
        cls._sql_constraints +[
            ('report_group_uniq', 'UNIQUE(report, group)', 'Report and Group '
                'must be unique.'),
            ]


class DimensionMixin:
    _order = 'sequence ASC'

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence')
    name = fields.Char('Name', required=True, on_change_with=['expression'])
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    expression = fields.Many2One('babi.expression', 'Expression', required=True)
    group_by = fields.Boolean('Group By This Dimension')

    def get_internal_name(self, name):
        return '%s_%d' % (unaccent(self.name)[:10], self.id)

    @staticmethod
    def order_sequence(tables):
        table, _ = tables[None]
        return [table.sequence == None, table.sequence]

    @staticmethod
    def default_group_by():
        return True

    def on_change_with_name(self):
        return self.expression.name if self.expression else None


class Dimension(ModelSQL, ModelView, DimensionMixin):
    "Dimension"
    __name__ = 'babi.dimension'

    @classmethod
    def __setup__(cls):
        super(Dimension, cls).__setup__()
        cls._sql_constraints += [
            ('report_and_name_unique', 'unique(report, name)',
                'Dimension name must be unique per report.'),
            ]

    @classmethod
    def update_order(cls, dimensions):
        Order = Pool().get('babi.order')
        cursor = Transaction().cursor
        dimension_ids = [x.id for x in dimensions]
        orders = Order.search([
                ('dimension', 'in', dimension_ids),
                ])
        existing = [x.dimension.id for x in orders]
        missing = set(dimension_ids) - set(existing)
        to_create = []
        for dimension in cls.browse(list(missing)):
            cursor.execute('SELECT MAX(sequence) FROM babi_order WHERE '
                'report=%s', (dimension.report.id,))
            sequence = cursor.fetchone()[0] or 0
            to_create.append({
                    'report': dimension.report.id,
                    'sequence': sequence + 1,
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
    def write(cls, dimensions, values):
        cls.update_order(dimensions)
        return super(Dimension, cls).write(dimensions, values)

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


class Measure(ModelSQL, ModelView):
    "Measure"
    __name__ = 'babi.measure'
    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
    sequence = fields.Integer('Sequence')
    name = fields.Char('Name', required=True, on_change_with=['expression'])
    internal_name = fields.Function(fields.Char('Internal Name'),
        'get_internal_name')
    expression = fields.Many2One('babi.expression', 'Expression', required=True)
    aggregate = fields.Selection(AGGREGATE_TYPES, 'Aggregate', required=True)
    internal_measures = fields.One2Many('babi.internal.measure',
        'measure', 'Internal Measures')
    progressbar = fields.Boolean('Progress Bar',
        help='Display a progress bar instead of a number.')

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

    def on_change_with_name(self):
        return self.expression.name if self.expression else None

    def get_internal_name(self, name):
        return '%s_%d' % (unaccent(self.name)[:10], self.id)

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
            cursor.execute('SELECT MAX(sequence) FROM babi_order WHERE report=%s',
                (measure.report.id,))
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
    def write(cls, measures, values):
        cls.update_order(measures)
        return super(Measure, cls).write(measures, values)

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
    _order = 'sequence ASC'

    report = fields.Many2One('babi.report', 'Report', required=True,
        ondelete='CASCADE')
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
    progressbar = fields.Boolean('Progress Bar')


class Order(ModelSQL, ModelView):
    "Order"
    __name__ = 'babi.order'
    _order = 'sequence ASC'

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
        cls._error_messages.update({
                'cannot_create_order_entry': ('Order entries are created '
                    'automatically'),
                'cannot_remove_order_entry': ('Order entries are deleted '
                    'automatically'),
                })
        cls._sql_constraints += [
            ('report_and_dimension_unique','UNIQUE(report, dimension)',
                'Dimension must be unique per report.'),
            ('report_and_measure_unique','UNIQUE(report, measure)',
                'Measure must be unique per report.'),
            ('dimension_or_measure', 'CHECK((dimension IS NULL AND measure '
                'IS NOT NULL) OR (dimension IS NOT NULL AND measure IS NULL))',
                'Only dimension or measure can be set.'),
            ]

    @classmethod
    def create(cls, values):
        if not Transaction().context.get('babi_order_force'):
            self.raise_user_error('cannot_create_order_entry')
        return super(Order, cls).create(values)

    @classmethod
    def delete(cls, orders):
        if not Transaction().context.get('babi_order_force'):
            self.raise_user_error('cannot_remove_order_entry')
        return super(Order, cls).delete(orders)


class ActWindow:
    __name__ = 'ir.action.act_window'
    babi_report = fields.Many2One('babi.report', 'BABI Report')


class Menu:
    __name__ = 'ir.ui.menu'
    babi_report = fields.Many2One('babi.report', 'BABI Report')


class Keyword:
    __name__ = 'ir.action.keyword'
    babi_report = fields.Many2One('babi.report', 'BABI Report')


class Model(ModelSQL, ModelView):
    __name__ = 'ir.model'
    babi_enabled = fields.Boolean('BI Enabled', help='Check if you want '
        'this model to be available in Business Intelligence reports.')

    # TODO: Consider using something smarter than __post_setup__()
    # for the last model of the module
    @classmethod
    def __post_setup__7777(cls):
        super(Model, cls).__post_setup__()
        Report = Pool().get('babi.report')
        cursor = Transaction().cursor
        TableHandler = backend.get('TableHandler')
        if TableHandler.table_exist(cursor, Report._table):
            # If new fields were added to babi.report and we're upgrading
            # the module, search will probably fail with a
            # psycopg2.ProgrammingError
            reports = Report.search([])
            for report in reports:
                report.validate_model()


class UpdateDataWizard():
    """ Wizard to Recalculate data """
    _name = 'bi.update_data.wizard'
    _description = 'Recalculate Data'

    def action_accept(self, cr, uid, ids, context=None):
        menu_obj = self.pool.get('ir.ui.menu')
        menu = menu_obj.browse(cr, uid, context.get('active_id'), context)

        report_obj = self.pool.get('bi.report')
        report_obj.update_data_background(cr, uid, [menu.report_id.id], context)

        return {}

    def action_cancel(self, cr, uid, ids, context=None):
        return {}


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
    valid_dimensions = fields.Many2Many('babi.dimension', None, None,
        'Valid Dimensions')
    dimension = fields.Many2One('babi.dimension','Dimension',
        required=True)
    measures = fields.Many2Many('babi.internal.measure', None, None, 'Measures',
        required=True)

    @classmethod
    def default_get(cls, fields, with_rec_name=True):
        pool = Pool()
        Report = pool.get('babi.report')
        model_name = Transaction().context.get('active_model')
        reports = Report.search([
                ('babi_model.model', '=', model_name),
                ])

        result = super(OpenChartStart, cls).default_get(fields, with_rec_name)
        if len(reports) != 1:
            return result

        report = reports[0]
        Model = pool.get(model_name)
        active_id, = Transaction().context.get('active_ids')
        record = Model(active_id)

        fields = []
        found = False
        for x in report.dimensions:
            if found:
                fields.append(x.id)
                continue
            if x.internal_name == str(record.babi_group):
                found = True

        if not fields:
            # If it was not found it means user clicked on 'root' babi_group
            fields = [x.id for x in report.dimensions]

        return {
            'model': report.babi_model.id,
            'dimension': fields[0] if fields else None,
            'valid_dimensions': fields,
            'measures': [x.id for x in report.internal_measures],
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
                'one_measure_in_pie_charts': ('Only one measure can be used in '
                    'pie charts.'),
                })

    def do_open_(self, action):
        pool = Pool()
        ActWindow = pool.get('ir.action.act_window')
        model_name = Transaction().context.get('active_model')
        Model = pool.get(model_name)

        active_ids = Transaction().context.get('active_ids')

        if len(self.start.measures) > 1 and self.start.graph_type == 'pie':
            self.raise_user_error('one_measure_in_pie_charts')

        actions = ActWindow.search([
                ('res_model', '=', model_name),
                ])
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
            'name': 'Open Chart',
            'model': model_name,
            'res_model': model_name,
            'type': 'ir.action.act_window',
            'pyson_domain': domain,
            'pyson_context': context,
            'pyson_order': '[]',
            'pyson_search_value': '[]',
            'domains': [],
            #'act_window_views': [],
            #'keywords': [],
            #'id': 7777,
            #'icon.rec_name': False,
            #'usage': None,
            #'pyson_domain': '[]',
            #'views': [],
            #'auto_refresh': 0,
            #'action': 9999,
            #'groups': [x.id for x in action.groups],
            #'active': True,
            #'window_name': True,
            #'icon': None,
            #'rec_name': 'MANOLITA',
            #'limit': 0,
            #'act_window_domains': [],
            #'domains': [],
            }, {}
