# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from trytond.model import fields
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Eval, Bool
from trytond.transaction import Transaction

__all__ = ['Cron']


class Cron:
    __metaclass__ = PoolMeta
    __name__ = "ir.cron"
    babi_report = fields.Many2One('babi.report', 'Babi Report')

    @classmethod
    def __setup__(cls):
        super(Cron, cls).__setup__()
        invisible = Bool(Eval('babi_report'))
        state = {'invisible': invisible}
        for field_name in ('model', 'function', 'args'):
            field = getattr(cls, field_name)
            states = field.states
            if 'invisible' not in states:
                states.update(state)
            else:
                states.update({
                        'invisible': states['invisible'] | invisible,
                        })
            if 'babi_report' not in field.depends:
                field.depends.append('babi_report')

    @classmethod
    def create(cls, vlist):
        for vals in vlist:
            if 'babi_report' in vals:
                vals['args'] = '(%s,)' % vals['babi_report']
        return super(Cron, cls).create(vlist)

    @classmethod
    def default_get(cls, fields, with_rec_name=True):
        User = Pool().get('res.user')
        res = super(Cron, cls).default_get(fields, with_rec_name)
        cron_user, = User.search([
                ('active', '=', False),
                ('login', '=', 'user_cron_trigger'),
                ])
        admin_user, = User.search([('login', '=', 'admin')])
        context = Transaction().context
        if context.get('babi_report', False):
            res['user'] = cron_user.id
            res['request_user'] = admin_user.id
            res['interval_type'] = 'days'
            res['repeat_missed'] = False
            res['model'] = 'babi.report'
            res['function'] = 'calculate_babi_report'
        return res
