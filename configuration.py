from trytond.model import ModelSQL, ModelView, fields, ModelSingleton
from trytond.pyson import Eval

__all__ = ['Configuration']


class Configuration(ModelSingleton, ModelSQL, ModelView):
    'Business Intelligence Configuration'
    __name__ = 'babi.configuration'

    default_timeout = fields.Integer('Timeout (s)')
