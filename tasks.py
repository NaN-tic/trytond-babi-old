from trytond_celery import TrytonTask
from celery import Celery
from trytond.pool import Pool
from trytond.transaction import Transaction

celery = Celery()
celery.config_from_object('trytond.modules.babi.celeryconfig')


@celery.task(base=TrytonTask)
def calculate_execution(execution_id, user_id=None):
    """ Calculates data for exectuion passed by parameters"""
    pool = Pool()
    User = pool.get('res.user')
    Execution = pool.get('babi.report.execution')
    if not user_id:
        user,  = User.search([
                ('login', '=', 'admin'),
            ])
        user_id = user.id
    with Transaction().set_user(user_id), Transaction().set_context(
            User.get_preferences(context_only=True)):
        execution = Execution(execution_id)
        Execution.calculate([execution])
