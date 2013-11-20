# -*- coding: utf-8 -*-
# Copyright (c) 2013, Cédric Krier
# Copyright (c) 2013, B2CK
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from celery import Task


class TrytonTask(Task):
    abstract = True

    def __call__(self, *args, **kwargs):
        database = self.app.conf.TRYTON_DATABASE
        config_file = self.app.conf.get('TRYTON_CONFIG')

        from trytond.config import CONFIG
        if CONFIG.configfile != config_file:
            CONFIG.update_etc(config_file)
            CONFIG.set_timezone()
        from trytond.pool import Pool
        from trytond.transaction import Transaction
        from trytond.cache import Cache
        from trytond import backend
        DatabaseOperationalError = backend.get('DatabaseOperationalError')

        Cache.clean(database)
        if database not in Pool.database_list():
            with Transaction().start(database, 0, readonly=True):
                Pool(database).init()
        with Transaction().start(database, 0) as transaction:
            try:
                result = super(TrytonTask, self).__call__(*args, **kwargs)
                transaction.cursor.commit()
            except DatabaseOperationalError, exc:
                transaction.cursor.rollback()
                raise self.retry(args=args, kwargs=kwargs, exc=exc,
                    countdown=0, max_retries=int(CONFIG['retry']))
        Cache.resets(database)
        return result
