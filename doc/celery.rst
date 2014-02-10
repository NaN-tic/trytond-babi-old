Introduction
============

Babi is fully integrated with Celery_. With this integration you can execute
report calculations in background (on a Celery worker).

Configuration
=============

This is an example of a simple configuration and only recomended for testing
envirorments. For more information, please refer to the `Celery Documentation`_

Installing Celery
-----------------

You must have the celery package installed on your system (or virtualenv). It
can be installed with the following command::

    pip install celery

Celery needs a message queue in order to work. The default one is RabbitMQ. On
Debian (and derivates) you can install it with the following command::

    apt-get install rabbitmq-server

Launching workers
-----------------

The trytond server will launch workers for each database when opening the pool.
This workers use the config defined in celeryconfig.py file from babi directory.

In order to add more workers on a database you must execute the following
command from the modules/babi directory::

    celery worker --app=tasks --queue=database --config=celeryconfig

The default config file uses TRYTON_DATABASE and TRYTON_CONFIG
environment variables, so you must define it otherwise the report executions
will fail.

To be able to have multiple workers on the same host with different database,
the database name is used as queue name.


.. _Celery: http://www.celeryproject.org
.. _Celery Documentation: http://docs.celeryproject.org/en/latest/index.html
