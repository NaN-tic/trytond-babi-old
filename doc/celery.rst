Introduction
============

Babi is fully integrated with Celery_. With this integration you can execute
report calculations in background (on a Celery worker).

Configuration
=============

This is an example of a simple configuration and only recomended for testing
envirorments. For more information, please refer to the `Celery Documentaion`_

Installing Celery
-----------------

You must have the celery package installed on your system (or virtualenv). It
can be installed with the following command:

pip install celery

Celery needs a message queue in order to work. The default one is RabbitMQ. On
Debian (and derivates) you can install it with the following command:

apt-get install rabbitmq-server

Launching workers
-----------------

In order to get your reports calculated in background, you must execute a
worker process and especify to it the available task.

In order to to this you must execute the following command from the module/babi
directory:

celeryd --app=tasks

You can specify the database and the trytond.conf fiel used by the workers on
the celeryconfig.py file

.. _Celery: http://www.celeryproject.org
.. _Celery Documentation: http://docs.celeryproject.org/en/latest/index.html
