# C179-DBIAIT
DBIAIT project for C179

# Python QGIS environment

## QGIS
Version is 3.16.0 Hannover

WARNING: Importing QGis forces the use of the default, global python interpreter.

## Python
Version is 3.6.9

## Windows
PYTHONUNBUFFERED=1;
OSGEO4W_ROOT=C:\OSGeo4W64;
PATH=%OSGEO4W_ROOT%\apps\qgis\bin\;%OSGEO4W_ROOT%\apps\Python37\;%OSGEO4W_ROOT%\apps\Python37\Scripts\;%OSGEO4W_ROOT%\apps\qt5\bin\;%OSGEO4W_ROOT%\apps\Python37\Scripts\;%OSGEO4W_ROOT%\bin\;C:\Windows\;C:\Windows\system32\WBem\;%OSGEO4W_ROOT%\apps\qgis\python\plugins;
PYTHONHOME=%OSGEO4W_ROOT%\apps\Python37;
QGIS_PREFIX_PATH=%OSGEO4W_ROOT%\apps\qgis;
QT_PLUGIN_PATH=%OSGEO4W_ROOT%\apps\qgis\qtplugins\;%OSGEO4W_ROOT%\apps\qt5\plugins;
PYTHONPATH=%OSGEO4W_ROOT%\apps\qgis\python

## Linux
export PYTHONPATH=/usr/share/qgis/python/:/usr/share/qgis/python/plugins:/usr/lib/python3/dist-packages/qgis:/usr/share/qgis/python/qgis

# Prerequisites

1. Install Postgres 10.6 with PostGis 3.0.1
2. Run SQL script from `database/install.sql` on the database
3. Install application requirements.

note: if some errors occurs with windows installation of the `python-ldap`, please install the library via whl file and then re-run the installation of the dependencies

# Scheduling system

## Starting the scheduler
The scheduler requires a task queue to be configured. By default RabbitMQ is used, but Redis can also be used (note: Redis requires change in settings.py file).
1. start RabbitMQ (may be dockerized version)
``` shell
docker run -d -p 15672:15672 -p 5672:5672 -p 5671:5671 --hostname my-rabbitmq rabbitmq:3-management
```
2. in a terminal with environment variables exported run (and don't kill!):
``` shell
python manage.py global_interpreter_rundramatiq
```
3. in a separate terminal run Django server
``` shell
python manage.py runserver 0.0.0.0:8000
```

Note: On every change of your Dramatiq tasks it is required to restart `dramatiq` process, otherwise your changes may not be included in the next background task execution


## Ordering Task execution
Background Task execution should be ordered similarly to Dramatiq's `GenericActor`, with the only difference that Task.send() method takes a single argument - ORM Task ID, which is created by pre_send() class method.

``` python
Task.send(Task.pre_send(*args, **kwargs))
```

In case ordering Task execution violates querying criteria, the `app.scheduler.exceptions.QueuingCriteriaViolated` will be raised.


## Adding new Tasks

All tasks should inherit from `app.scheduler.tasks.base_task.BaseTask` class, and they should always define the following methods and attributes:
- name: str - class property, the name of the Task. It is mainly useful to distinct process tasks. For other tasks, the convention is to use task type in lower case.
- task_type: str - class property, the type of the Task (e.g. `PROCESS`, `IMPORT`, `FREEZE`, `EXPORT`) 
- pre_send(): method - **class** method, determining whether the task may be queued and creating the Task ORM model instance for reporting task's status. This function should return ORM model Task's ID or raise `scheduler.exceptions.QueuingCriteriaViolated`.
- execute(): method - method containing all logic executed by the background executor. As the arguments it receives ORM Task's ID (`task_id`) along with args and kwargs defined in the ORM Task instance, which was created by `pre_send()` method. 
