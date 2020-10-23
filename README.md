# C179-DBIAIT
DBIAIT project for C179

# Python QGIS environment

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


## starting the scheduler
The scheduler requires a task queue to be configured. By default RabbitMQ is used, but Redis can also be used (note: Redis requires change in settings.py file).
1. start RabbitMQ (may be dockerized version)
``` shell
docker run -d -p 15672:15672 -p 5672:5672 -p 5671:5671 --hostname my-rabbitmq rabbitmq:3-management
```
2. in a terminal with environment variables exported run (and don't kill!):
``` shell
python manage.py rundramatiq
```
3. in a separate terminal run Django server
``` shell
python manage.py runserver 0.0.0.0:8000
```
