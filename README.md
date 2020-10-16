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
