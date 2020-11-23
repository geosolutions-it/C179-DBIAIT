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
3. Run SQL script from `database/functions.sql` on the database
4. Install application requirements.

note: if some errors occurs with windows installation of the `python-ldap`, please install the library via whl file and then re-run the installation of the dependencies

# System variables

## Application configuration 
This section contains information about the folder used by the authorized users (via FTP)
to configure the DBIAIT application in terms of import/export options and geopackages

**FTP_FOLDER**: folder representing the root of the FTP for the users

**IMPORT_FOLDER**: folder containing geopackages and the subfolder **config**
with the configuration files for the import process:
- domains.csv
- layers.json

**EXPORT_FOLDER**: folder containing the subfolder **config**
with the configuration files for the export process:
- xls.json
- shp.json
- NETSIC_SEED.xlsx

**TEMP_EXPORT_DIR**: folder containing temporary data and folders created by each export process 
```
/srv/ftp
└───import
|   └───config
|   |   |   domains.csv
|   |   |   layers.json
|   |   *.gpkg
└───export
|   └───config
|   |   |   xls.json
|   |   |   shp.json
|   |   |   NETSIC_SEED.xlsx
```

## QGIS

**QGIS_PATH**: path to the QGIS application

## Database

**DATABASE_HOST**: PostgreSQL host

**DATABASE_PORT**: PostgreSQL port (default is 5432)

**DATABASE_NAME**: name of the database

**DATABASE_USER**: username to connect to the database

**DATABASE_PASSWORD**: password to connect to the database

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


# Export
## Starting export task
There are two possibilities to manually start export task, but they both require django environment:
1. synchronously (for local testing)
``` python
from app.scheduler.tasks import ExportTask
from django.contrib.auth import get_user_model

user = get_user_model().objects.get(pk=<requesting_user_id>)
ExportTask.perform(ExportTask.pre_send(user))
```

2. asynchronously (with Dramatiq)
``` python
from app.scheduler.tasks import ExportTask
from django.contrib.auth import get_user_model

user = get_user_model().objects.get(pk=<requesting_user_id>)
ExportTask.send(ExportTask.pre_send(user))
```

**Warning**: Please note that a simple change of method will result in sync/async execution

**Warning**: Production trigger should never execute export synchronously!

## *.xlsx file export

### *.xlsx export file configuration

To export excel file it is required to set the configuration file, which defines sources and transformations,
which should be applied to fetched data.
To point at a certain file EXPORT_CONF_FILE environment variable should be set.
By default EXPORT_CONF_FILE points at `export/config/xls_config.json` from the main project directory.

EXPORT_CONF_FILE should contain a single object, with a `xls_sheet_configs` key containing a list of paths to the configs of *.xlsx sheets.
Paths may be eiter absolute, or relative to the EXPORT_CONF_FILE. 

Example EXPORT_CONF_FILE contents:
``` json
{
   "xls_sheet_configs": [
      "sheet_configs/config_sollev_pompe.json",
      "sheet_configs/config_potabilizzatori.json",
      "sheet_configs/config_sorgenti.json",
      "sheet_configs/config_pozzi.json",
      "sheet_configs/config_sollevamenti.json",
      "sheet_configs/config_scaricatori.json",
      "sheet_configs/config_pozzi_pompe.json"
   ]
}
```

Sheet config files should consist of **a single object**, with keys defined by the project documentation (see [documentation](https://docs.google.com/document/d/1votggD0JSr9v_pUVgbsAYOc1z_orJTHSEG1hO-TUB5k/edit#))

### validating config file
To validate the configuration file along with all sheet configs, in django environment, one can use the following script:
``` python
from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig
export_config = ExportConfig()
```

Creating an instance of ExportConfig, parses the config file and sheet config files, and validates the schema against the pattern.

In order to parse a single sheet configuration, the following may be done:
``` python 
from app.scheduler.tasks.export_definitions.config_scraper import export_config_schema

sheet_config_path = "<you_config_file_path>"

with open(sheet_config_path, 'r') as scp:
    sheet_config = json.load(scp)

export_config_schema.validate(sheet_config)
```