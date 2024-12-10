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
Install qgis
```
sudo apt-get update && sudo apt-get install qgis -y
```

Most probably the following library are required by the environment
(for more information visit [QGIS Documentation](https://www.qgis.org/resources/installation-guide/#debian--ubuntu))

```bash
sudo apt-get install gcc python3-dev libxml2-dev libxslt1-dev zlib1g-dev libsasl2-dev libldap2-dev build-essential libssl-dev libffi-dev libmysqlclient-dev libjpeg-dev libpq-dev libjpeg8-dev liblcms2-dev libblas-dev libatlas-base-dev
```

QGIS use the default python3 interpreter, discover the interpreter to install the requirements:

discover python
```
which python3
```

install the requirements:

```
/usr/bin/python3 -m pip install -r requirements.txt
```

set the global interpreter as VSCode python env

- CRTL+SHIFT+P
- search for "Python interpreter"
- Select the global python interpreter, be sure that the path retrieved before match with the one listed by vscode

Follow an example of launch.json

```
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: Django",
            "type": "debugpy",
            "request": "launch",
            "args": [
                "runserver"
            ],
            "django": true,
            "autoStartBrowser": false,
            "envFile": "${workspaceFolder}/.env",
            "program": "${workspaceFolder}/manage.py"
        }
    ]
}
```


Example of .env file:

```
PYTHONPATH=/usr/share/qgis/python/plugins
DATABASE_PASSWORD="geonode"
DATABASE_USER="geonode"
DATABASE_NAME="dbiait"
IMPORT_FOLDER="C:\Users\user\Documents\Projects\C179-DBIAIT\import"
URL_PATH_PREFIX=
LDAP_PASSWORD=password
#GDAL_VERSION=3.1.4
FREEZE_FEATURE_TOGGLE=True
```


# deprecated
```
export PYTHONPATH=/usr/share/qgis/python/:/usr/share/qgis/python/plugins:/usr/lib/python3/dist-packages/qgis:/usr/share/qgis/python/qgis
```
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

**EXPORT_FOLDER**: folder containing generated *.zip archives

**EXPORT_CONF_DIR**: (default: `EXPORT_FOLDER/config`) folder containing configuration subdirectories for the 'Analysis' and 'Freeze' schemas export.
Configuration for 'Analysis' schema is taken from `EXPORT_CONF_DIR/current/` directory and configurations for 'Freeze'
schemas are taken from `EXPORT_CONF_DIR/<year>/` directories.
Each configuration subdirectory needs to define the following:
- xls.json
- shp.json
- NETSIC_SEED.xlsx
- sheet_configs

```
/srv/ftp
└───import
|   └───config
|   |   |   domains.csv
|   |   |   layers.json
|   |   *.gpkg
└───export
|   └───config
|   |   └───current
|   |   |   └───sheet_configs
|   |   |   |   |   <sheet_1_config>.json
|   |   |   |   |   ...
|   |   |   |   |   <sheet_n_config>.json
|   |   |   |   xls.json
|   |   |   |   shp.json
|   |   |   |   NETSIC_SEED.xlsx
# not yet supported, prepared for FREEZE
|   |   └───<year>
|   |   |   |   xls.json
|   |   |   |   shp.json
|   |   |   |   NETSIC_SEED.xlsx
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
docker run -d -p 15672:15672 -p 5672:5672 -p 5671:5671 --hostname my-rabbitmq rabbitmq:3.6.10-management
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

## Export configuration

To start the export process, it is required to set the EXPORT_CONF_DIR environment variable, which points
at a directory meeting criteria of Application configuration.

For 'analysis' schema export configuration is taken from `current` subdirectory of EXPORT_CONF_DIR, and
for 'freeze' schema export is taken from `<year>` subdirectory, depending on export's year setting.

**Note**: In the production environment `<year>` directories should not be updated manually - they will
be copied from `current` when the freeze task is executed, to ensure consistency between stored historical
data and it's export configuration. 

- `xls.json` the file defining the sheet configurations list, as a relative path ot the `xls.json` file

    Example `xls.json` contents:
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

- `sheet_configs` directory (or similar, provided in `xls.json` file) containing sheet configuration
files for each Excel sheet. Sheet config files should consist of **a single object**, with keys defined
by the project documentation
(see [documentation](https://docs.google.com/document/d/1votggD0JSr9v_pUVgbsAYOc1z_orJTHSEG1hO-TUB5k/edit#)).

- `NETSIC_SEED.xlsx` seed Excel file to be used as a template for the export process, containing
defined sheets and their headers

- `shp.json` shapefile configuration file

 

## Validating config file

### Validating *.xls config file
To validate the configuration file along with all sheet configs, in django environment, one can use the following script:
``` python
from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig
export_config = ExportConfig()
```

Creating an instance of ExportConfig, parses the config file and sheet config files, and validates the schema against the pattern.

In order to parse a single sheet configuration, the following may be done:
``` python 
from app.scheduler.tasks.export_definitions.config_scraper import export_config_schema
import json

sheet_config_path = "<you_config_file_path>"

with open(sheet_config_path, 'r') as scp:
    sheet_config = json.load(scp)

export_config_schema.validate(sheet_config)
```

### Run test
All the tests runs under django environment.

To run them, use the following command:

```
Normal:
python manage.py test --settings=app.test_settings

Verbosity Mode:
python manage.py test --settings=app.test_settings -v 2

```


[conda](https://docs.anaconda.com/miniconda/)
```
conda create -n publiacqua -c conda-forge python=3.8
conda install -c conda-forge python-ldap==3.3.1
pip install -r requirements.txt
```