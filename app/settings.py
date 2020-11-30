"""
Django settings for C179 project.

Generated by 'django-admin startproject' using Django 1.11.29.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import os
import ast
import ldap

from app.utils import TemplateWithDefaults
from django_auth_ldap.config import LDAPSearch, GroupOfNamesType

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
_DEFAULT_SECRET_KEY = '*t*qdswr=g9*yuw^2$!v#srght!7xj2vdbs)#c5w=+fvtgpbq_'
SECRET_KEY = os.getenv('SECRET_KEY', _DEFAULT_SECRET_KEY)

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = ast.literal_eval(os.getenv('DEBUG', 'True'))

ALLOWED_HOSTS = []

if DEBUG:
    ALLOWED_HOSTS.append('*')

# Application definition

INSTALLED_APPS = [
    "django_dramatiq",
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # DBIAIT apps
    'app.authenticate',
    'app.scheduler',

    # Installed apps
    'rest_framework',
]

DRAMATIQ_BROKER = {
    "BROKER": os.getenv('DRAMATIQ_BROKER', "dramatiq.brokers.rabbitmq.RabbitmqBroker"),
    "OPTIONS": {
        "url": os.getenv('DRAMATIQ_BROKER_URL', "amqp://localhost:5672"),
    },
    "MIDDLEWARE": [
        "dramatiq.middleware.Prometheus",
        "dramatiq.middleware.AgeLimit",
        "dramatiq.middleware.TimeLimit",
        "dramatiq.middleware.Callbacks",
        "dramatiq.middleware.Retries",
        "django_dramatiq.middleware.AdminMiddleware",
        "django_dramatiq.middleware.DbConnectionsMiddleware",
    ]
}

DRAMATIQ_TASKS_DATABASE = "system"

LOGIN_URL = 'auth/'

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'app.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'app', 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'app.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

# database schemas translation to actual schema names
DATABASE_SCHEMAS = {
    'system': 'dbiait_system',
    'analysis': 'dbiait_analysis',
    'freeze': 'dbiait_freeze',
}

DATABASES = {
    'default': {
        'OPTIONS': {
            # extend searched schemas to enable all_domains table loading into analysis schema
            'options': f'-c search_path={DATABASE_SCHEMAS["system"]},public'
        }
    },
    'system': {
        'OPTIONS': {
            # extend searched schemas to enable all_domains table loading into analysis schema
            'options': f'-c search_path={DATABASE_SCHEMAS["system"]},public'
        }
    },
    'analysis': {
        'OPTIONS': {
            # extend searched schemas to enable all_domains table loading into analysis schema
            'options': f'-c search_path={DATABASE_SCHEMAS["analysis"]},public'
        }
    },
    'freeze': {
        'OPTIONS': {
            # extend searched schemas to enable all_domains table loading into analysis schema
            'options': f'-c search_path={DATABASE_SCHEMAS["freeze"]},public'
        }
    }
}
for db_key in DATABASES:
    DB = DATABASES[db_key]
    DB['ENGINE'] = 'django.db.backends.postgresql_psycopg2'
    DB['NAME'] = os.getenv('DATABASE_NAME', 'dbiait')
    DB['USER'] = os.getenv('DATABASE_USER', 'postgres')
    DB['PASSWORD'] = os.getenv('DATABASE_PASSWORD', '')
    DB['HOST'] = os.getenv('DATABASE_HOST', 'localhost')
    DB['PORT'] = os.getenv('DATABASE_PORT', 5432)


DATABASE_ROUTERS = ['app.scheduler.system_router.SystemRouter', 'app.scheduler.analysis_router.AnalysisRouter']


# database from DATABASES used by IMPORT, PROCESSING, EXPORT and FREEZE tasks
TASKS_DATABASE = 'system'

# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.getenv("STATIC_ROOT", os.path.join(BASE_DIR, "static_root"))
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'app', 'static')]


# -------------------------- LDAP ----------------------------------------------------------------------------------
# User to connect and search in the LDAP/AD
AUTH_LDAP_SERVER_URI = os.getenv("LDAP_HOST", "ldap://localhost:389/DC=publiacqua,DC=it")
AUTH_LDAP_BIND_DN = os.getenv("LDAP_USERNAME", "CN=webgis_ldap,OU=Users,OU=IT_Services,DC=publiacqua,DC=it")
AUTH_LDAP_BIND_PASSWORD = os.getenv("LDAP_PASSWORD", "W3bg1s2020PH2O!")

LDAP_OU_APP = os.getenv("LDAP_OU_APP", "MapStore")
LDAP_CN_OPERATOR = os.getenv("LDAP_CN_OPERATOR", "viewers")
LDAP_CN_MANAGEMENT = os.getenv("LDAP_CN_MANAGEMENT", "editors2")
LDAP_CN_ADMINISTRATOR = os.getenv("LDAP_CN_ADMINISTRATOR", "editors1")

AUTH_LDAP_USER_SEARCH = LDAPSearch(
    "",
    ldap.SCOPE_SUBTREE,
    "(&(objectCategory=Person)"
    "(|(memberOf=CN=editors1,OU=MapStore,OU=IT_Services,DC=publiacqua,DC=it)"
    "(memberOf=CN=editors2,OU=MapStore,OU=IT_Services,DC=publiacqua,DC=it)"
    "(memberOf=CN=viewers,OU=MapStore,OU=IT_Services,DC=publiacqua,DC=it))"
    "(sAMAccountName=%(user)s))")

AUTH_LDAP_USER_ATTR_MAP = {
    'first_name': os.getenv("LDAP_FIRST_NAME", 'givenName'),
    'last_name': os.getenv("LDAP_LAST_NAME", 'sn'),
    'email': os.getenv("LDAP_EMAIL", 'userPrincipalName'),
}

ldap_group_base = os.getenv("AUTH_LDAP_GROUP_BASE", "OU=MapStore,OU=IT_Services")
ldap_group_filter = os.getenv("AUTH_LDAP_GROUP_FILTER", "(|(CN=editors1)(CN=editors2)(CN=viewers))")
AUTH_LDAP_GROUP_SEARCH = LDAPSearch(ldap_group_base, ldap.SCOPE_SUBTREE, ldap_group_filter)
AUTH_LDAP_GROUP_TYPE = GroupOfNamesType(name_attr="cn")

AUTH_LDAP_USER_FLAGS_BY_GROUP = {
    'is_operator': "cn=viewers,{}".format(ldap_group_base),
    'is_staff': "cn=editors1,{}".format(ldap_group_base),
    'is_superuser': "cn=editors2,{}".format(ldap_group_base),
}

AUTH_LDAP_ALWAYS_UPDATE_USER = ast.literal_eval(os.getenv('AUTH_LDAP_ALWAYS_UPDATE_USER', 'True'))
#AUTH_LDAP_CACHE_GROUPS = True

# ---------------------------------------------------------------------------------------------------------------

AUTHENTICATION_BACKENDS = (
    'django_auth_ldap.backend.LDAPBackend',
    'django.contrib.auth.backends.ModelBackend',
)

# QGis installation path
QGIS_PATH = os.getenv("QGIS_PATH")

# Directory from which export files are selected
FTP_FOLDER = os.getenv("FTP_FOLDER", BASE_DIR)

# Directory from which geopackage, import configuration and domains.csv files are imported
IMPORT_FOLDER = os.getenv("IMPORT_FOLDER", os.path.join(FTP_FOLDER, "import"))
IMPORT_CONF_FILE = os.getenv("IMPORT_CONF_FILE", os.path.join(IMPORT_FOLDER, 'config', "layers.json"))
IMPORT_DOMAINS_FILE = os.getenv("IMPORT_DOMAINS_FILE", os.path.join(IMPORT_FOLDER, 'config', "domains.csv"))

# Directory in which generated exports are kept
EXPORT_FOLDER = os.getenv("EXPORT_FOLDER", os.path.join(BASE_DIR, "export"))

EXPORT_CONF_DIR = os.getenv("EXPORT_CONF_DIR", os.path.join(EXPORT_FOLDER, 'config'))

# TEMPLATE strings prepared for storing FREEZE configuration
EXPORT_CONF_FILE = TemplateWithDefaults(
    os.path.join(EXPORT_CONF_DIR, "$year", "xls.json"), defaults={'year': 'current'}
)
SHAPEFILE_EXPORT_CONFIG = TemplateWithDefaults(
    os.path.join(EXPORT_CONF_DIR, "$year", "shp.json"), defaults={'year': 'current'}
)
EXPORT_XLS_SEED_FILE = TemplateWithDefaults(
    os.path.join(EXPORT_CONF_DIR, "$year", "NETSIC_SEED.xlsx"), defaults={'year': 'current'}
)

PASSWORD_HASHERS = (
    'django.contrib.auth.hashers.PBKDF2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher',
    'django.contrib.auth.hashers.BCryptSHA256PasswordHasher',
    'django.contrib.auth.hashers.BCryptPasswordHasher',
    'django.contrib.auth.hashers.SHA1PasswordHasher',
    'django.contrib.auth.hashers.MD5PasswordHasher',
    'django.contrib.auth.hashers.UnsaltedMD5PasswordHasher',
    'django.contrib.auth.hashers.CryptPasswordHasher',
)

