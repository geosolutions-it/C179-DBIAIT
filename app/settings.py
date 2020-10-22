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

    'app.authenticate',
    'app.scheduler',
]

DRAMATIQ_BROKER = {
    "BROKER": "dramatiq.brokers.rabbitmq.RabbitmqBroker",
    "OPTIONS": {
        "url": "amqp://localhost:5672",
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

DRAMATIQ_TASKS_DATABASE = "default"

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

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('DATABASE_NAME', 'c179'),
        'USER': os.getenv('DATABASE_USER', 'postgres'),
        'PASSWORD': os.getenv('DATABASE_PASSWORD', ''),
        'HOST': os.getenv('DATABASE_HOST', 'localhost'),
        'PORT': os.getenv('DATABASE_PORT', 5432),
    }
}


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
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'app', 'static')]

AUTH_LDAP_SERVER_URI = os.getenv("LDAP_HOST", "ldap://127.0.0.1:10389")

AUTH_LDAP_BIND_DN = os.getenv("LDAP_USERNAME", "uid=admin,ou=system")
AUTH_LDAP_BIND_PASSWORD = os.getenv("LDAP_PASSWORD", "secret")
AUTH_LDAP_USER_DN_TEMPLATE = os.getenv("USER_DN_TEMPLATE", 'uid=%(user)s,ou=users,dc=wimpi,dc=net')

AUTH_LDAP_USER_ATTR_MAP = {
    'first_name': os.getenv("LDAP_FIRST_NAME", 'givenName'),
    'last_name': os.getenv("LDAP_LAST_NAME", 'sn'),
    'email': os.getenv("LDAP_EMAIL", 'mail'),
}

AUTH_LDAP_ALWAYS_UPDATE_USER = ast.literal_eval(os.getenv('AUTH_LDAP_ALWAYS_UPDATE_USER', 'True'))


AUTH_LDAP_FIND_GROUP_PERMS = True
AUTH_LDAP_CACHE_GROUPS = True

AUTH_LDAP_MIRROR_GROUPS = True

AUTH_LDAP_GROUP_BASE = os.getenv("AUTH_LDAP_GROUP_BASE", "ou=roles,dc=wimpi,dc=net")

AUTH_LDAP_GROUP_FILTER = os.getenv("AUTH_LDAP_GROUP_FILTER", "(|(CN=managements)(CN=administrators)(CN=operators))")

AUTH_LDAP_GROUP_SEARCH = LDAPSearch(AUTH_LDAP_GROUP_BASE, ldap.SCOPE_SUBTREE, AUTH_LDAP_GROUP_FILTER)
AUTH_LDAP_GROUP_TYPE = GroupOfNamesType(name_attr="cn")
AUTH_LDAP_USER_FLAGS_BY_GROUP = {
    'is_staff': "cn=managements,{}".format(AUTH_LDAP_GROUP_BASE),
    'is_superuser': "cn=administrators,{}".format(AUTH_LDAP_GROUP_BASE),
}

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'django_auth_ldap.backend.LDAPBackend',
)
