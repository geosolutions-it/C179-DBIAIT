import multiprocessing
import os

bind = '0.0.0.0:8000'

# workers
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'gthread'
worker_connections = 1000

# config
max_requests = 100 # https://docs.gunicorn.org/en/stable/settings.html#max-requests
threads = multiprocessing.cpu_count() * 2 # https://docs.gunicorn.org/en/stable/settings.html#threads
timeout = 30
keepalive = 2

deamon = not os.getenv("DEBUG", False)

wsgi_app = "app.wsgi:application"

# logging
errorlog = '-'
loglevel = 'info'
accesslog = '-'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

errorlog =  '/var/log/gunicorn/dbiait.log'
