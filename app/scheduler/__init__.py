from django.apps import AppConfig


class SchedulerConfig(AppConfig):
    name = 'app.scheduler'
    verbose_name = 'Scheduler'


default_app_config = "scheduler.SchedulerConfig"
