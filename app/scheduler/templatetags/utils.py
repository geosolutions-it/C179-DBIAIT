from app.scheduler.models import TaskStatus
from django import template

register = template.Library()


@register.simple_tag
def get_style_class(key):
    style_class_mapper = {
        TaskStatus.QUEUED: u"table-primary",
        TaskStatus.FAILED: u"table-danger",
        TaskStatus.RUNNING: u"table-info",
        TaskStatus.SUCCESS: u"table-success"
    }
    return style_class_mapper.get(key)


@register.simple_tag
def get_status_icon(key):
    style_class_mapper = {
        TaskStatus.QUEUED: u"fas fa-circle text-warning",
        TaskStatus.FAILED: u"fas fa-times-circle text-danger",
        TaskStatus.RUNNING: u"fas fa-sync fa-spin text-primary",
        TaskStatus.SUCCESS: u"fas fa-check-circle alert text-success"
    }
    return style_class_mapper.get(key)
