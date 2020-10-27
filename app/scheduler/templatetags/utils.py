from app.scheduler.models import status_icon_mapper, style_class_mapper
from django import template

register = template.Library()


@register.simple_tag
def get_style_class(key):
    return style_class_mapper.get(key)


@register.simple_tag
def get_status_icon(key):
    return status_icon_mapper.get(key)
