from app.scheduler.models import Task
from rest_framework import serializers


class ImportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task
        fields = [u'id', u'uuid', u'status', u'style_class', u'status_icon']
