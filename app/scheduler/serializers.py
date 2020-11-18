from app.scheduler.models import Task, ImportedLayer
from rest_framework import serializers


class ImportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task
        fields = [u'id', u'uuid', u'status', u'style_class', u'status_icon', u'progress']


class ImportedLayerSerializer(serializers.ModelSerializer):
    class Meta:
        model = ImportedLayer
        fields = [u'import_start_timestamp', u'import_end_timestamp', u'layer_name', u'status']


class ProcessSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task
        fields = [u'id', u'user', u'start_date', u'end_date',
                  u'status', u'style_class', u'status_icon', u'task_log']
