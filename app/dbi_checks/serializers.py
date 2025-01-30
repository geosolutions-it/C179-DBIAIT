from rest_framework import serializers

from app.dbi_checks.models import Task_CheckDbi, ImportedSheet


class CheckSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task_CheckDbi
        fields = [u'id', u'uuid', u'status', u'style_class', u'status_icon', u'progress']

class ImportedSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ImportedSheet
        fields = [u'sheet_name', u'file_name', u'import_start_timestamp', u'import_end_timestamp', u'status']

class CheckExportTaskSerializer(serializers.ModelSerializer):
    file_name = serializers.CharField(source='xlsx.name', read_only=True)
    analysis_year = serializers.CharField(source='xlsx.analysis_year', read_only=True)

    class Meta:
        model = Task_CheckDbi
        fields = (u'id', u'user', u'file_name', u'start_date', u'end_date',
                  u'analysis_year', u'status', u'style_class', u'status_icon', 
                  u'task_log')