from pathlib import Path

from rest_framework import serializers

from app.dbi_checks.models import Task_CheckDbi, ImportedSheet
from app.dbi_checks.utils import CheckType


class CheckSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task_CheckDbi
        fields = [u'id', u'uuid', u'status', u'style_class', u'status_icon', u'progress']

class ImportedSheetSerializer(serializers.ModelSerializer):
    class Meta:
        model = ImportedSheet
        fields = [u'sheet_name', u'file_name', u'import_start_timestamp', u'import_end_timestamp', u'status']

class CheckExportTaskSerializer(serializers.ModelSerializer):
    file_name = serializers.SerializerMethodField()
    second_file_name = serializers.SerializerMethodField()
    analysis_year = serializers.CharField(source='xlsx.analysis_year', read_only=True)
    check_name = serializers.SerializerMethodField()

    class Meta:
        model = Task_CheckDbi
        fields = (u'id', u'user', u'file_name', u'second_file_name', u'check_name', u'start_date', u'end_date',
                  u'analysis_year', u'status', u'style_class', u'status_icon', 
                  u'task_log')
    
    def get_file_name(self, obj):
        file_name = Path(obj.xlsx.file_path).name
        return str(file_name) if file_name else None

    def get_second_file_name(self, obj):
        file_path = obj.xlsx.second_file_path
        # Check if second_file_path exists, if not return "---"
        if file_path:
            return str(Path(file_path).name)
        return "---"
    
    def get_check_name(self, instance):
        # If you removed choices, manually map the stored value to the human-readable label
        check_type_mapping = {
            "CDO": "Consistenza delle opere",
            "DP": "Dati prioritati",
            "BDD": "Bontà dei dati"
        }
        return check_type_mapping.get(instance.check_type, instance.check_type)