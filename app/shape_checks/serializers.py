from pathlib import Path

from rest_framework import serializers

from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState

class ShapeCheckSerializer(serializers.ModelSerializer):
    class Meta:
        model = Task_CheckShape
        fields = [u'id', u'uuid', u'status', u'style_class', u'status_icon', u'progress']

class ShapeCheckProcessStateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShapeCheckProcessState
        fields = [u'process_type', u'sheet_name', u'file_name', u'import_start_timestamp', u'import_end_timestamp', u'status']

class ShapeCheckExportTaskSerializer(serializers.ModelSerializer):
    file_name = serializers.SerializerMethodField()
    second_file_name = serializers.SerializerMethodField()
    analysis_year = serializers.CharField(source='xlsx_dbf.analysis_year', read_only=True)
    check_name = serializers.SerializerMethodField()
    group = serializers.SerializerMethodField()

    class Meta:
        model = Task_CheckShape
        fields = (u'id', u'user', u'file_name', u'second_file_name', u'check_name', u'group', u'start_date', u'end_date',
                  u'analysis_year', u'status', u'style_class', u'status_icon', 
                  u'task_log')
    
    
    def get_file_name(self, obj):
        file_name = Path(obj.xlsx_dbf.file_path).name
        return str(file_name) if file_name else None

    def get_second_file_name(self, obj):
        file_path = obj.xlsx_dbf.second_file_path
        # Check if second_file_path exists, if not return "---"
        if file_path:
            return str(Path(file_path).name)
        return "---"
    
    def get_check_name(self, instance):
        # If you removed choices, manually map the stored value to the human-readable label
        check_type_mapping = {
            "ACQ": "SHP Acquedotto",
            "FGN": "SHP Fognatura"
        }
        return check_type_mapping.get(instance.check_type, instance.check_type)
    
    def get_group(self, obj):
        group_mapping = {
            "__all__": "Tutti i gruppi",
            "gruppo_codice_rete_e_tratto": "Codice Rete e Tratto",
            "gruppo_materiale_e_diametro": "Materiale e Diametro",
            "gruppo_anno_e_lunghezza": "Anno e Lunghezza",
            "gruppo_stato_conservazione_tipo_rete_tipo_acqua": "Stato Conservazione, Tipo Rete, Tipo Acqua",
            "gruppo_funzionamento_copertura_profondita": "Funzionamento, Copertura, Profondità",
            "gruppo_pressioni_telecontrollo_e_protezione_catodica": "Pressioni, Telecontrollo e Protezione Catodica",
            "gruppo_allacci_riparazioni_misuratori": "Allacci, Riparazioni, Misuratori",
            "gruppo_stato_opera_e_completezza": "Stato Opera e Completezza",
            "gruppo_controlli_aggregati": "Controlli Aggregati"

        }
        return group_mapping.get(obj.group, obj.group or "---")