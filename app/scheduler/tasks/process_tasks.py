import datetime

from app.scheduler import exceptions
from app.scheduler.models import Task
from app.scheduler.tasks.base_task import BaseTask
from app.scheduler.utils import Schema, TaskStatus, TaskType
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection
from django.db.models import Q


class BaseProcessTask(BaseTask):
    task_type = TaskType.PROCESS
    name = None
    schema = Schema.ANALYSIS
    algorithm = None

    class Meta:
        abstract = True
        queue_name = u"tasks"

    @classmethod
    def process_pre_send(cls):
        raise NotImplementedError

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
    ):
        # check for conflicting tasks
        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(
            (Q(schema=Schema.ANALYSIS) & Q(type=TaskType.PROCESS)) | (
                Q(schema=Schema.FREEZE) & Q(type=TaskType.EXPORT))
        )

        if colliding_tasks:
            raise exceptions.QueuingCriteriaViolated(
                f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )

        # check for conflicting processes
        colliding_processes = cls.process_pre_send()
        if colliding_processes:
            raise exceptions.QueuingCriteriaViolated(
                f"Following processes prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )

        current_task = Task.objects.create(
            requesting_user=requesting_user,
            schema=cls.schema,
            type=cls.task_type,
            name=cls.name,
            params={"kwargs": {}},
            start_date=datetime.datetime.now(),
        )

        return current_task.pk

    def execute(self, task_id: int, *args, gpkg_path: str = None, **kwargs) -> None:
        try:
            print(f"PROCESSING {self.name}...")
            analysis_cursor = connection.cursor()
            with analysis_cursor as cursor:
                cursor.callproc(
                    f"{settings.DATABASE_SCHEMAS[u'analysis']}.{self.algorithm}")
                result = cursor.fetchone()
        except Exception as e:
            print(e)
            result = False
        print(f"procedure {settings.DATABASE_SCHEMAS[u'analysis']}.{self.algorithm} => { result }")
        return result


class LocalitaIstat(BaseProcessTask):
    algorithm = u"populate_pop_res_loc"
    name = u"Località Istat"

    @classmethod
    def process_pre_send(cls):
        pass


class PercentualePopolazioneServitaPerLocalita(BaseProcessTask):
    algorithm = u"populate_distrib_loc_serv"
    name = u"Percentuale popolazione servita per località"

    @classmethod
    def process_pre_send(cls):
        pass


class PopolazioneResidenteIstatPerComune(BaseProcessTask):
    algorithm = u"populate_pop_res_comune"
    name = u"Popolazione residente Istat per comune"

    @classmethod
    def process_pre_send(cls):
        return Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING), name__in=["populate_pop_res_loc", "populate_distrib_loc_serv"],
        )


class PercentualePopolazioneServitaSullaRetePerComune(BaseProcessTask):
    algorithm = u"populate_distr_com_serv"
    name = u"Percentuale popolazione servita sulla rete per comune"

    @classmethod
    def process_pre_send(cls):
        pass


class ServizioUtenza(BaseProcessTask):
    algorithm = u"populate_utenza_servizio"
    name = u"Servizio utenza"

    @classmethod
    def process_pre_send(cls):
        pass


class AbitantiEquivalentiTrattatiDaDepuratoriOscaricoDiretto(BaseProcessTask):
    algorithm = u"populate_abitanti_trattati"
    name = u"Abitanti equivalenti trattati da depuratori o scarico diretto"

    @classmethod
    def process_pre_send(cls):
        pass


class Fognatura(BaseProcessTask):
    algorithm = u"populate_fognatura"
    name = u"Fognatura"

    @classmethod
    def process_pre_send(cls):
        pass


class Acquedotto(BaseProcessTask):
    algorithm = u"populate_acquedotto"
    name = u"Acquedotto"

    @classmethod
    def process_pre_send(cls):
        pass


class ArchiviPompe(BaseProcessTask):
    algorithm = u"populate_archivi_pompe"
    name = u"Archivio pompe"

    @classmethod
    def process_pre_send(cls):
        pass


class Networks(BaseProcessTask):
    algorithm = u"create_networks"
    name = u"Generazione grafo (rete idrica e fognaria)"

    @classmethod
    def process_pre_send(cls):
        pass


class GraphTemp(BaseProcessTask):
    algorithm = u"populate_temp_graph_tables"
    name = u"Tabelle di relazione"

    @classmethod
    def process_pre_send(cls):
        pass


class ResetStdaTables(BaseProcessTask):
    algorithm = u"reset_proc_stda_tables"
    name = u"Ripristino Tabelle Standalone"

    @classmethod
    def process_pre_send(cls):
        pass


class PopulateDistribuzioni(BaseProcessTask):
    algorithm = u"populate_accorpamento_distribuzioni"
    name = u"Accorpamento Distribuzioni"

    @classmethod
    def process_pre_send(cls):
        pass


process_mapper = {
    u"Ripristino Tabelle Standalone": ResetStdaTables,
    u"Località Istat": LocalitaIstat,
    u"Percentuale popolazione servita per località": PercentualePopolazioneServitaPerLocalita,
    u"Popolazione residente Istat per comune": PopolazioneResidenteIstatPerComune,
    u"Percentuale popolazione servita sulla rete per comune": PercentualePopolazioneServitaSullaRetePerComune,
    u"Servizio utenza": ServizioUtenza,
    u"Abitanti equivalenti trattati da depuratori o scarico diretto": AbitantiEquivalentiTrattatiDaDepuratoriOscaricoDiretto,
    u"Archivio pompe": ArchiviPompe,
    u"Tabelle di relazione": GraphTemp,
    u"Acquedotto": Acquedotto,
    u"Fognatura": Fognatura,
    u"Accorpamento Distribuzioni": PopulateDistribuzioni
}

