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
        colliding_processes = cls.process_pre_send(colliding_tasks)
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
        analysis_cursor = connection.cursor()
        with analysis_cursor as cursor:
            cursor.callproc(
                f"{settings.DATABASE_SCHEMAS[u'analysis']}.{self.algorithm}")
            result = cursor.fetchone()
        return result


class LocalitàIstat(BaseProcessTask):
    algorithm = u"populate_pop_res_loc"
    name = u"Località Istat"

    @classmethod
    def process_pre_send(cls):
        pass


class PercentualePopolazioneServitaPerLocalità(BaseProcessTask):
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


class ShapeFognatura(BaseProcessTask):
    algorithm = u"populate_fgn_shape"
    name = u"Shape Fognatura (FGN_SHAPE)"

    @classmethod
    def process_pre_send(cls):
        pass


class ShapeAcquedotto(BaseProcessTask):
    algorithm = u"populate_acq_shape"
    name = u"Shape Acquedotto (ACQ_SHAPE)"

    @classmethod
    def process_pre_send(cls):
        pass


process_mapper = {
    u"Località Istat": LocalitàIstat,
    u"Percentuale popolazione servita per località": PercentualePopolazioneServitaPerLocalità,
    u"Popolazione residente Istat per comune": PopolazioneResidenteIstatPerComune,
    u"Percentuale popolazione servita sulla rete per comune": PercentualePopolazioneServitaSullaRetePerComune,
    u"Servizio utenza": ServizioUtenza,
    u"Abitanti equivalenti trattati da depuratori o scarico diretto": AbitantiEquivalentiTrattatiDaDepuratoriOscaricoDiretto,
    u"Shape Fognatura (FGN_SHAPE)": ShapeFognatura,
    u"Shape Acquedotto (ACQ_SHAPE)": ShapeAcquedotto
}
