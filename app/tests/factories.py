import uuid

from app.scheduler.models import GeoPackage, Task
from app.scheduler.utils import TaskStatus
from django.contrib.auth import get_user_model
from factory import SubFactory, django


class UserFactory(django.DjangoModelFactory):
    class Meta:
        model = get_user_model()

    username = u"unique_username"
    email = u"paul_pogba@email.com"
    first_name = u"paul"
    last_name = u"pogba"


class GeoPackageFactory(django.DjangoModelFactory):
    class Meta:
        model = GeoPackage

    name = u"geopackage.gpkg"


class TaskFactory(django.DjangoModelFactory):
    class Meta:
        model = Task

    uuid = uuid.uuid1()
    requesting_user = SubFactory(UserFactory)
    schema = u"analysis"
    geopackage = SubFactory(GeoPackageFactory)
    type = u"Import"
    name = u"I am a task"
    status = TaskStatus.RUNNING
