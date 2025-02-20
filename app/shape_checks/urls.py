from app.shape_checks.views import (ShpAcqCheckView)
from django.urls import include, path


urlpatterns = [
    path(u"shape_checks/", include([
        path(u"", ShpAcqCheckView.as_view(), name=u'shp-acq-check-view')
        ])),
]
