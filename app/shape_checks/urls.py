from app.shape_checks.views import (ShpAcqCheckView,
                                    ShpAcqCheckStart)
from django.urls import include, path


urlpatterns = [
    path(u"shape_checks/", include([
        path(u"", ShpAcqCheckView.as_view(), name=u'shp-acq-check-view'),
        path(u"start_acq_check", ShpAcqCheckStart.as_view(), name=u'shp-acq-check-start-view'),
        ])),
]
