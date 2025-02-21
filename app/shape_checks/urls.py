from app.shape_checks.views import (ShpAcqCheckView,
                                    ShpAcqCheckStart,
                                    GetShapeCheckStatus,
                                    GetShapeCheckProcessState,
                                    ShapeChecksListView,
                                    GetShapeCheckExportStatus,
                                    ShapeChecksDownloadView)
from django.urls import include, path


urlpatterns = [
    path(u"shape_checks/", include([
        path(u"", ShpAcqCheckView.as_view(), name=u'shp-acq-check-view'),
        path(u"start_acq_check", ShpAcqCheckStart.as_view(), name=u'shp-acq-check-start-view'),
        path(u"api/", include([
            path("status_shape_check", GetShapeCheckStatus.as_view(),
                 name=u'get-shape-check-status-api-view'),
            path("shape-process-state-status/", GetShapeCheckProcessState.as_view(),
                 name=u'shape-process-state-status-api-view')
        ])),
        # add more views for other tabs here
        path(u"historical_shape_checks/", include([
            path(u"", ShapeChecksListView.as_view(), name=u'shape-checks-list-view'),
            path(u"status", GetShapeCheckExportStatus.as_view(), name="get-shape-check-export-status-api-view"),
            path(u"download/<int:task_id>", ShapeChecksDownloadView.as_view(),
             name=u'shape-checks-download-view'),
            ])),
        ])),
]
