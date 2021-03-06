from app.scheduler.views import (Configuration, Dashboard, ExportListView,
                                 ExportDownloadView, Freeze, GetImportStatus,
                                 GetProcessStatusListAPIView, HistoricalImport,
                                 Import, ProcessView, QueueImportView,
                                 QueueProcessView, GetImportedLayer, GetExportStatus, QueueFreezeView, GetFreezeStatus,
                                 HistoricalFreeze, GetFreezeLayer)
from django.urls import include, path

urlpatterns = [
    path(u"", Dashboard.as_view(), name=u'dashboard-view'),
    path(u"configuration/", Configuration.as_view(), name=u"configuration-view"),
    path(u"export/", include([
        path(u"", ExportListView.as_view(), name=u'export-view'),
        path("status", GetExportStatus.as_view(), name="get-export-status-api-view"),
        path(u"download/<int:task_id>", ExportDownloadView.as_view(),
             name=u'export-download-view'),
    ])),
    path(u"import/", include([
        path(u"", Import.as_view(), name=u'import-view'),
        path(u"historical/", HistoricalImport.as_view(),
             name=u'historical-import-view'),
        path(u"start/", QueueImportView.as_view(), name=u'queue-import-view'),
        path(u"api/", include([
            path("status", GetImportStatus.as_view(),
                 name=u'get-import-status-api-view'),
            path("status-single-task/", GetImportedLayer.as_view(),
                 name=u'get-single-import-status-api-view')
        ])),
    ])),
    path(u"process/", include([
        path(u"", ProcessView.as_view(), name=u'process-view'),
        path(u"start/", QueueProcessView.as_view(),
             name=u'queue-process-view'),
        path(u"api/", include([
            path(u"status/", GetProcessStatusListAPIView.as_view(),
                 name=u'get-process-status-api-view')
        ])),
    ])),
    path(u"freeze/", include([
        path(u"", Freeze.as_view(), name=u'freeze-view'),
        path(u"historical/", HistoricalFreeze.as_view(),
             name=u'historical-freeze-view'),
        path(u"start/", QueueFreezeView.as_view(), name=u'queue-freeze-view'),
        path(u"api/", include([
            path("status", GetFreezeStatus.as_view(),
                 name=u'get-freeze-status-api-view'),
            path("status-single-task/", GetFreezeLayer.as_view(),
                 name=u'get-single-freeze-status-api-view')
        ])),
    ])),
]
