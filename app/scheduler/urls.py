from app.scheduler.views import (Configuration, Dashboard, Export,
                                 ExportDownloadView, Freeze, GetImportStatus,
                                 GetProcessStatusListAPIView, HistoricalImport,
                                 Import, ProcessView, QueueImportView,
                                 QueueProcessView, Asdf)
from django.urls import include, path

urlpatterns = [
    path(u"", Dashboard.as_view(), name=u'dashboard-view'),
    path(u"configuration/", Configuration.as_view(), name=u"configuration-view"),
    path(u'asdf', Asdf.as_view()),
    path(u"import/", include([
        path(u"", Import.as_view(), name=u'import-view'),
        path(u"historical/", HistoricalImport.as_view(),
             name=u'historical-import-view'),
        path(u"dump/", Export.as_view(), name=u'export-view'),
        path(u"start/", QueueImportView.as_view(), name=u'queue-import-view'),
        path(u"download/<int:task_id>", ExportDownloadView.as_view(),
             name=u'export-download-view'),
        path(u"api/", include([
            path("status", GetImportStatus.as_view(),
                 name=u'get-import-status-api-view')
        ])),
    ])),
    path(u"process/", include([
        path(u"", ProcessView.as_view(), name=u'process-view'),
        path(u"start/<int:process_id>", QueueProcessView.as_view(),
             name=u'queue-process-view'),
        path(u"api/", include([
            path(u"status/<int:process_id>", GetProcessStatusListAPIView.as_view(),
                 name=u'get-process-status-api-view')
        ])),
    ])),
    path(u"freeze/", Freeze.as_view(), name=u"freeze-view"),
]
