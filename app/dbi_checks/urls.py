from app.dbi_checks.views import (Consistency_check, 
                                  PrioritizedData_check, 
                                  Consistency_check_start,
                                  GetCheckDbiStatus,
                                  GetImportedSheet,
                                  ChecksListView)
from django.urls import include, path

#temp import
from app.scheduler.views import GetExportStatus, ExportDownloadView, ExportListView

urlpatterns = [
    path(u"checks/", include([
        path(u"", Consistency_check.as_view(), name=u'consistency-check-view'),
        path(u"start_cons_check", Consistency_check_start.as_view(), name=u'cons-check-start-view'),
        path(u"api/", include([
            path("status_dbi_check", GetCheckDbiStatus.as_view(),
                 name=u'get-check-dbi-status-api-view'),
            path("status-dbi-check-single-task/", GetImportedSheet.as_view(),
                 name=u'get-dbi-check-single-import-status-api-view')
        ])),
        path(u"prioritized_data", PrioritizedData_check.as_view(), name=u'prioritized-data-check-view'),
        # add more views for other tabs here
        path(u"historical_checks/", include([
            path(u"", ChecksListView.as_view(), name=u'checks-list-view'),
            path("status", GetExportStatus.as_view(), name="get-export-status-api-view"),
            path(u"download/<int:task_id>", ExportDownloadView.as_view(),
             name=u'export-download-view'),
            ])),
        ])),
]
