from app.dbi_checks.views import (ConsistencyCheckView, 
                                  PrioritizedDataView, 
                                  ConsistencyCheckStart,
                                  GetCheckDbiStatus,
                                  GetImportedSheet,
                                  ChecksListView,
                                  ChecksDownloadView,
                                  GetCheckExportStatus)
from django.urls import include, path


urlpatterns = [
    path(u"checks/", include([
        path(u"", ConsistencyCheckView.as_view(), name=u'consistency-check-view'),
        path(u"start_cons_check", ConsistencyCheckStart.as_view(), name=u'cons-check-start-view'),
        path(u"api/", include([
            path("status_dbi_check", GetCheckDbiStatus.as_view(),
                 name=u'get-check-dbi-status-api-view'),
            path("status-dbi-check-single-task/", GetImportedSheet.as_view(),
                 name=u'get-dbi-check-single-import-status-api-view')
        ])),
        path(u"prioritized_data", PrioritizedDataView.as_view(), name=u'prioritized-data-view'),
        # add more views for other tabs here
        path(u"historical_checks/", include([
            path(u"", ChecksListView.as_view(), name=u'checks-list-view'),
            path(u"status", GetCheckExportStatus.as_view(), name="get-check-export-status-api-view"),
            path(u"download/<int:task_id>", ChecksDownloadView.as_view(),
             name=u'checks-download-view'),
            ])),
        ])),
]
