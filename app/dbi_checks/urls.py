from app.dbi_checks.views import (ConsistencyCheckView, 
                                  PrioritizedDataView, 
                                  ConsistencyCheckStart,
                                  PrioritizedDataCheckStart,
                                  GetConsistencyCheckStatus,
                                  GetPrioritizedDataCheckStatus,
                                  GetImportedSheet,
                                  ChecksListView,
                                  ChecksDownloadView,
                                  GetCheckExportStatus)
from django.urls import include, path


urlpatterns = [
    path(u"checks/", include([
        path(u"", ConsistencyCheckView.as_view(), name=u'consistency-check-view'),
        path(u"start_cons_check", ConsistencyCheckStart.as_view(), name=u'cons-check-start-view'),
        path(u"prioritized_data", PrioritizedDataView.as_view(), name=u'prioritized-data-view'),
        path(u"start_pd_check", PrioritizedDataCheckStart.as_view(), name=u'pd-check-start-view'),
        path(u"api/", include([
            path("status_cons_check", GetConsistencyCheckStatus.as_view(),
                 name=u'get-cons-check-status-api-view'),
            path("status_dp_check", GetPrioritizedDataCheckStatus.as_view(),
                 name=u'get-pd-check-status-api-view'),
            path("status-dbi-check-single-task/", GetImportedSheet.as_view(),
                 name=u'get-dbi-check-single-import-status-api-view')
        ])),
        # add more views for other tabs here
        path(u"historical_checks/", include([
            path(u"", ChecksListView.as_view(), name=u'checks-list-view'),
            path(u"status", GetCheckExportStatus.as_view(), name="get-check-export-status-api-view"),
            path(u"download/<int:task_id>", ChecksDownloadView.as_view(),
             name=u'checks-download-view'),
            ])),
        ])),
]
