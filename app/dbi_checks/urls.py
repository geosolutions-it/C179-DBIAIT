from app.dbi_checks.views import (ConsistencyCheckView, 
                                  PrioritizedDataView, 
                                  DataQualityView,
                                  ConsistencyCheckStart,
                                  PrioritizedDataCheckStart,
                                  DataQualityCheckStart,
                                  GetCheckStatus,
                                  GetProcessState,
                                  ChecksListView,
                                  ChecksDownloadView,
                                  GetCheckExportStatus)
from django.urls import include, path


urlpatterns = [
    path(u"dbi_checks/", include([
        path(u"", ConsistencyCheckView.as_view(), name=u'consistency-check-view'),
        path(u"start_cons_check", ConsistencyCheckStart.as_view(), name=u'cons-check-start-view'),
        path(u"prioritized_data", PrioritizedDataView.as_view(), name=u'prioritized-data-view'),
        path(u"start_pd_check", PrioritizedDataCheckStart.as_view(), name=u'pd-check-start-view'),
        path(u"data_quality", DataQualityView.as_view(), name=u'data-quality-view'),
        path(u"start_dq_check", DataQualityCheckStart.as_view(), name=u'dq-check-start-view'),
        path(u"api/", include([
            path("status_check", GetCheckStatus.as_view(),
                 name=u'get-check-status-api-view'),
            path("process-state-status/", GetProcessState.as_view(),
                 name=u'process-state-status-api-view')
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
