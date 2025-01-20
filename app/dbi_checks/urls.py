from app.dbi_checks.views import (Consistency_check, 
                                  PrioritizedData_check, 
                                  Consistency_check_start,
                                  GetCheckDbiStatus,
                                  GetImportedSheet)
from django.urls import include, path

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
        ])),
]
