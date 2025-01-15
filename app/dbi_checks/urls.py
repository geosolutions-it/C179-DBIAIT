from app.dbi_checks.views import Consistency_check, PrioritizedData_check, UploadExcelView
from django.urls import include, path

urlpatterns = [
    path(u"checks/", include([
        path(u"", Consistency_check.as_view(), name=u'consistency-check-view'),
        path(u"upload", UploadExcelView.as_view(), name=u'upload-excel-view'),
        path(u"prioritized_data", PrioritizedData_check.as_view(), name=u'prioritized-data-check-view'),
        # add more views for other tabs here
        ])),
]
