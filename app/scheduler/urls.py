from app.scheduler.views import Import, HistoricalImport, Export, Process, Freeze, Dashboard, Configuration
from django.urls import path, include

urlpatterns = [
    path(u"", Dashboard.as_view(), name=u'dashboard-view'),
    path(u"configuration/", Configuration.as_view(), name=u"configuration-view"),
    path(u"import/", include([
        path(u"", Import.as_view(), name=u'import-view'),
        path(u"historical/", HistoricalImport.as_view(), name=u'historical-import-view'),
        path(u"dump/", Export.as_view(), name=u'export-view'),
    ])),
    path(u"process/", Process.as_view(), name=u'process-view'),
    path(u"freeze/", Freeze.as_view(), name=u"freeze-view"),
]
