from C179.authenticate.views import LoginView, logout_view
from django.urls import path

urlpatterns = [
    path(u"", LoginView.as_view(), name=u'login-view'),
    path(u"logout/", logout_view, name=u"logout-view"),
]
