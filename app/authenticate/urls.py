import os

from app.authenticate.views import LoginView, logout_view
from django.urls import path
path_prefix = os.getenv("URL_PATH_PREFIX", "")

urlpatterns = [
    path("", LoginView.as_view(), name=u'login-view'),
    path(f"{path_prefix}logout/", logout_view, name=u"logout-view"),
]
