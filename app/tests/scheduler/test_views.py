from unittest.mock import patch

from app.tests.factories import TaskFactory
from django.conf import settings
from django.test import TestCase, override_settings
from django.urls import reverse
from rest_framework import status


class DownloadArchiveTests(TestCase):
    databases = '__all__'

    def setUp(self):
        self.task = TaskFactory.create()
        self.user = self.task.requesting_user
        self.user.set_password(u"very_secret")
        self.user.save()
        self.url = reverse(u"export-download-view",
                           kwargs={'task_id': self.task.id})

    def test_login_required(self):
        """
        Test the only authenticated users can access route
        """
        response = self.client.get(self.url)

        # redirect to login page
        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertEqual(f"/{response[u'Location']}",
                         f"{reverse(u'login-view')}?next={self.url}")

    @override_settings(EXPORT_FOLDER=settings.BASE_DIR)
    def test_file_not_found(self):
        """
        Test the user gets 404 error when file/task does not exist
        """
        url = reverse(u"export-download-view", kwargs={'task_id': 00000000000})
        self.client.login(username=self.user.username, password=u"very_secret")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch(u"builtins.open")
    @patch(u"os.path.exists", return_value=True)
    @override_settings(EXPORT_FOLDER=settings.BASE_DIR)
    def test_file_download(self, mock_file_exists, mock_builtin_open):
        """
        Test that files are downloaded when user is authenticated and file/task exist
        """
        self.client.login(username=self.user.username, password=u"very_secret")
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response[u"Content-Length"], '0')
        self.assertEqual(response[u"Content-Type"], u"application/zip")
        self.assertEqual(response[u"Content-Disposition"],
                         f"attachment; filename={self.task.pk}.zip")
