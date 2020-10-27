from os import fstat, listdir, path

from app.scheduler.models import Task, TaskStatus
from app.scheduler.serializers import ImportSerializer
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.urls import resolve, reverse
from django.views import View
from django.views.generic import ListView
from rest_framework import generics
from rest_framework.permissions import IsAuthenticated


class Dashboard(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dashboard/index.html')


class Import(LoginRequiredMixin, ListView):
    template_name = u'import/active-import.html'
    queryset = Task.objects.filter(type='IMPORT', status__in=[
                                   TaskStatus.RUNNING, TaskStatus.QUEUED])

    def get_geopackage_files(self):
        nfs_folder = settings.NFS_FOLDER
        filenames = listdir(nfs_folder)
        return [filename for filename in filenames if filename.endswith('.gpkg')]

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(Import, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Import': reverse('import-view'), 'Corrente': u"#"}
        context['current_url'] = current_url
        context['geopackage_files'] = self.get_geopackage_files()
        return context


class GetImportStatus(generics.ListAPIView):
    queryset = Task.objects.filter(type='IMPORT')
    serializer_class = ImportSerializer
    permission_classes = [IsAuthenticated]


class HistoricalImport(LoginRequiredMixin, ListView):
    template_name = u'import/historical-import.html'
    queryset = Task.objects.filter(type='IMPORT').exclude(
        status__in=[TaskStatus.RUNNING, TaskStatus.QUEUED])

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(HistoricalImport, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Import': reverse('import-view'), 'Storico': "#"}
        context['current_url'] = current_url
        return context


class Configuration(LoginRequiredMixin, View):
    def get(self, request):
        bread_crumbs = {
            'Configuration': reverse('configuration-view'),
        }
        nfs_folder = settings.NFS_FOLDER
        database_user = settings.DATABASES[u'default'][u'USER']
        database_port = settings.DATABASES[u'default'][u'PORT']
        database_host = settings.DATABASES[u'default'][u'HOST']
        environment = u'SVILUPPO' if settings.DEBUG else u'PRODUZIONE'
        context = {
            u'bread_crumbs': bread_crumbs,
            u'database_user': database_user,
            u'environment': environment,
            u'database_host': database_host,
            u'nfs_folder': nfs_folder,
            u'database_port': database_port
        }
        return render(request, 'configuration/base-configuration.html', context)


class QueueImportView(LoginRequiredMixin, View):
    def post(self, request):
        return redirect(reverse(u"import-view"))


class Export(LoginRequiredMixin, ListView):
    template_name = u'export/base-export.html'

    def get_queryset(self):
        schema = self.request.GET.get(u"schema")
        query_set = Task.objects.filter(type='IMPORT')
        if schema:
            query_set = query_set.filter(schema=schema)
        return query_set.exclude(status__in=[TaskStatus.RUNNING, TaskStatus.QUEUED])

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(Export, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {'Export': reverse('export-view')}
        context['current_url'] = current_url
        return context


class Process(LoginRequiredMixin, View):
    def get(self, request):
        bread_crumbs = {
            'Process': reverse('process-view'),
        }
        context = {'bread_crumbs': bread_crumbs}
        return render(request, 'process/base-process.html', context)


class Freeze(LoginRequiredMixin, View):
    def get(self, request):
        bread_crumbs = {
            'Freeze': reverse('freeze-view'),
        }
        context = {'bread_crumbs': bread_crumbs}
        return render(request, 'freeze/base-freeze.html', context)


class ExportDownloadView(LoginRequiredMixin, View):
    def get(self, request, task_id: int):
        file_path = path.join(settings.EXPORT_FOLDER, f"{task_id}.zip")
        if path.exists(file_path) and Task.objects.filter(id=task_id).exists():
            with open(file_path, u"rb") as file_obj:
                response = HttpResponse(
                    file_obj.read(), content_type=u"application/x-gzip")
                response[u"Content-Length"] = fstat(file_obj.fileno()).st_size
                response[u"Content-Type"] = u"application/zip"
                response[u"Content-Disposition"] = u"attachment; filename=export.zip"
            return response
        context = {u"error": u"Il file è stato eliminato o non è attualmente accessibile",
                   u"bread_crumbs": {u"Error": u"#"}}
        return render(request, u"errors/error.html", context=context, status=404)
