from os import fstat, listdir, path
from urllib import parse

from app.scheduler.exceptions import QueuingCriteriaViolated
from app.scheduler.models import Process, ProcessHistory, Task, TaskStatus
from app.scheduler.serializers import ImportSerializer, ProcessSerializer
from app.scheduler.tasks.process_tasks import ProcessTask
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import resolve, reverse
from django.views import View
from django.views.generic import ListView
from rest_framework import generics, status
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


class ProcessView(LoginRequiredMixin, ListView):
    def get(self, request):
        try:
            process_id = int(request.GET.get(u"process_id"))
        except TypeError:
            process_id = None
        error = request.GET.get(u"error")
        bread_crumbs = {
            u"Process": reverse(u"process-view"),
        }
        process_queryset = Process.objects.all()
        if process_id:
            process_history_queryset = ProcessHistory.objects.filter(
                process=int(process_id))
        else:
            process = process_queryset.first()
            process_id = int(process.pk) if process else None
            process_history_queryset = ProcessHistory.objects.filter(
                process=process)
        context = {
            u"bread_crumbs": bread_crumbs,
            u"process_queryset": process_queryset,
            u"process_history_queryset": process_history_queryset.order_by(u"-task__start_date"),
            u"process_id": process_id,
            u"error": error
        }
        return render(request, u"process/base-process.html", context)


class GetProcessStatusListAPIView(generics.ListAPIView):
    serializer_class = ProcessSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        process_id = self.kwargs[u"process_id"]
        process_history = ProcessHistory.objects.filter(
            process_id=process_id).values_list('task__id', flat=True)
        queryset = Task.objects.filter(
            type=u"PROCESS", id__in=process_history).order_by(u"-start_date")
        return queryset


class QueueProcessView(LoginRequiredMixin, ListView):
    def get(self, request, process_id):
        process_object = get_object_or_404(Process, pk=process_id)
        try:
            ProcessTask.send(ProcessTask.pre_send(
                requesting_user=request.user, process=process_object))
            return redirect(f"{reverse(u'process-view')}?process_id={process_id}")
        except QueuingCriteriaViolated as e:
            return redirect(f"{reverse(u'process-view')}?process_id={process_id}&error={parse.quote(str(e))}")


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
                response[u"Content-Disposition"] = f"attachment; filename={task_id}.zip"
            return response
        context = {u"error": f"Siamo spiacenti che l'archivio richiesto {task_id}.zip non sia presente",
                   u"bread_crumbs": {u"Error": u"#"}}
        return render(request, u"errors/error.html", context=context, status=status.HTTP_404_NOT_FOUND)
