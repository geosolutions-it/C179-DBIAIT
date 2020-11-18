from os import fstat, listdir, path
from urllib import parse

from app.scheduler.exceptions import QueuingCriteriaViolated, SchedulingParametersError
from app.scheduler.models import Task, TaskStatus
from app.scheduler.serializers import ImportSerializer, ProcessSerializer
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.urls import resolve, reverse
from django.views import View
from django.views.generic import ListView
from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated
from app.scheduler.tasks.process_tasks import process_mapper
from app.scheduler.tasks.import_task import ImportTask
from app.scheduler.tasks.export_task import ExportTask


class Dashboard(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dashboard/index.html')


class Import(LoginRequiredMixin, ListView):
    template_name = u'import/active-import.html'
    queryset = Task.objects.filter(type='IMPORT', status__in=[
                                   TaskStatus.RUNNING, TaskStatus.QUEUED]).order_by('-id')

    @staticmethod
    def get_geopackage_files():
        import_folder = settings.IMPORT_FOLDER
        filenames = listdir(import_folder)
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
    queryset = Task.objects.filter(type='IMPORT').order_by('-id')[:1]
    serializer_class = ImportSerializer
    permission_classes = [IsAuthenticated]


class HistoricalImport(LoginRequiredMixin, ListView):
    template_name = u'import/historical-import.html'
    queryset = Task.objects.filter(type='IMPORT').exclude(
        status__in=[TaskStatus.RUNNING, TaskStatus.QUEUED]).order_by('-id')

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
        ftp_folder = settings.FTP_FOLDER
        database_host = settings.DATABASES[u'system'][u'HOST']
        database_port = settings.DATABASES[u'system'][u'PORT']
        database_name = settings.DATABASES[u'system'][u'NAME']
        database_user = settings.DATABASES[u'system'][u'USER']

        environment = u'SVILUPPO' if settings.DEBUG else u'PRODUZIONE'
        context = {
            u'bread_crumbs': bread_crumbs,
            u'environment': environment,
            u'database_host': database_host,
            u'database_port': database_port,
            u'database_name': database_name,
            u'database_user': database_user,
            u'ftp_folder': ftp_folder,
            u'geopackages': Import.get_geopackage_files()
        }
        return render(request, 'configuration/base-configuration.html', context)


class QueueImportView(LoginRequiredMixin, View):
    def post(self, request):
        gpkg_name = request.POST.get(u"gpkg-name")
        try:
            ImportTask.send(ImportTask.pre_send(requesting_user=request.user, gpkg_name=gpkg_name))
            return redirect(reverse(u"import-view"))
        except QueuingCriteriaViolated as e:
            return redirect(reverse(u"import-view"))


class ExportListView(LoginRequiredMixin, ListView):
    template_name = u'export/base-export.html'
    queryset = Task.objects.filter(type=U"EXPORT").order_by(u"-start_date")

    def post(self, request,  *args, **kwargs):
        """
        Queue export task and return results of export status
        """
        export_schema = request.POST.get(u"export-schema")
        self.object_list = self.get_queryset()
        context = self.get_context_data()
        try:
            ExportTask.send(ExportTask.pre_send(requesting_user=request.user, schema=export_schema))
        except (QueuingCriteriaViolated, SchedulingParametersError) as e:
            context[u"error"] = str(e)
        return render(request, ExportListView.template_name, context)

    def get_context_data(self, **kwargs):
        """
        Create export template context
        """
        current_url = resolve(self.request.path_info).url_name
        context = super(ExportListView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {'Export': reverse('export-view')}
        context['current_url'] = current_url
        context['schemas'] = settings.DATABASE_SCHEMAS
        return context


class ProcessView(LoginRequiredMixin, ListView):
    def get(self, request):
        process_name = request.GET.get(u"process_name", next(iter(process_mapper)))
        error = request.GET.get(u"error")
        bread_crumbs = {
            u"Process": reverse(u"process-view"),
        }

        context = {
            u"bread_crumbs": bread_crumbs,
            u"processes": process_mapper,
            u"process_history_queryset": Task.objects.filter(name=process_name).order_by(u"-start_date"),
            u"active_process": process_name,
            u"error": error
        }
        return render(request, u"process/base-process.html", context)


class GetProcessStatusListAPIView(generics.ListAPIView):
    serializer_class = ProcessSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        process_name = self.request.GET.get(u"process_name")
        queryset = Task.objects.filter(
            type=u"PROCESS", name=process_name).order_by(u"-start_date")
        return queryset


class QueueProcessView(LoginRequiredMixin, View):
    def post(self, request):
        try:
            process_name = self.request.POST.get(u"process_name")
            process_method = process_mapper[process_name]
            process_method.send(process_method.pre_send(requesting_user=request.user))
            return redirect(f"{reverse(u'process-view')}?process_name={process_name}")
        except QueuingCriteriaViolated as e:
            return redirect(f"{reverse(u'process-view')}?process_name={process_name}&error={parse.quote(str(e))}")


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
