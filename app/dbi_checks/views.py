import os
import tempfile
import json

from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.views.generic import ListView
from django.views.generic.edit import FormView
from django.contrib import messages
from django.urls import resolve, reverse
from django.http import JsonResponse, HttpResponse
from django.conf import settings

from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated

from app.dbi_checks.forms import ExcelUploadForm
from app.dbi_checks.tasks.tasks import Import_DbiCheckTask
from app.settings import (
    DBI_A_1, 
    DBI_A,
    SHEETS_CONFIG,
    DBI_FORMULAS
)
from app.dbi_checks.serializers import (
    ConsistencyCheckSerializer, 
    ImportedSheetSerializer,
    CheckExportTaskSerializer
)

from app.dbi_checks.models import Task_CheckDbi, TaskStatus, ImportedSheet

# Check: consistenza delle opere
class Consistency_check(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/active-dbi-checks.html'
    queryset = Task_CheckDbi.objects.filter(imported=True, status__in=[
                                   TaskStatus.RUNNING, TaskStatus.QUEUED]).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(Consistency_check, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Checks DBI': reverse('consistency-check-view'), 'Consistenza delle opere': u"#"}
        context['current_url'] = current_url
        return context

class Consistency_check_start(LoginRequiredMixin, FormView):
    
    template_name = u'dbi_checks/active-dbi-checks.html'
    form_class = ExcelUploadForm

    def form_valid(self, form):
        xlsx_file1 = form.cleaned_data["xlsx_file1"]
        xlsx_file2 = form.cleaned_data["xlsx_file2"]

        # Get the original filenames
        xlsx_file_name1 = xlsx_file1.name
        xlsx_file_name2 = xlsx_file2.name

        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', mode='wb') as temp_file1:
            # Real file name definition instead of a random temp name
            xlsx_file1_path = os.path.join(tempfile.gettempdir(), xlsx_file_name1)
            with open(xlsx_file1_path, 'wb') as f:
                for chunk in xlsx_file1.chunks():
                   f.write(chunk)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', mode='wb') as temp_file2:
            # Real file name definition instead of a random temp name
            xlsx_file2_path = os.path.join(tempfile.gettempdir(), xlsx_file_name2)
            with open(xlsx_file2_path, 'wb') as f:
                for chunk in xlsx_file2.chunks():
                    f.write(chunk)

        # Load the DBI file sheets config json
        with open(SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
            dbi_a_config = sheets_config.get("DBI_A", {})
            dbi_a_1_config = sheets_config.get("DBI_A_1", {})

        # Load the DBI formulas json
        with open(DBI_FORMULAS, "r") as file:
            dbi_formulas = json.load(file)
            dbi_a_formulas = dbi_formulas.get("DBI_A_formulas", {})
            dbi_a_1_formulas = dbi_formulas.get("DBI_A_1_formulas", {})

        if os.path.exists(xlsx_file1_path) and os.path.exists(xlsx_file2_path):
            
            task_id = Import_DbiCheckTask.pre_send(self.request.user, 
                                                   xlsx_file1_path,
                                                   xlsx_file2_path,
                                                   DBI_A,
                                                   DBI_A_1,
                                                   dbi_a_config,
                                                   dbi_a_1_config,
                                                   dbi_a_formulas,
                                                   dbi_a_1_formulas,
                                                   year_required=True,
                                                   )
            
            Import_DbiCheckTask.send(task_id)
            return redirect(reverse(u"consistency-check-view"))
            
        else:
            messages.error(self.request, "File processing failed. Please check the file content.")

        return super().form_valid(form)

    def form_invalid(self, form):
        messages.error(self.request, "Something went wrong with the upload... Please try again")
        return super().form_invalid(form)
    
class GetCheckDbiStatus(generics.ListAPIView):
    queryset = Task_CheckDbi.objects.filter(imported=True).order_by('-id')[:1]
    serializer_class = ConsistencyCheckSerializer
    permission_classes = [IsAuthenticated]

class GetImportedSheet(generics.RetrieveAPIView):
    serializer_class = ImportedSheetSerializer
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        """
        Return only the ImportedSheet related to a specific uuid
        """
        task_id = request.query_params['task_id']
        response = [sheet.to_dict() for sheet in ImportedSheet.objects.filter(task__uuid=task_id).order_by('-id')]
        return JsonResponse(response, safe=False)
    
# Check: dati prioritati
class PrioritizedData_check(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dbi_checks/base-checks.html')

class ChecksListView(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/historical-checks.html'
    queryset = Task_CheckDbi.objects.filter(exported=True).order_by(u"-start_date")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Add breadcrumbs to the context
        context['bread_crumbs'] = {
            'Checks DBI': reverse('checks-list-view'),
            'Storico': u"#"
        }

        return context

class GetCheckExportStatus(generics.ListAPIView):
    queryset = Task_CheckDbi.objects.filter(exported=True).order_by('-id')
    serializer_class = CheckExportTaskSerializer

class ChecksDownloadView(LoginRequiredMixin, View):
    def get(self, request, task_id: int):
        file_path = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"task_{task_id}.zip")

        if os.path.exists(file_path) and Task_CheckDbi.objects.filter(id=task_id).exists():
            with open(file_path, u"rb") as file_obj:
                response = HttpResponse(
                    file_obj.read(), content_type=u"application/x-gzip")
                response[u"Content-Length"] = os.fstat(file_obj.fileno()).st_size
                response[u"Content-Type"] = u"application/zip"
                response[u"Content-Disposition"] = f"attachment; filename={task_id}.zip"
            return response
        context = {u"error": f"Siamo spiacenti che l'archivio richiesto {task_id}.zip non sia presente",
                   u"bread_crumbs": {u"Error": u"#"}}
        return render(request, u"errors/error.html", context=context, status=status.HTTP_404_NOT_FOUND)

