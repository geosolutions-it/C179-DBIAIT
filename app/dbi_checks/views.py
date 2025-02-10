import os
import tempfile
import json
import shutil

from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.views.generic import ListView
from django.views.generic.edit import FormView
from django.urls import resolve, reverse
from django.http import JsonResponse, HttpResponse
from django.conf import settings

from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated

from app.dbi_checks.forms import ExcelUploadForm
from app.dbi_checks.tasks.tasks import (
    ConsistencyCheckTask,
    PrioritizedDataCheckTask,
    DataQualityCheckTask,
)
from app.dbi_checks.utils import CheckType
from app.dbi_checks.tasks.checks_base_task import ChecksContext
from app.settings import (
    DBI_A_1, 
    DBI_A,
    DBI_PRIORITATI,
    DBI_BONTA_DEI_DATI,
    SHEETS_CONFIG,
    DBI_FORMULAS
)
from app.dbi_checks.serializers import (
    CheckSerializer, 
    ImportedSheetSerializer,
    CheckExportTaskSerializer
)

from app.dbi_checks.models import Task_CheckDbi, TaskStatus, ImportedSheet

import logging

logger = logging.getLogger(__name__)

# Check: consistenza delle opere
class ConsistencyCheckView(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/active-consistency-check.html'
    queryset = Task_CheckDbi.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=CheckType.CDO
                                   ).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(ConsistencyCheckView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Checks DBI': reverse('consistency-check-view'), 'Consistenza delle opere': u"#"}
        context['current_url'] = current_url
        return context

class ConsistencyCheckStart(LoginRequiredMixin, FormView):
    
    template_name = u'dbi_checks/active-consistency-check.html'
    form_class = ExcelUploadForm

    def form_valid(self, form):
        xlsx_file1 = form.cleaned_data["xlsx_file"]
        xlsx_file2 = form.cleaned_data.get("second_xlsx_file")

        if not xlsx_file2:
            logger.error(f"Both Excel files are required for this check.")
            return self.form_invalid(form)

        # Get the original filenames
        xlsx_file_name1 = xlsx_file1.name
        xlsx_file_name2 = xlsx_file2.name

        # internal uploaded path, and target temp path definition
        xlsx_file1_temp_path = xlsx_file1.temporary_file_path()
        xlsx_file1_uploaded_path = os.path.join(tempfile.gettempdir(), xlsx_file_name1)

        # Copy file in chunks for efficiency
        with open(xlsx_file1_temp_path, 'rb') as src_file:
            with open(xlsx_file1_uploaded_path, 'wb') as dst_file:
                shutil.copyfileobj(src_file, dst_file, length=1024*1024)

        xlsx_file2_temp_path = xlsx_file2.temporary_file_path()
        xlsx_file2_uploaded_path = os.path.join(tempfile.gettempdir(), xlsx_file_name2)

        # Copy file in chunks for efficiency
        with open(xlsx_file2_temp_path, 'rb') as src_file:
            with open(xlsx_file2_uploaded_path, 'wb') as dst_file:
                shutil.copyfileobj(src_file, dst_file, length=1024*1024)

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

        if os.path.exists(xlsx_file1_uploaded_path) and os.path.exists(xlsx_file2_uploaded_path):
    
            # set the checks context
            context = ChecksContext(
                xlsx_file1_uploaded_path,
                xlsx_file2_uploaded_path,
                DBI_A,
                DBI_A_1,
                dbi_a_config,
                dbi_a_1_config,
                dbi_a_formulas,
                dbi_a_1_formulas,
                file_year_required=True
            )
            context_data = {
                "args": context.args,
                "kwargs": context.kwargs
            }

            task_id = ConsistencyCheckTask.pre_send(self.request.user,
                                                    xlsx_file1_uploaded_path,
                                                    xlsx_file2_uploaded_path,
                                                    name="consistency_check",
                                                    check_type=CheckType.CDO
                                                    )
            
            ConsistencyCheckTask.send(task_id, context_data)

            return redirect(reverse(u"consistency-check-view"))
            
        else:
            logger.error("File processing failed. Please check the file content.")

        return super().form_valid(form)

    def form_invalid(self, form):
        logger.error(f"Something went wrong with the upload... Please try again")
        return super().form_invalid(form)
    
# Check: dati prioritati
class PrioritizedDataView(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/active-prioritized-data-check.html'
    queryset = Task_CheckDbi.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=CheckType.DP).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(PrioritizedDataView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Checks DBI': reverse('prioritized-data-view'), 'Dati Prioritati': u"#"}
        context['current_url'] = current_url
        return context
    
class PrioritizedDataCheckStart(LoginRequiredMixin, FormView):
    
    template_name = u'dbi_checks/active-prioritized-data-check.html'
    form_class = ExcelUploadForm

    def form_valid(self, form):
        xlsx_file = form.cleaned_data["xlsx_file"]

        # Get the original filenames
        xlsx_file_name = xlsx_file.name

        # internal uploaded path, and target temp path definition
        xlsx_file_temp_path = xlsx_file.temporary_file_path()
        xlsx_file_uploaded_path = os.path.join(tempfile.gettempdir(), xlsx_file_name)

        # Copy file in chunks for efficiency
        with open(xlsx_file_temp_path, 'rb') as src_file:
            with open(xlsx_file_uploaded_path, 'wb') as dst_file:
                shutil.copyfileobj(src_file, dst_file, length=1024*1024)

        # Load the DBI_PRIORITATI file sheets
        with open(SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
            dbi_prior_config = sheets_config.get("DBI_PRIORITATI", {})

        # Load the DBI PRIORITATI formulas
        with open(DBI_FORMULAS, "r") as file:
            dbi_formulas = json.load(file)
            dbi_prior_formulas = dbi_formulas.get("DBI_prior_formulas", {})

        if os.path.exists(xlsx_file_uploaded_path):

            # set the checks context
            context = ChecksContext(
                xlsx_file_uploaded_path,
                DBI_PRIORITATI,
                dbi_prior_config,
                dbi_prior_formulas,
                )
            context_data = {
                "args": context.args,
            }

            task_id = PrioritizedDataCheckTask.pre_send(self.request.user,
                                                        xlsx_file_uploaded_path,
                                                        name="prioritized_data_check",
                                                        check_type=CheckType.DP
                                                        )
            
            PrioritizedDataCheckTask.send(task_id=task_id, context_data=context_data)

            return redirect(reverse(u"prioritized-data-view"))
            
        else:
            logger.error("File processing failed. Please check the file content.")

        return super().form_valid(form)

    def form_invalid(self, form):
        logger.error(f"Something went wrong with the upload... Please try again")
        return super().form_invalid(form)
    
# Check: Bonta dei dati
class DataQualityView(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/active-data-quality-check.html'
    queryset = Task_CheckDbi.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=CheckType.BDD).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(DataQualityView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Checks DBI': reverse('data-quality-view'), 'Bontà dei dati': u"#"}
        context['current_url'] = current_url
        return context

class DataQualityCheckStart(LoginRequiredMixin, FormView):
    
    template_name = u'dbi_checks/active-data-quality-check.html'
    form_class = ExcelUploadForm

    def form_valid(self, form):
        xlsx_file = form.cleaned_data["xlsx_file"]

        # Get the original filenames
        xlsx_file_name = xlsx_file.name

        # internal uploaded path, and target temp path definition
        xlsx_file_temp_path = xlsx_file.temporary_file_path()
        xlsx_file_uploaded_path = os.path.join(tempfile.gettempdir(), xlsx_file_name)

        # Copy file in chunks for efficiency
        with open(xlsx_file_temp_path, 'rb') as src_file:
            with open(xlsx_file_uploaded_path, 'wb') as dst_file:
                shutil.copyfileobj(src_file, dst_file, length=1024*1024)

        # Load the Bonta dei dati file sheets
        with open(SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
            dbi_bonta_config = sheets_config.get("DBI_BONTA_DEI_DATI", {})

        # Load the DBI PRIORITATI formulas
        with open(DBI_FORMULAS, "r") as file:
            dbi_formulas = json.load(file)
            dbi_bonta_formulas = dbi_formulas.get("DBI_bonta_formulas", {})

        if os.path.exists(xlsx_file_uploaded_path):

            # set the checks context
            context = ChecksContext(
                xlsx_file_uploaded_path,
                DBI_BONTA_DEI_DATI,
                dbi_bonta_config,
                dbi_bonta_formulas,
                file_year_required=True
                )
            context_data = {
                "args": context.args,
                "kwargs": context.kwargs
            }

            task_id = DataQualityCheckTask.pre_send(self.request.user,
                                                        xlsx_file_uploaded_path,
                                                        name="data_quality_check",
                                                        check_type=CheckType.BDD
                                                        )
            
            DataQualityCheckTask.send(task_id=task_id, context_data=context_data)

            return redirect(reverse(u"data-quality-view"))
            
        else:
            logger.error("File processing failed. Please check the file content.")

        return super().form_valid(form)

    def form_invalid(self, form):
        logger.error(f"Something went wrong with the upload... Please try again")
        return super().form_invalid(form)

class GetCheckStatus(generics.ListAPIView):
    queryset = Task_CheckDbi.objects.filter(imported=True).order_by('-id')[:1]
    serializer_class = CheckSerializer
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

# Views for the history tab
class ChecksListView(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/historical-checks.html'
    queryset = Task_CheckDbi.objects.filter(exported=True).order_by(u"-start_date")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        current_url = resolve(self.request.path_info).url_name

        # Add breadcrumbs to the context
        context['bread_crumbs'] = {
            'Checks DBI': reverse('checks-list-view'),
            'Download': u"#"
        }
        # Get the current URL name
        context['current_url'] = current_url

        return context

class GetCheckExportStatus(generics.ListAPIView):
    queryset = Task_CheckDbi.objects.filter(exported=True).order_by('-id')
    serializer_class = CheckExportTaskSerializer

class ChecksDownloadView(LoginRequiredMixin, View):
    def get(self, request, task_id: int):
        file_path = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{task_id}.zip")

        if os.path.exists(file_path) and Task_CheckDbi.objects.filter(id=task_id).exists():
            with open(file_path, u"rb") as file_obj:
                response = HttpResponse(
                    file_obj.read(), content_type=u"application/x-gzip")
                response[u"Content-Length"] = os.fstat(file_obj.fileno()).st_size
                response[u"Content-Type"] = u"application/zip"
                response[u"Content-Disposition"] = f"attachment; filename=checks_task_{task_id}.zip"
            return response
        context = {u"error": f"Siamo spiacenti che l'archivio richiesto {task_id}.zip non sia presente",
                   u"bread_crumbs": {u"Error": u"#"}}
        return render(request, u"errors/error.html", context=context, status=status.HTTP_404_NOT_FOUND)

