import os
import tempfile
import json
import shutil

from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.views.generic import ListView
from django.views.generic.edit import FormView
from django.urls import resolve, reverse, reverse_lazy
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

from app.dbi_checks.serializers import (
    CheckSerializer, 
    ImportedSheetSerializer,
    CheckExportTaskSerializer
)

from app.dbi_checks.models import Task_CheckDbi, TaskStatus, ImportedSheet

import logging

logger = logging.getLogger(__name__)

# Check: consistenza delle opere main view
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
    
class BaseCheckStart(LoginRequiredMixin, FormView):
    
    form_class = ExcelUploadForm
    # Variables defined in the subclasses
    template_name = None
    redirected_view = None
    seed_file = None
    second_seed_file = None
    sheet_mapping_obj = None
    second_sheet_mapping_obj = None
    dbi_formulas_obj = None
    second_dbi_formulas_obj = None
    check_name = None
    check_type = None
    task_class = None

    def get_context_files(self, form):
        """
        Extract and return file data. Override for specific views if needed.
        """
        xlsx_file = form.cleaned_data.get("xlsx_file")
        second_xlsx_file = form.cleaned_data.get("second_xlsx_file")
        # Check if the check type is the Consistenza delle opera
        if self.check_type == CheckType.CDO and not second_xlsx_file:
            logger.error("Both Excel files are required for this check.")
            self.form_invalid(form)
        return xlsx_file, second_xlsx_file

    def save_context_file(self, file_obj, file_name):
        """
        Save the uploaded file to a temporary path.
        """
        temp_path = file_obj.temporary_file_path()
        uploaded_path = os.path.join(tempfile.gettempdir(), file_name)
        with open(temp_path, "rb") as src_file, open(uploaded_path, "wb") as dst_file:
            shutil.copyfileobj(src_file, dst_file, length=1024 * 1024)
        return uploaded_path

    def load_config(self):
        """
        Load seed files and configuration for sheets and formulas based on check type.
        """
        with open(settings.SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
        with open(settings.DBI_FORMULAS, "r") as file:
            dbi_formulas = json.load(file)

        if self.check_type == CheckType.CDO:
            return {
            "seed_file": self.seed_file,
            "second_seed_file": self.second_seed_file,
            "sheet_mapping_obj": sheets_config.get(self.sheet_mapping_obj, {}),
            "second_sheet_mapping_obj": sheets_config.get(self.second_sheet_mapping_obj, {}),
            "dbi_formulas_obj": dbi_formulas.get(self.dbi_formulas_obj, {}),
            "second_dbi_formulas_obj": dbi_formulas.get(self.second_dbi_formulas_obj, {}),
            }
        return {
            "seed_file": self.seed_file,
            "sheet_mapping_obj": sheets_config.get(self.sheet_mapping_obj, {}),
            "dbi_formulas_obj": dbi_formulas.get(self.dbi_formulas_obj, {}),
            }

    def form_valid(self, form):
        xlsx_file1, xlsx_file2 = self.get_context_files(form)
        uploaded_file_paths = [self.save_context_file(xlsx_file1, xlsx_file1.name)]

        if xlsx_file2:
            uploaded_file_paths.append(self.save_context_file(xlsx_file2, xlsx_file2.name))

        # Get the seed files and config
        config_data = self.load_config()

        if all(os.path.exists(path) for path in uploaded_file_paths):
            
            # set the context data
            context = ChecksContext(
                *uploaded_file_paths,
                **config_data
            )
            context_data = {
                "args": context.args, 
                "kwargs": context.kwargs
            }

            task_id = self.task_class.pre_send(
                self.request.user,
                *uploaded_file_paths,
                name=self.check_name,
                check_type=self.check_type
            )
            self.task_class.send(task_id=task_id, context_data=context_data)
            return redirect(self.get_success_url())
        else:
            logger.error("File processing failed.")
            return super().form_valid(form)

    def form_invalid(self, form):
        logger.error("Something went wrong with the upload... Please try again.")
        return super().form_invalid(form)

    def get_success_url(self):
        return reverse_lazy(self.redirected_view)

class ConsistencyCheckStart(BaseCheckStart):
    template_name = u'dbi_checks/active-consistency-check.html'
    redirected_view = u"consistency-check-view"
    seed_file = settings.DBI_A
    second_seed_file = settings.DBI_A_1
    sheet_mapping_obj = "DBI_A"
    second_sheet_mapping_obj = "DBI_A_1"
    dbi_formulas_obj = "DBI_A_formulas"
    second_dbi_formulas_obj = "DBI_A_1_formulas"
    check_name = "consistency_check"
    check_type = CheckType.CDO
    task_class = ConsistencyCheckTask

class PrioritizedDataCheckStart(BaseCheckStart):
    template_name = u'dbi_checks/active-prioritized-data-check.html'
    redirected_view = u'prioritized-data-view'
    seed_file = settings.DBI_PRIORITATI
    sheet_mapping_obj = "DBI_PRIORITATI"
    dbi_formulas_obj = "DBI_prior_formulas"
    check_name = "prioritized_data_check"
    check_type = CheckType.DP
    task_class = PrioritizedDataCheckTask

class DataQualityCheckStart(BaseCheckStart):
    template_name = u'dbi_checks/active-data-quality-check.html'
    redirected_view = u'data-quality-view'
    seed_file = settings.DBI_BONTA_DEI_DATI
    sheet_mapping_obj = "DBI_BONTA_DEI_DATI"
    dbi_formulas_obj = "DBI_bonta_formulas"
    check_name = "data_quality_check"
    check_type = CheckType.BDD
    task_class = DataQualityCheckTask

class GetCheckStatus(generics.ListAPIView):
    queryset = Task_CheckDbi.objects.filter(imported=True).order_by('-id')[:1]
    serializer_class = CheckSerializer
    permission_classes = [IsAuthenticated]


# API based views
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

