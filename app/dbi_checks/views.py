import os
import json

from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.views.generic import ListView
from django.views.generic.edit import FormView
from django.contrib import messages
from django.urls import resolve, reverse
from django.http import JsonResponse

from rest_framework import generics, status
from rest_framework.permissions import IsAuthenticated

from app.dbi_checks.forms import ExcelUploadForm
from app.dbi_checks.tasks.tasks import Import_DbiCheckTask
from app.settings import (
    UPLOADED_XLSX_FILES, 
    DBI_A_1, 
    DBI_A,
    SHEETS_CONFIG,
    DBI_FORMULAS
)
from app.dbi_checks.serializers import ConsistencyCheckSerializer, ImportedSheetSerializer

from app.dbi_checks.models import Task_CheckDbi, TaskStatus, ImportedSheet

# Check: consistenza delle opere
class Consistency_check(LoginRequiredMixin, ListView):
    template_name = u'dbi_checks/active-dbi-checks.html'
    queryset = Task_CheckDbi.objects.filter(type='IMPORT_CheckDbi', status__in=[
                                   TaskStatus.RUNNING, TaskStatus.QUEUED]).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(Consistency_check, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Checks DBI': reverse('consistency-check-view'), 'Consistenza delle opere': u"#"}
        context['current_url'] = current_url
        return context

# Check: dati prioritati
class PrioritizedData_check(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dbi_checks/base-checks.html')

class Consistency_check_start(LoginRequiredMixin, FormView):
    
    template_name = u'dbi_checks/active-dbi-checks.html'
    form_class = ExcelUploadForm

    def form_valid(self, form):
        xlsx_file1 = form.cleaned_data["xlsx_file1"]
        xlsx_file2 = form.cleaned_data["xlsx_file2"]

        xlsx_file1_path = os.path.join(UPLOADED_XLSX_FILES, xlsx_file1.name)
        xlsx_file2_path = os.path.join(UPLOADED_XLSX_FILES, xlsx_file2.name)

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

        with open(xlsx_file1_path, "wb+") as destination1:
            for chunk in xlsx_file1.chunks():
                destination1.write(chunk)
        with open(xlsx_file2_path, "wb+") as destination2:
            for chunk in xlsx_file2.chunks():
                destination2.write(chunk)

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
                                                   file_dependency=True,
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
    queryset = Task_CheckDbi.objects.filter(type='IMPORT_CheckDbi').order_by('-id')[:1]
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

