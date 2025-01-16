import os
import json
from django.shortcuts import render
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from django.views.generic.edit import FormView
from django.urls import reverse_lazy
from django.contrib import messages
from app.dbi_checks.forms import ExcelUploadForm
from app.dbi_checks.tasks import copy_to_dbi_files
from app.settings import (
    UPLOADED_XLSX_FILES, 
    DBI_A_1, 
    DBI_A,
    SHEETS_CONFIG,
)
from app.dbi_checks.utils import get_year
from dramatiq import broker

# Check: consistenza delle opere
class Consistency_check(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dbi_checks/base-checks.html')

# Check: dati prioritati
class PrioritizedData_check(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dbi_checks/base-checks.html')

class UploadExcelView(LoginRequiredMixin, FormView):
    
    template_name = "dbi_checks/base-checks.html"
    form_class = ExcelUploadForm
    success_url = reverse_lazy("upload-excel-view")


    def form_valid(self, form):
        xlsx_file1 = form.cleaned_data["xlsx_file1"]
        xlsx_file2 = form.cleaned_data["xlsx_file2"]

        xlsx_file1_path = os.path.join(UPLOADED_XLSX_FILES, xlsx_file1.name)
        xlsx_file2_path = os.path.join(UPLOADED_XLSX_FILES, xlsx_file2.name)

        # Load the JSON data from the file
        with open(SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
            dba_a_config = sheets_config.get("DBA_A", {})
            dba_a_1_config = sheets_config.get("DBA_A_1", {})


        with open(xlsx_file1_path, "wb+") as destination1:
            for chunk in xlsx_file1.chunks():
                destination1.write(chunk)
        with open(xlsx_file2_path, "wb+") as destination2:
            for chunk in xlsx_file2.chunks():
                destination2.write(chunk)

        # Get the year from each file
        current_year = get_year(xlsx_file1_path)
  
        # A temp check, this could be changed
        if current_year:
            # messages.success(self.request, "Files uploaded and processed successfully!")
            
            copy_to_dbi_files.send(
              xlsx_file1_path, 
              DBI_A_1, 
              dba_a_1_config, 
              next_args=[xlsx_file2_path, DBI_A, dba_a_config]
            )

        else:
            messages.error(self.request, "File processing failed. Please check the file content.")

        return super().form_valid(form)

    def form_invalid(self, form):
        messages.error(self.request, "Something went wrong with the upload... Please try again")
        return super().form_invalid(form)
