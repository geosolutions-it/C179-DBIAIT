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

from app.shape_checks.forms import ExcelDbfUploadForm
from app.shape_checks.utils import ShapeCheckType
from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState
from app.shape_checks.tasks.tasks import ShpAcqCheckTask, ShpFgnCheckTask
from app.shape_checks.serializers import (
    ShapeCheckSerializer,
    ShapeCheckProcessStateSerializer,
    ShapeCheckExportTaskSerializer
)

from app.dbi_checks.tasks.checks_base_task import ChecksContext
from app.dbi_checks.models import TaskStatus

from app.scheduler import exceptions

import logging

logger = logging.getLogger(__name__)

# Check: SHP Acquedotto
class ShpAcqCheckView(LoginRequiredMixin, ListView):
    template_name = u'shape_checks/active-shp-acq-check.html'
    queryset = Task_CheckShape.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=ShapeCheckType.ACQ
                                   ).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(ShpAcqCheckView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Check Shape': reverse('shp-acq-check-view'), 'SHP Acquedotto': u"#"}
        context['current_url'] = current_url
        return context
    
# Check: consistenza delle opere main view
class ShpFgnCheckView(LoginRequiredMixin, ListView):
    template_name = u'shape_checks/active-shp-fgn-check.html'
    queryset = Task_CheckShape.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=ShapeCheckType.FGN
                                   ).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(ShpFgnCheckView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Check Shape': reverse('shp-fgn-check-view'), 'SHP Fognatura': u"#"}
        context['current_url'] = current_url
        return context
    
class BaseShapeCheckStart(LoginRequiredMixin, FormView):
    
    form_class = ExcelDbfUploadForm
    # Variables defined in the subclasses
    template_name = None
    redirected_view = None
    seed_file = None
    sheet_mapping_obj = None
    check_name = None
    check_type = None
    task_class = None
    groups_key = None

    def get_context_files(self, form):
        """
        Extract and return file data. Override for specific views if needed.
        """
        xlsx_file = form.cleaned_data.get("xlsx_file")
        dbf_file = form.cleaned_data.get("dbf_file")
        return xlsx_file, dbf_file

    def save_context_file(self, file_obj, file_name):
        """
        Save the uploaded file to a temporary path.
        """
        temp_path = file_obj.temporary_file_path()
        uploaded_path = os.path.join(tempfile.gettempdir(), file_name)
        with open(temp_path, "rb") as src_file, open(uploaded_path, "wb") as dst_file:
            shutil.copyfileobj(src_file, dst_file, length=1024 * 1024)
        return uploaded_path

    def load_config(self, selected_group):
        """
        Load seed files and configuration for sheets and formulas based on check type.
        """
        with open(settings.SHEETS_CONFIG, "r") as file:
            sheets_config = json.load(file)
        with open(settings.SHAPE_GROUPS, "r") as file:
            shape_formulas = json.load(file)

        group_columns = {}

        group_set = shape_formulas.get(self.groups_key, {})

        if selected_group == "__all__":
            # If the user selects to run all the process we have
            # to add also the controlli_aggregat sheet
            base_group = group_set.get("__all__", {})
            extra_group = group_set.get("gruppo_controlli_aggregati", {})
            group_columns = {**base_group, **extra_group}
        elif selected_group:
            group_columns = group_set.get(selected_group, {})

        return {
            "seed_file": self.seed_file,
            "sheet_mapping_obj": sheets_config.get(self.sheet_mapping_obj, {}),
            "shape_formulas_obj": group_columns,
        }

    def form_valid(self, form):
        xlsx_file, dbf_file = self.get_context_files(form)
        uploaded_file_paths = [
            self.save_context_file(xlsx_file, xlsx_file.name),
            self.save_context_file(dbf_file, dbf_file.name)
            ]

        # Load group-specific filtering if applicable
        selected_group = self.request.POST.get("group", None)
        
        # Get the seed files and config
        config_data = self.load_config(selected_group)

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
            
            try:
                task_id = self.task_class.pre_send(
                    self.request.user,
                    *uploaded_file_paths,
                    name=self.check_name,
                    check_type=self.check_type,
                    group=selected_group
                )
                self.task_class.send(task_id=task_id, context_data=context_data)
                return redirect(self.get_success_url())
            except (exceptions.SchedulingParametersError, Exception) as e:
                logger.error(f"Unexpected error: {str(e)}")
                # Add error to the context
                return self.render_to_response(self.get_context_data(error=str(e)))

        else:
            logger.error("File processing failed.")
            return super().form_valid(form)

    def form_invalid(self, form):
        logger.error("Something went wrong with the upload... Please try again.")
        return super().form_invalid(form)

    def get_success_url(self):
        return reverse_lazy(self.redirected_view)

class ShpAcqCheckStart(BaseShapeCheckStart):
    template_name = u'shape_checks/active-shp-acq-check.html'
    redirected_view = u"shp-acq-check-view"
    seed_file = settings.SHP_ACQ
    sheet_mapping_obj = "CHECK_SHP_ACQ"
    check_name = "shp_acq_check"
    check_type = ShapeCheckType.ACQ
    task_class = ShpAcqCheckTask
    groups_key = "SHP_ACQ_GROUPS"


class ShpFgnCheckStart(BaseShapeCheckStart):
    template_name = u'shape_checks/active-shp-fgn-check.html'
    redirected_view = u"shp-fgn-check-view"
    seed_file = settings.SHP_FGN
    sheet_mapping_obj = "CHECK_SHP_FGN"
    check_name = "shp_fgn_check"
    check_type = ShapeCheckType.FGN
    task_class = ShpFgnCheckTask
    groups_key = "SHP_FGN_GROUPS"

# API views
class GetShapeCheckStatus(generics.ListAPIView):
    queryset = Task_CheckShape.objects.filter(imported=True).order_by('-id')[:1]
    serializer_class = ShapeCheckSerializer
    permission_classes = [IsAuthenticated]

class GetShapeCheckProcessState(generics.RetrieveAPIView):
    serializer_class = ShapeCheckProcessStateSerializer
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        """
        Return only the ProcessState related to a specific task uuid
        """
        task_id = request.query_params['task_id']
        response = [process_type.to_dict() for process_type in ShapeCheckProcessState.objects.filter(task__uuid=task_id).order_by('-id')]
        return JsonResponse(response, safe=False)
    
# Views for the history tab
class ShapeChecksListView(LoginRequiredMixin, ListView):
    template_name = u'shape_checks/historical-shape-checks.html'
    queryset = Task_CheckShape.objects.filter(exported=True).order_by(u"-start_date")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        current_url = resolve(self.request.path_info).url_name

        # Add breadcrumbs to the context
        context['bread_crumbs'] = {
            'Checks Shape': reverse('shape-checks-list-view'),
            'Download': u"#"
        }
        # Get the current URL name
        context['current_url'] = current_url

        return context

class GetShapeCheckExportStatus(generics.ListAPIView):
    queryset = Task_CheckShape.objects.filter(exported=True).order_by('-id')
    serializer_class = ShapeCheckExportTaskSerializer

class ShapeChecksDownloadView(LoginRequiredMixin, View):
    def get(self, request, task_id: int):
        file_path = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{task_id}.zip")

        if os.path.exists(file_path) and Task_CheckShape.objects.filter(id=task_id).exists():
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