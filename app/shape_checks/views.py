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
    ProcessStateSerializer,
    CheckExportTaskSerializer
)

from app.dbi_checks.models import Task_CheckDbi, TaskStatus, ProcessState
from app.scheduler import exceptions

import logging

logger = logging.getLogger(__name__)

# Check: consistenza delle opere main view
class ShpAcqCheckView(LoginRequiredMixin, ListView):
    template_name = u'shape_checks/active-shp-acq-check.html'
    queryset = Task_CheckDbi.objects.filter(imported=True, 
                                            status__in=[
                                                TaskStatus.RUNNING, 
                                                TaskStatus.QUEUED
                                                ],
                                            check_type=CheckType.CDO
                                   ).order_by('-id')

    def get_context_data(self, **kwargs):
        current_url = resolve(self.request.path_info).url_name
        context = super(ShpAcqCheckView, self).get_context_data(**kwargs)
        context['bread_crumbs'] = {
            'Check Shape': reverse('shp-acq-check-view'), 'SHP Acquedotto': u"#"}
        context['current_url'] = current_url
        return context