from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import render
from django.views import View
from django.urls import resolve
from django.urls import reverse


class Dashboard(LoginRequiredMixin, View):
    def get(self, request):
        return render(request, 'dashboard/index.html')


class Configuration(LoginRequiredMixin, View):
    def get(self, request):
        bread_crumbs = {
            'Configuration': reverse('configuration-view'),
        }
        context = {'bread_crumbs': bread_crumbs}
        return render(request, 'configuration/base-configuration.html', context)


class Import(LoginRequiredMixin, View):
    def get(self, request):
        current_url = resolve(request.path_info).url_name
        bread_crumbs = {
            'Import': reverse('import-view'),
            'Corrente': "#"
        }
        context = {"current_url": current_url, 'bread_crumbs': bread_crumbs}
        return render(request, 'import/base-import.html', context)

    def post(self, request):
        current_url = resolve(request.path_info).url_name
        bread_crumbs = {
            'Import': reverse('import-view'),
            'Corrente': "#"
        }
        context = {"current_url": current_url, 'bread_crumbs': bread_crumbs}
        return render(request, 'import/active-import.html', context)


class HistoricalImport(LoginRequiredMixin, View):
    def get(self, request):
        current_url = resolve(request.path_info).url_name
        bread_crumbs = {
            'Import': reverse('import-view'),
            'Storico': "#"
        }
        context = {"current_url": current_url, 'bread_crumbs': bread_crumbs}
        return render(request, 'import/historical-import.html', context)


class Export(LoginRequiredMixin, View):
    def get(self, request):
        bread_crumbs = {
            'Export': reverse('export-view'),
        }
        context = {'bread_crumbs': bread_crumbs}
        return render(request, 'export/base-export.html', context)


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
