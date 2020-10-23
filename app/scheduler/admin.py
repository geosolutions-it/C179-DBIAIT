from django.contrib import admin
from .models import Task, GeoPackage


class TaskAdmin(admin.ModelAdmin):
    list_display = ('id', 'type', 'name', 'geopackage', 'requesting_user', 'start_date', 'end_date', 'status')


class GeoPackageAdmin(admin.ModelAdmin):
    list_display = ('id', 'name')


admin.site.register(Task, TaskAdmin)
admin.site.register(GeoPackage, GeoPackageAdmin)
