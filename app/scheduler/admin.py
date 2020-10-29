from django.contrib import admin
from .models import Task, GeoPackage, Process


class TaskAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "type",
        "name",
        "geopackage",
        "requesting_user",
        "start_date",
        "end_date",
        "status",
    )
    list_filter = (
        "id",
        "type",
        "name",
        "geopackage",
        "requesting_user",
        "start_date",
        "end_date",
        "status",
    )
    search_fields = ("type", "name", "geopackage")

    def has_change_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class GeoPackageAdmin(admin.ModelAdmin):
    list_display = ("id", "name")
    list_filter = ("id", "name")
    search_fields = ("name",)

    def has_change_permission(self, request, obj=None):
        return False

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class ProcessAdmin(admin.ModelAdmin):
    pass


admin.site.register(Task, TaskAdmin)
admin.site.register(GeoPackage, GeoPackageAdmin)
admin.site.register(Process, ProcessAdmin)
