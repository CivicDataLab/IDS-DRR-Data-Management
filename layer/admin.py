from django.contrib import admin

from .models import *

class CustomUnitAdmin(admin.ModelAdmin):
    list_display = ["name", "symbol"]
    class Meta:
        model = Unit

class CustomGeoAdmin(admin.ModelAdmin):
    list_display = ["name", "code", "type"]
    class Meta:
        model = Geography

class CustomPageAdmin(admin.ModelAdmin):
    list_display = ["name"]
    class Meta:
        model = Page

class CustomDepartmentAdmin(admin.ModelAdmin):
    list_display = ["name", "geography"]
    class Meta:
        model = Department

class CustomSchemeAdmin(admin.ModelAdmin):
    list_display = ["name", "department"]
    class Meta:
        model = Scheme

class CustomIndicatorAdmin(admin.ModelAdmin):
    list_display = ["name", "type", "unit", "geography", "department", "scheme"]
    class Meta:
        model = Indicators

class CustomDataAdmin(admin.ModelAdmin):
    list_display = ["value", "indicator", "geography", "scheme"]
    class Meta:
        model = Data


admin.site.register(Unit, CustomUnitAdmin)
admin.site.register(Geography, CustomGeoAdmin)
admin.site.register(Page, CustomPageAdmin)
admin.site.register(Department, CustomDepartmentAdmin)
admin.site.register(Scheme, CustomSchemeAdmin)
admin.site.register(Indicators, CustomIndicatorAdmin)
admin.site.register(Data, CustomDataAdmin)