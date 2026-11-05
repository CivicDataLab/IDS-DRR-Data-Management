from django.contrib import admin

from .models import Data, Geography, Indicators, Unit


class CustomUnitAdmin(admin.ModelAdmin):
    list_display = ["name", "symbol"]
    class Meta:
        model = Unit

class CustomGeoAdmin(admin.ModelAdmin):
    list_display = ["name", "code", "type", "parentId"]
    class Meta:
        model = Geography

class CustomIndicatorAdmin(admin.ModelAdmin):
    list_display = [
        "name", "type", "display_order", "category",
        "is_raster_available", "raster_polarity",
    ]
    class Meta:
        model = Indicators

class CustomDataAdmin(admin.ModelAdmin):
    list_display = ["value", "indicator", "geography", "raster_file"]
    class Meta:
        model = Data


admin.site.register(Unit, CustomUnitAdmin)
admin.site.register(Geography, CustomGeoAdmin)
admin.site.register(Indicators, CustomIndicatorAdmin)
admin.site.register(Data, CustomDataAdmin)
