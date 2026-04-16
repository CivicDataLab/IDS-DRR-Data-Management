from django.conf import settings
from django.core.management.base import BaseCommand

from layer.geojson_import import import_geographies


class Command(BaseCommand):
    help = "Import Geography rows from the GeoJSON files listed in the configuration file."

    def handle(self, *args, **options):
        import_geographies(settings.CONFIG["geojson"], settings.CONFIG_DIR)
