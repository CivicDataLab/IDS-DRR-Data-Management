from django.conf import settings
from django.core.management.base import BaseCommand

from layer.importers.geography import import_geographies


class Command(BaseCommand):
    help = "Import geographic features from the GeoJSON files listed in the configuration file."

    def handle(self, *args, **options):
        import_geographies(settings.CONFIG["geojson"], settings.CONFIG_DIR)
        self.stdout.write(self.style.SUCCESS("Imported geographic features."))
