from django.conf import settings
from django.core.management.base import BaseCommand

from layer.cli import selected_states, state_names
from layer.importers.indicators import import_indicators


class Command(BaseCommand):
    help = "Import indicator definitions from the CSV files listed in the configuration file."

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            choices=state_names(),
            help="Restrict to one state (from [[states]] in the configuration file).",
        )

    def handle(self, *args, **options):
        import_indicators(selected_states(options.get("state")), settings.CONFIG_DIR)
        self.stdout.write(self.style.SUCCESS("Imported indicator definitions."))
