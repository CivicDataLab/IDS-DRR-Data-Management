from django.conf import settings
from django.core.management.base import BaseCommand

from layer.cli import selected_states, state_names
from layer.importers.data import import_values


class Command(BaseCommand):
    help = "Import indicator values from the CSV files listed in the configuration file."

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            choices=state_names(),
            help="Restrict to one state (from [[states]] in the configuration file).",
        )
        parser.add_argument(
            "--district",
            metavar="CODE",
            help="Restrict to one non-state geography (typically a district) by its code.",
        )

    def handle(self, *args, **options):
        import_values(selected_states(options.get("state")), settings.CONFIG_DIR, code=options.get("district"))
        self.stdout.write(self.style.SUCCESS("Imported indicator values."))
