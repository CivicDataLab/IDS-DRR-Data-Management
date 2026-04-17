import sys

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import ProtectedError
from django.db.models.signals import post_delete

from layer.cache_utils import (
    invalidate_data_caches,
    invalidate_geography_caches,
    invalidate_indicator_caches,
)
from layer.models import Data, Geography, Indicators, Unit
from layer.signals import (
    invalidate_data_cache,
    invalidate_geography_cache,
    invalidate_indicator_cache,
)


class Command(BaseCommand):
    help = (
        "Delete all rows populated by import_geojson, import_indicators, and "
        "import_data, so that a different configuration can be loaded from scratch. "
        "If Department or Scheme rows reference Geography, this command will abort."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_true",
            dest="noinput",
            help="Do not prompt for confirmation.",
        )

    def handle(self, *args, **options):
        counts = {
            "Data": Data.objects.count(),
            "Indicators": Indicators.objects.count(),
            "Unit": Unit.objects.count(),
            "Geography": Geography.objects.count(),
        }
        if not any(counts.values()):
            self.stdout.write("Nothing to delete.")
            return

        self.stdout.write("About to delete:")
        for model, n in counts.items():
            self.stdout.write(f"  {n:>8} {model}")

        if not options["noinput"]:
            if not sys.stdin.isatty():
                raise CommandError(
                    "Standard input is not a TTY. Pass --noinput to proceed, "
                    "or, if using Docker, re-run with: docker exec -it ..."
                )
            answer = input("Proceed? [y/N]: ")
            if answer.strip().lower() not in ("y", "yes"):
                raise CommandError("Aborted.")

        # Disconnect post_delete signals so Django can DELETE directly without
        # firing the per-instance cache-invalidation receiver for every row.
        post_delete.disconnect(invalidate_data_cache, sender=Data)
        post_delete.disconnect(invalidate_indicator_cache, sender=Indicators)
        post_delete.disconnect(invalidate_geography_cache, sender=Geography)
        try:
            with transaction.atomic():
                Data.objects.all().delete()
                # Break Indicator.parent self-references (PROTECT) before deleting.
                Indicators.objects.update(parent=None)
                Indicators.objects.all().delete()
                Unit.objects.all().delete()
                Geography.objects.all().delete()
        except ProtectedError as exc:
            raise CommandError(
                f"Cannot delete Geography while other rows reference it: {exc}. "
                "Remove those rows (e.g. Department, Scheme) first."
            ) from exc
        finally:
            post_delete.connect(invalidate_data_cache, sender=Data)
            post_delete.connect(invalidate_indicator_cache, sender=Indicators)
            post_delete.connect(invalidate_geography_cache, sender=Geography)

        # Fire each cache invalidation once, now that the tables are empty.
        invalidate_data_caches()
        invalidate_indicator_caches()
        invalidate_geography_caches()

        self.stdout.write(self.style.SUCCESS("Deleted."))
