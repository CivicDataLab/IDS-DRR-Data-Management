import difflib
from collections import defaultdict

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from layer.models import Geography


class Command(BaseCommand):
    help = "Cross-system consistency checks."

    def add_arguments(self, parser):
        parser.add_argument("--dataspace-url", default=None, help="DataSpace GraphQL endpoint")

    def handle(self, *args, **options):
        issues = 0
        if dataspace_url := options["dataspace_url"]:
            issues = self.check_map_chart_type_names(dataspace_url)
        else:
            self.stdout.write("No --dataspace-url given for [[chart_types]] check, skipping")

        if issues:
            raise CommandError(f"{issues} issue(s) found.")
        self.stdout.write(self.style.SUCCESS("All checks passed."))

    def check_map_chart_type_names(self, dataspace_url):
        """Ensure DataSpace names match Geography names, case-insensitively."""
        specs = settings.CONFIG.get("chart_types", [])
        if not specs:
            self.stdout.write("No [[chart_types]] section in configuration file, skipping")
            return 0

        self.stdout.write(f"Checking [[chart_types]] agreement with {dataspace_url} ...")

        dataspace_names = defaultdict(set)
        try:
            for dataset in self._dataspace_query(dataspace_url, "{ datasets { id } }", {})["datasets"]:
                for chart_details in self._dataspace_query(
                    dataspace_url,
                    "query($id: UUID!) { chartsDetails(datasetId: $id) { chartType chart } }",
                    {"id": dataset["id"]},
                )["chartsDetails"]:
                    dataspace_names[chart_details["chartType"]].update(
                        point["name"]
                        for series in chart_details["chart"].get("series", [])  # chart can be {}
                        if series["type"] == "map"
                        for point in series["data"]
                    )
        except requests.RequestException as exc:
            raise CommandError(f"Failed to query DataSpace: {exc}") from exc

        issues = 0
        for spec in specs:
            chart_type = spec["chart_type"]
            state = spec["state"]
            geo_type = spec["geo_type"]

            backend_names = {
                name.upper()  # views.py uppercases
                for name in Geography.objects.filter(
                    Q(parentId__name__iexact=state) | Q(parentId__parentId__name__iexact=state),
                    type=geo_type,
                ).values_list("name", flat=True)
            }

            if missing := sorted(dataspace_names.get(chart_type, set()) - backend_names):
                issues += 1
                self.stdout.write(self.style.WARNING(f"  {chart_type}: {len(missing)} name(s) in DataSpace only:"))
                for name in missing:
                    if suggestion := difflib.get_close_matches(name, backend_names, n=1, cutoff=0.7):
                        self.stdout.write(f"    {name} (did you mean: {suggestion[0]}?)")
                    else:
                        self.stdout.write(f"    {name}")

        if not issues:
            self.stdout.write(self.style.SUCCESS("  All names resolve."))
        return issues

    @staticmethod
    def _dataspace_query(url, query, variables):
        response = requests.post(url, json={"query": query, "variables": variables}, timeout=30)
        response.raise_for_status()
        return response.json()["data"]
