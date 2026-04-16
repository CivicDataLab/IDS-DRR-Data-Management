import glob
import io
import os
import time

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.models.signals import post_delete
from layer.models import Data, Geography, Indicators, Unit
from layer.signals import invalidate_data_cache
from layer.cache_utils import invalidate_data_caches
from D4D_ContextLayer.settings import WHITELIST_INDICATORS


def _get_indicator_parent_from_row(row, state):
    if row.parent and not isinstance(row.parent, float):
        try:
            return Indicators.objects.get(name=row.parent.strip(), geography=state)
        except Indicators.DoesNotExist:
            print(f"Failed to get the parent indicator for {row.indicatorSlug.lower()}")
    return None


def _get_indicator_unit_form_row(row):
    if row.unit and not isinstance(row.unit, float):
        try:
            unit_obj = Unit.objects.get(name=row.unit.lower())
        except Unit.DoesNotExist:
            unit_obj = Unit(name=row.unit.lower())
            unit_obj.save()
            print(f"Saved {unit_obj.name} to DB!")
    else:
        unit_obj = None
    return unit_obj


def import_state_data(df, indicators, g_code=None):
    if g_code:
        g_codes = [g_code]
    else:
        g_codes = df.index.unique().tolist()

    geographies = dict(
        Geography.objects.filter(code__in=g_codes)
        .exclude(type="STATE")
        .values_list("code", "pk")
    )

    for code in g_codes:
        if code not in geographies:
            print(f"Geography location for: {code} is missing")
    if not geographies:
        print("No matching geographies found, skipping data import")
        return

    rows = df[df.index.isin(geographies)]

    # Disconnect post_delete signal so Django can DELETE directly without
    # SELECTing all rows first to fire per-instance signals.
    post_delete.disconnect(invalidate_data_cache, sender=Data)
    try:
        print(f"Deleting existing data for {len(geographies)} geographies...", end=" ", flush=True)
        start = time.time()
        Data.objects.filter(
            geography_id__in=list(geographies.values()),
            data_period__in=rows["timeperiod"].unique().tolist(),
        ).delete()
        print(f"{time.time() - start:.1f}s")

        melted = rows.reset_index().melt(
            id_vars=["object-id", "timeperiod"],
            value_vars=[ind.slug for ind in indicators],
            var_name="slug",
            value_name="value",
        )

        # Map to foreign key IDs in vectorized pandas.
        melted["indicator_id"] = melted["slug"].map({ind.slug: ind.pk for ind in indicators})
        melted["geography_id"] = melted["object-id"].map(geographies)

        # COPY bypasses Django's ORM, so set added/modified manually
        # to emulate auto_now_add and auto_now on the Data model.
        now = pd.Timestamp.now(tz="UTC")
        melted["added"] = now
        melted["modified"] = now

        print(f"Creating {len(melted)} data points...", end=" ", flush=True)
        start = time.time()
        # Build a TSV buffer for PostgreSQL COPY, which expects tab-separated
        # values with \N for NULLs and no header row.
        buf = io.StringIO()
        melted[["value", "indicator_id", "geography_id", "timeperiod", "added", "modified"]].to_csv(
            buf, sep="\t", header=False, index=False, na_rep="\\N",
        )
        buf.seek(0)
        with connection.cursor() as cursor:
            cursor.copy_from(
                buf,
                Data._meta.db_table,
                columns=("value", "indicator_id", "geography_id", "data_period", "added", "modified"),
            )
        print(f"{time.time() - start:.1f}s")
    finally:
        post_delete.connect(invalidate_data_cache, sender=Data)
        # Perform the post_delete signal once.
        invalidate_data_caches()


def filter_indicators(df, indicators):
    cleaned_indicator = [ind for ind in indicators if ind.slug in df.columns]
    if missing := [ind.slug for ind in indicators if ind.slug not in df.columns]:
        print(f"Missing indicators: {', '.join(missing)}")
    return cleaned_indicator


def get_indicators(state):
    visible_indicators = Indicators.objects.filter(
        is_visible=True, geography__name__iexact=state
    )
    whitelist_indicators = Indicators.objects.filter(
        slug__in=WHITELIST_INDICATORS, geography__name__iexact=state
    )
    return list(visible_indicators) + list(whitelist_indicators)


def update_data(state, district):
    files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
    if state:
        indicators = get_indicators(state.replace("_", " "))
        files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
        state_files = [
            filename for filename in files if state.lower() in filename.lower()
        ]
        if not state_files:
            raise CommandError(f"Data file for state {state} missing.")
        filename = state_files[0]
        df = pd.read_csv(
            filename,
            index_col="object-id",
            dtype={"object-id": str},
            low_memory=False,
        )
        if district:
            print(f"\nImporting {district!r} data:")
            import_state_data(df, filter_indicators(df, indicators), district)
        else:
            print(f"\nImporting {state!r} data:")
            import_state_data(df, filter_indicators(df, indicators))
    else:
        for filename in files:
            df = pd.read_csv(
                filename,
                index_col="object-id",
                dtype={"object-id": str},
            )
            state = filename.split("/")[-1].replace("_data.csv", "")
            state = state.replace("_", " ")
            print(f"\nImporting {state!r} data:")
            indicators = get_indicators(state)
            import_state_data(df, filter_indicators(df, indicators))


def import_state_indicators(df: pd.DataFrame, state: Geography):
    for row in df.itertuples(index=False):
        indicator_slug = getattr(row, "indicatorSlug", "")

        try:
            indicator = Indicators.objects.get(
                slug=indicator_slug.lower(), geography=state
            )
            indicator.name = str(getattr(row, "indicatorTitle", "")).strip()
            indicator.long_description = (
                str(getattr(row, "indicatorDescription", "")).strip() or None
            )
            indicator.category = (
                str(getattr(row, "indicatorCategory", "")).strip() or None
            )
            indicator.unit = _get_indicator_unit_form_row(row)
            indicator.data_source = str(getattr(row, "datasource", "")).strip() or None
            indicator.parent = _get_indicator_parent_from_row(row, state)
            indicator.is_visible = str(getattr(row, "visible_on_platform", "")) == "y"
            indicator.save()

        except Indicators.DoesNotExist:
            unit_obj = _get_indicator_unit_form_row(row)
            parent_obj = _get_indicator_parent_from_row(row, state)

            indicator_obj = Indicators(
                name=str(getattr(row, "indicatorTitle", "")).strip(),
                slug=str(indicator_slug).lower().strip() if indicator_slug else None,
                long_description=str(getattr(row, "indicatorDescription", "")).strip()
                or None,
                category=str(getattr(row, "indicatorCategory", "")).strip() or None,
                unit=unit_obj,
                data_source=str(getattr(row, "datasource", "")).strip() or None,
                parent=parent_obj,
                is_visible=str(getattr(row, "visible_on_platform", "")) == "y",
                geography=state,
            )
            indicator_obj.save()


def update_indicators(state):
    files = glob.glob(os.getcwd() + "/layer/assets/indicators/*_indicators.csv")
    if state:
        state_files = [
            filename for filename in files if state.lower() in filename.lower()
        ]
        if not state_files:
            raise CommandError(f"Indicator file for state {state} missing.")
        filename = state_files[0]
        df = pd.read_csv(filename)
        state = state.replace("_", " ")
        state_geo = Geography.objects.get(name__iexact=state, type="STATE")
        import_state_indicators(df, state_geo)
    else:
        for filename in files:
            df = pd.read_csv(filename)
            state = filename.split("/")[-1].replace("_indicators.csv", "")
            state = state.replace("_", " ")
            state_geo = Geography.objects.get(name__iexact=state, type="STATE")
            import_state_indicators(df, state_geo)


class Command(BaseCommand):
    """
    A Django management command for importing indicators and state/district data.

    Run ``import_geojson`` first to load geographies.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            help="Ingest data just for the state",
        )
        parser.add_argument(
            "--district",
            help="District code to import the data",
        )

    def handle(self, *args, **options):
        state = options.get("state", None)
        district = options.get("district", None)

        start = time.time()
        update_indicators(state)
        print(f"Indicators: {time.time() - start:.1f}s")

        start = time.time()
        update_data(state, district)
        print(f"Data: {time.time() - start:.1f}s")
