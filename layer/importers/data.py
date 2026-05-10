"""Import indicator values from CSV files."""

import io
import time

import pandas as pd
from django.conf import settings
from django.core.management.base import CommandError
from django.core.management.color import color_style
from django.db import connection
from django.db.models import Q
from django.db.models.signals import post_delete

from layer.cache_utils import invalidate_data_caches
from layer.models import Data, Geography, Indicators
from layer.signals import invalidate_data_cache

style = color_style()


def _replace_values(df, indicators, code=None):
    # Get the geographic codes for which to import indicator values.
    codes = [code] if code else df.index.unique().tolist()

    # Load the geographies matching the geographic codes.
    geographies = dict(
        Geography.objects.filter(code__in=codes)
        .exclude(type="STATE")
        .values_list("code", "pk")
    )
    if missing := [code for code in codes if code not in geographies]:
        print(style.WARNING(f"  Missing geographies, by code: {', '.join(missing)}"))
    if not geographies:
        print(style.ERROR("  No matching geographies found, skipping import!"))
        return

    # Get the rows matching the loaded geographies.
    rows = df[df.index.isin(geographies)]

    # Prepare the rows for COPY.
    melted = rows.reset_index().melt(
        id_vars=["object-id", "timeperiod"],
        value_vars=list(indicators),
        var_name="slug",
        value_name="value",
    )
    now = pd.Timestamp.now(tz="UTC")
    # Map to foreign key IDs.
    melted["indicator_id"] = melted["slug"].map(indicators)
    melted["geography_id"] = melted["object-id"].map(geographies)
    # COPY bypasses Django's ORM, so set added/modified to emulate auto_now_add/auto_now.
    melted["added"] = now
    melted["modified"] = now

    # Disconnect post_delete signal so Django can DELETE directly without
    # SELECTing all rows first to fire per-instance signals.
    post_delete.disconnect(invalidate_data_cache, sender=Data)
    try:
        # Delete indicator values matching the loaded geographies and the time periods in the matching rows.
        print(f"  Deleting indicator values for {len(geographies)} geographies...", end=" ", flush=True)
        start = time.time()
        Data.objects.filter(
            geography_id__in=list(geographies.values()),
            data_period__in=rows["timeperiod"].unique().tolist(),
        ).delete()
        print(f"{time.time() - start:.1f}s")

        # COPY indicator values.
        print(f"  Creating {len(melted)} indicator values...", end=" ", flush=True)
        start = time.time()
        # Build a TSV buffer for PostgreSQL COPY, which expects tab-separated
        # values with \N for NULLs and no header row.
        buf = io.StringIO()
        melted[["value", "indicator_id", "geography_id", "timeperiod", "added", "modified"]].to_csv(
            buf, sep="\t", na_rep="\\N", header=False, index=False,
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


def import_values(specs, config_dir, code=None):
    """
    Import indicator values for each state in ``specs``.

    Each spec is a ``[[states]]`` entry from the configuration file with
    ``name`` and ``data`` (a path relative to ``config_dir``). Raises
    ``CommandError`` if a state has no indicators in the database or none
    of its indicators' slugs appear as columns in the CSV.
    """
    for spec in specs:
        # Read the configuration.
        name = spec["name"]
        path = config_dir / spec["data"]

        # Load visible and allow-list indicators, as a {slug: pk} dict.
        indicators = dict(
            Indicators.objects.filter(geography__name__iexact=name)
            .filter(Q(is_visible=True) | Q(slug__in=settings.WHITELIST_INDICATORS))
            .values_list("slug", "pk")
        )
        if not indicators:
            raise CommandError(
                f"No indicators in the database for state {name!r}. "
                f"Run 'manage.py import_indicators' first."
            )

        # Read the indicator values.
        print(f"Importing {(code or name)!r} indicator values from {path} ...")
        df = pd.read_csv(
            path,
            index_col="object-id",
            dtype={"object-id": str},
            low_memory=False,
        )

        # Determine which indicators are present.
        present = {}
        missing = []
        for slug, pk in indicators.items():
            if slug in df.columns:
                present[slug] = pk
            else:
                missing.append(slug)
        if missing:
            print(style.WARNING(f"  Missing indicators, by slug: {', '.join(missing)}"))
        if not present:
            raise CommandError(
                f"None of the existing indicators for state {name!r} "
                f"appear as columns in {path}."
            )

        # Replace the indicator values.
        _replace_values(df, present, code)
