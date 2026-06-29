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
from layer.importers.indicators import clean_value
from layer.models import Data, Geography, Indicators
from layer.signals import invalidate_data_cache

style = color_style()

_RASTER_FILE_RE = r"\.tiff?$"


def _attach_raster_files(melted, rows, raster_slugs):
    """Join ``{slug}-raster`` columns (or the slug column when it is raster-only)."""
    raster_frames = []
    wide = rows.reset_index()
    for slug in raster_slugs:
        column = f"{slug}-raster" if f"{slug}-raster" in wide.columns else slug
        if column not in wide.columns:
            continue
        part = wide[["object-id", "timeperiod", column]].copy()
        part["slug"] = slug
        part["raster_file"] = part[column].map(clean_value)
        raster_frames.append(part[["object-id", "timeperiod", "slug", "raster_file"]])

    if raster_frames:
        melted = melted.merge(
            pd.concat(raster_frames, ignore_index=True),
            on=["object-id", "timeperiod", "slug"],
            how="left",
        )
    else:
        melted["raster_file"] = None

    # Raster-only indicators (slug is e.g. ``land-surface-temperature-raster``).
    inline = melted["value"].astype(str).str.contains(_RASTER_FILE_RE, case=False, na=False)
    if inline.any():
        melted.loc[inline, "raster_file"] = melted.loc[inline, "value"].map(clean_value)
        melted.loc[inline, "value"] = pd.NA
    return melted


def _replace_values(df, indicators, module, code=None, raster_slugs=None):
    raster_slugs = raster_slugs or set()

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
    melted = _attach_raster_files(melted, rows, raster_slugs)

    now = pd.Timestamp.now(tz="UTC")
    # Map to foreign key IDs.
    melted["indicator_id"] = melted["slug"].map(indicators)
    melted["geography_id"] = melted["object-id"].map(geographies)
    # COPY bypasses Django's ORM, so set added/modified to emulate auto_now_add/auto_now.
    melted["added"] = now
    melted["modified"] = now
    melted["module"] = module

    # Disconnect post_delete signal so Django can DELETE directly without
    # SELECTing all rows first to fire per-instance signals.
    post_delete.disconnect(invalidate_data_cache, sender=Data)
    try:
        # Full state/module import: replace every period for these geographies
        # (the CSV is the source of truth). District-only import (--district):
        # only replace the periods present in the file.
        print(f"  Deleting {module!r} indicator values for {len(geographies)} geographies...", end=" ", flush=True)
        start = time.time()
        delete_qs = Data.objects.filter(
            module=module,
            geography_id__in=list(geographies.values()),
        )
        if code:
            delete_qs = delete_qs.filter(
                data_period__in=rows["timeperiod"].unique().tolist(),
            )
        delete_qs.delete()
        print(f"{time.time() - start:.1f}s")

        # COPY indicator values.
        print(f"  Creating {len(melted)} indicator values...", end=" ", flush=True)
        start = time.time()
        # Build a TSV buffer for PostgreSQL COPY, which expects tab-separated
        # values with \N for NULLs and no header row.
        buf = io.StringIO()
        melted[
            [
                "value", "raster_file", "indicator_id", "geography_id", "timeperiod",
                "added", "modified", "module",
            ]
        ].to_csv(buf, sep="\t", na_rep="\\N", header=False, index=False)
        buf.seek(0)
        with connection.cursor() as cursor:
            cursor.copy_from(
                buf,
                Data._meta.db_table,  # noqa: SLF001
                columns=(
                    "value", "raster_file", "indicator_id", "geography_id", "data_period",
                    "added", "modified", "module",
                ),
            )
        print(f"{time.time() - start:.1f}s")
    finally:
        post_delete.connect(invalidate_data_cache, sender=Data)
        # Perform the post_delete signal once.
        invalidate_data_caches()


def import_values(specs, config_dir, code=None):
    """
    Import indicator values for each state/module in ``specs``.

    Each spec is a ``[[states]]`` entry from the configuration file with a
    ``name`` and a list of ``modules`` (each with ``module`` and a ``data``
    path relative to ``config_dir``). Raises ``CommandError`` if a module has
    no indicators in the database or none of its indicators' slugs appear as
    columns in the CSV. A module whose CSV file does not exist yet is skipped.
    """
    for spec in specs:
        name = spec["name"]
        state_geography = Geography.objects.get(name__iexact=name, type="STATE")

        for module_spec in spec.get("modules", []):
            module = module_spec["module"]
            path = config_dir / module_spec["data"]
            if not path.is_file():
                print(style.WARNING(f"  Skipping {module!r} for {name!r}: {path} not found"))
                continue

            # Load visible and allow-list indicators for this module, as a {slug: pk} dict.
            indicators = dict(
                Indicators.objects.filter(geography__name__iexact=name, module=module)
                .filter(Q(is_visible=True) | Q(slug__in=settings.WHITELIST_INDICATORS))
                .values_list("slug", "pk")
            )
            if not indicators:
                raise CommandError(
                    f"No {module!r} indicators in the database for state {name!r}. "
                    f"Run 'manage.py import_indicators' first."
                )

            raster_slugs = set(
                Indicators.objects.filter(
                    geography=state_geography,
                    module=module,
                    is_raster_available=True,
                )
                .filter(Q(is_visible=True) | Q(slug__in=settings.WHITELIST_INDICATORS))
                .values_list("slug", flat=True)
            )

            print(f"Importing {module!r} {(code or name)!r} indicator values from {path} ...")
            df = pd.read_csv(path, low_memory=False)
            if "object-id" not in df.columns:
                raise CommandError(f"{path}: missing `object-id` column.")
            df = df.set_index("object-id")
            df.index = df.index.astype(str)

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
                    f"None of the existing {module!r} indicators for state {name!r} "
                    f"appear as columns in {path}."
                )

            _replace_values(df, present, module, code, raster_slugs=raster_slugs)
