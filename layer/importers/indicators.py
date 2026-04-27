"""Import indicator definitions from CSV files."""

import pandas as pd
from django.core.management.base import CommandError
from django.core.management.color import color_style

from layer.models import Geography, Indicators, Unit

style = color_style()

REQUIRED_COLUMNS = {
    "indicatorSlug",
    "indicatorTitle",
    "indicatorDescription",
    "indicatorCategory",
    "unit",
    "datasource",
    "parent",
    "visible_on_platform",
}


def clean_value(value):
    """Return the stripped string value, or None if missing."""
    if not value or isinstance(value, float):  # falsy or NaN
        return None
    return str(value).strip() or None


def import_indicators(specs, config_dir):
    """
    Import indicator definitions for each state in ``specs``.

    Each spec is a ``[[states]]`` entry from the configuration file with
    ``name`` and ``indicators`` (a path relative to ``config_dir``). Raises
    ``CommandError`` if the CSV is missing any of the required columns.
    """
    for spec in specs:
        # Read the configuration.
        name = spec["name"]
        path = config_dir / spec["indicators"]

        # Read the indicator definitions.
        df = pd.read_csv(path)
        if missing := REQUIRED_COLUMNS - set(df.columns):
            raise CommandError(f"{path}: missing columns {', '.join(sorted(missing))}")

        state_geography = Geography.objects.get(name__iexact=name, type="STATE")

        # Upsert the indicator definitions.
        for row in df.itertuples(index=False):
            # CSV sometimes has blank rows. pandas reads blanks as NaN. Skip those rows early.
            slug = clean_value(row.indicatorSlug)
            if slug is None:
                continue
            slug = slug.lower()

            # Get or create the unit.
            unit_name = clean_value(row.unit)
            if unit_name is None:
                unit = None
            else:
                unit, created = Unit.objects.get_or_create(name=unit_name.lower())
                if created:
                    print(f"Imported unit {unit.name!r}")

            # Get the parent.
            parent_name = clean_value(row.parent)
            if parent_name is None:
                parent = None
            else:
                try:
                    parent = Indicators.objects.get(name=parent_name, geography=state_geography)
                except Indicators.DoesNotExist:
                    print(style.WARNING(f"Missing parent indicator {parent_name!r} for {slug!r}"))
                    parent = None

            # Upsert the indicator definition.
            Indicators.objects.update_or_create(
                slug=slug,
                geography=state_geography,
                defaults={
                    "name": str(row.indicatorTitle).strip(),
                    "long_description": str(row.indicatorDescription).strip() or None,
                    "category": str(row.indicatorCategory).strip() or None,
                    "unit": unit,
                    "data_source": str(row.datasource).strip() or None,
                    "parent": parent,
                    "is_visible": str(row.visible_on_platform) == "y",
                },
            )
