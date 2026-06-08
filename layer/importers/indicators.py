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
    Import indicator definitions for each state/module in ``specs``.

    Each spec is a ``[[states]]`` entry from the configuration file with a
    ``name`` and a list of ``modules`` (each with ``module`` and an
    ``indicators`` path relative to ``config_dir``). Raises ``CommandError``
    if a present CSV is missing any of the required columns. A module whose
    CSV file does not exist yet is skipped with a warning.
    """
    for spec in specs:
        name = spec["name"]
        state_geography = Geography.objects.get(name__iexact=name, type="STATE")

        for module_spec in spec.get("modules", []):
            module = module_spec["module"]
            path = config_dir / module_spec["indicators"]
            if not path.is_file():
                print(style.WARNING(f"  Skipping {module!r} for {name!r}: {path} not found"))
                continue

            print(f"Importing {module!r} indicators for {name!r} from {path} ...")
            df = pd.read_csv(path)
            if missing := REQUIRED_COLUMNS - set(df.columns):
                raise CommandError(f"{path}: missing columns {', '.join(sorted(missing))}")

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

                # Get the parent (scoped to the same state and module).
                parent_name = clean_value(row.parent)
                if parent_name is None:
                    parent = None
                else:
                    try:
                        parent = Indicators.objects.get(
                            name=parent_name, geography=state_geography, module=module
                        )
                    except Indicators.DoesNotExist:
                        print(style.WARNING(f"Missing parent indicator {parent_name!r} for {slug!r}"))
                        parent = None

                # Upsert the indicator definition. Identity is (slug, geography, module),
                # so flood and heat indicators stay independent even on a slug clash.
                Indicators.objects.update_or_create(
                    slug=slug,
                    geography=state_geography,
                    module=module,
                    defaults={
                        "name": str(row.indicatorTitle).strip(),
                        "long_description": str(row.indicatorDescription).strip() or None,
                        "category": str(row.indicatorCategory).strip() or None,
                        "unit": unit,
                        "data_source": str(row.datasource).strip() or None,
                        "parent": parent,
                        "is_visible": str(row.visible_on_platform) == "y",
                        "IDS_dataSpace": clean_value(getattr(row, "IDS_dataSpace", None)),
                    },
                )
