from django.conf import settings
from django.core.checks import Warning, register
from django.db import DatabaseError
from django.db.models import Q


@register()
def configuration_file_present(app_configs, **kwargs):
    """
    Warn when no configuration file has been supplied.

    The API can still run against existing database contents, but the import
    management commands will have nothing to import.
    """
    messages = []
    # CONFIG is empty if the file at CONFIG_PATH is missing or empty.
    if not settings.CONFIG:
        messages.append(
            Warning(
                f"No configuration loaded from: {settings.CONFIG_PATH}",
                hint=(
                    "Supply a configuration file (and the files it references) "
                    "or set CONFIG_PATH to point to one. Without it, "
                    "import_geojson and import_data have nothing to import."
                ),
                id="layer.W001",
            )
        )
    else:
        for key in ("geojson", "states"):
            if key not in settings.CONFIG:
                messages.append(
                    Warning(
                        f"The configuration file is missing the [[{key}]] section.",
                        hint=f"The {key!r} importer will have nothing to import.",
                        id=f"layer.W002.{key}",
                    )
                )
    return messages


@register()
def chart_types_resolve_to_geographies(app_configs, **kwargs):
    """
    Warn when a [[chart_types]] entry matches no Geography rows, or none with a ``simple_geom``.

    Both cases cause /chart-types/<chart-type> to return an empty FeatureCollection,
    which renders as a blank map instead of an error.
    """
    from layer.models import Geography

    messages = []
    for spec in settings.CONFIG.get("chart_types", []):
        chart_type = spec["chart_type"]
        state = spec["state"]
        geo_type = spec["geo_type"]

        state_match = Q(parentId__name__iexact=state) | Q(parentId__parentId__name__iexact=state)
        try:
            total = Geography.objects.filter(state_match, type=geo_type).count()
            has_simple_geom = Geography.objects.filter(state_match, type=geo_type).exclude(simple_geom=None).count()
        except DatabaseError:  # e.g. not yet migrated
            return messages

        if total == 0:
            messages.append(
                Warning(
                    f"[[chart_types]] entry {chart_type!r} matches no Geography rows "
                    f"(state={state!r}, geo_type={geo_type!r}).",
                    hint="Run the import_geojson management command, or adjust `state` and `geo_type`.",
                    id="layer.W003",
                )
            )
        elif has_simple_geom == 0:
            messages.append(
                Warning(
                    f"[[chart_types]] entry {chart_type!r} matches {total} Geography row(s), but none populate "
                    "simple_geom. GET /chart-types/{chart_type} will return an empty FeatureCollection.",
                    hint="Run the import_geojson management command, or check `simplify_tolerance`.",
                    id="layer.W004",
                )
            )
    return messages
