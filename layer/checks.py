from django.conf import settings
from django.core.checks import Warning, register


@register()
def config_toml_present(app_configs, **kwargs):
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
