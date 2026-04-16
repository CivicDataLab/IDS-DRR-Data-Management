"""Import geographies from GeoJSON files."""

import json
from pathlib import Path

from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from django.core.management.base import CommandError

from layer.models import Geography


def _render_code(template, properties, prefix_parts=None):
    code = template.format(**properties)
    if prefix_parts is not None:
        code = "-".join(code.split("-")[:prefix_parts])
    return code


def _resolve_parent(parent_spec, properties):
    parent_type = parent_spec["type"]

    if "name" in parent_spec or "name_field" in parent_spec:
        name = parent_spec.get("name") or properties[parent_spec["name_field"]]
        try:
            return Geography.objects.get(name__iexact=name, type=parent_type)
        except Geography.DoesNotExist:
            pass

        if "create_code" in parent_spec:
            code = parent_spec["create_code"]
        elif "create_code_template" in parent_spec:
            code = _render_code(
                parent_spec["create_code_template"],
                properties,
                parent_spec.get("create_code_prefix_parts"),
            )
        else:
            raise CommandError(
                f"Parent {parent_type} '{name}' missing and no create_code configured"
            )

        obj = Geography(name=name.capitalize(), code=code, type=parent_type)
        obj.save()
        return obj

    code = _render_code(
        parent_spec["code_template"],
        properties,
        parent_spec.get("code_prefix_parts"),
    )
    return Geography.objects.get(code=code, type=parent_type)


def import_geographies(specs, config_dir):
    """
    Import features from the GeoJSON files described by `specs`.

    Paths in each spec's `path` field are resolved relative to `config_dir`.
    """
    for spec in specs:
        path = config_dir / spec["path"]
        print(f"Importing {path} ....")
        data = json.loads(path.read_text())

        geo_type = spec["geo_type"]
        name_field = spec["name_field"]
        code_template = spec["code_template"]
        code_prefix_parts = spec.get("code_prefix_parts")
        parent_spec = spec["parent"]

        for ft in data["features"]:
            properties = ft["properties"]
            geom = GEOSGeometry(json.dumps(ft["geometry"]))
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])

            code = _render_code(code_template, properties, code_prefix_parts)
            name = properties[name_field]
            parent_obj = _resolve_parent(parent_spec, properties)

            try:
                geo = Geography.objects.get(code=code, parentId=parent_obj)
                geo.name = name.capitalize()
                geo.geom = geom
                geo.type = geo_type
            except Geography.DoesNotExist:
                geo = Geography(
                    name=name.capitalize(),
                    code=code,
                    type=geo_type,
                    geom=geom,
                    parentId=parent_obj,
                )
            geo.save()
