"""Import geographic features from GeoJSON files."""

import json

from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from django.core.management.base import CommandError
from django.db import connection

from layer.models import Geography


def _snap_to_grid(geom, size):
    """Snap ``geom`` coordinates to ``size`` via ST_SnapToGrid."""
    # GEOS's Python bindings don't expose ST_SnapToGrid like ST_SimplifyPreserveTopology.
    with connection.cursor() as cursor:
        cursor.execute(
            # ST_MakeValid repairs any self-intersections.
            "SELECT ST_MakeValid(ST_SnapToGrid(%s::geometry, %s::float))",
            [geom.hexewkb.decode(), size],
        )
        return GEOSGeometry(cursor.fetchone()[0])


def _render_code(template, properties, prefix_parts=None):
    code = template.format(**properties)
    if prefix_parts is not None:
        code = "-".join(code.split("-")[:prefix_parts])
    return code


def _resolve_parent(parent_spec, properties):
    parent_type = parent_spec["type"]

    # Name lookup mode.
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
                f"Parent {parent_type} {name!r} does not exist and neither "
                f"`create_code` nor `create_code_template` is set to create it."
            )

        parent = Geography(name=name.capitalize(), code=code, type=parent_type)
        parent.save()
        return parent

    # Code lookup mode.
    code = _render_code(
        parent_spec["code_template"],
        properties,
        parent_spec.get("code_prefix_parts"),
    )
    return Geography.objects.get(code=code, type=parent_type)


def import_geographies(specs, config_dir):
    """
    Import features from the GeoJSON files described by ``specs``.

    Paths in each spec's ``path`` field are resolved relative to ``config_dir``.
    """
    for spec in specs:
        # Read the configuration.
        path = config_dir / spec["path"]
        geo_type = spec["geo_type"]
        name_field = spec["name_field"]
        code_template = spec["code_template"]
        code_prefix_parts = spec.get("code_prefix_parts")
        parent_spec = spec["parent"]

        tolerance = settings.CONFIG.get("simplify_tolerance", 0.003)
        grid_size = settings.CONFIG.get("snap_to_grid_size", 0.0001)

        # Read the geographic features.
        print(f"Importing {path} ....")
        data = json.loads(path.read_text())

        # Upsert the geographic features.
        for feature in data["features"]:
            properties = feature["properties"]
            geom = GEOSGeometry(json.dumps(feature["geometry"]))
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])

            # ST_SimplifyPreserveTopology can collapse a MultiPolygon with a single ring to a Polygon.
            simple_geom = geom.simplify(tolerance, preserve_topology=True)
            if isinstance(simple_geom, Polygon):
                simple_geom = MultiPolygon([simple_geom])
            # Snap to the grid after simplification to not break the topology.
            simple_geom = _snap_to_grid(simple_geom, grid_size)
            if isinstance(simple_geom, Polygon):
                simple_geom = MultiPolygon([simple_geom])

            code = _render_code(code_template, properties, code_prefix_parts)
            parent = _resolve_parent(parent_spec, properties)

            Geography.objects.update_or_create(
                code=code,
                parentId=parent,
                defaults={
                    "name": properties[name_field].capitalize(),
                    "type": geo_type,
                    "geom": geom,
                    "simple_geom": simple_geom,
                },
            )
