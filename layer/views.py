import json

from django.conf import settings
from django.db.models import Q
from django.http import Http404, JsonResponse

from layer.models import Geography


def _chart_type_spec(chart_type):
    """Look up a [[chart_types]] entry by its ``chart_type`` value."""
    for spec in settings.CONFIG.get("chart_types", []):
        if spec["chart_type"] == chart_type:
            return spec
    return None


def chart_type_geojson(request, chart_type):
    """
    Return a simplified GeoJSON FeatureCollection for a map chart type.

    Shape matches what echarts.registerMap expects: each feature's ``properties.name`` is the Geography.name
    (what the chart series' ``data[*].name`` is matched against).

    404 if ``chart_type`` is not configured in [[chart_types]].
    """
    spec = _chart_type_spec(chart_type)
    if spec is None:
        raise Http404(f"Chart type {chart_type!r} is not configured")

    state = spec["state"]
    geo_type = spec["geo_type"]

    # Uppercase `name` to match how DataSpace emits names in chart `series.data[*].name`.
    # echarts matches features to data points by exact string, so the two sides must agree.
    features = [
        {
            "type": "Feature",
            "properties": {"name": geography.name.upper()},
            "geometry": json.loads(geography.simple_geom.geojson),
        }
        for geography in Geography.objects.filter(
            Q(parentId__name__iexact=state) | Q(parentId__parentId__name__iexact=state),
            type=geo_type,
            simple_geom__isnull=False,
        )
    ]

    return JsonResponse({"type": "FeatureCollection", "features": features})
