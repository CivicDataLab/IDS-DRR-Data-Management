import json

from django.conf import settings
from django.db.models import Q
from django.http import Http404, HttpResponse, JsonResponse
from django.views.decorators.http import require_GET

from layer import raster
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


def _raster_query_params(request) -> dict:
    module = request.GET.get("module")
    indicator = request.GET.get("indicator")
    geography_code = request.GET.get("geography_code")
    period = request.GET.get("period")
    missing = [
        name
        for name, value in (
            ("module", module),
            ("indicator", indicator),
            ("geography_code", geography_code),
            ("period", period),
        )
        if not value
    ]
    if missing:
        return {"error": f"Missing query params: {', '.join(missing)}"}
    return {
        "module": module,
        "indicator": indicator,
        "geography_code": geography_code,
        "period": period,
    }


@require_GET
def raster_metadata(request):
    """
    TileJSON-style metadata for a clipped or full raster layer.

    Query params: ``module``, ``indicator``, ``geography_code``, ``period``.
    Colormap polarity comes from the indicator's imported ``rasterPolarity``.
    """
    params = _raster_query_params(request)
    if "error" in params:
        return JsonResponse(params, status=400)

    context = raster.get_raster_context(
        module=params["module"],
        indicator_slug=params["indicator"],
        geography_code=params["geography_code"],
        data_period=params["period"],
    )
    meta = raster.get_raster_metadata(context)
    # build_absolute_uri encodes `{z}` placeholders; tile clients need them literal.
    base = request.build_absolute_uri("/").rstrip("/")
    meta["tiles"] = [f"{base}{meta['tile_url_template']}"]
    return JsonResponse(meta)


@require_GET
def raster_tile(request, z: int, x: int, y: int):
    """
    XYZ raster tile (EPSG:3857) as PNG.

    Query params: ``module``, ``indicator``, ``geography_code``, ``period``.
    When config enables ``district_uses_state_level_raster_file`` for the
    state/module, the state GeoTIFF is masked to the requested geography boundary.
    """
    params = _raster_query_params(request)
    if "error" in params:
        return JsonResponse(params, status=400)

    context = raster.get_raster_context(
        module=params["module"],
        indicator_slug=params["indicator"],
        geography_code=params["geography_code"],
        data_period=params["period"],
    )
    try:
        body, content_type = raster.render_raster_tile(context, z, x, y)
    except Http404:
        raise
    except Exception as exc:
        raise Http404("Could not render tile") from exc
    return HttpResponse(body, content_type=content_type)


def _parse_coordinate(value: str | None, name: str) -> float | dict:
    if value is None or value == "":
        return {"error": f"Missing query param: {name}"}
    try:
        return float(value)
    except ValueError:
        return {"error": f"Invalid {name}: {value!r}"}


@require_GET
def raster_value(request):
    """
    Raw raster value at a WGS84 point (map identify / hover).

    Query params: ``module``, ``indicator``, ``geography_code``, ``period``,
    ``lat``, ``lng``.
    """
    params = _raster_query_params(request)
    if "error" in params:
        return JsonResponse(params, status=400)

    lat = _parse_coordinate(request.GET.get("lat"), "lat")
    if isinstance(lat, dict):
        return JsonResponse(lat, status=400)
    lng = _parse_coordinate(request.GET.get("lng"), "lng")
    if isinstance(lng, dict):
        return JsonResponse(lng, status=400)

    context = raster.get_raster_context(
        module=params["module"],
        indicator_slug=params["indicator"],
        geography_code=params["geography_code"],
        data_period=params["period"],
    )
    return JsonResponse(raster.get_raster_value(context, lat=lat, lng=lng))
