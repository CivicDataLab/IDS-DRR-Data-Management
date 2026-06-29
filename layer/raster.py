"""On-demand XYZ raster tiles from plugin GeoTIFF files."""

from __future__ import annotations

import contextlib
import json
import warnings
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

import numpy
from django.conf import settings
from django.contrib.gis.geos import Point
from django.core.cache import cache
from django.db.models import Q
from django.http import Http404
from rasterio.features import geometry_mask
from rasterio.warp import transform_geom    
from rio_tiler.colormap import cmap
from rio_tiler.errors import NoOverviewWarning, PointOutsideBounds, TileOutsideBounds
from rio_tiler.io import Reader
from rio_tiler.models import ImageData
from rio_tiler.utils import render

from layer.models import Data, Geography

_EMPTY_TILE_PNG: bytes | None = None


@contextlib.contextmanager
def _open_raster(path: Path):
    """Open a GeoTIFF with rio-tiler, ignoring overview performance warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", NoOverviewWarning)
        with Reader(path) as src:
            yield src


def _mask_nan_pixels(tile: ImageData) -> ImageData:
    """Mask NaN pixels so they render transparent (GeoTIFFs often omit nodata metadata)."""
    data = tile.array
    invalid = numpy.isnan(data.data)
    if numpy.ma.is_masked(data):
        invalid = numpy.logical_or(invalid, data.mask)
    tile.array = numpy.ma.masked_array(data.data, mask=invalid, fill_value=0)
    return tile


@dataclass(frozen=True)
class RasterStyle:
    """How single-band values are stretched and colored."""

    raster_polarity: bool = True

    def colormap(self):
        # rdylgn_r: green (low) → red (high); rdylgn: reversed (min dark).
        return cmap.get("rdylgn_r" if self.raster_polarity else "rdylgn")


@dataclass(frozen=True)
class RasterContext:
    """Resolved raster source for one indicator/geography/period request."""

    path: Path
    file_name: str
    module: str
    indicator_slug: str
    geography_code: str
    data_period: str
    clip_geometry: dict | None
    clip_to_geography: bool
    geography_bounds: list[float] | None = None
    raster_polarity: bool = True


def _module_config(state_name: str, module: str) -> dict | None:
    """Return the ``[[states.modules]]`` entry for *state_name* and *module*."""
    for state_spec in settings.CONFIG.get("states", []):
        if state_spec["name"].lower() != state_name.lower():
            continue
        for module_spec in state_spec.get("modules", []):
            if module_spec["module"] == module:
                return module_spec
    return None


def module_uses_state_level_raster(state_name: str, module: str) -> bool:
    """Whether config.toml clips district/block tiles from the state GeoTIFF."""
    module_spec = _module_config(state_name, module)
    if module_spec is None:
        return False
    return bool(module_spec.get("district_uses_state_level_raster_file", False))


def _transparent_tile_png() -> bytes:
    global _EMPTY_TILE_PNG
    if _EMPTY_TILE_PNG is None:
        _EMPTY_TILE_PNG = render(
            numpy.zeros((1, 256, 256), dtype=numpy.uint8),
            mask=numpy.zeros((256, 256), dtype=numpy.uint8),
        )
    return _EMPTY_TILE_PNG


def state_geography(geography: Geography) -> Geography:
    """Walk parent links until the root STATE geography is reached."""
    geo = geography
    while geo.parentId_id:
        geo = geo.parentId
    if geo.type != "STATE":
        raise Http404("Could not resolve state for geography")
    return geo


def resolve_raster_path(module: str, state_name: str, file_name: str) -> Path:
    """Resolve ``raster_folder`` from config.toml + filename from the data CSV."""
    if not file_name or file_name != Path(file_name).name:
        raise Http404("Invalid raster file name")

    module_spec = _module_config(state_name, module)
    raster_folder = module_spec.get("raster_folder") if module_spec else None
    if not raster_folder:
        raise Http404(f"No raster_folder for {state_name!r} {module!r} in config.toml")

    config_dir = settings.CONFIG_DIR.resolve()
    base = (config_dir / raster_folder).resolve()
    if not base.is_relative_to(config_dir):
        raise Http404("Invalid raster_folder in config")

    path = (base / file_name).resolve()
    if not path.is_relative_to(base) or not path.is_file():
        raise Http404("Raster file not found")
    return path


def get_raster_context(
    *,
    module: str,
    indicator_slug: str,
    geography_code: str,
    data_period: str,
) -> RasterContext:
    """
    Look up Data + indicator flags and resolve the on-disk GeoTIFF path.

    When ``district_uses_state_level_raster_file`` is set for this state and
    module in config.toml, tiles are masked to the requested geography polygon.

    STATE geographies usually have no ``Data`` row; in that case the state
    GeoTIFF filename is taken from any child geography row for the same period.
    """
    geography = (
        Geography.objects.select_related(
            "parentId",
            "parentId__parentId",
        )
        .filter(code=geography_code)
        .first()
    )
    if geography is None:
        raise Http404("Geography not found")

    data_qs = Data.objects.select_related(
        "indicator",
        "geography",
        "geography__parentId",
        "geography__parentId__parentId",
    ).filter(
        module=module,
        indicator__slug=indicator_slug,
        data_period=data_period,
        indicator__is_raster_available=True,
    ).exclude(Q(raster_file="") | Q(raster_file__isnull=True))

    data = data_qs.filter(geography__code=geography_code).first()
    if data is None and geography.type == "STATE":
        data = data_qs.filter(
            geography__code__startswith=f"{geography_code}-",
        ).first()

    if data is None:
        raise Http404("Raster data not found for this request")

    state = state_geography(geography)
    path = resolve_raster_path(module, state.name, data.raster_file)

    clip_to_geography = (
        geography.type != "STATE"
        and module_uses_state_level_raster(state.name, module)
    )
    clip_geometry = None
    geography_bounds = None
    if clip_to_geography:
        geom = geography.geom or geography.simple_geom
        if geom is None:
            raise Http404("Geography geometry required for state-raster clipping")
        clip_geometry = json.loads(geom.geojson)
        west, south, east, north = geom.extent
        geography_bounds = [west, south, east, north]

    return RasterContext(
        path=path,
        file_name=data.raster_file,
        module=module,
        indicator_slug=indicator_slug,
        geography_code=geography_code,
        data_period=data_period,
        clip_geometry=clip_geometry,
        clip_to_geography=clip_to_geography,
        geography_bounds=geography_bounds,
        raster_polarity=data.indicator.raster_polarity,
    )


def _get_value_range(path: Path) -> tuple[float, float]:
    cache_key = f"raster_stats:{path}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    with _open_raster(path) as src:
        stats = src.statistics()["b1"]
        vmin = float(stats.min)
        vmax = float(stats.max)
        if vmin >= vmax:
            vmax = vmin + 1.0

    cache.set(
        cache_key,
        (vmin, vmax),
        timeout=settings.CACHE_TIMEOUTS.get("raster_stats", 60 * 60 * 24),
    )
    return vmin, vmax


def _apply_clip_mask(tile: ImageData, clip_geometry: dict) -> ImageData:
    """Mask tile pixels outside a WGS84 GeoJSON geometry."""
    geom_3857 = transform_geom("EPSG:4326", "EPSG:3857", clip_geometry)
    outside = ~geometry_mask(
        [geom_3857],
        out_shape=(tile.height, tile.width),
        transform=tile.transform,
        invert=True,
    )
    tile.array.mask = numpy.logical_or(tile.array.mask, outside)
    return tile


def raster_style(context: RasterContext) -> RasterStyle:
    return RasterStyle(raster_polarity=context.raster_polarity)


def build_tile_url_template(context: RasterContext) -> str:
    query = urlencode({
        "module": context.module,
        "indicator": context.indicator_slug,
        "geography_code": context.geography_code,
        "period": context.data_period,
    })
    return f"/raster/tiles/{{z}}/{{x}}/{{y}}.png?{query}"


def get_raster_metadata(context: RasterContext) -> dict:
    style = raster_style(context)
    vmin, vmax = _get_value_range(context.path)

    with _open_raster(context.path) as src:
        raster_bounds = list(src.bounds)

    bounds = context.geography_bounds or raster_bounds

    return {
        "module": context.module,
        "indicator": context.indicator_slug,
        "geography_code": context.geography_code,
        "period": context.data_period,
        "file": context.file_name,
        "clip_to_geography": context.clip_to_geography,
        "bounds": bounds,
        "raster_bounds": raster_bounds,
        "center": [(bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2],
        "value_min": vmin,
        "value_max": vmax,
        "raster_polarity": style.raster_polarity,
        "tile_url_template": build_tile_url_template(context),
    }


def get_raster_value(
    context: RasterContext,
    *,
    lat: float,
    lng: float,
) -> dict:
    """Sample the raw raster value at a WGS84 coordinate."""
    inside_geography = True
    if context.clip_to_geography:
        row = (
            Geography.objects.filter(code=context.geography_code)
            .values_list("geom", "simple_geom")
            .first()
        )
        if row is not None:
            g = row[0] or row[1]
            if g is not None:
                inside_geography = g.contains(Point(lng, lat, srid=4326))
        if not inside_geography:
            return {
                "module": context.module,
                "indicator": context.indicator_slug,
                "geography_code": context.geography_code,
                "period": context.data_period,
                "lat": lat,
                "lng": lng,
                "value": None,
                "inside_raster": False,
                "inside_geography": False,
            }

    vmin, vmax = _get_value_range(context.path)

    try:
        with _open_raster(context.path) as src:
            pt = src.point(lng, lat)
    except PointOutsideBounds:
        return {
            "module": context.module,
            "indicator": context.indicator_slug,
            "geography_code": context.geography_code,
            "period": context.data_period,
            "lat": lat,
            "lng": lng,
            "value": None,
            "inside_raster": False,
            "inside_geography": inside_geography,
        }

    band = pt.array[0]
    if numpy.ma.is_masked(band) or not numpy.isfinite(band):
        value = None
    else:
        value = float(band)

    return {
        "module": context.module,
        "indicator": context.indicator_slug,
        "geography_code": context.geography_code,
        "period": context.data_period,
        "lat": lat,
        "lng": lng,
        "value": value,
        "value_min": vmin,
        "value_max": vmax,
        "inside_raster": value is not None,
        "inside_geography": inside_geography,
    }


def render_raster_tile(
    context: RasterContext,
    z: int,
    x: int,
    y: int,
) -> tuple[bytes, str]:
    style = raster_style(context)
    vmin, vmax = _get_value_range(context.path)

    clip_key = "clip" if context.clip_geometry else "full"
    cache_key = (
        f"raster_tile:{context.path}:{clip_key}:{context.geography_code}:"
        f"{z}:{x}:{y}:{int(style.raster_polarity)}:rdylgn"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return cached, "image/png"

    timeout = settings.CACHE_TIMEOUTS.get("raster_tiles", 60 * 60 * 24)

    with _open_raster(context.path) as src:
        try:
            tile = src.tile(x, y, z)
        except TileOutsideBounds:
            body = _transparent_tile_png()
            cache.set(cache_key, body, timeout=timeout)
            return body, "image/png"

    if context.clip_geometry:
        tile = _apply_clip_mask(tile, context.clip_geometry)

    _mask_nan_pixels(tile)
    tile.rescale(in_range=[(vmin, vmax)], out_dtype="uint8")
    body = tile.render(colormap=style.colormap())
    cache.set(cache_key, body, timeout=timeout)
    return body, "image/png"
