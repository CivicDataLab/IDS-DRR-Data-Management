import json
import logging
from collections import defaultdict
from datetime import date

import strawberry
import strawberry_django
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.contrib.gis.db.models.aggregates import Extent
from django.core.serializers import serialize
from django.db.models import F, Q
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension

from layer.cache_utils import cache_query
from layer.models import Data, Geography, Indicators

from . import types

logger = logging.getLogger(__name__)


def _leaflet_bounds(extent):
    """Convert a PostGIS extent (xmin, ymin, xmax, ymax) into Leaflet's [[S, W], [N, E]]."""
    if extent:
        return [[extent[1], extent[0]], [extent[3], extent[2]]]
    return None


def _group_data_by_geography_code(
    queryset,
    geographies: list[Geography],
    *,
    select_related: tuple[str, ...],
) -> defaultdict[str, list]:
    """Get matching Data rows in one query and group them by geography code."""
    code_by_id = {g.id: g.code for g in geographies}
    qs = queryset.filter(geography__in=geographies).select_related(*select_related)
    grouped = defaultdict(list)
    for data in qs:
        grouped[code_by_id[data.geography_id]].append(data)
    return grouped


@cache_query('table_data')
def get_district_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter,
) -> list[dict]:
    """
    Retrieve district-specific data based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        list[dict]: A list containing dictionary of districts
            mapping each to it's relevant data fields.

    """
    data_list = []

    if indc_filter:
        dataset_obj = Data.objects.filter(
            Q(indicator__slug=indc_filter.slug)
            | Q(indicator__parent__slug=indc_filter.slug),
            indicator__is_visible=True,
        )
    if data_filter:
        dataset_obj = dataset_obj.filter(data_period=data_filter.data_period)

    if len(geo_filter.code) <= 1:
        dataset_obj = dataset_obj.filter(
            Q(geography__parentId__code__in=geo_filter.code)
            | Q(geography__code__in=geo_filter.code)
        )
        geo_obj = Geography.objects.filter(
            Q(code__in=geo_filter.code) | Q(parentId__code__in=geo_filter.code)
        )
    else:
        geo_obj = Geography.objects.filter(code__in=geo_filter.code)

    geographies = list(geo_obj)
    data_by_geography_code = _group_data_by_geography_code(
        dataset_obj,
        geographies,
        select_related=("indicator", "indicator__unit"),
    )

    for geo in geographies:
        rows = data_by_geography_code[geo.code]
        if not rows:
            continue

        geography_type = geo.type.lower()
        data_dict = {
            geography_type: geo.name,
            f"{geography_type.replace(' ', '-')}-code": geo.code,
        }
        for obj in rows:
            unit = obj.indicator.unit.name if obj.indicator.unit else ""
            data_dict[obj.indicator.slug] = {
                "value": f"{obj.value} {unit}" if unit else str(obj.value),
                "title": obj.indicator.name,
            }
        data_list.append(data_dict)

    return sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )



@cache_query('table_data')
def get_table_data(
    indc_filter: types.IndicatorFilter | None = None,
    data_filter: types.DataFilter | None = None,
    geo_filter: types.GeoFilter | None = None,
) -> list[dict]:
    """
    Retrieve data to be displayed on table based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter, Optional): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter, Optional): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        list[dict]: A list containing dictionary of districts
            mapping each to it's relevant data fields.

    """
    data_list = []
    data_obj = Data.objects.filter(indicator__is_visible=True)

    # Filter by time period
    if data_filter:
        data_obj = data_obj.filter(data_period=data_filter.data_period)
    else:
        data_obj = data_obj.filter(data_period=settings.DEFAULT_TIME_PERIOD)

    # Filter by indicator
    if indc_filter:
        data_obj = data_obj.filter(
            Q(indicator__slug=indc_filter.slug)
            | Q(indicator__parent__slug=indc_filter.slug)
        )
    else:
        data_obj = data_obj.filter(indicator__parent__parent=None)

    # Filter by geography
    if geo_filter:
        if len(geo_filter.code) <= 1:
            data_obj = data_obj.filter(
                Q(geography__parentId__code__in=geo_filter.code)
                | Q(geography__code__in=geo_filter.code)
            )
            geo_obj = Geography.objects.filter(
                Q(code__in=geo_filter.code) | Q(parentId__code__in=geo_filter.code)
            )
        else:
            geo_obj = Geography.objects.filter(code__in=geo_filter.code)
    else:
        geo_obj = Geography.objects.filter(type="DISTRICT")

    geographies = list(geo_obj)
    data_by_geography_code = _group_data_by_geography_code(
        data_obj,
        geographies,
        select_related=("indicator", "indicator__unit"),
    )

    # Process geography and data for each region
    for geo in geographies:
        rows = data_by_geography_code[geo.code]
        if not rows:
            continue

        data_dict = {
            "type": geo.type,
            "region-name": geo.name,
            f"{geo.type.lower().replace(' ', '-')}-code": geo.code,
        }
        for obj in rows:
            unit = obj.indicator.unit.name if obj.indicator.unit else ""
            data_dict[obj.indicator.slug] = {
                "value": f"{obj.value} {unit}" if unit else str(obj.value),
                "title": obj.indicator.name,
            }

        # Reorder data_dict so that the selected indicator is first
        if indc_filter and indc_filter.slug in data_dict:
            selected_indicator = {indc_filter.slug: data_dict.pop(indc_filter.slug)}
            data_dict = {**selected_indicator, **data_dict}

        data_list.append(data_dict)

    # Prioritize district values at the top
    return sorted(data_list, key=lambda d: d.get("type") != "DISTRICT")



@cache_query('time_trends')
def get_time_trends(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter,
) -> dict:
    """
    Retrieve time trends data based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        to filter data based on defined fields from types.py.

    Returns:
        dict: A dictionary containing time trends data aggregated for each
        timestamp based on the specified filters.

    """
    # Parse the "YYYY_MM" period into a date (first of the month).
    year, month = (int(part) for part in data_filter.data_period.split("_"))
    date_object = date(year, month, 1)
    time_list = []

    # Get the list of data periods for the required time range.
    if data_filter.period == "3M":
        for i in range(4):
            tme = date_object - relativedelta(months=i)
            time_list.append(tme.strftime("%Y_%m"))
        time_list.reverse()
    elif data_filter.period == "1Y":
        for i in range(13):
            tme = date_object - relativedelta(months=i)
            time_list.append(tme.strftime("%Y_%m"))
        time_list.reverse()
    else:
        list_queryset = (
            Data.objects.values_list("data_period", flat=True)
            .annotate(custom_ordering=F("data_period"))
            .distinct()
            .order_by("custom_ordering")
        )
        time_list = list(list_queryset)

    # Filter the data.
    data_queryset = Data.objects.filter(
        Q(geography__parentId__code__in=geo_filter.code)
        | Q(geography__parentId__parentId__code__in=geo_filter.code)
        | Q(geography__code__in=geo_filter.code),
        indicator__slug=indc_filter.slug,
        data_period__in=time_list,
    ).select_related("geography")
    data_by_period = defaultdict(list)
    for data in data_queryset:
        data_by_period[data.data_period].append(data)

    # Creating initial dict structure.
    data_dict = {}
    data_dict[indc_filter.slug] = {}

    # Iterating over each data period to create a list of dicts.
    # Where each dict represents data for that district for that data period.
    for time in time_list:
        data_list = []
        for data in data_by_period[time]:
            geography_type_slug = data.geography.type.lower().replace(" ", "-")
            data_list.append({
                geography_type_slug: data.geography.name,
                f"{geography_type_slug}-code": data.geography.code,
                indc_filter.slug: data.value,
            })

        data_dict[indc_filter.slug][time] = data_list

    return data_dict


@cache_query('table_data')
def get_revenue_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter | None = None,
) -> list[dict]:
    """
    Retrieve revenue circle-specific data based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        list[dict]: A list containing dictionary of revenue circles
            mapping each to it's relevant data fields.

    """
    data_list = []

    geo_queryset = Geography.objects.filter(
        code__in=geo_filter.code
    ).select_related("parentId")

    rc_data_queryset = Data.objects.filter(
        Q(indicator__parent__slug=indc_filter.slug)
        | Q(indicator__slug=indc_filter.slug),
        indicator__is_visible=True,
    )
    rc_data_queryset = rc_data_queryset.filter(data_period=data_filter.data_period)

    geographies = list(geo_queryset)
    data_by_geography_code = _group_data_by_geography_code(
        rc_data_queryset,
        geographies,
        select_related=("indicator", "indicator__unit"),
    )

    for geo in geographies:
        rows = data_by_geography_code[geo.code]
        if not rows:
            continue

        geography_type = geo.type.lower()
        geography_type_slug = geography_type.replace(" ", "-")
        data_dict = {
            "type": geography_type,
            geography_type_slug: geo.name,
            f"{geography_type_slug}-code": geo.code,
        }
        if geo.parentId:
            parent = geo.parentId
            parent_type = parent.type.lower()
            parent_type_slug = parent_type.replace(" ", "-")
            data_dict["parent_type"] = parent_type
            data_dict[parent_type_slug] = parent.name
            data_dict[f"{parent_type_slug}-code"] = parent.code

        for obj in rows:
            unit = obj.indicator.unit.name if obj.indicator.unit else ""
            data_dict[obj.indicator.slug] = {
                "value": f"{obj.value} {unit}" if unit else str(obj.value),
                "title": obj.indicator.name,
            }

        data_list.append(data_dict)

    return sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )



@cache_query('map_data')
def get_revenue_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter | None = None,
) -> dict:
    """
    Retrieve revenue-circle map data based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        IMP: The code sent is statecode
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        dict: A GeoJSON-like dictionary representing revenue circle features with
        associated indicator data.

    """
    rcs = list(
        Geography.objects
        .filter(parentId__parentId__code__in=geo_filter.code)
        .select_related("parentId")
    )
    extent_by_code = {g.code: g.geom.extent if g.geom else None for g in rcs}
    geography_by_code = {g.code: g for g in rcs}
    geo_json = json.loads(serialize("geojson", rcs))

    rc_data = Data.objects.filter(
        indicator__slug=indc_filter.slug,
        data_period=data_filter.data_period,
        geography__parentId__parentId__code__in=geo_filter.code,
    ).select_related("geography")

    # Create a dictionary to store indicator data by geography code
    rc_data_map = {data.geography.code: data for data in rc_data}

    # Iterate over GeoJSON features and populate with indicator data
    for rc in geo_json["features"]:
        rc_code = rc["properties"]["code"]

        rc["properties"]["bounds"] = _leaflet_bounds(extent_by_code.get(rc_code))

        geo_object = geography_by_code.get(rc_code)
        if geo_object and geo_object.parentId:
            parent_code_key = (
                f"{geo_object.parentId.type.lower().replace(' ', '-')}-code"
            )
            rc["properties"][parent_code_key] = geo_object.parentId.code

        if rc_code in rc_data_map:
            # Add indicator slug and value to properties
            rc["properties"][indc_filter.slug] = rc_data_map[rc_code].value

        # Remove unnecessary keys
        rc["properties"].pop("parentId", None)
        rc["properties"].pop("pk", None)
        rc.pop("id", None)

    return geo_json


@cache_query('map_data')
def get_district_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter | None = None,
) -> dict:
    """
    Retrieve district map data based on specified filters.

    Args:
        indc_filter (types.IndicatorFilter): An IndicatorFilter object used
        to filter data based on defined fields from types.py.
        data_filter (types.DataFilter): An DataFilter object used
        to filter data based on defined fields from types.py.
        geo_filter (types.GeoFilter, optional): An GeoFilter object used
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        dict: A GeoJSON-like dictionary representing district features with
        associated indicator data.

    """
    # Convert geography objects to a GeoJson format.
    districts = list(
        Geography.objects.filter(type="DISTRICT", parentId__code__in=geo_filter.code)
    )
    extent_by_code = {g.code: g.geom.extent if g.geom else None for g in districts}
    geo_json = json.loads(serialize("geojson", districts))

    # Get Indicator Data for each district.
    district_data = Data.objects.filter(
        indicator__slug=indc_filter.slug,
        data_period=data_filter.data_period,
        geography__type="DISTRICT",
        geography__parentId__code__in=geo_filter.code,
    ).select_related("geography")

    # Create a dictionary to store indicator data by geography code
    district_data_map = {data.geography.code: data for data in district_data}

    # Iterate over GeoJSON features and populate with indicator data
    for district in geo_json["features"]:
        district_code = district["properties"]["code"]

        district["properties"]["bounds"] = _leaflet_bounds(
            extent_by_code.get(district_code)
        )

        if district_code in district_data_map:
            # Add indicator slug and value to properties
            district["properties"][indc_filter.slug] = (
                district_data_map[district_code].value
            )

        # Remove unnecessary keys
        district["properties"].pop("parentId", None)
        district["properties"].pop("pk", None)
        district.pop("id", None)

    return geo_json


@cache_query('indicators')
def get_indicators(
    indc_filter: types.IndicatorFilter | None = None,
    state_code: str | None = None,
) -> list:
    """
    Retrieve a list of indicators and associated data from the 'indicator' table.

    This function fetches indicators from the database, optionally filtered by the provided
    IndicatorFilter. It returns a list of dictionaries containing details about each indicator.

    Args:
        indc_filter (Optional[types.IndicatorFilter]): An optional IndicatorFilter object used
            to filter indicators based on defined fields from types.py. If provided, the function
            will filter indicators by slug or parent slug. Defaults to None.
        state_code (int): An integer representing the state code. Defaults to 18.

    Returns:
        list: A list of dictionaries, where each dictionary represents an indicator and contains
            the following keys: 'name', 'slug', 'long_description', 'short_description',
            'data_source', 'unit__name', and 'IDS_dataSpace'.

    Note:
        The function also prints the execution time, which might be useful for performance monitoring.

    """
    indcators = Indicators.objects.filter(is_visible=True)
    if state_code:
        indcators = indcators.filter(geography__code=state_code)
    if indc_filter:
        indcators = indcators.filter(
            Q(slug=indc_filter.slug) | Q(parent__slug=indc_filter.slug)
        )

    data_queryset = indcators.values(
        "name",
        "slug",
        "long_description",
        "short_description",
        "data_source",
        "unit__name",
        "IDS_dataSpace",
    )
    return list(data_queryset)


@cache_query('time_periods')
def get_timeperiod():
    # Use annotation to create a custom field for sorting
    data = (
        Data.objects.values_list("data_period", flat=True)
        .annotate(custom_ordering=F("data_period"))
        .distinct()
        .order_by("-custom_ordering")
    )

    # Create CustomDataPeriodList objects directly in the query
    return [types.CustomDataPeriodList(value=time) for time in data]



@cache_query('map_data')
def get_district_rev_circle(geo_filter: types.GeoFilter):
    normalized_type = geo_filter.type.upper().strip().replace("-", " ")

    # List of districts within the states in geo_filter.code.
    if normalized_type == "DISTRICT":
        districts = Geography.objects.filter(
            type=normalized_type,
            parentId__code__in=geo_filter.code,
        )
        return [
            {
                district.type.lower().replace(" ", "-"): district.name,
                "code": district.code,
            }
            for district in districts
        ]

    # Dict of each district's subdistricts, within the states in geo_filter.code.
    if normalized_type in settings.CONFIG.get("subdistrict_types", []):
        subdistricts = Geography.objects.filter(
            type=normalized_type,
            parentId__parentId__code__in=geo_filter.code,
        ).select_related("parentId")
        result = defaultdict(list)
        for subdistrict in subdistricts:
            result[subdistrict.parentId.name].append(
                {
                    subdistrict.type: subdistrict.name,
                    "code": subdistrict.code,
                    "district_code": subdistrict.parentId.code,
                }
            )
        return dict(result)

    return {}


@cache_query('indicators')
def get_child_indicators(
    parent_id: int | None = None, state_code: str | None = None
) -> list:
    visible = Indicators.objects.filter(is_visible=True)
    if state_code:
        visible = visible.filter(geography__code=state_code)

    children_by_parent_id = defaultdict(list)
    for indicator in visible:
        if indicator.parent_id is not None:
            children_by_parent_id[indicator.parent_id].append(indicator)

    def build(indicator):
        return {
            "slug": indicator.slug,
            "name": indicator.name,
            "description": indicator.long_description,
            "children": [build(child) for child in children_by_parent_id[indicator.id]],
            "IDS_dataSpace": indicator.IDS_dataSpace,
        }

    return [build(indicator) for indicator in visible.filter(parent__id=parent_id)]


@cache_query('states')
def get_states():
    specs = settings.CONFIG.get("states", [])
    visible = {spec["name"].lower(): spec.get("resource_id", "") for spec in specs if not spec.get("hidden", False)}
    if not visible:
        logger.error("No [[states]] configured." if not specs else "All [[states]] entries are hidden.")
        return []

    name_filter = Q()
    for name in visible:
        name_filter |= Q(name__iexact=name)
    state_geographies = list(Geography.objects.filter(name_filter, type="STATE"))
    if not state_geographies:
        return []

    root_state_code_by_id = {}
    state_code_by_id = {geo.id: geo.code for geo in state_geographies}
    parent_by_id = dict(Geography.objects.values_list("id", "parentId"))
    for geography_id in parent_by_id:
        # INVARIANT: Every geography roots at a STATE (the only parentId=NULL rows).
        root_id = geography_id
        while parent_by_id[root_id] is not None:
            root_id = parent_by_id[root_id]
        # Omit any non-visible or unconfigured states.
        state_code = state_code_by_id.get(root_id)
        if state_code is not None:
            root_state_code_by_id[geography_id] = state_code

    periods_by_state_code = defaultdict(set)
    # NOTE: If the database were multi-tenant (e.g. multiple frontends),
    # then it would be faster to add a geography_id__in filter.
    for geography_id, data_period in (
        Data.objects.values_list("geography_id", "data_period").distinct()
    ):
        state_code = root_state_code_by_id.get(geography_id)
        if state_code is not None:
            periods_by_state_code[state_code].add(data_period)
    periods_by_state_code = {
        state_code: sorted(periods, reverse=True)
        for state_code, periods in periods_by_state_code.items()
    }

    bbox_by_state_id = dict(
        Geography.objects.filter(parentId__in=state_geographies)
        .values_list("parentId")
        .annotate(bbox=Extent("geom"))
    )

    child_type_by_state_id = dict(
        Geography.objects.filter(parentId__parentId__in=state_geographies)
        .values_list("parentId__parentId", "type")
        .distinct()
    )

    states = []
    for state_geography in state_geographies:
        bbox = bbox_by_state_id.get(state_geography.id)
        time_periods = periods_by_state_code.get(state_geography.code, [])
        states.append(types.State(
            name=state_geography.name,
            slug=state_geography.slug,
            code=state_geography.code,
            child_type=child_type_by_state_id.get(state_geography.id),
            center=[(bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2]
            if bbox
            else None,
            bounds=_leaflet_bounds(bbox),
            resource_id=visible.get(state_geography.name.lower(), ""),
            time_periods=time_periods,
            latest_time_period=time_periods[0] if time_periods else None,
        ))
    return states


@strawberry.type
class Query:
    indicators: JSON = strawberry_django.field(resolver=get_indicators)
    district_view_data: JSON = strawberry_django.field(resolver=get_district_data)
    table_data: JSON = strawberry_django.field(resolver=get_table_data)
    indicators_by_category: JSON = strawberry_django.field(resolver=get_child_indicators)
    district_map_data: JSON = strawberry_django.field(resolver=get_district_map_data)
    get_time_trends: JSON = strawberry_django.field(resolver=get_time_trends)
    rev_circle_view_data: JSON = strawberry_django.field(resolver=get_revenue_data)
    rev_circle_map_data: JSON = strawberry_django.field(resolver=get_revenue_map_data)
    get_data_time_periods: list[types.CustomDataPeriodList] = strawberry_django.field(
        resolver=get_timeperiod
    )
    get_district_rev_circle: JSON = strawberry_django.field(
        resolver=get_district_rev_circle
    )
    get_states: list[types.State] = strawberry_django.field(resolver=get_states)


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
