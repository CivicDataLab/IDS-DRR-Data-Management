import json
import logging
from collections import defaultdict
from datetime import date

import strawberry
import strawberry_django
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.contrib.gis.db.models.aggregates import Union
from django.contrib.gis.db.models.functions import MakeValid
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
    data_dict = {}

    if indc_filter:
        dataset_obj = Data.objects.filter(
            Q(indicator__slug=indc_filter.slug)
            | Q(indicator__parent__slug=indc_filter.slug)
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

    for geo in geo_obj:
        for obj in dataset_obj.filter(geography=geo, indicator__is_visible=True):
            data_dict[obj.geography.type.lower()] = obj.geography.name
            data_dict[obj.geography.type.lower().replace(" ", "-") + "-code"] = (
                obj.geography.code
            )
            if obj.indicator.unit:
                unit = obj.indicator.unit.name
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value) + " " + unit,
                    "title": obj.indicator.name,
                }
            else:
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value),
                    "title": obj.indicator.name,
                }

        if data_dict:
            data_list.append(data_dict)
            data_dict = {}

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
    data_dict = {}
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

    # Process geography and data for each region
    for geo in geo_obj:
        for obj in data_obj.filter(geography=geo):
            data_dict["type"] = geo.type
            data_dict["region-name"] = obj.geography.name
            data_dict[obj.geography.type.lower().replace(" ", "-") + "-code"] = (
                obj.geography.code
            )
            if obj.indicator.unit:
                unit = obj.indicator.unit.name
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value) + " " + unit,
                    "title": obj.indicator.name,
                }
            else:
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value),
                    "title": obj.indicator.name,
                }

        if data_dict:
            # Reorder data_dict so that the selected indicator is first
            if indc_filter and indc_filter.slug in data_dict:
                selected_indicator = {indc_filter.slug: data_dict.pop(indc_filter.slug)}
                data_dict = {**selected_indicator, **data_dict}

            data_list.append(data_dict)
            data_dict = {}

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
    )

    # Creating initial dict structure.
    data_dict = {}
    data_dict[indc_filter.slug] = {}

    # Iterating over each data period to create a list of dicts.
    # Where each dict represents data for that district for that data period.
    for time in time_list:
        temp_dict = {}
        data_list = []
        filtered_queryset = data_queryset.filter(data_period=time)
        for data in filtered_queryset:
            temp_dict[data.geography.type.lower().replace(" ", "-")] = (
                data.geography.name
            )
            temp_dict[data.geography.type.lower().replace(" ", "-") + "-code"] = (
                data.geography.code
            )
            temp_dict[indc_filter.slug] = data.value
            data_list.append(temp_dict)
            temp_dict = {}

        data_dict[indc_filter.slug][time] = data_list

    return data_dict


@cache_query('table_data')
def get_revenue_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter | None = None,
) -> list[dict]:
    data_list = []
    data_dict = {}
    """Retrieve revenue circle-specific data based on specified filters.

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

    geo_queryset = Geography.objects.filter(code__in=geo_filter.code)

    rc_data_queryset = Data.objects.filter(
        Q(indicator__parent__slug=indc_filter.slug)
        | Q(indicator__slug=indc_filter.slug),
    )
    rc_data_queryset = rc_data_queryset.filter(data_period=data_filter.data_period)

    for geo in geo_queryset:
        for obj in rc_data_queryset.filter(geography=geo, indicator__is_visible=True):
            data_dict["type"] = obj.geography.type.lower()
            data_dict[obj.geography.type.lower().replace(" ", "-")] = obj.geography.name
            data_dict[(obj.geography.type + " code").lower().replace(" ", "-")] = (
                obj.geography.code
            )
            if obj.geography.parentId:
                parent = obj.geography.parentId
                data_dict["parent_type"] = parent.type.lower()
                data_dict[parent.type.lower().replace(" ", "-")] = parent.name
                data_dict[(parent.type.lower() + " code").lower().replace(" ", "-")] = (
                    parent.code
                )
            if obj.indicator.unit:
                unit = obj.indicator.unit.name
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value) + " " + unit,
                    "title": obj.indicator.name,
                }
            else:
                data_dict[obj.indicator.slug] = {
                    "value": str(obj.value),
                    "title": obj.indicator.name,
                }
        if data_dict:
            data_list.append(data_dict)
            data_dict = {}

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

        if rc_code in rc_data_map:
            data = rc_data_map[rc_code]
            geo_object = data.geography

            # Add parent district code to properties
            parent_code_key = (
                f"{geo_object.parentId.type.lower().replace(' ', '-')}-code"
            )
            rc["properties"][parent_code_key] = geo_object.parentId.code

            # Add indicator slug and value to properties
            rc["properties"][data.indicator.slug] = data.value

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
            data = district_data_map[district_code]

            # Add indicator slug and value to properties
            district["properties"][data.indicator.slug] = data.value

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
    indicator_list = []
    indicators = Indicators.objects.filter(parent__id=parent_id, is_visible=True)
    if state_code:
        indicators = indicators.filter(geography__code=state_code)
    for indicator in indicators:
        indicator_list.append(
            {
                "slug": indicator.slug,
                "name": indicator.name,
                "description": indicator.long_description,
                "children": get_child_indicators(indicator.id),
                "IDS_dataSpace": indicator.IDS_dataSpace,
            }
        )
    return indicator_list


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
    state_geographies = Geography.objects.filter(name_filter, type="STATE")

    states = []
    for state_geography in state_geographies:
        state_code = state_geography.code
        time_periods = list(
            Data.objects.filter(
                Q(geography__code=state_code) |
                Q(geography__parentId__code=state_code) |
                Q(geography__parentId__parentId__code=state_code)
            )
            .values_list("data_period", flat=True)
            .annotate(custom_ordering=F("data_period"))
            .distinct()
            .order_by("-custom_ordering")
        )
        valid_geometries = Geography.objects.filter(parentId=state_geography).annotate(
            valid_geom=MakeValid("geom")
        )
        state_geometry = valid_geometries.aggregate(union_geometry=Union("valid_geom"))[
            "union_geometry"
        ]
        state_centroid = state_geometry.centroid if state_geometry else None
        bounds = _leaflet_bounds(state_geometry.extent if state_geometry else None)
        grandchild = Geography.objects.filter(parentId__parentId__code=state_code).first()
        states.append({
            "name": state_geography.name,
            "slug": state_geography.slug,
            "code": state_code,
            "child_type": grandchild.type if grandchild else None,
            "center": (state_centroid.y, state_centroid.x) if state_centroid else None,
            "bounds": bounds,
            "resource_id": visible.get(state_geography.name.lower(), ""),
            "time_periods": time_periods,
            "latest_time_period": time_periods[0] if time_periods else None,
        })
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
    get_states: JSON = strawberry_django.field(resolver=get_states)


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
