import json
import timeit
import typing
from datetime import datetime
from typing import Optional

import strawberry
import strawberry_django
from dateutil.relativedelta import relativedelta
from django.contrib.gis.db.models.functions import Centroid, MakeValid
from django.contrib.gis.db.models.aggregates import Union
from django.core.serializers import serialize
from django.db.models import F, Q
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension
import geojson

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD
from . import types
from layer.models import Data, Geography, Indicators
from D4D_ContextLayer.settings import STATE_LIST

# from .mutation import Mutation


def bounding_box(coord_list):
    box = []
    for i in (0, 1):
        res = sorted(coord_list, key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))
    ret = [[box[1][0], box[0][0]], [box[1][1], box[0][1]]]
    return ret


def get_district_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter,
) -> list[dict]:
    """Retrieve district-specific data based on specified filters.

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
    starttime = timeit.default_timer()
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

    data_list = sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_list


def get_table_data(
    indc_filter: Optional[types.IndicatorFilter] = None,
    data_filter: Optional[types.DataFilter] = None,
    geo_filter: Optional[types.GeoFilter] = None,
) -> list[dict]:
    """Retrieve data to be displayed on table based on specified filters.

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
    starttime = timeit.default_timer()
    data_list = []
    data_dict = {}
    data_obj = Data.objects.filter(indicator__is_visible=True)

    # Filter by time period
    if data_filter:
        data_obj = data_obj.filter(data_period=data_filter.data_period)
    else:
        data_obj = data_obj.filter(data_period=DEFAULT_TIME_PERIOD)

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
    data_list = sorted(data_list, key=lambda d: d.get("type") != "DISTRICT")

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_list


def get_time_trends(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: types.GeoFilter,
) -> dict:
    starttime = timeit.default_timer()
    """Retrieve time trends data based on specified filters.

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
    # Parse the string into a datetime object.
    date_format = "%Y_%m"
    datetime_object = datetime.strptime(data_filter.data_period, date_format)
    time_list = []

    # Get the list of data periods for the required time range.
    if data_filter.period == "3M":
        for i in range(0, 4):
            tme = datetime_object - relativedelta(months=i)
            time_list.append(tme.strftime("%Y_%m"))
        time_list.reverse()
    elif data_filter.period == "1Y":
        for i in range(0, 13):
            tme = datetime_object - relativedelta(months=i)
            time_list.append(tme.strftime("%Y_%m"))
        time_list.reverse()
    else:
        list_queryset = (
            Data.objects.values_list("data_period", flat=True)
            .annotate(custom_ordering=F("data_period"))
            .distinct()
            .order_by("custom_ordering")
        )
        time_list = [time for time in list_queryset]

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

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


def get_revenue_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
) -> list[dict]:
    starttime = timeit.default_timer()
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

    # try:
    #     # Set the type filter based on state.
    #     geo_obj = Geography.objects.get(code__in=geo_filter.state_code, type="STATE")
    #     if geo_obj.name.title() == "Himachal Pradesh":
    #         geo_type = "TEHSIL"
    #     else:
    #         geo_type = "REVENUE CIRCLE"
    #
    #     # Geo object to iterate over.
    #     geo_queryset = Geography.objects.filter(type=geo_type)
    # except Geography.DoesNotExist:
    #     raise GraphQLError("Invalid state code!!")

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

    data_list = sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_list


def get_revenue_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
) -> dict:
    """Retrieve revenue-circle map data based on specified filters.

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
    starttime = timeit.default_timer()

    # Convert geography objects to a GeoJson format.
    # try:
    #     geo_object = Geography.objects.get(code__in=geo_filter.code, type="STATE")
    #     if geo_object.name.title() == "Himachal Pradesh":
    #         geo_type = "TEHSIL"
    #     else:
    #         geo_type = "REVENUE CIRCLE"
    # except Geography.DoesNotExist:
    #     raise GraphQLError("Invalid state code!!")

    geo_json = json.loads(
        serialize(
            "geojson",
            Geography.objects.filter(parentId__parentId__code__in=geo_filter.code),
        )
    )

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

    print("The time difference is :", timeit.default_timer() - starttime)
    return geo_json


def get_district_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
) -> dict:
    """Retrieve district map data based on specified filters.

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
    starttime = timeit.default_timer()

    # Convert geography objects to a GeoJson format.
    geo_json = json.loads(
        serialize(
            "geojson",
            Geography.objects.filter(
                type="DISTRICT", parentId__code__in=geo_filter.code
            ),
        )
    )

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
        if district_code in district_data_map:
            data = district_data_map[district_code]

            # Add bounding box of district
            poly = geojson.Polygon(district["geometry"]["coordinates"])
            district["properties"]["bounds"] = bounding_box(
                list(geojson.utils.coords(poly))
            )

            # Add indicator slug and value to properties
            district["properties"][data.indicator.slug] = data.value

            # Remove unnecessary keys
            district["properties"].pop("parentId", None)
            district["properties"].pop("pk", None)
            district.pop("id", None)

    print("The time difference is :", timeit.default_timer() - starttime)
    return geo_json


def get_indicators(
    indc_filter: Optional[types.IndicatorFilter] = None,
    state_code: Optional[int] = None,
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
            'data_source', and 'unit__name'.

    Note:
        The function also prints the execution time, which might be useful for performance monitoring.
    """
    start_time = timeit.default_timer()
    data_list = []

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
    )
    for data in data_queryset:
        data_list.append(data)

    print("The time difference is :", timeit.default_timer() - start_time)
    return data_list


# def get_model_indicators() -> list:
#     data_list = []

#     indc_obj = Indicators.objects.filter(
#         Q(parent__slug="risk-score") | Q(slug="risk-score")
#     )
#     for data in indc_obj:
#         data_list.append({"name": data.name, "slug": data.slug})

#     return data_list


def get_timeperiod():
    # Use annotation to create a custom field for sorting
    data = (
        Data.objects.values_list("data_period", flat=True)
        .annotate(custom_ordering=F("data_period"))
        .distinct()
        .order_by("-custom_ordering")
    )

    # Create CustomDataPeriodList objects directly in the query
    time_list = [types.CustomDataPeriodList(value=time) for time in data]
    # for time in data:
    #     time_list.append({"value":time})

    return time_list


def get_district_rev_circle(geo_filter: types.GeoFilter):
    starttime = timeit.default_timer()
    data_dict = {}
    data_list = []

    if geo_filter.type.upper() == "DISTRICT":
        geo_object = Geography.objects.filter(
            type=geo_filter.type.upper().strip().replace("-", " "),
            parentId__code__in=geo_filter.code,
        )
        for data in geo_object:
            data_list.append(
                {
                    f"{data.type.lower().replace(' ', '-')}": data.name,
                    "code": data.code,
                }
            )
        data_dict = data_list
    elif geo_filter.type.upper().strip().replace("-", " ") in [
        "REVENUE CIRCLE",
        "TEHSIL",
        "BLOCK",
    ]:
        geo_object = Geography.objects.filter(
            type=geo_filter.type.upper().strip().replace("-", " ")
        )
        for data in geo_object:
            rc_obj = geo_object.filter(parentId=data.parentId)
            if rc_obj.exists():
                for rc_data in rc_obj:
                    data_list.append(
                        {
                            f"{data.type}": rc_data.name,
                            "code": rc_data.code,
                            "district_code": data.parentId.code,
                        }
                    )
                data_dict[f"{data.parentId.name}"] = data_list
                data_list = []

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


def get_child_indicators(
    parent_id: Optional[int] = None, state_code: Optional[str] = None
) -> typing.List:
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
            }
        )
    return indicator_list


def get_states():
    try:
        with open("report_config.json", "r") as f:
            STATE_CONFIG_ALL = json.load(f)
    except FileNotFoundError:
        print("Configuration file not found in get states function.")
        return []

    all_states = Geography.objects.filter(type="STATE", code__in=STATE_LIST)
    states = []
    for state in all_states:
        state_details = {
            "name": state.name,
            "slug": state.slug,
            "code": state.code,
            "child_type": Geography.objects.filter(parentId__parentId__code=state.code)
            .first()
            .type,
        }
        valid_geometries = Geography.objects.filter(parentId=state).annotate(
            valid_geom=MakeValid("geom")
        )
        state_geometry = valid_geometries.aggregate(union_geometry=Union("valid_geom"))[
            "union_geometry"
        ]
        state_centroid = state_geometry.centroid if state_geometry else None
        state_details["center"] = (state_centroid.y, state_centroid.x)
        state_details["resource_id"] = STATE_CONFIG_ALL[state.code]["RESOURCE_ID"]
        states.append(state_details)
    return states


@strawberry.type
class Query:  # camelCase
    indicators: JSON = strawberry_django.field(resolver=get_indicators)
    districtViewData: JSON = strawberry_django.field(resolver=get_district_data)
    tableData: JSON = strawberry_django.field(resolver=get_table_data)
    indicatorsByCategory: JSON = strawberry_django.field(resolver=get_child_indicators)
    districtMapData: JSON = strawberry_django.field(resolver=get_district_map_data)
    getTimeTrends: JSON = strawberry_django.field(resolver=get_time_trends)
    revCircleViewData: JSON = strawberry_django.field(resolver=get_revenue_data)
    revCircleMapData: JSON = strawberry_django.field(resolver=get_revenue_map_data)
    getDataTimePeriods: list[types.CustomDataPeriodList] = strawberry_django.field(
        resolver=get_timeperiod
    )
    getDistrictRevCircle: JSON = strawberry_django.field(
        resolver=get_district_rev_circle
    )
    getStates: JSON = strawberry_django.field(resolver=get_states)


schema = strawberry.Schema(
    query=Query,
    # mutation=Mutation,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
