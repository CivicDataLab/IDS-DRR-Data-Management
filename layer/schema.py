import json
import timeit
import typing
from datetime import datetime
from typing import Optional

import strawberry
import strawberry_django
from dateutil.relativedelta import relativedelta
from django.core.serializers import serialize
from django.db.models import F, Q
from graphql import GraphQLError
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension
import geojson

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD
from . import types
from .models import Data, Geography, Indicators
from .utils import bounding_box


# from .mutation import Mutation


def get_district_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
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

    if geo_filter:
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
    else:
        geo_obj = Geography.objects.filter(type="DISTRICT")

    for geo in geo_obj:
        for obj in dataset_obj.filter(geography=geo, indicator__is_visible=True):
            data_dict[obj.geography.type.lower()] = obj.geography.name
            data_dict[obj.geography.type.lower().replace(" ", "-") + "-code"] = (
                obj.geography.code
            )
            # if obj.indicator.unit:
            #     unit = obj.indicator.unit.name
            #     data_dict[obj.indicator.name] = str(obj.value) + " " + unit
            # else:
            #     data_dict[obj.indicator.name] = str(obj.value)
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

    # filter_key = Indicators.objects.get(slug=indc_filter.slug)
    # data_list = sorted(data_list, key=lambda d: d[filter_key.name], reverse=True)
    # data_list = sorted(data_list, key=lambda d: d[indc_filter.slug], reverse=True)
    data_list = sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_list  # {"table_data": data_list}


def get_table_data(
    indc_filter: Optional[types.IndicatorFilter]= None,
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
        indicator__slug=indc_filter.slug,
        geography__code__in=geo_filter.code,
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
            temp_dict[data.geography.type.lower()] = data.geography.name
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

    geo_queryset = Geography.objects.filter(type="REVENUE CIRCLE")
    if geo_filter:
        geo_queryset = geo_queryset.filter(code__in=geo_filter.code)

    # rc_data_queryset = Data.objects.all()
    rc_data_queryset = Data.objects.filter(
        Q(indicator__parent__slug=indc_filter.slug)
        | Q(indicator__slug=indc_filter.slug),
        # data_period=data_filter.data_period,
    )
    rc_data_queryset = rc_data_queryset.filter(data_period=data_filter.data_period)

    for geo in geo_queryset:
        # filtered_queryset = rc_data_queryset.filter(
        #     geography=geo, data_period=data_filter.data_period
        # )
        # if indc_filter:
        #     filtered_queryset = filtered_queryset.filter(
        #         Q(indicator__parent__slug=indc_filter.slug)
        #         | Q(indicator__slug=indc_filter.slug)
        #     )
        # if filtered_queryset.exists():
        for obj in rc_data_queryset.filter(geography=geo, indicator__is_visible=True):
            # for obj in filtered_queryset:
            data_dict[obj.geography.type.lower().replace(" ", "-")] = obj.geography.name
            data_dict[(obj.geography.type + " code").lower().replace(" ", "-")] = (
                obj.geography.code
            )
            if obj.geography.parentId:
                parent = obj.geography.parentId
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
                # data_dict[obj.indicator.name] = str(obj.value)
        if data_dict:
            data_list.append(data_dict)
            data_dict = {}

    # filter_key = Indicators.objects.get(slug=indc_filter.slug)
    # data_list = sorted(data_list, key=lambda d: d[filter_key.name], reverse=True)
    # data_list = sorted(data_list, key=lambda d: d[indc_filter.slug], reverse=True)
    data_list = sorted(
        data_list,
        key=lambda d: float(d[indc_filter.slug]["value"].split()[0]),
        reverse=True,
    )

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_list  # {"table_data": data_list}


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
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        dict: A GeoJSON-like dictionary representing revenue circle features with
        associated indicator data.
    """
    starttime = timeit.default_timer()

    # Convert geography objects to a GeoJson format.
    try:
        geo_object = Geography.objects.get(code__in=geo_filter.code, type="STATE")
        if geo_object.name.title() == "Himachal Pradesh":
            geo_type = "SUB DISTRICT"
        else:
            geo_type = "REVENUE CIRCLE"
    except Geography.DoesNotExist:
        raise GraphQLError("Invalid state code!!")

    geo_json = json.loads(
        serialize(
            "geojson",
            Geography.objects.filter(
                type=geo_type, parentId__parentId__code__in=geo_filter.code
            ),
        )
    )

    rc_data = Data.objects.filter(
        indicator__slug=indc_filter.slug,
        data_period=data_filter.data_period,
        geography__type=geo_type,
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
            district["properties"]["bounds"] = bounding_box(list(geojson.utils.coords(poly)))

            # Add indicator slug and value to properties
            district["properties"][data.indicator.slug] = data.value

            # Remove unnecessary keys
            district["properties"].pop("parentId", None)
            district["properties"].pop("pk", None)
            district.pop("id", None)

    print("The time difference is :", timeit.default_timer() - starttime)
    return geo_json


def get_indicators(indc_filter: Optional[types.IndicatorFilter] = None) -> list:
    # TODO: Return obj rather than formatted dict. [Faster]
    """Return a list of indicators and associated data from the 'indicator' table.

    Args:
        indc_filter (types.IndicatorFilter, optional): An IndicatorFilter object used
        to filter data based on defined fields from types.py. Defaults to None.

    Returns:
        list: A list of dictionaries representing indicators and related data.
    """
    starttime = timeit.default_timer()
    data_list = []

    indc_obj = Indicators.objects.filter(is_visible=True)
    if indc_filter:
        indc_obj = indc_obj.filter(
            Q(slug=indc_filter.slug) | Q(parent__slug=indc_filter.slug)
        )

    data_queryset = indc_obj.values(
        "name", "slug", "long_description", "short_description", "data_source", "unit__name"
    )
    for data in data_queryset:
        data_list.append(data)

    print("The time difference is :", timeit.default_timer() - starttime)
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
        "SUB DISTRICT",
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
                            f"{data.type.lower().replace(' ', '-')}": rc_data.name,
                            "code": rc_data.code,
                            "district_code": data.parentId.code,
                        }
                    )
                data_dict[f"{data.parentId.name}"] = data_list
                data_list = []

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    # geography: list[types.Geography] = strawberry_django.field()
    # department: list[types.Department] = strawberry.django.field()
    # scheme: list[types.Scheme] = strawberry_django.field()
    indicators: JSON = strawberry_django.field(resolver=get_indicators)
    # indicatorsByCategory: JSON = strawberry_django.field(resolver=get_categories)
    # getFactors: JSON = strawberry_django.field(resolver=get_model_indicators)
    # data: list[types.Data] = strawberry_django.field()
    districtViewData: JSON = strawberry_django.field(resolver=get_district_data)
    tableData: JSON = strawberry_django.field(resolver=get_table_data)
    districtMapData: JSON = strawberry_django.field(resolver=get_district_map_data)
    getTimeTrends: JSON = strawberry_django.field(resolver=get_time_trends)
    revCircleViewData: JSON = strawberry_django.field(resolver=get_revenue_data)
    revCircleMapData: JSON = strawberry_django.field(resolver=get_revenue_map_data)
    # revCircleTimeTrends: JSON = strawberry_django.field(resolver=get_revenue_chart_data)
    getDataTimePeriods: list[types.CustomDataPeriodList] = strawberry_django.field(
        resolver=get_timeperiod
    )
    getDistrictRevCircle: JSON = strawberry_django.field(
        resolver=get_district_rev_circle
    )
    # barChart: types.BarChart = strawberry.django.field(resolver=get_bar_data)


schema = strawberry.Schema(
    query=Query,
    # mutation=Mutation,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
