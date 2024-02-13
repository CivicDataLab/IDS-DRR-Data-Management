import json
import timeit
from typing import Optional

import strawberry
import strawberry_django
from django.core.serializers import serialize
from django.db.models import F, Max, Q
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension

from . import types
from .models import Data, Geography, Indicators

# from .mutation import Mutation


def get_district_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
) -> list:
    starttime = timeit.default_timer()
    data_list = []
    data_dict = {}

    # dataset_obj = Data.objects.all()  # filter(geography__type="DISTRICT")
    if indc_filter:
        dataset_obj = Data.objects.filter(
            Q(indicator__slug=indc_filter.slug)
            | Q(indicator__parent__slug=indc_filter.slug)
        )  # .order_by("-value")
    if data_filter:
        dataset_obj = dataset_obj.filter(
            data_period=data_filter.data_period
        )  # .order_by("-value")

    if geo_filter:
        if len(geo_filter.code) <= 1:
            dataset_obj = dataset_obj.filter(
                Q(geography__parentId__code__in=geo_filter.code)
                | Q(geography__code__in=geo_filter.code)
            )  # .order_by("-value")
            geo_obj = Geography.objects.filter(
                Q(code__in=geo_filter.code) | Q(parentId__code__in=geo_filter.code)
            )
        else:
            geo_obj = Geography.objects.filter(code__in=geo_filter.code)
    else:
        geo_obj = Geography.objects.filter(type="DISTRICT")

    for geo in geo_obj:
        for obj in dataset_obj.filter(geography=geo):
            data_dict[obj.geography.type.lower()] = obj.geography.name
            data_dict[
                obj.geography.type.lower().replace(" ", "-") + "-code"
            ] = obj.geography.code
            data_dict[obj.indicator.slug] = obj.value

        if data_dict:
            data_list.append(data_dict)
            data_dict = {}

    print("The time difference is :", timeit.default_timer() - starttime)
    return {"table_data": data_list}


def get_district_chart_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
):
    starttime = timeit.default_timer()

    # Creating initial dict structure.
    data_dict = {}
    data_dict[data_filter.data_period] = {}
    data_dict[data_filter.data_period][indc_filter.slug] = {}

    # Get the required data for all districts.
    data_queryset = Data.objects.filter(
        indicator__slug=indc_filter.slug,
        data_period=data_filter.data_period,
        geography__type="DISTRICT",
    )
    # Filter data based on the selected districts.
    if geo_filter:
        data_queryset = data_queryset.filter(geography__code__in=geo_filter.code)

    # Iterate over each object.
    # Using geo code as key and {geo name: indicator value} as value.
    for data in data_queryset:
        data_dict[data_filter.data_period][indc_filter.slug][data.geography.code] = {
            data.geography.name: data.value
        }

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


def get_revenue_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
    for_map: bool = False,
) -> list:
    starttime = timeit.default_timer()
    data_list = []
    data_dict = {}

    geo_queryset = Geography.objects.filter(type="REVENUE CIRCLE")
    if geo_filter:
        geo_queryset = geo_queryset.filter(code__in=geo_filter.code)
        # geo_queryset = strawberry_django.filters.apply(geo_filter, geo_queryset)
    # print(geo_queryset)
    rc_data_queryset = Data.objects.all()

    for geo in geo_queryset:
        filtered_queryset = rc_data_queryset.filter(
            geography=geo, data_period=data_filter.data_period
        )
        if indc_filter:
            if for_map:
                filtered_queryset = filtered_queryset.filter(
                    indicator__slug=indc_filter.slug
                )
            else:
                filtered_queryset = filtered_queryset.filter(
                    Q(indicator__parent__slug=indc_filter.slug)
                    | Q(indicator__slug=indc_filter.slug)
                )
        if filtered_queryset.exists():
            for obj in filtered_queryset:
                data_dict[
                    obj.geography.type.lower().replace(" ", "-")
                ] = obj.geography.name
                data_dict[
                    (obj.geography.type + " code").lower().replace(" ", "-")
                ] = obj.geography.code
                data_dict[obj.indicator.slug] = round(obj.value, 3)
            if data_dict:
                data_list.append(data_dict)
                data_dict = {}
        else:
            break

    print("The time difference is :", timeit.default_timer() - starttime)
    return {"table_data": data_list}


def get_revenue_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
):
    starttime = timeit.default_timer()

    # Convert geography objects to a GeoJson format.
    geo_json = json.loads(
        serialize("geojson", Geography.objects.filter(type="REVENUE CIRCLE"))
    )

    # Get Indicator Data for each RC.
    # rc_data = get_revenue_data(
    #     indc_filter=indc_filter,
    #     data_filter=data_filter,
    #     geo_filter=geo_filter,
    #     for_map=True,
    # )
    rc_data = Data.objects.filter(
        indicator__slug=indc_filter.slug, data_period=data_filter.data_period
    )
    # print(len(rc_data))

    # Iterating over GeoJson and appending Indicator data for each RC.
    for rc in geo_json["features"]:
        for data in rc_data:
            if rc["properties"]["code"] == data.geography.code:
                # Get RC details
                geo_object = Geography.objects.get(code=data.geography.code)

                # Adding the District this RC belongs to.
                rc["properties"][
                    f"{geo_object.parentId.type.lower().replace(' ', '-') + '-code'}"
                ] = geo_object.parentId.code

                # Add other keys(Indicators) and its value to GeoJson.
                rc["properties"][f"{data.indicator.slug}"] = data.value

                break
            else:
                continue

        # Removing unnecessary values.
        rc["properties"].pop("parentId", None)
        rc["properties"].pop("pk", None)

    print("The time difference is :", timeit.default_timer() - starttime)
    return geo_json


def get_district_map_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
):
    starttime = timeit.default_timer()

    # Convert geography objects to a GeoJson format.
    geo_json = json.loads(
        serialize("geojson", Geography.objects.filter(type="DISTRICT"))
    )

    # Get Indicator Data for each district.
    rc_data = Data.objects.filter(
        indicator__slug=indc_filter.slug,
        data_period=data_filter.data_period,
        geography__type="DISTRICT",
    )

    # Iterating over GeoJson and appending Indicator data for each RC.
    for rc in geo_json["features"]:
        for data in rc_data:
            if rc["properties"]["code"] == data.geography.code:
                # Get RC details
                # geo_object = Geography.objects.get(code=data.geography.code)

                # Adding the District code this RC belongs to.
                # rc["properties"][
                #     f"{geo_object.type.lower().replace(' ', '-') + '-code'}"
                # ] = geo_object.parentId.code

                # Add other keys(Indicators) and its value to GeoJson.
                rc["properties"][f"{data.indicator.slug}"] = data.value

                break
            else:
                continue
        # Removing unnecessary values.
        rc["properties"].pop("parentId", None)
        rc["properties"].pop("pk", None)
        rc.pop("id", None)

    # else, get data on District level.
    # else:
    #     # Get Indicator Data for each District.
    #     rc_data = get_district_data(
    #         indc_filter=indc_filter, data_filter=data_filter, geo_filter=geo_filter
    #     )
    #     geo_json = json.loads(
    #         serialize("geojson", Geography.objects.filter(type="DISTRICT"))
    #     )

    #     # Iterating over GeoJson and appending Indicator data for each RC.
    #     for rc in geo_json["features"]:
    #         for data in rc_data["table_data"]:
    #             if rc["properties"]["code"] == data["district-code"]:
    #                 # Get RC details
    #                 geo_object = Geography.objects.get(code=data["district-code"])

    #                 # List the keys of Table Data.
    #                 key_list = list(data.keys())

    #                 # Remove the name of District and add other keys(Indicators) and its value to GeoJson.
    #                 key_list.remove("district")
    #                 key_list.remove("district-code")
    #                 for key in key_list:
    #                     rc["properties"][f"{key}"] = data[f"{key}"]
    #                 break
    #             else:
    #                 continue

    #         # Removing unnecessary values.
    #         rc["properties"].pop("parentId", None)
    #         rc["properties"].pop("pk", None)

    print("The time difference is :", timeit.default_timer() - starttime)
    return geo_json


def get_revenue_chart_data(
    indc_filter: types.IndicatorFilter,
    data_filter: types.DataFilter,
    geo_filter: Optional[types.GeoFilter] = None,
):
    starttime = timeit.default_timer()
    data_dict = {}

    # Creating initial dict structure.
    # TODO: Optimise it for multiple timeperiods; not supported currently. [Phase-2]
    # TODO: Optimise it for multiple indicators; not supported currently. [Phase-2]
    data_dict[data_filter.data_period] = {}
    data_dict[data_filter.data_period][indc_filter.slug] = {}

    # Get Indicator Data for each RC.
    rc_data = get_revenue_data(
        indc_filter=indc_filter,
        data_filter=data_filter,
        geo_filter=geo_filter,
        for_map=True,
    )

    for data in rc_data["table_data"]:
        # print(data[f"{indc_filter.slug}"])
        data_dict[data_filter.data_period][indc_filter.slug][
            data["revenue-circle-code"]
        ] = {data["revenue-circle"]: data[f"{indc_filter.slug}"]}

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


def get_categories() -> list:
    data_list = []
    data_dict = {}

    category_list = Indicators.objects.values_list("category", flat=True)
    # print(category_list)
    unqiue_categories = []
    [unqiue_categories.append(x) for x in category_list if x not in unqiue_categories]
    for catgry in unqiue_categories:
        filtered_queryset = Indicators.objects.filter(
            category=catgry, is_visible=True
        )  # .order_by("display_order")
        if filtered_queryset.exists():
            data_dict[catgry] = {}
            for obj in filtered_queryset:
                data_dict[catgry][obj.name] = obj.slug

            data_list.append(data_dict)
            data_dict = {}

    # print(data_list)
    return data_list


def get_model_indicators() -> list:
    data_list = []

    indc_obj = Indicators.objects.filter(
        Q(parent__slug="risk-score") | Q(slug="risk-score")
    )
    for data in indc_obj:
        data_list.append({"name": data.name, "slug": data.slug})

    return data_list


def get_timeperiod():
    # data = Data.objects.values_list("data_period", flat=True).distinct().order_by("-data_period")
    # time_list = []
    # for time in data:
    #     time_list.append(types.CustomDataPeriodList(value=time))
    # return time_list

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

    geo_object = Geography.objects.filter(
        type=geo_filter.type.upper().replace("-", " ")
    )
    if geo_object.exists():
        if geo_filter.type.upper() == "DISTRICT":
            for data in geo_object:
                data_list.append(
                    {
                        f"{data.type.lower().replace(' ', '-')}": data.name,
                        "code": data.code,
                    }
                )
            data_dict = data_list
        elif geo_filter.type.upper().replace("-", " ") == "REVENUE CIRCLE":
            for data in geo_object:
                rc_obj = geo_object.filter(parentId=data.parentId)
                if rc_obj.exists():
                    for rc_data in rc_obj:
                        data_list.append(
                            {
                                f"{data.type.lower().replace(' ', '-')}": rc_data.name,
                                "code": rc_data.code,
                            }
                        )
                    data_dict[f"{data.name}"] = data_list
                    data_list = []
        else:
            pass

    print("The time difference is :", timeit.default_timer() - starttime)
    return data_dict


@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    # geography: list[types.Geography] = strawberry_django.field()
    # department: list[types.Department] = strawberry.django.field()
    scheme: list[types.Scheme] = strawberry_django.field()
    indicators: list[types.Indicators] = strawberry_django.field()
    indicatorsByCategory: JSON = strawberry_django.field(resolver=get_categories)
    getFactors: JSON = strawberry_django.field(resolver=get_model_indicators)
    data: list[types.Data] = strawberry_django.field()
    districtViewData: JSON = strawberry_django.field(resolver=get_district_data)
    districtMapData: JSON = strawberry_django.field(resolver=get_district_map_data)
    districtViewChartData: JSON = strawberry_django.field(
        resolver=get_district_chart_data
    )
    revCircleViewTableData: JSON = strawberry_django.field(resolver=get_revenue_data)
    revCircleMapData: JSON = strawberry_django.field(resolver=get_revenue_map_data)
    revCircleViewChartData: JSON = strawberry_django.field(
        resolver=get_revenue_chart_data
    )
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
