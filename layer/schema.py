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

    if geo_filter and len(geo_filter.code) <= 1:
        geo_list = Geography.objects.filter(parentId__code__in=geo_filter.code)
        colated_queryset = Data.objects.filter(
            geography__parentId__code__in=geo_filter.code,
            data_period=data_filter.data_period,
        )
        for geo in geo_list:
            filtered_queryset = colated_queryset.filter(
                geography=geo, data_period=data_filter.data_period
            )
            if indc_filter:
                filtered_queryset = filtered_queryset.filter(
                    Q(indicator__parent__slug=indc_filter.slug)
                    | Q(indicator__slug=indc_filter.slug)
                )
                # if slug_catgry.exists():
                #     if slug_catgry[0].indicator.category == "Composite Score":
                #         filtered_queryset = filtered_queryset.filter(Q(indicator__parent__category=slug_catgry[0].indicator.category) | Q(indicator__category=slug_catgry[0].indicator.category))
                #     else:
                #         filtered_queryset = filtered_queryset.filter(indicator__category=slug_catgry[0].indicator.category)
            for obj in filtered_queryset:
                data_dict[
                    obj.geography.type.lower().replace(" ", "-")
                ] = obj.geography.name
                data_dict[
                    obj.geography.type.lower().replace(" ", "-") + "-code"
                ] = obj.geography.code
                data_dict[obj.indicator.slug] = obj.value
            if data_dict:
                data_list.append(data_dict)
                data_dict = {}

    else:
        geo_list = Data.objects.values_list(
            "geography__parentId__name", flat=True
        ).distinct()
        # a = Geography.objects.filter(type="DISTRICT")
        # print(len(geo_list))
        # for dis in a:
        #     if dis.name not in geo_list:
        #         print(dis.name)
        data_queryset = Data.objects.all()
        colated_queryset = (
            data_queryset.values(
                "indicator__slug",
                "indicator__parent__slug",
                "indicator__category",
                "geography__parentId__name",
                "geography__parentId__type",
                "geography__parentId__code",
                "indicator__data_source",
            ).annotate(indc_avg=Max("value"))
            # .order_by()
        )

        for geo in geo_list:
            # print(geo)
            filtered_queryset = colated_queryset.filter(
                geography__parentId__name=f"{geo}",
                data_period=data_filter.data_period,
            )
            if indc_filter:
                filtered_queryset = filtered_queryset.filter(
                    Q(indicator__parent__slug=indc_filter.slug)
                    | Q(indicator__slug=indc_filter.slug)
                )
                # if slug_catgry.exists():
                #     if slug_catgry[0]["indicator__category"] == "Main":
                #         filtered_queryset = filtered_queryset.filter(Q(indicator__parent__category=slug_catgry[0]["indicator__category"]) | Q(indicator__category=slug_catgry[0]["indicator__category"]))
                #     else:
                #         filtered_queryset = filtered_queryset.filter(indicator__category=slug_catgry[0]["indicator__category"])

            for obj in filtered_queryset:
                # print(obj)
                data_dict[obj["geography__parentId__type"].lower()] = obj[
                    "geography__parentId__name"
                ]
                data_dict[obj["geography__parentId__type"].lower() + "-code"] = obj[
                    "geography__parentId__code"
                ]
                data_dict[obj["indicator__slug"]] = round(obj["indc_avg"], 2)
            if data_dict:
                data_list.append(data_dict)
                data_dict = {}

    print("The time difference is :", timeit.default_timer() - starttime)
    return {"table_data": data_list}


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
        geo_queryset = strawberry_django.filters.apply(geo_filter, geo_queryset)
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
    # rc_data = get_revenue_data(indc_filter=indc_filter, data_filter=data_filter, geo_filter=geo_filter, for_map=True)
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

    # If geo_filter is applied, get data on RC level.
    if geo_filter:
        geo_json = json.loads(
            serialize(
                "geojson", Geography.objects.filter(parentId__code=geo_filter.code)
            )
        )
        rc_data = Data.objects.filter(
            indicator__slug=indc_filter.slug,
            data_period=data_filter.data_period,
            geography__parentId__code=geo_filter.code,
        )

        # Iterating over GeoJson and appending Indicator data for each RC.
        for rc in geo_json["features"]:
            for data in rc_data:
                if rc["properties"]["code"] == data.geography.code:
                    # Get RC details
                    geo_object = Geography.objects.get(code=data.geography.code)

                    # Adding the District code this RC belongs to.
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

    # else, get data on District level.
    else:
        # Get Indicator Data for each District.
        rc_data = get_district_data(
            indc_filter=indc_filter, data_filter=data_filter, geo_filter=geo_filter
        )
        geo_json = json.loads(
            serialize("geojson", Geography.objects.filter(type="DISTRICT"))
        )

        # Iterating over GeoJson and appending Indicator data for each RC.
        for rc in geo_json["features"]:
            for data in rc_data["table_data"]:
                if rc["properties"]["code"] == data["district-code"]:
                    # Get RC details
                    geo_object = Geography.objects.get(code=data["district-code"])

                    # List the keys of Table Data.
                    key_list = list(data.keys())

                    # Remove the name of District and add other keys(Indicators) and its value to GeoJson.
                    key_list.remove("district")
                    key_list.remove("district-code")
                    for key in key_list:
                        rc["properties"][f"{key}"] = data[f"{key}"]
                    break
                else:
                    continue

            # Removing unnecessary values.
            rc["properties"].pop("parentId", None)
            rc["properties"].pop("pk", None)

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


def get_district_chart_data(
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

    # Get Indicator Data for each District.
    rc_data = get_district_data(
        indc_filter=indc_filter, data_filter=data_filter, geo_filter=geo_filter
    )
    # print(rc_data)

    if geo_filter and len(geo_filter.code) <= 1:
        data_dict[data_filter.data_period][indc_filter.slug]["revenue-circle"] = {}
        # Getting the values for the district.
        parent_queryset = (
            Data.objects.filter(
                geography__parentId__in=geo_filter.code,
                data_period=data_filter.data_period,
                indicator__slug=indc_filter.slug,
            )
            .values(
                "indicator__slug",
                "geography__parentId__name",
                "geography__parentId__type",
                "geography__parentId__code",
            )
            .annotate(indc_avg=Max("value"))
        )

        # print(parent_queryset)
        data_dict[data_filter.data_period][indc_filter.slug][
            parent_queryset[0]["geography__parentId__type"].lower().replace(" ", "-")
        ] = {
            parent_queryset[0]["geography__parentId__name"]: parent_queryset[0][
                "indc_avg"
            ]
        }
    else:
        data_dict[data_filter.data_period][indc_filter.slug]["district"] = {}

    for data in rc_data["table_data"]:
        # print(data, data[f"{indc_filter.slug}"])
        if geo_filter and len(geo_filter.code) <= 1:
            data_dict[data_filter.data_period][indc_filter.slug]["revenue-circle"][
                data["revenue-circle"]
            ] = data
        elif geo_filter and len(geo_filter.code) > 1:
            geo_obj = Geography.objects.filter(code__in=geo_filter.code).values("name")
            for geo in geo_obj:
                # print(geo, data["district"])
                if geo["name"] == data["district"]:
                    data_dict[data_filter.data_period][indc_filter.slug]["district"][
                        data["district"]
                    ] = data[f"{indc_filter.slug}"]
        else:
            data_dict[data_filter.data_period][indc_filter.slug]["district"][
                data["district"]
            ] = data[f"{indc_filter.slug}"]

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
        Q(parent__slug="composite-score") | Q(slug="composite-score")
    )
    for data in indc_obj:
        data_list.append({"name": data.name, "slug": data.slug})
    #     data_dict[data.name] = data.slug
    # data_list.append(data_dict)

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
    data_dict = {}
    rc_list = []

    geo_object = Geography.objects.filter(code=geo_filter.code)
    if geo_object.exists():
        get_child_rc = Geography.objects.filter(parentId=geo_object[0].id)
        for rc in get_child_rc:
            rc_list.append(
                {
                    f"{rc.type.lower().replace(' ', '-')}": rc.name,
                    f"{rc.type.lower().replace(' ', '-')}" + "-code": rc.code,
                }
            )
        data_dict[geo_object[0].name] = rc_list

    return data_dict


@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    geography: list[types.Geography] = strawberry_django.field()
    # department: list[types.Department] = strawberry.django.field()
    scheme: list[types.Scheme] = strawberry_django.field()
    indicators: list[types.Indicators] = strawberry_django.field()
    indicatorsByCategory: JSON = strawberry_django.field(resolver=get_categories)
    getFactors: JSON = strawberry_django.field(resolver=get_model_indicators)
    data: list[types.Data] = strawberry_django.field()
    districtViewTableData: JSON = strawberry_django.field(resolver=get_district_data)
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
