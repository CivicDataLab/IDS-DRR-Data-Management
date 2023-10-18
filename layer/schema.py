import strawberry
import strawberry_django
import json

from django.db.models import Avg, Count
from typing import Optional
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension

from . import types
from .models import Geography, Data, Indicators


# def get_bar_data(self) -> str:
#     data_obj = Data.objects.all().values("value", "data_period")
#     # print(data_obj[0])
#     return types.BarChart(
#         x=[x.get("data_period") for x in data_obj], y=[y.get("value") for y in data_obj]
#     )


# def get_unit(filter:Optional[types.UnitFilter]=None):
#     print(filter)
#     obj = Unit.objects.all()
#     obj = strawberry_django.filters.apply(filter, obj)
#     return obj


def get_district_data(geo_filter: Optional[types.GeoFilter] = None) -> list:
    data_list = []
    data_dict = {}

    if geo_filter:
        geo_list = Geography.objects.filter(parentId=geo_filter.parentId.pk)
        colated_queryset = Data.objects.filter(
            geography__parentId=geo_filter.parentId.pk
        )
        for geo in geo_list:
            filtered_queryset = colated_queryset.filter(geography=geo)
            for obj in filtered_queryset:
                data_dict[obj.geography.type.lower()] = obj.geography.name
                data_dict[obj.indicator.slug] = obj.value
            data_list.append(data_dict)
            data_dict = {}

    else:
        geo_list = Data.objects.values_list(
            "geography__parentId__name", flat=True
        ).distinct()
        # print(geo_list)
        data_queryset = Data.objects.all()
        colated_queryset = (
            data_queryset.values(
                "indicator__slug",
                "geography__parentId__name",
                "geography__parentId__type",
            )
            .annotate(indc_avg=Avg("value"))
            .order_by()
        )

        for geo in geo_list:
            filtered_queryset = colated_queryset.filter(
                geography__parentId__name=f"{geo}"
            )
            for obj in filtered_queryset:
                data_dict[obj["geography__parentId__type"].lower()] = obj[
                    "geography__parentId__name"
                ]
                data_dict[obj["indicator__slug"]] = round(obj["indc_avg"], 2)
            data_list.append(data_dict)
            data_dict = {}

    return data_list


def get_revenue_data(geo_filter: Optional[types.GeoFilter] = None) -> list:
    data_list = []
    data_dict = {}

    geo_queryset = Geography.objects.filter(type="REVENUE CIRCLE")
    if geo_filter:
        geo_queryset = strawberry_django.filters.apply(geo_filter, geo_queryset)
    # print(geo_queryset)
    rc_data_queryset = Data.objects.all()

    for geo in geo_queryset:
        filtered_queryset = rc_data_queryset.filter(geography=geo)
        for obj in filtered_queryset:
            data_dict[obj.geography.type.lower()] = obj.geography.name
            data_dict[obj.indicator.slug] = obj.value
        data_list.append(data_dict)
        data_dict = {}

    return data_list

def get_categories():
    data_list = []
    data_dict = {}
    
    category_list = Indicators.objects.values_list('category', flat=True).distinct()
    # print(category_list)
    for catgry in category_list:
        filtered_queryset = Indicators.objects.filter(category=catgry)
        if filtered_queryset.exists():
            data_dict[catgry] = {}
            for obj in filtered_queryset:
                data_dict[catgry][obj.name] = obj.slug
                # print(data_dict)
                # data_dict[catgry] = {}
                # data_dict[f"child_{i}"] = obj.name
            data_list.append(data_dict)
            data_dict = {}
    
    # print(data_list)
    return data_list

@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    geography: list[types.Geography] = strawberry.django.field()
    # department: list[types.Department] = strawberry.django.field()
    scheme: list[types.Scheme] = strawberry.django.field()
    indicators: list[types.Indicators] = strawberry.django.field()
    indicatorsByCategory: JSON = strawberry.django.field(resolver=get_categories)
    data: list[types.Data] = strawberry.django.field()
    districtViewTableData: JSON = strawberry.django.field(resolver=get_district_data)
    revCricleViewTableData: JSON = strawberry.django.field(resolver=get_revenue_data)
    # barChart: types.BarChart = strawberry.django.field(resolver=get_bar_data)


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
