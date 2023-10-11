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


# def get_unit(filter: Optional[UnitFilter]):
#     print(filter)
#     obj = models.Unit.objects.all()
#     obj = strawberry_django.filters.apply(filter, obj)
#     return obj


def get_district_data() -> list:
    data_list = []
    data_dict = {}

    geo_list = Data.objects.values_list("geography__name", flat=True).distinct()
    data_queryset = Data.objects.all()
    colated_queryset = data_queryset.values(
        "indicator__name", "geography__parentId__name", "geography__parentId__type"
    ).annotate(indc_avg=Avg("value")).order_by()

    # print(filtered_queryset.filter(geography__name="Assam"))
    for geo in geo_list:
        filtered_queryset = colated_queryset.filter(geography__name=f"{geo}")
        for obj in filtered_queryset:
            data_dict[obj["geography__parentId__type"].lower()] = obj["geography__parentId__name"]
            data_dict[obj["indicator__name"]] = obj["indc_avg"]
        data_list.append(data_dict)
        data_dict = {}

    return data_list


@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    geography: list[types.Geography] = strawberry.django.field()
    # department: list[types.Department] = strawberry.django.field()
    scheme: list[types.Scheme] = strawberry.django.field()
    indicators: list[types.Indicators] = strawberry.django.field()
    data: list[types.Data] = strawberry.django.field()
    districtViewTableData: JSON = strawberry.django.field(resolver=get_district_data)
    # barChart: types.BarChart = strawberry.django.field(resolver=get_bar_data)


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
