import strawberry
import strawberry_django
import json
from typing import Optional
from strawberry.scalars import JSON
from strawberry_django.optimizer import DjangoOptimizerExtension

from . import types
from .models import Geography, Data


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


def get_district_data():
    data_list = []
    data_dict = {}

    data_queryset = Data.objects.all()
    for obj in data_queryset:
        data_dict[
            obj.geography.parentId.type
            if obj.geography.parentId
            else obj.geography.type
        ] = (
            obj.geography.parentId.name
            if obj.geography.parentId
            else obj.geography.name
        )
        data_dict[obj.indicator.name] = obj.value
        data_list.append(data_dict)
        data_dict = {}
    return data_list


@strawberry.type
class Query:  # camelCase
    # unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    geography: list[types.Geography] = strawberry.django.field()
    department: list[types.Department] = strawberry.django.field()
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
