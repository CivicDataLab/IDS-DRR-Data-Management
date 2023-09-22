from typing import Optional
import strawberry
import strawberry_django
from strawberry_django.optimizer import DjangoOptimizerExtension

from . import types, models
from .types import GeoFilter, UnitFilter

def get_bar_data(self) -> str:
    data_obj = models.Data.objects.all().values('value', 'data_period')
    print(data_obj[0])
    return types.BarChart(x=[x.get('data_period') for x in data_obj], y=[y.get('value') for y in data_obj])


def get_unit(filter: Optional[UnitFilter]):
    print(filter)
    obj = models.Unit.objects.all()
    obj = strawberry_django.filters.apply(filter, obj)
    return obj

@strawberry.type
class Query: #camelCase
    unit: list[types.Unit] = strawberry.django.field(resolver=get_unit)
    geography: list[types.Geography] = strawberry.django.field()
    department: list[types.Department] = strawberry.django.field()
    scheme: list[types.Scheme] = strawberry.django.field()
    indicators: list[types.Indicators] = strawberry.django.field()
    data: list[types.Data] = strawberry.django.field()
    barChart: types.BarChart = strawberry.django.field(resolver=get_bar_data)


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
