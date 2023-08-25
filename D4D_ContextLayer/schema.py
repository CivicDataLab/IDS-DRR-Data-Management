import strawberry
from strawberry_django.optimizer import DjangoOptimizerExtension

from .types import Indicator, IndicatorData


@strawberry.type
class Query:
    fruits: list[Indicator] = strawberry.django.field()


schema = strawberry.Schema(
    query=Query,
    extensions=[
        DjangoOptimizerExtension,
    ],
)
