import strawberry
from strawberry import auto

from . import models


@strawberry.django.type(models.Indicator)
class Indicator:
    programme: auto
    type: auto
    name: auto
    short_title: auto
    description: auto
    slug: auto
    active: auto
    location: auto
    formula: auto
    parent: auto


@strawberry.django.type(models.IndicatorData)
class IndicatorData:
    unit: auto
    indicator: Indicator
    date: auto
    value: auto
