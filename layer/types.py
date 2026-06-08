import datetime
from typing import Optional

import strawberry
import strawberry_django
from strawberry import auto

from . import models


@strawberry_django.filter(models.Unit)
class UnitFilter:
    name: str | None


@strawberry_django.type(models.Unit, fields="__all__", filters=UnitFilter)
class Unit:
    pass


@strawberry_django.filter(models.Geography)
class GeoFilter:
    name: str | None
    code: list[strawberry.ID] | None
    type: str | None


@strawberry_django.filter(models.Indicators)
class IndicatorFilter:
    name: str | None
    slug: str | None
    module: str | None


@strawberry_django.filter(models.Data)
class DataFilter:
    data_period: str | None
    period: str | None  # Required for time trends.


@strawberry_django.type(models.Geography, filters=GeoFilter)
class Geography:
    name: auto
    code: auto
    type: auto
    parent_id: Optional["Geography"] = strawberry_django.field(field_name="parentId")


@strawberry_django.type(models.Department)
class Department:
    name: auto
    description: auto
    geography: "Geography"


@strawberry_django.type(models.Scheme)
class Scheme:
    name: auto
    description: str | None = None
    slug: str | None = None
    department: Optional["Department"] = None


@strawberry_django.type(models.Indicators)
class Indicators:
    name: auto
    long_description: str | None = None
    short_description: str | None = None
    category: str | None = None
    type: auto
    slug: str | None = None
    unit: Unit
    geography: Optional["Geography"] = None
    department: Optional["Department"] = None
    scheme: Optional["Scheme"] = None
    parent: Optional["Indicators"]
    module: str | None = None


@strawberry_django.type(models.Data, filters=DataFilter)
class Data:
    value: int | None = None
    added: datetime.datetime
    indicator: "Indicators"
    geography: "Geography"
    scheme: Optional["Scheme"] = None
    data_period: str | None
    module: str | None = None


@strawberry.type
class CustomDataPeriodList:
    value: str
