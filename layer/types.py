import datetime
from typing import Optional

import strawberry
import strawberry_django
from strawberry import auto

from . import models

"""

Type Name should be in PascalCase.
Field Name should be in camelCase.

This is as per Apollo Schema naming convention.
Source:https://www.apollographql.com/docs/apollo-server/schema/schema/#naming-conventions

class User: <This is Type Name>
    name: str <This is Field Name>

NOTE: Field names in a Type should match with its Model counterpart.

"""


@strawberry_django.filter(models.Unit)
class UnitFilter:
    name: Optional[str]


@strawberry_django.type(models.Unit, fields="__all__", filters=UnitFilter)
class Unit:
    pass
    # name: auto
    # description: Optional[str]
    # symbol: auto


@strawberry_django.filter(models.Geography)
class GeoFilter:
    name: Optional[str]
    code: Optional[list[strawberry.ID]]
    type: Optional[str]


@strawberry_django.filter(models.Indicators)
class IndicatorFilter:
    name: Optional[str]
    slug: Optional[str]


@strawberry_django.filter(models.Data)
class DataFilter:
    data_period: Optional[str]
    period: Optional[str]


@strawberry_django.type(models.Geography, filters=GeoFilter)
class Geography:
    name: auto
    code: auto
    type: auto
    parentId: Optional["Geography"]


# @strawberry_django.type(models.Page)
# class Page:
#     name: Optional[str] = None
#     description: Optional[str] = None


@strawberry_django.type(models.Department)
class Department:
    name: auto
    description: auto
    geography: "Geography"


@strawberry_django.type(models.Scheme)
class Scheme:
    name: auto
    description: Optional[str] = None
    slug: Optional[str] = None
    department: Optional["Department"] = None


@strawberry_django.type(models.Indicators)
class Indicators:
    name: auto
    long_description: Optional[str] = None
    short_description: Optional[str] = None
    category: Optional[str] = None
    type: auto
    slug: Optional[str] = None
    unit: Unit
    geography: Optional["Geography"] = None
    department: Optional["Department"] = None
    # page: Optional[List[Page]] = None
    scheme: Optional["Scheme"] = None
    parent: Optional["Indicators"]


@strawberry_django.type(models.Data, filters=DataFilter)
class Data:
    value: Optional[int] = None
    added: datetime.datetime
    indicator: "Indicators"
    geography: "Geography"
    scheme: Optional["Scheme"] = None
    data_period: Optional[str]


# @strawberry.type
# class BarChart:
#     x: list[str]
#     y: list[str]


@strawberry.type
class CustomDataPeriodList:
    value: str
