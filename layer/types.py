import datetime
import strawberry

from typing import List, Optional
from strawberry import auto

from . import models

'''
NOTE

Type Name should be in PascalCase.
Field Name should be in camelCase.

This is as per Apollo Schema naming convention. Read more here - https://www.apollographql.com/docs/apollo-server/schema/schema/#naming-conventions

class User: <This is Type Name>
    name: str <This is Field Name>

'''
@strawberry.django.filter(models.Unit)
class UnitFilter:
    name: Optional[str]

@strawberry.django.type(models.Unit, fields="__all__", filters=UnitFilter)
class Unit:
    pass
    # name: auto
    # description: Optional[str]
    # symbol: auto

@strawberry.django.filter(models.Geography)
class GeoFilter:
    name: Optional[str]
    id: Optional[strawberry.ID]
    parentId: Optional["Geography"]

@strawberry.django.type(models.Geography, filters=GeoFilter)
class Geography:
    name: auto
    code: auto
    type: auto
    parentId: Optional["Geography"]

@strawberry.django.type(models.Page)
class Page:
    name: Optional[str] = None
    description: Optional[str] = None

@strawberry.django.type(models.Department)
class Department:
    name: auto
    description: auto
    geography: "Geography"

@strawberry.django.type(models.Scheme)
class Scheme:
    name: auto
    description: Optional[str] = None
    slug: Optional[str] = None
    department: "Department"

@strawberry.django.type(models.Indicators)
class Indicators:
    name: auto
    longDescription: Optional[str] = None
    shortDescription: Optional[str] = None
    category: Optional[str] = None
    type: auto
    slug: Optional[str] = None
    unit: Unit
    geography: "Geography"
    department: "Department"
    page: Optional[List[Page]] = None
    scheme: "Scheme"

@strawberry.django.type(models.Data)
class Data:
    value: Optional[int] = None
    added: datetime.datetime
    indicator: "Indicators"
    geography: "Geography"
    scheme: "Scheme"
    dataPeriod: Optional[str]

@strawberry.type
class BarChart:
    x: list[str]
    y: list[str]