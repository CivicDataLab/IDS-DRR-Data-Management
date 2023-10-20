import strawberry
import strawberry_django

from django.db.models import Avg, Q
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


def get_district_data(
    geo_filter: Optional[types.GeoFilter] = None,
    indc_filter: Optional[types.IndicatorFilter] = None,
) -> list:
    
    data_list = []
    data_dict = {}
    frims_data_list = []
    frims_data_dict = {}
    
    if geo_filter:
        geo_list = Geography.objects.filter(parentId__name=geo_filter.name)
        colated_queryset = Data.objects.filter(
            geography__parentId__name=geo_filter.name
        )
        for geo in geo_list:
            filtered_queryset = colated_queryset.filter(geography=geo)
            for obj in filtered_queryset:
                if obj.indicator.data_source == "FRIMS":
                    # print(obj["indicator__data_source"])
                    frims_data_dict[obj.geography.name.lower()] = obj.geography.name
                    frims_data_dict[obj.indicator.slug] = obj.value
            if frims_data_dict:
                frims_data_list.append(frims_data_dict)
                frims_data_dict = {}
            if indc_filter:
                slug_catgry = filtered_queryset.filter(indicator__slug=indc_filter.slug)
                if slug_catgry.exists():
                    if slug_catgry[0].indicator.category == "Main":
                        filtered_queryset = filtered_queryset.filter(Q(indicator__parent__category=slug_catgry[0]["indicator__category"]) | Q(indicator__category=slug_catgry[0]["indicator__category"]))
                    else:
                        filtered_queryset = filtered_queryset.filter(indicator__category=slug_catgry[0]["indicator__category"])
            for obj in filtered_queryset:
                data_dict[obj.geography.type.lower()] = obj.geography.name
                data_dict[obj.indicator.slug] = obj.value
            if data_dict:
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
                "indicator__parent__slug",
                "indicator__slug",
                "indicator__category",
                "geography__parentId__name",
                "geography__parentId__type",
                "indicator__data_source"
            )
            .annotate(indc_avg=Avg("value"))
            .order_by()
        )
        
        for geo in geo_list:
            print(geo)
            filtered_queryset = colated_queryset.filter(
                geography__parentId__name=f"{geo}"
            )
            # print(len(filtered_queryset))
            for obj in filtered_queryset:
                if obj["indicator__data_source"] == "FRIMS":
                    # print(obj["indicator__data_source"])
                    frims_data_dict[obj["geography__parentId__type"].lower()] = obj[
                    "geography__parentId__name"]
                    frims_data_dict[obj["indicator__slug"]] = round(obj["indc_avg"], 2)
            if frims_data_dict:
                frims_data_list.append(frims_data_dict)
                frims_data_dict = {}
            
            # print(filtered_queryset)
            if indc_filter:
                slug_catgry = filtered_queryset.filter(indicator__slug=indc_filter.slug)
                if slug_catgry.exists():
                    if slug_catgry[0]["indicator__category"] == "Main":
                        filtered_queryset = filtered_queryset.filter(Q(indicator__parent__category=slug_catgry[0]["indicator__category"]) | Q(indicator__category=slug_catgry[0]["indicator__category"]))
                    else:
                        filtered_queryset = filtered_queryset.filter(indicator__category=slug_catgry[0]["indicator__category"])
                # filtered_queryset = filtered_queryset.filter(
                #     Q(indicator__slug=indc_filter.slug)
                #     | Q(indicator__parent__slug=indc_filter.slug)
                # )
                # print(filtered_queryset)

            for obj in filtered_queryset:
                print(obj)
                data_dict[obj["geography__parentId__type"].lower()] = obj[
                    "geography__parentId__name"
                ]
                data_dict[obj["indicator__slug"]] = round(obj["indc_avg"], 2)
            if data_dict:
                data_list.append(data_dict)
                data_dict = {}

    return {'table_data': data_list, 'frims_data': frims_data_list}


def get_revenue_data(
    geo_filter: Optional[types.GeoFilter] = None,
    indc_filter: Optional[types.IndicatorFilter] = None,
) -> list:
    data_list = []
    data_dict = {}
    frims_data_list = []
    frims_data_dict = {}

    geo_queryset = Geography.objects.filter(type="REVENUE CIRCLE")
    if geo_filter:
        geo_queryset = strawberry_django.filters.apply(geo_filter, geo_queryset)
    # print(geo_queryset)
    rc_data_queryset = Data.objects.all()

    for geo in geo_queryset:
        filtered_queryset = rc_data_queryset.filter(geography=geo)
        for obj in filtered_queryset:
            if obj.indicator.data_source == "FRIMS":
                frims_data_dict[obj.geography.name.lower()] = obj.geography.name
                frims_data_dict[obj.indicator.slug] = obj.value
        if frims_data_dict:
            frims_data_list.append(frims_data_dict)
            frims_data_dict = {}
        if indc_filter:
            # print(indc_filter)
            slug_catgry = filtered_queryset.filter(indicator__slug=indc_filter.slug)
            if slug_catgry.exists():
                print(slug_catgry[0].indicator.category)
                if slug_catgry[0].indicator.category == "Main":
                    # print(filtered_queryset)
                    filtered_queryset = filtered_queryset.filter(Q(indicator__parent__category=slug_catgry[0]["indicator__category"]) | Q(indicator__category=slug_catgry[0]["indicator__category"]))
                    print(filtered_queryset)
                else:
                    filtered_queryset = filtered_queryset.filter(indicator__category=slug_catgry[0]["indicator__category"])
        for obj in filtered_queryset:
            data_dict[obj.geography.type.lower()] = obj.geography.name
            data_dict[obj.indicator.slug] = obj.value
        if data_dict:
            data_list.append(data_dict)
            data_dict = {}

    return {'table_data': data_list, 'frims_data': frims_data_list}


def get_categories() -> list:
    data_list = []
    data_dict = {}

    category_list = Indicators.objects.values_list("category", flat=True).distinct()
    # print(category_list)
    for catgry in category_list:
        filtered_queryset = Indicators.objects.filter(category=catgry).order_by("display_order")
        if filtered_queryset.exists():
            data_dict[catgry] = {}
            for obj in filtered_queryset:
                data_dict[catgry][obj.name] = obj.slug

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
