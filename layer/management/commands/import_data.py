import glob
import json
import os
import time

import pandas as pd
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
from layer.models import Data, Geography, Indicators, Unit


def migrate_indicators(filename="layer/assets/data_dict.csv"):
    df = pd.read_csv(filename)

    for row in df.itertuples(index=False):
        print("Processing Indicator -", row.indicatorSlug)
        try:
            Indicators.objects.get(slug=row.indicatorSlug.lower())
            print("Already Exists!")
        except Indicators.DoesNotExist:
            print("Processing Unit -", row.unit)
            unit_obj = _get_indicator_unit_form_row(row)
            parent_obj = _get_indicator_parent_from_row(row)

            indicator_obj = Indicators(
                name=row.indicatorTitle.strip(),
                slug=(
                    row.indicatorSlug.lower().strip() if row.indicatorSlug else None
                ),
                long_description=(
                    row.indicatorDescription.strip()
                    if row.indicatorDescription
                    else None
                ),
                # short_description = row.indicatorDescription if row.indicatorDescription else None,
                category=(
                    row.indicatorCategory.strip() if row.indicatorCategory else None
                ),
                # type = row.indicatorType if row.indicatorType else None
                unit=unit_obj,
                data_source=row.dataSource.strip() if row.dataSource else None,
                parent=parent_obj,
                is_visible=True if row.visible == "y" else False,
            )
            indicator_obj.save()
            print("Added indicator to the database.")
        print("---------------------------")


def _get_indicator_parent_from_row(row):
    parent_obj = None
    try:
        if row.parent and not isinstance(row.parent, float):
            parent_obj = Indicators.objects.get(name=row.parent.strip())
        else:
            pass
    except Indicators.DoesNotExist:
        print(
            f"Failed to get the parent indicator for {row.indicatorSlug.lower()}"
        )
    return parent_obj


def _get_indicator_unit_form_row(row):
    if row.unit and not isinstance(row.unit, float):
        try:
            unit_obj = Unit.objects.get(name=row.unit.lower())
            # print(f"Hey! Unit {unit_obj.name} already exists!")
        except Unit.DoesNotExist:
            unit_obj = Unit(name=row.unit.lower())
            unit_obj.save()
            print(f"Saved {unit_obj.name} to DB!")
    else:
        unit_obj = None
    return unit_obj


def update_indicators(filename="layer/data_dict.csv"):
    df = pd.read_csv(filename)
    for row in df.itertuples(index=False):
        slug = row.indicatorSlug
        print("Processing Indicator -", slug)
        try:
            indicator = Indicators.objects.get(slug=slug.lower())
            indicator.name = row.indicatorTitle.strip()
            indicator.long_description = row.indicatorDescription.strip()
            indicator.category = row.indicatorCategory.strip()
            indicator.unit = _get_indicator_unit_form_row(row)
            indicator.data_source = row.dataSource.strip() if row.dataSource else None
            indicator.parent = _get_indicator_parent_from_row(row)
            indicator.is_visible = True if row.visible == "y" else False
            indicator.save()
            print(f"updated Indicator - {slug}")

        except Indicators.DoesNotExist:
            print(f"Indicator with slug {slug} does not exist. ")


def migrate_geojson():
    files = sorted(glob.glob(os.getcwd() + "/layer/assets/geojson/*.geojson"))
    sorted_files = sorted(
        files,
        key=lambda x: ("_district" not in os.path.basename(x),
                       os.path.basename(x)),
    )

    for filename in sorted_files:
        with open(filename) as f:
            print(
                f"Adding data from {os.path.basename(filename)} to database....")
            data = json.load(f)

            file_name = data["name"]
            for ft in data["features"]:
                # print(type(ft))
                geom_str = json.dumps(ft["geometry"])
                # print(type(geom_str))
                geom = GEOSGeometry(geom_str)
                # print(type(geom))

                # try:
                if isinstance(geom, MultiPolygon):
                    pass
                elif isinstance(geom, Polygon):
                    geom = MultiPolygon([geom])

                if file_name == "assam_district_35":
                    geo_type = "DISTRICT"
                    code = ft["properties"]["ID"]
                    name = ft["properties"]["district"]
                    state = ft["properties"]["state"]
                    try:
                        parent_geo_obj = Geography.objects.get(
                            name__iexact=state, type="STATE"
                        )
                    except Geography.DoesNotExist:
                        parent_geo_obj = Geography(
                            name=state.capitalize(), code="18", type="STATE"
                        )
                        parent_geo_obj.save()

                elif file_name == "assam_revenue_circles_nov2022":
                    geo_type = "REVENUE CIRCLE"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["revenue_ci"]
                    district = ft["properties"]["district_3"]

                    parent_geo_obj = Geography.objects.get(
                        name__iexact=district, type="DISTRICT"
                    )
                elif file_name == "BharatMaps_HP_district":
                    geo_type = "DISTRICT"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["District"]
                    state = ft["properties"]["STATE"]
                    state_code = "02"  # TODO: add statecode to HP geojson
                    try:
                        parent_geo_obj = Geography.objects.get(
                            name__iexact=state, type="STATE"
                        )
                    except Geography.DoesNotExist:
                        parent_geo_obj = Geography(
                            name=state.capitalize(), code=state_code, type="STATE"
                        )
                        parent_geo_obj.save()

                elif file_name == "bharatmaps_HP_subdistricts":
                    geo_type = "SUB DISTRICT"
                    code = ft["properties"]["sdtcode11"]
                    name = ft["properties"]["sdtname"]
                    dtcode = ft["properties"]["dtcode11"]
                    parent_geo_obj = Geography.objects.get(
                        code=dtcode, type="DISTRICT")
                elif file_name == "hp_tehsil_temp":
                    geo_type = "TEHSIL"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["TEHSIL"]
                    dtcode = f'02-{ft["properties"]["dtcode11"]}'
                    parent_geo_obj = Geography.objects.get(
                        code=dtcode, type="DISTRICT")
                try:
                    geo_object = Geography.objects.get(code=code,
                                                       parentId=parent_geo_obj)
                    geo_object.name = name.capitalize()
                    geo_object.geom = geom
                    geo_object.type = geo_type
                except Geography.DoesNotExist:
                    geo_object = Geography(
                        name=name.capitalize(),
                        code=code,
                        type=geo_type,
                        geom=geom,
                        parentId=parent_geo_obj,
                    )
                geo_object.save()


if Data.objects.last() is not None:
    counter = Data.objects.last().pk
else:
    counter = 0


def generate_pk():
    global counter
    counter += 1
    return counter


def addDataRow(row, geography_obj, indicator):
    data_obj = Data(
        pk=generate_pk(),
        value=row[f"{indicator.slug}"],
        indicator=indicator,
        geography=geography_obj,
        data_period=row.timeperiod,
    )
    return data_obj


def import_geography_data(df, indicators, g_code):
    rows = df[df.index == g_code]
    if rows.empty:
        print(f"No entries in the state for geography code: {g_code}")
        return
    try:
        geography_obj = Geography.objects.get(Q(code=g_code), ~Q(type="STATE"))
    except Exception as e:
        print(f"Geography location for: {g_code} is missing")
    else:
        print(f"Updating datapoints for: {geography_obj.name}")
        for row in rows.itertuples():
            if Data.objects.filter(geography__code=geography_obj.code, data_period=row.timeperiod).count():
                Data.objects.filter(geography__code=geography_obj.code, data_period=row.timeperiod).all().delete()
        for index, row in rows.iterrows():
            data_objects = []
            for indicator in indicators:
                if indicator.slug in df.columns:
                    data_objects.append(addDataRow(row, geography_obj, indicator))
                else:
                    print(f"Indicator {indicator.slug} missing for {geography_obj.name}")
            Data.objects.bulk_create(data_objects)
        updated_data_count = Data.objects.filter(
            geography__code=geography_obj.code).count()


def import_state_data(df, indicators, g_code=None):
    if g_code:
        import_geography_data(df, indicators, g_code)
    else:
        [import_geography_data(df, indicators, g_code)
         for g_code in df.index.unique()]

def filter_indicators(df, indicators):
    cleaned_indicator = [ind for ind in indicators if ind.slug in df.columns]
    if missing := [ind.slug for ind in indicators if ind.slug not in df.columns]:
        print(f"Indicators: {', '.join(missing)} missing")
    return cleaned_indicator

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--state",
            help="Ingest data just for the state",
        )
        parser.add_argument(
            "--district",
            help="District code to import the data",
        )

    def handle(self, *args, **options):
        migrate_geojson()
        migrate_indicators()

        files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
        indicators = [
            indicator for indicator in Indicators.objects.filter(is_visible=True)]
        if options["state"]:
            files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
            state_files = [
                filename for filename in files if options["state"].lower() in filename.lower()]
            if not state_files:
                raise CommandError(
                    f"Data file for state {options['state']} missing.")
            filename = state_files[0]
            df = pd.read_csv(filename, index_col="object-id",
                             dtype={"object-id": str, "sdtcode11": str, "objectid": str})
            if options["district"]:
                import_state_data(df, filter_indicators(df, indicators), options["district"])
            else:
                import_state_data(df, filter_indicators(df, indicators))
        else:
            for filename in files:
                df = pd.read_csv(filename, index_col="object-id",
                                 dtype={"object-id": str, "sdtcode11": str, "objectid": str})
                import_state_data(df, filter_indicators(df, indicators))
