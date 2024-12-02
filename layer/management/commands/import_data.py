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
        key=lambda x: ("_district" not in os.path.basename(x), os.path.basename(x)),
    )

    for filename in sorted_files:
        with open(filename) as f:
            print(f"Adding data from {os.path.basename(filename)} to database....")
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
                    state_code = "02" # TODO: add statecode to HP geojson
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
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                elif file_name == "hp_tehsil_temp":
                    geo_type = "TEHSIL"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["TEHSIL"]
                    dtcode = f'02-{ft["properties"]["dtcode11"]}'
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
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

def migrate_data(filename=None):
    # Get all the data files from the directory.
    files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
    # Iterate over all the files.
    for filename in files:
        print("--------")
        print(f"Addind data from {os.path.basename(filename)} to database....")
        print("--------")
        time.sleep(3)

        # Using object-id as index, so they can be used as str and not int or float.
        df = pd.read_csv(filename, index_col="object-id", dtype={"object-id": str, "sdtcode11": str, "objectid": str})
        print(f"Total no of rows available - {df.shape[0]}")
        # Get all columns visible on the platform from DB.
        reqd_columns = Indicators.objects.filter(is_visible=True)

        i = 1
        # Iterate over each row and save the data in DB.
        for index, row in df.iterrows():
            print(f"Processing row - {i}")
            try:
                # Get the required geography object.
                if "assam" in filename.lower():
                    geography_obj = Geography.objects.get(Q(code=index), ~Q(type="STATE"))
                else:
                    geography_obj = Geography.objects.get(Q(code=index), ~Q(type="STATE"))
                    # if pd.isna(row["district"]):
                    #     geography_obj = Geography.objects.get(Q(code=str(row.sdtcode11).zfill(3)), ~Q(type="STATE"))
                    # else:
                    #     geography_obj = Geography.objects.get(Q(code=str(row.sdtcode11).zfill(5)), ~Q(type="STATE"))
                # Filter visible columns for Districts (Only factors, no variables).
                # if geography_obj.type == "DISTRICT":
                #     reqd_columns = reqd_columns.filter(
                #         Q(parent__slug="risk-score") | Q(slug="risk-score")
                #     )
            except Exception as e:
                print(e, row)
                break

            # Iterating over each indicator.
            # Each row has data for every indicator(factors+variables) for a time period.
            for indc_obj in reqd_columns:
                print(f"Adding data for RC - {geography_obj.name}")
                print(f"Adding data for Indicator - {indc_obj.slug}")
                existing = Data.objects.filter(indicator__slug=indc_obj.slug, geography__code=geography_obj.code,
                                               data_period=row.timeperiod)
                if existing.exists():
                    print(
                        f"Deleting existing objects for {indc_obj.slug} in {geography_obj.name} for period {row.timeperiod}")
                    [e.delete() for e in existing]
                try:
                    data_obj = Data(
                        value=row[f"{indc_obj.slug}"],
                        indicator=indc_obj,
                        geography=geography_obj,
                        data_period=row.timeperiod,
                    )
                    data_obj.save()
                except KeyError:
                    continue
            i += 1
        print(f"Total rows added to DB - {i}")


class Command(BaseCommand):
    def handle(self, *args, **options):
        migrate_geojson()
        migrate_indicators()
        migrate_data()
