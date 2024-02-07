import json

import pandas as pd
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from layer.models import Data, Geography, Indicators, Unit


def migrate_indicators(filename="layer/ids_drr_data_dict.csv"):
    df = pd.read_csv(filename)
    # print(df.shape)
    # print(df.columns)

    for row in df.itertuples(index=False):
        try:
            print("Processing Indicator -", row.indicatorSlug)
            try:
                Indicators.objects.get(slug=row.indicatorSlug.lower())
                print("Already Exists!")
            except Indicators.DoesNotExist:
                print("Processing Unit -", row.unit)
                if row.unit and not isinstance(row.unit, float):
                    try:
                        unit_obj = Unit.objects.get(name=row.unit.lower())
                        print(f"Hey! Unit {unit_obj.name} already exists!")
                    except Unit.DoesNotExist:
                        unit_obj = Unit(name=row.unit.lower())
                        unit_obj.save()
                        print(f"Saved {unit_obj.name} to DB!")
                else:
                    print("Skipping Indicator as no Unit was provided!")
                    continue
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

                indicator_obj = Indicators(
                    name=row.indicatorTitle.strip(),
                    slug=row.indicatorSlug.lower().strip()
                    if row.indicatorSlug
                    else None,
                    long_description=row.indicatorDescription.strip()
                    if row.indicatorDescription
                    else None,
                    # short_description = row.indicatorDescription if row.indicatorDescription else None,
                    category=row.indicatorCategory.strip()
                    if row.indicatorCategory
                    else None,
                    # type = row.indicatorType if row.indicatorType else None
                    unit=unit_obj,
                    data_source=row.dataSource.strip() if row.dataSource else None,
                    parent=parent_obj,
                    is_visible=True if row.visible == "y" else False,
                )
                indicator_obj.save()
                print("Added indicator to the database.")
            print("---------------------------")
        except Exception as e:
            print("Process Failed with error -", e)


def migrate_geojson(filename="layer/assam_revenue_circles_nov2022_4326.geojson"):
    with open(filename) as f:
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
                geo_object = Geography(
                    name=name.capitalize(), code=code, type=geo_type, geom=geom
                )
                geo_object.save()
            elif file_name == "assam_revenue_circles_nov2022":
                geo_type = "REVENUE CIRCLE"
                code = ft["properties"]["object_id"]
                name = ft["properties"]["revenue_ci"]
                district = ft["properties"]["district_1"]

                # print(district)
                parent_geo_obj = Geography.objects.get(
                    name__iexact=district, type="DISTRICT"
                )
                geo_object = Geography(
                    name=name.capitalize(),
                    code=code,
                    type=geo_type,
                    geom=geom,
                    parentId=parent_geo_obj,
                )
                geo_object.save()
            # else:
            #     raise TypeError(
            #         '{} not acceptable for this model'.format(geom.geom_type)
            #     )

            # except TypeError as e:
            #     print(e)
            # print(ft["geometry"]["coordinates"])


def migrate_data(filename="layer/MASTER_DATA_FRONTEND_2022onwards.csv"):
    df = pd.read_csv(filename)
    print(df.shape)
    # print(df.columns)
    # df = df.drop(columns=['district', 'rc_area', 'year', 'month', 'rank', 'topsis-score'])
    # data_columns = [cols for cols in df.columns] #.lower().replace("_", "-")
    # print(data_columns)

    # Get all columns from DB.
    reqd_columns = Indicators.objects.filter(
        is_visible=True
    )  # .values_list("name", flat=True)
    print(len(reqd_columns), reqd_columns[0])
    # indicators = indicator_df.columns
    # print(indicators)
    i = 1
    for index, row in df.iterrows():
        # print(row['timeperiod'])
        print(f"Processing row - {i}")
        try:
            geography_obj = Geography.objects.get(code=row.object_id)
            if geography_obj.type == "DISTRICT":
                reqd_columns = reqd_columns.filter(parent__slug="risk-score")
                print(len(reqd_columns))
        except Exception as e:
            print(e)
        # for indc in reqd_columns:
        for indc_obj in reqd_columns:
            print(f"Adding data for RC - {geography_obj.name}")
            print(f"Adding data for Indicator - {indc_obj.slug}")
            # name = indc_obj
            data_obj = Data(
                value=row[f"{indc_obj.slug}"],
                indicator=indc_obj,
                geography=geography_obj,
                data_period="2023_08",
            )
            data_obj.save()
        i += 1
        # else:
        #     continue
