import pandas as pd
from django.contrib.gis.geos import MultiPolygon, GEOSGeometry, Polygon
from layer.models import Indicators, Unit, Geography, Data
import json



def migrate_indicators(filename="layer/IDS-DRR Model Indicators | Data Dictionary - data_dictionary.csv"):
    df = pd.read_csv(filename)
    print(df.shape)
    print(df.columns)
    # df = df.drop(columns=['category'])
    # indicators = indicator_df.columns
    # print(indicators)
    for row in df.itertuples(index=False):
        try:
            print("Processing Indicator -", row.indicatorSlug)
            try:
                Indicators.objects.get(name=row.indicatorTitle)
                print("Already Exists!")
            except Indicators.DoesNotExist:
                print("Processing Unit -", row.unit)
                try:
                    unit_obj = Unit.objects.get(name=row.unit.lower())
                    print(f"Hey! Unit {unit_obj.name} already exists!")
                except Unit.DoesNotExist:
                    unit_obj = Unit(name=row.unit.lower())
                    unit_obj.save()
                    print(f"Saved {unit_obj.name} to DB!")
                parent_obj = None
                try:
                    if row.indicatorCategory:
                        parent_obj = Indicators.objects.get(name=row.indicatorCategory)
                    else:
                        pass
                except Indicators.DoesNotExist:
                    print(f"Failed to get the parent indicator for {row.indicatorSlug}")
            #                 continue
            #     parent_obj = None
            #     if row.indicatorCategory and row.indicatorTitle != 'Composite Score':
            #         # print(row.indicatorCategory)
            #         indicator = row.indicatorCategory.split(" ")
            #         # print(indicator)
            #         if indicator[-1] == "Indicators":
            #             indicator.pop()
            #             x = ""
            #             for indc in indicator:
            #                 x = x+indc+" "
            #             print(x)
            #             try:
            #                 parent_obj = Indicators.objects.get(name=x.strip())
            #             except Indicators.DoesNotExist:
            #                 print(f"Failed to get the parent indicator for {row.indicatorTitle}")
            #                 continue
            #         else:
            #             try:
            #                 parent_obj = Indicators.objects.get(name=row.indicatorCategory)
            #             except Indicators.DoesNotExist:
            #                 print(f"Failed to get the parent indicator for {row.indicatorCategory}")
            #                 continue
                indicator_obj = Indicators(
                    name = row.indicatorTitle,
                    slug = row.indicatorSlug.lower() if row.indicatorSlug else None,
                    long_description = row.indicatorDescription if row.indicatorDescription else None,
                    # short_description = row.indicatorDescription if row.indicatorDescription else None,
                    category = row.indicatorCategory if row.indicatorCategory else None,
                    # type = row.indicatorType if row.indicatorType else None
                    unit = unit_obj,
                    data_source = row.dataSource if row.dataSource else None,
                    parent=parent_obj,
                    is_visible = True if row.visible == 'y' else False,
                )
                indicator_obj.save()
                print("Added indicator to the database.")
        except Exception as e:
            print("Process Failed with error -", e)
            
    # rint(df)
    
    

def migrate_geojson(filename="layer/assam_revenue_circles_nov2022_4326.geojson"):
    
    with open(filename) as f:
        data = json.load(f)
        
        file_name = data["name"]
        for ft in data["features"]:
            # print(type(ft))
            geom_str = json.dumps(ft['geometry'])
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
                geo_object = Geography(name=name.capitalize(), code=code, type=geo_type, geom=geom)
                geo_object.save()
            elif file_name == "assam_revenue_circles_nov2022":
                geo_type = "REVENUE CIRCLE"
                code = ft["properties"]["object_id"]
                name = ft["properties"]["revenue_ci"]
                district = ft["properties"]["district_1"]
                
                # print(district)
                parent_geo_obj = Geography.objects.get(name__iexact=district, type="DISTRICT")
                geo_object = Geography(name=name.capitalize(), code=code, type=geo_type, geom=geom, parentId=parent_geo_obj)
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
    df = df.drop(columns=['district', 'rc_area', 'year', 'month', 'rank', 'topsis-score'])
    data_columns = [cols.lower().replace("_", "-") for cols in df.columns]
    # print(data_columns)
    
    # Get all columns from DB.
    reqd_columns = Indicators.objects.filter(is_visible=True) #.values_list("name", flat=True)
    print(len(reqd_columns), reqd_columns[0])
    # indicators = indicator_df.columns
    # print(indicators)
    i=1
    for index, row in df.iterrows():
        # print(row['timeperiod'])
        print(f"Processing row - {i}")
        try:
            geography_obj = Geography.objects.get(code=row.object_id)
        except Exception as e:
            print(e)
        # for indc in reqd_columns:
        for indc_obj in reqd_columns:
            # if indc in data_columns:
            # try:
            #     indicator_obj = Indicators.objects.get(slug=indc)
            # except Exception as e:
            #     print(e)
            print(f"Adding data for RC - {geography_obj.name}")
            print(f"Adding data for Indicator - {indc_obj.slug}")
            # name = indc_obj
            data_obj = Data(
                value = row[f'{indc_obj.slug}'],
                indicator = indc_obj,
                geography = geography_obj,
                data_period = row.timeperiod,
            )
            data_obj.save()
        i+=1
            # else:
            #     continue