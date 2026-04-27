import glob
import io
import json
import os
import time

import pandas as pd
from django.contrib.gis.geos import GEOSGeometry, MultiPolygon, Polygon
from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.models import Q
from django.db.models.signals import post_delete
from layer.models import Data, Geography, Indicators, Unit
from layer.signals import invalidate_data_cache
from layer.cache_utils import invalidate_data_caches
from D4D_ContextLayer.settings import WHITELIST_INDICATORS
import sys


def migrate_indicators(filename="layer/assets/indicators/data_dict.csv"):
    df = pd.read_csv(filename)

    for row in df.itertuples(index=False):
        print("Processing Indicator -", row.indicatorSlug)
        try:
            indicator = Indicators.objects.get(slug=row.indicatorSlug.lower())
            print("Already Exists! Updating")
            indicator.name = row.indicatorTitle.strip()
            indicator.long_description = row.indicatorDescription.strip()
            indicator.category = row.indicatorCategory.strip()
            indicator.unit = _get_indicator_unit_form_row(row)
            indicator.data_source = row.dataSource.strip() if row.dataSource else None
            indicator.parent = _get_indicator_parent_from_row(row)
            indicator.is_visible = True if row.visible == "y" else False
            indicator.save()
            print("updated Indicator -", row.indicatorSlug)
        except Indicators.DoesNotExist:
            print("Processing Unit -", row.unit)
            unit_obj = _get_indicator_unit_form_row(row)
            parent_obj = _get_indicator_parent_from_row(row)

            indicator_obj = Indicators(
                name=str(row.indicatorTitle).strip(),
                slug=(
                    str(row.indicatorSlug).lower().strip()
                    if row.indicatorSlug
                    else None
                ),
                long_description=(
                    str(row.indicatorDescription).strip()
                    if row.indicatorDescription
                    else None
                ),
                # short_description = row.indicatorDescription if row.indicatorDescription else None,
                category=(
                    str(row.indicatorCategory).strip()
                    if row.indicatorCategory
                    else None
                ),
                # type = row.indicatorType if row.indicatorType else None
                unit=unit_obj,
                data_source=str(row.datasource).strip() if row.datasource else None,
                parent=parent_obj,
                is_visible=True if str(row.visible_on_platform) == "y" else False,
            )
            indicator_obj.save()
            print("Added indicator to the database.")
        print("---------------------------")


def _get_indicator_parent_from_row(row, state):
    parent_obj = None
    try:
        if row.parent and not isinstance(row.parent, float):
            parent_obj = Indicators.objects.get(
                name=row.parent.strip(), geography=state
            )
        else:
            pass
    except Indicators.DoesNotExist:
        print(f"Failed to get the parent indicator for {row.indicatorSlug.lower()}")
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


# def update_indicators(filename="layer/data_dict.csv"):
#     df = pd.read_csv(filename)
#     for row in df.itertuples(index=False):
#         slug = row.indicatorSlug
#         print("Processing Indicator -", slug)
#         try:
#             indicator = Indicators.objects.get(slug=slug.lower())
#             indicator.name = row.indicatorTitle.strip()
#             indicator.long_description = row.indicatorDescription.strip()
#             indicator.category = row.indicatorCategory.strip()
#             indicator.unit = _get_indicator_unit_form_row(row)
#             indicator.data_source = row.dataSource.strip() if row.dataSource else None
#             indicator.parent = _get_indicator_parent_from_row(row)
#             indicator.is_visible = True if row.visible == "y" else False
#             indicator.save()
#             print(f"updated Indicator - {slug}")
#
#         except Indicators.DoesNotExist:
#             print(f"Indicator with slug {slug} does not exist. ")


def migrate_geojson():
    files = sorted(glob.glob(os.getcwd() + "/layer/assets/geojson/*.geojson"))
    sorted_files = sorted(
        files,
        key=lambda x: ("_district" not in os.path.basename(x), os.path.basename(x)),
    )

    for filename in sorted_files:
        with open(filename) as f:
            print(f"Importing {filename} ...")
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

                if file_name == "assam_district":
                    geo_type = "DISTRICT"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["dtname"]
                    state = "Assam"
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
                    district = ft["properties"]["dtname"]
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
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                elif file_name == "hp_tehsil_temp":
                    geo_type = "TEHSIL"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["TEHSIL"]
                    dtcode = f'02-{ft["properties"]["dtcode11"]}'
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                elif file_name == "odisha_district":
                    geo_type = "DISTRICT"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["dtname"]
                    state = "ODISHA"
                    state_code = "21"
                    try:
                        parent_geo_obj = Geography.objects.get(
                            name__iexact=state, type="STATE"
                        )
                    except Geography.DoesNotExist:
                        parent_geo_obj = Geography(
                            name=state.capitalize(), code=state_code, type="STATE"
                        )
                        parent_geo_obj.save()
                elif file_name == "odisha_block":
                    geo_type = "BLOCK"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["block_name"]
                    dtcode = f'21-{ft["properties"]["dtcode11"]}'
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                elif file_name == "uttar_pradesh_district":
                    geo_type = "DISTRICT"
                    code = "-".join(ft["properties"]["object_id"].split("-")[:2])
                    name = ft["properties"]["dtname"]
                    state = ft["properties"]["stname"]

                    try:
                        parent_geo_obj = Geography.objects.get(
                            name__iexact=state.capitalize(), type="STATE"
                        )
                    except Geography.DoesNotExist:
                        parent_geo_obj = Geography(
                            name=state.capitalize(), code="09", type="STATE"
                        )
                        parent_geo_obj.save()
                elif file_name == "uttar_pradesh_subdistrict":
                    geo_type = "SUB DISTRICT"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["sdtname"]
                    dtcode = "-".join(code.split("-")[:2])
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                elif file_name == "bihar_district":
                    geo_type = "DISTRICT"
                    state_code = "-".join(ft["properties"]["object_id"].split("-")[:1])
                    code = "-".join(ft["properties"]["object_id"].split("-")[:2])
                    name = ft["properties"]["dtname"]
                    state = ft["properties"]["stname"]

                    try:
                        parent_geo_obj = Geography.objects.get(
                            name__iexact=state.capitalize(), type="STATE"
                        )
                    except Geography.DoesNotExist:
                        parent_geo_obj = Geography(
                            name=state.capitalize(), code=state_code, type="STATE"
                        )
                        parent_geo_obj.save()
                elif file_name == "bihar_subdistrict":
                    geo_type = "BLOCK"
                    code = ft["properties"]["object_id"]
                    name = ft["properties"]["sdtname"]
                    dtcode = "-".join(code.split("-")[:2])
                    parent_geo_obj = Geography.objects.get(code=dtcode, type="DISTRICT")
                try:
                    geo_object = Geography.objects.get(
                        code=code, parentId=parent_geo_obj
                    )
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


def import_state_data(df, indicators, g_code=None):
    if g_code:
        g_codes = [g_code]
    else:
        g_codes = df.index.unique().tolist()

    geographies = dict(
        Geography.objects.filter(code__in=g_codes)
        .exclude(type="STATE")
        .values_list("code", "pk")
    )

    for code in g_codes:
        if code not in geographies:
            print(f"Geography location for: {code} is missing")
    if not geographies:
        print("No matching geographies found, skipping data import")
        return

    rows = df[df.index.isin(geographies)]

    # Disconnect post_delete signal so Django can DELETE directly without
    # SELECTing all rows first to fire per-instance signals.
    post_delete.disconnect(invalidate_data_cache, sender=Data)
    try:
        print(f"Deleting existing data for {len(geographies)} geographies...", end=" ", flush=True)
        start = time.time()
        Data.objects.filter(
            geography_id__in=list(geographies.values()),
            data_period__in=rows["timeperiod"].unique().tolist(),
        ).delete()
        print(f"{time.time() - start:.1f}s")

        melted = rows.reset_index().melt(
            id_vars=["object-id", "timeperiod"],
            value_vars=[ind.slug for ind in indicators],
            var_name="slug",
            value_name="value",
        )

        # Map to foreign key IDs in vectorized pandas.
        melted["indicator_id"] = melted["slug"].map({ind.slug: ind.pk for ind in indicators})
        melted["geography_id"] = melted["object-id"].map(geographies)

        # COPY bypasses Django's ORM, so set added/modified manually
        # to emulate auto_now_add and auto_now on the Data model.
        now = pd.Timestamp.now(tz="UTC")
        melted["added"] = now
        melted["modified"] = now

        print(f"Creating {len(melted)} data points...", end=" ", flush=True)
        start = time.time()
        # Build a TSV buffer for PostgreSQL COPY, which expects tab-separated
        # values with \N for NULLs and no header row.
        buf = io.StringIO()
        melted[["value", "indicator_id", "geography_id", "timeperiod", "added", "modified"]].to_csv(
            buf, sep="\t", header=False, index=False, na_rep="\\N",
        )
        buf.seek(0)
        with connection.cursor() as cursor:
            cursor.copy_from(
                buf,
                Data._meta.db_table,
                columns=("value", "indicator_id", "geography_id", "data_period", "added", "modified"),
            )
        print(f"{time.time() - start:.1f}s")
    finally:
        post_delete.connect(invalidate_data_cache, sender=Data)
        # Perform the post_delete signal once.
        invalidate_data_caches()


def filter_indicators(df, indicators):
    cleaned_indicator = [ind for ind in indicators if ind.slug in df.columns]
    if missing := [ind.slug for ind in indicators if ind.slug not in df.columns]:
        print(f"Missing indicators: {', '.join(missing)}")
    return cleaned_indicator


def get_indicators(state):
    visible_indicators = Indicators.objects.filter(
        is_visible=True, geography__name__iexact=state
    )
    indicators = [indicator for indicator in visible_indicators]
    try:
        whitelist_indicators = Indicators.objects.filter(
            slug__in=WHITELIST_INDICATORS, geography__name__iexact=state
        )
        return indicators + list(whitelist_indicators)
    except Indicators.DoesNotExist:
        return indicators


def update_data(state, district):
    files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
    if state:
        indicators = get_indicators(state.replace("_", " "))
        files = glob.glob(os.getcwd() + "/layer/assets/data/*_data.csv")
        state_files = [
            filename for filename in files if state.lower() in filename.lower()
        ]
        if not state_files:
            raise CommandError(f"Data file for state {state} missing.")
        filename = state_files[0]
        df = pd.read_csv(
            filename,
            index_col="object-id",
            dtype={"object-id": str, "sdtcode11": str, "objectid": str},
            low_memory=False,
        )
        if district:
            print(f"\nImporting {district!r} data:")
            import_state_data(df, filter_indicators(df, indicators), district)
        else:
            print(f"\nImporting {state!r} data:")
            import_state_data(df, filter_indicators(df, indicators))
    else:
        for filename in files:
            df = pd.read_csv(
                filename,
                index_col="object-id",
                dtype={"object-id": str, "sdtcode11": str, "objectid": str},
            )
            state = filename.split("/")[-1].replace("_data.csv", "")
            state = state.replace("_", " ")
            print(f"\nImporting {state!r} data:")
            indicators = get_indicators(state)
            import_state_data(df, filter_indicators(df, indicators))


def import_state_indicators(df: pd.DataFrame, state: Geography):
    for row in df.itertuples(index=False):
        indicator_slug = getattr(row, "indicatorSlug", "")

        # CSV sometimes has blank rows; pandas reads blanks as NaN (float).
        # Skip those rows early to avoid `.lower()` crashes.
        if pd.isna(indicator_slug) or not str(indicator_slug).strip():
            continue

        try:
            indicator = Indicators.objects.get(
                slug=str(indicator_slug).lower().strip(), geography=state
            )
            indicator.name = str(getattr(row, "indicatorTitle", "")).strip()
            indicator.long_description = (
                str(getattr(row, "indicatorDescription", "")).strip() or None
            )
            indicator.category = (
                str(getattr(row, "indicatorCategory", "")).strip() or None
            )
            indicator.unit = _get_indicator_unit_form_row(row)
            indicator.data_source = str(getattr(row, "datasource", "")).strip() or None
            indicator.parent = _get_indicator_parent_from_row(row, state)
            indicator.is_visible = str(getattr(row, "visible_on_platform", "")) == "y"
            if 'IDS_dataSpace' in df.columns:
                raw = df.iloc[i]['IDS_dataSpace']
                if pd.isna(raw):
                    indicator.IDS_dataSpace = None
                else:
                    link = str(raw).strip()
                    indicator.IDS_dataSpace = link or None
            indicator.save()
            # print("\rUpdated already existing indicator", flush=True)

        except Indicators.DoesNotExist:
            # unit = getattr(row, "unit", "")
            # print("\rProcessing Unit -", unit, flush=True)
            unit_obj = _get_indicator_unit_form_row(row)
            parent_obj = _get_indicator_parent_from_row(row, state)

            indicator_obj = Indicators(
                name=str(getattr(row, "indicatorTitle", "")).strip(),
                slug=str(indicator_slug).lower().strip() if indicator_slug else None,
                long_description=str(getattr(row, "indicatorDescription", "")).strip()
                or None,
                category=str(getattr(row, "indicatorCategory", "")).strip() or None,
                unit=unit_obj,
                data_source=str(getattr(row, "datasource", "")).strip() or None,
                parent=parent_obj,
                is_visible=str(getattr(row, "visible_on_platform", "")) == "y",
                geography=state,
            )
            if 'IDS_dataSpace' in df.columns:
                raw = df.iloc[i]['IDS_dataSpace']
                if pd.isna(raw):
                    indicator_obj.IDS_dataSpace = None
                else:
                    link = str(raw).strip()
                    indicator_obj.IDS_dataSpace = link or None
            indicator_obj.save()
            # print("\rAdded indicator to the database.", flush=True)


def update_indicators(state):
    files = glob.glob(os.getcwd() + "/layer/assets/indicators/*_indicators.csv")
    if state:
        state_files = [
            filename for filename in files if state.lower() in filename.lower()
        ]
        if not state_files:
            raise CommandError(f"Indicator file for state {state} missing.")
        filename = state_files[0]
        df = pd.read_csv(filename)
        state = state.replace("_", " ")
        state_geo = Geography.objects.get(name__iexact=state, type="STATE")
        import_state_indicators(df, state_geo)
    else:
        for filename in files:
            df = pd.read_csv(filename)
            state = filename.split("/")[-1].replace("_indicators.csv", "")
            state = state.replace("_", " ")
            state_geo = Geography.objects.get(name__iexact=state, type="STATE")
            import_state_indicators(df, state_geo)


class Command(BaseCommand):
    """
    A Django management command for importing geographical and indicator data.

    This command migrates geojson data, migrates indicators, and imports state and district data
    from CSV files. It can be run for all states or a specific state and district.
    """

    def add_arguments(self, parser):
        """
        Add command line arguments to the parser.

        Args:
            parser (ArgumentParser): The argument parser instance.
        """
        parser.add_argument(
            "--state",
            help="Ingest data just for the state",
        )
        parser.add_argument(
            "--district",
            help="District code to import the data",
        )

    def handle(self, *args, **options):
        """
        Execute the command to import geographical and indicator data.

        This method performs the following steps:
        1. Migrates geojson data
        2. Migrates indicators
        3. Imports state and/or district data from CSV files

        Args:
            *args: Variable length argument list.
            **options: Arbitrary keyword arguments. Expected keys are:
                - state (str, optional): The name of the state to import data for.
                - district (str, optional): The district code to import data for.

        Raises:
            CommandError: If the data file for the specified state is missing.

        Returns:
            None
        """
        state = options.get("state", None)
        district = options.get("district", None)

        start = time.time()
        migrate_geojson()
        print(f"Geojson: {time.time() - start:.1f}s")

        start = time.time()
        update_indicators(state)
        print(f"Indicators: {time.time() - start:.1f}s")

        start = time.time()
        update_data(state, district)
        print(f"Data: {time.time() - start:.1f}s")
