import datetime
from faker import Faker
import os
from functools import lru_cache
from io import BytesIO
from unicodedata import category

from django.http.response import async_to_sync
import httpx
from asgiref.sync import sync_to_async
from django.db.models import Q, F
from django.http import HttpResponse
from reportlab.lib import colors
from reportlab.lib.colors import HexColor, Color
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, ListFlowable, ListItem

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD, CHART_API_BASE_URL, DATA_RESOURCE_MAP
from layer.models import Data, Geography, Indicators

from collections import defaultdict

import requests
import io

# roads, bridge, embankments-affected
month_highlight_table_indicators = ["inundation-pct", "sum-population", "human-live-lost",
                                    "population-affected-total", "crop-area", "total-animal-affected", "total-tender-awarded-value", "roads", "bridge", "embankments-affected"]


def register_google_font(font_name, font_url_bold=None, font_url_regular=None):
    """Downloads and registers a Google Font with ReportLab, defaults to Helvetica if fails."""

    try:
        if font_url_bold:
            response_bold = requests.get(font_url_bold)
            response_bold.raise_for_status()
            font_data_bold = io.BytesIO(response_bold.content)
            pdfmetrics.registerFont(
                TTFont(f"{font_name}-Bold", font_data_bold))

        if font_url_regular:
            response_regular = requests.get(font_url_regular)
            response_regular.raise_for_status()
            font_data_regular = io.BytesIO(response_regular.content)
            pdfmetrics.registerFont(TTFont(font_name, font_data_regular))

        print(f"{font_name} registered successfully.")  # Success message

    except requests.exceptions.RequestException as e:
        print(f"Error registering {font_name}: {e}")
        print(f"Falling back to default font (Helvetica) for {font_name}.")
        # No need to explicitly register Helvetica, it's a built-in ReportLab font
        return False  # Indicate failure, but the code will continue

    return True  # Indicate success


# Custom Styles
styles = getSampleStyleSheet()
# Register Noto Sans (replace with your desired Google Font URLs)
# Replace with the actual URL
noto_sans_bold_url = "https://fonts.gstatic.com/s/notosans/v2/NotoSans-Bold.ttf"
# Replace with the actual URL
noto_sans_regular_url = "https://fonts.gstatic.com/s/notosans/v2/NotoSans-Regular.ttf"

font_registered = register_google_font(
    "NotoSans", noto_sans_bold_url, noto_sans_regular_url)

title_style = ParagraphStyle(
    "TitleStyle",
    parent=styles["Title"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=18,
    leading=22,
    alignment=1,  # Centered
)
heading_1_style = ParagraphStyle(
    "Heading1Style",
    parent=styles["Heading1"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=16,
    leading=16,
    spaceAfter=10,
)

heading_2_style = ParagraphStyle(
    "Heading2Style",
    parent=styles["Heading2"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=14,
    leading=18,
    spaceAfter=10,
)
heading_3_style = ParagraphStyle(
    "Heading3Style",
    parent=styles["Heading3"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=12,
    leading=20,
    spaceAfter=10,
)

body_style = ParagraphStyle(
    "BodyStyle",
    parent=styles["BodyText"],
    fontName="NotoSans" if font_registered else "Helvetica",
    fontSize=10,
)

table_header_style = ParagraphStyle(
    "TableHeaderStyle",
    parent=styles["BodyText"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=10,
    alignment=1,
    # leading=20,
    # spaceAfter=10,
)

table_body_style = ParagraphStyle(
    "TableBodyStyle",
    parent=styles["BodyText"],
    fontName="NotoSans" if font_registered else "Helvetica",
    fontSize=10,
    alignment=1,
)

# Global variables to set state and time period in page footers
page_level_state = ''
page_level_time_period = ''


def set_page_level_state_and_time_period(state, time_period):
    global page_level_state
    global page_level_time_period
    page_level_state = state
    page_level_time_period = time_period


async def fetch_chart(client, chart_payload, resource_id):
    output_path = f"layer/assets/charts/{Faker().file_name(extension='png')}"
    try:
        timeout = httpx.Timeout(10.0, read=None)
        response = await client.post(f"{CHART_API_BASE_URL}{resource_id}/?response_type=file", json=chart_payload, timeout=timeout)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            return output_path
        else:
            print(
                f"Failed to fetch chart:::::::::::::::: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching chart: {e}")
        return None


# @lru_cache
async def get_top_vulnerable_districts(time_period, geo_filter=None):
    def filter_data():
        data_obj = Data.objects.filter(data_period=time_period).select_related(
            "geography", "indicator", "indicator__parent", "indicator__parent__parent", "geography__parentId"
        )
        if geo_filter:
            data_obj = data_obj.filter(
                Q(geography__parentId__code=geo_filter) | Q(
                    geography__code=geo_filter)
            )
        else:
            data_obj = data_obj.filter(geography__parentId__parentId=None)
        data_obj = data_obj.filter(
            indicator__is_visible=True, indicator__parent=None).distinct()

        results = list(data_obj.order_by("-value"))

        unique_geographies = {}
        for item in results:
            geo_id = item.geography.id
            if geo_id not in unique_geographies:
                unique_geographies[geo_id] = item

        final_results = list(unique_geographies.values())

        final_results.sort(key=lambda x: x.value, reverse=True)

        return final_results[:5]

    return await sync_to_async(filter_data)()


# Group data by geography
async def group_by_geography(data_list, expected_indicators=[]):
    grouped_data = defaultdict(
        lambda: {"geography": None, "indicators": {}})

    for item in data_list:
        geography = item.geography
        indicator = item.indicator
        value = item.value

        # # Initialize geography if not already present
        if grouped_data[geography]["geography"] is None:
            grouped_data[geography]["geography"] = geography

        # Add indicator to the geography's indicators dictionary
        grouped_data[geography]["indicators"][indicator.slug] = value
    grouped_data = list(grouped_data.values())
    # find the missing indicators in each grouped_data in comparison with expected indicators and assign NA to the indicators
    for data_group in grouped_data:
        missing_indicators = set(expected_indicators) - \
            set(data_group["indicators"].keys())
        for indicator in missing_indicators:
            data_group["indicators"][indicator] = "NA"

    return grouped_data


# @lru_cache
async def get_major_indicators_data(time_period, geo_filter):

    data_obj = await sync_to_async(Data.objects.filter)(
        indicator__is_visible=True, indicator__parent__parent=None, data_period=time_period
    )

    data_obj = await sync_to_async(data_obj.select_related)(
        "geography",
        "indicator",
        "indicator__parent",
        "indicator__parent__parent",
        "geography__parentId",
    )

    data_obj = await sync_to_async(data_obj.filter)(
        Q(geography__parentId__code=geo_filter) | Q(
            geography__code=geo_filter)
    )

    data_list = await sync_to_async(data_obj.order_by)("-value")

    data_list = await sync_to_async(list)(data_list)
    result = await group_by_geography(data_list)
    result.sort(key=lambda x: (x['indicators']
                ["risk-score"], x['geography'].name))

    # Return top 5 if overall flood risk districts are <5 else return all districts with 5 overall score
    high_risk_districts = [
        district for district in result if district['indicators']["risk-score"] >= 5]

    return result[-5:] if len(high_risk_districts) < 5 else high_risk_districts


async def get_district_highlights(time_period, geo_filter):

    districts = await get_major_indicators_data(time_period, geo_filter)

    districts = [district['geography'] for district in districts]

    data = await sync_to_async(Data.objects.filter)(geography__in=districts, indicator__slug__in=month_highlight_table_indicators, data_period=time_period)

    data = await sync_to_async(data.select_related)(
        "geography",
        "indicator",
        "indicator__parent",
        "indicator__parent__parent",
        "geography__parentId",
    )
    data = await sync_to_async(list)(data)

    data = await group_by_geography(data, month_highlight_table_indicators)

    # add roads, bridge and embankments affected to create a new property infrastructure damaged
    for district in data:
        district['indicators']['infrastructure-damaged'] = district['indicators']['roads'] + \
            district['indicators']['bridge'] + \
            district['indicators']['embankments-affected']

    return data


@lru_cache
async def get_filtered_data(time_period, indicator_filter=None, geo_filter=None):
    def filter_data():
        data_obj = Data.objects.filter(
            indicator__is_visible=True, data_period=time_period
        ).select_related(
            "geography",
            "indicator",
            "indicator__parent",
            "indicator__parent__parent",
            "geography__parentId",
        )

        if indicator_filter:
            data_obj = data_obj.filter(
                Q(indicator__slug=indicator_filter) | Q(
                    indicator__parent__slug=indicator_filter)
            )
        else:
            data_obj = data_obj.filter(indicator__parent=None)

        if geo_filter:
            data_obj = data_obj.filter(
                Q(geography__parentId__code__in=[geo_filter]) | Q(
                    geography__code__in=[geo_filter])
            )

        return list(data_obj.order_by("-value"))

    return await sync_to_async(filter_data)()


async def generate_pdf(doc, elements):
    """
    Generate the PDF in a thread-safe way using sync_to_async.

    Args:
        doc (SimpleDocTemplate): The SimpleDocTemplate instance.
        elements (list): The list of elements to build the PDF.

    Returns:
        BytesIO: The generated PDF as a buffer.
    """
    pdf_buffer = BytesIO()
    await sync_to_async(doc.build)(elements)
    pdf_buffer.seek(0)
    return pdf_buffer


async def get_latest_time_period(geo_code=None):
    latest = (
        await Data.objects.values_list("data_period", flat=True)
        .annotate(custom_ordering=F("data_period"))
        .distinct()
        .order_by("-custom_ordering")
        .afirst()
    )

    if latest:
        return datetime.datetime.strptime(latest, "%Y_%m")
    return None


def get_last_three_months(date_obj):
    last_3_months = [(date_obj.month - i - 1) % 12 + 1 for i in range(3)]
    last_3_months_str = [
        f"{date_obj.year - ((date_obj.month - i - 1) // 12):04d}_{last_3_months[i]:02d}"
        for i in range(3)
    ]
    return last_3_months_str


def add_header_footer(canvas_obj, doc):
    """
    Add a header and footer to each page.

    Args:
        canvas_obj: The canvas object.
        doc: The document object.
    """
    width, height = A4

    # Add an image to the left in the header
    header_image_path = "layer/IDS_DRR_Logo.png"
    try:
        canvas_obj.drawImage(header_image_path, 40, height -
                             50, width=260, height=30, preserveAspectRatio=True, mask='auto')
    except Exception as e:
        print(f"Error loading header image: {e}")

    # Header
    # header_text = "IDS-DRR | Intelligent Data Solution for Disaster Risk Reduction"
    # canvas_obj.setFont("Helvetica-Bold", 8)
    # canvas_obj.drawString(40, height - 30, header_text)

    # Add an image to the right in the header
    header_image_path = "layer/CDL_Primary Logo.png"
    # draw Image with background transparent
    try:
        canvas_obj.drawImage(header_image_path, width - 100, height -
                             60, width=90, height=50, preserveAspectRatio=True, mask='auto')
    except Exception as e:
        print(f"Error loading header image: {e}")

    # Footer
    footer_text = f"State Report: {page_level_state} | {page_level_time_period}"
    canvas_obj.setFont("NotoSans" if font_registered else "Helvetica", 8)
    canvas_obj.drawString(40, 30, footer_text)  # Left-justified footer text

    # Page number on the right in the footer of {doc.page_count}
    page_number_text = f"Page {doc.page}"
    footer_width = pdfmetrics.stringWidth(
        page_number_text, "NotoSans" if font_registered else "Helvetica", 10)
    canvas_obj.drawString(width - footer_width - 40, 30,
                          page_number_text)  # Right-aligned page number


class CustomDocTemplate(SimpleDocTemplate):
    """
    Custom SimpleDocTemplate to add header and footer.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def build(
        self,
        flowables,
        onFirstPage=add_header_footer,
        onLaterPages=add_header_footer,
        canvasmaker=canvas.Canvas,
    ):
        """
        Overridden build method to add header and footer.
        """
        # self.page_count = len(
        #     flowables)  # Total page count for dynamic numbering

        # Below also didn't work
        # Track page count using afterPage hook
        # def onLaterPagesWithCount(canvas_obj, doc):
        #     self.page_count += 1
        #     onLaterPages(canvas_obj, doc)
        super().build(flowables, onFirstPage=onFirstPage,
                      onLaterPages=onLaterPages, canvasmaker=canvasmaker)


async def add_total_tender_awarded_value_chart(elements, time_period_prev_months_array, time_period, geo_filter):
    districts = await get_major_indicators_data(time_period, geo_filter)

    districts: list[Geography] = [district['geography']
                                  for district in districts]
    y_axis_columns = []
    for district in districts:
        y_axis_columns.append({
            "field_name": f"{district.code}",
            "label": f"{district.name}",
            "color": f"{Faker().color()}",
            "aggregate_type": "SUM"
        })

    async with httpx.AsyncClient() as client:
        chart_payload = {
            "chart_type": "GROUPED_BAR_VERTICAL",
            "x_axis_column": "financial-year",
            "time_column": "financial-year",
            "x_axis_label": "Financial Year",
            "y_axis_column": y_axis_columns,
            "y_axis_label": "Total render awarded value",
            "show_legend": "true",
            "filters": [
                {
                    "column": "financial-year",
                    "operator": "in",
                    "value": "2022-2023,2023-2024,2024-2025",
                },
                {
                    "column": "factor",
                    "operator": "==",
                    "value": "total-tender-awarded-value",
                }
            ],
        }

        chart = await fetch_chart(client, chart_payload, "a165cb92-8c92-49d5-83bb-d8a875c61a57")

        image_table_data = [[Image(chart, width=500, height=300)]]
        table_with_images = await get_table(image_table_data, [500, 200], TableStyle([
            ('GRID', (0, 0), (-1, -1), 0, colors.transparent),
            ("PADDING", (0, 0), (-1, -1), 5)
        ]))

        elements.append(table_with_images)
        elements.append(Spacer(1, 20))
    return elements


async def add_losses_and_damages_times_series(elements, time_period_prev_months_array, time_period, geo_filter):
    districts = await get_major_indicators_data(time_period, geo_filter)

    districts: list[Geography] = [district['geography']
                                  for district in districts]
    y_axis_columns = []
    for district in districts:
        y_axis_columns.append({
            "field_name": f"{district.code}",
            "label": f"{district.name}",
            "color": f"{Faker().color()}",
        })

    async with httpx.AsyncClient() as client:
        chart_payload1 = {
            "chart_type": "MULTILINE",
            "x_axis_column": "timeperiod",
            "x_axis_label": "Month",
            "y_axis_column": y_axis_columns,
            "y_axis_label": "Number of people affected",
            "show_legend": "true",
            "filters": [
                {
                    "column": "timeperiod",
                    "operator": "in",
                    "value": ",".join(time_period_prev_months_array),
                },
                {
                    "column": "factor",
                    "operator": "==",
                    "value": "population-affected-total",
                },
            ],
        }

        chart_payload2 = {
            "chart_type": "MULTILINE",
            "x_axis_column": "timeperiod",
            "x_axis_label": "Month",
            "y_axis_column": y_axis_columns,
            "y_axis_label": "Score",
            "show_legend": "true",
            "filters": [
                {
                    "column": "timeperiod",
                    "operator": "in",
                    "value": ",".join(time_period_prev_months_array),
                },
                {
                    "column": "factor",
                    "operator": "==",
                    "value": "total-infrastructure-damage",
                },
            ],
        }

        chart1 = await fetch_chart(client, chart_payload1, "a165cb92-8c92-49d5-83bb-d8a875c61a57")
        chart2 = await fetch_chart(client, chart_payload2, "a165cb92-8c92-49d5-83bb-d8a875c61a57")

        image_table_data = [[Image(chart1, width=300, height=200),
                             Image(chart2, width=300, height=200)]]
        table_with_images = await get_table(image_table_data, [300, 300], TableStyle([
            ('GRID', (0, 0), (-1, -1), 0, colors.transparent),
            ("PADDING", (0, 0), (-1, -1), 5)
        ]))

        elements.append(table_with_images)
        elements.append(Spacer(1, 20))
    return elements


async def cleanup_temp_files():
    """
    Cleanup temporary files generated during the report generation process.
    """
    import glob
    chart_files = glob.glob("layer/assets/charts/*.png")
    for file in chart_files:
        os.remove(file)


async def generate_report(request):
    if request.method == "GET":
        # Prepare PDF buffer and styles
        pdf_buffer = BytesIO()

        geo_code = request.GET.get("geo_code", "18")
        time_period = request.GET.get("time_period", DEFAULT_TIME_PERIOD)
        time_period_parsed = datetime.datetime.strptime(
            time_period, "%Y_%m")
        time_period_string = time_period_parsed.strftime("%B %Y")

        # Set the type filter based on state.
        state = await sync_to_async(Geography.objects.get)(code=geo_code, type="STATE")

        set_page_level_state_and_time_period(state.name, time_period_string)

        # doc = CustomDocTemplate(pdf_buffer, pagesize=A4)
        # Development mode to update a local document for testing
        doc = CustomDocTemplate("test_output.pdf", pagesize=A4)

        risk_mapping_text = {
            '1.0': 'Very Low',
            '2.0': 'Low',
            '3.0': 'Medium',
            '4.0': 'High',
            '5.0': 'Very High',
        }

        # Create a time period array with 2 months prior to current selected month along with the current month
        time_period_prev_months_array = [(time_period_parsed - datetime.timedelta(days=60)).strftime(
            "%Y_%m"), (time_period_parsed - datetime.timedelta(days=30)).strftime("%Y_%m"), time_period_parsed.strftime("%Y_%m")]

        # Elements list for PDF
        elements = []

        # --------------------------------------------------------
        # Title Section
        elements.append(
            Paragraph(f"State Report: {state.name} | {time_period_string}", title_style))
        elements.append(Spacer(1, 20))

        # Flood Risk Overview
        elements.append(Paragraph("Flood Risk Overview", heading_2_style))

        # --------------------------------------------------------
        # Overview Section
        try:
            data_obj = await get_top_vulnerable_districts(
                time_period, state.code
            )

            elements.append(Paragraph(
                f"As of {time_period_string}, the following 5 districts in {state.name} faced highest risk - ", body_style))

            elements.append(ListFlowable([
                ListItem(Paragraph(data.geography.name, body_style)) for data in data_obj
            ],  bulletType='1',  # Use '1' for numbered list
                start='1',       # Start numbering from 1
                # Overall indentation of the list (adjust as needed)
                leftIndent=12,
                # Indent the numbers by 18 points (adjust as needed)
                bulletFontSize=10,  # Set the font size of the numbers to match the text
                bulletColor=colors.black,
                bulletFormat="%s."
            ))

            elements.append(Paragraph(
                "<i>Note: The Flood Risk is calculated as a function of Hazard, Exposure, Vulnerability and Government Response.</i>", body_style))

            elements.append(Spacer(1, 20))
        except Exception as e:
            elements.append(
                Paragraph(f"Error fetching district data: {e}", body_style))

        # --------------------------------------------------------
        # Key Figures Section
        elements.append(
            Paragraph("Top most at risk districts: Key Figures", heading_2_style))

        # Factor wise risk assessment
        elements.append(
            Paragraph("Factor wise risk assessment", heading_3_style))

        majorIndicatorsData = await get_major_indicators_data(time_period, state.code)

        district_table_data = [
            [Paragraph(table_title, table_header_style) for table_title in [
                "District", "Risk Score", "Flood Hazard", "Exposure", "Vulnerability", "Government Response"]]
        ]
        for data in majorIndicatorsData:
            district_table_data.append([Paragraph(data['geography'].name, table_body_style), risk_mapping_text[str(data['indicators']["risk-score"])], risk_mapping_text[str(data['indicators']["flood-hazard"])], risk_mapping_text[str(
                data['indicators']["exposure"])], risk_mapping_text[str(data['indicators']["vulnerability"])], risk_mapping_text[str(data['indicators']["government-response"])]])

        district_table = await get_table(district_table_data, [100, 80, 80, 80, 80, 80])
        elements.append(district_table)
        elements.append(Spacer(1, 20))

        # Month Highlights sub-section
        elements.append(
            Paragraph(f"Highlights for the month of {time_period_string}", heading_3_style))

        # indicator_filter = request.GET.get("indicator")
        # data_obj = await get_filtered_data(time_period.strftime("%Y_%m"), None, geo_filter)
        data_obj = await get_district_highlights(
            time_period, state.code
        )

        # district_table_data = [
        #     [Paragraph(header, table_header_style) for header in ["District", "Inundation pct", "Sum Population", "Human Live Lost",
        #                                                           "population affected total", "crop area", "total animal affected",
        #                                                           "total tender awarded value"]]
        # ]

        # print()

        a = ["District", "Inundation pct", "Sum Population", "Human Live Lost",
             "Population Affected Total", "Crop Area", "No of Infrastructure damaged", "Total Animal Affected", "Total Tender Awarded Value"]
        b = []
        for header_value in a:
            b.append(Paragraph(header_value, table_header_style))
        district_table_data = [b]
        # district_table_data = [a]
        for data in data_obj:
            values = [data['indicators'][indicator]
                      for indicator in ["inundation-pct", "sum-population", "human-live-lost",
                                        "population-affected-total", "crop-area", "infrastructure-damaged", "total-animal-affected",  "total-tender-awarded-value"]]
            row = [Paragraph(data['geography'].name,
                             table_body_style)] + values
            district_table_data.append(row)

        district_table = await get_table(district_table_data, [70, 70, 70, 70, 70, 70, 70, 70])
        elements.append(district_table)
        elements.append(Spacer(1, 20))

        # Losses and Damages section
        elements.append(Paragraph("Losses and Damages", heading_2_style))

        time_period_str = ', '.join([datetime.datetime.strptime(
            period, "%Y_%m").strftime("%B %Y") for period in time_period_prev_months_array])

        elements.append(
            Paragraph(f"Time Series for {time_period_str}", heading_3_style))

        elements = await add_losses_and_damages_times_series(elements, time_period_prev_months_array, time_period, state.code)
        elements.append(Spacer(1, 5))
        # Add Government Response Spending
        elements.append(
            Paragraph("Government Response / Spending:", heading_2_style))

        elements.append(Paragraph(
            "SDRF Disbursement data Insights", heading_3_style))

        elements.append(Paragraph(
            "For the high risk districts, SDRF sanctions in previous 2 FYs. (Starting and End Point)", body_style))

        # indicator y axis sdrf-sanctions-awarded-value
        highlights_data = [
            [Paragraph(header_value, table_header_style) for header_value in ["District", "Amount Sanctioned as per 48th SEC meting", "Amount Sanctioned as per 49th SEC meting",
                                                                              "Amount Sanctioned as per 50th SEC meting", "Total Allocation"]],
            ["Charaide", "Data/number", "Data/number",
                "Data/number", "Data/number"],
            ["Dibrugar", "Data/number", "Data/number",
                "Data/number", "Data/number"],
            ["Sivsagar", "Data/number", "Data/number",
                "Data/number", "Data/number"],
            ["Cacha", "Data/number", "Data/number",
                "Data/number", "Data/number"],
            ["Tinsukia", "Data/number", "Data/number",
                "Data/number", "Data/number"],
        ]
        highlights_table = await get_table(highlights_data, [90, 90, 90, 90, 90])
        elements.append(highlights_table)
        elements.append(Spacer(1, 20))

        # E-tenders Data Insights sub-section
        # Insert Link to Assam Tenders Dashboard in heading later
        elements.append(Paragraph(
            "E-tenders Data Insights", heading_3_style))

        elements.append(Paragraph(
            "For identified high risk districts, e-tenders related to floods in previous 3 financial years (2022-2024)", body_style))

        elements = await add_total_tender_awarded_value_chart(elements, time_period_prev_months_array, time_period, state.code)
        # Key Insights Section
        elements = await append_insights_section(elements, time_period, state, time_period_parsed, time_period_string)
        # elements.append(PageBreak())

        elements = append_annexure_section(elements)

        # ------------------------------------------------------
        # Sections done until here

        # Generate PDF
        # doc.build(elements)
        pdf_buffer.seek(0)

        # Generate PDF to test while development
        await generate_pdf(doc, elements)

        # Sample response to test while development
        response = HttpResponse({"message": "Success"},
                                content_type="application/json")

        # response = HttpResponse(pdf_buffer, content_type="application/pdf")
        # response['Content-Disposition'] = 'attachment; filename="state_report_assam.pdf"'
        await cleanup_temp_files()
        return response

    return HttpResponse("Invalid HTTP method", status=405)


async def get_table(table_data, colWidths=None, table_style=TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), HexColor(0xDBF9E3)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("VALIGN", (0, 0), (-1, 0), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0),
         "NotoSans-Bold" if font_registered else "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]
)):
    table_view = Table(table_data, colWidths)

    table_view.setStyle(
        table_style
    )
    return table_view


def sort_data_dict_and_return_highest_key(data_dict):
    """
    A simple function to return key corresponding to highest values in a dictionary
    """

    if not data_dict:  # Handle empty dictionary
        return None

    sorted_items = sorted(
        data_dict.items(), key=lambda item: item[1], reverse=True)
    return sorted_items[0][0]


async def get_cumulative_indicator_value_for_last_three_years(time_period, indicator, district):
    """
    A simple function to return cumulative value for last three years for provided indicator and district

    args:
    time_period (int): The year from which 3 years of value is calculated
    indicator (str): The indicator for which cumulative value is to be calculated
    district (str): The district for which cumulative value is to be calculated

    returns:
    int: The cumulative value for the last three years

    """

    # create array of three years, current, previous and before previous
    calculation_years = [time_period, (time_period - 1), (time_period - 2)]

    # Preparing a collection of queries to check for all the years in the years array
    query_calc_years = Q()
    for year in calculation_years:
        query_calc_years |= Q(data_period__contains=str(year))

    data_obj = await sync_to_async(Data.objects.filter)(query_calc_years, indicator__is_visible=True, indicator__slug=indicator, geography__id=district)

    # add district is not null condition
    data = await sync_to_async(data_obj.select_related)(
        "geography", "indicator", "indicator__parent", "indicator__parent__parent", "geography__parentId"
    )

    results = await sync_to_async(data.distinct)()
    results = await sync_to_async(list)(results)

    # add all the values from all the results
    total = 0
    for result in results:
        total += result.value

    return total


async def append_insights_section(elements, time_period, state, time_period_parsed, time_period_string):
    elements.append(
        Paragraph(
            "Key Insights and Suggested Actions", heading_2_style)
    )

    # topsis_value = await get_topsis_score_for_given_values(time_period, state.code)

    major_indicators_districts = await get_major_indicators_data(time_period, state.code)
    # pick first three items in the list
    major_indicators_districts_top_3 = major_indicators_districts[:-2]

    # Get the cumulative tender value for top district for last three years
    cumulative_tender_value = await get_cumulative_indicator_value_for_last_three_years(
        time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts_top_3[0]['geography'].id)

    # Get district that received minimum amount from flood tenders for given time period
    district_that_received_minimum_amount_flood_tenders = await get_district_that_received_minimum_given_indicator(major_indicators_districts, 'total-tender-awarded-value', time_period)

    flood_tenders_amount_for_three_years_for_high_risk_district = await get_cumulative_indicator_value_for_last_three_years(time_period_parsed.year, 'total-tender-awarded-value', district_that_received_minimum_amount_flood_tenders.id)

    # Get indicator specific value from the major indicators list for the given district geography id
    inundation_area_of_district_with_min_amount = await get_indicator_value_for_specified_month(time_period, 'inundation-pct', district_that_received_minimum_amount_flood_tenders.id)

    # Get the total population exposed value for top district for provided time period (month)
    top_district_total_population_exposed = await get_indicator_value_for_specified_month(time_period, 'sum-population', major_indicators_districts[0]['geography'].id)

    # main insights
    main_insights = [
        # join geography name from major indicators, process each
        # f"As per {time_period_string}, most at risk districts are {', '.join([item['geography'].name for item in major_indicators_districts_top_3])}. The factors scoring lowest for {', '.join([f"{item['geography'].name} is {sort_data_dict_and_return_highest_key(item['indicators'])}" for item in major_indicators_districts_top_3])}",
        f"Despite receiving significant funds through SDRF in past 3 years. <#> public contracts in past 3 years totalling to {cumulative_tender_value} INR,  <District 1> experienced substantial losses and damages.",
        "For most at risk district <district 1>, <#> public contracts totalling to <# INR>  have been done in past 3 years for flood management. Biggest project undertaken in this district was <top contract in terms of amount for this district>.",
        "However, risk is high because of <factor> and <factor> showing need of more targeting intervention to address these.",
        "<District 1> has received <numbers> amount of money in past three years from SDRF. (If district is getting funds across multiple MoUs then the next line). Repeated funds through SDRF for district 1 and 2 shows focus on immediate relief and restoration efforts.",
        "Allocate sufficient and appropriate funding for DRR activities, including preparedness and mitigation to establish transparent mechanisms across line departments and key decision makers in DRR."
    ]

    suggestive_actions = [
        f"{major_indicators_districts_top_3[-1]['geography'].name} needs significant effort on Government Response as least money has been received through SDRF despite significant losses and damages.",
        f"{district_that_received_minimum_amount_flood_tenders.name} has received {flood_tenders_amount_for_three_years_for_high_risk_district} amount in terms of flood related tenders in past 3 years despite having among the highest Risk score",
        f"{district_that_received_minimum_amount_flood_tenders.name} needs effort on Hazard risk reduction as {inundation_area_of_district_with_min_amount} of its area experienced inundation this month.",
        f"{major_indicators_districts[0]['geography'].name} needs effort on exposure risk reduction seeing that Total Population Exposed this month is {top_district_total_population_exposed}."
    ]

    prepare_array = [ListItem(Paragraph(item, body_style)) for item in main_insights[:-1]] + [
        ListItem(Paragraph(main_insights[-1], body_style)),
        ListFlowable([ListItem(Paragraph(item, body_style)) for item in suggestive_actions],  bulletType='a',  # Use '1' for numbered list)
                     )
    ]

    elements.append(ListFlowable(prepare_array,  bulletType='1',  # Use '1' for numbered list
                                 start='1',       # Start numbering from 1
                                 # Overall indentation of the list (adjust as needed)
                                 leftIndent=12,
                                 # Indent the numbers by 18 points (adjust as needed)
                                 bulletFontSize=10,  # Set the font size of the numbers to match the text
                                 bulletColor=colors.black,
                                 bulletFormat="%s."))

    elements.append(Spacer(1, 20))

    return elements


async def get_indicator_value_for_specified_month(time_period, indicator, district):
    """
    A simple function to return indicator value for specified month for provided indicator and district

    args:
    time_period (int): The month for which value is calculated
    indicator (str): The indicator for which value is to be calculated
    district (str): The district for which value is to be calculated

    returns:
    int: The value for the specified month

    """

    data_obj = await sync_to_async(Data.objects.filter)(data_period=time_period, indicator__is_visible=True, indicator__slug=indicator, geography__id=district)

    results = await sync_to_async(data_obj.distinct)()
    results = await sync_to_async(list)(results)

    if len(results) == 0:
        return 0

    return results[0].value


async def get_district_that_received_minimum_given_indicator(district_list, indicator, time_period):
    """
    A simple function to return district that received minimum value for the given indicator and time period

    args:
    district_list (list): The list of districts for which value is to be calculated
    indicator (str): The indicator for which value is to be calculated
    time_period (int): The month for which value is calculated

    returns:
    dict: The geography object for the specified conditions
    """

    get_district_data_for_given_indicator = await sync_to_async(Data.objects.filter)(data_period=time_period, indicator__slug=indicator, geography__id__in=[district['geography'].id for district in district_list])

    get_district_data_for_given_indicator = await sync_to_async(get_district_data_for_given_indicator.select_related)(
        "geography",
        "indicator",
    )
    results = await sync_to_async(get_district_data_for_given_indicator.distinct)()
    results = await sync_to_async(list)(results)

    if len(results) == 0:
        return 0

    results = sorted(results, key=lambda x: x.value)

    return results[0].geography


def append_annexure_section(elements):
    elements.append(
        Paragraph("Annexure II: Definitions", heading_2_style)
    )
    elements.append(
        Paragraph("<b>Hazard</b> represents the extent and intensity of flooding due to factors like Rainfall & Land Characteristics", body_style)
    )
    elements.append(
        Paragraph("<b>Exposure</b> represents the total population inhabiting the place: Population & Total number of Households", body_style)
    )
    elements.append(
        Paragraph(
            "<b>Vulnerability</b> represents how the losses & damages compare against the socioeconomic indicators", body_style)
    )
    elements.append(
        Paragraph("<b>Government Response</b> represents the public investments through the tenders made for flood disaster management", body_style)
    )

    elements.append(Spacer(1, 20))

    return elements


async def get_topsis_score_for_given_values(time_period, state_code):
    """

    """

    data_obj = await sync_to_async(Data.objects.filter)(
        indicator__slug="topsis-score", data_period=time_period
    )

    data_obj = await sync_to_async(data_obj.select_related)(
        "geography",
        "indicator",
        "geography__parentId",
    )

    data_obj = await sync_to_async(data_obj.filter)(
        Q(geography__parentId__parentId__code=state_code)
    )

    data_list = await sync_to_async(data_obj.order_by)("-value")

    data_list = await sync_to_async(list)(data_list)
    if len(data_list) > 0:
        return data_list[0].value
    else:
        return '0'
