import datetime
from faker import Faker
import os
from functools import lru_cache
from io import BytesIO
from unicodedata import category

from django.http.response import async_to_sync
import httpx
from asgiref.sync import sync_to_async
from django.db.models import Q, F, Sum
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

# roads, bridge, embankments-affected
month_highlight_table_indicators = ["inundation-pct", "sum-population", "human-live-lost",
                                    "population-affected-total", "crop-area", "total-animal-affected", "roads", "bridge", "embankments-affected"]
chart_colors = ['#89672A', '#3B8F44', '#C41C8D', '#FB4E93', '#7B4DD9']


def register_font(font_name, font_url_bold=None, font_url_regular=None, font_url_italic=None):
    """Downloads and registers a Font with ReportLab, defaults to Helvetica if fails."""

    if font_url_bold or font_url_regular or font_url_italic:
        if font_url_bold:
            pdfmetrics.registerFont(
                TTFont(f"{font_name}-Bold", font_url_bold))

        if font_url_regular:
            pdfmetrics.registerFont(
                TTFont(font_name, font_url_regular))

        if font_url_italic:
            pdfmetrics.registerFont(
                TTFont(f"{font_name}-Italic", font_url_italic))

        print(f"{font_name} registered successfully.")  # Success message

        return True  # Indicate success
    else:
        return False


# Custom Styles
styles = getSampleStyleSheet()

# Font locations
noto_sans_bold_url = "layer/assets/fonts/noto-sans-800.ttf"
noto_sans_regular_url = "layer/assets/fonts/noto-sans-regular.ttf"
noto_sans_italic_url = "layer/assets/fonts/noto-sans-italic.ttf"

font_registered = register_font(
    "NotoSans", noto_sans_bold_url, noto_sans_regular_url, noto_sans_italic_url)

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

body_style_italic = ParagraphStyle(
    "BodyStyleItalic",
    parent=styles["BodyText"],
    fontName="NotoSans-Italic" if font_registered else "Helvetica-Italic",
    fontSize=10,
)

table_header_style = ParagraphStyle(
    "TableHeaderStyle",
    parent=styles["BodyText"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=8,
    alignment=1,
    # leading=20,
    # spaceAfter=10,
)

table_body_style = ParagraphStyle(
    "TableBodyStyle",
    parent=styles["BodyText"],
    fontName="NotoSans" if font_registered else "Helvetica",
    fontSize=8,
    alignment=1,
)

bold_table_body_style = ParagraphStyle(
    "TableBodyStyle",
    parent=styles["BodyText"],
    fontName="NotoSans-Bold" if font_registered else "Helvetica-Bold",
    fontSize=8,
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
            indicator__slug='topsis-score').distinct()

        results = list(data_obj.order_by("value"))

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
    top_districts = await get_top_vulnerable_districts(time_period, geo_filter)

    top_districts = [x.geography for x in top_districts]

    data_obj = await sync_to_async(Data.objects.filter)(
        indicator__is_visible=True, indicator__parent__parent=None, data_period=time_period, geography__in=top_districts
    )

    data_obj = await sync_to_async(data_obj.select_related)(
        "geography",
        "indicator",
        "indicator__parent",
        "indicator__parent__parent",
        "geography__parentId",
    )

    data_list = await sync_to_async(list)(data_obj)

    result = await group_by_geography(data_list)
    sorted_result = []
    for dist in top_districts:
        for item in result:
            if item["geography"].id == dist.id:
                sorted_result.append(item)
    # Return top 5 if` overall flood risk districts are <5 else return all districts with 5 overall score
    # high_risk_districts = [
    #     district for district in result if district['indicators']["risk-score"] >= 5]

    return sorted_result


def generate_financial_year_months(time_period):
    # Parse the input time_period
    year, month = map(int, time_period.split('_'))

    # Determine the financial year start and end
    if month <= 3:  # Financial year starts in April
        start_year = year - 1
        end_year = year
    else:
        start_year = year
        end_year = year + 1

    # Generate months for the financial year
    financial_year_months = []
    for m in range(4, 13):  # From April (4) to December (12)
        financial_year_months.append(f"{start_year}_{m:02d}")
    for m in range(1, 4):  # From January (1) to March (3) of the next year
        financial_year_months.append(f"{end_year}_{m:02d}")

    return financial_year_months


async def get_cumulative_value_for_financial_year(time_period, indicator, district):
    # generate current financial year months given time_period in yyyy_mm
    time_period_months = await sync_to_async(
        generate_financial_year_months)(time_period)

    data_value = await sync_to_async(Data.objects.filter)(geography=district, indicator__slug=indicator, data_period__in=time_period_months)
    data_value = await sync_to_async(data_value.aggregate)(Sum('value'))
    return data_value['value__sum']


async def get_district_highlights(time_period, geo_filter):

    districts = await get_top_vulnerable_districts(time_period, geo_filter)

    districts = [district.geography for district in districts]

    data = await sync_to_async(Data.objects.filter)(geography__in=districts, indicator__slug__in=month_highlight_table_indicators, data_period=time_period)

    data = await sync_to_async(data.select_related)(
        "geography",
        "indicator",
    )

    data = await sync_to_async(list)(data)

    data = await group_by_geography(data, month_highlight_table_indicators)

    # add roads, bridge and embankments affected to create a new property infrastructure damaged
    for district in data:
        district['indicators']['infrastructure-damaged'] = district['indicators']['roads'] + \
            district['indicators']['bridge'] + \
            district['indicators']['embankments-affected']

        district["indicators"].pop('roads')
        district["indicators"].pop('bridge')
        district["indicators"].pop('embankments-affected')

        # fetch the total-tender-awarded-value for the current financial year
        district['indicators']['total-tender-awarded-value'] = await get_cumulative_value_for_financial_year(time_period, 'total-tender-awarded-value', district['geography'])

    sorted_result = []
    for dist in districts:
        for item in data:
            if item["geography"].id == dist.id:
                sorted_result.append(item)

    return sorted_result


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


async def add_total_tender_awarded_value_chart(elements, time_period, geo_filter):
    districts = await get_top_vulnerable_districts(time_period, geo_filter)

    districts: list[Geography] = [district.geography
                                  for district in districts]
    y_axis_columns = []
    tender_chart_colors = chart_colors.copy()
    for district in districts:
        y_axis_columns.append({
            "field_name": f"{district.code}",
            "label": f"{district.name}",
            "color": tender_chart_colors.pop(0),
            "aggregate_type": "SUM"
        })

    async with httpx.AsyncClient() as client:
        chart_payload = {
            "chart_type": "GROUPED_BAR_VERTICAL",
            "x_axis_column": "financial-year",
            "x_axis_label": "Financial Year",
            "y_axis_column": y_axis_columns,
            "y_axis_label": "Total render awarded value",
            "show_legend": "true",
            "filters": [
                {
                    "column": "financial-year",
                    "operator": "in",
                    "value": ','.join(identify_and_get_prev_financial_years(time_period, 3)),
                },
                {
                    "column": "factor",
                    "operator": "==",
                    "value": "total-tender-awarded-value",
                }
            ],
        }
        try:
            chart = await fetch_chart(client, chart_payload, "a165cb92-8c92-49d5-83bb-d8a875c61a57")

            # image_table_data = [[Image(chart, width=500, height=300)]]
            # table_with_images = await get_table(image_table_data, [500, 200], TableStyle([
            #     ('GRID', (0, 0), (-1, -1), 0, colors.transparent),
            #     ("PADDING", (0, 0), (-1, -1), 5)
            # ]))

            # elements.append(table_with_images)
            elements.append(Image(chart, width=500, height=300))
            elements.append(Spacer(1, 5))
        except Exception as e:
            print(e)
    return elements


async def add_losses_and_damages_times_series(elements, time_period_prev_months_array, time_period, geo_filter):
    districts = await get_top_vulnerable_districts(time_period, geo_filter)

    districts: list[Geography] = [district.geography for district in districts]
    y_axis_columns = []
    lnd_chart_colors = chart_colors.copy()
    for district in districts:
        y_axis_columns.append({
            "field_name": f"{district.code}",
            "label": f"{district.name}",
            "color": lnd_chart_colors.pop(0),
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
            "y_axis_label": "Total Instances of Infrastructure Damage",
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

        try:
            chart1 = await fetch_chart(client, chart_payload1, "a165cb92-8c92-49d5-83bb-d8a875c61a57")
            chart2 = await fetch_chart(client, chart_payload2, "a165cb92-8c92-49d5-83bb-d8a875c61a57")
            elements.append(
                Paragraph("Total Population Affected by Floods", body_style))
            elements.append(Image(chart1, width=500, height=275))

            elements.append(
                Paragraph("Total Instances of Infrastructure Damage", body_style))
            elements.append(Image(chart2, width=500, height=275))
            elements.append(Spacer(1, 5))
        except Exception as e:
            print(e)

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

        doc = CustomDocTemplate(pdf_buffer, pagesize=A4)
        # Development mode to update a local document for testing
        # doc = CustomDocTemplate("test_output.pdf", pagesize=A4)

        doc.topMargin = 1 * inch
        doc.bottomMargin = 1 * inch
        doc.leftMargin = 0.5 * inch
        doc.rightMargin = 0.5 * inch

        risk_mapping_text = {
            '1.0': Paragraph('Very Low', table_body_style),
            '2.0': Paragraph('Low', table_body_style),
            '3.0': Paragraph('Medium', table_body_style),
            '4.0': Paragraph('High', table_body_style),
            '5.0': Paragraph('Very High', table_body_style),
        }

        bold_risk_mapping_text = {
            '1.0': Paragraph('Very Low', bold_table_body_style),
            '2.0': Paragraph('Low', bold_table_body_style),
            '3.0': Paragraph('Medium', bold_table_body_style),
            '4.0': Paragraph('High', bold_table_body_style),
            '5.0': Paragraph('Very High', bold_table_body_style),
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

        try:
            # Flood Risk Overview
            elements.append(Paragraph("Flood Risk Overview", heading_2_style))

            # --------------------------------------------------------
            # Overview Section
            top_vulnerable_districts_data_obj = await get_top_vulnerable_districts(
                time_period, state.code
            )

            elements.append(Paragraph(
                f"As of {time_period_string}, the following 5 districts in {state.name} faced highest risk - ", body_style))

            elements.append(ListFlowable([
                ListItem(Paragraph(data.geography.name, body_style)) for data in top_vulnerable_districts_data_obj
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
                "Note: The Flood Risk is calculated as a function of Hazard, Exposure, Vulnerability and Government Response.", body_style_italic))

            elements.append(Spacer(1, 20))
        except Exception as e:
            elements.append(
                Paragraph(f"Error fetching district data: {e}", body_style))

        # --------------------------------------------------------
        # Key Figures Section
        elements.append(
            Paragraph("Top most at-risk districts: Key Figures", heading_2_style))

        try:
            # Factor wise risk assessment
            elements.append(
                Paragraph("Factor wise risk assessment", heading_3_style))

            majorIndicatorsData = await get_major_indicators_data(time_period, state.code)

            district_table_data = [
                [Paragraph(table_title, table_header_style) for table_title in [
                    "District", "Risk Score", "Flood Hazard", "Exposure", "Vulnerability", "Government Response"]]
            ]
            for data in majorIndicatorsData:
                district_table_data.append([Paragraph(data['geography'].name, table_body_style), bold_risk_mapping_text[str(data['indicators']["risk-score"])], risk_mapping_text[str(data['indicators']["flood-hazard"])], risk_mapping_text[str(
                    data['indicators']["exposure"])], risk_mapping_text[str(data['indicators']["vulnerability"])], risk_mapping_text[str(data['indicators']["government-response"])]])

            district_table = await get_table(district_table_data, [100, 80, 80, 80, 80, 80])
            elements.append(district_table)
            elements.append(Spacer(1, 10))
        except Exception as e:
            elements.append(
                Paragraph(f"Error fetching major indicators data: {e}", body_style))

        # Month Highlights sub-section
        elements.append(
            Paragraph(f"Highlights for the month of {time_period_string}", heading_3_style))

        data_obj = await get_district_highlights(
            time_period, state.code
        )

        a = ["District Name", "% Area Inundated", "District Population", "Lives Lost (Confirmed + Missing)",
             "Population Affected (peak of the month)", "Crop Area Affected (Hectares)", "No. of Infrastructure Damage", "Animals Affected", "Cumulative Flood tenders for current F.Y (INR)"]
        b = []
        for header_value in a:
            b.append(Paragraph(header_value, table_header_style))
        district_table_data = [b]
        # district_table_data = [a]
        for data in data_obj:
            values = [Paragraph((lambda v: str(int(v)) if v not in ['NA', None] else 'NA')(data['indicators'].get(indicator))
                                if isinstance(data['indicators'].get(indicator), (int, float, str)) and str(data['indicators'].get(indicator)).replace('.', '', 1).isdigit()
                                else 'NA',
                                table_body_style)
                      for indicator in ["inundation-pct", "sum-population", "human-live-lost",
                                        "population-affected-total", "crop-area", "infrastructure-damaged", "total-animal-affected",  "total-tender-awarded-value"]]
            row = [Paragraph(data['geography'].name,
                             table_body_style)] + values
            district_table_data.append(row)

        district_table = await get_table(district_table_data, [70, 60, 60, 60, 60, 60, 60, 60])
        elements.append(district_table)
        # elements.append(Spacer(1, 5))

        # add page break
        elements.append(PageBreak())

        # Losses and Damages section
        elements.append(Paragraph("Losses and Damages", heading_2_style))

        time_period_str = ', '.join([datetime.datetime.strptime(
            period, "%Y_%m").strftime("%B %Y") for period in time_period_prev_months_array])

        elements.append(
            Paragraph(f"Time Series for {time_period_str}", heading_3_style))

        elements = await add_losses_and_damages_times_series(elements, time_period_prev_months_array, time_period, state.code)

        # add page break
        elements.append(PageBreak())

        # Add Government Response Spending
        elements.append(
            Paragraph("Government Response / Spending:", heading_2_style))

        # elements.append(Paragraph(
        #     "SDRF Disbursement data Insights", heading_3_style))

        # elements.append(Paragraph(
        #     "For the high risk districts, SDRF sanctions in previous 3 FYs.", body_style))

        # elements.append(Spacer(1, 2))

        # indicator y axis sdrf-sanctions-awarded-value
        # elements = await add_sdrf_section_for_top_districts(elements, time_period, state.code)

        # elements.append(Spacer(1, 5))

        # E-tenders Data Insights sub-section
        # Insert Link to Assam Tenders Dashboard in heading later
        elements.append(Paragraph("E-tenders Data Insights", heading_3_style))

        elements.append(Paragraph(
            "For identified high risk districts, e-tenders related to floods in previous 3 financial years (2022-2024)", body_style))
        elements.append(Spacer(1, 5))

        elements = await add_total_tender_awarded_value_chart(elements, time_period, state.code)

        # Key Insights Section
        elements = await append_insights_section(elements, time_period, state, time_period_parsed, time_period_string)
        # elements.append(PageBreak())

        elements = append_annexure_section(elements)

        elements = append_data_sources_section(elements)

        # ------------------------------------------------------
        # Sections done until here

        # Generate PDF
        doc.build(elements)
        pdf_buffer.seek(0)

        # Generate PDF to test while development
        # await generate_pdf(doc, elements)

        # Sample response to test while development
        # response = HttpResponse({"message": "Success"},
        # content_type="application/json")

        response = HttpResponse(pdf_buffer, content_type="application/pdf")
        response['Content-Disposition'] = f'attachment; filename="state_report_{state.name}_{time_period}.pdf"'
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
    return sorted_items


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

    indicator_mapping = {
        'risk-score': 'Overall Risk Score',
        'flood-hazard': 'Hazard',
        'exposure': 'Exposure',
        'vulnerability': 'Vulnerability',
        'government-response': 'Government Response'
    }

    major_indicators_districts = await get_major_indicators_data(time_period, state.code)
    # pick first three items in the list

    if len(major_indicators_districts) >= 5:
        major_indicators_districts_top_3 = major_indicators_districts[:-2]

        cumulative_sdrf_value_0 = await get_cumulative_indicator_value_for_last_three_years(time_period_parsed.year, 'sdrf-tenders-awarded-value', major_indicators_districts_top_3[0]['geography'].id)
        cumulative_total_flood_value_0 = await get_cumulative_indicator_value_for_last_three_years(time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts_top_3[0]['geography'].id)

        cumulative_total_flood_value_1 = await get_cumulative_indicator_value_for_last_three_years(time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts_top_3[1]['geography'].id)

        cumulative_total_flood_value_2 = await get_cumulative_indicator_value_for_last_three_years(time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts_top_3[2]['geography'].id)

        # Get the cumulative tender value for top district for last three years
        cumulative_tender_value = await get_cumulative_indicator_value_for_last_three_years(
            time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts_top_3[0]['geography'].id)

        # Get district that received minimum amount from flood tenders for given time period
        district_that_received_minimum_amount_flood_tenders = await get_district_that_received_min_max_given_indicator(major_indicators_districts, 'total-tender-awarded-value', time_period, 'min')

        district_with_highest_hazard_score = await get_district_that_received_min_max_given_indicator(major_indicators_districts, 'flood-hazard', time_period, 'max')

        area_inundated_pct_for_dist_with_high_hazard = await get_indicator_value_for_specified_month(time_period, 'inundation-pct', district_with_highest_hazard_score.id)

        district_with_highest_exposure = await get_district_that_received_min_max_given_indicator(major_indicators_districts, 'exposure', time_period, 'max')

        total_population_exposed_for_dist_with_highest_exposure = await get_indicator_value_for_specified_month(time_period, 'population-affected-total', district_with_highest_exposure.id)

        factors_scoring_lowest = ', '.join(
            [f"{item['geography'].name.title()} is {indicator_mapping[sort_data_dict_and_return_highest_key(item['indicators'])[1][0]]}" for item in major_indicators_districts_top_3])

        # main insights
        main_insights = [
            f"As per {time_period_string}, most at risk districts are {', '.join([item['geography'].name.title() for item in major_indicators_districts_top_3])}. The factors scoring lowest for {factors_scoring_lowest}",
            f"For most at risk district, {major_indicators_districts_top_3[0]['geography'].name.title()}, public contracts totalling to INR {cumulative_total_flood_value_0} have been awarded in past 3 years for flood management related activities and projects. Out of this, INR {cumulative_sdrf_value_0} has been spent on flood related tenders through SDRF.",
            f"For {major_indicators_districts_top_3[1]['geography'].name.title()}, public contracts totalling to INR {cumulative_total_flood_value_1} have been awarded in past 3 years for flood management related activities and projects and for {major_indicators_districts_top_3[2]['geography'].name.title()}, public contracts totalling to INR {cumulative_total_flood_value_2} have been awarded.",
            f"For {major_indicators_districts_top_3[0]['geography'].name.title()}, Risk is high because of {indicator_mapping[sort_data_dict_and_return_highest_key(major_indicators_districts_top_3[0]['indicators'])[1][0]]} and {indicator_mapping[sort_data_dict_and_return_highest_key(major_indicators_districts_top_3[0]['indicators'])[2][0]]} showing need of more targetted intervention to address these.",
            f"{major_indicators_districts_top_3[0]['geography'].name.title()} has received {cumulative_tender_value} amount in terms of flood related tenders in past 3 years despite having among the highest Risk score",
            f"{district_that_received_minimum_amount_flood_tenders.name.title()} needs significant effort on Government Response as least money has been received despite having among the highest Risk score.",
            f"{district_with_highest_hazard_score.name.title()} needs effort on Hazard risk reduction as {area_inundated_pct_for_dist_with_high_hazard} of its area experienced inundation this month.",
            f"{district_with_highest_exposure.name.title()} needs effort on exposure risk reduction, seeing that Total Population Exposed this month is {total_population_exposed_for_dist_with_highest_exposure}."
        ]

        prepare_array = [ListItem(Paragraph(item, body_style))
                         for item in main_insights]

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


async def get_district_that_received_min_max_given_indicator(district_list, indicator, time_period, min_max='min'):
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

    # Filter out entries with None as value
    results = [r for r in results if r.value is not None]

    if len(results) == 0:
        return 0

    results = sorted(results, key=lambda x: x.value)

    if min_max == 'min':
        return results[0].geography
    else:
        return results[-1].geography


def append_annexure_section(elements):
    elements.append(
        Paragraph("Annexure II: Definitions", heading_2_style)
    )

    list_of_defs = [
        "<b>Hazard</b> represents the extent and intensity of flooding due to factors like Rainfall & Land Characteristics",
        "<b>Exposure</b> represents the total population inhabiting the place: Population & Total number of Households",
        "<b>Vulnerability</b> represents how the losses & damages compare against the socioeconomic indicators",
        "<b>Government Response</b> represents the public investments through the tenders made for flood disaster management",
    ]

    elements.append(ListFlowable([ListItem(Paragraph(item, body_style)) for item in list_of_defs], bulletType='1',
                    start='1', leftIndent=12, bulletFontSize=10, bulletColor=colors.black, bulletFormat="%s."))

    elements.append(Spacer(1, 10))

    return elements


async def get_topsis_score_for_given_values(time_period, state_code):
    """

    """

    data_obj = await sync_to_async(Data.objects.filter)(
        indicator__slug="topsis-score", data_period=time_period, geography__parentId__code=state_code
    )

    data_obj = await sync_to_async(data_obj.select_related)(
        "geography",
        "indicator",
        "geography__parentId",
    )

    data_list = await sync_to_async(data_obj.order_by)("-value")

    data_list = await sync_to_async(list)(data_list)
    if len(data_list) > 0:
        return data_list[0].value
    else:
        return '0'


async def add_sdrf_section_for_top_districts(elements, time_period, geo_filter):
    districts = await get_top_vulnerable_districts(time_period, geo_filter)

    districts: list[Geography] = [district.geography for district in districts]

    y_axis_columns = []
    for district in districts:
        y_axis_columns.append({
            "field_name": f"{district.code}",
            "label": f"{district.name}",
            "color": f"{Faker().color()}",
        })

        # chart = await fetch_chart(client, chart_payload, "a165cb92-8c92-49d5-83bb-d8a875c61a57")

        # elements.append(Image(chart, width=500, height=300))
    return elements


def identify_and_get_prev_financial_years(time_period, number_of_years=3):
    time_period_parsed = datetime.datetime.strptime(time_period, "%Y_%m")

    # check if the month is before or after march
    if time_period_parsed.month <= 3:
        current_year = time_period_parsed.year - 1
    else:
        current_year = time_period_parsed.year

    financial_years = []

    for i in range(number_of_years):
        start = current_year - i
        end = start + 1
        financial_years.append(f"{start}-{end}")

    return financial_years


def append_data_sources_section(elements):
    elements.append(Paragraph("Data Sources", heading_2_style))

    list_of_sources = [
        "Source for Inundation Data: <u><a href='https://bhuvan-app1.nrsc.gov.in/disaster/disaster.php?id=flood'>Bhuvan</a></u>",
        "Source for population and demographic data: <u><a href='https://hub.worldpop.org/project/categories?id=3'>UN WorldPop</a></u>",
        "Source for Losses and Damages: <u><a href='https://www.asdma.gov.in/reports.html'>ASDMA DRIMS</a></u>",
        "Source for tender data: <u><a href='https://assamtenders.gov.in/nicgep/app?page=WebTenderStatusLists&service=page'>Assam GEPNiC e-tenders platform</a></u>",
    ]
    elements.append(
        ListFlowable(
            [ListItem(Paragraph(source, body_style))
             for source in list_of_sources],
            bulletType='1',
            start='1',
            leftIndent=12,
            bulletFontSize=10,
            bulletColor=colors.black,
            bulletFormat="%s."
        )
    )
    return elements
