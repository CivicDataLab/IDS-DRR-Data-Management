import datetime
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
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, ListFlowable, ListItem

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD, CHART_API_BASE_URL, DATA_RESOURCE_MAP
from layer.models import Data, Geography, Indicators

from collections import defaultdict

month_highlight_table_indicators = ["inundation-pct", "sum-population", "human-live-lost",
                                    "population-affected-total", "crop-area", "total-animal-affected", "total-tender-awarded-value"]

# Custom Styles
styles = getSampleStyleSheet()

title_style = ParagraphStyle(
    "TitleStyle",
    parent=styles["Title"],
    fontSize=18,
    leading=22,
    alignment=1,  # Centered
)
heading_1_style = ParagraphStyle(
    "Heading1Style",
    parent=styles["Heading1"],
    fontSize=16,
    leading=16,
    spaceAfter=10,
)

heading_2_style = ParagraphStyle(
    "Heading2Style",
    parent=styles["Heading2"],
    fontSize=14,
    leading=18,
    spaceAfter=10,
)
heading_3_style = ParagraphStyle(
    "Heading3Style",
    parent=styles["Heading3"],
    fontSize=12,
    leading=20,
    spaceAfter=10,
)

body_style = styles["BodyText"]

table_header_style = ParagraphStyle(
    "TableHeaderStyle",
    parent=styles["BodyText"],
    fontSize=12,
    alignment=1,
    # leading=20,
    # spaceAfter=10,
)

table_body_style = ParagraphStyle(
    "TableBodyStyle",
    parent=styles["BodyText"],
    fontSize=10,
    alignment=1,
)


async def fetch_chart(client, chart_payload, output_path, geo_filter):
    try:
        response = await client.post(f"{CHART_API_BASE_URL}{DATA_RESOURCE_MAP[geo_filter]}/?response_type=file", json=chart_payload)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            return output_path
        else:
            print(
                f"Failed to fetch chart:::::::::::::::: {response.status_code}, {response.text}")
            return None
    except Exception as e:
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

    # indicatorsList = Indicators.objects.filter(is_visible=True, parent__parent=None).select_related(
    #     "parent",
    #     "parent__parent",
    # )

    # indicatorsListQ = list(indicatorsList)

    # print([indi.id for indi in list(indicatorsList)])

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

    # Header
    header_text = "IDS-DRR | Intelligent Data Solution for Disaster Risk Reduction"
    canvas_obj.setFont("Helvetica-Bold", 8)
    canvas_obj.drawString(40, height - 30, header_text)

    # Add an image to the right in the header
    header_image_path = "layer/CDL_Primary Logo.png"
    try:
        canvas_obj.drawImage(header_image_path, width - 100, height -
                             50, width=50, height=30, preserveAspectRatio=True)
    except Exception as e:
        print(f"Error loading header image: {e}")

    # Footer
    footer_text = "State Report: Assam | May 2023"
    canvas_obj.setFont("Helvetica", 8)
    canvas_obj.drawString(40, 30, footer_text)  # Left-justified footer text

    # Page number on the right in the footer
    page_number_text = f"Page {doc.page} of {doc.page_count}"
    footer_width = stringWidth(page_number_text, "Helvetica", 10)
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
        self.page_count = len(
            flowables)  # Total page count for dynamic numbering
        super().build(flowables, onFirstPage=onFirstPage,
                      onLaterPages=onLaterPages, canvasmaker=canvasmaker)


async def generate_report(request):
    if request.method == "GET":
        # Prepare PDF buffer and styles
        pdf_buffer = BytesIO()
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

        geo_code = request.GET.get("geo_code", "18")
        time_period = request.GET.get("time_period", DEFAULT_TIME_PERIOD)
        time_period_parsed = datetime.datetime.strptime(
            time_period, "%Y_%m")
        time_period_string = time_period_parsed.strftime("%B %Y")

        # Create a time period array with 2 months prior to current selected month along with the current month
        time_period_prev_months_array = [(time_period_parsed - datetime.timedelta(days=60)).strftime(
            "%Y_%m"), (time_period_parsed - datetime.timedelta(days=30)).strftime("%Y_%m"), time_period_parsed.strftime("%Y_%m")]

        # Set the type filter based on state.
        state = await sync_to_async(Geography.objects.get)(code=geo_code, type="STATE")

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
            # if data.indicators["overall-flood-risk"]:
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
             "population affected total", "crop area", "total animal affected", "total tender awarded value"]
        b = []
        for header_value in a:
            b.append(Paragraph(header_value, table_header_style))
        district_table_data = [b]
        # district_table_data = [a]
        for data in data_obj:
            values = [data['indicators'][indicator]
                      for indicator in month_highlight_table_indicators]
            row = [Paragraph(data['geography'].name,
                             table_body_style)] + values
            district_table_data.append(row)

        district_table = await get_table(district_table_data, [70, 70, 70, 70, 70, 70, 70, 70])
        elements.append(district_table)
        elements.append(Spacer(1, 20))

        # Losses and Damages section
        elements.append(Paragraph("Losses and Damages", heading_2_style))

        elements.append(Paragraph(
            f"Times Series for {time_period_prev_months_array}", heading_3_style))
        async with httpx.AsyncClient() as client:
            chart_payload = {
                "chart_type": "GROUPED_BAR_VERTICAL",
                "x_axis_column": "timeperiod",
                "time_column": "timeperiod",
                "x_axis_label": "Month",
                "y_axis_column": [
                    {
                        "field_name": "population-affected-total",
                        "label": "Total Population affected",
                        "color": "#8B5E3C",
                    },
                ],
                "y_axis_label": "Score",
                "show_legend": "true",
                "filters": [
                    {
                        "column": "timeperiod",
                        "operator": "in",
                        "value": time_period_prev_months_array,
                    },
                    # {"column": "object-id", "operator": "==", "value": state.code},
                ],
            }

            chart_path = "bar_chart.png"
            await fetch_chart(client, chart_payload, chart_path, state.code)

            image_table_data = [[Image(chart_path, width=250, height=125),
                                 Image(chart_path, width=250, height=125)]]
            table_with_images = await get_table(image_table_data, [300, 300], TableStyle([
                ('GRID', (0, 0), (-1, -1), 0, colors.transparent),
                ("PADDING", (0, 0), (-1, -1), 5)
            ]))

            elements.append(table_with_images)
            elements.append(Spacer(1, 20))

        # Add Government Response Spending
        elements.append(Paragraph("Highlights", heading_2_style))
        highlights_data = [
            ["District", "% Area Inundated", "District Population", "Lives Lost", "Population Affected",
             "Crop Area Affected"],
            ["Charaide", "Data/number", "Data/number",
                "Data/number", "Data/number", "Data/number"],
            ["Dibrugar", "Data/number", "Data/number",
                "Data/number", "Data/number", "Data/number"],
            ["Sivsagar", "Data/number", "Data/number",
                "Data/number", "Data/number", "Data/number"],
            ["Cacha", "Data/number", "Data/number",
                "Data/number", "Data/number", "Data/number"],
            ["Tinsukia", "Data/number", "Data/number",
                "Data/number", "Data/number", "Data/number"],
        ]
        highlights_table = await get_table(highlights_data)
        elements.append(highlights_table)
        elements.append(Spacer(1, 20))

        # E-tenders Data Insights sub-section
        async with httpx.AsyncClient() as client:
            chart_payload = {
                "chart_type": "GROUPED_BAR_VERTICAL",
                "x_axis_column": "timeperiod",
                "time_column": "timeperiod",
                "x_axis_label": "Time Period",
                "y_axis_column": [
                    {
                        "field_name": "risk-score",
                        "label": "Risk Score",
                        "color": "#8B5E3C",
                    },
                    {"field_name": "exposure",
                        "label": "Exposure", "color": "#2E8B57"},
                    {
                        "field_name": "vulnerability",
                        "label": "Vulnerability",
                        "color": "#9370DB",
                    },
                    {
                        "field_name": "flood-hazard",
                        "label": "Flood Hazard",
                        "color": "#FFB347",
                    },
                    {
                        "field_name": "government-response",
                        "label": "Government Response",
                        "color": "#808000",
                    },
                ],
                "y_axis_label": "Score",
                "show_legend": "true",
                "filters": [
                    {
                        "column": "timeperiod",
                        "operator": "in",
                        "value": "2024_06,2024_05,2024_04,2024_03",
                    },
                    # {"column": "object-id", "operator": "==", "value": state.code},
                ],
            }

            chart_path = "bar_chart.png"
            await fetch_chart(client, chart_payload, chart_path, state.code)

            elements.append(Image(chart_path, width=400, height=200))
            elements.append(Spacer(1, 20))

        # Key Insights Section
        elements = await append_insights(elements, time_period, state, time_period_parsed, time_period_string)
        # elements.append(PageBreak())

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
        return response

    return HttpResponse("Invalid HTTP method", status=405)


async def get_table(table_data, colWidths=None, table_style=TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), HexColor(0xDBF9E3)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("VALIGN", (0, 0), (-1, 0), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
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


async def append_insights(elements, time_period, state, time_period_parsed, time_period_string):
    elements.append(
        Paragraph(
            "Key Insights and Suggested Actions", heading_2_style)
    )

    major_indicators_districts = await get_major_indicators_data(time_period, state.code)
    # pick first three items in the list
    major_indicators_districts = major_indicators_districts[:-2]

    cumulative_tender_value = await get_cumulative_indicator_value_for_last_three_years(
        time_period_parsed.year, 'total-tender-awarded-value', major_indicators_districts[0]['geography'].id)

    # main insights
    main_insights = [
        # join geography name from major indicators, process each
        f"As per {time_period_string}, most at risk districts are {', '.join([item['geography'].name for item in major_indicators_districts])}. The factors scoring lowest for {', '.join([f"{item['geography'].name} is {sort_data_dict_and_return_highest_key(item['indicators'])}" for item in major_indicators_districts])}",
        f"Despite receiving significant funds through SDRF in past 3 years. <#> public contracts in past 3 years totalling to {cumulative_tender_value} INR,  <District 1> experienced substantial losses and damages.",
        "For most at risk district <district 1>, <#> public contracts totalling to <# INR>  have been done in past 3 years for flood management. Biggest project undertaken in this district was <top contract in terms of amount for this district>.",
        "However, risk is high because of <factor> and <factor> showing need of more targeting intervention to address these.",
        "<District 1> has received <numbers> amount of money in past three years from SDRF. (If district is getting funds across multiple MoUs then the next line). Repeated funds through SDRF for district 1 and 2 shows focus on immediate relief and restoration efforts.",
        "Allocate sufficient and appropriate funding for DRR activities, including preparedness and mitigation to establish transparent mechanisms across line departments and key decision makers in DRR."
    ]

    suggestive_actions = [
        "<District 3> needs significant effort on Government Response as least money has been received through SDRF despite significant losses and damages."
        "<district in top 5 at risk that received minimum amount from flood tenders> has received <numbers> amount in terms of flood related tenders in past 3 years despite having among the highest Risk score",
        "<district in top 5 at risk that received minimum amount from flood tenders> needs effort on Hazard risk reduction as <numbers> of its area experienced inundation this month.",
        "<District 1> needs effort on exposure risk reduction seeing that Total Population Exposed this month is <numbers>."
    ]

    elements.append(ListFlowable([
        ListItem(Paragraph(item, body_style)) for item in main_insights
    ],  bulletType='1',  # Use '1' for numbered list
        start='1',       # Start numbering from 1
        # Overall indentation of the list (adjust as needed)
        leftIndent=12,
        # Indent the numbers by 18 points (adjust as needed)
        bulletFontSize=10,  # Set the font size of the numbers to match the text
        bulletColor=colors.black,
        bulletFormat="%s."))

    return elements
