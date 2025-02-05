import datetime
from io import BytesIO

import httpx
from asgiref.sync import sync_to_async
from django.db.models import Q, F
from django.http import HttpResponse
from reportlab.lib import colors
from reportlab.lib.colors import HexColor, Color
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD, CHART_API_BASE_URL, DATA_RESOURCE_MAP
from layer.models import Data


async def fetch_chart(client, chart_payload, output_path, geo_filter):
    try:
        response = await client.post(f"{CHART_API_BASE_URL}{DATA_RESOURCE_MAP[geo_filter]}/?response_type=file", json=chart_payload)
        print(response)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            return output_path
        else:
            print(f"Failed to fetch chart: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching chart: {e}")
        return None


async def get_top_vulnerable_districts(time_period, geo_filter=None):
    def filter_data():
        data_obj = Data.objects.filter(data_period=time_period).select_related(
            "geography", "indicator", "indicator__parent", "indicator__parent__parent", "geography__parentId"
        )
        if geo_filter:
            data_obj = data_obj.filter(
                Q(geography__parentId__code=geo_filter) | Q(geography__code=geo_filter)
            )
        else:
            data_obj = data_obj.filter(geography__parentId__parentId=None)
        data_obj = data_obj.filter(indicator__is_visible=True, indicator__parent=None)

        return list(data_obj.order_by("-value"))

    return await sync_to_async(filter_data)()


async def get_filtered_data(time_period, indicator_filter=None, geo_filter=None):
    def filter_data():
        data_obj = Data.objects.filter(indicator__is_visible=True, data_period=time_period).select_related(
            "geography", "indicator", "indicator__parent", "indicator__parent__parent", "geography__parentId"
        )

        if indicator_filter:
            data_obj = data_obj.filter(
                Q(indicator__slug=indicator_filter) | Q(indicator__parent__slug=indicator_filter)
            )
        else:
            data_obj = data_obj.filter(indicator__parent=None)

        if geo_filter:
            data_obj = data_obj.filter(
                Q(geography__parentId__code__in=[geo_filter]) | Q(geography__code__in=[geo_filter])
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
    latest = await Data.objects.values_list("data_period", flat=True).annotate(
        custom_ordering=F("data_period")
    ).distinct().order_by("-custom_ordering").afirst()

    if latest:
        return datetime.datetime.strptime(latest, "%Y_%m")
    return None


def get_last_three_months(date_obj):
    last_3_months = [(date_obj.month - i - 1) % 12 + 1 for i in range(3)]
    last_3_months_str = [
        f"{date_obj.year - ((date_obj.month - i - 1) // 12):04d}_{last_3_months[i]:02d}" for i in range(3)
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
        canvas_obj.drawImage(header_image_path, width - 100, height - 50, width=50, height=30, preserveAspectRatio=True)
    except Exception as e:
        print(f"Error loading header image: {e}")

    # Footer
    footer_text = "State Report: Assam | May 2023"
    canvas_obj.setFont("Helvetica", 8)
    canvas_obj.drawString(40, 30, footer_text)  # Left-justified footer text

    # Page number on the right in the footer
    page_number_text = f"Page {doc.page} of {doc.page_count}"
    footer_width = stringWidth(page_number_text, "Helvetica", 10)
    canvas_obj.drawString(width - footer_width - 40, 30, page_number_text)  # Right-aligned page number


class CustomDocTemplate(SimpleDocTemplate):
    """
    Custom SimpleDocTemplate to add header and footer.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def build(self, flowables, onFirstPage=add_header_footer, onLaterPages=add_header_footer,
              canvasmaker=canvas.Canvas):
        """
        Overridden build method to add header and footer.
        """
        self.page_count = len(flowables)  # Total page count for dynamic numbering
        super().build(flowables, onFirstPage=onFirstPage, onLaterPages=onLaterPages, canvasmaker=canvasmaker)


async def generate_report(request):
    if request.method == "GET":
        # Prepare PDF buffer and styles
        pdf_buffer = BytesIO()
        doc = CustomDocTemplate(pdf_buffer, pagesize=A4)
        styles = getSampleStyleSheet()

        # Custom Styles
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Title'],
            fontSize=18,
            leading=22,
            alignment=1,  # Centered
        )
        subtitle_style = ParagraphStyle(
            'SubtitleStyle',
            parent=styles['Heading2'],
            fontSize=14,
            leading=18,
            spaceAfter=10,
        )
        body_style = styles['BodyText']

        # Elements list for PDF
        elements = []

        # Title Section
        elements.append(Paragraph("State Report: Assam | May 2023", title_style))
        elements.append(Spacer(1, 20))

        # Flood Risk Overview
        elements.append(Paragraph("Flood Risk Overview", subtitle_style))

        # Add District Risk Table
        try:
            geo_filter = request.GET.get("geo_code")
            time_period = await get_latest_time_period(geo_filter)
            # indicator_filter = request.GET.get("indicator")
            # data_obj = await get_filtered_data(time_period.strftime("%Y_%m"), None, geo_filter)
            data_obj = await get_top_vulnerable_districts(time_period.strftime("%Y_%m"), geo_filter)

            district_table_data = [["District", "Flood Risk Level"]]
            for data in data_obj:
                district_table_data.append([data.geography.name, data.value])

            district_table = await get_table(district_table_data)
            elements.append(district_table)
            elements.append(Spacer(1, 20))
        except Exception as e:
            elements.append(Paragraph(f"Error fetching district data: {e}", body_style))

        # Add Key Figures Table
        key_figures_data = [
            ["District", "Overall Flood Risk", "Hazard Risk", "Exposure Risk", "Vulnerability Risk", "Gov. Response"],
            ["Charaide", "Very High", "Data/number", "Data/number", "Data/number", "No data"],
            ["Dibrugar", "Very High", "Data/number", "Data/number", "Data/number", "No data"],
            ["Sivsagar", "High", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["Cacha", "High", "Data/number", "Data/number", "Data/number", "No data"],
            ["Tinsukia", "Medium", "Data/number", "Data/number", "Data/number", "No data"],
        ]

        key_figures_table = await get_table(key_figures_data)
        elements.append(key_figures_table)
        elements.append(Spacer(1, 20))

        # Add Highlights
        elements.append(Paragraph("Highlights", subtitle_style))
        highlights_data = [
            ["District", "% Area Inundated", "District Population", "Lives Lost", "Population Affected",
             "Crop Area Affected"],
            ["Charaide", "Data/number", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["Dibrugar", "Data/number", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["Sivsagar", "Data/number", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["Cacha", "Data/number", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["Tinsukia", "Data/number", "Data/number", "Data/number", "Data/number", "Data/number"],
        ]
        highlights_table = await get_table(highlights_data)
        elements.append(highlights_table)
        elements.append(Spacer(1, 20))

        # Add Charts
        async with httpx.AsyncClient() as client:
            chart_payload = {
                "chart_type": "BAR_VERTICAL",
                "x_axis_column": "district",
                "y_axis_column": "risk-score",
                "aggregate_type": "sum",
                "show_legend": True,
            }
            chart_path = "bar_chart.png"
            await fetch_chart(client, chart_payload, chart_path, geo_filter)
            elements.append(Image(chart_path, width=400, height=200))
            elements.append(Spacer(1, 20))

        # Add Losses and Damages Section
        elements.append(PageBreak())
        elements.append(Paragraph("Losses and Damages - Time Series Past 3 Months", subtitle_style))
        damages_data = [
            ["District", "SDRF Allocation (48th)", "SDRF Allocation (49th)", "SDRF Allocation (50th)",
             "Total Allocation"],
            ["District1", "Data/number", "Data/number", "Data/number", "Data/number"],
            ["District2", "Data/number", "Data/number", "Data/number", "Data/number"],
        ]
        damages_table = await get_table(damages_data)
        elements.append(damages_table)

        # Generate PDF
        doc.build(elements)
        pdf_buffer.seek(0)
        response = HttpResponse(pdf_buffer, content_type="application/pdf")
        response['Content-Disposition'] = 'attachment; filename="state_report_assam.pdf"'
        return response

    return HttpResponse("Invalid HTTP method", status=405)


async def get_table(table_data):
    table_view = Table(table_data)
    table_view.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor(0xDBF9E3)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    return table_view
