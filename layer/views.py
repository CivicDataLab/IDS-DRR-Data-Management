import asyncio
import os
from io import BytesIO
import httpx
from asgiref.sync import sync_to_async
from django.db.models import Q
from django.http import HttpResponse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image

from D4D_ContextLayer.settings import DEFAULT_TIME_PERIOD, CHART_API_BASE_URL, RESOURCE_ID
from layer.models import Data


async def fetch_chart(client, chart_payload, output_path):
    try:
        response = await client.post(f"{CHART_API_BASE_URL}{RESOURCE_ID}/?response_type=file", json=chart_payload)
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
            data_obj = data_obj.filter(indicator__parent__parent=None)

        if geo_filter:
            data_obj = data_obj.filter(
                Q(geography__parentId__code__in=[geo_filter]) | Q(geography__code__in=[geo_filter])
            )

        return list(data_obj.order_by("-value")[:5])

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


async def generate_report(request):
    if request.method == "GET":
        # Prepare a single BytesIO buffer
        pdf_buffer = BytesIO()
        styles = getSampleStyleSheet()
        elements = [Paragraph("State Report: Assam | May 2023", styles['Title']), Spacer(1, 12)]

        # Fetch and filter data asynchronously
        try:
            time_period = request.GET.get("time_period", DEFAULT_TIME_PERIOD)
            indicator_filter = request.GET.get("indicator")
            geo_filter = request.GET.get("geo_filter")
            data_obj = await get_filtered_data(time_period, indicator_filter, geo_filter)
        except Exception as e:
            return HttpResponse(f"Error filtering data: {e}", status=500)

        # Populate table data with pre-fetched related objects
        district_table_data = [["District", "Flood Risk Level"]]
        for data in data_obj:
            district_table_data.append([data.geography.name, data.value])

        elements.append(await get_table(district_table_data))
        elements.append(Spacer(1, 12))

        # Prepare chart payloads dynamically
        chart_payloads = [
            {
                "chart_type": "BAR_VERTICAL",
                "x_axis_column": "district",
                "y_axis_column": "risk-score",
                "aggregate_type": "sum",
                "show_legend": True,
            },
            # {
            #     "chart_type": "LINE",
            #     "x_axis_column": "Month",
            #     "y_axis_column": "Flood Impact Score",
            #     "aggregate_type": "none",
            #     "show_legend": True,
            # }
        ]

        # Generate unique paths for charts
        chart_paths = [f"{chart_payload['chart_type'].lower()}_chart_{os.getpid()}.png" for chart_payload in
                       chart_payloads]

        # Asynchronously fetch charts
        async with httpx.AsyncClient() as client:
            fetch_tasks = [
                fetch_chart(client, {**payload}, path)
                for payload, path in zip(chart_payloads, chart_paths)
            ]
            fetched_chart_paths = await asyncio.gather(*fetch_tasks)

        # Embed fetched charts in PDF
        for chart_path in fetched_chart_paths:
            if chart_path:
                elements.append(Image(chart_path, width=400, height=200))
            else:
                elements.append(Paragraph("Error fetching visualization", styles['BodyText']))
            elements.append(Spacer(1, 12))



        # Generate the PDF
        try:
            doc = SimpleDocTemplate(pdf_buffer, pagesize=A4)
            doc.build(elements)
        except Exception as e:
            print(f"Error generating PDF: {e}")
            return HttpResponse(f"Error generating PDF: {e}", status=500)

        # Return PDF response
        pdf_buffer.seek(0)  # Ensure the buffer is at the beginning
        response = HttpResponse(pdf_buffer, content_type="application/pdf")
        response['Content-Disposition'] = 'attachment; filename="state_report_assam.pdf"'
        # Cleanup temporary files
        for path in chart_paths:
            if os.path.exists(path):
                os.remove(path)
        return response

    return HttpResponse("Invalid HTTP method", status=405)


async def get_table(table_data):
    district_table = Table(table_data)
    district_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    return district_table
