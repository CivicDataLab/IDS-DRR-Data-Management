"""
URL configuration for D4D_ContextLayer project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/

Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))

"""

from django.contrib import admin
from django.urls import include, path
from strawberry.django.views import GraphQLView

from layer import views
from layer.schema import schema

urlpatterns = [
    path("admin/", admin.site.urls),
    path("graphql", GraphQLView.as_view(schema=schema)),
    path("chart-types/<str:chart_type>", views.chart_type_geojson, name="chart_type_geojson"),
    path("raster/metadata", views.raster_metadata, name="raster_metadata"),
    path("raster/value", views.raster_value, name="raster_value"),
    path(
        "raster/tiles/<int:z>/<int:x>/<int:y>.png",
        views.raster_tile,
        name="raster_tile",
    ),
    path("", include("plugin.urls")),
]
