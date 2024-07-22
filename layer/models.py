from django.contrib.gis.db import models
from django.utils.text import slugify

# from django.db import models


class Unit(models.Model):
    name = models.CharField(null=False, unique=True)
    description = models.CharField(null=True, max_length=1500, blank=True)
    symbol = models.CharField(null=True, blank=True)


class Geography(models.Model):
    class GeoTypes(models.TextChoices):
        COUNTRY = "COUNTRY"
        STATE = "STATE"
        UT = "UT"
        DISTRICT = "DISTRICT"
        BLOCK = "BLOCK"
        VILLAGE = "VILLAGE"
        REVENUE_CIRCLE = "REVENUE CIRCLE"
        SUBDISTRICT = "SUB DISTRICT"
        # LA_CONSTITUTENCY = "LA_CONSTITUTENCY"
        # PA_CONSTITUTANCY = "PA_CONSTITUTANCY"

    name = models.CharField(max_length=100, unique=False)
    code = models.CharField(max_length=20, null=True)  # unique=True)
    type = models.CharField(max_length=15, choices=GeoTypes.choices)
    parentId = models.ForeignKey(
        "self", on_delete=models.CASCADE, null=True, default="", blank=True
    )
    geom = models.MultiPolygonField(null=True, blank=True)


class Department(models.Model):
    name = models.CharField(max_length=20, null=False)
    description = models.CharField(null=True, max_length=1500, blank=True)
    slug = models.SlugField(max_length=20, null=True, blank=True)
    geography = models.ForeignKey(Geography, on_delete=models.PROTECT)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(f"{self.name}")
        return super().save(*args, **kwargs)


class Scheme(models.Model):
    name = models.CharField(max_length=20, null=False)
    description = models.CharField(null=True, max_length=1500, blank=True)
    slug = models.SlugField(max_length=20, null=True, blank=True)
    department = models.ForeignKey(Department, on_delete=models.PROTECT)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(f"{self.name}")
        return super().save(*args, **kwargs)


class Indicators(models.Model):
    name = models.CharField(max_length=100, null=False)
    long_description = models.CharField(null=True, max_length=500, blank=True)
    short_description = models.CharField(null=True, max_length=150, blank=True)
    category = models.CharField(
        null=True,
        max_length=100,
        blank=True,
        help_text="Describes the type sub-indicators",
    )
    type = models.CharField(
        max_length=20,
        null=True,
        blank=True,
        help_text="Defines the type of indicator that is Raw, Derived, etc.",
    )
    slug = models.SlugField(max_length=50, null=True, blank=True)
    unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True, blank=True)
    geography = models.ForeignKey(
        Geography, on_delete=models.PROTECT, null=True, blank=True
    )
    department = models.ForeignKey(
        Department, on_delete=models.PROTECT, null=True, blank=True
    )
    data_source = models.CharField(max_length=100, null=True, blank=True)
    # page = models.ManyToManyField(Page, blank=True)
    scheme = models.ForeignKey(Scheme, on_delete=models.PROTECT, null=True, blank=True)
    parent = models.ForeignKey(
        "self",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        related_name="parent_field",
    )
    display_order = models.IntegerField(default=1)
    is_visible = models.BooleanField(null=False, blank=True, default=False)

    def save(self, *args, **kwargs):
        indc_obj = Indicators.objects.last()
        if indc_obj:
            self.display_order = indc_obj.display_order + 1
        if not self.slug:
            self.slug = slugify(f"{self.name}")
        return super().save(*args, **kwargs)

    class Meta:
        ordering = ["display_order"]


class Data(models.Model):
    value = models.FloatField(null=True, blank=True)
    added = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    indicator = models.ForeignKey(Indicators, on_delete=models.CASCADE, null=False)
    geography = models.ForeignKey(Geography, on_delete=models.PROTECT, null=False)
    scheme = models.ForeignKey(Scheme, on_delete=models.PROTECT, null=True, blank=True)
    data_period = models.CharField(max_length=100, null=True, blank=True)
