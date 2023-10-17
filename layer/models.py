# from django.contrib.gis.db import models
from django.utils.text import slugify
from django.db import models

class Unit(models.Model):
    class UnitTypes(models.TextChoices):
        NUMBER = "NUMBER"
        RUPEES = "RUPEES"
        RUPEESLAKHS = "RUPEESLAKHS"
        PERCENTAGE = "PERCENTAGE"
        SCORE = "SCORE"

    class UnitSymbolTypes(models.TextChoices):
        NUMBER = ""
        RUPEES = "₹"
        RUPEESLAKHS = "₹ (in Lacs)"
        PERCENTAGE = "%"

    name = models.CharField(null=False, choices=UnitTypes.choices, unique=True)
    description = models.CharField(null=True, max_length=1500, blank=True)
    symbol = models.CharField(null=False, choices=UnitSymbolTypes.choices, blank=True)


class Geography(models.Model):
    class GeoTypes(models.TextChoices):
        COUNTRY = "COUNTRY"
        STATE = "STATE"
        UT = "UT"
        DISTRICT = "DISTRICT"
        BLOCK = "BLOCK"
        VILLAGE = "VILLAGE"
        REVENUE_CIRCLE = "REVENUE CIRCLE"
        # LA_CONSTITUTENCY = "LA_CONSTITUTENCY"
        # PA_CONSTITUTANCY = "PA_CONSTITUTANCY"

    name = models.CharField(max_length=100, unique=True)
    code = models.CharField(max_length=20, null=False, unique=True)
    type = models.CharField(max_length=15, choices=GeoTypes.choices)
    parentId = models.ForeignKey("self", on_delete=models.CASCADE, null=True, default="", blank=True)


class Page(models.Model):
    name = models.CharField(max_length=20, null=True, blank=True)
    description = models.CharField(null=True, max_length=1500, blank=True)
    slug = models.CharField(max_length=20, null=True, blank=True)

    def save(self, *args, **kwargs):
        self.slug = slugify(f"{self.name}_{self.id}")
        return super().save(*args, **kwargs)


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
    short_description = models.CharField(null=True, max_length=100, blank=True)
    category = models.CharField(null=True, max_length=100, blank=True, help_text="Contains a list of sub-indicators.")
    type = models.CharField(max_length=20, null=False, help_text="Defines the type of indicator that is Raw, Derived, etc.")
    slug = models.SlugField(max_length=20, null=True, blank=True)
    unit = models.ForeignKey(Unit, on_delete=models.SET_NULL, null=True)
    geography = models.ForeignKey(Geography, on_delete=models.PROTECT, null=True, blank=True)
    department = models.ForeignKey(Department, on_delete=models.PROTECT, null=True, blank=True)
    data_source = models.CharField(max_length=100, null=True, blank=True)
    # page = models.ManyToManyField(Page, blank=True)
    scheme = models.ForeignKey(Scheme, on_delete=models.PROTECT, null=True, blank=True)
    parent = models.ForeignKey("self", blank=True, null=True, on_delete=models.PROTECT, related_name="parent_field")
    
    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(f"{self.name}")
        return super().save(*args, **kwargs)


class Data(models.Model):
    value = models.FloatField(null=True, blank=True)
    added = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    indicator = models.ForeignKey(Indicators, on_delete=models.CASCADE, null=False)
    geography = models.ForeignKey(Geography, on_delete=models.PROTECT, null=False)
    scheme = models.ForeignKey(Scheme, on_delete=models.PROTECT, null=True, blank=True)
    data_period = models.CharField(max_length=100, null=True, blank=True)
