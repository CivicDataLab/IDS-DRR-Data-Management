from django.contrib.gis.db import models


class ProgrammeTypes(models.TextChoices):
    SCHEME = "SCHEME"
    ACT = "ACT"
    PROGRAMME = "PROGRAMME"


class UnitTypes(models.TextChoices):
    NUMBER = "NUMBER"
    RUPEES = "RUPEES"
    RUPEESLAKHS = "RUPEESLAKHS"
    PERCENTAGE = "PERCENTAGE"


class Programme(models.Model):
    name = models.CharField(max_length=100)
    description = models.CharField(max_length=1500)
    type = models.CharField(blank=False, choices=ProgrammeTypes.choices)


class IndicatorTypes(models.Model):
    name = models.CharField(max_length=100)


class Indicator(models.Model):
    programme = models.ForeignKey(Programme, on_delete=models.CASCADE, blank=True)
    type = models.ForeignKey(IndicatorTypes, on_delete=models.CASCADE)
    name = models.CharField(max_length=100, blank=False)
    short_title = models.CharField(max_length=100, blank=False)
    description = models.CharField(max_length=1500)
    slug = models.CharField(max_length=100, unique=True, blank=False)
    active = models.BooleanField()
    location = models.PolygonField(blank=True)
    formula = models.CharField(blank=True)
    parent = models.ForeignKey("self", on_delete=models.CASCADE)


class IndicatorData(models.Model):
    unit = models.CharField(blank=False, choices=UnitTypes.choices)
    indicator = models.ForeignKey(Indicator, related_name='data', on_delete=models.CASCADE)
    date = models.DateTimeField()
    value = models.CharField(max_length=15, blank=False)
