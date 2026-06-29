from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("layer", "0004_geography_simple_geom_alter_geography_type_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="indicators",
            name="module",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="flood",
                help_text="Hazard module this indicator belongs to, e.g. 'flood', 'heat'.",
                max_length=50,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="indicators",
            name="is_raster_available",
            field=models.BooleanField(
                default=False,
                help_text="Heat only: whether a raster layer exists for this indicator.",
            ),
        ),
        migrations.AddField(
            model_name="data",
            name="module",
            field=models.CharField(
                blank=True,
                db_index=True,
                default="flood",
                help_text="Hazard module (mirrors indicator.module), e.g. 'flood', 'heat'.",
                max_length=50,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="data",
            name="raster_file",
            field=models.CharField(
                blank=True,
                help_text="Filename under RASTER_DATA_DIR (or full URL) for this row's raster layer.",
                max_length=500,
                null=True,
            ),
        ),
        migrations.AddIndex(
            model_name="data",
            index=models.Index(
                fields=["module", "data_period"], name="layer_data_module_period_idx"
            ),
        ),
    ]
