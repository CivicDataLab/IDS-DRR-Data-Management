from django.apps import AppConfig


class LayerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'layer'

    def ready(self):
        import layer.signals
