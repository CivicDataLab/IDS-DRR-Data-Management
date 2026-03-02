from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from layer.models import Data, Geography, Indicators
from layer.cache_utils import (
    invalidate_data_caches,
    invalidate_geography_caches,
    invalidate_indicator_caches,
)


@receiver(post_save, sender=Data)
@receiver(post_delete, sender=Data)
def invalidate_data_cache(sender, instance, **kwargs):
    """
    Invalidate data-dependent caches when Data model is updated or deleted.
    """
    invalidate_data_caches()


@receiver(post_save, sender=Geography)
@receiver(post_delete, sender=Geography)
def invalidate_geography_cache(sender, instance, **kwargs):
    """
    Invalidate geography-dependent caches when Geography model is updated or deleted.
    """
    invalidate_geography_caches()


@receiver(post_save, sender=Indicators)
@receiver(post_delete, sender=Indicators)
def invalidate_indicator_cache(sender, instance, **kwargs):
    """
    Invalidate indicator-dependent caches when Indicators model is updated or deleted.
    """
    invalidate_indicator_caches()
