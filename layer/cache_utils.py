import hashlib
import json
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)


def generate_cache_key(*args, **kwargs) -> str:
    """
    Generate a unique cache key based on function arguments.

    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        str: A unique cache key hash

    """
    key_data = {
        'args': [str(arg) for arg in args],
        'kwargs': {k: str(v) for k, v in sorted(kwargs.items())}
    }
    key_string = json.dumps(key_data, sort_keys=True)
    return hashlib.md5(key_string.encode()).hexdigest()


def cache_query(cache_type: str, timeout: int | None = None):
    """
    Decorator to cache query results with configurable timeout.

    Args:
        cache_type: Type of cache (maps to CACHE_TIMEOUTS in settings)
        timeout: Optional custom timeout in seconds (overrides default)

    Usage:
        @cache_query('map_data')
        def get_map_data(...):
            ...

    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Generate cache key from function name and arguments
            cache_key_base = f"{func.__name__}_{generate_cache_key(*args, **kwargs)}"

            # Try to get from cache
            cached_result = cache.get(cache_key_base)
            if cached_result is not None:
                logger.debug("Cache HIT for %s", func.__name__)
                return cached_result

            # Cache miss - execute function
            logger.debug("Cache MISS for %s", func.__name__)
            result = func(*args, **kwargs)

            # Determine timeout
            cache_timeout = timeout
            if cache_timeout is None:
                cache_timeouts = getattr(settings, 'CACHE_TIMEOUTS', {})
                cache_timeout = cache_timeouts.get(cache_type, 60 * 15)

            # Store in cache
            cache.set(cache_key_base, result, cache_timeout)

            return result
        return wrapper
    return decorator


def invalidate_cache_pattern(pattern: str, verbose: bool = False):
    """
    Invalidate all cache keys matching a pattern.

    Args:
        pattern: Pattern to match cache keys (e.g., 'get_states_*')
        verbose: If True, print invalidation messages (default: False)

    """
    try:
        cache.delete_pattern(f"*{pattern}*")
        if verbose:
            logger.info("Invalidated cache pattern: %s", pattern)
    except Exception:
        logger.error("Error invalidating cache pattern %s", pattern, exc_info=True)


def invalidate_data_caches(verbose: bool = False):
    """
    Invalidate all data-dependent caches.
    Should be called when data is updated.

    Args:
        verbose: If True, print invalidation messages (default: False)

    """
    patterns = [
        'get_district_data_*',
        'get_table_data_*',
        'get_time_trends_*',
        'get_revenue_data_*',
        'get_revenue_map_data_*',
        'get_district_map_data_*',
        'get_timeperiod_*',
    ]
    for pattern in patterns:
        invalidate_cache_pattern(pattern, verbose=verbose)


def invalidate_geography_caches(verbose: bool = False):
    """
    Invalidate geography-dependent caches.
    Should be called when geography data is updated.

    Args:
        verbose: If True, print invalidation messages (default: False)

    """
    patterns = [
        'get_states_*',
        'get_district_rev_circle_*',
        'get_district_map_data_*',
        'get_revenue_map_data_*',
    ]
    for pattern in patterns:
        invalidate_cache_pattern(pattern, verbose=verbose)


def invalidate_indicator_caches(verbose: bool = False):
    """
    Invalidate indicator-dependent caches.
    Should be called when indicators are updated.

    Args:
        verbose: If True, print invalidation messages (default: False)

    """
    patterns = [
        'get_indicators_*',
        'get_child_indicators_*',
    ]
    for pattern in patterns:
        invalidate_cache_pattern(pattern, verbose=verbose)


def clear_all_caches():
    """
    Clear all application caches.
    Use with caution - only for maintenance or major updates.
    """
    try:
        cache.clear()
        logger.info("All caches cleared successfully")
    except Exception:
        logger.error("Error clearing all caches", exc_info=True)
