"""Shared utilities for the import management commands."""

from django.conf import settings


def state_names():
    """Return the name of each state, for use as ``--state`` choices."""
    return [spec["name"] for spec in settings.CONFIG.get("states", [])]


def selected_states(arg):
    """
    Return the subset of ``settings.CONFIG["states"]`` to act on.

    With no ``arg``, returns every entry. With one, returns a list
    containing the single matching entry.
    """
    specs = settings.CONFIG.get("states", [])
    if arg:
        return [next(spec for spec in specs if spec["name"] == arg)]
    return specs
