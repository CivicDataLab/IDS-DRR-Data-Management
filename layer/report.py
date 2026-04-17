"""Shared utilities for the report functionality."""

import json

from django.conf import settings


def load_reports():
    """Return the report configuration, keyed by state code."""
    specs = settings.CONFIG.get("reports", {}).get("states")
    if specs:
        return specs
    path = settings.BASE_DIR / "report_config.json"
    if path.is_file():
        return json.loads(path.read_text())
    return {}
