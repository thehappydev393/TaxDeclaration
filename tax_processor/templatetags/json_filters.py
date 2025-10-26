
from django import template
import json
import re

register = template.Library()

@register.filter
def json_unformat(value):
    """
    Decodes a JSON string for display, stripping excess quotes/escapes.
    This version removes manual character decoding to prevent Mojibake.
    """
    if not value:
        return ""

    s = str(value)

    # 1. CRITICAL: Remove outer quotes that the form/template may add
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]

    try:
        # 2. Attempt to load the string as JSON
        # This handles decoding escape sequences like \n and \t implicitly.
        data = json.loads(s)

        # 3. Re-dump the object cleanly for indentation
        # Use ensure_ascii=False to ensure Armenian characters are preserved (not converted to \uXXXX)
        return json.dumps(data, indent=2, ensure_ascii=False)

    except Exception:
        # If decoding fails (e.g., if the user manually broke the JSON),
        # return the raw value so the user can fix it.
        return value
