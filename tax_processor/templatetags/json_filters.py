# tax_processor/templatetags/json_filters.py (Final Modification)

from django import template
import json

register = template.Library()

@register.filter
def json_unformat(value):
    """
    Decodes a JSON string, ensuring it is clean, indented, and free of
    literal outer quotes and escape characters for display in a textarea.
    """
    if not value:
        return ""

    try:
        # CRITICAL FIX 1: Ensure the value is a string, then remove the persistent outer quotes.
        # The form rendering adds quotes around the value, so we remove them.
        s = str(value).strip()
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1]

        # Step 2: Handle internal Python escape sequences (like \n, \t, \")
        # We must use decode('unicode_escape') to convert Python escape codes into real characters.
        # This requires converting the string back to bytes momentarily.
        # We also manually clean up escaped quotes and backslashes that Django/browsers add.
        s = s.encode('utf-8').decode('unicode_escape')

        # Step 3: Load the resulting string into a Python object to clean up the structure
        data = json.loads(s)

        # Step 4: Re-dump the object cleanly for indentation
        return json.dumps(data, indent=2, ensure_ascii=False)

    except Exception:
        # If any decoding or conversion fails, return the original value for manual inspection
        return value
