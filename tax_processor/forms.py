# tax_processor/forms.py

from django import forms
from datetime import date
from .models import Declaration, TaxRule, DeclarationPoint, UnmatchedTransaction

class UnescapedTextarea(forms.Textarea):
    """A Textarea widget that disables HTML escaping for its value."""
    def value_from_datadict(self, data, files, name):
        # Prevent premature HTML escaping if the data is being re-posted
        value = data.get(name)
        return value

    # CRITICAL METHOD: Forces Django to render the value without escaping it
    def render(self, name, value, attrs=None, renderer=None):
        if value is None:
            value = ''

        # Un-escape the string value before calling the base renderer
        # This is the most reliable way to force line breaks and spaces to display correctly.
        value = value.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')

        return super().render(name, value, attrs, renderer)

class StatementUploadForm(forms.Form):
    client_name = forms.CharField(
        max_length=100,
        label="Client/Reference Name",
        help_text="Used to uniquely group statements into a Declaration."
    )

    year = forms.IntegerField(
        label="Tax Year",
        initial=date.today().year,
        min_value=2000,
        max_value=2099,
        help_text="The declaration will cover this year plus January 31st of the following year."
    )

    statement_files = forms.FileField(
        label="Bank Statement Files (Excel or PDF)",
        widget=forms.FileInput,
        required=False,
        help_text="Select one or more statement files (Excel or PDF)."
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})

class TaxRuleForm(forms.ModelForm):
    """Form for creating and editing Tax Rules."""

    declaration_point = forms.ModelChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Declaration Point (Category)",
        help_text="Select the category this rule assigns transactions to."
    )

    class Meta:
        model = TaxRule
        fields = ['rule_name', 'priority', 'declaration_point', 'conditions_json', 'is_active']
        widgets = {
            'conditions_json': UnescapedTextarea(attrs={'rows': 10, 'cols': 80}),
            'priority': forms.NumberInput(attrs={'min': 1, 'max': 100}),
        }
        help_texts = {
            'conditions_json': 'Enter the rule logic as a JSON array (e.g., [{"logic": "AND", "checks": [...]}]).',
        }

# --- 3. Resolution Form (Updated to use ModelChoiceField) ---

class ResolutionForm(forms.Form):
    """Form for a Regular User to resolve an unmatched transaction."""

    # CRITICAL FIX: Change CharField to ModelChoiceField for resolved_point
    resolved_point = forms.ModelChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Final Tax Declaration Point",
        help_text="Select the exact category this transaction belongs to."
    )

    propose_rule = forms.BooleanField(
        required=False,
        label="Propose New Rule?",
        help_text="Check this box to suggest a new automated rule based on this transaction."
    )

    rule_notes = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={'rows': 4}),
        label="Rule Notes",
        help_text="Explain the conditions (e.g., 'Match if description contains X and amount is > Y')."
    )

    # Hidden field to hold the unmatched ID for processing
    unmatched_id = forms.IntegerField(widget=forms.HiddenInput())
