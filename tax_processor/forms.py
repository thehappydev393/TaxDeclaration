# tax_processor/forms.py

from django import forms
from datetime import date
from .models import Declaration, TaxRule # Import TaxRule model

class StatementUploadForm(forms.Form):
    # 1. Client/Reference Name added back
    client_name = forms.CharField(
        max_length=100,
        label="Client/Reference Name",
        help_text="Used to group statements into a Declaration."
    )
    # 2. Year remains the same
    year = forms.IntegerField(
        label="Tax Year",
        initial=date.today().year,
        min_value=2000,
        max_value=2099,
        help_text="The declaration will cover this year plus January 31st of the following year."
    )

    # 3. Multi-file upload setup
    statement_files = forms.FileField(
        label="Bank Statement Files (Excel or PDF)",
        widget=forms.FileInput,
        required=False,
        help_text="Select one or more statement files (Excel or PDF)."
    )

    # CRITICAL INJECTION: Force the 'multiple' attribute onto the widget
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})

class TaxRuleForm(forms.ModelForm):
    """Form for creating and editing Tax Rules."""
    class Meta:
        model = TaxRule
        fields = ['rule_name', 'priority', 'declaration_point', 'conditions_json', 'is_active']
        widgets = {
            'conditions_json': forms.Textarea(attrs={'rows': 10, 'cols': 80}),
            'declaration_point': forms.TextInput(attrs={'placeholder': 'e.g., Business Income - Sales'}),
            'priority': forms.NumberInput(attrs={'min': 1, 'max': 100}),
        }
        help_texts = {
            'conditions_json': 'Enter the rule logic as a JSON array (e.g., [{"logic": "AND", "checks": [...]}]).',
        }
