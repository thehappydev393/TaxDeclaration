# tax_processor/forms.py

from django import forms
from django.forms import formset_factory # Import formset_factory
from datetime import date
from .models import Declaration, TaxRule, DeclarationPoint, UnmatchedTransaction

# -----------------------------------------------------------
# Helper Widgets
# -----------------------------------------------------------

class UnescapedTextarea(forms.Textarea):
    """A Textarea widget that handles rendering clean, unescaped JSON strings."""
    def render(self, name, value, attrs=None, renderer=None):
        if value is None:
            value = ''
        return super().render(name, value, attrs, renderer)

# -----------------------------------------------------------
# Custom Choice Field for Display (Name - Description)
# -----------------------------------------------------------

class DeclarationPointChoiceField(forms.ModelChoiceField):
    """
    Custom field to display DeclarationPoint objects as "Name - Description...".
    """
    def label_from_instance(self, obj):
        # Format the display label for the dropdown
        description_preview = obj.description[:50]
        return f"{obj.name} - {description_preview}..."


# -----------------------------------------------------------
# 1. Statement Upload Form (Remains Unchanged)
# -----------------------------------------------------------

class StatementUploadForm(forms.Form):
    client_name = forms.CharField(
        max_length=100,
        label="Հաճախորդի անվանումը", # Client/Reference Name
        help_text="Օգտագործվում է Հայտարարգիրները մեկ Հայտարարության մեջ խմբավորելու համար:" # Used to group statements into a Declaration.
    )
    year = forms.IntegerField(
        label="Հարկային Տարի",
        initial=date.today().year,
        min_value=2020,
        max_value=2099,
        help_text="Հայտարարագիրը կներառի ընթացիկ տարին գումարած հաջորդ տարվա հունվարի 31-ը:"
    )

    statement_files = forms.FileField(
        label="Բանկային քաղվածքի ֆայլեր (Excel կամ PDF)",
        widget=forms.FileInput,
        required=False,
        help_text="Ընտրեք մեկ կամ մի քանի քաղվածքի ֆայլեր (Excel կամ PDF):"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})

# -----------------------------------------------------------
# NEW: Dynamic Condition Form
# -----------------------------------------------------------

# Define the choices for the dropdowns based on your models and rules_engine
TRANSACTION_FIELD_CHOICES = [
    ('description', 'Նկարագրություն (Description)'),
    ('sender', 'Ուղարկող (Sender)'),
    ('sender_account', 'Ուղարկողի Հաշիվ (Sender Account)'),
    ('amount', 'Գումար (Amount)'),
    ('currency', 'Արժույթ (Currency)'),
]

CONDITION_TYPE_CHOICES = [
    ('CONTAINS_KEYWORD', 'պարունակում է (contains keyword)'),
    ('EQUALS', 'հավասար է (equals)'),
    ('REGEX_MATCH', 'համընկնում է REGEX-ին (regex match)'),
    ('GREATER_THAN', 'մեծ է (>)'),
    ('LESS_THAN', 'փոքր է (<)'),
]

class ConditionForm(forms.Form):
    """
    A single form representing one check in the rules engine.
    e.g., [Field: description] [Type: CONTAINS_KEYWORD] [Value: 'Salary']
    """
    field = forms.ChoiceField(
        choices=TRANSACTION_FIELD_CHOICES,
        label="Դաշտ",
        widget=forms.Select(attrs={'class': 'condition-field'})
    )
    condition_type = forms.ChoiceField(
        choices=CONDITION_TYPE_CHOICES,
        label="Պայման",
        widget=forms.Select(attrs={'class': 'condition-type'})
    )
    value = forms.CharField(
        label="Արժեք",
        widget=forms.TextInput(attrs={'class': 'condition-value', 'placeholder': 'Enter value...'})
    )

# Create a FormSet factory from the new ConditionForm
# extra=1 shows one blank form by default.
# can_delete=True adds a checkbox for deleting rows.
BaseConditionFormSet = formset_factory(ConditionForm, extra=1, can_delete=True)


# -----------------------------------------------------------
# 2. Tax Rule Form (MODIFIED)
# -----------------------------------------------------------

class TaxRuleForm(forms.ModelForm):
    """
    Form for creating and editing Tax Rules.
    NOW uses a dynamic logic dropdown instead of the JSON textarea.
    """

    declaration_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Հայտարարագրման Կետ (Category)",
        help_text="Ընտրեք այն կատեգորիան, որին կփոխանցվեն համապատասխան գործարքները։"
    )

    # NEW: Logic dropdown
    logic = forms.ChoiceField(
        choices=[
            ('AND', 'Համընկնում են ԲՈԼՈՐ պայմանները (AND)'),
            ('OR', 'Համընկնում է ՑԱՆԿԱՑԱԾ պայման (OR)'),
        ],
        label="Կանոնի Տրամաբանություն",
        help_text="Ինչպես պետք է համակցվեն ստորև նշված պայմանները:"
    )

    class Meta:
        model = TaxRule
        # REMOVED 'conditions_json' from fields
        fields = ['rule_name', 'priority', 'declaration_point', 'is_active']
        widgets = {
            'priority': forms.NumberInput(attrs={'min': 1, 'max': 100}),
        }


# -----------------------------------------------------------
# 3. Resolution Form (For Regular User Review)
# -----------------------------------------------------------

class ResolutionForm(forms.Form):
    """Form for a Regular User to resolve an unmatched transaction."""

    # CRITICAL FIX: Use the custom choice field for the resolved point selection.
    resolved_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Վերջնական Հարկային Հայտարարագրման Կետ",
        help_text="Ընտրեք այն կատեգորիան, որին պետք է դասել այս գործարքը։"
    )

    propose_rule = forms.BooleanField(
        required=False,
        label="Առաջարկել Նոր Կանոն",
        help_text="Նշեք այս վանդակը՝ այս գործարքի հիման վրա ավտոմատացված նոր կանոն առաջարկելու համար։"
    )

    rule_notes = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={'rows': 4}),
        label="Կանոնի Նշումներ",
        help_text="Բացատրեք պայմանները (օրինակ՝ 'Համընկնում է, եթե նկարագրությունը պարունակում է X և գումարը > Y')."
    )

    # Hidden field to hold the unmatched ID for processing
    unmatched_id = forms.IntegerField(widget=forms.HiddenInput())
