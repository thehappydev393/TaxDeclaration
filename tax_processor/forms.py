# tax_processor/forms.py

from django import forms
from django.forms import formset_factory
from datetime import date
from .models import Declaration, TaxRule, DeclarationPoint, UnmatchedTransaction

# --- (Keep Helper Widgets and DeclarationPointChoiceField as they are) ---
class UnescapedTextarea(forms.Textarea):
    def render(self, name, value, attrs=None, renderer=None):
        if value is None: value = ''; return super().render(name, value, attrs, renderer)

class DeclarationPointChoiceField(forms.ModelChoiceField):
    def label_from_instance(self, obj):
        description_preview = obj.description[:50]; return f"{obj.name} - {description_preview}..."

# --- (Keep StatementUploadForm as is) ---
class StatementUploadForm(forms.Form):
    # ... (fields) ...
    client_name = forms.CharField(max_length=100, label="Հաճախորդի անվանումը", help_text="Վերնագրի համար:")
    year = forms.IntegerField(label="Հարկային Տարի", initial=date.today().year, min_value=2020, max_value=2099, help_text="Հայտարարագիրը կներառի ընթացիկ տարին գումարած հաջորդ տարվա հունվարի 31-ը:")
    statement_files = forms.FileField(label="Բանկային քաղվածքի ֆայլեր (Excel կամ PDF)", widget=forms.FileInput, required=False, help_text="Ընտրեք մեկ կամ մի քանի քաղվածքի ֆայլեր (Excel կամ PDF):")
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs); self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})


# --- (Keep ConditionForm, BaseConditionFormSet as they are) ---
TRANSACTION_FIELD_CHOICES = [ ('description', 'Նկարագրություն (Description)'), ('sender', 'Ուղարկող (Sender)'), ('sender_account', 'Ուղարկողի Հաշիվ (Sender Account)'), ('amount', 'Գումար (Amount)'), ('currency', 'Արժույթ (Currency)'),('statement__bank_name', 'Բանկի Անվանում (Bank Name)'),]
CONDITION_TYPE_CHOICES = [ ('CONTAINS_KEYWORD', 'պարունակում է (contains keyword)'), ('DOES_NOT_CONTAIN_KEYWORD', 'ՉԻ պարունակում (does NOT contain)'), ('EQUALS', 'հավասար է (equals)'), ('REGEX_MATCH', 'համընկնում է REGEX-ին (regex match)'), ('GREATER_THAN', 'մեծ է (>)'), ('GREATER_THAN_OR_EQUAL', 'մեծ է կամ հավասար (>=)'), ('LESS_THAN', 'փոքր է (<)'), ('LESS_THAN_OR_EQUAL', 'փոքր է կամ հավասար (<=)'),]
class ConditionForm(forms.Form):
    field = forms.ChoiceField(choices=TRANSACTION_FIELD_CHOICES, label="Դաշտ", widget=forms.Select(attrs={'class': 'condition-field'}))
    condition_type = forms.ChoiceField(choices=CONDITION_TYPE_CHOICES, label="Պայման", widget=forms.Select(attrs={'class': 'condition-type'}))
    value = forms.CharField(label="Արժեք", widget=forms.TextInput(attrs={'class': 'condition-value', 'placeholder': 'Մուտքագրեք արժեքը...'}))
BaseConditionFormSet = formset_factory(ConditionForm, extra=0, can_delete=True)

# --- TaxRuleForm MODIFIED ---
class TaxRuleForm(forms.ModelForm):
    # Make fields required=False so browser validation doesn't block submission
    # when the form is hidden. We will enforce requirement in the view conditionally.
    declaration_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Հայտարարագրման Կետ (Category)",
        help_text="Ընտրեք այն կատեգորիան, որին կփոխանցվեն համապատասխան գործարքները։",
        required=False # MODIFIED
    )
    logic = forms.ChoiceField(
        choices=[('AND', 'Համընկնում են ԲՈԼՈՐ պայմանները (AND)'), ('OR', 'Համընկնում է ՑԱՆԿԱՑԱԾ պայման (OR)'),],
        label="Կանոնի Տրամաբանություն",
        help_text="Ինչպես պետք է համակցվեն ստորև նշված պայմանները:",
        required=False # MODIFIED (Logic isn't needed if no conditions)
    )
    rule_name = forms.CharField(
        max_length=255,
        label="Կանոնի Անվանում",
        help_text="Տվեք հիշվող անուն այս կանոնին:",
        widget=forms.TextInput(attrs={'placeholder': 'Օրինակ՝ Ամսական Աշխատավարձ'}),
        required=False # MODIFIED
    )
    priority = forms.IntegerField(
        initial=50, min_value=1, max_value=100,
        label="Կանոնի Առաջնահերթություն",
        help_text="Ցածր թիվը նշանակում է ավելի բարձր առաջնահերթություն (1-100)։",
        widget=forms.NumberInput(attrs={'min': 1, 'max': 100}),
        required=False # MODIFIED
    )
    is_active = forms.BooleanField(initial=True, required=False, label="Ակտիվ")

    class Meta:
        model = TaxRule
        fields = ['rule_name', 'priority', 'declaration_point', 'logic', 'is_active']


# -----------------------------------------------------------
# 3. Resolution Form (Unchanged from previous version)
# -----------------------------------------------------------
class ResolutionForm(forms.Form):
    ACTION_CHOICES = [('resolve_only', 'Միայն Լուծել'), ('create_specific', 'Լուծել և Ստեղծել Հատուկ Կանոն'), ('propose_global', 'Լուծել և Առաջարկել Գլոբալ Կանոն'),]
    resolved_point = DeclarationPointChoiceField(queryset=DeclarationPoint.objects.all().order_by('name'), label="Վերջնական Հարկային Հայտարարագրման Կետ", help_text="Ընտրեք այն կատեգորիան, որին պետք է դասել այս գործարքը։")
    rule_action = forms.ChoiceField(choices=ACTION_CHOICES, widget=forms.RadioSelect, initial='resolve_only', label="Կանոնի Գործողություն", help_text="Ընտրեք՝ ինչպես վարվել այս լուծման հետ կանոնների առումով։")
    rule_notes = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 4}), label="Գլոբալ Կանոնի Առաջարկի Նշումներ", help_text="Բացատրեք պայմանները Superadmin-ի համար (օրինակ՝ 'Համընկնում է, եթե նկարագրությունը պարունակում է X և գումարը > Y').")
    unmatched_id = forms.IntegerField(widget=forms.HiddenInput())

class AddStatementsForm(forms.Form):
    """Simple form to upload additional statements to an existing Declaration."""
    statement_files = forms.FileField(
        label="Նոր Բանկային Քաղվածք(ներ)", # New Bank Statement(s)
        required=True, # Must upload at least one file
        help_text="Ընտրեք մեկ կամ մի քանի քաղվածքի ֆայլեր (Excel կամ PDF) ավելացնելու համար։" # Select one or more... to add.
    )

class TransactionEditForm(forms.Form):
    """Form to edit the assignment of a single transaction."""
    declaration_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Նշանակված Հայտարարագրման Կետ", # Assigned Declaration Point
        help_text="Ընտրեք նոր կետ կամ թողեք դատարկ՝ վերադարձնելու համար։", # Select new point or leave blank to revert.
        required=False # Allow blank selection to revert
    )

    revert_to_pending = forms.BooleanField(
        required=False,
        label="Վերադարձնել «Սպասում է Վերանայման» կարգավիճակին", # Revert to 'Pending Review' status
        help_text="Նշեք այս վանդակը՝ գործարքը վերանայման հերթ վերադարձնելու համար (կջնջի ընթացիկ նշանակումը)։" # Check this box to return the transaction to the review queue (will clear current assignment).
    )
