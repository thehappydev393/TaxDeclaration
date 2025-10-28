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
    client_name = forms.CharField(max_length=100, label="Հաճախորդի անվանումը", help_text="Օգտագործվում է Հայտարարգիրները մեկ Հայտարարության մեջ խմբավորելու համար:")
    year = forms.IntegerField(label="Հարկային Տարի", initial=date.today().year, min_value=2020, max_value=2099, help_text="Հայտարարագիրը կներառի ընթացիկ տարին գումարած հաջորդ տարվա հունվարի 31-ը:")
    statement_files = forms.FileField(label="Բանկային քաղվածքի ֆայլեր (Excel կամ PDF)", widget=forms.FileInput, required=False, help_text="Ընտրեք մեկ կամ մի քանի քաղվածքի ֆայլեր (Excel կամ PDF):")
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs); self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})

# --- (Keep ConditionForm, BaseConditionFormSet, TaxRuleForm as they are) ---
TRANSACTION_FIELD_CHOICES = [ ('description', 'Նկարագրություն (Description)'), ('sender', 'Ուղարկող (Sender)'), ('sender_account', 'Ուղարկողի Հաշիվ (Sender Account)'), ('amount', 'Գումար (Amount)'), ('currency', 'Արժույթ (Currency)'),]
CONDITION_TYPE_CHOICES = [ ('CONTAINS_KEYWORD', 'պարունակում է (contains keyword)'), ('DOES_NOT_CONTAIN_KEYWORD', 'ՉԻ պարունակում (does NOT contain)'), ('EQUALS', 'հավասար է (equals)'), ('REGEX_MATCH', 'համընկնում է REGEX-ին (regex match)'), ('GREATER_THAN', 'մեծ է (>)'), ('GREATER_THAN_OR_EQUAL', 'մեծ է կամ հավասար (>=)'), ('LESS_THAN', 'փոքր է (<)'), ('LESS_THAN_OR_EQUAL', 'փոքր է կամ հավասար (<=)'),]
class ConditionForm(forms.Form):
    field = forms.ChoiceField(choices=TRANSACTION_FIELD_CHOICES, label="Դաշտ", widget=forms.Select(attrs={'class': 'condition-field'}))
    condition_type = forms.ChoiceField(choices=CONDITION_TYPE_CHOICES, label="Պայման", widget=forms.Select(attrs={'class': 'condition-type'}))
    value = forms.CharField(label="Արժեք", widget=forms.TextInput(attrs={'class': 'condition-value', 'placeholder': 'Enter value...'}))
BaseConditionFormSet = formset_factory(ConditionForm, extra=0, can_delete=True)
class TaxRuleForm(forms.ModelForm):
    declaration_point = DeclarationPointChoiceField(queryset=DeclarationPoint.objects.all().order_by('name'), label="Հայտարարագրման Կետ (Category)", help_text="Ընտրեք այն կատեգորիան, որին կփոխանցվեն համապատասխան գործարքները։")
    logic = forms.ChoiceField(choices=[('AND', 'Համընկնում են ԲՈԼՈՐ պայմանները (AND)'), ('OR', 'Համընկնում է ՑԱՆԿԱՑԱԾ պայման (OR)'),], label="Կանոնի Տրամաբանություն", help_text="Ինչպես պետք է համակցվեն ստորև նշված պայմանները:")
    class Meta:
        model = TaxRule; fields = ['rule_name', 'priority', 'declaration_point', 'is_active']; widgets = { 'priority': forms.NumberInput(attrs={'min': 1, 'max': 100}), }

# -----------------------------------------------------------
# 3. Resolution Form (MODIFIED)
# -----------------------------------------------------------

class ResolutionForm(forms.Form):
    """
    Form for a Regular User to resolve an unmatched transaction AND
    optionally create a declaration-specific rule based on it.
    """
    resolved_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Վերջնական Հարկային Հայտարարագրման Կետ", # Final Tax Declaration Point
        help_text="Ընտրեք այն կատեգորիան, որին պետք է դասել այս գործարքը։" # Select the category...
    )

    # --- UPDATED Checkbox ---
    create_specific_rule = forms.BooleanField(
        required=False,
        label="Ստեղծել Հատուկ Կանոն Այս Հայտարարագրի Համար", # Create Specific Rule for this Declaration
        help_text="Նշեք այս վանդակը՝ այս գործարքի նկարագրության հիման վրա ավտոմատ կանոն ստեղծելու համար։" # Check to auto-create rule based on description.
    )

    # --- NEW Fields (shown conditionally via JS) ---
    rule_name = forms.CharField(
        required=False, # Required only if checkbox is checked (handled in view/JS)
        max_length=255,
        label="Նոր Կանոնի Անվանում", # New Rule Name
        help_text="Տվեք հիշվող անուն այս կանոնին (օրինակ՝ 'Ամսական Աշխատավարձ')։", # Give a memorable name...
        widget=forms.TextInput(attrs={'placeholder': 'Օրինակ՝ Ամսական Աշխատավարձ'}) # Example: Monthly Salary
    )

    priority = forms.IntegerField(
        required=False, # Required only if checkbox is checked
        initial=50,
        min_value=1,
        max_value=100,
        label="Կանոնի Առաջնահերթություն", # Rule Priority
        help_text="Ցածր թիվը նշանակում է ավելի բարձր առաջնահերթություն (1-100)։", # Lower number = higher priority
        widget=forms.NumberInput(attrs={'min': 1, 'max': 100})
    )
    # --- END NEW Fields ---

    # Removed rule_notes field

    # Hidden field to hold the unmatched ID for processing
    unmatched_id = forms.IntegerField(widget=forms.HiddenInput())
