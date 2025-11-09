# tax_processor/forms.py

from django import forms
from django.forms import formset_factory
from datetime import date
from .models import (
    Declaration, TaxRule, DeclarationPoint, UnmatchedTransaction,
    EntityTypeRule, TransactionScopeRule, Transaction # Make sure Transaction is imported
)

# --- (Keep Helper Widgets and DeclarationPointChoiceField as they are) ---
class UnescapedTextarea(forms.Textarea):
    def render(self, name, value, attrs=None, renderer=None):
        if value is None: value = ''; return super().render(name, value, attrs, renderer)

class DeclarationPointChoiceField(forms.ModelChoiceField):
    def label_from_instance(self, obj):
        description_preview = obj.description[:50]; return f"{obj.name} - {description_preview}..."

# --- (Keep StatementUploadForm as is) ---
class StatementUploadForm(forms.Form):
    client_name = forms.CharField(
        max_length=100,
        label="Հաճախորդի անվանումը (Ընկերություն կամ Անհատ)",
        help_text="Օրինակ՝ «Սարմեն» ՍՊԸ կամ Պողոս Պողոսյան"
    )
    first_name = forms.CharField(
        max_length=150,
        label="Հաճախորդի Անուն",
        required=True
    )
    last_name = forms.CharField(
        max_length=150,
        label="Հաճախորդի Ազգանուն",
        required=True
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
    field_order = ['client_name', 'first_name', 'last_name', 'year', 'statement_files']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['statement_files'].widget.attrs.update({'multiple': 'multiple'})
        if hasattr(self, 'field_order'):
            self.order_fields(self.field_order)


# --- UPDATED: TRANSACTION_FIELD_CHOICES ---
# Added new fields we can use as the *source* of a rule
TRANSACTION_FIELD_CHOICES = [
    ('description', 'Նկարագրություն (Description)'),
    ('sender', 'Ուղարկող (Sender)'),
    ('sender_account', 'Ուղարկողի Հաշիվ (Sender Account)'),
    ('amount', 'Գումար (Amount - Compares in AMD)'),
    ('currency', 'Արժույթ (Currency)'),
    ('statement__bank_name', 'Բանկի Անվանում (Bank Name)'),
    ('entity_type', 'Իրավական Կարգավիճակ (Entity Type)'),
    ('transaction_scope', 'Տարածք (Scope)'),
]
# --- END UPDATED ---

# --- UPDATED: CONDITION_TYPE_CHOICES ---
# Added new "Field Value" comparison types
CONDITION_TYPE_CHOICES = [
    ('CONTAINS_KEYWORD', 'պարունակում է (contains keyword)'),
    ('DOES_NOT_CONTAIN_KEYWORD', 'ՉԻ պարունակում (does NOT contain)'),
    ('EQUALS', 'հավասար է (equals)'),
    ('REGEX_MATCH', 'համընկնում է REGEX-ին (regex match)'),
    ('GREATER_THAN', 'մեծ է (>)'),
    ('GREATER_THAN_OR_EQUAL', 'մեծ է կամ հավասար (>=)'),
    ('LESS_THAN', 'փոքր է (<)'),
    ('LESS_THAN_OR_EQUAL', 'փոքր է կամ հավասար (<=)'),
    ('CONTAINS_FIELD_VALUE', 'պարունակում է դաշտի արժեքը (contains field value)'),
    ('NOT_CONTAINS_FIELD_VALUE', 'ՉԻ պարունակում դաշտի արժեքը (does not contain field value)'),
    ('EQUALS_FIELD_VALUE', 'հավասար է դաշտի արժեքին (equals field value)'),
]
# --- END UPDATED ---

# --- NEW: DYNAMIC_FIELD_CHOICES ---
# These are the fields a user can compare *against* (the "right side" of the rule)
DYNAMIC_FIELD_CHOICES = [
    ('statement__declaration__first_name', 'Հաճախորդի Անուն (Client First Name)'),
    ('statement__declaration__last_name', 'Հաճախորդի Ազգանուն (Client Last Name)'),
    ('sender', 'Ուղարկող (Sender)'), # Compare description to sender, etc.
    ('description', 'Նկարագրություն (Description)'),
]
# --- END NEW ---

# --- UPDATED: ConditionForm ---
class ConditionForm(forms.Form):
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
    # This is the original text input
    value = forms.CharField(
        label="Արժեք",
        widget=forms.TextInput(attrs={'class': 'condition-value-input form-control', 'placeholder': 'Մուտքագրեք արժեքը...'}),
        required=False # Now optional, will be toggled by JS
    )
    # This is the new dropdown for field-to-field comparison
    value_field = forms.ChoiceField(
        choices=DYNAMIC_FIELD_CHOICES,
        label="Համեմատվող Դաշտ", # Comparison Field
        widget=forms.Select(attrs={'class': 'condition-value-field form-control'}),
        required=False # Also optional
    )
# --- END UPDATED ---

BaseConditionFormSet = formset_factory(ConditionForm, extra=0, can_delete=True)


# --- Base Form for all Rule Types ---
class BaseRuleForm(forms.ModelForm):
    logic = forms.ChoiceField(
        choices=[('AND', 'Համընկնում են ԲՈԼՈՐ պայմանները (AND)'), ('OR', 'Համընկնում է ՑԱՆԿԱՑԾ պայման (OR)'),],
        label="Կանոնի Տրամաբանություն",
        help_text="Ինչպես պետք է համակցվեն ստորև նշված պայմանները:",
        required=False
    )
    rule_name = forms.CharField(
        max_length=255,
        label="Կանոնի Անվանում",
        help_text="Տվեք հիշվող անուն այս կանոնին:",
        widget=forms.TextInput(attrs={'placeholder': 'Օրինակ՝ Ամսական Աշխատավարձ'}),
        required=False
    )
    priority = forms.IntegerField(
        initial=50, min_value=1, max_value=100,
        label="Կանոնի Առաջնահերթություն",
        help_text="Ցածր թիվը նշանակում է ավելի բարձր առաջնահերթություն (1-100)։",
        widget=forms.NumberInput(attrs={'min': 1, 'max': 100}),
        required=False
    )
    is_active = forms.BooleanField(initial=True, required=False, label="Ակտիվ")


# --- TaxRuleForm inherits from BaseRuleForm ---
class TaxRuleForm(BaseRuleForm):
    declaration_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Հայտարարագրման Կետ (Category)",
        help_text="Ընտրեք այն կատեգորիան, որին կփոխանցվեն համապատասխան գործարքները։",
        required=False
    )
    class Meta:
        model = TaxRule
        fields = ['rule_name', 'priority', 'declaration_point', 'logic', 'is_active']


# --- EntityTypeRuleForm ---
class EntityTypeRuleForm(BaseRuleForm):
    entity_type_result = forms.ChoiceField(
        choices=Transaction.ENTITY_CHOICES,
        label="Արդյունքի Իրավական Կարգավիճակ",
        help_text="Եթե կանոնը համընկնի, գործարքին կտրվի այս կարգավիճակը:",
        required=False
    )
    class Meta:
        model = EntityTypeRule
        fields = ['rule_name', 'priority', 'entity_type_result', 'logic', 'is_active']


# --- TransactionScopeRuleForm ---
class TransactionScopeRuleForm(BaseRuleForm):
    scope_result = forms.ChoiceField(
        choices=Transaction.SCOPE_CHOICES,
        label="Արդյունքի Տարածք",
        help_text="Եթե կանոնը համընկնի, գործարքին կտրվի այս կարգավիճակը:",
        required=False
    )
    class Meta:
        model = TransactionScopeRule
        fields = ['rule_name', 'priority', 'scope_result', 'logic', 'is_active']

# -----------------------------------------------------------
# 3. Resolution Form
# -----------------------------------------------------------
class ResolutionForm(forms.Form):
    ACTION_CHOICES = [('resolve_only', 'Միայն Լուծել'), ('create_specific', 'Լուծել և Ստեղծել Հատուկ Կանոն'), ('propose_global', 'Լուծել և Առաջարկել Գլոբալ Կանոն'),]
    resolved_point = DeclarationPointChoiceField(queryset=DeclarationPoint.objects.all().order_by('name'), label="Վերջնական Հարկային Հայտարարագրման Կետ", help_text="Ընտրեք այն կատեգորիան, որին պետք է դասել այս գործարքը։")
    rule_action = forms.ChoiceField(choices=ACTION_CHOICES, widget=forms.RadioSelect, initial='resolve_only', label="Կանոնի Գործողություն", help_text="Ընտրեք՝ ինչպես վարվել այս լուծման հետ կանոնների առումով։")
    rule_notes = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 4}), label="Գլոբալ Կանոնի Առաջարկի Նշումներ", help_text="Բացատրեք պայմանները Superadmin-ի համար (օրինակ՝ 'Համընկնում է, եթե նկարագրությունը պարունակում է X և գումարը > Y').")
    unmatched_id = forms.IntegerField(widget=forms.HiddenInput())

class AddStatementsForm(forms.Form):
    statement_files = forms.FileField(
        label="Նոր Բանկային Քաղվածք(ներ)",
        required=True,
        help_text="Ընտրեք մեկ կամ մի քանի քաղվածքի ֆայլեր (Excel կամ PDF) ավելացնելու համար։"
    )

class TransactionEditForm(forms.Form):
    declaration_point = DeclarationPointChoiceField(
        queryset=DeclarationPoint.objects.all().order_by('name'),
        label="Նշանակված Հայտարարագրման Կետ",
        help_text="Ընտրեք նոր կետ կամ թողեք դատարկ՝ վերադարձնելու համար։",
        required=False
    )
    revert_to_pending = forms.BooleanField(
        required=False,
        label="Վերադարձնել «Սպասում է Վերանայման» կարգավիճակին",
        help_text="Նշեք այս վանդակը՝ գործարքը վերանայման հերթ վերադարձնելու համար (կջնջի ընթացիկ նշանակումը)։"
    )
