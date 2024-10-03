from django import forms

class UploadFileForm(forms.Form):
    BANK_CHOICES = [
        ('hdfc', 'HDFC'),
        ('icici', 'ICICI'),
        ('karur_vysya', 'Karur Vysya'),
    ]
    
    TRANSACTION_CHOICES = [
        ('booking', 'Booking'),
        ('refund', 'Refund'),
    ]
    
    bank_name = forms.ChoiceField(choices=BANK_CHOICES)
    transaction_type = forms.ChoiceField(choices=TRANSACTION_CHOICES)
    file = forms.FileField()
