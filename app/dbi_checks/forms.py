from django import forms

class ExcelUploadForm(forms.Form):
    xlsx_file = forms.FileField(label="Select the first Excel file", required=True)
    second_xlsx_file = forms.FileField(label="Select the second Excel file (optional)", required=False)