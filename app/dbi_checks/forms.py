from django import forms

class ExcelUploadForm(forms.Form):
    xlsx_file1 = forms.FileField(label="Select the first Excel file", required=True)
    xlsx_file2 = forms.FileField(label="Select the second Excel file (optional)", required=False)