from django import forms

class ExcelDbfUploadForm(forms.Form):
    xlsx_file = forms.FileField(label="Select an XLSX file", required=True)
    dbf_file = forms.FileField(label="Select a DBF file", required=True)