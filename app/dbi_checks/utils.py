import os
from app.settings import YEAR_VALUE
from openpyxl import load_workbook, Workbook

from django.utils import timezone

from app.settings import FOR_DOWNLOAD
from app.dbi_checks.models import ImportedSheet
from app.scheduler.utils import TaskStatus


class TaskType_CheckDbi:
    IMPORT_CheckDbi = "IMPORT_CheckDbi"
    PROCESS_CheckDbi = "PROCESS_CheckDbi"

def get_year(file_path):
        """
        This method get the year from the cell B8
        and creates the INPUT.xlsx file
        """
         
        try:
            # It's crucial to use the read_only argument because it's quite faster
            wb1 = load_workbook(file_path, read_only=True, data_only=True)
            # Get the required sheet and year value
            dati_sheet = wb1[YEAR_VALUE["sheet"]]
            year_value = dati_sheet.cell(row=YEAR_VALUE["row"], column=YEAR_VALUE["column"]).value

            # Create the INPUT.xlsx file which is requireed by DBA_A.xlsx
            wb2 = Workbook()
            ws = wb2.active
            ws.title = "Input anno"

            # Set a value for the cell A1
            ws['A1'] = year_value  # You can modify this value as needed
            wb2.save(os.path.join(FOR_DOWNLOAD, "INPUT.xlsx"))

        except Exception as e:
            print(f"Error processing files: {e}")
            return False

def import_sheet(task_instance, sheet, file_name, start_date, end_date, status):
    
    try:
        ImportedSheet.objects.create(
            task=task_instance,
            sheet_name=sheet.lower(),
            file_name=file_name,
            import_start_timestamp=start_date,
            import_end_timestamp=end_date,
            status=status
        )
    except Exception as e:
        print(sheet + ": " + str(e))
        
def get_last_data_row(sheet):
    last_row = 0
    for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=sheet.max_column):
        if any(cell.value is not None for cell in row):
            last_row = row[0].row
    return last_row
