import os

from django.db.models import ObjectDoesNotExist
from openpyxl import load_workbook, Workbook

from app.settings import CHECKS_FTP_FOLDER, YEAR_VALUE
from app.dbi_checks.models import ImportedSheet, Task_CheckDbi

import logging

logger = logging.getLogger(__name__)


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
            wb2.save(os.path.join(CHECKS_FTP_FOLDER, "INPUT.xlsx"))

        except Exception as e:
            print(f"Error processing files: {e}")
            return False

def import_sheet(task_id, sheet, file_name, start_date, end_date, status):
    
    try:
        task = Task_CheckDbi.objects.get(pk=task_id)

        ImportedSheet.objects.create(
            task=task,
            sheet_name=sheet.lower(),
            file_name=file_name,
            import_start_timestamp=start_date,
            import_end_timestamp=end_date,
            status=status
        )

        logger.info(f"Successfully imported sheet: {sheet} for task ID {task_id}")
        return True

    except ObjectDoesNotExist:
        logger.error(f"Task with ID {task_id} was not found: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while importing sheet: {str(e)}")
        raise
        
def get_last_data_row(sheet):
    last_row = 0
    for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=sheet.max_column):
        if any(cell.value is not None for cell in row):
            last_row = row[0].row
    return last_row
