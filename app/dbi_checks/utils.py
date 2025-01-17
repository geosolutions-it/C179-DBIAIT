import os
from app.settings import YEAR_VALUE
from openpyxl import load_workbook, Workbook
from app.settings import FOR_DOWNLOAD

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