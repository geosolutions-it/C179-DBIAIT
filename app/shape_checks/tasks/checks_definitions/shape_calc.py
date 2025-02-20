import os
import pathlib
import pandas
from dbfread import DBF

from openpyxl.formula.translate import Translator
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.styles import numbers

from django.utils import timezone
from django.db.models import ObjectDoesNotExist

from app.dbi_checks.models import TaskStatus, ProcessType
from app.dbi_checks.utils import YearHandler
from app.dbi_checks.tasks.checks_definitions.base_calc import BaseCalc

from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState


import logging

logger = logging.getLogger(__name__)


class ShapeCalc(BaseCalc):
    
    def __init__(
        self, 
        orm_task: Task_CheckShape,
        imported_file: str,
        imported_dbf_file: str,
        seed: str,
        config: str,
        formulas_config: str,
        export_dir: pathlib.Path,
        file_year_required: bool = False,
        task_progress: int = 0,
    ):
        super().__init__(orm_task,
                         imported_file,
                         seed,
                         config,
                         formulas_config,
                         export_dir,
                         file_year_required,
                         task_progress,
                         )
        self.imported_dbf_file = imported_dbf_file
    
    def drag_formulas(self, seed_wb):
        pass

        print("DBF file converted to output.csv")
    
    """
    def drag_formulas(self, seed_wb):
        # Iterate through each sheet to drag the formulas
        for sheet_name, f_location in self.formulas_config.items():

            start_date = timezone.now()
            
            if sheet_name in seed_wb.sheetnames:
                sheet = seed_wb[sheet_name]

                # Get column indexes
                start_col_index = column_index_from_string(f_location["start_col"])
                end_col_index = column_index_from_string(f_location["end_col"])
                start_row = f_location["start_row"]
                # Re-definition of the last row because the copied file is processed
                # without saving yet. We don't want to re-load it for time reasons
                last_row = self.get_last_data_row(sheet)

                # Copy formulas from row 4 to the rest of the rows
                for col_idx in range(start_col_index, end_col_index + 1):
                    column_letter = get_column_letter(col_idx)
                    # Get the formula in row 4
                    formula = sheet[f"{column_letter}{start_row}"].value
                    if isinstance(formula, str) and formula.startswith("="):
                        # Use the Translator to adjust the formula for each subsequent row
                        for row_idx in range(start_row + 1, last_row + 1):
                            translator = Translator(formula, f"{column_letter}{start_row}")
                            adjusted_formula = translator.translate_formula(f"{column_letter}{row_idx}")
                            # Set the adjusted formula in the target row
                            target_cell = sheet[f"{column_letter}{row_idx}"]
                            target_cell.value = adjusted_formula
                            # Explicit formatting
                            target_cell.number_format = numbers.FORMAT_GENERAL

                
                logger.info(f"The formulas were populated from sheet: {sheet_name}")
                # Call the import_process_state function with the process type CALCULATION and SUCCESS status
                end_date = timezone.now()
                self.import_process_state(self.orm_task.id, 
                                          ProcessType.CALCULATION.value,  
                                          os.path.basename(self.imported_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.SUCCESS,
                                          sheet=sheet_name
                                          )

            else:
                end_date = timezone.now()
                self.import_process_state(self.orm_task.id, 
                                          ProcessType.CALCULATION.value,
                                          os.path.basename(self.imported_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.FAILED,
                                          sheet=sheet_name
                                          )
                logger.warning(f"Something went wrong when filling out the formulas !")
    """
    
    
    def import_process_state(self, task_id, process_type, file_name, start_date, end_date, status, sheet=""):
    
        try:
            task = Task_CheckShape.objects.get(pk=task_id)

            ShapeCheckProcessState.objects.create(
                task=task,
                process_type = process_type,
                sheet_name=(sheet.lower() if sheet else ""),
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
    
    def year_to_file(self, seed_wb):
        # Write the year to the resulted file
        defined_year = YearHandler(self.imported_file).get_year()
        anno_sheet = seed_wb["ANNO INPUT"]
        anno_sheet['A1'] = defined_year
        logger.info(f"The year {defined_year} was copied to th DATI sheet")
    