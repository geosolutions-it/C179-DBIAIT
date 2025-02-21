import os
import shutil
import pathlib
import datetime

from django.utils import timezone
from django.db.models import ObjectDoesNotExist

from openpyxl.formula.translate import Translator
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.styles import numbers

from app.dbi_checks.models import Task_CheckDbi, ProcessState, TaskStatus, ProcessType
from app.dbi_checks.utils import YearHandler

import logging

logger = logging.getLogger(__name__)


class BaseCalc:

    def __init__(
        self, 
        orm_task: Task_CheckDbi,
        imported_file: str,
        seed: str,
        config: str,
        formulas_config: str,
        export_dir: pathlib.Path,
        file_year_required: bool = False,
        task_progress: int = 0
    ):
        """
        Initialization function of data export

        Parameters:
            export_dir: directory where the resulted xlsx files and log files should be stored
            orm_task: instance of the database Task reporting execution of this export task (default 100)
        """
        
        self.orm_task = orm_task
        self.imported_file = imported_file
        self.seed = seed
        self.config = config
        self.formulas_config = formulas_config
        self.export_dir = export_dir
        self.file_year_required = file_year_required
        self.task_progress = task_progress

        self.logger = None

        # make sure target location exists
        self.export_dir.parent.mkdir(parents=True, exist_ok=True)

    def run(self):

        logger.info(f"exported dir path: {self.export_dir}")

        # It's crucial to use the read_only argument because it's quite faster
        up_file = load_workbook(self.imported_file, read_only=True)
        seed_basename = os.path.basename(self.seed)
        seed_copy = shutil.copy(self.seed, f"{self.export_dir}/{seed_basename}")
        seed_wb = load_workbook(seed_copy, data_only=False)

        logger.info(f"{self.file_year_required}")

        if self.file_year_required:
            # Create the INPUT.xlsx file which it is needed by the DBI_A formulas
            success = YearHandler(self.imported_file, self.export_dir).set_year_to_file()
            if success:
                logger.info("Year set successfully and INPUT.xlsx created.")
            else:
                logger.info("Failed to set the year or create INPUT.xlsx.")

        # Iterate over the sheets to copy data
        for source_sheet, config in self.config.items():
            
            start_date = timezone.now()
            
            target_sheet = config["target"]
            
            # Convert column letters to numbers
            start_col = column_index_from_string(config["start_col"])
            # Start row of the seed file
            target_start_row = config["start_row"]
            # Start row of the uploaded file
            source_start_row = config["source_start_row"]

            if source_sheet in up_file.sheetnames and target_sheet in seed_wb.sheetnames:

                source = up_file[source_sheet]
                target = seed_wb[target_sheet]

                # Copy data based on the specified column range
                for row_idx, row in enumerate(source.iter_rows(
                        min_row=source_start_row, 
                        max_row=source.max_row, 
                        min_col=start_col, 
                        max_col=source.max_column), 
                    start=target_start_row):
                    for cell in row:
                        if cell.value is not None:
                            # Calculate the target row position based on the row index
                            target_cell = target.cell(row=row_idx, column=cell.column, value=cell.value)

                            # If the value is a date, set date format
                            if isinstance(cell.value, datetime.date):
                                target_cell.number_format = 'MM/DD/YYYY'
                                target_cell.value = cell.value
                            # If the value is a number, set General format to avoid date misinterpretation
                            elif isinstance(cell.value, (int, float)):
                                target_cell.number_format = 'General'
                                target_cell.value = cell.value
                            else:
                                # For any other type (e.g strings), set the appropriate format
                                target_cell.number_format = '@'
                                target_cell.value = cell.value
  
                logger.info(f"Copied data from sheet: {source_sheet} to {target_sheet}")
                
                # Call the import_process_state function with the process type COPY and SUCCESS status
                end_date = timezone.now()
                self.import_process_state(self.orm_task.id, 
                                          ProcessType.COPY.value,
                                          os.path.basename(self.imported_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.SUCCESS,
                                          sheet=source_sheet
                                          )

            else:
                end_date = timezone.now()
                self.import_process_state(self.orm_task.id, 
                                          ProcessType.COPY.value, 
                                          os.path.basename(self.imported_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.FAILED,
                                          sheet=source_sheet
                                          )
                logger.warning(f"Sheet {source_sheet} or {target_sheet} not found!")
            
        self.orm_task.progress += self.task_progress
        self.orm_task.save()

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

        # Write the year to the resulted file
        defined_year = YearHandler(self.imported_file).get_year()
        dati_sheet = seed_wb["DATI"]
        dati_sheet['B8'] = defined_year
        logger.info(f"The year {defined_year} was copied to th DATI sheet")
        
        self.orm_task.progress += self.task_progress
        self.orm_task.save()

        start_date = timezone.now()
        logger.info(f"The file is ready to be saved")
        # save logic
        seed_wb.save(seed_copy)
        del seed_wb

        # Clean up by deleting the import file
        os.remove(self.imported_file)
        logger.info(f"Final workbook save completed.")
        end_date = timezone.now()

        # save the save process in the ProcessState model
        self.import_process_state(self.orm_task.id, 
                                  ProcessType.SAVE.value, 
                                  os.path.basename(self.imported_file), 
                                  start_date, 
                                  end_date, 
                                  TaskStatus.SUCCESS
                                  )
        
        return True
    
    def import_process_state(self, task_id, process_type, file_name, start_date, end_date, status, sheet=""):
    
        try:
            task = Task_CheckDbi.objects.get(pk=task_id)

            ProcessState.objects.create(
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

    def get_last_data_row(self, sheet):
        last_row = 0
        for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=sheet.max_column):
           if any(cell.value is not None for cell in row):
                last_row = row[0].row
        return last_row
    