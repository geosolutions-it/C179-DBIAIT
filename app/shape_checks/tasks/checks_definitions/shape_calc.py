import os
import json
import pathlib
import pandas as pd
from dbfread import DBF
import gc

from openpyxl.formula.translate import Translator
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.styles import numbers

from django.utils import timezone
from django.db.models import ObjectDoesNotExist
from django.conf import settings

from app.dbi_checks.models import TaskStatus, ProcessType
from app.dbi_checks.utils import YearHandler
from app.dbi_checks.tasks.checks_definitions.base_calc import BaseCalc
from app.dbi_checks.tasks.checks_definitions.formulas_calc import CalcFormulas

from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState


import logging

logger = logging.getLogger(__name__)


class ShapeCalc(BaseCalc):
    
    def __init__(
        self, 
        orm_task, # Task_CheckShape
        imported_file: str,
        imported_dbf_file: str,
        sheet_for_dbf: str,
        seed: str,
        config: str,
        formulas_config: str,
        export_dir: pathlib.Path,
        file_year_required: bool = False,
        task_progress: int = 0,
        log_workbook = None
    ):
        super().__init__(orm_task,
                         imported_file,
                         seed,
                         config,
                         formulas_config,
                         export_dir,
                         file_year_required,
                         task_progress,
                         log_workbook,
                         )
        self.imported_dbf_file = imported_dbf_file
        self.sheet_for_dbf = sheet_for_dbf
    
    def drag_formulas(self, seed_wb):
        
        # Before dragging the formulas we have to copy
        # the DBF content to the specialized sheet
        dbf_copy_result = self.copy_from_dbf(seed_wb)

        if dbf_copy_result:
            super().drag_formulas(seed_wb)
    
    def copy_from_dbf(self, seed_wb):
        try:
            start_date = timezone.now()
            # Read DBF file into a DataFrame
            dbf_table = DBF(self.imported_dbf_file, load=True)
            df = pd.DataFrame(iter(dbf_table))

            with open(settings.DBF_TO_SHEET, "r") as file:
                dbf_to_sheet_config = json.load(file)

            self.orm_task.progress += self.task_progress
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error reading DBF file: {e}")
            return False
        
        try:
            # Get the sheet name where we will copy the DBF data
            sheet_mapping = dbf_to_sheet_config.get(self.sheet_for_dbf, {})
        
            start_row = sheet_mapping.get('start_row', 5)    
            start_col_letter = sheet_mapping.get('start_col', "A")
            start_col = column_index_from_string(start_col_letter)
            
            total_rows = len(df)
            total_cols = len(df.columns)

            # Load existing Excel sheet
            sheet_obj = seed_wb[self.sheet_for_dbf]

            logger.info(f"Start copying from DBF to Excel")
        
            # Use iter_rows() to access cells and assign values
            for row, df_row in zip(
                sheet_obj.iter_rows(min_row=start_row, max_row=start_row + total_rows - 1,
                                    min_col=start_col, max_col=start_col + total_cols - 1),
                df.itertuples(index=False, name=None)
            ):
                for cell, value in zip(row, df_row):
                    cell.value = value  # Assign value to each cell

            end_date = timezone.now()
            
            self.import_process_state(self.orm_task.id, 
                                          ProcessType.COPY.value,
                                          os.path.basename(self.imported_dbf_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.SUCCESS,
                                          sheet=self.sheet_for_dbf
                                          )

            logger.info(f"Copied {total_rows} rows from DBF to Excel")

            self.orm_task.progress += self.task_progress

            return True
        
        except (KeyError, Exception) as e:
            end_date = timezone.now()
            self.import_process_state(self.orm_task.id, 
                                          ProcessType.COPY.value,
                                          os.path.basename(self.imported_dbf_file), 
                                          start_date, 
                                          end_date, 
                                          TaskStatus.FAILED,
                                          sheet=self.sheet_for_dbf
                                          )
            logger.error(f"Error copying data to Excel: {e}")
            return False

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
        logger.info(f"The year {defined_year} was copied to the ANNO INPUT sheet")

    def log_file_manager(self, seed_wb):

        ## Configuration setup
        # prepare the logs workbook
        # Remove default sheet if it exists
        if "Sheet" in self.log_workbook.sheetnames and len(self.log_workbook.sheetnames) == 1:
            del self.log_workbook["Sheet"]
        
        sheet_name = "Logs"
        if sheet_name not in self.log_workbook.sheetnames:
            log_sheet = self.log_workbook.create_sheet(sheet_name)
            log_sheet.append(["File", 
                              "Foglio",
                              "Codice opera",
                              "Colonna check", 
                              "Tipo check", 
                              "Valore check errato col", 
                              "Valore errato col1", 
                              "Valore errato col2",
                              "Valore errato col3",
                              "Valore errato col4"

                              ])
        else:
            log_sheet = self.log_workbook[sheet_name]
        
        # Set the style in the log file
        self.set_logfile_style(log_sheet)
                
        # set the configs
        analysis_year = YearHandler(self.imported_file).get_year()

        # Get the seed_file name in order to retrieve it
        # from the log_mapping.json

        seed_filename = pathlib.Path(self.seed).stem
        seed_key = seed_filename.upper()
                
        with open(settings.LOG_MAPPING, "r") as file:
            log_mapping = json.load(file)

        # Open the json file with the verif shape formulas
        with open(settings.SHAPE_VERIF_FORMULAS, "r") as file:
            shape_verif_formulas = json.load(file)
                
        # Get the formulas and log mapping for the seed file
        verif_checks_config = log_mapping.get(seed_key, {})
        formulas_mapping = shape_verif_formulas.get(seed_key, {})
        
        ## Calculate the formulas of the checks for each sheet
        for sheet_name, f_location in formulas_mapping.items():

            pd_sheet = None
            start_date = timezone.now()
            
            if sheet_name in seed_wb.sheetnames:
                sheet = seed_wb[sheet_name]

                # Get column indexes
                start_col_index = column_index_from_string(f_location["start_col"])
                end_col_index = column_index_from_string(f_location["end_col"])
                start_row = f_location["start_row"]
                # Re-definition of the last row because the copied file is processed
                # without saving yet. We don't want to re-load it for time reasons
                if sheet_name in {"Controllo dati aggregati", "Controllo aggregati"}:
                    end_row = f_location.get("end_row", 3)
                else:
                    end_row = self.get_last_data_row(sheet)

                super().log_file_manager(seed_wb)
    