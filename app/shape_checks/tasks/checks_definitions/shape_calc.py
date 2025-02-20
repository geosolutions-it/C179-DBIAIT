from django.db.models import ObjectDoesNotExist

from app.dbi_checks.models import Task_CheckDbi, ProcessState, TaskStatus, ProcessType
from app.dbi_checks.utils import YearHandler
from app.dbi_checks.tasks.checks_definitions.base_calc import BaseCalc

from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState


import logging

logger = logging.getLogger(__name__)


class ShapeCalc(BaseCalc):
    
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
    