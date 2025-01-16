import os
import shutil
import dramatiq
from openpyxl import load_workbook
from openpyxl.utils import column_index_from_string
from app.settings import FOR_DOWNLOAD

import logging

logger = logging.getLogger(__name__)

# Tasks for check: consistenza delle opere 
@dramatiq.actor
def copy_to_dbi_files(file_path, seed, config, next_args=None):
    try:

        logger.info(f"Task started with file: {file_path}")

        # It's crucial to use the read_only argument because it's quite faster
        up_file = load_workbook(file_path, read_only=True)
        seed_basename = os.path.basename(seed)
        seed_copy = shutil.copy(seed, f"{FOR_DOWNLOAD}/{seed_basename}")
        seed_wb = load_workbook(seed_copy)

        # Iterate over the sheets to copy data
        for source_sheet, config in config.items():
            target_sheet = config["target"]
            
            # Convert column letters to numbers
            min_col = column_index_from_string(config["min_col"])
            min_row = config["min_row"]

            if source_sheet in up_file.sheetnames and target_sheet in seed_wb.sheetnames:

                source = up_file[source_sheet]
                target = seed_wb[target_sheet]

                chunk_size = 20

                # Copy data based on the specified column range
                # Usege of chunks to optimize large row ranges
                for start_row in range(min_row, source.max_row + 1, chunk_size):
                    end_row = min(start_row + chunk_size - 1, source.max_row)
                    for row in source.iter_rows(min_row=start_row, max_row=end_row, min_col=min_col, max_col=source.max_column):
                        for cell in row:
                            if cell.value is not None:
                                target.cell(row=cell.row, column=cell.column, value=cell.value)

                logger.info(f"Copied data from sheet: {source_sheet} to {target_sheet}")
            else:
                logger.warning(f"Sheet {source_sheet} or {target_sheet} not found!")

        # Save the changes to the file
        seed_wb.save(seed_copy)

        # Clean up by deleting the temporary files
        os.remove(file_path)

        # If next_args are provided, trigger the next task
        if next_args:
            copy_to_dbi_files.send(*next_args)
        
    except Exception as e:
        print(f"Error processing files in the background: {e}")