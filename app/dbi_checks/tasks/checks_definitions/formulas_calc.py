import re
import time
import json

import openpyxl.workbook

import formulas
import openpyxl
from openpyxl.utils.cell import column_index_from_string, get_column_letter



class CalcFormulas:
    """
    This class calculates different kinds of formulas
    using the Formulas module installed from here:
    https://github.com/geosolutions-it/formulas/tree/v1.2.8_fixed

    The formulas have to be parsed before being calculated.
    There are four kinds of groups:
    -- Simple formulas: They contain only built-in functions
        like COUNTIF and cells' references
    -- Formulas with ranges: They include also ranges like A:A
        which has to be calculated separately.
    -- Formulas with ranges from other sheet: They are similar
        with the formulas with ranges but in this case, the formulas
        take the data from other sheet.
    -- Formulas which retrieve the year value from the INPUT.xlsx file
        In our case we will just replace this file with the actual value
        because we have retrieve it in a previous step of the process.
    -- Formulas which retrieve data from other xlsx file. These formulas
        are included only in the DBI-A-1 file.
    """

    def __init__(self,
                 workbook: openpyxl.workbook.Workbook,
                 sheet: openpyxl.worksheet.worksheet.Worksheet,
                 start_row: int,
                 end_row: int,
                 start_col: int,
                 end_col:int,
                 analysis_year: int,
                 external_wb_path: str = None,
                 ):
        self.workbook = workbook
        self.sheet = sheet
        self.start_row = start_row
        self.end_row = end_row
        self.start_col = start_col
        self.end_col = end_col
        self.analysis_year = analysis_year
        self.external_wb_path = external_wb_path

    def main_calc(self):
        
        # Iterate through the columns in the specified range
        for col_idx in range(self.start_col, self.end_col + 1):

            col_letter = get_column_letter(col_idx)

            # Dictionary to set the variables of the formula
            variables = {}

            # Retrieve the formula from the first cell of the column
            formula_cell = self.sheet[f"{col_letter}{self.start_row}"]
                
            if formula_cell.data_type != 'f':  # Skip if not a formula
                continue
                
            formula = formula_cell.value
            print(f"Processing formula in {col_letter}{self.start_row}: {formula}")
                
            # Parse and compile the formula outside the loop for better performance
            parser = formulas.Parser()
            ast = parser.ast(formula)[1]
            compiled = ast.compile()

            # Check if the formulas include the INPUT.xlsx requirement
            if re.search(r"\[\d+\]Input anno", formula, re.IGNORECASE):
                # Replace the entire external reference with analysis_year
                formula = self.replace_with_year(formula)
                ast = parser.ast(formula)[1]
                compiled = ast.compile()

            # Regular expression to extract the column letters from the formula (e.g., X, L) and sheet names if exist.
            # For instance, it will catch the cell A2 but also the Fiumi!A2.
            columns_in_formula = re.findall(r'([A-Za-z_]+!)?([A-Z]{1,3})\d+', formula)
            # Remove '!' from sheet names if present
            columns_in_formula = [(match[0].rstrip('!'), match[1]) for match in columns_in_formula]
                
            # Check if the formula includes ranges e.g A:A or sheet with ranges. It catch patterns like: Fiumi_inreti!A:A
            # which is the way that Openpyxl interprets internally the actual patterns e.g $Fiumi_inreti.A:A 
            # ranges_in_formula = re.findall(r'(?:(?P<sheet>[A-Za-z_][\w]*)!)?(?P<range>\$?[A-Z]{1,3}:\$?[A-Z]{1,3})', formula)
            # this new pattern catches ranges with and without rows like B1:B2232
            ranges_in_formula = re.findall(r'(?:(?P<sheet>[A-Za-z_][\w]*)!)?(?P<range>\$?[A-Z]{1,3}:\$?[A-Z]{1,3}|\$?[A-Z]{1,3}\$?\d+:\$?[A-Z]{1,3}\$?\d+)', formula)   
            if ranges_in_formula:
                for match in ranges_in_formula:
                    sheet_name, col_ranges = match
                    # remove the $ from the col_ranges if exist
                    col_ranges = col_ranges.replace('$', '')
                    if sheet_name:
                        # check if the sheet is from another file, In this case the interpreted formula will be like: [2]FIUMI!B:B
                        second_file_check = re.search(r'\[(\d+)\]', formula)
                        if second_file_check:
                            linked_number = second_file_check.group(1)
                            external_wb = self.external_link_parser(linked_number)
                           
                            col_ranges = re.sub(r'(\d+)(?=\$?\d*$)', '', col_ranges)  # Remove row number after the first column
                            formatted_variable = f"[{linked_number}]{sheet_name.upper()}!{col_ranges}"
                            variables[formatted_variable] = self.calculate_range(formula, col_ranges, sheet_name, external_wb)
                        else:
                            formatted_variable = f"{sheet_name.upper()}!{col_ranges}"  # e.g FIUMI_INRETI!A:A
                            variables[formatted_variable] = self.calculate_range(formula, col_ranges, sheet_name)
                    else:
                        variables[col_ranges] = self.calculate_range(formula, col_ranges)
                
            # Iterate through each row for this column
            for row in self.sheet.iter_rows(min_row=self.start_row, max_row=self.end_row,
                                            min_col=col_idx, max_col=col_idx):
                cell = row[0]
                    
                # Retrieve the required values from the relevant cells
                for sheet_name, col in columns_in_formula:
                    ref_cell = f"{col}{cell.row}"
                    if not sheet_name:
                        value = self.sheet[ref_cell].value
                        # We set the key of the variables using the first row with data (4)
                        # because in that way have been compliled by Formulas. The result
                        # of course is updated with the new rows.
                        variables[f"{col}{self.start_row}"] = value if value is not None else 0  # Default to 0 if empty
                    else:
                        value = self.workbook[sheet_name][ref_cell].value
                        variables[f"{sheet_name.upper()}!{col}{self.start_row}"] = value if value is not None else 0  # Default to 0 if empty

                # Evaluate the formula with the given variables
                try:
                    calculated_result = compiled(**variables)
                except Exception as e:
                    calculated_result = f"Error: {e}"
                    
                print(f"Sheet: {self.sheet}, Row {cell.row} ({col_letter}{cell.row}): {calculated_result}")
                # Store the result in the target cell
                cell.value = calculated_result.item() if hasattr(calculated_result, "item") else calculated_result

        # return the caclulated_result as single value and not as a numpy array e.g Array("OK", dtype=object)
        return self.sheet

    def calculate_range(self, formula: str,  col_range: str, sheet_name: str = None, external_wb: openpyxl.workbook.Workbook = None) -> list:
        """
        Calculate the values in a given Excel range either from the initial sheet
        or from another one
        """
        vlookup_pattern = r'\bVLOOKUP\('

        if external_wb:
            current_sheet = external_wb[sheet_name]
        else:
            # Use the specified sheet or default to the current one
            current_sheet = self.sheet if not sheet_name else self.workbook[sheet_name]
        
        # Remove dollar signs for absolute references
        col_range = col_range.replace('$', '')

        # TODO See if we indeed need this function
        col_range = self.clean_range(col_range)
        
        # Split start and end of the range
        start_col, end_col = col_range.split(':')
        
        # Convert column letters to column numbers
        start_num = column_index_from_string(start_col)
        end_num = column_index_from_string(end_col)
        
        # Create an empty list to hold the values
        values = []
        
        # Iterate through the range of columns (same row range for each column)
        for col_num in range(start_num, end_num + 1):
            col_letter = get_column_letter(col_num)
            
            # Get values in the specified column
            column_values = [current_sheet[f'{col_letter}{row}'].value for row in range(self.start_row, self.get_last_data_row(current_sheet) + 1)]
            
            values.append(column_values)

        # Check if VLOOKUP exists in the formula first
        if re.search(vlookup_pattern, formula, re.IGNORECASE):
            # We need to build a 2D list in order to be interpretable by VLOOKUP
            # e.g [[search_col_value1, return_col_value1], 
            #      [search_col_value2, return_col_value2]
            #      ...]
            values = [list(tup) for tup in zip(*values)]
        
        return values
    
    def clean_range(self, col_range: str) -> str:
        """
        Remove row numbers from a given range and return only the column part.
        Example: "$B$1:$B$10" -> "$B:$B"
        """

        if ':' in col_range:
            start_cell, end_cell = col_range.split(':')
            
            # Use regex to remove row numbers, keeping only column letters
            start_col = re.sub(r'\d+', '', start_cell)
            end_col = re.sub(r'\d+', '', end_cell)
            
            # If the start and end columns are the same, return just one column reference
            if start_col == end_col:
                return f"{start_col}:{start_col}"
            else:
                return f"{start_col}:{end_col}"
        
        # If it's a single cell or range without row numbers, just return it as-is
        return re.sub(r'\d+', '', col_range)

  
    def replace_with_year(self, formula):
        return re.sub(
                   r"'?\[\d+\]Input anno'!\$[A-Z]+\$[0-9]+",
                   f"{self.analysis_year}",
                   formula
                   )
    
    def get_last_data_row(self, sheet):
        last_row = 0
        for row in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=sheet.max_column):
           if any(cell.value is not None for cell in row):
                last_row = row[0].row
        return last_row
    
    def external_link_parser(self, linked_number):
        
        # insert here the possible external files
        target_file = "DBI_A.xlsx"

        # Step 1: Extract all external links and map them to indices
        external_links_map = {}  # Stores {index_number: file_path}
        if self.workbook._external_links:
            for idx, link in enumerate(self.workbook._external_links, start=1):  # Excel indexes start at 1
                external_links_map[idx] = link.file_link.Target  # Map [1], [2], etc. to their file path

        # Step 2: Check if a linked number e.g [2] exists and matches the target file
        if int(linked_number) in external_links_map:
            if target_file in external_links_map[int(linked_number)]:
                print(f"{linked_number} correctly links to {target_file}")
                # load the external workbook
                external_wb = openpyxl.load_workbook(f'{self.external_wb_path}/DBI_A.xlsx', data_only=True)

                return external_wb
        else:
            print("[2] does not exist in the external links.")
            return False


