import re

import formulas
from openpyxl.utils.cell import get_column_letter

from app.dbi_checks.tasks.checks_definitions.formulas_calc import CalcFormulas



class ShapeCalcFormulas(CalcFormulas):

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
            if re.search(r"'ANNO INPUT'", formula, re.IGNORECASE):
                # Replace the entire external reference with analysis_year
                formula = self.replace_with_year(formula)
                ast = parser.ast(formula)[1]
                compiled = ast.compile()

            # Regular expression to extract the column letters from the formula (e.g., X, L) and sheet names if exist.
            # For instance, it will catch the cell A2 but also the Fiumi!A2.
            columns_in_formula = re.findall(r'([A-Za-z_]+!)?([A-Z]{1,3})\d+', formula)
            # Remove '!' from sheet names if present
            columns_in_formula = [(match[0].rstrip('!'), match[1]) for match in columns_in_formula]
            columns_in_formula = self.exclude_range_columns(columns_in_formula, formula)
                
            # Check if the formula includes ranges e.g A:A or sheet with ranges. It catch patterns like: Fiumi_inreti!A:A
            # which is the way that Openpyxl interprets internally the actual patterns e.g $Fiumi_inreti.A:A 
            # ranges_in_formula = re.findall(r'(?:(?P<sheet>[A-Za-z_][\w]*)!)?(?P<range>\$?[A-Z]{1,3}:\$?[A-Z]{1,3})', formula)
            # this new pattern catches ranges with and without rows like B1:B2232
            ranges_in_formula = re.findall(r'(?:(?P<sheet>[A-Za-z_][\w]*)!)?(?P<range>\$?[A-Z]{1,3}:\$?[A-Z]{1,3}|\$?[A-Z]{1,3}\$?\d+:\$?[A-Z]{1,3}\$?\d+)', formula)   
            # Regex of row_based_ranges with catching the sheet
            row_based_ranges = re.findall(r'(?:(?P<sheet>[A-Za-z_][\w]*)!)?(?P<col1>\$?[A-Z]{1,3})(?P<row>\d+):(?P<col2>\$?[A-Z]{1,3})(?P=row)', formula)

            # Regex to capture the absolute rows like B$1 (They are used in the DB_prioritari file)
            abs_rows = re.findall(r'\b[A-Z]+\$\d+\b', formula)
            
            if ranges_in_formula:
                for match in ranges_in_formula:
                    sheet_name, col_ranges = match
                    # remove the $ from the col_ranges if exist
                    col_ranges = col_ranges.replace('$', '')
                    # Skip row-based ranges (B4:BB4, A10:C10)**

                    # Check if the reange is row_based
                    if re.match(r'^(?P<col1>[A-Z]{1,3})(?P<row>\d+):(?P<col2>[A-Z]{1,3})(?P=row)$', col_ranges):
                        continue  # Skip this range, go to next match
                    
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
                
            if row_based_ranges:

                columns_in_formula = self.exclude_cols_from_row_ranges(row_based_ranges, columns_in_formula)
                for sheet_name, col1, row, col2 in row_based_ranges:
                    if not sheet_name:
                        # the structure of row_based_ranges is: [(col1, row, col2)]
                        var = f"{col1}{row}:{col2}{row}"
                        variables[var] = self.calculate_row_based_range([col1, row, col2]) # e.g variables[B4:BB4]
                    else:
                        # the structure of row_based_ranges is: [(col1, row, col2)]
                        var = f"{sheet_name.upper()}!{col1}{row}:{col2}{row}"
                        variables[var] = self.calculate_row_based_range([col1, row, col2]) # e.g variables[sheet!B4:BB4]
            if abs_rows:
                abs_rows = self.exclude_abs_range_columns(abs_rows, ranges_in_formula)
                for i in abs_rows:
                    # the format of the abs_rows is something like "['J$1']"
                    i = i.replace("$", "")
                    variables[i] = self.sheet[f"{i}"].value
            
            # Iterate through each row for this column
            for row in self.sheet.iter_rows(min_row=self.start_row, max_row=self.end_row,
                                            min_col=col_idx, max_col=col_idx):
                cell = row[0]
                    
                # Retrieve the required values from the relevant cells
                for sheet_name, col in columns_in_formula:
                    ref_cell = f"{col}{cell.row}"
                    if not sheet_name:
                        value = self.sheet[ref_cell].value
                        # We set the text values in uppercase:
                        value = self.cell_value_parser(value)
                        # We set the key of the variables using the first row with data (4)
                        # because in that way have been compliled by Formulas. The result
                        # of course is updated with the new rows.
                        variables[f"{col}{self.start_row}"] = value if value is not None else 0  # Default to None if 0
                    else:
                        value = self.workbook[sheet_name][ref_cell].value
                        # We set the text values in uppercase:
                        value = self.cell_value_parser(value)
                        variables[f"{sheet_name.upper()}!{col}{self.start_row}"] = value if value is not None else 0  # Default to None if 0

                # Evaluate the formula with the given variables
                try:
                    calculated_result = compiled(**variables)
                except Exception as e:
                    calculated_result = f"Error: {e}"

                result = calculated_result.item() if hasattr(calculated_result, "item") else calculated_result
                # convert the float to int if the result is float
                #if isinstance(result, float):
                #    result = int(round(result))

                # print(f"Sheet: {self.sheet}, Row {cell.row} ({col_letter}{cell.row}): {result}")
                # Store the result in the target cell
                cell.value = result

        # return the caclulated_result as single value and not as a numpy array e.g Array("OK", dtype=object)
        return self.sheet
    
    def replace_with_year(self, formula):
        pattern = r"\$?'ANNO INPUT'!\$?[A-Z]+\$?\d+|\$?'ANNO INPUT'![A-Z]+\d+"
        return re.sub(
                   pattern,
                   f"{self.analysis_year}",
                   formula
                   )
    
    def exclude_cols_from_row_ranges(self, row_based_ranges, columns_in_formula):
        excluded_columns = set()

        # Process the row-based ranges
        for sheet_name, start_col, row, end_col in row_based_ranges:
            # Add only the start and end columns to excluded_columns
            excluded_columns.add(start_col)
            excluded_columns.add(end_col)

        # Extract just the column names from columns_in_formula (which are the second element of the tuples)
        # Iterate over columns_in_formula and check if the column (second element) is in excluded_columns
        valid_columns = [
            (sheet_name, col) for sheet_name, col in columns_in_formula  # Keep the tuple structure intact
            if col not in excluded_columns  # Exclude columns in the row-based range
        ]

        return valid_columns


