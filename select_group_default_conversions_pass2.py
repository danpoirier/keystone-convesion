import datetime
import os
import re
import pandas as pd
import argparse

class GroupConversionSelector:
    # Constants for column names and values
    TYPE_COLUMN = 'TYPE'
    TYPE_COLUMN_VALUE_GROUP = 'GROUP'
    SOURCE_COLUMN = 'SOURCE_PATH'
    TARGET_COLUMN = 'TARGET_PATH'
    IS_SELECTED_COLUMN = 'IS_SELECTED'
    DO_NOT_MAP_COLUMN = 'DO NOT MAP'
    GROUP_IS_SELECTED_COLUMN = 'GROUP_IS_SELECTED'
    GROUP_VALIDATION_COLUMN = 'GROUP_VALIDATION'
    GROUP_NEEDS_PREDICATES = 'GROUP_NEEDS_PREDICATES'

    XPATH_SEPARATOR = '/'
    QUALIFIED_FIELD = '='

    def __init__(self, keystone_report, source, target, run_test=True, log=False):
        self.keystone_report = keystone_report
        self.source = source
        self.target = target
        self.run_test = run_test
        self.log_enabled = log

        # Set the display options for pandas
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        # Create the file name with the timestamp for logging
        current_time = datetime.datetime.now()
        self.timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        self.log_file_name = f"logfile_{self.timestamp}.log"
        os.makedirs('log', exist_ok=True)

        # Load the Keystone report data
        self.df = pd.read_excel(self.keystone_report)
        self.selected_field_df = self.df[(self.df[self.IS_SELECTED_COLUMN] == "YES") & 
                                         (self.df[self.DO_NOT_MAP_COLUMN] != "DO NOT MAP") & 
                                         (self.df[self.TYPE_COLUMN] != self.TYPE_COLUMN_VALUE_GROUP)]

        # Initialize columns for group selection and validation
        self.df[self.GROUP_IS_SELECTED_COLUMN] = ''
        if self.run_test:
            self.df[self.GROUP_VALIDATION_COLUMN] = ''
        self.df[self.GROUP_NEEDS_PREDICATES] = ''

    def log(self, message):
        # Log messages to a file if logging is enabled
        if self.log_enabled:
            with open('log/' + self.log_file_name, 'a') as f:
                print(message, file=f)
        print(message)

    def is_parent_of_a_selected_field(self, source_search_string, target_search_string):
        # Regular expression to match the pattern where search_string is the direct parent of a leaf
        source_pattern = re.compile(rf"^{re.escape(source_search_string)}(\[.*?\])?/[^/]+$")
        target_pattern = re.compile(rf"^{re.escape(target_search_string)}(\[.*?\])?/[^/]+$")

        # Check if any value in the target_column matches the pattern
        return self.selected_field_df.apply(lambda row: bool(source_pattern.match(row[self.SOURCE_COLUMN])) and 
                                            bool(target_pattern.match(row[self.TARGET_COLUMN])), axis=1).any()

    def extract_base_path(self, path):
        # Extract the base path from the given path
        last_xpath_separator_index = path.rfind(self.XPATH_SEPARATOR)
        if last_xpath_separator_index != -1:
            return path[:last_xpath_separator_index]
        return ''

    def check_errors(self, data):
        # Check for errors in the group selection process
        is_group_selected_column_has_selected_filter = data[self.GROUP_IS_SELECTED_COLUMN] == 'YES'
        selected_count = is_group_selected_column_has_selected_filter.sum()
        if selected_count == 0:
            data[self.GROUP_VALIDATION_COLUMN] = 'NO SELECTION'
        elif selected_count > 1:
            data[self.GROUP_VALIDATION_COLUMN] = 'MULTIPLE SELECTIONS'
        else:
            data[self.GROUP_VALIDATION_COLUMN] = 'OK'
        return data

    def select_unique_group(self, data):
        # Select a unique group based on various conditions
        type_value = data[self.TYPE_COLUMN].iloc[0]
        groupby_value = data[self.SOURCE_COLUMN].iloc[0]
        group_selected_count = 0

        # Check if the type is ambiguous GROUP
        if type_value == self.TYPE_COLUMN_VALUE_GROUP and len(data) > 1:
            for index, target_value in data[self.TARGET_COLUMN].items():
                # Validate that the group is a direct parent of a selected field
                if self.is_parent_of_a_selected_field(groupby_value, target_value):
                    data.loc[index, self.GROUP_IS_SELECTED_COLUMN] = 'YES'
                    group_selected_count += 1
                else:
                    data.loc[index, self.GROUP_IS_SELECTED_COLUMN] = 'NO'

            # If group is selected and there are multiple selections, check if the group needs predicates
            if group_selected_count > 1 and self.selected_field_df[self.SOURCE_COLUMN].str.startswith(groupby_value + '[').any():
                data[self.GROUP_NEEDS_PREDICATES] = 'YES'
            else: # in all otehr cases, the group does not need predicates. We will simplify the paths.
                data[self.GROUP_NEEDS_PREDICATES] = 'NO'
        else:
            data[self.GROUP_NEEDS_PREDICATES] = 'NO'
            data[self.GROUP_IS_SELECTED_COLUMN] = 'YES'
        return data

    def process(self):
        # Main processing function
        self.log("Processing...")

        # Apply the selection logic
        self.df = self.df.groupby(self.SOURCE_COLUMN, group_keys=False).apply(self.select_unique_group)

        # Group by 'SOURCE_PATH' and apply the check_errors function
        if self.run_test:
            self.df = self.df.groupby(self.SOURCE_COLUMN, group_keys=False).apply(self.check_errors)

        # Save the results to an Excel file
        output_file_name = f'conversion_analysis/select_{self.TARGET_COLUMN.lower()}_ambiguous_group_of_{self.source}_to_{self.target}_conversion_pass2.xlsx'
        self.df.to_excel(output_file_name, index=False)

        self.log("...")
        self.log(f"Processing complete. Results saved to '{output_file_name}'.")

if __name__ == "__main__":
    # Argument parser for command-line arguments
    parser = argparse.ArgumentParser(description='Select default conversion for a source given a Keystone report.')
    parser.add_argument('keystone_report', type=str, help='Path to your Keystone report. Need to use the second pass report.')
    parser.add_argument('source', type=str, help='Name and version of the canonical source, e.g., ShippingLabel 3.0')
    parser.add_argument('target', type=str, help='Name and version of the canonical target, e.g., Shipment 7.7')
    parser.add_argument('--run_test', action='store_true', default=True, help='Run the test at the end (default: False)')
    parser.add_argument('--log', action='store_true', default=False, help='Will log in log subdirectory. (default: False)')
    args = parser.parse_args()

    # Create an instance of the GroupConversionSelector class and run the process
    selector = GroupConversionSelector(args.keystone_report, args.source, args.target, args.run_test, args.log)
    selector.process()