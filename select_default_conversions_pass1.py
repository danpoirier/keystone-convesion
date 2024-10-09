import datetime
import os
import re
import pandas as pd
import argparse

class ConversionSelector:
    TYPE_COLUMN = 'TYPE' 
    TYPE_COLUMN_VALUE_GROUP = 'GROUP'
    SOURCE_COLUMN = 'SOURCE_PATH'
    TARGET_COLUMN = 'TARGET_PATH'

    XPATH_SEPARATOR = '/'
    QUALIFIED_FIELD = '='

    IS_SELECTED_COLUMN = 'IS_SELECTED'
    VALIDATION_COLUMN = 'VALIDATION'
    HEADER = '/Header/'
    ORDER_LEVEL = '/OrderLevel/'
    ITEM_LEVEL = '/ItemLevel/'

    # Ambiguity columns suffices
    AMBIGUITY_NORM_VS_QUAL = '_AMBIGUITY_NORM_VS_QUAL'
    AMBIGUITY_WITH_ORDER_LEVEL_VS_ITEMLEVEL = '_AMBIGUITY_WITH_ORDER_LEVEL_VS_ITEMLEVEL'
    AMBIGUITY_WITH_ORDER_LEVEL_VS_HEADER = '_AMBIGUITY_WITH_ORDER_LEVEL_VS_HEADER'
    AMBIGUITY_WITH_ADDRESS_ALTNAME = '_AMBIGUITY_WITH_ADDRESS_ALTNAME'
    AMBIGUITY_WITH_REF_VS_PRODDESC = '_AMBIGUITY_WITH_REF_VS_PRODDESC'
    DO_NOT_MAP = 'DO NOT MAP'

    def __init__(self, keystone_report, source, target, run_test=True, log=False):
        self.keystone_report = keystone_report
        self.source = source
        self.target = target
        self.run_test = run_test
        self.log_enabled = log
        self.collected_target_not_to_map = []

        # Set the display options
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        # Create the file name with the timestamp
        current_time = datetime.datetime.now()
        self.timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        self.log_file_name = f"logfile_{self.timestamp}.log"
        os.makedirs('log', exist_ok=True)

        # Load the data
        self.df = pd.read_excel(self.keystone_report)

        # Initialize columns
        self.df[self.AMBIGUITY_WITH_ORDER_LEVEL_VS_ITEMLEVEL] = ''
        self.df[self.AMBIGUITY_WITH_ORDER_LEVEL_VS_HEADER] = ''
        self.df[self.AMBIGUITY_NORM_VS_QUAL] = ''
        self.df[self.AMBIGUITY_WITH_ADDRESS_ALTNAME] = ''
        self.df[self.AMBIGUITY_WITH_REF_VS_PRODDESC] = ''
        self.df[self.DO_NOT_MAP] = ''
        self.df[self.IS_SELECTED_COLUMN] = ''
        if self.run_test:
            self.df[self.VALIDATION_COLUMN] = ''

        # Load input files
        #
        # Load the rules for moving to header level. This file indicate the XPaths that should be moved to the header level when
        # there is an ambiguity between the order level and the header level.
        self.move_to_header_level_rules_df = pd.read_excel("./input/header_vs_order_level_rules_simplified.xlsx")
        self.move_to_header_level_xpath_column = 'Paths to Header'
        
        # Load the rules for not normalizing. This file indicate the XPaths that should not be normalized.
        self.do_not_normalized_rules_df = pd.read_excel("./input/do_not_normalized_rules.xlsx")
        self.do_not_normalized_column = 'Exception Groups'
        
        # Load the rules for not mapping qualifiers. This file indicate the XPaths that should not be mapped. 
        # This happen when we have ambiguity between the normalized and the qualified fields and we chose 
        # normalized fields over qualified fields. In this case. we might end up with some qualified fields that
        # should not be mapped.
        self.do_not_map_qualifiers_rules_df = pd.read_excel("./input/donotmap_qualifiers_rules.xlsx")
        self.do_not_map_column = 'DO NOT MAP QUALS'

    def log(self, message):
        if self.log_enabled:
            with open('log/' + self.log_file_name, 'a') as f:
                print(message, file=f)
        print(message)

    # Check if the base path is in the rows
    def base_path_in_rows(self, rows, base_path, separator):
        is_base_path_in_rows = rows.str.startswith(base_path)
        nodes = base_path.split(separator)
        while len(nodes) > 0 and not is_base_path_in_rows.any(): 
            nodes.pop()
            base_path = separator.join(nodes)
            is_base_path_in_rows = rows.str.startswith(base_path)
            nodes = base_path.split(separator)
        return is_base_path_in_rows.any()

    # Using the 'move_to_header_level_rules_df' input file, check if the given xpath should be moved to the header level.
    def is_header_path(self, xpath):
        if self.move_to_header_level_xpath_column not in self.move_to_header_level_rules_df.columns:
            raise ValueError(f"The column '{self.move_to_header_level_xpath_column}' does not exist in the Excel file './input/header_vs_order_level_rules_simplified.xlsx'.")

        self.move_to_header_level_rules_df[self.move_to_header_level_xpath_column] = self.move_to_header_level_rules_df[self.move_to_header_level_xpath_column].astype(str).str.strip()
        xpath = str(xpath).strip()
        for xpath_pattern in self.move_to_header_level_rules_df[self.move_to_header_level_xpath_column]:
            regex_pattern = re.compile(re.escape(xpath_pattern).replace(r'\*', '.*'))
            if regex_pattern.match(xpath):
                return True
        return False

    # Check if the given xpath is an exception
    def is_an_exception(self, xpath, exception_column, exception_df, exception_file_path):
        if exception_column not in exception_df.columns:
            raise ValueError(f"The column '{exception_column}' does not exist in the Excel file '{exception_file_path}'.")

        for index, value in exception_df[exception_column].items():
            escaped_value = re.escape(value)
            if re.search(escaped_value, xpath):
                return True
        return False

    # Extract the base path from the given path
    def extract_base_path(self, path):
        last_xpath_separator_index = path.rfind(self.XPATH_SEPARATOR)
        if last_xpath_separator_index != -1:
            return path[:last_xpath_separator_index]
        return ''

    # Transform an XPath string into a different format using regular expressions.
    # The idea is to use to mode the predicate to the leaf position of the XPath.
    # For example, the XPath "root/node1[attr='value']/node2" will be transformed 
    # into "root/node1[attr='value']/attr", where the predicate "attr" is moved.
    # It is used to indicate that the qualified field should not be mapped if we
    # have an ambiguity between the normalized and the qualified fields and we 
    # chose normalized fields over qualified fields.
    def transform_xpath(self, xpath):
        pattern = r"^(.*?)/([^/]+)\[([^=]+)='([^']+)'\]/([^/]+)$"
        replacement = r"\1/\2[\3='\4']/\3"
        transformed_xpath = re.sub(pattern, replacement, xpath)
        return transformed_xpath

    # If all bu one row are unselected, select the last row.
    def select_last_row_if_rest_unselected(self, data):
        
        # Boolean filter to check if the IS_SELECTED_COLUMN is NO
        is_selected_column_has_unselected_filter = data[self.IS_SELECTED_COLUMN] == 'NO'
        unselected_count = is_selected_column_has_unselected_filter.sum()
        
        # If all but one row are unselected, select the last row
        if len(data) - unselected_count == 1:
            data.loc[~is_selected_column_has_unselected_filter, self.IS_SELECTED_COLUMN] = 'YES'
            return data
        else:
            return data
        
    # Select a unique path based on various conditions
    # The order of the conditions is important as we will resolve the ambiguity in the order of the conditions.
    # All validations are in fact unselecting rows that should not be selected, so the a combination of conditions
    # can be at played. Thus the order of the conditions is important.
    def select_unique_path(self, data):
        type_value = data[self.TYPE_COLUMN].iloc[0]
        
        # We select all group by default. The second script (select_group_default_conversions_pass2.py) will refine the selection of groups.
        if type_value == self.TYPE_COLUMN_VALUE_GROUP:
            data[self.IS_SELECTED_COLUMN] = 'YES'

        elif len(data) > 1: # Amviguity
            # Extract the name of the source (group by) value
            groupby_value = data[self.SOURCE_COLUMN].iloc[0]

            # Extract the name of the parent node of the group by value
            groupby_value_base_path = self.extract_base_path(groupby_value)
            
            ############################################################################################################
            # Resolves structural ambiguities: Order Level vs Item Level vs Header Level
            ############################################################################################################
            # Validate if at least one of the target values is at the order level
            order_level_filter_bolean = data[self.TARGET_COLUMN].str.contains(self.ORDER_LEVEL)
            if order_level_filter_bolean.any():
                
                # Validate if at least one of the target values is at the item level
                order_level_with_item_level_filter_bolean = data[self.TARGET_COLUMN].str.contains(self.ITEM_LEVEL)
                
                # Validate if at least one of the target values is at the header level
                order_level_with_header_filter_bolean = data[self.TARGET_COLUMN].str.contains(self.HEADER)

                # If there are target values at the order level and item level, we have an ambiguity that 
                # we will always resolve by selecting the order level.
                # This loop will in fact unselect the the rows that will not be selected.
                if order_level_with_item_level_filter_bolean.any():
                    for index, target_value in data[self.TARGET_COLUMN].items():
                        if self.ITEM_LEVEL in target_value and self.ORDER_LEVEL not in target_value:
                            data[self.AMBIGUITY_WITH_ORDER_LEVEL_VS_ITEMLEVEL] = 'YES'
                            data.loc[index, [self.IS_SELECTED_COLUMN]] = 'NO'
                
                # Else, if there are target values at the order level and header level, we have an ambiguity that is 
                # resolved by the config file (./input/header_vs_order_level_rules_simplified.xlsx) where we log the 
                # decision we made on which path will be moved at the header level.
                elif order_level_with_header_filter_bolean.any():
                    data[self.AMBIGUITY_WITH_ORDER_LEVEL_VS_HEADER] = 'YES'
                    if self.is_header_path(groupby_value_base_path):
                        data.loc[order_level_filter_bolean, self.IS_SELECTED_COLUMN] = 'NO'
                    else:
                        data.loc[order_level_with_header_filter_bolean, self.IS_SELECTED_COLUMN] = 'NO'
            ############################################################################################################
            

            # If all but one row are unselected, select the last row.
            data = self.select_last_row_if_rest_unselected(data)
            

            ############################################################################################################
            # Resolves qualifield vs normalized field ambiguities
            ############################################################################################################
            # Boolean filter that check for empty IS_SELECTED_COLUMN
            is_selected_column_is_empty_filter = data[self.IS_SELECTED_COLUMN] == ''

            # Boolean filter that check for unselected rows that have QUALIFIED_FIELD target
            qualified = data[is_selected_column_is_empty_filter][self.TARGET_COLUMN].str.contains(self.QUALIFIED_FIELD)
            
            # If some of the rows are qualified and some are not, we have an ambiguity that we will resolve by selecting
            # the normalized fields if it is not part of the exceptions (./input/do_not_normalized_rules.xlsx).
            if not qualified.all() and qualified.any():
                data[self.AMBIGUITY_NORM_VS_QUAL] = 'YES'
                if self.is_an_exception(groupby_value_base_path, self.do_not_normalized_column, self.do_not_normalized_rules_df, "./input/do_not_normalized_rules.xlsx"):
                    # Unselect the rows that are qualified and part of the exceptions
                    data.loc[qualified.index[~qualified], self.IS_SELECTED_COLUMN] = 'NO'
                else:
                    # Loop all the unselected rows that are qualified and set the IS_SELECTED_COLUMN to NO
                    for index in qualified.index[qualified]:
                        # Unselect the rows that are qualified and not part of the exceptions
                        data.loc[index, self.IS_SELECTED_COLUMN] = 'NO'
                        
                        # We will now have to indicate that the qualified field should not be mapped.
                        # Collect the target values (qualifier field) that should not be mapped
                        target_column_value = data.loc[index, self.TARGET_COLUMN]
                        do_not_map_qual_field = self.transform_xpath(target_column_value)
                        self.collected_target_not_to_map.append(do_not_map_qual_field)
            ############################################################################################################
            

            # If all but one row are unselected, select the last row.
            data = self.select_last_row_if_rest_unselected(data)

            
            ############################################################################################################
            # Resolve AddressAlternateName1 vs AddressAlternateName2 ambiguities
            ############################################################################################################
            # Boolean filter that check for empty IS_SELECTED_COLUMN
            is_selected_column_is_empty_filter = data[self.IS_SELECTED_COLUMN] == ''

            # Boolean filter that check for unselected rows that have AddressAlternateName target
            address_alternate_name = data[is_selected_column_is_empty_filter][self.TARGET_COLUMN].str.contains("AddressAlternateName")
            
            # If some of the rows are AddressAlternateName and some are not, we have an ambiguity that we will resolve by selecting
            # the AddressAlternateName target fields with the same leaf node name as the source one.
            if address_alternate_name.any():
                groupby_leaf_value = groupby_value.split(self.XPATH_SEPARATOR)[-1]
                for index, value in data[is_selected_column_is_empty_filter][self.TARGET_COLUMN].items():
                    target_column_leaf_value = value.split(self.XPATH_SEPARATOR)[-1]
                    # Unselect the rows that have different leaf node name
                    if target_column_leaf_value != groupby_leaf_value:
                        data[self.AMBIGUITY_WITH_ADDRESS_ALTNAME] = 'YES'
                        data.loc[index, self.IS_SELECTED_COLUMN] = 'NO'


            # If all but one row are unselected, select the last row.
            data = self.select_last_row_if_rest_unselected(data)

            ############################################################################################################
            # Resolve ItemLevel ProductOrItemDescription vs ItemLevel References ambiguities
            ############################################################################################################
            # Boolean filter that check for empty IS_SELECTED_COLUMN
            is_selected_column_is_empty_filter = data[self.IS_SELECTED_COLUMN] == ''

            # Boolean filter that check for unselected rows that have ItemLevel/ProductOrItemDescription target
            target_is_proddesc = data[is_selected_column_is_empty_filter][self.TARGET_COLUMN].str.contains('ItemLevel/ProductOrItemDescription')
            
            # Boolean filter that check for unselected rows that have ItemLevel/References target
            target_is_reference = data[is_selected_column_is_empty_filter][self.TARGET_COLUMN].str.contains('ItemLevel/References')
            
            # If some of the rows are ItemLevel/ProductOrItemDescription and some are ItemLevel/References, we have an ambiguity that we will 
            # resolve by always selecting the same group of the source.
            if target_is_proddesc.any() and target_is_reference.any():
                data[self.AMBIGUITY_WITH_REF_VS_PRODDESC] = 'YES'
                groupby_value_str = str(groupby_value)
                
                # Unselect the rows that have ItemLevel/References if the source is using ProductOrItemDescription
                if "ProductOrItemDescription" in groupby_value_str:
                    data.loc[target_is_reference.index[target_is_reference], self.IS_SELECTED_COLUMN] = 'NO'

                # Unselect the rows that have ItemLevel/ProductOrItemDescription if the source is using References
                else:
                    data.loc[target_is_proddesc.index[target_is_proddesc], self.IS_SELECTED_COLUMN] = 'NO'

            # If all but one row are unselected, select the last row.
            data = self.select_last_row_if_rest_unselected(data)
            
        # If there is only one row, we select it by default
        else:
            data[self.IS_SELECTED_COLUMN] = 'YES'
            
        return data

    # Validate that all fields have been selected unambiguously 
    def check_errors(self, data):
        is_selected_column_has_selected_filter = data[self.IS_SELECTED_COLUMN] == 'YES'
        selected_count = is_selected_column_has_selected_filter.sum()
        if selected_count == 0:
            data[self.VALIDATION_COLUMN] = 'NO SELECTION'
        elif selected_count > 1:
            data[self.VALIDATION_COLUMN] = 'MULTIPLE SELECTIONS'
        else:
            data[self.VALIDATION_COLUMN] = 'OK'
        return data

    # Main processing function
    def process(self):
        self.log(f"Analyzing the conversion ambiguities on the TARGET side of {self.source} to {self.target} conversion.")
        self.log("...")
        self.log("Processing...")

        # Group by 'SOURCE_PATH' and apply the select_unique_path function to resolve the field's ambiguities
        self.df = self.df.groupby(self.SOURCE_COLUMN, group_keys=False).apply(self.select_unique_path)
        
        # Group by 'SOURCE_PATH' and apply the check_errors function if requested
        if self.run_test:
            self.df = self.df.groupby(self.SOURCE_COLUMN, group_keys=False).apply(self.check_errors)

        # Indicates all the rows that should not be mapped (captured dusring the ambiguity resolution)
        for value in self.collected_target_not_to_map:
            row_index = self.df.loc[self.df[self.TARGET_COLUMN] == value].index
            self.df.loc[row_index, self.DO_NOT_MAP] = 'DO NOT MAP'

        # Save the results to an Excel file
        output_file_name = f'conversion_analysis/select_{self.TARGET_COLUMN.lower()}_field_ambiguities_of_{self.source}_to_{self.target}_conversion_pass1.xlsx'
        self.df.to_excel(output_file_name, index=False)

        self.log("...")
        self.log(f"Processing complete. Results saved to '{output_file_name}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Select default conversion for a source given a Keystone report.')
    parser.add_argument('keystone_report', type=str, help='Path to your Keystone report')
    parser.add_argument('source', type=str, help='Name and version of the canonical source, e.g., ShippingLabel 3.0')
    parser.add_argument('target', type=str, help='Name and version of the canonical target, e.g., Shipment 7.7')
    parser.add_argument('--run_test', action='store_true', default=True, help='Run the test at the end (default: False)')
    parser.add_argument('--log', action='store_true', default=False, help='Will log in log subdirectory. (default: False)')
    args = parser.parse_args()

    selector = ConversionSelector(args.keystone_report, args.source, args.target, args.run_test, args.log)
    selector.process()