import xml.etree.ElementTree as ET

# Define the nodes we are interested in
# Prompt for the XSD schema, the Business Object Name and the Business Object description
path_to_your_list_of_paths = input("Enter the path to your file containing the possible list of qualifiers: ")
qual_name = input("Enter the name of the qualifier you are extracting (for then name of the output file): ")

# Specify the input and output file names
output_file = 'qualifiers_extracts/' + qual_name + '.txt'

def transform_string(s):
    # Uppercase the string
    s = s.upper()
    # Replace spaces with underscores
    s = s.replace(' ', '_')
    # Remove others chars
    s = s.replace('(', '').replace(')', '')
    s = s.replace('[', '').replace(']', '')
    s = s.replace(',', '')
    s = s.replace('_-_', '_')
    return s

# Define a function to recursively extract documentation values from an XML element
def extract_documentation_values(element, file):
    # Check if the current element is a 'documentation' element
    if element.tag.endswith('documentation'):
        value = element.text
        if value:
            value = transform_string(value)
            file.write(f"- {value.strip()}\n")
    # Otherwise, recursively check its children
    # else:
    for child in element:
        extract_documentation_values(child, file)

# Define a function to read an XML file and extract documentation values
def process_xml_file(input_file, output_file):
    # Parse the XML file
    try:
        tree = ET.parse(input_file)
        root = tree.getroot()
    except ET.ParseError as e:
        return f"Error parsing XML: {e}"
    except FileNotFoundError:
        return f"Input file not found: {input_file}"

    # Open the output file
    with open(output_file, 'w') as file:
        # Recursively extract documentation values from the root element
        extract_documentation_values(root, file)


# Call the function with the input and output file names
process_xml_file(path_to_your_list_of_paths, output_file)
