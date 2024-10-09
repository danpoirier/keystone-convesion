import xml.etree.ElementTree as ET
import sys

# Define the nodes we are interested in
# Prompt for the XSD schema, the Business Object Name and the Business Object description
path_to_your_list_of_paths = input("Enter the path to your file containing the possible list of paths: ")
list_of_nodes = input("Enter all the nodes your are looking for (comme separated): ")
'''
all: Will return the paths that contain only the nodes you are looking for. The path cannot contain any other node.
any: Will return the paths that contain at least one of the node you are looking for. The path can contain other nodes.
leaf: Will return the paths that contain any of the nodes you are looking for and the node is a leaf node. The path can contain other nodes.
'''
contains_all_or_any_or_leaf = input("Indicate if you want to find all the nodes or any of the nodes (all/any/leaf): ")

if contains_all_or_any_or_leaf not in ["any","all","leaf"]:
    print("You have entered an incorrect value. Must be 'all' or 'any'.")
    sys.exit(1)

nodes_for_file_name = '_'.join([s.strip() for s in list_of_nodes.split(',')])
output_file = f"path_extracts/{contains_all_or_any_or_leaf}_{nodes_for_file_name}_from_{path_to_your_list_of_paths.split('/')[-1]}.txt"

def log(message, data=None):
    with open(output_file, 'a') as f:
        print(message, file=f)
        if data is not None:
            print(data, file=f)
            
# Split the input list_of_nodes by ',' to get a list of nodes
nodes = list_of_nodes.split(',')

# Strip leading and trailing whitespace from each element
nodes = [element.strip() for element in nodes]

# Convert the list into a set
set_of_nodes = set(nodes)


# Open the file and read lines
with open(path_to_your_list_of_paths, 'r') as file:
    for line in file:
        # Strip whitespace and split the line into components based on '/'
        components = line.strip().split('/')
        
        # Check if the set of components contains only our nodes of interest exactly
        if contains_all_or_any_or_leaf == "all":
            if set(components) == set_of_nodes:
                print(line.strip())  # Print the line if it matches
                log(line.strip())
        elif contains_all_or_any_or_leaf == "any":
            # Check if any of the components is in our nodes of interest
            if any(node in components for node in set_of_nodes):
                print(line.strip()) # Print the line if it matches  
                log(line.strip())
        else:   
            # Check if any of the nodes is leaf node
            for node in set_of_nodes:
                if components[-1] == node:
                    print(line.strip())
                    log(line.strip())
            

