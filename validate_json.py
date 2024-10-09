import json

file_path = 'output/shippinglabel_3.0_to_shipment_7.7_conversion.json'

try:
    with open(file_path, 'r') as file:
        json.load(file)
    print("JSON is valid")
except json.JSONDecodeError as e:
    print(f"JSON format error: {e}")