import os
from tracardi.service.utils.loaders import load_json

current_script_path = os.path.abspath(__file__)
current_script_directory = os.path.dirname(current_script_path)

# Metadata form files

pc_destinations = load_json(os.path.join(current_script_directory, '../../../preconfig/destinations.json'))
pc_resources = load_json(os.path.join(current_script_directory, '../../../preconfig/resources.json'))