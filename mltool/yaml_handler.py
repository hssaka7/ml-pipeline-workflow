import yaml
import os
import logging

logger = logging.getLogger(__name__)

class YamlCRUD:
    def __init__(self, file_path='data.yaml'):
        self.file_path = file_path

    def create_data(self, data):
        try:
            with open(self.file_path, 'w') as file:
                yaml.dump(data, file, default_flow_style=False)
            print(f"Data successfully written to {self.file_path}")
        except Exception as e:
            raise RuntimeError(f"Error writing data: {e}")

    def read_data(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File {self.file_path} does not exist.")
        try:
            with open(self.file_path, 'r') as file:
                data = yaml.safe_load(file)
            return data if data is not None else {}
        except Exception as e:
            raise RuntimeError(f"Error reading data: {e}")

    def update_data(self, new_data):
        try:
            data = self.read_data()
            for key, value in new_data.items():
                if key in data:
                    print(f"Warning: Key '{key}' already exists. Updating the value.")
                data[key] = value
            with open(self.file_path, 'w') as file:
                yaml.dump(data, file, default_flow_style=False)
            print(f"Data successfully updated in {self.file_path}")
        except Exception as e:
            raise RuntimeError(f"Error updating data: {e}")

    def delete_data(self, keys):
        try:
            data = self.read_data()
            for key in keys:
                if key in data:
                    del data[key]
                else:
                    raise KeyError(f"Warning: Key '{key}' does not exist.")
            with open(self.file_path, 'w') as file:
                yaml.dump(data, file, default_flow_style=False)
            print(f"Data successfully deleted in {self.file_path}")
        except Exception as e:
            raise RuntimeError(f"Error deleting data: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize the YAML CRUD object
    yaml_crud = YamlCRUD('data.yaml')

    # Create initial data
    initial_data = {
        'name': 'John Doe',
        'age': 30,
        'city': 'New York'
    }
    yaml_crud.create_data(initial_data)

    # Read and print data
    try:
        print("Read Data:", yaml_crud.read_data())
    except Exception as e:
        print(f"Read Error: {e}")

    # Update data
    new_data = {
        'age': 31,  # This key already exists
        'city': 'San Francisco'  # This key already exists
    }
    try:
        yaml_crud.update_data(new_data)
        print("Updated Data:", yaml_crud.read_data())
    except Exception as e:
        print(f"Update Error: {e}")

    # Delete data
    keys_to_delete = ['city']
    try:
        yaml_crud.delete_data(keys_to_delete)
        print("Data After Deletion:", yaml_crud.read_data())
    except Exception as e:
        print(f"Delete Error: {e}")
