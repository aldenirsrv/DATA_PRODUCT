from dags.utils import validate_yaml
import os

def test_all_yaml_validates():
    schema = "dags/schemas/data_product_schema.json"
    products_dir = "dags/products"
    for file in os.listdir(products_dir):
        if file.endswith(".yaml"):
            validate_yaml(os.path.join(products_dir, file), schema)