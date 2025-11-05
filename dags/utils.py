import yaml, json, jsonschema, os
from typing import Any
import os
import requests
from google.cloud import secretmanager

def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def validate_yaml(yaml_path: str, schema_path: str) -> dict:
    """Validate YAML config against schema. Raises ValidationError if invalid."""
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    with open(schema_path, "r") as f:
        schema = json.load(f)
    jsonschema.validate(instance=data, schema=schema)
    return data

def safe_get(d: dict, key: str, default: Any = None):
    return d[key] if key in d else default