"""
Enterprise-grade dynamic DAG factory.
Auto-discovers all YAML data products under /products and registers them as DAGs.

✔ Provider-agnostic (no hard-coded imports)
✔ Dynamic operator resolution by name
✔ Safe fallback if an operator package is missing
✔ Compatible with any Airflow environment
"""

import os
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from utils import validate_yaml, safe_get
from operators.custom_ops import TASK_FUNCTIONS


# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schemas", "data_product_schema.json")
PRODUCTS_DIR = os.path.join(os.path.dirname(__file__), "products")


# ---------------------------------------------------------------------
# Utility: dynamically import Airflow operator by class name
# ---------------------------------------------------------------------
def resolve_operator_class(operator_name: str):
    """
    Try to import operator dynamically by class name.
    Example: 'BashOperator' → airflow.operators.bash.BashOperator
    """
    candidate_modules = [
        f"airflow.operators.{operator_name.lower()}",
        f"airflow.operators.{operator_name}",
        "airflow.operators.bash",
        "airflow.operators.python",
        "airflow.sensors.http",
    ]
    for mod_path in candidate_modules:
        try:
            module = importlib.import_module(mod_path)
            if hasattr(module, operator_name):
                return getattr(module, operator_name)
        except ImportError:
            continue
    return None


# ---------------------------------------------------------------------
# DAG builder
# ---------------------------------------------------------------------
def create_dag_from_yaml(config_path: str):
    cfg = validate_yaml(config_path, SCHEMA_PATH)

    default_args = {
        "owner": cfg["owner"],
        "retries": cfg["retries"],
        "retry_delay": timedelta(minutes=cfg["retry_delay_minutes"]),
    }

    dag = DAG(
        dag_id=cfg["name"],
        description=cfg["description"],
        start_date=datetime.fromisoformat(cfg["start_date"]),
        schedule=cfg["schedule"],
        default_args=default_args,
        catchup=False,
        tags=["data_product"],
    )

    with dag:
        tasks = {}

        for t in cfg["tasks"]:
            op_type = t["operator"]
            params = t.get("params", {})

            # --------------------------------------------------
            # PythonOperator → Use our registered Python @tasks
            # --------------------------------------------------
            if op_type == "PythonOperator":
                func_name = t.get("function")
                func = TASK_FUNCTIONS.get(func_name)
                if not func:
                    raise ValueError(f"Function '{func_name}' not found in TASK_FUNCTIONS.")
                task = func.override(task_id=t["id"])(**params)

            # --------------------------------------------------
            # Built-in Airflow operators (agnostic)
            # --------------------------------------------------
            else:
                OperatorClass = resolve_operator_class(op_type)
                if not OperatorClass:
                    raise ImportError(f"Operator '{op_type}' could not be found in Airflow modules.")

                if op_type == "BashOperator":
                    task = OperatorClass(task_id=t["id"], bash_command=t["bash_command"])

                elif op_type == "HttpSensor":
                    task = OperatorClass(
                        task_id=t["id"],
                        http_conn_id=t["http_conn_id"],
                        endpoint=t["endpoint"],
                        poke_interval=safe_get(t, "poke_interval", 60),
                        timeout=safe_get(t, "timeout", 600),
                    )

                else:
                    # Generic operator instantiation
                    task = OperatorClass(task_id=t["id"], **params)

            tasks[t["id"]] = task

        # --------------------------------------------------
        # Sequential dependency chain
        # --------------------------------------------------
        task_ids = [t["id"] for t in cfg["tasks"]]
        for i in range(len(task_ids) - 1):
            tasks[task_ids[i]] >> tasks[task_ids[i + 1]]

    return dag


# ---------------------------------------------------------------------
# Auto-discover all YAML data products
# ---------------------------------------------------------------------
for filename in os.listdir(PRODUCTS_DIR):
    if not filename.endswith(".yaml"):
        continue
    try:
        path = os.path.join(PRODUCTS_DIR, filename)
        dag_obj = create_dag_from_yaml(path)
        globals()[dag_obj.dag_id] = dag_obj
        print(f"✅ Registered data product DAG: {dag_obj.dag_id}")
    except Exception as e:
        print(f"❌ Failed to register {filename}: {e}")