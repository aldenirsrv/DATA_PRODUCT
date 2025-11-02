from airflow.models import DagBag

def test_dags_load_cleanly():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"

def test_data_products_registered():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    dag_ids = dag_bag.dag_ids
    print(f"dag_ids: {dag_ids}")
    assert "weather_data_product" in dag_ids
    # assert "sales_data_product" in dag_ids