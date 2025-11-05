from airflow.decorators import task
from google.cloud import firestore
import pandas as pd, os

# ======================================================
# 3️⃣ Store to Firestore
# ======================================================
@task
def store_to_firestore(collection: str, upstream_data: str = None, **context):
    """
    Store data to Firestore with batch writes and deterministic document IDs.
    
    Args:
        collection: Firestore collection name
        upstream_data: JSON string from upstream task (automatically injected)
        **context: Airflow context
    
    Returns:
        dict: Status metadata containing success status, documents_inserted count, and collection name
    """
    # ✅ Validate input
    if not upstream_data:
        raise ValueError("No upstream data provided. Ensure 'depends_on_output' is set in YAML.")
    
    # ✅ Parse data from any upstream task
    df = pd.read_json(upstream_data, orient='records')
    
    # ✅ Validate environment
    project_id = os.getenv("GCP_PROJECT")
    if not project_id:
        raise ValueError("GCP_PROJECT environment variable not set")
    
    db = firestore.Client(project=project_id)
    
    # ✅ Batch write - MUCH faster (500 operations per batch)
    batch = db.batch()
    for idx, row in df.iterrows():
        # ✅ Deterministic document IDs (idempotent, can update/track)
        doc_id = f"{row['location']}_{row['timestamp']}"
        doc_ref = db.collection(collection).document(doc_id)
        batch.set(doc_ref, row.to_dict())
    
    # ✅ Single commit for all writes
    batch.commit()
    
    print(f"✅ Inserted {len(df)} documents into Firestore collection '{collection}'.")
    
    # ✅ Return metadata for downstream tasks
    return {
        "status": "success", 
        "documents_inserted": len(df), 
        "collection": collection
    }