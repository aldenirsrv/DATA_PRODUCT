def get_latest_xcom_df(context, key="latest_df"):
    """
    Dynamically pulls the most recent XCom with the given key
    from any upstream task.

    This makes tasks composable and decoupled from fixed task_ids.
    """
    ti = context["ti"]
    task = context["task"]
    upstream_task_ids = [t.task_id for t in task.upstream_list]

    if not upstream_task_ids:
        raise ValueError("No upstream tasks found to pull data from.")

    for upstream_task_id in reversed(upstream_task_ids):
        value = ti.xcom_pull(task_ids=upstream_task_id, key=key)
        if value:
            print(f"✅ Pulled XCom from upstream: {upstream_task_id}")
            return value

    raise ValueError(f"No XCom with key '{key}' found in upstream tasks.")