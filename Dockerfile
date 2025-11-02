FROM apache/airflow:2.10.0-python3.11

USER root
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

# ✅ Add PYTHONPATH
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY operators/ /opt/airflow/operators/
COPY tests/ /opt/airflow/tests/

RUN python -m compileall dags operators >/dev/null 2>&1 || true

ENTRYPOINT ["/entrypoint"]
CMD ["bash"]