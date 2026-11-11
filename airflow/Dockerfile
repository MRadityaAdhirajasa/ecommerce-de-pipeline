FROM apache/airflow:3.4.0

COPY requirements.txt /requirements.txt
# apache-airflow di-pin supaya pip nggak diam-diam naik/turunin versi Airflow
# saat menyelesaikan dependensi provider.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# dbt sengaja dipasang di virtualenv terpisah, bukan dicampur ke environment
# Airflow. dbt-core dan Airflow sering berebut versi pustaka yang sama, dan
# konflik itu termasuk penyebab gagal paling umum di setup seperti ini.
# DAG memanggilnya lewat path absolut: /home/airflow/dbt-venv/bin/dbt
RUN python -m venv /home/airflow/dbt-venv && \
    /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-databricks