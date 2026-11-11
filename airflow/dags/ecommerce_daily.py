"""Pipeline harian: satu tanggal data e-commerce dari laptop sampai tabel gold.

Satu run memproses satu tanggal. Dataset Kaggle ini data historis Oktober 2019,
jadi kedatangan data harian disimulasikan lewat backfill: start_date 2019-10-01,
end_date 2019-10-31, jadwal harian.

Urutan: cek file lokal -> upload ke Volume -> Spark job (bronze+silver) ->
dbt build (gold + tests) -> catat ringkasan ke ops.pipeline_runs.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

LANDING = Path("/opt/airflow/data/landing")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"


# retries=0: file yang hilang nggak akan muncul sendiri dalam 5 menit.
# Lebih baik gagal cepat dengan pesan jelas daripada nunggu 10 menit percuma.
@task(retries=0)
def check_landing_file(run_date: str) -> str:
    path = LANDING / f"date={run_date}" / "events.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"File landing untuk {run_date} tidak ada di {path}. "
            "Jalankan scripts/split_by_day.py dulu, atau cek mount "
            "../data/landing di docker-compose.yaml."
        )
    print(f"{path} ada, {path.stat().st_size:,} byte")
    return str(path)


@task
def upload_to_volume(local_path: str, run_date: str) -> int:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    target = f"{os.environ['DATABRICKS_VOLUME_PATH']}/date={run_date}/events.csv"
    local_size = Path(local_path).stat().st_size

    # overwrite=True bikin task ini idempotent: run ulang tanggal yang sama
    # cuma menimpa file yang sama, nggak numpuk.
    with open(local_path, "rb") as fh:
        w.files.upload(target, fh, overwrite=True)

    remote_size = w.files.get_metadata(target).content_length
    if remote_size != local_size:
        raise ValueError(
            f"Ukuran tidak cocok setelah upload: lokal {local_size:,} byte, "
            f"volume {remote_size:,} byte. Upload kemungkinan kepotong."
        )
    print(f"upload ok -> {target} ({remote_size:,} byte)")
    return remote_size


@task
def log_run_summary(run_date: str) -> dict:
    """Catat jumlah baris per layer ke ops.pipeline_runs.

    Ini bahan bukti di README bahwa pipeline-nya benar-benar jalan, dan
    sekaligus cara tercepat melihat kalau ada layer yang bolong.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rsplit("/", 1)[-1]

    def sql(statement: str):
        r = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
        )
        state = r.status.state.value if r.status else "UNKNOWN"
        if state != "SUCCEEDED":
            raise RuntimeError(f"SQL gagal ({state}): {r.status.error}")
        return r

    sql(
        """
        CREATE TABLE IF NOT EXISTS ecommerce.ops.pipeline_runs (
            run_date         DATE,
            bronze_rows      BIGINT,
            silver_rows      BIGINT,
            gold_sales_rows  BIGINT,
            gold_funnel_rows BIGINT,
            logged_at        TIMESTAMP
        )
        """
    )
    # DELETE lalu INSERT, bukan INSERT saja: supaya run ulang tanggal yang
    # sama mengganti barisnya, bukan menambah baris kedua.
    sql(f"DELETE FROM ecommerce.ops.pipeline_runs WHERE run_date = '{run_date}'")
    sql(
        f"""
        INSERT INTO ecommerce.ops.pipeline_runs
        SELECT DATE('{run_date}'),
          (SELECT count(*) FROM ecommerce.bronze.events        WHERE event_date = '{run_date}'),
          (SELECT count(*) FROM ecommerce.silver.events        WHERE event_date = '{run_date}'),
          (SELECT count(*) FROM ecommerce.gold.fct_daily_sales  WHERE event_date = '{run_date}'),
          (SELECT count(*) FROM ecommerce.gold.fct_daily_funnel WHERE event_date = '{run_date}'),
          current_timestamp()
        """
    )
    row = sql(
        f"""
        SELECT bronze_rows, silver_rows, gold_sales_rows, gold_funnel_rows
        FROM ecommerce.ops.pipeline_runs WHERE run_date = '{run_date}'
        """
    ).result.data_array[0]

    summary = {
        "run_date": run_date,
        "bronze_rows": int(row[0]),
        "silver_rows": int(row[1]),
        "gold_sales_rows": int(row[2]),
        "gold_funnel_rows": int(row[3]),
    }
    for k, v in summary.items():
        print(f"  {k:<18} {v}")

    if summary["bronze_rows"] == 0 or summary["silver_rows"] == 0:
        raise ValueError(f"Layer kosong untuk {run_date}: {summary}")
    return summary


@dag(
    dag_id="ecommerce_daily",
    description="Batch harian e-commerce: landing -> bronze -> silver -> gold",
    start_date=pendulum.datetime(2019, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2019, 10, 31, tz="UTC"),
    schedule="@daily",
    catchup=True,
    # Free Edition cuma punya satu SQL warehouse kecil. Lebih dari satu run
    # bersamaan bikin task rebutan warehouse dan malah lebih lambat.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ecommerce", "batch"],
    doc_md=__doc__,
)
def ecommerce_daily():
    run_date = "{{ ds }}"

    local_path = check_landing_file(run_date=run_date)
    uploaded = upload_to_volume(local_path=local_path, run_date=run_date)

    run_spark_job = DatabricksRunNowOperator(
        task_id="run_spark_job",
        databricks_conn_id="databricks_default",
        job_id=os.environ["DATABRICKS_JOB_ID"],
        job_parameters={"run_date": run_date},
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            "--vars '{run_date: {{ ds }}}'"
        ),
    )

    uploaded >> run_spark_job >> dbt_build >> log_run_summary(run_date=run_date)


ecommerce_daily()
