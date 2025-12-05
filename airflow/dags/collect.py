from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

CRM_CSV_PATH = "/opt/airflow/sample_files/crm.csv"
TELEMETRY_CSV_PATH = "/opt/airflow/sample_files/signals.csv"

DAG_ID = "daily_report"
OLAP_TABLE = "prosthesis_reports"

default_args = {
    "owner": "bionicpro",
    "retries": 0,
}

with DAG(
        dag_id=DAG_ID,
        description="ClickHouse витрина протезов",
        start_date=datetime(2025, 12, 1),
        schedule="0 2 * * *",
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        default_args=default_args,
) as dag:

    def extract_crm_fn(**context) -> None:
        if not os.path.exists(CRM_CSV_PATH):
            raise FileNotFoundError(f"CRM CSV не найден: {CRM_CSV_PATH}")
        print(f"CRM CSV найден: {CRM_CSV_PATH}")

        df = pd.read_csv(CRM_CSV_PATH)
        df = df.rename(columns={"id": "user_id"})[
            ["user_id", "name", "email", "age", "gender", "country"]
        ].rename(columns={"name": "crm_name", "age": "crm_age", "gender": "crm_gender"})

        context["ti"].xcom_push(key="crm_df", value=df.to_json(orient="records"))
        print(f"CRM: {len(df)} пользователей загружено")

    def extract_telemetry_fn(**context) -> None:
        if not os.path.exists(TELEMETRY_CSV_PATH):
            raise FileNotFoundError(f"Telemetry CSV не найден: {TELEMETRY_CSV_PATH}")

        df = pd.read_csv(TELEMETRY_CSV_PATH)
        df["date"] = context["ds"]
        df["date"] = pd.to_datetime(df["date"]).dt.date

        context["ti"].xcom_push(key="telemetry_df", value=df.to_json(orient="records"))
        print(f"Telemetry: {len(df)} сигналов загружено")

    def transform_join_fn(**context) -> None:
        ti = context["ti"]

        crm_json = ti.xcom_pull(task_ids="extract_crm", key="crm_df")
        telemetry_json = ti.xcom_pull(task_ids="extract_telemetry", key="telemetry_df")

        crm_df = pd.read_json(crm_json, orient="records")
        telemetry_df = pd.read_json(telemetry_json, orient="records")

        print(f"CRM records: {len(crm_df)}; Telemetry records: {len(telemetry_df)}")
        agg_telemetry = telemetry_df.groupby(
            ["user_id", "date", "prosthesis_type", "muscle_group"],
            as_index=False
        ).agg(
            signals_count=("signal_time", "count"),
            signal_frequency_avg=("signal_frequency", "mean"),
            signal_duration_avg=("signal_duration", "mean"),
            signal_amplitude_avg=("signal_amplitude", "mean"),
            signal_duration_total=("signal_duration", "sum")
        )
        print(f"Aggregated telemetry: {len(agg_telemetry)} records")

        mart_df = agg_telemetry.merge(
            crm_df,
            on="user_id",
            how="left"
        )

        available_cols = [col for col in [
            "user_id", "date", "crm_name", "crm_age", "crm_gender",
            "prosthesis_type", "muscle_group", "signals_count", "signal_frequency_avg",
            "signal_duration_avg", "signal_amplitude_avg", "signal_duration_total"
        ] if col in mart_df.columns]

        mart_df = mart_df[available_cols]

        ti.xcom_push(key="mart_df", value=mart_df.to_json(orient="records"))
        print(f"Витрина: {len(mart_df)} записей готовы для загрузки")

    def load_olap_fn(**context) -> None:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host="olap_db",
            database="default",
            username="default",
            password="",
            secure=False
        )

        ti = context["ti"]
        mart_json = ti.xcom_pull(task_ids="transform_join", key="mart_df")
        mart_df = pd.read_json(mart_json, orient="records")

        records = []
        for _, row in mart_df.iterrows():
            record = [
                str(row["user_id"]),
                str(row["date"].strftime("%Y-%m-%d")),
                str(row["crm_name"]),
                int(row["crm_age"]),
                str(row["crm_gender"]),
                str(row["prosthesis_type"]),
                str(row["muscle_group"]),
                int(row["signals_count"]),
                float(row["signal_frequency_avg"]),
                float(row["signal_duration_avg"]),
                float(row["signal_amplitude_avg"]),
                int(row["signal_duration_total"])
            ]
            records.append(record)

        client.insert(OLAP_TABLE, records,
                      column_names=['user_id', 'date', 'crm_name', 'crm_age', 'crm_gender',
                                    'prosthesis_type', 'muscle_group', 'signals_count', 'signal_frequency_avg',
                                    'signal_duration_avg', 'signal_amplitude_avg', 'signal_duration_total'])

        print(f"Загружено {len(records)} записей в {OLAP_TABLE}")
        client.close()

    extract_crm = PythonOperator(task_id="extract_crm", python_callable=extract_crm_fn)
    extract_telemetry = PythonOperator(task_id="extract_telemetry", python_callable=extract_telemetry_fn)
    transform_join = PythonOperator(task_id="transform_join", python_callable=transform_join_fn)
    load_olap = PythonOperator(task_id="load_olap", python_callable=load_olap_fn)

    [extract_crm, extract_telemetry] >> transform_join >> load_olap
