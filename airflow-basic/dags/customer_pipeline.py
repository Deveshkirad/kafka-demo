from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

INPUT_FILE = "/opt/airflow/data/customer.csv"
VALID_FILE = "/opt/airflow/output/valid_customers.csv"
OUTPUT_FILE = "/opt/airflow/output/processed_customers.csv"
NOTIFICATION_FILE = "/opt/airflow/output/notification.txt"


def extract_customers():

    df = pd.read_csv(INPUT_FILE)

    print("Customer data extracted")
    print(df.head())

    return df.to_dict("records")


def validate_customers(**context):

    customers = context["ti"].xcom_pull(
        task_ids="extract_customers"
    )

    valid_customers = []

    for customer in customers:

        customer_id = customer.get("id")
        name = customer.get("name")
        email = customer.get("email")

        if (
            pd.notna(customer_id)
            and pd.notna(name)
            and pd.notna(email)
            and str(name).strip()
            and str(email).strip()
        ):
            valid_customers.append(customer)

    valid_df = pd.DataFrame(valid_customers)

    valid_df.to_csv(VALID_FILE, index=False)

    print(f"Valid customers saved: {len(valid_customers)}")

    return VALID_FILE


def load_database(**context):

    valid_file = context["ti"].xcom_pull(
        task_ids="validate_customers"
    )

    df = pd.read_csv(valid_file)

    df["status"] = "Loaded"

    df.to_csv(OUTPUT_FILE, index=False)

    print("Data loaded successfully")

    return OUTPUT_FILE


def send_notification(**context):

    processed_file = context["ti"].xcom_pull(
        task_ids="load_database"
    )

    df = pd.read_csv(processed_file)

    with open(NOTIFICATION_FILE, "w") as file:

        for _, row in df.iterrows():

            file.write(
                f"Message sent to {row['name']} ({row['email']})\n"
            )

    print("Notification file created")

    return NOTIFICATION_FILE


with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
    )

    validate_task = PythonOperator(
        task_id="validate_customers",
        python_callable=validate_customers,
    )

    load_task = PythonOperator(
        task_id="load_database",
        python_callable=load_database,
    )

    notify_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    extract_task >> validate_task >> load_task >> notify_task