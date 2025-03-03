from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "email_on_failure": True,
}


with DAG(
    dag_id="initialize_tables",
    default_args=default_args,
    description="Create required database tables",
    schedule_interval=None,
    catchup=False,
    tags=["initialization", "setup"],
) as dag:
    # postgres-1
    create_orders_tables = PostgresOperator(
        task_id="create_orders_tables",
        postgres_conn_id="postgres_1",
        sql="""
        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY,
            customer_email VARCHAR(255) NOT NULL,
            order_date TIMESTAMP NOT NULL,
            amount NUMERIC(10, 2) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS orders_processed (
            order_id UUID PRIMARY KEY,
            processed BOOLEAN DEFAULT FALSE,
            processed_at TIMESTAMP DEFAULT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
        """
    )

    # postgres-2
    create_orders_eur_table = PostgresOperator(
        task_id="create_orders_eur_table",
        postgres_conn_id="postgres_2",
        sql="""
        CREATE TABLE IF NOT EXISTS orders_eur (
            order_id UUID PRIMARY KEY,
            customer_email VARCHAR(255) NOT NULL,
            order_date TIMESTAMP NOT NULL,
            original_amount NUMERIC(10, 2) NOT NULL,
            original_currency VARCHAR(3) NOT NULL,
            amount_eur NUMERIC(10, 2) NOT NULL,
            exchange_rate NUMERIC(16, 6) NOT NULL,
            rate_date TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transferred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_orders_eur_order_date ON orders_eur(order_date);
        """
    )

    create_orders_tables >> create_orders_eur_table