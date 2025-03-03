import os
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from airflow import settings
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "email_on_failure": True,
}

@task
def setup_postgres_connections() -> list[str]:
    """
    Create PostgreSQL connections in Airflow if they don't exist.
    
    Returns:
        List of connections that were created or already existed
    """
    
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    DB_NAME = os.environ.get("DB_NAME")

    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        raise ValueError("Required environment variables are not set")
    created_connections = []
    
    connections_to_create = [
        # postgres-1
        Connection(
            conn_id="postgres_1",
            conn_type="postgres",
            host="postgres-1",
            schema=DB_NAME,
            login=DB_USER,
            password=DB_PASSWORD,
            port=5432,
            description="Connection to postgres-1 database for order generation"
        ),
        # postgres-2
        Connection(
            conn_id="postgres_2",
            conn_type="postgres",
            host="postgres-2",
            schema=DB_NAME,
            login=DB_USER,
            password=DB_PASSWORD,
            port=5432,
            description="Connection to postgres-2 database for order processing"
        )
    ]

    # Create connections in the Airflow metadata database
    with settings.Session() as session:
        for conn in connections_to_create:
            conn_id = conn.conn_id
            existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            
            if not existing_conn:
                session.add(conn)
                created_connections.append(f"{conn_id} (created)")
            else:
                created_connections.append(f"{conn_id} (already exists)")
        
        session.commit()
        
        return created_connections

@task
def verify_connections() -> dict[str, dict[str, str]]:
    """
    Verify that the PostgreSQL connections are working.
    
    Returns:
        Results of connection tests
    """
    from airflow.hooks.base import BaseHook
    
    results = {}
    
    # Test connections
    for conn_id in ["postgres_1", "postgres_2"]:
        try:
            conn = BaseHook.get_connection(conn_id)
            hook = conn.get_hook()
            hook.test_connection()
            results[conn_id] = {"status": "success", "message": "Connection successful"}
        except Exception as e:
            results[conn_id] = {"status": "failure", "message": str(e)}
    
    return results

# Create DAG using context manager
with DAG(
    dag_id="setup_connections",
    default_args=default_args,
    description="Setup Airflow database connections",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["setup", "initialization"],
) as dag:
    connections = setup_postgres_connections()
    verify_connections()