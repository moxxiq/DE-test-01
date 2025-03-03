import logging
import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from faker import Faker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.environ.get("OPENEXCHANGERATES_API_KEY")
API_BASE_URL = "https://openexchangerates.org/api"
NUM_RECORDS = int(os.environ.get("BATCH_SIZE"))
ORDER_DATE_MIN_DAYS_AGO = int(os.environ.get("ORDER_DATE_MIN_DAYS_AGO"))
BASE_CURRENCY = os.environ.get("BASE_CURRENCY")

FALLBACK_RATES = {
    "AED": 3.8258, "AFN": 76.5274, "ALL": 99.1213, "AMD": 408.5892, "ANG": 1.8736,
    "AOA": 952.4454, "ARS": 1104.4226, "AUD": 1.6753, "AWG": 1.8750, "AZN": 1.7709,
    "BAM": 1.9553, "BBD": 2.0834, "BDT": 126.3052, "BGN": 1.9628, "BHD": 0.3918,
    "BIF": 3079.2016, "BMD": 1.0417, "BND": 1.4018, "BOB": 7.1831, "BRL": 6.1275,
    "BSD": 1.0417, "BTC": 0.0000, "BTN": 90.9004, "BWP": 14.3888, "BYN": 3.4021,
    "BZD": 2.0882, "CAD": 1.5046, "CDF": 2977.8503, "CHF": 0.9394, "CLF": 0.0258,
    "CLP": 990.0229, "CNH": 7.5999, "CNY": 7.5918, "COP": 4296.0942, "CRC": 527.5138,
    "CUC": 1.0417, "CUP": 26.8233, "CVE": 110.2352, "CZK": 25.0879, "DJF": 185.1163,
    "DKK": 7.4577, "DOP": 64.7572, "DZD": 140.6484, "EGP": 52.5511, "ERN": 15.6252,
    "ETB": 134.1088, "EUR": 1.0000, "FJD": 2.4204, "FKP": 0.8266, "GBP": 0.8266,
    "GEL": 2.9011, "GGP": 0.8266, "GHS": 16.1136, "GIP": 0.8266, "GMD": 74.4803,
    "GNF": 8991.3874, "GTQ": 8.0218, "GYD": 217.4911, "HKD": 8.1004, "HNL": 26.5786,
    "HRK": 7.5349, "HTG": 136.5517, "HUF": 401.9000, "IDR": 17193.5056, "ILS": 3.7481,
    "IMP": 0.8266, "INR": 90.9375, "IQD": 1361.8276, "IRR": 43867.8729, "ISK": 145.5023,
    "JEP": 0.8266, "JMD": 163.8823, "JOD": 0.7388, "JPY": 156.7265, "KES": 134.2625,
    "KGS": 91.0941, "KHR": 4169.8120, "KMF": 494.7995, "KPW": 937.5146, "KRW": 1520.8985,
    "KWD": 0.3216, "KYD": 0.8664, "KZT": 518.3067, "LAK": 22559.7747, "LBP": 93137.9759,
    "LKR": 306.9356, "LRD": 207.5146, "LSL": 19.1991, "LYD": 5.0805, "MAD": 10.3491,
    "MDL": 19.4396, "MGA": 4949.5172, "MKD": 61.5180, "MMK": 2185.4508, "MNT": 3539.6386,
    "MOP": 8.3301, "MRU": 41.3644, "MUR": 48.7508, "MVR": 16.0419, "MWK": 1802.6790,
    "MXN": 21.3505, "MYR": 4.6496, "MZN": 66.5740, "NAD": 19.1991, "NGN": 1561.2640,
    "NIO": 38.2589, "NOK": 11.6971, "NPR": 145.4287, "NZD": 1.8595, "OMR": 0.3999,
    "PAB": 1.0417, "PEN": 3.8199, "PGK": 4.1848, "PHP": 60.3312, "PKR": 290.6614,
    "PLN": 4.1795, "PYG": 8239.6199, "QAR": 3.7888, "RON": 4.9754, "RSD": 117.1677,
    "RUB": 92.0985, "RWF": 1487.5686, "SAR": 3.9068, "SBD": 8.7842, "SCR": 15.2763,
    "SDG": 626.0514, "SEK": 11.1716, "SGD": 1.4060, "SHP": 0.8266, "SLL": 21843.5705,
    "SOS": 594.0910, "SRD": 37.0240, "SSP": 135.6896, "STD": 23210.5710, "STN": 24.4928,
    "SVC": 9.0965, "SYP": 13543.9616, "SZL": 19.1914, "THB": 35.7177, "TJS": 11.3418,
    "TMT": 3.6459, "TND": 3.3061, "TOP": 2.5081, "TRY": 38.0379, "TTD": 7.0501,
    "TWD": 34.2667, "TZS": 2708.0, "UAH": 43.0954, "UGX": 3821.9614, "USD": 1.0417,
    "UYU": 44.1828, "UZS": 13405.5047, "VES": 67.0881, "VND": 26682.4542, "VUV": 123.6707,
    "WST": 2.9167, "XAF": 655.9568, "XAG": 0.0333, "XAU": 0.0004, "XCD": 2.8152,
    "XDR": 0.7919, "XOF": 655.9568, "XPD": 0.0011, "XPF": 119.3317, "XPT": 0.0011,
    "YER": 257.5562, "ZAR": 19.4137, "ZMW": 29.6014, "ZWL": 335.4219, "VEF": 6708911.5559,
}
FALLBACK_TIMESTAMP = datetime.fromtimestamp(1740981150)  # Unix timestamp when these rates were last updated


# Check that all required environment variables are set
if not API_KEY:
    raise ValueError("OPENEXCHANGERATES_API_KEY environment variable is not set")
if not all([NUM_RECORDS, ORDER_DATE_MIN_DAYS_AGO, BASE_CURRENCY]):
    raise ValueError("Required environment variables (BATCH_SIZE, ORDER_DATE_MIN_DAYS_AGO, BASE_CURRENCY) are not set")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "email_on_failure": True,
}


@task(retries=2, retry_delay=timedelta(minutes=1))
def get_available_currencies() -> list[str]:
    """
    Fetch available currencies from OpenExchangeRates API.
    
    Returns:
        List of currency codes available for exchange rate conversion

    """
    url = f"{API_BASE_URL}/currencies.json?app_id={API_KEY}"
    with requests.Session() as session:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return list(response.json().keys())


def get_exchange_rates(base: str = BASE_CURRENCY) -> dict[str, float]:
    """
    Fetch exchange rates from OpenExchangeRates API.
    
    Args:
        base: Base currency for rates (default from environment)
    
    Returns:
        Dictionary mapping currency codes to exchange rates

    """
    url = f"{API_BASE_URL}/latest.json?app_id={API_KEY}"
    with requests.Session() as session:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Rates are relative to the base currency (USD in free tier)
        rates = data["rates"]
        base_rate = rates[base]
        return {currency: rate / base_rate for currency, rate in rates.items()}


@task
def generate_orders_task(currencies: list) -> int:
    """
    Generate random order data and insert into postgres-1.
    
    Returns:
        Number of records generated
    """
    faker = Faker()

    # Connect to postgres-1
    pg_hook = PostgresHook(postgres_conn_id="postgres_1")
    
    # Generate order data
    now = datetime.now()
    orders_data = []
    
    for _ in range(NUM_RECORDS):
        order_id = uuid.uuid4()
        customer_email = faker.email()
        order_date = now - timedelta(
            days=random.randint(0, ORDER_DATE_MIN_DAYS_AGO),
            seconds=random.randint(0, 86399)
        )
        amount = round(random.uniform(1.0, 1000.0), 2)
        currency = random.choice(currencies)
        
        orders_data.append((order_id, customer_email, order_date, amount, currency))
    
    # Insert data using batch execution
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany("""
                INSERT INTO orders (order_id, customer_email, order_date, amount, currency)
                VALUES (%s, %s, %s, %s, %s)
            """, orders_data)
            conn.commit()
    
    logger.info(f"Generated {NUM_RECORDS} orders")
    return NUM_RECORDS



@task
def transfer_and_convert_data() -> int:
    """
    Transfer data from postgres-1 to postgres-2 with currency conversion.
    
    Args:
        exchange_rates: Dictionary of currency exchange rates

    Returns:
        Number of records transferred
    """
    # Get exchange rates
    exchange_rates = get_exchange_rates()
    rate_date = datetime.now()
    
    # Get unprocessed orders
    pg1_hook = PostgresHook(postgres_conn_id="postgres_1")
    orders = pg1_hook.get_records("""
        SELECT o.order_id, o.customer_email, o.order_date, o.amount, o.currency
        FROM orders o
        LEFT JOIN orders_processed p ON o.order_id = p.order_id
        WHERE p.order_id IS NULL OR p.processed = FALSE
    """)
    
    if not orders:
        logger.info("No new orders to transfer")
        return 0
    
    # Process orders
    pg2_hook = PostgresHook(postgres_conn_id="postgres_2")
    transfer_data = []
    processed_ids = []
    
    for order in orders:
        order_id, customer_email, order_date, amount, currency = order
        
        exchange_rate = exchange_rates.get(currency, None)
        # Use fallback rate if not available
        if not exchange_rate:
            exchange_rate = FALLBACK_RATES.get(currency)
            rate_date = FALLBACK_TIMESTAMP
        
        amount_eur = Decimal(amount) / Decimal(exchange_rate)
        amount_eur = amount_eur.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        
        transfer_data.append((
            order_id, customer_email, order_date, amount, currency,
            float(amount_eur), exchange_rate, rate_date
        ))
        processed_ids.append(order_id)
    
    if not transfer_data:
        logger.warning("No orders to transfer after currency validation")
        return 0
    
    # Insert into postgres-2
    with pg2_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.executemany("""
                INSERT INTO orders_eur (
                    order_id, customer_email, order_date, original_amount,
                    original_currency, amount_eur, exchange_rate, rate_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, transfer_data)
            conn.commit()
    
    # Mark as processed in postgres-1
    with pg1_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for order_id in processed_ids:
                cursor.execute("""
                    INSERT INTO orders_processed (order_id, processed, processed_at)
                    VALUES (%s, TRUE, NOW())
                    ON CONFLICT (order_id) DO UPDATE 
                    SET processed = TRUE, processed_at = NOW()
                """, (order_id,))
            conn.commit()
    
    transferred_count = len(transfer_data)
    logger.info(f"Transferred {transferred_count} orders")
    return transferred_count


with DAG(
    dag_id="generate_orders",
    default_args=default_args,
    description="Generate orders every 10 minutes",
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["orders"],
    max_active_runs=1,
) as generate_orders_dag:
    currencies = get_available_currencies()
    generate_orders_task(currencies)

with DAG(
    dag_id="transfer_orders",
    default_args=default_args,
    description="Transfer orders hourly",
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["orders"],
    max_active_runs=1,
) as transfer_orders_dag:
    transfer_and_convert_data()
