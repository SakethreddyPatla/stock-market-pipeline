import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
import requests
import snowflake.connector
from dotenv import load_dotenv
from config import STOCKS, BASE_URL, FUNCTION

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        schema="RAW",
    )

def fetch_daily_prices(symbol: str, output_size: str = "compact") -> dict | None:
    params = {
        "function": FUNCTION,
        "symbol": symbol,
        "outputsize": output_size,
        "apikey": os.getenv("ALPHA_VANTAGE_API_KEY"),
    }
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"API response keys for {symbol}: {list(data.keys())}")
        if "Error Message" in data:
            logger.error(f"API error for {symbol}: {data['Error Message']}")
            return None
        
        if "Note" in data:
            logger.error(f"Unexpected response for {symbol}: {list(data.keys())}")
            return None
        if "Information" in data:
            logger.warning(f"API info message: {data['Information']}")
            return None        
        if "Time Series (Daily)" not in data:
            logger.error(f"Fetched {record_count} daily records for {symbol}")
            return None
        
        record_count = len(data["Time Series (Daily)"])
        logger.info(f"Fetched {record_count} daily records for {symbol}")
        return data
    except requests.RequestException as e:
        logger.error(f"Request failed for {symbol}: {e}")
        return None
    
def load_to_snowflake(conn, symbol: str, api_response: dict):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO RAW.DAILY_PRICES_RAW (symbol, ingestion_date, api_response)
            SELECT %s, CURRENT_TIMESTAMP, PARSE_JSON(%s)
            """,
            (symbol, json.dumps(api_response)),
        )
        logger.info(f"loaded {symbol} into Snowflake RAW layer")
    except Exception as e:
        logger.error(f"Snowflake load failed for {symbol}: {e}")
        raise
    finally:
        cursor.close()

def run_pipeline(output_size: str = "compact"):
    logger.info("=" * 10)
    logger.info(f"Started ingestion pipeline | {len(STOCKS)} stocks | mode={output_size}")

    conn = get_snowflake_connection()
    success_count = 0
    fail_count = 0

    try:
        for stock in STOCKS:
            symbol = stock["symbol"]
            logger.info(f"Processing {symbol} ({stock['company']})..")
            data = fetch_daily_prices(symbol, output_size)
            if data:
                load_to_snowflake(conn, symbol, data)
                success_count += 1
            else:
                fail_count += 1
            time.sleep(15)
        conn.commit()
    finally:
        conn.close()
    logger.info("=" * 10)
    logger.info(f"Pipeline complete | Success: {success_count} | Failed: {fail_count}")

    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock data ingestion pipeline")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Fetch full history (20+ years) instead of last 100 days",
    )
    args = parser.parse_args()
    output_size = "full" if args.full else "compact"
    run_pipeline(output_size)