import asyncio
import aiohttp
import psycopg2
from datetime import datetime
import os
import dotenv
from db_utils import *
from var import *
from logging_config import get_logger
dotenv.load_dotenv()

logger = get_logger(__name__)

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

async def fetch_polymarket_data_(conn, endpoint: str):
    logger.info(f"Fetching {endpoint} data")
    url = BASE_URL + endpoint
    offset = 0
    last_page = False
    page_processed = 0
    total_processed = 0
    async with aiohttp.ClientSession() as session:
        while not last_page:
            async with session.get(f'{url}?limit={limit_field}&offset={offset}&order={order_field}&ascending={asc_field}') as resp:
                response = await resp.json()
                data = response if isinstance(response, list) else response.get('data', [])
                offset += limit_field
                if len(data) < limit_field:
                    logger.info(f"Received {len(data)} items (less than limit {limit_field}), this is the last page")
                    last_page = True                
                for item in data:
                    try:
                        insert_item(conn, item, endpoint)
                        page_processed += 1
                        total_processed += 1
                    except Exception as e:
                        logger.error(f"Error processing item: {e}")
                        continue
                logger.info(f"Processed {page_processed} items from {endpoint}")
                offset += limit_field
                await asyncio.sleep(10)

    logger.info(f"total {endpoint} processed: {total_processed} items")
    conn.close()

async def fetch_polymarket_data_markets(conn):
    await fetch_polymarket_data_(conn, MARKETS_ENDPOINT)

async def fetch_polymarket_data_events(conn):
    await fetch_polymarket_data_(conn, EVENTS_ENDPOINT)

async def fetch_polymarket_data_tags(conn):
    await fetch_polymarket_data_(conn, TAGS_ENDPOINT)

async def main():
    #await fetch_polymarket_data()
    conn = psycopg2.connect(**DB_CONFIG)
    await fetch_polymarket_data_events(conn)
    # pause
    time.sleep(100)
    await fetch_polymarket_data_markets(conn)
    conn.close()
if __name__ == "__main__":
    asyncio.run(main())
