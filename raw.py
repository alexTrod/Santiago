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

filters_gamma_url = lambda: f'limit={limit_field}&offset={offset_field}&order={order_field}&ascending={asc_field}'
filters_data_url = lambda: f'limit={limit_field}&offset={offset_field}'

async def fetch_polymarket_data_(conn, base_url: str, endpoint: str, filters: str):
    logger.info(f"Fetching {endpoint} data")
    last_page = False
    page_processed = 0
    total_processed = 0
    offset = 9000
    async with aiohttp.ClientSession() as session:
        while not last_page:
            async with session.get(f'{base_url}/{endpoint}?{filters}') as resp:
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
    await fetch_polymarket_data_(conn, BASE_DATA_URL, MARKETS_ENDPOINT, filters_data_url())

async def fetch_polymarket_data_events(conn):
    await fetch_polymarket_data_(conn, BASE_DATA_URL, EVENTS_ENDPOINT, filters_data_url())

async def fetch_polymarket_data_trades(conn):
    await fetch_polymarket_data_(conn, BASE_DATA_URL, TRADES_ENDPOINT, filters_gamma_url())
    
async def fetch_polymarket_data_tags(conn):
    await fetch_polymarket_data_(conn, BASE_GAMMA_URL, TAGS_ENDPOINT, filters_data_url())

async def main():
    #await fetch_polymarket_data()
    conn = psycopg2.connect(**DB_CONFIG)
    #time.sleep(100)
    await fetch_polymarket_data_trades(conn)
    conn.close()
if __name__ == "__main__":
    asyncio.run(main())
