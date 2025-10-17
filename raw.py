import asyncio
import aiohttp
import psycopg2
import os
from datetime import datetime
from db_utils import (
    insert_market, 
    insert_price_snapshot, 
    insert_orderbook_snapshot,
    batch_insert_price_snapshots
)

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'Gulf'),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5433')
}

async def fetch_polymarket_data():
    """
    Fetch market data and prices from Polymarket API and store in TimescaleDB
    """
    conn = psycopg2.connect(**DB_CONFIG)
    
    cursor = conn.cursor()
    cursor.execute("SET search_path TO public;")
    conn.commit()
    cursor.close()
    
    timestamp = datetime.utcnow()
    print("starting up")
    
    total_processed = 0
    offset = 0
    limit = 100
    
    async with aiohttp.ClientSession() as session:
        while True:
            print(f"Fetching markets with offset={offset}, limit={limit}")
            
            async with session.get(f'https://clob.polymarket.com/markets?limit={limit}&offset={offset}') as resp:
                response = await resp.json()
                markets = response if isinstance(response, list) else response.get('data', [])
                
                if not markets:
                    print("No more markets to process")
                    break
                
                print(f"Received {len(markets)} markets from API")
                
                page_processed = 0
                for market in markets:
                    if market.get('active') or market.get('closed'):
                        print(f"Processing: {market['question'][:60]}...")
                        
                        try:
                            insert_market(conn, market)
                            page_processed += 1
                            total_processed += 1
                            
                        except Exception as e:
                            print(f"  Error processing market: {e}")
                            continue
                
                print(f"Processed {page_processed} markets from this page")
                offset += limit
                
                await asyncio.sleep(1)
    
    print(f"Total processed: {total_processed} markets")
    conn.close()
async def main():
    await fetch_polymarket_data()

if __name__ == "__main__":
    asyncio.run(main())
