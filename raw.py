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
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'santiago'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

async def fetch_polymarket_data():
    """
    Fetch market data and prices from Polymarket API and store in TimescaleDB
    """
    conn = psycopg2.connect(**DB_CONFIG)
    timestamp = datetime.utcnow()
    print("starting up")
    async with aiohttp.ClientSession() as session:
        async with session.get('https://clob.polymarket.com/markets') as resp:
            response = await resp.json()
            print(f"response : {response}")
            markets = response if isinstance(response, list) else response.get('data', [])
            
            processed_count = 0
            price_snapshots = []
            
            for market in markets:
                if market.get('active') or  market.get('closed'):
                    print(f"Processing: {market['question'][:60]}...")
                    
                    try:
                        await insert_market(conn, market)
                        processed_count += 1
                        
                        if processed_count >= 1:
                            break
                            
                    except Exception as e:
                        print(f"  Error processing market: {e}")
                        continue
            

            
            print(f" processed {processed_count} markets")
    
    conn.close()
                
async def main():
    while True:
        await fetch_polymarket_data()
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
