import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, Any, List

def insert_market(conn, market: Dict[str, Any]):
    cursor = conn.cursor()
    end_date = market.get('end_date_iso')
    if end_date:
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    game_start = market.get('game_start_time')
    print(game_start)
    if game_start:
        game_start = datetime.fromisoformat(game_start.replace('Z', '+00:00'))
    
    query = """
        INSERT INTO markets (
            market_id, condition_id, question_id, question, description, market_slug,
            active, closed, archived, accepting_orders, enable_order_book,
            end_date_iso, game_start_time, minimum_order_size, minimum_tick_size,
            maker_base_fee, taker_base_fee, seconds_delay, fpmm,
            neg_risk, neg_risk_market_id, neg_risk_request_id,
            notifications_enabled, is_50_50_outcome, icon, image,
            tags, rewards, tokens, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (market_id) DO UPDATE SET
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            archived = EXCLUDED.archived,
            accepting_orders = EXCLUDED.accepting_orders,
            tokens = EXCLUDED.tokens,
            updated_at = CURRENT_TIMESTAMP
        RETURNING market_id;
    """

    try:
        cursor.execute(query, (
            market.get('condition_id') or market.get('market_slug'), 
            market.get('condition_id'),
            market.get('question_id'),
            market.get('question'),
            market.get('description'),
            market.get('market_slug'),
            market.get('active', False),
            market.get('closed', False),
            market.get('archived', False),
            market.get('accepting_orders', False),
            market.get('enable_order_book', False),
            end_date,
            game_start,
            market.get('minimum_order_size'),
            market.get('minimum_tick_size'),
            market.get('maker_base_fee', 0),
            market.get('taker_base_fee', 0),
            market.get('seconds_delay', 0),
            market.get('fpmm'),
            market.get('neg_risk', False),
            market.get('neg_risk_market_id'),
            market.get('neg_risk_request_id'),
            market.get('notifications_enabled', True),
            market.get('is_50_50_outcome', False),
            market.get('icon'),
            market.get('image'),
            market.get('tags', []),
            Json(market.get('rewards', {})),
            Json(market.get('tokens', [])),
            datetime.utcnow(),
            datetime.utcnow()
        ))
        
        market_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return market_id
    except Exception as e:
        print(f"ERROR executing query: {e}")
        print(f"Market data: {market.get('question')[:50]}")
        conn.rollback()
        cursor.close()
        raise


def insert_price_snapshot(conn, market_id: str, token: Dict[str, Any], timestamp: datetime = None):
    """
    Insert a price snapshot into the TimescaleDB hypertable
    This is optimized for high-frequency time-series inserts
    """
    cursor = conn.cursor()
    
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    query = """
        INSERT INTO price_snapshots (
            timestamp, market_id, token_id, outcome, price, winner
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    cursor.execute(query, (
        timestamp,
        market_id,
        token.get('token_id'),
        token.get('outcome'),
        token.get('price'),
        token.get('winner', False)
    ))
    
    conn.commit()
    cursor.close()


def batch_insert_price_snapshots(conn, snapshots: List[Dict[str, Any]]):
    """
    Batch insert multiple price snapshots - more efficient for bulk operations
    
    snapshots: List of dicts with keys: timestamp, market_id, token_id, outcome, price, winner
    """
    cursor = conn.cursor()
    
    query = """
        INSERT INTO price_snapshots (
            timestamp, market_id, token_id, outcome, price, winner
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    data = [
        (
            s.get('timestamp', datetime.utcnow()),
            s['market_id'],
            s['token_id'],
            s['outcome'],
            s['price'],
            s.get('winner', False)
        )
        for s in snapshots
    ]
    
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()


def insert_orderbook_snapshot(conn, market_id: str, token_id: str, orderbook: Dict[str, Any], timestamp: datetime = None):
    """
    Insert an orderbook snapshot into the TimescaleDB hypertable
    Includes calculated metrics like spread and volumes
    """
    cursor = conn.cursor()
    
    if timestamp is None:
        timestamp = datetime.utcnow()
    
    bids = orderbook.get('bids', [])
    asks = orderbook.get('asks', [])
    
    bid_volume = sum(float(bid.get('size', 0)) for bid in bids) if bids else 0
    ask_volume = sum(float(ask.get('size', 0)) for ask in asks) if asks else 0
    
    spread = None
    if bids and asks:
        best_bid = max(float(bid.get('price', 0)) for bid in bids)
        best_ask = min(float(ask.get('price', 0)) for ask in asks)
        spread = best_ask - best_bid
    
    query = """
        INSERT INTO orderbook_snapshots (
            timestamp, market_id, token_id, bids, asks, bid_volume, ask_volume, spread
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cursor.execute(query, (
        timestamp,
        market_id,
        token_id,
        Json(bids),
        Json(asks),
        bid_volume,
        ask_volume,
        spread
    ))
    
    conn.commit()
    cursor.close()


def get_latest_prices(conn, market_id: str = None) -> List[Dict]:
    """Get latest prices for all tokens or specific market"""
    cursor = conn.cursor()
    
    if market_id:
        query = """
            SELECT market_id, token_id, outcome, price, timestamp, winner
            FROM latest_prices
            WHERE market_id = %s;
        """
        cursor.execute(query, (market_id,))
    else:
        query = """
            SELECT market_id, token_id, outcome, price, timestamp, winner
            FROM latest_prices
            LIMIT 100;
        """
        cursor.execute(query)
    
    columns = ['market_id', 'token_id', 'outcome', 'price', 'timestamp', 'winner']
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    return results


def get_price_history(conn, market_id: str, token_id: str, hours: int = 24) -> List[Dict]:
    """Get price history for a specific token"""
    cursor = conn.cursor()
    
    query = """
        SELECT timestamp, price, winner
        FROM price_snapshots
        WHERE market_id = %s 
          AND token_id = %s
          AND timestamp > NOW() - INTERVAL '%s hours'
        ORDER BY timestamp DESC;
    """
    
    cursor.execute(query, (market_id, token_id, hours))
    
    columns = ['timestamp', 'price', 'winner']
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    return results


def get_hourly_ohlc(conn, market_id: str, token_id: str, hours: int = 168) -> List[Dict]:
    """
    Get hourly OHLC (Open, High, Low, Close) data from continuous aggregate
    Default: last 7 days (168 hours)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT bucket, outcome, open, high, low, close, avg_price, num_samples
        FROM price_1h
        WHERE market_id = %s 
          AND token_id = %s
          AND bucket > NOW() - INTERVAL '%s hours'
        ORDER BY bucket DESC;
    """
    
    cursor.execute(query, (market_id, token_id, hours))
    
    columns = ['bucket', 'outcome', 'open', 'high', 'low', 'close', 'avg_price', 'num_samples']
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    return results


def get_active_markets(conn) -> List[Dict]:
    """Get all active markets with their latest prices"""
    cursor = conn.cursor()
    
    query = """
        SELECT market_id, question, end_date_iso, market_slug, active, closed, tokens
        FROM active_markets_prices;
    """
    
    cursor.execute(query)
    
    columns = ['market_id', 'question', 'end_date_iso', 'market_slug', 'active', 'closed', 'tokens']
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    cursor.close()
    return results

