import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, Any, List
from var import *
from logging_config import get_logger

logger = get_logger(__name__)

def insert_trades(conn, trade: Dict[str, Any]):
    cursor = conn.cursor()
    
    # Convert Unix timestamp to datetime
    trade_time = datetime.fromtimestamp(trade.get('timestamp', 0))
    
    columns = [
        'proxy_wallet', 'transaction_hash', 'condition_id', 'side', 'asset',
        'size', 'price', 'timestamp', 'trade_time', 'title', 'slug', 'icon',
        'event_slug', 'outcome', 'outcome_index', 'name', 'pseudonym', 'bio',
        'profile_image', 'profile_image_optimized', 'created_at'
    ]
    placeholders = ', '.join(['%s'] * len(columns))
    
    query = f"""
        INSERT INTO public.trades (
            {', '.join(columns)}
        ) VALUES (
            {placeholders}
        )
        ON CONFLICT (trade_time, proxy_wallet, condition_id, transaction_hash) DO UPDATE SET
            size = EXCLUDED.size,
            price = EXCLUDED.price
        RETURNING proxy_wallet;
    """
    
    try:
        cursor.execute(query, (
            trade.get('proxyWallet'),
            trade.get('transactionHash'),
            trade.get('conditionId'),
            trade.get('side'),
            trade.get('asset'),
            trade.get('size'),
            trade.get('price'),
            trade.get('timestamp'),
            trade_time,
            trade.get('title'),
            trade.get('slug'),
            trade.get('icon'),
            trade.get('eventSlug'),
            trade.get('outcome'),
            trade.get('outcomeIndex'),
            trade.get('name'),
            trade.get('pseudonym'),
            trade.get('bio'),
            trade.get('profileImage'),
            trade.get('profileImageOptimized'),
            datetime.utcnow()
        ))
        
        proxy_wallet = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Trade inserted - Wallet: {proxy_wallet}, Condition: {trade.get('conditionId')}, Side: {trade.get('side')}, Size: {trade.get('size')}")
        return proxy_wallet
    except Exception as e:
        error_msg = f"Failed to insert trade - Wallet: {trade.get('proxyWallet')}, Condition: {trade.get('conditionId', 'N/A')}"
        logger.error(f"{error_msg} | Error: {str(e)}")
        logger.debug(f"Full trade data: {trade}")
        conn.rollback()
        return None
    
def insert_market(conn, market: Dict[str, Any]):
    cursor = conn.cursor()
    end_date = market.get('end_date_iso')
    
    if end_date:
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    game_start = market.get('game_start_time')
    
    if game_start:
        game_start = datetime.fromisoformat(game_start.replace('Z', '+00:00'))
    

    columns = [
        'market_id', 'condition_id', 'question_id', 'question', 'description', 
        'market_slug', 'active', 'closed', 'archived', 'accepting_orders',
        'enable_order_book', 'end_date_iso', 'game_start_time', 'minimum_order_size', 'minimum_tick_size',
        'maker_base_fee', 'taker_base_fee', 'seconds_delay', 'fpmm_live', 'neg_risk', 
        'neg_risk_market_id', 'clob_token_ids', 'notifications_enabled', 'icon', 'image',
        'liquidity', 'outcomes', 'volume_num', 'created_at', 'updated_at'
    ]
    placeholders = ', '.join(['%s'] * len(columns))
    
    query = f"""
        INSERT INTO public.markets (
            {', '.join(columns)}
        ) VALUES (
            {placeholders}
        )
        ON CONFLICT (market_id, created_at) DO UPDATE SET
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            archived = EXCLUDED.archived,
            accepting_orders = EXCLUDED.accepting_orders,
            updated_at = CURRENT_TIMESTAMP
        RETURNING market_id;
    """

    try:
        cursor.execute(query, (
            market.get('id'), 
            market.get('conditionId'),
            market.get('questionID'),
            market.get('question'),
            market.get('description'),
            market.get('slug'),
            market.get('active', False),
            market.get('closed', False),
            market.get('archived', False),
            market.get('acceptingOrders', False),
            market.get('enableOrderBook', False),
            end_date,
            game_start,
            market.get('orderMinSize'),
            market.get('orderPriceMinTickSize'),
            market.get('makerBaseFee', 0),
            market.get('takerBaseFee', 0),
            market.get('secondsDelay', 0),
            market.get('fpmmLive'),
            market.get('negRisk', False),
            market.get('negRiskMarketID'),
            market.get('clobTokenIds'),
            market.get('notificationsEnabled', True),
            market.get('icon'),
            market.get('image'),
            market.get('liquidity'),
            market.get('outcomes'),
            market.get('volumeNum'),
            market.get('createdAt'),
            market.get('updatedAt')
        ))
        logger.info(f"created at: {market.get('createdAt')}, id: {market.get('id')}")
        market_id = cursor.fetchone()[0]
        conn.commit()
        return market_id
    except Exception as e:
        error_msg = f"Failed to insert market - ID: {market.get('id')}, Question: {market.get('question', 'N/A')[:100]}"
        logger.error(f"{error_msg} | Error: {str(e)}")
        logger.debug(f"Full market data: {market}")
        conn.rollback()
        return None

def insert_event(conn, event: Dict[str, Any]):
    cursor = conn.cursor()
    
    start_date = event.get('startDate')
    if start_date:
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    
    creation_date = event.get('creationDate')
    if creation_date:
        creation_date = datetime.fromisoformat(creation_date.replace('Z', '+00:00'))
    else:
        creation_date = datetime.utcnow()
    
    end_date = event.get('endDate')
    if end_date:
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    image_optimized = Json(event.get('imageOptimized')) if event.get('imageOptimized') else None
    icon_optimized = Json(event.get('iconOptimized')) if event.get('iconOptimized') else None
    featured_image_optimized = Json(event.get('featuredImageOptimized')) if event.get('featuredImageOptimized') else None
    sub_events = Json(event.get('subEvents')) if event.get('subEvents') else None
    
    columns = [
        'id', 'ticker', 'slug', 'title', 'subtitle', 'description', 'resolution_source',
        'start_date', 'creation_date', 'end_date', 'published_at', 'created_at', 'updated_at',
        'image', 'icon', 'featured_image',
        'active', 'closed', 'archived', 'new', 'featured', 'restricted', 'is_template', 
        'comments_enabled', 'enable_order_book', 'neg_risk',
        'liquidity', 'volume', 'open_interest', 'competitive',
        'volume_24hr', 'volume_1wk', 'volume_1mo', 'volume_1yr',
        'liquidity_amm', 'liquidity_clob', 'neg_risk_fee_bips', 'comment_count',
        'sort_by', 'category', 'subcategory', 'template_variables',
        'created_by', 'updated_by', 'disqus_thread', 'parent_event', 'neg_risk_market_id',
        'image_optimized', 'icon_optimized', 'featured_image_optimized', 'sub_events'
    ]
    placeholders = ', '.join(['%s'] * len(columns))
    
    query = f"""
        INSERT INTO public.events (
            {', '.join(columns)}
        ) VALUES (
            {placeholders}
        )
        ON CONFLICT (id, created_at) DO UPDATE SET
            ticker = EXCLUDED.ticker,
            slug = EXCLUDED.slug,
            title = EXCLUDED.title,
            subtitle = EXCLUDED.subtitle,
            description = EXCLUDED.description,
            active = EXCLUDED.active,
            closed = EXCLUDED.closed,
            archived = EXCLUDED.archived,
            featured = EXCLUDED.featured,
            liquidity = EXCLUDED.liquidity,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            volume_24hr = EXCLUDED.volume_24hr,
            volume_1wk = EXCLUDED.volume_1wk,
            volume_1mo = EXCLUDED.volume_1mo,
            volume_1yr = EXCLUDED.volume_1yr,
            liquidity_amm = EXCLUDED.liquidity_amm,
            liquidity_clob = EXCLUDED.liquidity_clob,
            comment_count = EXCLUDED.comment_count,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
    """

    try:
        cursor.execute(query, (
            event.get('id'),
            event.get('ticker'),
            event.get('slug'),
            event.get('title') or f"Event {event.get('id', 'Unknown')}",  # title is NOT NULL
            event.get('subtitle'),
            event.get('description'),
            event.get('resolutionSource'),
            start_date,
            creation_date,
            end_date,
            event.get('published_at'),
            datetime.utcnow(),
            datetime.utcnow(),
            event.get('image'),
            event.get('icon'),
            event.get('featuredImage'),
            event.get('active', False),
            event.get('closed', False),
            event.get('archived', False),
            event.get('new', False),
            event.get('featured', False),
            event.get('restricted', False),
            event.get('isTemplate', False),
            event.get('commentsEnabled', True),
            event.get('enableOrderBook', False),
            event.get('negRisk', False),
            event.get('liquidity'),
            event.get('volume'),
            event.get('openInterest'),
            event.get('competitive'),
            event.get('volume24hr'),
            event.get('volume1wk'),
            event.get('volume1mo'),
            event.get('volume1yr'),
            event.get('liquidityAmm'),
            event.get('liquidityClob'),
            event.get('negRiskFeeBips'),
            event.get('commentCount', 0),
            event.get('sortBy'),
            event.get('category'),
            event.get('subcategory'),
            event.get('templateVariables'),
            event.get('createdBy'),
            event.get('updatedBy'),
            event.get('disqusThread'),
            event.get('parentEvent'),
            event.get('negRiskMarketID'),
            image_optimized,
            icon_optimized,
            featured_image_optimized,
            sub_events
        ))
        
        event_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"event created at: {event.get('createdAt')}, event id: {event.get('id')}")
        return event_id
    except Exception as e:
        error_msg = f"Failed to insert event - ID: {event.get('id')}, Title: {event.get('title', 'N/A')[:100]}"
        logger.error(f"{error_msg} | Error: {str(e)}")
        logger.debug(f"Full event data: {event}")
        conn.rollback()
        cursor.close()
        return None

def insert_tags(conn, tags: List[Dict[str, Any]]):
    cursor = conn.cursor()
    try:
        columns = [
            'id', 'label', 'slug', 'force_show', 'created_at', 
            'published_at', 'created_by', 'updated_by', 'updated_at', 'force_hide', 
            'is_carousel'
        ]
        placeholders = ', '.join(['%s'] * len(columns))
        
        query = f"""
            INSERT INTO tags ( 
            {', '.join(columns)}
            ) VALUES (
                {placeholders}
            ) 
            ON CONFLICT (id, created_at) DO UPDATE SET
                label = EXCLUDED.label,
                slug = EXCLUDED.slug,
                force_show = EXCLUDED.force_show,
                published_at = EXCLUDED.published_at,
                created_by = EXCLUDED.created_by,
                updated_by = EXCLUDED.updated_by,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """
        cursor.execute(query, (
            tags.get('id'),
            tags.get('label'),
            tags.get('slug'),
            tags.get('forceShow'),
            tags.get('createdAt'),
            tags.get('publishedAt'),
            tags.get('createdBy'),
            tags.get('updatedBy'),
            tags.get('updatedAt'),
            tags.get('forceHide'),
            tags.get('isCarousel')
            ))
        conn.commit()
        logger.info(f"tag created at: {tags.get('createdAt')}, tag id: {tags.get('id')}")
        return cursor.fetchone()[0]
    except Exception as e:
        error_msg = f"Failed to insert tag - ID: {tags.get('id')}, Label: {tags.get('label', 'N/A')}"
        logger.error(f"{error_msg} | Error: {str(e)}")
        logger.debug(f"Full tag data: {tags}")
        conn.rollback()
        return None
   
def insert_item(conn, item: Dict[str, Any], endpoint: str):
    if endpoint == MARKETS_ENDPOINT:
        return insert_market(conn, item)
    elif endpoint == EVENTS_ENDPOINT:
        return insert_event(conn, item)
    elif endpoint == TAGS_ENDPOINT:
        return insert_tags(conn, item)
    elif endpoint == TRADES_ENDPOINT:
        return insert_trades(conn, item)
    else:
        raise ValueError(f"Invalid endpoint: {endpoint}")