import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta, timezone
import uuid
import requests
import schedule
import time

# логин и пароль от клика
USER = "username"
PASS = "pass"

def wait_for_next_interval():
    """
    Ждет ближайшее время, кратное 5 минутам, перед первым запуском.
    """
    now = datetime.now(timezone.utc)
    seconds_to_wait = (5 - (now.minute % 5)) * 60 - now.second
    print(f"Ожидание {seconds_to_wait} секунд до первого запуска...")
    time.sleep(seconds_to_wait)

def get_time_range():
    """
    Возвращает временной диапазон за последние 5 минут (UTC) с округлением до минут.
    """
    now = datetime.now(timezone.utc)
    
    # Округляем start_time и end_time до минут (зануляем секунды)
    start_time = (now - timedelta(minutes=10)).replace(second=0, microsecond=0)
    end_time = (now - timedelta(minutes=5)).replace(second=0, microsecond=0)
    
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Запуск скрипта.")
    print(f"Загружаем данные за период: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"), start_time.strftime("%Y-%m-%d")

# Тест функции
start_time, end_time, date = get_time_range()

def load_sol_price_to_dataframe(start_time, end_time):
    """
    Получает цену SOL/USDT с Binance API и возвращает её в виде DataFrame.
    """
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    current_start = start_time

    while current_start < end_time:
        current_end = min(current_start + timedelta(minutes=1000), end_time)
        params = {
            "symbol": "SOLUSDT",
            "interval": "1m",
            "startTime": int(current_start.timestamp() * 1000),
            "endTime": int(current_end.timestamp() * 1000),
            "limit": 1000
        }
        response = requests.get(url, params=params)
        data = response.json()
        if isinstance(data, list):
            all_data.extend(data)
        else:
            print(f"Ошибка запроса: {data}")
            break
        current_start = current_end

    # Преобразуем в DataFrame
    sol_prices = pd.DataFrame(all_data, columns=[
        "timestamp", "open", "high", "low", "sol_price", "volume", "close_time",
        "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume", "ignore"
    ])

    sol_prices["timestamp"] = pd.to_datetime(sol_prices["timestamp"], unit="ms", utc=True)
    sol_prices["block_time_for_join"] = sol_prices["timestamp"].dt.floor("T").dt.strftime("%Y-%m-%d %H:%M:%S")
    sol_prices["sol_price"] = pd.to_numeric(sol_prices["sol_price"], errors="coerce").astype(float)
    sol_prices = sol_prices[["block_time_for_join", "sol_price"]]

    return sol_prices

def fetch_and_insert_data():
    """
    Выгружает данные из ClickHouse за последние 5 минут, загружает курс SOL/USD и вставляет их в ClickHouse.
    """
    while True:
        start_time, end_time, date = get_time_range()

        # Определяем временной диапазон для загрузки цен SOL/USDT
        start_time_bin = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_time_bin = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1, seconds=-1)).replace(tzinfo=timezone.utc)

        # Загружаем цены SOL/USDT в DataFrame
        sol_prices = load_sol_price_to_dataframe(start_time_bin, end_time_bin)

        with clickhouse_connect.get_client(
            host="chess-beta.api.web3engineering.co.uk",
            port=28123,
            username=USER,
            password=PASS,
            secure=True,
            connect_timeout=60000,
            session_id=str(uuid.uuid4()),
        ) as client:

            # Создание временной таблицы в ClickHouse в рамках той же сессии
            client.command("DROP TEMPORARY TABLE IF EXISTS sol_prices_tmp;")
            client.command("""
                CREATE TEMPORARY TABLE IF NOT EXISTS sol_prices_tmp (
                    block_time_for_join String,
                    sol_price Float32
                );
            """)

            # вставляем данные по новым монетам
            client.command(f"""
                INSERT INTO default.token_first_raydium_trade_dict (
                    coin
                    ,token_first_trade_time_utc
                    ,token_first_trade_dt
                )
                select
                    CASE
                        WHEN base_coin <> 'So11111111111111111111111111111111111111112' THEN base_coin
                        else quote_coin
                    end AS coin
                    ,formatDateTime(toTimeZone(min(block_time), 'UTC'), '%Y-%m-%d %H:%i:%S') as token_first_trade_time_utc
                    ,toDate(token_first_trade_time_utc) as token_first_trade_dt
                from
                    default.raydium_all_swaps as ras
                    left join default.token_first_raydium_trade_dict as tfrtd
                        on CASE
                                WHEN base_coin <> 'So11111111111111111111111111111111111111112' THEN base_coin
                                else quote_coin
                            end = tfrtd.coin
                where
                    toDate(toTimeZone(ras.block_time, 'UTC')) = '{date}'
                    and tfrtd.coin = ''
                group by
                    coin;
            """)


            # Вставка данных в ClickHouse
            client.insert("sol_prices_tmp", sol_prices.to_records(index=False).tolist(),
                          column_names=["block_time_for_join", "sol_price"])        

            client.command(f"DROP TEMPORARY TABLE IF EXISTS raydium_all_swaps_tmp;")
            client.command(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS raydium_all_swaps_tmp AS 
                SELECT DISTINCT 
                    signature,
                    CASE WHEN base_coin = 'So11111111111111111111111111111111111111112' THEN base_pool_balance_before ELSE quote_pool_balance_before END AS base_pool_balance_before_corrected,
                    CASE WHEN base_coin = 'So11111111111111111111111111111111111111112' THEN base_pool_balance_after ELSE quote_pool_balance_after END AS base_pool_balance_after_corrected,
                    CASE WHEN base_coin <> 'So11111111111111111111111111111111111111112' THEN base_pool_balance_before ELSE quote_pool_balance_before END AS quote_pool_balance_before_corrected,
                    CASE WHEN base_coin <> 'So11111111111111111111111111111111111111112' THEN base_pool_balance_after ELSE quote_pool_balance_after END AS quote_pool_balance_after_corrected,
                    CASE
                        WHEN base_coin <> 'So11111111111111111111111111111111111111112' THEN base_coin
                        else quote_coin
                    end as coin,
                    raydium_market_id,
                    CASE
                        WHEN base_coin = 'So11111111111111111111111111111111111111112' AND direction = 'B' THEN 'S'
                        WHEN base_coin = 'So11111111111111111111111111111111111111112' AND direction = 'S' THEN 'B'
                        WHEN quote_coin = 'So11111111111111111111111111111111111111112' AND direction = 'B' THEN 'B'
                        WHEN quote_coin = 'So11111111111111111111111111111111111111112' AND direction = 'S' THEN 'S'
                    END AS direction
                FROM raydium_all_swaps AS ras
                WHERE 
                    formatDateTime(toTimeZone(block_time, 'UTC'), '%Y-%m-%d %H:%i:%S') >= '{start_time}'
                    and formatDateTime(toTimeZone(block_time, 'UTC'), '%Y-%m-%d %H:%i:%S') <= '{end_time}';
            """)

            client.command(f"DROP TEMPORARY TABLE IF EXISTS raydium_only_sol_trades_tmp;")
            client.command(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS raydium_only_sol_trades_tmp AS 
                SELECT DISTINCT 
                    formatDateTime(toTimeZone(block_time, 'UTC'), '%Y-%m-%d %H:%i:%S') AS block_time_utc, 
                    formatDateTime(toTimeZone(toStartOfMinute(block_time), 'UTC'), '%Y-%m-%d %H:%i:%S') AS block_time_utc_for_join,
                    formatDateTime(toTimeZone(block_time, 'Europe/Moscow'), '%Y-%m-%d %H:%i:%S') AS block_time_gtm3,
                    ras.*
                FROM raydium_only_sol_trades AS ras
                WHERE 
                    block_time_utc >= '{start_time}'
                    and block_time_utc <= '{end_time}';
            """)

            client.command(f"DROP TABLE IF EXISTS agg_raydium_trades_tmp;")
            client.command(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS agg_raydium_trades_tmp AS 
                SELECT rost.*, 
                    rast.quote_pool_balance_before_corrected * pow(10, -6) AS quote_pool_balance_before,
                    rast.quote_pool_balance_after_corrected * pow(10, -6) AS quote_pool_balance_after,
                    rast.base_pool_balance_before_corrected * pow(10, -9) AS base_pool_balance_before,
                    rast.base_pool_balance_after_corrected * pow(10, -9) AS base_pool_balance_after,
                    rast.raydium_market_id as raydium_market_id
                FROM raydium_only_sol_trades_tmp AS rost
                LEFT JOIN raydium_all_swaps_tmp AS rast 
                    ON rost.signature = rast.signature
                    and rost.coin = rast.coin
                    and rost.direction = rast.direction
            """)

            client.command(f"DROP TABLE IF EXISTS agg_raydium_trades_before_mev_tmp;")
            client.command(f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS agg_raydium_trades_before_mev_tmp AS 
                SELECT 
                    generateUUIDv4() as transaction_id
                    ,toDate(art.block_time_utc) as block_date_utc
                    ,art.block_time_utc
                    ,art.block_time_gtm3
                    ,art.signature AS signature
                    ,art.signing_wallet AS signing_wallet
                    ,art.coin AS coin
                    ,art.raydium_market_id AS raydium_market_id
                    ,art.slot
                    ,art.tx_idx
                    ,art.direction
                    ,abs(toInt64(art.coin_amount))  * pow(10, -6) as coin_amount
                    ,abs(toInt64(lamports)) * pow(10, -9) as sol_amount
                    ,abs(toInt64(art.provided_gas_fee)) as provided_gas_fee
                    ,abs(toInt64(art.provided_gas_limit)) as provided_gas_limit
                    ,abs(toInt64(art.fee)) * pow(10, -9) as fee
                    ,abs(toInt64(art.consumed_gas)) as consumed_gas
                    ,art.quote_pool_balance_before
                    ,art.quote_pool_balance_after
                    ,art.base_pool_balance_before
                    ,art.base_pool_balance_after
                    ,divide(base_pool_balance_before, quote_pool_balance_before) AS price_before_deal
                    ,divide(base_pool_balance_after, quote_pool_balance_after) AS price_after_deal
                    ,(price_after_deal / price_before_deal - 1) AS slippage_perc
                    ,sp.sol_price
                    ,(sol_amount * sp.sol_price) as usdt_amount
                    ,((base_pool_balance_before * sp.sol_price) + (quote_pool_balance_before * price_before_deal * sp.sol_price)) as liquidity_before
                    ,((base_pool_balance_after * sp.sol_price) + (quote_pool_balance_after * price_after_deal * sp.sol_price)) as liquidity_after
                    ,tfrtd.token_first_trade_dt as token_first_trade_dt
                    ,tfrtd.token_first_trade_time_utc as token_first_trade_time_utc
                FROM 
                    agg_raydium_trades_tmp AS art
                    left join sol_prices_tmp as sp
                        on art.block_time_utc_for_join = sp.block_time_for_join
                    left join default.token_first_raydium_trade_dict as tfrtd
                        on art.coin = tfrtd.coin
            """)

            client.command(f"""drop temporary table if exists mev_tmp;""")
            client.command(f"""
                CREATE TEMPORARY TABLE mev_tmp AS
                select
                    mev_type
                    ,prev_transaction_id
                    ,transaction_id
                    ,next_transaction_id
                from
                (
                    WITH
                        lagInFrame(signing_wallet) OVER w AS prev_wallet,
                        lagInFrame(direction) OVER w AS prev_direction,
                        lagInFrame(slot) OVER w as prev_slot,
                        lagInFrame(tx_idx) OVER w as prev_tx_idx,
                        lagInFrame(transaction_id) OVER w as prev_transaction_id,
                        leadInFrame(signing_wallet) OVER w AS next_wallet,
                        leadInFrame(direction) OVER w AS next_direction,
                        leadInFrame(slot) OVER w as next_slot,
                        leadInFrame(tx_idx) OVER w as next_tx_idx,
                        leadInFrame(transaction_id) OVER w as next_transaction_id
                    SELECT
                        case
                            when
                                (prev_direction = direction)
                                and (direction <> next_direction)
                                and prev_wallet = next_wallet
                                and signing_wallet <> prev_wallet
                                and (prev_slot = slot)
                                and (next_slot = slot)
                                and (tx_idx - prev_tx_idx) = 1
                                and (next_tx_idx - tx_idx) = 1
                            then 1
                        end as mev_flg
                        ,direction as mev_type
                        ,prev_transaction_id
                        ,transaction_id
                        ,next_transaction_id
                    FROM
                        agg_raydium_trades_before_mev_tmp
                    WINDOW w AS (PARTITION BY raydium_market_id ORDER BY slot, tx_idx ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
                    order by
                        raydium_market_id
                        ,slot
                        ,tx_idx
                ) as tmp
                where
                    tmp.mev_flg = 1;
            """)            


            client.command(f"""drop temporary table if exists mev_victims_tmp;""")

            client.command(f"""
                    CREATE TEMPORARY TABLE mev_victims_tmp AS
                    SELECT
                        transaction_id,
                        mev_type
                    FROM mev_tmp;
            """)   

            client.command(f"""drop temporary table if exists mev_transactions_tmp;""")

            client.command(f"""
                CREATE TEMPORARY TABLE mev_transactions_tmp AS
                SELECT
                    distinct
                        transaction_id
                        ,mev_type
                FROM
                (            
                    SELECT
                        prev_transaction_id AS transaction_id,
                        mev_type
                    FROM mev_tmp
                    UNION ALL
                    SELECT
                        next_transaction_id AS transaction_id
                        ,mev_type
                    FROM mev_tmp
                ) t;
            """)

            client.command(f"""
                    INSERT INTO default.agg_raydium_trades_enriched (
                        transaction_id
                        ,block_date_utc
                        ,block_time_utc
                        ,block_time_gtm3
                        ,signature
                        ,signing_wallet
                        ,coin
                        ,raydium_market_id
                        ,slot
                        ,tx_idx
                        ,direction
                        ,coin_amount
                        ,sol_amount
                        ,provided_gas_fee
                        ,provided_gas_limit
                        ,fee
                        ,consumed_gas
                        ,quote_pool_balance_before
                        ,quote_pool_balance_after
                        ,base_pool_balance_before
                        ,base_pool_balance_after
                        ,price_before_deal
                        ,price_after_deal
                        ,slippage_perc
                        ,sol_price
                        ,usdt_amount
                        ,liquidity_before
                        ,liquidity_after
                        ,token_first_trade_dt
                        ,token_first_trade_time_utc
                        ,mev_transactio_flg         
                        ,mev_victim_flg
                        ,mev_type
                    )
                    select
                        artbm.*
                        ,case
                            when mt.transaction_id <> '00000000-0000-0000-0000-000000000000' then 1
                            else 0
                        end as mev_transactio_flg
                        ,case
                            when mv.transaction_id <> '00000000-0000-0000-0000-000000000000' then 1
                            else 0
                        end as mev_victim_flg
                        ,case
                            when mv.mev_type <> '' then mv.mev_type
                            when mt.mev_type <> '' then mt.mev_type
                            else ''
                        end as mev_type
                    from
                        agg_raydium_trades_before_mev_tmp as artbm
                        left join mev_victims_tmp as mv
                            on artbm.transaction_id = mv.transaction_id
                        left join mev_transactions_tmp as mt
                            on artbm.transaction_id = mt.transaction_id
            """)
            
        # Ждем до следующего четкого 5-минутного интервала
        now = datetime.now(timezone.utc)
        next_run_time = (now + timedelta(minutes=5 - now.minute % 5)).replace(second=0, microsecond=0)
        sleep_time = (next_run_time - datetime.now(timezone.utc)).total_seconds()
        print(f"Следующий запуск в {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}, спим {sleep_time} секунд...\n")
        time.sleep(sleep_time)
    
if __name__ == "__main__":
    wait_for_next_interval()  # Ожидание до ближайшего 5-минутного интервала
    fetch_and_insert_data()  # Первый запуск после ожидания