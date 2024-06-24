from decimal import Decimal
import os
from datetime import datetime, timezone, timedelta
import pytz
import time
import sys
import importlib
import logging
import traceback
from ast import literal_eval
import pandas as pd
import crontabula
import psycopg
from model import (
    insert_run,
    insert_order, 
    fetch_enabled_portfolios, 
    fetch_portfolio_broker, 
    fetch_orders_by_status,
    update_order, 
    fetch_available_cash, 
    fetch_positions, 
    update_portfolio,
    Portfolio,
    PortfolioRun,
    PortfolioOrder
)
from brokers import AlpacaBroker
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "strategies"))

log = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

DB_CONN_STRING = os.environ.get("TRADEBOT_DB_CONN")

def order_summary(o):
    return f"{o.side.upper()} " + (f"${o.notional} of {o.ticker}" if o.side == "buy" else f"{o.quantity} of {o.ticker}")

def round_time(dt=None, date_delta=timedelta(minutes=1), to='up'):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    seconds = (dt - dt.replace(hour=0, minute=0, second=0)).seconds

    if seconds % round_to == 0 and dt.microsecond == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            rounding = (seconds + dt.microsecond/1000000 + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + timedelta(0, rounding - seconds, - dt.microsecond)

def column_to_ticker(column):
    try:
        t = literal_eval(column) if isinstance(column, str) else tuple(column)
        if len(t) > 1:
            return t[-1]
    except:
        pass
    return column
        
def instantiate_broker(broker):
    if broker.type == "alpaca":
        creds = broker.credentials if broker.credentials else {}
        return AlpacaBroker(log, creds)
    return None
            
def is_market_open(portfolio):
    pf_module = importlib.import_module(portfolio.module)
    return pf_module.is_market_open()

pf_modules = {}
def instantiate_pf(portfolio, available_cash, positions):
    global pf_modules
    if portfolio.module not in pf_modules:
        pf_module = importlib.import_module(portfolio.module)
        pf_modules[portfolio.module] = pf_module
    pf_module = pf_modules[portfolio.module]
    pf_params = {"live": True, "pf_kwargs": {}}
    pf = pf_module.create_portfolio(pf_params)
    pf_params["pf_kwargs"]["init_cash"] = available_cash
    pf_params["pf_kwargs"]["init_position"] = [
        positions[ticker] if ticker in positions else 0 
        for ticker in list(pf.wrapper.columns)
    ]
    if not pf_module.can_trade(pf_params):
        return None
    return pf_module.create_portfolio(pf_params)

def run_portfolio(conn, portfolio):
    with conn.cursor() as cursor:
        log.info("Fetching broker...")
        broker_record = fetch_portfolio_broker(cursor, portfolio.id)
        log.info("Fetching available cash...")
        available_cash = fetch_available_cash(cursor, portfolio.id)
        log.info("Fetching positions...")
        positions = fetch_positions(cursor, portfolio.id)

    broker = instantiate_broker(broker_record)
    if broker:
        log.info("Verifying broker positions match ours...")
        broker_positions = broker.positions(portfolio)
        log.info("Our positions")
        log.info(positions)
        log.info("Broker positions")
        log.info(broker_positions)
        if positions != broker_positions:
            log.error("Positions do not match! Skipping...")
            return
        
    if not is_market_open(portfolio):
        log.info("Outside market hours for this portfolio. Skipping...")
        return
    
    log.info("Instantiating (running) portfolio...")
    status = "succeeded"
    err = None
    pf = None
    try:
        pf = instantiate_pf(portfolio, available_cash, positions)
        if pf is None:
            log.info("Unable to trade the portfolio right now, skipping")
            return
    except:
        log.exception("Error encountered instantiating portfolio...")
        status = "failed"
        err = traceback.format_exc()
        
    now = datetime.now(timezone.utc)
                            
    run = PortfolioRun(
        0,
        portfolio.id,
        status,
        now,
        err,
        False
    )
    with conn.cursor() as cursor:
        run_id = insert_run(cursor, run)
        
    if err is not None:
        return
    
    with conn.cursor() as cursor:
        portfolio_list = list(portfolio)
        portfolio_list[portfolio._fields.index("last_run_timestamp")] = now
        portfolio = Portfolio(*portfolio_list)
        log.info("Updating last run time for portfolio")
        update_portfolio(cursor, portfolio)
        
    records = pf.orders.records_readable
    if records.empty:
        log.info("No orders to create. Done!")
        return
    
    # Sort so sells come before buys
    records = records.sort_values(by=["Side"], ascending=False)
                    
    # Grab the next order to generate
    record = records.iloc[0]
    
    side = record["Side"].lower()
    notional = Decimal.min(Decimal(record["Size"]) * Decimal(record["Price"]), available_cash) if side == "buy" else None
    quantity = Decimal(record["Size"]) if side == "sell" else None

    # Only notify for open order if manual broker (broker is none)
    notified = broker is not None
    order = PortfolioOrder(
        0,
        portfolio.id,
        run_id,
        "open",
        column_to_ticker(record["Column"]),
        side,
        now,
        notional,
        quantity,
        None,
        None,
        None,
        None,
        None,
        notified
    )
    log.info(f"Creating order to {order_summary(order)}...")
        
    with conn.cursor() as cursor:
        order_id = insert_order(cursor, order)
        order = PortfolioOrder(order_id, *(list(order)[1:]))

        if broker:
            broker.submit_orders(portfolio, [order])


try:
    while True:
        time.sleep(10)
        
        # Fetch all active portfolios
        portfolios = []
        try:
            with psycopg.connect(DB_CONN_STRING) as conn:
                with conn.cursor() as cursor:
                    portfolios = fetch_enabled_portfolios(cursor)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            log.exception("Failed to fetch enabled portfolios")
            continue
            
        # Try to run each portfolio inside its own DB connection
        for portfolio in portfolios:
            with psycopg.connect(DB_CONN_STRING) as conn:
                log.info(f"Looking at portfolio '{portfolio.name}'...")
                
                try:
                    # Try to resolve the status of existing open orders
                    open_orders = []
                    with conn.cursor() as cursor:
                        broker_record = fetch_portfolio_broker(cursor, portfolio.id)
                        open_orders = fetch_orders_by_status(cursor, portfolio.id, "open")

                    if open_orders:
                        broker = instantiate_broker(broker_record)
                        if broker:
                            log.info("Attempting to automatically resolve open orders...")
                            with conn.cursor() as cursor:
                                for order in broker.resolve_orders(portfolio, open_orders):
                                    update_order(cursor, order)
                    
                    # Check for any remaining open orders after resolving
                    with conn.cursor() as cursor:
                        if fetch_orders_by_status(cursor, portfolio.id, "open"):
                            log.info("Portfolio has open orders that need to be resolved first, skipping.")
                            continue
                    
                    # NOTE: All timestamps in the DB and elsewhere in the codebase are in UTC. 
                    #       However, the one exception is that cron schedules are assumed to be
                    #       in America/New_York because NYSE always opens and closes at the same
                    #       times in America/New_York. Therefore, we need to convert times
                    #       to America/New_York before checking if now is the right time to run.
                    crontab = crontabula.parse(portfolio.schedule)
                    now = datetime.now(timezone.utc)
                    now_ny = now.astimezone(pytz.timezone('US/Eastern'))
                    start = (portfolio.start_timestamp if portfolio.last_run_timestamp is None else portfolio.last_run_timestamp).replace(tzinfo=pytz.UTC)
                    start_ny = start.astimezone(pytz.timezone('US/Eastern'))
                    # Round up to the nearest minute since crontabula ignores seconds when determining the next date time,
                    # which could result in running a portfolio 59 times in a row...
                    start_ny = round_time(start_ny)
                    # Crontabula always returns tz-unaware/tz-naive datetimes, so we have to explicitly say that the time 
                    # is America/New_York in order to compare with our _ny datetimes
                    run_times = crontab.date_times(start=start_ny)
                    next_run = pytz.timezone('US/Eastern').localize(next(run_times))
                    previous_next_run = next_run
                    while next_run < now_ny:
                        previous_next_run = next_run
                        next_run = pytz.timezone('US/Eastern').localize(next(run_times))
                    next_run = previous_next_run
                    # Make sure to only run the portfolio when we're exactly at after the next run time 
                    # AND we're still within the same hour. The latter is important so we don't run the portfolio at the
                    # wrong time of day if it fell behind somehow.
                    if now_ny >= next_run and pd.Timestamp(now_ny).floor(freq="H") == pd.Timestamp(next_run).floor(freq="H"):
                        log.info("Running the portfolio to look for new orders...")
                        run_portfolio(conn, portfolio)
                    else:
                        log.info("Nothing to do right now")
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    log.exception(f"Exception thrown trying to run portfolio '{portfolio.name}'")
except (KeyboardInterrupt, SystemExit):
    log.info("Shutting down...")
    sys.exit()
