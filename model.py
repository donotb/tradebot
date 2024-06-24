from collections import namedtuple
from decimal import Decimal
    
Broker = namedtuple("Broker", [
    "id", 
    "author",
    "name",
    "type", 
    "credentials"
])
Portfolio = namedtuple("Portfolio", [
    "id", 
    "author",
    "enabled", 
    "broker_id",
    "name",
    "shortname", 
    "module", 
    "schedule", 
    "start_timestamp",
    "last_run_timestamp"
])
PortfolioRun = namedtuple("PortfolioRun", [
    "id",
    "portfolio_id",
    "status",
    "timestamp",
    "error",
    "notified"
])
PortfolioOrder = namedtuple("PortfolioOrder", [
    "id", 
    "portfolio_id",
    "run_id",
    "status", 
    "ticker", 
    "side", 
    "create_timestamp", 
    "notional", 
    "quantity",
    "fill_timestamp", 
    "fill_quantity", 
    "fill_price", 
    "fill_fee", 
    "broker_order_id",
    "notified"
])
PortfolioCash = namedtuple("PortfolioCash", [
    "id", 
    "portfolio_id", 
    "event", 
    "event_timestamp",
    "amount",
    "order_id"
])
PortfolioPosition = namedtuple("PortfolioPosition", [
    "id", 
    "portfolio_id", 
    "event", 
    "event_timestamp",
    "ticker",
    "amount",
    "order_id"
])

def to_columnselect(namedtuple_type, prefix=""):
    columns = namedtuple_type._fields
    if prefix:
        columns = map(lambda column: prefix + "." + column, columns)
    return ", ".join(columns)

def fetch_portfolio_broker(cursor, pf_id):
    record = cursor.execute(f"""
        SELECT {to_columnselect(Broker, prefix="b")}   
        FROM portfolio pf 
        INNER JOIN broker b ON pf.broker_id = b.id
        WHERE pf.id = %s
    """, (int(pf_id),)).fetchone()
    return Broker(*record)

def fetch_broker(cursor, author, id):
    record = cursor.execute(f"""
        SELECT {to_columnselect(Broker)}   
        FROM broker b 
        WHERE id = %s AND author = %s
    """, (int(id),str(author))).fetchone()
    return Broker(*record)

def fetch_brokers(cursor, author):    
    records = cursor.execute(f"""
        SELECT {to_columnselect(Broker)}
        FROM broker
        WHERE author = %s
    """, (str(author),))
    brokers = []
    for record in records:
        brokers.append(Broker(*record))
    return brokers

def fetch_cash_history(cursor, pf_id):
    records = cursor.execute(f"""
        SELECT {to_columnselect(PortfolioCash, prefix="pfc")}         
        FROM portfolio pf 
        INNER JOIN portfolio_cash pfc ON pf.id = pfc.portfolio_id
        WHERE pf.id = %s
        ORDER BY event_timestamp ASC
    """, (int(pf_id),))
    history = []
    for record in records:
        history.append(PortfolioCash(*record))
    return history

def fetch_position_history(cursor, pf_id):
    records = cursor.execute(f"""
        SELECT {to_columnselect(PortfolioPosition, prefix="pfp")}         
        FROM portfolio pf 
        INNER JOIN portfolio_position pfp ON pf.id = pfp.portfolio_id
        WHERE pf.id = %s
        ORDER BY event_timestamp ASC
    """, (int(pf_id),))
    history = []
    for record in records:
        history.append(PortfolioPosition(*record))
    return history

def fetch_available_cash(cursor, pf_id):
    record = cursor.execute("""
        SELECT pfc.portfolio_id, SUM(pfc.amount)
        FROM portfolio pf 
        INNER JOIN portfolio_cash pfc ON pf.id = pfc.portfolio_id
        WHERE pf.id = %s
        GROUP BY pfc.portfolio_id
    """, (int(pf_id),)).fetchone()
    return Decimal(0 if record is None else record[1])

def fetch_positions(cursor, pf_id):
    records = cursor.execute("""
        SELECT pfp.portfolio_id, pfp.ticker, SUM(pfp.amount)
        FROM portfolio pf 
        INNER JOIN portfolio_position pfp ON pf.id = pfp.portfolio_id
        WHERE pf.id = %s
        GROUP BY pfp.portfolio_id, pfp.ticker
    """, (int(pf_id),))
    positions = {}
    for record in records:
        if record is None:
            continue
        positions[record[1]] = Decimal(record[2])
    return positions

def fetch_runs(cursor, pf_id):
    records = cursor.execute(f"""
        SELECT {to_columnselect(PortfolioRun, prefix="pfr")}         
        FROM portfolio_run pfr 
        WHERE pfr.portfolio_id = %s
    """, (int(pf_id),))
    runs = []
    for record in records:
        runs.append(PortfolioRun(*record))
    return runs

def fetch_orders_by_status(cursor, pf_id, status):
    records = cursor.execute(f"""
        SELECT {to_columnselect(PortfolioOrder, prefix="pfo")}         
        FROM portfolio pf 
        INNER JOIN portfolio_order pfo ON pf.id = pfo.portfolio_id
        WHERE pf.id = %s
        AND pfo.status = %s
    """, (int(pf_id), status))
    orders = []
    for record in records:
        orders.append(PortfolioOrder(*record))
    return orders

def fetch_portfolio(cursor, author, id):
    record = cursor.execute(f"""
        SELECT {to_columnselect(Portfolio)}   
        FROM portfolio
        WHERE id = %s AND author = %s
    """, (int(id), str(author))).fetchone()
    return Portfolio(*record)

def fetch_portfolios(cursor, author):    
    records = cursor.execute(f"""
        SELECT {to_columnselect(Portfolio)}
        FROM portfolio
        WHERE author = %s
    """, (str(author),))
    pfs = []
    for record in records:
        pfs.append(Portfolio(*record))
    return pfs

def fetch_enabled_portfolios(cursor):    
    records = cursor.execute(f"""
        SELECT {to_columnselect(Portfolio)}
        FROM portfolio
        WHERE enabled = %s
    """, (True,))
    pfs = []
    for record in records:
        pfs.append(Portfolio(*record))
    return pfs
    
def insert_broker(cursor, broker):
    record = cursor.execute("""
        INSERT INTO broker
        (
            author,
            name,
            type,
            credentials
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, 
    (
        str(broker.author), 
        broker.name, 
        broker.type, 
        broker.credentials, 
    )).fetchone()
    return record[0]

def insert_portfolio(cursor, portfolio):
    record = cursor.execute("""
        INSERT INTO portfolio
        (
            author,
            enabled,
            broker_id,
            name,
            shortname,
            module,
            schedule,
            start_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, 
    (
        str(portfolio.author), 
        portfolio.enabled, 
        int(portfolio.broker_id), 
        portfolio.name, 
        portfolio.shortname, 
        portfolio.module, 
        portfolio.schedule, 
        portfolio.start_timestamp,  
    )).fetchone()
    return record[0]

def update_portfolio(cursor, portfolio):
    cursor.execute("""
        UPDATE portfolio
        SET
            enabled = %s,
            last_run_timestamp = %s
        WHERE id = %s
    """, 
    (
        bool(portfolio.enabled),
        portfolio.last_run_timestamp,
        int(portfolio.id)
    ))
    
def insert_run(cursor, run):
    record = cursor.execute("""
        INSERT INTO portfolio_run
        (
            portfolio_id,
            status,
            timestamp,
            error,
            notified
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, 
    (
        int(run.portfolio_id), 
        run.status, 
        run.timestamp, 
        run.error, 
        bool(run.notified)
    )).fetchone()
    return record[0]

def insert_order(cursor, order):
    record = cursor.execute("""
        INSERT INTO portfolio_order
        (
            portfolio_id,
            run_id,
            status,
            ticker,
            side,
            create_timestamp,
            notional,
            quantity,
            notified
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, 
    (
        int(order.portfolio_id), 
        int(order.run_id),
        order.status, 
        order.ticker, 
        order.side, 
        order.create_timestamp, 
        order.notional, 
        order.quantity,
        bool(order.notified)
    )).fetchone()
    return record[0]

def update_run(cursor, run):
    cursor.execute("""
        UPDATE portfolio_run
        SET
            notified = %s
        WHERE id = %s
    """, 
    (
        bool(run.notified), 
        int(run.id)
    ))
    
def update_order(cursor, order):
    cursor.execute("""
        UPDATE portfolio_order
        SET
            status = %s,
            fill_timestamp = %s,
            fill_quantity = %s,
            fill_price = %s,
            fill_fee = %s,
            broker_order_id = %s,
            notified = %s
        WHERE id = %s
    """, 
    (
        order.status, 
        order.fill_timestamp, 
        order.fill_quantity, 
        order.fill_price, 
        order.fill_fee, 
        str(order.broker_order_id) if order.broker_order_id is not None else None, 
        bool(order.notified),
        int(order.id)
    ))
    
def insert_cash(cursor, cash):
    record = cursor.execute("""
        INSERT INTO portfolio_cash
        (
            portfolio_id,
            event,
            event_timestamp,
            amount,
            order_id
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, 
    (
        int(cash.portfolio_id), 
        cash.event,
        cash.event_timestamp,
        cash.amount,
        int(cash.order_id) if cash.order_id is not None else None
    )).fetchone()
    return record[0]
