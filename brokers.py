from collections import namedtuple
from decimal import Decimal
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, OrderStatus, TimeInForce
import alpaca_trade_api as tradeapi
import pandas as pd
from model import PortfolioOrder

class Broker:
    def __init__(self, log, credentials):
        self.log = log
        self.credentials = credentials

    def positions(self, portfolio):
        raise NotImplementedError()

    def resolve_orders(self, portfolio, open_orders):
        raise NotImplementedError()
    
    def submit_orders(self, portfolio, orders):
        raise NotImplementedError()
    
AlpacaOrderShim = namedtuple("OrderShim", ["symbol", "client_order_id", "side", "filled_qty", "filled_avg_price", "status"])
def to_order(order_series):
    return AlpacaOrderShim(
        order_series.loc["symbol"],
        order_series.loc["client_order_id"], 
        order_series.loc["side"],
        order_series.loc["filled_qty"],
        order_series.loc["filled_avg_price"],
        order_series.loc["status"],
    )
class AlpacaBroker(Broker):
    def __init__(self, log, credentials):
        super().__init__(log, credentials)
        self.trading_client = TradingClient(credentials["api_key"], credentials["secret_key"], paper=credentials["paper"])
        self.rest_api = tradeapi.REST(
            credentials["api_key"], 
            credentials["secret_key"], 
            "https://paper-api.alpaca.markets" if credentials["paper"] else "https://api.alpaca.markets"
        )
        
    def client_order_prefix(self, portfolio):
        return "%s_%d" % (portfolio.shortname, portfolio.id)

    def client_order_id(self, portfolio, order):
        return "%s_%d" % (self.client_order_prefix(portfolio), order.id)
    
    # From: https://alpaca.markets/learn/get-all-orders/
    def all_orders(self):
        CHUNK_SIZE = 500
        all_orders = []
        start_time = pd.to_datetime('now', utc=True)
        check_for_more_orders = True

        while check_for_more_orders:
            # Fetch a 'chunk' of orders and append it to our list
            api_orders = self.rest_api.list_orders(
                status='all',
                until=start_time.isoformat(),
                direction='desc',
                limit=CHUNK_SIZE,
                nested=False,
            )
            all_orders.extend(api_orders)

            if len(api_orders) == CHUNK_SIZE:
                # Since length equals the CHUNK_SIZE there may be more orders
                # Set the ending timestamp for the next chunk of orders
                # A hack to work around complex orders having the same submitted_at time
                # and avoid potentially missing one, is to get more than we need
                start_time = all_orders[-3].submitted_at
            else:
                # That was the final chunk so exit
                check_for_more_orders = False

        # Convert the list into a dataframe and drop any duplicate orders
        orders_df = pd.DataFrame([order._raw for order in all_orders])
        orders_df.drop_duplicates('id', inplace=True)
        return orders_df

    def orders(self, portfolio):
        orders = []
        for i, order_series in self.all_orders().iterrows():
            order = to_order(order_series)
            if not order.client_order_id.startswith(self.client_order_prefix(portfolio)):
                continue
            orders.append(order)
        return orders

    def filled_orders(self, portfolio):
        return list(filter(lambda order: order.status in ("filled", "partially_filled"), self.orders(portfolio)))

    def positions(self, portfolio):
        positions = {}
        for order in self.filled_orders(portfolio):
            ticker = order.symbol
            if ticker not in positions:
                positions[ticker] = Decimal(0)
            if order.side == OrderSide.BUY:
                positions[ticker] += Decimal(order.filled_qty)
            elif order.side == OrderSide.SELL:
                positions[ticker] -= Decimal(order.filled_qty)
        return positions

    def resolve_orders(self, portfolio, open_orders):
        resolved_orders = []
        for open_order in open_orders:
            client_order_id = self.client_order_id(portfolio, open_order)
            try:
                self.log.info(f"Looking up order {client_order_id} on Alpaca...")
                alpaca_order = self.trading_client.get_order_by_client_id(client_order_id)
            except: 
                self.log.exception(f"Exception looking up order {client_order_id}!")
                alpaca_order = None
            is_resolved = alpaca_order is None or (alpaca_order.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED))
            if is_resolved:
                if alpaca_order is None or (alpaca_order.filled_qty is None or alpaca_order.filled_qty == "" or float(alpaca_order.filled_qty) == 0):
                    status = "unfilled"
                    fill_timestamp = None
                    fill_quantity = None
                    fill_price = None
                    fill_fee = None
                    alpaca_id = None
                else:
                    status = "filled"
                    fill_timestamp = alpaca_order.filled_at
                    fill_quantity = alpaca_order.filled_qty
                    fill_price = alpaca_order.filled_avg_price
                    fill_fee = "0"
                    alpaca_id = str(alpaca_order.id)

                resolved_orders.append(PortfolioOrder(
                    open_order.id,
                    open_order.portfolio_id,
                    open_order.run_id,
                    status,
                    open_order.ticker,
                    open_order.side,
                    open_order.create_timestamp,
                    open_order.notional,
                    open_order.quantity,
                    fill_timestamp,
                    fill_quantity,
                    fill_price,
                    fill_fee,
                    alpaca_id,
                    False
                ))
        return resolved_orders

    def submit_orders(self, portfolio, orders):
        for order in orders:
            client_order_id = self.client_order_id(portfolio, order)
            self.log.info(f"Submitting order {client_order_id} to Alpaca...")
            # Buy the notional ($) amount
            if order.side == "buy":
                order_data = MarketOrderRequest(
                    symbol=order.ticker,
                    notional=order.notional,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_order_id
                )
            # Sell the quantity (shares) amount
            elif order.side == "sell":
                order_data = MarketOrderRequest(
                    symbol=order.ticker,
                    qty=order.quantity,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_order_id
                )
            self.trading_client.submit_order(order_data=order_data)