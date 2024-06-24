import vectorbtpro as vbt
import numpy as np
import pandas as pd
from util import current_1h_signals

def create_portfolio(params):
    live = params.get("live", False)
    start = params.get("start", "2021-10")
    end = params.get("end", None)
    btc_ticker = params.get("btc_ticker", "BTC/USD")
    deriv_ticker = params.get("deriv_ticker", "BITO")
    mean_window = params.get("mean_window", 5)
    trade_duration = params.get("trade_duration", 1) # the arb seems to last for only an hour
    
    data = [
        vbt.AlpacaData.fetch(
            btc_ticker,
            timeframe="1 hour",
            adjustment="all",
            start=start,
            end=end,
            client_type="crypto",
        ),
        vbt.AlpacaData.fetch(
            deriv_ticker,
            timeframe="1 hour",
            adjustment="all",
            start=start,
            end=end,
        )
    ]

    close = vbt.AlpacaData.merge(*data).get("Close")
    
    basis = close[btc_ticker] / close[deriv_ticker].ffill() # forward fill close price at end of trading day up til next trading open

    business_days = pd.date_range(start=close.index.min(), end=close.index.max(), freq="B")

    market_open_basis = basis.between_time('12:00', '12:00')
    market_open_basis = market_open_basis[market_open_basis.index.floor('D').isin(business_days)]

    entries = (market_open_basis.vbt > basis.vbt.rolling_mean(mean_window)).vbt.signals.fshift()
    exits = entries.vbt.signals.fshift(trade_duration)
    short_entries = (market_open_basis.vbt < basis.vbt.rolling_mean(mean_window)).vbt.signals.fshift()
    short_exits = short_entries.vbt.signals.fshift(trade_duration)

    pf = vbt.Portfolio.from_signals(
        close[deriv_ticker],
        entries=current_1h_signals(entries) if live else entries,
        exits=current_1h_signals(exits) if live else exits,
        short_entries=current_1h_signals(short_entries) if live else short_entries,
        short_exits=current_1h_signals(short_exits) if live else short_exits,
        freq="1h",
        size_type="valuepercent",
        size=1,
        min_size=0.01,
        cash_sharing=True,
        call_seq="auto",
        attach_call_seq=True,
        **params.get("pf_kwargs", {})
    )
    return pf
