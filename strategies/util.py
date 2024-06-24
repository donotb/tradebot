import pandas as pd

def current_1w_signals(signals):
    return current_candle_signals(signals, "W")

def current_1d_signals(signals):
    return current_candle_signals(signals, "D")

def current_4h_signals(signals):
    return current_candle_signals(signals, "4H")

def current_2h_signals(signals):
    return current_candle_signals(signals, "2H")

def current_1h_signals(signals):
    return current_candle_signals(signals, "H")

def current_30m_signals(signals):
    return current_candle_signals(signals, "30min")

def current_15m_signals(signals):
    return current_candle_signals(signals, "15min")

def current_5m_signals(signals):
    return current_candle_signals(signals, "5min")

def current_1m_signals(signals):
    return current_candle_signals(signals, "min")

def current_candle_signals(signals, timeframe):
    if signals is None:
        return None
    current_candle = pd.Timestamp.utcnow().floor(freq=timeframe)
    if current_candle not in signals.index:
        return (signals & False)
    return (signals & False) | signals.loc[[current_candle]]