import discord
from discord.ext import tasks
import psycopg
import os
import logging
from model import (
    PortfolioRun,
    PortfolioOrder, 
    update_run,
    update_order,
    fetch_enabled_portfolios,
    fetch_runs,
    fetch_orders_by_status
)

log = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(levelname)s %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

COMMAND_TIMEOUT = 60

DB_CONN_STRING = os.environ.get("TRADEBOT_DB_CONN")
DISCORD_TOKEN = os.environ.get("TRADEBOT_DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
bot = discord.Bot(intents=intents)

def broker_summary(b):
    return f"ID: {b.id}\nName: {b.name}\nType: {b.type}\n"

def portfolio_short_summary(p):
    return f"ID: {p.id}\nName: {p.name}\n"

def portfolio_summary(p):
    return f"ID: {p.id}\nName: {p.name}\nEnabled: {p.enabled}\nModule: {p.module}\nSchedule: {p.schedule}\nStart: {p.start_timestamp}\nLast Run: {p.last_run_timestamp}\n"

def run_summary(r):
    return f"Run ID: {r.id} - {r.status}" + (f"\n{r.error[:1000]}" if r.error else "")

def order_summary(o):
    summary = f"Order ID: {o.id} - {o.side.upper()} " + (f"${o.notional} of {o.ticker}" if o.side == "buy" else f"{o.quantity} of {o.ticker}")
    if o.status == "filled":
        summary += f" (Filled {o.fill_quantity} @ {o.fill_price})"
    elif o.status == "unfilled":
        summary += " (Not Filled)"
    return summary

@tasks.loop(seconds=10)
async def notify_orders():
    with psycopg.connect(DB_CONN_STRING, autocommit=True) as conn:
        with conn.cursor() as cursor:
            portfolios = fetch_enabled_portfolios(cursor)
        for portfolio in portfolios:
            try:
                author = await bot.fetch_user(int(portfolio.author))
                channel = await author.create_dm()

                with conn.cursor() as cursor:
                    new_runs = list(filter(lambda r: not r.notified, fetch_runs(cursor, portfolio.id)))
                    if new_runs:
                        run_descs = [run_summary(r) for r in new_runs]
                        await channel.send("```\n"+portfolio_summary(portfolio)+"\n\nNew Runs:\n- "+"\n- ".join(run_descs)+"```")
                        
                        for run in new_runs:
                            run_list = list(run)
                            run_list[run._fields.index("notified")] = True
                            run = PortfolioRun(*run_list)
                            update_run(cursor, run)
                            
                    new_orders = list(filter(lambda o: not o.notified, 
                        fetch_orders_by_status(cursor, portfolio.id, "open") + fetch_orders_by_status(cursor, portfolio.id, "filled")
                    ))
                    if new_orders:
                        order_descs = [order_summary(o) for o in new_orders]
                        await channel.send("```\n"+portfolio_summary(portfolio)+"\n\nNew Orders:\n- "+"\n- ".join(order_descs)+"```")
                        
                        for order in new_orders:
                            order_list = list(order)
                            order_list[order._fields.index("notified")] = True
                            order = PortfolioOrder(*order_list)
                            update_order(cursor, order)
            except:
                log.exception(f"Exception thrown in 'notify_orders' for portfolio {portfolio.id}")
                
                    
notify_orders.start()
bot.run(DISCORD_TOKEN)
