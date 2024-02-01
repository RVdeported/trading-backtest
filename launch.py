import yaml
from src.mdc import CSV_OB, MDC_csv
from src.omc import Order
from src.backtest import Backtest
from pathlib import Path
from src.strategy import BaseStrat
from matplotlib import pyplot as plt
from src.strats.jonrev import JonRev
from src.strats.smart import SMART
import datetime as dt

with open("./configs/test.yml", 'r') as f:
    a = yaml.safe_load(f)

strat = [
    SMART("../data/out_20k_5p.csv", k=2.0, order_due_ms=5400_000,  pair_colld_ms=300_000, enter_due_ms=60_000,  spread=0.005, clear_every_step=240_000),
    SMART("../data/out_20k_5p.csv", k=2.0, order_due_ms=10800_000, pair_colld_ms=300_000, enter_due_ms=60_000, spread=0.005, clear_every_step=240_000),
    SMART("../data/out_20k_5p.csv", k=2.5, order_due_ms=10800_000, pair_colld_ms=300_000, enter_due_ms=60_000,  spread=0.005, clear_every_step=240_000),
    SMART("../data/out_20k_5p.csv", k=3.0, order_due_ms=10800_000, pair_colld_ms=300_000, enter_due_ms=60_000, spread=0.005, clear_every_step=720_000)
]
backtest = Backtest("./configs/test.yml", "./data_3_fin/", strat, "pair_trade")
sharpe = [[] for n in backtest.strat_stats]

i = 0
while (backtest.step()):
    if i % 10000 == 0:
        print(i)
    i += 1
backtest.run.finish()