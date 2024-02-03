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
# db = CSV_OB(Path("/Users/ugrek/Desktop/PairTrade/Binance_BTCUSD_11_01.csv"), a['Dataset'])
# strat = [
#     # JonRev(clear_every_step=1021600, active_step=5,  num_pairs=6, w_size=1800,  recal_steps=30, spread=0.005, z=1.4, cooldown_ms=1200_000, order_due_ms=3600_000, trade_amnt=1000),
#     # JonRev(clear_every_step=1021600, active_step=5,  num_pairs=6, w_size=1800,  recal_steps=30, spread=0.005, z=1.8, cooldown_ms=1200_000, order_due_ms=3600_000, trade_amnt=1000),
#     # JonRev(clear_every_step=1021600, active_step=5,  num_pairs=6, w_size=3600,  recal_steps=30, spread=0.005, z=1.4, cooldown_ms=1200_000, order_due_ms=3600_000, trade_amnt=1000),
#     JonRev(max_trend_allowed=-1, clear_every_step=480, active_step=2,  num_pairs=12, w_size=600,  recal_steps=5, spread=0.007, z=2.0, cooldown_ms=60_000, order_due_ms=10800_000, trade_amnt=1000),
#     JonRev(max_trend_allowed=-1, clear_every_step=600, active_step=2,  num_pairs=12, w_size=600,  recal_steps=5, spread=0.005, z=2.0, cooldown_ms=60_000, order_due_ms=10800_000, trade_amnt=1000),
#     JonRev(max_trend_allowed=-1, clear_every_step=480, active_step=2,  num_pairs=12, w_size=1200, recal_steps=5, spread=0.007, z=2.0, cooldown_ms=60_000, order_due_ms=10800_000, trade_amnt=1000),
#     JonRev(max_trend_allowed=-1, clear_every_step=600, active_step=2,  num_pairs=12, w_size=1200, recal_steps=5, spread=0.005, z=2.0, cooldown_ms=60_000, order_due_ms=10800_000, trade_amnt=1000),
# ]

VTE_PATH_1 = "../data/out_1400_30s.csv"
VTE_PATH_2 = "../data/out_700_60s.csv"

strat = [
    SMART(VTE_PATH_1, k=2.0, order_due_ms=1800_000, pair_colld_ms=1_000, enter_due_ms=60_000, spread=0.005, clear_every_step=240_000),
    SMART(VTE_PATH_1, k=3.0, order_due_ms=1800_000, pair_colld_ms=1_000, enter_due_ms=60_000, spread=0.005, clear_every_step=240_000),
    SMART(VTE_PATH_2, k=2.0, order_due_ms=1800_000, pair_colld_ms=1_000, enter_due_ms=60_000, spread=0.005, clear_every_step=240_000),
    SMART(VTE_PATH_2, k=3.0, order_due_ms=1800_000, pair_colld_ms=1_000, enter_due_ms=60_000, spread=0.005, clear_every_step=720_000)
]
backtest = Backtest("./configs/test.yml", "./data_3_fin/", strat, "pair_trade")

i = 0
while (backtest.step()):
    if i % 10000 == 0:
        print(i)
    i += 1
backtest.run.finish()