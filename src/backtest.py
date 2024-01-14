import pandas as pd
import numpy as np
import yaml
from src.mdc import MDC_csv
from src.omc import OMC, MatchRes
from src.strategy import Strategy, StratStat
from dataclasses import dataclass

def read_yaml(path):
    with open(path, "r") as stream:
        try: return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None


class Backtest:
    def __init__(self, config_path, data_folder, strategies:list[Strategy]):
        self.c      = read_yaml(config_path)
        self.mdc    = MDC_csv(data_folder, self.c)
        self.strats = strategies # strategies 
        self.omc    = OMC(self.c["OMC"], self.mdc, self.mdc.names)

        self.ts     = self.mdc.ts
        self.strat_stats = [StratStat(i) for i in range(len(self.strats))]
        self.iter   = 1

        for i, n in enumerate(self.strats):
            n.id = i

    def step(self):
        ts = self.c["Assumptions"]["ts_step_ms"]
        self.mdc.update(ts)
        self.ts += ts * 1_000_000
        matched = self.omc.match_orders(self.ts)
        for res in matched:
            self.strats[res.order.strat_id].OnTrade(res, self.mdc, self.omc)
            self.strat_stats[res.order.strat_id].add_res(res)
            
        for i, strat in enumerate(self.strats):
            strat.OnObUpdate(self.mdc, self.omc, self.strat_stats[i])
        if self.iter % self.c["Assumptions"]["eval_every"] == 0:
            for stat in self.strat_stats:
                stat.add_val_point(self.mdc)
        self.iter += 1



    

