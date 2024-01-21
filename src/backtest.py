import pandas as pd
import numpy as np
import yaml
from src.mdc import MDC_csv
from src.omc import OMC, MatchRes
from src.strategy import Strategy, StratStat
from dataclasses import dataclass
import wandb

def read_yaml(path):
    with open(path, "r") as stream:
        try: return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None


class Backtest:
    def __init__(self, config_path, data_folder, strategies:list[Strategy], wandb_project = None):
        self.c      = read_yaml(config_path)
        self.mdc    = MDC_csv(data_folder, self.c)
        self.strats = strategies # strategies 
        self.omc    = OMC(self.c["OMC"], self.mdc, self.mdc.names)

        self.ts     = self.mdc.ts
        self.strat_stats = [StratStat(i) for i in range(len(self.strats))]
        self.iter   = 1
        self.run = None 
        if wandb_project is not None:
            self.run = wandb.init(wandb_project, config = self.c)

        for i, n in enumerate(self.strats):
            n.id = i
            self.run.config[f"config_{i+1}"] = n.c
            n.run = self.run

    def step(self):
        ts_step_ms = self.c["OMC"]["ts_step_ms"]
        if self.mdc.update(ts_step_ms) == -1:
            print("One or more instruments inactive, stopping...")
            return False
        self.ts += ts_step_ms * 1_000_000
        matched = self.omc.match_orders(self.ts)
        for res in matched:
            self.strats[res.order.strat_id].OnTrade(res, self.mdc, self.omc)
            self.strat_stats[res.order.strat_id].add_res(res)
            
        for i, strat in enumerate(self.strats):
            strat.OnObUpdate(self.mdc, self.omc, self.strat_stats[i])
        if self.iter % self.c["Assumptions"]["eval_every"] == 0:
            for i, stat in enumerate(self.strat_stats):
                stat.add_val_point(self.mdc, self.run)
        self.iter += 1

        return True



    

