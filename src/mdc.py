import pandas as pd
import numpy as np
from pathlib import Path
import os
from dataclasses import dataclass

@dataclass
class Row:
    ts     :int
    asks_qt:np.array
    bids_qt:np.array
    asks_px:np.array
    bids_px:np.array
    name   :str


class CSV_OB:
    def __init__(self, ds_path, config:dict, label = "NoLabel"):
        self.df     = pd.read_csv(ds_path).iloc[:, :60]
        self.n_ts   = config["ts_name"]
        self.n_qt_a = config["dep_name"].replace("[SIDE]", "ask").replace("[QT-PX]", "qt")
        self.n_qt_b = config["dep_name"].replace("[SIDE]", "bid").replace("[QT-PX]", "qt")
        self.n_px_a = config["dep_name"].replace("[SIDE]", "ask").replace("[QT-PX]", "px")
        self.n_px_b = config["dep_name"].replace("[SIDE]", "bid").replace("[QT-PX]", "px")
        self.config = config
        self.name   = label

        self.df[self.n_ts] = pd.to_datetime(self.df[self.n_ts], format = config["ts_format"]).astype('int64')
        self.df.sort_values(self.n_ts, ascending=True, inplace=True, ignore_index=True)
        self.i     = 1
        self.old_i = 0
        self.ts = config["start_ts"]
        self.finished = False
        self.finished = not self.actuate()
        self.upded    = False
        
     
    def actuate(self) -> bool:
        if self.finished:
            return False
        while(self.df.loc[self.i, self.n_ts] <= int(self.ts)):
            self.i += 1
            if self.i == self.df.shape[0]:
                return False
        return True

    def get_line(self, lvls=1) -> Row:
        a = np.zeros((4,lvls), dtype=np.float32)
        for i in range(lvls):
            a[0, i] = self.df.loc[self.i-1, self.n_qt_a.replace("[NUM]", str(i+1))]
            a[1, i] = self.df.loc[self.i-1, self.n_qt_b.replace("[NUM]", str(i+1))]
            a[2, i] = self.df.loc[self.i-1, self.n_px_a.replace("[NUM]", str(i+1))]
            a[3, i] = self.df.loc[self.i-1, self.n_px_b.replace("[NUM]", str(i+1))]

        return Row(self.df.loc[self.i-1, self.n_ts], a[0], a[1], a[2], a[3], self.name)
    
    def move(self, dt_ms:int) -> bool:
        if self.finished:
            return False
        self.ts += dt_ms*1_000_000
        old_i = self.i
        self.finished = not self.actuate()
        self.upded = old_i != self.i
        if self.upded: 
            self.old_i = old_i 
        return self.upded
        
        
class MDC_csv:
    def __init__(self, folder_path:str, config:dict):   
        self.dfs:CSV_OB = []
        self.names = {}

        tables = [Path(folder_path, n) for n in os.listdir(folder_path)]
        names  = [n[n.find("_")+1:n.find("_Bi")] for n in os.listdir(folder_path)]
        for i in range(len(tables)):
            self.dfs.append(CSV_OB(tables[i], config["Dataset"], names[i]))
            self.names[names[i]] = i
        
        self.ts    = config["Dataset"]["start_ts"]
        

    def update(self, dt_ms:int):
        self.ts += dt_ms * 1_000_000
        upded = 0
        for ob in self.dfs:
            if ob.finished:
                return -1
            upded += int(ob.move(dt_ms))
        return upded
    
    def get_upded_labels(self):
        res = []
        for ob in self.dfs:
            if ob.upded:
                res.append(ob.name)
        return res

    def get_all_lines(self, lvls=2, only_upded=False):
        res = []
        for ob in self.dfs:
            if (ob.upded and only_upded) or\
                (not only_upded):
                res.append(ob.get_line(lvls))
        return res
    
    def get_line(self, instr:str, lvls=2):
        return self.dfs[self.names[instr]].get_line(lvls)
    
    def get_vwap(self, instr:str, lvls=5):
        line = self.get_line(instr, lvls)
        qty_a = np.sum(line.asks_qt)
        qty_b = np.sum(line.bids_qt)
        qty   = np.min([qty_a, qty_b])

        qty_a = qty
        amnt  = 0.0
        lvl = 0
        while True:
            qt = np.min([line.asks_qt[lvl], qty_a])
            amnt += line.asks_px[lvl] * qt
            qty_a -= qt
            lvl += 1

            if qty_a <= 0.0 or lvl == lvls:
                break
        
        qty_a = qty
        lvl = 0
        while True:
            qt = np.min([line.bids_qt[lvl], qty_a])
            amnt += line.bids_px[lvl] * qt
            qty_a -= qt
            lvl += 1

            if qty_a <= 0.0 or lvl == lvls:
                break

        return amnt / (qty * 2)

        
    

    
