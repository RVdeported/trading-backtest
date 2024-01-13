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
        self.df     = pd.read_csv(ds_path)
        self.n_ts   = config["ts_name"]
        self.n_qt_a = config["dep_name"].replace("[SIDE]", "ask").replace("[QT-PX]", "qt")
        self.n_qt_b = config["dep_name"].replace("[SIDE]", "bid").replace("[QT-PX]", "qt")
        self.n_px_a = config["dep_name"].replace("[SIDE]", "ask").replace("[QT-PX]", "px")
        self.n_px_b = config["dep_name"].replace("[SIDE]", "bid").replace("[QT-PX]", "px")
        self.config = config
        self.name   = label

        self.df[self.n_ts] = pd.to_datetime(self.df[self.n_ts], format = config["ts_format"]).astype('int64')
        self.df.sort_values(self.n_ts, ascending=True, inplace=True, ignore_index=True)
        self.i = 1
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
    
@dataclass
class Order:
    px      :float
    qt      :float
    side_ask:bool
    name    :str
    ts      :int
    id      :int
    FoK     :bool = False    # or partial fill or kill

@dataclass
class MatchRes:
    amnt    :float
    qt      :float
    order   :Order
    partial :bool

class OMC:
    def __init__(self, config:dict, mdc:MDC_csv, names:dict):
        self.orders    = []
        self.mdc       = mdc
        self.names_idx = names
        self.lvl       = config["lvl"]

    def input_order(self, order:Order):
        self.orders.append(order)


    def match_orders(self, curr_ts:int):
        res       = []
        to_remove = []
        for j, order in enumerate(self.orders):
            if order.ts > curr_ts:
                continue
            row  = self.mdc.get_line(order.name, self.lvl)
            qt   = order.qt
            amnt = 0.0
            i = 0
            if order.side_ask:
                while qt > 0:
                    if row.bids_px[i] < order.px:
                        break
                    loc_qt = min(qt, row.bids_qt[i]) 
                    amnt += loc_qt * row.bids_px[i]
                    qt   -= loc_qt
                    i += 1
                    if i == self.lvl: break
            else:
                while qt > 0:
                    if row.asks_px[i] > order.px:
                        break
                    loc_qt = min(qt, row.asks_qt[i]) 
                    amnt += loc_qt * row.asks_px[i]
                    qt   -= loc_qt
                    i += 1
                    if i == self.lvl: break
            traded_qt = order.qt - qt
            order.qt  = qt
            if i != 0:
                if order.qt == 0.0: 
                    to_remove.append(j)
                res.append(MatchRes(amnt, traded_qt, order, qt != 0.0))
            if order.FoK:
                res.append(MatchRes(0.0, 0.0, order, True))
                to_remove.append(j)
            
        
        to_remove.reverse()
        for i in to_remove:
            del(self.orders[i])

        return res


    

