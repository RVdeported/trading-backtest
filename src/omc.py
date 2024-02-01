from dataclasses import dataclass
from src.mdc import MDC_csv
from enum import Enum
import numpy as np

@dataclass
class Order:
    px      :float
    qt      :float
    side_ask:bool
    name    :str
    ts      :int
    id      :int
    strat_id:int 
    FoK     :bool = False    # or partial fill or kill
    exec_time:int = -1  

class ExecStatus(Enum):
    FILLED   = 0
    PARTFILL = 1
    EXPIRED  = 2
    CANCELED = 3
    QUEUED   = 4
    OTHER    = 999


@dataclass
class MatchRes:
    amnt    :float
    qt      :float
    comm    :float
    order   :Order
    status  :ExecStatus = ExecStatus.OTHER
    ts      :int        = 0

class MatchMode(Enum):
    DEPTH   = 0
    CANDLE  = 1
    

class OMC:
    def __init__(self, config:dict, mdc:MDC_csv, names:dict):
        self.orders    = []
        self.mdc       = mdc
        self.names_idx = names
        self.lvl       = config["lvl"]
        self.lat       = config["latency"] * 1_000_000
        self.m_comm    = config["maker_comm"]
        self.t_comm    = config["taker_comm"]
        self.eps_ts    = config["ts_step_ms"]
        self.mode      = MatchMode.CANDLE if config["match_mode"] == "candle" else MatchMode.DEPTH

    def compose_order(self, px, qt, 
                      side_ask, name, id=0, 
                      FoK = False, strat_id = 0,
                      exec_time = -np.inf):
        self.input_order(Order(
            px, qt, side_ask, name, self.mdc.ts + self.lat, id, strat_id, FoK, exec_time
        ))

    def input_order(self, order:Order):
        self.orders.append(order)

    def cancel_order(self, id:int):
        for i in range(len(self.orders)-1, -1):
            if self.orders[i].id == id:
                return MatchRes(0, 0, 0.0, self.ordersp[i], ExecStatus.CANCELED, self.mdc.ts)

    def match_orders(self, curr_ts:int):
        if self.mode == MatchMode.CANDLE:
            return self.match_orders_candle(curr_ts)
        else: 
            return self.match_orders_depth(curr_ts)

    def match_orders_candle(self, curr_ts:int):
        res       = []
        to_remove = []
        for j, order in enumerate(self.orders):
            if order.ts > curr_ts + self.eps_ts:
                continue
            
            line  = self.mdc.get_line(order.name, self.lvl)
            if order.px is None:
                order.px = line.asks_px[0]
            row   = self.mdc.get_candle(order.name)
            qt   = order.qt
            comm_r = self.t_comm
            amnt = 0.0
            comm = 0.0
            i = 0
            if order.side_ask:
                if row.high >= order.px:
                    loc_qt = qt 
                    amnt += loc_qt * order.px
                    comm += amnt * comm_r
                    qt   -= qt
            else:
                if row.low <= order.px:
                    loc_qt = qt 
                    amnt += loc_qt * order.px
                    comm += amnt * comm_r
                    qt   -= qt
            traded_qt = order.qt - qt
            order.qt  = qt
            if order.qt == 0.0: 
                to_remove.append(j)
                res.append(MatchRes(amnt, traded_qt, comm, order, 
                                    ExecStatus.FILLED,
                                    self.mdc.ts))
            if ((order.FoK or 
                    (order.exec_time > 0 and curr_ts - order.ts > order.exec_time))
                    and order.qt != 0.0):
                res.append(MatchRes(0.0, 0.0, 0.0, order, ExecStatus.EXPIRED, self.mdc.ts))
                to_remove.append(j)

        to_remove.reverse()
        for i in to_remove:
            del(self.orders[i])

        return res

    def match_orders_depth(self, curr_ts:int):
        res       = []
        to_remove = []
        for j, order in enumerate(self.orders):
            if order.ts > curr_ts:
                continue
            row  = self.mdc.get_line(order.name, self.lvl)
            
            qt   = order.qt
            comm_r = self.t_comm if order.ts - curr_ts < self.eps_ts else self.m_comm
            amnt = 0.0
            comm = 0.0
            i = 0
            if order.side_ask:
                while qt > 0.0:
                    if order.px is None:
                        order.px = row.bids_px[0]
                    if row.bids_px[i] < order.px:
                        break
                    
                    loc_qt = np.min([qt, row.bids_qt[i]]) 
                    amnt += loc_qt * row.bids_px[i]
                    comm += amnt * comm_r
                    qt   -= loc_qt
                    i += 1
                    if i == self.lvl: break
            else:
                while qt > 0.0:
                    if row.asks_px[i] > order.px:
                        break
                    if order.px is None:
                        order.px = row.asks_px[0]
                    loc_qt = np.min([qt, row.asks_qt[i]]) 
                    amnt += loc_qt * row.asks_px[i]
                    comm += amnt * comm_r
                    qt   -= loc_qt
                    i += 1
                    if i == self.lvl: break
            traded_qt = order.qt - qt
            order.qt  = qt
            if i != 0:
                if order.qt == 0.0: 
                    to_remove.append(j)
                res.append(MatchRes(amnt, traded_qt, comm, order, 
                                    ExecStatus.FILLED if qt == 0.0 else ExecStatus.PARTFILL,
                                    self.mdc.ts))
            if ((order.FoK or 
                    (order.exec_time > 0 and curr_ts - order.ts > order.exec_time))
                    and order.qt != 0.0):
                res.append(MatchRes(0.0, 0.0, 0.0, order, ExecStatus.EXPIRED, self.mdc.ts))
                to_remove.append(j)
            
        
        to_remove.reverse()
        for i in to_remove:
            del(self.orders[i])

        return res

    def get_orders(self, strat_id):
        return [n for n in self.orders if n.strat_id == strat_id]
    

