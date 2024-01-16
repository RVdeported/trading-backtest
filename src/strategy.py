from .mdc import MDC_csv
from .omc import OMC
from src.omc import MatchRes, OMC, Order
from src.mdc import MDC_csv
from dataclasses import dataclass
import numpy as np
from uuid import uuid4

@dataclass
class StratRes:
    PnL:float       
    Return:float  
    end_qt:dict       
    end_balance:float 
    comm:float
    sharpe:float      
    balance:list      
    ts:list           


        
class StratStat:
    def __init__(self, id):
        self.trade_c  = 0
        self.invested = 0.0
        self.balance  = 0.0
        self.comm     = 0.0

        self.qt       = {}
        self.bal_dyn  = []
        self.ts       = []
        self.qt_dyn   = [{}]
        self.matchRes = []
        self.profit   = []
        self.base     = []
        self.strat_id = id

    def add_res(self, trade_res:MatchRes):
        self.trade_c += 1
        asset = trade_res.order.name
        if asset not in self.qt.keys():
            self.qt[asset] = 0.0
        if trade_res.order.side_ask:
            self.balance   += trade_res.amnt - trade_res.comm
            self.qt[asset] -= trade_res.qt
        else:
            self.balance   -= trade_res.amnt + trade_res.comm
            self.qt[asset] += trade_res.qt
        self.comm += trade_res.comm
        self.ts.append(trade_res.ts)
        self.qt_dyn.append(self.qt.copy())
        self.bal_dyn.append(self.balance)
        self.matchRes.append(trade_res)

    def add_val_point(self, mdc):
        val = self.balance
        for instr in self.qt.keys():
            line = mdc.get_line(instr=instr, lvls=1)
            val += (line.asks_px[0] + line.bids_px[0]) * 0.5 * self.qt[instr]
        self.profit.append(val)
        self.base.append(-1 * np.min(self.balance))
        self.ts.append(mdc.ts)

    def eval(self, risk_free_rate = 0.01):
        if len(self.bal_dyn) == 0:
            return StratRes(0.0,0.0,{},0.0,0.0,0.0,[],[])
        exp_ret = np.mean(self.profit) / self.profit[-1] - risk_free_rate
        sigm    = np.std(np.array(self.profit) - risk_free_rate)

        return StratRes(self.profit[-1], 
                        self.profit[-1] / np.max([np.abs(self.base[-1]), 100]), 
                        self.qt, self.balance, self.comm, 
                        exp_ret / sigm, self.bal_dyn, self.ts)
    

    

    

class Strategy:
    id = 0
    def __init__(self):
        pass

    def OnTrade(self, res:MatchRes, mdc:MDC_csv, omc:OMC):
        pass

    def OnObUpdate(self, mdc:MDC_csv, omc:OMC, strat_stats:StratStat):
        pass

    def CraeteOrder(self):
        pass

    def clear_inv(self, mdc:MDC_csv, omc:OMC, stats:StratStat):
        for instr in stats.qt.keys():
            if abs(stats.qt[instr]) < 1e-5:
                continue
            print(f"Clearing {self.id} {instr} {stats.qt[instr]}")
            if stats.qt[instr] > 0:
                omc.compose_order(0.000001,  stats.qt[instr], True,  instr, uuid4().int ,strat_id=self.id)
            else:
                omc.compose_order(9999999.9, -stats.qt[instr], False, instr, uuid4().int, strat_id=self.id)


class BaseStrat(Strategy):
    def __init__(self, chance_of_trade=0.1, spread = 0.005):
        super().__init__()
        self.r      = chance_of_trade
        self.spread = spread

    def OnObUpdate(self, mdc: MDC_csv, omc: OMC, strat_stats: StratStat):
        if np.random.random() < self.r:
            lines = mdc.get_all_lines(lvls=1)
            for line in lines:
                up = np.random.random() > 0.5
                if up:
                    buy_px = line.asks_px[0]
                    sell_px = buy_px * (1 + self.spread)
                else:
                    sell_px = line.bids_px[0]
                    buy_px  = sell_px * (1 - self.spread)

                omc.compose_order(buy_px, 0.001, True, line.name, strat_id = self.id)
                omc.compose_order(sell_px, 0.001, False, line.name, strat_id = self.id)
        