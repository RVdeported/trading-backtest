from .mdc import MDC_csv
from .omc import OMC
from src.omc import MatchRes, OMC, Order
from src.mdc import MDC_csv
from dataclasses import dataclass, asdict
import numpy as np
from uuid import uuid4
import wandb

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
    def __init__(self, strat):
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
        self.strat_id = strat.id
        self.strat    = strat

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

    def add_val_point(self, mdc, wandb_run = None):
        val = self.balance
        for instr in self.qt.keys():
            line = mdc.get_line(instr=instr, lvls=1)
            val += (line.asks_px[0] + line.bids_px[0]) * 0.5 * self.qt[instr]
        self.profit.append(val)
        self.base.append(self.balance)
        self.ts.append(mdc.ts)

        if wandb_run is not None:
            eval_res = asdict(self.eval(0.0))
            point = {}
            for k, v in eval_res.items():
                point[f"Strat_{self.strat_id+1}/{k}"] = v
            point[f"Strat_{self.strat_id+1}/active_deals"] = len(self.strat.active_deals)
            wandb_run.log(point)



    def eval(self, risk_free_rate = 0.01):
        if len(self.bal_dyn) == 0:
            return StratRes(0.0,0.0,{},0.0,0.0,0.0,[],[])
        exp_ret = np.array(self.profit) / np.max([-np.min(self.base), 100]) - risk_free_rate
        # exp_ret = np.mean(self.profit) / self.profit[-1] - risk_free_rate
        sigm    = np.std(exp_ret)
        sharpe = np.mean(exp_ret) / sigm

        return StratRes(self.profit[-1], 
                        self.profit[-1] / np.max([-np.min(self.base), 20000]), 
                        self.qt, self.balance, self.comm, 
                        sharpe, self.bal_dyn, self.ts)
    

    

    

class Strategy:
    id  = 0
    run = None
    c   = {}
    def __init__(self):
        pass

    def OnTrade(self, res:MatchRes, mdc:MDC_csv, omc:OMC):
        pass

    def OnObUpdate(self, mdc:MDC_csv, omc:OMC, strat_stats:StratStat):
        pass

    def CraeteOrder(self):
        pass

    def OnInit(self, mdc:MDC_csv, omc:OMC, strat_stats:StratStat):
        pass

    def clear_inv(self, mdc:MDC_csv, omc:OMC, stats:StratStat):
        for instr in stats.qt.keys():
            if abs(stats.qt[instr]) < 1e-5:
                continue
            print(f"Clearing {self.id} {instr} {stats.qt[instr]}")
            if stats.qt[instr] > 0:
                omc.compose_order(None,  stats.qt[instr], True,  instr, uuid4().int ,strat_id=self.id)
            else:
                omc.compose_order(None, -stats.qt[instr], False, instr, uuid4().int, strat_id=self.id)


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
        