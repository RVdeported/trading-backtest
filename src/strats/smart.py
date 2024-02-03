from ..omc import MatchRes
from ..mdc import MDC_csv
from ..omc import OMC, ExecStatus
from ..strategy import StratStat
from src.strategy import Strategy, MDC_csv, OMC, dataclass
import numpy  as np
import pandas as pd
import wandb
from uuid import uuid4

SQRT_2 = np.sqrt(2)

@dataclass
class Deal:
    id_enter:int
    id_exit:int
    instr:str
    ts:int
    enter_status: ExecStatus = ExecStatus.QUEUED
    exit_status : ExecStatus = ExecStatus.QUEUED
    closed_ts   :int         = 0
    curr_qt     :float       = 0.0
     

@dataclass
class Pair:
    name1     :int   
    name2     :int   
    mean      :float  
    std       :float   
    ratio     :float
    trade_ts  :int   = 0
    last_trade:int   = 0

class SMART(Strategy):
    def __init__(self, 
                 vte_path,
                 with_mu       = True,
                 num_pairs     = 3,
                 k             = 2.0, 
                 order_due_ms  = 15000,
                 pair_colld_ms = 2000,
                 trade_amnt_usd= 100,
                 spread        = 0.005,
                 active_step   = 60,
                 clear_every_step = 100,
                 enter_due_ms = 120_000,
                 max_pair_ord = 50
                 ):
        

        self.vte         = pd.read_csv(vte_path)
        self.vte["ts"]   = self.vte["ts"].astype(np.int64) * 1e6
        self.vte_i       = 0
        for n in ["ts", "kappa1", "theta1", "resFlag1", "vol1", "ratio1"]:
            assert (n in self.vte.columns)
        if with_mu:
            assert ("mu1" in self.vte.columns)

        self.max_pair_ord = max_pair_ord
        self.with_mu     = with_mu
        self.k           = k
        self.o_due       = int(order_due_ms * 1_000_000)
        self.enter_due   = enter_due_ms * 1_000_000
        self.pair_coold  = pair_colld_ms * 1_000_000
        self.spread      = spread
        self.active_step = active_step
        self.clear_every = clear_every_step
        self.num_pairs         = num_pairs
        self.trade_amnt        = trade_amnt_usd * 0.5
        self.c = {
            "vte_path"         : vte_path,
            "o_due"            : order_due_ms,
            "cooldown_ms"      : pair_colld_ms,
            "trade_amnt"       : trade_amnt_usd,
            "spread"           : spread,
            "active_step"      : active_step,
            "order_due_ms"     : order_due_ms,
            "enter_due_ms"     : enter_due_ms,
            "clear_every_step" : clear_every_step,
            "num_pairs"        : num_pairs,
        }
        
        self.last_deal   = 0

        self.active_deals      = []
        self.step_glob         = -1        
        self.pairs             = []
        self.done_deals        = []
        self.closing_ids       = []

        

        super().__init__()

    def OnInit(self, mdc:MDC_csv, omc:OMC, strat_stats:StratStat):
        assert(len(mdc.names) > 1)
        for i in range(1, len(mdc.names)):
            self.pairs.append(Pair(
                0, i, 0.0, 0.0, self.vte.iloc[0][f"ratio{i}"]))



    def OnTrade(self, res: MatchRes, mdc: MDC_csv, omc:OMC):
        for i, deal in enumerate(self.active_deals):
            if deal.id_enter == res.order.id:
                deal.enter_status = res.status
                if (res.status == ExecStatus.EXPIRED or 
                    res.status == ExecStatus.CANCELED):
                    omc.cancel_order(deal.id_exit)
                    deal.exit_status = ExecStatus.CANCELED

                    deal.closed_ts   = mdc.ts
                    self.done_deals.append(deal)
                    self.active_deals.pop(i)

                break
            elif deal.id_exit == res.order.id:
                deal.exit_status = res.status
                if (res.status == ExecStatus.CANCELED or 
                    res.status == ExecStatus.EXPIRED):
                    line = mdc.get_line(res.order.name, 1)
                    px = line.bids_px[0] if res.order.side_ask else line.asks_px[0]
                    closing_id = uuid4().int
                    omc.compose_order(px, res.order.qt, res.order.side_ask,
                                      res.order.name, closing_id, strat_id=self.id)
                    self.closing_ids.append(closing_id) 
                
                deal.closed_ts = mdc.ts
                self.done_deals.append(deal)
                self.active_deals.pop(i)
                break

    def ActuateVte(self, ts):
        while(self.vte.iloc[self.vte_i].ts < ts):
            if self.vte_i >= self.vte.shape[0]-1:
                print("VTE has ended")
                return False
            else: self.vte_i += 1

        if self.vte_i > 0:
            self.vte_i -= 1
        return self.vte.iloc[self.vte_i].ts < ts

    def OnObUpdate(self, mdc: MDC_csv, omc: OMC, strat_stats: StratStat):
        self.step_glob += 1

        if self.step_glob % self.clear_every == 0:
            self.clear_inv(mdc, omc, strat_stats)

        actuated = self.ActuateVte(mdc.ts)

        if actuated:
            self.MakeOrders(mdc,omc)

        if len(self.active_deals) == 0:
            self.clear_inv(mdc, omc, strat_stats)

    def MakeOrders(self, mdc: MDC_csv, omc: OMC):
        vte_row = self.vte.iloc[self.vte_i]
        lines = mdc.get_all_lines(1, False)
        px_prime = (lines[0].asks_px[0] + lines[0].bids_px[0]) * 0.5
        active_ord = np.zeros(len(self.pairs))
        for deal in self.active_deals:
            active_ord[mdc.names[deal.name2] - 1] += 1
        for i, pair in enumerate(self.pairs):
            if self.pair_coold > mdc.ts - pair.trade_ts:
                continue
            if vte_row[f"resFlag{i+1}"] != 0:
                continue
            if active_ord[i] >= self.max_pair_ord:
                continue

            pair.mean  = vte_row[f"theta{i+1}"]
            pair.ratio = vte_row[f"ratio{i+1}"]
            
            px = (lines[i+1].asks_px[0] + lines[i+1].bids_px[0]) * 0.5
            port_px = px_prime - pair.ratio * px
            crit_val = port_px - pair.mean
            if vte_row[f"kappa{i+1}"] < 1e-7:
                continue
            criterion= self.k * vte_row[f"vol{i+1}"] / (SQRT_2 * np.sqrt(vte_row[f"kappa{i+1}"]))
            # print(f"ts: {mdc.ts} pair: {pair.name1}|{pair.name2} -- mean {pair.mean}, ratio {pair.ratio}\
                #   crit_val {crit_val}, criterion {criterion}")
            if   crit_val > criterion:
                self.ComposePairOrders(pair, px_prime * (1 - 0.0005), px_prime * (1 - self.spread), 
                                             px       * (1 + 0.0005), px       * (1 + self.spread),
                                             omc, False)
            elif crit_val < -criterion:
                self.ComposePairOrders(pair, px_prime * (1 + 0.0005), px_prime * (1 + self.spread), 
                                             px       * (1 - 0.0005), px       * (1 - self.spread),
                                             omc, True)
            # if no trades -> finish
            else: continue    

            pair.trade_ts = mdc.ts
            

    def ComposePairOrders(
            self,
            pair:Pair,
            px_1_enter, px_1_exit,
            px_2_enter, px_2_exit,
            omc:OMC,
            prime_buy:bool = True):
        
        assert((px_1_enter < px_1_exit) == prime_buy)
        assert((px_2_enter > px_2_exit) == prime_buy)
        qt_1 = self.trade_amnt * 0.5 / px_1_enter
        qt_2 = self.trade_amnt * 0.5 / (px_2_enter * pair.ratio)

        strat_str = f"Strat_{self.id+1}"
        wandb.log({
            f"{strat_str}/px_1_enter" : px_1_enter,
            f"{strat_str}/px_2_enter" : px_2_enter,
            f"{strat_str}/px_1_exit" : px_1_exit,
            f"{strat_str}/px_2_exit" : px_2_exit,
            f"{strat_str}/theta" : pair.mean,
        }) 
        enter_id_1 = uuid4().int
        exit_id_1  = uuid4().int
        enter_id_2 = uuid4().int
        exit_id_2  = uuid4().int
        omc.compose_order(px_1_enter, qt_1,not prime_buy, omc.mdc.id_names[0], 
                          enter_id_1, exec_time=self.enter_due, strat_id=self.id)
        omc.compose_order(px_1_exit, qt_1,     prime_buy, omc.mdc.id_names[0], 
                          exit_id_1, exec_time=self.o_due, strat_id=self.id)
        
        omc.compose_order(px_2_enter, qt_2,    prime_buy, omc.mdc.id_names[pair.name2], 
                          enter_id_2, exec_time=self.enter_due, strat_id=self.id)
        omc.compose_order(px_2_exit,  qt_2, not prime_buy, omc.mdc.id_names[pair.name2], 
                          exit_id_2, exec_time=self.o_due, strat_id=self.id)
        
        self.active_deals.append(Deal(enter_id_1, exit_id_1, omc.mdc.id_names[0], omc.mdc.ts))
        self.active_deals.append(Deal(enter_id_2, exit_id_2, omc.mdc.id_names[0], omc.mdc.ts))

