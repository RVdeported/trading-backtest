from ..omc import MatchRes
from ..mdc import MDC_csv
from ..omc import OMC, ExecStatus
from ..strategy import StratStat
from src.strategy import Strategy, MDC_csv, OMC, dataclass
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import numpy as np
from uuid import uuid4

@dataclass
class Deal:
    id_enter:int
    id_exit:int
    instr:str
    ts:int
    enter_status: ExecStatus = ExecStatus.QUEUED
    exit_status : ExecStatus = ExecStatus.QUEUED
    closed_ts   :int         = 0
     

@dataclass
class Pair:
    name1     :int   
    name2     :int   
    mean      :float  
    std       :float   
    ratio     :float
    last_trade:int   = 0


class JonRev(Strategy):
    def __init__(self, 
                 num_pairs    = 3,
                 z            = 3.0, 
                 w_size       = 5000, 
                 recal_steps  = 100, 
                 order_due_ms = 15000,
                 cooldown_ms  = 2000,
                 trade_amnt   = 100,
                 spread       = 0.005,
                 active_step  = 60,
                 clear_every_step = 100):
        super().__init__()
        self.z = z
        self.w_size      = w_size
        self.recal_s     = recal_steps
        self.o_due       = int(order_due_ms * 1_000_000)
        self.cooldown    = cooldown_ms * 1_000_000
        self.spread      = spread
        self.active_step = active_step
        self.clear_every = clear_every_step

        self.md_buffer         = {}
        self.active_deals      = []
        self.need_to_calibrate = True
        self.step_c            = 0
        self.step_glob         = -1
        self.trade_amnt        = trade_amnt * 0.5
        
        self.num_pairs         = num_pairs
        self.pairs             = []

        self.done_deals        = []
        self.closing_ids       = []



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

                    # line = mdc.get_line(res.order.name, 1)
                    # px = line.bids_px[0] if res.order.side_ask else line.asks_px[0]
                    px = 0.00001 if res.order.side_ask else 99999999.9
                    closing_id = uuid4().int
                    omc.compose_order(px, res.order.qt, res.order.side_ask,
                                      res.order.name, closing_id, strat_id=self.id)
                    self.closing_ids.append(closing_id) 
                    
                deal.closed_ts = mdc.ts
                self.done_deals.append(deal)
                self.active_deals.pop(i)
                break

    def OnObUpdate(self, mdc: MDC_csv, omc: OMC, strat_stats: StratStat):
        self.step_glob += 1
        if self.step_glob % self.active_step != 0:
            return
        # if self.step_glob // self.active_step % self.clear_every == 0:
        #     self.clear_inv(mdc, omc, strat_stats)

        lines = mdc.get_all_lines(lvls=1,only_upded=False)
        for line in lines:
            if line.name not in self.md_buffer.keys():
                self.md_buffer[line.name] = []
            self.md_buffer[line.name].append(mdc.get_vwap(line.name, 4))
            if len(self.md_buffer[line.name]) > self.w_size:
                self.md_buffer[line.name].pop(0)
            
            assert len(self.md_buffer[line.name]) <= self.w_size

        if self.need_to_calibrate:
            self.need_to_calibrate = not self.recalibrate()
            if not self.need_to_calibrate: 
                self.step_c = 0           
        else:
            self.step_c += 1
            self.need_to_calibrate = self.step_c > self.recal_s

        if (len(self.active_deals) == 0) and (len(omc.get_orders(self.id)) == 0):
            self.clear_inv(mdc, omc, strat_stats)

        for pair in self.pairs:
            self.make_orders(pair, mdc, omc)

    def make_orders(self, pair:Pair, mdc: MDC_csv, omc: OMC):
        if mdc.ts - pair.last_trade < self.cooldown: 
            return False
        line1 = mdc.get_line(pair.name1, lvls=2)
        line2 = mdc.get_line(pair.name2, lvls=2)
        if line1.asks_px[1] == 0.0:
            line1.asks_px[1] = line1.asks_px[0]
        if line2.asks_px[1] == 0.0:
            line2.asks_px[1] = line2.asks_px[0]
        if line1.bids_px[1] == 0.0:
            line1.bids_px[1] = line1.bids_px[0]
        if line2.bids_px[1] == 0.0:
            line2.bids_px[1] = line2.bids_px[0]

        px1 = (line1.bids_px[0] + line1.asks_px[0]) * 0.5 
        px2 = (line2.bids_px[0] + line2.asks_px[0]) * 0.5 
        val = px1 + px2 * pair.ratio

        z = (val - pair.mean) / pair.std
        if abs(z) > self.z: 
            deal1    = Deal(uuid4().int, uuid4().int, pair.name1, mdc.ts)
            deal2    = Deal(uuid4().int, uuid4().int, pair.name2, mdc.ts)
            if pair.std > pair.mean:
                print("STD HIGHER THAN MEAN")
                return
            # coef = pair.std * abs(z) / pair.mean * 0.8
            coef = self.spread
            if z > 0:
                sell_px1 = line1.bids_px[0] * 1.0002
                sell_px2 = line2.bids_px[0] * 1.0002
                buy_px1  = sell_px1         * (1 - coef)
                buy_px2  = sell_px2         * (1 - coef)
                # sell_px1 = 0.00000000001
                # sell_px2 = 0.00000000001
                qt1      = self.trade_amnt / max(sell_px1, 0.0001)
                qt2      = self.trade_amnt / max(sell_px2, 0.0001)
                omc.compose_order(buy_px1, qt1, False, pair.name1, deal1.id_exit,    strat_id=self.id, exec_time=self.o_due)
                omc.compose_order(buy_px2, qt2, False, pair.name2, deal2.id_exit,    strat_id=self.id, exec_time=self.o_due)
                omc.compose_order(sell_px1, qt1, True, pair.name1, deal1.id_enter, FoK=True, strat_id=self.id)
                omc.compose_order(sell_px2, qt2, True, pair.name2, deal2.id_enter, FoK=True, strat_id=self.id)
            else:
                # buy_px1  = 99999999999999.9
                buy_px1  = line1.asks_px[0] * 0.9998
                # buy_px2  = 99999999999999.9 
                buy_px2  = line2.asks_px[0] * 0.9998
                sell_px1 = buy_px1 * (1 + coef)
                sell_px2 = buy_px2 * (1 + coef)
                qt1      = self.trade_amnt / max(buy_px1, 0.0001)
                qt2      = self.trade_amnt / max(buy_px2, 0.0001)
                omc.compose_order(buy_px1, qt1, False, pair.name1, deal1.id_enter, FoK=True, strat_id=self.id)
                omc.compose_order(buy_px2, qt2, False, pair.name2, deal2.id_enter, FoK=True, strat_id=self.id)
                omc.compose_order(sell_px1, qt1, True, pair.name1, deal1.id_exit,            strat_id=self.id, exec_time=self.o_due)
                omc.compose_order(sell_px2, qt2, True, pair.name2, deal2.id_exit,            strat_id=self.id, exec_time=self.o_due)

            self.active_deals.append(deal1) 
            self.active_deals.append(deal2)
            pair.last_trade = mdc.ts

    def recalibrate(self):
        instr     = list(self.md_buffer.keys())
        l         = len(instr)
        self.pairs = []
        done = False
        for i in range(l-1):
            if done: break
            for j in range(i+1, l):
                if (len(self.md_buffer[instr[i]]) < self.w_size or 
                    len(self.md_buffer[instr[j]]) < self.w_size):
                    continue
                assert len(self.md_buffer[instr[i]]) == self.w_size
                assert len(self.md_buffer[instr[j]]) == self.w_size
                ds = np.array([self.md_buffer[instr[i]], self.md_buffer[instr[j]]]).T
                if self.check_intgr(ds):
                    mustd = self.get_pair_stats(ds)
                    if mustd is None:
                        continue
                    if np.abs(mustd["std"] / mustd["mean"]) < self.spread / self.z:
                        continue
                    assert instr[i] != instr[j]
                    self.pairs.append(Pair(instr[i], instr[j], **mustd))
                    if len(self.pairs) >= self.num_pairs:
                        done = True
                        break

        return True

   


    @staticmethod
    def check_intgr(arrs:np.array):
        res = coint_johansen(arrs, det_order=0, k_ar_diff=2)
        return res.trace_stat[-1] > res.trace_stat_crit_vals[-1, -1]
    
    @staticmethod
    def get_pair_stats(arrs:np.array):
        if arrs[0, 1] == 0.0:
            return None
        ratio = arrs[0, 0] / arrs[0, 1]
        portf = np.array([n[0] + n[1] * ratio for n in arrs])
        return {"mean" : np.mean(portf), "std": np.std(portf), "ratio":ratio}