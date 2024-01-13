import pandas as pd
import numpy as np
import yaml
from src.mdc import MDC_csv, OMC


def read_yaml(path):
    with open(path, "r") as stream:
        try: return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None
            

class Backtest:
    def __init__(self, config_path, data_folder):
        self.c      = read_yaml(config_path)
        self.mdc    = MDC_csv(data_folder, self.c)
        self.strats = [] # strategies 
        self.omc    = OMC(self.c["OMC"], self.mdc, self.mdc.names)




