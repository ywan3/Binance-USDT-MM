from enum import Enum
from collections import deque
from datetime import datetime

import json
import sys
import requests
import websocket
import threading
import time
import inspect
import os
import signal
import hashlib
import hmac
import random


from stream_bn.common.utils import *


### when we say buy we mean swap the more base asset with the more volatile asset
### asset_1 : more volatile asset 
### asset_2 : more base asset
class BalanceSheetView:
    def __init__(self, asset_1_name, asset_1_quantity, asset_2_name, asset_2_quantity, exchange_ratio=None):
        self.asset_1_name = asset_1_name
        self.asset_2_name = asset_2_name
        self.asset_1_quantity = asset_1_quantity
        self.asset_2_quantity = asset_2_quantity
        self.exchange_ratio = exchange_ratio
        self.abort_buy_usdt_threshold = 0.1
        self.abort_sell_usdt_threshold = 0.9
        self.lock = threading.Lock()

    def get_exchange_ratio(self):
        with self.lock:
            ratio = self.exchange_ratio
        return ratio
    
    def set_exchange_ratio(self, new_ratio):
        with self.lock:
            self.exchange_ratio = new_ratio

    def set_balance(self, asset_1_quantity_delta, asset_2_quantity_delta):
        with self.lock:
            self.asset_1_quantity += asset_1_quantity_delta
            self.asset_2_quantity += asset_2_quantity_delta

    def get_balance(self):
        with self.lock:
            bs = [self.asset_1_name, self.asset_2_name]
        return bs
    
    # How much USDC we are trying to dump
    def get_bid_quantity(self):
        with self.lock:
            total_book_value = self.asset_1_quantity * self.exchange_ratio + self.asset_2_quantity
            asset_1_proportion = self.asset_1_quantity * self.exchange_ratio / total_book_value
            asset_2_proportion = self.asset_2_quantity / total_book_value
            quantity = (asset_2_proportion - asset_1_proportion) / 2 * self.asset_2_quantity
        
        return (max(min(265, quantity), 295) + random.randint(-50, 50))

    # How much BTC we are trying to dump
    def get_ask_quantity(self):
        with self.lock:
            total_book_value = self.asset_1_quantity * self.exchange_ratio + self.asset_2_quantity
            asset_1_proportion = self.asset_1_quantity * self.exchange_ratio / total_book_value
            asset_2_proportion = self.asset_2_quantity / total_book_value
            quantity = (asset_1_proportion - asset_2_proportion) / 2 * self.asset_1_quantity
        
        return (max(min(265, quantity), 295) + random.randint(-50, 50))

    def get_asset_1_name(self):
        return self.asset_1_name
    
    def get_asset_2_name(self):
        return self.asset_2_name
    
    def get_asset_proportion(self):
        with self.lock:
            total_book_value = self.asset_1_quantity * self.exchange_ratio + self.asset_2_quantity
            asset_1_proportion = self.asset_1_quantity * self.exchange_ratio / total_book_value
            asset_2_proportion = self.asset_2_quantity / total_book_value
        
        return [asset_1_proportion, asset_2_proportion] # usdt, usd
    