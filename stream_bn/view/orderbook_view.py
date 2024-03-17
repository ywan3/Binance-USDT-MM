from enum import Enum
from collections import deque
from datetime import datetime

import json
import sys
import requests
import websocket
import threading
import signal
import hashlib
import hmac
import time
import inspect
import os

from stream_bn.common.utils import *

PRICE_ACCURACY = 4 # 4 places after decimal
QUANTITY_ACCURACY = 0 # only whole number quantities
PRICE_TICK = 1 / 10 ** PRICE_ACCURACY
QUANTITY_TICK = 1 / 10 ** QUANTITY_ACCURACY

class OrderbookView:

    def __init__(self, bid_p, bid_q, ask_p, ask_q):
        self.bid_p = bid_p
        self.bid_q = bid_q
        self.best_bid_p = bid_p[0]
        self.best_bid_q = bid_q[0]
        
        self.ask_p = ask_p
        self.ask_q = ask_q
        self.best_ask_p = ask_p[0]
        self.best_ask_q = ask_q[0]

        self.correction_p = None
        self.in_buy_range = True 
        self.in_sell_range = True
        self.bid_lock = threading.Lock()
        self.ask_lock = threading.Lock()
        self.correction_lock = threading.Lock()



    def set_best_bid(self, bid_p, bid_q):
        with self.bid_lock:
            self.bid_p[0] = bid_p
            self.bid_q[0] = bid_q
    
    def set_best_ask(self, ask_p, ask_q):
        with self.ask_lock:
            self.ask_p[0] = ask_p
            self.ask_q[0] = ask_q
        

    def get_best_bid(self):
        with self.bid_lock:
            return [self.bid_p[0], self.bid_q[0]]
    
    def get_best_ask(self):
        with self.ask_lock:
            return [self.ask_p[0], self.ask_q[0]]


    def set_bid(self, bid_p, bid_q):
        with self.bid_lock:
            self.bid_p = bid_p
            self.bid_q = bid_q


    def set_ask(self, ask_p, ask_q):
        with self.ask_lock:
            self.ask_p = ask_p
            self.ask_q = ask_q
        

    def get_bid(self):
        with self.bid_lock:
            return [self.bid_p, self.bid_q]

    def get_ask(self):
        with self.ask_lock:
            return [self.ask_p, self.ask_q]


    def get_real_bid(self, our_quantity, our_price):

        if our_price is None:
            # Embark on empty order book
            with self.bid_lock:
                cumulative_quantity = 0
                last_price = -1
                for i in range(len(self.bid_p)):
                    price = self.bid_p[i]
                    quantity = self.bid_q[i]
                    cumulative_quantity += quantity
                    if cumulative_quantity > our_quantity:
                        if price + PRICE_TICK < last_price:
                            return self.bid_p[i + 1] + PRICE_TICK
                        else:
                            return price
                    last_price = price
                return self.bid_p[len(self.bid_p) - 1] + PRICE_TICK

        else:
            # Consider our own impact
            with self.bid_lock:
                cumulative_quantity = 0
                last_price = -1
                for i in range(len(self.bid_p)):
                    price = self.bid_p[i]
                    quantity = self.bid_q[i]
                    cumulative_quantity += quantity
                    if price == our_price:
                        cumulative_quantity -= our_quantity
                    if cumulative_quantity > our_quantity:
                        if price + PRICE_TICK < last_price:
                            return price + PRICE_TICK
                        else:
                            return price
                    last_price = price
                return self.bid_p[len(self.bid_p) - 1] + PRICE_TICK


    def get_real_ask(self, our_quantity, our_price):
        if our_price is None:
            # embark on empty orderbook
            with self.ask_lock:
                cumulative_quantity = 0
                last_price = -1
                for i in range(len(self.ask_p)):
                    price = self.ask_p[i]
                    quantity = self.ask_q[i]
                    cumulative_quantity += quantity
                    if cumulative_quantity > our_quantity:
                        if price + PRICE_TICK < last_price:
                            return price + PRICE_TICK
                        else:
                            return price
                    last_price = price
                return self.ask_p[len(self.ask_p) - 1] - PRICE_TICK
        
        else:
            # consider our own impact
            with self.ask_lock:
                cumulative_quantity = 0
                last_price = -1
                for i in range(len(self.ask_p)):
                    price = self.ask_p[i]
                    quantity = self.ask_q[i]
                    cumulative_quantity += quantity
                    if price == our_price:
                        cumulative_quantity -= our_quantity
                    if cumulative_quantity > our_quantity:
                        if price + PRICE_TICK < last_price:
                            return price + PRICE_TICK
                        else:
                            return price
                    last_price = price
                    
                return self.ask_p[len(self.ask_p) - 1] - PRICE_TICK
    
    
    def get_can_buy(self):
        if self.correction_p is None:
            return True
        with self.correction_lock:
            with self.bid_lock:
                new_state = (self.in_buy_range and (self.correction_p > self.bid_p[0] * 0.999)) or (not self.in_buy_range and (self.correction_p > (self.bid_p[0] + self.ask_p[0]) / 2))
                self.in_buy_range = new_state
                return new_state
    
    def get_can_sell(self):
        if self.correction_p is None:
            return True
        with self.correction_lock:
            with self.ask_lock:
                new_state = (self.in_sell_range and (self.correction_p < self.ask_p[0] * 1.001)) or (not self.in_sell_range and (self.correction_p < (self.bid_p[0] + self.ask_p[0]) / 2))
                self.in_sell_range = new_state
                return new_state

    def set_correction_price(self, price):
        with self.correction_lock:
            self.correction_p = price
            


class MovingAverage:
    def __init__(self, size=50):
        self.size = size
        self.data = deque(maxlen=size)
    
    def add_data(self, value):
        self.data.append(value)
    
    def get_moving_average(self):
        if not self.data:
            return 0
        return sum(self.data) / len(self.data)