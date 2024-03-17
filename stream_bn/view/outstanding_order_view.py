

import json
import sys
import time
import inspect
import os
import requests
import websocket
import threading
import signal
import hashlib
import hmac

from enum import Enum
from collections import deque
from datetime import datetime


from stream_bn.common.utils import *


# Unified order view:
# Extendable if we want post orders on different orderbook depth
class OutstandingOrderView:
    def __init__(self):
        self.new_buy = Order(-1.0, 0.0)
        self.new_sell = Order(100000000.0, 0.0)
        self.old_buy = None
        self.old_sell = None


# Single Order Object:
# 1. Order Metadata
# 2. Price
# 3. Quantity
class Order:
    def __init__(self, price, quantity, user_id=None, order_id=None, state=None):
        
        self.price = price
        self.quantity = quantity
        self.user_id = user_id
        self.order_id = order_id
        self.state = OrderStatus.NO_ORDER if state is None else state
        self.lock = threading.Lock()
        self.last_timestamp = int(time.time() * 1000)

    
    @staticmethod
    def create_from(new_obj):
        return Order(
            new_obj[0],
            new_obj[1],
            user_id=new_obj[2],
            order_id=new_obj[3],
            state=new_obj[4]
        )

    def get_last_timestamp(self):
        with self.lock:
            return self.last_timestamp


    #### Getters and Setters : Except for set_state as that's the state machine's job
    def get_order(self):
        with self.lock:
            order_obj = [self.price, self.quantity, self.user_id, self.order_id, self.state]
        return order_obj

    def get_state(self):
        with self.lock:
                state = self.state
        return state
    
    def get_price(self):
        with self.lock:
            price = self.price
        return price

    def set_price(self, price):
        with self.lock:
            self.price = price

    def get_quantity(self):
        with self.lock:
            quantity = self.quantity
        return quantity

    def set_quantity(self, quantity):
        with self.lock:
            self.quantity = quantity

    def get_user_id(self):
        with self.lock:
            return self.user_id

    def set_user_id(self, user_id):
        with self.lock:
            self.user_id = user_id

    def get_order_id(self):
        with self.lock:
            return self.order_id

    def set_order_id(self, order_id):
        with self.lock:
            self.order_id = order_id

    ## On new order confirmation or old order cancellation
    def delete(self):
        self.price = None
        self.quantity = None
        self.order_id = None
        self.user_id = None
        self.state = OrderStatus.NO_ORDER
    
    def delete_thread_safe(self):
        with self.lock:
            self.price = None
            self.quantity = None
            self.order_id = None
            self.user_id = None
            self.state = OrderStatus.NO_ORDER
            

    ###### Status State Machine
    def open_sent(self, user_id, price, quantity):
        with self.lock:
            if self.state == OrderStatus.NO_ORDER:
                self.user_id = user_id
                self.price = price
                self.quantity = quantity
                self.state = OrderStatus.NEW_IN_TRANSIT
                
            else:
                print(f"Attempted open_sent on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                sys.exit()

    def open_confirmed(self, order_id):
        with self.lock:
            
            if self.state == OrderStatus.NEW_IN_TRANSIT:
                self.state = OrderStatus.NEW_CONFIRMED
                self.order_id = order_id
            else:
                print(f"Attempted open_confirmed on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                sys.exit()

    def cancel_sent(self):
        with self.lock:
            if self.state == OrderStatus.NEW_CONFIRMED or self.state == OrderStatus.PARTLY_EXECUTED:
                self.state = OrderStatus.CANCELED_IN_TRANSIT
            else:
                print(f"Attempted cancel_sent on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                sys.exit()

    def cancel_confirmed(self):

        with self.lock:
            if self.state == OrderStatus.CANCELED_IN_TRANSIT:
                self.state = OrderStatus.NO_ORDER
                self.delete()
            else:
                print(f"Attempted cancel_confirmed on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                #sys.exit()

    def executed(self):
        with self.lock:
            if self.state == OrderStatus.NEW_CONFIRMED or self.state == OrderStatus.PARTLY_EXECUTED:
                self.state = OrderStatus.NO_ORDER
                self.delete()
                self.last_timestamp = int(time.time() * 1000)
            else:
                print(f"Attempted executed on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                sys.exit()

    def partly_executed(self, filled_quantity):
        with self.lock:
            if self.state == OrderStatus.NEW_CONFIRMED or self.state == OrderStatus.PARTLY_EXECUTED:
                self.state = OrderStatus.PARTLY_EXECUTED
                self.quantity -= filled_quantity
                self.last_timestamp = int(time.time() * 1000)
            else:
                print(f"Attempted executed on order status {self.state} with user id: {self.user_id} and order id: {self.order_id}")
                sys.exit()

    def set_state_secret(self, state):
        with self.lock:
            self.state = state

