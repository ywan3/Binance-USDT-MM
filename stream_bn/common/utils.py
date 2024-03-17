import threading
from enum import Enum
import types
import functools
from collections import deque
from datetime import datetime, timedelta
import time
import json


class RateLimiter:

    config_path = "./stream_bn/config/binance_config.json"
    with open("./stream_bn/config/binance_config.json", 'r') as file:
            data = json.load(file)  
    call_times = deque()
    max_call_num = data['order_rate_limit']['max_call_num']
    max_call_window_in_seconds = data['order_rate_limit']['call_interval_in_seconds']
    
    def __init__(self):
        pass
        

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            # Remove timestamps older than 10 seconds from the current time
            while RateLimiter.call_times and datetime.now() - RateLimiter.call_times[0] > timedelta(seconds=RateLimiter.max_call_window_in_seconds):
                RateLimiter.call_times.popleft()

            # Skip the call if max_call_num have been reached within the last 10 seconds
            if len(RateLimiter.call_times) >= RateLimiter.max_call_num:
                print(f"Function '{func.__name__}' skipped; call limit reached.")
                return None

            # Otherwise, proceed with the function call
            RateLimiter.call_times.append(datetime.now())
            print(f"Function '{func.__name__}' called {len(RateLimiter.call_times)} times in the last 10 seconds.")
            return func(*args, **kwargs)

        return wrapper



class ReadWriteLock:
    def __init__(self):
        self._readers = 0
        self._readers_lock = threading.Lock()
        self._resource_lock = threading.Lock()

    def acquire_read(self):
        with self._readers_lock:
            self._readers += 1
            if self._readers == 1:
                self._resource_lock.acquire()

    def release_read(self):
        with self._readers_lock:
            self._readers -= 1
            if self._readers == 0:
                self._resource_lock.release()

    def acquire_write(self):
        self._resource_lock.acquire()

    def release_write(self):
        self._resource_lock.release()

class OrderStatus(Enum):
    NO_ORDER = 1
    NEW_IN_TRANSIT = 2
    NEW_CONFIRMED = 3
    CANCELED_IN_TRANSIT = 4
    CANCELED_CONFIRMED = 5
    PARTLY_EXECUTED = 6


class CorrectionFeedPrediction(Enum):
    MARKET_MAKING_RANGE = 1
    BUY_SIGNAL_RANGE = 2
    SELL_SIGNAL_RANGE = 3


class BinancePayloadSample:
    
    
    buy_sample = {
        "id": "",
        "method": "order.place",
        "params": {
            "symbol": "USDTUSD",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.0001",
            "price": "0",
            "signature": "FILL ME"
        }
    }

    sell_sample = {
        "id": "",
        "method": "order.place",
        "params": {
            "symbol": "USDTUSD",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.0001",
            "price": "1000000",
            "signature": "FILL ME"
        }
    }
    
    buy_sample_test = {
        "id": "",
        "method": "order.test",
        "params": {
            "symbol": "USDTUSD",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.0001",
            "price": "0",
            "signature": "FILL ME"
        }
    }

    sell_sample_test = {
        "id": "",
        "method": "order.test",
        "params": {
            "symbol": "USDTUSD",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.0001",
            "price": "1000000",
            "signature": "FILL ME"
        }
    }

    cancel_sample = {
        "id": "",
        "method": "order.cancel",
        "params": {
            "symbol": "USDTUSD",
            "orderId": "",
            "apiKey": "",
            "signature": "",
            "timestamp": ""
        }
    }

    cancel_replace_buy_sample_unfilled = {
        "id": "",
        "method": "order.cancelReplace",
        "params": {
            "symbol": "USDTUSD",
            "cancelReplaceMode": "ALLOW_FAILURE",
            "cancelRestrictions" : "ONLY_NEW",
            "cancelOrderId": "",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "",
            "quantity": "",
            "apiKey": "",
            "timestamp": ""
        }
    }

    cancel_replace_buy_sample_partial_filled = {
        "id": "",
        "method": "order.cancelReplace",
        "params": {
            "symbol": "USDTUSD",
            "cancelReplaceMode": "ALLOW_FAILURE",
            "cancelRestrictions" : "ONLY_PARTIALLY_FILLED",
            "cancelOrderId": "",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "",
            "quantity": "",
            "apiKey": "",
            "timestamp": ""
        }
    }


    cancel_replace_sell_sample_unfilled = {
        "id": "",
        "method": "order.cancelReplace",
        "params": {
            "symbol": "USDTUSD",
            "cancelReplaceMode": "ALLOW_FAILURE",
            "cancelRestrictions" : "ONLY_NEW",
            "cancelOrderId": "",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "",
            "quantity": "",
            "apiKey": "",
            "timestamp": ""
        }
    }

    cancel_replace_sell_sample_partial_filled = {
        "id": "",
        "method": "order.cancelReplace",
        "params": {
            "symbol": "USDTUSD",
            "cancelReplaceMode": "ALLOW_FAILURE",
            "cancelRestrictions" : "ONLY_PARTIALLY_FILLED",
            "cancelOrderId": "",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": "",
            "quantity": "",
            "apiKey": "",
            "timestamp": ""
        }
    }

