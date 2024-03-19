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

from enum import Enum
from collections import deque
from datetime import datetime


from stream_bn.view.outstanding_order_view import *
from stream_bn.view.orderbook_view import *
from stream_bn.view.balance_sheet_view import *

from stream_bn.common.utils import *
from stream_bn.common.monitor import *
from stream_bn.common.logger import *


TIMESTAMP_SYNC_ITERATION = 1
PRICE_ACCURACY = 4 # 4 places after decimal
QUANTITY_ACCURACY = 0 # only whole number quantities

PRICE_TICK = 1 / 10 ** PRICE_ACCURACY
QUANTITY_TICK = 1 / 10 ** QUANTITY_ACCURACY
TRADE_POST_CD = 50 # 100ms cooldown between orders


ABORT_SIDE_THRESHOLD = 0.35 # threshold below which we'll stop new market making orders selling that asset
DEPTH_TOLERANCE = 100.0 # How much USD value we are willing to tolerate in the orderbook that's better than us

# Following units in number of price tickers


# Template for aggressive market making
DIST_FROM_BEST_BID = 1
DIST_FROM_BEST_ASK = 1
DIST_FROM_SECOND_BEST_BID = 6
DIST_FROM_SECOND_BEST_ASK = 6
MIN_SPREAD_FOR_MARKET_MAKING = 20

# Template for high volatility
# DIST_FROM_BEST_BID = 1
# DIST_FROM_BEST_ASK = 1
# DIST_FROM_SECOND_BEST_BID = 2
# DIST_FROM_SECOND_BEST_ASK = 2
# MIN_SPREAD_FOR_MARKET_MAKING = 10


#@Monitor.wrap_all_methods(Monitor.log_trace)
class MarketMakerEngine:


    timestamp_diff =  sum([float(requests.get('https://api.binance.us/api/v3/time').json()['serverTime']) - float(datetime.utcnow().timestamp() * 1000 - 14400000) for _ in range(TIMESTAMP_SYNC_ITERATION)]) / float(TIMESTAMP_SYNC_ITERATION)


    def __init__(self, config_json, api_ws, asset_1_name, asset_2_name, logger=None):
        
        self.asset_1_name = asset_1_name
        self.asset_2_name = asset_2_name
        self.outstanding_order_view = OutstandingOrderView()
        self.orderbook_view = OrderbookView([-1], [0.0001], [100000000], [0.0001])
        #self.order_daemon_thread = threading.Thread(target=self.daemon_handler_init)
        self.balance_sheet_view = BalanceSheetView(self.asset_1_name, 100, self.asset_2_name, 100)
        self.logger = logger
        
        self.data = config_json

        # Config
        self.trade_timestamp_difference = 0
        self.streams = self.data.get('streams', [])
        self.token_pair = self.data.get('token_pair', [])
        self.api_key_header = self.data.get('api_key', {})['header_field_name']
        self.api_key_value = self.data.get('api_key', {})['value']
        self.api_endpoint = self.data.get('api_endpoint', {})['url']
        self.stream_endpoint = self.data.get('stream_endpoint', {})['url']
        self.stream_url = self.stream_endpoint + '/'.join(self.streams)
        self.trade_url = self.data.get('trading_endpoint', {})['url']
        self.depth_tolerance = self.data.get('depth_tolerance', {})['value']

        # Order state machine lock
        self.buy_state_lock = threading.Lock()
        self.sell_state_lock = threading.Lock()
        self.active_trade_lock = threading.Lock()
        #self.blitzkrieg_lock = threading.Lock()
        #self.order_daemon_thread.start()
        time.sleep(2)
        self.trade_websocket = api_ws
        self.trade_websocket.start()

    def order_stream_handler(self, json_object):

        try:
            
            best_bid_p = round(float(json_object['b']), PRICE_ACCURACY)
            best_bid_q = float(json_object['B'])
            best_ask_p = round(float(json_object['a']), PRICE_ACCURACY)
            best_ask_q = float(json_object['A'])
            
            self.orderbook_view.set_best_bid(best_bid_p, best_bid_q)
            self.orderbook_view.set_best_ask(best_ask_p, best_ask_q)
            self.balance_sheet_view.set_exchange_ratio((best_bid_p + best_ask_p) / 2) 
            bid_ask_spread_exists = best_ask_p - best_bid_p >= MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK
            bid_ask_spread = best_ask_p - best_bid_p
            
            print(f"买一： {best_bid_p} {best_bid_q}  我的：{self.outstanding_order_view.new_buy.get_price()} {self.outstanding_order_view.new_buy.get_quantity()}  {self.outstanding_order_view.new_buy.get_state()}")
            print(f"卖一： {best_ask_p} {best_ask_q}  我的：{self.outstanding_order_view.new_sell.get_price()} {self.outstanding_order_view.new_sell.get_quantity()}  {self.outstanding_order_view.new_sell.get_state()}")
            print("")
            self.logger.log_message("orderbook_stream", 
f"""
买一： {best_bid_p}  {best_bid_q} 我的：{self.outstanding_order_view.new_buy.get_price()} {self.outstanding_order_view.new_buy.get_quantity()} {self.outstanding_order_view.new_buy.get_state()}
卖一： {best_ask_p}  {best_ask_q} 我的：{self.outstanding_order_view.new_sell.get_price()} {self.outstanding_order_view.new_sell.get_quantity()} {self.outstanding_order_view.new_sell.get_state()}
"""
            )

            # if bid_ask_spread <= MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
            #     self.blitzkrieg_lock.acquire()
            #     if not (abs(self.outstanding_order_view.new_buy.get_price() + 1.0) < 1e-11 and \
            #     abs(self.outstanding_order_view.new_sell.get_price() - 100000000.0) < 1e-11):
            #         pass
            #         #self.cancel_all()
            # elif bid_ask_spread >= MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK and self.blitzkrieg_lock.locked(): # random.randint(random.randint(2, 5), random.randint(8, 12))
            #     self.blitzkrieg_lock.release()


            # Handle posting order
            if self.outstanding_order_view.new_sell.get_state() == OrderStatus.NO_ORDER and bid_ask_spread_exists:
                self.sell()
            elif self.outstanding_order_view.new_sell.get_state() == OrderStatus.NEW_CONFIRMED and bid_ask_spread_exists:
                our_next_ask = self.orderbook_view.get_real_ask(self.outstanding_order_view.new_sell.get_quantity(), self.outstanding_order_view.new_sell.get_price())
                if our_next_ask != self.outstanding_order_view.new_sell.get_price():
                    self.replace_unfilled_sell()


            if self.outstanding_order_view.new_buy.get_state() == OrderStatus.NO_ORDER and bid_ask_spread_exists:
                self.buy()
            elif self.outstanding_order_view.new_buy.get_state() == OrderStatus.NEW_CONFIRMED and bid_ask_spread_exists:
                our_next_bid = self.orderbook_view.get_real_bid(self.outstanding_order_view.new_buy.get_quantity(), self.outstanding_order_view.new_buy.get_price())
                if our_next_bid != self.outstanding_order_view.new_buy.get_price():
                    self.replace_unfilled_buy()


        except Exception as e:
            print(e)


    


    # Correction stream logic
    # Whether I'm best and whether best and second-best has a large gap
        # if I'm best, and no large gap (Optimal scenario. Do nothing)
        
        # if I'm not best, and no large gap (Surpass best)

        # if I'm best, and large gap (Retrieve to second best + 1)

        # if I'm not best, and large gap (Best is too try-hard. Go to best - 1 tick)         
    def depth_order_stream_handler(self, json_object):
        try:
            if self.outstanding_order_view.new_buy.get_price() == -1.0 or self.outstanding_order_view.new_sell.get_price() == 100000000.0:
                return
            
            best_bid_p_arr = [round(float(bid[0]), PRICE_ACCURACY) for bid in json_object['bids'][:5]]
            best_bid_q_arr = [round(float(bid[1]), QUANTITY_ACCURACY) for bid in json_object['bids'][:5]]
            best_ask_p_arr = [round(float(ask[0]), PRICE_ACCURACY) for ask in json_object['asks'][:5]]
            best_ask_q_arr = [round(float(ask[1]), QUANTITY_ACCURACY) for ask in json_object['asks'][:5]]
            # print(f"买二： {best_bid_p_arr[1]}   我的：{self.outstanding_order_view.new_buy.get_price()} {self.outstanding_order_view.new_buy.get_state()}")
            # print(f"卖二： {best_ask_p_arr[1]}   我的：{self.outstanding_order_view.new_sell.get_price()} {self.outstanding_order_view.new_sell.get_state()}")
            # print("")

            self.orderbook_view.set_bid(best_bid_p_arr, best_bid_q_arr)
            self.orderbook_view.set_ask(best_ask_p_arr, best_ask_q_arr)
            bid_ask_spread_exists = best_ask_p_arr[0] - best_bid_p_arr[0] >= MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK
            

            if self.outstanding_order_view.new_buy.get_state() != OrderStatus.NEW_CONFIRMED or not bid_ask_spread_exists:
                pass
            else:
                am_I_best_buy = self.outstanding_order_view.new_buy.get_price() >= best_bid_p_arr[0] - 1 / 10 ** (PRICE_ACCURACY + 2)
                no_buy_gap = best_bid_p_arr[0] - best_bid_p_arr[1] < MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK
                if am_I_best_buy:
                    if no_buy_gap:
                        pass
                    else:
                        self.orderbook_view.set_best_bid(best_bid_p_arr[1], best_bid_q_arr[1])
                        self.replace_unfilled_buy()
                        self.orderbook_view.set_best_bid(best_bid_p_arr[0], best_bid_q_arr[0])
                        
                elif not no_buy_gap:
                    self.orderbook_view.set_best_bid(best_bid_p_arr[0] - PRICE_TICK, best_bid_q_arr[0])
                    self.replace_unfilled_buy()
                    self.orderbook_view.set_best_bid(best_bid_p_arr[0], best_bid_q_arr[0])
                else:
                    self.replace_unfilled_buy()


            if self.outstanding_order_view.new_sell.get_state() != OrderStatus.NEW_CONFIRMED:
                pass
            else:
                am_I_best_sell = self.outstanding_order_view.new_sell.get_price() <= best_ask_p_arr[0] + 1 / 10 ** (PRICE_ACCURACY + 2)
                no_sell_gap = best_ask_p_arr[1] - best_ask_p_arr[0] < MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK
                if am_I_best_sell:
                    if no_sell_gap:
                        pass
                    else:
                        self.orderbook_view.set_best_ask(best_ask_p_arr[1], best_ask_q_arr[1])
                        self.replace_unfilled_sell()
                        self.orderbook_view.set_best_ask(best_ask_p_arr[0], best_ask_q_arr[0])
                        
                elif not no_sell_gap:
                    self.orderbook_view.set_best_ask(best_ask_p_arr[0] + PRICE_TICK, best_ask_q_arr[0])
                    self.replace_unfilled_buy()
                    self.orderbook_view.set_best_ask(best_ask_p_arr[0], best_ask_q_arr[0])
                else:
                    self.replace_unfilled_sell()

        except Exception as e:
            print(e)


    def trade_stream_handler(self, json_object):

        try:
            order_price = float(json_object['p'])
            order_quantity = float(json_object['q'])
            is_buyer_maker = json_object['m']

            current_buy = self.outstanding_order_view.new_buy
            current_sell = self.outstanding_order_view.new_sell
            current_buy_order_id = current_buy.get_order_id()
            current_sell_order_id = current_sell.get_order_id()

            if is_buyer_maker:
                if current_buy_order_id == json_object['b']:
                    self.balance_sheet_view.set_balance(order_quantity, -order_price * order_quantity)
                    curr_quantity = current_buy.get_quantity()
                    
                    if order_quantity < curr_quantity:
                        current_buy.partly_executed(order_quantity)
                        print("buy executed partly in trade_stream")
                        self.replace_filled_buy()
                        log_string = f"{datetime.now()}, B price: {order_price}, quantity: {order_quantity}\n"
                        print(log_string)
                        self.logger.log_message("execution", log_string)
                    elif order_quantity > curr_quantity + 0.000001:
                        print(f"Error Confirmed buy quantity {order_quantity} > {current_buy.get_quantity()} more than submitted")
                        sys.exit()
                    else:
                        current_buy.executed()
                        self.buy()
                        log_string = f"{datetime.now()}, B price: {order_price}, quantity: {order_quantity}\n"
                        print(log_string)
                        self.logger.log_message("execution", log_string)
                else:
                    self.logger.log_message("trade_stream", f"{datetime.now()}, B price: {order_price}, quantity: {order_quantity}\n")
                

            if not is_buyer_maker:
                if current_sell_order_id == json_object['a']:
                    self.balance_sheet_view.set_balance(-order_quantity, order_price * order_quantity)
                    curr_quantity = current_sell.get_quantity()
                    
                    if order_quantity < curr_quantity:
                        current_sell.partly_executed(order_quantity)
                        print("sell executed partly in trade_stream")
                        self.replace_filled_sell()
                        log_string = f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n"
                        print(log_string)
                        self.logger.log_message("execution", log_string)
                    elif order_quantity > curr_quantity + 0.000001:
                        print(f"Error Confirmed sell quantity {order_quantity} > {current_sell.get_quantity()} more than submitted")
                        sys.exit()
                    else:
                        current_sell.executed()
                        self.sell()
                        log_string = f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n"
                        print(log_string)
                        self.logger.log_message("execution", log_string)
                else:
                    self.logger.log_message("trade_stream", f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n")


        

        except Exception as e:
            print(e)
    


    def cancel_replace_handler(self, json_object):
        try:
            if "cancelResult" in json_object or "newOrderResult" in json_object:
                if "cancelResult" in json_object:
                    if json_object['cancelResult'] == "SUCCESS":
                        
                        if self.outstanding_order_view.old_buy is not None and json_object['cancelResponse']['orderId'] == self.outstanding_order_view.old_buy.get_order_id():
                            self.outstanding_order_view.old_buy.cancel_confirmed()
                        elif self.outstanding_order_view.old_sell is not None and json_object['cancelResponse']['orderId'] == self.outstanding_order_view.old_sell.get_order_id():
                            self.outstanding_order_view.old_sell.cancel_confirmed()
                        else:
                            print(f"Cancelled non-existent order,{json_object}")
                            sys.exit()

                    else:
                        print(f"Error cancelling order, exiting. Message: {json_object}")
                        sys.exit()
                

                if "newOrderResult" in json_object:
                    if json_object['newOrderResult'] == "SUCCESS":
                        if json_object['newOrderResponse']['side'] == "BUY":
                            self.outstanding_order_view.new_buy.open_confirmed(json_object['newOrderResponse']['orderId'])

                            if json_object['newOrderResponse']['fills'] != []:
                                
                                [self.balance_sheet_view.set_balance(float(fill['qty']), - float(fill['price']) * float(fill['qty'])) for fill in json_object['newOrderResponse']['fills']]
                                total_execution_quantity = sum([float(fill['qty']) for fill in json_object['newOrderResponse']['fills']])
                                if total_execution_quantity + 0.000001 < self.outstanding_order_view.new_buy.get_quantity():
                                    self.outstanding_order_view.new_buy.partly_executed(total_execution_quantity)
                                    self.replace_filled_buy()
                                    log_string = [f"{datetime.now()}, B price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                                    print(log_string)
                                    self.logger.log_message("execution", log_string)
                                else:
                                    self.outstanding_order_view.new_buy.executed()
                                    self.buy()
                                    log_string = [f"{datetime.now()}, B price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                                    print(log_string)
                                    self.logger.log_message("execution", log_string)
                                
                                
                                

                        if json_object['newOrderResponse']['side'] == "SELL":
                            self.outstanding_order_view.new_sell.open_confirmed(json_object['newOrderResponse']['orderId'])

                            if json_object['newOrderResponse']['fills'] != []:
                                
                                [self.balance_sheet_view.set_balance(-float(fill['qty']), float(fill['price']) * float(fill['qty'])) for fill in json_object['newOrderResponse']['fills']]
                                total_execution_quantity = sum([float(fill['qty']) for fill in json_object['newOrderResponse']['fills']])
                                if total_execution_quantity + 0.000001 < self.outstanding_order_view.new_sell.get_quantity():
                                    self.outstanding_order_view.new_sell.partly_executed(total_execution_quantity)
                                    self.replace_filled_sell()
                                    log_string = [f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                                    print(log_string)
                                    self.logger.log_message("execution", log_string)
                                else:
                                    self.outstanding_order_view.new_sell.executed()
                                    self.sell()
                                    log_string = [f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                                    print(log_string)
                                    self.logger.log_message("execution", log_string)
                                
                                

                    else:
                        print("Error replacing order, exiting... Look for logs under ../logs/error")
                        sys.exit()
            return None
        except Exception as e:
            print(e)
        


    def confirmation_handler(self, json_object):
        try:

            if isinstance(json_object, list):
                
                self.outstanding_order_view.new_buy.cancel_confirmed()
                self.outstanding_order_view.new_buy.delete_thread_safe()
                
                self.outstanding_order_view.new_sell.cancel_confirmed()
                self.outstanding_order_view.new_sell.delete_thread_safe()
                return
            
            if json_object['side'] == 'BUY':
                self.outstanding_order_view.new_buy.open_confirmed(json_object['orderId'])
            
            if json_object['side'] == 'SELL':
                self.outstanding_order_view.new_sell.open_confirmed(json_object['orderId'])

            # Handle new order place message
            if json_object['fills'] != []:
                
                if json_object['side'] == "BUY":
                    [self.balance_sheet_view.set_balance(float(fill['qty']), -float(fill['price']) * float(fill['qty'])) for fill in json_object['newOrderResponse']['fills']]
                    total_execution_quantity = sum([float(fill['qty']) for fill in json_object['newOrderResponse']['fills']])
                    self.outstanding_order_view.new_buy.partly_executed(total_execution_quantity)
                    order_price = sum([fill['price'] for fill in json_object['newOrderResponse']['fills']]) / len(json_object['newOrderResponse']['fills'])
                    self.replace_filled_buy()
                    log_string = [f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                    print(log_string)
                    self.logger.log_message("execution", log_string)

                if json_object['side'] == "SELL":
                    [self.balance_sheet_view.set_balance(-float(fill['qty']), float(fill['price']) * float(fill['qty'])) for fill in json_object['newOrderResponse']['fills']]
                    total_execution_quantity = sum([float(fill['qty']) for fill in json_object['newOrderResponse']['fills']])
                    self.outstanding_order_view.new_sell.partly_executed(total_execution_quantity)
                    order_price = sum([fill['price'] for fill in json_object['newOrderResponse']['fills']]) / len(json_object['newOrderResponse']['fills'])
                    self.replace_filled_sell()
                    log_string = [f"{datetime.now()}, S price: {order_price}, quantity: {order_quantity}\n" for order_price, order_quantity in zip([float(fill['price']) for fill in json_object['newOrderResponse']['fills']], [float(fill['quantity']) for fill in json_object['newOrderResponse']['fills']])]
                    print(log_string)
                    self.logger.log_message("execution", log_string)
        except Exception as e:
            print(e)


    def pure_cancel_handler(self, json_object):

        if self.outstanding_order_view.old_buy is not None and self.outstanding_order_view.old_buy.get_order_id() == json_object['orderId']:
            self.outstanding_order_view.old_buy.cancel_confirmed()

        elif self.outstanding_order_view.old_sell is not None and self.outstanding_order_view.old_sell.get_order_id() == json_object['orderId']:
            self.outstanding_order_view.old_sell.cancel_confirmed()



    def balance_sheet_setup_handler(self, asset_1_quantity, asset_2_quantity):
        try:
            self.balance_sheet_view.set_balance(asset_1_quantity, asset_2_quantity)
        except Exception as e:
            print(e)


    def orderbook_setup_handler(self, bid_p, bid_q, ask_p, ask_q):
        try:
            self.orderbook_view.set_bid(bid_p, bid_q)
            self.orderbook_view.set_ask(ask_p, ask_q)
            self.balance_sheet_view.set_exchange_ratio((bid_p[0] + ask_p[0]) / 2) 
        except Exception as e:
            print(e)

    # # Daemon thread that handles too good / too bad orders but no order book/trade updates
    # def daemon_handler_init(self):
    #     while True:
    #         if len(self.orderbook_view.get_bid()[0]) > 1 and len(self.orderbook_view.get_ask()[0]) > 1:
    #             break

    #     self.daemon_handler_main()
    

    # def daemon_handler_main(self):
    #     # Bid too good
    #     while True:
    #         my_bid = self.outstanding_order_view.new_buy.get_price()
    #         if my_bid > self.orderbook_view.get_bid()[0][1] + DIST_FROM_SECOND_BEST_BID * 1 / 10 ** PRICE_ACCURACY: # Second best bid in orderbook
    #             self.replace_unfilled_buy()
    #         # Ask too good
    #         my_ask = self.outstanding_order_view.new_ask.get_price()
    #         if my_bid > self.orderbook_view.get_ask()[0][1] - DIST_FROM_SECOND_BEST_BID * 1 / 10 ** PRICE_ACCURACY:  # Second best ask in orderbook
    #             self.replace_unfilled_sell()


    def error_handler(self, json_object):
        try:
            self.logger.log_message("orderbook_stream",
f"""{json_object} 
""")    
            print(f"error: {json_object}")
            if json_object['status'] == 409:
                error_json = json_object['error']
                if error_json['code'] == -2021:
                    if error_json['data']['cancelResult'] == "FAILURE" and error_json['data']['newOrderResult'] == "FAILURE":
                        self.logger.log_message("orderbook_stream", "cancellation and new order both failed. Exiting...")
                        sys.exit()
                    
                    # reset buy or sell to NO_ORDER based on user id
                    if error_json['data']['cancelResult'] == "FAILURE":
                        # Buy cancellation failure
                        if error_json['data']['newOrderResponse']['side'] == "BUY":
                            # Reset cancellation buy state
                            self.outstanding_order_view.old_buy.cancel_confirmed()
                            self.outstanding_order_view.new_buy.open_confirmed(error_json['data']['newOrderResponse']['orderId'])
                        if error_json['data']['newOrderResponse']['side'] == "SELL":
                            # Reset cancellation sell state
                            self.outstanding_order_view.old_sell.cancel_confirmed()
                            self.outstanding_order_view.new_sell.open_confirmed(error_json['data']['newOrderResponse']['orderId'])
                        
                    
                    if error_json['data']['newOrderResult'] == "FAILURE":
                        # Buy replacement failure
                        if error_json['data']['cancelResponse']["side"] == "BUY":
                            # Re-buy
                            self.outstanding_order_view.new_buy.delete_thread_safe()
                            self.buy()
                        if error_json['data']['cancelResponse']["side"] == "SELL":
                            # Re-sell
                            self.outstanding_order_view.new_sell.delete_thread_safe()
                            self.sell()

                    
                    if json_object['id'] == self.outstanding_order_view.old_sell.get_user_id():
                        # Reset sell state
                        self.outstanding_order_view.old_sell.delete_thread_safe()
                        self.outstanding_order_view.new_sell.delete_thread_safe()
                        # Re-sell
                        self.sell()

            # Order rate limit broken
            if json_object['status'] == 429:
                self.logger.log_message("orderbook_stream", f"order rate limit broken at {datetime.now()}. Exiting...")
                sys.exit()
                
        except Exception as e:
            print(e)
    
    # Updates 
    def correction_feed_handler(self, new_price):
        self.orderbook_view.set_correction_price(new_price)
        



    @RateLimiter()
    def buy(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_buy.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.buy_state_lock:
                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] > (1 - mid_price) * 10 + 0.15 or not self.orderbook_view.get_can_buy() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    print("buy aborted")
                    return
                
                trade_sample_raw = self.construct_trade(BinancePayloadSample.buy_sample, False, False)
                self.outstanding_order_view.new_buy.open_sent(
                    trade_sample_raw['id'],
                    round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                    float(trade_sample_raw['params']['quantity'])
                )
                self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)
       
    @RateLimiter()
    def sell(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_sell.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.sell_state_lock:
                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] < (1 - mid_price) * 10 - 0.05 or not self.orderbook_view.get_can_sell() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    print("sell aborted")
                    return
                trade_sample_raw = self.construct_trade(BinancePayloadSample.sell_sample, False, False)
                self.outstanding_order_view.new_sell.open_sent(
                    trade_sample_raw['id'],
                    round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                    float(trade_sample_raw['params']['quantity'])
                )
                self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)
    

    # Below volume => Follow
    # Above volume => Beat
    @RateLimiter()
    def replace_unfilled_buy(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_buy.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.buy_state_lock:
                
                self.outstanding_order_view.old_buy = Order.create_from(self.outstanding_order_view.new_buy.get_order())
                self.outstanding_order_view.old_buy.cancel_sent()
                cancel_id = self.outstanding_order_view.new_buy.get_order_id()
                self.outstanding_order_view.new_buy.delete_thread_safe()

                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] <= (1 - mid_price) * 10 + 0.15 and self.orderbook_view.get_can_buy() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    # Cancel and replace
                    trade_sample_raw = self.construct_trade(
                        BinancePayloadSample.cancel_replace_buy_sample_unfilled, 
                        True, 
                        False,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
                    self.outstanding_order_view.new_buy.open_sent(
                        trade_sample_raw['id'],
                        round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                        float(trade_sample_raw['params']['quantity'])
                    )
                else:
                    # Pure cancel
                    trade_sample_raw = self.construct_pure_cancel(
                        BinancePayloadSample.cancel_sample,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)


    @RateLimiter()
    def replace_unfilled_sell(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_sell.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.sell_state_lock:


                self.outstanding_order_view.old_sell = Order.create_from(self.outstanding_order_view.new_sell.get_order())
                self.outstanding_order_view.old_sell.cancel_sent()
                cancel_id = self.outstanding_order_view.new_sell.get_order_id()
                self.outstanding_order_view.new_sell.delete_thread_safe()

                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] >= (1 - mid_price) * 10 - 0.05 and self.orderbook_view.get_can_sell() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    # Cancel and replace
                    trade_sample_raw = self.construct_trade(
                        BinancePayloadSample.cancel_replace_sell_sample_unfilled, 
                        True, 
                        False,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
                    self.outstanding_order_view.new_sell.open_sent(
                        trade_sample_raw['id'],
                        round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                        float(trade_sample_raw['params']['quantity'])
                    )
                else:
                    # Pure cancel
                    trade_sample_raw = self.construct_pure_cancel(
                        BinancePayloadSample.cancel_sample,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)


    @RateLimiter()
    def replace_filled_buy(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_buy.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.buy_state_lock:

                self.outstanding_order_view.old_buy = Order.create_from(self.outstanding_order_view.new_buy.get_order())
                self.outstanding_order_view.old_buy.cancel_sent()
                cancel_id = self.outstanding_order_view.new_buy.get_order_id()
                self.outstanding_order_view.new_buy.delete_thread_safe()

                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] > (1 - mid_price) * 10 + 0.15 and self.orderbook_view.get_can_buy() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    # Cancel and replace
                    trade_sample_raw = self.construct_trade(
                        BinancePayloadSample.cancel_replace_buy_sample_partial_filled, 
                        True, 
                        True,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
                    self.outstanding_order_view.new_buy.open_sent(
                        trade_sample_raw['id'],
                        round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                        float(trade_sample_raw['params']['quantity'])
                    )
                else:
                    # Pure cancel
                    trade_sample_raw = self.construct_pure_cancel(
                        BinancePayloadSample.cancel_sample,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)

    @RateLimiter()
    def replace_filled_sell(self):
        
        try:
            # acquired = self.blitzkrieg_lock.acquire(blocking=False)
            # if not acquired:
            #     return
            # if int(time.time() * 1000) - self.outstanding_order_view.new_sell.get_last_timestamp() < TRADE_POST_CD:
            #     self.blitzkrieg_lock.release()
            #     return
            with self.sell_state_lock:

                self.outstanding_order_view.old_sell = Order.create_from(self.outstanding_order_view.new_sell.get_order())
                self.outstanding_order_view.old_sell.cancel_sent()
                cancel_id = self.outstanding_order_view.new_sell.get_order_id()
                self.outstanding_order_view.new_sell.delete_thread_safe()

                mid_price = (self.orderbook_view.get_best_bid()[0] + self.orderbook_view.get_best_ask()[0]) / 2.0
                if self.balance_sheet_view.get_asset_proportion()[0] >= (1 - mid_price) * 10 - 0.05 and self.orderbook_view.get_can_sell() and self.orderbook_view.get_best_ask()[0] - self.orderbook_view.get_best_bid()[0] > MIN_SPREAD_FOR_MARKET_MAKING * PRICE_TICK:
                    # Cancel and replace
                    trade_sample_raw = self.construct_trade(
                        BinancePayloadSample.cancel_replace_sell_sample_partial_filled, 
                        True, 
                        True,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
                    self.outstanding_order_view.new_sell.open_sent(
                        trade_sample_raw['id'],
                        round(float(trade_sample_raw['params']['price']), PRICE_ACCURACY),
                        float(trade_sample_raw['params']['quantity'])
                    )
                else:
                    # Pure cancel
                    trade_sample_raw = self.construct_pure_cancel(
                        BinancePayloadSample.cancel_sample,
                        original_id = cancel_id
                    )
                    self.trade_websocket.sign_and_send(trade_sample_raw)
            #self.blitzkrieg_lock.release()
        except Exception as e:
            print(e)


    def cancel_all(self):
        try:
            order_cancellation_payload = {
                "id": str(int(datetime.utcnow().timestamp() * 1000 - 14400000)),
                "method": "openOrders.cancelAll",
                "params": {
                    "symbol": self.asset_1_name.upper() + self.asset_2_name.upper(),
                    "apiKey": self.api_key_value,
                    "signature": "",
                    "timestamp": str(int(datetime.utcnow().timestamp() * 1000 - 14400000))
                }
            }
            if self.oustanding_order_view.new_buy.get_state() != OrderStatus.NO_ORDER:
                self.outstanding_order_view.new_buy.cancel_sent()
            if self.outstanding_order_view.new_sell.get_state() != OrderStatus.NO_ORDER:
                self.outstanding_order_view.new_sell.cancel_sent()
            self.trade_websocket.sign_and_send(order_cancellation_payload)
        except Exception as e:
            print(e)



    def construct_pure_cancel(self, trade_sample, original_id=None):
        timestamp_now =  str(int(datetime.utcnow().timestamp() * 1000 - 14400000 + MarketMakerEngine.timestamp_diff))
        trade_sample['id'] = timestamp_now
        trade_sample['params']['timestamp'] = timestamp_now
        trade_sample['params']['orderId'] = original_id
        return trade_sample


    def construct_trade(self, trade_sample, is_cancel_replace, is_partly_executed, original_id=None):
        if is_cancel_replace:
            trade_sample['params']['cancelOrderId'] = original_id
        #if new order then always be best
        my_buy_q = -1.0
        my_sell_q = -1.0
        my_buy_p = 1.0
        my_sell_p = 100000000.0
        if not is_partly_executed:
            my_buy_q = self.balance_sheet_view.get_bid_quantity()
            my_sell_q = self.balance_sheet_view.get_ask_quantity() 
            my_buy_p = self.orderbook_view.get_real_bid(my_buy_q, self.outstanding_order_view.new_buy.get_price())
            my_sell_p = self.orderbook_view.get_real_ask(my_sell_q, self.outstanding_order_view.new_sell.get_price())
            # my_buy_p = self.orderbook_view.get_best_bid()[0] + PRICE_TICK #+ random.randint(1, 3) * PRICE_TICK
            # my_sell_p = self.orderbook_view.get_best_ask()[0] - PRICE_TICK #+ random.randint(-3, -1) * PRICE_TICK
            

        #if order partly executed then look for second-best
        else:
            my_buy_q = self.balance_sheet_view.get_bid_quantity()
            my_sell_q = self.balance_sheet_view.get_ask_quantity()
            my_buy_p = self.orderbook_view.get_real_bid(my_buy_q, self.outstanding_order_view.new_buy.get_price())
            my_sell_p = self.orderbook_view.get_real_ask(my_sell_q, self.outstanding_order_view.new_sell.get_price())
            #my_buy_p = self.orderbook_view.get_bid()[0][1] + PRICE_TICK #+ random.randint(1, 3) * PRICE_TICK
            #my_sell_p = self.orderbook_view.get_ask()[0][1] - PRICE_TICK #+ random.randint(-3, -1) * PRICE_TICK
            
        
        # current_bid_quantity = self.outstanding_order_view.new_buy.get_quantity()
        # current_ask_quantity = self.outstanding_order_view.new_sell.get_quantity()

        # if current_bid_quantity is not None:
        #     my_buy_q -= current_bid_quantity
        
        # if current_ask_quantity is not None:
        #     my_sell_q -= current_ask_quantity
        price = round(my_buy_p if trade_sample['params']['side'] == "BUY" else my_sell_p, PRICE_ACCURACY)
        quantity = round(my_buy_q if trade_sample['params']['side'] == "BUY" else my_sell_q, QUANTITY_ACCURACY)

        trade_sample['params']['price'] = price
        trade_sample['params']['quantity'] = format(quantity, '.5f')

        timestamp_now =  str(int(datetime.utcnow().timestamp() * 1000 - 14400000 + MarketMakerEngine.timestamp_diff))
        trade_sample['id'] = timestamp_now
        trade_sample['params']['timestamp'] = timestamp_now
        
        return trade_sample
