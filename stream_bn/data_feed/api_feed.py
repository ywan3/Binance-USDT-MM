import websocket
import threading
import json
import hashlib
import hmac
from stream_bn.common.monitor import *
from datetime import datetime
import requests
import sys


MIN_SPREAD_FOR_MARKET_MAKING = 20
PRICE_ACCURACY = 4 # 4 places after decimal
PRICE_TICK = 1 / 10 ** PRICE_ACCURACY

#@Monitor.wrap_all_methods(Monitor.log_trace)
class ApiFeed:
    def __init__(self, url, api_key, api_secret, asset_1_name, asset_2_name):
        self.message_handler_dict = {
            "cancel_replace" : None,
            "new_trade" : None, 
            "error" : None,
            "balance_sheet_setup" : None,
            "orderbook_setup": None,
            "initiate_buy_handler": None,
            "initiate_sell_handler" : None
        }
        self.url = url
        self.api_key = api_key
        self.api_secret = api_secret
        self.asset_1_name = asset_1_name.upper()
        self.asset_2_name = asset_2_name.upper()
        websocket.enableTrace(False)
        self.websocket = websocket.WebSocketApp(self.url,
                                    on_message=self.on_balance_sheet_setup_callback,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_open=self.on_open)
        self.thread = threading.Thread(target=self.websocket.run_forever)

    def start(self):
        self.thread.start()

    def register_hook(self, message_type, engine_hook):
        self.message_handler_dict[message_type] = engine_hook


    
    def on_open(self, ws):
        # Retrieve balance
        try:
            account_status_payload = {
                "id": str(int(datetime.utcnow().timestamp() * 1000 - 14400000)),
                "method": "account.status",
                "params": {
                    "apiKey": self.api_key,
                    "signature": "",
                    "timestamp": str(int(datetime.utcnow().timestamp() * 1000 - 14400000))
                }
            }
            self.sign_and_send(account_status_payload)
        except Exception as e:
            print(e)

    def on_balance_sheet_setup_callback(self, ws, message):
        try:
            balances = json.loads(message)['result']['balances']
            usdt_balance = float([balance["free"] for balance in balances if balance["asset"] == self.asset_1_name][0])
            usd_balance = float([balance["free"] for balance in balances if balance["asset"] == self.asset_2_name][0])
            self.message_handler_dict["balance_sheet_setup"](usdt_balance, usd_balance)
            
            order_cancellation_payload = {
                    "id": str(int(datetime.utcnow().timestamp() * 1000 - 14400000)),
                    "method": "openOrders.cancelAll",
                    "params": {
                        "symbol": self.asset_1_name + self.asset_2_name,
                        "apiKey": self.api_key,
                        "signature": "",
                        "timestamp": str(int(datetime.utcnow().timestamp() * 1000 - 14400000))
                    }
                }
            self.sign_and_send(order_cancellation_payload)
            print("balance sheet setup finished")
            self.websocket.on_message = self.on_order_cancellation_callback
        except Exception as e:
            print(e)

    
    def on_order_cancellation_callback(self, ws, message):
        try:
            # request whole order book
            orderbook_request_payload = {
                "id": str(int(datetime.utcnow().timestamp() * 1000 - 14400000)),
                "method": "depth",
                "params": {
                    "symbol": "USDTUSD",
                    "limit": 20
                }
            }
            self.websocket.send(json.dumps(orderbook_request_payload))
            print("all previous order cancelled")
            self.websocket.on_message = self.on_orderbook_setup_callback
        except Exception as e:
            print(e)
    
    def on_orderbook_setup_callback(self, ws, message):
        json_object = json.loads(message)   
        if "status" not in json_object or json_object["status"] != 200:
            print("Error setting up depth 20 orderbook. Exiting....")
            sys.exit()

        bids_json = json_object["result"]["bids"]
        asks_json = json_object["result"]["asks"]
        bid_p = [float(bid_json[0]) for bid_json in bids_json]
        bid_q = [float(bid_json[1]) for bid_json in bids_json]
        ask_p = [float(ask_json[0]) for ask_json in asks_json]
        ask_q = [float(ask_json[1]) for ask_json in asks_json]

        self.message_handler_dict['orderbook_setup'](bid_p, bid_q, ask_p, ask_q)
        print("orderbook setup. Sending trade")

        # send our own trade
        self.message_handler_dict['initiate_buy_handler']()
        self.message_handler_dict['initiate_sell_handler']()
        self.websocket.on_message = self.on_message


    def on_message(self, ws, message):
        json_object = json.loads(message)
        if 'error' in json_object:
            self.message_handler_dict['error'](json_object)
        elif 'result' in json_object and 'cancelResult' in json_object['result'] and 'newOrderResult' in json_object['result']:
            self.message_handler_dict['cancel_replace'](json_object['result'])
        elif 'result' in json_object:
            if json_object['result']['status'] == 'CANCELED':
                self.message_handler_dict['pure_cancel'](json_object['result'])
            else:
                self.message_handler_dict['new_trade'](json_object['result'])

            

    def on_error(self, ws, error):
        print(error)
        self.message_handler_dict['error'](error)


    def on_close(self, ws):
        print("api client on close")
        ws.close()



    def sign_and_send(self, raw_payload):
        raw_payload['params']['apiKey'] = self.api_key
        raw_message = '&'.join([f"{k}={v}" for k, v in sorted(raw_payload['params'].items()) if k != 'signature'])
        payload_signature = hmac.new(self.api_secret.encode(), raw_message.encode(), hashlib.sha256).hexdigest()
        raw_payload['params']['signature'] = payload_signature
        self.websocket.send(json.dumps(raw_payload))
        