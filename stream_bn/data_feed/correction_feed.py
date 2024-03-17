import json
import websocket
import time
import json
import threading
import numpy as np
from coinbase import jwt_generator
from datetime import datetime
import sys

#@Monitor.wrap_all_methods(Monitor.log_trace)
class CorrectionFeed:
    def __init__(self, config_path, logger=None):
        

        self.logger = logger
        with open(config_path, 'r') as file:
            self.data = json.load(file)
        
        self.api_key_raw = self.data['api_key']['value']
        self.api_secret_raw = self.data['api_secret']['value']
        self.org_id = self.data['org_id']['value']
        self.url = self.data['stream_endpoint']['value']
        self.api_key = self.api_key_raw
        self.api_secret = self.api_secret_raw
        self.handler_dict = {}
        self.handler_dict['market_trades'] = self.trade_handler
        self.handler_dict['heartbeats'] = self.heartbeat_handler
        self.init_stream_client()
    
    def register_hook(self, name, handler):
        self.handler_dict[name] = handler

    def trade_handler(self, json_object):

        try:
            
            trade_object_list = json_object[0]['trades']
            prices = np.array([float(trade_object['price']) for trade_object in trade_object_list])
            quantities = np.array([float(trade_object['size']) for trade_object in trade_object_list])
            maker_sides = [trade_object['side'] == 'BUY' for trade_object in trade_object_list]
            timestamps = [trade_object['time'] for trade_object in trade_object_list]
            total_volume = np.sum(quantities)
            vwap = np.sum(prices * quantities) / total_volume
            print(f"VWAP price: {vwap} at time: {timestamps[0]}")
            self.handler_dict["update_correction_price"](vwap)
        except Exception as e:
            print(e)

    def heartbeat_handler(self, json_object):
        pass

    
    def on_snapshot(self, ws, message):
        try:
            json_object = json.loads(message)['events'][0]
            
            if "type" in json_object and json_object['type'] == "snapshot":
                self.stream_websocket.on_message = self.on_message
                self.update_market_snapshot(json_object['trades'])
                
            elif "channel" in json_object and json_object['channel'] == "market_trades":
                self.stream_websocket.on_message = self.on_message
                self.handler_dict[json_object['channel']](json_object['events'])
            else:
                print(json_object)
                print("Received neither market update nor market snapshot. Exiting...")
                sys.exit()
        except Exception as e:
            print(e)
    
    
    def update_market_snapshot(self, json_object):
        print("Received market snapshot")

    
    def on_message(self, ws, message):
        json_object = json.loads(message)
        self.handler_dict[json_object['channel']](json_object['events'])


    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("stream client on close")
        ws.close()

    def on_open(self, ws):
        print("Opened connection")
        jwt_token = jwt_generator.build_ws_jwt(self.api_key, self.api_secret)
        
        subscribe_message = {
            "type": "subscribe",
            "product_ids": [
                "USDT-USD"
            ],
            "channel": "market_trades",
            "jwt": jwt_token,
            "timestamp": str(int(time.time()))
        }

        heartbeat_message = {
            "type": "subscribe",
            "product_ids": [
                "USDT-USD"
            ],
            "channel": "heartbeats",
            "jwt": jwt_token,
            "timestamp": str(int(time.time()))
        }


        self.stream_websocket.send(json.dumps(subscribe_message))
        self.stream_websocket.send(json.dumps(heartbeat_message))
    


    def init_stream_client(self):
        websocket.enableTrace(False)
        self.stream_websocket = websocket.WebSocketApp(self.url,
                                         on_message=self.on_snapshot,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open = self.on_open)
        self.client_thread = threading.Thread(target=self.stream_websocket.run_forever)         


    def start(self):
        self.client_thread.start()




