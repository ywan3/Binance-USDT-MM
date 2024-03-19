import websocket
import threading
import json
from stream_bn.common.monitor import *
import sys


class StreamFeed:

    with open("./stream_bn/config/binance_config.json", 'r') as file:
        asset_names = json.load(file)['token_pair']
        asset_1_name = asset_names[0]
        asset_2_name = asset_names[1]
    
    stream_url = None
    streams = None
    message_handler_dict = {
            f"{asset_1_name + asset_2_name}@bookTicker" : None,
            f"{asset_1_name + asset_2_name}@trade" : None,
            f"{asset_1_name + asset_2_name}@depth5@100ms": None
        }
    def __init__(self):
        
        if StreamFeed.stream_url is None:
            print(f"Error: Invalid stream_url {StreamFeed.stream_url}")
        
        self.websocket = websocket.WebSocketApp(StreamFeed.stream_url,
                                    on_message=StreamFeed.on_message,
                                    on_error=StreamFeed.on_error,
                                    on_close=StreamFeed.on_close,
                                    on_open=StreamFeed.on_open)

        self.thread = threading.Thread(target=self.websocket.run_forever)
        self.market_maker_success = False
    
    def register_hook(self, message_type, engine_hook):
        StreamFeed.message_handler_dict[message_type] = engine_hook
    
    def start(self):
        self.thread.start()
        self.market_maker_setup_finished = True

    @staticmethod
    def on_message(ws, message):
        json_object = json.loads(message)
        print(f"on_message {json_object}")
        if 'stream' in json_object:
            StreamFeed.message_handler_dict.get(json_object['stream'], lambda data: print(f"Failed to locate handler for stream {json_object['stream']}"))(json_object['data'])
        elif 'result' in json_object and json_object['result'] is None:
            #pass
            print("stream subscription success")
        else:
            print(f"Error on stream feed. Non-stream payload received: {json_object}")
            sys.exit()

    @staticmethod
    def on_error(ws, error):
        print("stream client on error")
        print("error occured ", error)
        
    @staticmethod
    def on_close(ws):
        print("stream client on close")
        ws.close()

    @staticmethod
    def on_open(ws):
        print("stream client on open")
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": StreamFeed.streams,
            "id": 1
        }
        ws.send(json.dumps(subscribe_message))