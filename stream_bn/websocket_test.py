import websocket
import threading
import json
import hashlib
import hmac
from datetime import datetime


api_key = "vp1XMq3pG8XjVQfkeiNyKzPvoZ0Wa8kATNredpRSYQFRTcAYbOCXrYaDD11qQGSw"
api_secret = "KsIuuSMRJz6gJQOJHhBhJxeoR49WoHIq17Ikr1IIjW6LlnDSgkyOEJj6wHSdEXW0"


def sign_and_send(websocket, api_key, api_secret, raw_payload):
    raw_payload['params']['apiKey'] = api_key
    raw_message = '&'.join([f"{k}={v}" for k, v in sorted(raw_payload['params'].items()) if k != 'signature'])
    payload_signature = hmac.new(api_secret.encode(), raw_message.encode(), hashlib.sha256).hexdigest()
    raw_payload['params']['signature'] = payload_signature
    print(raw_payload)
    print("sending payload")
    websocket.send(json.dumps(raw_payload))


def on_open(ws):
    # Retrieve balance
    try:
        print("on open")
        account_status_payload = {
            "id": str(int(datetime.utcnow().timestamp() * 1000 - 14400000)),
            "method": "account.status",
            "params": {
                "apiKey": api_key,
                "signature": "",
                "timestamp": str(int(datetime.utcnow().timestamp() * 1000 - 14400000))
            }
        }
        sign_and_send(ws, api_key, api_secret, account_status_payload)

    except Exception as e:
        print(e)



def on_error(ws, error):
    print(error)
    message_handler_dict['error'](json_object)


def on_close(ws):
    print("api client on close")
    ws.close()


def on_message(ws, message):
    print("on message")
    json_object = json.loads(message)
    print(json_object)




if __name__ == "__main__":
    websocket.enableTrace(False)

    url = "wss://ws-api.binance.us:443/ws-api/v3"

    websocket = websocket.WebSocketApp(url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open)

    websocket.run_forever()