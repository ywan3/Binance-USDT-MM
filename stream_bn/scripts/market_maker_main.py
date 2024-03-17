
from stream_bn.data_feed.stream_feed import *
from stream_bn.data_feed.api_feed import *
from stream_bn.data_feed.correction_feed import *
from stream_bn.data_feed.market_data_feed import *


from stream_bn.common.monitor import *

from stream_bn.main_engine.market_maker_engine import *
from stream_bn.main_engine.make_money_fund import *
import traceback

if __name__ == "__main__":

    try:

        with open("./stream_bn/config/binance_config.json", 'r') as file:
            data = json.load(file)

        api_key = data['api_key']['value']
        api_secret = data['api_secret']['value']
        stream_endpoint = data['stream_endpoint']['url'] + '/'.join(data['streams'])
        api_endpoint = data['api_endpoint']['url']
        asset_1_name = data['token_pair'][0] # Usually more non-basic
        asset_2_name = data['token_pair'][1] # Basic token (USD > USDC > USDT)


        logger = Logger()
        logger.register_log_type("execution", "./stream_bn/logs/execution_log.txt")
        logger.register_log_type("orderbook_stream", "./stream_bn/logs/program_log.txt")
        logger.register_log_type("trade_stream", "./stream_bn/logs/trade_stream_log.txt")


        StreamFeed.stream_url = stream_endpoint
        StreamFeed.streams = data['streams']
        stream_feed = StreamFeed()
        #correction_feed = CorrectionFeed('./stream_bn/config/coinbase_config.json')


        trade_websocket = ApiFeed(api_endpoint, api_key, api_secret, asset_1_name, asset_2_name)
        
        
        #market_data_feed = MarketDataFeed(stream_feed, correction_feed=correction_feed)
        market_data_feed = MarketDataFeed(stream_feed)
        market_maker_engine = MarketMakerEngine(data, trade_websocket, asset_1_name, asset_2_name, logger=logger)
        
        market_data_feed.stream_feed.register_hook("usdtusd@bookTicker", market_maker_engine.order_stream_handler)
        market_data_feed.stream_feed.register_hook("usdtusd@trade", market_maker_engine.trade_stream_handler)
        market_data_feed.stream_feed.register_hook("usdtusd@depth20@100ms", market_maker_engine.depth_order_stream_handler)
        #market_data_feed.correction_feed.register_hook("update_correction_price", market_maker_engine.correction_feed_handler)

        
        market_maker_engine.trade_websocket.register_hook("cancel_replace", market_maker_engine.cancel_replace_handler)
        market_maker_engine.trade_websocket.register_hook("new_trade", market_maker_engine.confirmation_handler)
        market_maker_engine.trade_websocket.register_hook("pure_cancel", market_maker_engine.pure_cancel_handler)
        market_maker_engine.trade_websocket.register_hook("error", market_maker_engine.error_handler)
        market_maker_engine.trade_websocket.register_hook("balance_sheet_setup", market_maker_engine.balance_sheet_setup_handler)
        market_maker_engine.trade_websocket.register_hook("orderbook_setup", market_maker_engine.orderbook_setup_handler)
        market_maker_engine.trade_websocket.register_hook("initiate_buy_handler", market_maker_engine.buy)
        market_maker_engine.trade_websocket.register_hook("initiate_sell_handler", market_maker_engine.sell)
        
        fund_main = Fund(market_data_feed, market_maker_engine)
        fund_main.start()
    except Exception as e:
        print(e)
        traceback.print_exc()
    
