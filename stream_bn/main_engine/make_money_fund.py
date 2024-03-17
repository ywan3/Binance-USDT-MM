### More External facing functionalities (potentially for moving between wallets)

class Fund:
    
    def __init__(self, market_data_feed, market_maker_engine):
        self.market_data_feed = market_data_feed
        self.market_maker_engine = market_maker_engine
    
    def start(self):
        self.market_data_feed.start_feed()
        print("fund started")
