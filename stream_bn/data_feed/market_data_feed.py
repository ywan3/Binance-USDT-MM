import time

class MarketDataFeed:
    
    def __init__(self, stream_feed, correction_feed=None):
        self.stream_feed = stream_feed
        self.correction_feed = correction_feed
    
    def start_feed(self):
       
        if self.correction_feed is not None:
            time.sleep(5)
            self.correction_feed.start()
        self.stream_feed.start()
        

