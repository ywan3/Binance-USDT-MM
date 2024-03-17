

class Logger:
    def __init__(self):
        self.log_path_list = {}
    
    def register_log_type(self, log_message_type, log_path):
        
        try:
            self.log_path_list[log_message_type] = open(log_path, "a")
        except Exception as e:
            print(e)

    def log_message(self, log_message_type, message_payload):
        self.log_path_list[log_message_type].write(message_payload)
        self.log_path_list[log_message_type].flush()