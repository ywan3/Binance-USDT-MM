
import inspect
import types
import functools
import os
from datetime import datetime
import random

from stream_bn.common.utils import *

class Monitor:
    file_handle = open("./stream_bn/logs/program_log.txt", "a")
    function_execution_time = {}
    random_explosion_second = random.randint(0, 7) + 13
    
    # Return caller filename and line number
    @staticmethod
    def get_caller_info():
        caller_frame = inspect.stack()[2]
        return [os.path.basename(caller_frame.filename), caller_frame.lineno]

    
    @staticmethod
    def log_trace(wrapped_func):
        """A simple decorator for wrapping methods."""
        @functools.wraps(wrapped_func)
        def wrapper(*args, **kwargs):
            Monitor.file_handle.write(f"{Monitor.get_caller_info()} called {wrapped_func.__name__}\n")
            print(f"{Monitor.get_caller_info()} called {wrapped_func.__name__}")
            

            current_ms_timestamp = datetime.now().timestamp() * 1000
            result = wrapped_func(*args, **kwargs)
            
            Monitor.function_execution_time[wrapped_func.__name__][0] += float(datetime.now().timestamp() * 1000) - current_ms_timestamp
            Monitor.function_execution_time[wrapped_func.__name__][1] += 1.0
            Monitor.file_handle.write(f"{wrapped_func.__name__} average execution time: {Monitor.function_execution_time[wrapped_func.__name__][0] / Monitor.function_execution_time[wrapped_func.__name__][1]} ms.\n")
            print(f"{wrapped_func.__name__} average execution time: {Monitor.function_execution_time[wrapped_func.__name__][0] / Monitor.function_execution_time[wrapped_func.__name__][1]} ms.")
            Monitor.file_handle.flush()
            return result
        return wrapper

    @staticmethod
    def wrap_all_methods(decorator):
        """Class decorator factory that wraps all methods with the given decorator."""
        def class_wrapper(cls):
            for name, method in cls.__dict__.items():
                if name == "depth_order_stream_handler":
                    continue
                Monitor.function_execution_time[name] = [0,0]
                if isinstance(method, (types.FunctionType, types.MethodType)):
                    # Wrap instance method
                    setattr(cls, name, decorator(method))
                elif isinstance(method, classmethod):
                    # Wrap class method
                    original_func = method.__func__
                    wrapped_func = decorator(original_func)
                    setattr(cls, name, classmethod(wrapped_func))
                elif isinstance(method, staticmethod):
                    # Wrap static method
                    original_func = method.__func__
                    wrapped_func = decorator(original_func)
                    setattr(cls, name, staticmethod(wrapped_func))
            return cls
        return class_wrapper

    @staticmethod
    def on_exit():
        print("logger exit")