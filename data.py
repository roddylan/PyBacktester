from abc import ABCMeta, abstractmethod
import datetime
import os, os.path

import numpy as np
import pandas as pd

from event import MarketEvent

class DataHandler:
    '''
    Abstract base class for providing market data to Strategy + Portfolio.
    '''

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        '''
        Returns the last bar updated
        '''
        raise NotImplementedError("Should implement get_latest_bar()")
    
    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        '''
        Returns a Python datetime obj. for the last bar
        '''
        raise NotImplementedError("Should implement get_latest_bars()")
    
    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        '''
        Returns one of the Open, High, Low, Close, Volume of OI
        from the last bar
        '''
        raise NotImplementedError("Should implement get_latest_bar_value()")
    
    @abstractmethod
    def get_latest_bar_values(self, symbol, val_type, N=1):
        '''
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available
        '''
        raise NotImplementedError("Should implement get_latest_bar_values()")
    
    @abstractmethod
    def update_bars(self):
        '''
        Pushes the latest bars to the barse_queue for each symbol
        in a tuple OHLCVI format:
        (datetime, open, high, low, close, volume, open interest)
        '''
        raise NotImplementedError("Should implement update_bars()")
    

# Import historical data via common sources
# TODO: convert to Securities Master DB
