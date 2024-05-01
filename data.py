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
class HistoricCSVDataHandler(DataHandler):
    '''
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.

    Converts csv (1 per symbol) to dict of pd.DataFrame-s; can be
    accessed by bar methods. 
    '''

    def __init__(self, events, csv_dir, symbol_list):
        '''
        Init the historic data handler by requesting the 
        location of the CSV files and a list of symbols

        Assume that all files are of form '<symbol>.csv', 
        where symbol is a string in the list
        
        Parameters:
        events - Event Queue
        csv_dir - absolute dir. path to CSV files
        symbol_list - list of symbol strings
        '''
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        '''
        Open CSV file from abs. dir., converts them to
        pd.DataFrame() w/in a symbol dictionary.

        Assume that the data is taken from Yahoo. 
        Thus, its format will be respected.
        '''
        comb_idx = None
        for s in self.symbol_list:
            # load .csv file w/ no header info, indexd on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, f"{s}.csv"), header=0, 
                             index_col=0, parse_dates=True,
                             names=[
                                 'datetime', 'open', 'high',
                                 'low', 'close', 'volume',
                                 'adj_close'
                             ]).sort()
            
            # combine the idx to pad forward values
            if comb_idx is None:
                comb_idx = self.symbol_data[s].index
            else:
                comb_idx.union(self.symbol_data[s].index)

            # set the latest symbol_data to None
            self.latest_symbol_data[s] = []


        # reindex dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_idx, method='pad'
            ).iterrows() # iterate over rows


    def _get_new_bar(self, symbol):
        '''
        Returns the latest bar from the data feed.
        Creates generator to provide a new bar; 
        subsequent call yield a new bar until 
        the end of the symbol data is reached
        '''
        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bar(self, symbol):
        '''
        Returns the last bar from the latest_symbol list.
        
        Provides either a bar or list of last N bars from 
        latest_symbol_data structure
        '''
        
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set")
            raise
        else:
            return bars_list[-1]
        
    def get_latest_bars(self, symbol, N=1):
        '''
        Returns the last N bars from the latest_symbol 
        list, or N-k if less available.
        
        Provides either a bar or list of last N bars from 
        latest_symbol_data structure
        '''
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]
        
    def get_latest_bar_datetime(self, symbol):
        '''
        Returns a python datetime obj. for the last bar
        '''
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set")
            raise
        else:
            return bars_list[-1][0]
        
    def get_latest_bar_value(self, symbol, val_type):
        '''
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object
        '''
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
        
    def get_latest_bar_values(self, symbol, val_type, N=1):
        '''
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available
        '''
        try:
            bars_list = self.get_latest_bars(symbol=symbol, N=N)
        except KeyError:
            print("That symbol is not available in the historical data set")
            raise
        else:
            return np.array(
                [getattr(b[1], val_type) for b in bars_list]
            )
        
    def update_bars(self):
        '''
        Pushes the latest bar to the latest_symbol_data
        structure for all symbols in the symbol list

        Generates MarketEvent that gets added to the queue 
        as it appends latest bars to dict
        '''
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

    
