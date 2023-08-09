import re
import warnings

import pandas as pd
import numpy as np
import pytz

from cachetools import TTLCache
from datetime import datetime, timedelta
from functools import reduce
from pandas import DataFrame

import talib.abstract as ta

import freqtrade.vendor.qtpylib.indicators as qtpylib
from freqtrade.exchange import timeframe_to_minutes, timeframe_to_prev_date
from freqtrade.strategy import (IStrategy, merge_informative_pair, stoploss_from_open, informative,
                                IntParameter, DecimalParameter, CategoricalParameter)
from freqtrade.persistence import Trade

import psycopg2
from questdb.ingress import Sender
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
#from prettytable import PrettyTable

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

""" 
Freqtrade Consumer Strategy 
to receive the dataframes of producers and store them in QuestDB for storage or further processing

Requirements:
pip install influxdb, questdb, cachetools

"""

class Consumer(IStrategy):
    
    def version(self) -> str:
        return "v1.0"
    
    # Trailing stop:
    trailing_stop = False
    
    timeframe = '5m'    
    can_short = True
        
    store_refresh_period = 300  # 5 minutes
    pair_cache = TTLCache(maxsize=100, ttl=store_refresh_period)
    
    stoploss = -0.06
    use_custom_stoploss = False
    
    use_entry_signal = True
    entry_profit_only = False
    ignore_roi_if_entry_signal = False

    process_only_new_candles = False # required for consumers
    use_custom_stoploss = False

    engine = None

    producers = ['ft_1', 'ft_2', 'ft_3', 'ft_4','ft_5']
    producer_tfs = {
        'ft_1': '5m',
        'ft_2': '5m',
        'ft_3': '5m',
        'ft_4': '5m',
        'ft_5': '5m',
    }

    def bot_start(self, **kwargs) -> None:
        conn_str = 'postgresql+psycopg2://admin:quest@questdb:8812/qdb'
        self.engine = create_engine(conn_str, echo=False)
        
        return

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
                        
        pair = metadata['pair']
        dataframe['mid'] = (dataframe['open'] + dataframe['close']) / 2.0
        timeframe = self.timeframe
        #log.debug(f"Processing indicators for pair {pair} with main timeframe {timeframe}")

        expected_columns = []  # Initialize the variable before the loop
        required_columns_default_0 = []
        required_columns_default_none = []
        
        for producer in self.producers:
            producer_timeframe = self.producer_tfs.get(producer, '5m')
            #log.debug(f"Fetching data for producer {producer} with timeframe {producer_timeframe}")

            producer_dataframe, _ = self.dp.get_producer_df(pair, producer_name=producer, timeframe=producer_timeframe)

            if not producer_dataframe.empty:
                #log.debug(f"Data for producer {producer} is not empty.")

                columns = [
                    f'enter_long_{producer}',
                    f'exit_long_{producer}',
                    f'enter_short_{producer}',
                    f'exit_short_{producer}',
                ]
                expected_columns.extend(columns)
                
                required_columns_default_0.extend([f'{col}_{producer}' for col in ['enter_long', 'enter_short', 'exit_long', 'exit_short']])
                required_columns_default_none.extend([f'{col}_{producer}' for col in ['enter_tag','exit_tag']])

                # Check if the suffixed versions of 'enter_long' and 'enter_short' are in producer_dataframe
                should_store_df = all(col in producer_dataframe.columns for col in ['enter_long', 'enter_short'])

                cache_key = f"{pair}_{producer}"
                if cache_key not in self.pair_cache and should_store_df:
                    sdf = producer_dataframe.copy()
                    self.store_df(sdf, producer, pair, self.engine, host='questdb', port=9009)
                    #log.debug(f"Storing dataframe for pair {pair} and producer {producer} completed.")
                    self.pair_cache[cache_key] = True

                dataframe = merge_informative_pair(dataframe, producer_dataframe, timeframe,
                                                producer_timeframe,  # Using the producer's timeframe here
                                                append_timeframe=False,
                                                suffix=producer, ffill=True)
            else:
                log.debug(f"Data for producer {producer} is empty.")
        
        for col in expected_columns:
            if col not in dataframe.columns:
                if col in required_columns_default_0:
                    dataframe[col] = 0
                elif col in required_columns_default_none:
                    dataframe[col] = None
                    
        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=14)
        dataframe['mfi'] = ta.MFI(dataframe, timeperiod=14)

        return dataframe
    
    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
    
        dataframe.loc[:, 'enter_tag'] = ''
        
        long_conditions = [(dataframe[f'enter_long_{producer}'].fillna(False).astype(bool)) for producer in self.producers if f'enter_long_{producer}' in dataframe.columns]
        short_conditions = [(dataframe[f'enter_short_{producer}'].fillna(False).astype(bool)) for producer in self.producers if f'enter_short_{producer}' in dataframe.columns]
        
        # Checking and combining all producer conditions
        if long_conditions:
            combined_long_condition = reduce(lambda x, y: x & y, long_conditions)
            dataframe.loc[combined_long_condition, 'enter_tag'] += 'consumer_enter '
            dataframe.loc[combined_long_condition, 'enter_long'] = 1
        else:
            dataframe['enter_long'] = 0
        
        if short_conditions:
            combined_short_condition = reduce(lambda x, y: x & y, short_conditions)
            dataframe.loc[combined_short_condition, 'enter_tag'] += 'consumer_enter '
            dataframe.loc[combined_short_condition, 'enter_short'] = 1
        else:
            dataframe['enter_short'] = 0

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
    
        dataframe.loc[:, 'exit_tag'] = ''
        
        long_exit_conditions = [(dataframe[f'exit_long_{producer}'].fillna(False).astype(bool)) for producer in self.producers if f'exit_long_{producer}' in dataframe.columns]
        short_exit_conditions = [(dataframe[f'exit_short_{producer}'].fillna(False).astype(bool)) for producer in self.producers if f'exit_short_{producer}' in dataframe.columns]
        
        # Checking and combining all producer conditions
        if long_exit_conditions:
            combined_long_exit_condition = reduce(lambda x, y: x & y, long_exit_conditions)
            dataframe.loc[combined_long_exit_condition, 'exit_tag'] += 'consumer_exit '
            dataframe.loc[combined_long_exit_condition, 'exit_long'] = 1
        else:
            dataframe['exit_long'] = 0
        
        if short_exit_conditions:
            combined_short_exit_condition = reduce(lambda x, y: x & y, short_exit_conditions)
            dataframe.loc[combined_short_exit_condition, 'exit_tag'] += 'consumer_exit '
            dataframe.loc[combined_short_exit_condition, 'exit_short'] = 1
        else:
            dataframe['exit_short'] = 0

        return dataframe
    
    
    def store_df(self, sdf: DataFrame, strategy: str, pair: str, engine, host: str = 'questdb', port: int = 9009) -> None:
        """ Store the sdf into a database for a specific strategy and pair. """
        #log.debug("Starting store_df function...")
        ft_pair = pair
        pair = pair.split('/')[0]
        sdf.insert(0, 'pair', pair)
        sdf.insert(1, 'ft_pair', ft_pair) 
        sdf.insert(2, 'time_interval', self.timeframe)
        sdf_clean = self.clean_dataframe(sdf)
        #log.debug("Cleaned the dataframe...")

        with engine.connect() as connection:
            try:
                last_inserted_timestamp = self.get_last_inserted_timestamp(connection, strategy, pair)
                log.debug(f"Last inserted row for strategy {strategy} and pair {pair}: {last_inserted_timestamp}")

                data_to_insert = self.get_data_to_insert(sdf_clean, last_inserted_timestamp)
                
                if not data_to_insert.empty:
                    self.send_data_to_sender(data_to_insert, host, port, strategy)
                    log.info(f"New rows stored for strategy: {strategy}, pair: {pair}")

                connection.commit()
            except DatabaseError as e:
                self.handle_database_error(e, sdf_clean, strategy, pair, host, port)

    def clean_dataframe(self, sdf_clean: DataFrame) -> DataFrame:
        
        sdf_clean.columns = [re.sub('[%&-]', '_', col) for col in sdf_clean.columns]
        sdf_clean = sdf_clean.replace(True, 1).replace(False, 0)
    
        return sdf_clean

    def get_last_inserted_timestamp(self, connection, strategy, pair) -> datetime:
        query = text(f'select max(timestamp) from {strategy} where pair = :pair')
        result = connection.execute(query, {"pair": pair}).fetchone()
        
        if result and result[0]:
            #log.debug(f"Timestamp retrieved from the database: {result[0]}")
            return result[0].replace(tzinfo=pytz.UTC)
        else:
            #log.debug("No rows found in the database. Returning earliest datetime in dataframe...")
            return datetime.min.replace(tzinfo=pytz.UTC)

    def send_data_to_sender(self, data_to_insert: DataFrame, host: str, port: int, strategy: str) -> None:
        with Sender(host, port) as sender:
            #log.debug(f"Sending data to QuestDB {host}:{port} for table {strategy}...")
            sender.dataframe(data_to_insert, table_name=strategy, at='date')
            sender.flush()
            #log.debug("Data sent successfully.")

    def handle_database_error(self, e: Exception, sdf: DataFrame, strategy: str, pair: str, host: str, port: int) -> None:
        if "table does not exist" in str(e).lower():
            log.debug("Table does not exist. QuestDB will create the table.")
            self.send_data_to_sender(sdf, host, port, strategy)
        else:
            log.error(f"Unhandled database error: {e}")

    def get_data_to_insert(self, sdf: DataFrame, last_inserted_timestamp: datetime) -> DataFrame:
        sdf['date'] = pd.to_datetime(sdf['date'])
        filtered_df = sdf[sdf['date'] > last_inserted_timestamp]
        return filtered_df
