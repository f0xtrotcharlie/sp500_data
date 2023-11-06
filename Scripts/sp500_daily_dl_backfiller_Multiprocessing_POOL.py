import os
import time
import sqlite3
import threading
import pandas as pd
import datetime as dt
import multiprocessing
import yfinance as yf
import concurrent.futures
import pandas_market_calendars as mcal

from sys import argv
from tqdm import tqdm
from io import StringIO
from functools import partial
from multiprocessing import Pool
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
 
#*******************************************
# ACTUALLY Multi threaded but safe insertion into SQL db
# Use Pool Multi processing
# Robust checking in database for date, if date exists, will NOT download
# Download only from last entry till specified date/ today date when 'last' is passed 
# Create a dictionary for caching to check speed up
# DO NOT run 'date last' twice in a row. If so, use SQL to drop duplicates, code below in ''' '''
# Change con database PATH to yours
# Change df_tickers read PATH to yours
# First run + pass 'last' for cron job for daily download
# Batch insert
#
# Pool Multi processing:
# https://www.youtube.com/watch?v=AZnGRKFUU0c&list=PLF3MFTbl2HDkZAWXSQ16dqpiNAd4AU74k&index=5&t=736s
# https://www.youtube.com/watch?v=X7vBbelRXn0&list=PLF3MFTbl2HDkZAWXSQ16dqpiNAd4AU74k&index=4&t=388s
#==================================
# Deprecated
#-----------------
# Threadpool Concurrent
#*******************************************

# Define your paths

# database = os.path.join(r"C:\Github\sp500_data\Scripts\min", "min_sp500_market_data.db")
# file_path = os.path.join(r"C:\Github\sp500_data\Scripts\min", "sp500_tickers.csv")
database = os.path.join(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\Scripts", "sp500_market_data.db")
file_path = os.path.join(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\Scripts", "sp500_tickers.csv")

# Initialize a lock for thread safety
yfinance_lock = threading.Lock()

# Create a dictionary for caching
data_exists_cache = {}

# Create a wrapper function to pass the required arguments
def wrapper(args):
    symbol, start, end = args
    return save_data_range(symbol, start, end)


def get_stock_data(symbol, start, end):
    with yfinance_lock:
        data = yf.download(symbol, start=start, end=end, progress=False)
        data.insert(0, "symbol", symbol)
        
        data.rename(columns={
            "Date": "date",
            "Symbol": "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        }, inplace=True)

        return data


# Helper function to check if data already exists in the database
def data_exists(symbol, start, end, thread_con):
    # Check if the result is already cached
    cache_key = (symbol, start, end)
    if cache_key in data_exists_cache:
        return data_exists_cache[cache_key]
    
    # Get the list of dates in the specified date range
    all_dates = pd.date_range(start=start, end=end).strftime('%Y-%m-%d').tolist()

    # Check if data exists for the symbol
    query = "SELECT DISTINCT SUBSTR(date, 1, 10) FROM stock_data WHERE symbol = ? AND SUBSTR(date, 1, 10) BETWEEN ? AND ?"
    params = (symbol, start, end)

    # count = con.execute(query, params).fetchone()[0]
    dates_in_db= [date[0] for date in thread_con.execute(query, params).fetchall()]

    # Find the dates that do not exist in the database
    dates_not_in_db = [date for date in all_dates if date not in dates_in_db]

    # Cache the result
    data_exists_cache[cache_key] = dates_in_db

    return dates_in_db


def save_data_range(symbol, start, end, pbar=None):
    try:
        # Create a new database connection for each thread
        thread_con = sqlite3.connect(database)  # Create a new connection

        # Clear the cache for the current cache_key
        cache_key = (symbol, start, end)
        if cache_key in data_exists_cache:
            del data_exists_cache[cache_key]

        # Check for dates that exist in the database and do not exist
        dates_in_db = data_exists(symbol, start, end, thread_con)
        all_dates = pd.date_range(start=start, end=end).strftime('%Y-%m-%d').tolist()
        dates_not_in_db = [date for date in all_dates if date not in dates_in_db]
        last_date_plus1 = (datetime.strptime(dates_in_db[-1], '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        # Handle the special case for "last", doesn't work for now. Use SQL code to drop duplicates, keep 1 copy
        if end == dt.datetime.today().strftime('%Y-%m-%d'):
            # Get data from the last date in db +1 (to avoid double entry) till end which takes in argv[2]
            data = get_stock_data(symbol, last_date_plus1, end)

            data.to_sql(
                "stock_data",
                thread_con,
                if_exists="append",
                index=True,
                chunksize=100,
            )

        else:
            # Get data from the last date in db +1 (to avoid double entry) till end which takes in argv[2]
            data = get_stock_data(symbol, last_date_plus1, end)

            data.to_sql(
                "stock_data",
                thread_con,
                if_exists="append",
                index=True,
                chunksize=100,
            )

        # Close the thread-specific database connection
        thread_con.close()

        print("")
        print(f"{symbol} saved between {start} and {end}")

    except Exception as e:
        error_message = f"Error downloading {symbol}: {str(e)}"
        print(error_message)
        return f"Error downloading: {symbol} - {str(e)}"
    
    
def main():
    df_tickers = pd.read_csv(file_path)     ## PATH

    # Total number of tickers being downloaded
    total_tickers = len(df_tickers)

    # Define the number of concurrent threads
    num_threads = 16

    # Define start and end here
    start = None
    end = None

    # Wipe screen
    print("\x1b[H\x1b[J")

    if len(argv) == 3 and argv[2] == "last":
        start = argv[1]
        end = dt.datetime.today().strftime('%Y-%m-%d')

    elif len(argv) == 3:
        # Code to handle the case with start and end dates
        start = argv[1]
        end = argv[2]

    # Create a list of tuples containing (symbol, start, end) for each ticker
    args_list = [(ticker, start, end) for ticker in df_tickers['tickers']]

    if start and end:
        with multiprocessing.Pool(processes=num_threads) as pool:
            results = list(pool.imap_unordered(wrapper, args_list))

        # Create a progress bar using tqdm at the bottom of the screen
        pbar = tqdm(total=total_tickers, desc="Downloading data", position=0, leave=True)

        for result in results:
            # Check if the result is an error message
            if isinstance(result, str):
                print(result)  # Print the error message if there's an issue
            pbar.update(1)  # Update the progress bar for each completed download

        pbar.close()  # Close the progress bar


    else:
        # ANSI escape code to clear the screen and move the cursor to the top
        print("\x1b[H\x1b[J")
        print("*************************************************************")
        print("S&P 500 data OHLCV Duplicate checker, DAILY download/Backfiller")
        print("*************************************************************")
        print("""
Requirements: 'sp500_tickers.csv' in same directory, else specify path in python code

Usage: python market_data.py <start_date> <end_date>
       python back_filler.py <start_date> last
Date format: 2023-01-01

DO NOT RUN '<start_date> last' twice!! Use SQL to delete & commit duplicates if so""")


#Main Executing code
if __name__ == "__main__":
    start_t = time.perf_counter()

    multiprocessing.set_start_method('spawn')  # Try 'fork' or 'spawn'
    main()

    end_t = time.perf_counter()
    total_duration = end_t - start_t
    print(f"Took {total_duration:.2f}s in total")