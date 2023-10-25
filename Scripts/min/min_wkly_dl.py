import os
import sqlite3
import threading
import pandas as pd
import datetime as dt
import yfinance as yf
import concurrent.futures
import pandas_market_calendars as mcal

from sys import argv
from tqdm import tqdm
from io import StringIO
from dateutil import parser
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

#*******************************************
# Change con database PATH
# Change df_tickers read PATH
# Create a dictionary for caching to check speed up
# Batch insert
# Threadpool Concurrent
#*******************************************

# Define your paths
# database = os.path.join(r"C:\Github\sp500_data\Scripts\min", "min_sp500_market_data.db")
# file_path = os.path.join(r"C:\Github\sp500_data\Scripts\min", "sp500_tickers.csv")


database = os.path.join(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\Scripts\min", "min_sp500_market_data.db")
file_path = os.path.join(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\Scripts", "sp500_tickers.csv")


# Initialize a lock for thread safety
yfinance_lock = threading.Lock()

# Create a dictionary for caching
data_exists_cache = {}

# Define a function to get stock data with thread safety
def get_stock_data_safe(symbol, start, end):
    with yfinance_lock:
        data = yf.download(symbol, start=start, end=end, progress=False, interval="1m")
        data.insert(0, "symbol", symbol)
        data.rename(columns={
            "Datetime": "date",
            "Symbol": "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        }, inplace=True)

        return data

def save_data_range(symbol, start, end, con):
    # Create a new database connection for each thread
    thread_con = sqlite3.connect(database)    ## PATH

    data = get_stock_data_safe(symbol, start, end)

    data.to_sql(
        "stock_data",
        thread_con,
        if_exists="append",
        index=True,
        chunksize=100,
    )
    print(f"{symbol} saved between {start} and {end}")

    # Close the thread-specific database connection
    thread_con.close()  # Close the connection after successfully saving the data

def download_and_save_data(symbol):
    try:
        save_data_range(symbol, start, end, con)
    except Exception as e:
        print(f"Error downloading {symbol}: {str(e)}")

#Main Executing code
if __name__ == "__main__":
    con = sqlite3.connect(database)    ## PATH
    df_tickers = pd.read_csv(file_path)    ## PATH

    # if len(argv) == 3:
    #     start = argv[1]
    #     end = argv[2]
    if len(argv) == 3 and argv[1] == "last" and argv[2] == "last":
        min_date = (dt.datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')

        start = min_date #argv[1]
        end = dt.datetime.today().strftime('%Y-%m-%d')        

        # # Rename the "date" column to "Datetime"
        con.execute("ALTER TABLE stock_data RENAME COLUMN date TO Datetime")
        con.commit()

        # Define the number of concurrent threads (adjust as needed)
        num_threads = 16

        # Total number of tickers being downloaded
        total_tickers = len(df_tickers)

        # Create a progress bar using tqdm at the bottom of the screen
        pbar = tqdm(total=total_tickers, desc="Downloading data", position=0, leave=True)

        # Use ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(download_and_save_data, symbol): symbol for symbol in df_tickers['tickers']}

            completed_count = 0
            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                completed_count += 1
                pbar.update(1)  # Update the progress bar for each completed download

        # # Rename the "Datetime" column to "date"
        con.execute("ALTER TABLE stock_data RENAME COLUMN Datetime TO date")
        con.commit()

        # Close the thread-specific database connection
        con.close()
        pbar.close()  # Close the progress bar

    else:
        # ANSI escape code to clear the screen and move the cursor to the top
        print("\x1b[H\x1b[J")
        print("*************************************************************")
        print("S&P 500 data OHLCV Duplicate checker, Daily download/Backfiller")
        print("*************************************************************")
        print("""
Requirements: 'sp500_tickers.csv' in same directory, else specify path in python code

Usage: python market_data.py <start_date> <end_date>
       python back_filler.py last last  (Auto calc last 7 days, max)
        
Date format: 2023-01-01

1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
              
Period: '1mo' Limits on intraday data:              
• 1m = max 7 days within last 30 days
• 30m = max 60 days
• 60m/1h = max 730 days
• else up to 90m = max 60 days

              """)
# DO NOT RUN <start_date> last twice!! Use SQL to delete & commit duplicates if so



