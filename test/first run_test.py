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
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor

#*******************************************
# Change con database PATH
# Change df_tickers read PATH
# Create a dictionary for caching to check speed up
# Batch insert
# Threadpool Concurrent
#*******************************************

# Initialize a lock for thread safety
yfinance_lock = threading.Lock()

# Create a dictionary for caching
data_exists_cache = {}

# Define a function to get stock data with thread safety
def get_stock_data_safe(symbol, start, end):
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

def save_data_range(symbol, start, end, con):
    # Create a new database connection for each thread
    thread_con = sqlite3.connect(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\test\test.db")

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
    con = sqlite3.connect(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\test\test.db")
    
    if len(argv) == 3:
        start = argv[1]
        end = argv[2]
        df_tickers = pd.read_csv(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\test\test.csv")

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

        # Close the thread-specific database connection
        con.close()
        pbar.close()  # Close the progress bar

    else:
        print("")
        print("*****************************")
        print("S&P 500 data OHLCV downloader")
        print("*****************************")
        print("1st time Backfilling")
        print("Requirements: 'sp500_tickers.csv' in the same directory, else specify the path in the python code")
        print("DO NOT RUN THIS TO BACKFILL, USE sp500_daily_dl_backfiller")
        print("")
        print("Usage: python market_data.py <start_date> <end_date>")
        print("Date format: 2023-01-01")
