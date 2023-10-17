import sqlite3
import pandas as pd
import yfinance as yf
import concurrent.futures 

from sys import argv
import concurrent.futures 
from io import StringIO
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor


# Check db for existing, if have, skips, avoid self duplication method even if run 1<
# Create a dictionary for caching to check speed up
# Batch insert
# Threadpool Concurrent



# Create a dictionary for caching
data_exists_cache = {}

# Helper function to check if data already exists in the database
def data_exists(symbol, start, end, con):
    # Check if the result is already cached
    cache_key = (symbol, start, end)
    if cache_key in data_exists_cache:
        return data_exists_cache[cache_key]
    
    query = f"SELECT COUNT(*) FROM stock_data WHERE symbol = ? AND date BETWEEN ? AND ?"
    params = (symbol, start, end)
    count = con.execute(query, params).fetchone()[0]

    # Cache the result
    data_exists_cache[cache_key] = count > 0

    return count > 0   

def get_stock_data(symbol, start, end):
    with StringIO() as buf, redirect_stdout(buf):
        data = yf.download(symbol, start=start, end=end)
    # data.reset_index(inplace=True)   #either this line or below
    data = data.set_index("date", inplace=True)

    data['symbol'] = symbol
    data.rename(columns={
        # "Date": "date",
        "symbol": "symbol",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    }, inplace=True)

    return data


def save_data_range(symbol, start, end, con):
    if not data_exists(symbol, start, end, con):
        data = get_stock_data(symbol, start, end)

        # Set the date column as the index
        data.set_index('date', inplace=True)

        # Rename the symbol column to match the symbol
        data.rename(columns={'symbol': symbol}, inplace=True)

        data.to_sql(
            "stock_data", 
            con, 
            if_exists="append", 
            index=True,
            chunksize=100
        )
        print(f"{symbol} saved between {start} and {end}")
    else:
        print(f"{symbol} data between {start} and {end} already exists. Skipping...")



if __name__ == "__main__":
    con = sqlite3.connect(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\PyQuant\sp500 data\sp500_market_data.db")


    if len(argv) == 3:
        start = argv[1]
        end = argv[2]
        df_tickers = pd.read_csv(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\PyQuant\sp500 data\Scripts\sp500_tickers.csv")
        # df_tickers = pd.read_csv("sp500_tickers.csv")

        for _, row in df_tickers.iterrows():
            symbol = row['tickers']
            save_data_range(symbol, start, end, con)


        # Define the number of concurrent threads (adjust as needed)
        num_threads = 16
        
        def download_and_save_data(symbol):
            save_data_range(symbol, start, end, con)
            print(f"{symbol} saved between {start} and {end}")
        
        # Use ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(lambda symbol: save_data_range(symbol, start, end, con), df_tickers['tickers'])

    
            completed_count = 0
            for _ in df_tickers.iterrows():
                completed_count += 1
                print(f"Progress: {completed_count}/{total_tickers}")

    else:
        print("")
        print("*****************************")
        print("S&P 500 data OHLCV downloader")
        print("*****************************")
        print("1st time Backfilling, ONLY RUN THIS ONCE!! Can run to backfill if miss some dates")
        print("Requirements: 'sp500_tickers.csv' in same directory")
        print("")
        print("Usage: python market_data.py <start_date> <end_date>")
        print("Date format: 2023-01-01")


# Add this line to close the SQLite connection when done
con.close()
# add to auto pickup sp500.csv file instead of having to manually set directory, still fail cant pick up
#  now still need manually set directory in code

