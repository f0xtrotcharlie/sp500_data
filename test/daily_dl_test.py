import sqlite3
import pandas as pd
import datetime as dt
import yfinance as yf
import concurrent.futures
import pandas_market_calendars as mcal

from sys import argv
from io import StringIO
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor

#*******************************************
# Change con database PATH
# Change df_tickers read PATH
# Check db for existing, if have, skips, avoid self duplication method even if run 1<
# Create a dictionary for caching to check speed up
# Batch insert
# Threadpool Concurrent
#*******************************************

#def create_stock_data_table(con):
#     query = '''
#     CREATE TABLE IF NOT EXISTS stock_data (
#         date DATE,
#         symbol TEXT,
#         open REAL,
#         high REAL,
#         low REAL,
#         close REAL,
#         adj_close REAL,
#         volume REAL,
#         PRIMARY KEY (date, symbol)
#     );
#     '''
#     con.execute(query)


# Create a dictionary for caching
data_exists_cache = {}

# Helper function to check if data already exists in the database
# def data_exists(symbol, start, end, con):
def data_exists(symbol, start, end, yfin_last_business_day, con):
    # Check if the result is already cached
    cache_key = (symbol, start, end)
    if cache_key in data_exists_cache:
        return data_exists_cache[cache_key]

    # Use the LIKE operator with a wildcard to match partial dates
    query = f"SELECT COUNT(*) FROM stock_data WHERE symbol = ? AND SUBSTR(date, 1, 10) BETWEEN ? AND ?"
    params = (symbol, f"{start}T00:00:00", f"{yfin_last_business_day}T00:00:00")
    count = con.execute(query, params).fetchone()[0]

    # Cache the result
    data_exists_cache[cache_key] = count > 0

    return count > 0


def get_stock_data(symbol, start, end):
    with StringIO() as buf, redirect_stdout(buf):
        data = yf.download(symbol, start=start, end=end)
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
    try:
        # Create a new database connection for each thread
        thread_con = sqlite3.connect(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\test\test.db")

        if not data_exists(symbol, start, end, yfin_last_business_day, thread_con):
        # if not data_exists(symbol, start, end, thread_con):
            data = get_stock_data(symbol, start, end)

            data.to_sql(
                "stock_data",
                thread_con,
                if_exists="append",
                index=True,
                chunksize=100,
            )
            print(f"{symbol} saved between {start} and {end}")
        else:
            print(f"{symbol} data between {start} and {end} already exists. Skipping...")
        
        # Close the thread-specific database connection
        thread_con.close()
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

        start_date = argv[1]
        end_date = argv[2]
        # Get the valid days within the specified date range
        valid_days = mcal.get_calendar('NYSE').valid_days(start_date=start_date, end_date=end_date)

        #Extract the last date from the valid days, -2 as we will always be running at 12 noon HKT/SGT
        sec_last_biz_day = valid_days[-3].strftime('%Y-%m-%d')
        last_business_day = valid_days[-2].strftime('%Y-%m-%d')  #this is correct but yfin doesnt include the right most date
        yfin_last_business_day = valid_days[-1].strftime('%Y-%m-%d')


        # Use ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(lambda symbol: save_data_range(symbol, start, end, con), df_tickers['tickers'])

        # Print the tickers that failed to download
        failed_tickers = [symbol for symbol in df_tickers['tickers'] if not data_exists(symbol, start, end, yfin_last_business_day, con)]
        if failed_tickers:
            print("")
            print("*************************************************")
            print("Failed to download data for the following tickers:")
            for symbol in failed_tickers:
                print(symbol)

            # Save the failed tickers to a text file
            with open("failed_tickers.txt", "w") as file:
                for symbol in failed_tickers:
                    file.write(symbol + "\n")
    

            # Total number of tickers being downloaded
            total_tickers = len(df_tickers)
    
            completed_count = 0
            for _ in df_tickers.iterrows():
                completed_count += 1

            print(f"Progress: {completed_count}/{total_tickers}") ##edit not updating well
        
        print("")
        print(f"Data downloaded for all symbols on {last_business_day}.")


    else:
        print("")
        print("*****************************")
        print("S&P 500 data OHLCV downloader, BACKFILLER + Duplicate checker")
        print("*****************************")
        print("")
        print("Requirements: 'sp500_tickers.csv' in same directory")
        print("")
        print("Usage: python back_filler.py <start_date> <end_date>")
        print("Date format: 2023-01-01")


# Add this line to close the SQLite connection when done
con.close()


