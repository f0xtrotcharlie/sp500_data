import sqlite3
import pandas as pd
import yfinance as yf
import datetime as dt
import concurrent.futures 
import pandas_market_calendars as mcal

from sys import argv
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from io import StringIO

# Setup cron job to run 12pm noon SGT/HKT every day
# Add error handling, for which tickers failed



# Define your desired date range
start_date = '2023-01-01'
end_date = dt.datetime.today().strftime('%Y-%m-%d')

# Get the valid days within the specified date range
valid_days = mcal.get_calendar('NYSE').valid_days(start_date=start_date, end_date=end_date)

#Extract the last date from the valid days, -2 as we will always be running at 12 noon HKT/SGT
sec_last_biz_day = valid_days[-3].strftime('%Y-%m-%d')
last_business_day = valid_days[-2].strftime('%Y-%m-%d')

# Helper function to check if data already exists in the database
def data_exists(symbol, start, end, con):
    query = f"SELECT COUNT(*) FROM stock_data WHERE symbol = ? AND date BETWEEN ? AND ?"
    params = (symbol, start, end)
    count = con.execute(query, params).fetchone()[0]
    return count > 0


# Function to format numeric values with commas
def format_with_commas(value):
    if isinstance(value, (int, float)):
        return '{:,.0f}'.format(value)  # Format as integer with commas
    return value  # Keep non-numeric values unchanged


def get_stock_data(symbol, start, end):
    with StringIO() as buf, redirect_stdout(buf):
        data = yf.download(symbol, start=start, end=end)
    data.insert(0, "symbol", symbol)
    
    data.rename(columns={
        "symbol": "symbol",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    }, inplace=True)

    data['volume'].apply(format_with_commas)

    return data


def save_data_range(symbol, start, end, con):
    if not data_exists(symbol, start, end, con):
        data = get_stock_data(symbol, start, end)

        data.to_sql(
            "stock_data", 
            con, 
            if_exists="append", 
            index=True
        )
        print(f"{symbol} saved between {start} and {end}")
    else:
        print(f"{symbol} data between {start} and {end} already exists. Skipping...")


def save_last_trading_session(symbol, con):
    try:
        if not data_exists(symbol, sec_last_biz_day, last_business_day, con):
            data = get_stock_data(symbol, sec_last_biz_day, last_business_day)

            # # Set the date column as the index
            # data.set_index('date', inplace=True)
            
            # # Rename the symbol column to match the symbol
            # data.rename(columns={'symbol': symbol}, inplace=True)

            data.to_sql(
                "stock_data", 
                con, 
                if_exists="append", 
                index=True
            )
            print(f"{symbol} saved for {last_business_day}")
        else:
            print(f"{symbol} data for {last_business_day} already exists. Skipping...")
    except Exception as e:
        
        print(f"Error downloading {symbol}: {str(e)}")        


#code that dowloads
if __name__ == "__main__":
    con = sqlite3.connect(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\sp500_market_data.db")

    if len(argv) == 2 and argv[1] == "last":
        df_tickers = pd.read_csv(r"C:\Users\Jonat\Documents\MEGAsync\MEGAsync\Github\sp500_data\Scripts\sp500_tickers.csv")
        
        for _, row in df_tickers.iterrows():
            symbol = row['tickers']
            save_last_trading_session(symbol, con)
            #print(f"{symbol} saved for the last business day")

        # Define the number of concurrent threads (adjust as needed)
        num_threads = 16
        
        def download_and_save_data(symbol):
            save_last_trading_session(symbol, con)
            print(f"{symbol} saved for the last business day")
        

        # Use ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(lambda symbol: save_last_trading_session(symbol, con), df_tickers['tickers'])

        # Print the tickers that failed to download
        failed_tickers = [symbol for symbol in df_tickers['tickers'] if not data_exists(symbol, sec_last_biz_day, last_business_day, con)]
        if failed_tickers:
            print("Failed to download data for the following tickers:")
            for symbol in failed_tickers:
                print(symbol)

            # Save the failed tickers to a text file
            with open("failed_tickers.txt", "w") as file:
                for symbol in failed_tickers:
                    file.write(symbol + "\n")


            # Total number of tickers being downloaded
            total_tickers = len(df_tickers)

            completed_count = 0  ##### smarter
            for _ in df_tickers.iterrows():
                completed_count += 1
                print(f"Progress: {completed_count}/{total_tickers}")

        print("")
        # print(f"Progress: {total_tickers}/{total_tickers}")
        print(f"Data downloaded for all symbols on {last_business_day}.")


        # Add this line to close the SQLite connection when done
        con.close()

    else:
        print("Usage: python market_data.py last")