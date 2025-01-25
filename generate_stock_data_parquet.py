import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import random
import string
import sys

# List of potential stock trading-related column names, including datetime columns
# Ensure that no column name is repeated
stock_columns = [
    "id", "business_date", "ticker", "open_price", "close_price", "high", "low", "volume",
    "market_cap", "pe_ratio", "dividend_yield", "moving_average", "macd", "rsi", "beta",
    "price_to_book", "price_to_sales", "eps", "average_daily_volume", "52_week_high",
    "52_week_low", "previous_close", "market_trend", "industry", "sector", "exchange",
    "currency", "current_ratio", "quick_ratio", "debt_to_equity", "price_to_earnings_growth",
    "book_value", "cash_flow", "free_cash_flow", "net_profit_margin", "operating_margin",
    "gross_margin", "return_on_equity", "return_on_assets", "volatility", "average_price",
    "beta_coefficient", "day_range", "intraday_high", "intraday_low", "intraday_open",
    "intraday_close", "closing_change", "trailing_pe", "forward_pe", "earnings_date",
    "option_volume", "call_put_ratio", "implied_volatility", "options_open_interest",
    "options_expiration_date", "dividend_date", "last_trade_time", "last_earnings_report_date",
    "datetime"  # Added multiple datetime-related columns
]

# Ensure there are no duplicates in the stock_columns list
stock_columns = list(dict.fromkeys(stock_columns))

def generate_random_data(n_columns, n_rows):
    data = {}

    # First column as 'dna_id' with sequential integer values
    data["id"] = np.arange(1, n_rows + 1)  # Generates sequential integers starting from 1

    # Second column as 'business_date' with datetime values (business days)
    business_dates = pd.date_range(start="2025-01-01", periods=n_rows, freq="B")
    data["business_date"] = business_dates

    # Generate a ticker column with random stock symbols
    data["ticker"] = [random.choice(["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "NFLX", "FB", "NVDA", "SPY", "SPX"]) for _ in range(n_rows)]

    # Generate the remaining stock-related columns
    for i in range(3, n_columns):  # Start from the 3rd column (skip dna_id and business_date)
        column_name = stock_columns[i - 1]  # Get the name from the list
        data_type = random.choice(['int', 'float', 'bool', 'string', 'datetime'])

        if data_type == 'int':
            data[column_name] = np.random.randint(0, 100, size=n_rows)
        elif data_type == 'float':
            data[column_name] = np.round(np.random.rand(n_rows) * 1000, 2)  # Stock prices, market cap, etc.
        elif data_type == 'bool':
            data[column_name] = np.random.choice([True, False], size=n_rows)
        elif data_type == 'string':
            data[column_name] = [''.join(random.choices(string.ascii_letters, k=5)) for _ in range(n_rows)]
        elif data_type == 'datetime':
            if column_name == "earnings_date":
                data[column_name] = pd.to_datetime(np.random.choice(pd.date_range('2025-01-01', '2025-12-31', freq='D'), size=n_rows))  # Earnings date within the year
            elif column_name == "option_expiration_date":
                data[column_name] = pd.to_datetime(np.random.choice(pd.date_range('2025-01-01', '2025-12-31', freq='W-MON'), size=n_rows))  # Weekly expiration dates
            elif column_name == "dividend_date":
                data[column_name] = pd.to_datetime(np.random.choice(pd.date_range('2025-01-01', '2025-12-31', freq='2W'), size=n_rows))  # Biweekly dividend dates
            elif column_name == "last_trade_time":
                data[column_name] = pd.date_range(start="2025-01-01", periods=n_rows, freq="15T")  # 15-minute intervals for trades
            elif column_name == "last_earnings_report_date":
                data[column_name] = pd.to_datetime(np.random.choice(pd.date_range('2025-01-01', '2025-12-31', freq='3MS'), size=n_rows))  # Quarterly earnings report date
            else:
                data[column_name] = pd.date_range(start="2025-01-01", periods=n_rows, freq="15T")  # Default datetime, 15-minute intervals

    return pd.DataFrame(data)

def save_to_parquet(df, file_path):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)

def main(n_columns, n_rows, file_path):
    df = generate_random_data(n_columns, n_rows)
    save_to_parquet(df, file_path)
    print(f"Parquet file generated: {file_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_parquet_file.py <n_columns> <n_rows> <output_file_path>")
        sys.exit(1)

    n_columns = int(sys.argv[1])
    n_rows = int(sys.argv[2])
    output_file_path = sys.argv[3]

    main(n_columns, n_rows, output_file_path)
