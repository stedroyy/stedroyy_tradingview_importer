import pandas as pd
import os
from datetime import datetime
import re
import pytz
import colorful as cf

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Eastern timezone
eastern = pytz.timezone("US/Eastern")

# Function to parse TradingView data line
def parse_tradingview_line(line):
    try:
        parts = line.strip().split(',')
        if len(parts) != 5:
            print(f"Invalid line format: {line}")
            return None

        timestamp_str = parts[0].replace('T', ' ')
        open_price = float(parts[1])
        high_price = float(parts[2])
        low_price = float(parts[3])
        close_price = float(parts[4])

        # Parse timestamp
        ts_dt = pd.to_datetime(timestamp_str)
        if ts_dt.tzinfo is None:
            ts_dt = eastern.localize(ts_dt)  # only localize if naive

        return {
            'ts_event': ts_dt,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price
        }
    except Exception as e:
        cf.print(f"{cf.red}Error parsing line: {line} - {e}{cf.reset}")
        return None

# Ensure any value is a tz-aware datetime
def ensure_tz(dt):
    if isinstance(dt, str):
        dt = pd.to_datetime(dt)
    if dt.tzinfo is None:
        dt = eastern.localize(dt)
    return dt

# Function to update or append data to the CSV
def update_csv(csv_file, tradingview_data):
    # Load existing CSV or create new
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        try:
            df = pd.read_csv(csv_file)
            # Convert ts_event to tz-aware datetime safely
            df['ts_event'] = df['ts_event'].apply(ensure_tz)
        except Exception:
            cf.print(f"{cf.yellow}CSV file empty or malformed. Creating a new DataFrame.{cf.reset}")
            df = pd.DataFrame(columns=['ts_event', 'open', 'high', 'low', 'close', 'volume'])
    else:
        cf.print(f"{cf.yellow}CSV file not found or empty. Creating a new DataFrame.{cf.reset}")
        df = pd.DataFrame(columns=['ts_event', 'open', 'high', 'low', 'close', 'volume'])

    # Append/update data
    for data in tradingview_data:
        if data is None:
            continue

        timestamp = ensure_tz(data['ts_event'])
        existing = df['ts_event'] == timestamp
        if existing.any():
            df.loc[existing, ['open', 'high', 'low', 'close']] = [
                data['open'], data['high'], data['low'], data['close']
            ]
        else:
            new_row = pd.DataFrame({
                'ts_event': [timestamp],
                'open': [data['open']],
                'high': [data['high']],
                'low': [data['low']],
                'close': [data['close']],
                'volume': [0.0]
            })
            # Match dtypes to existing DataFrame to avoid warnings/errors
            if not df.empty:
                new_row = new_row.astype(df.dtypes.to_dict(), errors='ignore')
            df = pd.concat([df, new_row], ignore_index=True)

    # Sort by tz-aware datetime
    df = df.sort_values('ts_event').reset_index(drop=True)

    # Save to CSV with tz-aware format YYYY-MM-DD HH:MM:SS-04:00
    df_to_save = df.copy()
    df_to_save['ts_event'] = df_to_save['ts_event'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S%z'))
    df_to_save['ts_event'] = df_to_save['ts_event'].str.replace(r'([+-]\d{2})(\d{2})$', r'\1:\2', regex=True)

    df_to_save.to_csv(csv_file, index=False)
    cf.print(f"{cf.green}Updated CSV file:{cf.reset} {csv_file}")

def main():
    csv_file = 'historical_ohlcv_15m.csv'
    print("Paste your TradingView data (one line per 15-minute interval).")
    print("Enter an empty line to finish input.")

    tradingview_data = []
    while True:
        line = input()
        if line.strip() == '':
            break
        parsed_data = parse_tradingview_line(line)
        if parsed_data:
            tradingview_data.append(parsed_data)

    if tradingview_data:
        update_csv(csv_file, tradingview_data)
    else:
        print("No valid data entered.")

if __name__ == "__main__":
    main()
