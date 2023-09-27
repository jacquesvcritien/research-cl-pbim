import sys
import os
from binance.client import Client
from datetime import datetime,timedelta
import json


def format_date(date_string):
    date = datetime.strptime(date_string, "%Y-%m-%d")
    return date.strftime("%d %b, %Y")
    
# Read args
args = sys.argv

if len(args) < 2:
    print("Please pass in a feed like: python binance-data-getter.py ETHUSD FROM TO")
    exit()

if len(args) < 3:
    print("Please pass in a FROM date like: python binance-data-getter.py ETHUSD 2021-01-01 TO")
    exit()

if len(args) < 4:
    print("Please pass in a FROM date like: python binance-data-getter.py ETHUSD 2021-01-01 2023-01-01")
    exit()


symbol = args[1].upper()
start_date = datetime.strptime(args[2], "%Y-%m-%d")
end_date = datetime.strptime(args[3], "%Y-%m-%d")

with open("./binance-credentials.json") as json_file:
    credentials = json.load(json_file)
    api_key = credentials["api_key"]
    api_secret = credentials["api_secret"]

# Initialize the Binance client
client = Client(api_key, api_secret, requests_params={'timeout': 30})

# Create an array to store the date ranges
date_ranges = []

# Calculate date ranges with 6-month intervals
while start_date <= end_date:
    date_ranges.append((start_date, start_date + timedelta(days=180)))
    start_date += timedelta(days=181)
    
output_data = []

for date_range in date_ranges:
    from_date = format_date(date_range[0].strftime("%Y-%m-%d"))
    to_date = format_date(date_range[1].strftime("%Y-%m-%d"))
    print("Getting data between "+from_date+" to "+to_date)

    # Fetch klines (candlestick data) for the specified date range
    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, from_date, to_date)

    # Print the retrieved klines data
    for kline in klines:
        timestamp, open_price, high, low, close, volume, close_timestamp, quote_asset_volume, num_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore = kline
        entry = {
            "Timestamp": timestamp,
            "Open": open_price,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume
        }
        output_data.append(entry)

# Write the JSON array to a file
output_file = "data/binance/binance_data_"+symbol+"_1min.json"
with open(output_file, "w") as json_output:
    json.dump(output_data, json_output, indent=4)

print(f"Data saved to {output_file}")