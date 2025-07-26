# scripts/kucoin_r2_uploader.py
import os
import requests
import json
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config

# --- Konfigurasi KuCoin API ---
# KuCoin API Endpoint untuk harga terakhir (ticker)
KUCOIN_TICKER_API = "https://api.kucoin.com/api/v1/market/orderbook/level1"
# KuCoin API Endpoint untuk data historis (kline)
KUCOIN_KLINE_API = "https://api.kucoin.com/api/v1/market/candles"
SYMBOL = "ARB-USDT" # Pasangan trading Arbitrum

# --- Konfigurasi Cloudflare R2 ---
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL") # Contoh: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME") # Nama bucket R2 Anda
R2_FILE_KEY = "arb_drawdown_data.json" # Nama file di R2

# --- Fungsi Pembantu Tanggal ---
def get_days_difference(date1_str, date2_obj=None):
    # date1_str: tanggal dalam format ISO string (dari API)
    # date2_obj: objek datetime (default ke UTC now)
    try:
        dt1 = datetime.fromisoformat(date1_str.replace("Z", "+00:00"))
    except ValueError: # Handle jika format tidak standar ISO (misal tanpa timezone)
        dt1 = datetime.strptime(date1_str, "%Y-%m-%dT%H:%M:%S")

    if date2_obj is None:
        dt2 = datetime.now(timezone.utc)
    else:
        dt2 = date2_obj

    diff_time = abs((dt2 - dt1).total_seconds())
    diff_days = round(diff_time / (60 * 60 * 24)) # Pembulatan ke hari terdekat
    return diff_days

def format_date_for_display(timestamp_ms):
    # Mengonversi timestamp milidetik ke objek datetime lokal
    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
    # Format untuk keterangan (DD Month YYYY)
    return dt_object.strftime("%d %B %Y")

def format_date_for_xaxis(timestamp_ms):
    # Mengonversi timestamp milidetik ke objek datetime lokal
    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
    # Format untuk sumbu X (DD Mon)
    return dt_object.strftime("%d %b")

# --- Pengambilan Data dari KuCoin ---
def fetch_kucoin_ticker_price(symbol):
    params = {"symbol": symbol}
    try:
        response = requests.get(KUCOIN_TICKER_API, params=params, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if data["code"] == "200000" and data["data"]:
            return float(data["data"]["price"])
        else:
            print(f"Error fetching ticker for {symbol}: {data.get('msg', 'Unknown error')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for ticker {symbol}: {e}")
        return None

def fetch_kucoin_kline_data(symbol, days=7):
    # KuCoin KLine API returns data for specified interval
    # We need to fetch data for 7 days, likely hourly or daily for ATH/ATL
    # For a granular 7-day chart, typically hourly is good.
    # '1hour', '4hour', '1day'
    klines_data = []
    end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)

    # KuCoin's kline API is designed for fixed time ranges or paginated.
    # For simplicity, we'll try to get 7 days of 1-hour data in one go if possible,
    # or loop if the limit is exceeded. KuCoin's API limit is 1500 per request.
    # 7 days * 24 hours = 168 data points, so one request is enough.
    
    # We will fetch daily data to quickly find ATH/ATL over a longer period (e.g., 365 days)
    # and 1-hour data for the last 7 days for the chart itself.

    # Fetch 1-day kline data for ATH/ATL calculation over a long history
    try:
        response_long_term = requests.get(
            KUCOIN_KLINE_API,
            params={"symbol": symbol, "type": "1day"},
            timeout=10
        )
        response_long_term.raise_for_status()
        long_term_data = response_long_term.json()
        if long_term_data["code"] == "200000" and long_term_data["data"]:
            prices_for_ath_atl = []
            for kline in long_term_data["data"]:
                prices_for_ath_atl.append(float(kline[2])) # High price
                prices_for_ath_atl.append(float(kline[3])) # Low price

            if not prices_for_ath_atl: # Handle empty prices_for_ath_atl
                return None, None, None, None, []

            # Find ATH/ATL from fetched historical data
            ath_val = max(prices_for_ath_atl)
            atl_val = min(prices_for_ath_atl)

            # Find date of ATH and ATL from the long-term data
            ath_date_str = None
            atl_date_str = None
            for kline in long_term_data["data"]:
                kline_timestamp = int(kline[0]) * 1000 # Convert to milliseconds
                kline_datetime = datetime.fromtimestamp(kline_timestamp / 1000, tz=timezone.utc)
                
                if float(kline[2]) == ath_val and ath_date_str is None: # High price
                    ath_date_str = kline_datetime.isoformat()
                if float(kline[3]) == atl_val and atl_date_str is None: # Low price
                    atl_date_str = kline_datetime.isoformat()
                if ath_date_str and atl_date_str: # Found both, break early
                    break
            
            # If ATH/ATL not found in current daily data (might be very old), default to None
            # Or consider fetching even longer history if this is a common issue for older coins
            if ath_date_str is None: # Fallback: take latest ATH/ATL data
                print("Warning: ATH date not found in current daily data. This might be an old ATH.")
                # You might need to adjust this logic if older ATH/ATL are critical.
                # KuCoin's ATH/ATL data might be in a different metadata endpoint or require very long kline history.
                # For this example, we'll assume the ATH/ATL from the fetched kline data is sufficient.
                # If specific ATH/ATL is needed, CoinGecko's /coins/{id} endpoint is better for that.
                # For KuCoin, you might need to scrape/parse their website or use a different endpoint if available.
                # For now, we simulate ATH/ATL from the fetched klines.
                pass
            if atl_date_str is None:
                print("Warning: ATL date not found in current daily data. This might be an old ATL.")
                pass


            # Fetch 1-hour kline data for the last 7 days for the chart
            response_7_day = requests.get(
                KUCOIN_KLINE_API,
                params={"symbol": symbol, "type": "1hour", "from": start_time_ms // 1000, "to": end_time_ms // 1000},
                timeout=10
            )
            response_7_day.raise_for_status()
            seven_day_data = response_7_day.json()

            if seven_day_data["code"] == "200000" and seven_day_data["data"]:
                # KuCoin returns klines in ascending order (oldest first)
                for kline in seven_day_data["data"]:
                    # kline format: [timestamp, open, close, high, low, amount, vol]
                    timestamp_ms = int(kline[0]) * 1000 # convert to milliseconds
                    price = float(kline[1]) # Using 'open' price for simplicity for chart points
                    klines_data.append({"timestamp": timestamp_ms, "price": price})
            else:
                print(f"Error fetching 7-day kline data for {symbol}: {seven_day_data.get('msg', 'Unknown error')}")
                return None, None, None, None, []

            return ath_val, ath_date_str, atl_val, atl_date_str, klines_data

        else:
            print(f"Error fetching long-term kline data for {symbol}: {long_term_data.get('msg', 'Unknown error')}")
            return None, None, None, None, []

    except requests.exceptions.RequestException as e:
        print(f"Request error for kline {symbol}: {e}")
        return None, None, None, None, []

# --- Upload ke Cloudflare R2 ---
def upload_to_r2(file_content, bucket_name, file_key):
    s3_client = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_content,
            ContentType="application/json",
            CacheControl="public, max-age=7200" # Cache selama 2 jam (7200 detik) sesuai update
        )
        print(f"Successfully uploaded {file_key} to R2 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to R2: {e}")
        raise

# --- Fungsi Utama ---
async def main():
    print(f"Starting data fetching for {SYMBOL}...")

    # Fetch ATH/ATL and 7-day historical data
    ath_value, ath_date_raw_str, atl_value, atl_date_raw_str, historical_prices = fetch_kucoin_kline_data(SYMBOL, days=365) # Fetch daily data for a year to find ATH/ATL, and 7 days hourly for chart

    if ath_value is None or atl_value is None:
        print("Failed to retrieve ATH/ATL data. Exiting.")
        return

    # Fetch current price
    current_price = fetch_kucoin_ticker_price(SYMBOL)
    if current_price is None:
        print("Failed to retrieve current price. Exiting.")
        return

    print(f"Current Price: ${current_price}")
    print(f"ATH: ${ath_value} on {ath_date_raw_str}")
    print(f"ATL: ${atl_value} on {atl_date_raw_str}")

    # Calculate days since ATH
    days_since_ath = get_days_difference(ath_date_raw_str) if ath_date_raw_str else 'N/A'

    # Prepare historical data for frontend (drawdown calculation happens in frontend)
    formatted_historical_data = []
    # Append current price to historical data for a more real-time feel on the chart
    historical_prices.append({"timestamp": int(datetime.now(timezone.utc).timestamp() * 1000), "price": current_price})

    # Sort historical_prices by timestamp to ensure chronological order
    historical_prices.sort(key=lambda x: x["timestamp"])

    for item in historical_prices:
        formatted_historical_data.append({
            "date": format_date_for_xaxis(item["timestamp"]),
            "price": item["price"] # Pass actual price, drawdown calculated on frontend
        })

    # Prepare data for JSON
    output_data = {
        "ath": {
            "value": ath_value,
            "date": format_date_for_display(datetime.fromisoformat(ath_date_raw_str.replace("Z", "+00:00")).timestamp() * 1000) if ath_date_raw_str else "N/A",
            "daysAgo": days_since_ath
        },
        "atl": {
            "value": atl_value,
            "date": format_date_for_display(datetime.fromisoformat(atl_date_raw_str.replace("Z", "+00:00")).timestamp() * 1000) if atl_date_raw_str else "N/A"
        },
        "currentPrice": current_price,
        "historicalPrices": formatted_historical_data,
        "lastUpdated": datetime.now(timezone.utc).isoformat()
    }

    json_output = json.dumps(output_data, indent=4)
    print("Generated JSON data:")
    print(json_output)

    # Upload to R2
    upload_to_r2(json_output, R2_BUCKET_NAME, R2_FILE_KEY)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

