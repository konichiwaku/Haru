# scripts/crypto_drawdown_uploader.py
import os
import requests
import json
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config

# --- Konfigurasi KuCoin API ---
KUCOIN_TICKER_API = "https://api.kucoin.com/api/v1/market/orderbook/level1"
KUCOIN_KLINE_API = "https://api.kucoin.com/api/v1/market/candles"

# --- DAFTAR KOIN YANG AKAN DIPROSES ---
# Format: {"Nama Koin": "SYMBOL-USDT di KuCoin", ...}
# Ganti dengan simbol KuCoin yang benar. Pastikan "PEPE-USDT" ada di KuCoin jika Anda menambahkannya.
COIN_CONFIGS = {
    "Arbitrum": "ARB-USDT",
    "Bitcoin": "BTC-USDT",
    "Solana": "SOL-USDT",
    "Litecoin": "LTC-USDT",
    "Pepe": "PEPE-USDT"
    # Tambahkan koin lain di sini sesuai kebutuhan
}

# --- Konfigurasi Cloudflare R2 ---
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL") # Contoh: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")
# R2_FILE_KEY akan menjadi dinamis berdasarkan nama koin

# --- Fungsi Pembantu Tanggal ---
def get_days_difference(date1_str, date2_obj=None):
    try:
        dt1 = datetime.fromisoformat(date1_str.replace("Z", "+00:00"))
    except ValueError:
        dt1 = datetime.strptime(date1_str, "%Y-%m-%dT%H:%M:%S")

    if date2_obj is None:
        dt2 = datetime.now(timezone.utc)
    else:
        dt2 = date2_obj

    diff_time = abs((dt2 - dt1).total_seconds())
    diff_days = round(diff_time / (60 * 60 * 24))
    return diff_days

def format_date_for_display(timestamp_ms):
    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt_object.strftime("%d %B %Y")

def format_date_for_xaxis(timestamp_ms):
    dt_object = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt_object.strftime("%d %b")

# --- Pengambilan Data dari KuCoin ---
def fetch_kucoin_ticker_price(symbol):
    params = {"symbol": symbol}
    try:
        response = requests.get(KUCOIN_TICKER_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data["code"] == "200000" and data["data"]:
            return float(data["data"]["price"])
        else:
            print(f"Error fetching ticker for {symbol}: {data.get('msg', 'Unknown error')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for ticker {symbol}: {e}")
        return None

def fetch_kucoin_kline_data(symbol, days_for_ath_atl=365, days_for_chart=7):
    # Fetch 1-day kline data for ATH/ATL calculation over a long history
    ath_val, ath_date_str, atl_val, atl_date_str = None, None, None, None

    # Try fetching up to 3 years of daily data for better ATH/ATL accuracy if available
    # KuCoin API max limit for kline is usually 1500 points per request.
    # 3 years * 365 days = ~1095 days, so one request might be sufficient.
    start_time_long = int((datetime.now(timezone.utc) - timedelta(days=days_for_ath_atl)).timestamp())

    try:
        response_long_term = requests.get(
            KUCOIN_KLINE_API,
            params={"symbol": symbol, "type": "1day", "from": start_time_long},
            timeout=15 # Increase timeout for potentially larger requests
        )
        response_long_term.raise_for_status()
        long_term_data = response_long_term.json()

        if long_term_data["code"] == "200000" and long_term_data["data"]:
            prices_for_ath_atl = []
            # Store (price, timestamp_ms) to correctly find corresponding date
            all_kline_data_for_ath_atl = []

            for kline in long_term_data["data"]:
                timestamp_ms = int(kline[0]) * 1000
                high_price = float(kline[2])
                low_price = float(kline[3])
                prices_for_ath_atl.extend([high_price, low_price])
                all_kline_data_for_ath_atl.append({"timestamp": timestamp_ms, "high": high_price, "low": low_price})

            if prices_for_ath_atl:
                ath_val = max(prices_for_ath_atl)
                atl_val = min(prices_for_ath_atl)

                # Find exact date for ATH/ATL
                for kline_point in all_kline_data_for_ath_atl:
                    if kline_point["high"] == ath_val and ath_date_str is None:
                        ath_date_str = datetime.fromtimestamp(kline_point["timestamp"] / 1000, tz=timezone.utc).isoformat()
                    if kline_point["low"] == atl_val and atl_date_str is None:
                        atl_date_str = datetime.fromtimestamp(kline_point["timestamp"] / 1000, tz=timezone.utc).isoformat()
                    if ath_date_str and atl_date_str:
                        break
            else:
                print(f"No long-term kline data available for {symbol}.")

        else:
            print(f"Error fetching long-term kline data for {symbol}: {long_term_data.get('msg', 'Unknown error')}")

    except requests.exceptions.RequestException as e:
        print(f"Request error for long-term kline {symbol}: {e}")

    # Fetch 1-hour kline data for the last 'days_for_chart' for the chart
    klines_data_for_chart = []
    end_time_chart_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time_chart_ms = int((datetime.now(timezone.utc) - timedelta(days=days_for_chart)).timestamp() * 1000)

    try:
        response_chart_data = requests.get(
            KUCOIN_KLINE_API,
            params={"symbol": symbol, "type": "1hour", "from": start_time_chart_ms // 1000, "to": end_time_chart_ms // 1000},
            timeout=10
        )
        response_chart_data.raise_for_status()
        chart_data = response_chart_data.json()

        if chart_data["code"] == "200000" and chart_data["data"]:
            for kline in chart_data["data"]:
                timestamp_ms = int(kline[0]) * 1000
                price = float(kline[1]) # Using 'open' price for simplicity for chart points
                klines_data_for_chart.append({"timestamp": timestamp_ms, "price": price})
        else:
            print(f"Error fetching chart kline data for {symbol}: {chart_data.get('msg', 'Unknown error')}")

    except requests.exceptions.RequestException as e:
        print(f"Request error for chart kline {symbol}: {e}")

    return ath_val, ath_date_str, atl_val, atl_date_str, klines_data_for_chart

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
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]):
        print("Error: R2 environment variables are not set. Exiting.")
        return

    for coin_name, kucoin_symbol in COIN_CONFIGS.items():
        print(f"\n--- Processing {coin_name} ({kucoin_symbol}) ---")

        # Fetch ATH/ATL and 7-day historical data
        ath_value, ath_date_raw_str, atl_value, atl_date_raw_str, historical_prices = fetch_kucoin_kline_data(kucoin_symbol, days_for_ath_atl=730) # Fetch 2 years (730 days) of daily data for better ATH/ATL

        if ath_value is None or atl_value is None or not historical_prices:
            print(f"Failed to retrieve complete data for {coin_name}. Skipping.")
            continue

        # Fetch current price
        current_price = fetch_kucoin_ticker_price(kucoin_symbol)
        if current_price is None:
            print(f"Failed to retrieve current price for {coin_name}. Skipping.")
            continue

        print(f"Current Price: ${current_price:.4f}")
        print(f"ATH: ${ath_value:.4f} on {ath_date_raw_str}")
        print(f"ATL: ${atl_value:.4f} on {atl_date_raw_str}")


        # Calculate days since ATH
        days_since_ath = get_days_difference(ath_date_raw_str) if ath_date_raw_str else 'N/A'

        # Append current price to historical data for a more real-time feel on the chart
        historical_prices.append({"timestamp": int(datetime.now(timezone.utc).timestamp() * 1000), "price": current_price})

        # Sort historical_prices by timestamp to ensure chronological order
        historical_prices.sort(key=lambda x: x["timestamp"])

        # Prepare historical data for frontend (drawdown calculation happens in frontend)
        # We only need to send the raw prices and timestamps, frontend will calculate drawdown
        formatted_historical_data = []
        for item in historical_prices:
            formatted_historical_data.append({
                "date": format_date_for_xaxis(item["timestamp"]), # Formatted for X-axis
                "price": item["price"]
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
        print(f"Generated JSON data for {coin_name}:")
        # print(json_output) # Uncomment for full debug

        # Determine R2 file key based on coin name (e.g., "arbitrum_drawdown_data.json")
        r2_file_key = f"{coin_name.lower().replace(' ', '_')}_drawdown_data.json"
        
        # Upload to R2
        try:
            upload_to_r2(json_output, R2_BUCKET_NAME, r2_file_key)
        except Exception as e:
            print(f"Failed to upload data for {coin_name} to R2: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

