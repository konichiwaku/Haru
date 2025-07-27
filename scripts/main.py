import os
import requests
import json
import boto3
from datetime import datetime, timedelta
import time # Tambahkan ini untuk fungsi sleep

# --- Konfigurasi Umum ---
KUCOIN_API_BASE = "https://api.kucoin.com/api/v1"
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
# Pastikan R2_PUBLIC_URL_BASE sesuai dengan URL bucket R2 Anda
R2_PUBLIC_URL_BASE = f"https://pub-{R2_ACCOUNT_ID}.r2.dev/{R2_BUCKET_NAME}"

# File di R2 yang menyimpan data ATH/ATL global untuk semua koin
# Pastikan path ini sesuai dengan lokasi 'all_coins_ath.json' di bucket R2 Anda
ALL_COINS_ATH_FILE = "all_coins_ath.json" # Perbaikan path dari pembahasan sebelumnya

# --- Fungsi Utility Cloudflare R2 ---
def get_r2_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )

def download_json_from_r2(client, key):
    try:
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        print(f"Successfully downloaded '{key}' from R2.")
        return json.loads(response['Body'].read().decode('utf-8'))
    except client.exceptions.NoSuchKey:
        print(f"File '{key}' not found in R2. Returning empty dict. Please ensure it's uploaded.")
        return {}
    except Exception as e:
        print(f"Error downloading '{key}' from R2: {e}")
        raise # Re-raise exception to stop execution if essential file is missing/inaccessible

def upload_json_to_r2(client, data, key):
    try:
        s3_object = json.dumps(data, indent=2)
        client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=key,
            Body=s3_object,
            ContentType='application/json',
            ACL='public-read' # Pastikan bisa diakses publik
        )
        print(f"Successfully uploaded '{key}' to R2.")
    except Exception as e:
        print(f"Error uploading '{key}' to R2: {e}")
        raise

# --- Fungsi KuCoin API ---
def get_kucoin_klines(symbol, interval="1day", limit=100): # Default limit 100 hari untuk chart data
    """
    Mengambil data candlestick dari KuCoin.
    interval: '1min', '3min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '6hour', '8hour', '12hour', '1day', '1week'
    limit: jumlah candlestick terakhir yang diambil. Max 1500 for 1day, 2000 for 1hour.
    """
    url = f"{KUCOIN_API_BASE}/market/candles?symbol={symbol}&type={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=15) # Tambahkan timeout yang lebih lama
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        klines = response.json().get('data', [])
        # Klines format: [timestamp, open, close, high, low, volume]
        # Pastikan timestamp adalah string atau integer
        return klines
    except requests.exceptions.RequestException as e:
        print(f"Error fetching klines for {symbol} from KuCoin: {e}")
        return []

# --- Fungsi Perhitungan ---
def calculate_drawdown_percentage(current_price, ath_value):
    if ath_value == 0:
        return 0.0
    return ((ath_value - current_price) / ath_value) * 100

def get_days_ago(target_timestamp_ms):
    if not target_timestamp_ms:
        return None
    target_date = datetime.fromtimestamp(target_timestamp_ms / 1000)
    today = datetime.now()
    delta = today - target_date
    return delta.days

# --- Fungsi Utama ---
def main():
    r2_client = get_r2_client()

    # 1. Unduh data ATH/ATL global untuk semua koin
    all_coins_ath_data = download_json_from_r2(r2_client, ALL_COINS_ATH_FILE)
    if not all_coins_ath_data:
        print("Initial 'all_coins_ath.json' is empty or not found. Please populate it manually on R2.")
        print("Example structure for a coin (e.g., 'ICP'):")
        print(json.dumps({
            "ICP": {
                "ath_value": 700.65,
                "ath_date_timestamp_ms": 1620691200000, # 11 May 2021
                "atl_value": 2.87,
                "atl_date_timestamp_ms": 1717027200000, # 29 May 2024
                "source": "manual_initial"
            }
        }, indent=2))
        return # Hentikan eksekusi jika file config tidak ada

    # Daftar koin yang akan diproses (Ambil dari keys di all_coins_ath_data)
    coin_symbols_to_process = all_coins_ath_data.keys()

    for coin_symbol in coin_symbols_to_process:
        print(f"\n--- Processing {coin_symbol} ---")
        
        ath_info = all_coins_ath_data.get(coin_symbol, {})
        current_ath_value = ath_info.get("ath_value", 0.0)
        current_ath_date_ms = ath_info.get("ath_date_timestamp_ms")
        current_atl_value = ath_info.get("atl_value", float('inf')) # Inisialisasi ATL dengan nilai sangat besar
        current_atl_date_ms = ath_info.get("atl_date_timestamp_ms")

        # 2. Ambil data candlestick terbaru dari KuCoin (misal 100 hari terakhir, interval 1 hari)
        # Sesuaikan 'limit' sesuai 'data_period_description' yang Anda inginkan
        klines = get_kucoin_klines(f"{coin_symbol}-USDT", interval="1day", limit=100) 

        if not klines:
            print(f"No klines data for {coin_symbol}. Skipping to next coin.")
            continue

        chart_data_points = []
        highest_price_in_fetched_klines = 0.0
        lowest_price_in_fetched_klines = float('inf')
        
        # KuCoin API mengembalikan data dari yang paling baru ke paling lama, kita ingin yang paling lama ke paling baru
        klines.reverse() 

        for kline in klines:
            # KuCoin kline format: [timestamp, open, close, high, low, volume, amount]
            try:
                timestamp_sec = int(kline[0])
                close_price = float(kline[2])
                high_price_kline = float(kline[3])
                low_price_kline = float(kline[4])
            except (ValueError, IndexError) as e:
                print(f"Skipping malformed kline data for {coin_symbol}: {kline} Error: {e}")
                continue

            # Update highest/lowest price dalam periode klines yang diambil
            if high_price_kline > highest_price_in_fetched_klines:
                highest_price_in_fetched_klines = high_price_kline
            if low_price_kline < lowest_price_in_fetched_klines:
                lowest_price_in_fetched_klines = low_price_kline

            # Hitung drawdown untuk titik data ini relatif terhadap current_ath_value
            # Penting: Pastikan current_ath_value tidak 0 saat ini untuk menghindari ZeroDivisionError
            drawdown_val = calculate_drawdown_percentage(close_price, current_ath_value)
            
            chart_data_points.append({
                "date": datetime.fromtimestamp(timestamp_sec).strftime("%Y-%m-%d"), # Format tanggal YYYY-MM-DD untuk chart JS
                "value": round(drawdown_val, 2)
            })
        
        # 3. Perbarui ATH global jika harga dari KuCoin lebih tinggi
        if highest_price_in_fetched_klines > current_ath_value:
            print(f"New ATH for {coin_symbol} detected: {highest_price_in_fetched_klines:.4f} (Old: {current_ath_value:.4f})")
            current_ath_value = highest_price_in_fetched_klines
            current_ath_date_ms = int(datetime.now().timestamp() * 1000)
            ath_info["ath_value"] = current_ath_value
            ath_info["ath_date_timestamp_ms"] = current_ath_date_ms
            ath_info["source"] = "kucoin_updated_ath"

        # 4. Perbarui ATL jika ditemukan nilai yang lebih rendah
        # Logika: Jika ATL yang baru ditemukan lebih rendah dari yang tersimpan, atau jika ATL tersimpan masih 0/inf.
        if lowest_price_in_fetched_klines > 0 and (lowest_price_in_fetched_klines < current_atl_value or current_atl_value == float('inf')):
            print(f"New ATL for {coin_symbol} detected (in fetched data): {lowest_price_in_fetched_klines:.4f} (Old: {current_atl_value:.4f})")
            current_atl_value = lowest_price_in_fetched_klines
            current_atl_date_ms = int(datetime.now().timestamp() * 1000) # Bisa jadi tanggal dari kline itu sendiri
            ath_info["atl_value"] = current_atl_value
            ath_info["atl_date_timestamp_ms"] = current_atl_date_ms
            ath_info["source"] = "kucoin_updated_atl"
        
        # Simpan pembaruan ATH/ATL kembali ke struktur all_coins_ath_data
        all_coins_ath_data[coin_symbol] = ath_info

        # 5. Ambil harga terkini (dari klines terakhir setelah reverse)
        current_price = float(klines[-1][2]) # Harga penutupan kline terakhir dari data yang sudah di-reverse

        # 6. Hitung drawdown saat ini
        drawdown_current = calculate_drawdown_percentage(current_price, current_ath_value)

        # 7. Siapkan data output JSON per koin
        output_data = {
            "coin_symbol": coin_symbol.upper(),
            "current_price": round(current_price, 4),
            "all_time_high": round(current_ath_value, 4),
            "all_time_high_date": datetime.fromtimestamp(current_ath_date_ms / 1000).strftime("%d %b %Y") if current_ath_date_ms else "N/A", # <-- Format baru
            "all_time_high_days_ago": get_days_ago(current_ath_date_ms),
            "all_time_low": round(current_atl_value, 4) if current_atl_value != float('inf') else "N/A", # Handle inf
            "all_time_low_date": datetime.fromtimestamp(current_atl_date_ms / 1000).strftime("%d %b %Y") if current_atl_date_ms else "N/A", # <-- Format baru
            "drawdown_percentage_current": round(drawdown_current, 2),
            "data_period_description": f"{len(chart_data_points)} Hari Terakhir + Real-Time", # Sesuaikan dengan limit klines
            "chart_data": chart_data_points
        }
        
        # 8. Unggah data output JSON per koin ke R2
        output_filename = f"data/{coin_symbol.lower()}_drawdown_data.json"
        upload_json_to_r2(r2_client, output_data, output_filename)

        # Tambahkan jeda kecil antar koin untuk mematuhi rate limit KuCoin
        time.sleep(0.05) # Jeda 50ms antar panggilan API

    # Setelah semua koin diproses, unggah kembali file ATH/ATL global yang mungkin sudah diperbarui
    print("\n--- Uploading updated all_coins_ath.json ---")
    upload_json_to_r2(r2_client, all_coins_ath_data, ALL_COINS_ATH_FILE)
    print("All coin data processed and uploaded.")

if __name__ == "__main__":
    main()
