import os
import requests
import json
import boto3
from datetime import datetime, timedelta

# --- Konfigurasi Umum ---
KUCOIN_API_BASE = "https://api.kucoin.com/api/v1"
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_PUBLIC_URL_BASE = f"https://pub-{R2_ACCOUNT_ID}.r2.dev/{R2_BUCKET_NAME}" # Ganti jika Anda pakai custom domain

# File di R2 yang menyimpan data ATH global untuk semua koin
ALL_COINS_ATH_FILE = "config/all_coins_ath.json"

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
        return json.loads(response['Body'].read().decode('utf-8'))
    except client.exceptions.NoSuchKey:
        print(f"File '{key}' not found in R2. Returning empty dict.")
        return {}
    except Exception as e:
        print(f"Error downloading '{key}' from R2: {e}")
        raise

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
def get_kucoin_klines(symbol, interval="1day", limit=30):
    """
    Mengambil data candlestick dari KuCoin.
    limit: jumlah candlestick terakhir yang diambil.
    """
    url = f"{KUCOIN_API_BASE}/market/candles?symbol={symbol}&type={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10) # Tambahkan timeout
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        klines = response.json().get('data', [])
        return klines
    except requests.exceptions.RequestException as e:
        print(f"Error fetching klines for {symbol} from KuCoin: {e}")
        return []

# --- Fungsi Perhitungan ---
def calculate_drawdown_percentage(current_price, ath_value):
    if ath_value == 0:
        return 0.0 # Hindari pembagian dengan nol
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

    # 1. Unduh data ATH global untuk semua koin
    all_coins_ath_data = download_json_from_r2(r2_client, ALL_COINS_ATH_FILE)
    if not all_coins_ath_data:
        print("Initial 'all_coins_ath.json' is empty or not found. Please populate it manually.")
        # Atau Anda bisa tambahkan logika untuk mencari ATH awal dari Coingecko di sini,
        # tapi itu akan makan rate limit Coingecko, jadi lebih baik manual di awal.
        return

    # Daftar koin yang akan diproses (Ambil dari keys di all_coins_ath_data)
    # Anda bisa juga membuat daftar koin terpisah jika struktur all_coins_ath_data berbeda
    coin_symbols_to_process = all_coins_ath_data.keys()

    processed_data_for_r2_upload = {} # Untuk menyimpan semua data yang akan diupload per koin

    for coin_symbol in coin_symbols_to_process:
        print(f"\n--- Processing {coin_symbol} ---")
        
        ath_info = all_coins_ath_data.get(coin_symbol, {})
        current_ath_value = ath_info.get("ath_value", 0.0)
        current_ath_date_ms = ath_info.get("ath_date_timestamp_ms")
        current_atl_value = ath_info.get("atl_value", 0.0)
        current_atl_date_ms = ath_info.get("atl_date_timestamp_ms")

        # 2. Ambil data candlestick terbaru dari KuCoin (misal 30 hari terakhir)
        # Type '1day' atau '8hour' jika mau lebih detail. Untuk chart gambar, 1 day cukup
        # Sesuaikan 'limit' dengan periode yang Anda inginkan di chart (misal 30 hari = limit 30)
        klines = get_kucoin_klines(f"{coin_symbol}-USDT", interval="1day", limit=30) 

        if not klines:
            print(f"No klines data for {coin_symbol}. Skipping.")
            continue

        chart_data_points = []
        highest_price_in_fetched_klines = 0.0
        lowest_price_in_fetched_klines = float('inf') # Untuk ATL, jika ATL terjadi di periode chart
        
        # Urutkan klines berdasarkan timestamp (KuCoin API biasanya sudah terurut, tapi baiknya dipastikan)
        # Klines format: [timestamp, open, close, high, low, volume]
        klines.sort(key=lambda x: int(x[0])) 

        for kline in klines:
            timestamp_sec = int(kline[0])
            close_price = float(kline[2])
            high_price_kline = float(kline[3])
            low_price_kline = float(kline[4])

            # Update highest/lowest price dalam periode klines yang diambil
            if high_price_kline > highest_price_in_fetched_klines:
                highest_price_in_fetched_klines = high_price_kline
            if low_price_kline < lowest_price_in_fetched_klines:
                lowest_price_in_fetched_klines = low_price_kline

            # Hitung drawdown untuk titik data ini relatif terhadap current_ath_value
            drawdown_val = calculate_drawdown_percentage(close_price, current_ath_value)
            
            chart_data_points.append({
                "date": datetime.fromtimestamp(timestamp_sec).strftime("%Y-%m-%d"), # Format tanggal YYYY-MM-DD
                "value": round(drawdown_val, 2) # Rundown ke 2 desimal
            })
        
        # 3. Update ATH global jika harga dari KuCoin lebih tinggi
        if highest_price_in_fetched_klines > current_ath_value:
            print(f"New ATH for {coin_symbol} detected: {highest_price_in_fetched_klines:.2f} (Old: {current_ath_value:.2f})")
            current_ath_value = highest_price_in_fetched_klines
            current_ath_date_ms = int(datetime.now().timestamp() * 1000) # Set tanggal ATH baru ke sekarang
            ath_info["ath_value"] = current_ath_value
            ath_info["ath_date_timestamp_ms"] = current_ath_date_ms
            ath_info["source"] = "kucoin_updated" # Tandai sumber update

        # Update ATL jika ATL terjadi di periode data yang baru diambil DAN lebih rendah dari ATL tersimpan
        # Catatan: Mencari ATL global dari KuCoin sangat sulit karena data historis terbatas.
        # ATL di sini mungkin lebih cenderung ke ATL dalam periode chart atau ATL yang 'baru'.
        if lowest_price_in_fetched_klines < current_atl_value or current_atl_value == 0:
            if lowest_price_in_fetched_klines > 0: # Pastikan bukan inf
                print(f"New ATL for {coin_symbol} detected in recent data: {lowest_price_in_fetched_klines:.4f} (Old: {current_atl_value:.4f})")
                current_atl_value = lowest_price_in_fetched_klines
                current_atl_date_ms = int(datetime.now().timestamp() * 1000) # Set tanggal ATL baru ke sekarang
                ath_info["atl_value"] = current_atl_value
                ath_info["atl_date_timestamp_ms"] = current_atl_date_ms
                # Source ATL bisa juga diperbarui jika diperlukan
        
        # Simpan pembaruan ATH/ATL kembali ke struktur all_coins_ath_data
        all_coins_ath_data[coin_symbol] = ath_info

        # 4. Ambil harga terkini (dari klines terakhir)
        current_price = float(klines[-1][2]) # Harga penutupan kline terakhir

        # 5. Hitung drawdown saat ini
        drawdown_current = calculate_drawdown_percentage(current_price, current_ath_value)

        # 6. Siapkan data output JSON per koin
        output_data = {
            "coin_symbol": coin_symbol.upper(),
            "current_price": round(current_price, 4),
            "all_time_high": round(current_ath_value, 4),
            "all_time_high_date": datetime.fromtimestamp(current_ath_date_ms / 1000).strftime("%d %B %Y") if current_ath_date_ms else "N/A",
            "all_time_high_days_ago": get_days_ago(current_ath_date_ms),
            "all_time_low": round(current_atl_value, 4), # Perhatikan, ini mungkin hanya ATL yang ditemukan dari data KuCoin yang diambil
            "all_time_low_date": datetime.fromtimestamp(current_atl_date_ms / 1000).strftime("%d %B %Y") if current_atl_date_ms else "N/A",
            "drawdown_percentage_current": round(drawdown_current, 2),
            "data_period_description": f"{len(chart_data_points)} Hari Terakhir + Real-Time", # Sesuaikan dengan limit klines
            "chart_data": chart_data_points
        }
        
        # 7. Unggah data output JSON per koin ke R2
        output_filename = f"data/{coin_symbol.lower()}_drawdown_data.json" # Contoh: data/arb_drawdown_data.json
        upload_json_to_r2(r2_client, output_data, output_filename)

        # (Opsional) Tambahkan jeda untuk mematuhi rate limit KuCoin, meskipun untuk 200 koin biasanya aman
        # import time
        # time.sleep(0.1) # Jeda 0.1 detik antar koin

    # Setelah semua koin diproses, unggah kembali file ATH global yang mungkin sudah diperbarui
    print("\n--- Uploading updated all_coins_ath.json ---")
    upload_json_to_r2(r2_client, all_coins_ath_data, ALL_COINS_ATH_FILE)
    print("All coin data processed and uploaded.")

if __name__ == "__main__":
    main()
