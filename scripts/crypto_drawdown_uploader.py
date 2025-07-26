import os
import requests
import json
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config

# ==============================================================================
# KONFIGURASI APLIKASI
# ==============================================================================

# --- KuCoin API Endpoints ---
KUCOIN_TICKER_API = "https://api.kucoin.com/api/v1/market/orderbook/level1"
KUCOIN_KLINE_API = "https://api.kucoin.com/api/v1/market/candles"

# --- Daftar Koin yang Akan Diproses ---
# Format: {"Nama Koin Tampilan": "SYMBOL-USDT di KuCoin"}
# Pastikan simbol KuCoin sudah benar (misal: "BTC-USDT", "ARB-USDT").
COIN_CONFIGS = {
    "Arbitrum": "ARB-USDT",
    "Bitcoin": "BTC-USDT",
    "Solana": "SOL-USDT",
    "Litecoin": "LTC-USDT",
    "Pepe": "PEPE-USDT",
    "Ethereum": "ETH-USDT",
    "Cardano": "ADA-USDT"
}

# --- Cloudflare R2 Konfigurasi (Ambil dari GitHub Secrets) ---
# Pastikan variabel lingkungan ini sudah disetel di GitHub Secrets Anda.
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL")  # Contoh: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")

# ==============================================================================
# FUNGSI PEMBANTU TANGGAL
# ==============================================================================

def get_days_difference(date_iso_str, reference_dt_obj=None):
    """
    Menghitung selisih hari dari ISO date string ke tanggal referensi.
    Args:
        date_iso_str (str): Tanggal dalam format ISO string (e.g., "2023-01-12T00:00:00+00:00").
        reference_dt_obj (datetime): Objek datetime referensi (default ke UTC now).
    Returns:
        int: Jumlah hari.
    """
    if not date_iso_str:
        return 'N/A'
    
    try:
        # Menangani format ISO 8601 dengan atau tanpa 'Z' dan memastikan timezone-aware
        dt1 = datetime.fromisoformat(date_iso_str.replace("Z", "+00:00"))
        if dt1.tzinfo is None: # Jika tidak ada info timezone, asumsikan UTC
            dt1 = dt1.replace(tzinfo=timezone.utc)
    except ValueError:
        # Fallback jika fromisoformat gagal, coba parsing lebih longgar dan asumsikan UTC
        try:
            dt1 = datetime.strptime(date_iso_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"Warning: Could not parse date string {date_iso_str}. Returning N/A for days difference.")
            return 'N/A'

    if reference_dt_obj is None:
        dt2 = datetime.now(timezone.utc)
    else:
        dt2 = reference_dt_obj

    # Pastikan kedua datetime objek timezone-aware sebelum melakukan operasi
    # Ini redundant jika dt1 sudah di-handle di atas, tapi untuk keamanan.
    if dt1.tzinfo is None: dt1 = dt1.replace(tzinfo=timezone.utc)
    if dt2.tzinfo is None: dt2 = dt2.replace(tzinfo=timezone.utc)

    diff_days = abs((dt2 - dt1).days) # Mengambil selisih hari secara langsung
    return diff_days

def format_date_for_display(timestamp_ms):
    """
    Mengonversi timestamp milidetik ke objek datetime lokal (WIB) dan format untuk tampilan.
    Contoh: "26 July 2025"
    """
    dt_object_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    wib_timezone = timezone(timedelta(hours=7)) # UTC+7 untuk WIB
    dt_object_wib = dt_object_utc.astimezone(wib_timezone)
    return dt_object_wib.strftime("%d %B %Y")

def format_date_for_xaxis(timestamp_ms):
    """
    Mengonversi timestamp milidetik ke objek datetime lokal (WIB) dan format untuk sumbu X grafik.
    Contoh: "26 Jul"
    """
    dt_object_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    wib_timezone = timezone(timedelta(hours=7)) # UTC+7 untuk WIB
    dt_object_wib = dt_object_utc.astimezone(wib_timezone)
    return dt_object_wib.strftime("%d %b")

# ==============================================================================
# PENGAMBILAN DATA DARI KUCOIN
# ==============================================================================

def fetch_kucoin_ticker_price(symbol):
    """Mengambil harga ticker terkini dari KuCoin."""
    params = {"symbol": symbol}
    try:
        response = requests.get(KUCOIN_TICKER_API, params=params, timeout=10)
        response.raise_for_status() # Akan memicu HTTPError untuk respons 4xx/5xx
        data = response.json()
        if data["code"] == "200000" and data["data"]:
            return float(data["data"]["price"])
        else:
            print(f"Error fetching ticker for {symbol}: {data.get('msg', 'Unknown error')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for ticker {symbol}: {e}")
        return None

def fetch_kucoin_kline_data(symbol, days_for_ath_atl=730, days_for_chart=7):
    """
    Mengambil data kline dari KuCoin untuk perhitungan ATH/ATL dan grafik historis.
    Args:
        symbol (str): Simbol pasangan perdagangan (e.g., "ARB-USDT").
        days_for_ath_atl (int): Jumlah hari riwayat 1-hari kline untuk ATH/ATL (default 2 tahun).
        days_for_chart (int): Jumlah hari riwayat 1-jam kline untuk grafik (default 7 hari).
    Returns:
        tuple: (ath_value, ath_date_iso_str, atl_value, atl_date_iso_str, klines_data_for_chart)
    """
    ath_val, ath_date_iso_str, atl_val, atl_date_iso_str = None, None, None, None
    
    # --- Ambil data kline 1-hari untuk perhitungan ATH/ATL selama sejarah panjang ---
    end_time_long_s = int(datetime.now(timezone.utc).timestamp())
    start_time_long_s = int((datetime.now(timezone.utc) - timedelta(days=days_for_ath_atl)).timestamp())

    long_term_klines = []
    try:
        response_long_term = requests.get(
            KUCOIN_KLINE_API,
            params={"symbol": symbol, "type": "1day", "from": start_time_long_s, "to": end_time_long_s},
            timeout=15
        )
        response_long_term.raise_for_status()
        long_term_data = response_long_term.json()

        if long_term_data["code"] == "200000" and long_term_data["data"]:
            # Data dari KuCoin biasanya dari terlama ke terbaru.
            for kline in long_term_data["data"]:
                timestamp_ms = int(kline[0]) * 1000
                high_price = float(kline[2])
                low_price = float(kline[3])
                long_term_klines.append({
                    "timestamp_ms": timestamp_ms, 
                    "high": high_price, 
                    "low": low_price
                })
            
            if long_term_klines:
                # Inisialisasi ATH/ATL dengan nilai pertama (dari data paling awal)
                ath_val = long_term_klines[0]['high']
                atl_val = long_term_klines[0]['low']
                # Ubah timestamp ke ISO format UTC
                ath_date_iso_str = datetime.fromtimestamp(long_term_klines[0]['timestamp_ms'] / 1000, tz=timezone.utc).isoformat()
                atl_date_iso_str = ath_date_iso_str 

                # Iterasi melalui semua kline untuk menemukan ATH dan ATL yang sebenarnya
                # dan tanggal terkaitnya. Jika ada duplikasi nilai, yang paling awal akan dipertahankan.
                for kline_entry in long_term_klines:
                    kline_timestamp = kline_entry['timestamp_ms']
                    kline_datetime_iso = datetime.fromtimestamp(kline_timestamp / 1000, tz=timezone.utc).isoformat()
                    
                    if kline_entry['high'] > ath_val:
                        ath_val = kline_entry['high']
                        ath_date_iso_str = kline_datetime_iso
                    
                    if kline_entry['low'] < atl_val:
                        atl_val = kline_entry['low']
                        atl_date_iso_str = kline_datetime_iso
            else:
                print(f"No valid prices for ATH/ATL for {symbol} within fetched history.")
                current_utc_iso = datetime.now(timezone.utc).isoformat()
                ath_val, ath_date_iso_str, atl_val, atl_date_iso_str = 0.0, current_utc_iso, 0.0, current_utc_iso

        else:
            print(f"Error fetching long-term kline data for {symbol}: {long_term_data.get('msg', 'Unknown error')}")
            current_utc_iso = datetime.now(timezone.utc).isoformat()
            ath_val, ath_date_iso_str, atl_val, atl_date_iso_str = 0.0, current_utc_iso, 0.0, current_utc_iso

    except requests.exceptions.RequestException as e:
        print(f"Request error for long-term kline {symbol}: {e}")
        current_utc_iso = datetime.now(timezone.utc).isoformat()
        ath_val, ath_date_iso_str, atl_val, atl_date_iso_str = 0.0, current_utc_iso, 0.0, current_utc_iso

    # --- Ambil data kline 1-jam untuk grafik historis (7 hari terakhir) ---
    klines_data_for_chart = []
    end_time_chart_s = int(datetime.now(timezone.utc).timestamp())
    start_time_chart_s = int((datetime.now(timezone.utc) - timedelta(days=days_for_chart)).timestamp())

    try:
        response_chart_data = requests.get(
            KUCOIN_KLINE_API,
            params={"symbol": symbol, "type": "1hour", "from": start_time_chart_s, "to": end_time_chart_s},
            timeout=10
        )
        response_chart_data.raise_for_status()
        chart_data = response_chart_data.json()

        if chart_data["code"] == "200000" and chart_data["data"]:
            for kline in chart_data["data"]:
                timestamp_ms = int(kline[0]) * 1000
                price = float(kline[1]) # Menggunakan harga 'open' untuk kesederhanaan titik chart
                klines_data_for_chart.append({"timestamp": timestamp_ms, "price": price})
        else:
            print(f"Error fetching chart kline data for {symbol}: {chart_data.get('msg', 'Unknown error')}")

    except requests.exceptions.RequestException as e:
        print(f"Request error for chart kline {symbol}: {e}")

    return ath_val, ath_date_iso_str, atl_val, atl_date_iso_str, klines_data_for_chart

# ==============================================================================
# UPLOAD KE CLOUDFLARE R2
# ==============================================================================

def upload_to_r2(file_content, bucket_name, file_key):
    """Mengunggah konten file ke Cloudflare R2."""
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
            CacheControl="public, max-age=7200" # Cache selama 2 jam (7200 detik)
        )
        print(f"Successfully uploaded {file_key} to R2 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to R2: {e}")
        raise

# ==============================================================================
# FUNGSI UTAMA
# ==============================================================================

async def main():
    """Fungsi utama untuk mengambil data dan mengunggahnya ke R2."""
    # Periksa apakah semua variabel lingkungan R2 sudah disetel
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]):
        print("Error: R2 environment variables are not set. Please check GitHub Secrets. Exiting.")
        return

    # Iterasi melalui setiap koin dalam konfigurasi
    for coin_name, kucoin_symbol in COIN_CONFIGS.items():
        print(f"\n--- Processing {coin_name} ({kucoin_symbol}) ---")

        # Ambil data ATH/ATL dan historis
        ath_value, ath_date_raw_str, atl_value, atl_date_raw_str, historical_prices = \
            fetch_kucoin_kline_data(kucoin_symbol, days_for_ath_atl=730)

        if ath_value is None or atl_value is None or not historical_prices:
            print(f"Failed to retrieve complete data for {coin_name}. Skipping.")
            continue

        # Ambil harga saat ini
        current_price = fetch_kucoin_ticker_price(kucoin_symbol)
        if current_price is None:
            print(f"Failed to retrieve current price for {coin_name}. Skipping.")
            continue

        print(f"Current Price: ${current_price:.4f}")
        print(f"ATH: ${ath_value:.4f} on {ath_date_raw_str}")
        print(f"ATL: ${atl_value:.4f} on {atl_date_raw_str}")

        # Hitung hari sejak ATH
        days_since_ath = get_days_difference(ath_date_raw_str) if ath_date_raw_str else 'N/A'
        
        # Tambahkan harga saat ini ke data historis untuk grafik yang lebih real-time
        current_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        historical_prices.append({"timestamp": current_timestamp_ms, "price": current_price})
        historical_prices.sort(key=lambda x: x["timestamp"]) # Pastikan urutan kronologis

        # Format data historis untuk frontend
        formatted_historical_data = []
        for item in historical_prices:
            formatted_historical_data.append({
                "date": format_date_for_xaxis(item["timestamp"]),
                "price": item["price"]
            })

        # Susun data output dalam format JSON
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
            "lastUpdated": datetime.now(timezone.utc).isoformat() # ISO 8601 format with 'Z' for UTC
        }

        json_output = json.dumps(output_data, indent=4)
        print(f"Generated JSON data for {coin_name}.")

        # Tentukan kunci file R2. Ini akan memastikan file disimpan di `aprice/`
        # Contoh: "aprice/arbitrum_drawdown_data.json"
        r2_file_key = f"aprice/{coin_name.lower().replace(' ', '_')}_drawdown_data.json" # Memperbaiki prefiks!
        
        # Unggah data JSON ke R2
        try:
            upload_to_r2(json_output, R2_BUCKET_NAME, r2_file_key)
        except Exception as e:
            print(f"Failed to upload data for {coin_name} to R2: {e}")

# ==============================================================================
# EKSEKUSI SKRIP
# ==============================================================================

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

