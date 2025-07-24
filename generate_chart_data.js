import fetch from 'node-fetch';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';

// --- Konfigurasi KuCoin (dari GitHub Secrets) ---
const KUCOIN_API_KEY = process.env.KUCOIN_API_KEY;
// const KUCOIN_API_SECRET = process.env.KUCOIN_API_SECRET; // Aktifkan jika diperlukan
// const KUCOIN_API_PASSPHRASE = process.env.KUCOIN_API_PASSPHRASE; // Aktifkan jika diperlukan

// --- Konfigurasi Cloudflare R2 (dari GitHub Secrets) ---
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_BUCKET_NAME = 'NAMA-BUCKET-R2-ANDA-DI-SINI'; // <<<--- WAJIB GANTI INI DENGAN NAMA BUCKET R2 ANDA

// --- Konfigurasi Umum ---
const BASE_URL = "https://api.kucoin.com";
// <<<--- WAJIB GANTI INI DENGAN DAFTAR LENGKAP 200/300 SIMBOL KOIN ANDA --->>>
const SYMBOLS = ["SOL-USDT", "BTC-USDT", "ETH-USDT", "XRP-USDT", "ADA-USDT", "DOGE-USDT", "BNB-USDT", "DOT-USDT", "LINK-USDT", "LTC-USDT"]; 
const KLINE_INTERVAL = "12hour"; // Interval candle data (misal: "1min", "1hour", "12hour", "1day")

// Fungsi untuk jeda (delay) agar tidak kena rate limit API
const delay = ms => new Promise(res => setTimeout(res, ms));

async function fetchKlineData(symbol, interval) {
    const endpoint = `/api/v1/market/candles`;
    const params = new URLSearchParams({
        symbol: symbol,
        type: interval
    });
    const headers = {
        "KC-API-KEY": KUCOIN_API_KEY,
        // "KC-API-SECRET": KUCOIN_API_SECRET, // Aktifkan jika diperlukan
        // "KC-API-PASSPHRASE": KUCOIN_API_PASSPHRASE // Aktifkan jika diperlukan
    };

    try {
        const response = await fetch(`${BASE_URL}${endpoint}?${params}`, { headers, timeout: 10000 });
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
        }
        const data = await response.json();
        return data.data || []; // Mengembalikan array data kline
    } catch (error) {
        console.error(`Error fetching data for ${symbol}:`, error.message);
        return [];
    }
}

// Fungsi untuk menghitung Drawdown sederhana dari ATH dalam rentang data yang diberikan
function calculateDrawdown(klineData) {
    if (!klineData || klineData.length === 0) return { drawdown: null, ath: null, currentPrice: null };

    let ath = 0;
    let currentPrice = parseFloat(klineData[klineData.length - 1][4]); // Close price dari candle terakhir

    for (const candle of klineData) {
        const high = parseFloat(candle[2]); // High price
        if (high > ath) {
            ath = high;
        }
    }

    if (ath === 0) return { drawdown: null, ath: null, currentPrice: null };

    const drawdown = ((ath - currentPrice) / ath) * 100;
    return {
        drawdown: parseFloat(drawdown.toFixed(2)),
        ath: parseFloat(ath.toFixed(8)),
        currentPrice: parseFloat(currentPrice.toFixed(8))
    };
}

// Fungsi untuk menemukan ATH/ATL dan tanggalnya dalam data yang diberikan
function findATHATL(klineData) {
    if (!klineData || klineData.length === 0) {
        return { ath: null, atl: null, athDate: null, atlDate: null };
    }

    let ath = -Infinity;
    let atl = Infinity;
    let athDate = null;
    let atlDate = null;

    for (const candle of klineData) {
        const high = parseFloat(candle[2]);
        const low = parseFloat(candle[3]);
        const timestamp = parseInt(candle[0]);
        const date = new Date(timestamp * 1000).toISOString();

        if (high > ath) {
            ath = high;
            athDate = date;
        }
        if (low < atl) {
            atl = low;
            atlDate = date;
        }
    }
    return { ath: parseFloat(ath.toFixed(8)), atl: parseFloat(atl.toFixed(8)), athDate, atlDate };
}

// Fungsi untuk mengunggah file ke Cloudflare R2
async function uploadToR2(fileContent, fileName, contentType) {
    const r2EndpointUrl = `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

    const s3Client = new S3Client({
        endpoint: r2EndpointUrl,
        region: 'auto', // Cloudflare R2 adalah global, 'auto' atau 'us-east-1' bisa digunakan
        credentials: {
            accessKeyId: R2_ACCESS_KEY_ID,
            secretAccessKey: R2_SECRET_ACCESS_KEY,
        }
    });

    const upload = new Upload({
        client: s3Client,
        params: {
            Bucket: R2_BUCKET_NAME,
            Key: fileName,
            Body: fileContent,
            ContentType: contentType,
            ACL: 'public-read' // Penting: Agar file bisa diakses publik melalui URL R2
        }
    });

    try {
        await upload.done();
        console.log(`File ${fileName} berhasil diunggah ke R2.`);
    } catch (e) {
        console.error(`Gagal mengunggah ${fileName} ke R2:`, e);
        throw e; // Lemparkan error agar job GitHub Actions gagal jika upload gagal
    }
}

// Fungsi utama yang akan dijalankan oleh GitHub Actions
async function main() {
    for (const symbol of SYMBOLS) {
        console.log(`Memproses ${symbol}...`);
        try {
            const klineData = await fetchKlineData(symbol, KLINE_INTERVAL);

            if (klineData.length > 0) {
                // Hitung drawdown dan dapatkan ATH/ATL
                const { drawdown, ath: dataRangeAth, currentPrice } = calculateDrawdown(klineData);
                const { ath: overallAth, atl: overallAtl, athDate, atlDate } = findATHATL(klineData);

                // Strukturkan data untuk JSON output
                const structuredData = {
                    symbol: symbol,
                    interval: KLINE_INTERVAL,
                    last_updated: new Date().toISOString(),
                    current_price: currentPrice,
                    all_time_high_in_data_range: overallAth,
                    ath_date_in_data_range: athDate,
                    all_time_low_in_data_range: overallAtl,
                    atl_date_in_data_range: atlDate,
                    drawdown_from_ath_in_data_range_percent: drawdown,
                    raw_kline_data: klineData.map(candle => ({ // Konversi array kline menjadi array objek
                        time: new Date(parseInt(candle[0]) * 1000).toISOString(),
                        open: parseFloat(candle[1]),
                        high: parseFloat(candle[2]),
                        low: parseFloat(candle[3]),
                        close: parseFloat(candle[4]),
                        volume: parseFloat(candle[5]),
                        amount: parseFloat(candle[6])
                    }))
                };

                // Unggah file JSON ke R2
                const jsonFileName = `${symbol.replace('-', '_').toLowerCase()}_latest.json`;
                const jsonContent = JSON.stringify(structuredData, null, 2); // Format JSON agar mudah dibaca
                await uploadToR2(jsonContent, jsonFileName, 'application/json');

                // Unggah file HTML ke R2 (Contoh tabel HTML sederhana)
                let htmlContent = `<h1>${symbol} Data - ${new Date().toLocaleString()}</h1>`;
                htmlContent += `<h3>Current Price: ${structuredData.current_price}</h3>`;
                htmlContent += `<h3>Drawdown (from ATH in data range): ${structuredData.drawdown_from_ath_in_data_range_percent}%</h3>`;
                htmlContent += `<h3>ATH in Data Range: ${structuredData.all_time_high_in_data_range} (${structuredData.ath_date_in_data_range ? new Date(structuredData.ath_date_in_data_range).toLocaleString() : 'N/A'})</h3>`;
                htmlContent += `<h3>ATL in Data Range: ${structuredData.all_time_low_in_data_range} (${structuredData.atl_date_in_data_range ? new Date(structuredData.atl_date_in_data_range).toLocaleString() : 'N/A'})</h3>`;

                htmlContent += '<table border="1" style="width:100%; border-collapse: collapse;"><thead><tr style="background-color:#f2f2f2;">';
                htmlContent += '<th>Time</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th><th>Amount</th>';
                htmlContent += '</tr></thead><tbody>';
                structuredData.raw_kline_data.forEach(row => {
                    htmlContent += '<tr>';
                    htmlContent += `<td>${new Date(row.time).toLocaleString()}</td><td>${row.open}</td><td>${row.high}</td><td>${row.low}</td><td>${row.close}</td><td>${row.volume}</td><td>${row.amount}</td>`;
                    htmlContent += '</tr>';
                });
                htmlContent += '</tbody></table>';

                const htmlFileName = `${symbol.replace('-', '_').toLowerCase()}_latest.html`;
                await uploadToR2(htmlContent, htmlFileName, 'text/html');

            } else {
                console.log(`Tidak ada data ditemukan untuk ${symbol}`);
            }

        } catch (error) {
            console.error(`Terjadi kesalahan umum saat memproses ${symbol}:`, error);
        }

        // Sangat penting: Tambahkan jeda untuk menghindari rate limit API KuCoin
        await delay(1000); // Jeda 1 detik (1000 milidetik) antar setiap panggilan API.
    }
}

main();
