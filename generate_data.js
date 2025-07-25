// generate_data.js
const fetch = require('node-fetch');
const fs = require('fs');

// Fungsi pembantu untuk format tanggal & hitung hari
function formatTanggalPendek(dateObj) {
  return dateObj.toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: '2-digit' });
}

function formatTanggalLengkap(dateObj) {
  return dateObj.toLocaleDateString('id-ID', { day: 'numeric', month: 'long', year: 'numeric' });
}

function hitungHariSejak(tanggalLalu) {
  const sekarang = new Date();
  const selisih = Math.floor((sekarang - tanggalLalu) / (1000 * 60 * 60 * 24));
  return selisih;
}

// Fungsi untuk menunda eksekusi (penting untuk rate limit API)
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- Fungsi untuk KuCoin API ---

// Ambil daftar simbol perdagangan (pasangan) dari KuCoin
async function fetchSymbols() {
  const url = "https://api.kucoin.com/api/v1/symbols";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Gagal mengambil daftar simbol dari KuCoin: ${res.statusText}`);
  const data = await res.json();
  if (data.code !== '200000') throw new Error(`KuCoin API Error (symbols): ${data.msg}`);
  return data.data;
}

// Ambil data historis (kumpulan lilin/kandil) dari KuCoin
async function fetchCandles(symbol, type = '1day', days = 365) {
  const endAt = Date.now();
  const startAt = endAt - (days * 24 * 60 * 60 * 1000);
  const url = `https://api.kucoin.com/api/v1/market/candles?symbol=${symbol}&type=${type}&startAt=${Math.floor(startAt / 1000)}&endAt=${Math.floor(endAt / 1000)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Gagal mengambil data lilin untuk ${symbol}: ${res.statusText}`);
  const data = await res.json();
  if (data.code !== '200000') throw new Error(`KuCoin API Error (candles for ${symbol}): ${data.msg}`);

  // Data lilin: [timestamp, open, close, high, low, volume, amount]
  // Kita perlu timestamp dan close price
  return data.data.map(candle => ({
    timestamp: parseInt(candle[0]) * 1000,
    price: parseFloat(candle[2])
  })).sort((a, b) => a.timestamp - b.timestamp);
}

// Ambil harga ticker saat ini
async function fetchTicker(symbol) {
  const url = `https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Gagal mengambil ticker untuk ${symbol}: ${res.statusText}`);
  const data = await res.json();
  if (data.code !== '200000') throw new Error(`KuCoin API Error (ticker for ${symbol}): ${data.msg}`);
  return parseFloat(data.data.price);
}

// --- Logika Utama ---
async function main() {
  console.log('Memulai pengambilan data dari KuCoin...');
  const allChartData = {};
  const coinLimit = 250; 

  try {
    const symbols = await fetchSymbols();
    const usdtPairs = symbols.filter(s => s.quoteCurrency === 'USDT' && s.enableTrading).slice(0, coinLimit);

    if (usdtPairs.length === 0) {
      console.warn("Tidak ada pasangan USDT yang ditemukan atau diizinkan untuk diperdagangkan. Selesai.");
      return;
    }

    let processedCount = 0;
    for (const pair of usdtPairs) {
      const symbol = pair.symbol; 
      const baseCurrency = pair.baseCurrency; 

      console.log(`[${++processedCount}/${usdtPairs.length}] Mengambil data untuk ${symbol}...`);

      try {
        await sleep(200); 

        const historicalCandles = await fetchCandles(symbol, '1day', 365); 
        const currentPrice = await fetchTicker(symbol);

        if (historicalCandles.length === 0) {
          console.warn(`[SKIP] Tidak ada data historis yang cukup untuk ${symbol}.`);
          continue;
        }

        let ath = 0;
        let athDate = null;
        for (const candle of historicalCandles) {
          if (candle.price > ath) {
            ath = candle.price;
            athDate = candle.timestamp;
          }
        }

        if (ath === 0 || athDate === null) {
            console.warn(`[SKIP] ATH tidak valid untuk ${symbol}.`);
            continue;
        }

        const athDateObj = new Date(athDate);
        const daysSinceATH = hitungHariSejak(athDateObj);
        const drawdownNow = ((currentPrice - ath) / ath) * 100;

        const recentHistorical = historicalCandles.slice(-7).map(candle => ({
            date: formatTanggalPendek(new Date(candle.timestamp)),
            drawdown: ((candle.price - ath) / ath * 100).toFixed(2)
        }));

        allChartData[baseCurrency] = {
          symbol: symbol,
          ath: ath,
          athDate: athDate,
          athDateFormatted: formatTanggalLengkap(athDateObj),
          daysSinceATH: daysSinceATH,
          currentPrice: currentPrice,
          currentDrawdown: drawdownNow.toFixed(2),
          historical: recentHistorical
        };

      } catch (coinError) {
        console.error(`[ERROR] Gagal memproses ${symbol}: ${coinError.message}`);
      }
    }

    fs.writeFileSync('chart_data.json', JSON.stringify(allChartData, null, 2));
    console.log(`\nData chart untuk ${Object.keys(allChartData).length} koin berhasil disimpan ke chart_data.json`);
    console.log('Ukuran file yang dihasilkan:', (fs.statSync('chart_data.json').size / (1024 * 1024)).toFixed(2), 'MB');

  } catch (error) {
    console.error('ERROR KRITIS: Gagal mengambil atau memproses data utama:', error);
    process.exit(1);
  }
}

main();
