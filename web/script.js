// COLORS
// '#0d1822'
// '#00405b'
// '#66fff2'
// '#fffe86'
// '#ffae21'
 
const apiBaseUrl = "http://localhost:9000/pool";

document.getElementById("loadBtn").addEventListener("click", () => {
  const address = document.getElementById("addressInput").value.trim();
  if (!address) {
    alert("Please enter a valid address.")
    return;
  }

  document.getElementById("info").innerText = "Loading data...";

  fetch(`${apiBaseUrl}/${address}?_=${Date.now()}`)
    .then(res => {
      if (!res.ok) throw new Error("Pool address not found");
      return res.json();
    })
    .then(metadata => renderChart(metadata))
    .catch(err => {
      document.getElementById("info").innerText = "";
      alert(err.message);
    });
});

function renderChart(metadata) {
  const container = document.getElementById("chart");
  container.innerHTML = "";

  const chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: {
      background: { color: '#0d1822' },
      textColor: '#ffae21',
    },
    grid: {
      vertLines: { color: '#00405b' },
      horzLines: { color: '#00405b' },
    },
    timeScale: {
      timeVisible: true,
      secondsVisible: false,
    },
  });

  const candlestickSeries = chart.addCandlestickSeries({
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderVisible: false,
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
  });

  const data = metadata.data.map(d => ({
    time: Number(d.epoch[0]),
    open: Number(d.open[0]),
    high: Number(d.high[0]),
    low: Number(d.low[0]),
    close: Number(d.close[0])
  }));  
  data.sort((a, b) => a.time - b.time);

  candlestickSeries.setData(data);
  
  chart.subscribeClick(param => {
    if (!param.time || !param.seriesData) return;
    const price = param.seriesData.get(candlestickSeries)?.close;
    const timestamp = new Date(param.time * 1000).toISOString();
    document.getElementById("info").innerText = `Marcado: ${timestamp} - Close: ${price}`;
  });
}
