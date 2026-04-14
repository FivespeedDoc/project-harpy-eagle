/* Function B - all drivers speed monitor */

(function () {
  "use strict";

  var grid = document.getElementById("all-monitor-grid");
  var statusEl = document.getElementById("all-monitor-status");
  var refreshInfo = document.getElementById("all-monitor-next-refresh");
  var panel = document.getElementById("panel-all-monitor");
  var tab = document.querySelector('.tab[data-tab="all-monitor"]');

  if (!grid || !statusEl || !refreshInfo) return;

  var charts = {};
  var cursors = {};
  var reachedEnd = {};
  var latestEls = {};
  var hasStarted = false;
  var intervalId = null;
  var countdownId = null;
  var nextRefreshAt = 0;

  var BATCH_SIZE = 50;
  var REFRESH_MS = 30000;
  var MAX_POINTS = 180;
  var SPEED_LIMIT = 120;

  function setStatus(message, isError) {
    statusEl.textContent = message;
    statusEl.classList.toggle("hidden", !message);
    statusEl.classList.toggle("is-error", !!message && !!isError);
  }

  function updateCountdownDisplay() {
    if (!nextRefreshAt) {
      refreshInfo.textContent = "Next refresh: \u2014";
      return;
    }

    var secondsLeft = Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 1000));
    refreshInfo.textContent = "Next refresh: " + secondsLeft + "s";
  }

  function startCountdown() {
    clearInterval(countdownId);
    updateCountdownDisplay();
    countdownId = setInterval(updateCountdownDisplay, 1000);
  }

  function formatChartTime(value) {
    if (!value) return "";
    var parts = String(value).split(" ");
    return parts.length > 1 ? parts[1] : String(value);
  }

  function makeTile(driverId) {
    var tile = document.createElement("article");
    tile.className = "all-monitor-tile";

    var header = document.createElement("div");
    header.className = "all-monitor-tile-header";

    var title = document.createElement("h3");
    title.textContent = driverId;

    var latest = document.createElement("span");
    latest.className = "all-monitor-latest";
    latest.textContent = "-- km/h";
    latestEls[driverId] = latest;

    var chartWrap = document.createElement("div");
    chartWrap.className = "all-monitor-chart-wrap";

    var canvas = document.createElement("canvas");
    canvas.className = "all-monitor-chart";

    header.appendChild(title);
    header.appendChild(latest);
    chartWrap.appendChild(canvas);
    tile.appendChild(header);
    tile.appendChild(chartWrap);
    grid.appendChild(tile);

    return canvas;
  }

  function initChart(driverId, canvas) {
    charts[driverId] = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: [],
        datasets: [
          {
            label: "Speed (km/h)",
            data: [],
            borderColor: "#3b7dd8",
            backgroundColor: "rgba(59,125,216,0.08)",
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.28,
            fill: true,
          },
          {
            label: "Speed Limit (120 km/h)",
            data: [],
            borderColor: "rgba(220,60,60,0.72)",
            borderWidth: 1.3,
            borderDash: [5, 4],
            pointRadius: 0,
            fill: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 300 },
        interaction: { mode: "index", intersect: false },
        scales: {
          x: {
            ticks: {
              autoSkip: true,
              includeBounds: true,
              maxRotation: 0,
              minRotation: 0,
              maxTicksLimit: 5,
              font: { size: 10 },
              callback: function (value) {
                return formatChartTime(this.getLabelForValue(value));
              },
            },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 160,
            ticks: { font: { size: 10 } },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: function (items) {
                return items.length ? items[0].label : "";
              },
            },
          },
        },
      },
    });
  }

  function trimSeries(chart) {
    var trim = chart.data.labels.length - MAX_POINTS;
    if (trim <= 0) return;

    chart.data.labels.splice(0, trim);
    chart.data.datasets[0].data.splice(0, trim);
    chart.data.datasets[1].data.splice(0, trim);
  }

  function clearDriver(driverId) {
    var chart = charts[driverId];
    if (!chart) return;

    chart.data.labels.length = 0;
    chart.data.datasets[0].data.length = 0;
    chart.data.datasets[1].data.length = 0;
    chart.update();

    if (latestEls[driverId]) {
      latestEls[driverId].textContent = "-- km/h";
    }
  }

  function applyRecords(driverId, records) {
    var chart = charts[driverId];
    if (!chart || !records.length) return;

    records.forEach(function (record) {
      chart.data.labels.push(record.time);
      chart.data.datasets[0].data.push(record.speed);
      chart.data.datasets[1].data.push(SPEED_LIMIT);
    });

    trimSeries(chart);
    chart.update();

    var latest = records[records.length - 1];
    if (latestEls[driverId]) {
      latestEls[driverId].textContent = Number(latest.speed || 0).toFixed(0) + " km/h";
    }
  }

  async function fetchDriverBatch(driverId) {
    if (reachedEnd[driverId]) {
      reachedEnd[driverId] = false;
      cursors[driverId] = null;
      clearDriver(driverId);
    }

    var url = "/api/speed/" + encodeURIComponent(driverId)
      + "?limit=" + BATCH_SIZE;
    if (cursors[driverId]) {
      url += "&cursor=" + encodeURIComponent(cursors[driverId]);
    }
    var resp = await fetch(url);
    var body = await resp.json();

    if (!resp.ok) {
      throw new Error(body.error || "Unable to load speed data for " + driverId + ".");
    }
    if (!body || !Array.isArray(body.records)) {
      throw new Error("The speed-monitor response had an unexpected format for " + driverId + ".");
    }

    if (body.count === 0 && cursors[driverId]) {
      cursors[driverId] = null;
      clearDriver(driverId);
      return fetchDriverBatch(driverId);
    }

    cursors[driverId] = body.next_cursor || null;
    reachedEnd[driverId] = !body.has_more;
    applyRecords(driverId, body.records);
  }

  async function fetchAllBatches() {
    var driverIds = Object.keys(charts);
    await Promise.all(driverIds.map(fetchDriverBatch));
    setStatus("", false);
  }

  function scheduleNextRefresh() {
    clearTimeout(intervalId);
    nextRefreshAt = Date.now() + REFRESH_MS;
    startCountdown();
    intervalId = setTimeout(function () {
      fetchAllBatches()
        .then(scheduleNextRefresh)
        .catch(function (err) {
          setStatus(err.message || "Unable to refresh all driver speed data.", true);
        });
    }, REFRESH_MS);
  }

  async function init() {
    if (!window.Chart) {
      setStatus("Chart library failed to load. Check the network connection.", true);
      return;
    }

    try {
      var resp = await fetch("/api/drivers");
      var drivers = await resp.json();

      if (!resp.ok) {
        throw new Error(drivers.error || "Unable to load drivers.");
      }
      if (!Array.isArray(drivers) || !drivers.length) {
        setStatus("No drivers were found in the generated speed data.", false);
        return;
      }

      drivers.slice(0, 10).forEach(function (driverId) {
        cursors[driverId] = null;
        reachedEnd[driverId] = false;
        initChart(driverId, makeTile(driverId));
      });

      await fetchAllBatches();
      scheduleNextRefresh();
    } catch (err) {
      setStatus(err.message || "Unable to initialize the all drivers monitor.", true);
    }
  }

  function startOnce() {
    if (hasStarted) return;
    hasStarted = true;
    init();
  }

  if (tab) {
    tab.addEventListener("click", startOnce);
  }

  if (panel && panel.classList.contains("active")) {
    startOnce();
  }
})();
