/* Function B chart module */

(function () {
  "use strict";

  var chartScroll = document.getElementById("speed-chart-scroll");
  var chartTrack = document.getElementById("speed-chart-track");
  var chartYAxis = document.querySelector(".chart-y-axis");

  var chart = null;
  var chartLabels = [];
  var chartSpeeds = [];
  var chartOverspeed = [];

  var CHART_PX_PER_POINT = 6;
  var CHART_MAX_POINTS = 1000;

  function resetViewport() {
    if (!chartScroll || !chartTrack) return;
    chartTrack.style.width = "100%";
    chartScroll.scrollLeft = 0;
  }

  function updateViewport(resetToRight) {
    if (!chartScroll || !chartTrack) return;

    var wasNearRight = (
      chartScroll.scrollLeft + chartScroll.clientWidth >= chartScroll.scrollWidth - 24
    );
    var baseWidth = Math.max(chartScroll.clientWidth - 24, 320);
    var desiredWidth = Math.max(baseWidth, chartLabels.length * CHART_PX_PER_POINT);

    chartTrack.style.width = desiredWidth + "px";
    if (chart) chart.resize();

    if (wasNearRight || resetToRight) {
      chartScroll.scrollLeft = chartScroll.scrollWidth;
    }
  }

  function updateYAxisLabels() {
    if (!chart || !chart.scales || !chart.scales.y || !chartYAxis) return;

    var yScale = chart.scales.y;
    var existingTicks = chartYAxis.querySelectorAll(".axis-tick");

    existingTicks.forEach(function (tick) {
      tick.remove();
    });

    yScale.ticks.forEach(function (scaleTick) {
      var tick = document.createElement("span");
      var value = scaleTick.value;

      tick.className = "axis-tick";
      tick.textContent = Number.isInteger(value) ? value : Number(value).toFixed(1);
      tick.style.top = yScale.getPixelForValue(value) + "px";
      chartYAxis.appendChild(tick);
    });
  }

  function formatChartTime(value) {
    if (!value) return "";
    var parts = String(value).split(" ");
    return parts.length > 1 ? parts[1] : String(value);
  }

  function init() {
    var canvas = document.getElementById("speed-chart");
    if (!canvas || !window.Chart) return;

    if (chart) chart.destroy();

    chart = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: chartLabels,
        datasets: [
          {
            label: "Speed (km/h)",
            data: chartSpeeds,
            borderColor: "#3b7dd8",
            backgroundColor: "rgba(59,125,216,0.08)",
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
            fill: true,
          },
          {
            label: "Speed Limit (120 km/h)",
            data: [],
            borderColor: "rgba(220,60,60,0.6)",
            borderWidth: 1.5,
            borderDash: [6, 4],
            pointRadius: 0,
            fill: false,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 400 },
        interaction: { mode: "index", intersect: false },
        layout: {
          padding: { left: 0 },
        },
        scales: {
          x: {
            offset: false,
            ticks: {
              autoSkip: true,
              includeBounds: true,
              maxRotation: 0,
              minRotation: 0,
              maxTicksLimit: 10,
              font: { size: 11 },
              callback: function (value) {
                return formatChartTime(this.getLabelForValue(value));
              },
            },
            afterFit: function (scale) {
              scale.paddingLeft = 0;
            },
          },
          y: {
            beginAtZero: true,
            suggestedMax: 160,
            ticks: {
              display: false,
              stepSize: 40,
            },
            border: { display: false },
            title: { display: false },
            afterFit: function (scale) {
              scale.width = 0;
            },
          },
        },
        onResize: function () {
          requestAnimationFrame(updateYAxisLabels);
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

  function clear() {
    chartLabels.length = 0;
    chartSpeeds.length = 0;
    chartOverspeed.length = 0;
    resetViewport();

    if (chart) {
      chart.data.datasets[1].data = [];
      chart.update();
      updateYAxisLabels();
    }
  }

  function update(records, resetToRight) {
    records.forEach(function (record) {
      chartLabels.push(record.time);
      chartSpeeds.push(record.speed);
      chartOverspeed.push(record.isOverspeed);
    });

    if (chartLabels.length > CHART_MAX_POINTS) {
      var trim = chartLabels.length - CHART_MAX_POINTS;
      chartLabels.splice(0, trim);
      chartSpeeds.splice(0, trim);
      chartOverspeed.splice(0, trim);
    }

    updateViewport(resetToRight);

    if (chart) {
      chart.data.datasets[1].data = chartLabels.map(function () { return 120; });
      chart.update();
      updateYAxisLabels();
    }
  }

  window.MonitorChart = {
    clear: clear,
    init: init,
    update: update,
  };
})();
