/* Function B monitor controls */

(function () {
  "use strict";

  var driverSelect = document.getElementById("driver-select");
  var btnStart = document.getElementById("btn-start");
  var btnStop = document.getElementById("btn-stop");
  var statusBadge = document.getElementById("monitor-status");
  var alertBox = document.getElementById("overspeed-alert");
  var panelStatus = document.getElementById("monitor-panel-status");
  var refreshInfo = document.getElementById("next-refresh");

  var intervalId = null;
  var countdownId = null;
  var currentCursor = null;
  var reachedEnd = false;
  var nextRefreshAt = 0;

  var BATCH_SIZE = 50;
  var REFRESH_MS = 30000;  // see project instruction document

  function setPanelStatus(message, isError) {
    panelStatus.textContent = message;
    panelStatus.classList.toggle("hidden", !message);
    panelStatus.classList.toggle("is-error", !!message && !!isError);
  }

  function resetMonitorUi() {
    clearTimeout(intervalId);
    intervalId = null;
    stopCountdown();
    driverSelect.disabled = false;
    btnStart.disabled = !driverSelect.value;
    btnStop.disabled = true;
    statusBadge.textContent = "Idle";
    statusBadge.classList.remove("running");
  }

  function handleMonitoringError(err) {
    resetMonitorUi();
    setPanelStatus(err.message || "Unable to load speed data.", true);
  }

  function clearData() {
    alertBox.classList.add("hidden");
    window.MonitorChart.clear();
    window.MonitorMap.clear();
  }

  async function loadDrivers() {
    try {
      var resp = await fetch("/api/drivers");
      var drivers = await resp.json();
      if (!resp.ok) {
        throw new Error(drivers.error || "Unable to load drivers.");
      }

      drivers.forEach(function (id) {
        var opt = document.createElement("option");
        opt.value = id;
        opt.textContent = id;
        driverSelect.appendChild(opt);
      });
      setPanelStatus("", false);
    } catch (err) {
      driverSelect.disabled = true;
      btnStart.disabled = true;
      setPanelStatus(err.message, true);
    }
  }

  async function fetchBatch() {
    var driverId = driverSelect.value;
    if (!driverId) return;

    if (reachedEnd) {
      reachedEnd = false;
      currentCursor = null;
      clearData();
    }

    var url = "/api/speed/" + encodeURIComponent(driverId) + "?limit=" + BATCH_SIZE;
    if (currentCursor) {
      url += "&cursor=" + encodeURIComponent(currentCursor);
    }
    var resp = await fetch(url);
    var body = await resp.json();
    if (!resp.ok) {
      throw new Error(body.error || "Unable to load speed data.");
    }
    if (!body || !Array.isArray(body.records)) {
      throw new Error("The speed-monitor response had an unexpected format.");
    }

    if (body.count === 0) {
      if (currentCursor) {
        currentCursor = null;
        clearData();
        return fetchBatch();
      }

      clearData();
      setPanelStatus("No speed data was generated for this driver yet.", false);
      return;
    }

    var hasOverspeed = body.records.some(function (record) {
      return record.isOverspeed === 1;
    });
    var isRestartBatch = !currentCursor;

    window.MonitorChart.update(body.records, isRestartBatch);
    window.MonitorMap.update(body.records);

    currentCursor = body.next_cursor || null;
    reachedEnd = !body.has_more;

    if (hasOverspeed) {
      alertBox.classList.remove("hidden");
    } else {
      alertBox.classList.add("hidden");
    }

    setPanelStatus("", false);
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
    countdownId = setInterval(function () {
      updateCountdownDisplay();
    }, 1000);
  }

  function stopCountdown() {
    clearInterval(countdownId);
    countdownId = null;
    nextRefreshAt = 0;
    refreshInfo.textContent = "Next refresh: \u2014";
  }

  function scheduleNextRefresh() {
    clearTimeout(intervalId);
    nextRefreshAt = Date.now() + REFRESH_MS;
    startCountdown();
    intervalId = setTimeout(function () {
      updateCountdownDisplay();
      fetchBatch()
        .then(scheduleNextRefresh)
        .catch(handleMonitoringError);
    }, REFRESH_MS);
  }

  loadDrivers();

  driverSelect.addEventListener("change", function () {
    btnStart.disabled = !driverSelect.value;
  });

  btnStart.addEventListener("click", async function () {
    if (!driverSelect.value) return;

    driverSelect.disabled = true;
    btnStart.disabled = true;
    btnStop.disabled = false;
    statusBadge.textContent = "Monitoring";
    statusBadge.classList.add("running");

    currentCursor = null;
    reachedEnd = false;

    window.MonitorChart.init();
    window.MonitorChart.clear();
    window.MonitorMap.init();
    window.MonitorMap.clear();

    try {
      await fetchBatch();
      scheduleNextRefresh();
    } catch (err) {
      handleMonitoringError(err);
    }
  });

  btnStop.addEventListener("click", function () {
    resetMonitorUi();
  });
})();
