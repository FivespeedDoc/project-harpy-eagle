/* Function A — Summary table */

(function () {
  "use strict";

  var tbody = document.querySelector("#summary-table tbody");
  var tableWrap = document.getElementById("summary-table-wrap");
  var statusEl = document.getElementById("summary-status");
  var rankingList = document.getElementById("risk-ranking-list");
  var startInput = document.getElementById("summary-start");
  var endInput = document.getElementById("summary-end");
  var applyPeriodBtn = document.getElementById("summary-apply-period");
  var resetPeriodBtn = document.getElementById("summary-reset-period");
  var loadingOverlay = document.getElementById("summary-loading-overlay");
  var columns = [
    "risk_rank",
    "driverID",
    "carPlateNumber",
    "period",
    "risk_score",
    "overspeed_count",
    "total_overspeed_time",
    "fatigue_count",
    "neutral_slide_count",
    "total_neutral_slide_time",
    "rapid_speedup_count",
    "rapid_slowdown_count",
    "hthrottle_stop_count",
    "oil_leak_count",
  ];

  function setStatus(message, isError) {
    statusEl.textContent = message;
    statusEl.classList.toggle("hidden", !message);
    statusEl.classList.toggle("is-error", !!message && !!isError);
    tableWrap.classList.toggle("hidden", !!message && !!isError);
  }

  function setLoading(isLoading) {
    if (loadingOverlay) {
      loadingOverlay.classList.toggle("hidden", !isLoading);
    }
  }

  function getRiskClass(riskLevel) {
    return String(riskLevel || "")
      .toLowerCase()
      .replace(/\s+/g, "-");
  }

  function formatCellValue(key, value) {
    if (value == null) return "";
    if (key === "risk_score") return Number(value).toFixed(1);
    if (key === "total_overspeed_time" || key === "total_neutral_slide_time") {
      return (Number(value) / 60).toFixed(1);
    }
    return String(value);
  }

  function formatPeriod(record) {
    if (record.period) return record.period;
    if (record.start_time || record.end_time) {
      return (record.start_time || "") + " ~ " + (record.end_time || "");
    }
    return "";
  }

  function buildPeriodCell(td, record) {
    td.className = "period-cell";
    td.innerHTML = "";

    var start = record.start_time || "";
    var end = record.end_time || "";
    if (!start && !end && record.period) {
      var parts = String(record.period).split(" ~ ");
      start = parts[0] || "";
      end = parts.slice(1).join(" ~ ");
    }

    var startLine = document.createElement("span");
    startLine.textContent = start ? start + " ~" : "";
    var endLine = document.createElement("span");
    endLine.textContent = end;

    td.appendChild(startLine);
    td.appendChild(endLine);
  }

  function buildRow(record) {
    var tr = document.createElement("tr");
    tr.className = "risk-row-" + getRiskClass(record.risk_level);

    columns.forEach(function (key) {
      var td = document.createElement("td");
      td.textContent = key === "period" ? formatPeriod(record) : formatCellValue(key, record[key]);
      if (key === "period") {
        buildPeriodCell(td, record);
      }
      if (key === "risk_score") {
        td.className = "risk-score-cell";
        td.innerHTML = "";
        var riskContent = document.createElement("span");
        riskContent.className = "risk-score-content";
        riskContent.appendChild(document.createTextNode(formatCellValue(key, record[key])));
        riskContent.appendChild(buildRiskBadge(record.risk_level));
        td.appendChild(riskContent);
      }
      tr.appendChild(td);
    });

    return tr;
  }

  function buildRiskBadge(riskLevel) {
    var badge = document.createElement("span");
    badge.className = "risk-badge " + getRiskClass(riskLevel);
    badge.textContent = riskLevel || "Unknown";
    return badge;
  }

  function buildRankingCard(record) {
    var card = document.createElement("article");
    card.className = "risk-card " + getRiskClass(record.risk_level);

    var rank = document.createElement("div");
    rank.className = "risk-card-rank";
    rank.textContent = "#" + record.risk_rank;

    var body = document.createElement("div");
    body.className = "risk-card-body";

    var driver = document.createElement("h4");
    driver.textContent = record.driverID;

    var plate = document.createElement("p");
    plate.textContent = record.carPlateNumber;

    body.appendChild(driver);
    body.appendChild(plate);
    body.appendChild(buildRiskBadge(record.risk_level));

    var score = document.createElement("div");
    score.className = "risk-card-score";
    score.textContent = Number(record.risk_score || 0).toFixed(1);

    card.appendChild(rank);
    card.appendChild(body);
    card.appendChild(score);

    return card;
  }

  function renderRanking(data) {
    rankingList.innerHTML = "";

    var rankedDrivers = data
      .slice()
      .sort(function (a, b) {
        return (a.risk_rank || 999) - (b.risk_rank || 999);
      })
      .slice(0, 3);

    var fragment = document.createDocumentFragment();
    rankedDrivers.forEach(function (record) {
      fragment.appendChild(buildRankingCard(record));
    });
    rankingList.appendChild(fragment);
  }

  function hasPeriodFilter() {
    return !!((startInput && startInput.value) || (endInput && endInput.value));
  }

  function buildSummaryUrl() {
    var params = new URLSearchParams();
    if (startInput && startInput.value) params.set("start", startInput.value);
    if (endInput && endInput.value) params.set("end", endInput.value);

    var query = params.toString();
    return "/api/summary" + (query ? "?" + query : "");
  }

  function parseDatasetTime(value) {
    if (!value) return null;

    var parts = String(value).replace("T", " ").split(/[- :]/).map(Number);
    if (parts.length < 5 || parts.some(isNaN)) return null;

    return new Date(
      parts[0],
      parts[1] - 1,
      parts[2],
      parts[3],
      parts[4],
      parts[5] || 0
    );
  }

  function padTimePart(value) {
    return String(value).padStart(2, "0");
  }

  function formatDatetimeLocal(date) {
    return [
      date.getFullYear(),
      padTimePart(date.getMonth() + 1),
      padTimePart(date.getDate()),
    ].join("-") + "T" + [
      padTimePart(date.getHours()),
      padTimePart(date.getMinutes()),
      padTimePart(date.getSeconds()),
    ].join(":");
  }

  function addMinutes(date, minutes) {
    return new Date(date.getTime() + minutes * 60 * 1000);
  }

  function getSummaryBounds(data) {
    var earliest = null;
    var latest = null;

    data.forEach(function (record) {
      var startTime = parseDatasetTime(record.start_time);
      var endTime = parseDatasetTime(record.end_time);

      if (startTime && (!earliest || startTime < earliest)) earliest = startTime;
      if (endTime && (!latest || endTime > latest)) latest = endTime;
    });

    if (!earliest || !latest) return null;

    return {
      start: addMinutes(earliest, -1),
      end: addMinutes(latest, 1),
    };
  }

  function setDefaultPeriod(data) {
    if (!startInput || !endInput || startInput.value || endInput.value) return;

    var bounds = getSummaryBounds(data);
    if (!bounds) return;

    startInput.value = formatDatetimeLocal(bounds.start);
    endInput.value = formatDatetimeLocal(bounds.end);
  }

  function formatDisplayTime(value) {
    return String(value || "").replace("T", " ");
  }

  function summaryPeriodMessage() {
    var start = startInput ? formatDisplayTime(startInput.value) : "";
    var end = endInput ? formatDisplayTime(endInput.value) : "";

    if (start && end) return "Currently displaying summary for " + start + " ~ " + end + ".";
    if (start) return "Currently displaying summary from " + start + ".";
    if (end) return "Currently displaying summary until " + end + ".";

    return "";
  }

  async function loadSummary() {
    try {
      if (startInput && endInput && startInput.value && endInput.value && startInput.value > endInput.value) {
        throw new Error("Start time must be earlier than or equal to end time.");
      }

      var isFilteredRequest = hasPeriodFilter();
      setLoading(true);
      var resp = await fetch(buildSummaryUrl());
      var data = await resp.json();
      if (!resp.ok) {
        throw new Error(data.error || "Unable to load the summary data.");
      }
      if (!Array.isArray(data)) {
        throw new Error("The summary response had an unexpected format.");
      }

      tbody.innerHTML = "";
      rankingList.innerHTML = "";

      if (!data.length) {
        setStatus(
          hasPeriodFilter()
            ? "No summary data was found for the selected period."
            : "No summary data was generated yet.",
          false
        );
        return;
      }

      if (!isFilteredRequest) setDefaultPeriod(data);

      setStatus(isFilteredRequest ? summaryPeriodMessage() : "", false);
      renderRanking(data);
      var fragment = document.createDocumentFragment();
      data.forEach(function (d) {
        fragment.appendChild(buildRow(d));
      });
      tbody.appendChild(fragment);
    } catch (err) {
      tbody.innerHTML = "";
      rankingList.innerHTML = "";
      setStatus(err.message, true);
    } finally {
      setLoading(false);
    }
  }

  if (applyPeriodBtn) {
    applyPeriodBtn.addEventListener("click", loadSummary);
  }

  if (resetPeriodBtn) {
    resetPeriodBtn.addEventListener("click", function () {
      if (startInput) startInput.value = "";
      if (endInput) endInput.value = "";
      loadSummary();
    });
  }

  [startInput, endInput].forEach(function (input) {
    if (!input) return;
    input.addEventListener("keydown", function (event) {
      if (event.key === "Enter") loadSummary();
    });
  });

  loadSummary();
})();
