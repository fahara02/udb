(function () {
  "use strict";

  var colors = {
    go: "#00e5ff",
    python: "#ff9f1c",
    typescript: "#3a86ff",
    php: "#9b7cff",
    csharp: "#22c55e",
    java: "#ff6b6b"
  };

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" }[c];
    });
  }
  function ms(n) {
    return typeof n === "number" && isFinite(n) ? n.toFixed(n >= 10 ? 1 : 2) + " ms" : "-";
  }
  function count(n) {
    return typeof n === "number" && isFinite(n) ? String(n) : "-";
  }
  function statusClass(s) {
    return s === "ok" ? "ok" : s === "skipped" ? "skip" : "fail";
  }
  function selectedValues(id) {
    var el = $(id);
    return el ? Array.prototype.map.call(el.selectedOptions || [], function (o) { return o.value; }) : [];
  }
  function uniqueSorted(values) {
    var seen = {};
    values.forEach(function (v) {
      if (v) seen[v] = true;
    });
    return Object.keys(seen).sort(function (a, b) { return a.localeCompare(b); });
  }

  function sdkAt(point, id) {
    return (point.sdks || []).find(function (s) { return s.id === id; });
  }

  function renderCurve(data) {
    var svg = $("bench-curve");
    var history = Array.isArray(data.history) ? data.history : [];
    var sdkIds = (data.sdks || []).map(function (s) { return s.id; });
    var series = sdkIds.map(function (id) {
      return {
        id: id,
        name: ((data.sdks || []).find(function (s) { return s.id === id; }) || {}).name || id,
        points: history.map(function (h, i) {
          var s = sdkAt(h, id);
          var v = s && typeof s.mean_service_latency_ms === "number" ? s.mean_service_latency_ms : null;
          return { x: i, y: v, label: h.release_tag || h.short_commit || "" };
        }).filter(function (p) { return p.y != null; })
      };
    }).filter(function (s) { return s.points.length > 0; });

    if (!series.length) {
      svg.innerHTML = '<text x="36" y="180" fill="#9aa7b8" font-size="18">No trend points yet. Run the benchmark workflow twice to form a curve.</text>';
      return;
    }

    var w = 980, h = 360, l = 70, r = 28, t = 26, b = 58;
    var all = [];
    series.forEach(function (s) { s.points.forEach(function (p) { all.push(p.y); }); });
    var maxY = Math.max.apply(null, all) * 1.15 || 1;
    var minY = 0;
    var maxX = Math.max(1, history.length - 1);
    function x(i) { return l + (i / maxX) * (w - l - r); }
    function y(v) { return t + (1 - ((v - minY) / (maxY - minY))) * (h - t - b); }
    var grid = "";
    for (var gi = 0; gi <= 4; gi++) {
      var gv = (maxY / 4) * gi;
      var yy = y(gv);
      grid += '<line x1="' + l + '" y1="' + yy + '" x2="' + (w - r) + '" y2="' + yy + '" stroke="rgba(154,167,184,.18)"/>';
      grid += '<text x="' + (l - 12) + '" y="' + (yy + 4) + '" fill="#9aa7b8" font-size="12" text-anchor="end">' + ms(gv).replace(" ms", "") + '</text>';
    }
    var paths = series.map(function (s) {
      var d = s.points.map(function (p, i) { return (i ? "L" : "M") + x(p.x).toFixed(1) + " " + y(p.y).toFixed(1); }).join(" ");
      var c = colors[s.id] || "#e8edf5";
      var dots = s.points.map(function (p) {
        return '<circle cx="' + x(p.x).toFixed(1) + '" cy="' + y(p.y).toFixed(1) + '" r="4" fill="' + c + '"><title>' + esc(s.name + " " + p.label + ": " + ms(p.y)) + '</title></circle>';
      }).join("");
      return '<path d="' + d + '" fill="none" stroke="' + c + '" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>' + dots;
    }).join("");
    var labels = "";
    history.forEach(function (p, i) {
      if (history.length > 8 && i % Math.ceil(history.length / 8) !== 0 && i !== history.length - 1) return;
      labels += '<text x="' + x(i).toFixed(1) + '" y="' + (h - 22) + '" fill="#9aa7b8" font-size="11" text-anchor="middle">' + esc(p.release_tag || p.short_commit || String(i + 1)) + '</text>';
    });
    var legend = series.map(function (s, i) {
      var lx = l + i * 138;
      var c = colors[s.id] || "#e8edf5";
      return '<circle cx="' + lx + '" cy="344" r="5" fill="' + c + '"/><text x="' + (lx + 10) + '" y="348" fill="#c9d1e8" font-size="12">' + esc(s.name) + '</text>';
    }).join("");
    svg.innerHTML = '<rect width="980" height="360" rx="14" fill="#11141c"/>' +
      grid +
      '<line x1="' + l + '" y1="' + (h - b) + '" x2="' + (w - r) + '" y2="' + (h - b) + '" stroke="rgba(154,167,184,.32)"/>' +
      '<line x1="' + l + '" y1="' + t + '" x2="' + l + '" y2="' + (h - b) + '" stroke="rgba(154,167,184,.32)"/>' +
      paths + labels + legend;
  }

  function fullRows(data) {
    var rows = [];
    (data.sdks || []).forEach(function (s) {
      (s.full_rpcs || []).forEach(function (r) {
        rows.push({
          sdkId: s.id,
          sdk: s.name,
          service: r.service || "",
          rpc: r.rpc || "",
          api: r.api || ((r.service || "") + "/" + (r.rpc || "")),
          kind: r.kind || "",
          err_code: r.err_code || "",
          p50_ms: r.p50_ms,
          p99_ms: r.p99_ms,
          mean_ms: r.mean_ms,
          min_ms: r.min_ms,
          max_ms: r.max_ms,
          iters: r.iters,
          note: r.note || ""
        });
      });
    });
    return rows;
  }

  function renderWorstTable(data) {
    var slowRows = [];
    (data.sdks || []).forEach(function (s) {
      var seen = {};
      // Failed RPCs first — a non-OK gRPC status is a FAILURE, never a latency sample.
      (s.failed_rpcs || []).forEach(function (r) {
        seen[r.rpc] = true;
        slowRows.push({ sdk: s.name, row: r, failed: true });
      });
      var source = (s.full_rpcs && s.full_rpcs.length) ? s.full_rpcs : (s.slowest || []);
      source.forEach(function (r) {
        var api = r.api || r.rpc;
        if (seen[api] || seen[r.rpc]) return;
        slowRows.push({ sdk: s.name, row: {
          rpc: api || r.rpc,
          kind: r.kind,
          err_code: r.err_code,
          p50_ms: r.p50_ms,
          p99_ms: r.p99_ms,
          mean_ms: r.mean_ms
        }, failed: !!r.err_code });
      });
    });
    // Failures float to the top; then sort the rest by p99 descending.
    slowRows.sort(function (a, b) {
      if (a.failed !== b.failed) return a.failed ? -1 : 1;
      return (b.row.p99_ms || 0) - (a.row.p99_ms || 0);
    });
    $("bench-slowest-rows").innerHTML = slowRows.slice(0, 40).map(function (x) {
      var r = x.row;
      var p99Cell = x.failed
        ? '<td><span class="bench-status fail">FAILED (' + esc(r.err_code || "ERR") + ')</span></td>'
        : '<td class="n">' + ms(r.p99_ms) + '</td>';
      return '<tr><td>' + esc(x.sdk) + '</td><td><code>' + esc(r.rpc) + '</code></td><td>' + esc(r.kind) + '</td>' +
        '<td class="n">' + ms(r.p50_ms) + '</td>' + p99Cell + '<td class="n">' + ms(r.mean_ms) + '</td></tr>';
    }).join("");
  }

  function renderFullExplorer(data) {
    var rows = fullRows(data);
    var sdkFilter = $("bench-sdk-filter");
    var apiFilter = $("bench-api-filter");
    var search = $("bench-search");
    var meta = $("bench-full-meta");
    var body = $("bench-full-rows");

    sdkFilter.innerHTML = (data.sdks || []).filter(function (s) {
      return (s.full_rpcs || []).length > 0;
    }).map(function (s) {
      return '<option value="' + esc(s.id) + '">' + esc(s.name) + ' (' + count((s.full_rpcs || []).length) + ')</option>';
    }).join("");
    apiFilter.innerHTML = uniqueSorted(rows.map(function (r) { return r.api; })).map(function (api) {
      return '<option value="' + esc(api) + '">' + esc(api) + '</option>';
    }).join("");

    function draw() {
      var sdkSelected = selectedValues("bench-sdk-filter");
      var apiSelected = selectedValues("bench-api-filter");
      var q = (search.value || "").trim().toLowerCase();
      var filtered = rows.filter(function (r) {
        if (sdkSelected.length && sdkSelected.indexOf(r.sdkId) < 0) return false;
        if (apiSelected.length && apiSelected.indexOf(r.api) < 0) return false;
        if (!q) return true;
        return [r.sdk, r.api, r.kind, r.err_code, r.note].join(" ").toLowerCase().indexOf(q) >= 0;
      }).sort(function (a, b) {
        return a.api.localeCompare(b.api) || a.sdk.localeCompare(b.sdk);
      });

      meta.textContent = rows.length
        ? "Showing " + filtered.length + " of " + rows.length + " full per-RPC rows."
        : "No full per-RPC table is published yet. SDKs that only publish slowest rows still appear in the worst performer table.";
      body.innerHTML = filtered.map(function (r) {
        var failed = !!r.err_code;
        return '<tr><td>' + esc(r.sdk) + '</td><td><code>' + esc(r.api) + '</code></td><td>' + esc(r.kind) + '</td>' +
          '<td>' + (failed ? '<span class="bench-status fail">' + esc(r.err_code) + '</span>' : '<span class="bench-status ok">OK</span>') + '</td>' +
          '<td class="n">' + ms(r.p50_ms) + '</td><td class="n">' + ms(r.p99_ms) + '</td><td class="n">' + ms(r.mean_ms) + '</td>' +
          '<td class="n">' + ms(r.min_ms) + '</td><td class="n">' + ms(r.max_ms) + '</td><td class="n">' + count(r.iters) + '</td>' +
          '<td>' + esc(r.note) + '</td></tr>';
      }).join("");
    }

    sdkFilter.onchange = draw;
    apiFilter.onchange = draw;
    search.oninput = draw;
    draw();
  }

  function render(data) {
    $("bench-status").style.display = "none";
    var release = data.release || {};
    $("bench-title").textContent = release.tag ? "UDB " + release.tag + " SDK benchmark" : "UDB SDK benchmark";
    $("bench-meta").innerHTML = [
      data.generated_at ? "Generated " + esc(data.generated_at) : "",
      release.asset ? "binary " + esc(release.asset) : "",
      data.git && data.git.short_commit ? "commit " + esc(data.git.short_commit) : ""
    ].filter(Boolean).join(" · ");

    var summary = data.summary || {};
    $("bench-kpis").innerHTML = [
      ["SDKs OK", count(summary.ok)],
      ["Failed", count(summary.failed)],
      ["Skipped", count(summary.skipped)],
      ["Measured RPCs", count(summary.measured_rpc_count)],
      ["Failed RPCs", count(summary.failed_rpc_count)]
    ].map(function (x) {
      return '<div class="bench-kpi"><span>' + esc(x[0]) + '</span><b>' + esc(x[1]) + '</b></div>';
    }).join("");

    $("bench-sdk-grid").innerHTML = (data.sdks || []).map(function (s) {
      var mean = s.summary && s.summary.mean_service_latency_ms;
      return '<article class="bench-sdk-card ' + statusClass(s.status) + '">' +
        '<div class="bench-sdk-top"><b>' + esc(s.name) + '</b><span class="' + statusClass(s.status) + '">' + esc(s.status) + '</span></div>' +
        '<div class="bench-sdk-main">' + esc(ms(mean)) + '</div>' +
        '<p>' + esc(s.note || ((s.summary && s.summary.rpc_count) ? s.summary.rpc_count + " RPCs measured" : "No live benchmark data")) + '</p>' +
        '</article>';
    }).join("");

    $("bench-summary-rows").innerHTML = (data.sdks || []).map(function (s) {
      var sm = s.summary || {};
      return '<tr><td><b>' + esc(s.name) + '</b></td><td><span class="bench-status ' + statusClass(s.status) + '">' + esc(s.status) + '</span></td>' +
        '<td class="n">' + count(sm.rpc_count) + '</td><td class="n">' + count(sm.service_count) + '</td>' +
        '<td class="n">' + ms(sm.mean_service_latency_ms) + '</td><td class="n">' + ms(sm.slowest_service_mean_ms) + '</td></tr>';
    }).join("");

    renderWorstTable(data);
    renderFullExplorer(data);
    renderCurve(data);
  }

  fetch("./bench-results.json", { cache: "no-store" })
    .then(function (r) {
      if (!r.ok) throw new Error("bench-results.json not found");
      return r.json();
    })
    .then(render)
    .catch(function (err) {
      $("bench-status").className = "callout";
      $("bench-status").textContent = "No benchmark JSON is published yet: " + err.message;
      renderCurve({ history: [], sdks: [] });
    });
})();
