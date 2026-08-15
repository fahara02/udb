(function () {
  "use strict";

  var colors = {
    go: "#008aa3",
    python: "#c95700",
    typescript: "#0b66d8",
    php: "#6f42c1",
    csharp: "#116329",
    java: "#b42318"
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
      svg.innerHTML = '<text x="36" y="180" fill="#526176" font-size="18">No trend points yet. Run the benchmark workflow twice to form a curve.</text>';
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
      grid += '<line x1="' + l + '" y1="' + yy + '" x2="' + (w - r) + '" y2="' + yy + '" stroke="#e5edf5"/>';
      grid += '<text x="' + (l - 12) + '" y="' + (yy + 4) + '" fill="#526176" font-size="12" text-anchor="end">' + ms(gv).replace(" ms", "") + '</text>';
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
      labels += '<text x="' + x(i).toFixed(1) + '" y="' + (h - 22) + '" fill="#526176" font-size="11" text-anchor="middle">' + esc(p.release_tag || p.short_commit || String(i + 1)) + '</text>';
    });
    var legend = series.map(function (s, i) {
      var lx = l + i * 138;
      var c = colors[s.id] || "#e8edf5";
      return '<circle cx="' + lx + '" cy="344" r="5" fill="' + c + '"/><text x="' + (lx + 10) + '" y="348" fill="#273247" font-size="12">' + esc(s.name) + '</text>';
    }).join("");
    svg.innerHTML = '<rect width="980" height="360" rx="14" fill="#ffffff"/>' +
      grid +
      '<line x1="' + l + '" y1="' + (h - b) + '" x2="' + (w - r) + '" y2="' + (h - b) + '" stroke="#cbd7e6"/>' +
      '<line x1="' + l + '" y1="' + t + '" x2="' + l + '" y2="' + (h - b) + '" stroke="#cbd7e6"/>' +
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
          api: r.api || r.operation_id || r.api_alias || r.wire_api || ((r.service || "") + "/" + (r.rpc || "")),
          api_alias: r.api_alias || "",
          operation_id: r.operation_id || "",
          wire_api: r.wire_api || ((r.service || "") + "/" + (r.rpc || "")),
          kind: r.kind || "",
          err_code: r.err_code || "",
          result_status: r.result_status || "",
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
        seen[r.api || r.rpc] = true;
        seen[r.wire_api || r.rpc] = true;
        slowRows.push({ sdk: s.name, row: r, failed: true });
      });
      var source = (s.full_rpcs && s.full_rpcs.length) ? s.full_rpcs : (s.slowest || []);
      source.forEach(function (r) {
        var api = r.api || r.operation_id || r.api_alias || r.wire_api || r.rpc;
        if (seen[api] || seen[r.rpc]) return;
        slowRows.push({ sdk: s.name, row: {
          rpc: api || r.rpc,
          wire_api: r.wire_api || r.rpc || "",
          api_alias: r.api_alias || "",
          operation_id: r.operation_id || "",
          kind: r.kind,
          err_code: r.err_code,
          p50_ms: r.p50_ms,
          p99_ms: r.p99_ms,
          mean_ms: r.mean_ms
        }, failed: !!r.err_code && r.err_code !== "CAPABILITY_SKIPPED" });
      });
    });
    // Failures float to the top; then sort the rest by p99 descending.
    slowRows.sort(function (a, b) {
      if (a.failed !== b.failed) return a.failed ? -1 : 1;
      return (b.row.p99_ms || 0) - (a.row.p99_ms || 0);
    });
    if (!slowRows.length) {
      $("bench-slowest-rows").innerHTML = '<tr><td colspan="6" class="empty-cell">No per-RPC latency rows are published yet.</td></tr>';
      return;
    }
    $("bench-slowest-rows").innerHTML = slowRows.slice(0, 40).map(function (x) {
      var r = x.row;
      var p99Cell = x.failed
        ? '<td><span class="bench-status fail">FAILED (' + esc(r.err_code || "ERR") + ')</span></td>'
        : '<td class="n">' + ms(r.p99_ms) + '</td>';
      var detail = r.wire_api && r.wire_api !== r.rpc ? '<br><small>' + esc(r.wire_api) + '</small>' : '';
      return '<tr><td>' + esc(x.sdk) + '</td><td><code>' + esc(r.rpc) + '</code>' + detail + '</td><td>' + esc(r.kind) + '</td>' +
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
      if (!rows.length) {
        meta.textContent = "No full per-RPC table is published yet.";
        body.innerHTML = '<tr><td colspan="12" class="empty-cell">No full per-RPC rows are published yet.</td></tr>';
        return;
      }
      var sdkSelected = selectedValues("bench-sdk-filter");
      var apiSelected = selectedValues("bench-api-filter");
      var q = (search.value || "").trim().toLowerCase();
      var filtered = rows.filter(function (r) {
        if (sdkSelected.length && sdkSelected.indexOf(r.sdkId) < 0) return false;
        if (apiSelected.length && apiSelected.indexOf(r.api) < 0) return false;
        if (!q) return true;
        return [r.sdk, r.api, r.api_alias, r.operation_id, r.wire_api, r.kind, r.err_code, r.note].join(" ").toLowerCase().indexOf(q) >= 0;
      }).sort(function (a, b) {
        return a.api.localeCompare(b.api) || a.sdk.localeCompare(b.sdk);
      });

      meta.textContent = "Showing " + filtered.length + " of " + rows.length + " full per-RPC rows.";
      if (!filtered.length) {
        body.innerHTML = '<tr><td colspan="12" class="empty-cell">No rows match the current filters.</td></tr>';
        return;
      }
      body.innerHTML = filtered.map(function (r) {
        var capabilitySkipped = r.err_code === "CAPABILITY_SKIPPED" || r.result_status === "capability_skipped";
        var failed = !!r.err_code && !capabilitySkipped;
        var resultBadge = failed
          ? '<span class="bench-status fail">' + esc(r.err_code) + '</span>'
          : capabilitySkipped
          ? '<span class="bench-status skip">CAPABILITY SKIPPED</span>'
          : '<span class="bench-status ok">OK</span>';
        return '<tr><td>' + esc(r.sdk) + '</td><td><code>' + esc(r.api) + '</code></td><td><code>' + esc(r.wire_api) + '</code></td><td>' + esc(r.kind) + '</td>' +
          '<td>' + resultBadge + '</td>' +
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
    var hasSdkRows = Array.isArray(data.sdks) && data.sdks.length > 0;
    var hasMeasurements = !!(data.summary && data.summary.measured_rpc_count);
    var summary = data.summary || {};
    var canonicalEvidence = data.schema_version === 2 &&
      data.evidence_status === "canonical_complete" &&
      data.benchmark_contract &&
      typeof data.benchmark_contract.canonical_manifest_sha256 === "string" &&
      data.benchmark_contract.canonical_manifest_sha256.length === 64 &&
      Number.isInteger(summary.attempted_rpc_count) &&
      summary.attempted_rpc_count === data.benchmark_contract.expected_attempted_rpc_count &&
      Number.isInteger(summary.measured_rpc_count) &&
      Number.isInteger(summary.capability_skipped_rpc_count) &&
      Number.isInteger(summary.failed_rpc_count) &&
      summary.measured_rpc_count + summary.capability_skipped_rpc_count + summary.failed_rpc_count === summary.attempted_rpc_count;
    if (hasSdkRows || hasMeasurements) {
      var failedRpcCount = data.summary && typeof data.summary.failed_rpc_count === "number" ? data.summary.failed_rpc_count : 0;
      $("bench-status").style.display = "block";
      $("bench-status").className = failedRpcCount || !canonicalEvidence ? "callout" : "callout cool";
      $("bench-status").textContent = !canonicalEvidence
        ? "Legacy/incomplete benchmark evidence. This historical artifact predates the canonical full-surface proof gate and must not be treated as a green release result."
        : failedRpcCount
        ? "Benchmark JSON loaded, but " + failedRpcCount + " RPC measurement" + (failedRpcCount === 1 ? "" : "s") + " returned non-OK status. See 'Failures and slowest RPCs' below."
        : "Canonical benchmark proof loaded. Every attempted RPC is accounted for as successful, capability-skipped, or failed.";
    } else {
      $("bench-status").className = "callout cool";
      $("bench-status").style.display = "block";
      $("bench-status").textContent = "Benchmark data is pending. CI will replace this placeholder with release, commit, SDK, and per-RPC measurements.";
    }
    var release = data.release || {};
    $("bench-title").textContent = release.tag ? "UDB " + release.tag + " SDK benchmark" : (hasSdkRows || hasMeasurements ? "UDB SDK benchmark" : "Benchmark data pending");
    var metaText = [
      data.generated_at ? "Generated " + esc(data.generated_at) : "",
      release.asset ? "binary " + esc(release.asset) : "",
      data.git && data.git.short_commit ? "commit " + esc(data.git.short_commit) : ""
    ].filter(Boolean).join(" · ");
    $("bench-meta").innerHTML = metaText || "No release run has been published into bench-results.json yet.";

    $("bench-kpis").innerHTML = [
      ["Evidence", canonicalEvidence ? "complete" : "legacy / incomplete"],
      ["SDKs OK", count(summary.ok)],
      ["Failed", count(summary.failed)],
      ["Skipped", count(summary.skipped)],
      ["Attempted RPCs", count(summary.attempted_rpc_count)],
      ["Successful RPCs", count(summary.measured_rpc_count)],
      ["Capability skipped", count(summary.capability_skipped_rpc_count)],
      ["Failed RPCs", count(summary.failed_rpc_count)]
    ].map(function (x) {
      return '<div class="bench-kpi"><span>' + esc(x[0]) + '</span><b>' + esc(x[1]) + '</b></div>';
    }).join("");

    $("bench-sdk-grid").innerHTML = (data.sdks || []).length ? (data.sdks || []).map(function (s) {
      var mean = s.summary && s.summary.mean_service_latency_ms;
      var displayStatus = canonicalEvidence ? s.status : "legacy / incomplete";
      return '<article class="bench-sdk-card ' + statusClass(displayStatus) + '">' +
        '<div class="bench-sdk-top"><b>' + esc(s.name) + '</b><span class="' + statusClass(displayStatus) + '">' + esc(displayStatus) + '</span></div>' +
        '<div class="bench-sdk-main">' + esc(ms(mean)) + '</div>' +
        '<p>' + esc(s.note || ((s.summary && s.summary.attempted_rpc_count) ? s.summary.attempted_rpc_count + " RPC attempts" : "No live benchmark data")) + '</p>' +
        '</article>';
    }).join("") : '<div class="bench-sdk-empty">No SDK benchmark runs are published yet.</div>';

    $("bench-summary-rows").innerHTML = (data.sdks || []).length ? (data.sdks || []).map(function (s) {
      var sm = s.summary || {};
      var displayStatus = canonicalEvidence ? s.status : "legacy / incomplete";
      return '<tr><td><b>' + esc(s.name) + '</b></td><td><span class="bench-status ' + statusClass(displayStatus) + '">' + esc(displayStatus) + '</span></td>' +
        '<td class="n">' + count(sm.attempted_rpc_count) + '</td><td class="n">' + count(sm.service_count) + '</td>' +
        '<td class="n">' + ms(sm.mean_service_latency_ms) + '</td><td class="n">' + ms(sm.slowest_service_mean_ms) + '</td></tr>';
    }).join("") : '<tr><td colspan="6" class="empty-cell">No SDK summary has been published yet.</td></tr>';

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
      $("bench-title").textContent = "Benchmark data pending";
      $("bench-meta").textContent = "The dashboard shell is ready; CI will populate live SDK results at bench-results.json.";
      $("bench-kpis").innerHTML = [
        ["SDKs OK", "-"],
        ["Failed", "-"],
        ["Skipped", "-"],
        ["Measured RPCs", "-"],
        ["Failed RPCs", "-"]
      ].map(function (x) {
        return '<div class="bench-kpi"><span>' + esc(x[0]) + '</span><b>' + esc(x[1]) + '</b></div>';
      }).join("");
      $("bench-sdk-grid").innerHTML = "";
      $("bench-summary-rows").innerHTML = '<tr><td colspan="6" class="empty-cell">No SDK summary has been published yet.</td></tr>';
      $("bench-slowest-rows").innerHTML = '<tr><td colspan="6" class="empty-cell">No per-RPC latency rows are published yet.</td></tr>';
      $("bench-full-meta").textContent = "No full per-RPC table is published yet.";
      $("bench-full-rows").innerHTML = '<tr><td colspan="12" class="empty-cell">No full per-RPC rows are published yet.</td></tr>';
      renderCurve({ history: [], sdks: [] });
    });
})();
