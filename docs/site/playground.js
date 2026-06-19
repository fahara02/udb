/* UDB proto playground — runs UDB's REAL parser in the browser.
 *
 * udb.wasm is `crates/udb-wasm` (a thin cdylib bridge) over `crates/udb-portable`,
 * which #[path]-includes the exact same parser/AST/checksum + manifest & DDL
 * generation + IR→SQL query-compiler source the UDB server compiles. So the catalog
 * schema, deterministic checksum, generated DDL, and compiled SQL shown here are all
 * produced by UDB itself — not a re-implementation. (Only the runtime — the gRPC
 * broker, DB drivers, and async execution — stays server-only.) */
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var enc = new TextEncoder(), dec = new TextDecoder();
  var ex = null; // wasm exports
  var runTimer = null;

  var EXAMPLES = {
    invoice:
      'syntax = "proto3";\n' +
      "package myapp.v1;\n\n" +
      "// A billing table with row-level security and per-column data classes.\n" +
      "message Invoice {\n" +
      '  option (myapp.v1.table) = {\n' +
      '    table_name: "invoices"  schema_name: "billing"\n' +
      "    is_table: true  enable_rls: true\n" +
      "  };\n\n" +
      '  string invoice_id  = 1 [(myapp.v1.column) = { column_name: "invoice_id" sql_type: "UUID" primary_key: true not_null: true }];\n' +
      '  string tenant_id   = 2 [(myapp.v1.column) = { column_name: "tenant_id"  tenant_column: true not_null: true }];\n' +
      '  string customer_id = 3 [(myapp.v1.column) = { column_name: "customer_id" sql_type: "UUID" not_null: true }];\n' +
      '  string email       = 4 [(myapp.v1.column) = { column_name: "email" sql_type: "TEXT" pii_kind: PII_KIND_EMAIL encrypt: true }];\n' +
      '  int64  total_cents = 5 [(myapp.v1.column) = { column_name: "total_cents" sql_type: "BIGINT" not_null: true }];\n' +
      '  string status      = 6 [(myapp.v1.column) = { column_name: "status" sql_type: "TEXT" }];\n' +
      "}",
    user:
      'syntax = "proto3";\n' +
      "package myapp.v1;\n\n" +
      "message User {\n" +
      '  option (myapp.v1.table) = { table_name: "users" schema_name: "app" is_table: true enable_rls: true };\n\n' +
      '  string id         = 1 [(myapp.v1.column) = { column_name: "id" sql_type: "UUID" primary_key: true not_null: true }];\n' +
      '  string tenant_id  = 2 [(myapp.v1.column) = { column_name: "tenant_id" tenant_column: true not_null: true }];\n' +
      '  string email      = 3 [(myapp.v1.column) = { column_name: "email" sql_type: "TEXT" pii_kind: PII_KIND_EMAIL encrypt: true not_null: true }];\n' +
      '  string full_name  = 4 [(myapp.v1.column) = { column_name: "full_name" sql_type: "TEXT" pii_kind: PII_KIND_NAME mask_in_logs: true }];\n' +
      '  string created_at = 5 [(myapp.v1.column) = { column_name: "created_at" sql_type: "TIMESTAMPTZ" is_created_at: true }];\n' +
      "}",
    broken:
      'syntax = "proto3";\n' +
      "package myapp.v1;\n\n" +
      "// Deliberately malformed — watch UDB's parser report a real diagnostic.\n" +
      "message Broken {\n" +
      '  option (myapp.v1.table) = { table_name: "broken" is_table: true };\n' +
      '  string id = 1 [(myapp.v1.column) = { column_name: "id" primary_key: true \n' +
      "}"
  };

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }

  // Call UDB's real parser (wasm) on `proto`, returning the parsed result object.
  function udbParse(proto, ns) {
    function writeStr(s) {
      var b = enc.encode(s);
      var p = ex.udb_alloc(b.length);
      new Uint8Array(ex.memory.buffer, p, b.length).set(b);
      return [p, b.length];
    }
    var src = writeStr(proto), nsp = writeStr(ns || "");
    var packed = ex.udb_parse(src[0], src[1], nsp[0], nsp[1]); // BigInt: (ptr<<32)|len
    var ptr = Number(packed >> 32n), len = Number(packed & 0xffffffffn);
    // Re-fetch the buffer: udb_parse may have grown (and detached) memory.
    var json = dec.decode(new Uint8Array(ex.memory.buffer, ptr, len));
    ex.udb_free(src[0], src[1]); ex.udb_free(nsp[0], nsp[1]); ex.udb_free(ptr, len);
    return JSON.parse(json);
  }

  function badge(label, cls) { return '<span class="cbadge ' + (cls || "") + '">' + label + "</span>"; }

  function render(res) {
    var body = $("out");
    if (!res.ok) {
      body.innerHTML = '<div class="rmeta"><span class="err">✗ parser error</span> ' + esc(res.error || "unknown") + "</div>";
      return;
    }
    var diags = res.diagnostics || [];
    var html = '<div class="rmeta">' +
      (diags.length ? '<span class="warn">⚠ ' + diags.length + " diagnostic" + (diags.length === 1 ? "" : "s") + "</span>" : '<span class="ok">✓ parsed</span>') +
      " · <b>" + res.table_count + "</b> table" + (res.table_count === 1 ? "" : "s") +
      ' · annotation v' + esc(res.annotation_version) + "</div>";

    if (res.checksum) {
      html += '<div class="checksum">manifest checksum (sha-256) <code>' + esc(res.checksum) + "</code></div>";
    }

    (res.schemas || []).forEach(function (s) {
      var sec = [];
      if (s.enable_rls) sec.push(badge("RLS", "rls"));
      if (s.force_rls) sec.push(badge("FORCE RLS", "rls"));
      if (s.audit_fields) sec.push(badge("audit", ""));
      if (s.soft_delete) sec.push(badge("soft-delete", ""));
      html += '<div class="schema">' +
        '<div class="shd"><b>' + esc(s.message_name) + "</b> → <code>" +
        esc((s.schema_name || "public")) + "." + esc(s.table_name) + "</code>" +
        '<span class="sbadges">' + sec.join("") + "</span></div>";
      html += '<div class="tablewrap" style="border:none;border-radius:0"><table><thead><tr>' +
        "<th>Proto field</th><th>DB column</th><th>SQL type</th><th>#</th><th>Constraints &amp; data class</th></tr></thead><tbody>";
      (s.columns || []).forEach(function (c) {
        var b = [];
        var fieldName = c.field_name || c.column_name || "";
        var columnName = c.column_name || c.field_name || "";
        if (c.is_primary) b.push(badge("PK", "pk"));
        if (c.not_null) b.push(badge("NOT NULL", ""));
        if (c.unique) b.push(badge("UNIQUE", ""));
        if (c.is_tenant || (c.security && c.security.is_tenant)) b.push(badge("tenant", "tenant"));
        if (c.is_pii || (c.security && c.security.is_pii)) b.push(badge("PII", "pii"));
        if (c.encrypted || c.is_encrypted || (c.security && c.security.is_encrypted)) b.push(badge("encrypted", "enc"));
        if (c.mask_in_logs || (c.security && c.security.mask_in_logs)) b.push(badge("mask-in-logs", "enc"));
        html += "<tr><td><code>" + esc(fieldName) + "</code></td>" +
          "<td><code>" + esc(columnName) + "</code></td>" +
          "<td>" + esc(c.sql_type || c.proto_type || "—") + "</td>" +
          '<td class="n">' + esc(c.field_number) + "</td>" +
          "<td>" + (b.length ? b.join(" ") : '<span style="color:var(--muted-2)">—</span>') + "</td></tr>";
      });
      html += "</tbody></table></div></div>";
    });

    // ── New portable subsystems, all running in your browser ──────────────────
    // Live IR→SQL compile of a sample SELECT (the real ir::compile pipeline).
    if (res.compiled_query && res.compiled_query.sql) {
      var cq = res.compiled_query;
      html += '<div class="genblock"><div class="ghd">IR → SQL compiler' +
        '<span class="gsub">' + esc(cq.operation) + ' on <code>' + esc(cq.message_type) +
        "</code> → <b>" + esc(cq.backend) + "</b> · compiled client-side</span></div>" +
        '<pre class="gsql">' + esc(cq.sql) + "</pre></div>";
    }
    // Generated bootstrap DDL (the real generation::sql renderer).
    if (res.ddl) {
      html += '<div class="genblock"><div class="ghd">Generated bootstrap DDL' +
        '<span class="gsub">generation::sql — the exact server DDL renderer, in WASM</span></div>' +
        '<pre class="gsql">' + esc(res.ddl) + "</pre></div>";
    }

    if (diags.length) {
      html += '<div class="diags"><b>Parser diagnostics</b>';
      diags.forEach(function (d) {
        html += '<div class="diag"><code>' + esc(d.line) + ":" + esc(d.column) + "</code> [" +
          esc(d.code) + "] " + esc(d.message) + "</div>";
      });
      html += "</div>";
    }
    body.innerHTML = html;
  }

  function nsFromProto(proto) {
    var m = /(?:^|\n)\s*package\s+([A-Za-z0-9_.]+)\s*;/.exec(proto);
    return m ? m[1] : "";
  }

  function run() {
    var proto = $("proto").value;
    try {
      render(udbParse(proto, nsFromProto(proto)));
    } catch (e) {
      $("out").innerHTML = '<div class="rmeta"><span class="err">✗ wasm error</span> ' + esc(e.message) + "</div>";
    }
  }

  function scheduleRun() {
    if (runTimer) clearTimeout(runTimer);
    runTimer = setTimeout(run, 300);
  }

  function fail(msg) {
    $("boot").innerHTML =
      '<p style="color:#b42318;font-weight:700;margin:0 0 8px">Could not load the UDB WASM module.</p>' +
      '<p style="margin:0;color:var(--muted)">' + esc(msg) + "</p>";
  }

  function wasmImports() {
    // The current Pages workflow rebuilds `crates/udb-wasm` as a plain C-ABI
    // cdylib. Older cached artifacts were wasm-bindgen builds and imported these
    // four helpers. Providing minimal shims keeps the playground bootable during
    // cache skew while preserving the exported C ABI used below.
    function decodeWasmString(ptr, len) {
      if (!ex || !ex.memory) return "";
      try { return dec.decode(new Uint8Array(ex.memory.buffer, ptr, len)); }
      catch (_) { return ""; }
    }
    return {
      __wbindgen_placeholder__: {
        __wbindgen_describe: function () {},
        __wbg___wbindgen_throw_1506f2235d1bdba0: function (ptr, len) {
          throw new Error(decodeWasmString(ptr, len) || "UDB WASM threw an exception");
        }
      },
      __wbindgen_externref_xform__: {
        __wbindgen_externref_table_set_null: function () {},
        __wbindgen_externref_table_grow: function () { return 0; }
      }
    };
  }

  function boot() {
    if (typeof WebAssembly === "undefined") { fail("This browser has no WebAssembly support."); return; }
    var imports = wasmImports();
    var load = WebAssembly.instantiateStreaming
      ? WebAssembly.instantiateStreaming(fetch("./udb.wasm"), imports).catch(function () {
          return fetch("./udb.wasm").then(function (r) { return r.arrayBuffer(); })
            .then(function (b) { return WebAssembly.instantiate(b, imports); });
        })
      : fetch("./udb.wasm").then(function (r) { return r.arrayBuffer(); })
          .then(function (b) { return WebAssembly.instantiate(b, imports); });

    load.then(function (res) {
      ex = res.instance.exports;
      $("proto").value = EXAMPLES.invoice;
      $("boot").style.display = "none";
      $("out").style.display = "block";
      run();
      $("run").addEventListener("click", run);
      $("proto").addEventListener("keydown", function (e) {
        if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); run(); }
      });
      $("proto").addEventListener("input", scheduleRun);
      $("examples").addEventListener("click", function (e) {
        var el = e.target.closest("[data-ex]");
        if (el) { $("proto").value = EXAMPLES[el.getAttribute("data-ex")] || ""; run(); }
      });
    }).catch(function (err) { fail(err && err.message ? err.message : String(err)); });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
