const { chromium } = require("playwright");
const fs = require("fs");

const base = "http://localhost:8000";
const pages = ["/", "/sdks.html", "/benchmarks.html", "/playground.html", "/api.html"];
const viewports = [
  { name: "narrow320", width: 320, height: 700 },
  { name: "mobile390", width: 390, height: 844 },
  { name: "tablet768", width: 768, height: 1024 },
  { name: "laptop1366", width: 1366, height: 768 },
  { name: "desktop1920", width: 1920, height: 1080 },
];

function fail(failures, label, message) {
  failures.push(`${label}: ${message}`);
}

async function main() {
  fs.mkdirSync("output/playwright", { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const failures = [];
  const results = [];

  for (const vp of viewports) {
    const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height } });
    for (const path of pages) {
      const label = `${vp.name} ${path}`;
      const page = await context.newPage();
      const errors = [];
      const failedResponses = [];
      page.on("console", (msg) => {
        if (msg.type() === "error") errors.push(msg.text());
      });
      page.on("pageerror", (err) => errors.push(err.message));
      page.on("response", (response) => {
        const url = response.url();
        if (url.startsWith(base) && response.status() >= 400) {
          failedResponses.push(`${response.status()} ${url}`);
        }
      });

      const response = await page.goto(base + path, { waitUntil: "networkidle", timeout: 45000 });
      if (!response || response.status() >= 400) {
        fail(failures, label, `route status ${response ? response.status() : "none"}`);
      }
      await page.waitForTimeout(250);

      const metrics = await page.evaluate(() => {
        const bench = document.querySelector(".bench-controls");
        const heroProof = document.querySelector(".hero-proof");
        const firstSection = document.querySelector("#why, #features, .bench-page, .api-reference");
        return {
          bodyOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
          bodyBg: getComputedStyle(document.body).backgroundColor,
          visibleSections: Array.from(document.querySelectorAll(".reveal")).filter((el) => getComputedStyle(el).opacity !== "0").length,
          revealTotal: document.querySelectorAll(".reveal").length,
          benchColumns: bench ? getComputedStyle(bench).gridTemplateColumns : "",
          benchScrollDelta: bench ? bench.scrollWidth - bench.clientWidth : 0,
          heroProofColumns: heroProof ? getComputedStyle(heroProof).gridTemplateColumns : "",
          firstSectionText: firstSection ? firstSection.textContent.trim().slice(0, 80) : "",
          h1: document.querySelector("h1")?.textContent.trim() || "",
        };
      });

      if (errors.length) fail(failures, label, `console errors: ${errors.join(" | ")}`);
      if (failedResponses.length) fail(failures, label, `failed resources: ${failedResponses.join(" | ")}`);
      if (metrics.bodyOverflow > 2) fail(failures, label, `body overflow ${metrics.bodyOverflow}px`);
      if (metrics.bodyBg !== "rgb(248, 250, 252)") fail(failures, label, `body bg ${metrics.bodyBg}`);
      if (metrics.revealTotal > 0 && metrics.visibleSections !== metrics.revealTotal) {
        fail(failures, label, `hidden reveal elements ${metrics.visibleSections}/${metrics.revealTotal}`);
      }

      if (vp.width < 900) {
        const toggle = await page.$(".nav-toggle");
        if (toggle) {
          await toggle.click();
          await page.waitForTimeout(350);
          const nav = await page.evaluate(() => {
            const el = document.querySelector(".nav-links");
            return {
              open: el.classList.contains("open"),
              overflowY: getComputedStyle(el).overflowY,
              scrollHeight: el.scrollHeight,
              clientHeight: el.clientHeight,
              lastLink: Array.from(el.querySelectorAll("a")).at(-1)?.textContent.trim() || "",
            };
          });
          if (!nav.open) fail(failures, label, "mobile nav did not open");
          if (!/auto|scroll/.test(nav.overflowY)) fail(failures, label, "mobile nav not scrollable");
          if (nav.clientHeight <= 0) fail(failures, label, "mobile nav clientHeight is zero");
          if (nav.lastLink !== "GitHub ↗") fail(failures, label, `last nav link missing (${nav.lastLink})`);
        }
      }

      if (path === "/benchmarks.html") {
        if (vp.width < 900) {
          const columns = metrics.benchColumns.trim().split(/\s+/).filter(Boolean).length;
          if (columns !== 1) fail(failures, label, `benchmark controls not single-column: ${metrics.benchColumns}`);
          if (metrics.benchScrollDelta > 2) fail(failures, label, `benchmark controls overflow ${metrics.benchScrollDelta}px`);
        }
        const searchOk = await page.evaluate(() => {
          const input = document.querySelector("#bench-search");
          if (!input) return false;
          input.value = "storage";
          input.dispatchEvent(new Event("input", { bubbles: true }));
          return document.querySelector("#bench-full-meta")?.textContent.length > 0;
        });
        if (!searchOk) fail(failures, label, "benchmark search input did not update metadata");
      }

      if (path === "/playground.html") {
        await page.waitForTimeout(500);
        const playground = await page.evaluate(() => {
          const out = document.querySelector("#out");
          const text = out ? out.textContent : "";
          return {
            outDisplay: out ? getComputedStyle(out).display : "",
            hasWasmError: /Could not load the UDB WASM module|Import #0|__wbindgen_placeholder__|instantiate\(\)/i.test(document.body.textContent),
            parsed: /parsed|catalog|CREATE TABLE|SELECT/i.test(text),
          };
        });
        if (playground.hasWasmError) fail(failures, label, "playground shows WASM load error");
        if (playground.outDisplay === "none") fail(failures, label, "playground output stayed hidden");
        if (!playground.parsed) fail(failures, label, "playground did not show parsed output");
      }

      if (path === "/api.html") {
        const swaggerOk = await page.evaluate(async () => {
          const r = await fetch("./api/udb-broker.swagger.json", { cache: "no-store" });
          if (!r.ok) return false;
          const j = await r.json();
          return !!(j.openapi || j.swagger || j.paths);
        });
        if (!swaggerOk) fail(failures, label, "swagger json did not load/parse");
      }

      results.push({ label, metrics });
      await page.close();
    }
    await context.close();
  }

  const contrast = await checkContrast(browser, failures);
  await browser.close();
  console.log(JSON.stringify({ results, contrast }, null, 2));
  if (failures.length) {
    console.error("\nFAILURES\n" + failures.join("\n"));
    process.exit(1);
  }
}

async function checkContrast(browser, failures) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();
  await page.goto(base + "/", { waitUntil: "networkidle" });
  const values = await page.evaluate(() => {
    const root = getComputedStyle(document.documentElement);
    return {
      text: root.getPropertyValue("--text").trim(),
      muted: root.getPropertyValue("--muted").trim(),
      muted2: root.getPropertyValue("--muted-2").trim(),
      bg: root.getPropertyValue("--bg").trim(),
      panel: root.getPropertyValue("--panel").trim(),
      cyan: root.getPropertyValue("--cyan").trim(),
      orange2: root.getPropertyValue("--orange-2").trim(),
      blue2: root.getPropertyValue("--blue-2").trim(),
    };
  });
  await context.close();

  const pairs = [
    ["text/bg", values.text, values.bg, 7],
    ["muted/bg", values.muted, values.bg, 4.5],
    ["muted2/bg", values.muted2, values.bg, 4.5],
    ["cyan/panel", values.cyan, values.panel, 4.5],
    ["orange2/panel", values.orange2, values.panel, 4.5],
    ["blue2/panel", values.blue2, values.panel, 4.5],
  ];
  const out = {};
  for (const [name, fg, bg, min] of pairs) {
    const ratio = contrastRatio(fg, bg);
    out[name] = Number(ratio.toFixed(2));
    if (ratio < min) failures.push(`contrast ${name}: ${ratio.toFixed(2)} < ${min}`);
  }
  return out;
}

function hexToRgb(hex) {
  const m = /^#([0-9a-f]{6})$/i.exec(hex.trim());
  if (!m) throw new Error(`bad color ${hex}`);
  const n = Number.parseInt(m[1], 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255].map((v) => v / 255);
}

function luminance(hex) {
  return hexToRgb(hex).map((v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4))
    .reduce((sum, v, i) => sum + v * [0.2126, 0.7152, 0.0722][i], 0);
}

function contrastRatio(a, b) {
  const l1 = luminance(a);
  const l2 = luminance(b);
  return (Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
