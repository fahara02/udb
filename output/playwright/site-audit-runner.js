const { chromium } = require("playwright");
const fs = require("fs");

async function main() {
  const browser = await chromium.launch({ headless: true });
  const pages = ["/", "/sdks.html", "/benchmarks.html", "/playground.html", "/api.html"];
  const viewports = [
    { name: "desktop", width: 1440, height: 900 },
    { name: "mobile", width: 390, height: 844 },
    { name: "narrow", width: 320, height: 700 },
  ];
  const results = [];
  fs.mkdirSync("output/playwright", { recursive: true });

  for (const vp of viewports) {
    const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height } });
    for (const path of pages) {
      const page = await context.newPage();
      const errors = [];
      page.on("console", (msg) => {
        if (msg.type() === "error") errors.push(msg.text());
      });
      page.on("pageerror", (err) => errors.push(err.message));

      await page.goto("http://localhost:8000" + path, { waitUntil: "networkidle", timeout: 30000 });
      await page.waitForTimeout(300);

      const metrics = await page.evaluate(() => {
        const nav = document.querySelector(".nav-links");
        const bench = document.querySelector(".bench-controls");
        return {
          bodyOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
          bodyBg: getComputedStyle(document.body).backgroundColor,
          navMaxHeight: nav ? getComputedStyle(nav).maxHeight : "",
          navOverflowY: nav ? getComputedStyle(nav).overflowY : "",
          benchColumns: bench ? getComputedStyle(bench).gridTemplateColumns : "",
          benchScrollDelta: bench ? bench.scrollWidth - bench.clientWidth : 0,
          h1: document.querySelector("h1")?.textContent.trim() || "",
        };
      });

      const screenshotKeys = new Set([
        "desktop /",
        "mobile /",
        "mobile /sdks.html",
        "mobile /benchmarks.html",
        "narrow /benchmarks.html",
        "desktop /playground.html",
      ]);
      if (screenshotKeys.has(`${vp.name} ${path}`)) {
        const slug = path === "/" ? "home" : path.slice(1).replace(".html", "");
        await page.screenshot({ path: `output/playwright/${slug}-${vp.name}.png`, fullPage: true });
      }

      if (vp.name !== "desktop") {
        const toggle = await page.$(".nav-toggle");
        if (toggle) {
          await toggle.click();
          await page.waitForTimeout(350);
          metrics.navOpen = await page.evaluate(() => {
            const nav = document.querySelector(".nav-links");
            return nav
              ? {
                  overflowY: getComputedStyle(nav).overflowY,
                  scrollHeight: nav.scrollHeight,
                  clientHeight: nav.clientHeight,
                }
              : null;
          });
        }
      }

      results.push({ viewport: vp.name, path, errors, metrics });
      await page.close();
    }
    await context.close();
  }

  await browser.close();

  const failures = [];
  for (const result of results) {
    const { viewport, path, errors, metrics } = result;
    if (errors.length) failures.push(`${viewport} ${path}: console errors: ${errors.join(" | ")}`);
    if (metrics.bodyOverflow > 2) failures.push(`${viewport} ${path}: body overflow ${metrics.bodyOverflow}px`);
    if (metrics.bodyBg !== "rgb(248, 250, 252)") failures.push(`${viewport} ${path}: body bg ${metrics.bodyBg}`);
    if (viewport !== "desktop" && metrics.navOpen && !/auto|scroll/.test(metrics.navOpen.overflowY)) {
      failures.push(`${viewport} ${path}: nav menu is not scrollable`);
    }
    if (viewport !== "desktop" && metrics.navOpen && metrics.navOpen.clientHeight <= 0) {
      failures.push(`${viewport} ${path}: nav menu did not open`);
    }
    if (path === "/benchmarks.html" && viewport !== "desktop") {
      const columns = metrics.benchColumns.trim().split(/\s+/).filter(Boolean).length;
      if (columns !== 1) failures.push(`${viewport} ${path}: benchmark filters not single-column (${metrics.benchColumns})`);
      if (metrics.benchScrollDelta > 2) failures.push(`${viewport} ${path}: benchmark controls overflow ${metrics.benchScrollDelta}px`);
    }
  }

  console.log(JSON.stringify(results, null, 2));
  if (failures.length) {
    console.error("\nFAILURES\n" + failures.join("\n"));
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
