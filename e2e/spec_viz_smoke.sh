#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-4177}"
SPEC_URL="http://127.0.0.1:${PORT}/visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html"
RESULT_DIR="${ROOT_DIR}/test-results"
CONSOLE_LOG="${RESULT_DIR}/spec_viz_smoke_console.log"
SERVER_LOG="${RESULT_DIR}/spec_viz_smoke_server.log"

mkdir -p "${RESULT_DIR}"
: >"${CONSOLE_LOG}"
: >"${SERVER_LOG}"

python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${ROOT_DIR}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

cleanup() {
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sleep 1

SPEC_VIZ_URL="${SPEC_URL}" SPEC_VIZ_CONSOLE_LOG="${CONSOLE_LOG}" node <<'NODE'
const fs = require('node:fs');
const { chromium } = require('@playwright/test');

const url = process.env.SPEC_VIZ_URL;
const consoleLogPath = process.env.SPEC_VIZ_CONSOLE_LOG;

(async () => {
  const logs = [];
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  page.on('console', (msg) => {
    logs.push({ type: msg.type(), text: msg.text() });
  });

  page.on('pageerror', (err) => {
    logs.push({ type: 'pageerror', text: String(err && (err.stack || err.message || err)) });
  });

  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForFunction(() => {
    const loader = document.getElementById('loadingOverlay');
    return Boolean(loader && loader.classList.contains('hidden'));
  }, { timeout: 60000 });
  await page.waitForSelector('#specContent', { timeout: 30000 });

  const diffLoadedBefore = await page.evaluate(() => {
    return performance
      .getEntriesByType('resource')
      .some((entry) => /diff2html/i.test(entry.name));
  });

  if (diffLoadedBefore) {
    throw new Error('diff2html loaded before diff panel opened');
  }

  await page.click('#tabDiff');
  await page.waitForSelector('#viewDiff:not(.hidden)', { timeout: 15000 });
  await page.waitForFunction(() => {
    const node = document.querySelector('#diffContent');
    if (!node) return false;
    return Boolean(node.querySelector('.d2h-wrapper') || node.querySelector('.spec-fallback-pre'));
  }, { timeout: 30000 });

  const diffLoadedAfter = await page.evaluate(() => {
    return performance
      .getEntriesByType('resource')
      .some((entry) => /diff2html/i.test(entry.name));
  });

  if (!diffLoadedAfter) {
    throw new Error('diff2html did not load after opening diff panel');
  }

  await page.click('#tabSpec');
  await page.waitForSelector('#viewSpec:not(.hidden)', { timeout: 15000 });
  await page.waitForFunction(() => {
    const node = document.querySelector('#specContent');
    return Boolean(node && node.textContent && node.textContent.trim().length > 0);
  }, { timeout: 30000 });

  const hasFatalPageError = logs.some((entry) => entry.type === 'pageerror');
  if (hasFatalPageError) {
    throw new Error('pageerror observed during smoke run');
  }

  fs.writeFileSync(consoleLogPath, logs.map((entry) => JSON.stringify(entry)).join('\n') + '\n', 'utf8');
  await browser.close();

  process.stdout.write('test_e2e_bd_3oan: PASS\n');
})().catch((err) => {
  fs.writeFileSync(consoleLogPath, JSON.stringify({ type: 'runner_error', text: String(err && (err.stack || err.message || err)) }) + '\n', 'utf8');
  process.stderr.write(`test_e2e_bd_3oan: FAIL - ${String(err && (err.message || err))}\n`);
  process.exit(1);
});
NODE
