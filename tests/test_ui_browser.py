"""Browser tests (Playwright + Chromium) against the REAL app: run_app serving the
UI on port 0, FakeAdapters pushing books.  Every visible control must change
state - no placeholder buttons.  Needs the ``playwright`` package and a Chromium
(PLAYWRIGHT_BROWSERS_PATH, /opt/pw-browsers in CI); otherwise the module is
skipped with that reason, never silently."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from latbot.app import run_app
from test_app import build, push_all, wait_for

playwright = pytest.importorskip("playwright.async_api", reason="playwright package not installed")
from playwright.async_api import async_playwright  # noqa: E402



def _chromium_executable() -> str | None:
    """A Chromium Playwright can drive: CHROMIUM_PATH, /opt/pw-browsers/chromium
    (the launcher symlink), or any chromium-*/chrome-linux/chrome under
    PLAYWRIGHT_BROWSERS_PATH.  None -> let Playwright use its own download."""
    env = os.environ.get("CHROMIUM_PATH")
    if env and Path(env).exists():
        return env
    root = Path(os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers"))
    link = root / "chromium"
    if link.is_file():
        return str(link)
    for cand in sorted(root.glob("chromium-*/chrome-linux/chrome"), reverse=True):
        return str(cand)
    return None


def _has_browser() -> bool:
    return _chromium_executable() is not None or Path.home().joinpath(".cache/ms-playwright").exists()


pytestmark = pytest.mark.skipif(not _has_browser(), reason="no Chromium for Playwright (set PLAYWRIGHT_BROWSERS_PATH or CHROMIUM_PATH)")



EMOJI_JS = r"""() => {
  const re = /[\u{1F000}-\u{1FAFF}\u{2600}-\u{27BF}\u{2B50}\u{2B55}\u{23E9}-\u{23FA}\u{25B6}\u{25B8}\u{2197}\u{FE0F}]/u;
  const found = [];
  const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  let n;
  while ((n = w.nextNode())) {
    if (re.test(n.nodeValue)) found.push(n.nodeValue.trim().slice(0, 60));
  }
  document.querySelectorAll('option').forEach(o => { if (re.test(o.textContent)) found.push('option: ' + o.textContent); });
  return found;
}"""


async def assert_no_emoji(page, where: str) -> None:
    found = await page.evaluate(EMOJI_JS)
    assert not found, f"emoji visible on {where}: {found}"


async def _server(tmp_path: Path):
    b = build(tmp_path, ui_push_interval_ms=50, default_opp_duration_s=60.0)
    task = asyncio.create_task(run_app(b.ctx, seconds=None, serve=True))
    await wait_for(lambda: b.ctx.server_port is not None or task.done(), timeout=10.0, step=0.05)
    assert not task.done(), "run_app ended before the server came up"
    return b, task, f"http://127.0.0.1:{b.ctx.server_port}"


async def _lag(b) -> None:
    await push_all(b.fakes, {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    b.clock.advance(0.5)
    await push_all(b.fakes, {"binance": 100.0, "kucoin": 100.0, "kraken": 100.0})
    b.clock.advance(1.0)
    await push_all(b.fakes, {"binance": 102.0, "kucoin": 102.0, "kraken": 100.0})
    await wait_for(lambda: len(b.ctx.opps.active()) == 1, timeout=5.0)


async def test_terminal_controls_do_real_work(tmp_path: Path) -> None:
    b, task, base = await _server(tmp_path)
    try:
        async with async_playwright() as pw:
            exe = _chromium_executable()
            browser = await pw.chromium.launch(executable_path=exe) if exe else await pw.chromium.launch()
            page = await browser.new_page(viewport={"width": 1280, "height": 900})
            errors: list[str] = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            await page.goto(base, wait_until="networkidle")
            await page.wait_for_selector("#ws-label:has-text('LIVE'), #ws-label:has-text('ONLINE'), #ws-dot", timeout=10000)

            from latbot.buildinfo import build_stamp
            await page.wait_for_selector(f"#build-stamp:has-text('{build_stamp()}')", timeout=5000)

            # 1. empty board is honest, DIAG explains why
            assert await page.locator("#cards-empty").is_visible()
            await assert_no_emoji(page, "empty board")
            await page.click("button.tab[data-tab='diag']")
            await page.wait_for_selector("#diag-why .why-line", timeout=5000)
            await assert_no_emoji(page, "diagnostics tab")
            why = await page.locator("#diag-why").inner_text()
            assert any(k in why for k in ("Рынок спокоен", "тика", "не подключена")), why  # fakes: no data yet
            assert "Exchanges connected" in await page.locator("#diag-funnel").inner_text()
            assert await page.locator("#diag-exchanges tr").count() == 3  # binance, kucoin, kraken rows with status
            await page.click("#diag-refresh")

            # 2. a real lag -> a card with MANUAL / AUTO and a plan
            await _lag(b)
            await page.click("button.tab[data-tab='opps']")
            await page.wait_for_selector(".card", timeout=5000)
            card = page.locator(".card").first
            await assert_no_emoji(page, "board with an opportunity (card, best panel, lights)")
            assert "BTC" in await card.inner_text()
            assert await page.locator("#cards .card .plan").count() == 1  # the full plan lives on the list card
            assert (await page.locator("#opps-count").inner_text()).strip() == "1"
            # hero: live badge with real counters, best card with a lag chart built from real prices
            badge = await page.locator("#hero-badge").inner_text()
            assert badge.startswith("Live monitoring") and "exchanges" in badge and "1 opportunity" in badge, badge
            await page.wait_for_selector("#best-body .lag-chart .chart-svg", timeout=5000)
            assert await page.locator("#best-body .chart-chip").count() >= 2  # mover + target, each with its own number
            assert await page.locator("#best-body .tf[aria-pressed='true']").inner_text() == "30s"
            await page.click("#best-body .tf:has-text('15s')")
            await page.wait_for_selector("#best-body .tf[aria-pressed='true']:has-text('15s')", timeout=3000)
            await page.wait_for_selector("#best-body .lag-chart .chart-svg, #best-body .lag-chart .chart-empty", timeout=5000)
            await assert_no_emoji(page, "best panel with lag chart")
            # counters strip sits under the hero on this tab, at the top on the others; hero buttons do real work
            assert await page.evaluate("document.getElementById('strips').parentNode.id") == "strip-slot"
            await page.click("#hero-diag")
            await page.wait_for_selector("#tab-diag:not(.hidden)", timeout=3000)
            assert await page.evaluate("document.getElementById('strips').nextElementSibling.tagName") == "MAIN"
            await page.click("button.tab[data-tab='opps']")
            await page.wait_for_selector("#tab-opps:not(.hidden) #strip-slot #strips", timeout=3000)
            await page.click("#hero-scroll")
            await page.wait_for_function("document.getElementById('filters').getBoundingClientRect().top < 300", timeout=3000)
            await page.wait_for_timeout(600)  # smooth scroll settles
            assert await page.evaluate(
                "document.getElementById('filters').getBoundingClientRect().top >= document.querySelector('.topbar').getBoundingClientRect().bottom - 1"
            ), "filters scrolled under the sticky top bar"
            # chart window buttons keep keyboard focus across the 2 s refreshes and card updates
            await page.evaluate("window.scrollTo(0, 0)")
            await page.focus("#best-body .tf[data-sec='60']")
            await page.keyboard.press("Enter")
            await page.wait_for_selector("#best-body .tf[data-sec='60'][aria-pressed='true']", timeout=3000)
            await page.wait_for_timeout(2600)
            assert await page.evaluate("document.activeElement && document.activeElement.dataset.sec") == "60"
            await page.click("#best-body .tf[data-sec='30']")
            await page.wait_for_selector("#best-body .tf[data-sec='30'][aria-pressed='true']", timeout=3000)
            # the closed settings drawer is not focusable
            assert await page.evaluate("getComputedStyle(document.getElementById('settings-drawer')).visibility") == "hidden"

            # Phase 3: the card shows its quality and leverage; the Market tab explains the context (off in tests)
            assert await page.locator("#cards .card .q-badge").count() == 1
            math = await page.locator("#cards .card .math-line").inner_text()  # lag - fees = NET, in one line
            assert "lag" in math and "fees + execution" in math and "NET" in math
            assert "Leverage" in await page.locator("#cards .card .lev-line").inner_text()
            await page.click("button.tab[data-tab='market']")
            await page.wait_for_selector("#tab-market:not(.hidden) #market-status .why-line", timeout=5000)
            assert "context_enabled" in await page.locator("#market-status").inner_text()
            assert "BTC" in await page.locator("#market-pool").inner_text()
            await assert_no_emoji(page, "market tab")
            await page.click("button.tab[data-tab='opps']")

            # 3. filters really filter
            await page.fill("#f-minnet", "50")  # nothing has NET >= 50 %
            await page.wait_for_selector("#cards-empty:not(.hidden)", timeout=3000)
            await page.click("#filters-reset")
            await page.wait_for_selector(".card", timeout=3000)

            # 4. MANUAL TRADE SESSION: Я ЗАШЁЛ -> amount -> exact plan -> Я КУПИЛ -> escort; collapse keeps the position
            btn = card.locator("button.btn-manual")
            assert "Я ЗАШЁЛ" in await btn.inner_text()
            await btn.click()
            await page.wait_for_selector("#modal-manual:not(.hidden) input[name='capital']", timeout=5000)
            await assert_no_emoji(page, "session: amount step")
            assert await page.evaluate("document.body.classList.contains('in-session')")
            await page.fill("#modal-manual input[name='capital']", "50")
            await page.click("#modal-manual button:has-text('ПОКАЗАТЬ ТОЧНЫЙ ПЛАН')")
            await page.wait_for_selector("#modal-manual button:has-text('Я КУПИЛ')", timeout=5000)
            await assert_no_emoji(page, "session: plan step")
            plan = (await page.locator("#manual-body").inner_text()).lower()  # labels are uppercased by CSS
            assert "купить" in plan and "потом продать" in plan and "ожидаемо" in plan and "≤" in plan and "≥" in plan
            assert "0.49995 btc" in plan  # exactly 50 EUR worth on the real book
            href = await page.locator("#modal-manual a.btn-open").get_attribute("href")
            assert href and "kraken" in href
            assert await page.locator("#modal-manual input[name='quote_amount']").input_value() == "50"
            await page.click("#modal-manual button:has-text('Я КУПИЛ')")
            await wait_for(lambda: b.ctx.risk.open_trades() == 1, timeout=5.0)
            await page.wait_for_selector("#modal-manual .escort:has-text('ЖДИ')", timeout=5000)
            await assert_no_emoji(page, "session: escort")
            await page.click("#manual-close")  # Свернуть: the position stays
            assert not await page.evaluate("document.body.classList.contains('in-session')")
            await page.wait_for_selector(".card button.btn-manual:has-text('СДЕЛКА')", timeout=5000)
            await page.click("button.tab[data-tab='trades']")
            await page.wait_for_selector("#trades-body tr", timeout=5000)
            row = page.locator("#trades-body tr").first
            assert "EXECUTING" in await row.inner_text()
            chart_calls: list[str] = []
            page.on("request", lambda req: chart_calls.append(req.url) if "/chart" in req.url else None)
            await page.wait_for_timeout(2600)
            assert chart_calls == [], f"lag chart polled while hidden: {chart_calls}"

            # 5. JOURNAL: note saved on the server, ANALYZE honestly disabled without a key
            await row.locator("button:has-text('JOURNAL')").click()
            await page.wait_for_selector("#modal-journal:not(.hidden)", timeout=3000)
            await page.wait_for_selector("#journal-analysis-note:has-text('NOT AVAILABLE')", timeout=5000)
            await page.fill("#journal-note", "вошёл по плану")
            await page.click("#journal-save")
            await page.wait_for_selector("#journal-note-status:has-text('saved')", timeout=5000)
            trade_id = b.ctx.execution.trades(1)[0].id
            assert b.ctx.db.get_trade_note(trade_id) == "вошёл по плану"
            shot = tmp_path / "shot.png"
            shot.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
            await page.set_input_files("#journal-file", str(shot))
            await page.click("#journal-upload")
            await page.wait_for_selector("#journal-shots .journal-shot", timeout=5000)
            assert await page.locator("#journal-shots button:has-text('ANALYZE')").is_disabled()
            assert len(b.ctx.db.list_screenshots(trade_id)) == 1
            await page.click("#journal-close")
            await page.wait_for_selector("#trades-body tr:has-text('note')", timeout=5000)

            # 6. settings drawer saves to the server
            await page.click("#settings-btn")
            await page.wait_for_selector("#settings-drawer[aria-hidden='false']", timeout=3000)
            await assert_no_emoji(page, "settings drawer")
            human = page.locator("#settings-form [name='human_execution_time_s']")
            await human.fill("9")
            await page.click("#settings-save")
            await wait_for(lambda: b.ctx.settings.human_execution_time_s == 9.0, timeout=5.0)
            await page.click("#settings-close")

            # 7. STATS tab renders (empty is a message, not a blank)
            await page.click("button.tab[data-tab='stats']")
            await page.wait_for_selector("#stats-empty:not(.hidden), #stats-body tr", timeout=5000)
            await assert_no_emoji(page, "stats tab")
            await page.wait_for_selector("#shadow-body tr", timeout=5000)  # the lag above was shadow-traded
            assert "BTC" in await page.locator("#shadow-body").inner_text()
            assert "ON" in await page.locator("#shadow-sum-body").inner_text()
            assert "Trades" in await page.locator("#tsum-body").inner_text()
            assert "$" not in await page.locator("#tsum-body").inner_text()  # money in the venue's EUR, not '$'
            assert "€" in await page.locator("#shadow-sum-body").inner_text()
            assert "Virtual" in await page.locator("#shadow-sum-body").inner_text()  # training balance / P&L shown
            assert "READY" in await page.locator("#ready-body").inner_text()  # live-readiness checklist renders
            # "New period": statistics count from now on, history stays
            assert "since" in await page.locator("#shadow-period").inner_text()
            before = b.ctx.shadow.period_start_ts
            b.clock.advance(5.0)
            await page.click("#shadow-new-period")
            await page.click("#confirm-ok")
            await wait_for(lambda: b.ctx.shadow.period_start_ts > before, timeout=5.0)

            # 8. the session comes back by itself after a reload, escorts to the target, saves the actual result
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_selector("#modal-manual:not(.hidden) .escort", timeout=8000)
            b.clock.advance(0.3)
            await push_all(b.fakes, {"kraken": 101.6})
            await page.wait_for_selector("#modal-manual .escort:has-text('ПРОДАВАЙ СЕЙЧАС')", timeout=5000)
            await page.click("#modal-manual button:has-text('Я УШЁЛ СО СДЕЛКИ')")
            await page.fill("#modal-manual input[name='reported_pnl_quote']", "0.4")
            await page.fill("#modal-manual textarea[name='note']", "вышел по цели")
            await page.click("#modal-manual button:has-text('СОХРАНИТЬ РЕЗУЛЬТАТ')")
            await page.wait_for_selector("#modal-manual:has-text('Сделка завершена и сохранена')", timeout=5000)
            await assert_no_emoji(page, "session: saved")
            closed = b.ctx.db.get_trade(trade_id)
            assert closed["state"] == "CLOSED" and closed["user_reported_pnl_quote"] == 0.4 and closed["user_note"] == "вышел по цели"
            await page.click("#modal-manual button:has-text('К МОНИТОРИНГУ')")
            await page.wait_for_selector("#modal-manual.hidden", state="attached", timeout=5000)
            assert not await page.evaluate("document.body.classList.contains('in-session')")
            assert b.ctx.risk.open_trades() == 0

            # 8. EMERGENCY STOP: confirm dialog -> server flag -> banner
            await page.click("#emergency-btn")
            await page.wait_for_selector("#modal-confirm:not(.hidden)", timeout=3000)
            await page.click("#confirm-ok")
            await wait_for(lambda: b.ctx.risk.emergency, timeout=5.0)
            await page.wait_for_selector("#emergency-banner:not(.hidden)", timeout=5000)
            assert not errors, errors
            await browser.close()
    finally:
        if not task.done():
            b.ctx.request_stop("test teardown") if b.ctx.request_stop else task.cancel()
            await asyncio.gather(task, return_exceptions=True)
