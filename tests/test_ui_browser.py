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
            await page.click("button.tab[data-tab='diag']")
            await page.wait_for_selector("#diag-why .why-line", timeout=5000)
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
            assert "BTC" in await card.inner_text()
            assert await card.locator(".plan").count() == 1
            assert (await page.locator("#opps-count").inner_text()).strip() == "1"

            # 3. filters really filter
            await page.fill("#f-minnet", "50")  # nothing has NET >= 50 %
            await page.wait_for_selector("#cards-empty:not(.hidden)", timeout=3000)
            await page.click("#filters-reset")
            await page.wait_for_selector(".card", timeout=3000)

            # 4. MANUAL: support -> entered -> trade row with JOURNAL
            await card.locator("button.btn-manual").click()
            await page.wait_for_selector("#modal-manual:not(.hidden)", timeout=3000)
            await page.wait_for_selector("#modal-manual form button[type=submit]", timeout=5000)
            await page.click("#modal-manual form button[type=submit]")
            await wait_for(lambda: b.ctx.risk.open_trades() == 1, timeout=5.0)
            await page.wait_for_selector("#modal-manual:has-text('SOLD'), #modal-manual:has-text('BOUGHT BACK')", timeout=5000)
            await page.click("#manual-close")
            await page.click("button.tab[data-tab='trades']")
            await page.wait_for_selector("#trades-body tr", timeout=5000)
            row = page.locator("#trades-body tr").first
            assert "EXECUTING" in await row.inner_text()

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
            human = page.locator("#settings-form [name='human_execution_time_s']")
            await human.fill("9")
            await page.click("#settings-save")
            await wait_for(lambda: b.ctx.settings.human_execution_time_s == 9.0, timeout=5.0)
            await page.click("#settings-close")

            # 7. STATS tab renders (empty is a message, not a blank)
            await page.click("button.tab[data-tab='stats']")
            await page.wait_for_selector("#stats-empty:not(.hidden), #stats-body tr", timeout=5000)
            await page.wait_for_selector("#shadow-body tr", timeout=5000)  # the lag above was shadow-traded
            assert "BTC" in await page.locator("#shadow-body").inner_text()
            assert "ON" in await page.locator("#shadow-sum-body").inner_text()
            assert "Trades" in await page.locator("#tsum-body").inner_text()

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
