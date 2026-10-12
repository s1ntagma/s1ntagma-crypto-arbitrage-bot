/* latbot terminal UI — vanilla JS, no build step, no CDN.
 *
 * Structure: state → utils (dom/format) → api/ws → model helpers → renderers
 * → modals (confirm / manual / auto) → emergency / settings → deep link → init.
 * Every piece of server data reaches the DOM through textContent (helper h()),
 * never through innerHTML, so nothing from the wire is interpreted as HTML.
 */
(function () {
  'use strict';

  // ------------------------------------------------------------------ state
  var TERMINAL = { CLOSED: 1, EXPIRED: 1, CANCELLED: 1, FAILED: 1 };
  var EXECUTING = { EXECUTING: 1, PARTIALLY_FILLED: 1, FILLED: 1 };
  var LIGHTS = {
    green: { emoji: '', cls: 'green', word: 'GREEN' },
    yellow: { emoji: '', cls: 'yellow', word: 'YELLOW' },
    red: { emoji: '', cls: 'red', word: 'RED' },
    unknown: { emoji: '', cls: 'muted', word: 'N/A' }
  };
  var SETTINGS_FIELDS = [
    'human_execution_time_s', 'telegram_profit_threshold_pct', 'capital_quote', 'max_trade_quote',
    'min_net_pct_show', 'min_movers', 'auto_confirm', 'alert_human_only', 'telegram_require_executable',
    'min_quality_score', 'shadow_min_net_pct', 'max_leverage', 'pool_enabled', 'context_enabled', 'ws_assets',
    'simple_mode', 'simple_entry_deadline_s', 'simple_max_hold_s',
    'ride_enabled', 'ride_entry_max_age_s', 'ride_reversal_pct', 'ride_stall_s', 'ride_trail_arm_pct', 'ride_trail_pct',
    'ride_stop_pct', 'ride_max_hold_s', 'shadow_balance_quote', 'shadow_unlimited', 'shadow_train_all_signals',
    'proven_gate_enabled', 'proven_min_trades', 'proven_min_p10',
    'autopilot', 'live_requires_ready', 'max_daily_loss_quote', 'max_exposure_per_exchange_quote', 'max_simultaneous_trades',
    'exchange_budget_quote'
  ];

  var state = {
    connected: false,
    mode: 'paper',
    emergency: false,
    emergencyReason: '',
    settings: {},
    health: {},
    dashboard: {},
    serverBest: null,
    opps: new Map(),
    history: new Map(),
    trades: new Map(),
    stats: [],
    balances: {},
    filters: { quick: 'ALL', exchange: '', asset: '', minNet: '', minWindow: '', human: '', bot: '', lowq: false },
    tab: 'opps',
    diag: null,
    journal: null,
    focusId: null,
    openWhy: new Set(),
    manual: null,
    auto: null,
    tradesLoaded: false,
    statsLoaded: false,
    chartSeconds: 30
  };
  try {
    var savedTf = parseInt(window.localStorage.getItem('latbot.chartSeconds'), 10);
    if (savedTf === 15 || savedTf === 30 || savedTf === 60) state.chartSeconds = savedTf;
  } catch (e) { /* storage blocked: default window */ }

  // ------------------------------------------------------------------ dom utils
  function $(id) { return document.getElementById(id); }

  function h(tag, attrs) {
    var el = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        var v = attrs[k];
        if (v === null || v === undefined || v === false) return;
        if (k === 'class') el.className = v;
        else if (k === 'text') el.textContent = String(v);
        else if (k.indexOf('on') === 0 && typeof v === 'function') el.addEventListener(k.slice(2).toLowerCase(), v);
        else if (k === 'dataset') Object.keys(v).forEach(function (dk) { el.dataset[dk] = String(v[dk]); });
        else el.setAttribute(k, v === true ? '' : String(v));
      });
    }
    for (var i = 2; i < arguments.length; i++) appendChild(el, arguments[i]);
    return el;
  }

  function appendChild(el, c) {
    if (c === null || c === undefined || c === false) return;
    if (Array.isArray(c)) { c.forEach(function (x) { appendChild(el, x); }); return; }
    el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
  }

  function clear(el) { while (el.firstChild) el.removeChild(el.firstChild); }

  function safeUrl(url) {
    return typeof url === 'string' && /^https?:\/\//i.test(url) ? url : '';
  }

  // ------------------------------------------------------------------ formatting
  var nf2 = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  var nf0 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });

  function num(v) {
    if (v === null || v === undefined || v === '') return NaN;
    var n = typeof v === 'number' ? v : parseFloat(v);
    return Number.isFinite(n) ? n : NaN;
  }

  function quoteSymbol(quote) {
    var q = String(quote || '').toUpperCase();
    if (q === 'EUR') return '€';
    if (q === 'GBP') return '£';
    if (q === '' || q === 'USD' || q === 'USDT' || q === 'USDC' || q === 'USDE' || q === 'DAI') return '$';
    return q + ' ';
  }

  function fmtAbs(abs) {
    if (abs >= 1) return nf2.format(abs);
    if (abs === 0) return '0.00';
    if (abs < 1e-6) return abs.toFixed(8);
    return String(parseFloat(abs.toPrecision(6)));
  }

  function fmtMoney(v, quote, opts) {
    var n = num(v);
    if (!Number.isFinite(n)) return '–';
    var sign = n < 0 ? '-' : (opts && opts.sign && n > 0 ? '+' : '');
    return sign + quoteSymbol(quote) + fmtAbs(Math.abs(n));
  }

  function fmtPct(v) {
    var n = num(v);
    if (!Number.isFinite(n)) return '–';
    return (n > 0 ? '+' : n < 0 ? '-' : '') + nf2.format(Math.abs(n)) + '%';
  }

  function fmtRate(v) {
    var n = num(v);
    return Number.isFinite(n) ? nf0.format(n * 100) + '%' : '–';
  }

  function fmtSecs(v, digits) {
    var n = num(v);
    return Number.isFinite(n) ? n.toFixed(digits === undefined ? 1 : digits) + ' s' : '–';
  }

  function fmtMs(v) {
    var n = num(v);
    return Number.isFinite(n) ? nf0.format(n) + ' ms' : '–';
  }

  function fmtInt(v) {
    var n = num(v);
    return Number.isFinite(n) ? nf0.format(n) : '–';
  }

  function fmtTime(ts) {
    var n = num(ts);
    if (!Number.isFinite(n) || n <= 0) return '–';
    var d = new Date(n * 1000);
    return d.toLocaleTimeString([], { hour12: false });
  }

  function fmtAmount(v) {
    var n = num(v);
    if (!Number.isFinite(n)) return '–';
    return String(parseFloat(n.toPrecision(6)));
  }

  function humanize(key) {
    return String(key).replace(/_/g, ' ').replace(/\bpct\b/g, '%').replace(/\bs\b/g, '(s)').replace(/\bms\b/g, '(ms)').toUpperCase();
  }

  function fmtAny(v, key) {
    if (v === null || v === undefined) return '–';
    if (typeof v === 'boolean') return v ? 'yes' : 'no';
    if (typeof v === 'number') {
      var k = String(key || '');
      if (/pct$/.test(k)) return fmtPct(v);
      if (/rate$|frequency$|ratio$/.test(k) && v >= 0 && v <= 1) return fmtRate(v);
      return Number.isInteger(v) ? nf0.format(v) : nf2.format(v);
    }
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
  }

  // ------------------------------------------------------------------ api
  function api(method, path, body) {
    var opts = { method: method, headers: {} };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    return fetch(path, opts).then(function (res) {
      return res.text().then(function (text) {
        var data = null;
        try { data = text ? JSON.parse(text) : null; } catch (e) { data = { error: text }; }
        if (!res.ok) {
          return { ok: false, status: res.status, error: (data && data.error) || ('HTTP ' + res.status), data: data };
        }
        return { ok: true, status: res.status, data: data };
      });
    }).catch(function (e) {
      return { ok: false, status: 0, error: 'network error: ' + (e && e.message ? e.message : e), data: null };
    });
  }

  function oppPath(id, suffix) {
    return '/api/opportunities/' + encodeURIComponent(id) + (suffix || '');
  }

  // ------------------------------------------------------------------ toasts
  function toast(message, kind) {
    var box = $('toasts');
    var el = h('div', { class: 'toast ' + (kind || ''), role: 'status', text: message });
    box.appendChild(el);
    setTimeout(function () { dismissToast(el); }, kind === 'err' ? 7000 : 4000);
  }

  function dismissToast(el) {
    if (!el.parentNode) return;
    el.classList.add('toast-out');
    setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, 220);
  }

  // ------------------------------------------------------------------ motion helpers
  // Animations are CSS classes; these helpers only decide WHEN a class is toggled.
  // uiMemo remembers what the DOM showed last time so a change can be highlighted.
  var uiMemo = { connected: null, bestId: null, healthCls: {}, loaded: false };

  function pulseOnce(el, cls, ms) {
    if (!el) return;
    el.classList.remove(cls);
    void el.offsetWidth;  // restart the CSS animation
    el.classList.add(cls);
    setTimeout(function () { el.classList.remove(cls); }, ms || 1000);
  }

  function flashChanged(oldRoot, newRoot, selector) {
    // Same structure, different text → brief highlight on the new element (no layout shift: tabular-nums).
    if (!oldRoot || !newRoot) return;
    var a = oldRoot.querySelectorAll(selector);
    var b = newRoot.querySelectorAll(selector);
    if (a.length !== b.length) return;
    for (var i = 0; i < b.length; i++) if (a[i].textContent !== b[i].textContent) b[i].classList.add('flash');
  }

  function markLightChanges(oldRoot, newRoot) {
    // HUMAN / BOT light colour changed → one-time pulse of the dot on the fresh card.
    if (!oldRoot || !newRoot) return;
    var a = oldRoot.querySelectorAll('.line[data-light]');
    var b = newRoot.querySelectorAll('.line[data-light]');
    if (a.length !== b.length) return;
    for (var i = 0; i < b.length; i++) {
      if (a[i].dataset.light !== b[i].dataset.light) {
        var dot = b[i].querySelector('.dot');
        if (dot) dot.classList.add('light-change');
      }
    }
  }

  function removeAfter(el, ms) {
    setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, ms);
  }

  // ------------------------------------------------------------------ websocket
  var ws = null;
  var wsBackoff = 500;
  var wsTimer = null;
  var pingTimer = null;

  function wsUrl() {
    return (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';
  }

  function connectWs() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    try {
      ws = new WebSocket(wsUrl());
    } catch (e) {
      scheduleReconnect();
      return;
    }
    ws.onopen = function () {
      state.connected = true;
      wsBackoff = 500;
      scheduleRender('topbar');
      clearInterval(pingTimer);
      pingTimer = setInterval(function () {
        if (ws && ws.readyState === WebSocket.OPEN) ws.send('ping');
      }, 20000);
    };
    ws.onmessage = function (ev) {
      var msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg && typeof msg === 'object') handleMessage(msg);
    };
    ws.onclose = function () {
      state.connected = false;
      clearInterval(pingTimer);
      scheduleRender('topbar');
      scheduleReconnect();
    };
    ws.onerror = function () { /* onclose follows */ };
  }

  function scheduleReconnect() {
    clearTimeout(wsTimer);
    wsTimer = setTimeout(connectWs, wsBackoff);
    wsBackoff = Math.min(10000, wsBackoff * 2);
  }

  function handleMessage(msg) {
    var d = msg.data;
    switch (msg.topic) {
      case 'snapshot':
        applySnapshot(d || {});
        break;
      case 'opp.new':
      case 'opp.update':
        if (d && d.id) { putOpp(d); scheduleRender('cards'); }
        break;
      case 'opp.state':
        if (d && d.opportunity && d.opportunity.id) {
          putOpp(d.opportunity);
          onOppState(d.opportunity, d);
          scheduleRender('cards');
        }
        break;
      case 'trade.update':
        if (d && d.id) {
          state.trades.set(d.id, d);
          onTradeUpdate(d);
          scheduleRender('trades');
        }
        break;
      case 'health':
        if (d && d.exchange) {
          state.health[d.exchange] = Object.assign({}, state.health[d.exchange] || {}, d);
          scheduleRender('health');
        }
        break;
      case 'dashboard':
        state.dashboard = d || {};
        scheduleRender('dashboard');
        break;
      case 'emergency':
        state.emergency = !!(d && d.active);
        state.emergencyReason = (d && d.reason) || '';
        scheduleRender('topbar');
        scheduleRender('cards');
        break;
      case 'settings':
        applySettings(d || {});
        break;
      case 'balance':
        if (d && d.exchange) state.balances[d.exchange] = d.balances || {};
        break;
      default:
        break;
    }
  }

  function applySnapshot(snap) {
    uiMemo.loaded = true;
    if (snap.state) applyState(snap.state);
    state.opps.clear();
    (snap.opportunities || []).forEach(putOpp);
    renderAll();
  }

  function applyState(s) {
    if (!s) return;
    state.mode = s.mode || state.mode;
    if (s.build && $('build-stamp')) $('build-stamp').textContent = 'build ' + s.build;
    state.emergency = !!s.emergency;
    state.emergencyReason = s.emergency_reason || '';
    if (s.settings) state.settings = s.settings;
    if (Array.isArray(s.health)) {
      var merged = {};
      s.health.forEach(function (hh) { if (hh && hh.exchange) merged[hh.exchange] = Object.assign({}, state.health[hh.exchange] || {}, hh); });
      state.health = merged;
    }
    if (s.dashboard) state.dashboard = s.dashboard;
    state.serverBest = s.best || null;
    if (s.best && s.best.id) putOpp(s.best);
  }

  function applySettings(s) {
    state.settings = s || {};
    if (s && s.mode) state.mode = s.mode;
    scheduleRender('topbar');
    scheduleRender('settings');
    scheduleRender('cards');
  }

  // ------------------------------------------------------------------ opportunity helpers
  function isTerminal(st) { return !!TERMINAL[st]; }
  function isExecuting(st) { return !!EXECUTING[st]; }

  function hasOpenManualPosition(o) {
    // EXECUTING with a trade that has no entry order = the owner reported the entry by hand (MANUAL).
    // When the trade record is not loaded yet the server decides: /manual answers status 'in_position' or not.
    if (!o || !isExecuting(o.state) || !o.execution_id) return false;
    var t = state.trades.get(o.execution_id);
    if (t && (t.entry_order || isTerminal(t.state))) return false;
    return true;
  }

  function putOpp(o) {
    if (!o || !o.id) return;
    o._recvAt = performance.now();
    if (isTerminal(o.state)) {
      state.opps.delete(o.id);
      state.history.set(o.id, o);
    } else {
      state.history.delete(o.id);
      state.opps.set(o.id, o);
    }
  }

  function getOpp(id) { return state.opps.get(id) || state.history.get(id) || null; }

  function remaining(o) {
    if (!o || isTerminal(o.state)) return 0;
    var base = num(o.remaining_s);
    if (!Number.isFinite(base)) return 0;
    return Math.max(0, base - (performance.now() - (o._recvAt || performance.now())) / 1000);
  }

  function netOf(o) { return num(o && o.econ && o.econ.net_pct); }
  function lightOf(o, who) { return String(o && o[who] && o[who].light || '').toLowerCase(); }
  function lightInfo(light) { return LIGHTS[String(light || '').toLowerCase()] || LIGHTS.unknown; }

  function pairName(o) {
    var sym = String(o.target_symbol || '');
    var core = sym.split(':')[0];
    return core || (o.asset + '/' + (o.quote || ''));
  }

  function quoteOf(o) {
    if (o.quote) return o.quote;
    var core = String(o.target_symbol || '').split(':')[0];
    return core.indexOf('/') >= 0 ? core.split('/')[1] : '';
  }

  function exchangeLabel(name) {
    var hh = state.health[name];
    if (hh && hh.label) return hh.label;
    var list = (state.settings && state.settings.exchanges) || [];
    for (var i = 0; i < list.length; i++) {
      if (list[i].name === name) return list[i].label || (name.charAt(0).toUpperCase() + name.slice(1));
    }
    return name ? name.charAt(0).toUpperCase() + name.slice(1) : '?';
  }

  function threshold() {
    var t = num(state.settings.telegram_profit_threshold_pct);
    return Number.isFinite(t) ? t : 3;
  }

  function displayQuote() {
    var q = state.settings && state.settings.quote_assets;
    return Array.isArray(q) && q.length ? q[0] : '';
  }

  function computeBest() {
    var best = null;
    state.opps.forEach(function (o) {
      if (isTerminal(o.state) || isExecuting(o.state)) return;
      if (lightOf(o, 'human') === 'red' && lightOf(o, 'bot') === 'red') return;
      if (remaining(o) <= 0) return;
      if (!qualityPassed(o)) return;
      if (!best || netOf(o) > netOf(best)) best = o;
    });
    return best;
  }

  function barClass(rem, o) {
    var est = num(o && o.human && o.human.est_execution_s);
    if (!Number.isFinite(est)) est = num(state.settings.human_execution_time_s) || 7;
    if (rem >= est * 1.5) return 'green';
    if (rem >= est) return 'yellow';
    return 'red';
  }

  // ------------------------------------------------------------------ render scheduling
  var dirty = new Set();
  var raf = 0;

  function scheduleRender(part) {
    dirty.add(part);
    if (!raf) raf = requestAnimationFrame(flushRender);
  }

  function flushRender() {
    raf = 0;
    var parts = new Set(dirty);
    dirty.clear();
    if (parts.has('topbar')) renderTopbar();
    if (parts.has('dashboard')) renderDashboard();
    if (parts.has('health')) renderHealth();
    if (parts.has('cards')) { renderFilters(); renderBest(); renderCards(); }
    if (parts.has('trades')) renderTrades();
    if (parts.has('stats')) renderStats();
    if (parts.has('settings')) renderSettings(false);
  }

  function renderAll() {
    renderTopbar();
    renderDashboard();
    renderHealth();
    renderFilters();
    renderBest();
    renderCards();
    renderTrades();
    renderStats();
    renderSettings(false);
  }

  // ------------------------------------------------------------------ renderers
  // Icons are built with the DOM (never with markup strings).
  function svgIcon(shapes) {
    var NS = 'http://www.w3.org/2000/svg';
    var svg = document.createElementNS(NS, 'svg');
    var attrs = { 'class': 'ico', viewBox: '0 0 24 24', width: '18', height: '18', fill: 'none', stroke: 'currentColor',
      'stroke-width': '2', 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' };
    Object.keys(attrs).forEach(function (k) { svg.setAttribute(k, attrs[k]); });
    shapes.forEach(function (sh) {
      var el = document.createElementNS(NS, sh[0]);
      Object.keys(sh[1]).forEach(function (k) { el.setAttribute(k, sh[1][k]); });
      svg.appendChild(el);
    });
    return svg;
  }
  function iconStop() {
    return svgIcon([['polygon', { points: '7.86 2 16.14 2 22 7.86 22 16.14 16.14 22 7.86 22 2 16.14 2 7.86 7.86 2' }],
      ['line', { x1: '12', y1: '8', x2: '12', y2: '12' }], ['line', { x1: '12', y1: '16', x2: '12.01', y2: '16' }]]);
  }
  function iconPlay() { return svgIcon([['polygon', { points: '6 4 20 12 6 20 6 4' }]]); }
  function setButtonLabel(btn, icon, text) {
    clear(btn);
    btn.appendChild(icon);
    btn.appendChild(h('span', { text: text }));
  }

  function renderTopbar() {
    var badge = $('mode-badge');
    var live = state.mode === 'live';
    badge.textContent = live ? 'LIVE' : 'PAPER';
    badge.className = 'mode-badge ' + (live ? 'live' : 'paper');
    document.title = (live ? '[LIVE] ' : '[PAPER] ') + 'latbot · latency terminal';

    $('ws-dot').className = 'dot ' + (state.connected ? 'green' : 'red');
    $('ws-label').textContent = state.connected ? 'live' : 'reconnecting…';
    if (uiMemo.connected === false && state.connected) pulseOnce($('ws-status'), 'reconnected');
    uiMemo.connected = state.connected;
    var topbar = document.querySelector('.topbar');
    if (topbar) topbar.classList.toggle('emergency', !!state.emergency);
    renderHeroBadge();

    var btn = $('emergency-btn');
    var closeBtn = $('close-positions-btn');
    var banner = $('emergency-banner');
    if (state.emergency) {
      setButtonLabel(btn, iconPlay(), 'RESUME TRADING');
      btn.className = 'btn btn-warn';
      closeBtn.classList.remove('hidden');
      banner.textContent = 'EMERGENCY STOP ACTIVE — new AUTO trades are blocked' + (state.emergencyReason ? ' (reason: ' + state.emergencyReason + ')' : '');
      banner.classList.remove('hidden');
    } else {
      setButtonLabel(btn, iconStop(), 'EMERGENCY STOP');
      btn.className = 'btn btn-danger';
      closeBtn.classList.add('hidden');
      banner.classList.add('hidden');
    }
  }

  function tile(label, valueNode, cls) {
    return h('div', { class: 'tile' + (cls ? ' ' + cls : '') },
      h('div', { class: 'tile-label', text: label }),
      h('div', { class: 'tile-value', title: valueNode instanceof Node ? valueNode.textContent : String(valueNode) }, valueNode));
  }

  function renderDashboard() {
    var d = state.dashboard || {};
    var q = displayQuote();
    var old = $('dashboard');
    var box = document.createDocumentFragment();
    var pnlNode = function (v) {
      var n = num(v);
      return h('span', { class: n > 0 ? 'pos' : n < 0 ? 'neg' : '', text: fmtMoney(v, q, { sign: true }) });
    };
    box.appendChild(tile('Active', fmtInt(d.active)));
    box.appendChild(tile('Today', fmtInt(d.today)));
    box.appendChild(tile('Lights', h('span', { class: 'tile-lights' },
      h('span', { class: 'light green' }, h('span', { class: 'dot green', 'aria-hidden': 'true' }), fmtInt(d.green) + ' green'),
      h('span', { class: 'light yellow' }, h('span', { class: 'dot yellow', 'aria-hidden': 'true' }), fmtInt(d.yellow) + ' yellow'),
      h('span', { class: 'light red' }, h('span', { class: 'dot red', 'aria-hidden': 'true' }), fmtInt(d.red) + ' red')), 'tile-wide'));
    box.appendChild(tile('Human OK', fmtInt(d.human_executable)));
    box.appendChild(tile('Bot OK', fmtInt(d.bot_executable)));
    box.appendChild(tile('TG alerts', fmtInt(d.telegram_alerts)));
    box.appendChild(tile('Paper P&L', pnlNode(d.paper_pnl)));
    box.appendChild(tile('Live P&L', pnlNode(d.live_pnl)));
    box.appendChild(tile('Realized', pnlNode(d.realized_pnl)));
    box.appendChild(tile('Failed', fmtInt(d.failed_trades)));
    box.appendChild(tile('Expired', fmtInt(d.expired)));
    box.appendChild(tile('Avg latency', fmtMs(d.avg_latency_ms)));
    box.appendChild(tile('Avg exec', fmtSecs(d.avg_execution_time_s)));
    box.appendChild(tile('Success', fmtRate(d.success_rate)));
    flashChanged(old, box, '.tile-value');
    clear(old);
    old.appendChild(box);
  }

  function renderHealth() {
    var box = $('health-strip');
    clear(box);
    var names = Object.keys(state.health).sort(function (a, b) {
      var ra = state.health[a].role === 'execution' ? 0 : 1;
      var rb = state.health[b].role === 'execution' ? 0 : 1;
      return ra - rb || a.localeCompare(b);
    });
    if (!names.length) {
      box.appendChild(h('span', { class: 'muted', text: 'No exchange adapters reported yet.' }));
      return;
    }
    names.forEach(function (name) {
      var hh = state.health[name];
      var pollOnly = !hh.ws_connected && hh.polling;
      var cls = pollOnly ? 'yellow' : !hh.ws_connected ? 'red' : (hh.usable === false || hh.stale || hh.maintenance) ? 'yellow' : 'green';
      var word = pollOnly ? 'poll' : cls === 'red' ? 'offline' : cls === 'yellow' ? 'degraded' : 'ok';
      var prev = uiMemo.healthCls[name];
      uiMemo.healthCls[name] = cls;
      var back = prev && prev !== 'green' && cls === 'green';
      var chip = h('span', { class: 'chip-health' + (back ? ' reconnected' : ''), title: hh.reason || '' },
        h('span', { class: 'dot ' + cls, 'aria-hidden': 'true' }),
        h('span', { text: hh.label || exchangeLabel(name) }),
        h('span', { class: 'muted', text: word }),
        h('span', { class: 'lat', text: 'ws ' + fmtMs(hh.ws_latency_ms) }));
      if (Number.isFinite(num(hh.order_latency_ms))) chip.appendChild(h('span', { class: 'lat', text: 'order ' + fmtMs(hh.order_latency_ms) }));
      if (hh.role === 'execution') chip.appendChild(h('span', { class: 'tag accent', text: 'target' }));
      if (hh.stale && !pollOnly) chip.appendChild(h('span', { class: 'tag yellow', text: 'stale' }));
      if (pollOnly) chip.title = 'WebSocket down; covered by REST ticker polling';
      if (hh.maintenance) chip.appendChild(h('span', { class: 'tag red', text: 'maint' }));
      if (hh.rate_limited_until_mono) chip.appendChild(h('span', { class: 'tag yellow', text: 'rate-limited' }));
      box.appendChild(chip);
    });
    renderHeroBadge();
  }

  function renderFilters() {
    var exchanges = new Set();
    var assets = new Set();
    var collect = function (o) { if (o.target_exchange) exchanges.add(o.target_exchange); if (o.asset) assets.add(o.asset); };
    state.opps.forEach(collect);
    state.history.forEach(collect);
    fillSelect($('f-exchange'), Array.from(exchanges).sort(), state.filters.exchange, exchangeLabel);
    fillSelect($('f-asset'), Array.from(assets).sort(), state.filters.asset, null);
    var chip = document.querySelector('#filter-quick [data-quick="THRESHOLD"]');
    if (chip) chip.textContent = '≥' + String(parseFloat(threshold().toFixed(2))) + '%';
    document.querySelectorAll('#filter-quick .chip').forEach(function (b) {
      b.classList.toggle('active', b.dataset.quick === state.filters.quick);
      b.setAttribute('aria-pressed', b.dataset.quick === state.filters.quick ? 'true' : 'false');
    });
  }

  function fillSelect(sel, values, current, labelFn) {
    var keep = current && values.indexOf(current) >= 0 ? current : '';
    clear(sel);
    sel.appendChild(h('option', { value: '', text: 'any' }));
    values.forEach(function (v) {
      sel.appendChild(h('option', { value: v, text: labelFn ? labelFn(v) : v }));
    });
    sel.value = keep;
  }

  // Phase 3: below the quality threshold (or vetoed) -> hidden by default; still traded virtually (training).
  function qualityPassed(o) {
    var q = o && o.quality;
    var qOk = !q || q.score == null || q.enforced === false || q.passed !== false;  // simple mode: info only
    return qOk && provenOk(o);
  }

  // Simple mode: only pairs that proved themselves on the bot's own virtual trades are offered.
  function provenOk(o) {
    var p = o && o.proven;
    return !p || !p.enforced || !!p.proven;
  }

  function provenBadge(o) {
    var p = o && o.proven;
    if (!p || p.n == null) return null;
    var cls = 'proven-badge ' + (p.status || 'training');
    var word = p.status === 'proven' ? 'Proven ' + p.wins + '/' + p.n : p.status === 'failing' ? 'Failing ' + p.wins + '/' + p.n
      : p.n >= p.need ? 'Not convincing ' + p.wins + '/' + p.n : 'Training ' + p.n + '/' + p.need;
    return h('span', { class: cls, title: p.text || '', text: word });
  }

  function bestSizeLine(o) {
    var b = o && o.best_size;
    if (!b || !Number.isFinite(num(b.capital_quote))) return null;
    var q = quoteOf(o);
    return h('div', { class: 'math-line best-size' }, 'Best size on this book: ' + fmtMoney(b.capital_quote, q) + ' -> ' +
      fmtMoney(b.net_quote, q, { sign: true }) + ' expected (NET ' + fmtPct(num(b.net_pct)) + ')');
  }

  function passesFilters(o) {
    var f = state.filters;
    var term = isTerminal(o.state);
    if (state.focusId === o.id) return true;
    if (!f.lowq && !term && !qualityPassed(o) && !(state.manual && state.manual.id === o.id) && !hasOpenManualPosition(o)) return false;
    switch (f.quick) {
      case 'ALL': if (term) return false; break;
      case 'MANUAL': if (term || lightOf(o, 'human') === 'red') return false; break;
      case 'AUTO': if (term || lightOf(o, 'bot') === 'red') return false; break;
      case 'THRESHOLD': if (term || netOf(o) < threshold()) return false; break;
      case 'PAPER': if (term || o.mode !== 'paper') return false; break;
      case 'LIVE': if (term || o.mode !== 'live') return false; break;
      case 'EXPIRED': if (!term) return false; break;
      default: break;
    }
    if (f.exchange && o.target_exchange !== f.exchange) return false;
    if (f.asset && o.asset !== f.asset) return false;
    if (f.minNet !== '' && Number.isFinite(num(f.minNet)) && netOf(o) < num(f.minNet)) return false;
    if (f.minWindow !== '' && Number.isFinite(num(f.minWindow)) && !term && remaining(o) < num(f.minWindow)) return false;
    if (f.human && lightOf(o, 'human') !== f.human) return false;
    if (f.bot && lightOf(o, 'bot') !== f.bot) return false;
    return true;
  }

  function visibleOpps() {
    var source = [];
    if (state.filters.quick === 'EXPIRED') {
      state.history.forEach(function (o) { source.push(o); });
    } else {
      state.opps.forEach(function (o) { source.push(o); });
      if (state.focusId && state.history.has(state.focusId)) source.push(state.history.get(state.focusId));
    }
    var list = source.filter(passesFilters);
    if (state.filters.quick === 'EXPIRED') {
      list.sort(function (a, b) { return num(b.detected_ts) - num(a.detected_ts); });
    } else {
      list.sort(function (a, b) {
        var ea = isExecuting(a.state) ? 0 : 1;
        var eb = isExecuting(b.state) ? 0 : 1;
        return ea - eb || netOf(b) - netOf(a);
      });
    }
    return list;
  }

  function renderBest() {
    var box = $('best-body');
    var best = computeBest();
    var id = best ? best.id : null;
    var old = box.firstElementChild;
    var fresh = best ? renderCard(best, { best: true })
      : h('div', { class: 'best-empty', text: lowQualityCount() > 0
        ? 'Nothing proven right now: ' + lowQualityCount() + ' offer(s) are still training or below quality (traded virtually; tick "Show training / low quality" below).'
        : 'No executable opportunity right now — waiting for a real lag (no data, no signal).' });
    if (best && old && id === uiMemo.bestId) {
      flashChanged(old, fresh, '.card-net-value, .kv-v');
      markLightChanges(old, fresh);
      // Keep the live chart node (and its focused/pressed window button) across card updates:
      // only refreshChart()/setChartSeconds() redraw it.
      var oc = old.querySelector('.lag-chart');
      var nc = fresh.querySelector('.lag-chart');
      if (oc && nc && oc.dataset.id === id) nc.parentNode.replaceChild(oc, nc);
    }
    clear(box);
    box.appendChild(fresh);
    if (id !== uiMemo.bestId && (old || best)) pulseOnce(box, 'swap', 400);
    uiMemo.bestId = id;
    if (best) refreshChart(false);
  }

  // ------------------------------------------------------------------ lag chart (best opportunity)
  // Real mid prices of the movers and the target (GET /api/opportunities/{id}/chart), all lines in %
  // from one pre-move reference. No history -> an honest empty state; nothing is invented.
  var chartCache = { id: null, seconds: 0, data: null, raw: '', at: 0 };
  var chartBusy = false;
  var SVG_NS = 'http://www.w3.org/2000/svg';
  var CHART_WINDOWS = [15, 30, 60];

  function svgNode(tag, attrs) {
    var el = document.createElementNS(SVG_NS, tag);
    Object.keys(attrs || {}).forEach(function (k) { el.setAttribute(k, String(attrs[k])); });
    for (var i = 2; i < arguments.length; i++) if (arguments[i]) el.appendChild(arguments[i]);
    return el;
  }

  function initials(name) {
    var t = String(exchangeLabel(name) || '?').replace(/[^A-Za-z0-9]/g, '');
    return (t.slice(0, 2) || '?').toUpperCase();
  }

  function chartVisible() {
    return state.tab === 'opps' && !document.hidden && modalHidden('manual');
  }

  function refreshChart(force) {
    var best = computeBest();
    if (!best || chartBusy || !chartVisible()) return;
    var same = chartCache.id === best.id && chartCache.seconds === state.chartSeconds;
    if (!force && same && Date.now() - chartCache.at < 1800) return;
    chartBusy = true;
    var id = best.id;
    var secs = state.chartSeconds;
    api('GET', oppPath(id, '/chart?seconds=' + secs)).then(function (r) {
      chartBusy = false;
      var cur = computeBest();
      if (r.ok) {
        var raw = JSON.stringify(r.data);
        var unchanged = chartCache.id === id && chartCache.seconds === secs && chartCache.raw === raw;
        chartCache = { id: id, seconds: secs, data: r.data, raw: raw, at: Date.now() };
        if (!unchanged && cur && cur.id === id && secs === state.chartSeconds) updateChart(cur);
      }
      // The best opportunity or the window changed while this request was in flight: ask again now.
      if (cur && (cur.id !== id || state.chartSeconds !== secs)) refreshChart(true);
    });
  }

  function setChartSeconds(sec) {
    if (state.chartSeconds === sec) return;
    state.chartSeconds = sec;
    try { window.localStorage.setItem('latbot.chartSeconds', String(sec)); } catch (e) { /* storage blocked */ }
    var best = computeBest();
    if (best) updateChart(best);
    refreshChart(true);
  }

  // Redraw legend + plot inside the existing chart node; the window buttons stay (focus and clicks survive).
  function updateChart(o) {
    var node = document.querySelector('#best-body .lag-chart');
    if (!node || node.dataset.id !== o.id) return;
    var parts = chartParts(o);
    var oldLegend = node.querySelector('.chart-legend');
    var oldFrame = node.querySelector('.chart-frame');
    if (oldLegend) oldLegend.parentNode.replaceChild(parts.legend, oldLegend);
    if (oldFrame) oldFrame.parentNode.replaceChild(parts.frame, oldFrame);
    node.querySelectorAll('.tf').forEach(function (b) {
      b.setAttribute('aria-pressed', num(b.dataset.sec) === state.chartSeconds ? 'true' : 'false');
    });
  }

  function seriesOrder(series) {
    return series.slice().sort(function (a, b) { return (a.role === 'target' ? 1 : 0) - (b.role === 'target' ? 1 : 0); });
  }

  function chartPlot(data) {
    // preserveAspectRatio=none stretches the plot to the frame; text and dots are HTML on top, so they never distort.
    var W = 600, H = 150, padT = 10, padB = 10;
    var secs = num(data.seconds) || state.chartSeconds;
    var lo = 0, hi = 0;
    data.series.forEach(function (s) { s.points.forEach(function (p) { lo = Math.min(lo, p[1]); hi = Math.max(hi, p[1]); }); });
    var span = hi - lo;
    if (span < 0.02) { lo -= 0.01; hi += 0.01; span = hi - lo; }
    lo -= span * 0.08; hi += span * 0.08;
    var X = function (t) { return ((t + secs) / secs) * W; };
    var Y = function (v) { return padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB); };
    var pctX = function (t) { return Math.max(0, Math.min(100, (X(t) / W) * 100)).toFixed(2) + '%'; };
    var pctY = function (v) { return ((Y(v) / H) * 100).toFixed(2) + '%'; };
    var plot = h('div', { class: 'chart-plot' });
    var svg = svgNode('svg', { 'class': 'chart-svg', viewBox: '0 0 ' + W + ' ' + H, preserveAspectRatio: 'none', 'aria-hidden': 'true', focusable: 'false' });
    var grad = svgNode('linearGradient', { id: 'chart-fill-target', x1: '0', y1: '0', x2: '0', y2: '1' },
      svgNode('stop', { offset: '0%', style: 'stop-color:var(--neon);stop-opacity:0.32' }),
      svgNode('stop', { offset: '100%', style: 'stop-color:var(--neon);stop-opacity:0' }));
    svg.appendChild(svgNode('defs', {}, grad));
    for (var g = 1; g <= 3; g++) {
      var gy = padT + (g / 4) * (H - padT - padB);
      svg.appendChild(svgNode('line', { 'class': 'chart-grid', x1: 0, x2: W, y1: gy, y2: gy, 'vector-effect': 'non-scaling-stroke' }));
    }
    svg.appendChild(svgNode('line', { 'class': 'chart-zero', x1: 0, x2: W, y1: Y(0), y2: Y(0), 'vector-effect': 'non-scaling-stroke' }));
    var det = num(data.detected_at_s);
    var showDet = Number.isFinite(det) && det >= -secs && det <= 0;
    if (showDet) svg.appendChild(svgNode('line', { 'class': 'chart-detect', x1: X(det), x2: X(det), y1: 0, y2: H, 'vector-effect': 'non-scaling-stroke' }));
    var signalIdx = 0;
    seriesOrder(data.series).forEach(function (s) {
      var d = s.points.map(function (p, i) { return (i ? 'L' : 'M') + X(p[0]).toFixed(1) + ' ' + Y(p[1]).toFixed(1); }).join(' ');
      var cls = 'chart-line ' + s.role;
      if (s.role === 'signal') { signalIdx++; if (signalIdx > 1) cls += ' s' + Math.min(signalIdx, 3); }
      if (s.role === 'target') {
        var first = s.points[0], last = s.points[s.points.length - 1];
        svg.appendChild(svgNode('path', { 'class': 'chart-area', fill: 'url(#chart-fill-target)',
          d: d + ' L' + X(last[0]).toFixed(1) + ' ' + H + ' L' + X(first[0]).toFixed(1) + ' ' + H + ' Z' }));
      }
      svg.appendChild(svgNode('path', { 'class': cls, d: d }));
      var end = s.points[s.points.length - 1];
      plot.appendChild(h('span', { class: 'chart-dot ' + s.role, 'aria-hidden': 'true', style: 'left:' + pctX(end[0]) + ';top:' + pctY(end[1]) }));
    });
    plot.insertBefore(svg, plot.firstChild);
    if (showDet) plot.appendChild(h('span', { class: 'chart-tag', 'aria-hidden': 'true', text: 'signal', style: 'left:' + pctX(det) }));
    var axis = h('div', { class: 'chart-axis', 'aria-hidden': 'true' }, h('span', { text: '-' + secs + ' s' }),
      h('span', { text: data.unit === 'pct_from_pre_move' ? '0 % = price before the move' : '0 % = window start' }), h('span', { text: 'now' }));
    var label = 'Price change over the last ' + secs + ' seconds: ' + data.series.map(function (s) {
      return exchangeLabel(s.exchange) + ' ' + fmtPct(s.points[s.points.length - 1][1]);
    }).join(', ');
    return h('div', { class: 'chart-wrap', role: 'img', 'aria-label': label }, plot, axis);
  }

  function chartParts(o) {
    var ready = chartCache.id === o.id && chartCache.seconds === state.chartSeconds && chartCache.data;
    var frame = h('div', { class: 'chart-frame' });
    var legend = h('div', { class: 'chart-legend' });
    if (!ready) {
      frame.appendChild(h('div', { class: 'chart-empty', text: 'Loading real price history…' }));
    } else if (!chartCache.data.has_data) {
      frame.appendChild(h('div', { class: 'chart-empty', text: 'Not enough real price points in this window yet. The chart shows only measured prices; nothing is drawn without data.' }));
    } else {
      frame.appendChild(chartPlot(chartCache.data));
      seriesOrder(chartCache.data.series).forEach(function (s) {
        var last = s.points[s.points.length - 1][1];
        legend.appendChild(h('span', { class: 'chart-chip ' + s.role },
          h('span', { class: 'badge', 'aria-hidden': 'true', text: initials(s.exchange) }),
          h('span', { class: 'cn', text: exchangeLabel(s.exchange) + (s.role === 'target' ? ' (target)' : '') }),
          h('span', { class: 'cv ' + (last > 0 ? 'pos' : last < 0 ? 'neg' : 'muted'), text: fmtPct(last) })));
      });
    }
    return { frame: frame, legend: legend };
  }

  function renderLagChart(o) {
    // aria-live off: the chart refreshes every 2 s and must not be re-announced inside the polite best panel.
    var wrap = h('div', { class: 'lag-chart', 'data-id': o.id, 'aria-live': 'off' });
    var toggle = h('div', { class: 'chart-toggle', role: 'group', 'aria-label': 'Chart window' });
    CHART_WINDOWS.forEach(function (sec) {
      toggle.appendChild(h('button', {
        class: 'tf', type: 'button', text: sec + 's', 'data-sec': String(sec), 'aria-pressed': state.chartSeconds === sec ? 'true' : 'false',
        onclick: function () { setChartSeconds(sec); }
      }));
    });
    var parts = chartParts(o);
    wrap.appendChild(h('div', { class: 'chart-head' }, parts.legend, toggle));
    wrap.appendChild(parts.frame);
    return wrap;
  }

  // ------------------------------------------------------------------ hero badge (live status, real numbers)
  function renderHeroBadge() {
    var el = $('hero-badge');
    if (!el) return;
    var names = Object.keys(state.health);
    var up = names.filter(function (n) { var x = state.health[n]; return x.ws_connected || x.polling; }).length;
    var active = state.opps.size;
    var ok = state.connected && up > 0;
    var text = !state.connected ? 'Reconnecting to the terminal…'
      : !names.length ? 'Starting: waiting for exchanges'
      : 'Live monitoring · ' + up + '/' + names.length + ' exchanges · ' + active + (active === 1 ? ' opportunity' : ' opportunities');
    if (el.textContent !== text) el.textContent = text;
    var badge = el.parentNode;
    if (badge && badge.classList) badge.classList.toggle('off', !ok);
    var dot = badge && badge.querySelector('.dot');
    if (dot) dot.className = 'dot ' + (ok ? 'green' : 'red');
  }

  function renderCards() {
    var box = $('cards');
    if (!uiMemo.loaded) {
      renderSkeleton(box);
      $('cards-empty').classList.add('hidden');
      $('opps-count').textContent = String(state.opps.size);
      return;
    }
    var list = visibleOpps();
    reconcileCards(box, list);
    renderEmpty(list.length);
    $('opps-count').textContent = String(state.opps.size);
    renderHeroBadge();
  }

  function liveAfter(node) {
    var n = node ? node.nextSibling : null;
    while (n && n.classList && n.classList.contains('card-exit')) n = n.nextSibling;
    return n;
  }

  function reconcileCards(box, list) {
    // Keyed by data-id: new cards fade+rise in, gone cards fade+collapse out, updated cards are swapped in place.
    var existing = {};
    Array.prototype.slice.call(box.children).forEach(function (el) {
      if (el.classList.contains('skeleton')) { box.removeChild(el); return; }
      if (el.classList.contains('card-exit')) return;
      if (el.dataset.id) existing[el.dataset.id] = el;
    });
    var keep = {};
    var cursor = null;
    list.forEach(function (o) {
      var fresh = renderCard(o, {});
      var old = existing[o.id];
      keep[o.id] = true;
      if (old) {
        flashChanged(old, fresh, '.card-net-value, .kv-v');
        markLightChanges(old, fresh);
        box.replaceChild(fresh, old);
      } else {
        var ghost = box.querySelector('.card-exit[data-id="' + CSS.escape(o.id) + '"]');
        if (ghost) box.removeChild(ghost);
        fresh.classList.add('card-enter');
        box.appendChild(fresh);
      }
      var expectedHere = cursor ? liveAfter(cursor) : liveAfter({ nextSibling: box.firstChild });
      if (expectedHere !== fresh) box.insertBefore(fresh, cursor ? cursor.nextSibling : box.firstChild);
      cursor = fresh;
    });
    Object.keys(existing).forEach(function (id) {
      if (keep[id]) return;
      var el = existing[id];
      el.classList.add('card-exit');
      el.setAttribute('aria-hidden', 'true');
      removeAfter(el, 240);
    });
  }

  function renderSkeleton(box) {
    if (box.querySelector('.skeleton')) return;
    clear(box);
    for (var i = 0; i < 3; i++) {
      box.appendChild(h('article', { class: 'card skeleton', 'aria-hidden': 'true' },
        h('div', { class: 'card-head' }, h('span', { class: 'sk w40' })),
        h('span', { class: 'sk tall' }),
        h('div', { class: 'card-grid' }, h('span', { class: 'sk w80' }), h('span', { class: 'sk w80' }), h('span', { class: 'sk w60' }), h('span', { class: 'sk w60' })),
        h('span', { class: 'sk w100' }),
        h('span', { class: 'sk w80' }),
        h('div', { class: 'card-actions' }, h('span', { class: 'sk btn' }), h('span', { class: 'sk btn' }))));
    }
  }

  function renderEmpty(visible) {
    var box = $('cards-empty');
    box.classList.toggle('hidden', visible > 0);
    if (visible > 0) return;
    var title = box.querySelector('[data-role="empty-title"]');
    var sub = box.querySelector('[data-role="empty-sub"]');
    if (!title || !sub) return;
    var n = Object.keys(state.health).length;
    var active = state.opps.size;
    if (state.filters.quick === 'EXPIRED') {
      title.textContent = 'No expired opportunities loaded.';
      sub.textContent = 'History fills in as windows close; every expired card keeps its WHY and whether the target caught up.';
    } else if (active === 0) {
      title.textContent = 'Watching ' + (n ? n + ' exchanges' : 'exchanges') + ' · no executable lag right now';
      sub.textContent = (state.settings && state.settings.simple_mode
        ? 'Simple mode: the coin moves on its most liquid venue while your venue has not moved yet. '
        : 'Signals need ≥2 venues to move while a target lags. ') + 'Every card is a measured lag: no data, no signal — nothing is invented to fill the grid.';
    } else if (!state.filters.lowq && lowQualityCount() > 0) {
      var lq = lowQualityCount();
      title.textContent = 'Nothing proven right now - the bot is training.';
      sub.textContent = lq + ' active opportunit' + (lq === 1 ? 'y is' : 'ies are') + ' on pairs that have not proven themselves yet (or below quality); they are traded virtually so the bot learns. Tick "Show training / low quality" to see ' + (lq === 1 ? 'it' : 'them') + '.';
    } else {
      title.textContent = 'No opportunities match the current filters.';
      sub.textContent = active + ' active opportunit' + (active === 1 ? 'y is' : 'ies are') + ' hidden by the filters — press Reset to see everything.';
    }
  }

  function lowQualityCount() {
    var n = 0;
    state.opps.forEach(function (o) { if (!isTerminal(o.state) && !qualityPassed(o)) n++; });
    return n;
  }

  function kv(k, v, cls) {
    return h('div', { class: 'kv' }, h('span', { class: 'kv-k', text: k }), h('span', { class: 'kv-v ' + (cls || ''), text: v, title: v }));
  }

  function renderCard(o, opts) {
    var term = isTerminal(o.state);
    var exec = isExecuting(o.state);
    var q = quoteOf(o);
    var econ = o.econ || {};
    var human = o.human || {};
    var bot = o.bot || {};
    var hl = lightInfo(human.light);
    var bl = lightInfo(bot.light);
    var net = netOf(o);
    var rem = remaining(o);
    var est = num(o.estimated_duration_s);
    var profit = num(econ.net_quote);
    var side = o.side || (o.direction > 0 ? 'LONG' : 'SHORT');

    var card = h('article', {
      class: 'card' + (term ? ' terminal' : '') + (exec ? ' executing' : '') + (opts.best ? ' best' : ''),
      'data-id': o.id, tabindex: '-1', 'aria-label': pairName(o) + ' ' + side + ' on ' + exchangeLabel(o.target_exchange)
    });

    card.appendChild(h('div', { class: 'card-head' },
      h('span', { class: 'card-pair', text: pairName(o), title: pairName(o) }),
      h('span', { class: 'card-side ' + (side === 'LONG' ? 'long' : 'short'), text: side }),
      h('span', { class: 'tag ' + (o.mode === 'live' ? 'live' : 'paper'), text: String(o.mode || '').toUpperCase() }),
      h('span', { class: 'state-badge', text: String(o.state || '').replace(/_/g, ' ') }),
      qualityBadge(o),
      provenBadge(o),
      h('span', { class: 'card-target', title: 'target exchange: ' + exchangeLabel(o.target_exchange) }, 'TARGET: ', h('b', { text: exchangeLabel(o.target_exchange) }))));

    card.appendChild(h('div', { class: 'card-net' },
      h('span', { class: 'card-net-label', text: 'NET PROFIT' }),
      h('span', { class: 'card-net-value ' + (net >= 0 ? 'pos' : 'neg'), text: fmtPct(net) }),
      Number.isFinite(num(o.max_net_pct)) && num(o.max_net_pct) > net + 0.005
        ? h('span', { class: 'muted', text: 'peak ' + fmtPct(o.max_net_pct) }) : null));

    if (opts.best) card.appendChild(renderLagChart(o));

    card.appendChild(h('div', { class: 'card-grid' },
      kv('BUY', fmtMoney(o.buy_price, q)),
      kv('SELL', fmtMoney(o.sell_price, q)),
      kv('CAPITAL', fmtMoney(econ.executable_quote, q) + (econ.liquidity_ok === false ? ' (thin book)' : '')),
      kv('EXPECTED PROFIT', fmtMoney(profit, q, { sign: true }), profit >= 0 ? 'pos' : 'neg')));

    var math = mathLine(o);
    if (math) card.appendChild(math);
    var bs = bestSizeLine(o);
    if (bs) card.appendChild(bs);
    var levLine = leverageLine(o);
    if (levLine) card.appendChild(levLine);

    if (term) {
      var dur = num(o.actual_duration_s);
      card.appendChild(h('div', { class: 'window' },
        h('div', { class: 'window-row' },
          h('span', { class: 'kv-k', text: o.state + (Number.isFinite(dur) ? ' after ' + fmtSecs(dur) : '') }),
          h('span', { class: 'muted', text: o.expire_reason || '', title: o.expire_reason || '' }))));
    } else {
      var frac = est > 0 ? Math.max(0, Math.min(1, rem / est)) : 0;
      // Same rule as tick(): a card swapped in by opp.update must not lose its
      // urgent state (and restart the bar pulse) until the next 250 ms tick.
      var urgent = rem > 0 && rem < 3;
      card.appendChild(h('div', { class: 'window' },
        h('div', { class: 'window-row' },
          h('span', { class: 'kv-k', text: 'OPPORTUNITY WINDOW' }),
          h('span', { class: 'kv-v' + (urgent ? ' urgent' : ''), 'data-role': 'window-val',
            text: rem > 0 ? '~' + rem.toFixed(1) + ' sec' : '0.0 sec · expiring' })),
        h('div', { class: 'bar', role: 'progressbar', 'aria-valuemin': '0', 'aria-valuemax': '100', 'aria-valuenow': String(Math.round(frac * 100)) },
          h('div', { class: 'bar-fill ' + barClass(rem, o) + (urgent ? ' urgent' : ''), 'data-role': 'bar', style: 'width:' + (frac * 100).toFixed(1) + '%' }))));
    }

    var humanParts = [fmtSecs(human.est_execution_s, 0) + ' execution'];
    humanParts.push(hl.cls === 'green' ? 'low risk' : hl.cls === 'yellow' ? 'high risk' : (human.label || 'NOT EXECUTABLE'));
    if (!term) humanParts.push('~' + fmtSecs(rem, 0) + ' window');
    var hn = num(human.stats_n);
    var hr = num(human.stats_rate);
    if (hn >= 5 && Number.isFinite(hr)) humanParts.push('by stats ' + Math.round(hr * hn) + '/' + hn + ' made it');
    card.appendChild(h('div', { class: 'line light ' + hl.cls, 'data-light': hl.cls },
      h('span', { class: 'dot ' + hl.cls, 'aria-hidden': 'true' }),
      h('span', { class: 'line-k', text: 'HUMAN' }),
      h('span', { text: humanParts.join(' / ') })));

    var botLine = h('div', { class: 'line light ' + bl.cls, 'data-light': bl.cls },
      h('span', { class: 'dot ' + bl.cls, 'aria-hidden': 'true' }),
      h('span', { class: 'line-k', text: 'BOT' }),
      h('span', { text: (bot.label || bl.word) + ' estimated execution: ' + fmtSecs(bot.est_execution_s) }));
    if (bl.cls !== 'green' && Array.isArray(bot.reasons) && bot.reasons.length) {
      botLine.appendChild(h('span', { class: 'reason', text: '· ' + bot.reasons[0], title: bot.reasons[0] }));
    }
    card.appendChild(botLine);

    // The best-opportunity panel stays compact; the full PLAN and WHY live on the card in the list below.
    var planEl = opts.best ? null : renderPlan(o);
    if (planEl) card.appendChild(planEl);
    if (!opts.best) card.appendChild(renderWhy(o));
    card.appendChild(renderFoot(o, term, exec));
    return card;
  }

  function qualityBadge(o) {
    var q = o.quality;
    if (!q || q.score == null) return null;
    var info = q.enforced === false;
    var cls = 'q-badge grade-' + String(q.grade || 'D').toLowerCase() + (q.passed === false && !info ? ' failed' : '');
    var title = (info ? 'simple mode: information only, does not block' : (q.passed === false ? 'below the quality threshold' : 'passed the quality check')) + (q.summary ? ': ' + q.summary : '');
    return h('span', { class: cls, title: title, text: 'Quality ' + q.score + ' · ' + (q.grade || '?') + (info ? ' (info)' : '') });
  }

  // The whole trade in one line: how far the target lags, how much of it it usually catches up,
  // what the venue takes (fees + execution) and what is left.
  function mathLine(o) {
    var sig = o.signal || {}, econ = o.econ || {};
    var lag = num(sig.lag_pct), cu = num(sig.expected_catchup_pct), ratio = num(sig.catchup_ratio), cost = num(econ.total_cost_pct);
    if (!Number.isFinite(lag) || !Number.isFinite(cost)) return null;
    var parts = [exchangeLabel(o.first_mover) + ' moved, lag ' + fmtPct(lag)];
    if (Number.isFinite(cu)) parts.push('expected catch-up ' + fmtPct(cu) + (Number.isFinite(ratio) ? ' (x' + ratio.toFixed(2) + ')' : ''));
    parts.push('fees + execution ' + cost.toFixed(2) + '%');
    parts.push('NET ' + fmtPct(num(econ.net_pct)));
    return h('div', { class: 'math-line' }, parts.join(' · '));
  }

  function leverageLine(o) {
    var st = state.settings || {};
    if (st.simple_mode && st.ride_enabled) {  // the ride exit rules decide, not the leverage plan's stop
      return h('div', { class: 'lev-line' }, 'Exit by ride rules · stop NET -' + num(st.ride_stop_pct).toFixed(2) +
        '% · leader turned -' + num(st.ride_reversal_pct).toFixed(2) + '% · spot venue, no leverage');
    }
    var lv = o.leverage;
    if (!lv || lv.recommended == null) return null;
    var parts = [];
    var rec = num(lv.recommended), sig = num(lv.max_by_signal);
    parts.push('Leverage ' + (rec > 1 ? rec + 'x' : '1x') + (lv.venue_kind === 'spot' ? ' (spot venue)' : ''));
    if (sig > 1 && sig > rec) parts.push('signal allows up to ' + sig + 'x');
    if (Number.isFinite(num(lv.stop_pct))) parts.push('stop -' + num(lv.stop_pct).toFixed(2) + '%');
    if (Number.isFinite(num(lv.liq_distance_pct)) && rec > 1) parts.push('liquidation -' + num(lv.liq_distance_pct).toFixed(1) + '%');
    if (Number.isFinite(num(lv.rr))) parts.push('R:R ' + num(lv.rr).toFixed(1));
    return h('div', { class: 'lev-line', title: (lv.reasons || []).join('; ') || lv.note || '' }, parts.join(' · '));
  }

  function qualityDetails(o) {
    var q = o.quality;
    if (!q || q.score == null) return null;
    var box = h('div', { class: 'q-details' });
    box.appendChild(h('div', { class: 'q-summary', text: q.summary || ('Quality ' + q.score + '/100 (' + (q.grade || '?') + ')') }));
    (q.vetoes || []).forEach(function (v) { box.appendChild(h('div', { class: 'q-veto', text: 'Block: ' + v })); });
    var list = h('ul', { class: 'q-factors' });
    (q.components || []).forEach(function (c) {
      list.appendChild(h('li', { class: c.available ? '' : 'na', text: c.label + ': ' + (c.available ? Math.round(num(c.value) * 100) + '/100' : 'no data') + (c.detail ? ' - ' + c.detail : '') }));
    });
    (q.factors || []).forEach(function (f) {
      var sc = num(f.score);
      var tag = !f.available ? 'no data' : f.veto ? 'BLOCK' : sc > 0.05 ? 'for' : sc < -0.05 ? 'against' : 'neutral';
      list.appendChild(h('li', { class: !f.available ? 'na' : sc > 0.05 ? 'pos' : sc < -0.05 ? 'neg' : '', text: f.label + ' [' + tag + ']' + (f.detail ? ' - ' + f.detail : '') }));
    });
    box.appendChild(list);
    return box;
  }

  // ------------------------------------------------------------------ MARKET tab (context behind every signal)
  function loadMarket() {
    return api('GET', '/api/context').then(function (r) {
      if (!r.ok) { toast('Market: ' + r.error, 'err'); return; }
      renderMarket(r.data || {});
    });
  }

  function fmtCountdown(sec) {
    var s = Math.abs(Math.round(num(sec)));
    if (!Number.isFinite(s)) return '—';
    if (s < 90) return s + ' s';
    if (s < 5400) return Math.round(s / 60) + ' min';
    if (s < 172800) { var m = Math.floor(s / 60); return Math.floor(m / 60) + ' h ' + (m % 60) + ' min'; }
    return Math.floor(s / 86400) + ' d';
  }

  function renderMarket(d) {
    var st = $('market-status');
    clear(st);
    if (!d.enabled) {
      st.appendChild(h('p', { class: 'why-line', text: d.reason || 'Market context is off.' }));
    } else {
      st.appendChild(h('p', { class: 'why-line muted', text: 'Updated ' + fmtTime(d.ts) + ' (refreshes every 30 s on this tab).' }));
      st.appendChild(h('p', { class: 'why-line', text: d.event_window
        ? 'High-impact US release window: signals need a higher quality score and leverage is reduced.'
        : 'No high-impact US release window right now.' }));
    }
    var macro = $('market-macro');
    clear(macro);
    var next = (d.macro && d.macro.next) || [];
    if (!next.length) macro.appendChild(h('tr', null, h('td', { class: 'muted', text: d.enabled ? 'No upcoming releases in the calendar (or no data yet).' : 'Off.' })));
    next.forEach(function (e) {
      var prof = e.btc_profile && e.btc_profile['60'];
      var hist = prof ? ('BTC 60 s: up ' + Math.round(num(prof.up_share) * 100) + '% of ' + prof.n) : 'no history yet';
      macro.appendChild(h('tr', null,
        h('td', { text: e.title + (e.impact === 'high' ? '' : ' (medium)') }),
        h('td', { class: 'muted', text: 'forecast ' + (e.forecast || '—') + ' · prev ' + (e.previous || '—') + ' · ' + hist }),
        h('td', { text: 'in ' + fmtCountdown(num(e.ts) - Date.now() / 1000) })));
    });
    var reg = $('market-regime');
    clear(reg);
    var rg = d.regime || {};
    ['BTC', 'ETH'].forEach(function (a) {
      var x = rg[a] || {};
      var txt = !d.enabled ? 'Off.'
        : Number.isFinite(num(x.ret_15m_pct)) && x.fresh !== false ? '15 min ' + fmtPct(x.ret_15m_pct) + ' · 1 h ' + fmtPct(x.ret_1h_pct) + ' · vol pct ' + (Number.isFinite(num(x.vol_pctile)) ? Math.round(num(x.vol_pctile) * 100) : '—')
        : num(x.minutes) > 0 ? 'minute data stale (' + x.minutes + ' bars, last ' + (x.last_minute_ts ? fmtTime(x.last_minute_ts) : '—') + ')'
        : 'no minute data yet';
      reg.appendChild(h('tr', null, h('td', { text: a }), h('td', { class: 'wrap', text: txt })));
    });
    var tf = d.tradfi && d.tradfi.quotes ? d.tradfi.quotes : (d.tradfi && d.tradfi.symbols) || null;
    var quotes = [];
    if (tf && typeof tf === 'object') Object.keys(tf).forEach(function (k) { if (tf[k] && typeof tf[k] === 'object') quotes.push(tf[k]); });
    quotes.forEach(function (q) {
      if (q.last == null) return;
      var chg = q.is_yield && q.chg_1d_bp != null ? (num(q.chg_1d_bp) >= 0 ? '+' : '') + num(q.chg_1d_bp).toFixed(1) + ' bp today' : fmtPct(q.chg_1d_pct) + ' today';
      reg.appendChild(h('tr', null, h('td', { text: q.key || q.symbol }), h('td', { text: String(q.last) + ' · ' + chg + ' · ' + (q.market_open ? 'open' : 'closed') + ', delayed' })));
    });
    if (rg.corr_btc_spx != null) reg.appendChild(h('tr', null, h('td', { text: 'corr BTC / S&P' }), h('td', { text: num(rg.corr_btc_spx).toFixed(2) + ' (4 h, 1 min)' })));
    var news = $('market-news');
    clear(news);
    var items = (d.news && d.news.items) || [];
    if (!items.length) news.appendChild(h('tr', null, h('td', { class: 'muted', colspan: '7', text: d.enabled ? 'No news yet (first poll in progress or sources unavailable).' : 'Off.' })));
    items.slice(0, 50).forEach(function (it) {
      var dir = num(it.direction);
      news.appendChild(h('tr', null,
        h('td', { class: 'mono', text: fmtTime(it.ts) }),
        h('td', { text: it.channel || it.source }),
        h('td', { class: 'wrap', text: it.title }),
        h('td', { text: it.category }),
        h('td', { text: (it.assets || []).map(function (a) { return a === '*' ? 'market' : a; }).join(', ') }),
        h('td', { class: dir > 0 ? 'pos' : dir < 0 ? 'neg' : 'muted', text: dir > 0 ? 'bullish' : dir < 0 ? 'bearish' : 'unclear' }),
        h('td', { class: 'num', text: Math.round(num(it.importance) * 100) + '%' })));
    });
    var der = $('market-derivs');
    clear(der);
    var rows = (d.derivs && d.derivs.assets) || [];
    if (!rows.length) der.appendChild(h('tr', null, h('td', { class: 'muted', colspan: '7', text: d.enabled ? 'No derivatives data yet.' : 'Off.' })));
    rows.forEach(function (x) {
      var fr = num(x.funding_rate_8h);
      der.appendChild(h('tr', null,
        h('td', { text: x.asset }),
        h('td', { class: 'num', text: Number.isFinite(fr) ? (fr * 100).toFixed(4) + '%' : '—' }),
        h('td', { class: 'num', text: x.funding_pct != null ? Math.round(num(x.funding_pct)) + '' : (x.funding_n != null ? 'history ' + x.funding_n + '/' + (x.funding_min_n || 288) : '—') }),
        h('td', { class: 'num', text: pctOrDash(x.oi_chg_15m_pct) }),
        h('td', { class: 'num', text: x.liq_long_usd_5m == null ? '—' : fmtMoney(x.liq_long_usd_5m, 'USD') }),
        h('td', { class: 'num', text: x.liq_short_usd_5m == null ? '—' : fmtMoney(x.liq_short_usd_5m, 'USD') }),
        h('td', { class: 'muted', text: x.reason || '' })));
    });
    var pool = $('market-pool');
    clear(pool);
    var p = d.pool || {};
    if (!p || p.enabled === false) pool.appendChild(h('tr', null, h('td', { class: 'muted', text: 'Pool is off: every listed coin is scanned.' })));
    else {
      pool.appendChild(h('tr', null, h('td', { text: 'In the pool (' + (p.size || 0) + ')' }), h('td', { class: 'wrap', text: (p.included || []).map(function (x) { return x.asset; }).join(', ') })));
      (p.excluded || []).slice(0, 25).forEach(function (x) {
        pool.appendChild(h('tr', null, h('td', { text: x.asset }), h('td', { class: 'wrap muted', text: (x.reasons || []).join('; ') })));
      });
    }
    var src = $('market-sources');
    clear(src);
    (d.sources || []).forEach(function (x) {
      src.appendChild(h('tr', null,
        h('td', { text: x.name }),
        h('td', { class: x.ok ? 'diag-ok' : 'diag-bad', text: x.ok ? 'ok' : x.enabled === false ? 'off' : x.last_error ? (x.last_ok_ts ? 'error (has older data)' : 'error') : 'no data' }),
        h('td', { class: 'wrap muted', text: x.last_error || x.note || '' })));
    });
    if (!(d.sources || []).length) src.appendChild(h('tr', null, h('td', { class: 'muted', text: d.enabled ? 'Starting…' : 'Off.' })));
  }

  var PLAN_ORDER = ['header', 'movers', 'buy', 'sell', 'timing', 'caveat', 'updated'];
  function renderPlan(o) {
    var plan = o && o.plan;
    if (!plan || typeof plan !== 'object') return null;
    var box = h('div', { class: 'plan' });
    var n = 0;
    PLAN_ORDER.forEach(function (k) {
      var t = plan[k];
      if (!t) return;
      n++;
      box.appendChild(h('div', { class: 'plan-line plan-' + k, text: String(t), title: String(t) }));
    });
    return n ? box : null;
  }

  function renderWhy(o) {
    var details = h('details', { class: 'why', open: state.openWhy.has(o.id) });
    details.addEventListener('toggle', function () {
      if (details.open) state.openWhy.add(o.id); else state.openWhy.delete(o.id);
    });
    details.appendChild(h('summary', { text: 'WHY?' }));
    details.appendChild(h('pre', { text: o.why || 'no explanation recorded' }));
    var qd = qualityDetails(o);
    if (qd) details.appendChild(qd);
    var reasons = [];
    (o.human && o.human.reasons || []).forEach(function (r) { reasons.push('HUMAN: ' + r); });
    (o.bot && o.bot.reasons || []).forEach(function (r) { reasons.push('BOT: ' + r); });
    if (o.econ && o.econ.liquidity_ok === false && o.econ.liquidity_reason) reasons.push('LIQUIDITY: ' + o.econ.liquidity_reason);
    if (reasons.length) details.appendChild(h('ul', null, reasons.map(function (r) { return h('li', { text: r }); })));
    var sig = o.signal || {};
    var meta = [];
    if (sig.first_mover) meta.push('first mover: ' + exchangeLabel(sig.first_mover) + ' ' + fmtPct(sig.first_move_pct));
    if (Number.isFinite(num(sig.lag_pct))) meta.push('lag ' + fmtPct(sig.lag_pct));
    if (Number.isFinite(num(sig.movers_count))) meta.push('movers ' + fmtInt(sig.movers_count));
    if (Number.isFinite(num(o.econ && o.econ.total_cost_pct))) meta.push('costs ' + fmtPct(o.econ.total_cost_pct));
    meta.push('updates ' + fmtInt(o.update_count));
    meta.push('id ' + o.id);
    details.appendChild(h('div', { class: 'card-meta' }, meta.map(function (m) { return h('span', { text: m }); })));
    return details;
  }

  function renderFoot(o, term, exec) {
    var foot = h('div', { class: 'card-foot' });
    var url = safeUrl(o.pair_url);
    if (url) {
      foot.appendChild(h('a', { class: 'link', href: url, target: '_blank', rel: 'noopener noreferrer', text: 'Open on ' + exchangeLabel(o.target_exchange) + '' }));
    }
    foot.appendChild(h('span', { class: 'card-meta' }, h('span', { text: 'seen ' + fmtTime(o.detected_ts) })));
    var manualMine = state.manual && state.manual.id === o.id && state.manual.phase !== 'done';
    // An open MANUAL position this tab does not own (page reload, another tab, WS reconnect): the exit must stay reportable.
    var manualOpen = !manualMine && hasOpenManualPosition(o);
    var humanRed = lightOf(o, 'human') === 'red';
    var botRed = lightOf(o, 'bot') === 'red';
    var manualDisabled = term || (exec && !manualMine && !manualOpen) || (humanRed && !manualMine && !manualOpen);
    var autoDisabled = term || exec || botRed || state.emergency || manualMine || !qualityPassed(o);
    var manualBtn = h('button', {
      class: 'btn btn-manual', type: 'button', disabled: manualDisabled,
      title: term ? 'opportunity is over' : manualOpen ? 'открытая ручная сделка: вернуться к сопровождению' : humanRed ? 'too fast for a human' : exec ? 'already executing' : 'зайти в сделку вручную: бот проведёт по шагам',
      text: manualMine || manualOpen ? 'Сделка' : 'Я ЗАШЁЛ',
      onclick: function () { openManual(o.id); }
    });
    var autoBtn = h('button', {
      class: 'btn btn-auto', type: 'button', disabled: autoDisabled,
      title: state.emergency ? 'EMERGENCY STOP active' : term ? 'opportunity is over' : !provenOk(o) ? 'pair not proven yet: AUTO is blocked' : !qualityPassed(o) ? 'below the quality threshold: AUTO is blocked' : botRed ? 'bot cannot execute this one' : exec ? 'already executing' : 'let the bot execute (revalidated first)',
      text: 'AUTO', onclick: function () { openAuto(o.id); }
    });
    foot.appendChild(h('div', { class: 'card-actions' }, manualBtn, autoBtn));
    return foot;
  }

  function renderTrades() {
    var body = $('trades-body');
    clear(body);
    var list = Array.from(state.trades.values()).sort(function (a, b) { return num(b.started_ts) - num(a.started_ts); });
    $('trades-count').textContent = String(list.length);
    $('trades-empty').classList.toggle('hidden', list.length > 0);
    list.forEach(function (t) {
      var q = quoteOf({ target_symbol: t.symbol });
      var pnl = num(t.realized_pnl_quote);
      var stateCell = h('td', null, h('span', { class: 'state-badge', text: t.state || '' }));
      if (isExecuting(t.state) && !t.entry_order && t.opportunity_id) {
        // Open MANUAL position (no entry order = bought by hand): second entry point to report the exit.
        stateCell.appendChild(h('button', {
          class: 'btn btn-manual btn-sm', type: 'button', text: 'Сделка', title: 'открытая ручная сделка: вернуться к сопровождению',
          onclick: function () { openManual(t.opportunity_id); }
        }));
      }
      var tr = h('tr', null,
        h('td', { class: 'mono', text: fmtTime(t.started_ts) }),
        h('td', null, h('span', { class: 'tag ' + (t.mode === 'live' ? 'live' : 'paper'), text: String(t.mode || '').toUpperCase() })),
        h('td', { text: (t.asset || '') + ' @ ' + exchangeLabel(t.exchange) }),
        h('td', null, h('span', { class: 'card-side ' + (t.side === 'SHORT' ? 'short' : 'long'), text: t.side || (t.direction > 0 ? 'LONG' : 'SHORT') })),
        h('td', { class: 'num', text: fmtMoney(t.capital_quote, q) }),
        h('td', { class: 'num', text: fmtPct(t.expected_net_pct) }),
        h('td', { class: 'num ' + (num(t.actual_net_pct) >= 0 ? 'pos' : 'neg'), text: fmtPct(t.actual_net_pct) }),
        h('td', { class: 'num', text: fmtMoney(t.expected_profit_quote, q, { sign: true }) }),
        h('td', { class: 'num ' + (pnl > 0 ? 'pos' : pnl < 0 ? 'neg' : ''), text: fmtMoney(t.realized_pnl_quote, q, { sign: true }) }),
        h('td', { class: 'num', text: fmtPct(t.actual_slippage_pct) }),
        h('td', { class: 'num', text: fmtSecs(t.execution_time_s) }),
        stateCell,
        h('td', { class: 'wrap', text: t.diff_reason || t.error || '' }),
        journalCell(t));
      body.appendChild(tr);
    });
  }

  function journalCell(t) {
    var j = t.journal || { note: '', screenshots: 0, analyzed: 0 };
    var parts = [];
    if (j.note) parts.push('note');
    if (j.screenshots) parts.push(j.screenshots + ' shot' + (j.screenshots > 1 ? 's' : ''));
    if (j.analyzed) parts.push(j.analyzed + ' reviewed');
    var td = h('td', null, h('button', {
      class: 'btn btn-ghost btn-sm', type: 'button', text: 'JOURNAL', title: 'note, screenshot, review',
      onclick: function () { openJournal(t.id); }
    }));
    if (parts.length) td.appendChild(h('div', { class: 'journal-badge', text: parts.join(' · ') }));
    return td;
  }

  // ------------------------------------------------------------------ trade journal
  function openJournal(tradeId) {
    state.journal = { tradeId: tradeId, data: null };
    var t = state.trades.get(tradeId);
    $('journal-trade').textContent = t ? (t.asset + ' @ ' + exchangeLabel(t.exchange) + ' · ' + (t.state || '') + ' · ' + fmtTime(t.started_ts)) : tradeId;
    $('journal-note').value = '';
    $('journal-note-status').textContent = '';
    clear($('journal-shots'));
    showModal('journal');
    api('GET', '/api/trades/' + encodeURIComponent(tradeId) + '/journal').then(function (r) {
      if (!r.ok) { toast('Journal: ' + r.error, 'err'); return; }
      state.journal.data = r.data;
      $('journal-note').value = r.data.note || '';
      renderJournal();
    });
  }

  function closeJournal() { hideModal('journal'); state.journal = null; }

  function renderJournal() {
    var j = state.journal && state.journal.data;
    if (!j) return;
    var hint = $('journal-analysis-note');
    hint.textContent = j.analysis_available
      ? 'ANALYZE sends the screenshot and the trade facts to Claude (' + (j.analysis_model || '') + ').'
      : 'ANALYZE: NOT AVAILABLE — add ANTHROPIC_API_KEY to .env to enable screenshot review.';
    var box = $('journal-shots');
    clear(box);
    (j.screenshots || []).forEach(function (s) {
      var card = h('div', { class: 'journal-shot' });
      card.appendChild(h('img', { src: s.url, alt: 'screenshot ' + s.id, loading: 'lazy' }));
      var row = h('div', { class: 'journal-row' });
      row.appendChild(h('span', { class: 'journal-badge', text: '#' + s.id + ' · ' + fmtTime(s.created_ts) + (s.analysis_model ? ' · ' + s.analysis_model : '') }));
      var btn = h('button', {
        class: 'btn btn-sm', type: 'button', text: s.analysis ? 'RE-ANALYZE' : 'ANALYZE',
        title: j.analysis_available ? 'ask Claude to review this screenshot' : 'not available without ANTHROPIC_API_KEY'
      });
      if (!j.analysis_available) btn.disabled = true;
      btn.addEventListener('click', function () { analyzeShot(s.id, btn); });
      row.appendChild(btn);
      card.appendChild(row);
      if (s.analysis) card.appendChild(h('p', { class: 'journal-analysis', text: s.analysis }));
      box.appendChild(card);
    });
    if (!(j.screenshots || []).length) box.appendChild(h('p', { class: 'muted', text: 'No screenshots yet.' }));
  }

  function saveJournalNote() {
    var jr = state.journal;
    if (!jr) return;
    var note = $('journal-note').value;
    $('journal-note-status').textContent = 'saving…';
    api('POST', '/api/trades/' + encodeURIComponent(jr.tradeId) + '/note', { note: note }).then(function (r) {
      if (!r.ok) { $('journal-note-status').textContent = ''; toast('Note: ' + r.error, 'err'); return; }
      $('journal-note-status').textContent = 'saved';
      var t = state.trades.get(jr.tradeId);
      if (t) { t.journal = t.journal || { note: '', screenshots: 0, analyzed: 0 }; t.journal.note = r.data.note; renderTrades(); }
    });
  }

  function uploadJournalShot() {
    var jr = state.journal;
    var input = $('journal-file');
    if (!jr || !input.files || !input.files[0]) { toast('Choose a screenshot file first', 'warn'); return; }
    var fd = new FormData();
    fd.append('file', input.files[0]);
    var btn = $('journal-upload');
    btn.disabled = true;
    fetch('/api/trades/' + encodeURIComponent(jr.tradeId) + '/screenshot', { method: 'POST', body: fd }).then(function (res) {
      return res.json().then(function (data) { return { ok: res.ok, data: data }; });
    }).catch(function (e) { return { ok: false, data: { error: String(e) } }; }).then(function (r) {
      btn.disabled = false;
      if (!r.ok) { toast('Upload: ' + ((r.data && r.data.error) || 'failed'), 'err'); return; }
      input.value = '';
      toast('Screenshot uploaded', 'ok');
      var t = state.trades.get(jr.tradeId);
      if (t) { t.journal = t.journal || { note: '', screenshots: 0, analyzed: 0 }; t.journal.screenshots += 1; renderTrades(); }
      openJournal(jr.tradeId);
    });
  }

  function analyzeShot(id, btn) {
    btn.disabled = true;
    btn.textContent = 'ANALYZING…';
    api('POST', '/api/screenshots/' + id + '/analyze').then(function (r) {
      if (!r.ok) { btn.disabled = false; btn.textContent = 'ANALYZE'; toast('Analyze: ' + r.error, 'err'); return; }
      toast('Review ready', 'ok');
      var jr = state.journal;
      if (jr) {
        var t = state.trades.get(jr.tradeId);
        if (t) { t.journal = t.journal || { note: '', screenshots: 0, analyzed: 0 }; t.journal.analyzed += 1; renderTrades(); }
        openJournal(jr.tradeId);
      }
    });
  }

  var STATS_ORDER = ['asset', 'exchange', 'first_mover_frequency', 'first_mover_count', 'lagging_count', 'median_reaction_delay_s',
    'catchup_ratio', 'median_duration_s', 'trades', 'success_rate', 'success_count', 'fail_count', 'false_signal_count',
    'human_success_rate', 'human_window_ok', 'human_window_total', 'catchup_rate', 'caught_up_ok', 'caught_up_total'];

  function renderStats() {
    var rows = Array.isArray(state.stats) ? state.stats : [];
    var head = $('stats-head');
    var body = $('stats-body');
    clear(head);
    clear(body);
    $('stats-empty').classList.toggle('hidden', rows.length > 0);
    if (!rows.length) return;
    var keys = new Set();
    rows.forEach(function (r) { Object.keys(r || {}).forEach(function (k) { if (typeof r[k] !== 'object' || r[k] === null) keys.add(k); }); });
    var cols = STATS_ORDER.filter(function (k) { return keys.has(k); });
    Array.from(keys).sort().forEach(function (k) { if (cols.indexOf(k) < 0) cols.push(k); });
    cols.forEach(function (k) { head.appendChild(h('th', { class: k === 'asset' || k === 'exchange' ? '' : 'num', text: humanize(k) })); });
    rows.forEach(function (r) {
      var tr = h('tr');
      cols.forEach(function (k) {
        var v = r[k];
        var text = k === 'exchange' ? exchangeLabel(String(v)) : fmtAny(v, k);
        tr.appendChild(h('td', { class: typeof v === 'number' ? 'num' : '', text: text }));
      });
      body.appendChild(tr);
    });
  }

  // ------------------------------------------------------------------ tabs / loading
  // Counters + exchange health sit under the hero on the opportunities tab (like the reference's ticker strip)
  // and at the top of every other tab.
  function placeStrips(name) {
    var strips = $('strips');
    if (!strips) return;
    var inHero = name === 'opps';
    var slot = $('strip-slot');
    if (inHero && strips.parentNode !== slot) slot.appendChild(strips);
    else if (!inHero && strips.parentNode === slot) document.body.insertBefore(strips, document.querySelector('main.main'));
  }

  function setTab(name) {
    state.tab = name;
    placeStrips(name);
    if (name === 'opps') setTimeout(function () { refreshChart(true); }, 0);
    document.querySelectorAll('.tab').forEach(function (b) { b.setAttribute('aria-selected', b.dataset.tab === name ? 'true' : 'false'); });
    ['opps', 'market', 'trades', 'stats', 'diag'].forEach(function (n) { $('tab-' + n).classList.toggle('hidden', n !== name); });
    if (name === 'market') loadMarket();
    if (name === 'trades' && !state.tradesLoaded) loadTrades();
    if (name === 'stats' && !state.statsLoaded) loadStats();
    if (name === 'diag') loadDiag();
  }

  function loadTrades() {
    return api('GET', '/api/trades?limit=500').then(function (r) {
      if (!r.ok) { toast('Trades: ' + r.error, 'err'); return; }
      (r.data || []).forEach(function (t) { if (t && t.id) state.trades.set(t.id, t); });
      state.tradesLoaded = true;
      renderTrades();
    });
  }

  // ------------------------------------------------------------------ diagnostics (why no signals)
  var FUNNEL_ORDER = ['exchanges_connected', 'exchanges_enabled', 'assets_scanned', 'ticks_received', 'episodes_detected',
    'candidates', 'rejected_total', 'signals', 'opportunities', 'active_now', 'expired_today', 'alerts',
    'shadow_opened', 'shadow_closed', 'shadow_skipped', 'training_opened', 'training_closed'];
  var FUNNEL_LABELS = {
    exchanges_connected: 'Exchanges connected', exchanges_enabled: 'Exchanges enabled', assets_scanned: 'Assets scanned',
    ticks_received: 'Ticks received', episodes_detected: 'Moves (episodes) detected', candidates: 'Candidates checked',
    rejected_total: 'Candidates rejected', signals: 'Passed filters (signals)', opportunities: 'Approved opportunities',
    active_now: 'Active now', expired_today: 'Expired by TTL today', alerts: 'Telegram alerts sent',
    shadow_opened: 'Shadow trades opened', shadow_closed: 'Shadow trades closed', shadow_skipped: 'Shadow skipped (no executable book)',
    training_opened: 'Training trades opened (rejected signals, virtual)', training_closed: 'Training trades closed'
  };
  var REASON_LABELS = {
    min_movers: 'Only one exchange moved (min_movers)', mover_volume: 'Mover 24h volume too low', mover_volume_unknown: 'Mover 24h volume unknown (no data)', target_no_data: 'Target has no fresh data',
    direction: 'Direction not allowed on target', target_volume: 'Target 24h volume too low', min_lag: 'Lag below min_lag_pct',
    no_fresh_book: 'No fresh target order book', fees: 'Rejected by fees (edge eaten by costs)', below_min_net: 'Edge below min net (even before fees)', liquidity: 'Book below min trade size'
  };

  function loadDiag() {
    return api('GET', '/api/diagnostics').then(function (r) {
      if (!r.ok) { toast('Diagnostics: ' + r.error, 'err'); return; }
      state.diag = r.data || null;
      renderDiag();
    });
  }

  function kvRows(body, pairs) {
    clear(body);
    pairs.forEach(function (p) {
      var tr = h('tr');
      tr.appendChild(h('td', { text: p[0] }));
      var td = h('td', { text: p[1] });
      if (p[2]) td.appendChild(h('div', { class: 'diag-recent', text: p[2] }));
      tr.appendChild(td);
      body.appendChild(tr);
    });
  }

  function renderDiag() {
    var d = state.diag;
    $('diag-empty').classList.toggle('hidden', !!d);
    if (!d) return;
    var why = $('diag-why');
    clear(why);
    (d.why || []).forEach(function (line) { why.appendChild(h('p', { class: 'why-line', text: line })); });
    if (d.error) why.appendChild(h('p', { class: 'why-line diag-bad', text: d.error }));
    var f = d.funnel || {};
    kvRows($('diag-funnel'), FUNNEL_ORDER.filter(function (k) { return k in f; }).map(function (k) { return [FUNNEL_LABELS[k] || humanize(k), String(f[k])]; }));
    var rej = f.rejected || {};
    var recent = d.recent_rejections || {};
    kvRows($('diag-reasons'), Object.keys(rej).map(function (k) {
      return [REASON_LABELS[k] || humanize(k), String(rej[k]), (recent[k] || []).slice(-3).join(', ')];
    }));
    var ex = $('diag-exchanges');
    clear(ex);
    Object.keys(d.exchanges || {}).forEach(function (name) {
      var x = d.exchanges[name];
      var tr = h('tr');
      tr.appendChild(h('td', { text: exchangeLabel(name) }));
      tr.appendChild(h('td', { text: x.role }));
      tr.appendChild(h('td', { class: x.connected ? 'diag-ok' : 'diag-bad', text: x.connected ? (x.stale ? 'stale' : 'yes') : 'NO' }));
      tr.appendChild(h('td', { class: 'num', text: String(x.books) }));
      tr.appendChild(h('td', { class: 'num', text: String(x.tickers) }));
      tr.appendChild(h('td', { class: 'num', text: String(x.trades) }));
      tr.appendChild(h('td', { class: 'num', text: String(x.polled == null ? 0 : x.polled) }));
      tr.appendChild(h('td', { class: 'num' + (x.crossed ? ' diag-bad' : ''), text: String(x.crossed == null ? 0 : x.crossed) }));
      tr.appendChild(h('td', { class: 'num', text: String(x.volumes == null ? '—' : x.volumes) }));
      tr.appendChild(h('td', { class: 'num', text: x.last_data_s == null ? '—' : x.last_data_s + ' s ago' }));
      tr.appendChild(h('td', { class: 'wrap ' + (x.status ? 'diag-bad' : ''), text: x.status || 'ok' }));
      ex.appendChild(tr);
    });
    var th = d.thresholds || {};
    kvRows($('diag-thresholds'), Object.keys(th).map(function (k) { return [k, String(th[k])]; }));
  }

  function loadStats() {
    loadSummaries();
    return api('GET', '/api/stats').then(function (r) {
      if (!r.ok) { toast('Stats: ' + r.error, 'err'); return; }
      state.stats = Array.isArray(r.data) ? r.data : [];
      state.statsLoaded = true;
      renderStats();
    });
  }

  // ------------------------------------------------------------------ trade + shadow statistics
  function pctOrDash(v) { return v == null ? '—' : fmtPct(v); }
  var EXIT_LABELS = { leader_reversed: 'leader turned', caught_up: 'caught up', trailing: 'trailing', stop: 'stop',
    max_hold: 'time limit', target: 'target', time: 'window over', no_data: 'no data' };
  function rateOrDash(v) { return v == null ? '—' : (v * 100).toFixed(0) + '%'; }

  function loadSummaries() {
    api('GET', '/api/trades/summary').then(function (r) {
      if (!r.ok) { toast('Trade stats: ' + r.error, 'err'); return; }
      var t = r.data || {};
      kvRows($('tsum-body'), [
        ['Trades', String(t.trades || 0)], ['Finished', String(t.finished || 0)], ['Open', String(t.open || 0)],
        ['Win rate', rateOrDash(t.win_rate)], ['Avg expected NET', pctOrDash(t.avg_expected_net_pct)],
        ['Avg actual NET', pctOrDash(t.avg_actual_net_pct)], ['Realized P&L', fmtMoney(t.pnl_quote || 0, displayQuote(), { sign: true })]
      ]);
    });
    api('GET', '/api/shadow?limit=100').then(function (r) {
      if (!r.ok) { toast('Shadow: ' + r.error, 'err'); return; }
      var d = r.data || {};
      var s = d.summary || {};
      var ps = num(s.period_start_ts);
      $('shadow-period').textContent = ps > 0
        ? 'Period: since ' + new Date(ps * 1000).toLocaleString([], { hour12: false }) + ' · ' + (s.all_time_closed || 0) + ' closed all time'
        : 'Period: all time';
      kvRows($('shadow-sum-body'), [
        ['Status', d.enabled ? 'ON (no orders, virtual)' : 'OFF (shadow_trading=false)'],
        (state.settings && state.settings.shadow_unlimited)
          ? ['Virtual P&L (unlimited stakes)', fmtMoney((s.balance_now || 0) - (s.balance_start || 0), displayQuote(), { sign: true }) +
             (s.readiness && s.readiness.avg_net_pct != null ? ' · avg NET ' + fmtPct(num(s.readiness.avg_net_pct)) : '')]
          : ['Virtual balance', s.balance_start == null ? '–' : fmtMoney(s.balance_start, displayQuote()) + ' → ' +
             fmtMoney(s.balance_now, displayQuote()) + (s.balance_pct == null ? '' : ' (' + fmtPct(s.balance_pct) + ')')],
        ['Today', fmtMoney(s.today_pnl_quote || 0, displayQuote(), { sign: true }) + ' · ' + (s.today_closed || 0) + ' trades'],
        (state.settings && state.settings.shadow_unlimited)
          ? ['Max drawdown (sum of NET %)', s.readiness && s.readiness.drawdown_pct != null ? num(s.readiness.drawdown_pct).toFixed(2) + '%' : '–']
          : ['Max drawdown', s.max_drawdown_pct == null ? '–' : num(s.max_drawdown_pct).toFixed(2) + '%'],
        ['Open', String(s.open || 0)], ['Closed', String(s.closed || 0)], ['Win rate', rateOrDash(s.win_rate)],
        ['Avg expected NET', pctOrDash(s.avg_expected_net_pct)], ['Avg actual NET', pctOrDash(s.avg_actual_net_pct)],
        ['Target reached', String(s.target_reached || 0)], ['Stopped', String(s.stopped || 0)],
        ['Exits', Object.keys(s.by_exit || {}).map(function (k) { return (EXIT_LABELS[k] || k) + ': ' + s.by_exit[k]; }).join(', ') || '–'],
        ['Closed without a fresh price (not learned)', String(s.no_data || 0)],
        ['Virtual P&L', fmtMoney(s.pnl_quote || 0, displayQuote(), { sign: true })],
        ['Skipped', Object.keys(s.skipped || {}).map(function (k) { return k + ': ' + s.skipped[k]; }).join(', ') || '0']
      ]);
      var rd = s.readiness || {};
      var rb = $('ready-body');
      clear(rb);
      rb.appendChild(h('tr', null, h('td', { class: rd.ready ? 'pos' : 'neg', text: rd.ready ? 'READY for real money' : 'NOT READY yet' }),
        h('td', { class: 'muted', text: rd.ready ? 'every check below passed on the virtual record' : 'keep training: the bot trades virtually until all checks pass' })));
      (rd.checks || []).forEach(function (c) {
        var v = c.value;
        var txt = v == null ? '–' : (c.key === 'avg_net' || c.key === 'drawdown') ? fmtPct(num(v)) : c.key === 'proven_win_rate' ? rateOrDash(v) : String(v);
        rb.appendChild(h('tr', null, h('td', { class: c.ok ? 'pos' : 'neg', text: (c.ok ? 'yes · ' : 'no · ') + c.label }), h('td', { class: 'num', text: txt })));
      });
      var pb = $('pairs-body');
      clear(pb);
      var pairs = d.pairs || [];
      $('pairs-empty').classList.toggle('hidden', pairs.length > 0);
      pairs.forEach(function (p) {
        pb.appendChild(h('tr', null,
          h('td', { text: p.asset + ' @ ' + exchangeLabel(p.target) }),
          h('td', null, h('span', { class: 'proven-badge ' + p.status, text: p.status })),
          h('td', { class: 'num', text: p.wins + '/' + p.n }),
          h('td', { class: 'num', text: p.p10 == null ? '–' : (num(p.p10) * 100).toFixed(0) + '%' }),
          h('td', { class: 'num ' + (num(p.avg_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(p.avg_net_pct) }),
          h('td', { class: 'wrap muted', text: p.text || '' })));
      });
      var sb = $('strategy-body');
      clear(sb);
      [['target', s.by_target || {}], ['asset', s.by_asset || {}]].forEach(function (pair) {
        Object.keys(pair[1]).forEach(function (name) {
          var g = pair[1][name];
          sb.appendChild(h('tr', null,
            h('td', { text: (pair[0] === 'target' ? exchangeLabel(name) : name) + ' (' + pair[0] + ')' }),
            h('td', { class: 'num', text: String(g.closed) }),
            h('td', { class: 'num', text: rateOrDash(g.win_rate) }),
            h('td', { class: 'num', text: pctOrDash(g.avg_expected_net_pct) }),
            h('td', { class: 'num ' + (num(g.avg_actual_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(g.avg_actual_net_pct) }),
            h('td', { class: 'num', text: String(g.target_reached) }),
            h('td', { class: 'num', text: fmtMoney(g.pnl_quote || 0, displayQuote(), { sign: true }) })));
        });
      });
      var tr = s.training || {};
      var tb = $('training-body');
      clear(tb);
      $('training-note').textContent = tr.closed
        ? 'Signals the model rejected on costs, entered virtually to measure them: ' + tr.closed + ' closed, win rate ' +
          rateOrDash(tr.win_rate) + ', real NET ' + pctOrDash(tr.avg_actual_net_pct) + ', the target gave ' +
          pctOrDash(tr.avg_capture_pct) + ' of an average lag of ' + pctOrDash(tr.avg_lag_pct) +
          '. These measurements teach the expected catch-up; they never become offers by themselves.'
        : 'No training trades yet: every priced signal the model rejects is entered virtually here (ride mode).';
      [['target', tr.by_target || {}], ['asset', tr.by_asset || {}]].forEach(function (pair) {
        Object.keys(pair[1]).forEach(function (name) {
          var g = pair[1][name];
          tb.appendChild(h('tr', null,
            h('td', { text: (pair[0] === 'target' ? exchangeLabel(name) : name) + ' (' + pair[0] + ')' }),
            h('td', { class: 'num', text: String(g.closed) }),
            h('td', { class: 'num', text: rateOrDash(g.win_rate) }),
            h('td', { class: 'num', text: pctOrDash(g.avg_lag_pct) }),
            h('td', { class: 'num', text: pctOrDash(g.avg_capture_pct) }),
            h('td', { class: 'num', text: pctOrDash(g.avg_expected_net_pct) }),
            h('td', { class: 'num ' + (num(g.avg_actual_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(g.avg_actual_net_pct) })));
        });
      });
      var gb = $('grade-body');
      clear(gb);
      var grades = s.by_grade || {};
      Object.keys(grades).sort().forEach(function (gname) {
        var g = grades[gname];
        gb.appendChild(h('tr', null,
          h('td', { text: gname }),
          h('td', { class: 'num', text: String(g.closed) }),
          h('td', { class: 'num', text: rateOrDash(g.win_rate) }),
          h('td', { class: 'num ' + (num(g.avg_actual_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(g.avg_actual_net_pct) }),
          h('td', { class: 'num', text: String(g.stopped || 0) }),
          h('td', { class: 'num', text: fmtMoney(g.pnl_quote || 0, displayQuote(), { sign: true }) }),
          h('td', { class: 'num', text: fmtMoney(g.pnl_leveraged_quote || 0, displayQuote(), { sign: true }) })));
      });
      if (!Object.keys(grades).length) gb.appendChild(h('tr', null, h('td', { class: 'muted', colspan: '7', text: 'No closed virtual trades yet: they appear as the bot trains.' })));
      var lr = d.learning || {};
      $('learning-note').textContent = lr.n ? ('Learning table: ' + lr.n + ' outcomes of comparable situations, base win rate ' + rateOrDash(lr.base_rate) + '. Weights are fixed; only measurements are learned.') : 'Learning table is empty: every closed virtual trade adds one outcome.';
      var body = $('shadow-body');
      clear(body);
      var rows = d.trades || [];
      $('shadow-empty').classList.toggle('hidden', rows.length > 0);
      rows.forEach(function (t) {
        body.appendChild(h('tr', null,
          h('td', { class: 'mono', text: fmtTime(t.opened_ts) }),
          h('td', { text: t.asset + ' @ ' + exchangeLabel(t.target) + (t.training ? ' · training' : '') }),
          h('td', { class: 'num', text: fmtMoney(t.entry_price, t.quote) }),
          h('td', { class: 'num', text: t.exit_price == null ? 'open · ' + fmtMoney(t.last_exit_price, t.quote) : fmtMoney(t.exit_price, t.quote) }),
          h('td', { text: exchangeLabel(t.first_mover || '') + (t.lag_pct ? ' · lag ' + fmtPct(t.lag_pct) : '') }),
          h('td', { class: 'num', text: pctOrDash(t.expected_net_pct) }),
          h('td', { class: 'num ' + (num(t.max_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(t.max_net_pct) }),
          h('td', { class: 'num ' + (num(t.actual_net_pct) >= 0 ? 'pos' : 'neg'), text: pctOrDash(t.actual_net_pct) }),
          h('td', { class: 'num', text: t.pnl_quote == null ? '—' : fmtMoney(t.pnl_quote, t.quote, { sign: true }) }),
          h('td', { class: 'wrap', text: t.open ? 'open' : t.exit_reason })));
      });
    });
  }

  function loadHistory() {
    return api('GET', '/api/opportunities?state=history&limit=500').then(function (r) {
      if (!r.ok) { toast('History: ' + r.error, 'err'); return; }
      (r.data || []).forEach(putOpp);
      scheduleRender('cards');
    });
  }

  // ------------------------------------------------------------------ modals (generic)
  var modalTimers = {};

  function showModal(name) {
    var el = $('modal-' + name);
    clearTimeout(modalTimers[name]);
    el.classList.remove('closing');
    el.classList.remove('hidden');
    var first = el.querySelector('button, input, select, a');
    if (first) first.focus();
  }

  function hideModal(name) {
    // Plays the close animation first; `closing` already counts as hidden for every state check.
    var el = $('modal-' + name);
    if (el.classList.contains('hidden') || el.classList.contains('closing')) return;
    el.classList.add('closing');
    clearTimeout(modalTimers[name]);
    modalTimers[name] = setTimeout(function () { el.classList.remove('closing'); el.classList.add('hidden'); }, 200);
  }

  function modalHidden(name) {
    var el = $('modal-' + name);
    return el.classList.contains('hidden') || el.classList.contains('closing');
  }

  var confirmResolve = null;

  function confirmDialog(opts) {
    $('confirm-title').textContent = opts.title || 'Confirm';
    var body = $('confirm-body');
    clear(body);
    (Array.isArray(opts.body) ? opts.body : [opts.body || '']).forEach(function (line) { body.appendChild(h('p', { text: line })); });
    var ok = $('confirm-ok');
    ok.textContent = opts.okLabel || 'Confirm';
    ok.className = 'btn ' + (opts.okClass || 'btn-danger');
    showModal('confirm');
    ok.focus();
    return new Promise(function (resolve) {
      confirmResolve = function (v) { confirmResolve = null; hideModal('confirm'); resolve(v); };
    });
  }

  // ------------------------------------------------------------------ MANUAL TRADE SESSION
  // "Я ЗАШЁЛ" -> amount -> exact plan -> "Я КУПИЛ" -> escort -> "Я УШЁЛ СО СДЕЛКИ" -> actual result
  // -> saved -> back to monitoring.  The screen belongs to this one trade; the board keeps updating
  // underneath (nothing is lost) and gets the full focus back when the trade is saved.
  var SESSION_RETURN_S = 8;
  var AMOUNT_CHIPS = [25, 50, 100, 250];

  function openManualTrade(id) {
    // A MANUAL position restored after a restart: the card is gone, the trade (EXECUTING, no entry order) is not.
    var found = null;
    state.trades.forEach(function (t) { if (t && t.opportunity_id === id && isExecuting(t.state) && !t.entry_order) found = t; });
    return found;
  }

  function inPosition(m) { return !!m && (m.phase === 'sell' || m.phase === 'result'); }

  function lastAmount() {
    try {
      var v = num(window.localStorage.getItem('latbot.manualAmount'));
      if (v > 0) return v;
    } catch (e) { /* storage blocked: fall back to the settings */ }
    var c = num(state.settings && state.settings.capital_quote);
    return c > 0 ? c : 100;
  }

  function rememberAmount(v) {
    try { window.localStorage.setItem('latbot.manualAmount', String(v)); } catch (e) { /* storage blocked */ }
  }

  function sessionOpen(on) { document.body.classList.toggle('in-session', !!on); }

  function assetName(o, s) {
    if (o && o.asset) return o.asset;
    var sym = (s && s.symbol) || '';
    return sym.split('/')[0] || '';
  }

  function openManual(id) {
    var o = getOpp(id);
    if (!o && !openManualTrade(id)) { toast('Opportunity ' + id + ' is not loaded.', 'warn'); return; }
    var m = state.manual;
    if (m && m.id === id && m.phase !== 'done') {
      showModal('manual');
      sessionOpen(true);
      renderManual(true);
      startManualPoll();
      return;
    }
    if (inPosition(m)) {
      toast('Сначала заверши открытую сделку — она снова на экране.', 'warn');
      openManual(m.id);
      return;
    }
    stopManualPoll();
    api('POST', oppPath(id, '/manual')).then(function (r) {
      if (!r.ok) { toast('Сделка: ' + r.error, 'err'); return; }
      var d = r.data || {};
      var fresh = { id: id, phase: 'amount', capital: null, support: d, supportAt: performance.now(), entered: null, trade: null,
        timer: null, polling: false, busy: false, error: '', rendered: '', returnAt: null, returnTimer: null };
      if (d.status === 'in_position' || d.trade_id) {
        // The entry was already reported (page reload, another tab, WS reconnect): resume the escort.
        fresh.phase = 'sell';
        fresh.entered = manualEntryFrom(o, d);
      }
      state.manual = fresh;
      showModal('manual');
      sessionOpen(true);
      renderManual(true);
      startManualPoll();
      scheduleRender('cards');
    });
  }

  function manualEntryFrom(o, d) {
    // Rebuild what submitEntered() keeps, from the support payload (+ the trade record when it is loaded).
    var t = (d.trade_id && state.trades.get(d.trade_id)) || {};
    var entry = num(d.entry_price);
    if (!Number.isFinite(entry)) entry = num(t.actual_entry_price);
    var quote = num(d.quote_amount);
    if (!Number.isFinite(quote)) quote = num(t.capital_quote);
    if (!Number.isFinite(quote)) quote = num(d.capital);
    var base = (entry > 0 && Number.isFinite(quote)) ? quote / entry : num(d.base_amount);
    return {
      resumed: true, trade_id: d.trade_id || t.id || null, entry_price: entry, quote_amount: quote, base_amount: base,
      exit_side: d.exit_side || entrySide(o, d).exit, exit_target: num(t.expected_exit_price), exit_target_now: d.exit_target_now,
      expected_net_pct: d.expected_net_pct !== undefined ? d.expected_net_pct : t.expected_net_pct,
      symbol: d.symbol || t.symbol, exchange: d.exchange || t.exchange
    };
  }

  function startManualPoll() {
    stopManualPoll();
    if (!state.manual) return;
    state.manual.timer = setInterval(pollManual, 300);
  }

  function stopManualPoll() {
    if (state.manual && state.manual.timer) { clearInterval(state.manual.timer); state.manual.timer = null; }
  }

  function supportPath(m) {
    var p = oppPath(m.id, '/manual/support');
    if (m.capital > 0 && !inPosition(m)) p += '?capital=' + encodeURIComponent(m.capital);
    return p;
  }

  function pollManual() {
    var m = state.manual;
    if (!m || m.phase === 'done' || m.polling) return;
    m.polling = true;
    api('GET', supportPath(m)).then(function (r) {
      m.polling = false;
      if (state.manual !== m) return;
      if (r.ok) {
        m.support = r.data || m.support;
        m.supportAt = performance.now();
        m.error = '';
      } else if (r.status === 404) {
        m.support = Object.assign({}, m.support, { valid: false, expired: true, message: 'OPPORTUNITY EXPIRED' });
        m.error = '';
      } else {
        m.error = r.error;
      }
      updateManualLive();
    });
  }

  function manualRemaining(m) {
    var s = m.support || {};
    var base = num(s.remaining_s);
    if (!Number.isFinite(base)) return 0;
    return Math.max(0, base - (performance.now() - (m.supportAt || performance.now())) / 1000);
  }

  function manualExpired(m) {
    var s = m.support || {};
    return !!(s.expired || s.valid === false);
  }

  function manualStatus(m) {
    var s = m.support || {};
    var rem = manualRemaining(m);
    if (inPosition(m) || s.status === 'in_position') {
      // A position already held is never "too fast": only the window can close (and no fresh data is said so).
      if (s.status === 'no_data') return { cls: 'yellow', text: 'NO FRESH DATA — exit at best price' };
      if (s.expired || rem <= 0) return { cls: 'yellow', text: 'WINDOW CLOSED — exit at best price' };
      return { cls: 'green', text: 'IN POSITION' };
    }
    if (s.status === 'no_data') return { cls: 'yellow', text: 'NO FRESH DATA' };
    if (s.status === 'mode_changed') return { cls: 'red', text: 'MODE CHANGED' };
    if (manualExpired(m)) return { cls: 'red', text: 'OPPORTUNITY EXPIRED' };
    var est = num(s.human && s.human.est_execution_s);
    if (!Number.isFinite(est)) est = num(state.settings.human_execution_time_s);
    if (s.status === 'too_fast' || (Number.isFinite(est) && rem < est)) return { cls: 'red', text: 'TOO FAST FOR HUMAN' };
    var li = lightInfo(s.human && s.human.light);
    return { cls: li.cls, text: (s.human && s.human.label) || li.word };
  }

  function entrySide(o, s) {
    var side = (s && s.side) || (o && o.side) || '';
    var dir = o ? num(o.direction) : NaN;
    var isLong = /long/i.test(String(side)) || side === 1 || side === '1' || (!side && dir > 0) || /^buy$/i.test(String(side));
    return isLong ? { entry: 'BUY', exit: 'SELL' } : { entry: 'SELL', exit: 'BUY' };
  }

  function openExchangeLink(s, o, exch) {
    var url = safeUrl(s.pair_url || o.pair_url);
    if (!url) return h('p', { class: 'hint', text: 'Ссылки на пару для ' + exch + ' нет — открой биржу вручную.' });
    return h('a', { class: 'btn btn-open', href: url, target: '_blank', rel: 'noopener noreferrer', text: 'ОТКРЫТЬ БИРЖУ · ' + exch });
  }

  function othersCount(m) {
    var n = 0;
    state.opps.forEach(function (o) { if (o && o.id !== m.id && !isTerminal(o.state)) n++; });
    return n;
  }

  function renderManual(force) {
    var m = state.manual;
    if (!m) return;
    var body = $('manual-body');
    if (!force && m.rendered === m.phase) { updateManualLive(); return; }
    m.rendered = m.phase;
    clear(body);
    var o = getOpp(m.id) || {};
    var s = m.support || {};
    var q = o.quote || s.quote || quoteOf(o);
    var sides = entrySide(o, s);
    var exch = exchangeLabel(s.exchange || o.target_exchange);
    var asset = assetName(o, s);
    $('manual-title').textContent = 'СДЕЛКА · ' + (s.symbol || pairName(o)) + ' · ' + exch;
    m.refs = {};
    var steps = h('ol', { class: 'session-steps', 'aria-label': 'Шаги сделки' });
    [['amount', 'Сумма'], ['buy', 'Покупка'], ['sell', 'Сопровождение'], ['result', 'Результат']].forEach(function (st, i, all) {
      var order = all.map(function (x) { return x[0]; });
      var cur = m.phase === 'done' ? 4 : order.indexOf(m.phase);
      steps.appendChild(h('li', { class: i < cur ? 'done' : i === cur ? 'current' : '', text: (i + 1) + '. ' + st[1] }));
    });
    body.appendChild(steps);

    if (m.phase === 'amount') {
      m.refs.banner = h('div', { class: 'alert hidden', role: 'status' });
      body.appendChild(m.refs.banner);
      var amountInput = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'capital', required: true,
        value: fmtInputNumber(m.capital || lastAmount()), 'aria-label': 'Сумма сделки' });
      m.refs.amountInput = amountInput;
      var chips = h('div', { class: 'amount-chips', role: 'group', 'aria-label': 'Быстрый выбор суммы' });
      AMOUNT_CHIPS.forEach(function (v) {
        chips.appendChild(h('button', { class: 'chip', type: 'button', text: fmtMoney(v, q), onclick: function () { amountInput.value = String(v); } }));
      });
      m.refs.maxChip = h('button', { class: 'chip', type: 'button', text: 'макс. стакана', onclick: function () {
        var cap = num((state.manual && state.manual.support || {}).capital);
        if (cap > 0) amountInput.value = fmtInputNumber(Math.floor(cap * 100) / 100);
      } });
      chips.appendChild(m.refs.maxChip);
      var showBtn = h('button', { class: 'btn btn-primary btn-lg', type: 'submit', text: 'ПОКАЗАТЬ ТОЧНЫЙ ПЛАН' });
      m.refs.showBtn = showBtn;
      body.appendChild(h('form', { class: 'block', onsubmit: function (ev) { ev.preventDefault(); chooseAmount(); } },
        h('div', { class: 'block-title', text: 'ШАГ 1 · СКОЛЬКО ВЛОЖИТЬ?' }),
        h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Сумма, ' + (q || 'quote') }), amountInput),
        chips,
        (m.refs.bookMax = h('p', { class: 'hint' })),
        h('div', { class: 'rows' },
          h('span', { class: 'k', text: 'NET сейчас' }), (m.refs.netNow = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Окно' }), (m.refs.remaining = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Статус' }), (m.refs.status = h('span', { class: 'status-flag' }))),
        (m.refs.formError = h('p', { class: 'status-line err' })),
        showBtn,
        h('div', { class: 'form-row' },
          h('button', { class: 'btn btn-ghost btn-sm', type: 'button', text: 'Отменить сделку', onclick: cancelManualOpp }))));
    } else if (m.phase === 'buy') {
      m.refs.banner = h('div', { class: 'alert hidden', role: 'status' });
      body.appendChild(m.refs.banner);
      var plan = h('div', { class: 'block' },
        h('div', { class: 'block-title ' + (sides.entry === 'BUY' ? 'buy' : 'sell'), text: 'ШАГ 2 · ' + (sides.entry === 'BUY' ? 'КУПИ' : 'ПРОДАЙ') + ' на ' + exch }),
        h('div', { class: 'rows session-rows' },
          h('span', { class: 'k', text: sides.entry === 'BUY' ? 'Купить' : 'Продать' }), (m.refs.buyAmt = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Цена входа' }), (m.refs.buyPx = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Сумма' }), (m.refs.capital = h('span', { class: 'v' })),
          h('span', { class: 'k', text: sides.exit === 'SELL' ? 'Потом продать' : 'Потом откупить' }), (m.refs.sellAmt = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Цена выхода' }), (m.refs.sellPx = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Ожидаемо' }), (m.refs.expect = h('span', { class: 'v big' }))));
      var mplan = (s.plan && typeof s.plan === 'object' && s.plan.caveat) ? s.plan : (o.plan || null);
      if (mplan && mplan.caveat) plan.appendChild(h('div', { class: 'plan-line plan-caveat', text: String(mplan.caveat) }));
      body.appendChild(plan);
      body.appendChild(openExchangeLink(s, o, exch));
      body.appendChild(h('div', { class: 'countdown' },
        h('span', null, 'Окно: ', (m.refs.remaining = h('span', { class: 'big' }))),
        h('span', null, 'Нужно человеку: ~', (m.refs.humanEst = h('span', { class: 'big' }))),
        h('span', null, (m.refs.status = h('span', { class: 'status-flag' })))));
      var priceInput = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'price', required: true, value: fmtInputNumber(s.entry_price_now) });
      var qtyInput = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'quote_amount', required: true, value: fmtInputNumber(m.capital || s.capital) });
      var boughtBtn = h('button', { class: 'btn btn-primary btn-lg', type: 'submit', text: sides.entry === 'BUY' ? 'Я КУПИЛ' : 'Я ПРОДАЛ' });
      m.refs.priceInput = priceInput;
      m.refs.qtyInput = qtyInput;
      m.refs.boughtBtn = boughtBtn;
      var form = h('form', { class: 'block', onsubmit: function (ev) { ev.preventDefault(); submitEntered(); } },
        h('div', { class: 'block-title', text: 'Купил? Впиши, что реально получилось' }),
        h('div', { class: 'form-row' },
          h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Цена покупки (' + (q || 'quote') + ')' }), priceInput),
          h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Потратил (' + (q || 'quote') + ')' }), qtyInput)),
        boughtBtn,
        (m.refs.formError = h('p', { class: 'status-line err' })),
        h('div', { class: 'form-row' },
          h('button', { class: 'btn btn-ghost btn-sm', type: 'button', text: 'Изменить сумму', onclick: function () { m.phase = 'amount'; renderManual(true); } }),
          h('button', { class: 'btn btn-ghost btn-sm', type: 'button', text: 'Отменить сделку', onclick: cancelManualOpp })));
      body.appendChild(form);
      m.refs.form = form;
    } else if (m.phase === 'sell') {
      var e = m.entered || {};
      body.appendChild(h('div', { class: 'alert ok', text: (e.resumed ? 'Открытая сделка восстановлена' : 'Покупка записана') + ': ' + fmtAmount(e.base_amount || s.base_amount) + ' ' + asset + ' по ' + fmtMoney(e.entry_price, q) + '.' }));
      m.refs.banner = h('div', { class: 'alert hidden', role: 'status' });
      body.appendChild(m.refs.banner);
      m.refs.escort = h('div', { class: 'escort wait', role: 'status', 'aria-live': 'polite' });
      body.appendChild(m.refs.escort);
      body.appendChild(h('div', { class: 'block' },
        h('div', { class: 'block-title ' + (sides.exit === 'SELL' ? 'sell' : 'buy'), text: 'ШАГ 3 · ' + (sides.exit === 'SELL' ? 'ПРОДАЖА' : 'ОТКУП') + ' на ' + exch }),
        h('div', { class: 'rows session-rows' },
          h('span', { class: 'k', text: sides.exit === 'SELL' ? 'Продать' : 'Откупить' }), (m.refs.sellAmt = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Цель' }), (m.refs.target = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Стакан даёт сейчас' }), (m.refs.exitNow = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Если выйти сейчас' }), (m.refs.pnlNow = h('span', { class: 'v big' })),
          h('span', { class: 'k', text: 'Окно' }), (m.refs.remaining = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Статус' }), (m.refs.status = h('span', { class: 'status-flag' })))));
      body.appendChild(openExchangeLink(s, o, exch));
      body.appendChild(h('button', { class: 'btn btn-danger btn-lg', type: 'button', text: 'Я УШЁЛ СО СДЕЛКИ',
        onclick: function () { m.phase = 'result'; renderManual(true); } }));
      body.appendChild(h('p', { class: 'hint', text: 'Сделка твоя, пока ты не нажмёшь «Я УШЁЛ СО СДЕЛКИ». Свернуть можно — она останется на карточке и вернётся при перезагрузке.' }));
    } else if (m.phase === 'result') {
      var en = m.entered || {};
      var exitPrice = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'price', required: true,
        value: fmtInputNumber(s.exit_price_now || s.exit_target_now || en.exit_target) });
      var pnlInput = h('input', { type: 'number', step: 'any', inputmode: 'decimal', name: 'reported_pnl_quote', placeholder: 'например 0.42 или -0.15' });
      var noteInput = h('textarea', { name: 'note', rows: '2', maxlength: '2000', placeholder: 'Что получилось, что нет (необязательно)' });
      m.refs.sellInput = exitPrice;
      m.refs.pnlInput = pnlInput;
      m.refs.noteInput = noteInput;
      m.refs.soldBtn = h('button', { class: 'btn btn-primary btn-lg', type: 'submit', text: 'СОХРАНИТЬ РЕЗУЛЬТАТ' });
      body.appendChild(h('form', { class: 'block', onsubmit: function (ev) { ev.preventDefault(); submitExited(); } },
        h('div', { class: 'block-title', text: 'ШАГ 4 · ТВОЙ ФАКТИЧЕСКИЙ РЕЗУЛЬТАТ' }),
        h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Цена, по которой вышел (' + (q || 'quote') + ')' }), exitPrice),
        h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Итог по бирже, ' + (q || 'quote') + ' (необязательно)' }), pnlInput),
        h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Заметка' }), noteInput),
        (m.refs.formError = h('p', { class: 'status-line err' })),
        m.refs.soldBtn,
        h('div', { class: 'form-row' },
          h('button', { class: 'btn btn-ghost btn-sm', type: 'button', text: 'Назад к сопровождению', onclick: function () { m.phase = 'sell'; renderManual(true); } }))));
    } else {
      var t = m.trade || {};
      var pnl = num(t.realized_pnl_quote);
      var rep = num(t.user_reported_pnl_quote);
      m.refs.returnIn = h('span', { class: 'strong' });
      body.appendChild(h('div', { class: 'result' },
        h('div', { class: 'alert ok', text: 'Сделка завершена и сохранена' }),
        h('div', { class: 'big ' + (pnl >= 0 ? 'pos' : 'neg'), text: fmtMoney(t.realized_pnl_quote, q, { sign: true }) + ' по твоей цене выхода' }),
        h('div', { class: 'rows' },
          h('span', { class: 'k', text: 'Ожидалось' }), h('span', { class: 'v', text: fmtPct(t.expected_net_pct) + ' · ' + fmtMoney(t.expected_profit_quote, q, { sign: true }) }),
          h('span', { class: 'k', text: 'Факт' }), h('span', { class: 'v ' + (num(t.actual_net_pct) >= 0 ? 'pos' : 'neg'), text: fmtPct(t.actual_net_pct) + ' · ' + fmtMoney(t.realized_pnl_quote, q, { sign: true }) }),
          Number.isFinite(rep) ? h('span', { class: 'k', text: 'Итог по бирже' }) : null,
          Number.isFinite(rep) ? h('span', { class: 'v ' + (rep >= 0 ? 'pos' : 'neg'), text: fmtMoney(rep, q, { sign: true }) }) : null,
          h('span', { class: 'k', text: 'Вход → выход' }), h('span', { class: 'v', text: fmtMoney(t.actual_entry_price, q) + ' → ' + fmtMoney(t.actual_exit_price, q) }),
          h('span', { class: 'k', text: 'Комиссии' }), h('span', { class: 'v', text: fmtMoney(t.actual_fees_quote, q) }),
          h('span', { class: 'k', text: 'Время до покупки' }), h('span', { class: 'v', text: fmtSecs(t.time_to_entry_s) }),
          h('span', { class: 'k', text: 'Успел в окно' }), h('span', { class: 'v', text: t.entered_in_window === true ? 'да' : t.entered_in_window === false ? 'нет' : '–' }),
          h('span', { class: 'k', text: 'Почему разница' }), h('span', { class: 'v', text: t.diff_reason || '–' })),
        h('p', { class: 'hint' }, 'Бот вернётся к поиску следующей сделки через ', m.refs.returnIn, ' с.'),
        h('div', { class: 'modal-actions' }, h('button', { class: 'btn btn-primary btn-lg', type: 'button', text: 'К МОНИТОРИНГУ', onclick: finishSession }))));
    }
    updateManualLive();
  }

  function fmtInputNumber(v) {
    var n = num(v);
    return Number.isFinite(n) ? String(parseFloat(n.toPrecision(10))) : '';
  }

  function updateManualLive() {
    var m = state.manual;
    var others = $('manual-others');
    if (others) {
      var n = m ? othersCount(m) : 0;
      others.textContent = n ? 'ещё ' + n + ' на доске — не теряются' : '';
    }
    if (!m || !m.refs) return;
    var r = m.refs;
    if (m.phase === 'done') {
      if (r.returnIn && m.returnAt) r.returnIn.textContent = String(Math.max(0, Math.ceil((m.returnAt - performance.now()) / 1000)));
      return;
    }
    var s = m.support || {};
    var o = getOpp(m.id) || {};
    var q = o.quote || s.quote || quoteOf(o);
    var asset = assetName(o, s);
    var st = manualStatus(m);
    var rem = manualRemaining(m);
    if (r.remaining) r.remaining.textContent = rem.toFixed(1) + ' с';
    if (r.humanEst) r.humanEst.textContent = fmtSecs((s.human && s.human.est_execution_s) || state.settings.human_execution_time_s, 0);
    if (r.status) { r.status.textContent = st.text; r.status.className = 'status-flag ' + st.cls; }
    if (r.netNow) r.netNow.textContent = fmtPct(s.net_now_pct);
    if (r.bookMax) r.bookMax.textContent = Number.isFinite(num(s.capital)) ? 'Стакан сейчас держит до ' + fmtMoney(s.capital, q) + ' на этой паре.' : '';
    if (r.buyAmt) r.buyAmt.textContent = fmtAmount(s.buy_amount != null ? s.buy_amount : s.base_amount) + ' ' + asset;
    if (r.buyPx) r.buyPx.textContent = (Number.isFinite(num(s.entry_cap_price)) ? '≤ ' + fmtMoney(s.entry_cap_price, q) : '–') + ' (сейчас ' + fmtMoney(s.entry_price_now, q) + ')';
    if (r.capital) r.capital.textContent = fmtMoney(s.capital, q) + (num(m.capital) > num(s.capital) + 0.005 ? ' — стакан держит меньше, чем ты хотел' : '');
    if (r.sellAmt) r.sellAmt.textContent = fmtAmount(s.sell_amount != null ? s.sell_amount : s.base_amount) + ' ' + asset;
    if (r.sellPx) r.sellPx.textContent = '≥ ' + fmtMoney(s.exit_target_now, q);
    if (r.expect) {
      r.expect.textContent = 'NET ' + fmtPct(s.net_now_pct) + ' · прибыль ' + fmtMoney(s.net_quote, q, { sign: true });
      r.expect.className = 'v big ' + (num(s.net_now_pct) >= 0 ? 'pos' : 'neg');
    }
    if (r.target) r.target.textContent = '≥ ' + fmtMoney(s.exit_target_now, q);
    if (r.exitNow) r.exitNow.textContent = fmtMoney(s.exit_price_now, q);
    if (r.pnlNow) {
      var pn = num(s.pnl_now_pct);
      r.pnlNow.textContent = Number.isFinite(pn) ? fmtPct(pn) + ' · ' + fmtMoney(s.pnl_now_quote, q, { sign: true }) : '–';
      r.pnlNow.className = 'v big ' + (pn >= 0 ? 'pos' : 'neg');
    }
    if (r.escort) {
      var sig = s.status === 'no_data' ? 'no_data' : (s.sell_signal || 'wait');
      r.escort.className = 'escort ' + sig;
      r.escort.textContent = s.status === 'no_data'
        ? 'Нет свежих данных по стакану — выходи по лучшей цене, которую видишь на бирже.'
        : (s.sell_text || (m.error ? 'нет связи с ботом: ' + m.error : 'ЖДИ: бот сравнивает стакан с целью…'));
    }
    var expired = manualExpired(m);
    if (r.banner) {
      var text = '';
      var cls = 'alert hidden';
      if (inPosition(m)) {
        if (m.error) { text = 'Нет связи с ботом: ' + m.error + ' — сделка твоя, пока ты не запишешь выход.'; cls = 'alert warn'; }
      }
      else if (expired) { text = (s.message || 'OPPORTUNITY EXPIRED') + ' — возможность закончилась, покупать уже поздно.'; cls = 'alert danger'; }
      else if (m.error) { text = 'Нет связи с ботом: ' + m.error; cls = 'alert warn'; }
      else if (st.cls === 'red') { text = st.text + (s.message && s.message !== st.text ? ' · ' + s.message : ''); cls = 'alert warn'; }
      r.banner.textContent = text;
      r.banner.className = cls;
    }
    if (m.phase === 'amount' && r.showBtn) r.showBtn.disabled = expired || m.busy;
    if (m.phase === 'buy' && r.form) {
      var lock = expired || m.busy;
      r.form.classList.toggle('locked', expired);
      r.priceInput.disabled = lock;
      r.qtyInput.disabled = lock;
      r.boughtBtn.disabled = lock;
    }
    if (m.phase === 'result' && r.soldBtn) {
      r.soldBtn.disabled = !!m.busy;
      r.sellInput.disabled = !!m.busy;
    }
  }

  function chooseAmount() {
    var m = state.manual;
    if (!m || m.busy || m.phase !== 'amount' || manualExpired(m)) return;
    var v = num(m.refs.amountInput.value);
    if (!(v > 0)) { m.refs.formError.textContent = 'Впиши сумму больше нуля.'; return; }
    m.capital = v;
    rememberAmount(v);
    m.busy = true;
    m.refs.formError.textContent = '';
    updateManualLive();
    api('GET', supportPath(m)).then(function (r) {
      if (state.manual !== m) return;
      m.busy = false;
      if (!r.ok) { m.refs.formError.textContent = r.error; updateManualLive(); return; }
      m.support = r.data || m.support;
      m.supportAt = performance.now();
      m.phase = 'buy';
      renderManual(true);
    });
  }

  function submitEntered() {
    var m = state.manual;
    if (!m || m.busy || m.phase !== 'buy' || manualExpired(m)) return;
    var price = num(m.refs.priceInput.value);
    var qty = num(m.refs.qtyInput.value);
    if (!(price > 0) || !(qty > 0)) { m.refs.formError.textContent = 'Впиши реальную цену покупки и потраченную сумму (обе больше нуля).'; return; }
    m.busy = true;
    m.refs.formError.textContent = '';
    updateManualLive();
    api('POST', oppPath(m.id, '/manual/entered'), { price: price, quote_amount: qty }).then(function (r) {
      if (state.manual !== m) return;
      m.busy = false;
      if (!r.ok) { m.refs.formError.textContent = r.error; updateManualLive(); return; }
      if (r.data && r.data.ok === false) {
        // Engine refused the entry (position already open, AUTO in progress, opportunity closed…).
        m.refs.formError.textContent = r.data.reason || r.data.message || 'entry rejected by the engine';
        if (r.data.message) m.support = Object.assign({}, m.support, { message: r.data.message });
        updateManualLive();
        return;
      }
      m.entered = Object.assign({ entry_price: price, quote_amount: qty }, r.data || {});
      m.phase = 'sell';
      renderManual(true);
      scheduleRender('cards');
    });
  }

  function submitExited() {
    var m = state.manual;
    if (!m || m.busy || m.phase !== 'result') return;
    var price = num(m.refs.sellInput.value);
    if (!(price > 0)) { m.refs.formError.textContent = 'Впиши реальную цену выхода (больше нуля).'; return; }
    var body = { price: price };
    var rawPnl = String(m.refs.pnlInput.value || '').trim();
    if (rawPnl !== '') {
      var rep = num(rawPnl);
      if (!Number.isFinite(rep)) { m.refs.formError.textContent = 'Итог по бирже — число (можно с минусом) или пусто.'; return; }
      body.reported_pnl_quote = rep;
    }
    var note = String(m.refs.noteInput.value || '').trim();
    if (note) body.note = note;
    m.busy = true;
    m.refs.formError.textContent = '';
    updateManualLive();
    api('POST', oppPath(m.id, '/manual/exited'), body).then(function (r) {
      if (state.manual !== m) return;
      m.busy = false;
      if (!r.ok) { m.refs.formError.textContent = r.error; updateManualLive(); return; }
      m.trade = r.data || {};
      m.phase = 'done';
      stopManualPoll();
      if (m.trade.id) { state.trades.set(m.trade.id, m.trade); scheduleRender('trades'); }
      m.returnAt = performance.now() + SESSION_RETURN_S * 1000;
      m.returnTimer = setInterval(function () {
        if (state.manual !== m) { clearInterval(m.returnTimer); return; }
        if (performance.now() >= m.returnAt) { finishSession(); return; }
        updateManualLive();
      }, 250);
      renderManual(true);
      scheduleRender('cards');
      toast('Сделка сохранена: ' + fmtMoney(m.trade.realized_pnl_quote, quoteOf({ target_symbol: m.trade.symbol }), { sign: true }), num(m.trade.realized_pnl_quote) >= 0 ? 'ok' : 'warn');
    });
  }

  function finishSession() {
    // Back to monitoring: full focus on the board and the next best opportunity.
    var m = state.manual;
    if (m && m.returnTimer) clearInterval(m.returnTimer);
    stopManualPoll();
    hideModal('manual');
    sessionOpen(false);
    state.manual = null;
    setTab('opps');
    scheduleRender('cards');
    toast('Снова ищу следующую сделку', 'ok');
  }

  function cancelManualOpp() {
    var m = state.manual;
    if (!m || inPosition(m) || m.phase === 'done') return;
    confirmDialog({ title: 'Отменить эту сделку?', body: 'Возможность уйдёт из активных (сделка не записывается).', okLabel: 'Отменить', okClass: 'btn-warn' }).then(function (ok) {
      if (!ok || state.manual !== m) return;
      api('POST', oppPath(m.id, '/cancel'), { reason: 'user cancelled in the manual session' }).then(function (r) {
        if (!r.ok) toast('Отмена: ' + r.error, 'err');
        stopManualPoll();
        hideModal('manual');
        sessionOpen(false);
        state.manual = null;
        scheduleRender('cards');
      });
    });
  }

  function closeManual() {
    // "Свернуть": an open position stays (card / reload bring it back); a saved trade returns to monitoring.
    var m = state.manual;
    if (m && m.phase === 'done') { finishSession(); return; }
    stopManualPoll();
    hideModal('manual');
    sessionOpen(false);
    if (m && !inPosition(m)) state.manual = null;
    scheduleRender('cards');
  }

  function restoreOpenSession() {
    // Page reload / new tab while a MANUAL position is open: the session takes the screen again.
    if (state.manual) return;
    api('GET', '/api/trades?limit=50').then(function (r) {
      if (!r.ok || state.manual) return;
      var open = null;
      (r.data || []).forEach(function (t) {
        if (!t || !t.id) return;
        state.trades.set(t.id, t);
        if (!open && t.manual && isExecuting(t.state) && !t.entry_order && t.opportunity_id) open = t;
      });
      if (open) openManual(open.opportunity_id);
    });
  }

  // ------------------------------------------------------------------ AUTO modal
  function openAuto(id) {
    var o = getOpp(id);
    if (!o) return;
    if (state.emergency) { toast('EMERGENCY STOP is active: AUTO is blocked.', 'warn'); return; }
    var a = { id: id, phase: 'loading', snapshot: null, result: null, trade: null, error: '', busy: false, abandoned: false };
    state.auto = a;
    showModal('auto');
    renderAuto();
    api('POST', oppPath(id, '/auto')).then(function (r) {
      var d = r.data || {};
      if (a.abandoned) {
        // Dialog closed while selecting: undo the selection the server made meanwhile (nothing cancelled).
        if (r.ok && d.confirmation_required) deselectAuto(a).then(function () { scheduleRender('cards'); });
        return;
      }
      if (state.auto !== a) return;
      if (!r.ok) { a.phase = 'error'; a.error = r.error; renderAuto(); return; }
      if (d.confirmation_required) {
        a.phase = 'confirm';
        a.snapshot = d.snapshot || {};
      } else if (d.ok === false && !(d.state && isTerminal(d.state))) {
        // Engine refused before anything was sent (risk limit, emergency, stale book…): HTTP 200 + ok:false.
        a.phase = 'error';
        a.error = d.reason || 'AUTO rejected by the engine';
      } else {
        a.phase = 'executing';
        a.result = d;
        if (d.trade && d.trade.id) { a.trade = d.trade; state.trades.set(d.trade.id, d.trade); }
        if (d.state && isTerminal(d.state)) a.phase = 'done';
      }
      renderAuto();
      scheduleRender('cards');
    });
  }

  function confirmAuto() {
    var a = state.auto;
    if (!a || a.busy || a.phase !== 'confirm') return;
    a.busy = true;
    renderAuto();
    api('POST', oppPath(a.id, '/auto/confirm')).then(function (r) {
      if (state.auto !== a) return;
      a.busy = false;
      if (!r.ok) { a.phase = 'error'; a.error = r.error; renderAuto(); return; }
      var d = r.data || {};
      a.result = d;
      a.phase = 'executing';
      if (d.trade && d.trade.id) { a.trade = d.trade; state.trades.set(d.trade.id, d.trade); scheduleRender('trades'); }
      if ((d.state && isTerminal(d.state)) || d.ok === false) a.phase = 'done';
      renderAuto();
      scheduleRender('cards');
    });
  }

  function deselectAuto(a) {
    // Back out of the confirmation: AUTO_SELECTED -> VALID. Nothing is cancelled - the lag may still be live.
    return api('POST', oppPath(a.id, '/auto/deselect')).then(function (r) {
      if (!r.ok && r.status !== 404) toast('AUTO: ' + r.error, 'warn');
      return r;
    });
  }

  function abortAuto() {
    // Executing phase only: abort the running trade (cancelled before the entry; an open position is exited at market).
    var a = state.auto;
    if (!a || a.phase !== 'executing') return;
    confirmDialog({ title: 'Abort the running AUTO trade?', body: 'Before the entry it is cancelled; an open position is exited at market.', okLabel: 'Abort trade', okClass: 'btn-warn' }).then(function (ok) {
      if (!ok || state.auto !== a) return;
      api('POST', oppPath(a.id, '/cancel'), { reason: 'user aborted AUTO' }).then(function (r) {
        if (!r.ok) toast('Abort: ' + r.error, 'err');
        else if (r.data && r.data.ok === false) toast('Abort: ' + (r.data.reason || 'refused'), 'warn');
        if (!modalHidden('auto')) renderAuto();
      });
    });
  }

  function closeAuto() {
    var a = state.auto;
    hideModal('auto');
    if (!a) return;
    if (a.phase === 'confirm' && a.busy) return;  // EXECUTE in flight: hide only, the dialog follows TRADE_UPDATE
    if (a.phase === 'confirm' || a.phase === 'loading') {
      // CANCEL, close button, backdrop and Escape all back out the same way - never through /cancel (that would kill a live lag).
      a.abandoned = true;
      state.auto = null;
      if (a.phase === 'confirm') deselectAuto(a).then(function () { scheduleRender('cards'); });
      return;
    }
    if (a.phase === 'done' || a.phase === 'error') state.auto = null;
  }

  function onTradeUpdate(t) {
    var a = state.auto;
    if (a && t.opportunity_id === a.id) {
      a.trade = t;
      if (a.phase === 'confirm' || a.phase === 'loading') a.phase = 'executing';
      if (isTerminal(t.state)) a.phase = 'done';
      if (!modalHidden('auto')) renderAuto();
    }
  }

  function onOppState(o, ev) {
    var a = state.auto;
    if (a && a.id === o.id && isTerminal(o.state) && a.phase === 'confirm') {
      a.phase = 'error';
      a.error = 'Opportunity is ' + o.state + (ev && ev.reason ? ': ' + ev.reason : '');
      if (!modalHidden('auto')) renderAuto();
    }
    var m = state.manual;
    if (m && m.id === o.id && isTerminal(o.state) && (m.phase === 'buy' || m.phase === 'amount')) {
      m.support = Object.assign({}, m.support, { valid: false, expired: true, message: 'OPPORTUNITY ' + o.state });
      updateManualLive();
    }
  }

  var AUTO_KNOWN = { net_now_pct: 1, expected_net_pct: 1, expected_profit_quote: 1, capital: 1, risk: 1, balance: 1, valid: 1, reason: 1, side: 1, exchange: 1, symbol: 1, entry_price: 1, exit_target: 1, remaining_s: 1 };

  function renderAuto() {
    var a = state.auto;
    var body = $('auto-body');
    clear(body);
    if (!a) return;
    var o = getOpp(a.id) || {};
    var snap = a.snapshot || {};
    var q = o.quote || quoteOf(o) || (snap.balance && snap.balance.quote) || '';
    $('auto-title').textContent = 'AUTO · ' + (snap.symbol || pairName(o)) + ' ' + (snap.side || o.side || '') + ' · ' + exchangeLabel(snap.exchange || o.target_exchange);

    if (a.phase === 'loading') {
      body.appendChild(h('p', { class: 'muted', text: 'Selecting and revalidating on the latest book…' }));
      return;
    }
    if (a.phase === 'error') {
      body.appendChild(h('div', { class: 'alert danger', text: a.error || 'AUTO failed' }));
      body.appendChild(h('div', { class: 'modal-actions' }, h('button', { class: 'btn btn-ghost', type: 'button', text: 'Close', onclick: closeAuto })));
      return;
    }
    if (a.phase === 'confirm') {
      var risk = snap.risk || {};
      var bal = snap.balance || {};
      var rows = h('div', { class: 'rows' });
      var row = function (k, v, cls) { rows.appendChild(h('span', { class: 'k', text: k })); rows.appendChild(h('span', { class: 'v ' + (cls || ''), text: v })); };
      row('CURRENT NET', fmtPct(snap.net_now_pct), num(snap.net_now_pct) >= 0 ? 'pos' : 'neg');
      row('EXPECTED NET (card)', fmtPct(snap.expected_net_pct));
      row('EXPECTED PROFIT', fmtMoney(snap.expected_profit_quote, q, { sign: true }));
      row('CAPITAL', fmtMoney(snap.capital, q));
      // risk.ok stays null until RiskManager.check actually ran (revalidation can fail earlier): only true is green.
      if (risk.ok === true) row('RISK', 'OK' + (Number.isFinite(num(risk.allowed)) ? ' · allowed ' + fmtMoney(risk.allowed, q) : ''), 'pos');
      else if (risk.ok === false) row('RISK', 'Blocked: ' + (risk.reason || 'blocked'), 'neg');
      else row('RISK', 'not checked' + (snap.valid === false ? ' (revalidation failed first)' : ''), 'muted');
      if (Number.isFinite(num(bal.free))) row('BALANCE', fmtMoney(bal.free, bal.quote || q) + ' free');
      else row('BALANCE', 'not checked', 'muted');
      if (snap.entry_price !== undefined) row('ENTRY', fmtMoney(snap.entry_price, q));
      if (snap.exit_target !== undefined) row('EXIT TARGET', fmtMoney(snap.exit_target, q));
      if (snap.remaining_s !== undefined) row('WINDOW', fmtSecs(snap.remaining_s));
      row('REVALIDATION', snap.valid === false ? 'Not valid: ' + (snap.reason || 'not valid now') : (snap.reason || 'Valid on the latest book'), snap.valid === false ? 'neg' : 'pos');
      Object.keys(snap).forEach(function (k) { if (!AUTO_KNOWN[k]) row(humanize(k), fmtAny(snap[k], k)); });
      body.appendChild(h('div', { class: 'block' }, h('div', { class: 'block-title', text: 'Confirm execution (' + String(state.mode).toUpperCase() + ')' }), rows));
      body.appendChild(h('p', { class: 'hint', text: 'The bot revalidates on the latest book right before the entry and before the exit. If the edge is gone it cancels instead of executing a stale snapshot.' }));
      var invalid = snap.valid === false;
      body.appendChild(h('div', { class: 'modal-actions' },
        h('button', { class: 'btn btn-ghost', type: 'button', text: 'CANCEL', title: 'close without executing — nothing is cancelled, the opportunity stays on the board', disabled: a.busy, onclick: closeAuto }),
        h('button', { class: 'btn btn-ghost', type: 'button', text: 'RE-CHECK', disabled: a.busy, onclick: function () { openAuto(a.id); } }),
        h('button', { class: 'btn btn-primary', type: 'button', text: a.busy ? 'EXECUTING…' : 'EXECUTE', disabled: a.busy || invalid, onclick: confirmAuto })));
      return;
    }
    // executing / done
    var res = a.result || {};
    var t = a.trade || res.trade || {};
    var tq = quoteOf({ target_symbol: t.symbol }) || q;
    var stateText = t.state || res.state || (a.phase === 'done' ? 'DONE' : 'EXECUTING');
    var ok = res.ok !== false && !(t.state === 'FAILED' || t.state === 'CANCELLED');
    body.appendChild(h('div', { class: 'alert ' + (a.phase === 'done' ? (ok ? 'ok' : 'danger') : 'warn'), text: stateText + (res.reason ? ' · ' + res.reason : '') }));
    var r2 = h('div', { class: 'rows' });
    var row2 = function (k, v, cls) { r2.appendChild(h('span', { class: 'k', text: k })); r2.appendChild(h('span', { class: 'v ' + (cls || ''), text: v })); };
    if (t.id) {
      row2('Trade', t.id);
      row2('Capital', fmtMoney(t.capital_quote, tq));
      row2('Expected NET', fmtPct(t.expected_net_pct));
      if (t.entry_order) row2('Entry order', (t.entry_order.status || '') + ' · filled ' + fmtAmount(t.entry_order.filled) + '/' + fmtAmount(t.entry_order.amount) + (t.entry_order.avg_price ? ' @ ' + fmtMoney(t.entry_order.avg_price, tq) : ''));
      if (t.exit_order) row2('Exit order', (t.exit_order.status || '') + ' · filled ' + fmtAmount(t.exit_order.filled) + '/' + fmtAmount(t.exit_order.amount) + (t.exit_order.avg_price ? ' @ ' + fmtMoney(t.exit_order.avg_price, tq) : ''));
      if (t.actual_net_pct !== null && t.actual_net_pct !== undefined) row2('Actual NET', fmtPct(t.actual_net_pct), num(t.actual_net_pct) >= 0 ? 'pos' : 'neg');
      if (t.realized_pnl_quote !== null && t.realized_pnl_quote !== undefined) row2('Realized', fmtMoney(t.realized_pnl_quote, tq, { sign: true }), num(t.realized_pnl_quote) >= 0 ? 'pos' : 'neg');
      if (t.execution_time_s !== null && t.execution_time_s !== undefined) row2('Execution time', fmtSecs(t.execution_time_s));
      if (t.diff_reason) row2('Diff reason', t.diff_reason);
      if (t.error) row2('Error', t.error, 'neg');
    } else {
      row2('Status', 'waiting for the first trade update…');
    }
    body.appendChild(h('div', { class: 'block' }, r2));
    if (Array.isArray(t.events) && t.events.length) {
      body.appendChild(h('div', { class: 'block' },
        h('div', { class: 'block-title', text: 'Timeline' }),
        h('ul', { class: 'muted' }, t.events.slice(-8).map(function (e) { return h('li', { text: fmtTime(e[0]) + ' · ' + e[1] }); }))));
    }
    var actions = h('div', { class: 'modal-actions' },
      h('button', { class: 'btn btn-ghost', type: 'button', text: a.phase === 'done' ? 'Close' : 'Hide (keeps running)', onclick: closeAuto }));
    if (a.phase === 'executing') {
      actions.appendChild(h('button', { class: 'btn btn-warn', type: 'button', text: 'ABORT', title: 'cancel before the entry / market-exit an open position', onclick: abortAuto }));
    }
    body.appendChild(actions);
  }

  // ------------------------------------------------------------------ emergency
  function emergencyClick() {
    if (state.emergency) {
      confirmDialog({ title: 'Resume trading?', body: 'AUTO trades will be allowed again.', okLabel: 'RESUME', okClass: 'btn-warn' }).then(function (ok) {
        if (!ok) return;
        api('POST', '/api/emergency/resume').then(function (r) {
          if (!r.ok) { toast('Resume: ' + r.error, 'err'); return; }
          state.emergency = false;
          state.emergencyReason = '';
          scheduleRender('topbar');
          scheduleRender('cards');
          toast('Trading resumed', 'ok');
        });
      });
      return;
    }
    confirmDialog({
      title: 'EMERGENCY STOP',
      body: ['Blocks every new AUTO trade and cancels pending orders.', 'Open positions stay open until you press CLOSE POSITIONS. History is kept.'],
      okLabel: 'STOP NOW', okClass: 'btn-danger'
    }).then(function (ok) {
      if (!ok) return;
      api('POST', '/api/emergency/stop', { reason: 'ui' }).then(function (r) {
        if (!r.ok) { toast('Emergency stop: ' + r.error, 'err'); return; }
        state.emergency = true;
        state.emergencyReason = (r.data && r.data.reason) || 'ui';
        scheduleRender('topbar');
        scheduleRender('cards');
        var open = r.data && Array.isArray(r.data.open_positions) ? r.data.open_positions.length : 0;
        toast('EMERGENCY STOP active' + (open ? ' · ' + open + ' open position(s) — use CLOSE POSITIONS' : ''), 'warn');
      });
    });
  }

  function closePositionsClick() {
    confirmDialog({ title: 'Close all open positions?', body: 'Every open position is exited at market on the current mode\'s adapters.', okLabel: 'CLOSE POSITIONS', okClass: 'btn-danger' }).then(function (ok) {
      if (!ok) return;
      api('POST', '/api/emergency/close').then(function (r) {
        if (!r.ok) { toast('Close positions: ' + r.error, 'err'); return; }
        var closed = (r.data && r.data.closed) || [];
        closed.forEach(function (t) { if (t && t.id) state.trades.set(t.id, t); });
        scheduleRender('trades');
        toast('Closed ' + closed.length + ' position(s)', 'ok');
      });
    });
  }

  // ------------------------------------------------------------------ settings drawer
  function openDrawer() {
    renderSettings(true);
    clearSettingsStatus();
    $('settings-drawer').classList.add('open');
    $('settings-drawer').setAttribute('aria-hidden', 'false');
    $('settings-btn').setAttribute('aria-expanded', 'true');
    $('drawer-backdrop').classList.remove('hidden');
    var first = $('settings-form').querySelector('input');
    if (first) first.focus();
  }

  function closeDrawer() {
    $('settings-drawer').classList.remove('open');
    $('settings-drawer').setAttribute('aria-hidden', 'true');
    $('settings-btn').setAttribute('aria-expanded', 'false');
    $('drawer-backdrop').classList.add('hidden');
  }

  function renderSettings(force) {
    var form = $('settings-form');
    if (!force && $('settings-drawer').classList.contains('open')) return; // do not clobber edits
    var s = state.settings || {};
    SETTINGS_FIELDS.forEach(function (name) {
      var el = form.elements[name];
      if (!el) return;
      if (el.type === 'checkbox') el.checked = !!s[name];
      else if (el.dataset.kind === 'budgets') el.value = budgetText(s[name]);
      else el.value = s[name] === undefined || s[name] === null ? '' : String(s[name]);
    });
    var mode = s.mode || state.mode || 'paper';
    var radios = form.querySelectorAll('input[name="mode"]');
    radios.forEach(function (r) { r.checked = r.value === mode; });
  }

  function clearSettingsStatus() {
    $('settings-status').textContent = '';
    $('settings-status').className = 'status-line';
  }

  // {"kraken": 200, "okx": 500} <-> "kraken=200, okx=500" (Settings: exchange_budget_quote)
  function budgetText(v) {
    if (!v || typeof v !== 'object') return '';
    return Object.keys(v).sort().map(function (k) { return k + '=' + v[k]; }).join(', ');
  }

  function readSettingsPatch() {
    var form = $('settings-form');
    var s = state.settings || {};
    var patch = {};
    SETTINGS_FIELDS.forEach(function (name) {
      var el = form.elements[name];
      if (!el) return;
      if (el.type === 'checkbox') {
        if (el.checked !== !!s[name]) patch[name] = el.checked;
      } else if (el.dataset.kind === 'budgets') {
        var text = el.value.trim();
        if (text !== budgetText(s[name])) patch[name] = text;  // "" clears every budget
      } else {
        if (el.value === '') return;
        var n = num(el.value);
        if (!Number.isFinite(n)) return;
        if (n !== num(s[name])) patch[name] = n;
      }
    });
    var modeEl = form.querySelector('input[name="mode"]:checked');
    var mode = modeEl ? modeEl.value : (s.mode || 'paper');
    if (mode !== (s.mode || 'paper')) {
      patch.paper_mode = mode === 'paper';
      patch.live_mode = mode === 'live';
    }
    return patch;
  }

  function saveSettings(ev) {
    ev.preventDefault();
    var patch = readSettingsPatch();
    var status = $('settings-status');
    if (!Object.keys(patch).length) { status.textContent = 'Nothing changed.'; status.className = 'status-line'; return; }
    var proceed = function () {
      status.textContent = 'Saving…';
      status.className = 'status-line';
      api('PUT', '/api/settings', patch).then(function (r) {
        if (!r.ok) { status.textContent = r.error; status.className = 'status-line err'; return; }
        var d = r.data || {};
        if (d.settings) applySettings(d.settings);
        status.textContent = 'Saved: ' + ((d.changed && d.changed.join(', ')) || 'no changes') + (d.saved === false ? ' (not written to config.json!)' : '');
        status.className = 'status-line ' + (d.saved === false ? 'err' : 'ok');
        toast('Settings saved', 'ok');
        renderSettings(true);
      });
    };
    if (patch.live_mode === true) {
      confirmDialog({ title: 'Switch to LIVE?', body: ['Real orders will be sent with your API keys.', 'PAPER and LIVE balances are separate; the badge turns red.'], okLabel: 'GO LIVE', okClass: 'btn-danger' }).then(function (ok) {
        if (ok) proceed(); else renderSettings(true);
      });
    } else {
      proceed();
    }
  }

  // ------------------------------------------------------------------ deep link
  function handleHash() {
    var m = /^#opp\/([A-Za-z0-9_-]+)$/.exec(location.hash || '');
    if (!m) return;
    var id = m[1];
    var focus = function () {
      state.focusId = id;
      state.openWhy.add(id);
      setTab('opps');
      renderFilters();
      renderBest();
      renderCards();
      requestAnimationFrame(function () {
        var el = $('cards').querySelector('[data-id="' + CSS.escape(id) + '"]') || $('best-body').querySelector('[data-id="' + CSS.escape(id) + '"]');
        if (!el) return;
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        el.classList.add('focus-flash');
        el.focus({ preventScroll: true });
        setTimeout(function () { el.classList.remove('focus-flash'); }, 2200);
      });
    };
    if (getOpp(id)) { focus(); return; }
    api('GET', oppPath(id)).then(function (r) {
      if (!r.ok || !r.data) { toast('Opportunity ' + id + ' not found', 'warn'); return; }
      putOpp(r.data);
      focus();
    });
  }

  // ------------------------------------------------------------------ live tick (client-side countdowns)
  function tick() {
    var zeroed = false;
    document.querySelectorAll('.card[data-id]').forEach(function (el) {
      var o = getOpp(el.dataset.id);
      if (!o || isTerminal(o.state)) return;
      var rem = remaining(o);
      var est = num(o.estimated_duration_s);
      var urgent = rem > 0 && rem < 3;
      var val = el.querySelector('[data-role="window-val"]');
      if (val) {
        val.textContent = rem > 0 ? '~' + rem.toFixed(1) + ' sec' : '0.0 sec · expiring';
        val.classList.toggle('urgent', urgent);
      }
      var bar = el.querySelector('[data-role="bar"]');
      if (bar) {
        var frac = est > 0 ? Math.max(0, Math.min(1, rem / est)) : 0;
        bar.style.width = (frac * 100).toFixed(1) + '%';
        bar.className = 'bar-fill ' + barClass(rem, o) + (urgent ? ' urgent' : '');
        var pb = bar.parentNode;
        if (pb) pb.setAttribute('aria-valuenow', String(Math.round(frac * 100)));
      }
      if (rem <= 0 && !o._zeroed) { o._zeroed = true; zeroed = true; }
    });
    if (zeroed) scheduleRender('cards');
    if (state.manual && !modalHidden('manual')) updateManualLive();
    marketTicks = (marketTicks + 1) % 120;  // the Market tab refreshes itself every 30 s while open
    if (marketTicks === 0 && state.tab === 'market' && !document.hidden) loadMarket();
    chartTicks = (chartTicks + 1) % 8;  // every 2 s while the best opportunity is on screen
    if (chartTicks === 0 && state.tab === 'opps' && !document.hidden) refreshChart(false);
  }
  var chartTicks = 0;
  var marketTicks = 0;

  // ------------------------------------------------------------------ wiring
  function bindUi() {
    document.querySelectorAll('.tab').forEach(function (b) { b.addEventListener('click', function () { setTab(b.dataset.tab); }); });
    document.querySelectorAll('#filter-quick .chip').forEach(function (b) {
      b.addEventListener('click', function () {
        state.filters.quick = b.dataset.quick;
        if (b.dataset.quick === 'EXPIRED') loadHistory();
        scheduleRender('cards');
      });
    });
    var bindFilter = function (id, key) {
      $(id).addEventListener('change', function () { state.filters[key] = $(id).value; scheduleRender('cards'); });
      $(id).addEventListener('input', function () { state.filters[key] = $(id).value; scheduleRender('cards'); });
    };
    bindFilter('f-exchange', 'exchange');
    bindFilter('f-asset', 'asset');
    bindFilter('f-minnet', 'minNet');
    bindFilter('f-minwin', 'minWindow');
    bindFilter('f-human', 'human');
    bindFilter('f-bot', 'bot');
    $('filters-reset').addEventListener('click', function () {
      state.filters = { quick: 'ALL', exchange: '', asset: '', minNet: '', minWindow: '', human: '', bot: '', lowq: false };
      $('f-lowq').checked = false;
      ['f-exchange', 'f-asset', 'f-minnet', 'f-minwin', 'f-human', 'f-bot'].forEach(function (id) { $(id).value = ''; });
      state.focusId = null;
      scheduleRender('cards');
    });
    $('hero-scroll').addEventListener('click', function () {
      var target = $('filters');
      var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      // #filters has scroll-margin-top = sticky top bar height, so the chips are not hidden behind it.
      if (target && target.scrollIntoView) target.scrollIntoView({ behavior: still ? 'auto' : 'smooth', block: 'start' });
    });
    $('hero-diag').addEventListener('click', function () { setTab('diag'); window.scrollTo(0, 0); });
    document.querySelector('.brand').addEventListener('click', function (ev) {
      ev.preventDefault();
      setTab('opps');
      window.scrollTo(0, 0);
    });
    $('f-lowq').addEventListener('change', function () { state.filters.lowq = $('f-lowq').checked; scheduleRender('cards'); });
    $('market-refresh').addEventListener('click', loadMarket);
    $('trades-refresh').addEventListener('click', loadTrades);
    $('stats-refresh').addEventListener('click', loadStats);
    $('shadow-new-period').addEventListener('click', function () {
      confirmDialog({ title: 'Start a new statistics period?', body: ['Shadow statistics will count virtual trades from now on.', 'Older trades stay in the history and the database.'], okLabel: 'New period', okClass: 'btn-primary' }).then(function (ok) {
        if (!ok) return;
        api('POST', '/api/stats/period').then(function (r) {
          if (!r.ok) { toast('New period: ' + r.error, 'err'); return; }
          toast('New statistics period started', 'ok');
          loadStats();
        });
      });
    });
    $('diag-refresh').addEventListener('click', loadDiag);

    $('emergency-btn').addEventListener('click', emergencyClick);
    $('close-positions-btn').addEventListener('click', closePositionsClick);
    $('settings-btn').addEventListener('click', function () {
      if ($('settings-drawer').classList.contains('open')) closeDrawer(); else openDrawer();
    });
    $('settings-close').addEventListener('click', closeDrawer);
    $('drawer-backdrop').addEventListener('click', closeDrawer);
    $('settings-form').addEventListener('submit', saveSettings);
    $('settings-reset').addEventListener('click', function () { renderSettings(true); clearSettingsStatus(); });

    $('manual-close').addEventListener('click', closeManual);
    $('auto-close').addEventListener('click', closeAuto);
    $('journal-close').addEventListener('click', closeJournal);
    $('journal-save').addEventListener('click', saveJournalNote);
    $('journal-upload').addEventListener('click', uploadJournalShot);
    $('modal-journal').addEventListener('click', function (ev) { if (ev.target === $('modal-journal')) closeJournal(); });
    $('confirm-ok').addEventListener('click', function () { if (confirmResolve) confirmResolve(true); });
    $('confirm-cancel').addEventListener('click', function () { if (confirmResolve) confirmResolve(false); });
    ['modal-manual', 'modal-auto', 'modal-confirm'].forEach(function (id) {
      $(id).addEventListener('click', function (ev) {
        if (ev.target !== $(id)) return;
        if (id === 'modal-confirm' && confirmResolve) confirmResolve(false);
        else if (id === 'modal-manual') closeManual();
        else if (id === 'modal-auto') closeAuto();
      });
    });
    document.addEventListener('keydown', function (ev) {
      if (ev.key !== 'Escape') return;
      if (!modalHidden('confirm')) { if (confirmResolve) confirmResolve(false); return; }
      if (!modalHidden('manual')) { closeManual(); return; }
      if (!modalHidden('journal')) { closeJournal(); return; }
      if (!modalHidden('auto')) { closeAuto(); return; }
      if ($('settings-drawer').classList.contains('open')) closeDrawer();
    });
    window.addEventListener('hashchange', handleHash);
  }


  // Emoji never reach the screen: backend strings (also sent to Telegram) are scrubbed here.
  var EMOJI_CLASS = '[\\u{1F000}-\\u{1FAFF}\\u{2600}-\\u{27BF}\\u{2B50}\\u{2B55}\\u{23E9}-\\u{23FA}\\u{25B6}\\u{25B8}\\u{2197}\\u{FE0F}\\u{200D}]';
  var EMOJI_TEST = new RegExp(EMOJI_CLASS, 'u');
  var EMOJI_RUN = new RegExp('\\s*' + EMOJI_CLASS + '+\\s*', 'gu');
  function scrubText(node) {
    var v = node.nodeValue;
    if (!v || !EMOJI_TEST.test(v)) return;
    node.nodeValue = v.replace(EMOJI_RUN, ' ').trim();
  }
  function scrubTree(root) {
    if (root.nodeType === 3) { scrubText(root); return; }
    if (root.nodeType !== 1) return;
    var w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    var n;
    while ((n = w.nextNode())) scrubText(n);
  }
  function startEmojiScrub() {
    scrubTree(document.body);
    new MutationObserver(function (list) {
      list.forEach(function (m) {
        if (m.type === 'characterData') scrubText(m.target);
        else m.addedNodes.forEach(scrubTree);
      });
    }).observe(document.body, { childList: true, subtree: true, characterData: true });
  }

  function init() {
    startEmojiScrub();
    placeStrips('opps');
    bindUi();
    renderAll();
    api('GET', '/api/state').then(function (r) {
      if (r.ok) applyState(r.data);
      return api('GET', '/api/opportunities?state=all&limit=500');
    }).then(function (r) {
      if (r && r.ok) (r.data || []).forEach(putOpp);
      uiMemo.loaded = true;
      renderAll();
      handleHash();
      restoreOpenSession();
    });
    connectWs();
    setInterval(tick, 250);
  }

  // Debug handle for manual QA in the browser console (read-only use).
  window.latbot = {
    state: state, fmtMoney: fmtMoney, fmtPct: fmtPct, fmtSecs: fmtSecs, remaining: remaining, computeBest: computeBest,
    // QA only: render a synthetic Opportunity.to_dict() payload in THIS tab. Nothing is sent to the server.
    inject: function (o) { putOpp(o); scheduleRender('cards'); },
    toast: toast
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
