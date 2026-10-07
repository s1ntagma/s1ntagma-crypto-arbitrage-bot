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
    green: { emoji: '🟢', cls: 'green', word: 'GREEN' },
    yellow: { emoji: '🟡', cls: 'yellow', word: 'YELLOW' },
    red: { emoji: '🔴', cls: 'red', word: 'RED' },
    unknown: { emoji: '⚪', cls: 'muted', word: 'N/A' }
  };
  var SETTINGS_FIELDS = [
    'human_execution_time_s', 'telegram_profit_threshold_pct', 'capital_quote', 'max_trade_quote',
    'min_net_pct_show', 'min_movers', 'auto_confirm', 'alert_human_only', 'telegram_require_executable'
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
    filters: { quick: 'ALL', exchange: '', asset: '', minNet: '', minWindow: '', human: '', bot: '' },
    tab: 'opps',
    focusId: null,
    openWhy: new Set(),
    manual: null,
    auto: null,
    tradesLoaded: false,
    statsLoaded: false
  };

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
    setTimeout(function () { if (el.parentNode) el.parentNode.removeChild(el); }, kind === 'err' ? 7000 : 4000);
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
    if (snap.state) applyState(snap.state);
    state.opps.clear();
    (snap.opportunities || []).forEach(putOpp);
    renderAll();
  }

  function applyState(s) {
    if (!s) return;
    state.mode = s.mode || state.mode;
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
  function renderTopbar() {
    var badge = $('mode-badge');
    var live = state.mode === 'live';
    badge.textContent = live ? 'LIVE' : 'PAPER';
    badge.className = 'mode-badge ' + (live ? 'live' : 'paper');
    document.title = (live ? '[LIVE] ' : '[PAPER] ') + 'latbot · latency terminal';

    $('ws-dot').className = 'dot ' + (state.connected ? 'green' : 'red');
    $('ws-label').textContent = state.connected ? 'live' : 'reconnecting…';

    var btn = $('emergency-btn');
    var closeBtn = $('close-positions-btn');
    var banner = $('emergency-banner');
    if (state.emergency) {
      btn.textContent = '▶ RESUME TRADING';
      btn.className = 'btn btn-warn';
      closeBtn.classList.remove('hidden');
      banner.textContent = '🛑 EMERGENCY STOP ACTIVE — new AUTO trades are blocked' + (state.emergencyReason ? ' · reason: ' + state.emergencyReason : '');
      banner.classList.remove('hidden');
    } else {
      btn.textContent = '🛑 EMERGENCY STOP';
      btn.className = 'btn btn-danger';
      closeBtn.classList.add('hidden');
      banner.classList.add('hidden');
    }
  }

  function tile(label, valueNode, cls) {
    return h('div', { class: 'tile' + (cls ? ' ' + cls : '') },
      h('div', { class: 'tile-label', text: label }),
      h('div', { class: 'tile-value' }, valueNode));
  }

  function renderDashboard() {
    var d = state.dashboard || {};
    var q = displayQuote();
    var box = $('dashboard');
    clear(box);
    var pnlNode = function (v) {
      var n = num(v);
      return h('span', { class: n > 0 ? 'pos' : n < 0 ? 'neg' : '', text: fmtMoney(v, q, { sign: true }) });
    };
    box.appendChild(tile('Active', fmtInt(d.active)));
    box.appendChild(tile('Today', fmtInt(d.today)));
    box.appendChild(tile('Lights', h('span', { class: 'tile-lights' },
      h('span', { class: 'light green', text: '🟢 ' + fmtInt(d.green) }),
      h('span', { class: 'light yellow', text: '🟡 ' + fmtInt(d.yellow) }),
      h('span', { class: 'light red', text: '🔴 ' + fmtInt(d.red) }))));
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
      var cls = !hh.ws_connected ? 'red' : (hh.usable === false || hh.stale || hh.maintenance) ? 'yellow' : 'green';
      var word = cls === 'red' ? 'offline' : cls === 'yellow' ? 'degraded' : 'ok';
      var chip = h('span', { class: 'chip-health', title: hh.reason || '' },
        h('span', { class: 'dot ' + cls, 'aria-hidden': 'true' }),
        h('span', { text: hh.label || exchangeLabel(name) }),
        h('span', { class: 'muted', text: word }),
        h('span', { class: 'lat', text: 'ws ' + fmtMs(hh.ws_latency_ms) }));
      if (Number.isFinite(num(hh.order_latency_ms))) chip.appendChild(h('span', { class: 'lat', text: 'order ' + fmtMs(hh.order_latency_ms) }));
      if (hh.role === 'execution') chip.appendChild(h('span', { class: 'tag accent', text: 'target' }));
      if (hh.stale) chip.appendChild(h('span', { class: 'tag yellow', text: 'stale' }));
      if (hh.maintenance) chip.appendChild(h('span', { class: 'tag red', text: 'maint' }));
      if (hh.rate_limited_until_mono) chip.appendChild(h('span', { class: 'tag yellow', text: 'rate-limited' }));
      box.appendChild(chip);
    });
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

  function passesFilters(o) {
    var f = state.filters;
    var term = isTerminal(o.state);
    if (state.focusId === o.id) return true;
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
    clear(box);
    var best = computeBest();
    if (!best) {
      box.appendChild(h('div', { class: 'best-empty', text: 'No executable opportunity right now — waiting for a real lag (no data, no signal).' }));
      return;
    }
    box.appendChild(renderCard(best, { best: true }));
  }

  function renderCards() {
    var box = $('cards');
    var list = visibleOpps();
    clear(box);
    list.forEach(function (o) { box.appendChild(renderCard(o, {})); });
    $('cards-empty').classList.toggle('hidden', list.length > 0);
    $('opps-count').textContent = String(state.opps.size);
  }

  function kv(k, v, cls) {
    return h('div', { class: 'kv' }, h('span', { class: 'kv-k', text: k }), h('span', { class: 'kv-v ' + (cls || ''), text: v }));
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
      h('span', { class: 'card-pair', text: (opts.best ? '🔥 ' : '') + pairName(o) }),
      h('span', { class: 'card-side ' + (side === 'LONG' ? 'long' : 'short'), text: side }),
      h('span', { class: 'tag ' + (o.mode === 'live' ? 'live' : 'paper'), text: String(o.mode || '').toUpperCase() }),
      h('span', { class: 'state-badge', text: o.state }),
      h('span', { class: 'card-target' }, 'TARGET: ', h('b', { text: exchangeLabel(o.target_exchange) }))));

    card.appendChild(h('div', { class: 'card-net' },
      h('span', { class: 'card-net-label', text: 'NET PROFIT' }),
      h('span', { class: 'card-net-value ' + (net >= 0 ? 'pos' : 'neg'), text: fmtPct(net) }),
      Number.isFinite(num(o.max_net_pct)) && num(o.max_net_pct) > net + 0.005
        ? h('span', { class: 'muted', text: 'peak ' + fmtPct(o.max_net_pct) }) : null));

    card.appendChild(h('div', { class: 'card-grid' },
      kv('BUY', fmtMoney(o.buy_price, q)),
      kv('SELL', fmtMoney(o.sell_price, q)),
      kv('CAPITAL', fmtMoney(econ.executable_quote, q) + (econ.liquidity_ok === false ? ' ⚠' : '')),
      kv('EXPECTED PROFIT', fmtMoney(profit, q, { sign: true }), profit >= 0 ? 'pos' : 'neg')));

    if (term) {
      var dur = num(o.actual_duration_s);
      card.appendChild(h('div', { class: 'window' },
        h('div', { class: 'window-row' },
          h('span', { class: 'kv-k', text: o.state + (Number.isFinite(dur) ? ' after ' + fmtSecs(dur) : '') }),
          h('span', { class: 'muted', text: o.expire_reason || '' }))));
    } else {
      var frac = est > 0 ? Math.max(0, Math.min(1, rem / est)) : 0;
      card.appendChild(h('div', { class: 'window' },
        h('div', { class: 'window-row' },
          h('span', { class: 'kv-k', text: 'OPPORTUNITY WINDOW' }),
          h('span', { class: 'kv-v', 'data-role': 'window-val', text: '~' + rem.toFixed(1) + ' sec' })),
        h('div', { class: 'bar', role: 'progressbar', 'aria-valuemin': '0', 'aria-valuemax': '100', 'aria-valuenow': String(Math.round(frac * 100)) },
          h('div', { class: 'bar-fill ' + barClass(rem, o), 'data-role': 'bar', style: 'width:' + (frac * 100).toFixed(1) + '%' }))));
    }

    var humanParts = [hl.emoji + ' ' + fmtSecs(human.est_execution_s, 0) + ' execution'];
    humanParts.push(hl.cls === 'green' ? 'low risk' : hl.cls === 'yellow' ? 'high risk' : (human.label || 'NOT EXECUTABLE'));
    if (!term) humanParts.push('~' + fmtSecs(rem, 0) + ' window');
    var hn = num(human.stats_n);
    var hr = num(human.stats_rate);
    if (hn >= 5 && Number.isFinite(hr)) humanParts.push('by stats ' + Math.round(hr * hn) + '/' + hn + ' made it');
    card.appendChild(h('div', { class: 'line light ' + hl.cls },
      h('span', { class: 'line-k', text: 'HUMAN' }),
      h('span', { text: humanParts.join(' / ') })));

    var botLine = h('div', { class: 'line light ' + bl.cls },
      h('span', { class: 'line-k', text: 'BOT' }),
      h('span', { text: bl.emoji + ' ' + (bot.label || bl.word) + ' estimated execution: ' + fmtSecs(bot.est_execution_s) }));
    if (bl.cls !== 'green' && Array.isArray(bot.reasons) && bot.reasons.length) {
      botLine.appendChild(h('span', { class: 'reason', text: '· ' + bot.reasons[0] }));
    }
    card.appendChild(botLine);

    card.appendChild(renderWhy(o));
    card.appendChild(renderFoot(o, term, exec));
    return card;
  }

  function renderWhy(o) {
    var details = h('details', { class: 'why', open: state.openWhy.has(o.id) });
    details.addEventListener('toggle', function () {
      if (details.open) state.openWhy.add(o.id); else state.openWhy.delete(o.id);
    });
    details.appendChild(h('summary', { text: 'WHY?' }));
    details.appendChild(h('pre', { text: o.why || 'no explanation recorded' }));
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
      foot.appendChild(h('a', { class: 'link', href: url, target: '_blank', rel: 'noopener noreferrer', text: 'Open on ' + exchangeLabel(o.target_exchange) + ' ↗' }));
    }
    foot.appendChild(h('span', { class: 'card-meta' }, h('span', { text: 'seen ' + fmtTime(o.detected_ts) })));
    var manualMine = state.manual && state.manual.id === o.id && state.manual.phase !== 'done';
    var humanRed = lightOf(o, 'human') === 'red';
    var botRed = lightOf(o, 'bot') === 'red';
    var manualDisabled = term || (exec && !manualMine) || (humanRed && !manualMine);
    var autoDisabled = term || exec || botRed || state.emergency || manualMine;
    var manualBtn = h('button', {
      class: 'btn btn-manual', type: 'button', disabled: manualDisabled,
      title: term ? 'opportunity is over' : humanRed ? 'too fast for a human' : exec ? 'already executing' : 'trade it by hand with live support',
      text: manualMine ? (state.manual.phase === 'sell' ? 'MANUAL · SELL ▸' : 'MANUAL ▸') : 'MANUAL',
      onclick: function () { openManual(o.id); }
    });
    var autoBtn = h('button', {
      class: 'btn btn-auto', type: 'button', disabled: autoDisabled,
      title: state.emergency ? 'EMERGENCY STOP active' : term ? 'opportunity is over' : botRed ? 'bot cannot execute this one' : exec ? 'already executing' : 'let the bot execute (revalidated first)',
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
        h('td', null, h('span', { class: 'state-badge', text: t.state || '' })),
        h('td', { class: 'wrap', text: t.diff_reason || t.error || '' }));
      body.appendChild(tr);
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
  function setTab(name) {
    state.tab = name;
    document.querySelectorAll('.tab').forEach(function (b) { b.setAttribute('aria-selected', b.dataset.tab === name ? 'true' : 'false'); });
    ['opps', 'trades', 'stats'].forEach(function (n) { $('tab-' + n).classList.toggle('hidden', n !== name); });
    if (name === 'trades' && !state.tradesLoaded) loadTrades();
    if (name === 'stats' && !state.statsLoaded) loadStats();
  }

  function loadTrades() {
    return api('GET', '/api/trades?limit=500').then(function (r) {
      if (!r.ok) { toast('Trades: ' + r.error, 'err'); return; }
      (r.data || []).forEach(function (t) { if (t && t.id) state.trades.set(t.id, t); });
      state.tradesLoaded = true;
      renderTrades();
    });
  }

  function loadStats() {
    return api('GET', '/api/stats').then(function (r) {
      if (!r.ok) { toast('Stats: ' + r.error, 'err'); return; }
      state.stats = Array.isArray(r.data) ? r.data : [];
      state.statsLoaded = true;
      renderStats();
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
  function showModal(name) {
    $('modal-' + name).classList.remove('hidden');
    var first = $('modal-' + name).querySelector('button, input, select, a');
    if (first) first.focus();
  }

  function hideModal(name) { $('modal-' + name).classList.add('hidden'); }
  function modalHidden(name) { return $('modal-' + name).classList.contains('hidden'); }

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

  // ------------------------------------------------------------------ MANUAL modal
  function openManual(id) {
    var o = getOpp(id);
    if (!o) return;
    var m = state.manual;
    if (m && m.id === id && m.phase !== 'done') {
      showModal('manual');
      renderManual(true);
      startManualPoll();
      return;
    }
    if (m && m.phase === 'sell') {
      toast('Finish the open manual position (' + m.id + ') first.', 'warn');
      return;
    }
    stopManualPoll();
    api('POST', oppPath(id, '/manual')).then(function (r) {
      if (!r.ok) { toast('MANUAL: ' + r.error, 'err'); return; }
      state.manual = { id: id, phase: 'buy', support: r.data || {}, supportAt: performance.now(), entered: null, trade: null, timer: null, polling: false, busy: false, error: '', rendered: '' };
      showModal('manual');
      renderManual(true);
      startManualPoll();
      scheduleRender('cards');
    });
  }

  function startManualPoll() {
    stopManualPoll();
    if (!state.manual) return;
    state.manual.timer = setInterval(pollManual, 300);
  }

  function stopManualPoll() {
    if (state.manual && state.manual.timer) { clearInterval(state.manual.timer); state.manual.timer = null; }
  }

  function pollManual() {
    var m = state.manual;
    if (!m || m.phase === 'done' || m.polling) return;
    m.polling = true;
    api('GET', oppPath(m.id, '/manual/support')).then(function (r) {
      m.polling = false;
      if (state.manual !== m) return;
      if (r.ok) {
        m.support = r.data || m.support;
        m.supportAt = performance.now();
        m.error = '';
      } else if (r.status === 404) {
        m.support = Object.assign({}, m.support, { valid: false, expired: true, message: '⚠️ OPPORTUNITY EXPIRED' });
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
    if (manualExpired(m)) return { cls: 'red', text: '⚠️ OPPORTUNITY EXPIRED' };
    var rem = manualRemaining(m);
    var est = num(s.human && s.human.est_execution_s);
    if (!Number.isFinite(est)) est = num(state.settings.human_execution_time_s);
    if (s.status === 'too_fast' || (Number.isFinite(est) && rem < est)) return { cls: 'red', text: '🔴 TOO FAST FOR HUMAN' };
    var li = lightInfo(s.human && s.human.light);
    return { cls: li.cls, text: li.emoji + ' ' + ((s.human && s.human.label) || li.word) };
  }

  function entrySide(o, s) {
    var side = (s && s.side) || (o && o.side) || '';
    var dir = o ? num(o.direction) : NaN;
    var isLong = /long/i.test(String(side)) || side === 1 || side === '1' || (!side && dir > 0) || /^buy$/i.test(String(side));
    return isLong ? { entry: 'BUY', exit: 'SELL' } : { entry: 'SELL', exit: 'BUY' };
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
    $('manual-title').textContent = 'MANUAL · ' + (s.symbol || pairName(o)) + ' ' + (o.side || '') + ' · ' + exch;
    m.refs = {};

    if (m.phase === 'buy') {
      m.refs.banner = h('div', { class: 'alert hidden', role: 'status' });
      body.appendChild(m.refs.banner);
      var buy = h('div', { class: 'block' },
        h('div', { class: 'block-title ' + (sides.entry === 'BUY' ? 'buy' : 'sell'), text: 'STEP 1 · ' + sides.entry + ' on ' + exch }),
        h('div', { class: 'rows' },
          h('span', { class: 'k', text: 'Pair' }), h('span', { class: 'v', text: s.symbol || pairName(o) }),
          h('span', { class: 'k', text: 'Price now' }), (m.refs.priceNow = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Amount' }), (m.refs.amount = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Capital' }), (m.refs.capital = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Exit target' }), (m.refs.exitTarget = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'NET now' }), (m.refs.netNow = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Slippage est.' }), (m.refs.slip = h('span', { class: 'v' }))));
      var url = safeUrl(s.pair_url || o.pair_url);
      if (url) buy.appendChild(h('a', { class: 'link', href: url, target: '_blank', rel: 'noopener noreferrer', text: 'Open on ' + exch + ' ↗' }));
      body.appendChild(buy);

      body.appendChild(h('div', { class: 'countdown' },
        h('span', null, 'OPPORTUNITY: ', (m.refs.remaining = h('span', { class: 'big' })), ' remaining'),
        h('span', null, 'HUMAN EXECUTION: ~', (m.refs.humanEst = h('span', { class: 'big' }))),
        h('span', null, 'STATUS ', (m.refs.status = h('span', { class: 'status-flag' })))));

      var priceInput = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'price', required: true, value: fmtInputNumber(s.entry_price_now) });
      var qtyInput = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'quote_amount', required: true, value: fmtInputNumber(s.capital) });
      var boughtBtn = h('button', { class: 'btn btn-primary', type: 'submit' });
      m.refs.priceInput = priceInput;
      m.refs.qtyInput = qtyInput;
      m.refs.boughtBtn = boughtBtn;
      var refreshLabel = function () {
        boughtBtn.textContent = 'I ' + (sides.entry === 'BUY' ? 'BOUGHT' : 'SOLD') + ' AT ' + fmtMoney(priceInput.value, q);
      };
      priceInput.addEventListener('input', refreshLabel);
      refreshLabel();
      var form = h('form', { class: 'block', onsubmit: function (ev) { ev.preventDefault(); submitEntered(); } },
        h('div', { class: 'block-title', text: 'Tell the bot what you actually got' }),
        h('div', { class: 'form-row' },
          h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Fill price (' + (q || 'quote') + ')' }), priceInput),
          h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Capital used (' + (q || 'quote') + ')' }), qtyInput),
          boughtBtn),
        (m.refs.formError = h('p', { class: 'status-line err' })),
        h('div', { class: 'form-row' },
          h('button', { class: 'btn btn-ghost btn-sm', type: 'button', text: 'Cancel opportunity', onclick: cancelManualOpp })));
      body.appendChild(form);
      m.refs.form = form;
    } else if (m.phase === 'sell') {
      var e = m.entered || {};
      body.appendChild(h('div', { class: 'alert ok', text: 'Entry recorded: ' + sides.entry + ' ' + fmtAmount(e.base_amount || s.base_amount) + ' at ' + fmtMoney(e.entry_price, q) + '. Now the exit leg.' }));
      m.refs.banner = h('div', { class: 'alert hidden', role: 'status' });
      body.appendChild(m.refs.banner);
      body.appendChild(h('div', { class: 'block' },
        h('div', { class: 'block-title ' + (sides.exit === 'SELL' ? 'sell' : 'buy'), text: 'STEP 2 · ' + sides.exit + ' on ' + exch }),
        h('div', { class: 'rows' },
          h('span', { class: 'k', text: 'Exit target' }), h('span', { class: 'v', text: fmtMoney(e.exit_target || s.exit_target_now, q) }),
          h('span', { class: 'k', text: 'Target now (live)' }), (m.refs.exitNow = h('span', { class: 'v' })),
          h('span', { class: 'k', text: 'Amount' }), h('span', { class: 'v', text: fmtAmount(e.base_amount || s.base_amount) }),
          h('span', { class: 'k', text: 'Est. fees' }), h('span', { class: 'v', text: fmtMoney(e.estimated_fees_quote, q) }),
          h('span', { class: 'k', text: 'NET now' }), (m.refs.netNow = h('span', { class: 'v' })))));
      body.appendChild(h('div', { class: 'countdown' },
        h('span', null, 'OPPORTUNITY: ', (m.refs.remaining = h('span', { class: 'big' })), ' remaining'),
        h('span', null, 'STATUS ', (m.refs.status = h('span', { class: 'status-flag' })))));
      var sellPrice = h('input', { type: 'number', step: 'any', min: '0', inputmode: 'decimal', name: 'price', required: true, value: fmtInputNumber(e.exit_target || s.exit_target_now) });
      var soldBtn = h('button', { class: 'btn btn-primary', type: 'submit' });
      m.refs.sellInput = sellPrice;
      m.refs.soldBtn = soldBtn;
      var refreshSold = function () { soldBtn.textContent = 'I ' + (sides.exit === 'SELL' ? 'SOLD' : 'BOUGHT BACK') + ' AT ' + fmtMoney(sellPrice.value, q); };
      sellPrice.addEventListener('input', refreshSold);
      refreshSold();
      body.appendChild(h('form', { class: 'block', onsubmit: function (ev) { ev.preventDefault(); submitExited(); } },
        h('div', { class: 'form-row' },
          h('label', { class: 'field' }, h('span', { class: 'field-label', text: 'Exit price (' + (q || 'quote') + ')' }), sellPrice),
          soldBtn),
        (m.refs.formError = h('p', { class: 'status-line err' })),
        h('p', { class: 'hint', text: 'The position is yours until you record the exit — this dialog stays available from the card.' })));
    } else {
      var t = m.trade || {};
      var pnl = num(t.realized_pnl_quote);
      body.appendChild(h('div', { class: 'result' },
        h('div', { class: 'alert ' + (pnl >= 0 ? 'ok' : 'warn'), text: 'Trade closed · ' + (t.state || 'CLOSED') }),
        h('div', { class: 'big ' + (pnl >= 0 ? 'pos' : 'neg'), text: fmtMoney(t.realized_pnl_quote, q, { sign: true }) + ' realized' }),
        h('div', { class: 'rows' },
          h('span', { class: 'k', text: 'Expected NET' }), h('span', { class: 'v', text: fmtPct(t.expected_net_pct) }),
          h('span', { class: 'k', text: 'Actual NET' }), h('span', { class: 'v ' + (num(t.actual_net_pct) >= 0 ? 'pos' : 'neg'), text: fmtPct(t.actual_net_pct) }),
          h('span', { class: 'k', text: 'Expected profit' }), h('span', { class: 'v', text: fmtMoney(t.expected_profit_quote, q, { sign: true }) }),
          h('span', { class: 'k', text: 'Entry / exit' }), h('span', { class: 'v', text: fmtMoney(t.actual_entry_price, q) + ' → ' + fmtMoney(t.actual_exit_price, q) }),
          h('span', { class: 'k', text: 'Slippage' }), h('span', { class: 'v', text: fmtPct(t.actual_slippage_pct) }),
          h('span', { class: 'k', text: 'Fees' }), h('span', { class: 'v', text: fmtMoney(t.actual_fees_quote, q) }),
          h('span', { class: 'k', text: 'Execution time' }), h('span', { class: 'v', text: fmtSecs(t.execution_time_s) }),
          h('span', { class: 'k', text: 'Diff reason' }), h('span', { class: 'v', text: t.diff_reason || '–' })),
        h('div', { class: 'modal-actions' }, h('button', { class: 'btn btn-primary', type: 'button', text: 'Close', onclick: closeManual }))));
    }
    updateManualLive();
  }

  function fmtInputNumber(v) {
    var n = num(v);
    return Number.isFinite(n) ? String(parseFloat(n.toPrecision(10))) : '';
  }

  function updateManualLive() {
    var m = state.manual;
    if (!m || !m.refs || m.phase === 'done') return;
    var s = m.support || {};
    var o = getOpp(m.id) || {};
    var q = o.quote || s.quote || quoteOf(o);
    var st = manualStatus(m);
    var rem = manualRemaining(m);
    var r = m.refs;
    if (r.remaining) r.remaining.textContent = rem.toFixed(1) + ' s';
    if (r.humanEst) r.humanEst.textContent = fmtSecs((s.human && s.human.est_execution_s) || state.settings.human_execution_time_s, 0);
    if (r.status) { r.status.textContent = st.text; r.status.className = 'status-flag ' + st.cls; }
    if (r.priceNow) r.priceNow.textContent = fmtMoney(s.entry_price_now, q);
    if (r.amount) r.amount.textContent = fmtAmount(s.base_amount);
    if (r.capital) r.capital.textContent = fmtMoney(s.capital, q);
    if (r.exitTarget) r.exitTarget.textContent = fmtMoney(s.exit_target_now, q);
    if (r.exitNow) r.exitNow.textContent = fmtMoney(s.exit_target_now, q);
    if (r.netNow) r.netNow.textContent = fmtPct(s.net_now_pct);
    if (r.slip) r.slip.textContent = fmtPct(s.expected_slippage_pct);
    var expired = manualExpired(m);
    if (r.banner) {
      var text = '';
      var cls = 'alert hidden';
      if (expired) { text = s.message || '⚠️ OPPORTUNITY EXPIRED'; cls = 'alert danger'; }
      else if (m.error) { text = 'support unavailable: ' + m.error; cls = 'alert warn'; }
      else if (st.cls === 'red') { text = st.text + (s.message && s.message !== st.text ? ' · ' + s.message : ''); cls = 'alert warn'; }
      if (m.phase === 'sell' && expired) { text = '⚠️ OPPORTUNITY EXPIRED — window closed. Exit at the best price the book gives you.'; cls = 'alert warn'; }
      r.banner.textContent = text;
      r.banner.className = cls;
    }
    if (m.phase === 'buy' && r.form) {
      var lock = expired || m.busy;
      r.form.classList.toggle('locked', expired);
      r.priceInput.disabled = lock;
      r.qtyInput.disabled = lock;
      r.boughtBtn.disabled = lock;
    }
    if (m.phase === 'sell' && r.soldBtn) {
      r.soldBtn.disabled = !!m.busy;
      r.sellInput.disabled = !!m.busy;
    }
  }

  function submitEntered() {
    var m = state.manual;
    if (!m || m.busy || m.phase !== 'buy' || manualExpired(m)) return;
    var price = num(m.refs.priceInput.value);
    var qty = num(m.refs.qtyInput.value);
    if (!(price > 0) || !(qty > 0)) { m.refs.formError.textContent = 'Enter the real fill price and the capital you used (both > 0).'; return; }
    m.busy = true;
    m.refs.formError.textContent = '';
    updateManualLive();
    api('POST', oppPath(m.id, '/manual/entered'), { price: price, quote_amount: qty }).then(function (r) {
      if (state.manual !== m) return;
      m.busy = false;
      if (!r.ok) { m.refs.formError.textContent = r.error; updateManualLive(); return; }
      m.entered = Object.assign({ entry_price: price, quote_amount: qty }, r.data || {});
      m.phase = 'sell';
      renderManual(true);
      scheduleRender('cards');
    });
  }

  function submitExited() {
    var m = state.manual;
    if (!m || m.busy || m.phase !== 'sell') return;
    var price = num(m.refs.sellInput.value);
    if (!(price > 0)) { m.refs.formError.textContent = 'Enter the real exit price (> 0).'; return; }
    m.busy = true;
    m.refs.formError.textContent = '';
    updateManualLive();
    api('POST', oppPath(m.id, '/manual/exited'), { price: price }).then(function (r) {
      if (state.manual !== m) return;
      m.busy = false;
      if (!r.ok) { m.refs.formError.textContent = r.error; updateManualLive(); return; }
      m.trade = r.data || {};
      m.phase = 'done';
      stopManualPoll();
      if (m.trade.id) { state.trades.set(m.trade.id, m.trade); scheduleRender('trades'); }
      renderManual(true);
      scheduleRender('cards');
      toast('Manual trade recorded: ' + fmtMoney(m.trade.realized_pnl_quote, quoteOf({ target_symbol: m.trade.symbol }), { sign: true }), num(m.trade.realized_pnl_quote) >= 0 ? 'ok' : 'warn');
    });
  }

  function cancelManualOpp() {
    var m = state.manual;
    if (!m || m.phase !== 'buy') return;
    confirmDialog({ title: 'Cancel this opportunity?', body: 'It is removed from the active list (no trade is recorded).', okLabel: 'Cancel opportunity', okClass: 'btn-warn' }).then(function (ok) {
      if (!ok || state.manual !== m) return;
      api('POST', oppPath(m.id, '/cancel'), { reason: 'user cancelled in MANUAL dialog' }).then(function (r) {
        if (!r.ok) toast('Cancel: ' + r.error, 'err');
        stopManualPoll();
        hideModal('manual');
        state.manual = null;
        scheduleRender('cards');
      });
    });
  }

  function closeManual() {
    var m = state.manual;
    stopManualPoll();
    hideModal('manual');
    if (m && m.phase !== 'sell') state.manual = null;
    scheduleRender('cards');
  }

  // ------------------------------------------------------------------ AUTO modal
  function openAuto(id) {
    var o = getOpp(id);
    if (!o) return;
    if (state.emergency) { toast('EMERGENCY STOP is active: AUTO is blocked.', 'warn'); return; }
    state.auto = { id: id, phase: 'loading', snapshot: null, result: null, trade: null, error: '', busy: false };
    showModal('auto');
    renderAuto();
    api('POST', oppPath(id, '/auto')).then(function (r) {
      var a = state.auto;
      if (!a || a.id !== id) return;
      if (!r.ok) { a.phase = 'error'; a.error = r.error; renderAuto(); return; }
      var d = r.data || {};
      if (d.confirmation_required) {
        a.phase = 'confirm';
        a.snapshot = d.snapshot || {};
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

  function cancelAuto() {
    var a = state.auto;
    if (!a) { hideModal('auto'); return; }
    api('POST', oppPath(a.id, '/cancel'), { reason: 'user cancelled AUTO' }).then(function (r) {
      if (!r.ok) toast('Cancel: ' + r.error, 'err');
      hideModal('auto');
      state.auto = null;
      scheduleRender('cards');
    });
  }

  function closeAuto() {
    hideModal('auto');
    if (state.auto && (state.auto.phase === 'done' || state.auto.phase === 'error')) state.auto = null;
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
    if (m && m.id === o.id && isTerminal(o.state) && m.phase === 'buy') {
      m.support = Object.assign({}, m.support, { valid: false, expired: true, message: '⚠️ OPPORTUNITY ' + o.state });
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
      row('RISK', risk.ok === false ? '🔴 ' + (risk.reason || 'blocked') : '🟢 OK' + (Number.isFinite(num(risk.allowed)) ? ' · allowed ' + fmtMoney(risk.allowed, q) : ''), risk.ok === false ? 'neg' : 'pos');
      row('BALANCE', Number.isFinite(num(bal.free)) ? fmtMoney(bal.free, bal.quote || q) + ' free' : '–');
      if (snap.entry_price !== undefined) row('ENTRY', fmtMoney(snap.entry_price, q));
      if (snap.exit_target !== undefined) row('EXIT TARGET', fmtMoney(snap.exit_target, q));
      if (snap.remaining_s !== undefined) row('WINDOW', fmtSecs(snap.remaining_s));
      row('REVALIDATION', snap.valid === false ? '🔴 ' + (snap.reason || 'not valid now') : '🟢 ' + (snap.reason || 'valid on the latest book'), snap.valid === false ? 'neg' : 'pos');
      Object.keys(snap).forEach(function (k) { if (!AUTO_KNOWN[k]) row(humanize(k), fmtAny(snap[k], k)); });
      body.appendChild(h('div', { class: 'block' }, h('div', { class: 'block-title', text: 'Confirm execution (' + String(state.mode).toUpperCase() + ')' }), rows));
      body.appendChild(h('p', { class: 'hint', text: 'The bot revalidates on the latest book right before the entry and before the exit. If the edge is gone it cancels instead of executing a stale snapshot.' }));
      var invalid = snap.valid === false;
      body.appendChild(h('div', { class: 'modal-actions' },
        h('button', { class: 'btn btn-ghost', type: 'button', text: 'CANCEL', disabled: a.busy, onclick: cancelAuto }),
        h('button', { class: 'btn btn-ghost', type: 'button', text: 'RE-CHECK', disabled: a.busy, onclick: function () { openAuto(a.id); } }),
        h('button', { class: 'btn btn-auto', type: 'button', text: a.busy ? 'EXECUTING…' : 'EXECUTE', disabled: a.busy || invalid, onclick: confirmAuto })));
      return;
    }
    // executing / done
    var res = a.result || {};
    var t = a.trade || res.trade || {};
    var tq = quoteOf({ target_symbol: t.symbol }) || q;
    var stateText = t.state || res.state || (a.phase === 'done' ? 'DONE' : 'EXECUTING');
    var ok = res.ok !== false && !(t.state === 'FAILED' || t.state === 'CANCELLED');
    body.appendChild(h('div', { class: 'alert ' + (a.phase === 'done' ? (ok ? 'ok' : 'danger') : 'warn'), text: (a.phase === 'done' ? (ok ? '✅ ' : '⛔ ') : '⏳ ') + stateText + (res.reason ? ' · ' + res.reason : '') }));
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
    body.appendChild(h('div', { class: 'modal-actions' },
      h('button', { class: 'btn btn-ghost', type: 'button', text: a.phase === 'done' ? 'Close' : 'Hide (keeps running)', onclick: closeAuto })));
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
      title: '🛑 EMERGENCY STOP',
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

  function readSettingsPatch() {
    var form = $('settings-form');
    var s = state.settings || {};
    var patch = {};
    SETTINGS_FIELDS.forEach(function (name) {
      var el = form.elements[name];
      if (!el) return;
      if (el.type === 'checkbox') {
        if (el.checked !== !!s[name]) patch[name] = el.checked;
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
        setTimeout(function () { el.classList.remove('focus-flash'); }, 2500);
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
      var val = el.querySelector('[data-role="window-val"]');
      if (val) val.textContent = rem > 0 ? '~' + rem.toFixed(1) + ' sec' : '0.0 sec · expiring';
      var bar = el.querySelector('[data-role="bar"]');
      if (bar) {
        var frac = est > 0 ? Math.max(0, Math.min(1, rem / est)) : 0;
        bar.style.width = (frac * 100).toFixed(1) + '%';
        bar.className = 'bar-fill ' + barClass(rem, o);
        var pb = bar.parentNode;
        if (pb) pb.setAttribute('aria-valuenow', String(Math.round(frac * 100)));
      }
      if (rem <= 0 && !o._zeroed) { o._zeroed = true; zeroed = true; }
    });
    if (zeroed) scheduleRender('cards');
    if (state.manual && !modalHidden('manual')) updateManualLive();
  }

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
      state.filters = { quick: 'ALL', exchange: '', asset: '', minNet: '', minWindow: '', human: '', bot: '' };
      ['f-exchange', 'f-asset', 'f-minnet', 'f-minwin', 'f-human', 'f-bot'].forEach(function (id) { $(id).value = ''; });
      state.focusId = null;
      scheduleRender('cards');
    });
    $('trades-refresh').addEventListener('click', loadTrades);
    $('stats-refresh').addEventListener('click', loadStats);

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
      if (!modalHidden('auto')) { closeAuto(); return; }
      if ($('settings-drawer').classList.contains('open')) closeDrawer();
    });
    window.addEventListener('hashchange', handleHash);
  }

  function init() {
    bindUi();
    renderAll();
    api('GET', '/api/state').then(function (r) {
      if (r.ok) applyState(r.data);
      return api('GET', '/api/opportunities?state=all&limit=500');
    }).then(function (r) {
      if (r && r.ok) (r.data || []).forEach(putOpp);
      renderAll();
      handleHash();
    });
    connectWs();
    setInterval(tick, 250);
  }

  // Debug handle for manual QA in the browser console (read-only use).
  window.latbot = { state: state, fmtMoney: fmtMoney, fmtPct: fmtPct, fmtSecs: fmtSecs, remaining: remaining, computeBest: computeBest };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
