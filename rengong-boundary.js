(function () {
  const API_BASE = "";
  const W1_START_MIN = 19 * 60 + 30;

  const state = {
    wave: "W1",
    stores: [],
    routes: [],
    rules: [],
    storeOrder: [],
    routeOrder: [],
    sort: { store: null, route: { key: "delivery_date", state: "desc", fallback: [{ key: "route_id", state: "asc" }] } },
  };

  const els = {
    waveSelector: document.getElementById("waveSelector"),
    waveMsg: document.getElementById("waveMsg"),
    reloadBtn: document.getElementById("reloadBtn"),
    storeIdSearch: document.getElementById("storeIdSearch"),
    storeNameSearch: document.getElementById("storeNameSearch"),
    storeTimeStart: document.getElementById("storeTimeStart"),
    storeTimeEnd: document.getElementById("storeTimeEnd"),
    storeMinValidSamples: document.getElementById("storeMinValidSamples"),
    storeBody: document.getElementById("storeBody"),
    routeIdSearch: document.getElementById("routeIdSearch"),
    routeNameSearch: document.getElementById("routeNameSearch"),
    vehicleSearch: document.getElementById("vehicleSearch"),
    routeDateStart: document.getElementById("routeDateStart"),
    routeDateEnd: document.getElementById("routeDateEnd"),
    routeCategoryFilter: document.getElementById("routeCategoryFilter"),
    routeStatusFilter: document.getElementById("routeStatusFilter"),
    routeBody: document.getElementById("routeBody"),
    rulesWrap: document.getElementById("rulesWrap"),
  };

  const STORE_COLUMNS = {
    store_id: "number",
    store_name: "text",
    earliest_arrival_time: "w1time",
    avg_arrival_time: "w1time",
    latest_arrival_time: "w1time",
    early_allowed_minutes: "number",
    late_allowed_minutes: "number",
    valid_sample_count: "number",
  };
  const ROUTE_COLUMNS = {
    delivery_date: "date",
    vehicle_id: "text",
    route_id: "number",
    route_name: "text",
    route_category: "text",
    store_count: "number",
    route_load_w1: "number",
    normal_hard_limit: "number",
    is_airport_route: "bool",
    is_over_normal_limit: "bool",
    is_dirty_candidate: "bool",
  };

  function escapeHtml(v) {
    return String(v ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function toCnBool(v) {
    if (v === true) return "鏄?;
    if (v === false) return "鍚?;
    return "-";
  }

  function toNum(v, d = 3) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "-";
    return n.toFixed(d);
  }

  function parseDateVal(v) {
    const t = String(v || "").trim();
    if (!t) return -Infinity;
    const dt = new Date(t);
    return Number.isFinite(dt.getTime()) ? dt.getTime() : -Infinity;
  }

  function parseHm(v) {
    const m = String(v || "").trim().match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const h = Number(m[1]);
    const mm = Number(m[2]);
    if (h < 0 || h > 23 || mm < 0 || mm > 59) return null;
    return h * 60 + mm;
  }

  // W1璺ㄥ崍澶滄帓搴忥細19:30~23:59 褰撳ぉ锛?0:00~08:00 娆℃棩
  function parseW1TimeSort(v) {
    const m = parseHm(v);
    if (m == null) return Number.POSITIVE_INFINITY;
    if (m >= W1_START_MIN) return m - W1_START_MIN;
    if (m <= 8 * 60) return (24 * 60 - W1_START_MIN) + m;
    return Number.POSITIVE_INFINITY - 1;
  }

  function cmpByType(a, b, type) {
    if (type === "number") return Number(a || 0) - Number(b || 0);
    if (type === "date") return parseDateVal(a) - parseDateVal(b);
    if (type === "w1time") return parseW1TimeSort(a) - parseW1TimeSort(b);
    if (type === "bool") return (a === true ? 1 : 0) - (b === true ? 1 : 0);
    return String(a || "").localeCompare(String(b || ""), "zh-CN");
  }

  function cycleSort(table, key) {
    const cur = state.sort[table];
    if (!cur || cur.key !== key) {
      state.sort[table] = { key, state: "asc" };
      return;
    }
    if (cur.state === "asc") cur.state = "desc";
    else if (cur.state === "desc") state.sort[table] = null;
    else cur.state = "asc";
  }

  function applySortIndicators() {
    document.querySelectorAll("th[data-table]").forEach((th) => {
      const table = th.getAttribute("data-table");
      const key = th.getAttribute("data-key");
      const cfg = state.sort[table];
      if (cfg && cfg.key === key) th.setAttribute("data-sort-state", cfg.state || "");
      else th.removeAttribute("data-sort-state");
    });
  }

  function sortRows(rows, table, columns, defaultOrder) {
    const cfg = state.sort[table];
    if (!cfg) return rows.slice().sort((a, b) => defaultOrder(a, b));
    const type = columns[cfg.key] || "text";
    const factor = cfg.state === "desc" ? -1 : 1;
    return rows.slice().sort((a, b) => {
      const c = cmpByType(a[cfg.key], b[cfg.key], type);
      if (c !== 0) return c * factor;
      return defaultOrder(a, b);
    });
  }

  async function fetchData() {
    const query = new URLSearchParams({ wave: state.wave }).toString();
    const resp = await fetch(`${API_BASE}/rengong/boundary-page-data?${query}`, { cache: "no-store" });
    const json = await resp.json();
    if (!resp.ok || json.ok === false) throw new Error(json.error || "鍔犺浇澶辫触");
    state.stores = (json.store_arrival_windows || []).map((x, i) => ({ ...x, __idx: i }));
    state.routes = (json.route_load_profiles || []).map((x, i) => ({ ...x, __idx: i }));
    state.rules = json.load_rules || [];
    state.storeOrder = state.stores.slice().sort((a, b) => parseW1TimeSort(a.avg_arrival_time) - parseW1TimeSort(b.avg_arrival_time)).map((x) => x.__idx);
    state.routeOrder = state.routes.slice().sort((a, b) => {
      const d = parseDateVal(b.delivery_date) - parseDateVal(a.delivery_date);
      if (d !== 0) return d;
      return Number(a.route_id || 0) - Number(b.route_id || 0);
    }).map((x) => x.__idx);
    state.sort.store = null;
    state.sort.route = null;
    els.waveMsg.textContent = json.message || "";
  }

  function defaultStoreOrder(a, b) {
    return state.storeOrder.indexOf(a.__idx) - state.storeOrder.indexOf(b.__idx);
  }
  function defaultRouteOrder(a, b) {
    return state.routeOrder.indexOf(a.__idx) - state.routeOrder.indexOf(b.__idx);
  }

  function renderStoreTable() {
    const idQ = String(els.storeIdSearch.value || "").trim().toLowerCase();
    const nameQ = String(els.storeNameSearch.value || "").trim().toLowerCase();
    const tStart = parseHm(els.storeTimeStart.value);
    const tEnd = parseHm(els.storeTimeEnd.value);
    const minSamples = Number(els.storeMinValidSamples.value || "");

    let rows = state.stores.filter((r) => {
      if (idQ && !String(r.store_id || "").toLowerCase().includes(idQ)) return false;
      if (nameQ && !String(r.store_name || "").toLowerCase().includes(nameQ)) return false;
      if (Number.isFinite(minSamples) && minSamples >= 0 && Number(r.valid_sample_count || 0) < minSamples) return false;
      if (tStart != null || tEnd != null) {
        const m = parseHm(r.avg_arrival_time);
        if (m == null) return false;
        if (tStart != null && m < tStart) return false;
        if (tEnd != null && m > tEnd) return false;
      }
      return true;
    });

    rows = sortRows(rows, "store", STORE_COLUMNS, defaultStoreOrder);
    if (!rows.length) {
      els.storeBody.innerHTML = '<tr><td colspan="8" class="bd-empty">鏃犲尮閰嶆暟鎹?/td></tr>';
      return;
    }
    els.storeBody.innerHTML = rows.map((r) => `
      <tr>
        <td title="${escapeHtml(r.store_id)}">${escapeHtml(r.store_id)}</td>
        <td title="${escapeHtml(r.store_name)}">${escapeHtml(r.store_name)}</td>
        <td>${escapeHtml(r.earliest_arrival_time || "-")}</td>
        <td>${escapeHtml(r.avg_arrival_time || "-")}</td>
        <td>${escapeHtml(r.latest_arrival_time || "-")}</td>
        <td>${toNum(r.early_allowed_minutes, 1)}</td>
        <td>${toNum(r.late_allowed_minutes, 1)}</td>
        <td>${escapeHtml(r.valid_sample_count ?? "-")}</td>
      </tr>
    `).join("");
  }

  function renderRouteTable() {
    const routeQ = String(els.routeIdSearch.value || "").trim().toLowerCase();
    const routeNameQ = String(els.routeNameSearch.value || "").trim().toLowerCase();
    const vehQ = String(els.vehicleSearch.value || "").trim().toLowerCase();
    const ds = String(els.routeDateStart.value || "").trim();
    const de = String(els.routeDateEnd.value || "").trim();
    const cat = String(els.routeCategoryFilter.value || "all");
    const status = String(els.routeStatusFilter.value || "all");

    let rows = state.routes.filter((r) => {
      if (routeQ && !String(r.route_id || "").toLowerCase().includes(routeQ)) return false;
      if (routeNameQ && !String(r.route_name || "").toLowerCase().includes(routeNameQ)) return false;
      if (vehQ && !String(r.vehicle_id || "").toLowerCase().includes(vehQ)) return false;
      if (ds && String(r.delivery_date || "") < ds) return false;
      if (de && String(r.delivery_date || "") > de) return false;
      if (cat === "normal" && String(r.route_category || "").trim() !== "normal") return false;
      if (cat === "airport" && String(r.route_category || "").trim() !== "棣栭兘鏈哄満") return false;
      if (status === "over" && !r.is_over_normal_limit) return false;
      if (status === "airport" && !r.is_airport_route) return false;
      if (status === "dirty" && !r.is_dirty_candidate) return false;
      return true;
    });
    rows = sortRows(rows, "route", ROUTE_COLUMNS, defaultRouteOrder);
    if (!rows.length) {
      els.routeBody.innerHTML = '<tr><td colspan="11" class="bd-empty">鏃犲尮閰嶆暟鎹?/td></tr>';
      return;
    }
    els.routeBody.innerHTML = rows.map((r) => `
      <tr>
        <td>${escapeHtml(r.delivery_date || "-")}</td>
        <td title="${escapeHtml(r.vehicle_id || "-")}">${escapeHtml(r.vehicle_id || "-")}</td>
        <td>${escapeHtml(r.route_id || "-")}</td>
        <td title="${escapeHtml(r.route_name || "-")}">${escapeHtml(r.route_name || "-")}</td>
        <td>${String(r.route_category || "").trim() === "棣栭兘鏈哄満" ? "棣栭兘鏈哄満" : "鏅€?}</td>
        <td>${escapeHtml(r.store_count ?? "-")}</td>
        <td>${toNum(r.route_load_w1, 3)}</td>
        <td>${toNum(r.normal_hard_limit, 3)}</td>
        <td>${toCnBool(r.is_airport_route)}</td>
        <td>${toCnBool(r.is_over_normal_limit)}</td>
        <td>${toCnBool(r.is_dirty_candidate)}</td>
      </tr>
    `).join("");
  }

  function renderRules() {
    const typeMap = { normal: "鏅€氱嚎璺?, airport: "棣栭兘鏈哄満绾胯矾", dirty_filter: "鑴忔暟鎹繃婊? };
    const categoryMap = { normal: "鏅€氱嚎璺?, "棣栭兘鏈哄満": "棣栭兘鏈哄満" };
    const formatPreferred = (arr) => {
      if (!Array.isArray(arr) || !arr.length) return "-";
      return arr.join("銆?);
    };
    if (!state.rules.length) {
      els.rulesWrap.innerHTML = '<div class="bd-empty">鏆傛棤瑙勫垯鏁版嵁</div>';
      return;
    }
    els.rulesWrap.innerHTML = state.rules.map((r) => `
      <article class="bd-card">
        <h3>${escapeHtml(typeMap[r.rule_type] || r.rule_type || "-")}</h3>
        <div class="bd-kv"><span class="bd-k">绾胯矾绫诲瀷</span><span class="bd-v">${escapeHtml(categoryMap[r.route_category] || r.route_category || "-")}</span></div>
        <div class="bd-kv"><span class="bd-k">瑁呰浇纭笂闄?/span><span class="bd-v">${r.hard_load_limit == null ? "-" : escapeHtml(r.hard_load_limit)}</span></div>
        <div class="bd-kv"><span class="bd-k">鏄惁蹇界暐瑁呰浇涓婇檺</span><span class="bd-v">${toCnBool(r.ignore_load_limit)}</span></div>
        <div class="bd-kv"><span class="bd-k">浼樺厛杞﹁締</span><span class="bd-v">${escapeHtml(formatPreferred(r.preferred_vehicle_ids))}</span></div>
        <div class="bd-kv"><span class="bd-k">鏄惁鍏佽澶氳稛</span><span class="bd-v">${toCnBool(r.allow_multi_trip)}</span></div>
        <div class="bd-kv"><span class="bd-k">鑴忔暟鎹槇鍊?/span><span class="bd-v">${r.dirty_load_threshold == null ? "-" : escapeHtml(r.dirty_load_threshold)}</span></div>
        <div class="bd-kv"><span class="bd-k">鏄惁鎺掗櫎鏈哄満绾?/span><span class="bd-v">${toCnBool(r.dirty_exclude_airport)}</span></div>
        <div class="bd-kv bd-kv-desc">
          <span class="bd-k">瑙勫垯璇存槑</span>
          <span class="bd-v bd-v-desc">${escapeHtml(r.rule_desc || "-")}</span>
        </div>
      </article>
    `).join("");
  }

  function renderAll() {
    renderRules();
    renderStoreTable();
    renderRouteTable();
    applySortIndicators();
  }

  function bindSortEvents() {
    document.querySelectorAll("th[data-table]").forEach((th) => {
      th.addEventListener("click", () => {
        const table = th.getAttribute("data-table");
        const key = th.getAttribute("data-key");
        cycleSort(table, key);
        if (table === "store") renderStoreTable();
        else renderRouteTable();
        applySortIndicators();
      });
    });
  }

  async function reload() {
    try {
      await fetchData();
      renderAll();
    } catch (e) {
      els.waveMsg.textContent = e.message || "鍔犺浇澶辫触";
    }
  }

  els.waveSelector.addEventListener("change", () => {
    state.wave = els.waveSelector.value;
    reload();
  });
  els.reloadBtn.addEventListener("click", reload);

  [
    els.storeIdSearch, els.storeNameSearch, els.storeTimeStart, els.storeTimeEnd, els.storeMinValidSamples,
  ].forEach((el) => el.addEventListener("input", renderStoreTable));

  [
    els.routeIdSearch, els.routeNameSearch, els.vehicleSearch, els.routeDateStart, els.routeDateEnd,
  ].forEach((el) => el.addEventListener("input", renderRouteTable));
  [els.routeCategoryFilter, els.routeStatusFilter].forEach((el) => el.addEventListener("change", renderRouteTable));

  bindSortEvents();
  reload();
})();

