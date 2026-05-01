(function () {
  const API_BASE = "";

  const els = {
    refreshBtn: document.getElementById("refreshBtn"),
    loadDayBtn: document.getElementById("loadDayBtn"),
    dayInput: document.getElementById("dayInput"),
    searchInput: document.getElementById("searchInput"),
    solverReadySelect: document.getElementById("solverReadySelect"),
    queryState: document.getElementById("queryState"),
    summaryCards: document.getElementById("summaryCards"),
    waveBreakdown: document.getElementById("waveBreakdown"),

    storeGroupMeta: document.getElementById("storeGroupMeta"),
    storeGroupSummary: document.getElementById("storeGroupSummary"),
    storeGroupBody: document.getElementById("storeGroupBody"),

    dispatchSimMeta: document.getElementById("dispatchSimMeta"),
    dispatchSimSummary: document.getElementById("dispatchSimSummary"),
    dispatchSimBody: document.getElementById("dispatchSimBody"),

    singleRouteMeta: document.getElementById("singleRouteMeta"),
    singleRouteSummary: document.getElementById("singleRouteSummary"),
    singleRouteBody: document.getElementById("singleRouteBody"),

    singleRoutePortraitMeta: document.getElementById("singleRoutePortraitMeta"),
    portraitCategoryBody: document.getElementById("portraitCategoryBody"),
    portraitDistributionBody: document.getElementById("portraitDistributionBody"),
    humanTemplateMeta: document.getElementById("humanTemplateMeta"),
    humanTemplateSummary: document.getElementById("humanTemplateSummary"),
    humanTemplateDoubleBody: document.getElementById("humanTemplateDoubleBody"),
    humanTemplateSingleBody: document.getElementById("humanTemplateSingleBody"),
    humanTemplateVehicleBody: document.getElementById("humanTemplateVehicleBody"),

    multiMeta: document.getElementById("multiMeta"),
    singleMeta: document.getElementById("singleMeta"),
    vehicleMeta: document.getElementById("vehicleMeta"),
    multiBody: document.getElementById("multiBody"),
    singleBody: document.getElementById("singleBody"),
    vehicleBody: document.getElementById("vehicleBody"),
  };

  const state = {
    selectedDate: "",
    query: "",
    solverReady: "all",
    overview: null,
    rows: { multi: [], single: [], vehicle: [] },
    sort: {
      multi: { key: "shop_code", order: "asc" },
      single: { key: "shop_code", order: "asc" },
      vehicle: { key: "plateNo", order: "asc" },
    },
  };

  function escapeHtml(v) {
    return String(v ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatNumber(v, digits = 0) {
    const n = Number(v);
    return Number.isFinite(n) ? n.toFixed(digits) : "--";
  }

  function parseTimeToMinutes(value) {
    const text = String(value || "").trim();
    const m = text.match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return Number.POSITIVE_INFINITY;
    return Number(m[1]) * 60 + Number(m[2]);
  }

  function parseSortableValue(item, key) {
    const numberKeys = new Set([
      "shop_code", "delivery_count", "wave1_load", "wave2_load", "wave3_load", "wave4_load",
      "original_load", "total_resolved_load", "active_wave_load", "unload_minutes",
      "difficulty", "tolerate_minutes", "solver_ready_flag", "capacity", "speed", "canCold",
    ]);
    const timeKeys = new Set(["first_wave_time", "second_wave_time", "expected_time"]);
    if (numberKeys.has(key)) {
      const n = Number(item[key]);
      return Number.isFinite(n) ? n : Number.NEGATIVE_INFINITY;
    }
    if (timeKeys.has(key)) return parseTimeToMinutes(item[key]);
    return String(item[key] ?? "").toLowerCase();
  }

  function sortRows(rows, table) {
    const cfg = state.sort[table];
    if (!cfg || !cfg.key) return rows.slice();
    const factor = cfg.order === "desc" ? -1 : 1;
    return rows.slice().sort((a, b) => {
      const av = parseSortableValue(a, cfg.key);
      const bv = parseSortableValue(b, cfg.key);
      if (av < bv) return -1 * factor;
      if (av > bv) return 1 * factor;
      return (Number(a.shop_code) || 0) - (Number(b.shop_code) || 0);
    });
  }

  function splitTokens(value) {
    return String(value || "")
      .split(/[,\s|锛屻€乚+/)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function splitDrivers(value) {
    return String(value || "")
      .split(/\s*\|\s*|[,锛屻€乗s]+/)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function buildVehicleRows(rows) {
    const map = new Map();
    for (const row of rows || []) {
      const plateCandidates = splitTokens(row.source_vehicle_ids);
      if (row.plate_no) plateCandidates.push(String(row.plate_no).trim());
      const drivers = splitDrivers(row.source_drivers);
      const fallbackDriver = drivers.length === 1 ? drivers[0] : "--";
      plateCandidates.forEach((plate, idx) => {
        if (!plate) return;
        const driverName = drivers[idx] || fallbackDriver || "--";
        if (!map.has(plate)) {
          map.set(plate, {
            plateNo: plate,
            driverName,
            type: "4.2绫冲帰寮忚揣杞?,
            capacity: 100,
            speed: 38,
            canCold: 0,
            status: "鏈娇鐢?,
          });
        } else {
          const old = map.get(plate);
          if ((old.driverName === "--" || !old.driverName) && driverName !== "--") {
            old.driverName = driverName;
          }
        }
      });
    }
    return Array.from(map.values());
  }

  async function fetchJson(path, query = "") {
    const url = `${API_BASE}${path}${query ? `?${query}` : ""}`;
    const response = await fetch(url, { cache: "no-store" });
    const result = await response.json();
    if (!response.ok || result.ok === false) {
      throw new Error(result.error || `HTTP ${response.status}`);
    }
    return result;
  }

  function buildDayQuery() {
    const params = new URLSearchParams();
    if (state.selectedDate) params.set("date", state.selectedDate);
    if (state.query) params.set("q", state.query);
    if (state.solverReady !== "all") params.set("solverReady", state.solverReady);
    return params.toString();
  }

  function cellBox(value, cls = "") {
    return `<span class="rg-cell-box ${cls}">${escapeHtml(value)}</span>`;
  }

  function rowReadyBadge(item) {
    return Number(item.solver_ready_flag) === 1
      ? '<span class="rg-cell-box rg-cell-box-status rg-cell-box-ready">鍙洿鎺ユ眰瑙?/span>'
      : '<span class="rg-cell-box rg-cell-box-status rg-cell-box-miss">寰呰ˉ鏁版嵁</span>';
  }

  function buildSummaryCards(day) {
    const cards = [
      { label: "褰撳ぉ搴楅摵鏁?, value: formatNumber(day.total_rows, 0), sub: `鍙洿鎺ユ眰瑙?${formatNumber(day.ready_rows, 0)} 瀹禶 },
      { label: "寰呰ˉ鏁版嵁", value: formatNumber(day.not_ready_rows, 0), sub: `鍗犳瘮 ${formatNumber(day.total_rows ? (day.not_ready_rows / day.total_rows) * 100 : 0, 1)}%` },
      { label: "鍘熷璐ч噺鍚堣", value: formatNumber(day.original_load_sum, 3), sub: "淇濈暀浜哄伐鍘熷璐ч噺鍙ｅ緞" },
      { label: "鎶樼畻璐ч噺鍚堣", value: formatNumber(day.resolved_load_sum, 6), sub: "涓庡師姹傝В鍙ｅ緞涓€鑷? },
    ];
    els.summaryCards.innerHTML = cards.map((c) => `
      <article class="rg-kpi-card">
        <div class="rg-kpi-label">${c.label}</div>
        <div class="rg-kpi-value">${c.value}</div>
        <div class="rg-kpi-sub">${c.sub}</div>
      </article>
    `).join("");
  }

  function renderWaveBreakdown(items) {
    if (!items || !items.length) {
      els.waveBreakdown.innerHTML = '<div class="rg-empty">褰撳ぉ娌℃湁娉㈡鏁版嵁</div>';
      return;
    }
    els.waveBreakdown.innerHTML = items.map((item) => `
      <article class="rg-stack-item">
        <div class="rg-stack-head">
          <div class="rg-stack-title">${escapeHtml(item.wave_belongs || "鏈槧灏?)}</div>
          <div class="rg-stack-meta">${formatNumber(item.row_count, 0)} 瀹讹紝宸插氨缁?${formatNumber(item.ready_rows, 0)} 瀹?/div>
        </div>
        <div class="rg-stack-meta">鍘熷璐ч噺 ${formatNumber(item.original_load_sum, 3)} 锝?鎶樼畻璐ч噺 ${formatNumber(item.resolved_load_sum, 6)}</div>
      </article>
    `).join("");
  }

  function renderStoreGroupSummary(stats) {
    const s = stats || {};
    const cards = [
      { label: "鎬婚棬搴楃粍鏁伴噺", value: formatNumber(s.total_groups, 0), sub: "鍚屾棩鍚岃溅 route_id=X 涓?X+100" },
      { label: "骞冲潎姣忕粍搴楁暟", value: formatNumber(s.avg_store_count, 2), sub: "鍘婚噸搴楅摵闆嗗悎骞冲潎鍊? },
      { label: "鏈€澶у簵鏁?, value: formatNumber(s.max_store_count, 0), sub: "鍗曠粍涓婇檺" },
      { label: "鏈€灏忓簵鏁?, value: formatNumber(s.min_store_count, 0), sub: "鍗曠粍涓嬮檺" },
      { label: "骞冲潎浜岄厤澧炲姞姣斾緥", value: `${(Number(s.avg_second_extra_ratio || 0) * 100).toFixed(2)}%`, sub: "鎸夛紙浜岄厤鏂板搴楁暟 / 涓€閰嶅簵鏁帮級璁＄畻" },
      { label: "浜岄厤姣斾竴閰嶅20%浠ヤ笂缁勬暟", value: formatNumber(s.second_extra_over_20_count, 0), sub: "second_extra_ratio > 20%" },
      { label: "浜岄厤绛変簬涓€閰嶇粍鏁?, value: formatNumber(s.second_equals_first_count, 0), sub: "second_store_count = first_store_count" },
    ];
    els.storeGroupSummary.innerHTML = cards.map((c) => `
      <article class="rg-kpi-card rg-kpi-card-compact">
        <div class="rg-kpi-label">${c.label}</div>
        <div class="rg-kpi-value">${c.value}</div>
        <div class="rg-kpi-sub">${c.sub}</div>
      </article>
    `).join("");
  }

  function renderStoreGroupRows(items) {
    if (!items || !items.length) {
      els.storeGroupBody.innerHTML = '<tr><td colspan="13" class="rg-empty">褰撳ぉ娌℃湁鍙岄厤闂ㄥ簵缁?/td></tr>';
      return;
    }
    els.storeGroupBody.innerHTML = items.map((item) => {
      const ids = Array.isArray(item.store_ids) ? item.store_ids : [];
      const shortText = ids.length > 20 ? `${ids.slice(0, 20).join(",")} ...` : ids.join(",");
      return `
        <tr>
          <td>${escapeHtml(item.delivery_date || "--")}</td>
          <td>${cellBox(item.vehicle_id || "--", "rg-cell-box-vehicle")}</td>
          <td>${cellBox(item.pair_label || "--", "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.first_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.second_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.overlap_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.union_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(`${(Number(item.jaccard || 0) * 100).toFixed(2)}%`, "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.core_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.extra_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(`${(Number(item.second_extra_ratio || 0) * 100).toFixed(2)}%`, "rg-cell-box-small")}</td>
          <td>${cellBox(`${(Number(item.second_vs_first_ratio || 0) * 100).toFixed(2)}%`, "rg-cell-box-small")}</td>
          <td title="${escapeHtml(ids.join(","))}">${escapeHtml(shortText || "-")}</td>
        </tr>
      `;
    }).join("");
  }

  function renderDispatchSimSummary(stats) {
    const s = stats || {};
    const cards = [
      { label: "鎬婚棬搴楃粍鏁伴噺", value: formatNumber(s.total_groups, 0), sub: "鍙浆鎹㈣皟搴﹁緭鍏ョ殑缁勬暟" },
      { label: "骞冲潎鎬诲簵鏁?, value: formatNumber(s.avg_total_store_count, 2), sub: "姣忕粍涓€閰?浜岄厤鍚堝苟鍚庡簵鏁? },
      { label: "鏈€澶ф€诲簵鏁?, value: formatNumber(s.max_total_store_count, 0), sub: "鍗曠粍涓婇檺" },
      { label: "鏈€灏忔€诲簵鏁?, value: formatNumber(s.min_total_store_count, 0), sub: "鍗曠粍涓嬮檺" },
    ];
    els.dispatchSimSummary.innerHTML = cards.map((c) => `
      <article class="rg-kpi-card rg-kpi-card-compact">
        <div class="rg-kpi-label">${c.label}</div>
        <div class="rg-kpi-value">${c.value}</div>
        <div class="rg-kpi-sub">${c.sub}</div>
      </article>
    `).join("");
  }

  function renderDispatchSimRows(items) {
    if (!items || !items.length) {
      els.dispatchSimBody.innerHTML = '<tr><td colspan="5" class="rg-empty">褰撳ぉ娌℃湁鍙浆鎹㈢殑闂ㄥ簵缁?/td></tr>';
      return;
    }
    els.dispatchSimBody.innerHTML = items.map((item) => `
      <tr>
        <td>${cellBox(item.vehicle_id || "--", "rg-cell-box-vehicle")}</td>
        <td>${cellBox(item.pair_label || "--", "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave1_store_count, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave2_store_count, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.total_store_count, 0), "rg-cell-box-small")}</td>
      </tr>
    `).join("");
  }

  function renderSingleRouteSummary(stats) {
    const s = stats || {};
    const cards = [
      { label: "鍗曢厤绾胯矾鏁伴噺", value: formatNumber(s.total_routes, 0), sub: "鏈弬涓?X / X+100 鐨?route_id" },
      { label: "骞冲潎搴楁暟", value: formatNumber(s.avg_store_count, 2), sub: "鍘婚噸搴楁暟鍧囧€? },
      { label: "鏈€澶у簵鏁?, value: formatNumber(s.max_store_count, 0), sub: "鍗曟潯绾胯矾涓婇檺" },
      { label: "鏈€灏忓簵鏁?, value: formatNumber(s.min_store_count, 0), sub: "鍗曟潯绾胯矾涓嬮檺" },
    ];
    els.singleRouteSummary.innerHTML = cards.map((c) => `
      <article class="rg-kpi-card rg-kpi-card-compact">
        <div class="rg-kpi-label">${c.label}</div>
        <div class="rg-kpi-value">${c.value}</div>
        <div class="rg-kpi-sub">${c.sub}</div>
      </article>
    `).join("");
  }

  function renderSingleRouteRows(items) {
    if (!items || !items.length) {
      els.singleRouteBody.innerHTML = '<tr><td colspan="6" class="rg-empty">褰撳ぉ娌℃湁鍗曢厤绾胯矾</td></tr>';
      return;
    }
    els.singleRouteBody.innerHTML = items.map((item) => {
      const ids = Array.isArray(item.store_ids) ? item.store_ids : [];
      const shortText = ids.length > 20 ? `${ids.slice(0, 20).join(",")} ...` : ids.join(",");
      return `
        <tr>
          <td>${escapeHtml(item.delivery_date || "--")}</td>
          <td>${cellBox(item.vehicle_id || "--", "rg-cell-box-vehicle")}</td>
          <td>${cellBox(item.route_id || "--", "rg-cell-box-small")}</td>
          <td>${escapeHtml(item.route_name || "--")}</td>
          <td>${cellBox(formatNumber(item.store_count, 0), "rg-cell-box-small")}</td>
          <td title="${escapeHtml(ids.join(","))}">${escapeHtml(shortText || "-")}</td>
        </tr>
      `;
    }).join("");
  }

  function renderSingleRoutePortrait(portrait) {
    const p = portrait || {};
    const categories = Array.isArray(p.categories) ? p.categories : [];
    const d = p.distribution || {};

    if (!categories.length) {
      els.portraitCategoryBody.innerHTML = '<tr><td colspan="5" class="rg-empty">褰撳ぉ娌℃湁鍙垎鏋愮殑鍗曢厤绾胯矾</td></tr>';
    } else {
      els.portraitCategoryBody.innerHTML = categories.map((item) => `
        <tr>
          <td>${escapeHtml(item.category || "--")}</td>
          <td>${cellBox(formatNumber(item.count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.avg_store_count, 2), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.max_store_count, 0), "rg-cell-box-small")}</td>
          <td>${cellBox(formatNumber(item.min_store_count, 0), "rg-cell-box-small")}</td>
        </tr>
      `).join("");
    }

    const distRows = [
      { label: "鍗曞簵绾胯矾鏁伴噺锛?1搴楋級", value: d.single_store_routes },
      { label: "灏忕嚎璺紙<=3搴楋級", value: d.small_routes },
      { label: "涓嚎璺紙4~6搴楋級", value: d.medium_routes },
      { label: "澶х嚎璺紙>=7搴楋級", value: d.large_routes },
      { label: "鍗曢厤绾胯矾鎬绘暟", value: d.total_routes },
    ];
    els.portraitDistributionBody.innerHTML = distRows.map((item) => `
      <tr>
        <td>${escapeHtml(item.label)}</td>
        <td>${cellBox(formatNumber(item.value, 0), "rg-cell-box-small")}</td>
      </tr>
    `).join("");
  }

  function renderHumanStructureTemplate(template) {
    const t = template || {};
    const d = t.doubleTemplate || {};
    const s = t.singleTemplate || {};
    const v = t.vehicleUsageTemplate || {};
    const sizeDist = s.size_distribution || {};
    const categoryShare = Array.isArray(s.category_share) ? s.category_share : [];
    const firstDist = Array.isArray(d.first_store_count_distribution) ? d.first_store_count_distribution : [];
    const secondDist = Array.isArray(d.second_store_count_distribution) ? d.second_store_count_distribution : [];
    const overlapDist = Array.isArray(d.overlap_rate_distribution) ? d.overlap_rate_distribution : [];

    const summaryCards = [
      { label: "鍙岄厤妯℃澘鍧囧€硷紙涓€閰嶅簵鏁帮級", value: formatNumber(d.avg_first_store_count, 2), sub: "鍩轰簬 X / X+100 闂ㄥ簵缁? },
      { label: "鍙岄厤妯℃澘鍧囧€硷紙浜岄厤搴楁暟锛?, value: formatNumber(d.avg_second_store_count, 2), sub: "鍩轰簬 X / X+100 闂ㄥ簵缁? },
      { label: "骞冲潎浜岄厤鏂板姣斾緥", value: `${(Number(d.avg_second_extra_ratio || 0) * 100).toFixed(2)}%`, sub: "鎸夛紙浜岄厤鏂板搴楁暟 / 涓€閰嶅簵鏁帮級璁＄畻" },
      { label: "鍗曢厤绾胯矾鎬婚噺", value: formatNumber(sizeDist.total_routes, 0), sub: "鐢卞崟閰嶇嚎璺敾鍍忕粺璁? },
    ];
    if (els.humanTemplateSummary) {
      els.humanTemplateSummary.innerHTML = summaryCards.map((c) => `
        <article class="rg-kpi-card rg-kpi-card-compact">
          <div class="rg-kpi-label">${c.label}</div>
          <div class="rg-kpi-value">${c.value}</div>
          <div class="rg-kpi-sub">${c.sub}</div>
        </article>
      `).join("");
    }

    const doubleRows = [
      { k: "骞冲潎涓€閰嶅簵鏁?, v: formatNumber(d.avg_first_store_count, 2) },
      { k: "骞冲潎浜岄厤搴楁暟", v: formatNumber(d.avg_second_store_count, 2) },
      { k: "骞冲潎浜岄厤鏂板姣斾緥", v: `${(Number(d.avg_second_extra_ratio || 0) * 100).toFixed(2)}%` },
      { k: "涓€閰嶅簵鏁板垎甯?, v: firstDist.length ? firstDist.map((x) => `${x.value}搴?${x.count}`).join(" 锝?") : "-" },
      { k: "浜岄厤搴楁暟鍒嗗竷", v: secondDist.length ? secondDist.map((x) => `${x.value}搴?${x.count}`).join(" 锝?") : "-" },
      { k: "閲嶅悎鐜囧垎甯?, v: overlapDist.length ? overlapDist.map((x) => `${x.range}:${x.count}`).join(" 锝?") : "-" },
    ];
    if (els.humanTemplateDoubleBody) {
      els.humanTemplateDoubleBody.innerHTML = doubleRows.map((row) => `
        <tr><td>${escapeHtml(row.k)}</td><td>${escapeHtml(row.v)}</td></tr>
      `).join("");
    }

    const singleRows = [
      { k: "鍚勭被鍨嬫暟閲忓崰姣?, v: categoryShare.length ? categoryShare.map((x) => `${x.category}:${(Number(x.ratio || 0) * 100).toFixed(1)}%(${x.count})`).join(" 锝?") : "-" },
      { k: "鍚勭被鍨嬪钩鍧囧簵鏁?, v: categoryShare.length ? categoryShare.map((x) => `${x.category}:${formatNumber(x.avg_store_count, 2)}`).join(" 锝?") : "-" },
      { k: "鍗曞簵绾胯矾鏁伴噺", v: formatNumber(sizeDist.single_store_routes, 0) },
      { k: "灏忕嚎璺?<=3搴?", v: formatNumber(sizeDist.small_routes, 0) },
      { k: "涓嚎璺?4~6搴?", v: formatNumber(sizeDist.medium_routes, 0) },
      { k: "澶х嚎璺?>=7搴?", v: formatNumber(sizeDist.large_routes, 0) },
    ];
    if (els.humanTemplateSingleBody) {
      els.humanTemplateSingleBody.innerHTML = singleRows.map((row) => `
        <tr><td>${escapeHtml(row.k)}</td><td>${escapeHtml(row.v)}</td></tr>
      `).join("");
    }

    const vehicleRows = [
      { k: "涓€澶╁弻閰嶇粍鏁伴噺", v: formatNumber(v.double_group_count, 0) },
      { k: "涓€澶╁崟閰嶇嚎璺暟閲?, v: formatNumber(v.single_route_count, 0) },
      { k: "鎬昏溅杈嗘暟", v: formatNumber(v.total_vehicle_count, 0) },
      { k: "鍙岄厤浠诲姟寤鸿杞﹁締鏁?, v: formatNumber(v.suggested_vehicle_for_double_tasks, 0) },
      { k: "鍗曢厤浠诲姟寤鸿杞﹁締鏁?, v: formatNumber(v.suggested_vehicle_for_single_tasks, 0) },
    ];
    if (els.humanTemplateVehicleBody) {
      els.humanTemplateVehicleBody.innerHTML = vehicleRows.map((row) => `
        <tr><td>${escapeHtml(row.k)}</td><td>${escapeHtml(row.v)}</td></tr>
      `).join("");
    }
  }

  function buildSingleRoutePortraitFromRoutes(singleRoutes) {
    const routes = Array.isArray(singleRoutes) ? singleRoutes : [];
    const counters = {
      airport: [],
      rail: [],
      subway: [],
      other: [],
    };
    const distribution = {
      single_store_routes: 0,
      small_routes: 0,
      medium_routes: 0,
      large_routes: 0,
      total_routes: routes.length,
    };
    routes.forEach((r) => {
      const routeName = String(r.route_name || "").toLowerCase();
      const storeCount = Number(r.store_count) || 0;
      if (storeCount <= 1) distribution.single_store_routes += 1;
      if (storeCount <= 3) distribution.small_routes += 1;
      else if (storeCount <= 6) distribution.medium_routes += 1;
      else distribution.large_routes += 1;
      if (routeName.includes("鏈哄満")) counters.airport.push(storeCount);
      else if (routeName.includes("楂橀搧") || routeName.includes("鍗楃珯") || routeName.includes("鍖楃珯")) counters.rail.push(storeCount);
      else if (routeName.includes("鍦伴搧")) counters.subway.push(storeCount);
      else counters.other.push(storeCount);
    });
    const makeCat = (label, values) => ({
      category: label,
      count: values.length,
      avg_store_count: values.length ? (values.reduce((a, b) => a + b, 0) / values.length) : 0,
      max_store_count: values.length ? Math.max(...values) : 0,
      min_store_count: values.length ? Math.min(...values) : 0,
    });
    return {
      categories: [
        makeCat("鏈哄満", counters.airport),
        makeCat("楂橀搧/鍗楃珯/鍖楃珯", counters.rail),
        makeCat("鍦伴搧", counters.subway),
        makeCat("鍏朵粬", counters.other),
      ],
      distribution,
    };
  }

  function renderMultiRows(items) {
    if (!items || !items.length) {
      els.multiBody.innerHTML = '<tr><td colspan="14" class="rg-empty">褰撳ぉ娌℃湁涓€鏃ュ閰嶅簵閾?/td></tr>';
      return;
    }
    els.multiBody.innerHTML = items.map((item) => `
      <tr>
        <td>${cellBox(item.shop_code, "rg-cell-box-id")}</td>
        <td>
          <div class="rg-store-name-stack">
            <span class="rg-store-name-id">搴楅摵缂栧彿: ${escapeHtml(item.shop_code)}</span>
            ${cellBox(item.store_name || "--", "rg-cell-box-name")}
          </div>
        </td>
        <td>${cellBox(item.district || "--", "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.delivery_count, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave1_load, 3), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave2_load, 3), "rg-cell-box-small")}</td>
        <td>${cellBox(item.first_wave_time || "--:--", "rg-cell-box-time")}</td>
        <td>${cellBox(item.second_wave_time || "--:--", "rg-cell-box-time")}</td>
        <td>${cellBox(item.wave_belongs || "--", "rg-cell-box-wave")}</td>
        <td>${cellBox(formatNumber(item.unload_minutes, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.difficulty, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.tolerate_minutes, 0), "rg-cell-box-small")}</td>
        <td>${rowReadyBadge(item)}</td>
        <td>${cellBox(item.source_vehicle_ids || "-", "rg-cell-box-vehicle")}</td>
      </tr>
    `).join("");
  }

  function renderSingleRows(items) {
    if (!items || !items.length) {
      els.singleBody.innerHTML = '<tr><td colspan="13" class="rg-empty">褰撳ぉ娌℃湁涓€鏃ヤ竴閰嶅簵閾?/td></tr>';
      return;
    }
    els.singleBody.innerHTML = items.map((item) => `
      <tr>
        <td>${cellBox(item.shop_code, "rg-cell-box-id")}</td>
        <td>
          <div class="rg-store-name-stack">
            <span class="rg-store-name-id">搴楅摵缂栧彿: ${escapeHtml(item.shop_code)}</span>
            ${cellBox(item.store_name || "--", "rg-cell-box-name")}
          </div>
        </td>
        <td>${cellBox(item.district || "--", "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.delivery_count, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave3_load, 3), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.wave4_load, 3), "rg-cell-box-small")}</td>
        <td>${cellBox(item.expected_time || "--:--", "rg-cell-box-time")}</td>
        <td>${cellBox(item.wave_belongs || "--", "rg-cell-box-wave")}</td>
        <td>${cellBox(formatNumber(item.unload_minutes, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.difficulty, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.tolerate_minutes, 0), "rg-cell-box-small")}</td>
        <td>${rowReadyBadge(item)}</td>
        <td>${cellBox(item.source_vehicle_ids || "-", "rg-cell-box-vehicle")}</td>
      </tr>
    `).join("");
  }

  function renderVehicleRows(items) {
    if (!items || !items.length) {
      els.vehicleBody.innerHTML = '<tr><td colspan="7" class="rg-empty">褰撳ぉ鏈尮閰嶅埌鍙敤杞﹀彿</td></tr>';
      return;
    }
    els.vehicleBody.innerHTML = items.map((item) => `
      <tr>
        <td>${cellBox(item.plateNo || "--", "rg-cell-box-vehicle")}</td>
        <td>${cellBox(item.driverName || "--", "rg-cell-box-name")}</td>
        <td>${cellBox(item.type || "--", "rg-cell-box-name")}</td>
        <td>${cellBox(formatNumber(item.capacity, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(formatNumber(item.speed, 0), "rg-cell-box-small")}</td>
        <td>${cellBox(Number(item.canCold) ? "鍙€佸喎钘? : "鍚?, "rg-cell-box-small")}</td>
        <td>${cellBox(item.status || "鏈娇鐢?, "rg-cell-box-status rg-cell-box-miss")}</td>
      </tr>
    `).join("");
  }

  function renderTables() {
    renderMultiRows(sortRows(state.rows.multi, "multi"));
    renderSingleRows(sortRows(state.rows.single, "single"));
    renderVehicleRows(sortRows(state.rows.vehicle, "vehicle"));
    updateSortHeaderState();
  }

  function updateSortHeaderState() {
    document.querySelectorAll(".rg-sortable").forEach((th) => {
      const table = th.dataset.table;
      const key = th.dataset.key;
      const btn = th.querySelector(".rg-sort-btn");
      if (!btn) return;
      const cfg = state.sort[table];
      const active = !!cfg && cfg.key === key;
      th.classList.toggle("is-active", active);
      th.classList.toggle("is-desc", active && cfg.order === "desc");
      if (!active) btn.dataset.arrow = "鈫?;
      else btn.dataset.arrow = cfg.order === "asc" ? "鈫? : "鈫?;
    });
  }

  function bindSortHeaders() {
    document.querySelectorAll(".rg-sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const table = th.dataset.table;
        const key = th.dataset.key;
        if (!table || !key || !state.sort[table]) return;
        const cfg = state.sort[table];
        if (cfg.key === key) cfg.order = cfg.order === "asc" ? "desc" : "asc";
        else {
          cfg.key = key;
          cfg.order = "asc";
        }
        renderTables();
      });
    });
  }

  async function loadSummary() {
    const result = await fetchJson("/rengong/summary");
    state.overview = result.overview || {};
    if (!state.selectedDate) {
      state.selectedDate = state.overview?.date_max || "";
      if (state.selectedDate && els.dayInput) els.dayInput.value = state.selectedDate;
    }
  }

  async function loadDayView() {
    if (!state.selectedDate) {
      els.queryState.textContent = "璇峰厛閫夋嫨鏃ユ湡";
      return;
    }
    try {
      els.queryState.textContent = "姝ｅ湪璇诲彇褰撳ぉ缁忛獙鏁版嵁...";
      console.log("[缁忛獙椤礭 loadDayView date=", state.selectedDate);

      const result = await fetchJson("/rengong/day-view", buildDayQuery());
      const day = result.daySummary || {};
      const multi = result.multiDeliveryStores || [];
      const single = result.singleDeliveryStores || [];
      const waveBreakdown = result.waveBreakdown || [];

      const storeGroups = result.storeGroups || [];
      const storeGroupStats = result.storeGroupStats || {};
      const dispatchSimulations = result.dispatchInputSimulations || [];
      const dispatchSimStats = result.dispatchInputSimStats || {};
      const singleRoutes = result.singleRoutes || [];
      const singleRouteStats = result.singleRouteStats || {};
      const singleRoutePortrait = result.singleRoutePortrait || buildSingleRoutePortraitFromRoutes(singleRoutes);
      const humanStructureTemplate = result.humanStructureTemplate || {};

      buildSummaryCards(day);
      renderWaveBreakdown(waveBreakdown);
      renderStoreGroupSummary(storeGroupStats);
      renderStoreGroupRows(storeGroups);
      renderDispatchSimSummary(dispatchSimStats);
      renderDispatchSimRows(dispatchSimulations);
      renderSingleRouteSummary(singleRouteStats);
      renderSingleRouteRows(singleRoutes);
      renderSingleRoutePortrait(singleRoutePortrait);
      renderHumanStructureTemplate(humanStructureTemplate);

      state.rows.multi = multi;
      state.rows.single = single;
      state.rows.vehicle = buildVehicleRows([...multi, ...single]);
      renderTables();

      const singleWave3Sum = single.reduce((sum, row) => sum + (Number(row.wave3_load) || 0), 0);
      const singleWave4Sum = single.reduce((sum, row) => sum + (Number(row.wave4_load) || 0), 0);
      els.multiMeta.textContent = `鍏?${formatNumber(multi.length, 0)} 瀹?锝?涓€娉㈡璐ч噺鍚堣 ${formatNumber(day.multi_wave1_sum, 3)} 锝?浜屾尝娆¤揣閲忓悎璁?${formatNumber(day.multi_wave2_sum, 3)}`;
      els.singleMeta.textContent = `鍏?${formatNumber(single.length, 0)} 瀹?锝?涓夋尝娆¤揣閲忓悎璁?${formatNumber(singleWave3Sum, 3)} 锝?鍥涙尝娆¤揣閲忓悎璁?${formatNumber(singleWave4Sum, 3)}`;

      if (els.storeGroupMeta) {
        els.storeGroupMeta.textContent = `鍏?${formatNumber(storeGroupStats.total_groups, 0)} 缁?锝?骞冲潎姣忕粍 ${formatNumber(storeGroupStats.avg_store_count, 2)} 瀹?锝?褰撳墠浠呭睍绀哄墠 ${formatNumber(storeGroups.length, 0)} 鏉;
      }
      if (els.dispatchSimMeta) {
        els.dispatchSimMeta.textContent = `鍏?${formatNumber(dispatchSimStats.total_groups, 0)} 缁?锝?骞冲潎鎬诲簵鏁?${formatNumber(dispatchSimStats.avg_total_store_count, 2)} 锝?褰撳墠浠呭睍绀哄墠 ${formatNumber(dispatchSimulations.length, 0)} 鏉;
      }
      if (els.singleRouteMeta) {
        els.singleRouteMeta.textContent = `鍏?${formatNumber(singleRouteStats.total_routes, 0)} 鏉?锝?骞冲潎搴楁暟 ${formatNumber(singleRouteStats.avg_store_count, 2)} 锝?褰撳墠浠呭睍绀哄墠 ${formatNumber(singleRoutes.length, 0)} 鏉;
      }
      if (els.singleRoutePortraitMeta) {
        const totalPortraitRoutes = Number(singleRoutePortrait?.distribution?.total_routes || 0);
        els.singleRoutePortraitMeta.textContent = `鎸夌嚎璺悕绉板垎绫荤粺璁★紙鍏?${formatNumber(totalPortraitRoutes, 0)} 鏉″崟閰嶇嚎璺級`;
      }
      if (els.humanTemplateMeta) {
        els.humanTemplateMeta.textContent = "浠呭睍绀哄垎甯冧笌缁熻妯℃澘锛屼笉杈撳嚭鍏蜂綋杞︾墝缁戝畾鏂规";
      }
      if (els.vehicleMeta) {
        els.vehicleMeta.textContent = `鍘婚噸杞﹀彿 ${formatNumber(state.rows.vehicle.length, 0)} 鍙?锝?榛樿鐘舵€佸叏閮ㄢ€滄湭浣跨敤鈥漙;
      }
      els.queryState.textContent = `宸插姞杞?${state.selectedDate}`;
    } catch (error) {
      const message = String(error?.message || error || "鏈煡閿欒");
      els.queryState.textContent = `璇诲彇澶辫触锛?{message}`;
      els.storeGroupSummary.innerHTML = "";
      els.dispatchSimSummary.innerHTML = "";
      els.singleRouteSummary.innerHTML = "";
      if (els.humanTemplateSummary) els.humanTemplateSummary.innerHTML = "";
      els.storeGroupBody.innerHTML = `<tr><td colspan="13" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.dispatchSimBody.innerHTML = `<tr><td colspan="5" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.singleRouteBody.innerHTML = `<tr><td colspan="6" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.portraitCategoryBody.innerHTML = `<tr><td colspan="5" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.portraitDistributionBody.innerHTML = `<tr><td colspan="2" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      if (els.humanTemplateDoubleBody) els.humanTemplateDoubleBody.innerHTML = `<tr><td colspan="2" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      if (els.humanTemplateSingleBody) els.humanTemplateSingleBody.innerHTML = `<tr><td colspan="2" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      if (els.humanTemplateVehicleBody) els.humanTemplateVehicleBody.innerHTML = `<tr><td colspan="2" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.multiBody.innerHTML = `<tr><td colspan="14" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.singleBody.innerHTML = `<tr><td colspan="13" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
      els.vehicleBody.innerHTML = `<tr><td colspan="7" class="rg-empty">璇诲彇澶辫触锛?{escapeHtml(message)}</td></tr>`;
    }
  }

  function refreshStateFromInputs() {
    state.selectedDate = String(els.dayInput?.value || "").trim();
    state.query = String(els.searchInput?.value || "").trim();
    state.solverReady = String(els.solverReadySelect?.value || "all").trim();
  }

  async function loadCurrentSelection() {
    refreshStateFromInputs();
    await loadDayView();
  }

  async function initPage() {
    els.queryState.textContent = "姝ｅ湪璇诲彇鍙敤鏃ユ湡...";
    await loadSummary();
    if (!state.selectedDate) {
      els.queryState.textContent = "缁忛獙搴撻噷鏆傛棤鍙敤鏃ユ湡";
      return;
    }
    await loadCurrentSelection();
  }

  if (els.loadDayBtn) els.loadDayBtn.addEventListener("click", () => loadCurrentSelection());
  if (els.refreshBtn) els.refreshBtn.addEventListener("click", () => loadCurrentSelection());
  if (els.searchInput) {
    els.searchInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadCurrentSelection();
    });
  }
  if (els.solverReadySelect) els.solverReadySelect.addEventListener("change", () => loadCurrentSelection());

  bindSortHeaders();
  updateSortHeaderState();
  initPage();
})();

