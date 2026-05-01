/*
 * 混沌（推演页）前端脚本。
 *
 * 这里集中维护两类推演：
 * 1. 单店时间推演：同一波次做改前/改后 A/B 真求解。
 * 2. 减车/可行性推演：围绕失败样本、约束诊断和建议规则展开。
 */
(function () {
    const GA_BACKEND_URL = "http://127.0.0.1:8765";

    let currentBatch = null;
    let currentProcessedData = null;
    let latestBatchId = null;
    let allShops = [];
    let shopWaveTimes = [];
    let latestBatchDate = "";
    let simulationLogCursor = 0;
    let simulationLogTaskId = "";
    let simulationLogTimer = null;
    let latestSimulationResult = null;
    let latestSimulationRequest = null;
    let simulationReplayLines = [];
    let activeReportTab = "adjustments";
    let simulateMode = "fleet_feasibility";
    let modalRouteDecisionState = {};

    function formatNumber(num, decimals = 1) {
        const value = Number(num);
        if (!Number.isFinite(value)) return "--";
        return value.toFixed(decimals);
    }

    function formatDateTime(dateStr) {
        const text = String(dateStr || "").trim();
        if (!text) return "--";
        return text.replace(" ", "\n").replace(/-/g, "/").slice(5, 16);
    }

    function normalizeStoreCode(value) {
        return String(value ?? "").trim();
    }

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function getStoreCode(store) {
        return normalizeStoreCode(
            store?.shop_code ?? store?.shopCode ?? store?.store_id ?? store?.storeId ?? store?.id ?? store?.code
        );
    }

    function getStoreName(store) {
        return String(store?.shop_name ?? store?.shopName ?? store?.store_name ?? store?.storeName ?? store?.name ?? "").trim();
    }

    function getStoreWaveBelongs(store) {
        return String(store?.wave_belongs ?? store?.waveBelongs ?? "").trim();
    }

    function getCurrentTimeFromWaveMap(waveInfo, waveBelongs) {
        if (!waveInfo) return "--:--";
        const waveText = String(waveBelongs || waveInfo.wave_belongs || "");
        if (waveText.includes("1")) return waveInfo.w1_time || "--:--";
        if (waveText.includes("2")) return waveInfo.w2_time || "--:--";
        if (waveText.includes("3")) return waveInfo.w3_time || "--:--";
        if (waveText.includes("4")) return waveInfo.w4_time || "--:--";
        return waveInfo.w1_time || waveInfo.w2_time || waveInfo.w3_time || waveInfo.w4_time || "--:--";
    }

    function getBestResult(snapshot) {
        const results = Array.isArray(snapshot?.results) ? snapshot.results : [];
        if (!results.length) return null;
        const activeKey = String(snapshot?.activeResultKey || "").trim();
        if (activeKey) {
            const active = results.find((item) => String(item?.key || "").trim() === activeKey);
            if (active) return active;
        }
        return results
            .slice()
            .sort((a, b) => Number(b?.metrics?.score || 0) - Number(a?.metrics?.score || 0))[0] || results[0];
    }

    function normalizeVehiclePlans(bestResult) {
        const solution = bestResult?.solution;
        if (Array.isArray(solution)) {
            const flattened = [];
            solution.forEach((waveGroup) => {
                if (Array.isArray(waveGroup)) {
                    waveGroup.forEach((plan) => {
                        if (plan && typeof plan === "object" && !Array.isArray(plan)) {
                            flattened.push(plan);
                        }
                    });
                } else if (waveGroup && typeof waveGroup === "object") {
                    flattened.push(waveGroup);
                }
            });
            return flattened;
        }
        if (Array.isArray(solution?.plans)) return solution.plans;
        if (Array.isArray(solution?.vehiclePlans)) return solution.vehiclePlans;
        return [];
    }

    function extractWaveData(snapshot) {
        const bestResult = getBestResult(snapshot);
        const metrics = bestResult?.metrics || {};
        const plans = normalizeVehiclePlans(bestResult);
        const waveIds = ["W1", "W2", "W3", "W4"];
        const result = { W1: null, W2: null, W3: null, W4: null };

        waveIds.forEach((waveId) => {
            const wavePlans = plans.filter((plan) => String(plan?.waveId || "").trim().toUpperCase() === waveId);
            const trips = wavePlans.flatMap((plan) => Array.isArray(plan?.trips) ? plan.trips : []);
            const scheduledStoreIds = new Set();
            trips.forEach((trip) => {
                (trip?.route || []).forEach((storeId) => {
                    const normalized = normalizeStoreCode(storeId);
                    if (normalized) scheduledStoreIds.add(normalized);
                });
            });
            const totalDistance = wavePlans.reduce((sum, plan) => sum + Number(plan?.totalDistance || 0), 0);
            const loadRates = wavePlans
                .map((plan) => Number(plan?.totalLoad || 0) > 0 && Number(plan?.totalDistance || 0) >= 0 ? Number(plan?.totalLoad || 0) : null)
                .filter((item) => item !== null);
            const metricWave = metrics?.waves?.[waveId] || {};
            const unscheduled = Array.isArray(metricWave?.unscheduledStores)
                ? metricWave.unscheduledStores.length
                : Number(metricWave?.unscheduledCount || 0);

            result[waveId] = {
                wave_id: waveId,
                scheduled_count: scheduledStoreIds.size || Number(metricWave?.scheduledCount || 0),
                unscheduled_count: unscheduled,
                total_distance_km: totalDistance || Number(metricWave?.totalDistance || 0),
                total_load_rate: Number(metricWave?.loadRate || 0),
                vehicles: wavePlans.map((plan) => ({
                    plate_no: String(plan?.vehicle?.plateNo || ""),
                    routes: (plan?.trips || []).map((trip) => trip?.route || []),
                    total_distance_km: Number(plan?.totalDistance || 0),
                    load_rate: Number(plan?.tripCount ? (plan?.totalLoad || 0) : plan?.loadRate || 0)
                }))
            };
        });

        return result;
    }

    function extractTotalVehicles(snapshot) {
        const bestResult = getBestResult(snapshot);
        const plans = normalizeVehiclePlans(bestResult);
        const used = plans.filter((plan) => Array.isArray(plan?.trips) && plan.trips.length > 0);
        return used.length || (Array.isArray(snapshot?.vehicles) ? snapshot.vehicles.length : 0);
    }

    function extractTotalDistance(snapshot) {
        const bestResult = getBestResult(snapshot);
        const metrics = bestResult?.metrics || {};
        if (Number.isFinite(Number(metrics?.totalDistance))) return Number(metrics.totalDistance);
        return normalizeVehiclePlans(bestResult).reduce((sum, plan) => sum + Number(plan?.totalDistance || 0), 0);
    }

    async function fetchWithTimeout(url, options, timeout = 30000) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);
        try {
            const response = await fetch(url, { ...options, signal: controller.signal });
            clearTimeout(timeoutId);
            return response;
        } catch (error) {
            clearTimeout(timeoutId);
            throw error;
        }
    }

    function formatDateOnly(dateObj) {
        const year = dateObj.getFullYear();
        const month = String(dateObj.getMonth() + 1).padStart(2, "0");
        const day = String(dateObj.getDate()).padStart(2, "0");
        return `${year}-${month}-${day}`;
    }

    function buildRecentDateCandidates(days = 7) {
        const result = [];
        const today = new Date();
        for (let index = 0; index < days; index += 1) {
            const current = new Date(today);
            current.setDate(today.getDate() - index);
            result.push(formatDateOnly(current));
        }
        return result;
    }

    async function loadBatchList(page = 1, pageSize = 50) {
        try {
            const today = new Date().toISOString().slice(0, 10);
            const response = await fetchWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ date: today, page, pageSize })
            }, 10000);
            console.log("鎵规鍒楄〃鍝嶅簲鐘舵€?", response.status);
            const result = await response.json();
            result._httpStatus = response.status;
            console.log("鎵规鍒楄〃鍝嶅簲鏁版嵁:", result);
            return Array.isArray(result?.items) ? result.items : [];
        } catch (error) {
            console.error("加载批次列表出错:", error);
            return [];
        }
    }

    async function loadBatchDetail(batchId) {
        try {
            const response = await fetchWithTimeout(`${GA_BACKEND_URL}/archive/get`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ id: batchId })
            }, 15000);
            const result = await response.json();
            console.log("批次详情响应数据:", result);
            if (!result?.item) throw new Error("获取批次详情失败");
            currentBatch = result.item;
            currentProcessedData = processSnapshot(currentBatch);
            latestSimulationResult = null;
            latestSimulationRequest = null;
            simulationReplayLines = [];
            window.__tuiyanDebug = { currentBatch, currentProcessedData, allShops, shopWaveTimes };
            return currentProcessedData;
        } catch (error) {
            console.error("加载批次详情出错:", error);
            return null;
        }
    }

    async function requestBatchListByDate(date, page = 1, pageSize = 50) {
        try {
            const response = await fetchWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ date, page, pageSize })
            }, 10000);
            console.log("鎵规鍒楄〃鍝嶅簲鐘舵€?", response.status, "date=", date);
            const result = await response.json();
            console.log("鎵规鍒楄〃鍝嶅簲鏁版嵁:", result);
            return Array.isArray(result?.items) ? result.items : [];
        } catch (error) {
            console.error("加载批次列表出错:", error);
            return [];
        }
    }

    async function loadBatchList(page = 1, pageSize = 50) {
        const candidateDates = buildRecentDateCandidates(7);
        for (const date of candidateDates) {
            const items = await requestBatchListByDate(date, page, pageSize);
            if (items.length) {
                latestBatchDate = date;
                return items;
            }
        }
        latestBatchDate = "";
        return [];
    }

    async function loadLatestBatch() {
        const list = await loadBatchList(1, 1);
        if (!list.length) return null;
        latestBatchId = list[0].id;
        return loadBatchDetail(latestBatchId);
    }

    async function loadShops() {
        try {
            const response = await fetchWithTimeout(`${GA_BACKEND_URL}/shops/list`, {
                method: "GET",
                headers: { "Content-Type": "application/json" }
            }, 10000);
            const result = await response.json();
            console.log("门店基础信息响应数据:", result);
            allShops = Array.isArray(result?.shops) ? result.shops : [];
            return allShops;
        } catch (error) {
            console.error("加载门店信息出错:", error);
            return [];
        }
    }

    async function loadShopWaveTimes() {
        try {
            const response = await fetchWithTimeout(`${GA_BACKEND_URL}/store-wave-timing-resolved/list?limit=5000`, {
                method: "GET",
                headers: { "Content-Type": "application/json" }
            }, 10000);
            const result = await response.json();
            console.log("门店波次时间响应数据:", result);
            shopWaveTimes = Array.isArray(result?.items) ? result.items : [];
            return shopWaveTimes;
        } catch (error) {
            console.error("加载门店波次时间出错:", error);
            return [];
        }
    }

    function processSnapshot(snapshot) {
        if (!snapshot) return null;
        console.log("processSnapshot结果结构:", {
            snapshot_id: snapshot?.id,
            result: snapshot?.result,
            results: snapshot?.results,
            activeResultKey: snapshot?.activeResultKey,
            item_result: snapshot?.item?.result,
            item_results: snapshot?.item?.results
        });

        const shopTimeMap = new Map();
        for (const item of shopWaveTimes) {
            const code = getStoreCode(item);
            if (!code) continue;
            shopTimeMap.set(code, {
                wave_belongs: String(item.wave_belongs || ""),
                w1_time: item.first_wave_time || "",
                w2_time: item.second_wave_time || "",
                w3_time: item.arrival_time_w3 || "",
                w4_time: item.arrival_time_w4 || ""
            });
        }

        const shopMetaMap = new Map();
        for (const shop of allShops) {
            const code = getStoreCode(shop);
            if (!code) continue;
            shopMetaMap.set(code, {
                name: getStoreName(shop),
                wave_belongs: getStoreWaveBelongs(shop),
                allowed_late: Number(shop.allowed_late_minutes || 10),
                difficulty: Number(shop.difficulty || 1)
            });
        }

        const stores = Array.isArray(snapshot.stores) ? snapshot.stores.map((store) => {
            const code = getStoreCode(store);
            const meta = shopMetaMap.get(code) || {};
            const waveInfo = shopTimeMap.get(code) || {};
            return {
                code,
                name: getStoreName(store) || meta.name || "--",
                wave_belongs: getStoreWaveBelongs(store) || meta.wave_belongs || waveInfo.wave_belongs || "--",
                current_time: getCurrentTimeFromWaveMap(waveInfo, getStoreWaveBelongs(store) || meta.wave_belongs),
                allowed_late: Number(store.allowedLateMinutes || store.parking || meta.allowed_late || 10)
            };
        }) : [];

        return {
            batch_id: String(snapshot.id || ""),
            strategy: String(snapshot.algorithms || snapshot.strategy || snapshot.activeResultKey || ""),
            created_at: String(snapshot.createdAt || ""),
            total_stores: stores.length,
            total_vehicles: extractTotalVehicles(snapshot),
            total_distance_km: extractTotalDistance(snapshot),
            stores,
            wave_results: extractWaveData(snapshot),
            shop_time_map: shopTimeMap,
            shop_meta_map: shopMetaMap
        };
    }

    const REALTIME_DISPATCH_CONTEXT_KEY = "dispatchRealtimeContext";

    function getRealtimeDispatchContext() {
        try {
            if (window.__dispatchRealtimeContext && typeof window.__dispatchRealtimeContext === "object") {
                return window.__dispatchRealtimeContext;
            }
        } catch (error) {
            console.warn("读取 window.__dispatchRealtimeContext 失败:", error);
        }
        try {
            const raw = localStorage.getItem(REALTIME_DISPATCH_CONTEXT_KEY);
            if (!raw) return null;
            const parsed = JSON.parse(raw);
            return parsed && typeof parsed === "object" ? parsed : null;
        } catch (error) {
            console.warn("读取本地调度上下文失败:", error);
            return null;
        }
    }

    function renderSimulationProgress(status, steps, payload) {
        const statusEl = document.getElementById("simulate-progress-status");
        const stepsEl = document.getElementById("simulate-progress-steps");
        const debugEl = document.getElementById("simulate-debug-output");
        if (statusEl) {
            statusEl.className = `simulate-progress-status ${status || "idle"}`;
            statusEl.textContent =
                status === "running" ? "执行中" :
                status === "success" ? "已完成" :
                status === "error" ? "失败" :
                "待执行";
        }
        if (stepsEl) {
            const list = Array.isArray(steps) && steps.length ? steps : [{ text: "等待开始推演...", state: "" }];
            stepsEl.innerHTML = list.map((step) => {
                const state = String(step?.state || "").trim();
                const className = ["simulate-step", state ? `is-${state}` : ""].filter(Boolean).join(" ");
                return `<div class="${className}">${escapeHtml(String(step?.text || ""))}</div>`;
            }).join("");
        }
        if (debugEl) {
            if (!payload) {
                debugEl.textContent = "暂无过程数据";
            } else {
                try {
                    debugEl.textContent = JSON.stringify(payload, null, 2);
                } catch (error) {
                    debugEl.textContent = String(payload);
                }
            }
        }
    }

    function appendRawRelayLine(text) {
        const line = String(text || "");
        if (!line) return;
        if (typeof window.appendRelayConsoleLine === "function") {
            window.appendRelayConsoleLine(line);
            return;
        }
        console.log(line);
    }

    function openSimulationRelayConsole() {
        if (typeof window.openRelayConsoleModal === "function") {
            window.openRelayConsoleModal("求解过程可视化窗口");
        }
    }

    function stopSimulationLogPolling() {
        if (simulationLogTimer) {
            clearTimeout(simulationLogTimer);
            simulationLogTimer = null;
        }
    }

    function buildSimulationTaskId() {
        return `sim_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    }

    async function pollSimulationTaskLog(taskId, finalFetch = false) {
        if (!taskId) return;
        try {
            const response = await fetch(`${GA_BACKEND_URL}/simulate/task-log?taskId=${encodeURIComponent(taskId)}&cursor=${simulationLogCursor}`);
            const result = await response.json();
            const data = result?.data || {};
            const lines = Array.isArray(data.lines) ? data.lines : [];
            lines.forEach((line) => {
                appendRawRelayLine(line);
                simulationReplayLines.push(String(line || ""));
            });
            if (lines.length > 0 && activeReportTab === "replay") {
                renderSimulationReport();
            }
            simulationLogCursor = Number(data.next_cursor || simulationLogCursor || 0);
            if (data.done || finalFetch) {
                stopSimulationLogPolling();
                return;
            }
            simulationLogTimer = setTimeout(() => {
                void pollSimulationTaskLog(taskId, false);
            }, 600);
        } catch (error) {
            if (!finalFetch) {
                simulationLogTimer = setTimeout(() => {
                    void pollSimulationTaskLog(taskId, false);
                }, 1000);
            }
        }
    }

    function startSimulationLogPolling(taskId) {
        stopSimulationLogPolling();
        simulationLogTaskId = taskId;
        simulationLogCursor = 0;
        simulationReplayLines = [];
        void pollSimulationTaskLog(taskId, false);
    }

    // 推演接口统一入口。
    // 前端先把模式、批次、目标、店铺/车辆约束整理成 requestPayload，再由后端决定具体走哪条推演分支。
    async function callSimulateAPI(requestPayload) {
        try {
            const realtimeContext = getRealtimeDispatchContext();
            const response = await fetch(`${GA_BACKEND_URL}/simulate/optimize-time`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    ...requestPayload,
                    batch_id: currentProcessedData?.batch_id,
                    realtime_context: realtimeContext,
                    task_id: requestPayload?.task_id
                })
            });
            const result = await response.json();
            result._httpStatus = response.status;
            if (result.success) {
                console.log("推演成功:", result?.data?.improvements);
            }
            return result;
        } catch (error) {
            console.error("推演接口调用失败:", error);
            return { success: false, message: error.message };
        }
    }

    // 单店推演结果优先从数据库回读，避免页面只依赖首次返回的临时 JSON。
    async function loadSingleStoreSimulationReport(taskId) {
        try {
            const response = await fetch(`${GA_BACKEND_URL}/simulate/single-report?taskId=${encodeURIComponent(taskId)}`);
            const result = await response.json();
            result._httpStatus = response.status;
            return result;
        } catch (error) {
            console.error("加载单店推演审计结果失败:", error);
            return { success: false, message: error.message };
        }
    }

    async function saveSingleRouteDecisions(taskId, decisions) {
        try {
            const response = await fetch(`${GA_BACKEND_URL}/simulate/single-route-decisions`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ taskId, decisions })
            });
            const result = await response.json();
            result._httpStatus = response.status;
            return result;
        } catch (error) {
            console.error("保存单店线路决策失败:", error);
            return { success: false, message: error.message };
        }
    }

    // 在线路人工确认/再优化之后，基于“新的剩余集”继续发起下一轮真求解。
    async function continueSingleRouteSolve(taskId) {
        const reduceVehicleCount = Number(document.getElementById("continue-reduce-vehicles")?.value || 0);
        const minStoresPerVehicle = Number(document.getElementById("continue-min-stores")?.value || 0);
        const minLoadRate = Number(document.getElementById("continue-min-load-rate")?.value || 0);
        try {
            const response = await fetch(`${GA_BACKEND_URL}/simulate/single-route-continue`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    taskId,
                    solve_target: {
                        reduce_vehicle_count: reduceVehicleCount,
                        min_stores_per_vehicle: minStoresPerVehicle,
                        min_load_rate: minLoadRate
                    }
                })
            });
            const result = await response.json();
            result._httpStatus = response.status;
            return result;
        } catch (error) {
            console.error("继续求解剩余集失败:", error);
            return { success: false, message: error.message };
        }
    }

    // 推演弹窗总渲染函数。
    // 这里会同时拼出：A/B 对照、线路详情、其他线路、当前候选线路、人工决策与下一轮输入。
    function showSimulateModal(result, storeId, newTime) {
        const modal = document.getElementById("simulateModal");
        const modalContent = document.getElementById("modalContent");
        const saveBtn = document.getElementById("modalSaveRouteBtn");
        const continueBtn = document.getElementById("modalContinueRouteBtn");
        if (!modal || !modalContent) return;

        const before = result?.data?.before || {};
        const after = result?.data?.after || {};
        const report = result?.data?.report || {};
        const auditProof = result?.data?.audit_proof || {};
        const scheduleDiagnostics = result?.data?.schedule_diagnostics || null;
        const abCompare = result?.data?.ab_compare || {};
        const routeComp = result?.data?.route_comparison || {};
        const oldRoutes = Array.isArray(routeComp?.old_routes) ? routeComp.old_routes : [];
        const newRoute = routeComp?.new_route || null;
        const otherRoutes = routeComp?.other_routes || {};
        const currentCandidateRoutes = Array.isArray(result?.data?.current_candidate_routes) ? result.data.current_candidate_routes : [];
        const remainingStateSummary = result?.data?.remaining_state_summary || {};
        const manualDecisionSummary = result?.data?.manual_decision_summary || {};
        const taskId = String(result?.task_id || result?.data?.task_audit?.task_id || "").trim();
        const persistedDecisions = result?.data?.route_decisions || {};
        modalRouteDecisionState = { ...persistedDecisions };
        const abBase = abCompare?.baseline || {};
        const abSim = abCompare?.simulated || {};
        const abDelta = abCompare?.delta || {};
        const vehiclesSaved = Number(before.total_vehicles || 0) - Number(after.total_vehicles || 0);
        const mileageSaved = Number(before.total_mileage || 0) - Number(after.total_mileage || 0);

        const deltaColor = (val) => Number(val || 0) === 0 ? "#334155" : "#dc2626";
        const deltaPrefix = (val) => Number(val || 0) > 0 ? "+" : "";
        const baselineText = report?.baseline_text
            || `基线方案（改前）：本波次参与排线门店 ${Number(abBase.candidate || 0)} 家，成功安排 ${Number(abBase.assigned || 0)} 家，未安排 ${Number(abBase.pending || 0)} 家；使用车辆 ${Number(abBase.vehicles || 0)} 台，总里程 ${formatNumber(abBase.mileage, 1)} 公里。`;
        const simulatedText = report?.simulated_text
            || `推演方案（改后）：仅将店铺 ${escapeHtml(storeId)} 的到店时间调整到 ${escapeHtml(newTime)} 后，本波次参与排线门店 ${Number(abSim.candidate || 0)} 家，成功安排 ${Number(abSim.assigned || 0)} 家，未安排 ${Number(abSim.pending || 0)} 家；使用车辆 ${Number(abSim.vehicles || 0)} 台，总里程 ${formatNumber(abSim.mileage, 1)} 公里。`;
        const deltaText = report?.delta_text
            || `这次改单店时间后的变化为：安排门店变化 ${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)} 家，车辆变化 ${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)} 台，总里程变化 ${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)} 公里。`;
        const proofText = report?.variable_proof || auditProof?.proof_text || "仅变更目标店时间，其余输入保持一致。";

        let html = `
            <div style="background: #f0fdf4; padding: 12px; border-radius: 8px; margin-bottom: 16px;">
                <strong>优化效果</strong><br>
                用车: ${Number(before.total_vehicles || 0)} 辆 -> ${Number(after.total_vehicles || 0)} 辆 (${vehiclesSaved > 0 ? `减少 ${vehiclesSaved} 辆` : "未减少"})<br>
                里程: ${Number(before.total_mileage || 0).toFixed(1)} km -> ${Number(after.total_mileage || 0).toFixed(1)} km (${mileageSaved > 0 ? `减少 ${mileageSaved.toFixed(1)} km` : "未减少"})
            </div>
        `;

        html += `<div style="background: #eef2ff; padding: 12px; border-radius: 8px; margin-bottom: 16px;">
            <strong>A/B 对照（${escapeHtml(abCompare?.wave_id || report?.wave_id || "--")}）</strong><br>
            <div style="margin-top: 8px;">${escapeHtml(baselineText)}</div>
            <div style="margin-top: 6px;">${escapeHtml(simulatedText)}</div>
            <div style="margin-top: 8px;">
                差值：
                安排门店 <span style="color:${deltaColor(abDelta.assigned)}">${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)}</span> 家，
                未安排 <span style="color:${deltaColor(abDelta.pending)}">${deltaPrefix(abDelta.pending)}${Number(abDelta.pending || 0)}</span> 家，
                车辆 <span style="color:${deltaColor(abDelta.vehicles)}">${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)}</span> 台，
                里程 <span style="color:${deltaColor(abDelta.mileage)}">${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)}</span> 公里
            </div>
            <div style="margin-top: 8px;"><strong>${escapeHtml(deltaText)}</strong></div>
        </div>`;

        if (remainingStateSummary?.summary) {
            html += `<div style="background:#ecfeff; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>当前待再优化集合</strong><br>
                ${escapeHtml(String(remainingStateSummary.summary || ""))}<br>
                <span style="font-size:12px;color:#475569;">状态序号 ${Number(remainingStateSummary.state_seq || 0)} ｜ 可再利用车辆 ${Number(remainingStateSummary.remaining_vehicle_count || 0)} 台 ｜ 待再优化店铺 ${Number(remainingStateSummary.remaining_store_count || 0)} 家</span>
            </div>`;
        }
        if (manualDecisionSummary?.action_type === "manual_route_selection") {
            const pendingStores = Array.isArray(remainingStateSummary?.remaining_stores) ? remainingStateSummary.remaining_stores : [];
            html += `<div style="background:#fff7ed; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>下一轮求解输入</strong><br>
                已确定线路 ${Number(manualDecisionSummary.confirm_count || 0)} 条；再优化线路 ${Number(manualDecisionSummary.reoptimize_count || 0)} 条；释放车辆 ${Number(manualDecisionSummary.released_vehicle_count || 0)} 台。<br>
                你标记为“再优化”的线路已恢复成待再优化店铺集合，下方就是下一轮真求解的输入店铺（前 20 家）：<br>
                <div style="margin-top:8px; max-height:180px; overflow:auto; background:#fff; border:1px solid #fed7aa; border-radius:8px; padding:8px;">
                    ${pendingStores.slice(0, 20).map((item, idx) => `
                        <div style="display:flex; gap:12px; font-size:13px; padding:4px 0; border-bottom:${idx < Math.min(pendingStores.length, 20) - 1 ? "1px solid #ffedd5" : "none"};">
                            <span style="width:84px; color:#0f172a;">${escapeHtml(item.shop_code || "--")}</span>
                            <span style="flex:1; color:#334155;">${escapeHtml(item.shop_name || "--")}</span>
                            <span style="width:70px; color:#475569;">${escapeHtml(item.expected_time || "--:--")}</span>
                        </div>
                    `).join("") || '<div style="color:#64748b;">暂无待再优化店铺</div>'}
                </div>
                <div style="display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; margin-top:12px;">
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>减少几辆车</span>
                        <input id="continue-reduce-vehicles" type="number" min="0" value="0" style="padding:8px 10px; border:1px solid #cbd5e1; border-radius:8px;">
                    </label>
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>每车至少几个店</span>
                        <input id="continue-min-stores" type="number" min="0" value="0" style="padding:8px 10px; border:1px solid #cbd5e1; border-radius:8px;">
                    </label>
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>装载率不低于多少%</span>
                        <input id="continue-min-load-rate" type="number" min="0" max="1000" value="0" style="padding:8px 10px; border:1px solid #cbd5e1; border-radius:8px;">
                    </label>
                </div>
            </div>`;
        }

        const getLoadSummary = (route) => {
            const actualLoad = Number(route?.actual_load_value || 0);
            const capacityLimit = Number(route?.capacity_limit || 1.2);
            const loadPercent = Number(route?.total_load || 0);
            return {
                actualLoad,
                capacityLimit,
                loadPercent,
                overload: capacityLimit > 0 && actualLoad > capacityLimit + 1e-6
            };
        };

        const getRouteDecisionState = (route) => {
            const routeKey = String(route?.route_key || "").trim();
            const state = modalRouteDecisionState[routeKey] || {};
            return {
                routeKey,
                checked: Boolean(state.checked),
                action: String(state.action || "confirm")
            };
        };

        const renderRouteDetail = (title, route, accentColor, options = {}) => {
            if (!route) return "";
            const { selectable = false, snapshotType = null } = options;
            const stops = Array.isArray(route.stops) ? route.stops : [];
            const routeState = getRouteDecisionState(route);
            const loadSummary = getLoadSummary(route);
            const rows = stops.map((stop, idx) => {
                const shopCode = String(stop?.shop_code || "").trim();
                const shopName = String(stop?.shop_name || shopCode || "--").trim();
                const isTarget = shopCode === String(storeId || "").trim();
                const expectedTime = String(stop?.expected_time || "--:--").trim() || "--:--";
                const scheduledTime = String(stop?.scheduled_time || stop?.arrival || "求解器未返回到店时间").trim() || "求解器未返回到店时间";
                const boxes = Number(stop?.boxes || 0);
                const legText = String(stop?.route_leg_text || "--").trim() || "--";
                return `
                    <tr>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${idx + 1}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${escapeHtml(shopCode)}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:normal; word-break:break-all;">${escapeHtml(shopName)}${isTarget ? ' <span style="color:#dc2626;font-weight:600;">(目标店)</span>' : ""}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap; color:#475569;">${escapeHtml(legText)}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap; ${isTarget ? "color:#dc2626;font-weight:600;" : ""}">${escapeHtml(expectedTime)}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${escapeHtml(scheduledTime)}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${Number.isFinite(boxes) ? boxes.toFixed(1) : "0.0"}</td>
                    </tr>
                `;
            }).join("");
            return `
                <div class="simulate-route-card" data-route-key="${escapeHtml(routeState.routeKey)}" data-route-wave="${escapeHtml(route.wave || "--")}" data-route-vehicle="${escapeHtml(route.vehicle || "--")}" data-route-trip="${Number(route.trip || 1)}" data-snapshot-type="${escapeHtml(snapshotType || (selectable ? "simulated" : "baseline"))}" style="background:#ffffff; border:1px solid #dbeafe; border-left:4px solid ${accentColor}; border-radius:8px; padding:10px; margin-top:10px;">
                    <div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom:6px;">
                        <div style="font-weight:700;">${escapeHtml(title)}：${escapeHtml(route.wave || "--")} · ${escapeHtml(route.vehicle || "--")} · 第${Number(route.trip || 1)}趟</div>
                        ${selectable ? `
                            <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
                                <label style="display:flex; gap:6px; align-items:center; font-size:12px;">
                                    <input type="checkbox" class="simulate-route-check" ${routeState.checked ? "checked" : ""}>
                                    选中
                                </label>
                                <select class="simulate-route-action" style="padding:4px 8px; border:1px solid #cbd5e1; border-radius:6px; font-size:12px;">
                                    <option value="confirm" ${routeState.action === "confirm" ? "selected" : ""}>确定</option>
                                    <option value="reoptimize" ${routeState.action === "reoptimize" ? "selected" : ""}>再优化</option>
                                </select>
                            </div>
                        ` : ""}
                    </div>
                    <div style="margin-bottom:8px; color:#334155;">
                        线路里程 ${Number(route.total_distance || 0).toFixed(1)} km ｜ 装载 ${formatNumber(loadSummary.actualLoad, 3)} / ${formatNumber(loadSummary.capacityLimit, 3)} ｜ 装载率 <span style="color:${loadSummary.overload ? "#dc2626" : "#334155"}; font-weight:${loadSummary.overload ? "700" : "400"};">${formatNumber(loadSummary.loadPercent, 1)}%</span>${loadSummary.overload ? ' <span style="color:#dc2626;">(超限)</span>' : ""}
                    </div>
                    <div style="overflow:auto hidden;">
                        <table style="width:100%; border-collapse:collapse; font-size:13px; table-layout:fixed;">
                            <thead>
                                <tr style="background:#f8fafc;">
                                    <th style="text-align:left; padding:6px 8px; width:52px;">序号</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">店铺编号</th>
                                    <th style="text-align:left; padding:6px 8px; width:24%;">店名</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">里程变化</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">期望到店</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">调度到店</th>
                                    <th style="text-align:left; padding:6px 8px; width:72px;">货量</th>
                                </tr>
                            </thead>
                            <tbody>${rows || '<tr><td colspan="7" style="padding:8px;">无停靠明细</td></tr>'}</tbody>
                        </table>
                    </div>
                </div>
            `;
        };
        const renderRouteGroupDetails = (title, routes, accentColor, selectable = false, snapshotType = null) => {
            const items = Array.isArray(routes) ? routes : [];
            if (!items.length) return "";
            return `
                <div style="background:#ffffff; border:1px solid #dbeafe; border-radius:8px; padding:10px; margin-top:10px;">
                    <div style="font-weight:700; margin-bottom:6px;">${escapeHtml(title)}</div>
                    ${items.map((route, idx) => renderRouteDetail(`${title}${idx + 1}`, route, accentColor, { selectable, snapshotType })).join("")}
                </div>
            `;
        };

        const oldRoute = oldRoutes.length ? oldRoutes[0] : null;
        if (oldRoute || newRoute) {
            html += `<div style="background:#f8fafc; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>两条线路详情（按停靠顺序）</strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">说明：期望到店时间来自本次A/B求解输入；里程变化来自数据库实际里程；推演线路中的目标店期望时间即你输入的推演时间。</div>
                ${renderRouteDetail("改前线路", oldRoute, "#ef4444", { snapshotType: "baseline" })}
                ${renderRouteDetail("改后线路", newRoute, "#22c55e", { selectable: true, snapshotType: "simulated" })}
            </div>`;
        }

        if ((otherRoutes?.baseline && otherRoutes.baseline.length) || (otherRoutes?.simulated && otherRoutes.simulated.length)) {
            html += `<div style="background:#f8fafc; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>其他线路结果</strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">这里展示同一波次下，除目标锚点线路之外的其余线路逐店明细；改后线路支持勾选并标注“确定/再优化”。</div>
                ${renderRouteGroupDetails("改前其余线路", otherRoutes?.baseline || [], "#ef4444", false, "baseline")}
                ${renderRouteGroupDetails("改后其余线路", otherRoutes?.simulated || [], "#22c55e", true, "simulated")}
            </div>`;
        }

        if (currentCandidateRoutes.length) {
            html += `<div style="background:#f0fdf4; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>继续求解后的当前候选线路</strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">以下线路基于你刚刚确认后的剩余车辆/剩余门店重新真求解得到。你可以继续勾选“确定/再优化”，再点保存线路决策，随后继续下一轮求解。</div>
                ${renderRouteGroupDetails("当前候选线路", currentCandidateRoutes, "#16a34a", true, "iterative")}
            </div>`;
        }

        html += `<div style="background: #fef3c7; padding: 12px; border-radius: 8px;">
            <strong>结论与口径说明</strong><br>
            ${escapeHtml(proofText)}<br>
            当前弹窗已与“推演过程可视化”口径对齐，均按同一波次的 A/B 结果展示。
        </div>`;
        if (scheduleDiagnostics && (scheduleDiagnostics.summary || scheduleDiagnostics.reason)) {
            html += `<div style="background:#fff7ed; padding:12px; border-radius:8px; margin-top:12px;">
                <strong>调度时间来源诊断</strong><br>
                ${escapeHtml(String(scheduleDiagnostics.summary || ""))}<br>
                ${escapeHtml(String(scheduleDiagnostics.reason || ""))}
            </div>`;
        }

        modalContent.innerHTML = html;
        modal.style.display = "flex";

        const closeBtn = document.getElementById("closeModalBtn");
        const confirmBtn = document.getElementById("modalConfirmBtn");
        const saveRouteBtn = document.getElementById("modalSaveRouteBtn");
        const continueRouteBtn = document.getElementById("modalContinueRouteBtn");
        const closeModal = () => { modal.style.display = "none"; };
        if (closeBtn) closeBtn.onclick = closeModal;
        if (confirmBtn) confirmBtn.onclick = closeModal;
        if (saveRouteBtn) {
            saveRouteBtn.onclick = async () => {
                if (!taskId) return;
                const cards = Array.from(modalContent.querySelectorAll(".simulate-route-card[data-snapshot-type='simulated'], .simulate-route-card[data-snapshot-type='iterative']"));
                const decisions = cards.map((card) => {
                    const check = card.querySelector(".simulate-route-check");
                    const action = card.querySelector(".simulate-route-action");
                    return {
                        route_key: String(card.dataset.routeKey || "").trim(),
                        wave_id: String(card.dataset.routeWave || "").trim(),
                        vehicle_no: String(card.dataset.routeVehicle || "").trim(),
                        trip_no: Number(card.dataset.routeTrip || 1),
                        checked: Boolean(check?.checked),
                        action: String(action?.value || "confirm")
                    };
                }).filter((item) => item.route_key);
                saveRouteBtn.disabled = true;
                const originalText = saveRouteBtn.textContent;
                saveRouteBtn.textContent = "保存中...";
                const saveResult = await saveSingleRouteDecisions(taskId, decisions);
                saveRouteBtn.disabled = false;
                saveRouteBtn.textContent = originalText;
                if (!saveResult?.success) {
                    alert(`保存线路决策失败：${saveResult?.message || saveResult?.error || "未知错误"}`);
                    return;
                }
                const refreshed = await loadSingleStoreSimulationReport(taskId);
                if (refreshed?.success) {
                    showSimulateModal(refreshed, storeId, newTime);
                } else {
                    alert(`线路决策已保存，但刷新报告失败：${refreshed?.message || "未知错误"}`);
                }
            };
        }
        if (continueRouteBtn) {
            continueRouteBtn.onclick = async () => {
                if (!taskId) return;
                continueRouteBtn.disabled = true;
                const originalText = continueRouteBtn.textContent;
                continueRouteBtn.textContent = "求解中...";
                const solveResult = await continueSingleRouteSolve(taskId);
                continueRouteBtn.disabled = false;
                continueRouteBtn.textContent = originalText;
                if (!solveResult?.success) {
                    alert(`继续求解失败：${solveResult?.message || solveResult?.error || "未知错误"}`);
                    return;
                }
                const refreshed = await loadSingleStoreSimulationReport(taskId);
                if (refreshed?.success) {
                    showSimulateModal(refreshed, storeId, newTime);
                } else {
                    alert(`继续求解已完成，但刷新报告失败：${refreshed?.message || "未知错误"}`);
                }
            };
        }
    }

    function setupReportTabs() {
        const tabs = Array.from(document.querySelectorAll(".tuiyan-report-tab"));
        if (!tabs.length) return;
        tabs.forEach((tab) => {
            if (tab.dataset.bound === "1") return;
            tab.dataset.bound = "1";
            tab.addEventListener("click", () => {
                activeReportTab = String(tab.dataset.reportTab || "adjustments");
                renderSimulationReport();
            });
        });
    }

    function getSelectedSimulationWaves() {
        const checks = Array.from(document.querySelectorAll(".simulate-wave-checkbox"));
        return checks.filter((item) => item.checked).map((item) => String(item.value || "").trim()).filter(Boolean);
    }

    function renderSimulationModeUI() {
        const modeSelect = document.getElementById("simulate-mode");
        const btn = document.getElementById("simulate-btn");
        if (modeSelect) simulateMode = String(modeSelect.value || "fleet_feasibility");

        const fleetNodes = Array.from(document.querySelectorAll(".sim-mode-fleet"));
        const storeNodes = Array.from(document.querySelectorAll(".sim-mode-store"));
        const isFleet = simulateMode === "fleet_feasibility";
        fleetNodes.forEach((node) => node.classList.toggle("tab-hidden", !isFleet));
        storeNodes.forEach((node) => node.classList.toggle("tab-hidden", isFleet));

        if (btn) btn.textContent = isFleet ? "开始减车推演" : "开始单店推演";
    }

    function setupSimulationModeControls() {
        const modeSelect = document.getElementById("simulate-mode");
        if (!modeSelect || modeSelect.dataset.bound === "1") return;
        modeSelect.dataset.bound = "1";
        modeSelect.addEventListener("change", renderSimulationModeUI);
        renderSimulationModeUI();
    }

    function renderReportPaneVisibility() {
        const tabs = Array.from(document.querySelectorAll(".tuiyan-report-tab"));
        const panes = {
            adjustments: document.getElementById("simulate-report-adjustments"),
            failures: document.getElementById("simulate-report-failures"),
            replay: document.getElementById("simulate-report-replay")
        };
        tabs.forEach((tab) => {
            const key = String(tab.dataset.reportTab || "");
            tab.classList.toggle("is-active", key === activeReportTab);
        });
        Object.entries(panes).forEach(([key, pane]) => {
            if (!pane) return;
            pane.classList.toggle("tab-hidden", key !== activeReportTab);
        });
    }

    function renderSimulationReport() {
        const summaryEl = document.getElementById("simulate-report-summary");
        const decisionEl = document.getElementById("simulate-report-decision");
        const diagnosisEl = document.getElementById("simulate-report-diagnosis");
        const adjustmentsEl = document.getElementById("simulate-report-adjustments");
        const failuresEl = document.getElementById("simulate-report-failures");
        const replayEl = document.getElementById("simulate-report-replay");
        if (!summaryEl || !decisionEl || !diagnosisEl || !adjustmentsEl || !failuresEl || !replayEl) return;

        const baseline = currentProcessedData || {};
        const resultData = latestSimulationResult?.data || null;
        const request = latestSimulationRequest || {};
        const mode = String(request.mode || simulateMode || "fleet_feasibility");
        const before = resultData?.before || null;
        const after = resultData?.after || null;
        const improvements = resultData?.improvements || {};
        const affectedShops = Array.isArray(improvements?.affected_shops) ? improvements.affected_shops : [];
        const failureLogs = Array.isArray(resultData?.failure_logs) ? resultData.failure_logs : [];
        const attemptTrace = resultData?.attempt_trace_summary || {};

        if (!resultData) {
            summaryEl.innerHTML = `当前基准批次 <strong>${escapeHtml(baseline.batch_id || "--")}</strong>，总用车 ${Number(baseline.total_vehicles || 0)} 辆，总里程 ${formatNumber(baseline.total_distance_km || 0)} km。`;
            decisionEl.innerHTML = "等待推演结果。执行一次推演后，这里会给出可执行建议（时间调整/波次调整/约束放宽）。";
            diagnosisEl.innerHTML = "等待推演结果。执行一次推演后，这里会显示约束分布（时间窗/波次结束/里程/容量/门店数）。";
            adjustmentsEl.innerHTML = '<div class="tuiyan-report-item">暂无调整清单</div>';
            failuresEl.innerHTML = '<div class="tuiyan-report-item">暂无失败样本</div>';
            replayEl.innerHTML = '<div class="tuiyan-report-item">暂无试插回放</div>';
            renderReportPaneVisibility();
            return;
        }

        const vehiclesBefore = Number(before?.total_vehicles || 0);
        const vehiclesAfter = Number(after?.total_vehicles || 0);
        const mileageBefore = Number(before?.total_mileage || 0);
        const mileageAfter = Number(after?.total_mileage || 0);
        const vehiclesSaved = Number.isFinite(improvements?.vehicles_saved) ? Number(improvements.vehicles_saved) : (vehiclesBefore - vehiclesAfter);
        const mileageSaved = Number.isFinite(improvements?.mileage_saved) ? Number(improvements.mileage_saved) : (mileageBefore - mileageAfter);
        const waveText = Array.isArray(resultData?.route_changes?.affected_waves) && resultData.route_changes.affected_waves.length
            ? resultData.route_changes.affected_waves.join(" / ")
            : "--";

        const reasonMap = {};
        failureLogs.forEach((row) => {
            const key = String(row?.failure_type || "unknown").trim() || "unknown";
            reasonMap[key] = Number(reasonMap[key] || 0) + 1;
        });
        const diagnosisLines = Object.entries(reasonMap)
            .sort((a, b) => b[1] - a[1])
            .map(([key, count]) => {
                const zh = {
                    arrival_window: "时间窗超时",
                    wave_end: "波次结束超时",
                    mileage: "里程超限",
                    capacity: "装载超限",
                    max_stops: "门店数超限",
                    unknown: "未知约束"
                }[key] || key;
                return `<div>${escapeHtml(zh)}：${count} 家</div>`;
            });
        diagnosisEl.innerHTML = diagnosisLines.length
            ? diagnosisLines.join("")
            : "<div>本次推演未产生失败样本。</div>";

        if (mode === "fleet_feasibility") {
            const scopeWaves = Array.isArray(request.selected_waves) && request.selected_waves.length
                ? request.selected_waves
                : (waveText === "--" ? [] : waveText.split("/").map((item) => item.trim()).filter(Boolean));
            const failureByWave = {};
            failureLogs.forEach((item) => {
                const w = String(item?.wave_id || "--").trim() || "--";
                failureByWave[w] = Number(failureByWave[w] || 0) + 1;
            });
            const waveResultText = (scopeWaves.length ? scopeWaves : Object.keys(failureByWave)).map((waveId) => {
                const count = Number(failureByWave[waveId] || 0);
                return `${escapeHtml(waveId)} 未安排 ${count} 家`;
            }).join("，");

            summaryEl.innerHTML = `结论：我们尝试用 <strong>${Number(request.target_vehicle_count || vehiclesAfter || 0)}</strong> 辆车完成 <strong>${escapeHtml((scopeWaves.length ? scopeWaves.join("/") : waveText) || "--")}</strong>，结果 ${waveResultText || "所有目标波次均可完成"}。`;
            decisionEl.innerHTML = [
                "建议按以下顺序处理：先改门店时间，再放宽里程上限，最后调整波次截止时间。",
                "若失败样本主要为波次结束超时，优先改截止时刻；若主要为里程超限，优先改里程上限。",
                "每次调整后保持同样车辆数复算，直到目标波次无未安排门店。"
            ].map((line) => `<div>${escapeHtml(line)}</div>`).join("");

            const timeAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("time"))
                .slice(0, 80)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 店${escapeHtml(item.shop_code || "--")}</strong>：建议时间 ${escapeHtml(item.suggested_time || "--")}（当前 ${escapeHtml(item.current_time || "--")}）</div>`);
            const mileageAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("mileage"))
                .slice(0, 40)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 店${escapeHtml(item.shop_code || "--")}</strong>：里程上限建议放宽到 ${formatNumber(item.required_limit_km, 1)} km（当前 ${formatNumber(item.current_limit_km, 1)} km）</div>`);
            const waveEndAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("wave_end"))
                .slice(0, 40)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 店${escapeHtml(item.shop_code || "--")}</strong>：波次截止建议调整到 ${escapeHtml(item.suggested_wave_end_time || "--")}</div>`);
            const adviceBlocks = [...timeAdvice, ...mileageAdvice, ...waveEndAdvice];
            adjustmentsEl.innerHTML = adviceBlocks.length
                ? adviceBlocks.join("")
                : '<div class="tuiyan-report-item">当前车辆规模下，暂无额外规则调整建议。</div>';
        } else {
            const auditProof = resultData?.audit_proof || {};
            const abCompare = resultData?.ab_compare || {};
            const abBase = abCompare?.baseline || {};
            const abSim = abCompare?.simulated || {};
            const abDelta = abCompare?.delta || {};
            const report = resultData?.report || {};
            const hasReadableReport = Boolean(report?.baseline_text || report?.simulated_text || report?.delta_text);
            if (hasReadableReport) {
                summaryEl.innerHTML = `结论：<strong>${escapeHtml(report.delta_text || "本次推演已完成，请查看下方对照详情。")}</strong>`;
            } else {
                summaryEl.innerHTML = [
                    `结论：用车 <strong>${vehiclesBefore} -> ${vehiclesAfter}</strong>（${vehiclesSaved >= 0 ? `减少 ${vehiclesSaved}` : `增加 ${Math.abs(vehiclesSaved)}`} 辆）`,
                    `里程 <strong>${mileageBefore.toFixed(1)} -> ${mileageAfter.toFixed(1)} km</strong>（${mileageSaved >= 0 ? `减少 ${mileageSaved.toFixed(1)}` : `增加 ${Math.abs(mileageSaved).toFixed(1)}`} km）`,
                    `受影响波次 <strong>${escapeHtml(waveText)}</strong>`,
                    `落库：店铺尝试 ${Number(attemptTrace.store_attempt_rows || 0)} 条，试插明细 ${Number(attemptTrace.insert_trial_rows || 0)} 条`
                ].join(" | ");
            }

            const firstAdjust = affectedShops[0] || {};
            const decisionLines = [
                `唯一变量证明：${escapeHtml(report?.variable_proof || auditProof?.proof_text || "仅变更目标店时间，其余输入保持一致。")}`,
                `输入指纹：${escapeHtml(String(auditProof?.fixed_input_fingerprint || "--"))}`,
                `优先建议：先验证本店调整在当前波次是否带来可见收益，再决定是否扩展到同类门店。`
            ];
            decisionEl.innerHTML = decisionLines.map((line) => `<div>${escapeHtml(line)}</div>`).join("");

            const deltaColor = (val) => Number(val || 0) === 0 ? "#334155" : "#dc2626";
            const deltaPrefix = (val) => Number(val || 0) > 0 ? "+" : "";
            const baselineCn = `改前：参与排线 ${Number(abBase.candidate || 0)} 家，成功安排 ${Number(abBase.assigned || 0)} 家，未安排 ${Number(abBase.pending || 0)} 家；使用车辆 ${Number(abBase.vehicles || 0)} 台，总里程 ${formatNumber(abBase.mileage, 1)} 公里。`;
            const simulatedCn = `改后：参与排线 ${Number(abSim.candidate || 0)} 家，成功安排 ${Number(abSim.assigned || 0)} 家，未安排 ${Number(abSim.pending || 0)} 家；使用车辆 ${Number(abSim.vehicles || 0)} 台，总里程 ${formatNumber(abSim.mileage, 1)} 公里。`;
            adjustmentsEl.innerHTML = `
                <div class="tuiyan-report-item">
                    <div><strong>A/B 对照（${escapeHtml(abCompare?.wave_id || report?.wave_id || "--")}）</strong></div>
                    <div class="tuiyan-report-kv">${escapeHtml(report?.baseline_text || baselineCn)}</div>
                    <div class="tuiyan-report-kv">${escapeHtml(report?.simulated_text || simulatedCn)}</div>
                    <div class="tuiyan-report-kv">
                        差值：安排门店 <span style="color:${deltaColor(abDelta.assigned)}">${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)}</span> 家 |
                        未安排 <span style="color:${deltaColor(abDelta.pending)}">${deltaPrefix(abDelta.pending)}${Number(abDelta.pending || 0)}</span> 家 |
                        车辆 <span style="color:${deltaColor(abDelta.vehicles)}">${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)}</span> 台 |
                        里程 <span style="color:${deltaColor(abDelta.mileage)}">${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)} 公里</span>
                    </div>
                    ${report?.delta_text ? `<div class="tuiyan-report-kv" style="margin-top:8px;"><strong>${escapeHtml(report.delta_text)}</strong></div>` : ""}
                </div>
                ${(affectedShops.length ? affectedShops : [{ shop_code: "--", old_wave: "--", new_wave: "--", old_time: "--", new_time: "--", reason: "--" }]).map((shop, index) => `
                    <div class="tuiyan-report-item">
                        <div><strong>${index + 1}. 店${escapeHtml(shop.shop_code || "--")}</strong>：${escapeHtml(shop.old_wave || "--")} -> ${escapeHtml(shop.new_wave || "--")}，${escapeHtml(shop.old_time || "--")} -> <span style="color:#dc2626">${escapeHtml(shop.new_time || "--")}</span></div>
                        <div class="tuiyan-report-kv">原因：${escapeHtml(shop.reason || "调整建议")}</div>
                    </div>
                `).join("")}
            `;
        }

        failuresEl.innerHTML = failureLogs.length
            ? failureLogs.slice(0, 200).map((item) => `
                <div class="tuiyan-report-item">
                    <div><strong>${escapeHtml(item.wave_id || "--")} | 店${escapeHtml(item.shop_code || "--")}</strong>：${escapeHtml(item.suggestion_type || item.failure_type || "--")}</div>
                    <div class="tuiyan-report-kv">当前 ${escapeHtml(item.current_time || "--")}，预计 ${escapeHtml(item.expected_arrival || "--")}，最晚 ${escapeHtml(item.latest_allowed || "--")}，超出 ${Number(item.over_minutes || item.late_minutes || 0)} 分钟</div>
                    <div>${escapeHtml(item.suggestion_text || "暂无建议")}</div>
                </div>
            `).join("")
            : '<div class="tuiyan-report-item">本次无失败样本。</div>';

        const replayItems = simulationReplayLines
            .filter((line) => /尝试#|尝试结束|失败样本|插入回放已落库/.test(String(line || "")))
            .slice(-300);
        replayEl.innerHTML = replayItems.length
            ? replayItems.map((line) => `<div class="tuiyan-report-item">${escapeHtml(line)}</div>`).join("")
            : '<div class="tuiyan-report-item">暂无回放日志。</div>';

        if (mode === "single_store") {
            failuresEl.innerHTML = '<div class="tuiyan-report-item">单店模式默认不展示与目标店无关的失败样本。</div>';
            replayEl.innerHTML = '<div class="tuiyan-report-item">单店模式默认不展示试插回放，查看“推演过程”中的 A/B 日志即可。</div>';
        }

        renderReportPaneVisibility();
    }

    function renderBenchmarkInfo() {
        const container = document.getElementById("benchmark-info");
        if (!container) return;
        if (!currentProcessedData) {
            container.textContent = "暂无基准方案";
            return;
        }
        container.innerHTML = `基准: ${escapeHtml(currentProcessedData.batch_id)} | 用车 ${currentProcessedData.total_vehicles} 辆 | 里程 ${formatNumber(currentProcessedData.total_distance_km)} km | ${escapeHtml(formatDateTime(currentProcessedData.created_at))}`;
    }

    async function renderBatchSelector() {
        const selectEl = document.getElementById("batch-select");
        if (!selectEl) return;
        selectEl.innerHTML = '<option value="">加载中...</option>';
        const list = await loadBatchList(1, 50);
        selectEl.innerHTML = "";
        if (!list.length) {
            selectEl.innerHTML = '<option value="">暂无历史批次</option>';
            return;
        }

        list.forEach((batch) => {
            const option = document.createElement("option");
            option.value = batch.id;
            option.textContent = `${batch.id} (${formatDateTime(batch.createdAt)}) - ${batch.algorithms || batch.strategy || "unknown"}`;
            if (batch.id === latestBatchId) option.selected = true;
            selectEl.appendChild(option);
        });

        if (!selectEl.dataset.bound) {
            selectEl.dataset.bound = "1";
            selectEl.addEventListener("change", async (e) => {
                const batchId = String(e.target.value || "");
                if (!batchId) return;
                const btn = document.getElementById("simulate-btn");
                if (btn) btn.disabled = true;
                await loadBatchDetail(batchId);
                renderAllOutputs();
                if (btn) btn.disabled = false;
            });
        }
    }

    function renderStoreTable() {
        const container = document.getElementById("store-table-body");
        if (!container) return;
        const stores = currentProcessedData?.stores || [];
        if (!stores.length) {
            container.innerHTML = '<tr><td colspan="8" style="text-align:center">暂无数据，请先选择批次</td></tr>';
            return;
        }

        container.innerHTML = stores.slice(0, 30).map((store) => `
            <tr>
                <td>${escapeHtml(store.code)}</td>
                <td>${escapeHtml(store.name)}</td>
                <td>${escapeHtml(store.wave_belongs)}</td>
                <td>${escapeHtml(store.current_time)}</td>
                <td>--:--</td>
                <td>-</td>
                <td>-</td>
                <td>${store.allowed_late <= 10 ? '<span class="priority-high">高</span>' : store.allowed_late <= 20 ? '<span class="priority-medium">中</span>' : '<span class="priority-low">低</span>'}</td>
            </tr>
        `).join("");
    }

    function renderWaveSummary() {
        const container = document.getElementById("wave-summary");
        if (!container) return;
        const waveResults = currentProcessedData?.wave_results;
        if (!waveResults) {
            container.innerHTML = "暂无数据，请先选择批次";
            return;
        }

        const waves = ["W1", "W2", "W3", "W4"];
        container.innerHTML = `<div class="wave-summary-grid">${
            waves.map((waveId) => {
                const data = waveResults[waveId];
                if (!data) {
                    return `<div class="wave-card"><div class="wave-title">${waveId}</div><div class="wave-stats">暂无数据</div></div>`;
                }
                return `
                    <div class="wave-card">
                        <div class="wave-title">${waveId}</div>
                        <div class="wave-stats">
                            调度门店: ${data.scheduled_count}<br>
                            未调度门店: ${data.unscheduled_count}<br>
                            总里程: ${formatNumber(data.total_distance_km)} km<br>
                            用车数: ${data.vehicles.length}
                        </div>
                    </div>
                `;
            }).join("")
        }</div>`;
    }

    function renderRankings() {
        const container = document.getElementById("rankings");
        if (!container) return;
        const waves = Object.values(currentProcessedData?.wave_results || {}).filter(Boolean);
        if (!waves.length) {
            container.innerHTML = "暂无数据";
            return;
        }
        const rankedByDistance = waves
            .slice()
            .sort((a, b) => Number(b.total_distance_km || 0) - Number(a.total_distance_km || 0));
        container.innerHTML = `
            <ul class="rank-list">
                ${rankedByDistance.map((item, index) => `
                    <li class="rank-item">
                        <span class="rank-number">${index + 1}</span>
                        <span class="rank-store">${item.wave_id}</span>
                        <span class="rank-time">门店 ${item.scheduled_count}</span>
                        <span class="rank-save">${formatNumber(item.total_distance_km)} km</span>
                    </li>
                `).join("")}
            </ul>
        `;
    }

    function renderGlobalPlan() {
        const container = document.getElementById("global-plan");
        if (!container) return;
        if (!currentProcessedData) {
            container.innerHTML = "暂无数据";
            return;
        }
        container.innerHTML = `
            <div class="global-summary">
                <div style="font-weight:600; margin-bottom:8px;">当前基准方案</div>
                <div class="global-metrics">
                    <div class="metric">
                        <div class="metric-value">${currentProcessedData.total_vehicles}</div>
                        <div class="metric-label">总用车</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${formatNumber(currentProcessedData.total_distance_km)}</div>
                        <div class="metric-label">总里程(km)</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${currentProcessedData.total_stores}</div>
                        <div class="metric-label">门店数</div>
                    </div>
                </div>
            </div>
            <div class="route-preview">
                <div class="route-item">
                    <div class="route-vehicle">当前批次</div>
                    <div class="route-stores">${escapeHtml(currentProcessedData.batch_id)}</div>
                </div>
                <div class="route-item">
                    <div class="route-vehicle">算法</div>
                    <div class="route-stores">${escapeHtml(currentProcessedData.strategy || "--")}</div>
                </div>
                <div class="route-item">
                    <div class="route-vehicle">创建时间</div>
                    <div class="route-stores">${escapeHtml(currentProcessedData.created_at || "--")}</div>
                </div>
            </div>
        `;
    }

    function setupSimulationControls() {
        const simulateBtn = document.getElementById("simulate-btn");
        if (!simulateBtn || simulateBtn.dataset.bound) return;
        simulateBtn.dataset.bound = "1";
        simulateBtn.addEventListener("click", async () => {
            renderSimulationModeUI();
            const isFleetMode = simulateMode === "fleet_feasibility";
            const storeId = document.getElementById("simulate-store-id")?.value.trim();
            const newTime = document.getElementById("simulate-new-time")?.value;
            const target = document.getElementById("simulate-target")?.value || "min_vehicles";
            const targetVehicles = Number(document.getElementById("simulate-target-vehicles")?.value || 0);
            const selectedWaves = getSelectedSimulationWaves();
            const realtimeContext = getRealtimeDispatchContext();
            const startedAt = Date.now();
            const newWave = "W1";

            if (!isFleetMode) {
                if (!storeId) {
                    renderSimulationProgress("error", [{ text: "输入校验失败：缺少店铺编号", state: "error" }], { storeId, newTime, target });
                    alert("请输入店铺编号");
                    return;
                }
                if (!newTime) {
                    renderSimulationProgress("error", [{ text: "输入校验失败：缺少新收货时间", state: "error" }], { storeId, newTime, target });
                    alert("请选择新时间");
                    return;
                }
            } else {
                if (!Number.isFinite(targetVehicles) || targetVehicles <= 0) {
                    renderSimulationProgress("error", [{ text: "输入校验失败：目标车辆数无效", state: "error" }], { targetVehicles });
                    alert("请输入有效的目标车辆数");
                    return;
                }
                if (!selectedWaves.length) {
                    renderSimulationProgress("error", [{ text: "输入校验失败：未选择推演波次", state: "error" }], { selectedWaves });
                    alert("请至少选择一个推演波次");
                    return;
                }
            }
            if (!realtimeContext || !Array.isArray(realtimeContext.vehicles) || !realtimeContext.vehicles.length) {
                renderSimulationProgress("error", [{ text: "未检测到当前车辆列表", state: "error" }], { hasRealtimeContext: Boolean(realtimeContext) });
                alert("请先回到调度页导入当前车辆列表并保存最新货量，再进入推演页。");
                return;
            }

            const taskId = buildSimulationTaskId();
            openSimulationRelayConsole();
            startSimulationLogPolling(taskId);

            renderSimulationProgress("running", [
                { text: "1. 校验输入参数", state: "success" },
                { text: `2. 读取当前调度上下文：车辆 ${realtimeContext.vehicles.length} 辆`, state: "success" },
                { text: "3. 提交后台推演请求", state: "active" },
                { text: "4. 等待后台求解返回", state: "" },
                { text: "5. 渲染结果", state: "" }
            ], {
                batch_id: currentProcessedData?.batch_id,
                storeId,
                newWave,
                newTime,
                target,
                mode: simulateMode,
                target_vehicle_count: targetVehicles,
                selected_waves: selectedWaves,
                vehicles: realtimeContext.vehicles.length,
                waves: Array.isArray(realtimeContext.waves) ? realtimeContext.waves.length : 0
            });

            simulateBtn.disabled = true;
            simulateBtn.textContent = "推演中...";
            try {
                const requestPayload = isFleetMode
                    ? {
                        mode: "fleet_feasibility",
                        target,
                        target_vehicle_count: targetVehicles,
                        selected_waves: selectedWaves,
                        adjustments: [],
                        task_id: taskId
                    }
                    : {
                        mode: "single_store",
                        target,
                        adjustments: [{
                            shop_code: storeId,
                            new_wave: newWave,
                            new_time: newTime
                        }],
                        task_id: taskId
                    };
                latestSimulationRequest = requestPayload;
                const result = await callSimulateAPI(requestPayload);
                await pollSimulationTaskLog(taskId, true);
                if (result?.success) {
                    let displayResult = result;
                    if (!isFleetMode) {
                        const dbResult = await loadSingleStoreSimulationReport(taskId);
                        if (dbResult?.success) {
                            displayResult = dbResult;
                        }
                    }
                    latestSimulationResult = displayResult;
                    renderSimulationProgress("success", [
                        { text: "1. 校验输入参数", state: "success" },
                        { text: `2. 读取当前调度上下文：车辆 ${realtimeContext.vehicles.length} 辆`, state: "success" },
                        { text: "3. 提交后台推演请求", state: "success" },
                        { text: "4. 后台求解完成", state: "success" },
                        { text: `5. 渲染结果（耗时 ${((Date.now() - startedAt) / 1000).toFixed(1)} 秒）`, state: "success" }
                    ], displayResult);
                    renderSimulationReport();
                    if (!isFleetMode) {
                        showSimulateModal(displayResult, storeId, newTime);
                    }
                } else {
                    latestSimulationResult = null;
                    renderSimulationProgress("error", [
                        { text: "1. 校验输入参数", state: "success" },
                        { text: `2. 读取当前调度上下文：车辆 ${realtimeContext.vehicles.length} 辆`, state: "success" },
                        { text: "3. 提交后台推演请求", state: "success" },
                        { text: "4. 后台返回失败", state: "error" },
                        { text: `5. 停止渲染（耗时 ${((Date.now() - startedAt) / 1000).toFixed(1)} 秒）`, state: "error" }
                    ], result);
                    alert(`推演失败：${result?.message || "未知错误"}`);
                }
            } catch (error) {
                await pollSimulationTaskLog(taskId, true);
                renderSimulationProgress("error", [
                    { text: "1. 校验输入参数", state: "success" },
                    { text: `2. 读取当前调度上下文：车辆 ${realtimeContext?.vehicles?.length || 0} 辆`, state: "success" },
                    { text: "3. 调用推演接口", state: "error" }
                ], { error: error?.message || String(error) });
                alert(`推演失败：${error?.message || ""}`);
            } finally {
                stopSimulationLogPolling();
                simulateBtn.disabled = false;
                renderSimulationModeUI();
            }
        });
    }

    function renderAllOutputs() {
        renderBenchmarkInfo();
        renderSimulationReport();
        renderStoreTable();
        renderWaveSummary();
        renderRankings();
        renderGlobalPlan();
    }

    // 混沌页初始化入口。
    // 页面打开后会在这里拉取批次、店铺、波次时间、报告区默认状态等前置数据。
    async function initBusinessFunctions() {
        console.log("推演页业务功能初始化...");
        const container = document.querySelector("#tuiyanPage");
        if (container) container.style.opacity = "0.5";
        try {
            await Promise.all([loadShops(), loadShopWaveTimes()]);
            await loadLatestBatch();
            await renderBatchSelector();
            setupReportTabs();
            setupSimulationModeControls();
            renderSimulationProgress("idle", [{ text: "等待开始推演...", state: "" }], null);
            renderAllOutputs();
            setupSimulationControls();
            window.__tuiyanDebug = { currentBatch, currentProcessedData, allShops, shopWaveTimes };
            console.log("推演页初始化完成", { currentProcessedData });
        } catch (error) {
            console.error("业务初始化失败:", error);
            const benchmarkInfo = document.getElementById("benchmark-info");
            if (benchmarkInfo) benchmarkInfo.textContent = "加载失败，请检查后端服务";
        } finally {
            if (container) container.style.opacity = "1";
        }
    }

    function initPageSwitch() {
        const page = document.getElementById("tuiyanPage");
        const openBtn = document.getElementById("openTuiyanBtn");
        const closeBtn = document.getElementById("closeTuiyanBtn");
        const shell = document.querySelector("main.shell");
        if (!page || !openBtn || !closeBtn || !shell) return;

        const allSections = Array.from(shell.children).filter((node) => node !== page);
        const hiddenFlag = "tuiyanHiddenBeforeOpen";

        const openPage = () => {
            allSections.forEach((node) => {
                node.dataset[hiddenFlag] = node.classList.contains("tab-hidden") ? "1" : "0";
                node.classList.add("tab-hidden");
                node.setAttribute("aria-hidden", "true");
            });
            page.classList.remove("tab-hidden");
            page.setAttribute("aria-hidden", "false");
            window.scrollTo({ top: 0, behavior: "smooth" });
            void initBusinessFunctions();
        };

        const closePage = () => {
            allSections.forEach((node) => {
                if (node.dataset[hiddenFlag] !== "1") {
                    node.classList.remove("tab-hidden");
                    node.removeAttribute("aria-hidden");
                }
                delete node.dataset[hiddenFlag];
            });
            page.classList.add("tab-hidden");
            page.setAttribute("aria-hidden", "true");
            window.scrollTo({ top: 0, behavior: "smooth" });
        };

        openBtn.addEventListener("click", openPage);
        closeBtn.addEventListener("click", closePage);

        if (sessionStorage.getItem("openChaosOnLanding") === "1") {
            sessionStorage.removeItem("openChaosOnLanding");
            openPage();
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initPageSwitch, { once: true });
    } else {
        initPageSwitch();
    }
})();
