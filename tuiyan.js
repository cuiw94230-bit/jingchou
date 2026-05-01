/*
 * 娣锋矊锛堟帹婕旈〉锛夊墠绔剼鏈€? *
 * 杩欓噷闆嗕腑缁存姢涓ょ被鎺ㄦ紨锛? * 1. 鍗曞簵鏃堕棿鎺ㄦ紨锛氬悓涓€娉㈡鍋氭敼鍓?鏀瑰悗 A/B 鐪熸眰瑙ｃ€? * 2. 鍑忚溅/鍙鎬ф帹婕旓細鍥寸粫澶辫触鏍锋湰銆佺害鏉熻瘖鏂拰寤鸿瑙勫垯灞曞紑銆? */
(function () {
    const GA_BACKEND_URL = "";

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
            console.log("閹佃顐奸崚妤勩€冮崫宥呯安閻樿埖鈧?", response.status);
            const result = await response.json();
            result._httpStatus = response.status;
            console.log("閹佃顐奸崚妤勩€冮崫宥呯安閺佺増宓?", result);
            return Array.isArray(result?.items) ? result.items : [];
        } catch (error) {
            console.error("鍔犺浇鎵规鍒楄〃鍑洪敊:", error);
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
            console.log("鎵规璇︽儏鍝嶅簲鏁版嵁:", result);
            if (!result?.item) throw new Error("鑾峰彇鎵规璇︽儏澶辫触");
            currentBatch = result.item;
            currentProcessedData = processSnapshot(currentBatch);
            latestSimulationResult = null;
            latestSimulationRequest = null;
            simulationReplayLines = [];
            window.__tuiyanDebug = { currentBatch, currentProcessedData, allShops, shopWaveTimes };
            return currentProcessedData;
        } catch (error) {
            console.error("鍔犺浇鎵规璇︽儏鍑洪敊:", error);
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
            console.log("閹佃顐奸崚妤勩€冮崫宥呯安閻樿埖鈧?", response.status, "date=", date);
            const result = await response.json();
            console.log("閹佃顐奸崚妤勩€冮崫宥呯安閺佺増宓?", result);
            return Array.isArray(result?.items) ? result.items : [];
        } catch (error) {
            console.error("鍔犺浇鎵规鍒楄〃鍑洪敊:", error);
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
            console.log("闂ㄥ簵鍩虹淇℃伅鍝嶅簲鏁版嵁:", result);
            allShops = Array.isArray(result?.shops) ? result.shops : [];
            return allShops;
        } catch (error) {
            console.error("鍔犺浇闂ㄥ簵淇℃伅鍑洪敊:", error);
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
            console.log("闂ㄥ簵娉㈡鏃堕棿鍝嶅簲鏁版嵁:", result);
            shopWaveTimes = Array.isArray(result?.items) ? result.items : [];
            return shopWaveTimes;
        } catch (error) {
            console.error("鍔犺浇闂ㄥ簵娉㈡鏃堕棿鍑洪敊:", error);
            return [];
        }
    }

    function processSnapshot(snapshot) {
        if (!snapshot) return null;
        console.log("processSnapshot缁撴灉缁撴瀯:", {
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
            console.warn("璇诲彇 window.__dispatchRealtimeContext 澶辫触:", error);
        }
        try {
            const raw = localStorage.getItem(REALTIME_DISPATCH_CONTEXT_KEY);
            if (!raw) return null;
            const parsed = JSON.parse(raw);
            return parsed && typeof parsed === "object" ? parsed : null;
        } catch (error) {
            console.warn("璇诲彇鏈湴璋冨害涓婁笅鏂囧け璐?", error);
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
                status === "running" ? "鎵ц涓? :
                status === "success" ? "宸插畬鎴? :
                status === "error" ? "澶辫触" :
                "寰呮墽琛?;
        }
        if (stepsEl) {
            const list = Array.isArray(steps) && steps.length ? steps : [{ text: "绛夊緟寮€濮嬫帹婕?..", state: "" }];
            stepsEl.innerHTML = list.map((step) => {
                const state = String(step?.state || "").trim();
                const className = ["simulate-step", state ? `is-${state}` : ""].filter(Boolean).join(" ");
                return `<div class="${className}">${escapeHtml(String(step?.text || ""))}</div>`;
            }).join("");
        }
        if (debugEl) {
            if (!payload) {
                debugEl.textContent = "鏆傛棤杩囩▼鏁版嵁";
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
            window.openRelayConsoleModal("姹傝В杩囩▼鍙鍖栫獥鍙?);
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

    // 鎺ㄦ紨鎺ュ彛缁熶竴鍏ュ彛銆?    // 鍓嶇鍏堟妸妯″紡銆佹壒娆°€佺洰鏍囥€佸簵閾?杞﹁締绾︽潫鏁寸悊鎴?requestPayload锛屽啀鐢卞悗绔喅瀹氬叿浣撹蛋鍝潯鎺ㄦ紨鍒嗘敮銆?    async function callSimulateAPI(requestPayload) {
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
                console.log("鎺ㄦ紨鎴愬姛:", result?.data?.improvements);
            }
            return result;
        } catch (error) {
            console.error("鎺ㄦ紨鎺ュ彛璋冪敤澶辫触:", error);
            return { success: false, message: error.message };
        }
    }

    // 鍗曞簵鎺ㄦ紨缁撴灉浼樺厛浠庢暟鎹簱鍥炶锛岄伩鍏嶉〉闈㈠彧渚濊禆棣栨杩斿洖鐨勪复鏃?JSON銆?    async function loadSingleStoreSimulationReport(taskId) {
        try {
            const response = await fetch(`${GA_BACKEND_URL}/simulate/single-report?taskId=${encodeURIComponent(taskId)}`);
            const result = await response.json();
            result._httpStatus = response.status;
            return result;
        } catch (error) {
            console.error("鍔犺浇鍗曞簵鎺ㄦ紨瀹¤缁撴灉澶辫触:", error);
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
            console.error("淇濆瓨鍗曞簵绾胯矾鍐崇瓥澶辫触:", error);
            return { success: false, message: error.message };
        }
    }

    // 鍦ㄧ嚎璺汉宸ョ‘璁?鍐嶄紭鍖栦箣鍚庯紝鍩轰簬鈥滄柊鐨勫墿浣欓泦鈥濈户缁彂璧蜂笅涓€杞湡姹傝В銆?    async function continueSingleRouteSolve(taskId) {
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
            console.error("缁х画姹傝В鍓╀綑闆嗗け璐?", error);
            return { success: false, message: error.message };
        }
    }

    // 鎺ㄦ紨寮圭獥鎬绘覆鏌撳嚱鏁般€?    // 杩欓噷浼氬悓鏃舵嫾鍑猴細A/B 瀵圭収銆佺嚎璺鎯呫€佸叾浠栫嚎璺€佸綋鍓嶅€欓€夌嚎璺€佷汉宸ュ喅绛栦笌涓嬩竴杞緭鍏ャ€?    function showSimulateModal(result, storeId, newTime) {
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
            || `鍩虹嚎鏂规锛堟敼鍓嶏級锛氭湰娉㈡鍙備笌鎺掔嚎闂ㄥ簵 ${Number(abBase.candidate || 0)} 瀹讹紝鎴愬姛瀹夋帓 ${Number(abBase.assigned || 0)} 瀹讹紝鏈畨鎺?${Number(abBase.pending || 0)} 瀹讹紱浣跨敤杞﹁締 ${Number(abBase.vehicles || 0)} 鍙帮紝鎬婚噷绋?${formatNumber(abBase.mileage, 1)} 鍏噷銆俙;
        const simulatedText = report?.simulated_text
            || `鎺ㄦ紨鏂规锛堟敼鍚庯級锛氫粎灏嗗簵閾?${escapeHtml(storeId)} 鐨勫埌搴楁椂闂磋皟鏁村埌 ${escapeHtml(newTime)} 鍚庯紝鏈尝娆″弬涓庢帓绾块棬搴?${Number(abSim.candidate || 0)} 瀹讹紝鎴愬姛瀹夋帓 ${Number(abSim.assigned || 0)} 瀹讹紝鏈畨鎺?${Number(abSim.pending || 0)} 瀹讹紱浣跨敤杞﹁締 ${Number(abSim.vehicles || 0)} 鍙帮紝鎬婚噷绋?${formatNumber(abSim.mileage, 1)} 鍏噷銆俙;
        const deltaText = report?.delta_text
            || `杩欐鏀瑰崟搴楁椂闂村悗鐨勫彉鍖栦负锛氬畨鎺掗棬搴楀彉鍖?${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)} 瀹讹紝杞﹁締鍙樺寲 ${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)} 鍙帮紝鎬婚噷绋嬪彉鍖?${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)} 鍏噷銆俙;
        const proofText = report?.variable_proof || auditProof?.proof_text || "浠呭彉鏇寸洰鏍囧簵鏃堕棿锛屽叾浣欒緭鍏ヤ繚鎸佷竴鑷淬€?;

        let html = `
            <div style="background: #f0fdf4; padding: 12px; border-radius: 8px; margin-bottom: 16px;">
                <strong>浼樺寲鏁堟灉</strong><br>
                鐢ㄨ溅: ${Number(before.total_vehicles || 0)} 杈?-> ${Number(after.total_vehicles || 0)} 杈?(${vehiclesSaved > 0 ? `鍑忓皯 ${vehiclesSaved} 杈哷 : "鏈噺灏?})<br>
                閲岀▼: ${Number(before.total_mileage || 0).toFixed(1)} km -> ${Number(after.total_mileage || 0).toFixed(1)} km (${mileageSaved > 0 ? `鍑忓皯 ${mileageSaved.toFixed(1)} km` : "鏈噺灏?})
            </div>
        `;

        html += `<div style="background: #eef2ff; padding: 12px; border-radius: 8px; margin-bottom: 16px;">
            <strong>A/B 瀵圭収锛?{escapeHtml(abCompare?.wave_id || report?.wave_id || "--")}锛?/strong><br>
            <div style="margin-top: 8px;">${escapeHtml(baselineText)}</div>
            <div style="margin-top: 6px;">${escapeHtml(simulatedText)}</div>
            <div style="margin-top: 8px;">
                宸€硷細
                瀹夋帓闂ㄥ簵 <span style="color:${deltaColor(abDelta.assigned)}">${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)}</span> 瀹讹紝
                鏈畨鎺?<span style="color:${deltaColor(abDelta.pending)}">${deltaPrefix(abDelta.pending)}${Number(abDelta.pending || 0)}</span> 瀹讹紝
                杞﹁締 <span style="color:${deltaColor(abDelta.vehicles)}">${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)}</span> 鍙帮紝
                閲岀▼ <span style="color:${deltaColor(abDelta.mileage)}">${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)}</span> 鍏噷
            </div>
            <div style="margin-top: 8px;"><strong>${escapeHtml(deltaText)}</strong></div>
        </div>`;

        if (remainingStateSummary?.summary) {
            html += `<div style="background:#ecfeff; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>褰撳墠寰呭啀浼樺寲闆嗗悎</strong><br>
                ${escapeHtml(String(remainingStateSummary.summary || ""))}<br>
                <span style="font-size:12px;color:#475569;">鐘舵€佸簭鍙?${Number(remainingStateSummary.state_seq || 0)} 锝?鍙啀鍒╃敤杞﹁締 ${Number(remainingStateSummary.remaining_vehicle_count || 0)} 鍙?锝?寰呭啀浼樺寲搴楅摵 ${Number(remainingStateSummary.remaining_store_count || 0)} 瀹?/span>
            </div>`;
        }
        if (manualDecisionSummary?.action_type === "manual_route_selection") {
            const pendingStores = Array.isArray(remainingStateSummary?.remaining_stores) ? remainingStateSummary.remaining_stores : [];
            html += `<div style="background:#fff7ed; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>涓嬩竴杞眰瑙ｈ緭鍏?/strong><br>
                宸茬‘瀹氱嚎璺?${Number(manualDecisionSummary.confirm_count || 0)} 鏉★紱鍐嶄紭鍖栫嚎璺?${Number(manualDecisionSummary.reoptimize_count || 0)} 鏉★紱閲婃斁杞﹁締 ${Number(manualDecisionSummary.released_vehicle_count || 0)} 鍙般€?br>
                浣犳爣璁颁负鈥滃啀浼樺寲鈥濈殑绾胯矾宸叉仮澶嶆垚寰呭啀浼樺寲搴楅摵闆嗗悎锛屼笅鏂瑰氨鏄笅涓€杞湡姹傝В鐨勮緭鍏ュ簵閾猴紙鍓?20 瀹讹級锛?br>
                <div style="margin-top:8px; max-height:180px; overflow:auto; background:#fff; border:1px solid #fed7aa; border-radius:8px; padding:8px;">
                    ${pendingStores.slice(0, 20).map((item, idx) => `
                        <div style="display:flex; gap:12px; font-size:13px; padding:4px 0; border-bottom:${idx < Math.min(pendingStores.length, 20) - 1 ? "1px solid #ffedd5" : "none"};">
                            <span style="width:84px; color:#0f172a;">${escapeHtml(item.shop_code || "--")}</span>
                            <span style="flex:1; color:#334155;">${escapeHtml(item.shop_name || "--")}</span>
                            <span style="width:70px; color:#475569;">${escapeHtml(item.expected_time || "--:--")}</span>
                        </div>
                    `).join("") || '<div style="color:#64748b;">鏆傛棤寰呭啀浼樺寲搴楅摵</div>'}
                </div>
                <div style="display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; margin-top:12px;">
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>鍑忓皯鍑犺締杞?/span>
                        <input id="continue-reduce-vehicles" type="number" min="0" value="0" style="padding:8px 10px; border:1px solid #cbd5e1; border-radius:8px;">
                    </label>
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>姣忚溅鑷冲皯鍑犱釜搴?/span>
                        <input id="continue-min-stores" type="number" min="0" value="0" style="padding:8px 10px; border:1px solid #cbd5e1; border-radius:8px;">
                    </label>
                    <label style="display:flex; flex-direction:column; gap:6px; font-size:13px;">
                        <span>瑁呰浇鐜囦笉浣庝簬澶氬皯%</span>
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
                const scheduledTime = String(stop?.scheduled_time || stop?.arrival || "姹傝В鍣ㄦ湭杩斿洖鍒板簵鏃堕棿").trim() || "姹傝В鍣ㄦ湭杩斿洖鍒板簵鏃堕棿";
                const boxes = Number(stop?.boxes || 0);
                const legText = String(stop?.route_leg_text || "--").trim() || "--";
                return `
                    <tr>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${idx + 1}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:nowrap;">${escapeHtml(shopCode)}</td>
                        <td style="padding:6px 8px; border-bottom:1px solid #e2e8f0; white-space:normal; word-break:break-all;">${escapeHtml(shopName)}${isTarget ? ' <span style="color:#dc2626;font-weight:600;">(鐩爣搴?</span>' : ""}</td>
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
                        <div style="font-weight:700;">${escapeHtml(title)}锛?{escapeHtml(route.wave || "--")} 路 ${escapeHtml(route.vehicle || "--")} 路 绗?{Number(route.trip || 1)}瓒?/div>
                        ${selectable ? `
                            <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
                                <label style="display:flex; gap:6px; align-items:center; font-size:12px;">
                                    <input type="checkbox" class="simulate-route-check" ${routeState.checked ? "checked" : ""}>
                                    閫変腑
                                </label>
                                <select class="simulate-route-action" style="padding:4px 8px; border:1px solid #cbd5e1; border-radius:6px; font-size:12px;">
                                    <option value="confirm" ${routeState.action === "confirm" ? "selected" : ""}>纭畾</option>
                                    <option value="reoptimize" ${routeState.action === "reoptimize" ? "selected" : ""}>鍐嶄紭鍖?/option>
                                </select>
                            </div>
                        ` : ""}
                    </div>
                    <div style="margin-bottom:8px; color:#334155;">
                        绾胯矾閲岀▼ ${Number(route.total_distance || 0).toFixed(1)} km 锝?瑁呰浇 ${formatNumber(loadSummary.actualLoad, 3)} / ${formatNumber(loadSummary.capacityLimit, 3)} 锝?瑁呰浇鐜?<span style="color:${loadSummary.overload ? "#dc2626" : "#334155"}; font-weight:${loadSummary.overload ? "700" : "400"};">${formatNumber(loadSummary.loadPercent, 1)}%</span>${loadSummary.overload ? ' <span style="color:#dc2626;">(瓒呴檺)</span>' : ""}
                    </div>
                    <div style="overflow:auto hidden;">
                        <table style="width:100%; border-collapse:collapse; font-size:13px; table-layout:fixed;">
                            <thead>
                                <tr style="background:#f8fafc;">
                                    <th style="text-align:left; padding:6px 8px; width:52px;">搴忓彿</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">搴楅摵缂栧彿</th>
                                    <th style="text-align:left; padding:6px 8px; width:24%;">搴楀悕</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">閲岀▼鍙樺寲</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">鏈熸湜鍒板簵</th>
                                    <th style="text-align:left; padding:6px 8px; width:92px;">璋冨害鍒板簵</th>
                                    <th style="text-align:left; padding:6px 8px; width:72px;">璐ч噺</th>
                                </tr>
                            </thead>
                            <tbody>${rows || '<tr><td colspan="7" style="padding:8px;">鏃犲仠闈犳槑缁?/td></tr>'}</tbody>
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
                <strong>涓ゆ潯绾胯矾璇︽儏锛堟寜鍋滈潬椤哄簭锛?/strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">璇存槑锛氭湡鏈涘埌搴楁椂闂存潵鑷湰娆/B姹傝В杈撳叆锛涢噷绋嬪彉鍖栨潵鑷暟鎹簱瀹為檯閲岀▼锛涙帹婕旂嚎璺腑鐨勭洰鏍囧簵鏈熸湜鏃堕棿鍗充綘杈撳叆鐨勬帹婕旀椂闂淬€?/div>
                ${renderRouteDetail("鏀瑰墠绾胯矾", oldRoute, "#ef4444", { snapshotType: "baseline" })}
                ${renderRouteDetail("鏀瑰悗绾胯矾", newRoute, "#22c55e", { selectable: true, snapshotType: "simulated" })}
            </div>`;
        }

        if ((otherRoutes?.baseline && otherRoutes.baseline.length) || (otherRoutes?.simulated && otherRoutes.simulated.length)) {
            html += `<div style="background:#f8fafc; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>鍏朵粬绾胯矾缁撴灉</strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">杩欓噷灞曠ず鍚屼竴娉㈡涓嬶紝闄ょ洰鏍囬敋鐐圭嚎璺箣澶栫殑鍏朵綑绾胯矾閫愬簵鏄庣粏锛涙敼鍚庣嚎璺敮鎸佸嬀閫夊苟鏍囨敞鈥滅‘瀹?鍐嶄紭鍖栤€濄€?/div>
                ${renderRouteGroupDetails("鏀瑰墠鍏朵綑绾胯矾", otherRoutes?.baseline || [], "#ef4444", false, "baseline")}
                ${renderRouteGroupDetails("鏀瑰悗鍏朵綑绾胯矾", otherRoutes?.simulated || [], "#22c55e", true, "simulated")}
            </div>`;
        }

        if (currentCandidateRoutes.length) {
            html += `<div style="background:#f0fdf4; padding:12px; border-radius:8px; margin-bottom:16px;">
                <strong>缁х画姹傝В鍚庣殑褰撳墠鍊欓€夌嚎璺?/strong>
                <div style="font-size:12px;color:#64748b;margin-top:4px;">浠ヤ笅绾胯矾鍩轰簬浣犲垰鍒氱‘璁ゅ悗鐨勫墿浣欒溅杈?鍓╀綑闂ㄥ簵閲嶆柊鐪熸眰瑙ｅ緱鍒般€備綘鍙互缁х画鍕鹃€夆€滅‘瀹?鍐嶄紭鍖栤€濓紝鍐嶇偣淇濆瓨绾胯矾鍐崇瓥锛岄殢鍚庣户缁笅涓€杞眰瑙ｃ€?/div>
                ${renderRouteGroupDetails("褰撳墠鍊欓€夌嚎璺?, currentCandidateRoutes, "#16a34a", true, "iterative")}
            </div>`;
        }

        html += `<div style="background: #fef3c7; padding: 12px; border-radius: 8px;">
            <strong>缁撹涓庡彛寰勮鏄?/strong><br>
            ${escapeHtml(proofText)}<br>
            褰撳墠寮圭獥宸蹭笌鈥滄帹婕旇繃绋嬪彲瑙嗗寲鈥濆彛寰勫榻愶紝鍧囨寜鍚屼竴娉㈡鐨?A/B 缁撴灉灞曠ず銆?        </div>`;
        if (scheduleDiagnostics && (scheduleDiagnostics.summary || scheduleDiagnostics.reason)) {
            html += `<div style="background:#fff7ed; padding:12px; border-radius:8px; margin-top:12px;">
                <strong>璋冨害鏃堕棿鏉ユ簮璇婃柇</strong><br>
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
                saveRouteBtn.textContent = "淇濆瓨涓?..";
                const saveResult = await saveSingleRouteDecisions(taskId, decisions);
                saveRouteBtn.disabled = false;
                saveRouteBtn.textContent = originalText;
                if (!saveResult?.success) {
                    alert(`淇濆瓨绾胯矾鍐崇瓥澶辫触锛?{saveResult?.message || saveResult?.error || "鏈煡閿欒"}`);
                    return;
                }
                const refreshed = await loadSingleStoreSimulationReport(taskId);
                if (refreshed?.success) {
                    showSimulateModal(refreshed, storeId, newTime);
                } else {
                    alert(`绾胯矾鍐崇瓥宸蹭繚瀛橈紝浣嗗埛鏂版姤鍛婂け璐ワ細${refreshed?.message || "鏈煡閿欒"}`);
                }
            };
        }
        if (continueRouteBtn) {
            continueRouteBtn.onclick = async () => {
                if (!taskId) return;
                continueRouteBtn.disabled = true;
                const originalText = continueRouteBtn.textContent;
                continueRouteBtn.textContent = "姹傝В涓?..";
                const solveResult = await continueSingleRouteSolve(taskId);
                continueRouteBtn.disabled = false;
                continueRouteBtn.textContent = originalText;
                if (!solveResult?.success) {
                    alert(`缁х画姹傝В澶辫触锛?{solveResult?.message || solveResult?.error || "鏈煡閿欒"}`);
                    return;
                }
                const refreshed = await loadSingleStoreSimulationReport(taskId);
                if (refreshed?.success) {
                    showSimulateModal(refreshed, storeId, newTime);
                } else {
                    alert(`缁х画姹傝В宸插畬鎴愶紝浣嗗埛鏂版姤鍛婂け璐ワ細${refreshed?.message || "鏈煡閿欒"}`);
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

        if (btn) btn.textContent = isFleet ? "寮€濮嬪噺杞︽帹婕? : "寮€濮嬪崟搴楁帹婕?;
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
            summaryEl.innerHTML = `褰撳墠鍩哄噯鎵规 <strong>${escapeHtml(baseline.batch_id || "--")}</strong>锛屾€荤敤杞?${Number(baseline.total_vehicles || 0)} 杈嗭紝鎬婚噷绋?${formatNumber(baseline.total_distance_km || 0)} km銆俙;
            decisionEl.innerHTML = "绛夊緟鎺ㄦ紨缁撴灉銆傛墽琛屼竴娆℃帹婕斿悗锛岃繖閲屼細缁欏嚭鍙墽琛屽缓璁紙鏃堕棿璋冩暣/娉㈡璋冩暣/绾︽潫鏀惧锛夈€?;
            diagnosisEl.innerHTML = "绛夊緟鎺ㄦ紨缁撴灉銆傛墽琛屼竴娆℃帹婕斿悗锛岃繖閲屼細鏄剧ず绾︽潫鍒嗗竷锛堟椂闂寸獥/娉㈡缁撴潫/閲岀▼/瀹归噺/闂ㄥ簵鏁帮級銆?;
            adjustmentsEl.innerHTML = '<div class="tuiyan-report-item">鏆傛棤璋冩暣娓呭崟</div>';
            failuresEl.innerHTML = '<div class="tuiyan-report-item">鏆傛棤澶辫触鏍锋湰</div>';
            replayEl.innerHTML = '<div class="tuiyan-report-item">鏆傛棤璇曟彃鍥炴斁</div>';
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
                    arrival_window: "鏃堕棿绐楄秴鏃?,
                    wave_end: "娉㈡缁撴潫瓒呮椂",
                    mileage: "閲岀▼瓒呴檺",
                    capacity: "瑁呰浇瓒呴檺",
                    max_stops: "闂ㄥ簵鏁拌秴闄?,
                    unknown: "鏈煡绾︽潫"
                }[key] || key;
                return `<div>${escapeHtml(zh)}锛?{count} 瀹?/div>`;
            });
        diagnosisEl.innerHTML = diagnosisLines.length
            ? diagnosisLines.join("")
            : "<div>鏈鎺ㄦ紨鏈骇鐢熷け璐ユ牱鏈€?/div>";

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
                return `${escapeHtml(waveId)} 鏈畨鎺?${count} 瀹禶;
            }).join("锛?);

            summaryEl.innerHTML = `缁撹锛氭垜浠皾璇曠敤 <strong>${Number(request.target_vehicle_count || vehiclesAfter || 0)}</strong> 杈嗚溅瀹屾垚 <strong>${escapeHtml((scopeWaves.length ? scopeWaves.join("/") : waveText) || "--")}</strong>锛岀粨鏋?${waveResultText || "鎵€鏈夌洰鏍囨尝娆″潎鍙畬鎴?}銆俙;
            decisionEl.innerHTML = [
                "寤鸿鎸変互涓嬮『搴忓鐞嗭細鍏堟敼闂ㄥ簵鏃堕棿锛屽啀鏀惧閲岀▼涓婇檺锛屾渶鍚庤皟鏁存尝娆℃埅姝㈡椂闂淬€?,
                "鑻ュけ璐ユ牱鏈富瑕佷负娉㈡缁撴潫瓒呮椂锛屼紭鍏堟敼鎴鏃跺埢锛涜嫢涓昏涓洪噷绋嬭秴闄愶紝浼樺厛鏀归噷绋嬩笂闄愩€?,
                "姣忔璋冩暣鍚庝繚鎸佸悓鏍疯溅杈嗘暟澶嶇畻锛岀洿鍒扮洰鏍囨尝娆℃棤鏈畨鎺掗棬搴椼€?
            ].map((line) => `<div>${escapeHtml(line)}</div>`).join("");

            const timeAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("time"))
                .slice(0, 80)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 搴?{escapeHtml(item.shop_code || "--")}</strong>锛氬缓璁椂闂?${escapeHtml(item.suggested_time || "--")}锛堝綋鍓?${escapeHtml(item.current_time || "--")}锛?/div>`);
            const mileageAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("mileage"))
                .slice(0, 40)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 搴?{escapeHtml(item.shop_code || "--")}</strong>锛氶噷绋嬩笂闄愬缓璁斁瀹藉埌 ${formatNumber(item.required_limit_km, 1)} km锛堝綋鍓?${formatNumber(item.current_limit_km, 1)} km锛?/div>`);
            const waveEndAdvice = failureLogs
                .filter((item) => String(item?.suggestion_type || "").includes("wave_end"))
                .slice(0, 40)
                .map((item) => `<div class="tuiyan-report-item"><strong>${escapeHtml(item.wave_id || "--")} | 搴?{escapeHtml(item.shop_code || "--")}</strong>锛氭尝娆℃埅姝㈠缓璁皟鏁村埌 ${escapeHtml(item.suggested_wave_end_time || "--")}</div>`);
            const adviceBlocks = [...timeAdvice, ...mileageAdvice, ...waveEndAdvice];
            adjustmentsEl.innerHTML = adviceBlocks.length
                ? adviceBlocks.join("")
                : '<div class="tuiyan-report-item">褰撳墠杞﹁締瑙勬ā涓嬶紝鏆傛棤棰濆瑙勫垯璋冩暣寤鸿銆?/div>';
        } else {
            const auditProof = resultData?.audit_proof || {};
            const abCompare = resultData?.ab_compare || {};
            const abBase = abCompare?.baseline || {};
            const abSim = abCompare?.simulated || {};
            const abDelta = abCompare?.delta || {};
            const report = resultData?.report || {};
            const hasReadableReport = Boolean(report?.baseline_text || report?.simulated_text || report?.delta_text);
            if (hasReadableReport) {
                summaryEl.innerHTML = `缁撹锛?strong>${escapeHtml(report.delta_text || "鏈鎺ㄦ紨宸插畬鎴愶紝璇锋煡鐪嬩笅鏂瑰鐓ц鎯呫€?)}</strong>`;
            } else {
                summaryEl.innerHTML = [
                    `缁撹锛氱敤杞?<strong>${vehiclesBefore} -> ${vehiclesAfter}</strong>锛?{vehiclesSaved >= 0 ? `鍑忓皯 ${vehiclesSaved}` : `澧炲姞 ${Math.abs(vehiclesSaved)}`} 杈嗭級`,
                    `閲岀▼ <strong>${mileageBefore.toFixed(1)} -> ${mileageAfter.toFixed(1)} km</strong>锛?{mileageSaved >= 0 ? `鍑忓皯 ${mileageSaved.toFixed(1)}` : `澧炲姞 ${Math.abs(mileageSaved).toFixed(1)}`} km锛塦,
                    `鍙楀奖鍝嶆尝娆?<strong>${escapeHtml(waveText)}</strong>`,
                    `钀藉簱锛氬簵閾哄皾璇?${Number(attemptTrace.store_attempt_rows || 0)} 鏉★紝璇曟彃鏄庣粏 ${Number(attemptTrace.insert_trial_rows || 0)} 鏉
                ].join(" | ");
            }

            const firstAdjust = affectedShops[0] || {};
            const decisionLines = [
                `鍞竴鍙橀噺璇佹槑锛?{escapeHtml(report?.variable_proof || auditProof?.proof_text || "浠呭彉鏇寸洰鏍囧簵鏃堕棿锛屽叾浣欒緭鍏ヤ繚鎸佷竴鑷淬€?)}`,
                `杈撳叆鎸囩汗锛?{escapeHtml(String(auditProof?.fixed_input_fingerprint || "--"))}`,
                `浼樺厛寤鸿锛氬厛楠岃瘉鏈簵璋冩暣鍦ㄥ綋鍓嶆尝娆℃槸鍚﹀甫鏉ュ彲瑙佹敹鐩婏紝鍐嶅喅瀹氭槸鍚︽墿灞曞埌鍚岀被闂ㄥ簵銆俙
            ];
            decisionEl.innerHTML = decisionLines.map((line) => `<div>${escapeHtml(line)}</div>`).join("");

            const deltaColor = (val) => Number(val || 0) === 0 ? "#334155" : "#dc2626";
            const deltaPrefix = (val) => Number(val || 0) > 0 ? "+" : "";
            const baselineCn = `鏀瑰墠锛氬弬涓庢帓绾?${Number(abBase.candidate || 0)} 瀹讹紝鎴愬姛瀹夋帓 ${Number(abBase.assigned || 0)} 瀹讹紝鏈畨鎺?${Number(abBase.pending || 0)} 瀹讹紱浣跨敤杞﹁締 ${Number(abBase.vehicles || 0)} 鍙帮紝鎬婚噷绋?${formatNumber(abBase.mileage, 1)} 鍏噷銆俙;
            const simulatedCn = `鏀瑰悗锛氬弬涓庢帓绾?${Number(abSim.candidate || 0)} 瀹讹紝鎴愬姛瀹夋帓 ${Number(abSim.assigned || 0)} 瀹讹紝鏈畨鎺?${Number(abSim.pending || 0)} 瀹讹紱浣跨敤杞﹁締 ${Number(abSim.vehicles || 0)} 鍙帮紝鎬婚噷绋?${formatNumber(abSim.mileage, 1)} 鍏噷銆俙;
            adjustmentsEl.innerHTML = `
                <div class="tuiyan-report-item">
                    <div><strong>A/B 瀵圭収锛?{escapeHtml(abCompare?.wave_id || report?.wave_id || "--")}锛?/strong></div>
                    <div class="tuiyan-report-kv">${escapeHtml(report?.baseline_text || baselineCn)}</div>
                    <div class="tuiyan-report-kv">${escapeHtml(report?.simulated_text || simulatedCn)}</div>
                    <div class="tuiyan-report-kv">
                        宸€硷細瀹夋帓闂ㄥ簵 <span style="color:${deltaColor(abDelta.assigned)}">${deltaPrefix(abDelta.assigned)}${Number(abDelta.assigned || 0)}</span> 瀹?|
                        鏈畨鎺?<span style="color:${deltaColor(abDelta.pending)}">${deltaPrefix(abDelta.pending)}${Number(abDelta.pending || 0)}</span> 瀹?|
                        杞﹁締 <span style="color:${deltaColor(abDelta.vehicles)}">${deltaPrefix(abDelta.vehicles)}${Number(abDelta.vehicles || 0)}</span> 鍙?|
                        閲岀▼ <span style="color:${deltaColor(abDelta.mileage)}">${deltaPrefix(abDelta.mileage)}${formatNumber(abDelta.mileage, 1)} 鍏噷</span>
                    </div>
                    ${report?.delta_text ? `<div class="tuiyan-report-kv" style="margin-top:8px;"><strong>${escapeHtml(report.delta_text)}</strong></div>` : ""}
                </div>
                ${(affectedShops.length ? affectedShops : [{ shop_code: "--", old_wave: "--", new_wave: "--", old_time: "--", new_time: "--", reason: "--" }]).map((shop, index) => `
                    <div class="tuiyan-report-item">
                        <div><strong>${index + 1}. 搴?{escapeHtml(shop.shop_code || "--")}</strong>锛?{escapeHtml(shop.old_wave || "--")} -> ${escapeHtml(shop.new_wave || "--")}锛?{escapeHtml(shop.old_time || "--")} -> <span style="color:#dc2626">${escapeHtml(shop.new_time || "--")}</span></div>
                        <div class="tuiyan-report-kv">鍘熷洜锛?{escapeHtml(shop.reason || "璋冩暣寤鸿")}</div>
                    </div>
                `).join("")}
            `;
        }

        failuresEl.innerHTML = failureLogs.length
            ? failureLogs.slice(0, 200).map((item) => `
                <div class="tuiyan-report-item">
                    <div><strong>${escapeHtml(item.wave_id || "--")} | 搴?{escapeHtml(item.shop_code || "--")}</strong>锛?{escapeHtml(item.suggestion_type || item.failure_type || "--")}</div>
                    <div class="tuiyan-report-kv">褰撳墠 ${escapeHtml(item.current_time || "--")}锛岄璁?${escapeHtml(item.expected_arrival || "--")}锛屾渶鏅?${escapeHtml(item.latest_allowed || "--")}锛岃秴鍑?${Number(item.over_minutes || item.late_minutes || 0)} 鍒嗛挓</div>
                    <div>${escapeHtml(item.suggestion_text || "鏆傛棤寤鸿")}</div>
                </div>
            `).join("")
            : '<div class="tuiyan-report-item">鏈鏃犲け璐ユ牱鏈€?/div>';

        const replayItems = simulationReplayLines
            .filter((line) => /灏濊瘯#|灏濊瘯缁撴潫|澶辫触鏍锋湰|鎻掑叆鍥炴斁宸茶惤搴?.test(String(line || "")))
            .slice(-300);
        replayEl.innerHTML = replayItems.length
            ? replayItems.map((line) => `<div class="tuiyan-report-item">${escapeHtml(line)}</div>`).join("")
            : '<div class="tuiyan-report-item">鏆傛棤鍥炴斁鏃ュ織銆?/div>';

        if (mode === "single_store") {
            failuresEl.innerHTML = '<div class="tuiyan-report-item">鍗曞簵妯″紡榛樿涓嶅睍绀轰笌鐩爣搴楁棤鍏崇殑澶辫触鏍锋湰銆?/div>';
            replayEl.innerHTML = '<div class="tuiyan-report-item">鍗曞簵妯″紡榛樿涓嶅睍绀鸿瘯鎻掑洖鏀撅紝鏌ョ湅鈥滄帹婕旇繃绋嬧€濅腑鐨?A/B 鏃ュ織鍗冲彲銆?/div>';
        }

        renderReportPaneVisibility();
    }

    function renderBenchmarkInfo() {
        const container = document.getElementById("benchmark-info");
        if (!container) return;
        if (!currentProcessedData) {
            container.textContent = "鏆傛棤鍩哄噯鏂规";
            return;
        }
        container.innerHTML = `鍩哄噯: ${escapeHtml(currentProcessedData.batch_id)} | 鐢ㄨ溅 ${currentProcessedData.total_vehicles} 杈?| 閲岀▼ ${formatNumber(currentProcessedData.total_distance_km)} km | ${escapeHtml(formatDateTime(currentProcessedData.created_at))}`;
    }

    async function renderBatchSelector() {
        const selectEl = document.getElementById("batch-select");
        if (!selectEl) return;
        selectEl.innerHTML = '<option value="">鍔犺浇涓?..</option>';
        const list = await loadBatchList(1, 50);
        selectEl.innerHTML = "";
        if (!list.length) {
            selectEl.innerHTML = '<option value="">鏆傛棤鍘嗗彶鎵规</option>';
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
            container.innerHTML = '<tr><td colspan="8" style="text-align:center">鏆傛棤鏁版嵁锛岃鍏堥€夋嫨鎵规</td></tr>';
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
                <td>${store.allowed_late <= 10 ? '<span class="priority-high">楂?/span>' : store.allowed_late <= 20 ? '<span class="priority-medium">涓?/span>' : '<span class="priority-low">浣?/span>'}</td>
            </tr>
        `).join("");
    }

    function renderWaveSummary() {
        const container = document.getElementById("wave-summary");
        if (!container) return;
        const waveResults = currentProcessedData?.wave_results;
        if (!waveResults) {
            container.innerHTML = "鏆傛棤鏁版嵁锛岃鍏堥€夋嫨鎵规";
            return;
        }

        const waves = ["W1", "W2", "W3", "W4"];
        container.innerHTML = `<div class="wave-summary-grid">${
            waves.map((waveId) => {
                const data = waveResults[waveId];
                if (!data) {
                    return `<div class="wave-card"><div class="wave-title">${waveId}</div><div class="wave-stats">鏆傛棤鏁版嵁</div></div>`;
                }
                return `
                    <div class="wave-card">
                        <div class="wave-title">${waveId}</div>
                        <div class="wave-stats">
                            璋冨害闂ㄥ簵: ${data.scheduled_count}<br>
                            鏈皟搴﹂棬搴? ${data.unscheduled_count}<br>
                            鎬婚噷绋? ${formatNumber(data.total_distance_km)} km<br>
                            鐢ㄨ溅鏁? ${data.vehicles.length}
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
            container.innerHTML = "鏆傛棤鏁版嵁";
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
                        <span class="rank-time">闂ㄥ簵 ${item.scheduled_count}</span>
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
            container.innerHTML = "鏆傛棤鏁版嵁";
            return;
        }
        container.innerHTML = `
            <div class="global-summary">
                <div style="font-weight:600; margin-bottom:8px;">褰撳墠鍩哄噯鏂规</div>
                <div class="global-metrics">
                    <div class="metric">
                        <div class="metric-value">${currentProcessedData.total_vehicles}</div>
                        <div class="metric-label">鎬荤敤杞?/div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${formatNumber(currentProcessedData.total_distance_km)}</div>
                        <div class="metric-label">鎬婚噷绋?km)</div>
                    </div>
                    <div class="metric">
                        <div class="metric-value">${currentProcessedData.total_stores}</div>
                        <div class="metric-label">闂ㄥ簵鏁?/div>
                    </div>
                </div>
            </div>
            <div class="route-preview">
                <div class="route-item">
                    <div class="route-vehicle">褰撳墠鎵规</div>
                    <div class="route-stores">${escapeHtml(currentProcessedData.batch_id)}</div>
                </div>
                <div class="route-item">
                    <div class="route-vehicle">绠楁硶</div>
                    <div class="route-stores">${escapeHtml(currentProcessedData.strategy || "--")}</div>
                </div>
                <div class="route-item">
                    <div class="route-vehicle">鍒涘缓鏃堕棿</div>
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
                    renderSimulationProgress("error", [{ text: "杈撳叆鏍￠獙澶辫触锛氱己灏戝簵閾虹紪鍙?, state: "error" }], { storeId, newTime, target });
                    alert("璇疯緭鍏ュ簵閾虹紪鍙?);
                    return;
                }
                if (!newTime) {
                    renderSimulationProgress("error", [{ text: "杈撳叆鏍￠獙澶辫触锛氱己灏戞柊鏀惰揣鏃堕棿", state: "error" }], { storeId, newTime, target });
                    alert("璇烽€夋嫨鏂版椂闂?);
                    return;
                }
            } else {
                if (!Number.isFinite(targetVehicles) || targetVehicles <= 0) {
                    renderSimulationProgress("error", [{ text: "杈撳叆鏍￠獙澶辫触锛氱洰鏍囪溅杈嗘暟鏃犳晥", state: "error" }], { targetVehicles });
                    alert("璇疯緭鍏ユ湁鏁堢殑鐩爣杞﹁締鏁?);
                    return;
                }
                if (!selectedWaves.length) {
                    renderSimulationProgress("error", [{ text: "杈撳叆鏍￠獙澶辫触锛氭湭閫夋嫨鎺ㄦ紨娉㈡", state: "error" }], { selectedWaves });
                    alert("璇疯嚦灏戦€夋嫨涓€涓帹婕旀尝娆?);
                    return;
                }
            }
            if (!realtimeContext || !Array.isArray(realtimeContext.vehicles) || !realtimeContext.vehicles.length) {
                renderSimulationProgress("error", [{ text: "鏈娴嬪埌褰撳墠杞﹁締鍒楄〃", state: "error" }], { hasRealtimeContext: Boolean(realtimeContext) });
                alert("璇峰厛鍥炲埌璋冨害椤靛鍏ュ綋鍓嶈溅杈嗗垪琛ㄥ苟淇濆瓨鏈€鏂拌揣閲忥紝鍐嶈繘鍏ユ帹婕旈〉銆?);
                return;
            }

            const taskId = buildSimulationTaskId();
            openSimulationRelayConsole();
            startSimulationLogPolling(taskId);

            renderSimulationProgress("running", [
                { text: "1. 鏍￠獙杈撳叆鍙傛暟", state: "success" },
                { text: `2. 璇诲彇褰撳墠璋冨害涓婁笅鏂囷細杞﹁締 ${realtimeContext.vehicles.length} 杈哷, state: "success" },
                { text: "3. 鎻愪氦鍚庡彴鎺ㄦ紨璇锋眰", state: "active" },
                { text: "4. 绛夊緟鍚庡彴姹傝В杩斿洖", state: "" },
                { text: "5. 娓叉煋缁撴灉", state: "" }
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
            simulateBtn.textContent = "鎺ㄦ紨涓?..";
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
                        { text: "1. 鏍￠獙杈撳叆鍙傛暟", state: "success" },
                        { text: `2. 璇诲彇褰撳墠璋冨害涓婁笅鏂囷細杞﹁締 ${realtimeContext.vehicles.length} 杈哷, state: "success" },
                        { text: "3. 鎻愪氦鍚庡彴鎺ㄦ紨璇锋眰", state: "success" },
                        { text: "4. 鍚庡彴姹傝В瀹屾垚", state: "success" },
                        { text: `5. 娓叉煋缁撴灉锛堣€楁椂 ${((Date.now() - startedAt) / 1000).toFixed(1)} 绉掞級`, state: "success" }
                    ], displayResult);
                    renderSimulationReport();
                    if (!isFleetMode) {
                        showSimulateModal(displayResult, storeId, newTime);
                    }
                } else {
                    latestSimulationResult = null;
                    renderSimulationProgress("error", [
                        { text: "1. 鏍￠獙杈撳叆鍙傛暟", state: "success" },
                        { text: `2. 璇诲彇褰撳墠璋冨害涓婁笅鏂囷細杞﹁締 ${realtimeContext.vehicles.length} 杈哷, state: "success" },
                        { text: "3. 鎻愪氦鍚庡彴鎺ㄦ紨璇锋眰", state: "success" },
                        { text: "4. 鍚庡彴杩斿洖澶辫触", state: "error" },
                        { text: `5. 鍋滄娓叉煋锛堣€楁椂 ${((Date.now() - startedAt) / 1000).toFixed(1)} 绉掞級`, state: "error" }
                    ], result);
                    alert(`鎺ㄦ紨澶辫触锛?{result?.message || "鏈煡閿欒"}`);
                }
            } catch (error) {
                await pollSimulationTaskLog(taskId, true);
                renderSimulationProgress("error", [
                    { text: "1. 鏍￠獙杈撳叆鍙傛暟", state: "success" },
                    { text: `2. 璇诲彇褰撳墠璋冨害涓婁笅鏂囷細杞﹁締 ${realtimeContext?.vehicles?.length || 0} 杈哷, state: "success" },
                    { text: "3. 璋冪敤鎺ㄦ紨鎺ュ彛", state: "error" }
                ], { error: error?.message || String(error) });
                alert(`鎺ㄦ紨澶辫触锛?{error?.message || ""}`);
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

    // 娣锋矊椤靛垵濮嬪寲鍏ュ彛銆?    // 椤甸潰鎵撳紑鍚庝細鍦ㄨ繖閲屾媺鍙栨壒娆°€佸簵閾恒€佹尝娆℃椂闂淬€佹姤鍛婂尯榛樿鐘舵€佺瓑鍓嶇疆鏁版嵁銆?    async function initBusinessFunctions() {
        console.log("鎺ㄦ紨椤典笟鍔″姛鑳藉垵濮嬪寲...");
        const container = document.querySelector("#tuiyanPage");
        if (container) container.style.opacity = "0.5";
        try {
            await Promise.all([loadShops(), loadShopWaveTimes()]);
            await loadLatestBatch();
            await renderBatchSelector();
            setupReportTabs();
            setupSimulationModeControls();
            renderSimulationProgress("idle", [{ text: "绛夊緟寮€濮嬫帹婕?..", state: "" }], null);
            renderAllOutputs();
            setupSimulationControls();
            window.__tuiyanDebug = { currentBatch, currentProcessedData, allShops, shopWaveTimes };
            console.log("鎺ㄦ紨椤靛垵濮嬪寲瀹屾垚", { currentProcessedData });
        } catch (error) {
            console.error("涓氬姟鍒濆鍖栧け璐?", error);
            const benchmarkInfo = document.getElementById("benchmark-info");
            if (benchmarkInfo) benchmarkInfo.textContent = "鍔犺浇澶辫触锛岃妫€鏌ュ悗绔湇鍔?;
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

