/*
 * EN: Core module header and section note.
 * CN: 核心模块头说明与分段提示。
 */
const STORAGE_KEY = "dispatch_saved_plans_v6";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const RUN_ARCHIVE_KEY = "dispatch_run_archive_v1";
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const DEEPSEEK_SETTINGS_KEY = "dispatch_deepseek_settings_v1";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const AMAP_WEB_SERVICE_KEY = "3ba73c0e0906dcbe77eeb85f3a5c343d";
const AMAP_JS_WEB_KEY = "28741158a28eba2a5182757cf0d6c059";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const AMAP_DISTANCE_CACHE_KEY = "dispatch_amap_distance_cache_v1";
const AMAP_ROUTE_CACHE_KEY = "dispatch_amap_route_cache_v1";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const AMAP_ORIGIN_BATCH_SIZE = 20;
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const RECOMMENDED_PLAN_TASK_DATE_KEY = "dispatch_recommended_task_date_v1";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const RUN_REGION_SCHEME_SELECTED_KEY = "dispatch_run_region_scheme_selected_v1";
const ENFORCED_VEHICLE_TYPE = "4.2米厢式货车";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const LOAD_CONVERT_CAPACITY_MAP = {
  rpcs: 207,
  rcase: 380,
  bpcs: 120,
  bpaper: 380,
  apcs: 350,
  apaper: 380,
  rpaper: 380,
};
const LOAD_CONVERT_WAVE1_FIELDS = ["rpcs", "rcase", "bpcs", "bpaper", "rpaper"];
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const LOAD_CONVERT_WAVE2_FIELDS = ["apcs", "apaper"];
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const algorithmMeta = {
  vrptw: { label: "VRPTW贪心插入启发式", description: "纯前端时间维度插入式贪心 VRPTW 启发式。" },
  hybrid: { label: "混合VRPTW启发式", description: "在贪心插入初始解上叠加禁忌搜索与大邻域搜索的纯前端混合启发式。" },
  ga: { label: "遗传算法（GA）", description: "基于贪心初始解做轻量扰动。" },
  tabu: { label: "禁忌搜索", description: "围绕初始解做邻域替换。" },
  lns: { label: "大邻域搜索", description: "局部拆解后重新插入门店。" },
  savings: { label: "Clark-Wright节约法", description: "按节约值合并路线，再做 2-opt 与车辆分配。" },
  sa: { label: "模拟退火", description: "用退火接受准则跳出局部最优，再配合局部优化收敛。" },
  aco: { label: "蚁群算法（ACO）", description: "用信息素和启发式构造门店序列，再做局部改进。" },
  pso: { label: "粒子群算法", description: "用优先级实数编码、速度更新和群体最优引导搜索。" },
  vehicle: { label: "车辆驱动构造", description: "按车辆逐台装填门店，先严格可行再做一次松弛重试。" },
  relay: { label: "接力求解", description: "先生成初排，再按阶段串联全局搜索与局部优化，逐段接棒打磨最终方案。" },
};
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getAlgorithmQuickGuide(key) {
  const guides = {
    aco: "🐜 蚁群算法(ACO)：模拟蚂蚁觅食，信息素引导搜索。适合100-300家门店，能找到优质路径但收敛稍慢。",
    pso: "🐦 粒子群算法(PSO)：模拟鸟群捕食，群体智能引导。适合50-150家门店，收敛快但可能早熟。",
    vehicle: "🚚 车辆驱动构造：按车辆逐台塞店，优先提升覆盖率，适合调试和保底排满。",
    sa: "🔥 模拟退火(SA)：模拟金属退火，概率性接受差解。适合任意规模，能跳出局部最优。",
    tabu: "📋 禁忌搜索(Tabu)：带记忆的邻域搜索。适合小规模精确优化，避免走回头路。",
    lns: "🔨 大邻域搜索(LNS)：破坏-修复框架。适合200-500家门店，能大规模重构解。",
    hybrid: "🎯 混合算法：SA+LNS+Tabu三阶段接力。追求极致质量时使用，耗时最长。",
    ga: "🧬 遗传算法(GA)：模拟自然选择，进化搜索。全局能力强，但收敛慢。",
    savings: "💰 Clark-Wright节约法：经典构造法，快速生成初始解。速度快，质量稳定。",
    vrptw: "⏰ VRPTW贪心插入：时间窗优先，逐个插入。可行性好，适合快速初排。"
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return guides[key] || `${key}算法，详情请查看系统文档。`;
}
const I18N = {
  zh: {
    algorithmLabels: { vrptw: "VRPTW贪心插入启发式", hybrid: "混合VRPTW启发式", ga: "遗传算法（GA）", tabu: "禁忌搜索", lns: "大邻域搜索", savings: "Clark-Wright节约法", sa: "模拟退火", aco: "蚁群算法（ACO）", pso: "粒子群算法", vehicle: "车辆驱动构造", relay: "接力求解" },
    algorithmDescriptions: {
      vrptw: "纯前端时间维度插入式贪心 VRPTW 启发式。",
      hybrid: "在贪心插入初始解上叠加禁忌搜索与大邻域搜索的纯前端混合启发式。",
      ga: "基于贪心初始解做轻量扰动。",
      tabu: "围绕初始解做邻域替换。",
      lns: "局部拆解后重新插入门店。",
      savings: "按节约值合并路线，再做 2-opt 与车辆分配。",
      sa: "用退火接受准则跳出局部最优，再配合局部优化收敛。",
      aco: "用信息素和启发式构造门店序列，再做局部改进。",
      pso: "用优先级实数编码、速度更新和群体最优引导搜索。",
      vehicle: "按车辆逐台装填门店，先严格可行，再做一次软时间窗重试。",
      relay: "先出初排，再按阶段串联全局搜索与局部优化，生成单条最终接力解。",
    },
  },
  ja: {
    algorithmLabels: { vrptw: "VRPTW貪欲挿入法", hybrid: "ハイブリッドVRPTW", ga: "遺伝的アルゴリズム", tabu: "タブー探索", lns: "大近傍探索", savings: "Clark-Wright節約法", sa: "焼きなまし法", aco: "蟻コロニー最適化", pso: "粒子群最適化", vehicle: "車両駆動構築", relay: "リレー最適化" },
    algorithmDescriptions: {
      vrptw: "フロントエンド完結の時間次元挿入型・貪欲 VRPTW ヒューリスティック。",
      hybrid: "貪欲挿入の初期解にタブー探索と大近傍探索を重ねる純フロントエンド混合ヒューリスティック。",
      ga: "貪欲初期解をベースに軽量な摂動を行います。",
      tabu: "初期解の近傍を探索し、反復を避けます。",
      lns: "一部店舗を外して再挿入し、局所改善します。",
      savings: "節約値順にルートを統合し、その後 2-opt と車両割当を行います。",
      sa: "焼きなましの受理規則で局所最適から抜け出し、局所改善で収束させます。",
      aco: "フェロモンとヒューリスティックで訪問順を構築し、その後に局所改善します。",
      pso: "優先度の実数符号化と速度更新で、個体最良・全体最良に向けて探索します。",
      vehicle: "車両ごとに店舗を詰める構築法で、まず厳密可行、次に緩和再試行します。",
      relay: "初期案を作成した後、段階ごとに全域探索と局所改善をつなぎ、最終案へ磨き上げます。",
    },
  },
};
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function lang() { return state.language || "zh"; }
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function algoLabel(key) { return I18N[lang()].algorithmLabels[key] || algorithmMeta[key]?.label || key; }
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function algoDescription(key) { return I18N[lang()].algorithmDescriptions[key] || algorithmMeta[key]?.description || ""; }
const DEFAULT_STRATEGY_CONFIG = {
  deliveryMode: "singleDailyWave",
  optimizeGoal: "balanced",
  loadDistanceBias: 0,
  latenessTolerance: "medium",
  vehicleCostBias: 50,
  dualWaveWeight: 50,
  crossRegionPenaltyWeight: 50,
  waveDelayPenalty: 50,
  lateRouteStrength: "medium",
  deliveryDifficultyMode: "time",
  difficultyTier1Unlimited: false,
  difficultyTier1Limit: 1,
  difficultyTier2Unlimited: false,
  difficultyTier2Limit: 2,
  difficultyTier3Unlimited: true,
  difficultyTier3Limit: 0,
  difficultyScoreLimit: 8,
  maxSolveCapacity: 1.0,
  defaultSpeedKmh: 38,
  w3SpeedKmh: 48,
  w1w2RelayMaxKm: 240,
  w3OneWayMaxKm: 260,
  w3ExcludePriorVehicles: true,
};
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function cloneDefaultStrategyConfig() {
  return { ...DEFAULT_STRATEGY_CONFIG };
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function normalizeStrategyConfig(input = {}) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const scoreLimit = Number(input.difficultyScoreLimit ?? DEFAULT_STRATEGY_CONFIG.difficultyScoreLimit);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tier1Limit = Number(input.difficultyTier1Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier1Limit);
  const tier2Limit = Number(input.difficultyTier2Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier2Limit);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const tier3Limit = Number(input.difficultyTier3Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier3Limit);
  const toBool = (value, defaultValue) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (value === undefined || value === null) return Boolean(defaultValue);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (typeof value === "string") {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const v = value.trim().toLowerCase();
      if (["true", "1", "yes", "on"].includes(v)) return true;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (["false", "0", "no", "off"].includes(v)) return false;
    }
    return Boolean(value);
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxSolveCapacityRaw = Number(input.maxSolveCapacity ?? DEFAULT_STRATEGY_CONFIG.maxSolveCapacity);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const defaultSpeedRaw = Number(input.defaultSpeedKmh ?? DEFAULT_STRATEGY_CONFIG.defaultSpeedKmh);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const w3SpeedRaw = Number(input.w3SpeedKmh ?? DEFAULT_STRATEGY_CONFIG.w3SpeedKmh);
  const relayMaxRaw = Number(input.w1w2RelayMaxKm ?? DEFAULT_STRATEGY_CONFIG.w1w2RelayMaxKm);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const w3OneWayRaw = Number(input.w3OneWayMaxKm ?? DEFAULT_STRATEGY_CONFIG.w3OneWayMaxKm);
  return {
    deliveryMode: ["singleDailyWave", "doubleDailyWave"].includes(String(input.deliveryMode || "")) ? String(input.deliveryMode) : DEFAULT_STRATEGY_CONFIG.deliveryMode,
    optimizeGoal: ["load", "balanced", "distance"].includes(String(input.optimizeGoal || "")) ? String(input.optimizeGoal) : DEFAULT_STRATEGY_CONFIG.optimizeGoal,
    loadDistanceBias: Math.max(-100, Math.min(100, Number(input.loadDistanceBias ?? DEFAULT_STRATEGY_CONFIG.loadDistanceBias))),
    latenessTolerance: ["strict", "medium", "loose"].includes(String(input.latenessTolerance || "")) ? String(input.latenessTolerance) : DEFAULT_STRATEGY_CONFIG.latenessTolerance,
    vehicleCostBias: Math.max(0, Math.min(100, Number(input.vehicleCostBias ?? DEFAULT_STRATEGY_CONFIG.vehicleCostBias))),
    dualWaveWeight: Math.max(0, Math.min(100, Number(input.dualWaveWeight ?? DEFAULT_STRATEGY_CONFIG.dualWaveWeight))),
    crossRegionPenaltyWeight: Math.max(0, Math.min(100, Number(input.crossRegionPenaltyWeight ?? DEFAULT_STRATEGY_CONFIG.crossRegionPenaltyWeight))),
    waveDelayPenalty: Math.max(0, Math.min(100, Number(input.waveDelayPenalty ?? DEFAULT_STRATEGY_CONFIG.waveDelayPenalty))),
    lateRouteStrength: ["low", "medium", "high"].includes(String(input.lateRouteStrength || "")) ? String(input.lateRouteStrength) : DEFAULT_STRATEGY_CONFIG.lateRouteStrength,
    deliveryDifficultyMode: ["time", "count", "score"].includes(String(input.deliveryDifficultyMode || "")) ? String(input.deliveryDifficultyMode) : DEFAULT_STRATEGY_CONFIG.deliveryDifficultyMode,
    difficultyTier1Unlimited: toBool(input.difficultyTier1Unlimited, DEFAULT_STRATEGY_CONFIG.difficultyTier1Unlimited),
    difficultyTier1Limit: Math.max(0, Math.min(999, Number.isFinite(tier1Limit) ? Math.round(tier1Limit) : DEFAULT_STRATEGY_CONFIG.difficultyTier1Limit)),
    difficultyTier2Unlimited: toBool(input.difficultyTier2Unlimited, DEFAULT_STRATEGY_CONFIG.difficultyTier2Unlimited),
    difficultyTier2Limit: Math.max(0, Math.min(999, Number.isFinite(tier2Limit) ? Math.round(tier2Limit) : DEFAULT_STRATEGY_CONFIG.difficultyTier2Limit)),
    difficultyTier3Unlimited: toBool(input.difficultyTier3Unlimited, DEFAULT_STRATEGY_CONFIG.difficultyTier3Unlimited),
    difficultyTier3Limit: Math.max(0, Math.min(999, Number.isFinite(tier3Limit) ? Math.round(tier3Limit) : DEFAULT_STRATEGY_CONFIG.difficultyTier3Limit)),
    difficultyScoreLimit: Math.max(1, Math.min(999, Number.isFinite(scoreLimit) ? Math.round(scoreLimit) : DEFAULT_STRATEGY_CONFIG.difficultyScoreLimit)),
    maxSolveCapacity: Math.max(0.1, Math.min(9.99, Number.isFinite(maxSolveCapacityRaw) ? maxSolveCapacityRaw : DEFAULT_STRATEGY_CONFIG.maxSolveCapacity)),
    defaultSpeedKmh: Math.max(1, Math.min(120, Number.isFinite(defaultSpeedRaw) ? defaultSpeedRaw : DEFAULT_STRATEGY_CONFIG.defaultSpeedKmh)),
    w3SpeedKmh: Math.max(1, Math.min(120, Number.isFinite(w3SpeedRaw) ? w3SpeedRaw : DEFAULT_STRATEGY_CONFIG.w3SpeedKmh)),
    w1w2RelayMaxKm: Math.max(1, Math.min(2000, Number.isFinite(relayMaxRaw) ? relayMaxRaw : DEFAULT_STRATEGY_CONFIG.w1w2RelayMaxKm)),
    w3OneWayMaxKm: Math.max(1, Math.min(2000, Number.isFinite(w3OneWayRaw) ? w3OneWayRaw : DEFAULT_STRATEGY_CONFIG.w3OneWayMaxKm)),
    w3ExcludePriorVehicles: toBool(input.w3ExcludePriorVehicles, DEFAULT_STRATEGY_CONFIG.w3ExcludePriorVehicles),
  };
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function applyOptimizeGoalPreset(goal, strategyConfig = state.strategyConfig, force = false) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const next = { ...(strategyConfig || cloneDefaultStrategyConfig()) };
  next.optimizeGoal = ["load", "balanced", "distance"].includes(goal) ? goal : "balanced";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (force || !state.strategyConfigTouched?.loadDistanceBias) {
    if (next.optimizeGoal === "load") {
      next.loadDistanceBias = -60;
    } else if (next.optimizeGoal === "distance") {
      next.loadDistanceBias = 60;
    } else {
      next.loadDistanceBias = 0;
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return normalizeStrategyConfig(next);
}
function updateStrategyConfigUI() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const cfg = state.strategyConfig || cloneDefaultStrategyConfig();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const deliveryMode = document.getElementById("strategyDeliveryModeSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (deliveryMode) deliveryMode.value = cfg.deliveryMode || "singleDailyWave";
  const goalSelect = document.getElementById("strategyOptimizeGoalSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (goalSelect) goalSelect.value = cfg.optimizeGoal || "balanced";
  const loadDistanceBias = document.getElementById("strategyLoadDistanceBiasInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (loadDistanceBias) loadDistanceBias.value = String(cfg.loadDistanceBias ?? 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const latenessTolerance = document.getElementById("strategyLatenessToleranceSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (latenessTolerance) latenessTolerance.value = cfg.latenessTolerance || "medium";
  const vehicleCostBias = document.getElementById("strategyVehicleCostBiasInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (vehicleCostBias) vehicleCostBias.value = String(cfg.vehicleCostBias ?? 50);
  const dualWave = document.getElementById("strategyDualWaveWeightInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (dualWave) dualWave.value = String(cfg.dualWaveWeight ?? 50);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const crossRegion = document.getElementById("strategyCrossRegionPenaltyWeightInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (crossRegion) crossRegion.value = String(cfg.crossRegionPenaltyWeight ?? 50);
  const waveDelay = document.getElementById("strategyWaveDelayPenaltyInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (waveDelay) waveDelay.value = String(cfg.waveDelayPenalty ?? 50);
  const lateRoute = document.getElementById("strategyLateRouteStrengthSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lateRoute) lateRoute.value = cfg.lateRouteStrength || "medium";
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const diffMode = document.getElementById("strategyDifficultyModeSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (diffMode) diffMode.value = cfg.deliveryDifficultyMode || "time";
  const tier1Unlimited = document.getElementById("strategyDifficultyTier1UnlimitedInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (tier1Unlimited) tier1Unlimited.checked = Boolean(cfg.difficultyTier1Unlimited);
  const tier1Limit = document.getElementById("strategyDifficultyTier1LimitInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tier1Limit) tier1Limit.value = String(cfg.difficultyTier1Limit ?? 1);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const tier2Unlimited = document.getElementById("strategyDifficultyTier2UnlimitedInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tier2Unlimited) tier2Unlimited.checked = Boolean(cfg.difficultyTier2Unlimited);
  const tier2Limit = document.getElementById("strategyDifficultyTier2LimitInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (tier2Limit) tier2Limit.value = String(cfg.difficultyTier2Limit ?? 2);
  const tier3Unlimited = document.getElementById("strategyDifficultyTier3UnlimitedInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tier3Unlimited) tier3Unlimited.checked = Boolean(cfg.difficultyTier3Unlimited);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const tier3Limit = document.getElementById("strategyDifficultyTier3LimitInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tier3Limit) tier3Limit.value = String(cfg.difficultyTier3Limit ?? 0);
  const diffScoreLimit = document.getElementById("strategyDifficultyScoreLimitInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (diffScoreLimit) diffScoreLimit.value = String(cfg.difficultyScoreLimit ?? 8);
  updateDifficultyTierLimitInputState(cfg);
  updateDifficultyModeFieldVisibility(cfg.deliveryDifficultyMode || "time");
}

function updateDifficultyModeFieldVisibility(mode) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const countGroup = document.getElementById("strategyDifficultyCountGroup");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const scoreGroup = document.getElementById("strategyDifficultyScoreGroup");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (countGroup) countGroup.style.display = mode === "count" ? "" : "none";
  if (scoreGroup) scoreGroup.style.display = mode === "score" ? "" : "none";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function updateDifficultyTierLimitInputState(strategyCfg = state.strategyConfig || cloneDefaultStrategyConfig()) {
  const cfg = strategyCfg || cloneDefaultStrategyConfig();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tier1Limit = document.getElementById("strategyDifficultyTier1LimitInput");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const tier2Limit = document.getElementById("strategyDifficultyTier2LimitInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tier3Limit = document.getElementById("strategyDifficultyTier3LimitInput");
  if (tier1Limit) tier1Limit.disabled = Boolean(cfg.difficultyTier1Unlimited);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (tier2Limit) tier2Limit.disabled = Boolean(cfg.difficultyTier2Unlimited);
  if (tier3Limit) tier3Limit.disabled = Boolean(cfg.difficultyTier3Unlimited);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildBackendStrategyConfig(strategyConfigInput = state.strategyConfig) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const cfg = normalizeStrategyConfig(strategyConfigInput || {});
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const distanceWeight = Math.max(0, Math.min(100, Math.round((cfg.loadDistanceBias + 100) / 2)));
  const loadWeight = 100 - distanceWeight;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const latenessStrengthMap = { strict: "high", medium: "medium", loose: "low" };
  return {
    deliveryMode: cfg.deliveryMode,
    optimizeGoal: cfg.optimizeGoal,
    allowLate: cfg.latenessTolerance !== "strict",
    latenessStrength: latenessStrengthMap[cfg.latenessTolerance] || "medium",
    allowMultiTrip: cfg.vehicleCostBias < 65,
    vehicleBusyCostWeight: cfg.vehicleCostBias,
    dualWaveWeight: cfg.dualWaveWeight,
    crossRegionPenaltyWeight: cfg.crossRegionPenaltyWeight,
    loadWeight,
    distanceWeight,
    waveDelayPenalty: cfg.waveDelayPenalty,
    lateRouteStrength: cfg.lateRouteStrength,
    deliveryDifficultyMode: cfg.deliveryDifficultyMode,
    difficultyTier1Unlimited: cfg.difficultyTier1Unlimited,
    difficultyTier1Limit: cfg.difficultyTier1Limit,
    difficultyTier2Unlimited: cfg.difficultyTier2Unlimited,
    difficultyTier2Limit: cfg.difficultyTier2Limit,
    difficultyTier3Unlimited: cfg.difficultyTier3Unlimited,
    difficultyTier3Limit: cfg.difficultyTier3Limit,
    difficultyScoreLimit: cfg.difficultyScoreLimit,
    maxSolveCapacity: cfg.maxSolveCapacity,
    defaultSpeedKmh: cfg.defaultSpeedKmh,
    w3SpeedKmh: cfg.w3SpeedKmh,
    w1w2RelayMaxKm: cfg.w1w2RelayMaxKm,
    w3OneWayMaxKm: cfg.w3OneWayMaxKm,
    w3ExcludePriorVehicles: cfg.w3ExcludePriorVehicles,
  };
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const UI_TEXT = {
  zh: {
    showcase: "系统介绍", reload: "重新加载固定门店", save: "保存当前方案", exportResult: "导出当前结果",
    storeInfo: "门店信息", vehicleInfo: "车辆信息", waveConfig: "波次配置", algoRun: "算法与执行", result: "调度结果", saved: "已保存方案",
    storeDesc: "门店数据已内置到系统中，当前可直接编辑基础属性。", vehicleDesc: "车辆可通过 TXT 导入，也可以手工补充。", waveDesc: "声明可配送波次、各波次开始结束时间，以及该波次包含的门店编号。", algoDesc: "可单独生成方案，也可同时勾选两种算法做对比。最低装载率会作为排序偏好，而非硬性卡死条件。", resultDesc: "系统会展示门店路线、距离、调度过程，以及人工调车入口。", savedDesc: "方案保存在当前浏览器本地，可作为历史排线记录。",
    foldStore: "折叠店铺明细", unfoldStore: "展开店铺明细", foldVehicle: "折叠车辆明细", unfoldVehicle: "展开车辆明细",
    addStore: "新增门店", addVehicle: "新增车辆", addWave: "新增波次",
    dispatchStart: "首次发车时间", maxKm: "单车往返总里程上限（公里）", minLoad: "单车最低装载率（%）", ignoreWaves: "忽略波次", autoWave: "按固定门店自动分波次", concentrateLate: "迟到线路尽量集中",
    targetScore: "目标评分下限",
    singleWaveDistance: "单波次距离阈值（公里）", singleWaveStart: "单波次开始", singleWaveEnd: "单波次结束", singleWaveMode: "单波次截止规则",
    returnEnd: "回库截止", serviceEnd: "完店截止", generate: "生成调度结果", close: "关闭",
    processTitle: "调度过程可视化", showcaseTitle: "系统能力总览",
    storeTableHeaders: ["编号", "名称", "区域", "经度", "纬度", "次数", "一波次货量", "二波次货量", "冷藏比例", "一配时间", "二配时间", "所属波次", "卸货分钟", "难度", "允许偏差(分)", "状态", "车号", ""],
    vehicleTableHeaders: ["车号", "司机名", "车型", "容量", "速度", "可送冷藏", "状态", ""],
    waveTableHeaders: ["波次", "开始", "结束", "截止规则", "包含门店", ""],
    used: "已派车", idle: "未使用", del: "删除", scheduled: "已调度", unscheduled: "未调度", assignedVehicle: "对应车号",
    chooseFile: "选择文件",
    noFileChosen: "未选择文件",
    allDay: "全天统一调度", route: "路线", totalDistance: "总里程", avgLoad: "平均单趟装载率", fleetLoad: "车队利用率", fleetLoadHint: "按已用车辆容量折算，多趟时可超过100%", score: "综合得分", onTime: "准时率",
    detail: "明细", viewViz: "查看可视化", adjustTo: "调整到车辆...", confirmAdjust: "确认调整", noOtherVehicle: "当前波次没有其他车辆可接手",
    inserted: "手工插入", leg: "本段", arrive: "到达", unloadStart: "开始卸货", leave: "离开", desired: "期望到达", unloadMinutes: "卸货", minutes: "分钟", notLate: "准时", late: "超时窗",
    storesCount: "门店数", totalLoad: "累计装载", maxTrip: "最高单趟", trips: "趟数", totalRoundKm: "往返总里程", loadPreferredMet: "达到装载偏好", loadPreferredMiss: "未达装载偏好",
    tripNo: "第", tripSuffix: "趟", returnTime: "回库时间", backDistance: "回仓距离", tripRoundKm: "单趟往返总里程", tripLoadRate: "单趟装载率", overWave: "超波次",
    generatedSingle: "系统已生成调度方案。", generatedCompare: "系统已完成双算法对比。", savedEmpty: "还没有保存过调度方案。",
    multiStoreDaily: "多门店单日多次配送调度",
    targetAchieved: "已达到目标评分",
    targetMissed: "未达到目标评分",
    noVehicles: "请先录入或导入车辆信息。",
    noStores: "当前固定门店数据为空。",
    noWaves: "请先配置至少一个波次。",
    regularMissing: "普通波次还有 {count} 家门店未分配：{names}",
    unscheduledStores: "未调度门店",
    scheduledStores: "已调度门店",
    hardArrivalHint: "到店要求时间已作为强约束；超出允许到店偏差的门店会优先保留为未调度。",
      lateFocusHint: "已启用迟到线路集中策略，系统会优先减少出现迟到的线路数。",
      noUnscheduled: "全部门店已调度。",
      unscheduledSummary: "当前尚有 {count} 家门店未调度：{names}",
    unscheduledReasonArrival: "到店时间超出允许偏差窗口",
      unscheduledReasonWave: "波次截止时间不足",
      unscheduledReasonMileage: "里程约束不足",
      unscheduledReasonCapacity: "车辆容量不足",
      unscheduledReasonSlot: "现有车辆时序已满，无法再插入",
      unscheduledReasonMixed: "多重约束叠加，当前无法插入",
      unscheduledReasonTitle: "未调度原因",
      unscheduledDetails: "未调度明细",
      importedVehicles: "已导入 {count} 辆车辆。",
    importVehicleFailed: "车辆 TXT 未解析出有效车号，请检查编码或每行一辆车的格式。",
    autoWaveBuilt: "已按固定门店自动生成 {count} 个波次。",
    chooseDifferentVehicle: "请选择一个不同的目标车辆。",
    storeNotOnVehicle: "未在 {plate} 上找到门店 {store}。",
    transferFailed: "门店 {store} 无法从 {source} 调整到 {target}，请检查时间、里程或容量。",
    transferSuccess: "已将门店 {store} 从 {source} 调整到 {target}，并仅重算这两辆车。",
    rescheduleAgain: "再次调度",
    saveScheduledResult: "保存当前结果",
    rescheduleSection: "未调度门店补调",
    rescheduleHint: "保留已调度线路，优先用空闲车辆继续补调未安排出去的门店。",
    exportLiveUnscheduled: "导出当前未调度ID",
    unscheduledMismatch: "口径不一致：补调面板={panel}，明细筛选={detail}。请停止使用该结果并反馈。",
    manualAssignVehicle: "手工指定车号",
    manualAssignPlaceholder: "指定车号...",
    confirmAssign: "指定调度",
    rescheduleProgress: "本轮新增调度 {count} 家门店。",
    rescheduleNoProgress: "本轮未能新增调度门店。",
    noAssignableVehicle: "当前没有可用于补调的车辆。",
    assignFailed: "门店 {store} 指定给 {plate} 失败，请检查时间、里程或容量。",
    assignSuccess: "门店 {store} 已指定给 {plate}，并完成局部重算。",
    savedAt: "保存时间",
    wavesLabel: "波次",
    waveModeReturn: "车辆需在 {time} 前回库",
    waveModeService: "车辆需在 {time} 前完成最后一家店",
    waveSingleHint: "该组作为单波次店铺单列求解，且单车只跑一趟",
    waveRegularHint: "普通波次按此时间硬约束执行",
    includedStores: "包含门店",
    allStores: "全部门店",
    overtimeTrips: "超波次线路",
    playback: "回放",
    selectedAlgorithms: "当前已勾选算法",
    noneSelected: "未选择",
    staticMap: "静态区域地图",
    routeLegend: "线路图例",
    depot: "库房",
    viewMap: "查看地图",
    routeMap: "看线路图",
    routeMapHint: "按当前线路顺序在地图上描点连线，优先使用高德驾车路径缓存。",
    routeStopSeq: "顺序",
    routePlanArrival: "计划到店",
    routeDesiredArrival: "希望到店",
    routeStopName: "店铺",
    analyticsTitle: "数字驾驶舱",
    analyticsDesc: "用图形方式展示调度效率、算法差异、波次负荷与区域分布。",
    dashboard: "核心指标",
    algoCompare: "算法对比",
    gantt: "波次甘特图",
    loadCurve: "车辆装载曲线",
    spatial: "区域散点分层",
    progressTitle: "生成进度",
    storesToday: "今日门店",
    usedVehiclesShort: "已用车",
    idleVehiclesShort: "闲置车",
    overtimeTripsShort: "超时线路",
    routeDigest: "线路摘要",
    routeDigestHint: "每条线展示门店数、总里程、装载率，方便直接比较线路结构。",
    algorithmScore: "算法得分",
    scoreBreakdownTitle: "评分拆解",
    onTimeScore: "准时得分",
    distanceScoreLabel: "距离得分",
    loadScoreLabel: "装载得分",
    preferenceScoreLabel: "偏好得分",
    progressIdle: "等待生成方案",
    progressPreparing: "正在整理门店、车辆与波次约束…",
    progressRunning: "正在运行 {algo}…",
    progressFinishing: "正在汇总图形化结果…",
    progressDone: "图形驾驶舱已刷新",
    tripLabel: "趟次",
    singleWaveLabel: "单波次",
    regularWaveLabel: "普通波次",
    loadAxis: "装载率",
    timeAxis: "时间",
    scatterNear: "近距门店",
    scatterFar: "远距门店",
    scatterSingle: "单波次店铺",
    noChartData: "当前没有足够数据生成图形。",
    voiceBroadcast: "语音播报",
    voiceAsk: "语音提问",
    mascotTitle: "鲸略使助手",
    mascotDesc: "用更温和、更清晰的方式播报当前调度摘要与风险提示。",
    speechUnsupported: "当前浏览器不支持语音播报。",
    speechListenUnsupported: "当前浏览器不支持语音识别。",
    speechMicPreparing: "正在申请麦克风权限，浏览器弹窗请直接点允许，允许后会立刻开始收音。",
    speechMicDenied: "麦克风权限未开启，请在浏览器地址栏里允许麦克风后再试。",
    speechMicFailed: "麦克风初始化失败，请检查浏览器是否允许当前页面使用麦克风。",
    speechListening: "正在听，请直接提问。",
    speechHeard: "已听到：{text}",
    speechAnswerPrefix: "数字助理回答：",
    deepseekKeyLabel: "DeepSeek Key",
    deepseekModelLabel: "DeepSeek模型",
    deepseekModeLabel: "助手模式",
    deepseekModeDispatch: "调度助手",
    deepseekModeGeneral: "通用助手",
    deepseekStyleLabel: "回答风格",
    deepseekStyleBrief: "简洁回答",
    deepseekStyleDetailed: "详细分析",
    deepseekSave: "保存DeepSeek配置",
    deepseekAsk: "问DeepSeek",
    deepseekAskPlaceholder: "例如：为什么这次 W2 用车偏多？还能怎么优化？",
    deepseekSaved: "DeepSeek 配置已保存在当前浏览器。",
    deepseekMissingKey: "请先填入 DeepSeek API Key。",
    deepseekThinking: "DeepSeek 正在结合当前调度结果思考…",
    deepseekFailed: "DeepSeek 调用失败：{message}",
    deepseekReady: "DeepSeek 已接入当前页面，可直接文字提问或配合语音提问。",
    deepseekLocalFallback: "当前未配置 DeepSeek，仍使用本地按钮助手回答。",
    deepseekAnswerPrefix: "DeepSeek回答：",
    exportNoResult: "当前还没有可导出的调度结果。",
    exportDone: "已导出当前调度结果。",
    exportFilePrefix: "调度结果",
    solveStrategy: "求解策略",
    optimizeGoal: "优化目标",
    strategyManual: "手动勾选",
    strategyQuick: "快速初排",
    strategyDeep: "深度优化",
    strategyGlobal: "全局搜索",
    strategyRelay: "接力求解",
    strategyFree: "自由求解",
    strategyCompare: "多算法对比",
    goalBalanced: "综合平衡",
    goalOnTime: "准点优先",
    goalDistance: "里程优先",
    goalVehicles: "最少用车",
    goalLoad: "装载优先",
    quickSolve: "快速初排",
    deepOptimize: "继续优化",
    globalSearch: "全局搜索",
    relaySolve: "接力求解",
    freeSolve: "自由求解",
    multiCompare: "多算法对比",
    gaBackendChecking: "GA后台：检查中…",
    gaBackendOnline: "GA后台：已连通（Python）",
    gaBackendOffline: "GA后台：未连通（需先恢复后端）",
  },
  ja: {
    showcase: "システム紹介", reload: "固定店舗を再読込", save: "現在の案を保存", exportResult: "現在結果を出力",
    storeInfo: "店舗情報", vehicleInfo: "車両情報", waveConfig: "波次設定", algoRun: "アルゴリズム実行", result: "配送結果", saved: "保存済み案",
    storeDesc: "店舗データはシステムに内蔵されており、基本属性を直接編集できます。", vehicleDesc: "車両は TXT 取込にも手入力にも対応します。", waveDesc: "配送波次、各波次の開始終了時間、対象店舗番号を定義します。", algoDesc: "単独案生成にも、複数アルゴリズム比較にも対応します。最低積載率はハード制約ではなく優先度として扱います。", resultDesc: "店舗ルート、距離、配送過程、手動車両調整を表示します。", savedDesc: "案は現在のブラウザに保存され、履歴として参照できます。",
    foldStore: "店舗明細を折りたたむ", unfoldStore: "店舗明細を展開", foldVehicle: "車両明細を折りたたむ", unfoldVehicle: "車両明細を展開",
    addStore: "店舗追加", addVehicle: "車両追加", addWave: "波次追加",
    dispatchStart: "初回出発時刻", maxKm: "車両往復総距離上限（km）", minLoad: "車両最低積載率（%）", ignoreWaves: "波次を無視", autoWave: "固定店舗で自動波次分割", concentrateLate: "遅着ラインを集中",
    targetScore: "目標スコア下限",
    singleWaveDistance: "単独波次距離しきい値（km）", singleWaveStart: "単独波次開始", singleWaveEnd: "単独波次終了", singleWaveMode: "単独波次締切ルール",
    returnEnd: "帰庫締切", serviceEnd: "最終店完了締切", generate: "配送結果を生成", close: "閉じる",
    processTitle: "配送過程の可視化", showcaseTitle: "システム能力の総覧",
    storeTableHeaders: ["番号", "名称", "エリア", "経度", "緯度", "配送回数", "一波次貨量", "二波次貨量", "冷蔵比率", "一配時刻", "二配時刻", "所属波次", "荷下ろし分", "難易度", "許容偏差(分)", "状態", "車番", ""],
    vehicleTableHeaders: ["車番", "運転手名", "車種", "容量", "速度", "冷蔵可", "状態", ""],
    waveTableHeaders: ["波次", "開始", "終了", "締切ルール", "対象店舗", ""],
    used: "配車済み", idle: "待機", scheduled: "割当済み", unscheduled: "未割当", assignedVehicle: "担当車番",
    chooseFile: "ファイル選択",
    noFileChosen: "未選択",
    allDay: "終日一括配送", route: "ルート", totalDistance: "総距離", avgLoad: "平均単便積載率", fleetLoad: "車隊稼働率", fleetLoadHint: "使用車両容量ベース換算。複数便時は100%超もあります", score: "総合スコア", onTime: "時間内率",
    detail: "明細", viewViz: "可視化を見る", adjustTo: "移動先車両...", confirmAdjust: "調整確定", noOtherVehicle: "この波次では他に引受車両がありません",
    inserted: "手動挿入", leg: "区間", arrive: "到着", unloadStart: "荷下ろし開始", leave: "出発", desired: "希望到着", unloadMinutes: "荷下ろし", minutes: "分", notLate: "時間内", late: "時間窓超過",
    storesCount: "店舗数", totalLoad: "累計積載", maxTrip: "単趟最大", trips: "便数", totalRoundKm: "往復総距離", loadPreferredMet: "積載優先達成", loadPreferredMiss: "積載優先未達",
    tripNo: "第", tripSuffix: "便", returnTime: "帰庫時刻", backDistance: "帰庫距離", tripRoundKm: "単便往復距離", tripLoadRate: "単便積載率", overWave: "波次超過",
    generatedSingle: "配送案を生成しました。", generatedCompare: "複数アルゴリズム比較を生成しました。", savedEmpty: "まだ保存済み案はありません。",
    multiStoreDaily: "多店舗単日複数回配送調度",
    targetAchieved: "目標スコア達成",
    targetMissed: "目標スコア未達",
    noVehicles: "先に車両情報を入力または取込してください。",
    noStores: "現在、固定店舗データがありません。",
    noWaves: "少なくとも 1 つの波次を設定してください。",
    regularMissing: "通常波次で未割当の店舗が {count} 件あります：{names}",
    unscheduledStores: "未割当店舗",
    scheduledStores: "割当済店舗",
    hardArrivalHint: "到着要求時刻は強制約で扱い、許容偏差を超える店舗は未割当に回します。",
      lateFocusHint: "遅着ライン集中戦略を有効化しています。遅着が発生する車線数を優先的に抑えます。",
      noUnscheduled: "全店舗を割り当て済みです。",
      unscheduledSummary: "現在、未割当店舗が {count} 件あります：{names}",
    unscheduledReasonArrival: "到着要求時刻の許容偏差に収まりません",
      unscheduledReasonWave: "波次の締切時間が不足しています",
      unscheduledReasonMileage: "距離制約が不足しています",
      unscheduledReasonCapacity: "車両容量が不足しています",
      unscheduledReasonSlot: "既存車両の時系列に挿入余地がありません",
      unscheduledReasonMixed: "複数制約が重なり、現在は挿入できません",
      unscheduledReasonTitle: "未割当理由",
      unscheduledDetails: "未割当明細",
      importedVehicles: "{count} 台の車両を取込しました。",
    importVehicleFailed: "車両 TXT から有効な車番を解析できませんでした。文字コードまたは 1 行 1 台の形式を確認してください。",
    autoWaveBuilt: "固定店舗に基づき {count} 個の波次を自動生成しました。",
    chooseDifferentVehicle: "異なる目標車両を選択してください。",
    storeNotOnVehicle: "{plate} 上に店舗 {store} が見つかりません。",
    transferFailed: "店舗 {store} を {source} から {target} へ移せません。時間・距離・容量を確認してください。",
    transferSuccess: "店舗 {store} を {source} から {target} に移し、この 2 台のみ再計算しました。",
    rescheduleAgain: "再割当",
    saveScheduledResult: "現在結果を保存",
    rescheduleSection: "未割当店舗の再割当",
    rescheduleHint: "既存の割当ルートを保持しつつ、空き車両を優先して未割当店舗を追加で割り当てます。",
    exportLiveUnscheduled: "現在の未割当IDを出力",
    unscheduledMismatch: "集計不一致：再割当パネル={panel}、明細フィルタ={detail}。この結果の利用を停止して報告してください。",
    manualAssignVehicle: "手動で車番指定",
    manualAssignPlaceholder: "車番を指定...",
    confirmAssign: "指定割当",
    rescheduleProgress: "今回の再割当で {count} 店舗を追加で割り当てました。",
    rescheduleNoProgress: "今回の再割当では追加割当がありませんでした。",
    noAssignableVehicle: "現在、再割当に使える車両がありません。",
    assignFailed: "店舗 {store} を {plate} に指定できませんでした。時間・距離・容量を確認してください。",
    assignSuccess: "店舗 {store} を {plate} に指定し、局所再計算を完了しました。",
    savedAt: "保存時刻",
    wavesLabel: "波次",
    waveModeReturn: "車両は {time} までに帰庫する必要があります",
    waveModeService: "車両は {time} までに最終店舗を完了する必要があります",
    waveSingleHint: "このグループは単独波次として別処理され、1 台 1 便のみです",
    waveRegularHint: "通常波次はこの時間制約をハード適用します",
    includedStores: "対象店舗",
    allStores: "全店舗",
    overtimeTrips: "波次超過ライン",
    playback: "リプレイ",
    selectedAlgorithms: "現在有効なアルゴリズム",
    noneSelected: "未選択",
    staticMap: "静的エリアマップ",
    routeLegend: "ルート凡例",
    depot: "倉庫",
    viewMap: "地図を見る",
    routeMap: "ルート図",
    routeMapHint: "現在の訪問順で地図上にルートを描き、高德の走行ルートキャッシュを優先的に利用します。",
    routeStopSeq: "順番",
    routePlanArrival: "予定到店",
    routeDesiredArrival: "希望到店",
    routeStopName: "店舗",
    analyticsTitle: "デジタルダッシュボード",
    analyticsDesc: "配車効率、アルゴリズム差、波次負荷、エリア分布をグラフで確認できます。",
    dashboard: "主要指標",
    algoCompare: "アルゴリズム比較",
    gantt: "波次タイムライン",
    loadCurve: "車両積載推移",
    spatial: "エリア分布",
    progressTitle: "生成進捗",
    storesToday: "本日店舗",
    usedVehiclesShort: "使用車",
    idleVehiclesShort: "待機車",
    overtimeTripsShort: "超時ライン",
    routeDigest: "ルート要約",
    routeDigestHint: "各ルートの店舗数・総距離・積載率を並べて、その場で構造差を見比べられます。",
    algorithmScore: "アルゴリズム得点",
    scoreBreakdownTitle: "スコア内訳",
    onTimeScore: "定時得点",
    distanceScoreLabel: "距離得点",
    loadScoreLabel: "積載得点",
    preferenceScoreLabel: "嗜好得点",
    progressIdle: "案の生成待機中です",
    progressPreparing: "店舗・車両・波次制約を整理しています…",
    progressRunning: "{algo} を実行しています…",
    progressFinishing: "図形ダッシュボードを集計しています…",
    progressDone: "ダッシュボードを更新しました",
    tripLabel: "便",
    singleWaveLabel: "単独波次",
    regularWaveLabel: "通常波次",
    loadAxis: "積載率",
    timeAxis: "時間",
    scatterNear: "近距離店舗",
    scatterFar: "遠距離店舗",
    scatterSingle: "単独波次店舗",
    noChartData: "現在、図形を生成するための十分なデータがありません。",
    voiceBroadcast: "音声ブリーフィング",
    voiceAsk: "音声質問",
    mascotTitle: "鯨略使アシスタント",
    mascotDesc: "現在の調度状況と注意点を、やわらかく分かりやすく案内します。",
    speechUnsupported: "このブラウザでは音声読み上げに対応していません。",
    speechListenUnsupported: "このブラウザでは音声認識に対応していません。",
    speechMicPreparing: "マイク権限を申請中です。ブラウザのポップアップで許可すると、そのまま音声受付を開始します。",
    speechMicDenied: "マイク権限が無効です。ブラウザのアドレスバーでマイクを許可してから再試行してください。",
    speechMicFailed: "マイクの初期化に失敗しました。ブラウザでこのページのマイク利用が許可されているか確認してください。",
    speechListening: "質問を聞いています。続けて話してください。",
    speechHeard: "聞き取った内容：{text}",
    speechAnswerPrefix: "デジタル助手の回答：",
    deepseekKeyLabel: "DeepSeek Key",
    deepseekModelLabel: "DeepSeekモデル",
    deepseekModeLabel: "助手モード",
    deepseekModeDispatch: "配車助手",
    deepseekModeGeneral: "汎用助手",
    deepseekStyleLabel: "回答スタイル",
    deepseekStyleBrief: "簡潔回答",
    deepseekStyleDetailed: "詳細分析",
    deepseekSave: "DeepSeek設定を保存",
    deepseekAsk: "DeepSeekに質問",
    deepseekAskPlaceholder: "例：今回の W2 で使用車両が多い理由と改善方向は？",
    deepseekSaved: "DeepSeek 設定をこのブラウザに保存しました。",
    deepseekMissingKey: "先に DeepSeek API Key を入力してください。",
    deepseekThinking: "DeepSeek が現在の配車結果を踏まえて考えています…",
    deepseekFailed: "DeepSeek 呼び出しに失敗しました：{message}",
    deepseekReady: "DeepSeek がこの画面に接続されました。テキストでも音声でも質問できます。",
    deepseekLocalFallback: "DeepSeek が未設定のため、現在はローカルのボタン助手で回答します。",
    deepseekAnswerPrefix: "DeepSeek回答：",
    exportNoResult: "現在、出力できる配送結果がありません。",
    exportDone: "現在の配送結果を出力しました。",
    exportFilePrefix: "配送結果",
    solveStrategy: "解法モード",
    optimizeGoal: "目標方針",
    strategyManual: "手動選択",
    strategyQuick: "クイック初期案",
    strategyDeep: "継続改善",
    strategyGlobal: "全域探索",
    strategyRelay: "リレー最適化",
    strategyFree: "自由求解",
    strategyCompare: "複数案比較",
    goalBalanced: "総合バランス",
    goalOnTime: "定時最優先",
    goalDistance: "距離最優先",
    goalVehicles: "車両数最少",
    goalLoad: "積載優先",
    quickSolve: "クイック初期案",
    deepOptimize: "継続改善",
    globalSearch: "グローバル探索",
    relaySolve: "リレー最適化",
    freeSolve: "自由求解",
    multiCompare: "複数案比較",
    gaBackendChecking: "GAバックエンド：確認中…",
    gaBackendOnline: "GAバックエンド：接続済み（Python）",
    gaBackendOffline: "GAバックエンド：未接続（バックエンド復旧が必要）",
  },
};
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function L(key) { return UI_TEXT[lang()]?.[key] ?? key; }
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function LT(key, vars = {}) {
  let text = L(key);
  Object.entries(vars).forEach(([name, value]) => {
    text = text.replaceAll(`{${name}}`, String(value));
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return text;
}
function vehicleTypeLabel(type) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lang() !== "ja") return type || "";
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = {
    "常温车": "常温車",
    "常温+冷藏混合车": "常温・冷蔵混合車",
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return map[type] || type || "";
}

const DC = { id: "DC", name: "配送中心", address: "北京市顺义区天柱路20号", lng: 116.58848, lat: 40.04776 };
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const MASCOT_IMAGE_SRC = "./assets/mascot-whale.png";
const TRUCK_IMAGE_LOW = "assets/truck-30-v2.jpg";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const TRUCK_IMAGE_MID = "assets/truck-60-v2.jpg";
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const TRUCK_IMAGE_HIGH = "assets/truck-full-v2.jpg";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const ALGORITHM_ORDER = ["vrptw", "hybrid", "ga", "tabu", "lns", "savings", "sa", "aco", "pso", "vehicle"];
const MAX_FREE_SOLVE_ALGOS = 5;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const REALTIME_DISPATCH_CONTEXT_KEY = "dispatchRealtimeContext";
const state = {
  vehicles: [],
  stores: [],
  waves: [],
  lastResults: [],
  activeResultKey: "",
  language: "zh",
  settings: {
    dispatchStartTime: "19:10",
    maxRouteKm: 220,
    minLoadRate: 0,
    targetScore: 0,
    ignoreWaves: false,
    concentrateLate: false,
    singleWaveDistanceKm: 70,
    singleWaveStart: "19:10",
    singleWaveEnd: "05:30",
    singleWaveEndMode: "service",
    solveStrategy: "manual",
    optimizeGoal: "balanced",
    customAlgorithms: ["vrptw", "savings"],
  },
  strategyConfig: {
    deliveryMode: "singleDailyWave",
    optimizeGoal: "balanced",
    loadDistanceBias: 0,
    latenessTolerance: "medium",
    vehicleCostBias: 50,
    dualWaveWeight: 50,
    crossRegionPenaltyWeight: 50,
    waveDelayPenalty: 50,
    lateRouteStrength: "medium",
    deliveryDifficultyMode: "time",
    difficultyTier1Unlimited: false,
    difficultyTier1Limit: 1,
    difficultyTier2Unlimited: false,
    difficultyTier2Limit: 2,
    difficultyTier3Unlimited: true,
    difficultyTier3Limit: 0,
    difficultyScoreLimit: 8,
    maxSolveCapacity: 1.0,
    defaultSpeedKmh: 38,
    w3SpeedKmh: 48,
    w1w2RelayMaxKm: 240,
    w3OneWayMaxKm: 260,
    w3ExcludePriorVehicles: true,
  },
  strategyConfigTouched: {
    loadDistanceBias: false,
  },
  ai: {
    deepseekApiKey: "",
    deepseekModel: "deepseek-chat",
    mode: "dispatch",
    answerStyle: "brief",
    questionDraft: "",
    lastAnswer: "",
    lastQuestion: "",
    loading: false,
  },
  ui: {
      generating: false,
      progress: 0,
      progressText: "",
      speaking: false,
      listening: false,
      micPermission: "unknown",
      micPriming: false,
      storeSortField: "id",
      storeSortDir: "asc",
      vehicleSortField: "plateNo",
      vehicleSortDir: "asc",
      waveSortField: "waveId",
      waveSortDir: "asc",
      storeSearchQuery: "",
      vehicleSearchQuery: "",
      waveSearchQuery: "",
      storeLocatedIndex: -1,
      vehicleLocatedIndex: -1,
      waveSelectedIndex: 0,
      waveStoreSearchQuery: "",
      solveWaveSelectedIds: ["ALL"],
      dataArchiveDate: "",
      dataArchiveKeyword: "",
      archivePage: 1,
      archiveDateFilter: "",
      waveSolverWaveId: "",
      waveSolverAlgo: "hybrid",
      resultDetailExpanded: false,
      resultDetailWaveIds: [],
      assistantExpanded: false,
      assistantDockState: "collapsed",
      assistantDockPosition: null,
      mainTab: "basic",
      storeDataSource: "sample",
      vehicleDataSource: "sample",
      storeWaveResolvedSortField: "shop_code",
      storeWaveResolvedSortDir: "asc",
    },
  };
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let processTypingTimers = [];
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let routeLeafletMap = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let routeLeafletLayerGroup = null;
let routeAmapMap = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let routeAmapMarkers = [];
let routeAmapPolyline = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let relayConsoleLines = [];
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let relayStageReporter = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let relayConsolePendingLogLines = [];
let relayConsoleLogFlushTimer = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const GA_BACKEND_URL = "http://127.0.0.1:8765";
const USE_FULL_DISTANCE_MATRIX_FROM_BACKEND = true;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let gaBackendHealth = { available: null, checkedAt: 0 };
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let gaBackendStatusTimer = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let archiveBackendCache = { date: "", page: 1, items: [], totalPages: 1, total: 0, loading: false };
let dataArchiveCache = { date: "", keyword: "", items: [], selectedId: "" };
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let recommendedPlanCache = { taskDate: "", items: [], loading: false };
let recommendedPlanSelectedCache = { taskDate: "", selected: null, loading: false };
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let storeWaveResolvedCache = { items: [], count: 0, total: 0, limit: 200 };
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionSchemeCache = { items: [], loading: false };
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let runRegionCache = { items: [], loading: false };
let runRegionStorePoints = [];
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionMap = null;
let runRegionStoreMarkers = [];
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let runRegionPolygons = new Map();
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionDraftPolygon = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let runRegionEditingId = null;
let runRegionMouseTool = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionPolygonEditor = null;
let runRegionMapRetryTimer = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let runRegionStoreVisibilityMode = "show_all";
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionTargetRegionId = "all";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let runRegionCheckedRegionIds = new Set();
let runRegionSelectedSchemeNo = "";
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let runRegionScheme1AutoGenerated = false;
let loadConvertPreviewCache = null;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let assistantDockDragState = null;
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
let amapCacheSyncPending = false;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let amapCacheSyncTimer = null;
const STRICT_ALGO_TRUTH_MODE = false;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const BACKEND_ONLY_REAL_ALGOS = new Set(["vrptw", "savings", "ga", "hybrid", "tabu", "lns", "sa", "aco", "pso", "vehicle"]);
const IGNORE_CAPACITY_CONSTRAINT = false;
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const MAIN_TAB_KEYS = new Set(["basic", "region", "strategy", "solve", "result"]);

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function loadDeepSeekSettings() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const raw = localStorage.getItem(DEEPSEEK_SETTINGS_KEY);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!raw) return;
    const parsed = JSON.parse(raw);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ai.deepseekApiKey = String(parsed.deepseekApiKey || "");
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    state.ai.deepseekModel = String(parsed.deepseekModel || "deepseek-chat");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ai.mode = String(parsed.mode || "dispatch");
    state.ai.answerStyle = String(parsed.answerStyle || "brief");
  } catch {}
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function saveDeepSeekSettings() {
  localStorage.setItem(DEEPSEEK_SETTINGS_KEY, JSON.stringify({
    deepseekApiKey: state.ai.deepseekApiKey || "",
    deepseekModel: state.ai.deepseekModel || "deepseek-chat",
    mode: state.ai.mode || "dispatch",
    answerStyle: state.ai.answerStyle || "brief",
  }));
}

function getTripTruckVisual(trip) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rate = trip?.loadRate || 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (rate < 0.45) return {
    src: TRUCK_IMAGE_LOW,
    badge: lang() === "ja" ? "赤字" : "亏了",
    cls: "low",
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (rate < 0.8) return {
    src: TRUCK_IMAGE_MID,
    badge: lang() === "ja" ? "達標" : "达标",
    cls: "mid",
  };
  return {
    src: TRUCK_IMAGE_HIGH,
    badge: lang() === "ja" ? "盈利" : "赚了",
    cls: "high",
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const FIXED_STORES = [
  ["391001","月坛北街",0.000000,0.000000,""],
  ["391012","金融街",0.000000,0.000000,""],
  ["391028","金融街中心",0.000000,0.000000,""],
  ["391030","庄胜崇光",0.000000,0.000000,""],
  ["391074","德胜国际中心",0.000000,0.000000,""],
  ["391094","紫金印象",0.000000,0.000000,""],
  ["391409","北京地铁鼓楼大街站店",0.000000,0.000000,""],
  ["391705","北京怀柔城市客厅店",0.000000,0.000000,""],
  ["391709","北京大钟寺广场店",0.000000,0.000000,""],
  ["391712","北京金融科技学院店",0.000000,0.000000,""],
  ["391715","石景山金隅科技大厦店",0.000000,0.000000,""],
  ["391718","北京地铁12号线驼房营站店",0.000000,0.000000,""],
  ["391721","北京丽泽平安金融中心店",0.000000,0.000000,""],
  ["391722","鼎好电子大厦",0.000000,0.000000,""],
  ["392029","富华大厦",0.000000,0.000000,""],
  ["392084","南锣鼓巷地铁站",0.000000,0.000000,""],
  ["392144","前门大街",0.000000,0.000000,""],
  ["392179","东四北大街店",0.000000,0.000000,""],
  ["392315","地铁和平里北街站店",0.000000,0.000000,""],
  ["392394","北京地铁崇文门站店",0.000000,0.000000,""],
  ["392395","北京地铁东四站店",0.000000,0.000000,""],
  ["392408","北京地铁安德里北街站店",0.000000,0.000000,""],
  ["392525","北京环球贸易中心店",0.000000,0.000000,""],
  ["392647","北京泓晟国际中心",0.000000,0.000000,""],
  ["392678","北京地铁雍和宫站店",0.000000,0.000000,""],
  ["392707","北京地铁3号线朝阳站店",0.000000,0.000000,""],
  ["392708","北京地铁3号线东坝站店",0.000000,0.000000,""],
  ["392710","北京地铁3号线团结湖站店",0.000000,0.000000,""],
  ["392712","北京恒毅大厦店",0.000000,0.000000,""],
  ["392713","北京地铁什刹海站店",0.000000,0.000000,""],
  ["392715","北京地铁欢乐谷站店",0.000000,0.000000,""],
  ["392716","北京地铁角门西站店",0.000000,0.000000,""],
  ["392717","北京地铁丰台站店",0.000000,0.000000,""],
  ["392718","腾讯北京总部店",0.000000,0.000000,""],
  ["392719","金隅智造工场店新店",0.000000,0.000000,""],
  ["392720","朝阳高铁站店",0.000000,0.000000,""],
  ["392721","延庆环球新意百货店",0.000000,0.000000,""],
  ["392722","北京长楹天街店",0.000000,0.000000,""],
  ["392723","北京四惠金地名京店",0.000000,0.000000,""],
  ["393003","环球金融中心",0.000000,0.000000,""],
  ["393009","民生大厦",0.000000,0.000000,""],
  ["393010","双井首城国际",0.000000,0.000000,""],
  ["393016","奥林匹克公园",0.000000,0.000000,""],
  ["393032","长富宫",0.000000,0.000000,""],
  ["393033","四方桥东北侧店",0.000000,0.000000,""],
  ["393034","劲松海文大厦",0.000000,0.000000,""],
  ["393035","管庄新天地",0.000000,0.000000,""],
  ["393046","小天竺路",0.000000,0.000000,""],
  ["393067","望京北望金辉大厦",0.000000,0.000000,""],
  ["393082","兆泰国际中心",0.000000,0.000000,""],
  ["393117","十里堡地铁站",0.000000,0.000000,""],
  ["393151","首都机场T3二层到达区外",0.000000,0.000000,""],
  ["393152","首都机场T3三层出发区东指廊",0.000000,0.000000,""],
  ["393153","首都机场T3三层出发区西指廊",0.000000,0.000000,""],
  ["393155","首都机场T3四层出发区外东",0.000000,0.000000,""],
  ["393156","首都机场T3D出发区",0.000000,0.000000,""],
  ["393167","奥林匹克森林公园（南园）店",0.000000,0.000000,""],
  ["393168","奥林匹克森林公园（北园）店",0.000000,0.000000,""],
  ["393188","发展大厦店",0.000000,0.000000,""],
  ["393206","东亚望京中心店",0.000000,0.000000,""],
  ["393220","梵石中心店",0.000000,0.000000,""],
  ["393244","利星行中心店",0.000000,0.000000,""],
  ["393303","利星行中心二店",0.000000,0.000000,""],
  ["393380","光明大厦店",0.000000,0.000000,""],
  ["393399","北京地铁草房站店",0.000000,0.000000,""],
  ["393416","北京地铁六道口店",0.000000,0.000000,""],
  ["393425","北京地铁西土城店",0.000000,0.000000,""],
  ["393426","北京地铁望京西站店",0.000000,0.000000,""],
  ["393546","北京浦项中心店",0.000000,0.000000,""],
  ["393557","北京鸿懋商务大厦店",0.000000,0.000000,""],
  ["393651","北京平安国际金融中心店",0.000000,0.000000,""],
  ["393675","北京阿里巴巴总部店",0.000000,0.000000,""],
  ["393688","北京广播大厦酒店店",0.000000,0.000000,""],
  ["393706","古北水镇三号店",0.000000,0.000000,""],
  ["393707","北京科技财富中心店",0.000000,0.000000,""],
  ["393708","北京怀柔商业街店",0.000000,0.000000,""],
  ["393709","京东总部2号院店",0.000000,0.000000,""],
  ["393710","北京地铁潘家园站店",0.000000,0.000000,""],
  ["393711","微软中国研发集团总部店",0.000000,0.000000,""],
  ["393715","首都国际机场T3四层出发层外西",0.000000,0.000000,""],
  ["393717","北京南站地铁层1店",0.000000,0.000000,""],
  ["393718","北京南站出发层1店",0.000000,0.000000,""],
  ["393720","昌平新新公寓店",0.000000,0.000000,""],
  ["394024","融科资讯中心",0.000000,0.000000,""],
  ["394038","联想新园区",0.000000,0.000000,""],
  ["394048","海淀杏石口路",0.000000,0.000000,""],
  ["394062","火器营地铁站",0.000000,0.000000,""],
  ["394069","互联网创新中心",0.000000,0.000000,""],
  ["394070","木荷路华为园区",0.000000,0.000000,""],
  ["394072","超市发万柳",0.000000,0.000000,""],
  ["394073","超市发花园路",0.000000,0.000000,""],
  ["394075","超市发羊坊店",0.000000,0.000000,""],
  ["394085","超市发甘家口",0.000000,0.000000,""],
  ["394089","超市发上地",0.000000,0.000000,""],
  ["394096","超市发农大",0.000000,0.000000,""],
  ["394103","超市发科南路",0.000000,0.000000,""],
  ["394104","超市发玉泉路",0.000000,0.000000,""],
  ["394105","超市发西长安街",0.000000,0.000000,""],
  ["394110","新浪总部大厦",0.000000,0.000000,""],
  ["394112","超市发海淀大街",0.000000,0.000000,""],
  ["394118","超市发学院路超罗",0.000000,0.000000,""],
  ["394122","联想三标大厦",0.000000,0.000000,""],
  ["394123","联想北研园楼大厦",0.000000,0.000000,""],
  ["394142","超市发双榆树超罗",0.000000,0.000000,""],
  ["394150","网易研发中心",0.000000,0.000000,""],
  ["394196","超市发岭南路店",0.000000,0.000000,""],
  ["394234","超市发罗森北清路店",0.000000,0.000000,""],
  ["394258","超市发罗森北安河店",0.000000,0.000000,""],
  ["394405","北京地铁育新站店",0.000000,0.000000,""],
  ["394406","北京地铁平安里站店",0.000000,0.000000,""],
  ["394419","北京地铁车道沟站店",0.000000,0.000000,""],
  ["394437","超市发罗森清河店",0.000000,0.000000,""],
  ["394451","清华科技园店",0.000000,0.000000,""],
  ["394485","永泰生活服务中心店",0.000000,0.000000,""],
  ["394488","超市发罗森太舟坞店",0.000000,0.000000,""],
  ["394489","超市发罗森六里屯店",0.000000,0.000000,""],
  ["394510","东升科技园店",0.000000,0.000000,""],
  ["394512","超市发罗森北清路二店",0.000000,0.000000,""],
  ["394526","华为北研所K区店",0.000000,0.000000,""],
  ["394560","超市发罗森北安河二店",0.000000,0.000000,""],
  ["394620","北京快手元中心店",0.000000,0.000000,""],
  ["394630","超市发罗森四季青路店",0.000000,0.000000,""],
  ["394705","平谷步行街店",0.000000,0.000000,""],
  ["394720","北京站广场东侧店",0.000000,0.000000,""],
  ["395017","汉威国际广场",0.000000,0.000000,""],
  ["395092","泥洼地铁站",0.000000,0.000000,""],
  ["395274","蒲黄榆地铁站店",0.000000,0.000000,""],
  ["395302","丽泽平安幸福中心店",0.000000,0.000000,""],
  ["395411","北京地铁六里桥站店",0.000000,0.000000,""],
  ["395412","北京地铁郭公庄站店",0.000000,0.000000,""],
  ["395413","北京地铁丰台科技园区站店",0.000000,0.000000,""],
  ["395470","丰科中心店",0.000000,0.000000,""],
  ["395606","福海公寓",0.000000,0.000000,""],
  ["395708","北京站6号候车室店",0.000000,0.000000,""],
  ["395710","北京首都机场T3三层出发区北指廊店",0.000000,0.000000,""],
  ["396053","房山南大街",0.000000,0.000000,""],
  ["396054","良乡拱辰南大街",0.000000,0.000000,""],
  ["396058","房山绿城百合",0.000000,0.000000,""],
  ["396081","房山燕化医院",0.000000,0.000000,""],
  ["396141","大兴星牌共享际",0.000000,0.000000,""],
  ["396255","东方航空公司总部店",0.000000,0.000000,""],
  ["396276","京东总部1号店",0.000000,0.000000,""],
  ["396388","北京鸿坤广场店",0.000000,0.000000,""],
  ["396449","大兴国际氢能示范区店",0.000000,0.000000,""],
  ["396495","大兴南航总部基地店",0.000000,0.000000,""],
  ["396706","北京南站出发层2店",0.000000,0.000000,""],
  ["397398","北京地铁物资学院路站店",0.000000,0.000000,""],
  ["397682","北京通州新光大",0.000000,0.000000,""],
  ["398090","超市发天通苑西区",0.000000,0.000000,""],
  ["398170","昌平未来科学城未来中心店",0.000000,0.000000,""],
  ["398439","北京地铁昌平站店",0.000000,0.000000,""],
  ["398558","北京怀柔雁栖人才社区店",0.000000,0.000000,""],
  ["398689","回龙观体育公园店",0.000000,0.000000,""],
  ["398708","北京站4号候车室店",0.000000,0.000000,""],
  ["398718","北京北站店",0.000000,0.000000,""],
  ["399519","古北水镇一号店",0.000000,0.000000,""],
  ["399520","古北水镇二号店",0.000000,0.000000,""],
  ["399548","北京中海大厦店",0.000000,0.000000,""],
  ["399569","西长安中骏世界城店",0.000000,0.000000,""],
  ["399670","北京门头沟长安天街店",0.000000,0.000000,""],
  ["399722","北京祥云小镇店",0.000000,0.000000,""],
  ["409392","天津市蓟州区天一绿海店",0.000000,0.000000,""],
  ["461717","廊坊燕郊夏威夷蓝湾店",0.000000,0.000000,""],
  ["462710","承德市德汇大厦店",0.000000,0.000000,""],
  ["462717","承德宽广时代广场店",0.000000,0.000000,""],
  ["466370","廊坊香河新开街店",0.000000,0.000000,""],
  ["466435","廊坊香河新华街店",0.000000,0.000000,""],
  ["466455","廊坊燕郊尚京广场店",0.000000,0.000000,""],
  ["466506","廊坊燕郊诺亚大厦店",0.000000,0.000000,""],
  ["466653","廊坊燕郊燕京理工学院店",0.000000,0.000000,""],
  ["466697","廊坊燕郊33号街区店",0.000000,0.000000,""],
  ["467708","三河文化艺术大街店",0.000000,0.000000,""],
  ["469475","承德市二仙居大街店",0.000000,0.000000,""],
  ["469476","承德市小佟沟路店",0.000000,0.000000,""],
  ["469517","承德市福地华园店",0.000000,0.000000,""],
  ["469559","承德市锦绣大街店",0.000000,0.000000,""],
  ["469585","承德御龙瀚府店",0.000000,0.000000,""],
  ["469588","承德铂悦山店",0.000000,0.000000,""],
  ["469659","承德兴盛丽水店",0.000000,0.000000,""],
  ["469660","承德银星丽苑店",0.000000,0.000000,""],
  ["469691","承德市嘉和广场店",0.000000,0.000000,""],
  ["471708","张家口崇礼汤印店-富龙子",0.000000,0.000000,""],
  ["478480","张家口容辰庄园店",0.000000,0.000000,""],
  ["478481","张家口明德南路店",0.000000,0.000000,""],
  ["478566","张家口宣化八中店",0.000000,0.000000,""],
  ["478567","张家口宣化兴泰店",0.000000,0.000000,""],
  ["478571","张家口宣化鼓楼店",0.000000,0.000000,""],
  ["478572","张家口宣化光大店",0.000000,0.000000,""],
  ["478573","张家口宣化皇城店",0.000000,0.000000,""],
  ["478581","张家口古宏大街店",0.000000,0.000000,""],
  ["478582","张家口北方学院西校区店",0.000000,0.000000,""],
  ["478583","张家口怀来新城佳苑店",0.000000,0.000000,""],
  ["478584","张家口怀来新东方店",0.000000,0.000000,""],
  ["478610","张家口职业技术学院店",0.000000,0.000000,""],
  ["478648","张家口乐享城店",0.000000,0.000000,""],
];

const STORE_WAVE_TIME_PRESETS = {
  "391001": { w1: "21:30", w2: "04:30" },
  "391012": { w1: "21:00", w2: "01:50" },
  "391028": { w1: "21:30", w2: "02:15" },
  "391030": { w1: "22:50", w2: "04:30" },
  "391074": { w1: "23:00", w2: "02:40" },
  "391094": { w1: "00:30", w2: "04:10" },
  "391409": { w2: "05:30" },
  "391705": { w2: "22:10" },
  "391709": { w2: "02:30" },
  "391712": { w1: "00:00", w2: "02:00" },
  "391715": { w1: "23:40", w2: "02:20" },
  "391718": { w2: "04:50" },
  "391721": { w2: "00:01" },
  "391722": { w2: "03:10" },
  "392029": { w1: "21:40", w2: "02:20" },
  "392084": { w1: "23:40", w2: "04:20" },
  "392144": { w1: "23:20", w2: "03:50" },
  "392179": { w1: "23:10", w2: "03:50" },
  "392315": { w2: "05:30" },
  "392394": { w1: "23:50", w2: "05:00" },
  "392395": { w2: "05:00" },
  "392408": { w2: "05:30" },
  "392525": { w1: "00:00", w2: "01:30" },
  "392647": { w2: "01:20" },
  "392678": { w1: "21:00", w2: "05:30" },
  "392707": { w2: "06:00" },
  "392708": { w2: "05:30" },
  "392710": { w2: "05:00" },
  "392712": { w1: "23:30", w2: "02:00" },
  "392713": { w2: "05:45" },
  "392715": { w2: "05:00" },
  "392716": { w2: "06:00" },
  "392717": { w2: "05:45" },
  "392718": { w1: "23:30", w2: "04:20" },
  "392719": { w1: "21:30", w2: "03:00" },
  "392720": { w2: "23:00" },
  "392721": { w2: "22:50" },
  "392722": { w1: "22:50", w2: "05:00" },
  "392723": { w1: "21:50", w2: "03:40" },
  "393003": { w2: "03:30" },
  "393009": { w1: "22:30", w2: "02:55" },
  "393010": { w1: "21:30", w2: "02:30" },
  "393016": { w1: "23:30", w2: "04:00" },
  "393032": { w1: "22:10", w2: "02:45" },
  "393033": { w2: "04:30" },
  "393034": { w1: "21:50", w2: "02:50" },
  "393035": { w1: "22:20", w2: "03:00" },
  "393046": { w2: "00:01" },
  "393067": { w1: "20:20", w2: "01:30" },
  "393082": { w1: "21:10", w2: "01:50" },
  "393117": { w1: "21:20", w2: "04:20" },
  "393151": { w1: "18:00" },
  "393152": { w1: "18:00" },
  "393153": { w1: "18:00" },
  "393155": { w1: "18:00" },
  "393156": { w1: "18:00" },
  "393167": { w2: "06:00" },
  "393168": { w2: "06:00" },
  "393188": { w1: "23:20", w2: "04:30" },
  "393206": { w1: "21:40", w2: "02:40" },
  "393220": { w1: "21:00", w2: "04:00" },
  "393244": { w1: "20:30", w2: "01:00" },
  "393303": { w1: "21:00", w2: "02:20" },
  "393380": { w1: "22:50", w2: "04:20" },
  "393399": { w2: "05:20" },
  "393416": { w2: "04:50" },
  "393425": { w2: "06:00" },
  "393426": { w2: "05:30" },
  "393546": { w1: "21:50", w2: "02:10" },
  "393557": { w1: "22:10", w2: "03:10" },
  "393651": { w1: "20:40", w2: "02:00" },
  "393675": { w1: "21:00", w2: "01:35" },
  "393688": { w1: "22:40", w2: "03:15" },
  "393706": { w2: "04:00" },
  "393707": { w1: "23:10", w2: "04:10" },
  "393708": { w2: "23:30" },
  "393709": { w2: "02:00" },
  "393710": { w2: "05:50" },
  "393711": { w2: "01:00" },
  "393715": { w1: "18:00" },
  "393717": { w2: "23:00" },
  "393718": { w2: "23:00" },
  "393720": { w2: "05:00" },
  "394024": { w1: "21:00", w2: "01:30" },
  "394038": { w1: "21:40", w2: "01:30" },
  "394048": { w1: "21:00", w2: "04:30" },
  "394062": { w1: "23:10", w2: "04:20" },
  "394069": { w1: "21:40", w2: "02:15" },
  "394070": { w1: "23:00", w2: "03:30" },
  "394072": { w1: "22:20", w2: "02:50" },
  "394073": { w1: "22:30", w2: "03:30" },
  "394075": { w1: "23:30", w2: "03:00" },
  "394085": { w1: "22:00", w2: "04:00" },
  "394089": { w1: "22:45", w2: "03:20" },
  "394096": { w1: "21:00", w2: "04:50" },
  "394103": { w1: "21:30", w2: "02:00" },
  "394104": { w1: "22:00", w2: "03:40" },
  "394105": { w1: "00:00", w2: "03:30" },
  "394110": { w1: "22:45", w2: "03:40" },
  "394112": { w1: "22:50", w2: "04:00" },
  "394118": { w1: "23:00", w2: "03:00" },
  "394122": { w2: "02:30" },
  "394123": { w2: "02:10" },
  "394142": { w1: "22:00", w2: "02:20" },
  "394150": { w1: "22:00", w2: "03:00" },
  "394196": { w2: "04:50" },
  "394234": { w1: "21:20", w2: "01:50" },
  "394258": { w1: "22:20", w2: "02:50" },
  "394405": { w2: "04:50" },
  "394406": { w1: "23:40", w2: "05:00" },
  "394419": { w2: "05:30" },
  "394437": { w1: "22:10", w2: "03:00" },
  "394451": { w1: "22:40", w2: "03:40" },
  "394485": { w1: "22:30", w2: "04:00" },
  "394488": { w1: "23:30", w2: "04:00" },
  "394489": { w2: "05:15" },
  "394510": { w1: "22:00", w2: "03:30" },
  "394512": { w1: "21:00", w2: "01:30" },
  "394526": { w2: "04:30" },
  "394560": { w1: "21:50", w2: "02:20" },
  "394620": { w1: "21:00", w2: "01:30" },
  "394630": { w1: "21:30", w2: "04:00" },
  "394705": { w2: "01:00" },
  "394720": { w2: "23:00" },
  "395017": { w2: "04:00" },
  "395092": { w2: "01:10" },
  "395274": { w1: "22:20", w2: "03:20" },
  "395302": { w2: "00:30" },
  "395411": { w2: "04:30" },
  "395412": { w2: "05:30" },
  "395413": { w2: "05:00" },
  "395470": { w2: "03:40" },
  "395606": { w2: "03:20" },
  "395708": { w2: "23:00" },
  "395710": { w1: "18:00" },
  "396053": { w2: "04:30" },
  "396054": { w2: "03:10" },
  "396058": { w2: "02:10" },
  "396081": { w2: "03:40" },
  "396141": { w2: "00:50" },
  "396255": { w2: "00:20" },
  "396276": { w2: "01:30" },
  "396388": { w2: "01:20" },
  "396449": { w2: "23:30" },
  "396495": { w2: "00:01" },
  "396706": { w2: "23:00" },
  "397398": { w2: "04:30" },
  "397682": { w1: "23:35", w2: "02:30" },
  "398090": { w1: "21:00", w2: "02:30" },
  "398170": { w1: "20:30", w2: "02:00" },
  "398439": { w2: "05:30" },
  "398558": { w2: "23:00" },
  "398689": { w2: "05:30" },
  "398708": { w2: "23:00" },
  "398718": { w1: "22:00", w2: "04:00" },
  "399519": { w2: "02:30" },
  "399520": { w2: "03:00" },
  "399548": { w1: "23:15", w2: "03:10" },
  "399569": { w1: "22:45", w2: "02:50" },
  "399670": { w1: "00:15", w2: "01:50" },
  "399722": { w2: "01:30" },
  "409392": { w2: "02:00" },
  "461717": { w2: "01:40" },
  "462710": { w2: "03:50" },
  "462717": { w2: "00:01" },
  "466370": { w2: "02:30" },
  "466435": { w2: "03:30" },
  "466455": { w2: "01:00" },
  "466506": { w2: "00:01" },
  "466653": { w2: "04:00" },
  "466697": { w2: "00:30" },
  "467708": { w2: "03:00" },
  "469475": { w2: "03:30" },
  "469476": { w2: "03:10" },
  "469517": { w2: "01:30" },
  "469559": { w2: "04:10" },
  "469585": { w2: "00:01" },
  "469588": { w2: "23:30" },
  "469659": { w2: "03:30" },
  "469660": { w2: "02:30" },
  "469691": { w2: "02:00" },
  "471708": { w2: "03:30" },
  "478480": { w2: "01:00" },
  "478481": { w2: "01:30" },
  "478566": { w2: "01:15" },
  "478567": { w2: "00:01" },
  "478571": { w2: "01:00" },
  "478572": { w2: "01:30" },
  "478573": { w2: "23:40" },
  "478581": { w2: "01:50" },
  "478582": { w2: "02:30" },
  "478583": { w2: "01:45" },
  "478584": { w2: "02:00" },
  "478610": { w2: "00:00" },
  "478648": { w2: "00:30" },
};

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
const STORE_WAVE_BELONGS_PRESETS = {
  "391001": "1,2",
  "391012": "1,2",
  "391028": "1,2",
  "391030": "1,2",
  "391074": "1,2",
  "391094": "1,2",
  "391409": "2",
  "391705": "3",
  "391709": "2",
  "391712": "1,2",
  "391715": "1,2",
  "391718": "3",
  "391721": "3",
  "391722": "2",
  "392029": "1,2",
  "392084": "1,2",
  "392144": "1,2",
  "392179": "1,2",
  "392315": "2",
  "392394": "1,2",
  "392395": "2",
  "392408": "3",
  "392525": "1,2",
  "392647": "2",
  "392678": "1,2",
  "392707": "3",
  "392708": "3",
  "392710": "3",
  "392712": "1,2",
  "392713": "3",
  "392715": "2",
  "392716": "3",
  "392717": "3",
  "392718": "1,2",
  "392719": "1,2",
  "392720": "3",
  "392721": "3",
  "392722": "1,2",
  "392723": "1,2",
  "393003": "2",
  "393009": "1,2",
  "393010": "1,2",
  "393016": "1,2",
  "393032": "1,2",
  "393033": "2",
  "393034": "1,2",
  "393035": "1,2",
  "393046": "3",
  "393067": "1,2",
  "393082": "1,2",
  "393117": "1,2",
  "393151": "4",
  "393152": "4",
  "393153": "4",
  "393155": "4",
  "393156": "4",
  "393167": "2",
  "393168": "2",
  "393188": "1,2",
  "393206": "1,2",
  "393220": "1,2",
  "393244": "1,2",
  "393303": "1,2",
  "393380": "1,2",
  "393399": "3",
  "393416": "2",
  "393425": "2",
  "393426": "2",
  "393546": "1,2",
  "393557": "1,2",
  "393651": "1,2",
  "393675": "1,2",
  "393688": "1,2",
  "393706": "3",
  "393707": "1,2",
  "393708": "3",
  "393709": "2",
  "393710": "3",
  "393711": "2",
  "393715": "4",
  "393717": "3",
  "393718": "3",
  "393720": "2",
  "394024": "1,2",
  "394038": "1,2",
  "394048": "1,2",
  "394062": "1,2",
  "394069": "1,2",
  "394070": "1,2",
  "394072": "1,2",
  "394073": "1,2",
  "394075": "1,2",
  "394085": "1,2",
  "394089": "1,2",
  "394096": "1,2",
  "394103": "1,2",
  "394104": "1,2",
  "394105": "1,2",
  "394110": "1,2",
  "394112": "1,2",
  "394118": "1,2",
  "394122": "2",
  "394123": "2",
  "394142": "1,2",
  "394150": "1,2",
  "394196": "2",
  "394234": "1,2",
  "394258": "1,2",
  "394405": "2",
  "394406": "1,2",
  "394419": "2",
  "394437": "1,2",
  "394451": "1,2",
  "394485": "1,2",
  "394488": "1,2",
  "394489": "2",
  "394510": "1,2",
  "394512": "1,2",
  "394526": "2",
  "394560": "1,2",
  "394620": "1,2",
  "394630": "1,2",
  "394705": "3",
  "394720": "3",
  "395017": "2",
  "395092": "3",
  "395274": "1,2",
  "395302": "3",
  "395411": "3",
  "395412": "3",
  "395413": "3",
  "395470": "2",
  "395606": "2",
  "395708": "3",
  "395710": "4",
  "396053": "3",
  "396054": "3",
  "396058": "3",
  "396081": "3",
  "396141": "3",
  "396255": "3",
  "396276": "2",
  "396388": "3",
  "396449": "3",
  "396495": "3",
  "396706": "3",
  "397398": "3",
  "397682": "1,2",
  "398090": "1,2",
  "398170": "1,2",
  "398439": "2",
  "398558": "3",
  "398689": "2",
  "398708": "3",
  "398718": "1,2",
  "399519": "3",
  "399520": "3",
  "399548": "1,2",
  "399569": "1,2",
  "399670": "1,2",
  "399722": "2",
  "409392": "3",
  "461717": "3",
  "462710": "3",
  "462717": "3",
  "466370": "3",
  "466435": "3",
  "466455": "3",
  "466506": "3",
  "466653": "3",
  "466697": "3",
  "467708": "3",
  "469475": "3",
  "469476": "3",
  "469517": "3",
  "469559": "3",
  "469585": "3",
  "469588": "3",
  "469659": "3",
  "469660": "3",
  "469691": "3",
  "471708": "3",
  "478480": "3",
  "478481": "3",
  "478566": "3",
  "478567": "3",
  "478571": "3",
  "478572": "3",
  "478573": "3",
  "478581": "3",
  "478582": "3",
  "478583": "3",
  "478584": "3",
  "478610": "3",
  "478648": "3",
};

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
const AUTO_BY_W2_STORE_IDS = new Set([
  "391409",
  "391705",
  "391709",
  "391718",
  "391721",
  "391722",
  "392315",
  "392395",
  "392408",
  "392647",
  "392707",
  "392708",
  "392710",
  "392713",
  "392715",
  "392716",
  "392717",
  "392720",
  "392721",
  "393003",
  "393033",
  "393046",
  "393167",
  "393168",
  "393399",
  "393416",
  "393425",
  "393426",
  "393706",
  "393708",
  "393709",
  "393710",
  "393711",
  "393717",
  "393718",
  "393720",
  "394122",
  "394123",
  "394196",
  "394405",
  "394419",
  "394489",
  "394526",
  "394705",
  "394720",
  "395017",
  "395092",
  "395302",
  "395411",
  "395412",
  "395413",
  "395470",
  "395606",
  "395708",
  "396053",
  "396054",
  "396058",
  "396081",
  "396141",
  "396255",
  "396276",
  "396388",
  "396449",
  "396495",
  "396706",
  "397398",
  "398439",
  "398558",
  "398689",
  "398708",
  "399519",
  "399520",
  "399722",
  "409392",
  "461717",
  "462710",
  "462717",
  "466370",
  "466435",
  "466455",
  "466506",
  "466653",
  "466697",
  "467708",
  "469475",
  "469476",
  "469517",
  "469559",
  "469585",
  "469588",
  "469659",
  "469660",
  "469691",
  "471708",
  "478480",
  "478481",
  "478566",
  "478567",
  "478571",
  "478572",
  "478573",
  "478581",
  "478582",
  "478583",
  "478584",
  "478610",
  "478648",
]);

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function normalizeStoreCode(id) {
  const raw = String(id || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (raw === "93003") return "393003";
  return raw;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function isAutoByW2Store(id) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return AUTO_BY_W2_STORE_IDS.has(normalizeStoreCode(id));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getStoreWavePreset(id) {
  const raw = normalizeStoreCode(id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (STORE_WAVE_TIME_PRESETS[raw]) return STORE_WAVE_TIME_PRESETS[raw];
  const compatId = raw.length === 5 ? `3${raw}` : "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return compatId ? STORE_WAVE_TIME_PRESETS[compatId] || null : null;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function toMinutes(v) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const [h, m] = String(v || "00:00").split(":").map(Number);
  return (h || 0) * 60 + (m || 0);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function alignMinuteToDispatch(minute, dispatchStartMin) {
  return minute < dispatchStartMin ? minute + 1440 : minute;
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function formatTime(v) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const day = 24 * 60;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const n = ((Math.round(v) % day) + day) % day;
  return `${String(Math.floor(n / 60)).padStart(2, "0")}:${String(n % 60).padStart(2, "0")}`;
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function formatRate(v) { return `${(v * 100).toFixed(1)}%`; }
function formatMinutesValue(v) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const n = Number(v || 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return Number(n.toFixed(1)).toString();
}
function parseStoreIds(v) { return String(v || "").split(",").map((x) => x.trim()).filter(Boolean); }
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function normalizeWaveBelongsInput(v) {
  return Array.from(new Set(
    String(v || "")
      .split(",")
      .map((x) => x.trim())
      .filter((x) => /^\d+$/.test(x))
  )).join(",");
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function parseWaveBelongs(v) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const normalized = normalizeWaveBelongsInput(v);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return normalized ? normalized.split(",") : [];
}
function clone(v) { return JSON.parse(JSON.stringify(v)); }

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function syncRealtimeDispatchContext() {
  const context = {
    vehicles: clone(state.vehicles || []),
    waves: clone(state.waves || []),
    settings: clone(state.settings || {}),
    strategyConfig: clone(state.strategyConfig || {}),
    activeResultKey: String(state.activeResultKey || "").trim(),
    updatedAt: new Date().toISOString(),
  };
  window.__dispatchRealtimeContext = context;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    localStorage.setItem(REALTIME_DISPATCH_CONTEXT_KEY, JSON.stringify(context));
  } catch (error) {
    console.warn("[simulate] sync realtime dispatch context failed:", error);
  }
}
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildWaveSpan(start, end) { const startMin = toMinutes(start); let endMin = toMinutes(end); if (endMin <= startMin) endMin += 1440; return { startMin, endMin }; }

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildStores() {
  return FIXED_STORES
    .filter(([id]) => getStoreWavePreset(id))
    .map(([id, name, lng, lat, district], index) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const normalizedId = normalizeStoreCode(id);
      const preset = getStoreWavePreset(normalizedId) || {};
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const autoByW2 = isAutoByW2Store(normalizedId);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const waveArrivals = {
        w1: autoByW2 ? "" : (preset.w1 || ""),
        w2: preset.w2 || "",
        ...(preset.w3 ? { w3: preset.w3 } : {}),
        ...(preset.w4 ? { w4: preset.w4 } : {}),
      };
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const waveBelongs = normalizeWaveBelongsInput(STORE_WAVE_BELONGS_PRESETS[normalizedId] || [waveArrivals.w1 ? "1" : "", waveArrivals.w2 ? "2" : ""].filter(Boolean).join(","));
      return syncStoreWaveLoads({
        id: normalizedId, name, lng, lat, district, address: district,
        tripCount: Math.max(1, waveBelongs ? waveBelongs.split(",").length : 1),
        waveBelongs,
        rpcs: 8 + (index % 10),
        rcase: 0,
        bpcs: 0,
        bpaper: 0,
        apcs: 0,
        apaper: 0,
        rpaper: 0,
        coldRatio: 0,
        desiredArrival: waveArrivals.w1 || "",
        waveArrivals,
        wave1Mode: autoByW2 ? "AUTO_BY_W2" : "FIXED",
        serviceMinutes: 10 + (index % 4) * 2,
        difficulty: 1 + (index % 4) * 0.1,
        parking: 10,
      });
    });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildAutoWaves(stores) {
  const waveBuckets = { 1: [], 2: [], 3: [], 4: [] };
  stores.forEach((store) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let belongs = parseWaveBelongs(store.waveBelongs || "");
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!belongs.length) {
      belongs = [];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (store.waveArrivals?.w1) belongs.push("1");
      if (store.waveArrivals?.w2) belongs.push("2");
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const seen = new Set();
    belongs.forEach((waveNo) => {
      if (!["1", "2", "3", "4"].includes(waveNo)) return;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (seen.has(waveNo)) return;
      seen.add(waveNo);
      waveBuckets[Number(waveNo)].push(store.id);
    });
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return [
    { waveId: "W1", start: "19:10", end: "23:59", endMode: "return", storeIds: waveBuckets[1].join(",") },
    { waveId: "W2", start: "21:00", end: "07:00", endMode: "service", storeIds: waveBuckets[2].join(",") },
    { waveId: "W3", start: "19:30", end: "07:00", endMode: "service", storeIds: waveBuckets[3].join(",") },
    { waveId: "W4", start: "12:00", end: "18:00", endMode: "service", storeIds: waveBuckets[4].join(",") },
  ];
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function sampleData() {
  const stores = buildStores();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    vehicles: [
      { plateNo: "京A-1001", driverName: "", type: ENFORCED_VEHICLE_TYPE, capacity: 120, speed: 38, canCarryCold: true },
      { plateNo: "京A-1002", driverName: "", type: "4.2米厢式货车", capacity: 100, speed: 38, canCarryCold: false },
      { plateNo: "京A-1003", driverName: "", type: "4.2米厢式货车", capacity: 100, speed: 38, canCarryCold: false },
      { plateNo: "京A-1004", driverName: "", type: "4.2米厢式货车", capacity: 100, speed: 38, canCarryCold: false },
    ],
    stores,
    waves: buildAutoWaves(stores),
  };
}

function toHHMMText(v) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const raw = String(v || "").trim();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!raw) return "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const m = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return raw;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return `${String(Number(m[1])).padStart(2, "0")}:${m[2]}`;
}

function mapBackendShopRow(row) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const code = normalizeStoreCode(row.shop_code || "");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveBelongs = normalizeWaveBelongsInput(row.wave_belongs || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const w1 = toHHMMText(row.wave1_time);
  const w2 = toHHMMText(row.wave2_time);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const w3 = toHHMMText(row.arrival_time_w3 || row.wave3_time || row.wave3_arrival || "");
  const w4 = toHHMMText(row.arrival_time_w4 || row.wave4_time || row.wave4_arrival || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const autoByW2 = isAutoByW2Store(code);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return syncStoreWaveLoads({
    id: code,
    name: row.shop_name || "",
    district: row.district || "",
    lng: Number(row.lng || 0),
    lat: Number(row.lat || 0),
    address: row.address || row.detailed_address || row.district || "",
    detailedAddress: row.detailed_address || "",
    tripCount: Math.max(1, Number(row.trip_count || (waveBelongs ? waveBelongs.split(",").length : 1))),
    waveBelongs,
    rpcs: Number(row.rpcs || 0),
    rcase: Number(row.rcase || 0),
    bpcs: Number(row.bpcs || 0),
    bpaper: Number(row.bpaper || 0),
    apcs: Number(row.apcs || 0),
    apaper: Number(row.apaper || 0),
    rpaper: Number(row.rpaper || 0),
    coldRatio: Number(row.cold_ratio || 0),
    desiredArrival: autoByW2 ? "" : (w1 || ""),
    waveArrivals: {
      w1: autoByW2 ? "" : (w1 || ""),
      ...(w2 ? { w2 } : {}),
      ...(w3 ? { w3 } : {}),
      ...(w4 ? { w4 } : {}),
    },
    wave1Mode: autoByW2 ? "AUTO_BY_W2" : (w1 ? "FIXED" : ""),
    serviceMinutes: Number(row.service_minutes || 15),
    difficulty: Number(row.difficulty || 1),
    parking: Number(row.allowed_late_minutes || 10),
    status: row.schedule_status || "",
    plateNo: row.plate_no || "",
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function fetchStoresFromBackend() {
  const available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/shops/list`, {}, 3000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!payload?.ok || !Array.isArray(payload.shops)) return [];
  return payload.shops.map(mapBackendShopRow).filter((store) => store.id && store.name);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function mapWmsStoreRow(row) {
  const id = normalizeStoreCode(row?.shop_code || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return syncStoreWaveLoads({
    id,
    name: String(row?.shop_name || "").trim(),
    district: String(row?.district || "").trim(),
    lng: Number(row?.lng || 0),
    lat: Number(row?.lat || 0),
    address: String(row?.district || "").trim(),
    detailedAddress: "",
    tripCount: 1,
    waveBelongs: "1,2",
    rpcs: 0,
    rcase: 0,
    bpcs: 0,
    bpaper: 0,
    apcs: 0,
    apaper: 0,
    rpaper: 0,
    coldRatio: 0,
    desiredArrival: "02:45",
    waveArrivals: { w1: "02:45", w2: "01:50" },
    wave1Mode: "FIXED",
    serviceMinutes: 15,
    difficulty: 1,
    parking: 10,
    status: "",
    plateNo: "",
  });
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function mapWmsVehicleRow(row) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    plateNo: String(row?.plate_no || "").trim(),
    driverName: String(row?.driver_name || "").trim(),
    type: ENFORCED_VEHICLE_TYPE,
    capacity: 100,
    speed: 38,
    canCarryCold: false,
  };
}

async function fetchWmsStoresFromBackend() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/stores`, {}, 8000);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!response.ok) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return payload.items.map((row) => mapWmsStoreRow(row)).filter((item) => item.id && item.name);
}

async function fetchWmsVehiclesFromBackend() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const available = await ensureGaBackendAvailable(true);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/vehicles`, {}, 8000);
  if (!response.ok) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return payload.items.map((row) => mapWmsVehicleRow(row)).filter((item) => item.plateNo);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function fetchWmsCargoLatestMap() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return new Map();
}

function computeResolvedLoadsFromCleanCargoAndTiming(cargo = {}, timing = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const belongsText = normalizeWaveBelongsInput(String(timing?.wave_belongs || ""));
  const has1 = /(^|,)\s*1\s*(,|$)/.test(belongsText);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const has2 = /(^|,)\s*2\s*(,|$)/.test(belongsText);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const has3 = /(^|,)\s*3\s*(,|$)/.test(belongsText);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const has4 = /(^|,)\s*4\s*(,|$)/.test(belongsText);
  const only2 = belongsText === "2";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const rpcs = Number(cargo?.rpcs || 0);
  const rcase = Number(cargo?.rcase || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const bpcs = Number(cargo?.bpcs || 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const bpaper = Number(cargo?.bpaper || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const apcs = Number(cargo?.apcs || 0);
  const apaper = Number(cargo?.apaper || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const rpaper = Number(cargo?.rpaper || 0);
  const baseW1 = (rpcs / 207) + (rcase / 380) + (bpcs / 120) + (bpaper / 380) + (rpaper / 380);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseW2 = (apcs / 350) + (apaper / 380);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const total = baseW1 + baseW2;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let w1 = 0;
  let w2 = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let w3 = 0;
  let w4 = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (only2) {
    w2 = total;
  } else if (has3 || has4) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (has3) w3 = total;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (has4) w4 = total;
  } else if (has1 || has2) {
    w1 = baseW1;
    w2 = baseW2;
  }
  return {
    wave_belongs: belongsText,
    wave1_load: Number(w1.toFixed(6)),
    wave2_load: Number(w2.toFixed(6)),
    wave3_load: Number(w3.toFixed(6)),
    wave4_load: Number(w4.toFixed(6)),
    total_resolved_load: Number((w1 + w2 + w3 + w4).toFixed(6)),
    first_wave_time: String(timing?.first_wave_time || "").trim(),
    second_wave_time: String(timing?.second_wave_time || "").trim(),
    arrival_time_w3: String(timing?.arrival_time_w3 || "").trim(),
    arrival_time_w4: String(timing?.arrival_time_w4 || "").trim(),
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function fetchCleanCargoRawMap(shopCodes = []) {
  const available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) return new Map();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const codes = Array.isArray(shopCodes) ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean) : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/clean-cargo-raw/list?limit=5000`, {}, 12000);
  if (!response.ok) return new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const allowed = codes.length ? new Set(codes) : null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = new Map();
  payload.items.forEach((row) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const key = normalizeStoreCode(row?.shop_code || "");
    if (!key) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (allowed && !allowed.has(key)) return;
    if (map.has(key)) return;
    map.set(key, {
      rpcs: Number(row?.rpcs || 0),
      rcase: Number(row?.rcase || 0),
      bpcs: Number(row?.bpcs || 0),
      bpaper: Number(row?.bpaper || 0),
      apcs: Number(row?.apcs || 0),
      apaper: Number(row?.apaper || 0),
      rpaper: Number(row?.rpaper || 0),
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return map;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function fetchStoreWaveTimingMap(shopCodes = []) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const available = await ensureGaBackendAvailable(true);
  if (!available) return new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const codes = Array.isArray(shopCodes) ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean) : [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/store-wave-timing-resolved/list?limit=5000`, {}, 12000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return new Map();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!payload?.ok || !Array.isArray(payload.items)) return new Map();
  const allowed = codes.length ? new Set(codes) : null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = new Map();
  payload.items.forEach((row) => {
    const key = normalizeStoreCode(row?.shop_code || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!key) return;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (allowed && !allowed.has(key)) return;
    map.set(key, {
      wave_belongs: String(row?.wave_belongs || "").trim(),
      first_wave_time: toHHMMText(String(row?.first_wave_time || "").trim()),
      second_wave_time: toHHMMText(String(row?.second_wave_time || "").trim()),
      arrival_time_w3: toHHMMText(String(row?.arrival_time_w3 || "").trim()),
      arrival_time_w4: toHHMMText(String(row?.arrival_time_w4 || "").trim()),
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return map;
}

async function fetchStoreWaveResolvedLoadMap(shopCodes = []) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const codes = Array.isArray(shopCodes)
    ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean)
    : [];
  const [cargoMap, timingMap] = await Promise.all([
    fetchCleanCargoRawMap(codes),
    fetchStoreWaveTimingMap(codes),
  ]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const allKeys = new Set([...cargoMap.keys(), ...timingMap.keys()]);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = new Map();
  allKeys.forEach((key) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const resolved = computeResolvedLoadsFromCleanCargoAndTiming(cargoMap.get(key) || {}, timingMap.get(key) || {});
    map.set(key, { shop_code: key, ...resolved });
  });
  return map;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function fetchStoreWaveResolvedList({ shopCode = "", waveBelongs = "", limit = 200 } = {}) {
  const available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) return { ok: false, items: [], count: 0, total: 0, error: "backend_unavailable" };
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const params = new URLSearchParams();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (shopCode) params.set("shopCode", String(shopCode).trim());
  if (waveBelongs) params.set("waveBelongs", String(waveBelongs).trim());
  params.set("limit", String(Math.max(1, Math.min(2000, Number(limit || 200) || 200))));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/store-wave-load-resolved/list?${params.toString()}`, {}, 12000);
  if (!response.ok) return { ok: false, items: [], count: 0, total: 0, error: `http_${response.status}` };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const payload = await response.json();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return payload || { ok: false, items: [], count: 0, total: 0 };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildStoreWaveResolvedQueryConfirmText(payload = {}) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const byWave = { "1": [], "2": [], "3": [], "4": [] };
  items.forEach((row) => {
    const code = String(row?.shop_code || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const belongs = parseWaveBelongs(String(row?.wave_belongs || "").trim());
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (belongs.includes("1")) byWave["1"].push(`${code} | 货量=${Number(row?.wave1_load || 0)} | 时间=${String(row?.first_wave_time || "").trim() || "(空)"}`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (belongs.includes("2")) byWave["2"].push(`${code} | 货量=${Number(row?.wave2_load || 0)} | 时间=${String(row?.second_wave_time || "").trim() || "(空)"}`);
    if (belongs.includes("3")) byWave["3"].push(`${code} | 货量=${Number(row?.wave3_load || 0)} | 时间=${String(row?.arrival_time_w3 || "").trim() || "(空)"}`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (belongs.includes("4")) byWave["4"].push(`${code} | 货量=${Number(row?.wave4_load || 0)} | 时间=${String(row?.arrival_time_w4 || "").trim() || "(空)"}`);
  });
  return [
    "即将写入“折算货量查询”展示区（来源：store_wave_load_resolved）",
    `返回条数：${Number(payload?.count || items.length || 0)} / 总计：${Number(payload?.total || items.length || 0)}`,
    "",
    `W1 明细(${byWave["1"].length})`,
    ...byWave["1"],
    "",
    `W2 明细(${byWave["2"].length})`,
    ...byWave["2"],
    "",
    `W3 明细(${byWave["3"].length})`,
    ...byWave["3"],
    "",
    `W4 明细(${byWave["4"].length})`,
    ...byWave["4"],
    "",
    "点击“确定”继续写入查询结果，点击“取消”放弃本次写入。"
  ].join("\n");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function showStoreWaveResolvedQueryConfirmDialog(reportText) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return new Promise((resolve) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const overlay = document.createElement("div");
    overlay.style.position = "fixed";
    overlay.style.inset = "0";
    overlay.style.background = "rgba(0,0,0,0.35)";
    overlay.style.zIndex = "99999";
    overlay.style.display = "flex";
    overlay.style.alignItems = "center";
    overlay.style.justifyContent = "center";

    const panel = document.createElement("div");
    panel.style.width = "min(960px, 94vw)";
    panel.style.maxHeight = "86vh";
    panel.style.background = "#fff";
    panel.style.borderRadius = "12px";
    panel.style.boxShadow = "0 12px 28px rgba(0,0,0,0.24)";
    panel.style.display = "flex";
    panel.style.flexDirection = "column";

    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const title = document.createElement("div");
    title.textContent = "查询确认（store_wave_load_resolved）";
    title.style.padding = "14px 16px 8px";
    title.style.fontWeight = "700";
    title.style.fontSize = "20px";

    const content = document.createElement("pre");
    content.textContent = String(reportText || "");
    content.style.margin = "0 16px";
    content.style.padding = "10px";
    content.style.border = "1px solid #d9e2ec";
    content.style.borderRadius = "8px";
    content.style.background = "#f8fafc";
    content.style.overflow = "auto";
    content.style.whiteSpace = "pre-wrap";
    content.style.wordBreak = "break-word";
    content.style.fontFamily = "Consolas, 'Courier New', monospace";
    content.style.fontSize = "13px";
    content.style.lineHeight = "1.35";
    content.style.maxHeight = "58vh";

    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const footer = document.createElement("div");
    footer.style.display = "flex";
    footer.style.gap = "12px";
    footer.style.justifyContent = "flex-end";
    footer.style.padding = "12px 16px 16px";

    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.textContent = "取消";
    cancelBtn.style.minWidth = "96px";
    cancelBtn.style.height = "40px";

    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const okBtn = document.createElement("button");
    okBtn.type = "button";
    okBtn.textContent = "确定";
    okBtn.style.minWidth = "96px";
    okBtn.style.height = "40px";

    const finish = (ok) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      try { document.body.removeChild(overlay); } catch {}
      resolve(Boolean(ok));
    };
    cancelBtn.addEventListener("click", () => finish(false));
    okBtn.addEventListener("click", () => finish(true));
    overlay.addEventListener("click", (event) => {
      if (event.target === overlay) finish(false);
    });

    footer.appendChild(cancelBtn);
    footer.appendChild(okBtn);
    panel.appendChild(title);
    panel.appendChild(content);
    panel.appendChild(footer);
    overlay.appendChild(panel);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    document.body.appendChild(overlay);
  });
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function renderStoreWaveResolvedTable(payload = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const summary = document.getElementById("storeWaveResolvedSummary");
  const body = document.getElementById("storeWaveResolvedTableBody");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!summary || !body) return;
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const sortField = String(state.ui.storeWaveResolvedSortField || "shop_code");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const sortDir = String(state.ui.storeWaveResolvedSortDir || "asc") === "desc" ? "desc" : "asc";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const toSortValue = (row, field) => {
    if (field === "updated_at") return String(row?.updated_at || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (field === "wave_belongs" || field === "shop_code" || field === "first_wave_time" || field === "second_wave_time" || field === "arrival_time_w3" || field === "arrival_time_w4") {
      return String(row?.[field] || "");
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return Number(row?.[field] || 0) || 0;
  };
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const items = rawItems.slice().sort((a, b) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const av = toSortValue(a, sortField);
    const bv = toSortValue(b, sortField);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let cmp = 0;
    if (typeof av === "number" && typeof bv === "number") cmp = av - bv;
    else cmp = String(av).localeCompare(String(bv), "zh-Hans-CN", { numeric: true });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return sortDir === "asc" ? cmp : -cmp;
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const count = Number(payload.count || items.length || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const total = Number(payload.total || count || 0);
  const limit = Number(payload.limit || 0);
  summary.textContent = `当前返回 ${count} 条 / 总计 ${total} 条${limit ? `（limit=${limit}）` : ""}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="12" class="muted">无匹配数据</td></tr>`;
    return;
  }
  body.innerHTML = items.map((row) => `
    <tr>
      <td>${escapeHtml(String(row.shop_code || ""))}</td>
      <td>${escapeHtml(String(row.wave_belongs || ""))}</td>
      <td>${formatLoadConvertValue(row.wave1_load || 0)}</td>
      <td>${formatLoadConvertValue(row.wave2_load || 0)}</td>
      <td>${formatLoadConvertValue(row.wave3_load || 0)}</td>
      <td>${formatLoadConvertValue(row.wave4_load || 0)}</td>
      <td>${formatLoadConvertValue(row.total_resolved_load || 0)}</td>
      <td>${escapeHtml(String(row.first_wave_time || ""))}</td>
      <td>${escapeHtml(String(row.second_wave_time || ""))}</td>
      <td>${escapeHtml(String(row.arrival_time_w3 || ""))}</td>
      <td>${escapeHtml(String(row.arrival_time_w4 || ""))}</td>
      <td>${escapeHtml(String(row.updated_at || ""))}</td>
    </tr>
  `).join("");
}

function updateStoreWaveResolvedSortMarks() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.querySelectorAll("[data-store-wave-sort]").forEach((btn) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const field = String(btn.getAttribute("data-store-wave-sort") || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const active = field === String(state.ui.storeWaveResolvedSortField || "");
    const mark = active ? (state.ui.storeWaveResolvedSortDir === "desc" ? "▼" : "▲") : "";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const markNode = btn.querySelector(".store-wave-sort-mark");
    if (markNode) markNode.textContent = mark;
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function toggleStoreWaveResolvedSort(field) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const nextField = String(field || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!nextField) return;
  if (state.ui.storeWaveResolvedSortField === nextField) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    state.ui.storeWaveResolvedSortDir = state.ui.storeWaveResolvedSortDir === "asc" ? "desc" : "asc";
  } else {
    state.ui.storeWaveResolvedSortField = nextField;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ui.storeWaveResolvedSortDir = "asc";
  }
  renderStoreWaveResolvedTable(storeWaveResolvedCache);
  updateStoreWaveResolvedSortMarks();
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function queryStoreWaveResolvedTable({ needConfirm = false } = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const shopCode = String(document.getElementById("storeWaveResolvedShopCodeInput")?.value || "").trim();
  const waveBelongs = String(document.getElementById("storeWaveResolvedWaveBelongsInput")?.value || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const limit = Number(document.getElementById("storeWaveResolvedLimitInput")?.value || 200);
  const payload = await fetchStoreWaveResolvedList({ shopCode, waveBelongs, limit });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (needConfirm) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const confirmText = buildStoreWaveResolvedQueryConfirmText(payload);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const ok = await showStoreWaveResolvedQueryConfirmDialog(confirmText);
    if (!ok) return;
  }
  storeWaveResolvedCache = {
    items: Array.isArray(payload?.items) ? payload.items : [],
    count: Number(payload?.count || 0),
    total: Number(payload?.total || 0),
    limit: Number(payload?.limit || limit || 200),
  };
  renderStoreWaveResolvedTable(payload);
  updateStoreWaveResolvedSortMarks();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function buildResolvedLoadRowsFromDualTables() {
  const storeIds = (Array.isArray(state.stores) ? state.stores : [])
    .map((store) => normalizeStoreCode(store?.id || store?.shopCode || ""))
    .filter(Boolean);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const resolvedMap = await fetchStoreWaveResolvedLoadMap(storeIds);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const rows = [];
  resolvedMap.forEach((resolved, shopCode) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const belongs = String(resolved?.wave_belongs || "").trim();
    const belongsSet = new Set(parseWaveBelongs(belongs));
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const wave1 = belongsSet.has("1") ? (Number(resolved?.wave1_load || 0) || 0) : 0;
    const wave2 = belongsSet.has("2") ? (Number(resolved?.wave2_load || 0) || 0) : 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave3 = belongsSet.has("3") ? (Number(resolved?.wave3_load || 0) || 0) : 0;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const wave4 = belongsSet.has("4") ? (Number(resolved?.wave4_load || 0) || 0) : 0;
    rows.push({
      shop_code: shopCode,
      wave_belongs: belongs,
      wave1_load: wave1,
      wave2_load: wave2,
      wave3_load: wave3,
      wave4_load: wave4,
      total_resolved_load: wave1 + wave2 + wave3 + wave4,
      first_wave_time: belongsSet.has("1") ? String(resolved?.first_wave_time || "").trim() : "",
      second_wave_time: belongsSet.has("2") ? String(resolved?.second_wave_time || "").trim() : "",
      arrival_time_w3: belongsSet.has("3") ? String(resolved?.arrival_time_w3 || "").trim() : "",
      arrival_time_w4: belongsSet.has("4") ? String(resolved?.arrival_time_w4 || "").trim() : "",
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return rows;
}

async function saveDualTableLoadsToBackend() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const available = await ensureGaBackendAvailable(true);
  if (!available) throw new Error("backend_unavailable");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rows = await buildResolvedLoadRowsFromDualTables();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const formatWaveDetails = (waveNo, fieldLoad, fieldTime) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const picked = rows.filter((row) => parseWaveBelongs(row.wave_belongs || "").includes(String(waveNo)));
    const lines = picked.map((row) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const load = Number(row?.[fieldLoad] || 0);
      const time = String(row?.[fieldTime] || "").trim();
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return `${row.shop_code} | 货量=${load} | 时间=${time || "(空)"}`;
    });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return { count: picked.length, lines };
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const w1 = formatWaveDetails("1", "wave1_load", "first_wave_time");
  const w2 = formatWaveDetails("2", "wave2_load", "second_wave_time");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const w3 = formatWaveDetails("3", "wave3_load", "arrival_time_w3");
  const w4 = formatWaveDetails("4", "wave4_load", "arrival_time_w4");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetchJsonWithTimeout(
    `${GA_BACKEND_URL}/store-wave-load-resolved/save`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ rows })
    },
    20000
  );
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!response.ok) {
    throw new Error(`http_${response.status}`);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const payload = await response.json();
  const reportText = [
    "保存双表货量结果（写入目标表：store_wave_load_resolved）",
    `总行数：${rows.length}`,
    `后端返回 upserted：${Number(payload?.upserted || 0)}`,
    "",
    `W1 写入 ${w1.count} 行：`,
    ...w1.lines,
    "",
    `W2 写入 ${w2.count} 行：`,
    ...w2.lines,
    "",
    `W3 写入 ${w3.count} 行：`,
    ...w3.lines,
    "",
    `W4 写入 ${w4.count} 行：`,
    ...w4.lines,
  ].join("\n");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { ...(payload || {}), reportText };
}

function showSaveDualLoadsReport(reportText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const text = String(reportText || "").trim();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!text) {
    window.alert("保存完成");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const popup = window.open("", "_blank", "width=980,height=760");
  if (!popup) {
    window.alert(text);
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const escaped = escapeHtml(text);
  popup.document.open();
  popup.document.write(`
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8" />
      <title>保存双表货量明细</title>
      <style>
        body { margin: 0; padding: 12px; font-family: Consolas, "Courier New", monospace; background: #f5f7fa; color: #102a43; }
        pre { white-space: pre-wrap; word-break: break-word; line-height: 1.35; font-size: 13px; margin: 0; }
      </style>
    </head>
    <body><pre>${escaped}</pre></body>
    </html>
  `);
  popup.document.close();
}

function normalizeStoreLoadDetails(store = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    rpcs: Math.max(0, Number(store.rpcs || 0) || 0),
    rcase: Math.max(0, Number(store.rcase || 0) || 0),
    bpcs: Math.max(0, Number(store.bpcs || 0) || 0),
    bpaper: Math.max(0, Number(store.bpaper || 0) || 0),
    apcs: Math.max(0, Number(store.apcs || 0) || 0),
    apaper: Math.max(0, Number(store.apaper || 0) || 0),
    rpaper: Math.max(0, Number(store.rpaper || 0) || 0),
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function normalizeStoreWaveLoads(store = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const details = normalizeStoreLoadDetails(store);
  const tripCount = Math.max(1, Number(store.tripCount || 1) || 1);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const belongs = parseWaveBelongs(store.waveBelongs || "");
  const onlyWave2 = belongs.length === 1 && belongs[0] === "2";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const hasWave3 = belongs.includes("3");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const hasWave4 = belongs.includes("4");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave1GroupLoad = details.rpcs + details.rcase + details.bpcs + details.bpaper + details.rpaper;
  const wave2GroupLoad = details.apcs + details.apaper;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const rawTotal = wave1GroupLoad + wave2GroupLoad;
  let wave1TotalLoadBase = Math.max(0, Number(store.wave1TotalLoadBase || 0) || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let wave2TotalLoadBase = Math.max(0, Number(store.wave2TotalLoadBase || 0) || 0);
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave1Load = onlyWave2 ? 0 : (tripCount === 2 ? wave1GroupLoad : (wave1GroupLoad + wave2GroupLoad));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave2Load = onlyWave2 ? rawTotal : (tripCount === 2 ? wave2GroupLoad : 0);
  const wave1TotalLoad = onlyWave2 ? 0 : (tripCount === 2 ? wave1TotalLoadBase : (wave1TotalLoadBase + wave2TotalLoadBase));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave2TotalLoad = onlyWave2 ? (wave1TotalLoadBase + wave2TotalLoadBase) : (tripCount === 2 ? wave2TotalLoadBase : 0);
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  const baseW3Load = rawTotal;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseW4Load = rawTotal;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const baseW3TotalLoad = wave1TotalLoadBase + wave2TotalLoadBase;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseW4TotalLoad = wave1TotalLoadBase + wave2TotalLoadBase;
  const wave3Load = hasWave3 ? baseW3Load : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave4Load = hasWave4 ? baseW4Load : 0;
  const wave3TotalLoad = hasWave3 ? baseW3TotalLoad : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave4TotalLoad = hasWave4 ? baseW4TotalLoad : 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const totalLoad = wave1TotalLoad + wave2TotalLoad;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const total = wave1Load + wave2Load;
  return {
    ...details,
    wave1Load,
    wave2Load,
    wave3Load,
    wave4Load,
    boxes: total,
    wave1TotalLoadBase,
    wave2TotalLoadBase,
    wave1TotalLoad,
    wave2TotalLoad,
    wave3TotalLoad,
    wave4TotalLoad,
    totalLoad,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function syncStoreWaveLoads(store = {}) {
  const next = normalizeStoreWaveLoads(store);
  store.rpcs = next.rpcs;
  store.rcase = next.rcase;
  store.bpcs = next.bpcs;
  store.bpaper = next.bpaper;
  store.apcs = next.apcs;
  store.apaper = next.apaper;
  store.rpaper = next.rpaper;
  store.wave1Load = next.wave1Load;
  store.wave2Load = next.wave2Load;
  store.wave3Load = next.wave3Load;
  store.wave4Load = next.wave4Load;
  store.boxes = next.boxes;
  store.wave1TotalLoadBase = next.wave1TotalLoadBase;
  store.wave2TotalLoadBase = next.wave2TotalLoadBase;
  store.wave1TotalLoad = next.wave1TotalLoad;
  store.wave2TotalLoad = next.wave2TotalLoad;
  store.wave3TotalLoad = next.wave3TotalLoad;
  store.wave4TotalLoad = next.wave4TotalLoad;
  store.totalLoad = next.totalLoad;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return store;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function applyWmsCargoToStores(stores, cargoMap) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!Array.isArray(stores)) return [];
  const map = cargoMap instanceof Map ? cargoMap : new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return stores.map((store) => {
    const key = normalizeStoreCode(store?.id || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const cargo = map.get(key);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!cargo) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return syncStoreWaveLoads({
        ...store,
        rpcs: 0,
        rcase: 0,
        bpcs: 0,
        bpaper: 0,
        apcs: 0,
        apaper: 0,
        rpaper: 0,
        wave1TotalLoadBase: 0,
        wave2TotalLoadBase: 0,
        totalLoad: 0,
      });
    }
    return syncStoreWaveLoads({
      ...store,
      totalLoad: Number(cargo.totalLoad || 0),
      wave1TotalLoadBase: Number(cargo.wave1TotalLoadBase || 0),
      wave2TotalLoadBase: Number(cargo.wave2TotalLoadBase || 0),
      rpcs: Number(cargo.rpcs || 0),
      rcase: Number(cargo.rcase || 0),
      bpcs: Number(cargo.bpcs || 0),
      bpaper: Number(cargo.bpaper || 0),
      apcs: Number(cargo.apcs || 0),
      apaper: Number(cargo.apaper || 0),
      rpaper: Number(cargo.rpaper || 0),
    });
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function enrichStores(stores) {
  return stores.map((s) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const merged = { ...s };
    syncStoreWaveLoads(merged);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      ...merged,
      lng: Number(merged.lng || 0),
      lat: Number(merged.lat || 0),
      rpcs: Number(merged.rpcs || 0),
      rcase: Number(merged.rcase || 0),
      bpcs: Number(merged.bpcs || 0),
      bpaper: Number(merged.bpaper || 0),
      apcs: Number(merged.apcs || 0),
      apaper: Number(merged.apaper || 0),
      rpaper: Number(merged.rpaper || 0),
      wave1Load: Number(merged.wave1Load || 0),
      wave2Load: Number(merged.wave2Load || 0),
      wave3Load: Number(merged.wave3Load || 0),
      wave4Load: Number(merged.wave4Load || 0),
      wave1TotalLoad: Number(merged.wave1TotalLoad || 0),
      wave2TotalLoad: Number(merged.wave2TotalLoad || 0),
      wave3TotalLoad: Number(merged.wave3TotalLoad || 0),
      wave4TotalLoad: Number(merged.wave4TotalLoad || 0),
      totalLoad: Number(merged.totalLoad || 0),
      boxes: Number(merged.boxes || 0),
      tripCount: Math.max(1, Number(merged.tripCount || 1)),
      waveBelongs: normalizeWaveBelongsInput(merged.waveBelongs || ""),
      coldRatio: Number(merged.coldRatio || 0),
      difficulty: Number(merged.difficulty || 1),
      parking: Number(merged.parking || 10),
      desiredArrival: merged.desiredArrival || "",
      waveArrivals: {
        ...(merged.waveArrivals || {}),
        w1: merged.waveArrivals?.w1 || "",
      },
      serviceMinutes: Number(merged.serviceMinutes ?? merged.windowEnd ?? 15),
      actualServiceMinutes: Number(merged.serviceMinutes ?? merged.windowEnd ?? 15) * Math.max(0.1, Number(merged.difficulty || 1)),
    };
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getStoreTimingForWave(store, wave, dispatchStartMin) {
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let desiredArrival = "";
  if (waveId === "W1" || waveId === "1" || waveId === "FIRST" || waveId.includes("W1")) {
    desiredArrival = String(store.waveArrivals?.w1 || "").trim();
  } else if (waveId === "W2" || waveId === "2" || waveId === "SECOND" || waveId.includes("W2")) {
    desiredArrival = String(store.waveArrivals?.w2 || "").trim();
  } else if (waveId === "W3" || waveId === "3" || waveId === "THIRD" || waveId.includes("W3")) {
    desiredArrival = String(store.waveArrivals?.w3 || "").trim();
  } else if (waveId === "W4" || waveId === "4" || waveId === "FOURTH" || waveId.includes("W4")) {
    desiredArrival = String(store.waveArrivals?.w4 || "").trim();
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!desiredArrival) {
    throw new Error(`missing_desired_arrival:${store?.id || ""}:${waveId || "UNKNOWN"}`);
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const desiredArrivalMin = alignMinuteToDispatch(toMinutes(desiredArrival), dispatchStartMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const allowedLateMinutes = Number(store.allowedLateMinutes ?? store.parking ?? 10);
  return {
    desiredArrival,
    desiredArrivalMin,
    allowedLateMinutes,
    latestAllowedArrivalMin: desiredArrivalMin + allowedLateMinutes,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function isStoreInWave(store, wave) {
  const waveNo = String(wave?.waveId || "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const belongs = parseWaveBelongs(store?.waveBelongs || "");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!belongs.length) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveNo.includes("W1") || waveNo === "1" || waveNo === "FIRST") return belongs.includes(1);
  if (waveNo.includes("W2") || waveNo === "2" || waveNo === "SECOND") return belongs.includes(2);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (waveNo.includes("W3") || waveNo === "3" || waveNo === "THIRD") return belongs.includes(3);
  if (waveNo.includes("W4") || waveNo === "4" || waveNo === "FOURTH") return belongs.includes(4);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return false;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function isSecondDeliveryWave(wave = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
  return waveId === "W2" || waveId === "2" || waveId === "SECOND" || waveId.includes("W2");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getVehicleSolveCapacity(vehicle = {}) {
  const cap = Number(vehicle?.solveCapacity || 1);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return cap > 0 ? cap : 1;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getStoreSolveLoadForWave(store = {}, wave = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
  const wave1 = Math.max(0, Number(store?.wave1TotalLoad || 0) || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave2 = Math.max(0, Number(store?.wave2TotalLoad || 0) || 0);
  const wave3 = Math.max(0, Number(store?.wave3TotalLoad || 0) || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave4 = Math.max(0, Number(store?.wave4TotalLoad || 0) || 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (waveId === "W4" || waveId === "4" || waveId === "FOURTH" || waveId.includes("W4")) return wave4;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveId === "W3" || waveId === "3" || waveId === "THIRD" || waveId.includes("W3")) return wave3;
  return isSecondDeliveryWave(wave) ? wave2 : wave1;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getStoreWaveLoadByWaveId(store = {}, waveId = "") {
  const normalized = String(waveId || "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (normalized === "W1" || normalized === "1" || normalized === "FIRST") {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return Math.max(0, Number(store?.resolvedWave1Load ?? 0) || 0);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (normalized === "W2" || normalized === "2" || normalized === "SECOND") {
    return Math.max(0, Number(store?.resolvedWave2Load ?? 0) || 0);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (normalized === "W3" || normalized === "3" || normalized === "THIRD") {
    return Math.max(0, Number(store?.resolvedWave3Load ?? 0) || 0);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (normalized === "W4" || normalized === "4" || normalized === "FOURTH") {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return Math.max(0, Number(store?.resolvedWave4Load ?? 0) || 0);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return 0;
}

function isStoreCandidateForWaveRule(store = {}, waveId = "") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveText = String(waveId || "").trim().toUpperCase();
  const belongs = parseWaveBelongs(store?.waveBelongs || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const belongsWave =
    (waveText === "W1" && belongs.includes("1"))
    || (waveText === "W2" && belongs.includes("2"))
    || (waveText === "W3" && belongs.includes("3"))
    || (waveText === "W4" && belongs.includes("4"));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!belongsWave) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return getStoreWaveLoadByWaveId(store, waveText) > 0;
}

function calcLoadConvertTerm(details, field) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const quantity = Math.max(0, Number(details?.[field] || 0) || 0);
  const capacity = Math.max(0, Number(LOAD_CONVERT_CAPACITY_MAP[field] || 0) || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const value = capacity > 0 ? (quantity / capacity) : 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { field, quantity, capacity, value };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildStoreLoadConvertPreview(store = {}) {
  const details = normalizeStoreLoadDetails(store);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const belongs = parseWaveBelongs(store.waveBelongs || "");
  const onlyWave2 = belongs.length === 1 && belongs[0] === "2";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const hasWave3 = belongs.includes("3");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const hasWave4 = belongs.includes("4");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tripCount = Math.max(1, Number(store.tripCount || 1) || 1);
  const wave1Terms = LOAD_CONVERT_WAVE1_FIELDS.map((field) => calcLoadConvertTerm(details, field));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave2Terms = LOAD_CONVERT_WAVE2_FIELDS.map((field) => calcLoadConvertTerm(details, field));
  const wave1Base = wave1Terms.reduce((sum, term) => sum + term.value, 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave2Base = wave2Terms.reduce((sum, term) => sum + term.value, 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave34Base = wave1Base + wave2Base;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave1Total = onlyWave2 ? 0 : (tripCount === 2 ? wave1Base : (wave1Base + wave2Base));
  const wave2Total = onlyWave2 ? (wave1Base + wave2Base) : (tripCount === 2 ? wave2Base : 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave3Total = hasWave3 ? wave34Base : 0;
  const wave4Total = hasWave4 ? wave34Base : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    storeId: normalizeStoreCode(store.id || ""),
    storeName: String(store.name || ""),
    tripCount,
    onlyWave2,
    wave1Base,
    wave2Base,
    wave1Total,
    wave2Total,
    wave3Total,
    wave4Total,
    wave1Terms,
    wave2Terms,
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildLoadConvertPreview(stores = []) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const items = (Array.isArray(stores) ? stores : []).map((store, index) => ({
    index,
    ...buildStoreLoadConvertPreview(store),
  }));
  const wave1Sum = items.reduce((sum, item) => sum + Number(item.wave1Total || 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave2Sum = items.reduce((sum, item) => sum + Number(item.wave2Total || 0), 0);
  const wave3Sum = items.reduce((sum, item) => sum + Number(item.wave3Total || 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave4Sum = items.reduce((sum, item) => sum + Number(item.wave4Total || 0), 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { items, wave1Sum, wave2Sum, wave3Sum, wave4Sum };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function roundLoadConvertValue(value, digits = 2) {
  const n = Number(value || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(digits));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function formatLoadConvertValue(value) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return roundLoadConvertValue(value, 2);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderLoadConvertModal(preview) {
  const summary = document.getElementById("loadConvertSummary");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const table = document.getElementById("loadConvertTable");
  if (!summary || !table) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave1Sum = formatLoadConvertValue(preview?.wave1Sum || 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const wave2Sum = formatLoadConvertValue(preview?.wave2Sum || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave3Sum = formatLoadConvertValue(preview?.wave3Sum || 0);
  const wave4Sum = formatLoadConvertValue(preview?.wave4Sum || 0);
  summary.innerHTML = `
    <span class="chip">一波次折算合计：${wave1Sum}</span>
    <span class="chip">二波次折算合计：${wave2Sum}</span>
    <span class="chip">三波次折算合计：${wave3Sum}</span>
    <span class="chip">四波次折算合计：${wave4Sum}</span>
    <span class="chip">门店数：${(preview?.items || []).length}</span>
  `;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const rows = (preview?.items || []).map((item) => {
    const wave1Expr = item.onlyWave2
      ? "仅2波次店：一波次=0"
      : (item.wave1Terms.map((term) => `${term.field}:${term.quantity}/${term.capacity || 0}=${formatLoadConvertValue(term.value)}`).join(" + ") || "0");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave2ExprTerms = item.onlyWave2 ? [...item.wave1Terms, ...item.wave2Terms] : item.wave2Terms;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const wave2Expr = wave2ExprTerms.map((term) => `${term.field}:${term.quantity}/${term.capacity || 0}=${formatLoadConvertValue(term.value)}`).join(" + ") || "0";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `
      <tr>
        <td>${escapeHtml(item.storeId)}</td>
        <td>${escapeHtml(item.storeName || "-")}</td>
        <td>${item.tripCount}</td>
        <td>${formatLoadConvertValue(item.wave1Total)}</td>
        <td title="${escapeHtml(wave1Expr)}">${escapeHtml(wave1Expr)}</td>
        <td>${formatLoadConvertValue(item.wave2Total)}</td>
        <td title="${escapeHtml(wave2Expr)}">${escapeHtml(wave2Expr)}</td>
        <td>${formatLoadConvertValue(item.wave3Total)}</td>
        <td>${formatLoadConvertValue(item.wave4Total)}</td>
      </tr>
    `;
  }).join("");
  table.innerHTML = `
    <thead>
      <tr>
        <th>店铺编号</th>
        <th>店铺名称</th>
        <th>次数</th>
        <th>一波次折算</th>
        <th>一波次计算过程</th>
        <th>二波次折算</th>
        <th>二波次计算过程</th>
        <th>三波次折算</th>
        <th>四波次折算</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  `;
}

function applyLoadConvertPreviewToStores(preview) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!preview || !Array.isArray(preview.items)) return;
  preview.items.forEach((item) => {
    const store = state.stores[item.index];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!store) return;
    store.wave1TotalLoadBase = roundLoadConvertValue(item.wave1Base || 0, 2);
    store.wave2TotalLoadBase = roundLoadConvertValue(item.wave2Base || 0, 2);
    syncStoreWaveLoads(store);
    store.wave1TotalLoad = roundLoadConvertValue(store.wave1TotalLoad || 0, 2);
    store.wave2TotalLoad = roundLoadConvertValue(store.wave2TotalLoad || 0, 2);
    store.wave3TotalLoad = roundLoadConvertValue(store.wave3TotalLoad || 0, 2);
    store.wave4TotalLoad = roundLoadConvertValue(store.wave4TotalLoad || 0, 2);
    store.totalLoad = roundLoadConvertValue(store.totalLoad || 0, 2);
  });
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function applyStoreWaveResolvedLoadsToStores(stores, resolvedMap) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!Array.isArray(stores)) return [];
  const map = resolvedMap instanceof Map ? resolvedMap : new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return stores.map((store) => {
    const key = normalizeStoreCode(store?.id || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const resolved = map.get(key);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!resolved) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const next = syncStoreWaveLoads({
        ...store,
        wave1TotalLoadBase: 0,
        wave2TotalLoadBase: 0,
        wave3TotalLoadBase: 0,
        wave4TotalLoadBase: 0,
        wave3Load: 0,
        wave4Load: 0,
        wave3TotalLoad: 0,
        wave4TotalLoad: 0,
        totalLoad: 0,
      });
      next.resolvedWave1Load = 0;
      next.resolvedWave2Load = 0;
      next.resolvedWave3Load = 0;
      next.resolvedWave4Load = 0;
      next.resolvedTotalLoad = 0;
      return next;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const next = syncStoreWaveLoads({
      ...store,
      waveBelongs: String(resolved.wave_belongs || "").trim(),
      wave1TotalLoadBase: Number(resolved.wave1_load || 0),
      wave2TotalLoadBase: Number(resolved.wave2_load || 0),
      wave3TotalLoadBase: Number(resolved.wave3_load || 0),
      wave4TotalLoadBase: Number(resolved.wave4_load || 0),
      totalLoad: Number(resolved.total_resolved_load || 0),
    });
    next.wave1Load = Number(resolved.wave1_load || 0);
    next.wave2Load = Number(resolved.wave2_load || 0);
    next.wave3Load = Number(resolved.wave3_load || 0);
    next.wave4Load = Number(resolved.wave4_load || 0);
    next.wave1TotalLoad = Number(resolved.wave1_load || 0);
    next.wave2TotalLoad = Number(resolved.wave2_load || 0);
    next.wave3TotalLoad = Number(resolved.wave3_load || 0);
    next.wave4TotalLoad = Number(resolved.wave4_load || 0);
    next.totalLoad = Number(resolved.total_resolved_load || 0);
    next.boxes = Number(resolved.total_resolved_load || 0);
    next.resolvedWave1Load = Number(resolved.wave1_load || 0);
    next.resolvedWave2Load = Number(resolved.wave2_load || 0);
    next.resolvedWave3Load = Number(resolved.wave3_load || 0);
    next.resolvedWave4Load = Number(resolved.wave4_load || 0);
    next.resolvedTotalLoad = Number(resolved.total_resolved_load || 0);
    const belongs = parseWaveBelongs(next.waveBelongs || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const arrivals = {};
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (belongs.includes("1")) arrivals.w1 = String(resolved.first_wave_time || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (belongs.includes("2")) arrivals.w2 = String(resolved.second_wave_time || "").trim();
    if (belongs.includes("3")) arrivals.w3 = String(resolved.arrival_time_w3 || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (belongs.includes("4")) arrivals.w4 = String(resolved.arrival_time_w4 || "").trim();
    next.waveArrivals = arrivals;
    return next;
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function openLoadConvertModal() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const modal = document.getElementById("loadConvertModal");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!modal) return;
  loadConvertPreviewCache = buildLoadConvertPreview(state.stores);
  renderLoadConvertModal(loadConvertPreviewCache);
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

function closeLoadConvertModal() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const modal = document.getElementById("loadConvertModal");
  if (!modal) return;
  applyLoadConvertPreviewToStores(loadConvertPreviewCache);
  loadConvertPreviewCache = null;
  renderStoresTable();
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
let pendingSolveContinuation = null;

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function describeSolveField(value, { required = false } = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const text = value === null || value === undefined || String(value).trim() === "" ? "缺失" : String(value).trim();
  const ok = text !== "缺失";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    ok,
    text,
    label: ok ? "已取得" : (required ? "缺失（会卡住求解）" : "缺失"),
  };
}

async function buildSolveDiagnoseReport() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stores = Array.isArray(state.stores) ? state.stores : [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const vehicles = Array.isArray(state.vehicles) ? state.vehicles : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waves = Array.isArray(state.waves) ? state.waves : [];
  const dispatchStartMin = toMinutes(state.settings.dispatchStartTime || "19:10");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const resolvedPayload = await fetchStoreWaveResolvedList({ limit: 5000 });
  const resolvedItems = Array.isArray(resolvedPayload?.items) ? resolvedPayload.items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const resolvedByShop = new Map();
  resolvedItems.forEach((row) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const key = normalizeStoreCode(row?.shop_code || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!key) return;
    resolvedByShop.set(key, row);
  });
  const scenarioFields = [
    ["dispatchStartMin", describeSolveField(dispatchStartMin, { required: true })],
    ["maxRouteKm", describeSolveField(state.settings.maxRouteKm, { required: true })],
    ["concentrateLate", describeSolveField(state.settings.concentrateLate)],
    ["dist", { ok: false, text: "未在弹窗阶段构建；求解时 buildScenario 才会生成", label: "未直接取得" }],
    ["storeMap", { ok: stores.length > 0, text: `当前门店数 ${stores.length}`, label: stores.length > 0 ? "已取得" : "缺失（无门店）" }],
    ["vehicles", { ok: vehicles.length > 0, text: `当前车辆数 ${vehicles.length}`, label: vehicles.length > 0 ? "已取得" : "缺失（无车辆）" }],
  ];

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeRows = stores.map((store) => {
    const id = String(store?.id || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const row = resolvedByShop.get(normalizeStoreCode(id)) || {};
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const belongsText = String(row?.wave_belongs || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const belongs = parseWaveBelongs(belongsText);
    const wave1 = String(row?.first_wave_time || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const wave2 = String(row?.second_wave_time || "").trim();
    const wave3 = String(row?.arrival_time_w3 || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave4 = String(row?.arrival_time_w4 || "").trim();
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const waveTime = { 1: wave1, 2: wave2, 3: wave3, 4: wave4 };
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const requiredWaves = belongs.length ? belongs : [1];
    const timingIssues = requiredWaves.filter((waveNo) => !String(waveTime[waveNo] || "").trim()).map((waveNo) => `W${waveNo}缺`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const nonRequiredPresent = [1, 2, 3, 4]
      .filter((waveNo) => !requiredWaves.includes(waveNo))
      .filter((waveNo) => String(waveTime[waveNo] || "").trim())
      .map((waveNo) => `W${waveNo}有值但未参与`);
    return {
      id,
      name: String(store?.name || "").trim(),
      boxes: Number(row?.total_resolved_load || 0),
      waveBelongs: belongsText,
      desiredArrival: requiredWaves.map((waveNo) => `W${waveNo}=${String(waveTime[waveNo] || "").trim() || "缺"}`).join(" / "),
      latestAllowedArrivalMin: Number.isFinite(Number(store?.parking ?? store?.allowedLateMinutes)) ? Number(store?.parking ?? store?.allowedLateMinutes) : 0,
      actualServiceMinutes: Number(store?.serviceMinutes ?? 0),
      serviceMinutes: Number(store?.serviceMinutes || 0),
      coldRatio: Number(store?.coldRatio || 0),
      difficulty: Number(store?.difficulty || 0),
      parking: Number(store?.parking || 0),
      lng: Number(store?.lng || 0),
      lat: Number(store?.lat || 0),
      requiredWaves: requiredWaves.map((waveNo) => `W${waveNo}`).join("、"),
      directStatus: (!resolvedByShop.has(normalizeStoreCode(id)))
        ? "当前门店在 store_wave_load_resolved 中无记录"
        : (timingIssues.length ? `当前求解相关波次缺少 ${timingIssues.join("，")}` : "当前求解相关波次都已取得"),
      fallbackStatus: nonRequiredPresent.length ? `其他波次出现数值会污染业务：${nonRequiredPresent.join("，")}` : "其他波次为空正常",
    };
  });

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleRows = vehicles.map((vehicle) => ({
    plateNo: String(vehicle?.plateNo || "").trim(),
    driverName: String(vehicle?.driverName || "").trim(),
    type: String(vehicle?.type || "").trim(),
    capacity: Number(vehicle?.capacity || 0),
    speed: Number(vehicle?.speed || 0),
    canCarryCold: Boolean(vehicle?.canCarryCold),
    priorRegularDistance: Number(vehicle?.priorRegularDistance || 0),
    priorWaveCount: Number(vehicle?.priorWaveCount || 0),
    earliestDepartureMin: Number(vehicle?.earliestDepartureMin || 0),
    routes: Array.isArray(vehicle?.routes) ? vehicle.routes.length : 0,
  }));

  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveRows = waves.map((wave) => ({
    waveId: String(wave?.waveId || "").trim(),
    startMin: Number(wave?.startMin || 0),
    endMin: Number(wave?.endMin || 0),
    endMode: String(wave?.endMode || "").trim(),
    relaxEnd: Boolean(wave?.relaxEnd),
    singleWave: Boolean(wave?.singleWave),
    isNightWave: Boolean(wave?.isNightWave),
    earliestDepartureMin: Number(wave?.earliestDepartureMin || 0),
    storeCount: parseStoreIds(wave?.storeIds || "").length,
  }));

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const selectedAlgorithms = (state.settings.solveStrategy || "manual") === "free"
    ? getEffectiveFreeAlgorithms()
    : strategyPreset(state.settings.solveStrategy || "manual", state.settings.optimizeGoal || "balanced").algorithms;

  return {
    summary: {
      strategy: currentStrategyLabel(),
      goal: currentGoalLabel(),
      algorithms: selectedAlgorithms.map((key) => algoLabel(key)).join("、"),
      storeCount: stores.length,
      vehicleCount: vehicles.length,
      waveCount: waves.length,
    },
    scenarioFields,
    storeRows,
    vehicleRows,
    waveRows,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function renderSolveDiagnoseModal(report) {
  const summary = document.getElementById("solveDiagnoseSummary");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const body = document.getElementById("solveDiagnoseBody");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!summary || !body) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const summaryParts = [
    `当前策略：${escapeHtml(report?.summary?.strategy || "-")}`,
    `目标：${escapeHtml(report?.summary?.goal || "-")}`,
    `算法：${escapeHtml(report?.summary?.algorithms || "-")}`,
    `门店 ${Number(report?.summary?.storeCount || 0)} 家`,
    `车辆 ${Number(report?.summary?.vehicleCount || 0)} 台`,
    `波次 ${Number(report?.summary?.waveCount || 0)} 个`,
  ];
  summary.innerHTML = summaryParts.map((text) => `<span class="chip">${text}</span>`).join("");

  const scenarioRows = (report?.scenarioFields || []).map(([key, item]) => `
    <tr>
      <td>${escapeHtml(key)}</td>
      <td>${escapeHtml(item.label || "")}</td>
      <td>${escapeHtml(item.text || "")}</td>
    </tr>
  `).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeRows = (report?.storeRows || []).map((row) => `
    <tr>
      <td>${escapeHtml(row.id || "")}</td>
      <td>${escapeHtml(row.name || "")}</td>
      <td>${escapeHtml(row.waveBelongs || "")}</td>
      <td>${escapeHtml(row.requiredWaves || "")}</td>
      <td>${escapeHtml(String(row.boxes ?? 0))}</td>
      <td>${escapeHtml(String(row.directStatus || ""))}</td>
      <td>${escapeHtml(String(row.fallbackStatus || ""))}</td>
      <td>${escapeHtml(String(row.desiredArrival || ""))}</td>
      <td>${escapeHtml(String(row.actualServiceMinutes ?? 0))}</td>
      <td>${escapeHtml(String(row.serviceMinutes ?? 0))}</td>
      <td>${escapeHtml(String(row.parking ?? 0))}</td>
    </tr>
  `).join("");
  const vehicleRows = (report?.vehicleRows || []).map((row) => `
    <tr>
      <td>${escapeHtml(row.plateNo || "")}</td>
      <td>${escapeHtml(row.driverName || "")}</td>
      <td>${escapeHtml(row.type || "")}</td>
      <td>${escapeHtml(String(row.capacity ?? 0))}</td>
      <td>${escapeHtml(String(row.speed ?? 0))}</td>
      <td>${escapeHtml(row.canCarryCold ? "是" : "否")}</td>
      <td>${escapeHtml(String(row.priorRegularDistance ?? 0))}</td>
      <td>${escapeHtml(String(row.priorWaveCount ?? 0))}</td>
      <td>${escapeHtml(String(row.earliestDepartureMin ?? 0))}</td>
      <td>${escapeHtml(String(row.routes ?? 0))}</td>
    </tr>
  `).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveRows = (report?.waveRows || []).map((row) => `
    <tr>
      <td>${escapeHtml(row.waveId || "")}</td>
      <td>${escapeHtml(String(row.startMin ?? 0))}</td>
      <td>${escapeHtml(String(row.endMin ?? 0))}</td>
      <td>${escapeHtml(row.endMode || "")}</td>
      <td>${escapeHtml(row.relaxEnd ? "是" : "否")}</td>
      <td>${escapeHtml(row.singleWave ? "是" : "否")}</td>
      <td>${escapeHtml(row.isNightWave ? "是" : "否")}</td>
      <td>${escapeHtml(String(row.earliestDepartureMin ?? 0))}</td>
      <td>${escapeHtml(String(row.storeCount ?? 0))}</td>
    </tr>
  `).join("");
  body.innerHTML = `
    <div class="table-wrap" style="color:#111;">
      <table class="load-convert-table">
        <thead><tr><th>字段</th><th>状态</th><th>说明</th></tr></thead>
        <tbody>${scenarioRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">门店字段诊断：只看该门店所属波次的时间。未参与波次为空是正常业务状态，未参与波次如果有值会被视为污染。</div>
      <table class="load-convert-table">
        <thead><tr><th>门店</th><th>名称</th><th>波次归属</th><th>应检查波次</th><th>boxes</th><th>时间状态</th><th>说明</th><th>时间内容</th><th>实际服务</th><th>备用服务</th><th>parking</th></tr></thead>
        <tbody>${storeRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">车辆字段诊断：capacity / speed / 可否冷链 / 历史里程 / 历史波次 / routes 是否能直接取得。</div>
      <table class="load-convert-table">
        <thead><tr><th>车牌</th><th>司机</th><th>车型</th><th>capacity</th><th>speed</th><th>冷链</th><th>历史里程</th><th>历史波次</th><th>最早出车</th><th>routes长度</th></tr></thead>
        <tbody>${vehicleRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">波次字段诊断：看看当前波次是否完整、是否是夜波/单波次、以及店铺数量是否对得上。</div>
      <table class="load-convert-table">
        <thead><tr><th>波次</th><th>开始</th><th>结束</th><th>结束口径</th><th>放宽结束</th><th>单波次</th><th>夜波</th><th>最早出车</th><th>门店数</th></tr></thead>
        <tbody>${waveRows}</tbody>
      </table>
    </div>
  `;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function openSolveDiagnoseModal(report) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const modal = document.getElementById("solveDiagnoseModal");
  if (!modal) return;
  renderSolveDiagnoseModal(report);
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function closeSolveDiagnoseModal() {
  const modal = document.getElementById("solveDiagnoseModal");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function createEmptyMatrix(nodeIds) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const matrix = {};
  nodeIds.forEach((fromId) => {
    matrix[fromId] = {};
    nodeIds.forEach((toId) => {
      matrix[fromId][toId] = fromId === toId ? 0 : null;
    });
  });
  return matrix;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function estimateStraightDurationMinutes(distanceKm, speedKmh = 38) {
  return (Number(distanceKm || 0) / Math.max(1, Number(speedKmh || 38))) * 60;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildStraightDistanceData(stores) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const points = new Map([[DC.id, DC], ...stores.map((s) => [s.id, s])]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nodes = [DC.id, ...stores.map((s) => s.id)];
  const dist = createEmptyMatrix(nodes);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const duration = createEmptyMatrix(nodes);
  for (const a of nodes) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const b of nodes) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (a === b) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const p1 = points.get(a);
      const p2 = points.get(b);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const km = Math.sqrt(((p1.lng - p2.lng) * 92) ** 2 + ((p1.lat - p2.lat) * 111) ** 2);
      const distanceKm = Number(Math.max(1, km).toFixed(1));
      dist[a][b] = distanceKm;
      duration[a][b] = estimateStraightDurationMinutes(distanceKm);
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { dist, duration, source: "straight" };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildDistanceTable(stores) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return buildStraightDistanceData(stores).dist;
}

function loadAmapDistanceCache() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    return JSON.parse(localStorage.getItem(AMAP_DISTANCE_CACHE_KEY) || "{}");
  } catch {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {};
  }
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function saveAmapDistanceCache(cache) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    localStorage.setItem(AMAP_DISTANCE_CACHE_KEY, JSON.stringify(cache));
    scheduleAmapCacheSync();
  } catch {
    // EN: Business note for nearby logic.
    // CN: 附近逻辑的业务提示。
  }
}

function loadAmapRouteCache() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    return JSON.parse(localStorage.getItem(AMAP_ROUTE_CACHE_KEY) || "{}");
  } catch {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {};
  }
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function saveAmapRouteCache(cache) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    localStorage.setItem(AMAP_ROUTE_CACHE_KEY, JSON.stringify(cache));
    scheduleAmapCacheSync();
  } catch {
    // EN: Business note for nearby logic.
    // CN: 附近逻辑的业务提示。
  }
}

function scheduleAmapCacheSync(delayMs = 1800) {
  amapCacheSyncPending = true;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (amapCacheSyncTimer) return;
  amapCacheSyncTimer = setTimeout(() => {
    amapCacheSyncTimer = null;
    void flushAmapCacheToBackend();
  }, delayMs);
}

function getAmapCacheKey(fromNode, toNode) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `${fromNode.lng},${fromNode.lat}->${toNode.lng},${toNode.lat}`;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function parsePolylinePoints(polyline) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return String(polyline || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.split(",").map(Number))
    .filter((pair) => pair.length === 2 && Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
}

function serializePolylinePoints(points) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return points.map((pair) => `${Number(pair[0]).toFixed(6)},${Number(pair[1]).toFixed(6)}`).join(";");
}

function simplifyPolylinePoints(points, maxPoints = 90) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (points.length <= maxPoints) return points;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const step = Math.ceil(points.length / maxPoints);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const simplified = points.filter((_, index) => index % step === 0);
  const last = points[points.length - 1];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!simplified.length || simplified[simplified.length - 1][0] !== last[0] || simplified[simplified.length - 1][1] !== last[1]) simplified.push(last);
  return simplified;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function fetchAmapDrivingPolyline(originNode, destinationNode) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const origin = `${originNode.lng},${originNode.lat}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const destination = `${destinationNode.lng},${destinationNode.lat}`;
  const url = `https://restapi.amap.com/v3/direction/driving?key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}&origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(destination)}&extensions=all&strategy=0`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const response = await fetch(url);
  if (!response.ok) throw new Error(`AMap route HTTP ${response.status}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const payload = await response.json();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (String(payload.status) !== "1" || !Array.isArray(payload.route?.paths) || !payload.route.paths.length) {
    throw new Error(payload.info || "AMap driving route failed");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const steps = payload.route.paths[0]?.steps || [];
  const points = steps.flatMap((step) => parsePolylinePoints(step.polyline));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!points.length) return [[originNode.lng, originNode.lat], [destinationNode.lng, destinationNode.lat]];
  return points;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function fetchAmapDistanceBatch(originNodes, destinationNode) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!originNodes.length || !AMAP_WEB_SERVICE_KEY) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const origins = originNodes.map((node) => `${node.lng},${node.lat}`).join("|");
  const destination = `${destinationNode.lng},${destinationNode.lat}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const url = `https://restapi.amap.com/v3/distance?key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}&origins=${encodeURIComponent(origins)}&destination=${encodeURIComponent(destination)}&type=1`;
  const response = await fetch(url);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) throw new Error(`AMap distance HTTP ${response.status}`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (String(payload.status) !== "1" || !Array.isArray(payload.results)) {
    throw new Error(payload.info || "AMap distance failed");
  }
  return payload.results.map((item) => ({
    distanceKm: Number(item.distance || 0) / 1000,
    durationMinutes: Number(item.duration || 0) / 60,
  }));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function buildDistanceData(stores) {
  const DISTANCE_PROGRESS_VERBOSE = false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const emit = (text) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!DISTANCE_PROGRESS_VERBOSE) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!text) return;
    reportRelayStageProgress(String(text));
  };
  const emitChunkedPairs = (title, pairs, chunkSize = 8) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!DISTANCE_PROGRESS_VERBOSE) return;
    const list = Array.isArray(pairs) ? pairs : [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!list.length) {
      emit(`${title}：0 条。`);
      return;
    }
    emit(`${title}：${list.length} 条。`);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (let i = 0; i < list.length; i += chunkSize) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const chunk = list.slice(i, i + chunkSize);
      emit(`${title}明细 ${i + 1}-${Math.min(i + chunkSize, list.length)}：${chunk.join(" | ")}`);
    }
  };
  if (USE_FULL_DISTANCE_MATRIX_FROM_BACKEND) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    try {
      const storeIds = (stores || []).map((item) => String(item?.id || "").trim()).filter(Boolean);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (storeIds.length > 0) {
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const params = new URLSearchParams({
          storeIds: storeIds.join(","),
          includeDuration: "true",
          strict: "false",
        });
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 5000);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        try {
          const response = await fetch(`${GA_BACKEND_URL}/distance-matrix/full?${params.toString()}`, {
            method: "GET",
            signal: controller.signal,
          });
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          if (response.ok) {
            // EN: Control point for business behavior.
            // CN: 影响业务行为的控制节点。
            const data = await response.json();
            // EN: Key step in this business flow.
            // CN: 当前业务流程中的关键步骤。
            const missingCount = Number(data?.missingCount || 0);
            if (data?.ok && missingCount === 0 && data?.dist && data?.duration) {
              // EN: Key step in this business flow.
              // CN: 当前业务流程中的关键步骤。
              // EN: Control point for business behavior.
              // CN: 影响业务行为的控制节点。
              const dominantSource = String(data?.dbDominantSource || "");
              const sourceCounts = data?.dbSourceCounts || {};
              console.log(`[buildDistanceData] matrix source=database-full nodeCount=${Number(data?.nodeCount || 0)} dbReadMs=${Number(data?.dbReadMs || 0)} dbDominantSource=${dominantSource}`);
              reportRelayStageProgress(`距离矩阵来源确认：database-full，主数据源=${dominantSource || "unknown"}，missing=0，node=${Number(data?.nodeCount || 0)}，dbReadMs=${Number(data?.dbReadMs || 0)}。`);
              // EN: Key step in this business flow.
              // CN: 当前业务流程中的关键步骤。
              return {
                dist: data.dist,
                duration: data.duration,
                source: "database-full",
                cacheHitPairs: Number(data?.pairCount || 0),
                fetchedPairs: 0,
                distDbStats: {
                  fullMatrix: true,
                  missingCount: 0,
                  nodeCount: Number(data?.nodeCount || 0),
                  pairCount: Number(data?.pairCount || 0),
                  dbReadMs: Number(data?.dbReadMs || 0),
                  updatedAt: String(data?.updatedAt || ""),
                  dbDominantSource: dominantSource,
                  dbSourceCounts: sourceCounts,
                },
              };
            }
            throw new Error(`distance matrix incomplete, missing=${missingCount}`);                                                                                           
          } else {
            throw new Error(`distance matrix http=${response.status}`);                                                 
          }
        } finally {
          clearTimeout(timer);
        }
      }
    } catch (error) {
      throw new Error(`distance matrix required: ${error?.message || error}`);                                  
    }
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const straight = buildStraightDistanceData(stores);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nodes = [DC, ...stores];
  const nodeIds = nodes.map((node) => node.id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const totalPairCount = Math.max(0, nodes.length * (nodes.length - 1));
  emit(`距离矩阵构建：节点 ${nodes.length}（库房+门店），目标有向边 ${totalPairCount} 条。`);
  if (!AMAP_WEB_SERVICE_KEY || typeof fetch !== "function") {
    emit("距离矩阵构建：高德能力不可用，直接回退直线距离。");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { ...straight, source: "straight-fallback", cacheHitPairs: 0, fetchedPairs: 0, distDbStats: { fullMatrix: false, missingCount: -1 } };
  }

  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const dist = createEmptyMatrix(nodeIds);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const duration = createEmptyMatrix(nodeIds);
  const cache = loadAmapDistanceCache();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let cacheHitPairs = 0;
  let fetchedPairs = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const cacheDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;

  nodes.forEach((fromNode) => {
    nodes.forEach((toNode) => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (fromNode.id === toNode.id) return;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const cached = cache[getAmapCacheKey(fromNode, toNode)];
      if (!cached) return;
      dist[fromNode.id][toNode.id] = Number(cached.distanceKm || 0);
      duration[fromNode.id][toNode.id] = Number(cached.durationMinutes || 0);
      cacheHitPairs += 1;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (DISTANCE_PROGRESS_VERBOSE) {
        cacheDetails.push(`${fromNode.id}->${toNode.id} ${Number(cached.distanceKm || 0).toFixed(2)}km/${Number(cached.durationMinutes || 0).toFixed(1)}m`);
      }
    });
  });
  emitChunkedPairs("距离缓存命中", cacheDetails || [], 6);

  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const destinationNode of nodes) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const pendingOrigins = nodes.filter((originNode) => originNode.id !== destinationNode.id && !(dist[originNode.id]?.[destinationNode.id] > 0));
      emit(`距离拉取：目标点 ${destinationNode.id}，待拉取起点 ${pendingOrigins.length}。`);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      for (let start = 0; start < pendingOrigins.length; start += AMAP_ORIGIN_BATCH_SIZE) {
        const batch = pendingOrigins.slice(start, start + AMAP_ORIGIN_BATCH_SIZE);
        emit(`距离拉取：目标点 ${destinationNode.id}，批次 ${Math.floor(start / AMAP_ORIGIN_BATCH_SIZE) + 1}/${Math.max(1, Math.ceil(pendingOrigins.length / AMAP_ORIGIN_BATCH_SIZE))}，起点 ${batch.map((x) => x.id).join("、")}。`);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const results = await fetchAmapDistanceBatch(batch, destinationNode);
        const fetchedDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;
        batch.forEach((originNode, index) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const result = results[index];
          // EN: Control point for business behavior.
          // CN: 影响业务行为的控制节点。
          if (!result) return;
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const distanceKm = Number(result.distanceKm || 0);
          const durationMinutes = Number(result.durationMinutes || 0);
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          // EN: Control point for business behavior.
          // CN: 影响业务行为的控制节点。
          if (!(distanceKm > 0) || !(durationMinutes > 0)) return;
          dist[originNode.id][destinationNode.id] = distanceKm;
          duration[originNode.id][destinationNode.id] = durationMinutes;
          cache[getAmapCacheKey(originNode, destinationNode)] = { distanceKm, durationMinutes };
          fetchedPairs += 1;
          if (DISTANCE_PROGRESS_VERBOSE) {
            fetchedDetails.push(`${originNode.id}->${destinationNode.id} ${distanceKm.toFixed(2)}km/${durationMinutes.toFixed(1)}m`);
          }
        });
        emitChunkedPairs(`距离拉取结果（目标${destinationNode.id}）`, fetchedDetails || [], 6);
      }
    }
    saveAmapDistanceCache(cache);
  } catch (error) {
    console.warn("AMap distance fetch failed, fallback to straight-line data.", error);
    emit(`距离拉取异常：${error?.message || error}，将自动补齐直线距离。`);
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const fallbackDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;
  nodes.forEach((fromNode) => {
    nodes.forEach((toNode) => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (fromNode.id === toNode.id) return;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!(dist[fromNode.id][toNode.id] > 0)) {
        dist[fromNode.id][toNode.id] = straight.dist[fromNode.id][toNode.id];
        duration[fromNode.id][toNode.id] = straight.duration[fromNode.id][toNode.id];
        if (DISTANCE_PROGRESS_VERBOSE) {
          fallbackDetails.push(`${fromNode.id}->${toNode.id} ${Number(straight.dist[fromNode.id][toNode.id] || 0).toFixed(2)}km/${Number(straight.duration[fromNode.id][toNode.id] || 0).toFixed(1)}m`);
        }
        return;
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!(duration[fromNode.id][toNode.id] > 0)) duration[fromNode.id][toNode.id] = straight.duration[fromNode.id][toNode.id];
    });
  });
  emitChunkedPairs("距离直线补齐", fallbackDetails || [], 6);
  const fallbackCount = DISTANCE_PROGRESS_VERBOSE ? fallbackDetails.length : Math.max(0, totalPairCount - cacheHitPairs - fetchedPairs);
  emit(`距离矩阵完成：缓存命中 ${cacheHitPairs}，新拉取 ${fetchedPairs}，直线补齐 ${fallbackCount}，最终总边 ${totalPairCount}。`);

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    dist,
    duration,
    source: fetchedPairs > 0 || cacheHitPairs > 0 ? "amap" : "straight-fallback",
    cacheHitPairs,
    fetchedPairs,
    distDbStats: { fullMatrix: false, missingCount: -1 },
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getTravelMinutes(scenario, fromId, toId, speedKmh = 38) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const durationMinutes = Number(scenario?.duration?.[fromId]?.[toId] || 0);
  if (durationMinutes > 0) return durationMinutes;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const distanceKm = Number(scenario?.dist?.[fromId]?.[toId] || 0);
  return estimateStraightDurationMinutes(distanceKm, speedKmh);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getTravelMinutesSolverConsistent(scenario, fromId, toId, speedKmh = 38) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const distanceKm = Number(scenario?.dist?.[fromId]?.[toId] || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return distanceKm / Math.max(Number(speedKmh || 0), 1) * 60;
}

function routeNodeDisplay(id, scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (id === DC.id) return L("depot");
  const store = scenario?.storeMap?.get(id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return store ? `${store.id} ${store.name}` : String(id || "");
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildRouteDisplay(routeIds = [], scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return [DC.id, ...routeIds, DC.id].map((id) => routeNodeDisplay(id, scenario)).join(" → ");
}

function shortenMapName(name, limit = 14) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const text = String(name || "");
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function computeMapOverlayMarkers(markers = [], polyline = []) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const allPoints = [...polyline, ...markers.map((item) => [Number(item.lng), Number(item.lat)])]
    .filter((point) => Array.isArray(point) && point.length === 2 && Number.isFinite(point[0]) && Number.isFinite(point[1]));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!allPoints.length) return markers.map((item) => ({ ...item, xPercent: 50, yPercent: 50 }));
  const lngs = allPoints.map((point) => point[0]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const lats = allPoints.map((point) => point[1]);
  const minLng = Math.min(...lngs);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxLng = Math.max(...lngs);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const minLat = Math.min(...lats);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxLat = Math.max(...lats);
  const lngSpan = Math.max(0.00001, maxLng - minLng);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const latSpan = Math.max(0.00001, maxLat - minLat);
  const padX = 9;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const padY = 10;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return markers.map((item) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const ratioX = (Number(item.lng) - minLng) / lngSpan;
    const ratioY = (Number(item.lat) - minLat) / latSpan;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      ...item,
      xPercent: padX + ratioX * (100 - padX * 2),
      yPercent: 100 - (padY + ratioY * (100 - padY * 2)),
    };
  });
}

async function getTripRouteMapData(result, wave, plan, trip) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const routeCache = loadAmapRouteCache();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const nodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId) || { id: stop.storeId, name: stop.storeName, lng: 0, lat: 0 }), DC]
    .filter((node) => Number.isFinite(Number(node.lng)) && Number.isFinite(Number(node.lat)));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const combinedPoints = [];
  const legs = [];

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let index = 0; index < nodes.length - 1; index += 1) {
    const fromNode = nodes[index];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const toNode = nodes[index + 1];
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const cacheKey = getAmapCacheKey(fromNode, toNode);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let points = parsePolylinePoints(routeCache[cacheKey]?.polyline || "");
    let source = routeCache[cacheKey]?.source || "cache";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!points.length) {
      try {
        points = await fetchAmapDrivingPolyline(fromNode, toNode);
        routeCache[cacheKey] = { polyline: serializePolylinePoints(points), source: "amap" };
        saveAmapRouteCache(routeCache);
        source = "amap";
      } catch (error) {
        console.warn("AMap driving polyline fetch failed, fallback to straight leg.", error);
        points = [[Number(fromNode.lng), Number(fromNode.lat)], [Number(toNode.lng), Number(toNode.lat)]];
        routeCache[cacheKey] = { polyline: serializePolylinePoints(points), source: "fallback" };
        saveAmapRouteCache(routeCache);
        source = "fallback";
      }
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const normalizedPoints = points.filter((pair) => pair.length === 2);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (combinedPoints.length && normalizedPoints.length) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const [firstLng, firstLat] = normalizedPoints[0];
      const [lastLng, lastLat] = combinedPoints[combinedPoints.length - 1];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (Math.abs(firstLng - lastLng) < 1e-6 && Math.abs(firstLat - lastLat) < 1e-6) normalizedPoints.shift();
    }
    combinedPoints.push(...normalizedPoints);
    legs.push({
      fromName: fromNode.id === DC.id ? L("depot") : fromNode.name,
      toName: toNode.id === DC.id ? L("depot") : toNode.name,
      source,
    });
  }

  const simplified = simplifyPolylinePoints(combinedPoints, 120);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const markerNodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId)).filter(Boolean)];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const markers = computeMapOverlayMarkers(markerNodes.map((node, index) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const label = index === 0 ? "D" : String(index);
    const fullName = node.id === DC.id ? L("depot") : `${node.id} ${node.name}`;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      label,
      lng: Number(node.lng),
      lat: Number(node.lat),
      name: fullName,
      shortName: node.id === DC.id ? L("depot") : shortenMapName(node.name || node.id, 12),
    };
  }), simplified);

  const depotParam = `${DC.lng},${DC.lat}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stopParam = markers.slice(1).map((item) => `${item.lng},${item.lat}`).join(";");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const pathParam = serializePolylinePoints(simplified);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const markerSegments = [`mid,0x3366FF,D:${depotParam}`];
  if (stopParam) markerSegments.push(`small,0xD97706,:${stopParam}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const staticMapUrl = `https://restapi.amap.com/v3/staticmap?size=1100*640&scale=2&traffic=1&markers=${encodeURIComponent(markerSegments.join("|"))}&paths=8,0x2563EB,0.95,,0:${encodeURIComponent(pathParam)}&key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}`;

  return {
    title: `${plan.vehicle.plateNo} · ${wave.waveId} · ${L("tripNo")}${trip.tripNo}${L("tripSuffix")}`,
    staticMapUrl,
    markers,
    legs,
    pointCount: simplified.length,
    polylinePoints: simplified,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getSingleWaveStoreIds(stores, dist, thresholdKm) {
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return [];
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function validateInput() {
  if (!state.vehicles.length) return L("noVehicles");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!state.stores.length) return L("noStores");
  if (state.settings.ignoreWaves) return "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!state.waves.length) return L("noWaves");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const stores = enrichStores(state.stores);
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const regularIds = new Set(stores.map((store) => store.id));
  const covered = new Set();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const wave of state.waves) {
    parseStoreIds(wave.storeIds).forEach((id) => {
      if (regularIds.has(id)) covered.add(id);
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const missing = [...regularIds].filter((id) => !covered.has(id));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (missing.length) return LT("regularMissing", { count: missing.length, names: `${missing.slice(0, 8).join(",")}${missing.length > 8 ? "..." : ""}` });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return "";
}

async function buildScenario() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const stores = enrichStores(state.stores);
  reportRelayStageProgress(`场景构建：开始整理门店主数据，共 ${stores.length} 家。`);
  const dispatchStartMin = toMinutes(state.settings.dispatchStartTime || "19:10");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const normalizedStores = stores.map((store) => ({
      ...store,
      desiredArrivalMin: alignMinuteToDispatch(toMinutes(store.waveArrivals?.w1 || store.desiredArrival), dispatchStartMin),
      allowedLateMinutes: Number(store.parking || 10),
      latestAllowedArrivalMin: alignMinuteToDispatch(toMinutes(store.waveArrivals?.w1 || store.desiredArrival), dispatchStartMin) + Number(store.parking || 10),
      actualServiceMinutes: Number(store.actualServiceMinutes || (Number(store.serviceMinutes || 15) * Math.max(0.1, Number(store.difficulty || 1)))),
    }));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeRows = normalizedStores.map((store) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const desired = String(store.waveArrivals?.w1 || store.desiredArrival || "");
    return `${store.id}[波次:${store.waveBelongs || "-"} 货量:${Number(store.totalResolvedLoad || 0).toFixed(6)} W1:${Number(store.resolvedWave1Load || 0).toFixed(6)} W2:${Number(store.resolvedWave2Load || 0).toFixed(6)} W3:${Number(store.resolvedWave3Load || 0).toFixed(6)} W4:${Number(store.resolvedWave4Load || 0).toFixed(6)} 期望:${desired || "--:--"} 允许偏差:${Number(store.parking || 0)} 卸货:${Number(store.actualServiceMinutes || 0)}]`;
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let i = 0; i < storeRows.length; i += 15) {
    const chunk = storeRows.slice(i, i + 15);
    reportRelayStageProgress(`场景构建：门店明细 ${i + 1}-${Math.min(i + 15, storeRows.length)}：${chunk.join(" | ")}`);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const distanceData = await buildDistanceData(normalizedStores);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const dist = distanceData.dist;
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const singleWaveThreshold = Number(state.settings.singleWaveDistanceKm || 70);
  const singleWaveStoreIds = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const singleWaveIdSet = new Set();
  const regularStoreIds = normalizedStores.map((store) => store.id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waves = state.settings.ignoreWaves
    ? [{
      waveId: "ALL",
        start: state.settings.dispatchStartTime || "19:10",
      end: formatTime(dispatchStartMin + 1440),
      startMin: dispatchStartMin,
      endMin: dispatchStartMin + 1440,
      endMode: "service",
      relaxEnd: true,
      storeList: normalizedStores.map((s) => s.id),
    }]
    : (() => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const normalWaves = state.waves.map((w) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const span = buildWaveSpan(w.start, w.end);
        let { startMin, endMin } = span;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        while (endMin < dispatchStartMin) {
          startMin += 1440;
          endMin += 1440;
        }
        const waveId = String(w.waveId || "").trim().toUpperCase();
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const isNightWave = waveId === "W1" || waveId === "W2";
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        return {
          ...w,
          waveId: w.waveId,
          endMode: w.endMode || "return",
          startMin,
          endMin,
          relaxEnd: false,
          singleWave: false,
          isNightWave,
          earliestDepartureMin: waveId === "W2" ? 23 * 60 : startMin,
          nightGroup: isNightWave ? "NIGHT" : "",
          // EN: Business note for nearby logic.
          // CN: 附近逻辑的业务提示。
          storeList: parseStoreIds(w.storeIds),
        };
      }).filter((wave) => wave.storeList.length);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return normalWaves;
    })();
  reportRelayStageProgress(`场景构建：波次共 ${waves.length} 个。`);
  waves.forEach((wave) => {
    reportRelayStageProgress(`场景构建：${wave.waveId} ${wave.start || "--:--"}-${wave.end || "--:--"}，门店 ${Array.isArray(wave.storeList) ? wave.storeList.length : 0} 家，门店清单：${(wave.storeList || []).join("、") || "-"}`);
  });
  reportRelayStageProgress(`场景构建：车辆共 ${state.vehicles.length} 台。`);
  state.vehicles.forEach((v) => {
    reportRelayStageProgress(`场景构建：车辆 ${v.plateNo || "-"} 类型=${ENFORCED_VEHICLE_TYPE} 容量=${Number(v.capacity || 0)} 速度=${Number(v.speed || 0)} 冷链=${Boolean(v.canCarryCold)}`);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const backendStrategyConfig = buildBackendStrategyConfig(state.strategyConfig);
  return {
    vehicles: state.vehicles.map((v) => ({
      ...v,
      type: ENFORCED_VEHICLE_TYPE,
      vehicle_type: ENFORCED_VEHICLE_TYPE,
      nominalCapacity: Number(v.capacity || 0),
      capacity: 1,
      solveCapacity: 1,
      speed: Number(backendStrategyConfig.defaultSpeedKmh || 38),
      canCarryCold: Boolean(v.canCarryCold),
    })),
    stores: normalizedStores,
    storeMap: new Map(normalizedStores.map((s) => [s.id, s])),
    waves,
    dist,
    duration: distanceData.duration,
    distanceSource: distanceData.source,
    distanceCacheHitPairs: distanceData.cacheHitPairs,
    distanceFetchedPairs: distanceData.fetchedPairs,
    distDbStats: distanceData.distDbStats || { fullMatrix: false, missingCount: -1 },
    strategyConfigResolved: backendStrategyConfig,
    maxRouteKm: Number(state.settings.maxRouteKm || 220),
    dispatchStartMin,
    concentrateLate: Boolean(state.settings.concentrateLate),
    singleWaveThreshold,
    singleWaveStoreIds,
    regularStoreIds,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function createVehiclePlan(vehicle, waveId, startMin, scenario, priorStats = {}) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const earliestDepartureMin = Number(priorStats.earliestDepartureMin || startMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    vehicle,
    waveId,
    trips: [],
    availableTime: Math.max(startMin, earliestDepartureMin, scenario?.dispatchStartMin ?? startMin),
    totalDistance: 0,
    totalLoad: 0,
    tripCount: 0,
    feasible: true,
    priorRegularDistance: Number(priorStats.priorRegularDistance || 0),
    priorWaveCount: Number(priorStats.priorWaveCount || 0),
    earliestDepartureMin,
  };
}

function flattenPlanStoreIds(plan) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return plan.trips.flatMap((trip) => trip.stops.map((stop) => stop.storeId));
}

function getUsageMap(metrics) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return new Map((metrics?.usageList || []).map((item) => [item.plateNo, item]));
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getUsedVehicleSet() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const used = new Set();
  state.lastResults.forEach((result) => {
    result.solution.flat().forEach((plan) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (plan.trips?.length) used.add(plan.vehicle.plateNo);
    });
  });
  return used;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function normalizeStoreKey(id) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const raw = String(id ?? "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!raw) return "";
  if (/^\d+$/.test(raw)) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (raw.length === 5) return `3${raw}`;
    if (raw.length === 6 && raw.startsWith("3")) return raw;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return raw;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildStoreKeyVariants(id) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const raw = String(id ?? "").trim();
  const normalized = normalizeStoreKey(raw);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const variants = new Set([raw, normalized]);
  if (/^\d{6}$/.test(normalized) && normalized.startsWith("3")) {
    variants.add(normalized.slice(1));
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return [...variants].filter(Boolean);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildStoreWaveAssignmentKey(storeId, waveId) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const sid = normalizeStoreKey(storeId);
  const wid = String(waveId || "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!sid || !wid) return "";
  return `${sid}|${wid}`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildStoreAssignmentMapFromSolution(solution = []) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = new Map();
  (solution || []).forEach((wavePlans) => {
    (wavePlans || []).forEach((plan) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const plateNo = plan?.vehicle?.plateNo || "";
      const waveId = plan?.waveId || "";
      (plan?.trips || []).forEach((trip) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const stopIds = [];
        (trip?.stops || []).forEach((stop) => {
          if (stop?.storeId != null) stopIds.push(stop.storeId);
        });
        (trip?.route || []).forEach((storeId) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          if (storeId != null) stopIds.push(storeId);
        });
        stopIds.forEach((storeId) => {
          buildStoreKeyVariants(storeId).forEach((storeKey) => {
            // EN: Control point for business behavior.
            // CN: 影响业务行为的控制节点。
            const compositeKey = buildStoreWaveAssignmentKey(storeKey, waveId);
            // EN: Key step in this business flow.
            // CN: 当前业务流程中的关键步骤。
            if (compositeKey) {
              map.set(compositeKey, {
                plateNo,
                waveId,
                tripNo: trip?.tripNo || 1,
              });
            }
            // EN: Business note for nearby logic.
            // CN: 附近逻辑的业务提示。
            map.set(storeKey, {
              plateNo,
              waveId,
              tripNo: trip?.tripNo || 1,
            });
          });
        });
      });
    });
  });
  return map;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getActiveStoreAssignmentMap() {
  const result = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (result?.storeAssignmentMap instanceof Map && result.storeAssignmentMap.size) return result.storeAssignmentMap;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (result?.solution?.length) {
    result.storeAssignmentMap = buildStoreAssignmentMapFromSolution(result.solution);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return result.storeAssignmentMap;
  }
  const fallback = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  state.lastResults.forEach((oneResult) => {
    const oneMap = oneResult?.storeAssignmentMap instanceof Map && oneResult.storeAssignmentMap.size
      ? oneResult.storeAssignmentMap
      : buildStoreAssignmentMapFromSolution(oneResult?.solution || []);
    oneMap.forEach((value, key) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!fallback.has(key)) fallback.set(key, value);
    });
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return fallback;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getStoreAssignmentByRule(store = {}, assignmentMap = new Map()) {
  const variants = buildStoreKeyVariants(store?.id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveOrder = ["W1", "W2", "W3", "W4"];
  for (const waveId of waveOrder) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!isStoreCandidateForWaveRule(store, waveId)) continue;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (const variant of variants) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const key = buildStoreWaveAssignmentKey(variant, waveId);
      if (!key) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const hit = assignmentMap.get(key);
      if (hit && String(hit.plateNo || "").trim()) return hit;
    }
  }
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const variant of variants) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const hit = assignmentMap.get(variant);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (hit && String(hit.plateNo || "").trim()) return hit;
  }
  return null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getSortConfig(kind) {
  if (kind === "vehicle") return { fieldKey: "vehicleSortField", dirKey: "vehicleSortDir" };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (kind === "wave") return { fieldKey: "waveSortField", dirKey: "waveSortDir" };
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { fieldKey: "storeSortField", dirKey: "storeSortDir" };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function toggleDataTableSort(kind, field) {
  const cfg = getSortConfig(kind);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (state.ui[cfg.fieldKey] === field) {
    state.ui[cfg.dirKey] = state.ui[cfg.dirKey] === "asc" ? "desc" : "asc";
  } else {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ui[cfg.fieldKey] = field;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    state.ui[cfg.dirKey] = "asc";
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (kind === "vehicle") renderVehicles();
  else if (kind === "wave") renderWaves();
  else renderStoresTable();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildSortMark(kind, field) {
  const cfg = getSortConfig(kind);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (state.ui[cfg.fieldKey] !== field) return "";
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return state.ui[cfg.dirKey] === "asc" ? " ▲" : " ▼";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildDataTableHtml({ tableKind = "store", columns = [], rows = [], tableClass = "" } = {}) {
  const colgroup = columns.map((column) => `<col style="width:${column.width || 120}px;">`).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const thead = columns.map((column) => {
    if (column.sortable && column.sortField) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (column.headerHtml) {
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        return `<th><button class="data-table-sort data-table-sort-rich" data-table-kind="${tableKind}" data-table-sort="${column.sortField}">${column.headerHtml}<span class="data-table-sort-mark">${buildSortMark(tableKind, column.sortField).trim()}</span></button></th>`;
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return `<th><button class="data-table-sort" data-table-kind="${tableKind}" data-table-sort="${column.sortField}">${escapeHtml(String(column.label || ""))}${buildSortMark(tableKind, column.sortField)}</button></th>`;
    }
    if (column.headerHtml) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      return `<th>${column.headerHtml}</th>`;
    }
    return `<th>${escapeHtml(String(column.label || ""))}</th>`;
  }).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const body = rows.length ? rows.join("") : `<tr><td colspan="${columns.length}" class="muted">${L("noChartData")}</td></tr>`;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return `<table class="data-table ${tableClass}"><colgroup>${colgroup}</colgroup><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getWaveStoreNameList(storeIdsText) {
  const ids = parseStoreIds(storeIdsText);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeById = new Map(state.stores.map((store) => [normalizeStoreCode(store.id), store]));
  return ids.map((id) => storeById.get(normalizeStoreCode(id))?.name || id);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderWaveStoreNameTags(storeIdsText, previewCount = 3) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const names = getWaveStoreNameList(storeIdsText);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!names.length) return `<span class="muted">-</span>`;
  const preview = names.slice(0, previewCount);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const remain = Math.max(0, names.length - preview.length);
  const title = escapeHtml(names.join(" / "));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const chips = preview.map((name) => `<span class="wave-store-chip">${escapeHtml(name)}</span>`).join("");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const suffix = remain > 0 ? `<span class="wave-store-more">+${remain}</span>` : "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `<div class="wave-store-chip-wrap" title="${title}">${chips}${suffix}</div>`;
}

function createStop(store, timing, legDistance, arrival, start, leave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const lateMinutes = Math.max(0, arrival - timing.desiredArrivalMin);
  const overToleranceMinutes = Math.max(0, arrival - timing.latestAllowedArrivalMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
      storeId: store.id,
      storeName: store.name,
      address: store.address,
      arrival,
      start,
      leave,
      onTime: arrival <= timing.latestAllowedArrivalMin,
      lateMinutes,
      overToleranceMinutes,
      desiredArrival: timing.desiredArrival,
      serviceMinutes: store.actualServiceMinutes,
      allowedLateMinutes: timing.allowedLateMinutes,
    distance: legDistance,
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function summarizePlan(plan) {
  plan.totalDistance = plan.trips.reduce((sum, trip) => sum + trip.totalDistance, 0);
  plan.totalLoad = plan.trips.reduce((sum, trip) => sum + trip.loadBoxes, 0);
  plan.tripCount = plan.trips.length;
  plan.availableTime = plan.trips.length ? plan.trips[plan.trips.length - 1].finish : plan.availableTime;
  plan.feasible = true;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return plan;
}

function createEmptyTrip(plan) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    tripNo: plan.trips.length + 1,
    route: [],
    currentNode: DC.id,
    currentTime: plan.availableTime,
    loadBoxes: 0,
    totalDistance: 0,
    loadRate: 0,
    onTimeCount: 0,
    finish: plan.availableTime,
    waveLateMinutes: 0,
    waveLateType: "return",
    stops: [],
  };
}

function getSolveRelayMaxKm(scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const v = Number(scenario?.strategyConfigResolved?.w1w2RelayMaxKm ?? scenario?.strategyConfig?.w1w2RelayMaxKm ?? state?.strategyConfig?.w1w2RelayMaxKm ?? 240);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return Number.isFinite(v) && v > 0 ? v : 240;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getSolveW3OneWayMaxKm(scenario) {
  const v = Number(scenario?.strategyConfigResolved?.w3OneWayMaxKm ?? scenario?.strategyConfig?.w3OneWayMaxKm ?? state?.strategyConfig?.w3OneWayMaxKm ?? 260);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return Number.isFinite(v) && v > 0 ? v : 260;
}

function isW3WaveForSolve(wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return String(wave?.waveId || "").trim().toUpperCase() === "W3";
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildTripFromRoute(route, vehicle, scenario, wave, startTime, tripNo, options = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentNode = DC.id;
  let currentTime = startTime;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let loadBoxes = 0;
  let totalDistance = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let outboundDistance = 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let onTimeCount = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let lateStoreCount = 0;
  let lateMinutes = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let overToleranceMinutes = 0;
  const w3OneWayMaxKm = getSolveW3OneWayMaxKm(scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const isW3Wave = isW3WaveForSolve(wave);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const stops = [];

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const storeId of route) {
    const store = scenario.storeMap.get(storeId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!store) return null;
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    // EN: Business note for nearby logic.
    // CN: 附近逻辑的业务提示。
    loadBoxes += getStoreSolveLoadForWave(store, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!IGNORE_CAPACITY_CONSTRAINT && loadBoxes > getVehicleSolveCapacity(vehicle)) return null;

    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const legDistance = scenario.dist[currentNode][store.id];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const travelMinutes = legDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
    const arrival = currentTime + travelMinutes;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const start = arrival;
    const leave = start + store.actualServiceMinutes;
    totalDistance += legDistance;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (isW3Wave) {
      outboundDistance += legDistance;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (outboundDistance > w3OneWayMaxKm) return null;
    }

    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const stop = createStop(store, timing, legDistance, arrival, start, leave);
    if (stop.onTime) onTimeCount += 1;
    else lateStoreCount += 1;
    lateMinutes += stop.lateMinutes || 0;
    overToleranceMinutes += stop.overToleranceMinutes || 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!options.allowToleranceBreak && stop.overToleranceMinutes > 0) return null;
    stops.push(stop);
    currentNode = store.id;
    currentTime = leave;
  }

  const backDistance = route.length ? scenario.dist[currentNode][DC.id] : 0;
  totalDistance += backDistance;

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backTravelMinutes = backDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const finish = currentTime + backTravelMinutes;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveLateType = wave.endMode || "return";
  const serviceEnd = stops.length ? stops[stops.length - 1].leave : startTime;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveLateMinutes = waveLateType === "return"
    ? Math.max(0, finish - wave.endMin)
    : Math.max(0, serviceEnd - wave.endMin);
  if (!wave.relaxEnd && waveLateMinutes > 0) return null;

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    tripNo,
    route: [...route],
    currentNode: DC.id,
    currentTime: finish,
    loadBoxes,
    totalDistance,
    loadRate: getVehicleSolveCapacity(vehicle) ? loadBoxes / getVehicleSolveCapacity(vehicle) : 0,
    outboundDistance,
    onTimeCount,
    lateStoreCount,
    lateMinutes,
    overToleranceMinutes,
    finish,
    waveLateMinutes,
    waveLateType,
    hardArrivalBroken: overToleranceMinutes > 0,
    stops,
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function mapTripFailureLabel(code) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const key = String(code || "").trim();
  if (key === "capacity") return "容量";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (key === "arrival_window") return "时间窗";
  if (key === "wave_end") return "波次截止";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (key === "max_route_km" || key === "max_route_km_single" || key === "max_route_km_return" || key === "night_regular_distance") return "里程";
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (key === "store_missing") return "门店缺失";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return "综合";
}

function diagnoseTripBuildFailure(route, vehicle, scenario, wave, startTime, options = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let currentNode = DC.id;
  let currentTime = startTime;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let loadBoxes = 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let totalDistance = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let outboundDistance = 0;
  const w3OneWayMaxKm = getSolveW3OneWayMaxKm(scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const isW3Wave = isW3WaveForSolve(wave);
  for (const storeId of route) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const store = scenario.storeMap.get(storeId);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!store) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (isW3Wave) console.log(`[W3] 路线拒绝: store_missing, storeId=${storeId}`);
      return { code: "store_missing", label: mapTripFailureLabel("store_missing"), storeId };
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    loadBoxes += getStoreSolveLoadForWave(store, wave);
    if (!IGNORE_CAPACITY_CONSTRAINT && loadBoxes > getVehicleSolveCapacity(vehicle)) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (isW3Wave) {
        console.log(`[W3] 路线拒绝: capacity, storeId=${storeId}, loadBoxes=${loadBoxes}, capacity=${getVehicleSolveCapacity(vehicle)}`);
      }
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      return { code: "capacity", label: mapTripFailureLabel("capacity"), storeId };
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const legDistance = scenario.dist[currentNode][store.id];
    const travelMinutes = legDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const arrival = currentTime + travelMinutes;
    const leave = arrival + store.actualServiceMinutes;
    totalDistance += legDistance;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (isW3Wave) {
      outboundDistance += legDistance;
      console.log(`[W3] 检查路线: storeId=${storeId}, legDistance=${legDistance}, outboundDistance累计=${outboundDistance}, limit=${w3OneWayMaxKm}`);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (outboundDistance > w3OneWayMaxKm) {
        console.log(`[W3] 路线拒绝: max_route_km_single, storeId=${storeId}, outboundDistance=${outboundDistance}, limit=${w3OneWayMaxKm}`);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return { code: "max_route_km_single", label: mapTripFailureLabel("max_route_km_single"), storeId };
      }
    }
    const overTolerance = Math.max(0, arrival - timing.latestAllowedArrivalMin);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!options.allowToleranceBreak && overTolerance > 0) {
      if (isW3Wave) {
        console.log(`[W3] 路线拒绝: arrival_window, storeId=${storeId}, arrival=${arrival}, latestAllowed=${timing.latestAllowedArrivalMin}, overTolerance=${overTolerance}`);
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return { code: "arrival_window", label: mapTripFailureLabel("arrival_window"), storeId };
    }
    currentNode = store.id;
    currentTime = leave;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const backDistance = route.length ? scenario.dist[currentNode][DC.id] : 0;
  totalDistance += backDistance;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backTravelMinutes = backDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
  const finish = currentTime + backTravelMinutes;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const serviceEnd = currentTime;
  const waveLateMinutes = (wave.endMode || "return") === "return"
    ? Math.max(0, finish - wave.endMin)
    : Math.max(0, serviceEnd - wave.endMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!wave.relaxEnd && waveLateMinutes > 0) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (isW3Wave) {
      console.log(`[W3] 路线拒绝: wave_end, storeId=${route[route.length - 1]}, finish=${finish}, serviceEnd=${serviceEnd}, endMin=${wave.endMin}, endMode=${wave.endMode || "return"}`);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { code: "wave_end", label: mapTripFailureLabel("wave_end"), storeId: route[route.length - 1] };
  }
  if (isW3Wave) console.log(`[W3] 路线拒绝: unknown, storeId=${route[0]}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { code: "unknown", label: mapTripFailureLabel("unknown"), storeId: route[0] };
}

function rebuildPlanFromRoutesWithReason(vehicle, routes, scenario, wave, priorStats = {}, options = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const plan = createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, priorStats);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const regularMileageCap = getSolveRelayMaxKm(scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let availableTime = plan.availableTime;
  for (const route of routes) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!route.length) continue;
    const trip = buildTripFromRoute(route, vehicle, scenario, wave, availableTime, plan.trips.length + 1, {
      ...options,
      solverConsistentTravel: true,
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!trip) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const failure = diagnoseTripBuildFailure(route, vehicle, scenario, wave, availableTime, options);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return { plan: null, failure };
    }
    plan.trips.push(trip);
    availableTime = trip.finish;
  }
  plan.availableTime = availableTime;
  summarizePlan(plan);
  if (wave.isNightWave && plan.priorRegularDistance + plan.totalDistance > regularMileageCap) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      plan: null,
      failure: { code: "night_regular_distance", label: mapTripFailureLabel("night_regular_distance"), storeId: "" },
    };
  }
  return { plan, failure: null };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function rebuildPlanFromRoutes(vehicle, routes, scenario, wave, priorStats = {}, options = {}) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return rebuildPlanFromRoutesWithReason(vehicle, routes, scenario, wave, priorStats, options).plan;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function computePlanLateness(plan, scenario) {
  return plan.trips.reduce((sum, trip) => sum + trip.stops.reduce((tripSum, stop) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const store = scenario.storeMap.get(stop.storeId);
      const timing = getStoreTimingForWave(store, { waveId: plan.waveId }, scenario.dispatchStartMin);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return tripSum + Math.max(0, stop.arrival - (timing?.latestAllowedArrivalMin || 0));
    }, 0), 0);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function computePlanViolation(plan) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return plan.trips.reduce((sum, trip) => sum + (trip.overToleranceMinutes || 0), 0);
}

function computePlanCostBreakdown(plan, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const vehicleBusyPenalty = wave.singleWave ? 0 : plan.priorRegularDistance * 1.2 + plan.priorWaveCount * 150;
  const latenessMinutes = computePlanLateness(plan, scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const arrivalViolationMinutes = computePlanViolation(plan);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const latenessPenalty = latenessMinutes * 60;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const arrivalViolationPenalty = arrivalViolationMinutes * 20000;
  const waveLateMinutes = plan.trips.reduce((sum, trip) => sum + (trip.waveLateMinutes || 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const waveLatePenalty = waveLateMinutes * 80;
  const extraTripCount = (!wave.singleWave && plan.tripCount > 1) ? (plan.tripCount - 1) : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const extraTripPenalty = extraTripCount * 180;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const distanceCost = plan.totalDistance * 0.45;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const loadBonus = plan.totalLoad * 0.08;
  const lateRouteCount = plan.trips.filter((trip) => (trip.overToleranceMinutes || 0) > 0).length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const lateRoutePenalty = scenario.concentrateLate ? lateRouteCount * 1600 : lateRouteCount * 240;
  const totalCost = arrivalViolationPenalty + latenessPenalty + waveLatePenalty + vehicleBusyPenalty + extraTripPenalty + lateRoutePenalty + distanceCost - loadBonus;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    totalCost,
    latenessMinutes,
    latenessPenalty,
    arrivalViolationMinutes,
    arrivalViolationPenalty,
    waveLateMinutes,
    waveLatePenalty,
    vehicleBusyPenalty,
    extraTripCount,
    extraTripPenalty,
    lateRouteCount,
    lateRoutePenalty,
    distanceCost,
    loadBonus,
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function computePlanCost(plan, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return computePlanCostBreakdown(plan, scenario, wave).totalCost;
}

function computePlansCostBreakdown(plans, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const total = {
    totalCost: 0,
    latenessMinutes: 0,
    latenessPenalty: 0,
    arrivalViolationMinutes: 0,
    arrivalViolationPenalty: 0,
    waveLateMinutes: 0,
    waveLatePenalty: 0,
    vehicleBusyPenalty: 0,
    extraTripCount: 0,
    extraTripPenalty: 0,
    lateRouteCount: 0,
    lateRoutePenalty: 0,
    distanceCost: 0,
    loadBonus: 0,
  };
  (plans || []).forEach((plan) => {
    const item = computePlanCostBreakdown(plan, scenario, wave);
    Object.keys(total).forEach((key) => {
      total[key] += Number(item[key] || 0);
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return total;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function formatWaveCostBreakdown(breakdown) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!breakdown) return "";
  const parts = [
    `里程 ${Number(breakdown.distanceCost || 0).toFixed(1)}`,
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (Number(breakdown.latenessPenalty || 0) > 0) parts.push(`晚到 ${Number(breakdown.latenessPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.arrivalViolationPenalty || 0) > 0) parts.push(`超允许偏差 ${Number(breakdown.arrivalViolationPenalty || 0).toFixed(1)}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (Number(breakdown.waveLatePenalty || 0) > 0) parts.push(`波次超时 ${Number(breakdown.waveLatePenalty || 0).toFixed(1)}`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (Number(breakdown.vehicleBusyPenalty || 0) > 0) parts.push(`车辆续跑 ${Number(breakdown.vehicleBusyPenalty || 0).toFixed(1)}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (Number(breakdown.extraTripPenalty || 0) > 0) parts.push(`多趟次 ${Number(breakdown.extraTripPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.lateRoutePenalty || 0) > 0) parts.push(`晚到线路 ${Number(breakdown.lateRoutePenalty || 0).toFixed(1)}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (Number(breakdown.loadBonus || 0) > 0) parts.push(`装载抵扣 -${Number(breakdown.loadBonus || 0).toFixed(1)}`);
  return parts.join("，");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildTripCandidate(plan, store, scenario, wave, debug = false, options = {}) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const vehicle = plan.vehicle;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!IGNORE_CAPACITY_CONSTRAINT && getStoreSolveLoadForWave(store, wave) > getVehicleSolveCapacity(vehicle)) return null;

  const baseRoutes = plan.trips.map((trip) => [...trip.route]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const candidates = [];

  for (let tripIndex = 0; tripIndex < baseRoutes.length; tripIndex += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const route = baseRoutes[tripIndex];
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (let insertAt = 0; insertAt <= route.length; insertAt += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const nextRoutes = baseRoutes.map((item) => [...item]);
      nextRoutes[tripIndex].splice(insertAt, 0, store.id);
      const nextPlan = rebuildPlanFromRoutes(vehicle, nextRoutes, scenario, wave, {
        priorRegularDistance: plan.priorRegularDistance,
        priorWaveCount: plan.priorWaveCount,
      }, options);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!nextPlan) continue;
      candidates.push({
        nextPlan,
        cost: computePlanCost(nextPlan, scenario, wave),
        debug: {
          mode: "insert",
          tripIndex,
          insertAt,
          routePreview: nextRoutes.map((route) => route.join("->")).join(" | "),
          totalDistance: nextPlan.totalDistance,
          tripCount: nextPlan.tripCount,
        },
      });
    }
  }

  if (!(wave.singleWave && baseRoutes.length)) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextRoutes = baseRoutes.map((item) => [...item]);
    nextRoutes.push([store.id]);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const nextPlan = rebuildPlanFromRoutes(vehicle, nextRoutes, scenario, wave, {
      priorRegularDistance: plan.priorRegularDistance,
      priorWaveCount: plan.priorWaveCount,
    }, options);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (nextPlan) {
      candidates.push({
        nextPlan,
        cost: computePlanCost(nextPlan, scenario, wave),
        debug: {
          mode: "new-trip",
          tripIndex: nextRoutes.length - 1,
          insertAt: 0,
          routePreview: nextRoutes.map((route) => route.join("->")).join(" | "),
          totalDistance: nextPlan.totalDistance,
          tripCount: nextPlan.tripCount,
        },
      });
    }
  }

  if (!candidates.length) return null;
  candidates.sort((a, b) => a.cost - b.cost);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!debug) return candidates[0];
  return {
    ...candidates[0],
    candidates: candidates.map((item) => ({
      ...item.debug,
      cost: item.cost,
      chosenTripNo: item.nextPlan.trips.find((trip) => trip.route.includes(store.id))?.tripNo || 1,
      finish: item.nextPlan.trips[item.nextPlan.trips.length - 1]?.finish || item.nextPlan.availableTime,
      waveLateMinutes: item.nextPlan.trips.reduce((sum, trip) => sum + (trip.waveLateMinutes || 0), 0),
    })),
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildPlanForStoreOrder(vehicle, orderedStores, scenario, wave, priorStats = {}, options = {}) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let plan = createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, priorStats);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const store of orderedStores) {
    const candidate = buildTripCandidate(plan, store, scenario, wave, false, options);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!candidate) return null;
    plan = candidate.nextPlan;
  }
  return plan;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function localizeUnscheduledReason(reason) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const map = {
    arrival: "unscheduledReasonArrival",
    wave: "unscheduledReasonWave",
    mileage: "unscheduledReasonMileage",
    capacity: "unscheduledReasonCapacity",
    slot: "unscheduledReasonSlot",
    mixed: "unscheduledReasonMixed",
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return L(map[reason] || "unscheduledReasonMixed");
}

function getDisplayUnscheduledReason(item) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const reason = String(item?.reason || "").trim().toLowerCase();
  const reasonText = String(item?.reasonText || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const isCapacity = reason === "capacity" || /capacity|容量/.test(reasonText.toLowerCase());
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (IGNORE_CAPACITY_CONSTRAINT && isCapacity) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return localizeUnscheduledReason("slot");
  }
  return reasonText || localizeUnscheduledReason(reason);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function diagnoseStoreVehicleConstraint(store, plan, scenario, wave) {
  const vehicle = plan.vehicle;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!IGNORE_CAPACITY_CONSTRAINT && getStoreSolveLoadForWave(store, wave) > getVehicleSolveCapacity(vehicle)) return "capacity";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const directDistance = scenario.dist[DC.id][store.id];
  const roundDistance = directDistance + scenario.dist[store.id][DC.id];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (isW3WaveForSolve(wave) && directDistance > getSolveW3OneWayMaxKm(scenario)) return "mileage";
  const regularMileageCap = getSolveRelayMaxKm(scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (wave.isNightWave && Number(plan.priorRegularDistance || 0) + roundDistance > regularMileageCap) return "mileage";
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const depart = Math.max(plan.availableTime || wave.startMin, wave.startMin, scenario.dispatchStartMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const arrival = depart + getTravelMinutes(scenario, DC.id, store.id, vehicle.speed);
  const leave = arrival + (store.actualServiceMinutes || store.serviceMinutes || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const finish = leave + getTravelMinutes(scenario, store.id, DC.id, vehicle.speed);
  if (arrival > timing.latestAllowedArrivalMin) return "arrival";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!wave.relaxEnd && (((wave.endMode || "return") === "return" && finish > wave.endMin) || ((wave.endMode || "return") !== "return" && leave > wave.endMin))) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      return "wave";
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return "slot";
}

function diagnoseUnscheduledStore(store, plans, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const stats = { arrival: 0, wave: 0, mileage: 0, capacity: 0, slot: 0 };
  for (const plan of plans) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const strictCandidate = buildTripCandidate(plan, store, scenario, wave, false, { allowToleranceBreak: false });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (strictCandidate) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return { reason: "slot", detail: localizeUnscheduledReason("slot"), stats };
    }
    const relaxedCandidate = buildTripCandidate(plan, store, scenario, wave, false, { allowToleranceBreak: true });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (relaxedCandidate) {
      stats.arrival += 1;
      continue;
    }
    const reason = diagnoseStoreVehicleConstraint(store, plan, scenario, wave);
    stats[reason] += 1;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ranked = Object.entries(stats).sort((a, b) => b[1] - a[1]);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const bestReason = (stats.arrival > 0 ? "arrival" : ranked[0]?.[0]) || "mixed";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { reason: bestReason, detail: localizeUnscheduledReason(bestReason), stats };
}

function formatUnscheduledDetails(unscheduledStores, limit = 8) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return (unscheduledStores || []).slice(0, limit).map((item) => `${item.storeName}（${getDisplayUnscheduledReason(item)}）`).join("、");
}

function summarizeUnscheduledReasons(unscheduledStores) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const buckets = new Map();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const item of unscheduledStores || []) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const key = getDisplayUnscheduledReason(item);
    buckets.set(key, (buckets.get(key) || 0) + 1);
  }
  return [...buckets.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([label, count]) => `${label} × ${count}`)
    .join("；");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function evaluateStoreInsertionChoices(plans, store, scenario, wave, traceMode = false) {
  const attemptModes = scenario.concentrateLate
    ? [{ allowToleranceBreak: false }, { allowToleranceBreak: true }]
    : [{ allowToleranceBreak: false }];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleEvaluations = [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const mode of attemptModes) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const ranked = [];
    for (let i = 0; i < plans.length; i += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const candidate = buildTripCandidate(plans[i], store, scenario, wave, traceMode, mode);
      if (!candidate) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const chosenTripNo = candidate.candidates?.[0]?.chosenTripNo || candidate.nextPlan.trips.find((trip) => trip.route.includes(store.id))?.tripNo || 1;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const item = {
        planIndex: i,
        nextPlan: candidate.nextPlan,
        cost: candidate.cost,
        mode: mode.allowToleranceBreak ? "relaxed" : "strict",
        chosenTripNo,
        plateNo: plans[i].vehicle.plateNo,
        optionCount: candidate.candidates?.length || 1,
        bestPreview: candidate.candidates?.[0]?.routePreview || candidate.nextPlan.trips.map((trip) => trip.route.join("->")).join(" | "),
        candidates: candidate.candidates || [],
      };
      ranked.push(item);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (traceMode) {
        vehicleEvaluations.push({
          plateNo: item.plateNo,
          optionCount: item.optionCount,
          bestCost: item.cost,
          bestPreview: item.bestPreview,
          chosenTripNo: item.chosenTripNo,
          candidates: item.candidates,
          mode: item.mode,
        });
      }
    }
    ranked.sort((a, b) => a.cost - b.cost);
    if (ranked.length) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      return {
        best: ranked[0],
        second: ranked[1] || null,
        vehicleEvaluations,
      };
    }
  }
  return { best: null, second: null, vehicleEvaluations };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function computeRegretPriority(bestChoice, secondChoice, store, wave, scenario) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const regretBase = (secondChoice?.cost ?? (bestChoice.cost + 35)) - bestChoice.cost;
  const urgency = Math.max(0, (scenario.dispatchStartMin + 1440) - timing.latestAllowedArrivalMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const remoteBonus = scenario.dist?.[DC.id]?.[store.id] || 0;
  return regretBase * 1.6 + remoteBonus * 0.05 + urgency * 0.02;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function greedySolve(scenario, seed = 0, traceMode = false) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const solution = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const regularVehicleStats = new Map();
  const traceLog = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const unscheduledStores = [];
  for (const wave of scenario.waves) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const plans = scenario.vehicles.map((vehicle) => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const prior = regularVehicleStats.get(vehicle.plateNo) || {};
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const earliestDepartureMin = wave.isNightWave
        ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(prior.nightAvailableMin || wave.startMin))
        : wave.startMin;
      return createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, {
        ...prior,
        earliestDepartureMin,
      });
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const stores = wave.storeList
      .map((id) => scenario.storeMap.get(id))
      .filter(Boolean)
      .sort((a, b) => {
        const timingA = getStoreTimingForWave(a, wave, scenario.dispatchStartMin);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const timingB = getStoreTimingForWave(b, wave, scenario.dispatchStartMin);
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        return timingA.desiredArrivalMin - timingB.desiredArrivalMin || a.id.localeCompare(b.id);
      });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (seed && stores.length > 1) {
      for (let i = stores.length - 1; i > 0; i -= 1) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const j = Math.floor(((i + 1) * ((seed % 11) + 1)) % stores.length);
        [stores[i], stores[j]] = [stores[j], stores[i]];
      }
    }
    const unrouted = [...stores];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    while (unrouted.length) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      let bestDecision = null;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const infeasible = [];
      for (const store of unrouted) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const { best, second, vehicleEvaluations } = evaluateStoreInsertionChoices(plans, store, scenario, wave, traceMode);
        if (!best) {
          infeasible.push({ store, vehicleEvaluations });
          continue;
        }
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const priority = computeRegretPriority(best, second, store, wave, scenario);
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (!bestDecision || priority > bestDecision.priority || (priority === bestDecision.priority && best.cost < bestDecision.best.cost)) {
          bestDecision = { store, best, priority, vehicleEvaluations };
        }
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!bestDecision) {
        for (const item of infeasible) {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          // EN: Control point for business behavior.
          // CN: 影响业务行为的控制节点。
          const diagnosis = diagnoseUnscheduledStore(item.store, plans, scenario, wave);
          const timing = getStoreTimingForWave(item.store, wave, scenario.dispatchStartMin);
          unscheduledStores.push({
            waveId: wave.waveId,
            storeId: item.store.id,
            storeName: item.store.name,
            desiredArrival: timing.desiredArrival,
            reason: diagnosis.reason,
            reasonText: diagnosis.detail,
          });
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          if (traceMode) {
            traceLog.push({
              algorithmKey: "vrptw",
              waveId: wave.waveId,
              storeId: item.store.id,
              storeName: item.store.name,
              chosenPlate: "",
              chosenTripNo: 0,
              bestCost: null,
              skipped: true,
              reason: diagnosis.reason || "mixed",
              reasonText: diagnosis.detail,
              vehicleEvaluations: item.vehicleEvaluations.sort((a, b) => (a.bestCost ?? Number.POSITIVE_INFINITY) - (b.bestCost ?? Number.POSITIVE_INFINITY)),
            });
          }
        }
        unrouted.length = 0;
        break;
      }
      plans[bestDecision.best.planIndex] = bestDecision.best.nextPlan;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (traceMode) {
        traceLog.push({
          algorithmKey: "vrptw",
          waveId: wave.waveId,
          storeId: bestDecision.store.id,
          storeName: bestDecision.store.name,
          chosenPlate: plans[bestDecision.best.planIndex].vehicle.plateNo,
          chosenTripNo: bestDecision.best.chosenTripNo || 1,
          bestCost: bestDecision.best.cost,
          regretScore: bestDecision.priority,
          mode: bestDecision.best.mode,
          vehicleEvaluations: bestDecision.vehicleEvaluations.sort((a, b) => (a.bestCost ?? Number.POSITIVE_INFINITY) - (b.bestCost ?? Number.POSITIVE_INFINITY)),
        });
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const removeIndex = unrouted.findIndex((item) => item.id === bestDecision.store.id);
      if (removeIndex >= 0) unrouted.splice(removeIndex, 1);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (wave.isNightWave) {
      plans.forEach((plan) => {
        const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (plan.trips.length) {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            priorRegularDistance: prev.priorRegularDistance + plan.totalDistance,
            priorWaveCount: prev.priorWaveCount + 1,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, plan.availableTime),
          });
        } else {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            ...prev,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, prev.nightAvailableMin || wave.startMin),
          });
        }
      });
    }
    solution.push(plans);
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return traceMode ? { solution, traceLog, unscheduledStores } : { solution, unscheduledStores };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function cloneWaveRouteState(plans) {
  return plans.map((plan) => ({
    vehicle: plan.vehicle,
    priorRegularDistance: plan.priorRegularDistance,
    priorWaveCount: plan.priorWaveCount,
    earliestDepartureMin: plan.earliestDepartureMin,
    routes: plan.trips.map((trip) => [...trip.route]),
  }));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function rebuildWavePlansFromState(routeState, scenario, wave) {
  const plans = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const degradedVehicles = [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const stateItem of routeState) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const rebuilt = rebuildPlanFromRoutesWithReason(
      stateItem.vehicle,
      stateItem.routes,
      scenario,
      wave,
      {
        priorRegularDistance: stateItem.priorRegularDistance,
        priorWaveCount: stateItem.priorWaveCount,
        earliestDepartureMin: stateItem.earliestDepartureMin,
      },
    );
    let plan = rebuilt.plan;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!plan) {
      const emptyPlan = rebuildPlanFromRoutesWithReason(
        stateItem.vehicle,
        [],
        scenario,
        wave,
        {
          priorRegularDistance: stateItem.priorRegularDistance,
          priorWaveCount: stateItem.priorWaveCount,
          earliestDepartureMin: stateItem.earliestDepartureMin,
        },
      );
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!emptyPlan.plan) return null;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const plate = String(stateItem?.vehicle?.plateNo || "").trim() || "-";
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const fail = rebuilt.failure || {};
      degradedVehicles.push({
        plate,
        reasonLabel: mapTripFailureLabel(fail.code),
        storeId: String(fail.storeId || "").trim(),
      });
      plan = emptyPlan.plan;
    }
    plans.push(plan);
  }
  if (degradedVehicles.length) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const examples = degradedVehicles.slice(0, 8).map((item) => (
      item.storeId
        ? `${item.plate}[${item.reasonLabel}/${item.storeId}]`
        : `${item.plate}[${item.reasonLabel}]`
    ));
    reportRelayStageProgress(`后端重建降级：${degradedVehicles.length} 台车辆路线不合法，已置空（${examples.join("、")}）`);
  }
  return plans;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function wavePlansCost(plans, scenario, wave) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return plans.reduce((sum, plan) => sum + computePlanCost(plan, scenario, wave), 0);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function approximateRouteDistance(route = [], scenario) {
  if (!route.length) return 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let total = Number(scenario?.dist?.[DC.id]?.[route[0]] || 0);
  for (let i = 0; i < route.length - 1; i += 1) {
    total += Number(scenario?.dist?.[route[i]]?.[route[i + 1]] || 0);
  }
  total += Number(scenario?.dist?.[route[route.length - 1]]?.[DC.id] || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return total;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes = []) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const targets = touchedVehicleIndexes.length
    ? touchedVehicleIndexes.map((index) => routeState[index]).filter(Boolean)
    : routeState;
  return targets.reduce((sum, item) => sum + item.routes.reduce((routeSum, route) => routeSum + approximateRouteDistance(route, scenario), 0), 0);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function normalizeRouteBuckets(routeState) {
  routeState.forEach((item) => {
    item.routes = item.routes.filter((route) => route.length);
  });
}

function createSeededRandom(seed = 42) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let value = seed % 2147483647;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (value <= 0) value += 2147483646;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return () => {
    value = value * 16807 % 2147483647;
    return (value - 1) / 2147483646;
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function hashRouteState(routeState) {
  return routeState
    .map((item) => `${item.vehicle.plateNo}:${item.routes.map((route) => route.join(">")).join("|")}`)
    .join("||");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function pushTraceEvent(traceLog, event, limit = 160) {
  traceLog.push(event);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (traceLog.length > limit) traceLog.splice(0, traceLog.length - limit);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function mergeTraceLogs(baseTraceLog, extraTraceLog, limit = 220) {
  (extraTraceLog || []).forEach((event) => pushTraceEvent(baseTraceLog, event, limit));
  return baseTraceLog;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function summarizeRouteState(routeState) {
  return routeState
    .map((item) => `${item.vehicle.plateNo}[${item.routes.map((route) => route.join("->")).join(" | ") || "-"}]`)
    .join(" / ");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function flattenStoresFromRouteState(routeState) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return routeState.flatMap((item) => item.routes.flatMap((route) => route));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function evaluateRouteState(routeState, scenario, wave) {
  const plans = rebuildWavePlansFromState(routeState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!plans) return null;
  const planCosts = plans.map((plan) => computePlanCost(plan, scenario, wave));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { plans, cost: planCosts.reduce((sum, value) => sum + value, 0), planCosts };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function evaluateRouteStateIncremental(routeState, scenario, wave, baselineEval = null, touchedVehicleIndexes = []) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!baselineEval?.plans?.length || !touchedVehicleIndexes.length) {
    return evaluateRouteState(routeState, scenario, wave);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const plans = baselineEval.plans.map((plan) => clone(plan));
  const planCosts = [...(baselineEval.planCosts || baselineEval.plans.map((plan) => computePlanCost(plan, scenario, wave)))];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const vehicleIndex of [...new Set(touchedVehicleIndexes)].sort((a, b) => a - b)) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const stateItem = routeState[vehicleIndex];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!stateItem) continue;
    const rebuilt = rebuildSingleStatePlan(stateItem, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!rebuilt) return null;
    plans[vehicleIndex] = rebuilt;
    planCosts[vehicleIndex] = computePlanCost(rebuilt, scenario, wave);
  }
  return { plans, cost: planCosts.reduce((sum, value) => sum + value, 0), planCosts };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function rebuildSingleStatePlan(stateItem, scenario, wave) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return rebuildPlanFromRoutes(
    stateItem.vehicle,
    stateItem.routes,
    scenario,
    wave,
    {
      priorRegularDistance: stateItem.priorRegularDistance,
      priorWaveCount: stateItem.priorWaveCount,
      earliestDepartureMin: stateItem.earliestDepartureMin,
    },
  );
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function estimateRouteInsertDelta(route, storeId, scenario) {
  const fromDepot = Number(scenario?.dist?.[DC.id]?.[storeId] || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const toDepot = Number(scenario?.dist?.[storeId]?.[DC.id] || 0);
  if (!route.length) return fromDepot + toDepot;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let best = Number.POSITIVE_INFINITY;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let position = 0; position <= route.length; position += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const prevId = position === 0 ? DC.id : route[position - 1];
    const nextId = position === route.length ? DC.id : route[position];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const delta = Number(scenario?.dist?.[prevId]?.[storeId] || 0)
      + Number(scenario?.dist?.[storeId]?.[nextId] || 0)
      - Number(scenario?.dist?.[prevId]?.[nextId] || 0);
    if (delta < best) best = delta;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return best;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function rankVehicleIndexesForStoreInsertion(routeState, storeId, scenario, limit = 0) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ranked = routeState
    .map((stateItem, vehicleIndex) => {
      const routeDeltas = stateItem.routes.map((route) => estimateRouteInsertDelta(route, storeId, scenario));
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const bestExistingDelta = routeDeltas.length ? Math.min(...routeDeltas) : Number.POSITIVE_INFINITY;
      const newTripDelta = Number(scenario?.dist?.[DC.id]?.[storeId] || 0) + Number(scenario?.dist?.[storeId]?.[DC.id] || 0);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const bestDelta = Math.min(bestExistingDelta, newTripDelta);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      return {
        vehicleIndex,
        score: bestDelta + stateItem.routes.length * 1.5,
      };
    })
    .sort((a, b) => a.score - b.score)
    .map((item) => item.vehicleIndex);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return limit > 0 ? ranked.slice(0, Math.min(limit, ranked.length)) : ranked;
}

function insertStoresIntoRouteState(baseState, storeOrder, scenario, wave, options = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const states = clone(baseState);
  const candidateVehicleLimit = Number(options.candidateVehicleLimit || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const storeId of storeOrder) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const store = scenario.storeMap.get(storeId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!store) continue;
    let bestIndex = -1;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let bestCost = Number.POSITIVE_INFINITY;
    let bestRoutes = null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const primaryIndexes = rankVehicleIndexesForStoreInsertion(states, storeId, scenario, candidateVehicleLimit);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const fallbackIndexes = candidateVehicleLimit > 0
      ? Array.from({ length: states.length }, (_, index) => index).filter((index) => !primaryIndexes.includes(index))
      : [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const candidateIndexes = [...primaryIndexes, ...fallbackIndexes];
    for (const vehicleIndex of candidateIndexes) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const currentPlan = rebuildSingleStatePlan(states[vehicleIndex], scenario, wave);
      if (!currentPlan) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const candidate = buildTripCandidate(currentPlan, store, scenario, wave, false);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!candidate || candidate.cost >= bestCost) continue;
      bestIndex = vehicleIndex;
      bestCost = candidate.cost;
      bestRoutes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (bestIndex < 0 || !bestRoutes) return null;
    states[bestIndex].routes = bestRoutes;
    normalizeRouteBuckets(states);
  }
  return states;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function collectStorePositions(routeState) {
  const positions = [];
  routeState.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        positions.push({ vehicleIndex, tripIndex, stopIndex, storeId, routeLength: route.length });
      });
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return positions;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function removeStoresFromRouteState(routeState, removedStoreIds) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const removedSet = new Set(removedStoreIds);
  const next = clone(routeState);
  next.forEach((stateItem) => {
    stateItem.routes = stateItem.routes
      .map((route) => route.filter((storeId) => !removedSet.has(storeId)))
      .filter((route) => route.length);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return next;
}

function computeStoreRelatedness(storeAId, storeBId, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (storeAId === storeBId) return 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeA = scenario.storeMap.get(storeAId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeB = scenario.storeMap.get(storeBId);
  if (!storeA || !storeB) return Number.POSITIVE_INFINITY;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const distance = scenario.dist?.[storeAId]?.[storeBId] || scenario.dist?.[storeBId]?.[storeAId] || 0;
  const timingA = getStoreTimingForWave(storeA, wave, scenario.dispatchStartMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const timingB = getStoreTimingForWave(storeB, wave, scenario.dispatchStartMin);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const timeGap = Math.abs(timingA.desiredArrivalMin - timingB.desiredArrivalMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const loadGap = Math.abs(getStoreSolveLoadForWave(storeA, wave) - getStoreSolveLoadForWave(storeB, wave));
  return distance + timeGap * 0.25 + loadGap * 0.6;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function estimateRemovalSavings(routeState, scenario) {
  const scores = [];
  routeState.forEach((stateItem) => {
    stateItem.routes.forEach((route) => {
      route.forEach((storeId, stopIndex) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const prevId = stopIndex === 0 ? DC.id : route[stopIndex - 1];
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const nextId = stopIndex === route.length - 1 ? DC.id : route[stopIndex + 1];
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const saving = (scenario.dist?.[prevId]?.[storeId] || 0) + (scenario.dist?.[storeId]?.[nextId] || 0) - (scenario.dist?.[prevId]?.[nextId] || 0);
        scores.push({ storeId, saving });
      });
    });
  });
  return scores.sort((a, b) => b.saving - a.saving);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function crossoverRouteStates(parentA, parentB, scenario, wave, random) {
  const isCompareMode = (state.settings.solveStrategy || "manual") === "compare";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const child = cloneWaveRouteState(parentA);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const inheritedVehicleIndexes = new Set();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const inheritedStores = new Set();
  child.forEach((item, vehicleIndex) => {
    const takeParentA = random() < 0.5;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!takeParentA) {
      item.routes = [];
      return;
    }
    inheritedVehicleIndexes.add(vehicleIndex);
    item.routes.forEach((route) => route.forEach((storeId) => inheritedStores.add(storeId)));
  });
  const remainingOrder = [
    ...flattenStoresFromRouteState(parentB).filter((storeId) => !inheritedStores.has(storeId)),
    ...flattenStoresFromRouteState(parentA).filter((storeId) => !inheritedStores.has(storeId)),
  ].filter((storeId, index, list) => list.indexOf(storeId) === index);
  child.forEach((item, vehicleIndex) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!inheritedVehicleIndexes.has(vehicleIndex)) item.routes = [];
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return insertStoresIntoRouteState(child, remainingOrder, scenario, wave, {
    candidateVehicleLimit: isCompareMode ? 8 : 0,
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function tournamentSelect(population, random, size = 3) {
  let best = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let i = 0; i < size; i += 1) {
    const candidate = population[Math.floor(random() * population.length)];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!best || candidate.cost < best.cost) best = candidate;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return best;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function pickRemovalCount(totalStops, random) {
  if (totalStops <= 3) return 1;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const ratio = 0.12 + random() * 0.12;
  return Math.max(1, Math.min(Math.max(3, Math.floor(totalStops * 0.25)), Math.floor(totalStops * ratio)));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function randomRelocateNeighbor(routeState, wave, random) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const states = clone(routeState);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nonEmptySources = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        nonEmptySources.push({ vehicleIndex, tripIndex, stopIndex, storeId });
      });
    });
  });
  if (!nonEmptySources.length) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const source = nonEmptySources[Math.floor(random() * nonEmptySources.length)];
  const sourceRoute = states[source.vehicleIndex].routes[source.tripIndex];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const [storeId] = sourceRoute.splice(source.stopIndex, 1);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!sourceRoute.length) states[source.vehicleIndex].routes.splice(source.tripIndex, 1);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const possibleTargets = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      for (let insertAt = 0; insertAt <= route.length; insertAt += 1) {
        possibleTargets.push({ vehicleIndex, tripIndex, insertAt, mode: "insert" });
      }
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!(wave.singleWave && stateItem.routes.length)) {
      possibleTargets.push({ vehicleIndex, tripIndex: stateItem.routes.length, insertAt: 0, mode: "new-trip" });
    }
  });
  if (!possibleTargets.length) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = possibleTargets[Math.floor(random() * possibleTargets.length)];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (target.mode === "new-trip") states[target.vehicleIndex].routes.push([storeId]);
  else states[target.vehicleIndex].routes[target.tripIndex].splice(target.insertAt, 0, storeId);
  normalizeRouteBuckets(states);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    state: states,
    meta: {
      type: "relocate",
      storeId,
      tabuKeys: [`store:${storeId}`, `target:${target.vehicleIndex}:${target.tripIndex}`],
      touchedVehicleIndexes: [...new Set([source.vehicleIndex, target.vehicleIndex])],
    },
  };
}

function mutateRouteState(routeState, scenario, wave, random, steps = 1) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let current = clone(routeState);
  for (let step = 0; step < steps; step += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const roll = random();
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let next = null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (roll < 0.35) next = randomRelocateNeighbor(current, wave, random)?.state || null;
    else if (roll < 0.6) next = randomSwapNeighbor(current, random)?.state || null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    else if (roll < 0.8) next = randomTwoOptNeighbor(current, random)?.state || null;
    else next = randomLnsNeighbor(current, scenario, wave, random)?.state || null;
    if (!next) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const rebuilt = rebuildWavePlansFromState(next, scenario, wave);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!rebuilt) continue;
    current = cloneWaveRouteState(rebuilt);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return current;
}

function randomRelocateMove(routeState, wave, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return randomRelocateNeighbor(routeState, wave, random)?.state || null;
}

function randomSwapNeighbor(routeState, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const states = clone(routeState);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const positions = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        positions.push({ vehicleIndex, tripIndex, stopIndex, storeId });
      });
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (positions.length < 2) return null;
  const first = positions[Math.floor(random() * positions.length)];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let second = positions[Math.floor(random() * positions.length)];
  let guard = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  while (guard < 12 && first.vehicleIndex === second.vehicleIndex && first.tripIndex === second.tripIndex && first.stopIndex === second.stopIndex) {
    second = positions[Math.floor(random() * positions.length)];
    guard += 1;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (first.vehicleIndex === second.vehicleIndex && first.tripIndex === second.tripIndex && first.stopIndex === second.stopIndex) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const a = states[first.vehicleIndex].routes[first.tripIndex][first.stopIndex];
  const b = states[second.vehicleIndex].routes[second.tripIndex][second.stopIndex];
  states[first.vehicleIndex].routes[first.tripIndex][first.stopIndex] = b;
  states[second.vehicleIndex].routes[second.tripIndex][second.stopIndex] = a;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    state: states,
    meta: {
      type: "swap",
      storeIds: [a, b],
      tabuKeys: [`store:${a}`, `store:${b}`],
      touchedVehicleIndexes: [...new Set([first.vehicleIndex, second.vehicleIndex])],
    },
  };
}

function randomSwapMove(routeState, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return randomSwapNeighbor(routeState, random)?.state || null;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function randomTwoOptNeighbor(routeState, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const states = clone(routeState);
  const longRoutes = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (route.length >= 3) longRoutes.push({ vehicleIndex, tripIndex, route });
    });
  });
  if (!longRoutes.length) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = longRoutes[Math.floor(random() * longRoutes.length)];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const route = states[target.vehicleIndex].routes[target.tripIndex];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const i = Math.floor(random() * (route.length - 1));
  const j = i + 1 + Math.floor(random() * (route.length - i - 1));
  states[target.vehicleIndex].routes[target.tripIndex] = route.slice(0, i).concat(route.slice(i, j + 1).reverse(), route.slice(j + 1));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    state: states,
    meta: {
      type: "twoopt",
      tabuKeys: [`route:${target.vehicleIndex}:${target.tripIndex}`, `edge:${route[i]}:${route[j]}`],
      touchedVehicleIndexes: [target.vehicleIndex],
    },
  };
}

function randomTwoOptMove(routeState, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return randomTwoOptNeighbor(routeState, random)?.state || null;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function lnsRepairGreedy(routeState, removedStoreIds, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let states = clone(routeState);
  for (const storeId of removedStoreIds) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const store = scenario.storeMap.get(storeId);
    if (!store) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let bestState = null;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let bestCost = Number.POSITIVE_INFINITY;
    states.forEach((stateItem, vehicleIndex) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const plan = rebuildPlanFromRoutes(
        stateItem.vehicle,
        stateItem.routes,
        scenario,
        wave,
        {
          priorRegularDistance: stateItem.priorRegularDistance,
          priorWaveCount: stateItem.priorWaveCount,
        },
      );
      if (!plan) return;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const candidate = buildTripCandidate(plan, store, scenario, wave, false);
      if (!candidate) return;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const nextState = clone(states);
      nextState[vehicleIndex].routes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const rebuilt = rebuildWavePlansFromState(nextState, scenario, wave);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!rebuilt) return;
      const cost = wavePlansCost(rebuilt, scenario, wave);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (cost < bestCost) {
        bestCost = cost;
        bestState = nextState;
      }
    });
    if (!bestState) return null;
    states = bestState;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return states;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function lnsRepair(routeState, removedStoreIds, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return lnsRepairGreedy(routeState, removedStoreIds, scenario, wave);
}

function lnsRepairRegret(routeState, removedStoreIds, scenario, wave, regretK = 2) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let states = clone(routeState);
  const pending = [...removedStoreIds];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  while (pending.length) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let bestChoice = null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const storeId of pending) {
      const store = scenario.storeMap.get(storeId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!store) continue;
      const ranked = [];
      states.forEach((stateItem, vehicleIndex) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const plan = rebuildSingleStatePlan(stateItem, scenario, wave);
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (!plan) return;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const candidate = buildTripCandidate(plan, store, scenario, wave, false);
        if (!candidate) return;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const nextState = clone(states);
        nextState[vehicleIndex].routes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
        const rebuilt = rebuildWavePlansFromState(nextState, scenario, wave);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (!rebuilt) return;
        ranked.push({
          storeId,
          nextState,
          cost: wavePlansCost(rebuilt, scenario, wave),
        });
      });
      ranked.sort((a, b) => a.cost - b.cost);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!ranked.length) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const best = ranked[0];
      const compare = ranked[Math.min(regretK - 1, ranked.length - 1)];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const regret = (compare?.cost ?? (best.cost + 30)) - best.cost;
      if (!bestChoice || regret > bestChoice.regret || (regret === bestChoice.regret && best.cost < bestChoice.best.cost)) {
        bestChoice = { storeId, best, regret };
      }
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!bestChoice) return null;
    states = bestChoice.best.nextState;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const index = pending.indexOf(bestChoice.storeId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (index >= 0) pending.splice(index, 1);
  }
  return states;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function destroyRandom(routeState, scenario, wave, random, removeCount) {
  const storeRefs = collectStorePositions(routeState);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!storeRefs.length) return null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const pool = [...storeRefs];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const removed = [];
  while (removed.length < removeCount && pool.length) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const pickIndex = Math.floor(random() * pool.length);
    removed.push(pool.splice(pickIndex, 1)[0].storeId);
  }
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "random",
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function destroyShaw(routeState, scenario, wave, random, removeCount) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeRefs = collectStorePositions(routeState);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!storeRefs.length) return null;
  const seed = storeRefs[Math.floor(random() * storeRefs.length)].storeId;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const ranked = [...new Set(storeRefs.map((item) => item.storeId))]
    .map((storeId) => ({ storeId, score: computeStoreRelatedness(seed, storeId, scenario, wave) }))
    .sort((a, b) => a.score - b.score);
  const removed = ranked.slice(0, removeCount).map((item) => item.storeId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "shaw",
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function destroyWorst(routeState, scenario, wave, random, removeCount) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ranked = estimateRemovalSavings(routeState, scenario);
  if (!ranked.length) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const jittered = ranked
    .map((item, index) => ({ ...item, rankScore: item.saving - random() * (8 + index * 0.1) }))
    .sort((a, b) => b.rankScore - a.rankScore);
  const removed = [...new Set(jittered.slice(0, removeCount).map((item) => item.storeId))];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "worst",
  };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function runLnsIteration(routeState, scenario, wave, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const totalStops = flattenStoresFromRouteState(routeState).length;
  if (!totalStops) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const removeCount = pickRemovalCount(totalStops, random);
  const destroyRoll = random();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let destroyed = null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (destroyRoll < 0.34) destroyed = destroyRandom(routeState, scenario, wave, random, removeCount);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  else if (destroyRoll < 0.68) destroyed = destroyShaw(routeState, scenario, wave, random, removeCount);
  else destroyed = destroyWorst(routeState, scenario, wave, random, removeCount);
  if (!destroyed) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const repairRoll = random();
  const repairedState = repairRoll < 0.45
    ? lnsRepairGreedy(destroyed.partialState, destroyed.removedStoreIds, scenario, wave)
    : lnsRepairRegret(destroyed.partialState, destroyed.removedStoreIds, scenario, wave, repairRoll < 0.75 ? 2 : 3);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!repairedState) return null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    state: repairedState,
    meta: {
      type: "lns",
      destroyKey: destroyed.destroyKey,
      repairKey: repairRoll < 0.45 ? "greedy" : (repairRoll < 0.75 ? "regret-2" : "regret-3"),
      removedStoreIds: destroyed.removedStoreIds,
      tabuKeys: destroyed.removedStoreIds.map((storeId) => `store:${storeId}`),
      touchedVehicleIndexes: repairedState.map((_, index) => index),
    },
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function randomLnsNeighbor(routeState, scenario, wave, random) {
  return runLnsIteration(routeState, scenario, wave, random);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function randomLnsMove(routeState, scenario, wave, random) {
  return randomLnsNeighbor(routeState, scenario, wave, random)?.state || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function sampleNeighborhood(routeState, scenario, wave, random, sampleSize = 20, includeLns = true) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const sampled = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const seen = new Set();
  const baselineEval = evaluateRouteState(routeState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!baselineEval) return sampled;
  const baselineApprox = approximateTouchedDistance(routeState, scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let i = 0; i < sampleSize; i += 1) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const roll = random();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let neighbor = null;
    if (roll < 0.4) neighbor = randomRelocateNeighbor(routeState, wave, random);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    else if (roll < 0.7) neighbor = randomSwapNeighbor(routeState, random);
    else if (roll < 0.9) neighbor = randomTwoOptNeighbor(routeState, random);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    else if (includeLns) neighbor = randomLnsNeighbor(routeState, scenario, wave, random);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!neighbor) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const signature = hashRouteState(neighbor.state);
    if (seen.has(signature)) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const touchedVehicleIndexes = neighbor.meta?.touchedVehicleIndexes || [];
    const approxCandidate = approximateTouchedDistance(neighbor.state, scenario, touchedVehicleIndexes);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const approxCurrent = touchedVehicleIndexes.length ? approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes) : baselineApprox;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const allowExpensiveEval = approxCandidate <= approxCurrent + 8 || neighbor.meta?.type === "lns" || random() < 0.22;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!allowExpensiveEval) continue;
    const evaluated = evaluateRouteStateIncremental(neighbor.state, scenario, wave, baselineEval, touchedVehicleIndexes);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!evaluated) continue;
    seen.add(signature);
    sampled.push({
      ...neighbor,
      signature,
      plans: evaluated.plans,
      cost: evaluated.cost,
    });
  }
  sampled.sort((a, b) => a.cost - b.cost);
  return sampled;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function sampleSingleNeighbor(routeState, scenario, wave, random, includeLns = true, maxAttempts = 6) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const baselineEval = evaluateRouteState(routeState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!baselineEval) return null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const roll = random();
    let neighbor = null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (roll < 0.46) neighbor = randomRelocateNeighbor(routeState, wave, random);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    else if (roll < 0.76) neighbor = randomSwapNeighbor(routeState, random);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    else if (roll < 0.94) neighbor = randomTwoOptNeighbor(routeState, random);
    else if (includeLns) neighbor = randomLnsNeighbor(routeState, scenario, wave, random);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!neighbor) continue;
    const touchedVehicleIndexes = neighbor.meta?.touchedVehicleIndexes || [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const approxCandidate = approximateTouchedDistance(neighbor.state, scenario, touchedVehicleIndexes);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const approxCurrent = approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (approxCandidate > approxCurrent + 12 && neighbor.meta?.type !== "lns" && attempt < maxAttempts - 1) continue;
    const evaluated = evaluateRouteStateIncremental(neighbor.state, scenario, wave, baselineEval, touchedVehicleIndexes);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!evaluated) continue;
    return {
      ...neighbor,
      signature: hashRouteState(neighbor.state),
      plans: evaluated.plans,
      cost: evaluated.cost,
    };
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return null;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function localImproveState(routeState, scenario, wave, random, rounds = 6) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentState = clone(routeState);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!currentEval) return null;
  if (rounds <= 0) return { state: currentState, plans: currentEval.plans, cost: currentEval.cost };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let round = 0; round < rounds; round += 1) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const neighborhood = sampleNeighborhood(currentState, scenario, wave, random, 10, round % 2 === 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const improving = neighborhood.find((item) => item.cost + 1e-6 < currentEval.cost);
    if (!improving) break;
    currentState = improving.state;
    currentEval = { plans: improving.plans, cost: improving.cost };
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { state: currentState, plans: currentEval.plans, cost: currentEval.cost };
}

async function cooperativeYield() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  await sleep(0);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithVrptwBackend(initialPlans, scenario, wave, randomSeed = 7) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "vrptw", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("vrptw_BACKEND_REQUIRED:no_result");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithSavingsBackend(initialPlans, scenario, wave, randomSeed = 13) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "savings", randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("savings_BACKEND_REQUIRED:no_result");
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithVehicleDrivenBackend(initialPlans, scenario, wave, randomSeed = 17) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "vehicle", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("vehicle_BACKEND_REQUIRED:no_result");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithHybrid(initialPlans, scenario, wave, randomSeed = 42) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "hybrid", randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (backendResult?.plans?.length) return backendResult;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const random = createSeededRandom(randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  let currentPlans = currentEval.plans;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentCost = currentEval.cost;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestState = cloneWaveRouteState(currentPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let noImproveRounds = 0;
  const traceLog = [
    {
      algorithmKey: "hybrid",
      scope: "wave",
      waveId: wave.waveId,
      stage: "hybrid-start",
      textZh: `混合阶段从贪心插入初始解启动，初始综合成本 ${currentCost.toFixed(1)}。`,
      textJa: `混合段階は貪欲挿入の初期解から開始し、初期総合コストは ${currentCost.toFixed(1)} です。`,
      },
  ];
  reportRelayStageProgress(`${wave.waveId} 混合VRPTW已启动，当前波次内部代价 ${currentCost.toFixed(1)}，开始做混合邻域迭代。`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let iteration = 0; iteration < 110; iteration += 1) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (iteration && iteration % 6 === 0) await cooperativeYield();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const neighborhood = sampleNeighborhood(currentState, scenario, wave, random, 24, true);
    if (!neighborhood.length) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let candidate = neighborhood.find((item) => item.cost + 1e-6 < currentCost);
    if (!candidate && noImproveRounds >= 4) candidate = neighborhood[0];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!candidate) {
      noImproveRounds += 1;
      continue;
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const temperature = Math.max(0.03, 1 - iteration / 110);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const accept = candidate.cost + 1e-6 < currentCost || random() < Math.exp((currentCost - candidate.cost) / Math.max(0.001, temperature));
    if (!accept) {
      noImproveRounds += 1;
      continue;
    }
    currentState = candidate.state;
    currentPlans = candidate.plans;
    currentCost = candidate.cost;
    noImproveRounds = candidate.cost + 1e-6 < bestCost ? 0 : noImproveRounds + 1;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (traceLog.length < 18) {
      pushTraceEvent(traceLog, {
        algorithmKey: "hybrid",
        scope: "wave",
        waveId: wave.waveId,
        stage: "hybrid-iteration",
        iteration,
        moveKey: candidate.meta?.type || "mixed",
        candidateCost: candidate.cost,
        bestCost: Math.min(bestCost, candidate.cost),
      textZh: `第 ${iteration + 1} 轮接受 ${candidate.meta?.type || "mixed"} 动作，候选波次内部代价 ${candidate.cost.toFixed(1)}，当前最好 ${Math.min(bestCost, candidate.cost).toFixed(1)}。`,
        textJa: `${iteration + 1} 回目で ${candidate.meta?.type || "mixed"} 動作を採用し、候補コスト ${candidate.cost.toFixed(1)}、現時点の最良は ${Math.min(bestCost, candidate.cost).toFixed(1)}。`,
      });
    }
    if (candidate.cost + 1e-6 < bestCost) {
      bestCost = candidate.cost;
      bestState = cloneWaveRouteState(candidate.plans);
      bestPlans = candidate.plans;
      reportRelayStageProgress(`${wave.waveId} 混合VRPTW在第 ${iteration + 1} 轮刷新最优，波次内部代价降到 ${bestCost.toFixed(1)}。`);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (traceLog.length < 22) {
        pushTraceEvent(traceLog, {
          algorithmKey: "hybrid",
          scope: "wave",
          waveId: wave.waveId,
          stage: "hybrid-best",
          iteration,
          moveKey: candidate.meta?.type || "mixed",
          bestCost,
      textZh: `第 ${iteration + 1} 轮刷新最优解，动作 ${candidate.meta?.type || "mixed"}，新的波次内部代价 ${bestCost.toFixed(1)}。`,
          textJa: `${iteration + 1} 回目で最良解を更新し、動作 ${candidate.meta?.type || "mixed"}、新しい最良コストは ${bestCost.toFixed(1)}。`,
        });
      }
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if ((iteration + 1) % 18 === 0) {
      reportRelayStageProgress(`${wave.waveId} 混合VRPTW已跑到第 ${iteration + 1}/110 轮，当前最好波次内部代价 ${bestCost.toFixed(1)}。`);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (noImproveRounds >= 6) {
      const kick = localImproveState(bestState, scenario, wave, random, 3);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (kick && kick.cost + 1e-6 < bestCost) {
        currentState = kick.state;
        currentPlans = kick.plans;
        currentCost = kick.cost;
        bestState = cloneWaveRouteState(kick.plans);
        bestPlans = kick.plans;
        bestCost = kick.cost;
        noImproveRounds = 0;
      }
    }
  }

  const finalPlans = rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans;
  pushTraceEvent(traceLog, {
    algorithmKey: "hybrid",
    scope: "wave",
    waveId: wave.waveId,
    stage: "hybrid-finish",
    bestCost,
    textZh: `混合阶段结束，最终波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `混合段階が終了し、最終最良総合コストは ${bestCost.toFixed(1)} です。`,
  });
  reportRelayStageProgress(`${wave.waveId} 混合VRPTW结束，最终波次内部代价 ${bestCost.toFixed(1)}。`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { plans: finalPlans, traceLog };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithTabu(initialPlans, scenario, wave, randomSeed = 77) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "tabu", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentPlans = currentEval.plans;
  let currentCost = currentEval.cost;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let bestCost = currentCost;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const tabu = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "tabu",
    scope: "wave",
    waveId: wave.waveId,
    stage: "tabu-start",
    bestCost,
    textZh: `禁忌搜索从当前可行解出发，初始波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `タブー探索は現在の可行解から開始し、初期コストは ${bestCost.toFixed(1)} です。`,
  });
  reportRelayStageProgress(`${wave.waveId} 禁忌搜索已启动，初始波次内部代价 ${bestCost.toFixed(1)}。`);
  for (let iteration = 0; iteration < 120; iteration += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (iteration && iteration % 6 === 0) await cooperativeYield();
    let bestNeighbor = null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const candidates = sampleNeighborhood(currentState, scenario, wave, random, 28, true);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (const candidate of candidates) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const tabuBlocked = (candidate.meta?.tabuKeys || []).some((key) => iteration < (tabu.get(key) || -1));
      const aspiration = candidate.cost + 1e-6 < bestCost;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (tabuBlocked && !aspiration) continue;
      if (!bestNeighbor || candidate.cost < bestNeighbor.cost) bestNeighbor = candidate;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!bestNeighbor) continue;
    currentState = bestNeighbor.state;
    currentPlans = bestNeighbor.plans;
    currentCost = bestNeighbor.cost;
    (bestNeighbor.meta?.tabuKeys || [bestNeighbor.signature]).forEach((key, index) => {
      tabu.set(key, iteration + 8 + index);
    });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (iteration < 12) {
      pushTraceEvent(traceLog, {
        algorithmKey: "tabu",
        scope: "wave",
        waveId: wave.waveId,
        stage: "tabu-iteration",
        iteration,
        candidateCost: currentCost,
        bestCost: Math.min(bestCost, currentCost),
        moveKey: bestNeighbor.meta?.type || "mixed",
        textZh: `第 ${iteration + 1} 轮从邻域样本中选出当前最优邻居，动作 ${bestNeighbor.meta?.type || "mixed"}，成本 ${currentCost.toFixed(1)}。`,
        textJa: `${iteration + 1} 回目で近傍サンプルから最良隣接解を採用し、動作 ${bestNeighbor.meta?.type || "mixed"}、コストは ${currentCost.toFixed(1)} です。`,
      });
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (currentCost < bestCost) {
      bestCost = currentCost;
      bestState = cloneWaveRouteState(currentPlans);
      bestPlans = currentPlans;
      reportRelayStageProgress(`${wave.waveId} 禁忌搜索在第 ${iteration + 1} 轮刷新最优，波次内部代价 ${bestCost.toFixed(1)}。`);
      pushTraceEvent(traceLog, {
        algorithmKey: "tabu",
        scope: "wave",
        waveId: wave.waveId,
        stage: "tabu-best",
        iteration,
        bestCost,
      textZh: `第 ${iteration + 1} 轮刷新禁忌搜索最优解，新的波次内部代价 ${bestCost.toFixed(1)}。`,
        textJa: `${iteration + 1} 回目でタブー探索の最良解を更新し、新しい最良コストは ${bestCost.toFixed(1)} です。`,
      });
    }
    if ((iteration + 1) % 20 === 0) {
      reportRelayStageProgress(`${wave.waveId} 禁忌搜索已跑到第 ${iteration + 1}/120 轮，当前最好波次内部代价 ${bestCost.toFixed(1)}。`);
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const finalPlans = rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans;
  pushTraceEvent(traceLog, {
    algorithmKey: "tabu",
    scope: "wave",
    waveId: wave.waveId,
    stage: "tabu-finish",
    bestCost,
    textZh: `禁忌搜索结束，最终波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `タブー探索が終了し、最終最良コストは ${bestCost.toFixed(1)} です。`,
  });
  reportRelayStageProgress(`${wave.waveId} 禁忌搜索结束，最终波次内部代价 ${bestCost.toFixed(1)}。`);
  return { plans: finalPlans, traceLog };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function optimizeWaveWithLns(initialPlans, scenario, wave, randomSeed = 109) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "lns", randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let currentPlans = currentEval.plans;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentCost = currentEval.cost;
  let bestState = cloneWaveRouteState(currentPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "lns",
    scope: "wave",
    waveId: wave.waveId,
    stage: "lns-start",
    bestCost,
    textZh: `大邻域搜索从初始解出发，开始做 destroy / repair 迭代，初始波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `大近傍探索は初期解から開始し、destroy / repair を反復します。初期コストは ${bestCost.toFixed(1)} です。`,
  });
  reportRelayStageProgress(`${wave.waveId} 大邻域搜索已启动，初始波次内部代价 ${bestCost.toFixed(1)}。`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let iteration = 0; iteration < 70; iteration += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (iteration && iteration % 5 === 0) await cooperativeYield();
    const candidate = runLnsIteration(currentState, scenario, wave, random);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!candidate) continue;
    const candidatePlans = rebuildWavePlansFromState(candidate.state, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!candidatePlans) continue;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const candidateCost = wavePlansCost(candidatePlans, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const improved = candidateCost < currentCost;
    const accepted = improved || random() < 0.10;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!accepted) continue;
    currentState = candidate.state;
    currentPlans = candidatePlans;
    currentCost = candidateCost;
    if (iteration < 12) {
      pushTraceEvent(traceLog, {
        algorithmKey: "lns",
        scope: "wave",
        waveId: wave.waveId,
        stage: "lns-iteration",
        iteration,
        candidateCost,
        accepted,
        destroyKey: candidate.meta?.destroyKey || "random",
        repairKey: candidate.meta?.repairKey || "greedy",
        textZh: `第 ${iteration + 1} 轮完成一次 ${candidate.meta?.destroyKey || "random"} destroy + ${candidate.meta?.repairKey || "greedy"} repair，候选成本 ${candidateCost.toFixed(1)}${improved ? "，优于当前解并被接受" : "，作为扰动解被接受"}。`,
        textJa: `${iteration + 1} 回目で ${candidate.meta?.destroyKey || "random"} destroy + ${candidate.meta?.repairKey || "greedy"} repair を完了し、候補コスト ${candidateCost.toFixed(1)}${improved ? " は現解より良く採用されました" : " は攪乱解として採用されました"}。`,
      });
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (candidateCost < bestCost) {
      bestCost = candidateCost;
      bestState = cloneWaveRouteState(candidatePlans);
      bestPlans = candidatePlans;
      reportRelayStageProgress(`${wave.waveId} 大邻域在第 ${iteration + 1} 轮刷新最优，波次内部代价 ${bestCost.toFixed(1)}。`);
      pushTraceEvent(traceLog, {
        algorithmKey: "lns",
        scope: "wave",
        waveId: wave.waveId,
        stage: "lns-best",
        iteration,
        bestCost,
        textZh: `第 ${iteration + 1} 轮刷新 LNS 最优解，波次内部代价 ${bestCost.toFixed(1)}。`,
        textJa: `${iteration + 1} 回目で LNS の最良解を更新し、最良コストは ${bestCost.toFixed(1)} です。`,
      });
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if ((iteration + 1) % 14 === 0) {
      reportRelayStageProgress(`${wave.waveId} 大邻域已跑到第 ${iteration + 1}/70 轮，当前最好波次内部代价 ${bestCost.toFixed(1)}。`);
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const finalPlans = rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans;
  pushTraceEvent(traceLog, {
    algorithmKey: "lns",
    scope: "wave",
    waveId: wave.waveId,
    stage: "lns-finish",
    bestCost,
    textZh: `大邻域搜索结束，最终波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `大近傍探索が終了し、最終最良コストは ${bestCost.toFixed(1)} です。`,
  });
  reportRelayStageProgress(`${wave.waveId} 大邻域搜索结束，最终波次内部代价 ${bestCost.toFixed(1)}。`);
  return { plans: finalPlans, traceLog };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 2500) {
  const controller = new AbortController();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function ensureGaBackendAvailable(force = false) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const now = Date.now();
  if (!force && gaBackendHealth.available !== null && now - gaBackendHealth.checkedAt < 15000) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return gaBackendHealth.available;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/health`, {}, 1200);
    gaBackendHealth = { available: response.ok, checkedAt: now };
    return response.ok;
  } catch {
    gaBackendHealth = { available: false, checkedAt: now };
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return false;
  }
}

function renderGaBackendStatus() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const bar = document.getElementById("gaBackendStatusBar");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!bar) return;
  bar.classList.remove("is-online", "is-offline", "is-checking");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (gaBackendHealth.available === true) {
    bar.classList.add("is-online");
    bar.textContent = L("gaBackendOnline");
    return;
  }
  if (gaBackendHealth.available === false) {
    bar.classList.add("is-offline");
    bar.textContent = L("gaBackendOffline");
    return;
  }
  bar.classList.add("is-checking");
  bar.textContent = L("gaBackendChecking");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function refreshGaBackendStatus(force = false) {
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await ensureGaBackendAvailable(force);
  } catch {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
  } finally {
    renderGaBackendStatus();
  }
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function startGaBackendStatusMonitor() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (gaBackendStatusTimer) clearInterval(gaBackendStatusTimer);
  refreshGaBackendStatus(true);
  scheduleAmapCacheSync(2600);
  gaBackendStatusTimer = setInterval(() => {
    refreshGaBackendStatus(true);
  }, 6000);
}

async function flushAmapCacheToBackend() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!amapCacheSyncPending) return false;
  amapCacheSyncPending = false;
  let available = await ensureGaBackendAvailable();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) {
    available = await ensureGaBackendAvailable(true);
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) {
    amapCacheSyncPending = true;
    scheduleAmapCacheSync(8000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return false;
  }
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const distanceCache = loadAmapDistanceCache();
    const routeCache = loadAmapRouteCache();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/amap-cache/sync`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        distanceCache,
        routeCache,
        maxRows: 12000,
      }),
    }, 12000);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) {
      amapCacheSyncPending = true;
      scheduleAmapCacheSync(8000);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return false;
    }
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return Boolean(data?.ok);
  } catch {
    amapCacheSyncPending = true;
    scheduleAmapCacheSync(8000);
    return false;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function saveRunArchiveToBackend(snapshot) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!snapshot?.id) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let available = await ensureGaBackendAvailable();
  if (!available) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    await sleep(160);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ snapshot }),
    }, 8000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!response.ok) return false;
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return Boolean(data?.ok);
  } catch {
    return false;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function listRunArchivesFromBackend(date, page = 1, pageSize = 6) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let available = await ensureGaBackendAvailable();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) {
    await sleep(160);
    available = await ensureGaBackendAvailable(true);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return null;
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date, page, pageSize }),
    }, 10000);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) return null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const data = await response.json();
    if (!data?.ok) return null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      items: Array.isArray(data.items) ? data.items : [],
      totalPages: Number(data.totalPages || 1),
      total: Number(data.total || 0),
      page: Number(data.page || page),
      pageSize: Number(data.pageSize || pageSize),
    };
  } catch {
    return null;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function getRunArchiveFromBackend(archiveId) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!archiveId) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return null;
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/get`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: archiveId }),
    }, 8000);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) return null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const data = await response.json();
    if (!data?.ok || !data?.found) return null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return data.item || null;
  } catch {
    return null;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getCurrentTaskDate() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const now = new Date();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function formatRecommendedScore(value) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (value === null || value === undefined || value === "") return "-";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(4) : String(value);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function formatRecommendedScoreLabel(value) {
  if (value === null || value === undefined || value === "") return "待计算";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const num = Number(value);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return Number.isFinite(num) ? num.toFixed(4) : String(value);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderRecommendedPlans() {
  const mount = document.getElementById("recommendedPlansSection");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!mount) return;
  const taskDate = recommendedPlanCache.taskDate || getCurrentTaskDate();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const items = Array.isArray(recommendedPlanCache.items) ? recommendedPlanCache.items : [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const selected = recommendedPlanSelectedCache.taskDate === taskDate ? recommendedPlanSelectedCache.selected : null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const emptyState = items.length
    ? items.map((item) => `
        <div class="recommended-item" data-candidate-id="${item.id}">
          <div class="recommended-item-main">
            <div class="recommended-rank">#${item.rankNo || "-"}</div>
            <div class="recommended-meta">
              <div><strong>source_run_id:</strong> ${item.sourceRunId || "-"}</div>
              <div><strong>similarity_score:</strong> ${formatRecommendedScoreLabel(item.similarityScore)}</div>
              <div><strong>created_at:</strong> ${item.snapshot?.created_at || "-"}</div>
              <div><strong>strategy:</strong> ${item.snapshot?.strategy || "-"}</div>
              <div><strong>goal:</strong> ${item.snapshot?.goal || "-"}</div>
              <div><strong>best_score:</strong> ${formatRecommendedScore(item.snapshot?.best_score)}</div>
            </div>
          </div>
          <button class="secondary select-recommended-plan" data-candidate-id="${item.id}">选择此方案</button>
        </div>
      `).join("")
    : `<div class="empty-state">暂无推荐方案</div>`;
  mount.innerHTML = `
    <div class="panel-head">
      <div>
        <h2>历史方案推荐</h2>
        <p>当前任务日期：${taskDate}</p>
      </div>
      <div class="toolbar inline-toolbar">
        <button id="fetchRecommendedPlansBtn" class="primary">获取推荐方案</button>
      </div>
    </div>
    <div class="recommended-selected">
      <div class="panel-head">
        <div>
          <h3>当前已选方案</h3>
          <p>${selected ? `sourceRunId: ${selected.sourceRunId || "-"} | created_at: ${selected.snapshot?.created_at || selected.createdAt || "-"} | strategy: ${selected.snapshot?.strategy || "-"} | goal: ${selected.snapshot?.goal || "-"} | best_score: ${formatRecommendedScore(selected.snapshot?.best_score)}` : "暂无已选方案"}</p>
        </div>
      </div>
    </div>
    <div id="recommendedPlansList" class="recommended-list">
      ${emptyState}
    </div>
  `;
  const fetchBtn = document.getElementById("fetchRecommendedPlansBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (fetchBtn) {
    fetchBtn.onclick = async () => {
      await refreshRecommendedPlans(taskDate, true);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      await refreshRecommendedPlanSelected(taskDate, true);
    };
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  document.querySelectorAll(".select-recommended-plan").forEach((button) => {
    button.onclick = async () => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const candidateId = Number(button.dataset.candidateId || 0);
      await selectRecommendedPlan(taskDate, candidateId);
    };
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function refreshRecommendedPlanSelected(taskDate = getCurrentTaskDate(), force = false) {
  if (!taskDate) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (recommendedPlanSelectedCache.loading && !force) return;
  recommendedPlanSelectedCache.loading = true;
  renderRecommendedPlans();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/current?taskDate=${encodeURIComponent(taskDate)}`, {}, 5000);
    if (!response.ok) {
      recommendedPlanSelectedCache = { taskDate, selected: null, loading: false };
      renderRecommendedPlans();
      return;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const data = await response.json();
    recommendedPlanSelectedCache = {
      taskDate: data?.taskDate || taskDate,
      selected: data?.selected || null,
      loading: false,
    };
    renderRecommendedPlans();
  } catch {
    recommendedPlanSelectedCache = { taskDate, selected: null, loading: false };
    renderRecommendedPlans();
  }
}

async function refreshRecommendedPlans(taskDate = getCurrentTaskDate(), force = false) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!taskDate) return;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (recommendedPlanCache.loading && !force) return;
  recommendedPlanCache.loading = true;
  renderRecommendedPlans();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/list?taskDate=${encodeURIComponent(taskDate)}`, {}, 5000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) {
      recommendedPlanCache = { taskDate, items: [], loading: false };
      renderRecommendedPlans();
      return;
    }
    const data = await response.json();
    recommendedPlanCache = {
      taskDate: data?.taskDate || taskDate,
      items: Array.isArray(data?.items) ? data.items.map((item) => ({
        ...item,
        snapshot: item.snapshot || item.snapshot_json || {},
      })) : [],
      loading: false,
    };
    localStorage.setItem(RECOMMENDED_PLAN_TASK_DATE_KEY, recommendedPlanCache.taskDate);
    renderRecommendedPlans();
  } catch {
    recommendedPlanCache = { taskDate, items: [], loading: false };
    renderRecommendedPlans();
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function selectRecommendedPlan(taskDate, candidateId) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!taskDate || !candidateId) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/select`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ taskDate, candidateId }),
    }, 5000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) return;
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!data?.ok) return;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    await refreshRecommendedPlans(taskDate, true);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await refreshRecommendedPlanSelected(taskDate, true);
  } catch {}
}

function setRunRegionStatus(text = "") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const el = document.getElementById("runRegionStatus");
  if (!el) return;
  el.textContent = text || "";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getSelectedRunRegionSchemeNo() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return String(runRegionSelectedSchemeNo || "").trim();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function setSelectedRunRegionSchemeNo(value = "") {
  runRegionSelectedSchemeNo = String(value || "").trim();
  if (runRegionSelectedSchemeNo) {
    localStorage.setItem(RUN_REGION_SCHEME_SELECTED_KEY, runRegionSelectedSchemeNo);
  } else {
    localStorage.removeItem(RUN_REGION_SCHEME_SELECTED_KEY);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const regionSelect = document.getElementById("runRegionSchemeSelect");
  if (regionSelect) regionSelect.value = runRegionSelectedSchemeNo;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveSelect = document.getElementById("solveRegionSchemeSelect");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (solveSelect) solveSelect.value = runRegionSelectedSchemeNo;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function normalizeRunRegionPath(path) {
  if (!Array.isArray(path)) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return path
    .map((point) => {
      if (Array.isArray(point) && point.length >= 2) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return [Number(point[0]), Number(point[1])];
      }
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (point && typeof point === "object") {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return [Number(point.lng ?? point.x), Number(point.lat ?? point.y)];
      }
      return null;
    })
    .filter((point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function fallbackRunRegionStorePoints() {
  return (state.stores || [])
    .map((store) => ({
      store_id: String(store.id || ""),
      store_name: String(store.name || ""),
      lng: Number(store.lng || 0),
      lat: Number(store.lat || 0),
    }))
    .filter((item) => item.store_id && Number.isFinite(item.lng) && Number.isFinite(item.lat) && (Math.abs(item.lng) > 1e-9 || Math.abs(item.lat) > 1e-9));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getPathFromRunRegionPolygon(polygon) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!polygon?.getPath) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rawPath = polygon.getPath() || [];
  return rawPath
    .map((point) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (Array.isArray(point) && point.length >= 2) return [Number(point[0]), Number(point[1])];
      if (point && typeof point.getLng === "function" && typeof point.getLat === "function") {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return [Number(point.getLng()), Number(point.getLat())];
      }
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (point && typeof point === "object") return [Number(point.lng), Number(point.lat)];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return null;
    })
    .filter((point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]));
}

function stopRunRegionDrawing(removeDraft = false) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (runRegionPolygonEditor) {
    try { runRegionPolygonEditor.close(); } catch {}
    runRegionPolygonEditor = null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (runRegionMouseTool) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    try { runRegionMouseTool.close(true); } catch {}
    runRegionMouseTool = null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (removeDraft && runRegionDraftPolygon && !runRegionEditingId) {
    try { runRegionDraftPolygon.setMap(null); } catch {}
  }
  runRegionDraftPolygon = null;
  runRegionEditingId = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const saveBtn = document.getElementById("runRegionSaveBtn");
  if (saveBtn) saveBtn.disabled = true;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function ensureRunRegionMapReady() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const container = document.getElementById("runRegionMap");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!container) return false;
  if (runRegionMap) return true;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!window.AMap?.Map) {
    container.innerHTML = `<div class="empty-state">高德地图未加载完成，请稍后重试。</div>`;
    return false;
  }
  container.innerHTML = "";
  runRegionMap = new window.AMap.Map("runRegionMap", {
    viewMode: "2D",
    zoom: 11,
    mapStyle: "amap://styles/normal",
    resizeEnable: true,
    dragEnable: true,
    zoomEnable: true,
    scrollWheel: true,
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    runRegionMap.addControl(new window.AMap.ToolBar({ position: "RB" }));
    runRegionMap.addControl(new window.AMap.Scale());
  } catch {}
  runRegionMap.setZoomAndCenter(11, [Number(DC.lng), Number(DC.lat)]);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return true;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function normalizeRunRegionStoreVisibilityMode(mode) {
  const value = String(mode || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (["show_all", "show_in_region", "hide_all", "hide_in_region"].includes(value)) return value;
  return "show_all";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function ensureRunRegionCheckedSelection(items) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const list = Array.isArray(items) ? items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const validIds = new Set(list.map((item) => String(item.id)));
  const next = new Set();
  runRegionCheckedRegionIds.forEach((id) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (validIds.has(String(id))) next.add(String(id));
  });
  if (!next.size && list.length) {
    list.forEach((item) => next.add(String(item.id)));
  }
  runRegionCheckedRegionIds = next;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getSelectedRunRegionItems() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!items.length) return [];
  return items.filter((item) => runRegionCheckedRegionIds.has(String(item.id)));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function getSelectedRunRegionStoreIdSet() {
  const set = new Set();
  getSelectedRunRegionItems().forEach((item) => {
    (item.storeIds || []).forEach((storeId) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const sid = String(storeId || "").trim();
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (sid) set.add(sid);
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return set;
}

function getRunRegionTargetPaths() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const items = getSelectedRunRegionItems();
  if (!items.length) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (String(runRegionTargetRegionId || "all") === "all") {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return items.map((item) => normalizeRunRegionPath(item.path)).filter((path) => path.length >= 3);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = items.find((item) => String(item.id) === String(runRegionTargetRegionId));
  if (!target) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const path = normalizeRunRegionPath(target.path);
  return path.length >= 3 ? [path] : [];
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function isPointInRunRegionPath(point, path) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!Array.isArray(path) || path.length < 3 || !Array.isArray(point) || point.length < 2) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const x = Number(point[0]);
  const y = Number(point[1]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
  let inside = false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let i = 0, j = path.length - 1; i < path.length; j = i, i += 1) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const xi = Number(path[i][0]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const yi = Number(path[i][1]);
    const xj = Number(path[j][0]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const yj = Number(path[j][1]);
    const intersects = ((yi > y) !== (yj > y))
      && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-12) + xi);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (intersects) inside = !inside;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return inside;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function isRunRegionStorePointInTarget(item, paths) {
  const lng = Number(item?.lng);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const lat = Number(item?.lat);
  if (!Number.isFinite(lng) || !Number.isFinite(lat) || !paths.length) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const point = [lng, lat];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return paths.some((path) => isPointInRunRegionPath(point, path));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getVisibleRunRegionStorePoints() {
  const mode = normalizeRunRegionStoreVisibilityMode(runRegionStoreVisibilityMode);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (mode === "hide_all") return [];
  const points = Array.isArray(runRegionStorePoints) ? runRegionStorePoints : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const selectedItems = getSelectedRunRegionItems();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!selectedItems.length) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const selectedStoreIdSet = getSelectedRunRegionStoreIdSet();
  const selectedPaths = selectedItems.map((item) => normalizeRunRegionPath(item.path)).filter((path) => path.length >= 3);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const scopedPoints = points.filter((item) => {
    const sid = String(item?.store_id || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (sid && selectedStoreIdSet.has(sid)) return true;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (selectedPaths.length) return isRunRegionStorePointInTarget(item, selectedPaths);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return false;
  });
  if (mode === "show_all") return scopedPoints;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const paths = getRunRegionTargetPaths();
  if (!paths.length) return mode === "show_in_region" ? [] : scopedPoints;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (mode === "show_in_region") {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return scopedPoints.filter((item) => isRunRegionStorePointInTarget(item, paths));
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (mode === "hide_in_region") {
    return scopedPoints.filter((item) => !isRunRegionStorePointInTarget(item, paths));
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return scopedPoints;
}

function renderRunRegionStoreMarkers() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!runRegionMap || !window.AMap?.Marker) return;
  runRegionStoreMarkers.forEach((marker) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    try { marker.setMap(null); } catch {}
  });
  runRegionStoreMarkers = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const visiblePoints = getVisibleRunRegionStorePoints();
  runRegionStoreMarkers = visiblePoints.flatMap((item) => {
    const lng = Number(item.lng);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const lat = Number(item.lat);
    if (!Number.isFinite(lng) || !Number.isFinite(lat)) return [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const marker = new window.AMap.Marker({
      position: [lng, lat],
      anchor: "center",
      content: '<div class="run-region-store-point"></div>',
      offset: new window.AMap.Pixel(-6, -6),
      zIndex: 120,
      bubble: true,
    });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const labelText = `${item.store_name || item.store_id || ""}`.trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (labelText) {
      marker.setLabel({
        direction: "right",
        offset: new window.AMap.Pixel(8, -2),
        content: `<span class="run-region-store-name">${escapeHtml(labelText)}</span>`,
      });
    }
    marker.setMap(runRegionMap);
    marker.setTitle(`${item.store_id} ${item.store_name}`.trim());
    return marker;
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function renderRunRegionPolygons() {
  if (!runRegionMap || !window.AMap?.Polygon) return;
  runRegionPolygons.forEach((polygon) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    try { polygon.setMap(null); } catch {}
  });
  runRegionPolygons.clear();
  getSelectedRunRegionItems().forEach((item, index) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const path = normalizeRunRegionPath(item.path);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (path.length < 3) return;
    const polygon = new window.AMap.Polygon({
      path,
      strokeColor: "#ef4444",
      strokeWeight: 2,
      strokeOpacity: 0.95,
      fillColor: index % 2 === 0 ? "#f59e0b" : "#10b981",
      fillOpacity: 0.24,
      zIndex: 130 + index,
      bubble: true,
    });
    polygon.setMap(runRegionMap);
    runRegionPolygons.set(String(item.id), polygon);
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function fitRunRegionMapView() {
  if (!runRegionMap) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const overlays = [...runRegionStoreMarkers, ...Array.from(runRegionPolygons.values())];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!overlays.length) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    runRegionMap.setFitView(overlays, false, [56, 56, 56, 56], 16);
  } catch {}
}

function renderRunRegionSchemeOptions() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const items = Array.isArray(runRegionSchemeCache.items) ? runRegionSchemeCache.items : [];
  const options = [
    `<option value="">请选择分区方案号</option>`,
    ...items.map((item) => `<option value="${escapeHtml(item.schemeNo)}">${escapeHtml(`${item.schemeNo} | ${item.name}${item.enabled ? "" : "（停用）"}`)}</option>`),
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const regionSelect = document.getElementById("runRegionSchemeSelect");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (regionSelect) {
    regionSelect.innerHTML = options.join("");
    regionSelect.value = getSelectedRunRegionSchemeNo();
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveSelect = document.getElementById("solveRegionSchemeSelect");
  if (solveSelect) {
    solveSelect.innerHTML = options.join("");
    solveSelect.value = getSelectedRunRegionSchemeNo();
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const selected = items.find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
  const noInput = document.getElementById("runRegionSchemeNoInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nameInput = document.getElementById("runRegionSchemeNameInput");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const enabledInput = document.getElementById("runRegionSchemeEnabledInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (selected) {
    if (noInput) noInput.value = selected.schemeNo;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (nameInput) nameInput.value = selected.name || "";
    if (enabledInput) enabledInput.checked = Boolean(selected.enabled);
  } else {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (nameInput) nameInput.value = "";
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (enabledInput) enabledInput.checked = true;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderRunRegionTargetRegionOptions() {
  const select = document.getElementById("runRegionTargetRegion");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!select) return;
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const previous = String(runRegionTargetRegionId || "all");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const options = [
    `<option value="all">全部</option>`,
    ...items.map((item) => `<option value="${item.id}">${escapeHtml((item.regionCode ? `方案1-${item.regionCode}` : "") || item.name || `运行区-${item.id}`)}</option>`),
  ];
  select.innerHTML = options.join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const keep = previous === "all" || items.some((item) => String(item.id) === previous);
  runRegionTargetRegionId = keep ? previous : "all";
  select.value = runRegionTargetRegionId;
}

function renderRunRegionList() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const mount = document.getElementById("runRegionList");
  if (!mount) return;
  renderRunRegionSchemeOptions();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!getSelectedRunRegionSchemeNo()) {
    runRegionTargetRegionId = "all";
    renderRunRegionTargetRegionOptions();
    mount.innerHTML = `<div class="empty-state">请先选择分区方案号。</div>`;
    return;
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  ensureRunRegionCheckedSelection(items);
  renderRunRegionTargetRegionOptions();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!items.length) {
    mount.innerHTML = `<div class="empty-state">暂无运行区，请先绘制并保存。</div>`;
    return;
  }
  const checkedCount = items.filter((item) => runRegionCheckedRegionIds.has(String(item.id))).length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const allChecked = checkedCount > 0 && checkedCount === items.length;
  const listHeader = `
    <div class="run-region-check-toolbar">
      <label class="run-region-check-inline">
        <input type="checkbox" id="runRegionCheckAll" ${allChecked ? "checked" : ""}>
        <span>全部</span>
      </label>
      <span class="run-region-check-summary">已选 ${checkedCount}/${items.length}</span>
    </div>
  `;
  mount.innerHTML = items.map((item) => `
    <article class="run-region-item" data-region-id="${item.id}">
      <div class="run-region-item-title">
        <label class="run-region-check-inline">
          <input type="checkbox" class="run-region-visibility-check" data-region-id="${item.id}" ${runRegionCheckedRegionIds.has(String(item.id)) ? "checked" : ""}>
          <span>${escapeHtml(item.name || `运行区-${item.id}`)}</span>
        </label>
      </div>
      <div class="run-region-item-meta">方案号: ${escapeHtml(item.schemeNo || "-")} | 分区号: ${escapeHtml(item.regionCode || "-")} | ID: ${item.id} | 围栏点: ${(item.path || []).length}</div>
      <div class="run-region-item-meta">门店集合(${(item.storeIds || []).length}): ${escapeHtml((item.storeNames && item.storeNames.length ? item.storeNames : item.storeIds || []).join("、") || "-")}</div>
      <div class="run-region-item-meta">更新时间: ${escapeHtml(String(item.updatedAt || item.createdAt || "-"))}</div>
      <div class="run-region-item-actions">
        <button class="secondary run-region-edit-btn" data-region-id="${item.id}">编辑</button>
        <button class="alert run-region-delete-btn" data-region-id="${item.id}">删除</button>
      </div>
    </article>
  `).join("");
  mount.innerHTML = `${listHeader}${mount.innerHTML}`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function listRunRegionSchemesFromBackend() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let available = await ensureGaBackendAvailable();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/list`, {}, 6000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!response.ok) return [];
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!data?.ok || !Array.isArray(data.items)) return [];
    return data.items.map((item) => ({
      id: Number(item.id || 0),
      schemeNo: String(item.schemeNo || item.scheme_no || "").trim(),
      name: String(item.name || "").trim(),
      enabled: Boolean(item.enabled),
      createdAt: item.createdAt || item.created_at || "",
      updatedAt: item.updatedAt || item.updated_at || "",
    })).filter((item) => item.schemeNo);
  } catch {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return [];
  }
}

async function listRunRegionsFromBackend(schemeNo) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const scheme = String(schemeNo || "").trim();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!scheme) return [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return [];
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/list?schemeNo=${encodeURIComponent(scheme)}`, {}, 6000);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) return [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const data = await response.json();
    if (!data?.ok || !Array.isArray(data.items)) return [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return data.items.map((item) => ({
      id: Number(item.id || 0),
      schemeNo: String(item.schemeNo || item.scheme_no || "").trim(),
      regionCode: String(item.regionCode || item.region_code || "").trim(),
      name: String(item.name || "").trim(),
      path: normalizeRunRegionPath(item.path || item.polygon || item.polygonPath),
      storeIds: Array.isArray(item.storeIds) ? item.storeIds.map((v) => String(v || "").trim()).filter(Boolean) : [],
      storeNames: Array.isArray(item.storeNames) ? item.storeNames.map((v) => String(v || "").trim()).filter(Boolean) : [],
      createdAt: item.createdAt || item.created_at || "",
      updatedAt: item.updatedAt || item.updated_at || "",
    })).filter((item) => item.id > 0 && item.path.length >= 3);
  } catch {
    return [];
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function generateRunRegionScheme1FromBackend() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let available = await ensureGaBackendAvailable();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/generate-scheme1`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    }, 15000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!response.ok) return false;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return Boolean(data?.ok);
  } catch {
    return false;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function listStorePointsForRunRegion() {
  let available = await ensureGaBackendAvailable();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!available) available = await ensureGaBackendAvailable(true);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) return fallbackRunRegionStorePoints();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/stores/points`, {}, 6000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) return fallbackRunRegionStorePoints();
    const data = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!data?.ok || !Array.isArray(data.items)) return fallbackRunRegionStorePoints();
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const points = data.items
      .map((item) => ({
        store_id: String(item.store_id || item.storeId || ""),
        store_name: String(item.store_name || item.storeName || ""),
        lng: Number(item.lng),
        lat: Number(item.lat),
      }))
      .filter((item) => item.store_id && Number.isFinite(item.lng) && Number.isFinite(item.lat));
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return points.length ? points : fallbackRunRegionStorePoints();
  } catch {
    return fallbackRunRegionStorePoints();
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function createRunRegionOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const data = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!data?.ok) return null;
  return data.item || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function createRunRegionSchemeOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const data = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!data?.ok) return null;
  return data.item || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function updateRunRegionSchemeOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const data = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!data?.ok) return null;
  return data.item || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function deleteRunRegionSchemeOnBackend(schemeId) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id: Number(schemeId || 0) }),
  }, 6000);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!response.ok) return false;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const data = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return Boolean(data?.ok);
}

async function updateRunRegionOnBackend(payload) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  if (!response.ok) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const data = await response.json();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!data?.ok) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return data.item || null;
}

async function deleteRunRegionOnBackend(regionId) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id: Number(regionId || 0) }),
  }, 6000);
  if (!response.ok) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const data = await response.json();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return Boolean(data?.ok);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function refreshRunRegionData(force = false) {
  if ((runRegionCache.loading || runRegionSchemeCache.loading) && !force) return;
  runRegionCache.loading = true;
  runRegionSchemeCache.loading = true;
  renderRunRegionList();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const mapReady = await ensureRunRegionMapReady();
  if (!mapReady && !window.AMap?.Map) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (runRegionMapRetryTimer) clearTimeout(runRegionMapRetryTimer);
    runRegionMapRetryTimer = setTimeout(() => {
      runRegionMapRetryTimer = null;
      void refreshRunRegionData(true);
    }, 1200);
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const [rawSchemes, points] = await Promise.all([
      listRunRegionSchemesFromBackend(),
      listStorePointsForRunRegion(),
    ]);
    let schemes = Array.isArray(rawSchemes) ? rawSchemes : [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!runRegionScheme1AutoGenerated && !schemes.some((item) => item.schemeNo === "1")) {
      const generated = await generateRunRegionScheme1FromBackend();
      runRegionScheme1AutoGenerated = Boolean(generated);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (generated) {
        schemes = await listRunRegionSchemesFromBackend();
      }
    }
    runRegionSchemeCache = { items: schemes, loading: false };
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const savedSchemeNo = String(localStorage.getItem(RUN_REGION_SCHEME_SELECTED_KEY) || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const current = getSelectedRunRegionSchemeNo() || savedSchemeNo;
    const active = schemes.find((item) => item.schemeNo === current) || schemes.find((item) => item.enabled) || schemes[0] || null;
    setSelectedRunRegionSchemeNo(active?.schemeNo || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let regions = await listRunRegionsFromBackend(getSelectedRunRegionSchemeNo());
    if (!runRegionScheme1AutoGenerated && String(active?.schemeNo || "") === "1" && !regions.length) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const generated = await generateRunRegionScheme1FromBackend();
      runRegionScheme1AutoGenerated = Boolean(generated);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (generated) {
        regions = await listRunRegionsFromBackend("1");
      }
    }
    runRegionCache = { items: regions, loading: false };
    ensureRunRegionCheckedSelection(regions);
    runRegionStorePoints = points;
    renderRunRegionList();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (mapReady) {
      renderRunRegionStoreMarkers();
      renderRunRegionPolygons();
      fitRunRegionMapView();
    }
  } catch {
    runRegionCache.loading = false;
    runRegionSchemeCache.loading = false;
  }
  runRegionCache.loading = false;
  runRegionSchemeCache.loading = false;
  renderRunRegionList();
}

async function startRunRegionDraw() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!getSelectedRunRegionSchemeNo()) {
    setRunRegionStatus("请先选择分区方案号。");
    return;
  }
  const mapReady = await ensureRunRegionMapReady();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!mapReady) {
    setRunRegionStatus("地图不可用，无法绘制围栏。");
    return;
  }
  stopRunRegionDrawing(true);
  runRegionEditingId = null;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const nameInput = document.getElementById("runRegionNameInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (nameInput) nameInput.focus();
  if (!window.AMap?.MouseTool) {
    setRunRegionStatus("绘图插件未加载，请刷新页面后重试。");
    return;
  }
  runRegionMouseTool = new window.AMap.MouseTool(runRegionMap);
  runRegionMouseTool.polygon({
    strokeColor: "#ef4444",
    strokeWeight: 2,
    strokeOpacity: 0.95,
    fillColor: "#f59e0b",
    fillOpacity: 0.28,
  });
  runRegionMouseTool.on("draw", (event) => {
    runRegionDraftPolygon = event?.obj || null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (runRegionMouseTool) {
      try { runRegionMouseTool.close(true); } catch {}
      runRegionMouseTool = null;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (runRegionDraftPolygon && window.AMap?.PolygonEditor) {
      runRegionPolygonEditor = new window.AMap.PolygonEditor(runRegionMap, runRegionDraftPolygon);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      try { runRegionPolygonEditor.open(); } catch {}
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const saveBtn = document.getElementById("runRegionSaveBtn");
    if (saveBtn) saveBtn.disabled = false;
    setRunRegionStatus("围栏已绘制，可拖拽编辑后保存。");
  });
  setRunRegionStatus("请在地图上点击绘制运行区围栏。");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function editRunRegion(regionId) {
  const id = String(regionId || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!id) return;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const item = (runRegionCache.items || []).find((x) => String(x.id) === id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!item) return;
  const mapReady = await ensureRunRegionMapReady();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!mapReady) return;
  stopRunRegionDrawing(false);
  const polygon = runRegionPolygons.get(id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!polygon) return;
  runRegionEditingId = Number(id);
  runRegionDraftPolygon = polygon;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (window.AMap?.PolygonEditor) {
    runRegionPolygonEditor = new window.AMap.PolygonEditor(runRegionMap, polygon);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    try { runRegionPolygonEditor.open(); } catch {}
  }
  const nameInput = document.getElementById("runRegionNameInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (nameInput) nameInput.value = item.name || "";
  const saveBtn = document.getElementById("runRegionSaveBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (saveBtn) saveBtn.disabled = false;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  try { runRegionMap.setFitView([polygon], false, [40, 40, 40, 40], 17); } catch {}
  setRunRegionStatus(`正在编辑运行区：${item.name || item.id}`);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function cancelRunRegionEdit() {
  stopRunRegionDrawing(true);
  const nameInput = document.getElementById("runRegionNameInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (nameInput) nameInput.value = "";
  setRunRegionStatus("已取消编辑。");
  await refreshRunRegionData(true);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function saveRunRegionDraft() {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const schemeNo = getSelectedRunRegionSchemeNo();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!schemeNo) {
    setRunRegionStatus("请先选择分区方案号。");
    return;
  }
  const nameInput = document.getElementById("runRegionNameInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const name = String(nameInput?.value || "").trim();
  if (!name) {
    setRunRegionStatus("请先填写运行区名称。");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const path = normalizeRunRegionPath(getPathFromRunRegionPolygon(runRegionDraftPolygon));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (path.length < 3) {
    setRunRegionStatus("围栏点位不足，至少需要3个点。");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) {
    setRunRegionStatus("后端不可用，无法保存运行区。");
    return;
  }
  let saved = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (runRegionEditingId) {
    saved = await updateRunRegionOnBackend({ id: runRegionEditingId, schemeNo, name, path });
  } else {
    saved = await createRunRegionOnBackend({ schemeNo, name, path });
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!saved) {
    setRunRegionStatus("保存失败，请重试。");
    return;
  }
  stopRunRegionDrawing(true);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (nameInput) nameInput.value = "";
  setRunRegionStatus("运行区已保存。");
  await refreshRunRegionData(true);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function removeRunRegion(regionId) {
  const id = Number(regionId || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (id <= 0) return;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const ok = await deleteRunRegionOnBackend(id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!ok) {
    setRunRegionStatus("删除失败，请重试。");
    return;
  }
  if (runRegionEditingId === id) {
    stopRunRegionDrawing(true);
  }
  setRunRegionStatus("运行区已删除。");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  await refreshRunRegionData(true);
}

function bindRunRegionEvents() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const schemeSelect = document.getElementById("runRegionSchemeSelect");
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (schemeSelect) {
    schemeSelect.addEventListener("change", () => {
      setSelectedRunRegionSchemeNo(schemeSelect.value || "");
      runRegionTargetRegionId = "all";
      runRegionCheckedRegionIds = new Set();
      void refreshRunRegionData(true);
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveSchemeSelect = document.getElementById("solveRegionSchemeSelect");
  if (solveSchemeSelect) {
    solveSchemeSelect.addEventListener("change", () => {
      setSelectedRunRegionSchemeNo(solveSchemeSelect.value || "");
      runRegionTargetRegionId = "all";
      runRegionCheckedRegionIds = new Set();
      void refreshRunRegionData(true);
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  document.getElementById("runRegionSchemeCreateBtn")?.addEventListener("click", async () => {
    const schemeNo = String(document.getElementById("runRegionSchemeNoInput")?.value || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const name = String(document.getElementById("runRegionSchemeNameInput")?.value || "").trim();
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const enabled = Boolean(document.getElementById("runRegionSchemeEnabledInput")?.checked);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!schemeNo || !name) {
      setRunRegionStatus("请填写方案号和方案名称。");
      return;
    }
    const created = await createRunRegionSchemeOnBackend({ schemeNo, name, enabled });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!created) {
      setRunRegionStatus("分区方案新增失败，请重试。");
      return;
    }
    setSelectedRunRegionSchemeNo(created.schemeNo || schemeNo);
    setRunRegionStatus("分区方案已新增。");
    await refreshRunRegionData(true);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("runRegionSchemeUpdateBtn")?.addEventListener("click", async () => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const selected = (runRegionSchemeCache.items || []).find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!selected) {
      setRunRegionStatus("请先选择要更新的分区方案。");
      return;
    }
    const name = String(document.getElementById("runRegionSchemeNameInput")?.value || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const enabled = Boolean(document.getElementById("runRegionSchemeEnabledInput")?.checked);
    if (!name) {
      setRunRegionStatus("请填写方案名称。");
      return;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const updated = await updateRunRegionSchemeOnBackend({ id: selected.id, name, enabled });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!updated) {
      setRunRegionStatus("分区方案更新失败，请重试。");
      return;
    }
    setRunRegionStatus("分区方案已更新。");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await refreshRunRegionData(true);
  });
  document.getElementById("runRegionSchemeDeleteBtn")?.addEventListener("click", async () => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const selected = (runRegionSchemeCache.items || []).find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
    if (!selected) {
      setRunRegionStatus("请先选择要删除的分区方案。");
      return;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const ok = await deleteRunRegionSchemeOnBackend(selected.id);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!ok) {
      setRunRegionStatus("删除分区方案失败（可能仍有围栏绑定）。");
      return;
    }
    setSelectedRunRegionSchemeNo("");
    setRunRegionStatus("分区方案已删除。");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await refreshRunRegionData(true);
  });
  const modeSelect = document.getElementById("runRegionStoreVisibilityMode");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (modeSelect) {
    runRegionStoreVisibilityMode = normalizeRunRegionStoreVisibilityMode(modeSelect.value);
    modeSelect.addEventListener("change", () => {
      runRegionStoreVisibilityMode = normalizeRunRegionStoreVisibilityMode(modeSelect.value);
      renderRunRegionStoreMarkers();
      renderRunRegionPolygons();
      fitRunRegionMapView();
    });
  }
  const targetRegionSelect = document.getElementById("runRegionTargetRegion");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (targetRegionSelect) {
    runRegionTargetRegionId = String(targetRegionSelect.value || "all");
    targetRegionSelect.addEventListener("change", () => {
      runRegionTargetRegionId = String(targetRegionSelect.value || "all");
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (runRegionTargetRegionId === "all") {
        runRegionCheckedRegionIds = new Set((runRegionCache.items || []).map((item) => String(item.id)));
      } else {
        runRegionCheckedRegionIds = new Set([String(runRegionTargetRegionId)]);
      }
      renderRunRegionList();
      renderRunRegionStoreMarkers();
      renderRunRegionPolygons();
      fitRunRegionMapView();
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("runRegionDrawBtn")?.addEventListener("click", () => {
    void startRunRegionDraw();
  });
  document.getElementById("runRegionSaveBtn")?.addEventListener("click", () => {
    void saveRunRegionDraft();
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  document.getElementById("runRegionCancelBtn")?.addEventListener("click", () => {
    void cancelRunRegionEdit();
  });
  document.getElementById("runRegionRefreshBtn")?.addEventListener("click", () => {
    void refreshRunRegionData(true);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("runRegionList")?.addEventListener("click", (event) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const editBtn = event.target.closest(".run-region-edit-btn");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (editBtn) {
      void editRunRegion(editBtn.dataset.regionId);
      return;
    }
    const deleteBtn = event.target.closest(".run-region-delete-btn");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (deleteBtn) {
      void removeRunRegion(deleteBtn.dataset.regionId);
    }
  });
  document.getElementById("runRegionList")?.addEventListener("change", (event) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const target = event.target;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!(target instanceof HTMLInputElement)) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (target.id === "runRegionCheckAll") {
      if (target.checked) {
        runRegionCheckedRegionIds = new Set((runRegionCache.items || []).map((item) => String(item.id)));
      } else {
        runRegionCheckedRegionIds = new Set();
      }
      runRegionTargetRegionId = "all";
      renderRunRegionList();
      renderRunRegionStoreMarkers();
      renderRunRegionPolygons();
      fitRunRegionMapView();
      return;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (target.classList.contains("run-region-visibility-check")) {
      const regionId = String(target.dataset.regionId || "").trim();
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!regionId) return;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (target.checked) runRegionCheckedRegionIds.add(regionId);
      else runRegionCheckedRegionIds.delete(regionId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const checkedIds = Array.from(runRegionCheckedRegionIds.values());
      runRegionTargetRegionId = checkedIds.length === 1 ? checkedIds[0] : "all";
      renderRunRegionList();
      renderRunRegionStoreMarkers();
      renderRunRegionPolygons();
      fitRunRegionMapView();
    }
  });
}

async function refreshArchiveBackendCache(date, page = 1, pageSize = 6) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!date) return;
  if (archiveBackendCache.loading) return;
  archiveBackendCache.loading = true;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const remote = await listRunArchivesFromBackend(date, page, pageSize);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!remote) return;
    archiveBackendCache = {
      date,
      page: remote.page,
      items: remote.items,
      totalPages: Math.max(1, remote.totalPages),
      total: remote.total,
      loading: false,
    };
    renderSavedPlans();
  } finally {
    archiveBackendCache.loading = false;
  }
}

function _debugExtractStoreId(item) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (item && typeof item === "object") {
    const keys = ["storeId", "store_id", "id", "code", "shop_code", "shopCode", "storeCode"];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const key of keys) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const value = item[key];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (value !== undefined && value !== null && String(value).trim()) return normalizeStoreCode(value);
    }
  }
  return normalizeStoreCode(item);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildGaBackendPayload(initialPlans, scenario, wave, randomSeed = 203) {
  const backendStrategyConfig = buildBackendStrategyConfig(state.strategyConfig);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const requireNumberForBackend = (store, field) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const value = Number(store?.[field]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!Number.isFinite(value)) {
      throw new Error(`missing_${field}:${store?.id || ""}:${wave?.waveId || ""}`);
    }
    return value;
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const candidateStoreIds = (wave.storeList || []).filter((storeId) => {
    const store = scenario.storeMap.get(storeId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!store) return false;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return isStoreCandidateForWaveRule(store, wave?.waveId || "");
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const relevantIds = [DC.id, ...candidateStoreIds];
  const dist = {};
  relevantIds.forEach((fromId) => {
    dist[fromId] = {};
    relevantIds.forEach((toId) => {
      dist[fromId][toId] = Number(scenario?.dist?.[fromId]?.[toId] || 0);
    });
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const stores = candidateStoreIds.map((storeId) => {
    const store = scenario.storeMap.get(storeId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!store) throw new Error(`missing_store:${storeId}:${wave?.waveId || ""}`);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {
      id: store.id,
      name: store.name,
      boxes: requireNumberForBackend(store, "boxes"),
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      desiredArrivalMin: Number(timing.desiredArrivalMin),
      latestAllowedArrivalMin: Number(timing.latestAllowedArrivalMin),
      actualServiceMinutes: requireNumberForBackend(store, "actualServiceMinutes"),
      serviceMinutes: requireNumberForBackend(store, "serviceMinutes"),
      coldRatio: requireNumberForBackend(store, "coldRatio"),
      difficulty: requireNumberForBackend(store, "difficulty"),
      parking: requireNumberForBackend(store, "parking"),
    };
  });
  const waveIdForSolve = String(wave?.waveId || "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const solveSpeedKmh = waveIdForSolve === "W3"
    ? Number(backendStrategyConfig.w3SpeedKmh || 48)
    : Number(backendStrategyConfig.defaultSpeedKmh || 38);
  const solveCapacity = Math.max(0.1, Number(backendStrategyConfig.maxSolveCapacity || 1));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicles = initialPlans.map((plan) => ({
    plateNo: plan.vehicle.plateNo,
    type: ENFORCED_VEHICLE_TYPE,
    vehicle_type: ENFORCED_VEHICLE_TYPE,
    capacity: solveCapacity,
    solveCapacity,
    speed: solveSpeedKmh,
    canCarryCold: Boolean(plan.vehicle.canCarryCold),
    priorRegularDistance: Number(plan.priorRegularDistance || 0),
    priorWaveCount: Number(plan.priorWaveCount || 0),
    earliestDepartureMin: Number(plan.earliestDepartureMin || wave.startMin || scenario.dispatchStartMin),
    routes: (plan.trips || []).map((trip) => [...(trip.route || [])]),
  }));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    randomSeed,
    regionSchemeNo: getSelectedRunRegionSchemeNo(),
    useRecommendedPlan: Boolean(recommendedPlanSelectedCache.selected),
    recommendedPlanTaskDate: recommendedPlanSelectedCache.taskDate || getCurrentTaskDate(),
    strategyConfig: backendStrategyConfig,
    compareMode: (state.settings.solveStrategy || "manual") === "compare",
    optimizeGoal: state.settings.optimizeGoal || "balanced",
    scenario: {
      dispatchStartMin: Number(scenario.dispatchStartMin || 0),
      maxRouteKm: Number(scenario.maxRouteKm || 220),
      concentrateLate: Boolean(scenario.concentrateLate),
    },
    wave: {
      waveId: wave.waveId,
      startMin: Number(wave.startMin || 0),
      endMin: Number(wave.endMin || 0),
      endMode: wave.endMode || "return",
      relaxEnd: Boolean(wave.relaxEnd),
      singleWave: Boolean(wave.singleWave),
      isNightWave: Boolean(wave.isNightWave),
      earliestDepartureMin: Number(wave.earliestDepartureMin || wave.startMin || 0),
    },
    stores,
    vehicles,
    dist,
    distDbStats: {
      ...(scenario?.distDbStats || {}),
      fullMatrix: Boolean(scenario?.distanceSource === "database-full" && Number(scenario?.distDbStats?.missingCount || 0) === 0),
      missingCount: Number(scenario?.distDbStats?.missingCount ?? -1),
    },
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildWaveOptimizerBackendPayload(initialPlans, scenario, wave, algorithmKey, randomSeed = 203) {
  return {
    ...buildGaBackendPayload(initialPlans, scenario, wave, randomSeed),
    algorithmKey,
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function sanitizeBackendRoutes(routes = [], allowedStoreSet = new Set()) {
  const seen = new Set();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return (Array.isArray(routes) ? routes : [])
    .map((route) => (Array.isArray(route) ? route : []))
    .map((route) => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const removed = [];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const sanitized = route.filter((storeId) => {
        const normalized = String(storeId ?? "").trim();
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (!normalized || !allowedStoreSet.has(normalized) || seen.has(normalized)) {
          removed.push({
            raw: storeId,
            normalized,
            inAllowed: allowedStoreSet.has(normalized),
            duplicated: seen.has(normalized),
          });
          return false;
        }
        seen.add(normalized);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return true;
      }).map((storeId) => String(storeId).trim());
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (removed.length) console.log("[W3] sanitize过滤:", removed);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return sanitized;
    })
    .filter((route) => route.length);
}

function buildAllowedStoreSetForBackendRebuild(wave, strategyAudit = null, scenario = null) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const storeMap = scenario?.storeMap instanceof Map ? scenario.storeMap : null;
  const base = new Set((wave?.storeList || []).map((storeId) => String(storeId).trim()).filter((storeId) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!storeId) return false;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!storeMap) return true;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const store = storeMap.get(storeId);
    if (!store) return false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return isStoreCandidateForWaveRule(store, wave?.waveId || "");
  }));
  if (!strategyAudit || typeof strategyAudit !== "object") return base;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const removed = new Set([
    ...(Array.isArray(strategyAudit.filteredZeroLoadStoreIds) ? strategyAudit.filteredZeroLoadStoreIds : []),
    ...(Array.isArray(strategyAudit.filteredWaveScopeStoreIds) ? strategyAudit.filteredWaveScopeStoreIds : []),
  ].map((storeId) => String(storeId || "").trim()).filter(Boolean));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!removed.size) return base;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const filtered = new Set();
  base.forEach((storeId) => {
    if (!removed.has(storeId)) filtered.add(storeId);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (String(wave?.waveId || "").trim().toUpperCase() === "W3") {
    console.log("[W3] allowedStoreSet size:", filtered.size);
    console.log("[W3] allowedStoreSet sample:", [...filtered].slice(0, 20));
  }
  return filtered;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function collectAssignedStoreSetFromRouteState(routeState = [], allowedStoreSet = null) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const assigned = new Set();
  (Array.isArray(routeState) ? routeState : []).forEach((stateItem) => {
    (stateItem?.routes || []).forEach((route) => {
      (route || []).forEach((sid) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const nsid = String(sid ?? "").trim();
        if (!nsid) return;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (allowedStoreSet instanceof Set && !allowedStoreSet.has(nsid)) return;
        assigned.add(nsid);
      });
    });
  });
  return assigned;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function collectAssignedStoreSetFromPlans(plans = [], allowedStoreSet = null) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const assigned = new Set();
  (Array.isArray(plans) ? plans : []).forEach((plan) => {
    (plan?.trips || []).forEach((trip) => {
      (trip?.route || []).forEach((sid) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const nsid = String(sid ?? "").trim();
        if (!nsid) return;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (allowedStoreSet instanceof Set && !allowedStoreSet.has(nsid)) return;
        assigned.add(nsid);
      });
    });
  });
  return assigned;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function computePendingStoreIds(candidateStoreSet = new Set(), assignedStoreSet = new Set()) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return [...candidateStoreSet].filter((storeId) => !assignedStoreSet.has(storeId));
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function reportRebuildSetStats(waveId, stage, candidateStoreSet, assignedStoreSet, pendingStoreIds) {
  const waveTag = String(waveId || "-");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const candidateCount = candidateStoreSet instanceof Set ? candidateStoreSet.size : 0;
  const assignedCount = assignedStoreSet instanceof Set ? assignedStoreSet.size : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const pendingCount = Array.isArray(pendingStoreIds) ? pendingStoreIds.length : 0;
  reportRelayStageProgress(`集合核对（${waveTag}/${stage}）：candidate=${candidateCount}，assigned=${assignedCount}，pending=${pendingCount}。`);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function rebuildWavePlansFromBackendState(bestState, initialPlans, scenario, wave, strategyAudit = null) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!Array.isArray(bestState) || !bestState.length) return null;
  const normalizePlateNo = (plateNo) => String(plateNo || "").replace(/\s+/g, "").trim().toUpperCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const backendByPlate = new Map(
    bestState
      .map((item) => ({ key: normalizePlateNo(item?.plateNo), item }))
      .filter((row) => row.key)
      .map((row) => [row.key, row.item]),
  );
  const allowedStoreSet = buildAllowedStoreSetForBackendRebuild(wave, strategyAudit, scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (String(wave?.waveId || "").trim().toUpperCase() === "W3" && (!strategyAudit || typeof strategyAudit !== "object")) {
    console.log("[W3] allowedStoreSet size:", allowedStoreSet.size);
    console.log("[W3] allowedStoreSet sample:", [...allowedStoreSet].slice(0, 20));
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const baselineState = cloneWaveRouteState(initialPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const fallbackFailures = [];
  let routeState = initialPlans.map((plan, index) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const planPlate = normalizePlateNo(plan?.vehicle?.plateNo);
    const backendItem = (planPlate && backendByPlate.get(planPlate)) || {};
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!backendItem?.plateNo) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const hasBaselineRoutes = Array.isArray(baselineState[index]?.routes) && baselineState[index].routes.length > 0;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (hasBaselineRoutes) {
        reportRelayStageProgress(`后端回填提示（${wave?.waveId || "-"}）：车牌 ${plan?.vehicle?.plateNo || "-"} 未在 bestState 匹配，回退基线线路。`);
      }
    }
    const sanitizedRoutes = Array.isArray(backendItem.routes)
      ? sanitizeBackendRoutes(backendItem.routes, allowedStoreSet)
      : baselineState[index].routes.map((route) => [...route]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (String(wave?.waveId || "").trim().toUpperCase() === "W3") {
      console.log("[W3] 车辆", backendItem.plateNo || plan?.vehicle?.plateNo || "-", "原始routes:", backendItem.routes);
      console.log("[W3] 车辆", backendItem.plateNo || plan?.vehicle?.plateNo || "-", "sanitized后:", sanitizedRoutes);
    }
    const backendSpeed = Number(backendItem.speed);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const vehicle = {
      ...plan.vehicle,
      speed: Number.isFinite(backendSpeed) && backendSpeed > 0 ? backendSpeed : plan.vehicle.speed,
    };
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      vehicle,
      priorRegularDistance: Number(backendItem.priorRegularDistance ?? plan.priorRegularDistance ?? 0),
      priorWaveCount: Number(backendItem.priorWaveCount ?? plan.priorWaveCount ?? 0),
      earliestDepartureMin: Number(backendItem.earliestDepartureMin ?? plan.earliestDepartureMin ?? wave.startMin),
      routes: sanitizedRoutes,
    };
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const seen = new Set();
  routeState.forEach((stateItem) => {
    stateItem.routes = (stateItem.routes || [])
      .map((route) => route.filter((storeId) => {
        if (!allowedStoreSet.has(storeId) || seen.has(storeId)) return false;
        seen.add(storeId);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        return true;
      }))
      .filter((route) => route.length);
  });
  routeState = routeState.map((stateItem, index) => {
    if (rebuildSingleStatePlan(stateItem, scenario, wave)) return stateItem;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const plateNo = String(stateItem?.vehicle?.plateNo || baselineState[index]?.vehicle?.plateNo || "").trim();
    fallbackFailures.push(plateNo || `vehicle#${index + 1}`);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      ...baselineState[index],
      routes: sanitizeBackendRoutes(baselineState[index].routes.map((route) => [...route]), allowedStoreSet),
    };
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let preRebuildAssigned = collectAssignedStoreSetFromRouteState(routeState, allowedStoreSet);
  let preRebuildPending = computePendingStoreIds(allowedStoreSet, preRebuildAssigned);
  reportRebuildSetStats(wave?.waveId || "-", "重建前", allowedStoreSet, preRebuildAssigned, preRebuildPending);

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let preRebuildRound = 0;
  while (preRebuildPending.length && preRebuildRound < 6) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const repaired = insertStoresIntoRouteState(routeState, preRebuildPending, scenario, wave, { candidateVehicleLimit: 8 });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!repaired) {
      reportRelayStageProgress(`重建补排第${preRebuildRound + 1}轮无可行插入位，仍有 ${preRebuildPending.length} 家待排。`);
      break;
    }
    routeState = repaired;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextAssigned = collectAssignedStoreSetFromRouteState(routeState, allowedStoreSet);
    const nextPending = computePendingStoreIds(allowedStoreSet, nextAssigned);
    reportRebuildSetStats(wave?.waveId || "-", `重建补排第${preRebuildRound + 1}轮后`, allowedStoreSet, nextAssigned, nextPending);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (nextPending.length >= preRebuildPending.length) {
      reportRelayStageProgress(`重建补排第${preRebuildRound + 1}轮无进展（待排 ${preRebuildPending.length}→${nextPending.length}），停止继续补排。`);
      preRebuildAssigned = nextAssigned;
      preRebuildPending = nextPending;
      break;
    }
    preRebuildAssigned = nextAssigned;
    preRebuildPending = nextPending;
    preRebuildRound += 1;
  }

  let rebuiltPlans = rebuildWavePlansFromState(routeState, scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!rebuiltPlans) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const fallbackNote = fallbackFailures.length
      ? `，重建失败回退车辆 ${fallbackFailures.length} 台（示例：${fallbackFailures.slice(0, 6).join("、")}）`
      : "";
    reportRelayStageProgress(`后端结果重建失败：allowed=${allowedStoreSet.size}，已分配=${preRebuildAssigned.size}，待补=${preRebuildPending.length}${fallbackNote}。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return null;
  }

  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。
  let secondRound = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  while (secondRound < 6) {
    const secondAssigned = collectAssignedStoreSetFromPlans(rebuiltPlans, allowedStoreSet);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const secondPending = computePendingStoreIds(allowedStoreSet, secondAssigned);
    reportRebuildSetStats(wave?.waveId || "-", secondRound === 0 ? "二次补排前" : `二次补排第${secondRound}轮前`, allowedStoreSet, secondAssigned, secondPending);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!secondPending.length) break;

    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const retryState = cloneWaveRouteState(rebuiltPlans);
    const secondRepaired = insertStoresIntoRouteState(retryState, secondPending, scenario, wave, { candidateVehicleLimit: 8 });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!secondRepaired) {
      reportRelayStageProgress(`二次补排第${secondRound + 1}轮无可行插入位，仍有 ${secondPending.length} 家待排。`);
      break;
    }
    const secondPlans = rebuildWavePlansFromState(secondRepaired, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!secondPlans) {
      reportRelayStageProgress(`二次补排第${secondRound + 1}轮未通过重建校验，仍有 ${secondPending.length} 家待排。`);
      break;
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const afterAssigned = collectAssignedStoreSetFromPlans(secondPlans, allowedStoreSet);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const afterPending = computePendingStoreIds(allowedStoreSet, afterAssigned);
    reportRebuildSetStats(wave?.waveId || "-", `二次补排第${secondRound + 1}轮后`, allowedStoreSet, afterAssigned, afterPending);
    rebuiltPlans = secondPlans;
    if (afterPending.length >= secondPending.length) {
      reportRelayStageProgress(`二次补排第${secondRound + 1}轮无进展（待排 ${secondPending.length}→${afterPending.length}），停止继续补排。`);
      break;
    }
    secondRound += 1;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return rebuiltPlans;
}

async function tryOptimizeWaveWithPythonGA(initialPlans, scenario, wave, randomSeed = 203) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let available = await ensureGaBackendAvailable();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await sleep(180);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) {
    reportRelayStageProgress(`GA Python 后台当前不可达，${wave.waveId} 先回退到前端遗传算法。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return null;
  }
  const payload = buildGaBackendPayload(initialPlans, scenario, wave, randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    reportRelayStageProgress(`GA ${wave.waveId} 正在切到 Python 后台求解，浏览器主线程不再硬扛这一棒。`);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/ga-optimize-wave`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }, 600000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!response.ok) {
      await sleep(220);
      response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/ga-optimize-wave`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }, 600000);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    reportStrategyAuditToRelayConsole(data.strategyAudit, "遗传算法（GA）");
    reportBackendUnscheduledToRelayConsole(data.unscheduledStores, "遗传算法（GA）", wave.waveId || "-");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const plans = rebuildWavePlansFromBackendState(data.bestState || [], initialPlans, scenario, wave, data.strategyAudit || null);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (Array.isArray(data.traceLog) && data.traceLog.length) {
      data.traceLog.forEach((event) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const message = (lang() === "ja" ? event?.textJa : event?.textZh) || event?.textZh || event?.textJa || "";
        if (message) reportRelayStageProgress(message);
      });
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!plans) {
      const backendRouteCount = Array.isArray(data.bestState)
        ? data.bestState.reduce((sum, item) => sum + ((item?.routes || []).length || 0), 0)
        : 0;
      throw new Error(`Backend state rebuild failed (vehicles=${Array.isArray(data.bestState) ? data.bestState.length : 0}, routes=${backendRouteCount})`);
    }
    reportRelayStageProgress(`GA ${wave.waveId} 已从 Python 后台成功接回，当前波次不用再回退前端遗传算法。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {
      plans,
      traceLog: Array.isArray(data.traceLog) ? data.traceLog : [],
      unscheduledStores: Array.isArray(data.unscheduledStores) ? data.unscheduledStores : [],
    };
  } catch (error) {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
    reportRelayStageProgress(`GA Python 后台本轮失败（${error?.message || "unknown"}），本轮已中止，不再回退前端。`);
    throw new Error(`GA_BACKEND_REQUIRED:${error?.message || "unknown"}`);
  }
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, algorithmKey, randomSeed = 203) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (STRICT_ALGO_TRUTH_MODE) {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} 已切回前端真实算法求解（真实性优先模式），本轮不走 Python 近似后端。`);
    return null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let available = await ensureGaBackendAvailable();
  if (!available) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await sleep(180);
    available = await ensureGaBackendAvailable(true);
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!available) {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} Python 后台当前不可达。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (BACKEND_ONLY_REAL_ALGOS.has(String(algorithmKey || ""))) {
      throw new Error(`${algorithmKey}_BACKEND_REQUIRED:unreachable`);
    }
    return null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const payload = buildWaveOptimizerBackendPayload(initialPlans, scenario, wave, algorithmKey, randomSeed);
  try {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} ${wave.waveId} 正在切到 Python 后台求解，浏览器主线程不再硬扛这一棒。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wave-optimize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }, 600000);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!response.ok) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      await sleep(220);
      response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wave-optimize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }, 600000);
    }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const data = await response.json();
    reportStrategyAuditToRelayConsole(data.strategyAudit, algoLabel(algorithmKey));
    reportBackendUnscheduledToRelayConsole(data.unscheduledStores, algoLabel(algorithmKey), wave.waveId || "-");
    const plans = rebuildWavePlansFromBackendState(data.bestState || [], initialPlans, scenario, wave, data.strategyAudit || null);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (Array.isArray(data.traceLog) && data.traceLog.length) {
      data.traceLog.forEach((event) => {
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const message = (lang() === "ja" ? event?.textJa : event?.textZh) || event?.textZh || event?.textJa || "";
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (message) reportRelayStageProgress(message);
      });
    }
    if (["vrptw", "savings"].includes(String(algorithmKey || "").trim())) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const iterTag = `${String(algorithmKey || "").trim()}-python-iteration`;
      const hasConstructiveIteration = Array.isArray(data.traceLog)
        && data.traceLog.some((event) => String(event?.stage || "").trim() === iterTag);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!hasConstructiveIteration) {
        throw new Error(`${algorithmKey}_BACKEND_STALE_OR_INVALID_TRACE`);
      }
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!plans) throw new Error("Backend state rebuild failed");
    reportRelayStageProgress(`${algoLabel(algorithmKey)} ${wave.waveId} 已从 Python 后台成功接回，当前波次不用再回退前端 ${algoLabel(algorithmKey)}。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {
      plans,
      traceLog: Array.isArray(data.traceLog) ? data.traceLog : [],
      unscheduledStores: Array.isArray(data.unscheduledStores) ? data.unscheduledStores : [],
    };
  } catch (error) {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
    const raw = String(error?.message || "unknown");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const staleHint = raw.includes("_BACKEND_STALE_OR_INVALID_TRACE")
      ? "后端返回的日志结构无效（大概率是旧进程未重启），请重启 GA 后台后重试。"
      : raw;
    reportRelayStageProgress(`${algoLabel(algorithmKey)} Python 后台本轮失败（${staleHint}）。`);
    if (BACKEND_ONLY_REAL_ALGOS.has(String(algorithmKey || ""))) {
      throw new Error(`${algorithmKey}_BACKEND_REQUIRED:${error?.message || "unknown"}`);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return null;
  }
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithGA(initialPlans, scenario, wave, randomSeed = 203) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonGA(initialPlans, scenario, wave, randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now());
  const random = createSeededRandom(randomSeed);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const initialState = cloneWaveRouteState(initialPlans);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const traceLog = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const isCompareMode = (state.settings.solveStrategy || "manual") === "compare";
  const populationSize = isCompareMode ? 6 : 12;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const generations = isCompareMode ? 8 : 22;
  const eliteCount = isCompareMode ? 2 : 3;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stagnationLimit = isCompareMode ? 3 : 6;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const improvementThreshold = isCompareMode ? 0.2 : 0.1;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const seedImproveRounds = isCompareMode ? 0 : 2;
  const offspringImproveRounds = isCompareMode ? 0 : 2;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const immigrantImproveRounds = isCompareMode ? 0 : 1;
  const population = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const seen = new Set();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const profile = { seedMs: 0, crossoverMs: 0, offspringEvalMs: 0, immigrantMs: 0 };

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  function addIndividual(state) {
    const signature = hashRouteState(state);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (seen.has(signature)) return false;
    const improved = localImproveState(state, scenario, wave, random, seedImproveRounds) || evaluateRouteState(state, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!improved) return false;
    seen.add(signature);
    population.push({ state: cloneWaveRouteState(improved.plans), plans: improved.plans, cost: improved.cost, signature });
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return true;
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const seedStart = now();
  addIndividual(initialState);
  for (let guard = 0; guard < (isCompareMode ? 24 : 80) && population.length < populationSize; guard += 1) {
    addIndividual(mutateRouteState(initialState, scenario, wave, random, 1 + Math.floor(random() * 4)));
  }
  profile.seedMs += now() - seedStart;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!population.length) return { plans: initialPlans, traceLog };

  pushTraceEvent(traceLog, {
    algorithmKey: "ga",
    scope: "wave",
    waveId: wave.waveId,
    stage: "ga-start",
    bestCost: Math.min(...population.map((item) => item.cost)),
    textZh: isCompareMode ? `遗传算法已生成对比种群 ${population.length} 个个体，按轻量对比预算开始做选择 / 交叉 / 变异。` : `遗传算法已生成初始种群 ${population.length} 个个体，开始做选择 / 交叉 / 变异。`,
    textJa: isCompareMode ? `遺伝的アルゴリズムは比較用の軽量集団 ${population.length} 個体を生成し、選択 / 交叉 / 変異を開始します。` : `遺伝的アルゴリズムは初期集団 ${population.length} 個体を生成し、選択 / 交叉 / 変異を開始します。`,
  });
  if (isCompareMode) {
    pushTraceEvent(traceLog, {
      algorithmKey: "ga",
      scope: "wave",
      waveId: wave.waveId,
      stage: "ga-profile-seed",
      textZh: `GA 性能剖析已开启：初始种群阶段耗时约 ${(profile.seedMs / 1000).toFixed(2)} 秒。`,
      textJa: `GA の性能プロファイルを有効化しました。初期集団の所要時間は約 ${(profile.seedMs / 1000).toFixed(2)} 秒です。`,
    });
    reportRelayStageProgress(`GA 剖析启动：${wave.waveId} 初始种群阶段耗时约 ${(profile.seedMs / 1000).toFixed(2)} 秒。`);
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let bestCostSeen = Infinity;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let stagnantGenerations = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let generation = 0; generation < generations; generation += 1) {
    const generationStart = now();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    let generationCrossoverMs = 0;
    let generationOffspringEvalMs = 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let generationImmigrantMs = 0;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (generation && generation % 2 === 0) await cooperativeYield();
    population.sort((a, b) => a.cost - b.cost);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextPopulation = population.slice(0, eliteCount).map((item) => ({ ...item, state: cloneWaveRouteState(item.state), plans: item.plans.map((plan) => clone(plan)) }));
    let offspringAttempts = 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const maxOffspringAttempts = isCompareMode ? populationSize * 18 : populationSize * 36;
    while (nextPopulation.length < populationSize && offspringAttempts < maxOffspringAttempts) {
      offspringAttempts += 1;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const parentA = tournamentSelect(population, random, 3);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const parentB = tournamentSelect(population, random, 3);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const crossoverStart = now();
      let childState = crossoverRouteStates(parentA.state, parentB.state, scenario, wave, random);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!childState) childState = mutateRouteState(parentA.state, scenario, wave, random, 2);
      if (random() < 0.85) childState = mutateRouteState(childState, scenario, wave, random, random() < 0.6 ? 1 : 2);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const crossoverSpent = now() - crossoverStart;
      generationCrossoverMs += crossoverSpent;
      profile.crossoverMs += crossoverSpent;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const evalStart = now();
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const improved = localImproveState(childState, scenario, wave, random, offspringImproveRounds) || evaluateRouteState(childState, scenario, wave);
      const evalSpent = now() - evalStart;
      generationOffspringEvalMs += evalSpent;
      profile.offspringEvalMs += evalSpent;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!improved) continue;
      nextPopulation.push({
        state: cloneWaveRouteState(improved.plans),
        plans: improved.plans,
        cost: improved.cost,
        signature: hashRouteState(childState),
      });
    }
    if (nextPopulation.length < populationSize) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const fallbackSource = nextPopulation[0] || population[0];
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      while (fallbackSource && nextPopulation.length < populationSize) {
        nextPopulation.push({
          ...fallbackSource,
          state: cloneWaveRouteState(fallbackSource.state),
          plans: fallbackSource.plans.map((plan) => clone(plan)),
        });
      }
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-guard-fill",
        generation,
        textZh: `第 ${generation + 1} 代后代生成尝试达到上限 ${maxOffspringAttempts} 次，仍未填满种群，本代剩余个体改用当前最优个体补位，避免陷入无休止尝试。`,
        textJa: `第 ${generation + 1} 世代では子個体生成の試行が上限 ${maxOffspringAttempts} 回に達しても集団を埋めきれなかったため、残りは現時点の最良個体で補完し、無限試行を防ぎます。`,
      });
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (isCompareMode) {
        reportRelayStageProgress(`GA ${wave.waveId} 第 ${generation + 1} 代子代生成已尝试 ${maxOffspringAttempts} 次仍未填满种群，已改用当前最优个体补位，避免卡死。`);
      }
    }
    if (generation % 5 === 4 && population.length) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const immigrantStart = now();
      const immigrant = mutateRouteState(population[0].state, scenario, wave, random, 3);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const immigrantEval = localImproveState(immigrant, scenario, wave, random, immigrantImproveRounds) || evaluateRouteState(immigrant, scenario, wave);
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const immigrantSpent = now() - immigrantStart;
      generationImmigrantMs += immigrantSpent;
      profile.immigrantMs += immigrantSpent;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (immigrantEval) {
        nextPopulation[nextPopulation.length - 1] = {
          state: cloneWaveRouteState(immigrantEval.plans),
          plans: immigrantEval.plans,
          cost: immigrantEval.cost,
          signature: hashRouteState(immigrant),
        };
      }
    }
    population.splice(0, population.length, ...nextPopulation);
    population.sort((a, b) => a.cost - b.cost);
    const generationBest = population[0].cost;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (bestCostSeen - generationBest >= improvementThreshold) {
      bestCostSeen = generationBest;
      stagnantGenerations = 0;
    } else {
      stagnantGenerations += 1;
      bestCostSeen = Math.min(bestCostSeen, generationBest);
    }
    if (generation < 10) {
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-generation",
        generation,
        bestCost: generationBest,
        textZh: `第 ${generation + 1} 代完成选择、交叉与变异，当前种群最优成本 ${generationBest.toFixed(1)}。`,
        textJa: `${generation + 1} 世代で選択・交叉・変異を完了し、現在の集団最良コストは ${generationBest.toFixed(1)} です。`,
      });
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (isCompareMode) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const generationTotalMs = now() - generationStart;
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-profile-generation",
        generation,
        bestCost: generationBest,
        textZh: `GA 第 ${generation + 1} 代耗时约 ${(generationTotalMs / 1000).toFixed(2)} 秒，其中交叉/修复 ${(generationCrossoverMs / 1000).toFixed(2)} 秒，子代评估 ${(generationOffspringEvalMs / 1000).toFixed(2)} 秒，移民 ${(generationImmigrantMs / 1000).toFixed(2)} 秒。`,
        textJa: `GA 第 ${generation + 1} 世代の所要時間は約 ${(generationTotalMs / 1000).toFixed(2)} 秒で、内訳は交叉/修復 ${(generationCrossoverMs / 1000).toFixed(2)} 秒、子代評価 ${(generationOffspringEvalMs / 1000).toFixed(2)} 秒、移民 ${(generationImmigrantMs / 1000).toFixed(2)} 秒です。`,
      });
      reportRelayStageProgress(`GA ${wave.waveId} 第 ${generation + 1} 代耗时约 ${(generationTotalMs / 1000).toFixed(2)} 秒，其中交叉/修复 ${(generationCrossoverMs / 1000).toFixed(2)} 秒，子代评估 ${(generationOffspringEvalMs / 1000).toFixed(2)} 秒，移民 ${(generationImmigrantMs / 1000).toFixed(2)} 秒。当前最优代价 ${generationBest.toFixed(1)}。`);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (isCompareMode && stagnantGenerations >= stagnationLimit) {
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-early-stop",
        generation,
        bestCost: generationBest,
        textZh: `连续 ${stagnationLimit} 代提升不足 ${improvementThreshold.toFixed(1)}，对比模式下提前收工，避免继续空转。`,
        textJa: `${stagnationLimit} 世代連続で改善幅が ${improvementThreshold.toFixed(1)} 未満のため、比較モードでは早めに打ち切って空転を防ぎます。`,
      });
      break;
    }
  }

  population.sort((a, b) => a.cost - b.cost);
  pushTraceEvent(traceLog, {
    algorithmKey: "ga",
    scope: "wave",
    waveId: wave.waveId,
    stage: "ga-finish",
    bestCost: population[0].cost,
    textZh: `遗传算法结束，最终种群最优成本 ${population[0].cost.toFixed(1)}。`,
    textJa: `遺伝的アルゴリズムが終了し、最終集団の最良コストは ${population[0].cost.toFixed(1)} です。`,
  });
  if (isCompareMode) {
    pushTraceEvent(traceLog, {
      algorithmKey: "ga",
      scope: "wave",
      waveId: wave.waveId,
      stage: "ga-profile-summary",
      bestCost: population[0].cost,
      textZh: `GA 剖析汇总：初始种群 ${(profile.seedMs / 1000).toFixed(2)} 秒，交叉/修复 ${(profile.crossoverMs / 1000).toFixed(2)} 秒，子代评估 ${(profile.offspringEvalMs / 1000).toFixed(2)} 秒，移民 ${(profile.immigrantMs / 1000).toFixed(2)} 秒。`,
      textJa: `GA プロファイル集計：初期集団 ${(profile.seedMs / 1000).toFixed(2)} 秒、交叉/修復 ${(profile.crossoverMs / 1000).toFixed(2)} 秒、子代評価 ${(profile.offspringEvalMs / 1000).toFixed(2)} 秒、移民 ${(profile.immigrantMs / 1000).toFixed(2)} 秒です。`,
    });
    reportRelayStageProgress(`GA 剖析汇总：初始种群 ${(profile.seedMs / 1000).toFixed(2)} 秒，交叉/修复 ${(profile.crossoverMs / 1000).toFixed(2)} 秒，子代评估 ${(profile.offspringEvalMs / 1000).toFixed(2)} 秒，移民 ${(profile.immigrantMs / 1000).toFixed(2)} 秒。`);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { plans: population[0].plans, traceLog };
}

async function optimizeWaveWithSA(initialPlans, scenario, wave, randomSeed = 307) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "sa", randomSeed);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (backendResult?.plans?.length) return backendResult;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentCost = currentEval.cost;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let currentPlans = currentEval.plans;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestCost = currentCost;
  let temperature = Math.max(12, currentCost * 0.12);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const finalTemperature = 0.25;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const coolingRate = 0.93;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "sa",
    scope: "wave",
    waveId: wave.waveId,
    stage: "sa-start",
    bestCost,
    textZh: `模拟退火从初始解启动，初始温度 ${temperature.toFixed(1)}，初始波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `シミュレーテッドアニーリングを初期解から開始し、初期温度 ${temperature.toFixed(1)}、初期コスト ${bestCost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 模拟退火已启动，初始温度 ${temperature.toFixed(1)}，初始波次内部代价 ${bestCost.toFixed(1)}。`);
  let epoch = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  while (temperature > finalTemperature && epoch < 28) {
    if (epoch) await cooperativeYield();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (let inner = 0; inner < 8; inner += 1) {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const candidate = sampleSingleNeighbor(currentState, scenario, wave, random, inner === 0 || (epoch + inner) % 5 === 0, 5);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!candidate) continue;
      const delta = candidate.cost - currentCost;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const accepted = delta < 0 || random() < Math.exp(-delta / Math.max(0.001, temperature));
      if (!accepted) continue;
      currentState = candidate.state;
      currentPlans = candidate.plans;
      currentCost = candidate.cost;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (traceLog.length < 18) {
        pushTraceEvent(traceLog, {
          algorithmKey: "sa",
          scope: "wave",
          waveId: wave.waveId,
          stage: "sa-iteration",
          epoch,
          moveKey: candidate.meta?.type || "mixed",
          candidateCost: candidate.cost,
          temperature,
          accepted,
          textZh: `温度 ${temperature.toFixed(2)} 下接受 ${candidate.meta?.type || "mixed"} 邻域，候选成本 ${candidate.cost.toFixed(1)}。`,
          textJa: `温度 ${temperature.toFixed(2)} で ${candidate.meta?.type || "mixed"} 近傍を採用し、候補コストは ${candidate.cost.toFixed(1)}。`,
        });
      }
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (candidate.cost + 1e-6 < bestCost) {
        bestCost = candidate.cost;
        bestState = cloneWaveRouteState(candidate.plans);
        bestPlans = candidate.plans;
        reportRelayStageProgress(`${wave.waveId} 模拟退火在第 ${epoch + 1} 轮刷新最优，波次内部代价 ${bestCost.toFixed(1)}。`);
        pushTraceEvent(traceLog, {
          algorithmKey: "sa",
          scope: "wave",
          waveId: wave.waveId,
          stage: "sa-best",
          epoch,
          bestCost,
      textZh: `模拟退火刷新最优解，新的波次内部代价 ${bestCost.toFixed(1)}。`,
          textJa: `焼きなましで最良解を更新し、新しい最良コストは ${bestCost.toFixed(1)}。`,
        });
      }
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if ((epoch + 1) % 4 === 0) {
      reportRelayStageProgress(`${wave.waveId} 模拟退火已跑到第 ${epoch + 1}/28 轮，当前温度 ${temperature.toFixed(2)}，当前最好波次内部代价 ${bestCost.toFixed(1)}。`);
    }
    temperature *= coolingRate;
    epoch += 1;
  }
  const polished = localImproveState(bestState, scenario, wave, random, 4);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (polished && polished.cost + 1e-6 < bestCost) {
    bestState = cloneWaveRouteState(polished.plans);
    bestPlans = polished.plans;
    bestCost = polished.cost;
  }
  pushTraceEvent(traceLog, {
    algorithmKey: "sa",
    scope: "wave",
    waveId: wave.waveId,
    stage: "sa-finish",
    bestCost,
    textZh: `模拟退火结束，最终波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `シミュレーテッドアニーリングが終了し、最終最良コストは ${bestCost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 模拟退火结束，最终波次内部代价 ${bestCost.toFixed(1)}。`);
  return { plans: rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans, traceLog };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildPheromoneMapForWave(wave, initialValue = 0.2) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const pheromone = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ids = [DC.id, ...wave.storeList];
  ids.forEach((fromId) => {
    const row = new Map();
    ids.forEach((toId) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (fromId !== toId) row.set(toId, initialValue);
    });
    pheromone.set(fromId, row);
  });
  return pheromone;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function pheromoneValue(pheromone, fromId, toId, fallback = 0.2) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return pheromone.get(fromId)?.get(toId) ?? fallback;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function depositRoutePheromone(pheromone, routeState, deposit) {
  routeState.forEach((item) => {
    item.routes.forEach((route) => {
      let prevId = DC.id;
      route.forEach((storeId) => {
        pheromone.get(prevId)?.set(storeId, pheromoneValue(pheromone, prevId, storeId) + deposit);
        prevId = storeId;
      });
      pheromone.get(prevId)?.set(DC.id, pheromoneValue(pheromone, prevId, DC.id) + deposit);
    });
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function evaporatePheromone(pheromone, rho = 0.12) {
  pheromone.forEach((row) => {
    row.forEach((value, key) => {
      row.set(key, Math.max(0.01, value * (1 - rho)));
    });
  });
}

function selectAcoNextStore(currentId, remainingStoreIds, pheromone, scenario, wave, random, alpha = 1, beta = 2.2) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const weighted = remainingStoreIds.map((storeId) => {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const pher = Math.pow(pheromoneValue(pheromone, currentId, storeId), alpha);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const heuristic = 1 / Math.max(1, (scenario.dist?.[currentId]?.[storeId] || 1));
    const timing = getStoreTimingForWave(scenario.storeMap.get(storeId), wave, scenario.dispatchStartMin);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const urgency = 1 + Math.max(0, (1440 - timing.latestAllowedArrivalMin) / 1440);
    const weight = pher * Math.pow(heuristic * urgency, beta);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { storeId, weight };
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const total = weighted.reduce((sum, item) => sum + item.weight, 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (total <= 0) return remainingStoreIds[0];
  let cursor = random() * total;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const item of weighted) {
    cursor -= item.weight;
    if (cursor <= 0) return item.storeId;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return weighted[weighted.length - 1]?.storeId || remainingStoreIds[0];
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function constructAcoState(seedState, pheromone, scenario, wave, random) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const allStoreIds = [...new Set(flattenStoresFromRouteState(seedState))];
  if (!allStoreIds.length) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const order = [];
  const remaining = [...allStoreIds];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let currentId = DC.id;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  while (remaining.length) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextId = selectAcoNextStore(currentId, remaining, pheromone, scenario, wave, random);
    order.push(nextId);
    remaining.splice(remaining.indexOf(nextId), 1);
    currentId = nextId;
  }
  const emptyState = seedState.map((item) => ({
    vehicle: item.vehicle,
    priorRegularDistance: item.priorRegularDistance,
    priorWaveCount: item.priorWaveCount,
    routes: [],
  }));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const inserted = insertStoresIntoRouteState(emptyState, order, scenario, wave);
  if (!inserted) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return inserted;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithACO(initialPlans, scenario, wave, randomSeed = 409) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "aco", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const random = createSeededRandom(randomSeed);
  const initialState = cloneWaveRouteState(initialPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const initialEval = evaluateRouteState(initialState, scenario, wave);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!initialEval) return { plans: initialPlans, traceLog: [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const pheromone = buildPheromoneMapForWave(wave, 0.18);
  let bestState = cloneWaveRouteState(initialEval.plans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestPlans = initialEval.plans;
  let bestCost = initialEval.cost;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "aco",
    scope: "wave",
    waveId: wave.waveId,
    stage: "aco-start",
    bestCost,
    textZh: `蚁群算法启动，先初始化信息素，再构造候选门店序列，初始波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `蟻コロニー最適化を開始し、フェロモンを初期化して候補順序を構築します。初期コストは ${bestCost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 蚁群算法已启动，初始波次内部代价 ${bestCost.toFixed(1)}。`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let iteration = 0; iteration < 26; iteration += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (iteration) await cooperativeYield();
    const ants = [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (let ant = 0; ant < 10; ant += 1) {
      const candidateState = constructAcoState(bestState, pheromone, scenario, wave, random);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!candidateState) continue;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const improved = localImproveState(candidateState, scenario, wave, random, 2) || evaluateRouteState(candidateState, scenario, wave);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!improved) continue;
      ants.push({
        state: cloneWaveRouteState(improved.plans),
        plans: improved.plans,
        cost: improved.cost,
      });
    }
    if (!ants.length) continue;
    ants.sort((a, b) => a.cost - b.cost);
    evaporatePheromone(pheromone, 0.1);
    ants.slice(0, 4).forEach((ant, rank) => {
      depositRoutePheromone(pheromone, ant.state, 4 / Math.max(1, ant.cost) * (4 - rank));
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (ants[0].cost + 1e-6 < bestCost) {
      bestCost = ants[0].cost;
      bestState = cloneWaveRouteState(ants[0].plans);
      bestPlans = ants[0].plans;
      reportRelayStageProgress(`${wave.waveId} 蚁群在第 ${iteration + 1} 轮刷新最优，波次内部代价 ${bestCost.toFixed(1)}。`);
      pushTraceEvent(traceLog, {
        algorithmKey: "aco",
        scope: "wave",
        waveId: wave.waveId,
        stage: "aco-best",
        iteration,
        bestCost,
        textZh: `第 ${iteration + 1} 轮蚁群刷新最优解，新的波次内部代价 ${bestCost.toFixed(1)}。`,
        textJa: `${iteration + 1} 回目で蟻群が最良解を更新し、新しい最良コストは ${bestCost.toFixed(1)}。`,
      });
    }
    if ((iteration + 1) % 5 === 0) {
      reportRelayStageProgress(`${wave.waveId} 蚁群已跑到第 ${iteration + 1}/26 轮，本轮最优候选代价 ${ants[0].cost.toFixed(1)}，全局最优 ${bestCost.toFixed(1)}。`);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (traceLog.length < 18) {
      pushTraceEvent(traceLog, {
        algorithmKey: "aco",
        scope: "wave",
        waveId: wave.waveId,
        stage: "aco-iteration",
        iteration,
        bestCost: ants[0].cost,
        textZh: `第 ${iteration + 1} 轮信息素完成挥发与强化，本轮最优候选成本 ${ants[0].cost.toFixed(1)}。`,
        textJa: `${iteration + 1} 回目でフェロモンの蒸発と強化を完了し、この回の最良候補コストは ${ants[0].cost.toFixed(1)}。`,
      });
    }
  }
  pushTraceEvent(traceLog, {
    algorithmKey: "aco",
    scope: "wave",
    waveId: wave.waveId,
    stage: "aco-finish",
    bestCost,
    textZh: `蚁群算法结束，最终波次内部代价 ${bestCost.toFixed(1)}。`,
    textJa: `蟻コロニー最適化が終了し、最終最良コストは ${bestCost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 蚁群算法结束，最终波次内部代价 ${bestCost.toFixed(1)}。`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { plans: bestPlans, traceLog };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function decodePsoPriorityState(priorityVector, templateState, scenario, wave) {
  const storeIds = flattenStoresFromRouteState(templateState);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const ordered = storeIds
    .map((storeId, index) => ({ storeId, priority: priorityVector[index] ?? 0 }))
    .sort((a, b) => b.priority - a.priority)
    .map((item) => item.storeId);
  const emptyState = templateState.map((item) => ({
    vehicle: item.vehicle,
    priorRegularDistance: item.priorRegularDistance,
    priorWaveCount: item.priorWaveCount,
    routes: [],
  }));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return insertStoresIntoRouteState(emptyState, ordered, scenario, wave);
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
async function optimizeWaveWithPSO(initialPlans, scenario, wave, randomSeed = 503) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "pso", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const random = createSeededRandom(randomSeed);
  const templateState = cloneWaveRouteState(initialPlans);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeIds = flattenStoresFromRouteState(templateState);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!storeIds.length) return { plans: initialPlans, traceLog: [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dimension = storeIds.length;
  const particleCount = Math.min(18, Math.max(10, dimension));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const iterations = 36;
  const c1 = 1.55;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const c2 = 1.55;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const vmax = 0.35;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  let globalBest = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const particles = [];
  const describeCurrentWaveCost = (plans) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const breakdown = computePlansCostBreakdown(plans, scenario, wave);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return `当前这波次的内部代价约 ${Number(breakdown.totalCost || 0).toFixed(1)}，组成是：${formatWaveCostBreakdown(breakdown)}。`;
  };

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  function evaluateParticle(position) {
    const state = decodePsoPriorityState(position, templateState, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!state) return null;
    const improved = localImproveState(state, scenario, wave, random, 2) || evaluateRouteState(state, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!improved) return null;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    return {
      position: [...position],
      state: cloneWaveRouteState(improved.plans),
      plans: improved.plans,
      cost: improved.cost,
    };
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let i = 0; i < particleCount; i += 1) {
    const position = Array.from({ length: dimension }, () => random());
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const velocity = Array.from({ length: dimension }, () => (random() - 0.5) * 0.2);
    const evaluated = evaluateParticle(position);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!evaluated) continue;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const particle = {
      position,
      velocity,
      bestPosition: [...position],
      bestCost: evaluated.cost,
      bestState: evaluated.state,
      bestPlans: evaluated.plans,
    };
    particles.push(particle);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!globalBest || evaluated.cost < globalBest.cost) {
      globalBest = {
        position: [...position],
        state: evaluated.state,
        plans: evaluated.plans,
        cost: evaluated.cost,
      };
    }
  }
  if (!particles.length || !globalBest) return { plans: initialPlans, traceLog: [] };

  pushTraceEvent(traceLog, {
    algorithmKey: "pso",
    scope: "wave",
    waveId: wave.waveId,
    stage: "pso-start",
    bestCost: globalBest.cost,
    textZh: `粒子群算法启动，粒子数 ${particles.length}，维度 ${dimension}，初始波次内部代价 ${globalBest.cost.toFixed(1)}。`,
    textJa: `粒子群最適化を開始し、粒子数 ${particles.length}、次元 ${dimension}、初期の全体最良コストは ${globalBest.cost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 已进入粒子群搜索，当前处理 ${dimension} 家门店、${particles.length} 个粒子。先用现有方案做起跑线，再让粒子群反复调整门店优先级。${describeCurrentWaveCost(globalBest.plans)}`);

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let stagnation = 0;
  for (let iter = 0; iter < iterations; iter += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (iter) await cooperativeYield();
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const inertia = 0.78 - (0.45 * iter / iterations);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let improvedThisRound = false;
    particles.forEach((particle, index) => {
      for (let d = 0; d < dimension; d += 1) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const r1 = random();
        const r2 = random();
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        let velocity = inertia * particle.velocity[d]
          + c1 * r1 * (particle.bestPosition[d] - particle.position[d])
          + c2 * r2 * (globalBest.position[d] - particle.position[d]);
        velocity = Math.max(-vmax, Math.min(vmax, velocity));
        particle.velocity[d] = velocity;
        particle.position[d] = Math.max(0, Math.min(1, particle.position[d] + velocity));
      }
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const evaluated = evaluateParticle(particle.position);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!evaluated) return;
      if (evaluated.cost + 1e-6 < particle.bestCost) {
        particle.bestCost = evaluated.cost;
        particle.bestPosition = [...particle.position];
        particle.bestState = evaluated.state;
        particle.bestPlans = evaluated.plans;
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (evaluated.cost + 1e-6 < globalBest.cost) {
        globalBest = {
          position: [...particle.position],
          state: evaluated.state,
          plans: evaluated.plans,
          cost: evaluated.cost,
        };
        improvedThisRound = true;
        pushTraceEvent(traceLog, {
          algorithmKey: "pso",
          scope: "wave",
          waveId: wave.waveId,
          stage: "pso-best",
          iteration: iter,
          particle: index,
          bestCost: globalBest.cost,
          textZh: `第 ${iter + 1} 轮粒子 ${index + 1} 刷新全局最优，新的波次内部代价 ${globalBest.cost.toFixed(1)}。`,
          textJa: `${iter + 1} 回目で粒子 ${index + 1} が全体最良を更新し、新しい最良コストは ${globalBest.cost.toFixed(1)}。`,
        });
        if ((iter + 1) <= 3 || (iter + 1) % 6 === 0) {
          reportRelayStageProgress(`${wave.waveId} 第 ${iter + 1} 轮里，粒子 ${index + 1} 刷新了这一波次的当前最优。说明它确实找到更顺的排法了。${describeCurrentWaveCost(globalBest.plans)}`);
        }
      }
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (traceLog.length < 20) {
      pushTraceEvent(traceLog, {
        algorithmKey: "pso",
        scope: "wave",
        waveId: wave.waveId,
        stage: "pso-iteration",
        iteration: iter,
        inertia,
        bestCost: globalBest.cost,
        textZh: `第 ${iter + 1} 轮完成速度与位置更新，惯性权重 ${inertia.toFixed(2)}，当前波次内部代价 ${globalBest.cost.toFixed(1)}。`,
        textJa: `${iter + 1} 回目で速度と位置の更新を完了し、慣性重み ${inertia.toFixed(2)}、現在の全体最良コストは ${globalBest.cost.toFixed(1)}。`,
      });
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if ((iter + 1) === 1 || (iter + 1) % 6 === 0 || iter === iterations - 1) {
      reportRelayStageProgress(`${wave.waveId} 粒子群已跑到第 ${iter + 1}/${iterations} 轮。这段时间主要在反复调整门店先后顺序和车辆归属。${describeCurrentWaveCost(globalBest.plans)}`);
    }
    stagnation = improvedThisRound ? 0 : stagnation + 1;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (stagnation >= 8) {
      const restartCount = Math.max(1, Math.floor(particles.length * 0.25));
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      for (let i = particles.length - restartCount; i < particles.length; i += 1) {
        particles[i].position = Array.from({ length: dimension }, () => random());
        particles[i].velocity = Array.from({ length: dimension }, () => (random() - 0.5) * 0.2);
      }
      reportRelayStageProgress(`${wave.waveId} 连续几轮没有明显提升，粒子群刚刚重启了 ${restartCount} 个表现最差的粒子，避免大家一直围着同一条旧路线空转。`);
      stagnation = 0;
    }
  }
  const polished = localImproveState(globalBest.state, scenario, wave, random, 5);
  reportRelayStageProgress(`${wave.waveId} 的粒子群主体搜索跑完了，正在做最后一轮局部微调，把已经找到的好方案再抛光一下。${describeCurrentWaveCost(globalBest.plans)}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (polished && polished.cost + 1e-6 < globalBest.cost) {
    globalBest = {
      position: globalBest.position,
      state: cloneWaveRouteState(polished.plans),
      plans: polished.plans,
      cost: polished.cost,
    };
  }
  pushTraceEvent(traceLog, {
    algorithmKey: "pso",
    scope: "wave",
    waveId: wave.waveId,
    stage: "pso-finish",
    bestCost: globalBest.cost,
    textZh: `粒子群算法结束，最终波次内部代价 ${globalBest.cost.toFixed(1)}。`,
    textJa: `粒子群最適化が終了し、最終最良コストは ${globalBest.cost.toFixed(1)}。`,
  });
  reportRelayStageProgress(`${wave.waveId} 的粒子群搜索完成。接下来会回到整盘方案上判断这一棒值不值得正式接过去。${describeCurrentWaveCost(globalBest.plans)}`);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { plans: globalBest.plans, traceLog };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildSeedPlansForWave(scenario, wave, regularVehicleStats = new Map(), basePlans = []) {
  return scenario.vehicles.map((vehicle) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const prior = regularVehicleStats.get(vehicle.plateNo) || {};
    const earliestDepartureMin = wave.isNightWave
      ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(prior.nightAvailableMin || wave.startMin))
      : wave.startMin;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const basePlan = (basePlans || []).find((plan) => plan.vehicle.plateNo === vehicle.plateNo);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!basePlan?.trips?.length) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, {
        ...prior,
        earliestDepartureMin,
      });
    }
    return rebuildPlanFromRoutes(
      vehicle,
      basePlan.trips.map((trip) => [...trip.route]),
      scenario,
      wave,
      {
        ...prior,
        earliestDepartureMin,
      },
    ) || createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, {
      ...prior,
      earliestDepartureMin,
    });
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function calculateRouteDistanceFromIds(route, scenario) {
  if (!route.length) return 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let total = scenario.dist?.[DC.id]?.[route[0]] || 0;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let i = 0; i < route.length - 1; i += 1) total += scenario.dist?.[route[i]]?.[route[i + 1]] || 0;
  total += scenario.dist?.[route[route.length - 1]]?.[DC.id] || 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return total;
}

function hasSavingsStandaloneFeasibility(route, scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return scenario.vehicles.some((vehicle) => {
    const plan = rebuildPlanFromRoutes(vehicle, [route], scenario, wave, {
      priorRegularDistance: 0,
      priorWaveCount: 0,
      earliestDepartureMin: wave.isNightWave ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(scenario.dispatchStartMin || wave.startMin)) : wave.startMin,
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return !!plan;
  });
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildSavingsCandidatesForWave(scenario, wave) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const savings = [];
  for (let i = 0; i < wave.storeList.length; i += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (let j = i + 1; j < wave.storeList.length; j += 1) {
      const storeAId = wave.storeList[i];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const storeBId = wave.storeList[j];
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const saving = (scenario.dist?.[DC.id]?.[storeAId] || 0) + (scenario.dist?.[DC.id]?.[storeBId] || 0) - (scenario.dist?.[storeAId]?.[storeBId] || 0);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (saving <= 0) continue;
      savings.push({ storeAId, storeBId, saving });
    }
  }
  savings.sort((a, b) => b.saving - a.saving);
  return savings;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function buildSavingsMergeOptions(routeA, routeB, storeAId, storeBId) {
  const options = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const aFront = routeA[0] === storeAId;
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const aBack = routeA[routeA.length - 1] === storeAId;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const bFront = routeB[0] === storeBId;
  const bBack = routeB[routeB.length - 1] === storeBId;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!(aFront || aBack) || !(bFront || bBack)) return options;
  if (aBack && bFront) options.push(routeA.concat(routeB));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (aFront && bBack) options.push(routeB.concat(routeA));
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (aFront && bFront) options.push([...routeA].reverse().concat(routeB));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (aBack && bBack) options.push(routeA.concat([...routeB].reverse()));
  return options.filter((route, index, list) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const key = route.join(">");
    return list.findIndex((item) => item.join(">") === key) === index;
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function twoOptRouteIds(route, scenario, wave) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let best = [...route];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let bestDistance = calculateRouteDistanceFromIds(best, scenario);
  let improved = true;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  while (improved) {
    improved = false;
    for (let i = 0; i < best.length - 1; i += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      for (let j = i + 1; j < best.length; j += 1) {
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const candidate = best.slice(0, i).concat(best.slice(i, j + 1).reverse(), best.slice(j + 1));
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const candidateDistance = calculateRouteDistanceFromIds(candidate, scenario);
        if (candidateDistance + 1e-6 >= bestDistance) continue;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (!hasSavingsStandaloneFeasibility(candidate, scenario, wave)) continue;
        best = candidate;
        bestDistance = candidateDistance;
        improved = true;
        break;
      }
      if (improved) break;
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return best;
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function assignSavingsRoutesToPlans(routes, seedPlans, scenario, wave, traceLog) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const plans = seedPlans.map((plan) => clone(plan));
  const unscheduledStores = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const sortedRoutes = [...routes].sort((a, b) => (b.totalBoxes - a.totalBoxes) || (b.route.length - a.route.length));
  for (const item of sortedRoutes) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let best = null;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    for (let i = 0; i < plans.length; i += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const candidateRoutes = plans[i].trips.map((trip) => [...trip.route]).concat([item.route]);
      const nextPlan = rebuildPlanFromRoutes(
        plans[i].vehicle,
        candidateRoutes,
        scenario,
        wave,
        {
          priorRegularDistance: plans[i].priorRegularDistance,
          priorWaveCount: plans[i].priorWaveCount,
          earliestDepartureMin: plans[i].earliestDepartureMin,
        },
      );
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!nextPlan) continue;
      const cost = computePlanCost(nextPlan, scenario, wave);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!best || cost < best.cost) best = { planIndex: i, nextPlan, cost };
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (best) {
      plans[best.planIndex] = best.nextPlan;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (traceLog.length < 48) {
        pushTraceEvent(traceLog, {
          algorithmKey: "savings",
          scope: "wave",
          waveId: wave.waveId,
          stage: "savings-assignment",
          textZh: `将节约法生成线路 ${item.route.join("->")} 分配给 ${plans[best.planIndex].vehicle.plateNo}，综合成本 ${best.cost.toFixed(1)}。`,
          textJa: `節約法で生成したルート ${item.route.join("->")} を ${plans[best.planIndex].vehicle.plateNo} に割り当て、総合コストは ${best.cost.toFixed(1)}。`,
        });
      }
      continue;
    }
    for (const storeId of item.route) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const store = scenario.storeMap.get(storeId);
      if (!store) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      let inserted = false;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      for (let i = 0; i < plans.length; i += 1) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const candidate = buildTripCandidate(plans[i], store, scenario, wave, false, { allowToleranceBreak: false });
        if (!candidate) continue;
        plans[i] = candidate.nextPlan;
        inserted = true;
        break;
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!inserted) {
        const diagnosis = diagnoseUnscheduledStore(store, plans, scenario, wave);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
        unscheduledStores.push({
          waveId: wave.waveId,
          storeId: store.id,
          storeName: store.name,
          desiredArrival: timing.desiredArrival,
          reason: diagnosis.reason,
          reasonText: diagnosis.detail,
        });
      }
    }
  }
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { plans, unscheduledStores };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function solveWaveBySavings(scenario) {
  const optimizedSolution = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const traceLog = [];
  const regularVehicleStats = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const unscheduledStores = [];
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const maxVehicleCapacity = Math.max(...scenario.vehicles.map((vehicle) => Number(vehicle.capacity || 0)), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const wave of scenario.waves) {
    const seedPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const routeRefs = new Map();
    let routes = wave.storeList
      .map((storeId) => scenario.storeMap.get(storeId))
      .filter(Boolean)
      .map((store) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const item = {
          id: `cw_${wave.waveId}_${store.id}`,
          route: [store.id],
          totalBoxes: Number(getStoreSolveLoadForWave(store, wave) || 0),
        };
        routeRefs.set(store.id, item);
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        return item;
      });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const savings = buildSavingsCandidatesForWave(scenario, wave);
    pushTraceEvent(traceLog, {
      algorithmKey: "savings",
      scope: "wave",
      waveId: wave.waveId,
      stage: "savings-start",
      textZh: `Clark-Wright 节约法启动，先生成 ${routes.length} 条单店线路，再计算 ${savings.length} 个节约值。`,
      textJa: `Clark-Wright 節約法を開始し、まず ${routes.length} 本の単店舗ルートを作成し、その後 ${savings.length} 個の節約値を計算します。`,
    });
    for (const saving of savings) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const routeA = routeRefs.get(saving.storeAId);
      const routeB = routeRefs.get(saving.storeBId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!routeA || !routeB || routeA === routeB) continue;
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      if (!IGNORE_CAPACITY_CONSTRAINT && routeA.totalBoxes + routeB.totalBoxes > maxVehicleCapacity) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const options = buildSavingsMergeOptions(routeA.route, routeB.route, saving.storeAId, saving.storeBId);
      let bestRoute = null;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      let bestDistance = Number.POSITIVE_INFINITY;
      for (const option of options) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (!hasSavingsStandaloneFeasibility(option, scenario, wave)) continue;
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        const distance = calculateRouteDistanceFromIds(option, scenario);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (distance < bestDistance) {
          bestDistance = distance;
          bestRoute = option;
        }
      }
      if (!bestRoute) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const merged = {
        id: `${routeA.id}_${routeB.id}`,
        route: bestRoute,
        totalBoxes: routeA.totalBoxes + routeB.totalBoxes,
      };
      routes = routes.filter((item) => item !== routeA && item !== routeB);
      routes.push(merged);
      merged.route.forEach((storeId) => routeRefs.set(storeId, merged));
      if (traceLog.length < 36) {
        pushTraceEvent(traceLog, {
          algorithmKey: "savings",
          scope: "wave",
          waveId: wave.waveId,
          stage: "savings-merge",
          textZh: `节约值 ${saving.saving.toFixed(1)} 驱动合并 ${saving.storeAId} 与 ${saving.storeBId}，形成线路 ${bestRoute.join("->")}。`,
          textJa: `節約値 ${saving.saving.toFixed(1)} に基づき ${saving.storeAId} と ${saving.storeBId} を統合し、ルート ${bestRoute.join("->")} を形成。`,
        });
      }
    }
    routes = routes.map((item) => ({
      ...item,
      route: twoOptRouteIds(item.route, scenario, wave),
    }));
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const assigned = assignSavingsRoutesToPlans(routes, seedPlans, scenario, wave, traceLog);
    optimizedSolution.push(assigned.plans);
    unscheduledStores.push(...assigned.unscheduledStores);
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (wave.isNightWave) {
      assigned.plans.forEach((plan) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
        if (plan.trips.length) {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            priorRegularDistance: prev.priorRegularDistance + plan.totalDistance,
            priorWaveCount: prev.priorWaveCount + 1,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, plan.availableTime),
          });
        } else {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            ...prev,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, prev.nightAvailableMin || wave.startMin),
          });
        }
      });
    }
    pushTraceEvent(traceLog, {
      algorithmKey: "savings",
      scope: "wave",
      waveId: wave.waveId,
      stage: "savings-finish",
      textZh: `Clark-Wright 节约法完成 ${wave.waveId}，最终生成 ${assigned.plans.flatMap((plan) => plan.trips).length} 趟线路。`,
      textJa: `Clark-Wright 節約法が ${wave.waveId} を完了し、最終的に ${assigned.plans.flatMap((plan) => plan.trips).length} 便を生成。`,
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return { solution: optimizedSolution, traceLog, unscheduledStores };
}

async function solveWaveByWaveWithOptimizer(scenario, optimizer, baseSeed = 0, optimizerKey = "hybrid") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseRun = greedySolve(scenario, baseSeed, true);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!baseRun?.solution?.length) return { solution: [], traceLog: [], unscheduledStores: baseRun?.unscheduledStores || [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const optimizedSolution = [];
  let traceLog = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const backendUnscheduledStores = [];
  let backendUnscheduledProvided = false;
  pushTraceEvent(traceLog, {
    algorithmKey: optimizerKey,
    scope: "wave",
    stage: `${optimizerKey}-seed`,
    textZh: `${algoLabel(optimizerKey)} 以快速初排结果作为起跑基线，以下展示的是该算法自身的继续优化过程。`,
    textJa: `${algoLabel(optimizerKey)} は初期配車を起点に最適化を継続します。以下は当該アルゴリズム自身のログです。`,
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const regularVehicleStats = new Map();
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (let waveIndex = 0; waveIndex < scenario.waves.length; waveIndex += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (waveIndex) await cooperativeYield();
    const wave = scenario.waves[waveIndex];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const seededPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, baseRun.solution[waveIndex] || []);
    const optimized = await optimizer(seededPlans, scenario, wave, baseSeed * 100 + 42 + waveIndex);
    optimizedSolution.push(optimized.plans);
    traceLog = traceLog.concat(optimized.traceLog || []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (Array.isArray(optimized.unscheduledStores)) {
      backendUnscheduledProvided = true;
      backendUnscheduledStores.push(...optimized.unscheduledStores);
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (wave.isNightWave) {
      optimized.plans.forEach((plan) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
        if (plan.trips.length) {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            priorRegularDistance: prev.priorRegularDistance + plan.totalDistance,
            priorWaveCount: prev.priorWaveCount + 1,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, plan.availableTime),
          });
        } else {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            ...prev,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, prev.nightAvailableMin || wave.startMin),
          });
        }
      });
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  return {
    solution: optimizedSolution,
    traceLog,
    unscheduledStores: backendUnscheduledProvided ? backendUnscheduledStores : (baseRun.unscheduledStores || []),
  };
}

async function improveSolutionByWaveOptimizer(baseSolution, scenario, optimizer, baseSeed = 0) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!baseSolution?.length) return { solution: [], traceLog: [] };
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const optimizedSolution = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let traceLog = [];
  const regularVehicleStats = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const baseMetrics = evaluateSolution(baseSolution, scenario, []);
  const waveIssueScores = scenario.waves.map((wave, waveIndex) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const plans = baseSolution[waveIndex] || [];
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const trips = plans.flatMap((plan) => plan.trips || []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lateStops = trips.reduce((sum, trip) => sum + (trip.lateStoreCount || 0), 0);
    const overTolerance = trips.reduce((sum, trip) => sum + (trip.overToleranceMinutes || 0), 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const lowLoadTrips = trips.filter((trip) => (trip.loadRate || 0) < 0.55).length;
    const mileageRef = isW3WaveForSolve(wave) ? getSolveW3OneWayMaxKm(scenario) : getSolveRelayMaxKm(scenario);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const longTrips = trips.filter((trip) => (trip.totalDistance || 0) > Math.max(80, mileageRef * 0.45)).length;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const score = lateStops * 8 + overTolerance * 0.2 + lowLoadTrips * 2 + longTrips * 2;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { waveIndex, score, hasTrips: trips.length > 0 };
  });
  const focusWaveIndexes = new Set(
    waveIssueScores
      .filter((item) => item.hasTrips)
      .sort((a, b) => b.score - a.score)
      .slice(0, Math.min(2, Math.max(1, waveIssueScores.filter((item) => item.hasTrips).length)))
      .filter((item) => item.score > 0)
      .map((item) => item.waveIndex)
  );
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!focusWaveIndexes.size) {
    waveIssueScores.forEach((item) => {
      if (item.hasTrips) focusWaveIndexes.add(item.waveIndex);
    });
  }
  reportRelayStageProgress(`这一棒不会整盘重算，而是优先盯住问题更重的波次：${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("、")}。其它波次先沿用上一轮结果。`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let waveIndex = 0; waveIndex < scenario.waves.length; waveIndex += 1) {
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (waveIndex) await cooperativeYield();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave = scenario.waves[waveIndex];
    const seededPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, baseSolution[waveIndex] || []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!focusWaveIndexes.has(waveIndex)) {
      reportRelayStageProgress(`${wave.waveId} 当前问题不重，这一棒先不动它，直接沿用上一轮排法。`);
      optimizedSolution.push(seededPlans);
      traceLog.push({
        algorithmKey: "focus",
        scope: "wave",
        waveId: wave.waveId,
        stage: "focus-skip",
        textZh: `${wave.waveId} 当前问题不重，继续沿用上一轮结果，不做重复重算。`,
        textJa: `${wave.waveId} は現時点で問題が重くないため、前輪結果をそのまま引き継ぎます。`,
      });
      if (wave.isNightWave) {
        seededPlans.forEach((plan) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
          // EN: Control point for business behavior.
          // CN: 影响业务行为的控制节点。
          if (plan.trips.length) {
            regularVehicleStats.set(plan.vehicle.plateNo, {
              priorRegularDistance: prev.priorRegularDistance + plan.totalDistance,
              priorWaveCount: prev.priorWaveCount + 1,
              nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, plan.availableTime),
            });
          }
        });
      }
      continue;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const tripCount = seededPlans.flatMap((plan) => plan.trips || []).length;
    reportRelayStageProgress(`开始细修 ${wave.waveId}。这波当前有 ${tripCount} 趟线路，优化器会只在这部分里重排，看看能不能压里程、少用车，或者把评分再往上顶。`);
    const optimized = await optimizer(seededPlans, scenario, wave, baseSeed * 100 + 91 + waveIndex);
    optimizedSolution.push(optimized.plans);
    traceLog = traceLog.concat(optimized.traceLog || []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const beforeDistance = seededPlans.reduce((sum, plan) => sum + (plan.totalDistance || 0), 0);
    const afterDistance = optimized.plans.reduce((sum, plan) => sum + (plan.totalDistance || 0), 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const distanceDelta = afterDistance - beforeDistance;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const distanceDeltaLabel = `${distanceDelta > 0 ? "+" : ""}${distanceDelta.toFixed(1)}`;
    reportRelayStageProgress(`${wave.waveId} 这波已经算完。优化前里程约 ${beforeDistance.toFixed(1)} km，优化后约 ${afterDistance.toFixed(1)} km，变化 ${distanceDeltaLabel} km。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (wave.isNightWave) {
      optimized.plans.forEach((plan) => {
        const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        // EN: Control point for business behavior.
        // CN: 影响业务行为的控制节点。
        if (plan.trips.length) {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            priorRegularDistance: prev.priorRegularDistance + plan.totalDistance,
            priorWaveCount: prev.priorWaveCount + 1,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, plan.availableTime),
          });
        } else {
          regularVehicleStats.set(plan.vehicle.plateNo, {
            ...prev,
            nightAvailableMin: Math.max(wave.earliestDepartureMin || wave.startMin, prev.nightAvailableMin || wave.startMin),
          });
        }
      });
    }
  }
  const optimizedMetrics = evaluateSolution(optimizedSolution, scenario, []);
  traceLog.push({
    algorithmKey: "focus",
    scope: "wave",
    stage: "focus-summary",
    textZh: `本轮优化优先处理了 ${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("、")}，其它波次直接承接上一轮结果，避免整盘重复重算。优化前评分 ${baseMetrics.score.toFixed(1)}，优化后评分 ${optimizedMetrics.score.toFixed(1)}。`,
    textJa: `本輪は ${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("、")} を優先的に最適化し、他の波次は前輪結果をそのまま引き継いで全面再計算を避けました。最適化前スコア ${baseMetrics.score.toFixed(1)}、最適化後スコア ${optimizedMetrics.score.toFixed(1)}。`,
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { solution: optimizedSolution, traceLog };
}

// EN: Control point for business behavior.
// CN: 影响业务行为的控制节点。
function solveFixedMembershipPlan(vehicle, storeIds, scenario, wave, priorStats = {}) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storesBase = storeIds.map((id) => scenario.storeMap.get(id)).filter(Boolean);
  let bestPlan = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  let bestScore = Number.POSITIVE_INFINITY;
  for (const seed of [0, 1, 2]) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const stores = [...storesBase].sort((a, b) => {
      // EN: Control point for business behavior.
      // CN: 影响业务行为的控制节点。
      const timingA = getStoreTimingForWave(a, wave, scenario.dispatchStartMin);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const timingB = getStoreTimingForWave(b, wave, scenario.dispatchStartMin);
      return timingA.desiredArrivalMin - timingB.desiredArrivalMin || a.id.localeCompare(b.id);
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (seed) {
      for (let i = stores.length - 1; i > 0; i -= 1) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const j = (i + seed) % stores.length;
        [stores[i], stores[j]] = [stores[j], stores[i]];
      }
    }
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    const plan = buildPlanForStoreOrder(vehicle, stores, scenario, wave, priorStats);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!plan) continue;
    const score = computePlanCost(plan, scenario, wave);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (score < bestScore) {
      bestScore = score;
      bestPlan = plan;
    }
  }
  return bestPlan;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function evaluateSolution(solution, scenario = null, unscheduledStores = []) {
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  if (!solution.length) return { feasible: false, score: 0, totalStops: 0, totalOnTime: 0, totalDistance: 0, loadRate: 0, fleetLoadRate: 0, totalLoadBoxes: 0, unscheduledStores, unscheduledCount: unscheduledStores.length, scheduledCount: 0, scheduledByWave: [] };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const plans = solution.flat();
  const trips = plans.flatMap((p) => p.trips);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const scheduledByWave = (scenario?.waves || []).map((wave) => ({ waveId: String(wave?.waveId || ""), count: 0 }));
  solution.forEach((wavePlans, waveIndex) => {
    const count = (wavePlans || []).reduce((sum, plan) => sum + (plan.trips || []).reduce((tripSum, trip) => tripSum + ((trip.stops || []).length || 0), 0), 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (scheduledByWave[waveIndex]) {
      scheduledByWave[waveIndex].count = count;
    } else {
      scheduledByWave.push({ waveId: `W${waveIndex + 1}`, count });
    }
  });
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const totalStops = trips.reduce((sum, t) => sum + t.stops.length, 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const totalOnTime = trips.reduce((sum, t) => sum + (t.onTimeCount || 0), 0);
  const totalDistance = plans.reduce((sum, p) => sum + p.totalDistance, 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const loadRate = trips.length ? trips.reduce((sum, t) => sum + (t.loadRate || 0), 0) / trips.length : 0;
  const totalLoadBoxes = plans.reduce((sum, p) => sum + Number(p.totalLoad || 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lateStoreCount = trips.reduce((sum, t) => sum + (t.lateStoreCount || 0), 0);
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  const lateRouteCount = trips.reduce((sum, t) => sum + ((t.overToleranceMinutes || 0) > 0 ? 1 : 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const overToleranceCount = trips.reduce((sum, t) => sum + t.stops.filter((stop) => (stop.overToleranceMinutes || 0) > 0).length, 0);
  const usage = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  // EN: Control point for business behavior.
  // CN: 影响业务行为的控制节点。
  for (const plan of plans) {
    if (!plan.trips.length) continue;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const key = plan.vehicle.plateNo;
    // EN: Control point for business behavior.
    // CN: 影响业务行为的控制节点。
    if (!usage.has(key)) usage.set(key, { load: 0, cap: plan.vehicle.capacity, trips: [] });
    usage.get(key).load += plan.totalLoad;
    usage.get(key).trips.push(...plan.trips);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const preferredTarget = Number(state.settings.minLoadRate || 0);
  const usageList = [...usage.entries()].map(([plateNo, item]) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const achievedRate = item.cap > 0 ? item.load / item.cap : 0;
    const maxTripLoad = item.trips.reduce((max, trip) => Math.max(max, trip.loadBoxes || 0), 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return {
      plateNo,
      achievedRate,
      preferredMet: achievedRate >= preferredTarget,
      maxTripLoad,
      cap: item.cap,
      load: item.load,
    };
  });
  const preferredMetCount = usageList.filter((item) => item.preferredMet).length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const preferredShortfall = usageList.reduce((sum, item) => sum + Math.max(0, preferredTarget - item.achievedRate), 0);
  const usedVehicleCount = usageList.length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const unusedVehicleCount = Math.max(0, state.vehicles.length - usedVehicleCount);
  const usedVehicleCapacity = usageList.reduce((sum, item) => sum + Number(item.cap || 0), 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const fleetLoadRate = usedVehicleCapacity ? totalLoadBoxes / usedVehicleCapacity : 0;
  const onTimeRatio = totalOnTime / Math.max(totalStops, 1);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const completionRatio = totalStops / Math.max(totalStops + unscheduledStores.length, 1);
  const distanceScore = Math.max(0, Math.min(1, (3500 - totalDistance) / 3500));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const loadScore = Math.max(0, Math.min(1, loadRate));
  const preferenceScoreRaw = (preferredMetCount / Math.max(usageList.length, 1)) - preferredShortfall * 0.02;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const preferenceScore = Math.max(0, Math.min(1, preferenceScoreRaw));
  const vehicleScore = Math.max(0, Math.min(1, (Math.max(state.vehicles.length, 1) - usedVehicleCount + 1) / Math.max(state.vehicles.length, 1)));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const goal = state.settings.optimizeGoal || "balanced";
  const goalWeights = {
    balanced: { completion: 0.40, onTime: 0.35, distance: 0.15, load: 0.10, vehicles: 0.00 },
    ontime: { completion: 0.34, onTime: 0.46, distance: 0.10, load: 0.04, vehicles: 0.06 },
    distance: { completion: 0.30, onTime: 0.26, distance: 0.30, load: 0.06, vehicles: 0.08 },
    vehicles: { completion: 0.30, onTime: 0.22, distance: 0.14, load: 0.04, vehicles: 0.30 },
    load: { completion: 0.30, onTime: 0.24, distance: 0.10, load: 0.28, vehicles: 0.08 },
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const weights = goalWeights[goal] || goalWeights.balanced;
  const score = (
    weights.completion * completionRatio +
    weights.onTime * onTimeRatio +
    weights.distance * distanceScore +
    weights.load * loadScore +
    weights.vehicles * vehicleScore
  ) * 100;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    feasible: totalStops > 0 || !unscheduledStores.length,
    score,
    totalStops,
    totalOnTime,
    totalDistance,
    loadRate,
    fleetLoadRate,
    totalLoadBoxes,
    lateStoreCount,
    lateRouteCount,
    overToleranceCount,
    completionRatio,
    preferredMetCount,
    preferredVehicleCount: usageList.length,
    usedVehicleCount,
    unusedVehicleCount,
    usageList,
    unscheduledStores,
    unscheduledCount: unscheduledStores.length,
    scheduledCount: totalStops,
    scheduledByWave,
    scoreBreakdown: {
      completionRatio,
      onTimeRatio,
      distanceScore,
      loadScore,
      vehicleScore,
      preferenceScore,
      optimizeGoal: goal,
    },
  };
}

function computeFinalPendingByWave(solution = [], scenario = null) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const unscheduled = [];
  if (!scenario || !Array.isArray(scenario.waves) || !Array.isArray(scenario.stores)) return unscheduled;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const assignmentMap = buildStoreAssignmentMapFromSolution(solution || []);
  const sourceStoreMap = scenario.storeMap instanceof Map
    ? scenario.storeMap
    : new Map((scenario.stores || []).map((store) => [String(store?.id || "").trim(), store]).filter(([id]) => id));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeMap = new Map();
  sourceStoreMap.forEach((store, id) => {
    const raw = String(id || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const normalized = normalizeStoreKey(raw);
    if (raw && !storeMap.has(raw)) storeMap.set(raw, store);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (normalized && !storeMap.has(normalized)) storeMap.set(normalized, store);
  });
  for (const wave of (scenario.waves || [])) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const waveId = String(wave?.waveId || "").trim().toUpperCase();
    const seen = new Set();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const storeId of (wave?.storeList || [])) {
      const sid = normalizeStoreKey(storeId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!sid || seen.has(sid)) continue;
      seen.add(sid);
      const store = storeMap.get(sid);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!store) continue;
      if (!isStoreCandidateForWaveRule(store, waveId)) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const variants = buildStoreKeyVariants(sid);
      let assigned = false;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      for (const variant of variants) {
        const key = buildStoreWaveAssignmentKey(variant, waveId);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const hit = key ? assignmentMap.get(key) : null;
        if (hit && String(hit.plateNo || "").trim()) {
          assigned = true;
          break;
        }
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!assigned) {
        unscheduled.push({
          waveId,
          storeId: sid,
          storeName: String(store?.name || ""),
          reason: "no_plate",
          reasonText: "本波次有货但无车牌号",
          source: "final_state",
        });
      }
    }
  }
  return unscheduled;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function computeWaveCandidateAssignedPendingStats(solution = [], scenario = null) {
  const stats = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!scenario || !Array.isArray(scenario.waves) || !Array.isArray(scenario.stores)) return stats;
  const assignmentMap = buildStoreAssignmentMapFromSolution(solution || []);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const sourceStoreMap = scenario.storeMap instanceof Map
    ? scenario.storeMap
    : new Map((scenario.stores || []).map((store) => [String(store?.id || "").trim(), store]).filter(([id]) => id));
  const storeMap = new Map();
  sourceStoreMap.forEach((store, id) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const raw = String(id || "").trim();
    const normalized = normalizeStoreKey(raw);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (raw && !storeMap.has(raw)) storeMap.set(raw, store);
    if (normalized && !storeMap.has(normalized)) storeMap.set(normalized, store);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const wave of (scenario.waves || [])) {
    const waveId = String(wave?.waveId || "").trim().toUpperCase();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const candidateSet = new Set();
    const assignedSet = new Set();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const storeId of (wave?.storeList || [])) {
      const sid = normalizeStoreKey(storeId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!sid || candidateSet.has(sid)) continue;
      const store = storeMap.get(sid);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!store) continue;
      if (!isStoreCandidateForWaveRule(store, waveId)) continue;
      candidateSet.add(sid);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const variants = buildStoreKeyVariants(sid);
      for (const variant of variants) {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const key = buildStoreWaveAssignmentKey(variant, waveId);
        const hit = key ? assignmentMap.get(key) : null;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (hit && String(hit.plateNo || "").trim()) {
          assignedSet.add(sid);
          break;
        }
      }
    }
    const pendingSet = new Set([...candidateSet].filter((sid) => !assignedSet.has(sid)));
    stats.push({
      waveId,
      candidateCount: candidateSet.size,
      assignedCount: assignedSet.size,
      pendingCount: pendingSet.size,
      candidateIds: [...candidateSet],
      assignedIds: [...assignedSet],
      pendingIds: [...pendingSet],
    });
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return stats;
}

function reportWaveCandidateAssignedPendingStats(tag = "结果", solution = [], scenario = null) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rows = computeWaveCandidateAssignedPendingStats(solution, scenario);
  rows.forEach((row) => {
    reportRelayStageProgress(`集合核对（${row.waveId}/${tag}）：candidate=${row.candidateCount}，assigned=${row.assignedCount}，pending=${row.pendingCount}。`);
  });
  return rows;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function applyFinalRuleToResult(result, scenario) {
  if (!result || typeof result !== "object") return result;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const unscheduled = computeFinalPendingByWave(result.solution || [], scenario);
  result.unscheduledStores = unscheduled;
  result.metrics = evaluateSolution(result.solution || [], scenario, unscheduled);
  result.storeAssignmentMap = buildStoreAssignmentMapFromSolution(result.solution || []);
  result.waveSetStats = computeWaveCandidateAssignedPendingStats(result.solution || [], scenario);
  return result;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildRelayStagePlan(selectedSet = [], optimizeGoal = "balanced", storeCount = 0) {
  const goal = String(optimizeGoal || "balanced");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const initialCandidates = selectedSet.filter((key) => ["vrptw", "savings"].includes(key));
  const globalCandidates = selectedSet.filter((key) => ["ga", "aco", "pso"].includes(key));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const localCandidates = selectedSet.filter((key) => ["hybrid", "lns", "tabu", "sa"].includes(key));
  const initialKeys = initialCandidates.length ? initialCandidates : ["vrptw", "savings"];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const globalPriorityMap = {
    balanced: ["pso", "ga", "aco"],
    ontime: ["ga", "pso", "aco"],
    distance: ["aco", "pso", "ga"],
    vehicles: ["pso", "ga", "aco"],
    load: ["pso", "ga", "aco"],
  };
  const localPriorityMap = {
    balanced: ["hybrid", "tabu", "lns", "sa"],
    ontime: ["hybrid", "tabu", "lns", "sa"],
    distance: ["lns", "tabu", "hybrid", "sa"],
    vehicles: ["hybrid", "lns", "tabu", "sa"],
    load: ["hybrid", "lns", "tabu", "sa"],
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const globalPriority = globalPriorityMap[goal] || globalPriorityMap.balanced;
  const localPriority = localPriorityMap[goal] || localPriorityMap.balanced;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxGlobalStages = Math.min(1, globalCandidates.length);
  const maxLocalStages = storeCount >= 90 ? 1 : storeCount >= 50 ? 2 : 2;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const pickedGlobals = globalPriority.filter((key) => globalCandidates.includes(key)).slice(0, maxGlobalStages);
  const pickedLocals = localPriority.filter((key) => localCandidates.includes(key)).slice(0, maxLocalStages);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stageKeys = [...pickedGlobals, ...pickedLocals];
  return {
    initialKeys,
    stageKeys: stageKeys.length ? stageKeys : ["hybrid"],
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByVRPTW(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithVrptwBackend, 0, "vrptw");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "vrptw", label: algoLabel("vrptw"), description: algoDescription("vrptw"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByHybrid(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithHybrid, 0, "hybrid");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "hybrid", label: algoLabel("hybrid"), description: algoDescription("hybrid"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByGA(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithGA, 1, "ga");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "ga", label: algoLabel("ga"), description: algoDescription("ga"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByTabu(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithTabu, 2, "tabu");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "tabu", label: algoLabel("tabu"), description: algoDescription("tabu"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByLNS(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithLns, 4, "lns");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "lns", label: algoLabel("lns"), description: algoDescription("lns"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveBySavings(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithSavingsBackend, 3, "savings");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "savings", label: algoLabel("savings"), description: algoDescription("savings"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveBySA(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithSA, 6, "sa");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "sa", label: algoLabel("sa"), description: algoDescription("sa"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByACO(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithACO, 8, "aco");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "aco", label: algoLabel("aco"), description: algoDescription("aco"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByPSO(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithPSO, 10, "pso");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "pso", label: algoLabel("pso"), description: algoDescription("pso"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByVehicle(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithVehicleDrivenBackend, 12, "vehicle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const s = run.solution;
  return applyFinalRuleToResult({ key: "vehicle", label: algoLabel("vehicle"), description: algoDescription("vehicle"), solution: s, traceLog: run.traceLog || [] }, scenario);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function solveByRelay(scenario, selectedKeys = [], baseResult = null) {
  const selectedSet = [...new Set((selectedKeys || []).filter(Boolean))];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const relayPlan = buildRelayStagePlan(selectedSet, state.settings.optimizeGoal, scenario.stores.length);
  const initialKeys = relayPlan.initialKeys;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const optimizerMap = {
    ga: optimizeWaveWithGA,
    aco: optimizeWaveWithACO,
    pso: optimizeWaveWithPSO,
    hybrid: optimizeWaveWithHybrid,
    lns: optimizeWaveWithLns,
    tabu: optimizeWaveWithTabu,
    sa: optimizeWaveWithSA,
  };
  const stageKeys = relayPlan.stageKeys;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traceLog = [];
  const relayLog = (text) => appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${text}`);
  relayStageReporter = relayLog;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
  pushTraceEvent(traceLog, {
    algorithmKey: "relay",
    scope: "wave",
    stage: "relay-start",
    textZh: `接力求解启动：初排阶段 ${initialKeys.map((key) => algoLabel(key)).join(" / ")}，后续阶段 ${stageKeys.length ? stageKeys.map((key) => algoLabel(key)).join(" -> ") : "无" }。`,
    textJa: `リレー求解を開始します。初期段階は ${initialKeys.map((key) => algoLabel(key)).join(" / ")}、後続段階は ${stageKeys.length ? stageKeys.map((key) => algoLabel(key)).join(" -> ") : "なし"} です。`,
  });
  relayLog(`接力求解启动。第一阶段先从 ${initialKeys.map((key) => algoLabel(key)).join(" / ")} 里挑一个更好的初排方案。后续不会把所有算法机械跑满，而是按当前目标挑最值得接棒的几棒。`);
  relayLog(`下面日志里提到的“波次内部代价”，不是最后给客户看的综合评分，而是算法内部比较路线优劣的尺子。它主要由里程成本、晚到罚分、超允许偏差罚分、波次超时罚分、车辆续跑罚分、额外趟次罚分，再减去装载抵扣组成。`);

  let current = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (baseResult?.solution?.length) {
    current = applyFinalRuleToResult({
      key: "relay-seed",
      label: algoLabel("relay"),
      description: algoDescription("relay"),
      solution: baseResult.solution,
      traceLog: baseResult.traceLog || [],
    }, scenario);
    relayLog(`检测到你上一轮已经有可用方案，所以这一轮接力直接从现有结果起跑，不再重复重建初排。当前起跑指标：${relayMetricSummary(current.metrics)}。`);
  } else {
    for (let index = 0; index < initialKeys.length; index += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const key = initialKeys[index];
      relayLog(`正在执行初排候选 ${algoLabel(key)}，目的是先拿到一版能用的基础方案。`);
      const candidate = await ({ vrptw: solveByVRPTW, savings: solveBySavings }[key])(scenario);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!current || candidate.metrics.score > current.metrics.score) current = candidate;
      relayLog(`${algoLabel(key)} 完成。${relayMetricSummary(candidate.metrics)}。`);
      pushTraceEvent(traceLog, {
        algorithmKey: "relay",
        scope: "wave",
        stage: "relay-initial",
        waveId: scenario.waves[0]?.waveId || "ALL",
        textZh: `初排候选 ${algoLabel(key)} 完成，评分 ${candidate.metrics.score.toFixed(1)}，${current.key === candidate.key ? "暂列当前接力首棒" : "未超过当前首棒"}。`,
        textJa: `初期候補 ${algoLabel(key)} が完了し、スコアは ${candidate.metrics.score.toFixed(1)}、${current.key === candidate.key ? "現在の第一走者として採用" : "現在の先頭案は維持"}。`,
      });
      await cooperativeYield();
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!current) return applyFinalRuleToResult({ key: "relay", label: algoLabel("relay"), description: algoDescription("relay"), solution: [], traceLog }, scenario);
  relayLog(`初排阶段结束，当前接力首棒是 ${algoLabel(current.key)}。首棒的关键指标是：${relayMetricSummary(current.metrics)}。接下来进入继续优化阶段，共计划 ${stageKeys.length} 棒。`);

  let noImproveStages = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let index = 0; index < stageKeys.length; index += 1) {
    const key = stageKeys[index];
    relayLog(`第 ${index + 2} 阶段由 ${algoLabel(key)} 接棒。它会在当前最好方案基础上继续找更好的排法。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const beforeMetrics = current.metrics;
    const improved = await improveSolutionByWaveOptimizer(current.solution, scenario, optimizerMap[key], 30 + index);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const metrics = evaluateSolution(improved.solution, scenario, computeFinalPendingByWave(improved.solution, scenario));
    const scoreGain = metrics.score - current.metrics.score;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const accepted = scoreGain > 1e-6;
    const materialGain = scoreGain >= 0.25
      || (metrics.unscheduledCount || 0) < (beforeMetrics.unscheduledCount || 0)
      || (metrics.totalDistance || 0) + 3 < (beforeMetrics.totalDistance || 0)
      || (metrics.totalOnTime || 0) > (beforeMetrics.totalOnTime || 0);
    relayLog(`${algoLabel(key)} 本轮结果：${relayMetricSummary(metrics)}。相比上一棒，${describeRelayMetricDelta(beforeMetrics, metrics)}。${accepted ? `因为新分数超过当前 ${beforeMetrics.score.toFixed(1)}，所以这一棒正式接棒。` : `因为新分数没有超过当前 ${beforeMetrics.score.toFixed(1)}，所以这一棒没有接过去。`}`);
    pushTraceEvent(traceLog, {
      algorithmKey: "relay",
      scope: "wave",
      stage: "relay-stage",
      waveId: scenario.waves[0]?.waveId || "ALL",
      textZh: `${index + 2} 阶段由 ${algoLabel(key)} 接棒，候选评分 ${metrics.score.toFixed(1)}，${accepted ? `优于当前 ${current.metrics.score.toFixed(1)}，正式接棒` : `未超过当前 ${current.metrics.score.toFixed(1)}，保留原方案`}。`,
      textJa: `${index + 2} 段階は ${algoLabel(key)} が引き継ぎ、候補スコア ${metrics.score.toFixed(1)}、${accepted ? `現行 ${current.metrics.score.toFixed(1)} を上回ったため採用` : `現行 ${current.metrics.score.toFixed(1)} を超えず元案を維持`}。`,
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (accepted) {
      current = applyFinalRuleToResult({
        key: "relay",
        label: algoLabel("relay"),
        description: algoDescription("relay"),
        solution: improved.solution,
        traceLog: improved.traceLog || [],
      }, scenario);
      noImproveStages = materialGain ? 0 : noImproveStages + 1;
      if (!materialGain) {
        relayLog(`这一棒虽然分数略有上升，但提升很有限。我会把它接住，同时开始关注是否该提前收工，避免空耗时间。`);
      }
    } else {
      noImproveStages += 1;
    }
    mergeTraceLogs(traceLog, improved.traceLog || []);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (noImproveStages >= 2 && index < stageKeys.length - 1) {
      relayLog(`连续 ${noImproveStages} 棒没有带来实质提升，后面的接力先提前停止，避免继续空转耗时。`);
      pushTraceEvent(traceLog, {
        algorithmKey: "relay",
        scope: "wave",
        stage: "relay-early-stop",
        waveId: scenario.waves[0]?.waveId || "ALL",
        textZh: `连续 ${noImproveStages} 个阶段未带来实质提升，接力提前收工。`,
        textJa: `${noImproveStages} 段階連続で実質改善が出なかったため、リレーを早期終了しました。`,
      });
      break;
    }
    await cooperativeYield();
  }

  pushTraceEvent(traceLog, {
    algorithmKey: "relay",
    scope: "wave",
    stage: "relay-finish",
    textZh: `接力求解完成，最终采用方案评分 ${current.metrics.score.toFixed(1)}。`,
    textJa: `リレー求解が完了し、最終採用スコアは ${current.metrics.score.toFixed(1)} です。`,
  });
  relayLog(`接力求解完成。最终方案评分 ${current.metrics.score.toFixed(1)}，已调度 ${current.metrics.scheduledCount || 0} 家，未调度 ${(current.metrics.unscheduledCount || 0)} 家。`);

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return applyFinalRuleToResult({
    key: "relay",
    label: algoLabel("relay"),
    description: algoDescription("relay"),
    solution: current.solution,
    traceLog,
  }, scenario);
  } finally {
    relayStageReporter = null;
  }
}

function diagnoseScenarioFeasibility(scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const issues = [];
  for (const wave of scenario.waves) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (wave.relaxEnd) continue;
    for (const id of wave.storeList) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const store = scenario.storeMap.get(id); if (!store) continue;
      const fastest = scenario.vehicles[0]; if (!fastest) continue;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const directDistance = scenario.dist[DC.id][store.id];
      const soloDistance = directDistance + scenario.dist[store.id][DC.id];
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const arrival = Math.max(wave.startMin, scenario.dispatchStartMin) + getTravelMinutes(scenario, DC.id, store.id, fastest.speed);
      const start = arrival;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const leave = start + (store.actualServiceMinutes || store.serviceMinutes || 0);
      const finish = leave + getTravelMinutes(scenario, store.id, DC.id, fastest.speed);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (isW3WaveForSolve(wave) && directDistance > getSolveW3OneWayMaxKm(scenario)) {
        issues.push(`${wave.waveId} 的门店 ${store.name} 单独到店就要 ${directDistance.toFixed(1)} km`);
      }
      else if ((wave.endMode || "return") === "return" ? finish > wave.endMin : leave > wave.endMin) {
        issues.push(`${wave.waveId} 的门店 ${store.name} 单独执行也会晚于波次${(wave.endMode || "return") === "return" ? "回库" : "完店"}截止`);
      }
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (issues.length >= 6) return issues;
    }
  }
  return issues;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function formatScheduledByWave(metrics = {}) {
  const list = Array.isArray(metrics.scheduledByWave) ? metrics.scheduledByWave : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!list.length) return `${L("scheduledStores")} ${metrics.scheduledCount || 0}`;
  return `${L("scheduledStores")}（${list.map((item) => `${item.waveId || "-"}:${Number(item.count || 0)}`).join(" | ")}）`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderSummary() {
  const best = [...state.lastResults].sort((a, b) => b.metrics.score - a.metrics.score)[0];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("summaryCards").innerHTML = state.lastResults.map((result) => `
    <article class="metric-card ${state.activeResultKey === result.key ? "metric-card-active" : ""}">
      <p class="label">${algoLabel(result.key)}</p>
      <div class="value">${result.metrics.score.toFixed(1)}</div>
      ${Number(state.settings.targetScore || 0) > 0 ? `<p class="hint">${L("targetScore")} ${Number(state.settings.targetScore).toFixed(1)}，${result.metrics.score >= Number(state.settings.targetScore || 0) ? L("targetAchieved") : L("targetMissed")}</p>` : ""}
      <p class="hint">${lang() === "ja" ? "スコアは総合評価で、数値が高いほど良いです。" : "评分代表综合表现，数值越高越好。"}</p>
      <p class="hint">${L("dispatchStart")} ${state.settings.dispatchStartTime}，${formatScheduledByWave(result.metrics)}，${L("unscheduledStores")} ${result.metrics.unscheduledCount}，${L("onTime")}率 ${formatRate(result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1))}，${L("totalDistance")} ${result.metrics.totalDistance.toFixed(1)} km，${L("avgLoad")} ${formatRate(result.metrics.loadRate)}，${L("fleetLoad")} ${formatRate(result.metrics.fleetLoadRate)}，${lang() === "ja" ? `使用車両 ${result.metrics.usedVehicleCount} 台 / 待機車両 ${result.metrics.unusedVehicleCount} 台` : `已用车辆 ${result.metrics.usedVehicleCount} 辆 / 未用车辆 ${result.metrics.unusedVehicleCount} 辆`}</p>
      ${best && best.key !== result.key ? `<p class="hint">${lang() === "ja" ? `現在の最良案 ${algoLabel(best.key)} と比べると、スコア差 ${Math.abs(best.metrics.score - result.metrics.score).toFixed(1)}、距離差 ${Math.abs(best.metrics.totalDistance - result.metrics.totalDistance).toFixed(1)} km、使用車両差 ${Math.abs((best.metrics.usedVehicleCount || 0) - (result.metrics.usedVehicleCount || 0))} 台です。` : `与当前最佳 ${algoLabel(best.key)} 相比：分数差 ${Math.abs(best.metrics.score - result.metrics.score).toFixed(1)}，里程差 ${Math.abs(best.metrics.totalDistance - result.metrics.totalDistance).toFixed(1)} km，用车差 ${Math.abs((best.metrics.usedVehicleCount || 0) - (result.metrics.usedVehicleCount || 0))} 辆。`}</p>` : ""}
      <p class="hint">${lang() === "ja" ? "100点制。未遅着率45% + 距離25% + 平均単便積載率15% + 積載優先達成率15%。より時間通りで、距離が短く、積載が良いほど高得点です。" : "100分制。未晚到率45% + 距离25% + 平均单趟装载率15% + 装载偏好达成率15%。也就是说，越准时、越省里程、越能装，分数越高。"}</p>
      <p class="hint">${L("hardArrivalHint")}${state.settings.concentrateLate ? ` ${L("lateFocusHint")}` : ""}</p>
        <p class="hint">${result.metrics.unscheduledCount ? LT("unscheduledSummary", { count: result.metrics.unscheduledCount, names: formatUnscheduledDetails(result.metrics.unscheduledStores, 8) }) : L("noUnscheduled")}</p>
        ${result.metrics.unscheduledCount ? `<p class="hint">${lang() === "ja" ? `未割当の主因：${summarizeUnscheduledReasons(result.metrics.unscheduledStores)}` : `未调度主因：${summarizeUnscheduledReasons(result.metrics.unscheduledStores)}`}${result.metrics.unusedVehicleCount > 0 ? (lang() === "ja" ? `。なお待機車両は ${result.metrics.unusedVehicleCount} 台あり、主因は車両不足ではなく制約側です。` : `。当前仍有 ${result.metrics.unusedVehicleCount} 辆闲置车，主因不是车不够，而是约束不满足。`) : ""}</p>` : ""}
        <button class="secondary view-result-detail" data-result-key="${result.key}">${algoLabel(result.key)} ${L("detail")}</button>
      </article>
  `).join("");
}

function normalizeMainTab(tabKey) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const key = String(tabKey || "").trim();
  return MAIN_TAB_KEYS.has(key) ? key : "basic";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function applyMainTabVisibility() {
  const activeTab = normalizeMainTab(state.ui.mainTab);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.mainTab = activeTab;
  document.querySelectorAll("[data-main-tab]").forEach((section) => {
    section.classList.toggle("tab-hidden", section.dataset.mainTab !== activeTab);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.querySelectorAll("[data-main-tab-btn]").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.mainTabBtn === activeTab);
  });
}

function setMainTab(tabKey) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.mainTab = normalizeMainTab(tabKey);
  applyMainTabVisibility();
}

function bindMainTabs() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tabs = document.getElementById("mainTabs");
  if (!tabs) return;
  tabs.addEventListener("click", (event) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const btn = event.target.closest("[data-main-tab-btn]");
    if (!btn) return;
    setMainTab(btn.dataset.mainTabBtn);
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function ensureAnalyticsMount() {
  const summarySection = document.getElementById("summaryCards");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const resultPanels = document.getElementById("resultPanels");
  const resultSection = resultPanels?.closest("section");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!summarySection || !resultSection) return null;
  let panel = document.getElementById("analyticsPanel");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (panel) {
    panel.setAttribute("data-main-tab", "solve");
    if (panel.previousElementSibling !== summarySection) {
      summarySection.insertAdjacentElement("afterend", panel);
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return panel;
  }
  summarySection.insertAdjacentHTML("afterend", `
    <section id="analyticsPanel" class="panel analytics-panel" data-main-tab="solve">
      <div class="panel-head">
        <div>
          <h2 id="analyticsTitle"></h2>
          <p id="analyticsDesc"></p>
        </div>
      </div>
      <div id="generationProgress" class="progress-shell"></div>
      <div id="analyticsContent" class="analytics-grid"></div>
    </section>
  `);
  return document.getElementById("analyticsPanel");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function ensureGanttMount() {
  const resultPanels = document.getElementById("resultPanels");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const resultSection = resultPanels?.closest("section");
  if (!resultSection) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let panel = document.getElementById("ganttPanel");
  if (panel) {
    panel.setAttribute("data-main-tab", "solve");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (panel.previousElementSibling !== resultSection) {
      resultSection.insertAdjacentElement("afterend", panel);
    }
    return panel;
  }
  resultSection.insertAdjacentHTML("afterend", `
    <section id="ganttPanel" class="panel analytics-panel gantt-panel-shell" data-main-tab="solve">
      <div class="panel-head">
        <div>
          <h2 id="ganttTitle"></h2>
          <p id="ganttDesc"></p>
        </div>
      </div>
      <div id="ganttContent"></div>
    </section>
  `);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return document.getElementById("ganttPanel");
}

function dockAssistantPanel() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const assistantCard = document.querySelector("#analyticsContent .mascot-card");
  if (!assistantCard) {
    dock.innerHTML = "";
    dock.classList.add("is-empty");
    dock.classList.remove("state-collapsed", "state-half", "state-full");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const normalizeDockState = (value) => {
    const key = String(value || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (key === "collapsed" || key === "half" || key === "full") return key;
    return "half";
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.assistantDockState = normalizeDockState(state.ui.assistantDockState);
  dock.innerHTML = "";
  const controls = document.createElement("div");
  controls.className = "assistant-dock-controls";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const labels = lang() === "ja"
    ? { collapsed: "折りたたむ", half: "簡版", full: "全量" }
    : { collapsed: "折叠", half: "简版", full: "全量" };
  controls.innerHTML = `
    <span class="assistant-dock-move" title="${lang() === "ja" ? "ドラッグして移動" : "拖拽移动"}">⋮⋮</span>
    <button type="button" class="assistant-dock-toggle" data-assistant-dock-state="collapsed">${labels.collapsed}</button>
    <button type="button" class="assistant-dock-toggle" data-assistant-dock-state="half">${labels.half}</button>
    <button type="button" class="assistant-dock-toggle" data-assistant-dock-state="full">${labels.full}</button>
  `;
  dock.appendChild(controls);
  dock.appendChild(assistantCard);
  dock.classList.remove("is-empty");
  dock.classList.remove("state-collapsed", "state-half", "state-full");
  dock.classList.add(`state-${state.ui.assistantDockState}`);
  applyAssistantDockPosition();
}

function applyAssistantDockPosition() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const pos = state.ui.assistantDockPosition;
  if (!pos || !Number.isFinite(pos.left) || !Number.isFinite(pos.top)) {
    dock.style.left = "";
    dock.style.top = "";
    dock.style.right = "";
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rect = dock.getBoundingClientRect();
  const maxLeft = Math.max(0, window.innerWidth - rect.width - 8);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxTop = Math.max(0, window.innerHeight - rect.height - 8);
  const left = Math.max(0, Math.min(Math.round(pos.left), maxLeft));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const top = Math.max(0, Math.min(Math.round(pos.top), maxTop));
  dock.style.left = `${left}px`;
  dock.style.top = `${top}px`;
  dock.style.right = "auto";
}

function startAssistantDockDrag(event) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!event || event.button !== 0) return;
  const dock = document.getElementById("assistantDock");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!dock) return;
  const handle = event.target?.closest?.(".assistant-dock-controls, #assistantDock.state-collapsed");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!handle) return;
  if (event.target?.closest?.(".assistant-dock-toggle")) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rect = dock.getBoundingClientRect();
  assistantDockDragState = {
    offsetX: event.clientX - rect.left,
    offsetY: event.clientY - rect.top,
  };
  state.ui.assistantDockPosition = { left: rect.left, top: rect.top };
  dock.classList.add("is-dragging");
  event.preventDefault();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function moveAssistantDockDrag(event) {
  if (!assistantDockDragState) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rect = dock.getBoundingClientRect();
  const maxLeft = Math.max(0, window.innerWidth - rect.width - 8);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxTop = Math.max(0, window.innerHeight - rect.height - 8);
  state.ui.assistantDockPosition = {
    left: Math.max(0, Math.min(event.clientX - assistantDockDragState.offsetX, maxLeft)),
    top: Math.max(0, Math.min(event.clientY - assistantDockDragState.offsetY, maxTop)),
  };
  applyAssistantDockPosition();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function stopAssistantDockDrag() {
  if (!assistantDockDragState) return;
  assistantDockDragState = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("assistantDock")?.classList.remove("is-dragging");
}

function locateStoreRow(queryText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const assignmentMap = getActiveStoreAssignmentMap();
  const match = state.stores.findIndex((store) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const assignment = getStoreAssignmentByRule(store, assignmentMap);
    const plate = String(assignment?.plateNo || store?.plateNo || "").toLowerCase();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const hay = `${store.id || ""} ${store.name || ""} ${plate}`.toLowerCase();
    return hay.includes(query);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.storeLocatedIndex = match;
  renderStoresTable();
  const rowInput = document.querySelector(`input[data-kind="store"][data-index="${match}"]`);
  rowInput?.closest("tr")?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  rowInput?.focus?.();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return match >= 0;
}

function locateVehicleRow(queryText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const match = state.vehicles.findIndex((vehicle) => {
    const hay = `${vehicle.plateNo || ""} ${vehicle.driverName || ""}`.toLowerCase();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return hay.includes(query);
  });
  state.ui.vehicleLocatedIndex = match;
  renderVehicles();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rowInput = document.querySelector(`#vehicleTable input[data-kind="vehicle"][data-index="${match}"]`);
  rowInput?.closest("tr")?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  rowInput?.focus?.();
  return match >= 0;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function locateWaveItem(queryText) {
  const query = String(queryText || "").trim().toLowerCase();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!query) return false;
  const match = state.waves.findIndex((wave) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const names = getWaveStoreNameList(wave.storeIds).join(" ");
    const hay = `${wave.waveId || ""} ${names}`.toLowerCase();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return hay.includes(query);
  });
  if (match < 0) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.waveSelectedIndex = match;
  renderWaves();
  const btn = document.querySelector(`.wave-left-item[data-wave-select-index="${match}"]`);
  btn?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return true;
}

async function saveBaseDataSnapshot(sourceModule) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const available = await ensureGaBackendAvailable(true);
  if (!available) {
    throw new Error("backend_unavailable");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const now = new Date();
  const snapshot = {
    id: `data-${sourceModule}-${now.getTime()}`,
    createdAt: `${now.getFullYear()}/${String(now.getMonth() + 1).padStart(2, "0")}/${String(now.getDate()).padStart(2, "0")} ${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}:${String(now.getSeconds()).padStart(2, "0")}`,
    createdDate: `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`,
    strategy: "data_archive",
    goal: "base_data",
    activeResultKey: "",
    bestScore: 0,
    resultCount: 0,
    algorithms: "base_data",
    sourceModule,
    metaType: "base_data",
    stores: clone(state.stores),
    vehicles: clone(state.vehicles),
    waves: clone(state.waves),
    settings: clone(state.settings),
    results: [],
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/save`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ snapshot }),
  }, 10000);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const payload = await response.json();
  if (!payload?.ok) throw new Error(payload?.error || "archive_save_failed");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return snapshot.id;
}

function renderDataArchivePanels() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const table = document.getElementById("dataArchiveTable");
  const storeTable = document.getElementById("dataArchiveStoreTable");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const status = document.getElementById("dataArchiveQueryStatus");
  if (!table || !storeTable || !status) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const items = Array.isArray(dataArchiveCache.items) ? dataArchiveCache.items : [];
  const columns = [
    { label: "档案ID", width: 260 },
    { label: "保存时间", width: 170 },
    { label: "来源模块", width: 100 },
    { label: "门店数", width: 80 },
    { label: "车辆数", width: 80 },
    { label: "波次数", width: 80 },
    { label: "操作", width: 100 },
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rows = items.map((item) => `
    <tr class="${dataArchiveCache.selectedId === item.id ? "located-row" : ""}">
      <td title="${escapeHtml(String(item.id || ""))}">${escapeHtml(String(item.id || ""))}</td>
      <td>${escapeHtml(String(item.createdAt || ""))}</td>
      <td>${escapeHtml(String(item.sourceModule || ""))}</td>
      <td>${Number(item.storeCount || 0)}</td>
      <td>${Number(item.vehicleCount || 0)}</td>
      <td>${Number(item.waveCount || 0)}</td>
      <td><button class="mini" data-data-archive-view="${escapeHtml(String(item.id || ""))}">查看</button></td>
    </tr>
  `);
  table.innerHTML = buildDataTableHtml({ tableKind: "dataArchive", columns, rows, tableClass: "data-archive-table" });
  const selected = items.find((item) => item.id === dataArchiveCache.selectedId) || items[0] || null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!selected) {
    status.textContent = "暂无基础资料档案。";
    storeTable.innerHTML = "";
    return;
  }
  dataArchiveCache.selectedId = selected.id;
  status.textContent = `当前档案：${selected.id}，门店 ${selected.storeCount} 家。`;
  const storeColumns = [
    { label: "编号", width: 110 },
    { label: "名称", width: 220 },
    { label: "区域", width: 120 },
    { label: "经度", width: 140 },
    { label: "纬度", width: 140 },
    { label: "一波次货量", width: 110 },
    { label: "二波次货量", width: 110 },
    { label: "冷藏比例", width: 90 },
    { label: "所属波次", width: 110 },
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeRows = (selected.stores || []).map((store) => {
    const loads = normalizeStoreWaveLoads(store || {});
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `
    <tr>
      <td>${escapeHtml(String(store.id || ""))}</td>
      <td title="${escapeHtml(String(store.name || ""))}">${escapeHtml(String(store.name || ""))}</td>
      <td>${escapeHtml(String(store.district || ""))}</td>
      <td>${Number(store.lng || 0)}</td>
      <td>${Number(store.lat || 0)}</td>
      <td>${Number(loads.wave1Load || 0)}</td>
      <td>${Number(loads.wave2Load || 0)}</td>
      <td>${Number(store.coldRatio || 0)}</td>
      <td>${escapeHtml(String(store.waveBelongs || ""))}</td>
    </tr>
  `;
  });
  storeTable.innerHTML = buildDataTableHtml({ tableKind: "dataArchiveStore", columns: storeColumns, rows: storeRows, tableClass: "data-archive-store-table" });
}

async function queryDataArchives() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dateInput = document.getElementById("dataArchiveDateInput");
  const keywordInput = document.getElementById("dataArchiveKeywordInput");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const status = document.getElementById("dataArchiveQueryStatus");
  if (!dateInput || !keywordInput || !status) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dateValue = String(dateInput.value || todayDateKey());
  const keyword = String(keywordInput.value || "").trim().toLowerCase();
  dataArchiveCache.date = dateValue;
  dataArchiveCache.keyword = keyword;
  status.textContent = "正在查询...";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: dateValue, page: 1, pageSize: 50 }),
    }, 8000);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const rawItems = Array.isArray(payload?.items) ? payload.items : [];
    const items = rawItems
      .filter((item) => item && item.metaType === "base_data")
      .map((item) => ({
        id: String(item.id || ""),
        createdAt: item.createdAt || "",
        sourceModule: item.sourceModule || "",
        stores: Array.isArray(item.stores) ? item.stores : [],
        storeCount: Array.isArray(item.stores) ? item.stores.length : 0,
        vehicleCount: Array.isArray(item.vehicles) ? item.vehicles.length : 0,
        waveCount: Array.isArray(item.waves) ? item.waves.length : 0,
      }))
      .filter((item) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (!keyword) return true;
        const text = `${item.id} ${item.sourceModule} ${(item.stores || []).map((store) => `${store.id || ""} ${store.name || ""} ${store.plateNo || ""}`).join(" ")}`.toLowerCase();
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        return text.includes(keyword);
      });
    dataArchiveCache.items = items;
    dataArchiveCache.selectedId = items[0]?.id || "";
    renderDataArchivePanels();
  } catch (error) {
    status.textContent = `查询失败：${error?.message || ""}`;
  }
}

function setWmsSyncStatus(text) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const el = document.getElementById("wmsSyncStatus");
  if (el) el.textContent = String(text || "");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function currentStoreSource() {
  const value = String(state.ui.storeDataSource || "sample").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return value === "real" ? "real" : "sample";
}

function currentVehicleSource() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const value = String(state.ui.vehicleDataSource || "sample").trim();
  return value === "real" ? "real" : "sample";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function refreshBaseDataBySource() {
  const sample = sampleData();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeSource = currentStoreSource();
  const vehicleSource = currentVehicleSource();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let stores = [];
  let vehicles = [];

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (storeSource === "real") {
    stores = await fetchWmsStoresFromBackend();
  } else {
    try {
      stores = await fetchStoresFromBackend();
    } catch {}
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!stores.length) stores = sample.stores;
  }

  if (vehicleSource === "real") {
    vehicles = await fetchWmsVehiclesFromBackend();
  } else {
    vehicles = sample.vehicles;
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const resolvedMap = await fetchStoreWaveResolvedLoadMap(stores.map((item) => item?.id).filter(Boolean));
  stores = applyStoreWaveResolvedLoadsToStores(stores, resolvedMap);

  state.stores = stores;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.vehicles = vehicles;
  state.waves = buildAutoWaves(state.stores);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.lastResults = [];
  state.activeResultKey = "";
  renderAll();
  setWmsSyncStatus(`店铺来源=${storeSource === "real" ? "真实业务" : "样本"}；车辆来源=${vehicleSource === "real" ? "真实业务" : "样本"}；货量/波次/时间来源=wms_cargo_raw_clean_snapshot + store_wave_timing_resolved(${resolvedMap.size}店)。`);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function refreshWmsStatus() {
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/status`, {}, 5000);
    if (!response.ok) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const payload = await response.json();
    if (!payload?.ok) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const counts = payload.counts || {};
    const latestBatch = payload.latestBatch || {};
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const mode = latestBatch.mode || "-";
    const finishedAt = latestBatch.finished_at || latestBatch.finishedAt || "-";
    setWmsSyncStatus(`WMS同步状态：店铺${counts.shops || 0}，车辆${counts.vehicles || 0}，货量${counts.cargo || 0}，装载${counts.carload || 0}，到店${counts.arrivaltime || 0}；最近批次=${mode} ${finishedAt}`);
  } catch {}
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function triggerWmsFetch() {
  const box = document.getElementById("validationBox");
  setWmsSyncStatus("正在抓取WMS（只读）...");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let password = "";
  for (let i = 0; i < 2; i += 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/fetch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(password ? { password } : {}),
    }, 120000);
    const payload = response.ok ? await response.json() : { ok: false, error: `HTTP ${response.status}` };
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (payload?.needPassword) {
      const promptText = i === 0 ? "请输入WMS数据库密码（仅本地加密保存）：" : "密码错误，请重新输入WMS数据库密码：";
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const typed = window.prompt(promptText, "");
      if (!typed) {
        setWmsSyncStatus("已取消WMS抓取。");
        return;
      }
      password = String(typed || "").trim();
      continue;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!payload?.ok) {
      const err = payload?.error || "wms_fetch_failed";
      setWmsSyncStatus(`WMS抓取失败：${err}`);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (box) box.textContent = `WMS抓取失败：${err}`;
      return;
    }
    const tableSummary = Array.isArray(payload?.tables) ? payload.tables : [];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const inserted = tableSummary.reduce((sum, item) => sum + Number(item?.insertedRows || 0), 0);
    const skipped = tableSummary.reduce((sum, item) => sum + Number(item?.skippedRows || 0), 0);
    setWmsSyncStatus(`WMS抓取完成：批次${payload.batchId || "-"}，新增${inserted}，去重跳过${skipped}。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (box) box.textContent = `WMS只读抓取完成：新增${inserted}，跳过${skipped}。`;
    await refreshWmsStatus();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    await refreshBaseDataBySource();
    return;
  }
  setWmsSyncStatus("WMS抓取失败：密码校验未通过。");
}

function miniKpiCard(label, value, sub = "", accent = "") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `<article class="kpi-card ${accent}"><p class="kpi-label">${escapeHtml(label)}</p><div class="kpi-value" data-animate-number="${escapeHtml(String(value))}">${escapeHtml(String(value))}</div>${sub ? `<p class="kpi-sub">${escapeHtml(sub)}</p>` : ""}</article>`;
}

function polarPoint(cx, cy, r, angleDeg) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rad = (angleDeg - 90) * Math.PI / 180;
  return [cx + Math.cos(rad) * r, cy + Math.sin(rad) * r];
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function collectComparisonRouteRows(result) {
  const rows = [];
  (result.solution || []).forEach((plans, waveIndex) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave = result.scenario?.waves?.[waveIndex];
    (plans || []).forEach((plan) => {
      (plan.trips || []).forEach((trip) => {
        rows.push({
          waveId: wave?.waveId || "W?",
          plateNo: plan.vehicle?.plateNo || "--",
          tripNo: trip.tripNo || 1,
          storesCount: (trip.route || []).length,
          totalDistance: Number(trip.totalDistance || 0),
          loadRate: Number(trip.loadRate || 0),
        });
      });
    });
  });
  return rows.sort((a, b) =>
    a.waveId.localeCompare(b.waveId, "zh-CN")
    || a.plateNo.localeCompare(b.plateNo, "zh-CN")
    || a.tripNo - b.tripNo
  );
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderComparisonRouteDigest(result) {
  const rows = collectComparisonRouteRows(result);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!rows.length) return `<p class="algo-route-empty">${L("noChartData")}</p>`;
  return `<div class="algo-route-digest">
    <div class="algo-route-digest-head">
      <strong>${L("routeDigest")}</strong>
      <span>${L("routeDigestHint")}</span>
    </div>
    <div class="algo-route-list">
      ${rows.map((row) => `
        <div class="algo-route-row">
          <div class="algo-route-line">${escapeHtml(`${row.waveId} · ${row.plateNo} · ${L("tripLabel")}${row.tripNo}`)}</div>
          <div class="algo-route-metrics">
            <span>${L("storesCount")} ${row.storesCount}</span>
            <span>${L("tripRoundKm")} ${row.totalDistance.toFixed(1)} km</span>
            <span>${L("tripLoadRate")} ${formatRate(row.loadRate)}</span>
          </div>
        </div>
      `).join("")}
    </div>
  </div>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderExecutiveCompare(results) {
  if (!results.length) return `<div class="chart-card wide-card"><div class="chart-title">${L("algoCompare")}</div><div class="note">${L("noChartData")}</div></div>`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ranked = [...results].sort((a, b) => b.metrics.score - a.metrics.score);
  return `<div class="chart-card wide-card executive-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("algoCompare")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "総分・時間順守・距離・積載の4視点で比較します。" : "从总分、时效、距离、装载四个角度看算法差异。"}</p>
      </div>
    </div>
    <div class="algo-board">
      ${ranked.map((result, idx) => {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const score = result.metrics.score || 0;
        const onTime = (result.metrics.scoreBreakdown?.onTimeRatio || 0) * 100;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const distance = (result.metrics.scoreBreakdown?.distanceScore || 0) * 100;
        const load = (result.metrics.scoreBreakdown?.loadScore || 0) * 100;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const pref = (result.metrics.scoreBreakdown?.preferenceScore || 0) * 100;
        return `<article class="algo-rank-card ${idx === 0 ? "best-card" : ""}">
          <div class="algo-rank-top">
            <div>
              <p class="algo-rank-order">TOP ${idx + 1}</p>
              <h3>${escapeHtml(algoLabel(result.key))}</h3>
            </div>
            <div class="algo-score-badge">${score.toFixed(1)}</div>
          </div>
          <div class="algo-meta-grid">
            <span>${L("onTime")} ${formatRate(result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1))}</span>
            <span>${L("totalDistance")} ${result.metrics.totalDistance.toFixed(1)} km</span>
            <span>${L("avgLoad")} ${formatRate(result.metrics.loadRate)}</span>
            <span>${L("fleetLoad")} ${formatRate(result.metrics.fleetLoadRate)}</span>
            <span>${L("usedVehiclesShort")} ${result.metrics.usedVehicleCount}</span>
          </div>
          <div class="score-stack">
            <div class="score-segment score-on-time" style="width:${onTime}%"></div>
            <div class="score-segment score-distance" style="width:${distance}%"></div>
            <div class="score-segment score-load" style="width:${load}%"></div>
            <div class="score-segment score-pref" style="width:${pref}%"></div>
          </div>
          <div class="score-break-grid">
            <span>${L("onTimeScore")} ${onTime.toFixed(0)}</span>
            <span>${L("distanceScoreLabel")} ${distance.toFixed(0)}</span>
            <span>${L("loadScoreLabel")} ${load.toFixed(0)}</span>
            <span>${L("preferenceScoreLabel")} ${pref.toFixed(0)}</span>
          </div>
          ${renderComparisonRouteDigest(result)}
        </article>`;
      }).join("")}
    </div>
  </div>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderComparisonBars(results) {
  if (!results.length) return "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const max = Math.max(...results.map((r) => r.metrics.score), 1);
  return `<div class="chart-card wide-card"><div class="chart-title">${L("algorithmScore")}</div>${results.map((result, idx) => `
    <div class="bar-row">
      <span class="bar-label">${escapeHtml(algoLabel(result.key))}</span>
      <div class="bar-track"><div class="bar-fill series-${idx % 5}" style="width:${(result.metrics.score / max) * 100}%"></div></div>
      <strong>${result.metrics.score.toFixed(1)}</strong>
    </div>
  `).join("")}</div>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function collectGanttRows(result) {
  const rows = [];
  result.solution.forEach((plans, waveIndex) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave = result.scenario.waves[waveIndex];
    plans.forEach((plan) => {
      plan.trips.forEach((trip) => {
        const firstStop = trip.stops[0];
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const serviceEnd = trip.stops[trip.stops.length - 1]?.leave ?? trip.finish;
        rows.push({
          label: `${plan.vehicle.plateNo} · ${wave.waveId} · ${L("tripLabel")}${trip.tripNo}`,
          start: firstStop ? firstStop.arrival : wave.startMin,
          end: wave.waveId === "W1" ? trip.finish : serviceEnd,
          waveType: wave.singleWave ? "single" : "regular",
          overtime: trip.waveLateMinutes || 0,
        });
      });
    });
  });
  return rows;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderGantt(result) {
  const rows = collectGanttRows(result);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!rows.length) return `<div class="chart-card"><div class="chart-title">${L("gantt")}</div><div class="note">${L("noChartData")}</div></div>`;
  const minTime = Math.min(...rows.map((r) => r.start));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const maxTime = Math.max(...rows.map((r) => r.end));
  const span = Math.max(60, maxTime - minTime);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const svgHeight = 60 + rows.length * 34;
  const rowSvg = rows.map((row, index) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const y = 32 + index * 34;
    const x = 150 + ((row.start - minTime) / span) * 760;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const w = Math.max(12, ((row.end - row.start) / span) * 760);
    return `<g>
      <text x="10" y="${y + 14}" class="gantt-label">${escapeHtml(row.label)}</text>
      <rect x="${x}" y="${y}" width="${w}" height="20" rx="8" class="gantt-bar ${row.waveType === "single" ? "single" : "regular"}" />
      ${row.overtime > 0 ? `<rect x="${x + w - 10}" y="${y}" width="10" height="20" rx="0" class="gantt-overtime"/>` : ""}
    </g>`;
  }).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ticks = Array.from({ length: 6 }, (_, i) => {
    const value = minTime + (span / 5) * i;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const x = 150 + (760 / 5) * i;
    return `<g><line x1="${x}" y1="12" x2="${x}" y2="${svgHeight - 10}" class="gantt-grid"/><text x="${x - 12}" y="12" class="gantt-tick">${formatTime(value)}</text></g>`;
  }).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const overtimeCount = rows.filter((row) => row.overtime > 0).length;
  const singleCount = rows.filter((row) => row.waveType === "single").length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `<div class="chart-card wide-card gantt-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("gantt")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "車両×波次×便の時間配置を一眼で確認できます。" : "按车辆 × 波次 × 趟次查看时间占用与超时位置。"}</p>
      </div>
      <div class="gantt-summary">
        <span class="summary-chip">${L("tripLabel")} ${rows.length}</span>
        <span class="summary-chip">${L("singleWaveLabel")} ${singleCount}</span>
        <span class="summary-chip danger">${L("overtimeTripsShort")} ${overtimeCount}</span>
      </div>
    </div>
    <svg viewBox="0 0 940 ${svgHeight}" class="gantt-svg">${ticks}${rowSvg}</svg>
  </div>`;
}

function renderLoadCurves(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const candidates = result.solution.flat().filter((plan) => plan.trips.length).slice(0, 8);
  if (!candidates.length) return `<div class="chart-card"><div class="chart-title">${L("loadCurve")}</div><div class="note">${L("noChartData")}</div></div>`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const series = candidates.map((plan, idx) => {
    const points = [{ x: 0, y: 0 }];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let step = 1;
    plan.trips.forEach((trip) => {
      trip.stops.forEach((stop) => {
        const load = trip.stops.slice(0, trip.stops.indexOf(stop) + 1).reduce((sum, item) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const stopStore = result.scenario.storeMap.get(item.storeId);
          return sum + getStoreSolveLoadForWave(stopStore, { waveId: plan.waveId });
        }, 0);
        points.push({ x: step, y: Math.min(1, load / Math.max(getVehicleSolveCapacity(plan.vehicle), 1)) });
        step += 1;
      });
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { label: plan.vehicle.plateNo, points, cls: `series-${idx % 5}` };
  });
  const maxX = Math.max(...series.map((s) => s.points[s.points.length - 1]?.x || 1), 1);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lines = series.map((s) => {
    const d = s.points.map((p, i) => `${i ? "L" : "M"} ${40 + (p.x / maxX) * 500} ${220 - p.y * 170}`).join(" ");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `<g><path d="${d}" class="load-path ${s.cls}"/><text x="560" y="${40 + series.indexOf(s) * 18}" class="load-legend ${s.cls}">${escapeHtml(s.label)}</text></g>`;
  }).join("");
  return `<div class="chart-card"><div class="chart-title">${L("loadCurve")}</div><svg viewBox="0 0 640 250" class="load-svg">
    <line x1="40" y1="220" x2="560" y2="220" class="axis-line"/><line x1="40" y1="20" x2="40" y2="220" class="axis-line"/>
    <text x="8" y="30" class="axis-text">100%</text><text x="12" y="220" class="axis-text">0%</text><text x="520" y="242" class="axis-text">${escapeHtml(L("timeAxis"))}</text>
    ${lines}
  </svg></div>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderSpatialScatter(result) {
  const stores = result.scenario.stores;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!stores.length) return `<div class="chart-card"><div class="chart-title">${L("spatial")}</div><div class="note">${L("noChartData")}</div></div>`;
  const lngs = [DC.lng, ...stores.map((s) => s.lng)];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lats = [DC.lat, ...stores.map((s) => s.lat)];
  const minLng = Math.min(...lngs), maxLng = Math.max(...lngs), minLat = Math.min(...lats), maxLat = Math.max(...lats);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const normX = (lng) => 40 + ((lng - minLng) / Math.max(0.0001, maxLng - minLng)) * 560;
  const normY = (lat) => 240 - ((lat - minLat) / Math.max(0.0001, maxLat - minLat)) * 180;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const singleSet = new Set(result.scenario.singleWaveStoreIds || []);
  const points = stores.map((store) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const cls = singleSet.has(store.id) ? "single" : ((result.scenario.dist[DC.id][store.id] || 0) > result.scenario.singleWaveThreshold ? "far" : "near");
    return `<g><circle cx="${normX(store.lng)}" cy="${normY(store.lat)}" r="${singleSet.has(store.id) ? 7 : 5}" class="scatter-point ${cls}"/><text x="${normX(store.lng) + 8}" y="${normY(store.lat) - 8}" class="scatter-label">${escapeHtml(store.id)}</text></g>`;
  }).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dc = `<g><circle cx="${normX(DC.lng)}" cy="${normY(DC.lat)}" r="9" class="scatter-dc"/><text x="${normX(DC.lng) + 12}" y="${normY(DC.lat) - 10}" class="scatter-label">${escapeHtml(L("depot"))}</text></g>`;
  return `<div class="chart-card wide-card"><div class="chart-title">${L("spatial")}</div><svg viewBox="0 0 640 280" class="scatter-svg">
    <rect x="20" y="20" width="590" height="220" rx="18" class="scatter-bg"/>
    ${dc}${points}
  </svg>
  <div class="legend-row"><span class="legend-item"><i class="legend-dot near"></i>${L("scatterNear")}</span><span class="legend-item"><i class="legend-dot far"></i>${L("scatterFar")}</span><span class="legend-item"><i class="legend-dot single"></i>${L("scatterSingle")}</span></div>
  </div>`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderDashboard(result) {
  const overtimeTrips = result.solution.flat().flatMap((plan) => plan.trips).filter((trip) => (trip.waveLateMinutes || 0) > 0).length;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const onTimeRatio = result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1);
  const scoreState = result.metrics.score >= 80 ? "state-good" : result.metrics.score >= 60 ? "state-warn" : "state-bad";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const onTimeState = onTimeRatio >= 0.98 ? "state-good" : onTimeRatio >= 0.9 ? "state-warn" : "state-bad";
  const distanceState = result.metrics.totalDistance <= 2300 ? "state-good" : result.metrics.totalDistance <= 2800 ? "state-warn" : "state-bad";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const loadState = result.metrics.loadRate >= 0.65 ? "state-good" : result.metrics.loadRate >= 0.45 ? "state-warn" : "state-bad";
  const mascot = renderMascotAssistant(result, overtimeTrips);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `<div class="chart-card wide-card cockpit-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("dashboard")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "当日の配車状況を一枚で把握する管理ビューです。" : "一眼查看当日调度质量、资源占用和时效表现。"}</p>
      </div>
      <div class="cockpit-highlight ${scoreState}">
        <span>${L("score")}</span>
        <strong data-animate-number="${result.metrics.score.toFixed(1)}">${result.metrics.score.toFixed(1)}</strong>
      </div>
    </div>
    <div class="cockpit-body">
      <div class="cockpit-grid">
        ${miniKpiCard(L("storesToday"), String(result.metrics.totalStops), `${L("wavesLabel")} ${result.scenario.waves.length}`, "kpi-hero")}
        ${miniKpiCard(L("usedVehiclesShort"), String(result.metrics.usedVehicleCount), `${L("idleVehiclesShort")} ${result.metrics.unusedVehicleCount}`, `accent-used ${result.metrics.unusedVehicleCount > 0 ? "state-good" : "state-warn"}`)}
        ${miniKpiCard(L("totalDistance"), `${result.metrics.totalDistance.toFixed(1)} km`, lang() === "ja" ? "短いほど高評価" : "越短越优", distanceState)}
        ${miniKpiCard(L("onTime"), formatRate(onTimeRatio), `${L("overtimeTripsShort")} ${overtimeTrips}`, onTimeState)}
        ${miniKpiCard(L("avgLoad"), formatRate(result.metrics.loadRate), `${algoLabel(result.key)}`, loadState)}
        ${miniKpiCard(L("fleetLoad"), formatRate(result.metrics.fleetLoadRate), L("fleetLoadHint"), result.metrics.fleetLoadRate >= 0.9 ? "state-good" : result.metrics.fleetLoadRate >= 0.65 ? "state-warn" : "state-bad")}
      </div>
      ${mascot}
    </div>
  </div>`;
}

function buildMascotSnapshot(result, overtimeTrips) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (result?.metrics) {
    return {
      score: result.metrics.score,
      usedVehicleCount: result.metrics.usedVehicleCount,
      unusedVehicleCount: result.metrics.unusedVehicleCount,
      totalDistance: result.metrics.totalDistance,
      loadRate: result.metrics.loadRate,
      fleetLoadRate: result.metrics.fleetLoadRate,
      onTimeRatio: result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1),
      overtimeTrips,
      algorithm: algoLabel(result.key),
      riskText: overtimeTrips > 0
        ? (lang() === "ja" ? `超時ライン ${overtimeTrips} 件に注意。` : `当前有 ${overtimeTrips} 条超时线路需要关注。`)
        : (lang() === "ja" ? "超時ラインはありません。" : "当前没有超时线路。"),
      summaryChips: [
        `<span class="summary-chip">${escapeHtml(algoLabel(result.key))}</span>`,
        `<span class="summary-chip">${L("score")} ${result.metrics.score.toFixed(1)}</span>`,
      `<span class="summary-chip">${L("usedVehiclesShort")} ${result.metrics.usedVehicleCount}</span>`,
      `<span class="summary-chip">${L("fleetLoad")} ${formatRate(result.metrics.fleetLoadRate)}</span>`,
      ].join(""),
    };
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    score: 0,
    usedVehicleCount: state.vehicles.length,
    unusedVehicleCount: state.vehicles.length,
    totalDistance: 0,
    loadRate: 0,
    onTimeRatio: 1,
    overtimeTrips: 0,
    algorithm: lang() === "ja" ? "待機中" : "待机中",
    riskText: state.ui.micPriming
      ? L("speechMicPreparing")
      : (lang() === "ja"
          ? "まだ正式な调度结果はありません。先に質問を聞くことも、生成後に要約を聞くこともできます。"
          : "当前还没有正式调度结果。你可以先问按钮用途，生成后再听结果摘要。"),
    summaryChips: [
      `<span class="summary-chip">${escapeHtml(lang() === "ja" ? "待機中" : "待机中")}</span>`,
      `<span class="summary-chip">${L("storesToday")} ${state.stores.length}</span>`,
      `<span class="summary-chip">${L("usedVehiclesShort")} ${state.vehicles.length}</span>`,
    ].join(""),
  };
}

function renderMascotAssistant(result, overtimeTrips) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const snapshot = buildMascotSnapshot(result, overtimeTrips);
  const speakPayload = escapeHtml(JSON.stringify({
    score: snapshot.score,
    usedVehicleCount: snapshot.usedVehicleCount,
    unusedVehicleCount: snapshot.unusedVehicleCount,
    totalDistance: snapshot.totalDistance,
    loadRate: snapshot.loadRate,
    onTimeRatio: snapshot.onTimeRatio,
    overtimeTrips: snapshot.overtimeTrips,
    algorithm: snapshot.algorithm,
  }));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const deepseekReady = Boolean(String(state.ai.deepseekApiKey || "").trim());
  const assistantExpanded = !!state.ui.assistantExpanded;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `
    <aside class="mascot-card">
      <div class="mascot-visual" aria-hidden="true">
        <div class="mascot-avatar-shell">
          <div class="mascot-avatar ${state.ui.speaking ? "is-speaking" : ""}">
            <img class="mascot-photo" src="${MASCOT_IMAGE_SRC}" alt="${escapeHtml(L("mascotTitle"))}" onerror="this.style.display='none'; this.parentElement.classList.add('image-missing');">
            <div class="mascot-fallback" aria-hidden="true">
              <div class="mascot-fallback-badge">AI</div>
              <div class="mascot-fallback-title">${L("mascotTitle")}</div>
              <div class="mascot-fallback-sub">${state.lang === "ja" ? "Dispatch Core" : "Dispatch Core"}</div>
            </div>
            <span class="mascot-status-dot"></span>
          </div>
        </div>
      </div>
      <div class="mascot-copy">
        <div class="mascot-title-row">
          <div>
            <p class="mascot-title">${L("mascotTitle")}</p>
            <p class="kpi-sub">${L("mascotDesc")}</p>
          </div>
          <div class="mascot-action-row">
            <button class="alert speech-trigger" data-speech='${speakPayload}'>${L("voiceBroadcast")}</button>
            <button class="secondary speech-listen-trigger">${state.ui.listening || state.ui.micPriming ? `${L("voiceAsk")}...` : L("voiceAsk")}</button>
          </div>
        </div>
        <div class="mascot-brief">
          ${snapshot.summaryChips}
        </div>
        <p class="mascot-risk">${escapeHtml(snapshot.riskText)}</p>
        <div class="assistant-inline-actions compact-assistant-actions">
          <button class="${assistantExpanded ? "secondary" : "primary"} toggle-assistant-panel">
            ${lang() === "ja" ? (assistantExpanded ? "助手を閉じる" : "助手を開く") : (assistantExpanded ? "收起助手" : "打开助手")}
          </button>
        </div>
        ${assistantExpanded ? `
        <div class="assistant-config">
          <label class="assistant-field">
            <span>${L("deepseekKeyLabel")}</span>
            <input id="deepseekApiKeyInput" type="password" placeholder="sk-..." value="${escapeHtml(state.ai.deepseekApiKey || "")}">
          </label>
          <label class="assistant-field">
            <span>${L("deepseekModelLabel")}</span>
            <select id="deepseekModelSelect">
              <option value="deepseek-chat" ${state.ai.deepseekModel === "deepseek-chat" ? "selected" : ""}>deepseek-chat</option>
              <option value="deepseek-reasoner" ${state.ai.deepseekModel === "deepseek-reasoner" ? "selected" : ""}>deepseek-reasoner</option>
            </select>
          </label>
          <label class="assistant-field">
            <span>${L("deepseekModeLabel")}</span>
            <select id="deepseekModeSelect">
              <option value="dispatch" ${state.ai.mode === "dispatch" ? "selected" : ""}>${L("deepseekModeDispatch")}</option>
              <option value="general" ${state.ai.mode === "general" ? "selected" : ""}>${L("deepseekModeGeneral")}</option>
            </select>
          </label>
          <label class="assistant-field">
            <span>${L("deepseekStyleLabel")}</span>
            <select id="deepseekStyleSelect">
              <option value="brief" ${state.ai.answerStyle === "brief" ? "selected" : ""}>${L("deepseekStyleBrief")}</option>
              <option value="detailed" ${state.ai.answerStyle === "detailed" ? "selected" : ""}>${L("deepseekStyleDetailed")}</option>
            </select>
          </label>
          <div class="assistant-inline-actions">
            <button class="secondary save-deepseek-config">${L("deepseekSave")}</button>
            <span class="assistant-status">${deepseekReady ? L("deepseekReady") : L("deepseekLocalFallback")}</span>
          </div>
        </div>
        <div class="assistant-ask-box">
          <textarea id="assistantQuestionInput" class="assistant-question-input" rows="3" placeholder="${escapeHtml(L("deepseekAskPlaceholder"))}">${escapeHtml(state.ai.questionDraft || "")}</textarea>
          <div class="assistant-inline-actions">
            <button class="alert ask-deepseek-btn" ${state.ai.loading ? "disabled" : ""}>${state.ai.loading ? escapeHtml(L("deepseekThinking")) : escapeHtml(L("deepseekAsk"))}</button>
          </div>
          ${state.ai.lastQuestion ? `<p class="assistant-last-question"><strong>Q:</strong> ${escapeHtml(state.ai.lastQuestion)}</p>` : ""}
          ${state.ai.lastAnswer ? `<div class="assistant-answer"><strong>${L("deepseekAnswerPrefix")}</strong><p>${escapeHtml(state.ai.lastAnswer)}</p></div>` : ""}
        </div>
        ` : ""}
      </div>
    </aside>
  `;
}

function buildSpeechText(payload) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lang() === "ja") {
    return `ミツアナグマ配車官より報告します。現在の推奨アルゴリズムは${payload.algorithm}です。総合スコアは${payload.score.toFixed(1)}点。使用車両は${payload.usedVehicleCount}台、待機車両は${payload.unusedVehicleCount}台、総距離は${payload.totalDistance.toFixed(1)}キロ、平均単便積載率は${Math.round(payload.loadRate * 100)}パーセント、車隊稼働率は${Math.round((payload.fleetLoadRate || 0) * 100)}パーセント、未遅着率は${Math.round(payload.onTimeRatio * 100)}パーセントです。${payload.overtimeTrips > 0 ? `なお、超時ラインが${payload.overtimeTrips}件あります。` : "超時ラインはありません。"}`
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `鲸略使助手汇报。当前推荐算法是${payload.algorithm}。综合评分${payload.score.toFixed(1)}分。已用车辆${payload.usedVehicleCount}辆，闲置车辆${payload.unusedVehicleCount}辆，总里程${payload.totalDistance.toFixed(1)}公里，平均单趟装载率${Math.round(payload.loadRate * 100)}%，车队利用率${Math.round((payload.fleetLoadRate || 0) * 100)}%，未晚到率${Math.round(payload.onTimeRatio * 100)}%。${payload.overtimeTrips > 0 ? `另外还有${payload.overtimeTrips}条超时线路需要关注。` : "当前没有超时线路。"}`
}

function assistantButtonCatalog() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return [
    {
      key: "generate",
      aliases: ["生成调度结果", "生成结果", "开始调度", "run", "generate", "配車生成", "生成"],
      answerZh: "“生成调度结果”会按当前门店、车辆、波次、算法和目标设置，正式开始计算调度方案。",
      answerJa: "「生成调度结果」は、現在の店舗・車両・波次・アルゴリズム・目標設定に基づいて、本番の配車計算を開始します。",
    },
    {
      key: "reload",
      aliases: ["重新加载固定门店", "重新加载", "加载样例", "reload", "サンプル再読込", "再加载"],
      answerZh: "“重新加载固定门店”会把系统恢复到当前内置的固定门店样例数据。",
      answerJa: "「重新加载固定门店」は、現在内蔵している固定店舗サンプルに戻します。",
    },
    {
      key: "save",
      aliases: ["保存当前方案", "保存方案", "save", "保存"],
      answerZh: "“保存当前方案”会把当前结果存到浏览器本地，方便之后继续看或对比。",
      answerJa: "「保存当前方案」は、現在の結果をブラウザのローカルに保存して、あとで見直したり比較できるようにします。",
    },
    {
      key: "export",
      aliases: ["导出当前结果", "导出结果", "export", "导出"],
      answerZh: "“导出当前结果”会把当前选中的调度方案导出成文本结果。",
      answerJa: "「导出当前结果」は、現在選択中の配車結果をテキストとして書き出します。",
    },
    {
      key: "autowave",
      aliases: ["自动分波次", "按固定门店自动分波次", "分波次", "auto wave", "波次自動生成"],
      answerZh: "“按固定门店自动分波次”会根据当前门店规则自动生成一版波次配置。",
      answerJa: "「按固定门店自动分波次」は、現在の店舗ルールに基づいて波次設定を自動生成します。",
    },
    {
      key: "quick",
      aliases: ["快速初排", "快速", "初排", "quick"],
      answerZh: "“快速初排”会直接切到快速构造模式，并马上生成一版可用初稿。",
      answerJa: "「快速初排」は、素早い初期構築モードに切り替えて、すぐ使える初稿を作ります。",
    },
    {
      key: "deep",
      aliases: ["继续优化", "深度优化", "优化", "deep"],
      answerZh: "“继续优化”会切到改进型算法模式，在现有思路上继续深挖更好的结果。",
      answerJa: "「継続改善」は、改善系アルゴリズムに切り替えて、現在の案をさらに磨き込みます。",
    },
    {
      key: "global",
      aliases: ["全局搜索", "全局", "global"],
      answerZh: "“全局搜索”会启用更偏全局探索的算法，尝试找到完全不同的排线结构。",
      answerJa: "「全局搜索」は、大域探索寄りのアルゴリズムを使って、まったく違う配線構造を探します。",
    },
    {
      key: "relay",
      aliases: ["接力求解", "接力", "relay"],
      answerZh: "“接力求解”会先做初排，再让后续算法一棒一棒接着优化，并实时打印每一阶段在做什么。",
      answerJa: "「リレー最適化」は、まず初期案を作り、その後のアルゴリズムが段階ごとに引き継いで改善し、各段階の内容も順に表示します。",
    },
    {
      key: "compare",
      aliases: ["多算法对比", "对比", "compare", "比較"],
      answerZh: "“多算法对比”会并排跑多种算法，方便你看同一批数据下谁更好。",
      answerJa: "「多算法对比」は、複数アルゴリズムを並列に走らせて、同じデータでどれが良いか比較しやすくします。",
    },
    {
      key: "routeMap",
      aliases: ["看线路图", "线路图", "route map", "地図"],
      answerZh: "“看线路图”会打开可拖动、可缩放的路线地图，并标出顺序号和店名。",
      answerJa: "「ルート図」は、ドラッグや拡大縮小ができる地図を開き、訪問順と店舗名を表示します。",
    },
    {
      key: "viz",
      aliases: ["查看可视化", "可视化", "回放", "playback"],
      answerZh: "“查看可视化”会打开过程回放，告诉你算法是怎么一步一步排出来的。",
      answerJa: "「可視化を見る」は、計算の流れを開き、アルゴリズムがどう組み立てたかを順に確認できます。",
    },
  ];
}

function normalizeAssistantQuery(text) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return String(text || "").toLowerCase().replace(/\s+/g, "");
}

function buildAssistantAnswer(queryText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const normalized = normalizeAssistantQuery(queryText);
  const catalog = assistantButtonCatalog();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const askAll = ["每个按钮", "所有按钮", "按钮名字", "按钮名称", "有哪些按钮", "全部按钮", "ボタン", "すべてのボタン", "ボタン名"].some((token) => normalized.includes(normalizeAssistantQuery(token)));
  if (askAll) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lines = catalog.map((item) => lang() === "ja" ? item.answerJa : item.answerZh);
    return `${lang() === "ja" ? "この画面でよく使うボタンは主に次の通りです。" : "这页常用按钮主要有这些。"} ${lines.join(" ")}`;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const matched = catalog.find((item) => item.aliases.some((alias) => normalized.includes(normalizeAssistantQuery(alias))));
  if (matched) return lang() === "ja" ? matched.answerJa : matched.answerZh;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return lang() === "ja"
    ? "質問は聞き取れましたが、まだ特定のボタンに結び付いていません。たとえば「クイック初期案は何をするの」や「この画面のボタンは何ができるの」と聞いてください。"
    : "我听到了你的问题，但还没匹配到具体按钮。你可以直接问，比如“快速初排是干什么的”或者“每个按钮都有什么用”。";
}

function buildDeepSeekDispatchContext(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const now = new Date();
  const nowZh = now.toLocaleString("zh-CN", { hour12: false });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const briefMode = (state.ai.answerStyle || "brief") !== "detailed";
  
  const algorithmKnowledge = {
    aco: "蚁群算法(ACO)：模拟蚂蚁觅食的信息素正反馈机制。适合100-300家门店的中等规模问题，能自动发现优质路径模式。特点是：信息素挥发率0.25、6只蚂蚁、精英策略。优点是能找到全局较优解，缺点是前期收敛较慢。",
    pso: "粒子群算法(PSO)：模拟鸟群捕食的社会学习行为。适合50-150家门店。特点是：20个粒子、惯性权重0.7递减、个体最优+群体最优引导。优点是收敛快、适合连续优化，缺点是可能早熟。",
    sa: "模拟退火(SA)：模拟金属退火过程的概率性搜索。适合任意规模。特点是：初始温度5000、降温速率0.97、Metropolis接受准则、自适应重启。优点是能跳出局部最优，缺点是参数敏感。",
    tabu: "禁忌搜索(Tabu)：带短期记忆的邻域搜索。适合小规模精确优化。特点是：禁忌表长度12、特赦准则、40个邻域候选。优点是能避免循环搜索，缺点是记忆开销大。",
    lns: "大邻域搜索(LNS)：破坏-修复双阶段优化。适合200-500家门店。特点是：破坏比例0.35、支持随机/最差破坏、贪婪/遗憾修复。优点是能大规模重构解，缺点是修复质量依赖破坏策略。",
    hybrid: "混合算法：串联SA+LNS+Tabu三阶段。先全局探索，再局部精修，最后收口。适合追求极致质量时使用。优点是综合各算法优点，缺点是耗时最长。",
    ga: "遗传算法(GA)：模拟自然选择的进化算法。适合中等规模。特点是：锦标赛选择、部分映射交叉、三种变异算子、精英保留。优点是全局搜索能力强，缺点是收敛慢。",
    savings: "Clark-Wright节约法：经典构造启发式。按节约值合并路线，快速生成初始解。优点是速度快、解质量稳定，缺点是难突破局部最优。",
    vrptw: "VRPTW贪心插入：按时间窗优先排序，逐个插入门店。优点是可行性好、约束处理严格，缺点是解质量依赖排序。"
  };
  
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const costFormula = `成本公式 = 到店超偏差罚分(超1分钟+20000) + 晚到罚分(超1分钟+60) + 超波次罚分(超1分钟+80) + 车辆续跑罚分(前波次里程×1.2 + 前波次趟数×150) + 额外趟次罚分(每多1趟+180) + 超时路线罚分(每条+1600或240) + 距离成本(每公里0.45) - 装载奖励(每箱0.08)

解读：到店超偏差罚分极高(20000)意味着系统优先保证门店准时；数值越小越好，0最理想。`;

  if (!result?.metrics) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `【系统状态】当前没有调度结果。
【可用功能】你可以问我：这9种算法有什么区别？VRPTW和节约法哪个好？蚁群算法怎么工作的？成本是怎么算的？未调度门店怎么办？
【回答风格】${briefMode ? "简洁回答，3-6行即可。" : "可以详细分析，但要抓住重点。"}
【当前时间】${nowZh}，以此为准。`;
  }

  const waveSummaries = result.solution.map((plans, idx) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave = result.scenario.waves[idx];
    const totalTrips = plans.reduce((s, p) => s + (p.trips?.length || 0), 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const totalDist = plans.reduce((s, p) => s + (p.totalDistance || 0), 0);
    const usedVehicles = plans.filter(p => p.trips?.length).length;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lateTrips = plans.flatMap(p => p.trips || []).filter(t => (t.waveLateMinutes || 0) > 0).length;
    return `【${wave.waveId}】${wave.start}-${wave.end}：用车${usedVehicles}辆，${totalTrips}趟，里程${totalDist.toFixed(1)}km，超时线路${lateTrips}条。`;
  }).join("\n");

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const algorithmKey = result.key;
  const algorithmDesc = algorithmKnowledge[algorithmKey] || `${algorithmKey}算法，详情请查看系统文档。`;

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `【角色】你是鲸略使调度求解器的智能调度助手，专精于VRPTW算法解读和调度结果分析。

【你能做的事】1.解释9种调度算法的原理、优缺点和适用场景 2.分析当前调度结果的质量和问题 3.解读成本拆分的含义 4.建议如何改善未调度门店 5.回答算法对比、参数调优等问题

【当前调度结果】
- 使用的算法: ${result.label}
- 综合评分: ${result.metrics.score.toFixed(1)}/100
- 已调度门店: ${result.metrics.scheduledCount || 0}家
- 未调度门店: ${result.metrics.unscheduledCount || 0}家
- 准时率: ${((result.metrics.totalOnTime || 0) / Math.max(result.metrics.totalStops || 1, 1) * 100).toFixed(1)}%
- 总里程: ${(result.metrics.totalDistance || 0).toFixed(1)} km
- 平均装载率: ${((result.metrics.loadRate || 0) * 100).toFixed(1)}%
- 车队利用率: ${((result.metrics.fleetLoadRate || 0) * 100).toFixed(1)}%
- 已用车辆: ${result.metrics.usedVehicleCount || 0}辆
- 闲置车辆: ${result.metrics.unusedVehicleCount || 0}辆

【各波次详情】
${waveSummaries}

【成本公式】
${costFormula}

【算法知识】
当前使用的${result.label}：${algorithmDesc}

【未调度门店】
${result.metrics.unscheduledCount ? (result.metrics.unscheduledStores || []).slice(0, 10).map(s => `- ${s.storeName}(${s.storeId})：${s.reasonText || '约束不满足'}`).join("\n") : "无未调度门店"}

【回答要求】
1. 基于以上真实数据回答，不要编造
2. ${briefMode ? "回答要简洁，3-6行说清核心问题" : "可以详细分析，但要结构清晰"}
3. 遇到未调度门店，给出具体改善建议（放宽时间窗、调整波次、增加车辆等）
4. 遇到算法问题，结合算法特点解释
5. 使用中文，语气专业但不生硬

【当前时间】${nowZh}

用户问题：`;
}

function buildDeepSeekGeneralContext() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const now = new Date();
  const nowZh = now.toLocaleString("zh-CN", { hour12: false });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nowJa = now.toLocaleString("ja-JP", { hour12: false });
  const briefMode = (state.ai.answerStyle || "brief") !== "detailed";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return lang() === "ja"
    ? [
        "あなたはこのページ内で動作する汎用アシスタントです。",
        `現在のローカル日時は ${nowJa} です。日付や時刻に関する質問は必ずこれを基準にしてください。`,
        "一般的な質問にも答えてよいですが、このページ上の配車結果がある場合は、その文脈も活用してください。",
        "不確かなことは断定せず、簡潔かつ実用的に答えてください。",
        briefMode
          ? "既定では 4 〜 8 行程度の短い回答にし、Markdown の表や長い箇条書きは避けてください。"
          : "今回はやや詳しく答えてよいですが、Markdown の表は使わず、読みやすさを優先してください。",
      ].join("\n")
    : [
        "你是当前页面里的通用助手。",
        `当前本地日期时间是 ${nowZh}，凡是涉及今天几号、现在几点、星期几，都必须以这个时间为准。`,
        "你可以回答一般问题；如果问题与当前页面调度结果有关，也可以结合页面上下文。",
        "不确定的事不要硬编，回答尽量简洁、实用。",
        briefMode
          ? "默认只给 4 到 8 行的短回答，不要输出 Markdown 表格或超长大段。"
          : "这次允许详细一点，但不要输出 Markdown 表格，也不要过度冗长。",
      ].join("\n");
}

function detectAssistantPageName() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const path = String(window.location.pathname || "").toLowerCase();
  if (path.includes("dengtang")) return "登堂";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (path.includes("rengong-boundary")) return "缘起·性空";
  if (path.includes("rengong")) return "坐照";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (path.includes("chaos-jianwei")) return "见微";
  if (path.includes("kuijing")) return "窥径";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (path.includes("tuiyan") || path.includes("chaos")) return "混沌";
  const chaosPanel = document.getElementById("tuiyanPage");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (chaosPanel && chaosPanel.style.display !== "none" && !chaosPanel.hidden) return "混沌";
  return "坐照";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function detectAssistantDate() {
  const candidateIds = [
    "dateInput",
    "routeDateStart",
    "routeDateEnd",
    "dataArchiveDateInput",
    "archiveDateFilterInput",
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const id of candidateIds) {
    const el = document.getElementById(id);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (el && String(el.value || "").trim()) return String(el.value).trim();
  }
  const firstDateInput = document.querySelector('input[type="date"][value]:not([value=""])');
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (firstDateInput && String(firstDateInput.value || "").trim()) return String(firstDateInput.value).trim();
  return "";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function detectAssistantRouteId() {
  const byState =
    (state && state.ui && (state.ui.selectedRouteId || state.ui.routeId || state.ui.currentRouteId)) || "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (String(byState || "").trim()) return String(byState).trim();
  const activeRouteNode = document.querySelector("[data-route-id].active, [data-route-id].is-active, tr[data-route-id].selected");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (activeRouteNode) {
    const rid = activeRouteNode.getAttribute("data-route-id");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (rid && String(rid).trim()) return String(rid).trim();
  }
  return "";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function detectAssistantFilters() {
  const parts = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ids = ["routeStatusFilter", "routeCategoryFilter", "rdGroupFilter", "rdTypeFilter", "rdRouteNameFilter"];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!el) return;
    if (el.tagName === "SELECT" && el.multiple) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const values = Array.from(el.selectedOptions || [])
        .map((o) => String(o.textContent || o.value || "").trim())
        .filter(Boolean);
      if (values.length) parts.push(`${id}:${values.join("|")}`);
      return;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const value = String(el.value || "").trim();
    if (value) parts.push(`${id}:${value}`);
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const activeFilterBtns = Array.from(document.querySelectorAll(".dt-filter-btn.active, .bd-filter-btn.active"))
    .map((el) => String(el.textContent || "").trim())
    .filter(Boolean);
  if (activeFilterBtns.length) parts.push(`active:${activeFilterBtns.join("|")}`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return parts.join("; ");
}

function buildAssistantContextBlock(questionText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const q = String(questionText || "");
  const debug = /debug\s*=\s*true/i.test(q);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ctx = {
    page: detectAssistantPageName() || "坐照",
    date: detectAssistantDate() || "",
    route_id: detectAssistantRouteId() || "",
    filters: detectAssistantFilters() || "全部",
    debug: debug ? "true" : "false",
  };
  return [
    "[CTX]",
    `page=${ctx.page}`,
    `date=${ctx.date}`,
    `route_id=${ctx.route_id}`,
    `filters=${ctx.filters}`,
    `debug=${ctx.debug}`,
    "[/CTX]",
    "",
  ].join("\n");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function askDeepSeekAssistant(questionText, result) {
  const apiKey = String(state.ai.deepseekApiKey || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!apiKey) throw new Error(L("deepseekMissingKey"));
  const systemPrompt = state.ai.mode === "general"
    ? buildDeepSeekGeneralContext()
    : buildDeepSeekDispatchContext(result);
  
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let enhancedQuestion = questionText;
  const lowerQuestion = questionText.toLowerCase();
  
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lowerQuestion.includes("算法") || lowerQuestion.includes("区别") || lowerQuestion.includes("哪个好")) {
    enhancedQuestion = `关于算法对比的问题：${questionText}\n\n请结合我上面提供的9种算法知识库来回答，说明各自特点和适用场景。`;
  } else if (lowerQuestion.includes("为什么") || lowerQuestion.includes("原因")) {
    enhancedQuestion = `关于原因分析的问题：${questionText}\n\n请基于上面提供的调度结果数据来分析原因，给出具体解释。`;
  } else if (lowerQuestion.includes("怎么") || lowerQuestion.includes("如何") || lowerQuestion.includes("建议")) {
    enhancedQuestion = `关于改进建议的问题：${questionText}\n\n请基于当前未调度门店和约束情况，给出具体可操作的建议。`;
  } else if (lowerQuestion.includes("成本") || lowerQuestion.includes("代价") || lowerQuestion.includes("评分")) {
    enhancedQuestion = `关于成本评分的问题：${questionText}\n\n请参考上面的成本公式来回答，解释各项罚分的含义。`;
  }
  
  const ctxPrefix = buildAssistantContextBlock(questionText);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const response = await fetch(`${GA_BACKEND_URL}/deepseek-chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      apiKey,
      model: state.ai.deepseekModel || "deepseek-chat",
      temperature: 0.4,
      max_tokens: 2048,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: `${ctxPrefix}${enhancedQuestion}` },
      ],
    }),
  });
  if (!response.ok) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const text = await response.text();
    throw new Error(`HTTP ${response.status}${text ? ` - ${text.slice(0, 180)}` : ""}`);
  }
  const payload = await response.json();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!payload?.ok) throw new Error(payload?.error || "deepseek_proxy_failed");
  let answer = String(payload?.content || "").trim();
  
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (answer.length < 30 && (answer.includes("sorry") || answer.includes("无法") || answer.includes("不能"))) {
    answer = `抱歉，我理解这个问题可能有困难。\n\n您可以这样问我：\n- "为什么这次用了${result?.label || '当前'}算法？"\n- "当前结果有什么问题？"\n- "未调度门店怎么改善？"\n- "ACO和PSO算法有什么区别？"\n\n当前调度评分${result?.metrics?.score?.toFixed(1) || '?'}分，${result?.metrics?.unscheduledCount ? `有${result.metrics.unscheduledCount}家门店未调度` : '全部门店已调度'}。`;
  }
  
  return answer;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function submitAssistantQuestion(questionText, { speak = false } = {}) {
  const text = String(questionText || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const box = document.getElementById("validationBox");
  if (!text) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (box) box.textContent = lang() === "ja" ? "先に質問を入力してください。" : "请先输入问题。";
    return;
  }
  const activeResult = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0] || null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ai.lastQuestion = text;
  if (!state.ai.deepseekApiKey) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const localAnswer = buildAssistantAnswer(text);
    state.ai.lastAnswer = localAnswer;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (box) box.textContent = `${L("deepseekLocalFallback")} ${L("speechAnswerPrefix")} ${localAnswer}`;
    renderAnalytics();
    if (speak) speakAssistantAnswer(localAnswer);
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ai.loading = true;
  state.ai.lastAnswer = "";
  renderAnalytics();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (box) box.textContent = L("deepseekThinking");
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const answer = await askDeepSeekAssistant(text, activeResult);
    state.ai.lastAnswer = answer || (lang() === "ja" ? "回答が空でした。" : "这次回答内容为空。");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (box) box.textContent = `${L("deepseekAnswerPrefix")} ${state.ai.lastAnswer}`;
    if (speak) speakAssistantAnswer(state.ai.lastAnswer);
  } catch (error) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const message = error?.message || (lang() === "ja" ? "不明なエラー" : "未知错误");
    if (box) box.textContent = LT("deepseekFailed", { message });
  } finally {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ai.loading = false;
    renderAnalytics();
  }
}

function speakAssistantAnswer(text) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!("speechSynthesis" in window) || typeof SpeechSynthesisUtterance === "undefined") {
    document.getElementById("validationBox").textContent = L("speechUnsupported");
    return;
  }
  window.speechSynthesis.cancel();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.speaking = true;
  document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.add("is-speaking"));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  utterance.rate = lang() === "ja" ? 0.98 : 0.92;
  utterance.pitch = 1;
  utterance.onend = () => {
    state.ui.speaking = false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  utterance.onerror = () => {
    state.ui.speaking = false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  window.speechSynthesis.speak(utterance);
}

async function ensureAssistantMicPermission() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (state.ui.micPermission === "granted") return true;
  if (state.ui.micPermission === "denied") {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const deniedBox = document.getElementById("validationBox");
    if (deniedBox) deniedBox.textContent = L("speechMicDenied");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return false;
  }
  if (!navigator.mediaDevices?.getUserMedia) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return true;
  }
  if (state.ui.micPriming) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const box = document.getElementById("validationBox");
  state.ui.micPriming = true;
  renderAnalytics();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (box) box.textContent = L("speechMicPreparing");
  try {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    stream.getTracks().forEach((track) => track.stop());
    state.ui.micPermission = "granted";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return true;
  } catch (error) {
    const denied = ["NotAllowedError", "PermissionDeniedError", "SecurityError"].includes(error?.name || "");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ui.micPermission = denied ? "denied" : "error";
    if (box) box.textContent = denied ? L("speechMicDenied") : L("speechMicFailed");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return false;
  } finally {
    state.ui.micPriming = false;
    renderAnalytics();
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function startAssistantListening() {
  const Recognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!Recognition) {
    document.getElementById("validationBox").textContent = L("speechListenUnsupported");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (state.ui.listening || state.ui.micPriming) return;
  const micReady = await ensureAssistantMicPermission();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!micReady) return;
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.listening = true;
  renderAnalytics();
  if (box) box.textContent = L("speechListening");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const recognition = new Recognition();
  recognition.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  recognition.interimResults = false;
  recognition.maxAlternatives = 1;
  recognition.onresult = (event) => {
    const text = event.results?.[0]?.[0]?.transcript?.trim() || "";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ai.questionDraft = text;
    if (box) box.textContent = LT("speechHeard", { text });
    void submitAssistantQuestion(text, { speak: true });
  };
  recognition.onerror = (event) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (event?.error === "not-allowed" || event?.error === "service-not-allowed") {
      state.ui.micPermission = "denied";
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (box) box.textContent = L("speechMicDenied");
      return;
    }
    if (event?.error === "aborted") {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (box) box.textContent = lang() === "ja" ? "ブラウザの権限ポップアップで一度中断されました。1 秒ほど待ってからもう一度話しかけてください。" : "刚刚被浏览器权限弹窗打断了，请稍等一秒后再问。";
      return;
    }
    if (box) box.textContent = lang() === "ja" ? "音声認識に失敗しました。少し待ってからもう一度お試しください。" : "语音识别失败，请稍等片刻后再试。";
  };
  recognition.onend = () => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    state.ui.listening = false;
    renderAnalytics();
  };
  recognition.start();
}

function triggerSpeech(payloadText) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!("speechSynthesis" in window) || typeof SpeechSynthesisUtterance === "undefined") {
    document.getElementById("validationBox").textContent = L("speechUnsupported");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let payload;
  try {
    payload = JSON.parse(payloadText);
  } catch {
    return;
  }
  window.speechSynthesis.cancel();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.speaking = true;
  document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.add("is-speaking"));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const utterance = new SpeechSynthesisUtterance(buildSpeechText(payload));
  utterance.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  utterance.rate = lang() === "ja" ? 0.95 : 0.9;
  utterance.pitch = 1;
  utterance.onend = () => {
    state.ui.speaking = false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  utterance.onerror = () => {
    state.ui.speaking = false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  window.speechSynthesis.speak(utterance);
}

function animateDashboardNumbers() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.querySelectorAll("[data-animate-number]").forEach((node) => {
    const raw = node.dataset.animateNumber;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!raw || node.dataset.animated === raw) return;
    node.dataset.animated = raw;
    const match = String(raw).match(/-?\d+(\.\d+)?/);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!match) {
      node.textContent = raw;
      return;
    }
    const target = Number(match[0]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const suffix = String(raw).replace(match[0], "");
    const duration = 700;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const start = performance.now();
    const decimals = match[0].includes(".") ? match[0].split(".")[1].length : 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const step = (now) => {
      const progress = Math.min(1, (now - start) / duration);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const eased = 1 - Math.pow(1 - progress, 3);
      node.textContent = `${(target * eased).toFixed(decimals)}${suffix}`;
      if (progress < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderGenerationProgress() {
  const mount = document.getElementById("generationProgress");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!mount) return;
  const progress = Math.max(0, Math.min(100, state.ui.progress || 0));
  mount.innerHTML = `
    <div class="chart-card progress-card">
      <div class="chart-title">${L("progressTitle")}</div>
      <div class="progress-track"><div class="progress-fill" style="width:${progress}%"></div></div>
      <div class="progress-meta"><strong>${progress.toFixed(0)}%</strong><span>${escapeHtml(state.ui.progressText || L("progressIdle"))}</span></div>
    </div>
  `;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderAnalytics() {
  const panel = ensureAnalyticsMount();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const ganttPanel = ensureGanttMount();
  if (!panel || !ganttPanel) return;
  applyMainTabVisibility();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("analyticsTitle").textContent = L("analyticsTitle");
  document.getElementById("analyticsDesc").textContent = L("analyticsDesc");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("ganttTitle").textContent = L("gantt");
  document.getElementById("ganttDesc").textContent = lang() === "ja"
    ? "車両 × 波次 × 便の時間配置を下段で確認できます。"
    : "在下方单独查看车辆 × 波次 × 趟次的时间占用。";
  renderGenerationProgress();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const mount = document.getElementById("analyticsContent");
  const ganttMount = document.getElementById("ganttContent");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!mount) return;
  if (!ganttMount) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const activeResult = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  if (!activeResult) {
    mount.innerHTML = `
      <div class="chart-card wide-card cockpit-card">
        <div class="chart-head">
          <div>
            <div class="chart-title">${L("dashboard")}</div>
            <p class="kpi-sub">${lang() === "ja" ? "まだ結果がなくても、ここで鯨略使アシスタントを起動できます。" : "即使还没生成调度结果，也可以先在这里唤起鲸略使助手。"}</p>
          </div>
          <div class="cockpit-highlight state-warn">
            <span>${lang() === "ja" ? "状態" : "状态"}</span>
            <strong>${lang() === "ja" ? "待機中" : "待机中"}</strong>
          </div>
        </div>
        <div class="cockpit-body">
          ${renderMascotAssistant(null, 0)}
        </div>
      </div>
    `;
    ganttMount.innerHTML = `<div class="chart-card wide-card"><div class="note">${L("noChartData")}</div></div>`;
    dockAssistantPanel();
    return;
  }
  mount.innerHTML = `
    ${renderDashboard(activeResult)}
    ${renderExecutiveCompare(state.lastResults)}
    ${renderComparisonBars(state.lastResults)}
  `;
  ganttMount.innerHTML = renderGantt(activeResult);
  dockAssistantPanel();
  animateDashboardNumbers();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildTargetScoreAdvice(bestResult) {
  const target = Number(state.settings.targetScore || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!target || !bestResult?.metrics?.feasible) return "";
  const gap = target - bestResult.metrics.score;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (gap <= 0) {
    return lang() === "ja"
      ? `現在の最良案 ${algoLabel(bestResult.key)} は目標スコア ${target.toFixed(1)} を達成しています。`
      : `当前最佳方案 ${algoLabel(bestResult.key)} 已达到目标评分 ${target.toFixed(1)}。`;
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const assumptions = [];
  if (bestResult.metrics.totalDistance > 260) {
    assumptions.push(lang() === "ja"
      ? "通常波次の距離上限を少し緩める、または遠距離店舗を単独波次へ多く移す"
      : "适度放宽普通波次的里程上限，或把更多远距离门店划入单波次");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if ((bestResult.metrics.totalOnTime / Math.max(bestResult.metrics.totalStops, 1)) < 0.98) {
    assumptions.push(lang() === "ja"
      ? "波次の時間帯を広げる、または希望到着時刻を分散させる"
      : "拉宽波次时段，或把期望到达时间分散一些");
  }
  if (bestResult.metrics.loadRate < 0.65) {
    assumptions.push(lang() === "ja"
      ? "車両を少し減らすか、手動調車で積載を寄せる"
      : "适度减少启用车辆，或通过手工调车把装载集中一些");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if ((bestResult.metrics.unusedVehicleCount || 0) === 0) {
    assumptions.push(lang() === "ja"
      ? "車両数を追加して、通常波次の偏りをさらに平準化する"
      : "增加可用车辆，进一步摊平普通波次压力");
  }
  if (!assumptions.length) {
    assumptions.push(lang() === "ja"
      ? "一部店舗の希望到着時刻・波次・車両割当を人手で調整すれば、目標に近づける可能性があります"
      : "通过人工微调部分门店的期望到达、波次或车辆归属，评分还有继续上探的空间");
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return lang() === "ja"
    ? `現在の最良案は ${algoLabel(bestResult.key)} の ${bestResult.metrics.score.toFixed(1)} で、目標 ${target.toFixed(1)} まであと ${gap.toFixed(1)} です。業務前提を次の方向で改善すれば到達しやすくなります：${assumptions.join("；")}。`
    : `当前最佳方案是 ${algoLabel(bestResult.key)}，评分 ${bestResult.metrics.score.toFixed(1)}，距离目标 ${target.toFixed(1)} 还差 ${gap.toFixed(1)}。如果业务前提允许，朝这些方向优化更容易实现目标：${assumptions.join("；")}。`;
}

function renderSingleWaveInfo() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const info = document.getElementById("singleWaveInfo");
  if (!info) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stores = enrichStores(state.stores);
  if (!stores.length) {
    info.textContent = lang() === "ja" ? "現在、店舗データがありません。" : "当前没有门店数据。";
    return;
  }
  info.textContent = lang() === "ja"
    ? "単独波次の自動分流は停止中です。すべて業務波次（W1/W2/W3/W4）で求解します。"
    : "单波次自动分流已停用。当前所有门店都按业务波次（W1/W2/W3/W4）求解。";
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function applyLanguage() {
  document.documentElement.lang = lang() === "ja" ? "ja" : "zh-CN";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.title = lang() === "ja" ? "鯨略使オプティマイザー" : "鲸略使调度求解器";
  const tabTitles = lang() === "ja"
    ? { basic: "基礎資料", region: "運行区定義", strategy: "調度戦略", solve: "求解と比較", result: "調度結果" }
    : { basic: "基础资料", region: "运行区定义", strategy: "调度策略", solve: "求解与对比", result: "调度结果" };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tabBasicBtn = document.getElementById("mainTabBasicBtn");
  const tabRegionBtn = document.getElementById("mainTabRegionBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tabStrategyBtn = document.getElementById("mainTabStrategyBtn");
  const tabSolveBtn = document.getElementById("mainTabSolveBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tabResultBtn = document.getElementById("mainTabResultBtn");
  if (tabBasicBtn) tabBasicBtn.textContent = tabTitles.basic;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tabRegionBtn) tabRegionBtn.textContent = tabTitles.region;
  if (tabStrategyBtn) tabStrategyBtn.textContent = tabTitles.strategy;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (tabSolveBtn) tabSolveBtn.textContent = tabTitles.solve;
  if (tabResultBtn) tabResultBtn.textContent = tabTitles.result;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const hero = document.querySelector(".hero");
  if (hero) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const eyebrow = hero.querySelector(".eyebrow");
    if (eyebrow) eyebrow.textContent = "Cetacean Optimizer";
    hero.querySelector("h1").textContent = lang() === "ja" ? "鯨略使オプティマイザー" : "鲸略使调度求解器";
    hero.querySelector(".lead").textContent = lang() === "ja" ? "現在の版は実用性を優先し、多車両・多波次・多店舗単日複数回配送、手動車両調整、単独波次店舗、配送可視化に対応しています。" : "当前版本优先追求可用，支持多车、多波次、多门店单日多次配送调度、手工调车、单波次店铺，以及调度过程可视化。";
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("openShowcaseBtn").textContent = L("showcase");
  document.getElementById("loadSampleBtn").textContent = L("reload");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("savePlanBtn").textContent = L("save");
  const exportResultBtn = document.getElementById("exportResultBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (exportResultBtn) exportResultBtn.textContent = L("exportResult");
  document.getElementById("toggleStorePanelBtn").textContent = document.getElementById("storePanelBody").classList.contains("collapsed") ? L("unfoldStore") : L("foldStore");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("toggleVehiclePanelBtn").textContent = document.getElementById("vehiclePanelBody").classList.contains("collapsed") ? L("unfoldVehicle") : L("foldVehicle");
  document.getElementById("addStoreBtn").textContent = L("addStore");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("addVehicleBtn").textContent = L("addVehicle");
  const locateText = lang() === "ja" ? "検索" : "定位";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const saveDataText = lang() === "ja" ? "資料保存" : "保存资料";
  const storeImportText = lang() === "ja" ? "店舗導入" : "导入门店";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleImportText = lang() === "ja" ? "車両導入" : "导入车辆";
  const batchReserveText = lang() === "ja" ? "一括操作（予約）" : "批量操作（预留）";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveImportText = lang() === "ja" ? "波次導入" : "导入波次";
  const storeFileTrigger = document.getElementById("storeFileTrigger");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleFileTrigger = document.getElementById("vehicleFileTrigger");
  const waveImportBtn = document.getElementById("waveImportBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeBatchBtn = document.getElementById("storeBatchBtn");
  const vehicleBatchBtn = document.getElementById("vehicleBatchBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveBatchBtn = document.getElementById("waveBatchBtn");
  const storeLocateBtn = document.getElementById("storeLocateBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleLocateBtn = document.getElementById("vehicleLocateBtn");
  const waveLocateBtn = document.getElementById("waveLocateBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const saveStoreDataBtn = document.getElementById("saveStoreDataBtn");
  const saveVehicleDataBtn = document.getElementById("saveVehicleDataBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const saveWaveDataBtn = document.getElementById("saveWaveDataBtn");
  const dataArchiveQueryBtn = document.getElementById("dataArchiveQueryBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (storeFileTrigger) storeFileTrigger.textContent = storeImportText;
  if (vehicleFileTrigger) vehicleFileTrigger.textContent = vehicleImportText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveImportBtn) waveImportBtn.textContent = waveImportText;
  if (storeBatchBtn) storeBatchBtn.textContent = batchReserveText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (vehicleBatchBtn) vehicleBatchBtn.textContent = batchReserveText;
  if (waveBatchBtn) waveBatchBtn.textContent = batchReserveText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (storeLocateBtn) storeLocateBtn.textContent = locateText;
  if (vehicleLocateBtn) vehicleLocateBtn.textContent = locateText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveLocateBtn) waveLocateBtn.textContent = locateText;
  if (saveStoreDataBtn) saveStoreDataBtn.textContent = saveDataText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (saveVehicleDataBtn) saveVehicleDataBtn.textContent = saveDataText;
  if (saveWaveDataBtn) saveWaveDataBtn.textContent = saveDataText;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (dataArchiveQueryBtn) dataArchiveQueryBtn.textContent = lang() === "ja" ? "照会" : "查询";
  const calcWaveLoadBtn = document.getElementById("calcWaveLoadBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (calcWaveLoadBtn) calcWaveLoadBtn.textContent = lang() === "ja" ? "貨量換算" : "货量折算";
  const saveDualStoreLoadsBtn = document.getElementById("saveDualStoreLoadsBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (saveDualStoreLoadsBtn) saveDualStoreLoadsBtn.textContent = lang() === "ja" ? "保存双表貨量" : "保存双表货量";
  const closeLoadConvertBtn = document.getElementById("closeLoadConvertModalBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (closeLoadConvertBtn) closeLoadConvertBtn.textContent = lang() === "ja" ? "適用して閉じる" : "应用并关闭";
  const loadConvertTitle = document.getElementById("loadConvertModalTitle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (loadConvertTitle) loadConvertTitle.textContent = lang() === "ja" ? "貨量換算結果" : "货量折算结果";
  renderWaveSolverPanel();
  const storeFile = document.getElementById("storeFile");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const vehicleFile = document.getElementById("vehicleFile");
  setImportFileTag("store", storeFile?.files?.[0]?.name || "");
  setImportFileTag("vehicle", vehicleFile?.files?.[0]?.name || "");
  document.getElementById("addWaveBtn").textContent = L("addWave");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("dispatchStartLabel").textContent = L("dispatchStart");
  document.getElementById("maxRouteKmLabel").textContent = L("maxKm");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("minLoadRateLabel").textContent = L("minLoad");
  document.getElementById("targetScoreLabel").textContent = L("targetScore");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const targetScoreInput = document.getElementById("targetScoreInput");
  if (targetScoreInput) {
    targetScoreInput.min = "0";
    targetScoreInput.max = "100";
    targetScoreInput.step = "0.1";
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("ignoreWavesText").textContent = L("ignoreWaves");
  const concentrateLateText = document.getElementById("concentrateLateText");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (concentrateLateText) concentrateLateText.textContent = L("concentrateLate");
  document.getElementById("autoWaveBtn").textContent = L("autoWave");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("singleWaveDistanceLabel").textContent = L("singleWaveDistance");
  document.getElementById("singleWaveStartLabel").textContent = L("singleWaveStart");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("singleWaveEndLabel").textContent = L("singleWaveEnd");
  document.getElementById("singleWaveModeLabel").textContent = L("singleWaveMode");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveStrategyLabel = document.getElementById("solveStrategyLabel");
  if (solveStrategyLabel) solveStrategyLabel.textContent = L("solveStrategy");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const optimizeGoalLabel = document.getElementById("optimizeGoalLabel");
  if (optimizeGoalLabel) optimizeGoalLabel.textContent = L("optimizeGoal");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveRegionSchemeLabel = document.getElementById("solveRegionSchemeLabel");
  if (solveRegionSchemeLabel) solveRegionSchemeLabel.textContent = lang() === "ja" ? "分区方案番号" : "分区方案号";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("generateBtn").textContent = L("generate");
  document.getElementById("closeProcessModalBtn").textContent = L("close");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("closeShowcaseModalBtn").textContent = L("close");
  document.getElementById("processModalTitle").textContent = L("processTitle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("showcaseModalTitle").textContent = L("showcaseTitle");
  const solveDiagnoseTitle = document.getElementById("solveDiagnoseModalTitle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (solveDiagnoseTitle) solveDiagnoseTitle.textContent = lang() === "ja" ? "求解前診断" : "求解前诊断";
  const closeSolveDiagnoseBtn = document.getElementById("closeSolveDiagnoseModalBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (closeSolveDiagnoseBtn) closeSolveDiagnoseBtn.textContent = lang() === "ja" ? "閉じる" : "关闭";
  const cancelSolveDiagnoseBtn = document.getElementById("cancelSolveDiagnoseBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (cancelSolveDiagnoseBtn) cancelSolveDiagnoseBtn.textContent = lang() === "ja" ? "求解をやめる" : "取消求解";
  const continueSolveDiagnoseBtn = document.getElementById("continueSolveDiagnoseBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (continueSolveDiagnoseBtn) continueSolveDiagnoseBtn.textContent = lang() === "ja" ? "続けて求解" : "继续求解";
  document.getElementById("languageSelect").value = lang();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const setPanelHeadText = (panelId, titleText, descText = null) => {
    const panel = document.getElementById(panelId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!panel) return;
    const titleEl = panel.querySelector(".panel-head h2");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (titleEl && titleText) titleEl.textContent = titleText;
    if (descText !== null) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const descEl = panel.querySelector(".panel-head p");
      if (descEl) descEl.textContent = descText;
    }
  };
  setPanelHeadText("storeInfoPanel", L("storeInfo"), L("storeDesc"));
  setPanelHeadText("vehicleInfoPanel", L("vehicleInfo"), L("vehicleDesc"));
  setPanelHeadText("waveConfigPanel", L("waveConfig"), L("waveDesc"));
  setPanelHeadText("wmsSyncPanel", lang() === "ja" ? "WMS業務同期" : "WMS业务抓取", lang() === "ja" ? "遠端WMS五表を読み取り専用で同期。店舗/車両は样本・実业务を切替でき、貨量はCargoQTYを強制適用します。" : "一键只读抓取远端WMS五表。店铺和车辆可选样本/真实业务，货量统一使用抓取的CargoQTY。");
  setPanelHeadText("dataArchivePanel", lang() === "ja" ? "档案照会" : "档案查询", lang() === "ja" ? "基础资料档案を照会し、緯度経度を含む完全データを確認できます。" : "查询基础资料档案，并查看完整经纬度门店数据。");
  setPanelHeadText("strategyPanel", L("algoRun"), L("algoDesc"));
  setPanelHeadText("solveComparePanel", tabTitles.solve, lang() === "ja" ? "求解実行・進捗・アルゴリズム比較をこのセクションで扱います。" : "在本区进行求解执行、进度跟踪与算法对比。");
  setPanelHeadText("dispatchResultPanel", L("result"));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const savePanel = document.getElementById("savedPlans")?.closest(".panel");
  if (savePanel) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const saveTitle = savePanel.querySelector(".panel-head h2");
    const saveDesc = savePanel.querySelector(".panel-head p");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (saveTitle) saveTitle.textContent = L("saved");
    if (saveDesc) saveDesc.textContent = L("savedDesc");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const singleWaveModeSelect = document.getElementById("singleWaveEndModeInput");
  if (singleWaveModeSelect?.options[0]) singleWaveModeSelect.options[0].text = L("returnEnd");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (singleWaveModeSelect?.options[1]) singleWaveModeSelect.options[1].text = L("serviceEnd");
  const algoLabels = document.querySelectorAll(".algo-box label");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (algoLabels[0]) algoLabels[0].lastChild.textContent = ` ${algoLabel("vrptw")}`;
  if (algoLabels[1]) algoLabels[1].lastChild.textContent = ` ${algoLabel("hybrid")}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (algoLabels[2]) algoLabels[2].lastChild.textContent = ` ${algoLabel("ga")}`;
  if (algoLabels[3]) algoLabels[3].lastChild.textContent = ` ${algoLabel("tabu")}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (algoLabels[4]) algoLabels[4].lastChild.textContent = ` ${algoLabel("lns")}`;
  if (algoLabels[5]) algoLabels[5].lastChild.textContent = ` ${algoLabel("savings")}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (algoLabels[6]) algoLabels[6].lastChild.textContent = ` ${algoLabel("sa")}`;
  if (algoLabels[7]) algoLabels[7].lastChild.textContent = ` ${algoLabel("aco")}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (algoLabels[8]) algoLabels[8].lastChild.textContent = ` ${algoLabel("pso")}`;
  if (algoLabels[9]) algoLabels[9].lastChild.textContent = ` ${algoLabel("vehicle")}`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const solveStrategySelect = document.getElementById("solveStrategySelect");
  if (solveStrategySelect?.options[0]) solveStrategySelect.options[0].text = L("strategyManual");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (solveStrategySelect?.options[1]) solveStrategySelect.options[1].text = L("strategyQuick");
  if (solveStrategySelect?.options[2]) solveStrategySelect.options[2].text = L("strategyDeep");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (solveStrategySelect?.options[3]) solveStrategySelect.options[3].text = L("strategyGlobal");
  if (solveStrategySelect?.options[4]) solveStrategySelect.options[4].text = L("strategyRelay");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (solveStrategySelect?.options[5]) solveStrategySelect.options[5].text = L("strategyFree");
  if (solveStrategySelect?.options[6]) solveStrategySelect.options[6].text = L("strategyCompare");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (solveStrategySelect) solveStrategySelect.value = state.settings.solveStrategy || "manual";
  const optimizeGoalSelect = document.getElementById("optimizeGoalSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (optimizeGoalSelect?.options[0]) optimizeGoalSelect.options[0].text = L("goalBalanced");
  if (optimizeGoalSelect?.options[1]) optimizeGoalSelect.options[1].text = L("goalOnTime");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (optimizeGoalSelect?.options[2]) optimizeGoalSelect.options[2].text = L("goalDistance");
  if (optimizeGoalSelect?.options[3]) optimizeGoalSelect.options[3].text = L("goalVehicles");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (optimizeGoalSelect?.options[4]) optimizeGoalSelect.options[4].text = L("goalLoad");
  if (optimizeGoalSelect) optimizeGoalSelect.value = state.settings.optimizeGoal || "balanced";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const quickSolveBtn = document.getElementById("quickSolveBtn");
  if (quickSolveBtn) quickSolveBtn.textContent = L("quickSolve");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const deepOptimizeBtn = document.getElementById("deepOptimizeBtn");
  if (deepOptimizeBtn) deepOptimizeBtn.textContent = L("deepOptimize");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const globalSearchBtn = document.getElementById("globalSearchBtn");
  if (globalSearchBtn) globalSearchBtn.textContent = L("globalSearch");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wmsFetchBtn = document.getElementById("wmsFetchBtn");
  if (wmsFetchBtn) wmsFetchBtn.textContent = lang() === "ja" ? "WMS同期" : "一键抓取WMS";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const runRegionSchemeCreateBtn = document.getElementById("runRegionSchemeCreateBtn");
  if (runRegionSchemeCreateBtn) runRegionSchemeCreateBtn.textContent = lang() === "ja" ? "方案追加" : "新增方案";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const runRegionSchemeUpdateBtn = document.getElementById("runRegionSchemeUpdateBtn");
  if (runRegionSchemeUpdateBtn) runRegionSchemeUpdateBtn.textContent = lang() === "ja" ? "方案更新" : "更新方案";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const runRegionSchemeDeleteBtn = document.getElementById("runRegionSchemeDeleteBtn");
  if (runRegionSchemeDeleteBtn) runRegionSchemeDeleteBtn.textContent = lang() === "ja" ? "方案削除" : "删除方案";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeSourceSelect = document.getElementById("storeSourceSelect");
  if (storeSourceSelect?.options?.[0]) storeSourceSelect.options[0].text = lang() === "ja" ? "样本" : "样本";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (storeSourceSelect?.options?.[1]) storeSourceSelect.options[1].text = lang() === "ja" ? "実业务" : "真实业务";
  const vehicleSourceSelect = document.getElementById("vehicleSourceSelect");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (vehicleSourceSelect?.options?.[0]) vehicleSourceSelect.options[0].text = lang() === "ja" ? "样本" : "样本";
  if (vehicleSourceSelect?.options?.[1]) vehicleSourceSelect.options[1].text = lang() === "ja" ? "実业务" : "真实业务";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const relaySolveBtn = document.getElementById("relaySolveBtn");
  if (relaySolveBtn) relaySolveBtn.textContent = L("relaySolve");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const freeSolveBtn = document.getElementById("freeSolveBtn");
  if (freeSolveBtn) freeSolveBtn.textContent = L("freeSolve");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const multiCompareBtn = document.getElementById("multiCompareBtn");
  if (multiCompareBtn) multiCompareBtn.textContent = L("multiCompare");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const strategyHint = document.getElementById("strategyHint");
  if (strategyHint) strategyHint.textContent = buildStrategyHint();
  renderStrategyPreviewControls();
  syncAlgorithmControls();
  renderAlgorithmPool();
  renderGaBackendStatus();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!state.lastResults.length && state.stores.length) {
    document.getElementById("validationBox").textContent = lang() === "ja"
      ? `固定店舗 ${state.stores.length} 件を読み込み、通常波次 ${state.waves.length} 件を生成しました。`
      : `已加载固定门店 ${state.stores.length} 家，并自动分成 ${state.waves.length} 个普通波次。`;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function setupHeroQuickActions() {
  const hero = document.querySelector(".hero");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const loadBtn = document.getElementById("loadSampleBtn");
  const saveBtn = document.getElementById("savePlanBtn");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!hero || !loadBtn || !saveBtn) return;
  let strip = document.querySelector(".quick-actions");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!strip) {
    strip = document.createElement("section");
    strip.className = "quick-actions";
    hero.insertAdjacentElement("afterend", strip);
  }
  strip.append(loadBtn, saveBtn);
}

function ensureConcentrateLateControl() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (document.getElementById("concentrateLateInput")) return;
  const ignoreWrap = document.getElementById("ignoreWavesLabel");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!ignoreWrap?.parentElement) return;
  const label = document.createElement("label");
  label.id = "concentrateLateLabel";
  label.innerHTML = `<input id="concentrateLateInput" type="checkbox"> <span id="concentrateLateText"></span>`;
  ignoreWrap.insertAdjacentElement("afterend", label);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("concentrateLateInput").checked = Boolean(state.settings.concentrateLate);
  const text = document.getElementById("concentrateLateText");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (text) text.textContent = L("concentrateLate");
}

function renderTripLegs(trip, scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const legs = [];
  let from = DC.id;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const stop of trip.stops) {
    legs.push(`${from}→${stop.storeId} ${stop.distance.toFixed(1)}km`);
    from = stop.storeId;
  }
  if (trip.stops.length) legs.push(`${from}→${DC.id} ${scenario.dist[from][DC.id].toFixed(1)}km`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return legs.join(" / ");
}

function buildProcessSteps(plan, trip, scenario) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const storeMap = scenario.storeMap;
  const steps = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let cumulativeLoad = 0;
  let cumulativeDistance = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (let index = 0; index < trip.stops.length; index += 1) {
    const stop = trip.stops[index];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const store = storeMap.get(stop.storeId);
    cumulativeLoad += getStoreSolveLoadForWave(store, { waveId: plan.waveId });
    cumulativeDistance += stop.distance;
    const returnDistance = scenario.dist[stop.storeId][DC.id];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const estimatedBack = stop.leave + getTravelMinutes(scenario, stop.storeId, DC.id, plan.vehicle.speed);
    steps.push({
      index: index + 1,
      storeId: stop.storeId,
      storeName: stop.storeName,
      loadBoxes: cumulativeLoad,
      loadRate: getVehicleSolveCapacity(plan.vehicle) ? cumulativeLoad / getVehicleSolveCapacity(plan.vehicle) : 0,
      legDistance: stop.distance,
      cumulativeDistance,
      arrival: stop.arrival,
      leave: stop.leave,
      estimatedBack,
      onTime: stop.onTime,
      route: [DC.id, ...trip.stops.slice(0, index + 1).map((item) => item.storeId)].join(" → "),
    });
  }
  return steps;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function traceAlgorithmKeysForResult(result) {
  const key = String(result?.key || "").trim();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!key) return new Set();
  if (key === "relay") return new Set(["relay"]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return new Set([key]);
}

function buildAlgorithmNarrativeHint(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const key = String(result?.key || "");
  const hintsZh = {
    hybrid: "混合启发式会先做扰动探索，再做局部精修，日志重点看“动作”和“刷新最优”轮次。",
    tabu: "禁忌搜索会记录禁忌动作与最优刷新，日志重点看“避免回头路”后的改进轨迹。",
    lns: "大邻域搜索会反复 destroy/repair，日志重点看大扰动后是否出现显著降本。",
    sa: "模拟退火会出现“接受差解”的阶段，日志重点看温度下降后如何逐步收敛。",
    aco: "蚁群算法会按轮次强化优路径，日志重点看轮次内最优与全局最优变化。",
    pso: "粒子群会记录粒子刷新最优与重启，日志重点看群体最优何时被突破。",
    ga: "遗传算法会记录世代迭代、改进幅度和提前收敛，日志重点看代际最优变化。",
    savings: "节约法是构造型算法，日志重点看节约值合并与车辆分配。",
    vrptw: "VRPTW 初排是可行性优先，日志重点看门店插入比较与约束可行性。",
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const hintsJa = {
    hybrid: "ハイブリッドは広域探索と局所改善を組み合わせます。動作種別と最良更新回を重点確認します。",
    tabu: "タブー探索は戻り手を禁じます。最良更新の軌跡で改善の質を確認します。",
    lns: "LNS は destroy/repair を繰り返します。大きな再構成後の改善幅を重点確認します。",
    sa: "SA は一時的に劣解受理が発生します。温度低下後の収束過程を確認します。",
    aco: "ACO は反復で良経路を強化します。各反復の最良値推移を確認します。",
    pso: "PSO は粒子群で最良解を更新します。群最良が更新される局面を確認します。",
    ga: "GA は世代ごとに進化します。世代最良と早期収束の発生点を確認します。",
    savings: "節約法は構成型です。節約値による統合と配車結果を確認します。",
    vrptw: "VRPTW 初期配車は可行性優先です。挿入比較と制約充足を確認します。",
  };
  return lang() === "ja" ? (hintsJa[key] || "") : (hintsZh[key] || "");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildTraceNarrative(result, plan, trip, wave) {
  const breakdown = computePlanCostBreakdown(plan, result.scenario, wave);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const algorithmKey = String(result?.key || "").trim();
  const supportsStoreInsertionTrace = new Set(["vrptw", "savings", "relay", "focus"]).has(algorithmKey);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const keys = traceAlgorithmKeysForResult(result);
  const hasKeyFilter = keys.size > 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveLogs = (result.traceLog || []).filter((entry) => {
    if (entry.waveId !== wave.waveId || entry.scope !== "wave") return false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!hasKeyFilter) return true;
    return keys.has(String(entry.algorithmKey || ""));
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const traces = (result.traceLog || []).filter((entry) => {
    if (entry.waveId !== wave.waveId) return false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!(entry.chosenPlate === plan.vehicle.plateNo && entry.chosenTripNo === trip.tripNo)) return false;
    if (!hasKeyFilter) return true;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return keys.has(String(entry.algorithmKey || ""));
  });
  const lines = [
    lang() === "ja" ? "> 真の探索ログ" : "> 真实算法搜索日志",
    lang() === "ja"
      ? "> 上段はアルゴリズムが実際に受理した探索イベント、下段はこの便に入る店舗の割当比較です。"
      : "> 上面先展示算法真正执行并接受的搜索事件，下面再展示这条线路内门店的插入比较。",
    lang() === "ja"
      ? "> コスト式: 遅着ペナルティ + 波次超過ペナルティ + 車両繁忙ペナルティ + 追加便ペナルティ + 距離コスト - 積載ボーナス。値が低いほど良いです。"
      : "> 成本公式: 晚到罚分 + 超波次罚分 + 车辆繁忙罚分 + 额外趟次罚分 + 距离成本 - 装载奖励。数值越低越好。",
    lang() === "ja"
      ? `> 現在ルートのコスト内訳: 遅着ペナルティ ${breakdown.latenessPenalty.toFixed(1)} / 波次超過ペナルティ ${breakdown.waveLatePenalty.toFixed(1)} / 距離コスト ${breakdown.distanceCost.toFixed(1)} / 積載ボーナス ${breakdown.loadBonus.toFixed(1)} / 繁忙ペナルティ ${breakdown.vehicleBusyPenalty.toFixed(1)} / 追加便ペナルティ ${breakdown.extraTripPenalty.toFixed(1)} / 総コスト ${breakdown.totalCost.toFixed(1)}`
      : `> 当前线路成本拆分: 晚到罚分 ${breakdown.latenessPenalty.toFixed(1)} / 超波次罚分 ${breakdown.waveLatePenalty.toFixed(1)} / 距离成本 ${breakdown.distanceCost.toFixed(1)} / 装载奖励 ${breakdown.loadBonus.toFixed(1)} / 繁忙罚分 ${breakdown.vehicleBusyPenalty.toFixed(1)} / 额外趟次罚分 ${breakdown.extraTripPenalty.toFixed(1)} / 总成本 ${breakdown.totalCost.toFixed(1)}`,
    "",
  ];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const fallbackWaveLogs = (result.traceLog || []).filter((entry) => {
    if (entry.scope !== "wave") return false;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!hasKeyFilter) return true;
    return keys.has(String(entry.algorithmKey || ""));
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveLogs.length) {
    lines.push(lang() === "ja" ? "[波次レベル探索（当該波次）]" : "[波次级搜索（当前波次）]");
    waveLogs.slice(0, 28).forEach((entry) => {
      const stage = String(entry.stage || "").trim();
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const stageTag = stage ? `[${stage}] ` : "";
      const body = (lang() === "ja" ? entry.textJa : entry.textZh) || entry.textZh || entry.textJa || "";
      lines.push(`${stageTag}${body}`);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const cb = entry?.costBreakdown || null;
      if (cb && typeof cb === "object") {
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const line = lang() === "ja"
          ? `  cost = 到店超偏差罚分 ${Number(cb.arrivalViolationPenalty || 0).toFixed(1)} + 晚到罚分 ${Number(cb.latenessPenalty || 0).toFixed(1)} + 超波次罚分 ${Number(cb.waveLatePenalty || 0).toFixed(1)} + 车辆续跑罚分 ${Number(cb.vehicleBusyPenalty || 0).toFixed(1)} + 额外趟次罚分 ${Number(cb.extraTripPenalty || 0).toFixed(1)} + 超时路线罚分 ${Number(cb.lateRoutePenalty || 0).toFixed(1)} + 距离成本 ${Number(cb.distanceCost || 0).toFixed(1)} - 装载抵扣 ${Number(cb.loadBonus || 0).toFixed(1)}`
          : `  cost = 到店超偏差罚分 ${Number(cb.arrivalViolationPenalty || 0).toFixed(1)} + 晚到罚分 ${Number(cb.latenessPenalty || 0).toFixed(1)} + 超波次罚分 ${Number(cb.waveLatePenalty || 0).toFixed(1)} + 车辆续跑罚分 ${Number(cb.vehicleBusyPenalty || 0).toFixed(1)} + 额外趟次罚分 ${Number(cb.extraTripPenalty || 0).toFixed(1)} + 超时路线罚分 ${Number(cb.lateRoutePenalty || 0).toFixed(1)} + 距离成本 ${Number(cb.distanceCost || 0).toFixed(1)} - 装载抵扣 ${Number(cb.loadBonus || 0).toFixed(1)}`;
        lines.push(line);
      }
    });
    lines.push("");
  } else if (fallbackWaveLogs.length) {
    lines.push(lang() === "ja" ? "[波次レベル探索（アルゴリズム全体）]" : "[波次级搜索（该算法全局）]");
    lines.push(lang() === "ja"
      ? "> この便に対応する波次ログはありません。代わりに本ラウンドで当該アルゴリズムが実行した探索ログを表示します。"
      : "> 当前这趟没有命中该算法的本波次日志，下面展示该算法本轮真实执行的全局搜索日志。");
    fallbackWaveLogs.slice(0, 28).forEach((entry) => {
      const stage = String(entry.stage || "").trim();
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const stageTag = stage ? `[${stage}] ` : "";
      const body = (lang() === "ja" ? entry.textJa : entry.textZh) || entry.textZh || entry.textJa || "";
      lines.push(`${stageTag}${body}`);
    });
    lines.push("");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!supportsStoreInsertionTrace) {
    const iterCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("iteration")).length;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const bestCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("best")).length;
    const startCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("start")).length;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const finishCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("finish")).length;
    const costLines = waveLogs.filter((entry) => entry?.costBreakdown && typeof entry.costBreakdown === "object").length;
    lines.push(lang() === "ja" ? "[探索日志统计]" : "[搜索日志统计]");
    lines.push(lang() === "ja"
      ? `${algoLabel(algorithmKey)} の本輪ログ: 開始 ${startCount} 件 / 反復 ${iterCount} 件 / 最良更新 ${bestCount} 件 / 終了 ${finishCount} 件 / コスト内訳付き ${costLines} 件。`
      : `${algoLabel(algorithmKey)} 本轮日志统计：开始 ${startCount} 条 / 迭代 ${iterCount} 条 / 刷新最优 ${bestCount} 条 / 结束 ${finishCount} 条 / 含成本拆分 ${costLines} 条。`);
    lines.push("");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return lines;
  }
  if (!traces.length) {
    lines.push(lang() === "ja" ? "[便内割当比較]" : "[趟内分配比较]");
    lines.push(lang() === "ja"
      ? "この便に直結する挿入比較ログは未記録です（上段に実際の探索ログを表示）。"
      : "该算法本轮未记录“这趟直连的插入比较日志”（上面已展示真实搜索日志）。");
    lines.push("");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return lines;
  }
  lines.push(lang() === "ja" ? "[便内割当比較]" : "[趟内分配比较]");
  traces.forEach((entry, index) => {
    lines.push(lang() === "ja"
      ? `[計算 ${index + 1}] 店舗 ${entry.storeId} ${entry.storeName}`
      : `[计算 ${index + 1}] 门店 ${entry.storeId} ${entry.storeName}`);
    lines.push(lang() === "ja"
      ? `  目的: どの車両のどの便・どの位置に入れると総コストが最も低くなるかを比較`
      : `  目标: 比较“插入哪辆车、哪一趟、哪个位置”时整体验证后的综合成本`);
    entry.vehicleEvaluations.slice(0, 4).forEach((item, vehicleRank) => {
      lines.push(lang() === "ja"
        ? `  候補車 ${vehicleRank + 1}: ${item.plateNo} | 挿入案 ${item.optionCount} 個 | 最良コスト ${item.bestCost.toFixed(1)} | 採用便 ${item.chosenTripNo}`
        : `  候选车 ${vehicleRank + 1}: ${item.plateNo} | 插入方案 ${item.optionCount} 个 | 最优成本 ${item.bestCost.toFixed(1)} | 落在第 ${item.chosenTripNo} 趟`);
      item.candidates.slice(0, 2).forEach((candidate, candidateIndex) => {
        lines.push(lang() === "ja"
          ? `    - 案 ${candidateIndex + 1}: ${candidate.mode === "new-trip" ? "新便" : `第${candidate.tripIndex + 1}便・位置${candidate.insertAt + 1}`} | ルート ${candidate.routePreview} | 距離 ${candidate.totalDistance.toFixed(1)} km | 波次超過 ${candidate.waveLateMinutes.toFixed(0)} 分`
          : `    - 方案 ${candidateIndex + 1}: ${candidate.mode === "new-trip" ? "新开一趟" : `第${candidate.tripIndex + 1}趟第${candidate.insertAt + 1}个位置`} | 路线 ${candidate.routePreview} | 里程 ${candidate.totalDistance.toFixed(1)} km | 超波次 ${candidate.waveLateMinutes.toFixed(0)} 分`);
      });
    });
    lines.push(lang() === "ja"
      ? `  => 最終採用: ${entry.chosenPlate} / 第${entry.chosenTripNo}便 / コスト ${entry.bestCost.toFixed(1)}`
      : `  => 最终采用: ${entry.chosenPlate} / 第${entry.chosenTripNo}趟 / 成本 ${entry.bestCost.toFixed(1)}`);
    lines.push(lang() === "ja"
      ? "  理由: その時点で制約を守りつつ、最も低コストだったため"
      : "  原因: 在满足当前约束的候选方案里，这个方案综合成本最低");
    lines.push("");
  });
  return lines;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildProcessNarrative(result, plan, trip, scenario, wave, isMultiCompare = false) {
  const steps = buildProcessSteps(plan, trip, scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lines = [
    lang() === "ja" ? `> ${wave.waveId} / ${plan.vehicle.plateNo} / 第${trip.tripNo}便 ${L("playback")}` : `> ${wave.waveId} / ${plan.vehicle.plateNo} / 第${trip.tripNo}${L("tripSuffix")} ${L("playback")}`,
    `> ${L("route")}: ${[DC.id, ...trip.route, DC.id].join(" -> ")}`,
    `> ${L("tripRoundKm")}: ${trip.totalDistance.toFixed(1)} km`,
    `> ${L("tripLoadRate")}: ${formatRate(trip.loadRate)}`,
    "",
  ];
  lines.push(...buildTraceNarrative(result, plan, trip, wave));
  if (isMultiCompare) lines.push(lang() === "ja" ? "> 以下は当該アルゴリズムの最終ルート実行ログです。" : "> 以下是该算法最终线路执行日志。");
  lines.push(lang() === "ja" ? "> 以下は最終確定ルートの実行回放です。" : "> 以下是最终确定线路的执行回放。");
  lines.push("");
  steps.forEach((step) => {
    lines.push(lang() === "ja" ? `[ステップ ${step.index}] ${step.storeId} ${step.storeName} を追加` : `[步骤 ${step.index}] 加入 ${step.storeId} ${step.storeName}`);
    lines.push(`  ${L("route")}: ${step.route}`);
    lines.push(lang() === "ja"
      ? `  ${L("leg")}距離: +${step.legDistance.toFixed(1)} km | 累計距離: ${step.cumulativeDistance.toFixed(1)} km`
      : `  ${L("leg")}距离: +${step.legDistance.toFixed(1)} km | 累计距离: ${step.cumulativeDistance.toFixed(1)} km`);
    lines.push(`  ${L("totalLoad")}: ${step.loadBoxes}/${getVehicleSolveCapacity(plan.vehicle)} | ${L("avgLoad")}: ${formatRate(step.loadRate)}`);
    lines.push(`  ${L("arrive")}: ${formatTime(step.arrival)} | ${L("unloadMinutes")}: ${formatMinutesValue(step.storeId ? scenario.storeMap.get(step.storeId)?.actualServiceMinutes || scenario.storeMap.get(step.storeId)?.serviceMinutes || 0 : 0)} ${L("minutes")} | ${L("leave")}: ${formatTime(step.leave)}`);
    lines.push(`  ${L("desired")}: ${trip.stops[step.index - 1]?.desiredArrival || "--:--"} | ${lang() === "ja" ? "帰庫見込み" : "预计回仓"}: ${formatTime(step.estimatedBack)}`);
    lines.push(`  ${lang() === "ja" ? "状態" : "状态"}: ${step.onTime ? L("notLate") : L("late")}`);
    lines.push("");
  });
  lines.push(lang() === "ja" ? "> リプレイ終了" : "> 回放结束");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return lines.join("\n");
}

function buildPenaltyRuleCard() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `
    <div class="penalty-rule-card">
      <div class="penalty-rule-title">${lang() === "ja" ? "ペナルティルール" : "罚分规则说明"}</div>
      <div class="penalty-rule-list">
        <span class="chip">晚到罚分：每超 1 分钟 +60</span>
        <span class="chip">超允许偏差：每超 1 分钟 +20000</span>
        <span class="chip">超波次罚分：每超 1 分钟 +80</span>
        <span class="chip">额外趟次：每多 1 趟 +180</span>
        <span class="chip">距离成本：每 1 km 按 0.45 折算</span>
        <span class="chip">装载奖励：每多 1 箱按 0.08 抵扣</span>
      </div>
      <p class="penalty-rule-note">${lang() === "ja" ? "いま超過がなければ該当ペナルティは 0 と表示されます。超過が発生すると、分単位または便単位で加算され、固定で 1 回だけではありません。" : "当前没有超出时，对应罚分会显示为 0；一旦超出，会按分钟或按趟次继续累加，不是固定只罚一次。"}</p>
    </div>
  `;
}

function buildShowcaseNarrative() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const currentPreset = strategyPreset(state.settings.solveStrategy || "quick", state.settings.optimizeGoal || "balanced");
  const selectedAlgorithms = (currentPreset.algorithms || []).map((key) => algoLabel(key));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lang() === "ja") {
    return [
      "都市配送AI調度システム",
      "",
      "中核ポジション",
      "これは単なる配車ツールではありません。アルゴリズム意思決定チェーン、説明可能な調度、全工程の履歴保持を備えた AI 調度オペレーティングシステムです。",
      "",
      "市販の大半の調度システムは、入力データ → 単一アルゴリズム実行 → 表出力の 3 段階で終わります。配車担当は結果を受け取っても、その案がなぜ出たのか、なぜ別案ではないのかを知ることができません。",
      "",
      "このシステムは違います。9 つのアルゴリズムが協調して意思決定する過程をそのまま可視化し、配車担当が見て、理解し、介入し、振り返れるようにします。経営層は追跡し、比較し、判断できます。",
      "",
      "適用業種：チェーン小売、飲食店舗、医薬配送、コミュニティ EC、コールドチェーン生鮮、セントラルキッチン配送、ガソリンスタンド補充、共同配送 3PL など。「同じ車群で、1 日複数回、各拠点が固有の時間窓と補充リズムを持つ」業務ならそのまま展開できます。",
      "",
      "鲸略使調度ソルバー - 製品概要",
      "9 大アルゴリズムエンジン、真の工業級最適化。",
      "鲸略使調度ソルバーは VRPTW（時間窓付き車両経路問題）向けの企業級インテリジェント調度システムであり、コアは 9 種の本格メタヒューリスティックです。単なるルール寄せ集めではありません。",
      "",
      "コアアルゴリズムマトリクス",
      "群知能系：ACO（蟻コロニー最適化）は情報素揮発とエリート戦略を実装。PSO（粒子群最適化）は速度-位置更新を実装。",
      "局所探索系：SA（焼きなまし）は Metropolis 基準と適応リスタートを採用。Tabu（タブー探索）は禁忌表と特赦機構を実装。LNS（大近傍探索）は destroy-repair の 2 段最適化を実装。",
      "進化計算系：GA（遺伝的アルゴリズム）はトーナメント選択、多点交叉、3 種の変異演算子をサポート。",
      "混合・構築系：Hybrid は SA+LNS+Tabu の 3 段最適化を直列化。Clark-Wright 節約法と VRPTW 貪欲挿入で高品質初期解を生成。",
      "",
      "自動チューニングと並列加速",
      "ベイズ最適化（ガウス過程代理モデル）を統合し、最適パラメータ探索を自動化。チューニング効率を約 70% 向上。",
      "マルチコア並列評価と GPU（CuPy）加速をサポートし、8 コア環境で最大 5.2 倍の高速化。",
      "",
      "アルゴリズム構成",
      "構築ヒューリスティック（2 種）→ 初期解を高速生成",
      "改良アルゴリズム（6 種）→ 深度最適化",
      "混合戦略（1 種）→ 長所を統合",
      "",
      "鲸略使調度ソルバー - 9 大アルゴリズムコア",
      "1. ACO：情報素正帰還に基づく群知能手法。情報素行列、確率選択、エリート蟻戦略、情報素揮発を実装。",
      "2. PSO：粒子の位置・速度を反復更新し、個体最良と全体最良を保持。慣性重み減衰で探索と収束を両立。",
      "3. SA：Metropolis 受理規則と温度減衰を採用。局所最適脱出のための確率ジャンプ、適応再始動と多近傍演算子を搭載。",
      "4. Tabu：禁忌表で最近の移動を記録し、特赦規則と渇望水準で破禁。近傍探索で継続的に改善。",
      "5. LNS：破壊と修復の 2 段フレーム。破壊はランダム除去/最悪除去、修復は貪欲挿入/遺憾挿入、さらに SA 受理規則を併用。",
      "6. GA：トーナメント選択、部分写像交叉、3 種変異（再配置・交換・2-opt）、エリート保持を実装。",
      "7. Hybrid：SA→LNS→Tabu の 3 段を直列接続し、前段結果を後段へ引き継ぐ統合最適化。",
      "8. Clark-Wright 節約法：節約値計算、可行ルート統合、制約チェックで高品質初期解を高速生成。",
      "9. VRPTW 貪欲挿入：時間窓優先で店舗を並べ、最安挿入で逐次配置し、可行性を逐次検証。",
      "",
      "9 大アルゴリズムの分散型インテリジェンス",
      "アルゴリズムの寄せ集めではなく、役割配備です。各アルゴリズムは調度チェーンの中で置き換え不能な職責を担います。",
      "",
      "VRPTW：時間窓のゲートキーパー。時間窓違反のルートは初期案の段階で通しません。",
      "Clark-Wright：距離と車両数の最適化器。ルート結合の節約値から車両数下界の参考を与えます。",
      "GA：全体構造探索器。初期案の質に依存せず、大きな解空間を荒く探ります。",
      "PSO：群知能探索器。複数粒子の並行探索で、直感的でない帰属再編を見つけるのが得意です。",
      "ACO：経路フェロモン蓄積器。正のフィードバックで良い経路構造を繰り返し強化します。",
      "Tabu：局所深掘り器。悪い波次を監視し、同じ失敗手に戻らないようにします。",
      "LNS：大近傍修復器。悪いルートの局所構造を壊し、より良い断片で置き換えます。",
      "Hybrid：多戦略融合エンジン。GA 的探索と局所探索を融合し、探索と利用のバランスを取ります。",
      "SA：焼きなまし収束器。最終段で敢えて劣解も受け入れ、最後の局所最適にはまり込むのを防ぎます。",
      "",
      "これら 9 つは一列に流して終わりではありません。現在の求解段階、目標、既存結果の質を見て、どれを何本、どの順でリレーするかを動的に決めます。",
      "",
      "リレー求解：初期案から仕上げまでの知的意思決定チェーン",
      "従来システム：1 本のアルゴリズムを回す → 結果を出す → 終了。",
      "本システム：アルゴリズムをバトンでつなぎ、各段階に明確な役割と採用条件を持たせます。",
      "",
      "第一棒（初期案）     → VRPTW / Clark-Wright を並列実行し、2 つの骨格案を作成",
      "         ↓",
      "第二棒（全域探索）   → GA / PSO / ACO から目標に応じて 1〜2 本を選び、大規模再構成",
      "         ↓",
      "第三棒（局所強化）   → Hybrid / Tabu / LNS が悪い波次を噛み、細部まで磨く",
      "         ↓",
      "第四棒（収束）       → SA が最後の落とし穴を飛び越え、これ以上改善しないと確認して停止",
      "",
      "各段階の終了時にシステムは次を判定します。",
      "接棒条件を満たすか（内部代価の低下が閾値を超えたか）",
      "次の棒を飛ばすか（現案が十分に良いか）",
      "悪化が大きい場合に巻き戻して再試行するか",
      "",
      "これはブラックボックスではありません。各棒の入力、出力、判断理由、内部代価の分解が画面に継続表示されます。",
      "",
      "AI 中枢：3×3 アルゴリズムプールのリアルタイム可視化",
      "中央の 3×3 パネルで、今回どのアルゴリズムが実際に点灯しているかをすぐ確認できます。",
      `現在の方式：${currentStrategyLabel()} / 現在の目標：${currentGoalLabel()} / 今回のアルゴリズムチェーン：${selectedAlgorithms.join(" / ") || "なし"}`,
      "",
      "調度中に見えるもの：",
      "どのアルゴリズムが点灯しているか（本ラウンドで実際に呼ばれたもの）",
      "各アルゴリズムの役割（初期案 / 探索 / 強化 / 収束）",
      "今が第何棒か（リレー進捗）",
      "内部代価の曲線がどう動いているか（案の質の時間軸）",
      "",
      "勘に頼らず、闇雲に試さず、各アルゴリズムの貢献度を見える形にします。",
      "",
      "データ層の業務知能",
      "これは汎用 VRPTW ソルバーではありません。多店舗・一日多配・高頻度補充の業務に合わせて深くチューニングした調度エンジンです。",
      "",
      "3 つの中核メカニズム",
      "1. 多波次データの分離管理",
      "波次数は固定しません。2 便、3 便、4 便、5 便…実業務に合わせて設定できます。各波次は店舗一覧、時間窓、サービス時間を独立管理し、その波次需要がない拠点は無理に入れません。理論上は入るが現場では配送不能、という偽案を防ぎます。",
      "",
      "2. 超遠距離拠点の独立評価",
      "単波次で距離が極端に遠い拠点は、倉庫→拠点の去程距離だけを評価し、帰庫まで含めた全行程一律判定で誤って落としません。合理的な遠距離専用便を守りつつ、本当に不可達な拠点は見抜きます。",
      "",
      "3. 路網エンジン + スマートキャッシュ",
      "高德や百度などの商用路網 API を優先し、実距離・実所要時間で案を作ります。同時にローカルキャッシュを持ち、同じ倉庫-店舗ペアは初回だけ真面目に取得し、以後は即時再利用します。実行性とコストを両立します。",
      "",
      "現実に解いている問題",
      "チェーンブランド向け：一日多配、店舗ごとに補充頻度が違い、一部は朝だけ・一部は夜だけでも、そのまま運用できます。",
      "3PL 向け：複数ブランド、複数倉庫、複数時間窓ルールを 1 つのシステムで統合調度できます。",
      "配車担当向け：この波次に出すべきでない拠点を手動で除外する必要がなく、システムが自動でルールに従ってふるい分けます。",
      "経営層向け：路網 API はキャッシュされるため、1 枚の調度票で何百回も API を焼くことがありません。",
      "",
      "説明可能な調度：答えだけでなく証拠を出す",
      "どの結果を開いても、次が確認できます。",
      "なぜこの店舗がこの車両に入ったか（時間窓整合、距離便益、積載維持）",
      "なぜこの波次がさらに圧縮されなかったか（分布の疎さ、時間窓の硬さ、車数制約）",
      "なぜ未割当になったか（超距離、超時間、空き車両なし、ルール除外）",
      "内部代価の分解（時間窓罰、超載罰、距離罰など）",
      "",
      "経営層が知りたいのはアルゴリズム名ではなく、「なぜ今日は案 1 で、案 3 ではないのか」です。本システムはその証拠チェーンを出します。",
      "",
      "求解アーカイブ：各ラウンド結果を永久に残す",
      "市販システムでは第 5 ラウンドを回すと第 3 ラウンドが消えがちです。",
      "本システムは各ラウンドを自動保存し、ページ送りで見返し、比較し、復元できます。",
      "",
      "配車担当の使い方：",
      "快速初排 → アーカイブ",
      "继续优化を 3 回 → アーカイブ",
      "やはり第 2 ラウンドが良い → 第 2 ラウンドを復元 → そこから再最適化",
      "",
      "上書きせず、失わず、各判断を痕跡として残します。",
      "",
      "デジタル調度官：音声対話を前置",
      "従来は結果が出るまで聞けませんでした。",
      "本システムは計算前から対話できます。",
      "",
      "音声質問：「前のラウンドであの店舗が入らなかったのはなぜ？」",
      "音声ブリーフィング：「今回の調度は 47 店舗、6 台、総距離 318km、前回より 12km 短縮しました。」",
      "",
      "待たせず、先に待機します。",
      "",
      "経営層の視点",
      "このシステムが経営層に価値を持つのは、見た目ではなく次の点です。",
      "結果が追跡可能：各ラウンドの案が残るため、意思決定が勘にならない",
      "理由が説明可能：どのルートもどの店舗帰属も理由を示せる",
      "過程が監査可能：誰がいつ何を触り、システムがどの接力を走らせたか残る",
      "比較が定量化：複数案を並べ、代価・台数・距離・違反数を同時比較できる",
      "コストが制御可能：路網 API はキャッシュし、算法チェーンも無駄なく編成する",
      "",
      "これは配車担当だけの操作台ではなく、配送意思決定全体の証拠センターです。",
      "",
      "一言でまとめると",
      "市面でも珍しい、9 大アルゴリズムを役割配備し、接力求解を透明化し、調度判断を説明可能にし、全履歴を残す多波次配送調度システムです。",
      "",
      "業種を選ばず、業態を選ばず、「計算できる」から「正しく計算でき、説明でき、残せて、追及にも耐えられる」へ進めます。",
    ].join("\n");
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return [
    "城市配送AI调度系统",
    "",
    "核心定位",
    "这不是一套排车工具。这是一套具备算法决策链、可解释调度、全流程留痕的AI调度操作系统。",
    "",
    "市面上绝大多数调度系统做的是三件事：输入数据 → 跑一个算法 → 输出表格。调度员拿到结果，不知道这个结果怎么来的，也不知道为什么不是另一个结果。",
    "",
    "这套系统不一样。它把九套算法的协同决策过程完整暴露出来，让调度员能看到、能理解、能干预、能复盘。让管理层能追溯、能对比、能决策。",
    "",
    "适用行业：连锁零售、餐饮门店、医药配送、社区电商、冷链生鲜、中央厨房配送、石油站点补货、第三方统仓共配……凡是涉及「同一批车、一天多趟、每家站点有自己的时间窗口和补货节奏」的业务，都能直接落地。",
    "",
    "鲸略使调度求解器 - 产品简介",
    "九大算法引擎，真正的工业级优化。",
    "鲸略使调度求解器是一款面向VRPTW（带时间窗车辆路径问题）的企业级智能调度系统，核心算法库包含九种真正的元启发式算法，绝非简单的规则堆砌。",
    "",
    "核心算法矩阵",
    "群智能类：蚁群算法（ACO）配备完整信息素挥发与精英策略；粒子群算法（PSO）实现真正的速度-位置更新机制。",
    "局部搜索类：模拟退火（SA）采用Metropolis准则与自适应重启；禁忌搜索（Tabu）拥有完整的禁忌表和特赦机制；大邻域搜索（LNS）实现destroy-repair双阶段优化。",
    "进化计算类：遗传算法（GA）支持锦标赛选择、多点交叉与三种变异算子。",
    "混合与构造类：Hybrid混合器串联SA+LNS+Tabu三阶段优化；Clark-Wright节约法和VRPTW贪心插入提供高质量初始解。",
    "",
    "智能调参与并行加速",
    "集成贝叶斯优化（高斯过程代理模型），自动搜索最优参数组合，调参效率提升70%。",
    "支持多核并行评估与GPU加速（CuPy），8核环境下加速比达5.2倍。",
    "",
    "算法集合结构",
    "构造启发式（2种）→ 快速生成初始解",
    "改进算法（6种）→ 深度优化",
    "混合策略（1种）→ 取长补短",
    "",
    "鲸略使调度求解器 - 九大算法核心",
    "1. 蚁群算法（ACO）：基于信息素正反馈机制，包含信息素矩阵维护、轮盘赌概率选择、精英蚂蚁策略与信息素挥发机制。",
    "2. 粒子群算法（PSO）：通过粒子位置与速度迭代更新寻优，记录个体最优与全局最优，并采用惯性权重衰减平衡探索与开发。",
    "3. 模拟退火（SA）：采用Metropolis接受准则和温度衰减策略，内置自适应重启机制与多邻域算子。",
    "4. 禁忌搜索（Tabu）：维护禁忌表记录近期移动，通过特赦准则和渴望水平破禁，结合邻域搜索持续改进。",
    "5. 大邻域搜索（LNS）：采用破坏与修复双阶段框架，破坏支持随机移除和最差移除，修复支持贪婪插入和遗憾插入，并引入模拟退火接受准则。",
    "6. 遗传算法（GA）：采用锦标赛选择、部分映射交叉与三种变异算子（重定位、交换、2-opt），并实施精英保留机制。",
    "7. 混合算法（Hybrid）：串联模拟退火、大邻域搜索和禁忌搜索三个阶段，将前序结果传递给后续算法。",
    "8. Clark-Wright节约法：通过计算节约值、合并可行路线并检查约束条件，快速生成高质量初始解。",
    "9. VRPTW贪心插入：按最早时间窗优先排序门店，采用最便宜插入策略并实时验证可行性。",
    "",
    "九大算法的分布式智能",
    "不是算法堆砌，是算法岗位化。每一套算法在调度链中承担不可替代的职能：",
    "",
    "VRPTW：时间窗守门人。硬约束第一关，违反时间窗的线路根本不出现在初排中。",
    "Clark-Wright：里程与车数优化器。从合并收益出发，给出车数下界的理论参考。",
    "GA：全局结构搜索器。在大解空间中暴力探索，不依赖初排质量。",
    "PSO：群体协同探索器。多粒子并行，擅长发现非直觉的归属关系重组。",
    "ACO：路径信息素累积器。通过正反馈机制，让好的路径结构被反复强化。",
    "Tabu：局部深度挖掘器。盯住坏波次，禁止走回头路，强制跳出局部陷阱。",
    "LNS：大邻域修复器。摧毁坏线路的局部结构，用更优片段替换。",
    "Hybrid：多策略融合引擎。在 GA 框架中嵌入局部搜索，平衡探索与利用。",
    "SA：退火收口器。在最后阶段接受差解，避免卡在最后一层最优。",
    "",
    "品牌隐喻：鲸类本领与求解机制的暗合",
    "这套系统并不是随便选了一个鲸鱼 Logo。鲸类的捕食、迁徙、协同与感知方式，和这套求解器里的多算法协同机制高度同构。",
    "",
    "1. 座头鲸的“气泡网捕食” ↔ 大邻域搜索 + 禁忌搜索",
    "座头鲸会先吐气泡形成“网”，把猎物大范围圈住，再在圈内反复下潜、精细捕食。",
    "对应到求解器里：大邻域搜索负责破坏部分路线、重构结构，相当于先吐气泡画圈；禁忌搜索负责在邻域内反复交换门店、持续修补，相当于圈内精细下潜。",
    "对应到求解过程，就是先大范围扰动，再局部精修。",
    "",
    "2. 鲸的“长途迁徙 + 局部觅食” ↔ 模拟退火的“高温探索 + 低温收敛”",
    "鲸会先进行长距离迁徙，广泛探索新海域；到达高价值海域后，再集中觅食。",
    "对应到求解器里：模拟退火高温阶段接受差解、主动跳出局部最优，相当于迁徙探索；低温阶段只保留更优解、逐步收紧，相当于抵达好海域后的稳定觅食。",
    "对应到求解过程，就是先放开探索，再收紧收敛。",
    "",
    "3. 虎鲸的“群体狩猎分工” ↔ 粒子群算法的“个体经验 + 群体协作”",
    "虎鲸群捕猎时会分工：有的驱赶、有的拦截、有的主攻，但整体始终围绕群体目标行动。",
    "对应到求解器里：每个粒子都有自己的位置、速度和个体最优经验；群体最优则持续牵引整体方向。",
    "对应到群体智能机制，就是个体经验与群体协作共同形成整体智能。",
    "",
    "4. 抹香鲸的“深潜觅食” ↔ Clark-Wright 节约法的“一次深潜到位”",
    "抹香鲸能够一次深潜到极深海域，在有限时间里精准完成觅食，再高效返回。",
    "对应到求解器里：Clark-Wright 节约法不依赖长链迭代，而是直接依据节约量完成路线合并，一次构造到位。",
    "对应到构造型算法机制，就是不反复折腾，一次深潜把事办完。",
    "",
    "5. 鲸的“回声定位” ↔ 综合评分体系的“多维反馈导向”",
    "鲸依靠回声定位感知周围环境，判断猎物位置、方向和追击价值。",
    "对应到求解器里：综合评分就是算法的回声反馈。准时率 45% + 里程 25% + 装载率 15% + 偏好 15%，共同决定一个方案值不值得被保留、下一步该往哪里优化。",
    "对应到评分反馈机制，就是不只看单一指标，而是依靠多维反馈做判断。",
    "",
    "最有意思的一点是：整套系统本身就像一个“算法鲸群”。",
    "九套算法同时在场，有的负责大范围探索（SA / PSO / ACO），有的负责局部精修（禁忌搜索 / 大邻域搜索），有的负责快速给出基线（VRPTW / Clark-Wright），最后由评分体系决定谁的结果被采纳。",
    "这本质上就是一群算法鲸在协同捕食同一个目标：找到更优的调度方案。",
    "",
    "这九套算法不是串行跑一遍就结束。系统根据当前求解阶段、方案目标、已有结果质量，动态决定调用哪几套、以什么顺序、接力多少次。",
    "",
    "接力求解：从初排到收口的智能决策链",
    "传统系统：跑一个算法 → 出结果 → 结束。",
    "这套系统：算法接力跑，每一棒都有明确任务和判断标准。",
    "",
    "第一棒（初排）     → VRPTW / Clark-Wright 并行，给出两版骨架方案",
    "         ↓",
    "第二棒（全局探索） → GA / PSO / ACO 根据目标选择 1-2 套，大规模重构",
    "         ↓",
    "第三棒（局部强化） → Hybrid / Tabu / LNS 咬住坏波次，逐条打磨",
    "         ↓",
    "第四棒（收口）     → SA 跳坑，确认无法进一步改善才终止",
    "",
    "每一棒结束时，系统会判断：",
    "接棒条件是否满足（代价下降是否达到阈值）",
    "是否跳过下一棒（当前方案已足够好）",
    "是否回退重跑（某一步恶化超出预期）",
    "",
    "这不是黑箱。每一棒的输入、输出、决策理由、代价拆解，都在界面上持续输出。",
    "",
    "AI中枢：九宫格实时可视化",
    "系统中央有一块 3×3 的算法池面板。",
    `当前模式：${currentStrategyLabel()}；当前目标：${currentGoalLabel()}；本轮计划调用：${selectedAlgorithms.join(" / ") || "无"}`,
    "",
    "每一轮求解过程中，调度员能直接看到：",
    "哪些算法被点亮（本轮实际调用）",
    "每个算法承担什么角色（初排 / 探索 / 强化 / 收口）",
    "当前处于第几棒（接力进度）",
    "代价曲线如何变化（方案质量的时间线）",
    "",
    "不靠猜。不靠经验盲测。每一轮算法的贡献度是可见的。",
    "",
    "数据层的业务智能",
    "这不是一个通用的 VRPTW 求解器。这是一套为多门店、一日多配、敏捷补货场景深度定制的调度引擎。",
    "",
    "三套核心机制",
    "1. 多波次数据分离管理",
    "波次数不写死。两配、三配、四配、五配……按业务实际配置。每个波次独立维护门店清单、时间窗口、服务时长。没有某波次需求的站点，不会被系统强行塞入。时间窗回归真实补货节奏，不产生「理论上能排、实际上送不了」的假方案。",
    "",
    "2. 超远站点独立考核规则",
    "单波次中距离异常的站点，系统采用独立代价函数——只考核从仓库到站点的去程里程，不再用「去程+回库」的全程里程一刀切。避免合理线路因返程空驶被误杀，同时保留对真正不可达站点的识别能力。",
    "",
    "3. 路网引擎 + 智能缓存",
    "优先调用高德 / 百度等商用路网接口获取真实距离与时长，确保方案可执行。同时建立本地缓存机制：同一对站点-仓库的组合，第一次认真拉取，后续直接命中缓存。成本和响应速度兼顾，不会重复烧接口费。",
    "",
    "解决的真实问题",
    "对连锁品牌：一日多配、不同门店不同补货频次、部分站点只配早不配晚——系统天然支持，不用改代码。",
    "对第三方物流：服务多个品牌、多个仓库、多套时间窗规则——一套系统统一调度，不来回切换。",
    "对调度员：不用手动剔除「不该出现在这一波」的站点，系统自动按规则过滤。",
    "对管理层：路网 API 有缓存，算力成本可控，不会被一张调度单烧掉几百次接口调用。",
    "",
    "可解释调度：不是只给答案，是给证据链",
    "调度员点击任何一个结果，能看到：",
    "为什么这个门店归这辆车（时间窗匹配 / 里程收益 / 避免超载）",
    "为什么这个波次没被优化掉（门店分布稀疏 / 时间窗刚性 / 车数约束）",
    "为什么某些门店未被调度（超距 / 超时 / 无可用车辆 / 规则过滤）",
    "罚分规则的内部代价拆解（时间窗惩罚 / 超载惩罚 / 里程惩罚各自多少）",
    "",
    "管理层不需要看懂算法。管理层需要看懂“为什么今天用方案 1 而不是方案 3”。这套系统提供的就是这个证据链。",
    "",
    "求解档案：每一轮结果永久留痕",
    "市面系统：算完第 5 轮，第 3 轮就没了。",
    "这套系统：自动存档每一轮求解结果。后续可以翻页回看、对比、恢复历史方案。",
    "",
    "调度员可以这样工作：",
    "快速初排 → 存档",
    "继续优化三轮 → 存档",
    "发现第 2 轮方案更好 → 恢复第 2 轮 → 在此基础上继续优化",
    "",
    "不覆盖。不丢失。每一轮思考都有痕迹。",
    "",
    "数字调度官：语音交互前置",
    "传统系统：等结果算完，才能看、才能问。",
    "这套系统：求解前即可对话。",
    "",
    "语音提问：“为什么上一轮那家店没排进去？”",
    "语音播报：“本轮共调度 47 家门店，使用 6 辆车，总里程 318 公里，较上一轮下降 12 公里。”",
    "",
    "提前待机，不等人。",
    "",
    "管理层的视角",
    "这套系统对管理层有价值，不是因为界面好看，而是因为：",
    "结果可追溯：每一轮方案都有存档，决策不是拍脑袋",
    "原因可解释：任何一条线路、一个门店的归属，都能说出为什么",
    "过程可审计：调度员做了哪些操作、系统自动跑了哪些接力、谁在什么时间点干预了什么，都有记录",
    "对比可量化：多方案并排对比，代价、车数、里程、违反约束数一目了然",
    "成本可控制：路网 API 有缓存，不重复调取；算法链可配置，不浪费算力",
    "",
    "它不是调度员一个人的操作台。它是整个配送决策的证据中心。",
    "",
    "一句话总结",
    "市面唯一一套将九大算法岗位化、接力求解透明化、调度决策可解释化、全流程留痕永久化的多波次配送调度系统。",
    "",
    "不限行业，不限业态。从“算得出”到“算得对、讲得清、留得住、经得起追问”。",
  ].join("\n");
}

function clearProcessTypingTimers() {
  processTypingTimers.forEach((timer) => clearTimeout(timer));
  processTypingTimers = [];
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function typeProcessText(box, text) {
  box.textContent = "";
  let index = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const punctuationPauses = new Set(["，", "。", "：", "；", "！", "？", ",", ".", ":", ";", "!", "?", "\n"]);
  const tick = () => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (index >= text.length) {
      return;
    }
    const nextChar = text[index];
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const step = /[a-zA-Z0-9]/.test(nextChar) ? 2 : 1;
    index = Math.min(text.length, index + step);
    box.textContent = text.slice(0, index);
    box.scrollTop = box.scrollHeight;
    const pause = punctuationPauses.has(text[index - 1]) ? 220 : 95;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const timer = setTimeout(tick, pause);
    processTypingTimers.push(timer);
  };
  tick();
}

function openProcessModal(resultKey, waveId, plateNo, tripNo) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const matched = state.lastResults.map((result) => {
    const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (waveIndex < 0) return null;
    const plan = result.solution[waveIndex].find((item) => item.vehicle.plateNo === plateNo);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const trip = plan?.trips.find((item) => String(item.tripNo) === String(tripNo));
    if (!plan || !trip) return null;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { result, wave: result.scenario.waves[waveIndex], plan, trip };
  }).filter(Boolean);
  if (!matched.length) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const modal = document.getElementById("processModal");
  const preferredIndex = matched.findIndex((item) => item.result.key === resultKey);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (preferredIndex > 0) {
    const [preferred] = matched.splice(preferredIndex, 1);
    matched.unshift(preferred);
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("processModalTitle").textContent = lang() === "ja" ? `${plateNo} · ${waveId} · 第 ${tripNo} 便 · 複数アルゴリズム${L("playback")}` : `${plateNo} · ${waveId} · 第 ${tripNo} ${L("tripSuffix")} · 多算法${L("playback")}`;
  const grid = document.getElementById("processGrid");
  grid.innerHTML = matched.map((item, index) => `
    <section class="process-pane">
      <div class="process-pane-head">
        <strong>${algoLabel(item.result.key)}</strong>
        <span class="chip">${L("route")} ${buildRouteDisplay(item.trip.route, item.result.scenario)}</span>
      </div>
      ${buildPenaltyRuleCard()}
      <pre id="processTypewriter-${index}" class="terminal-view"></pre>
    </section>
  `).join("");
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  clearProcessTypingTimers();
  matched.forEach((item, index) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const box = document.getElementById(`processTypewriter-${index}`);
    if (box) typeProcessText(box, buildProcessNarrative(item.result, item.plan, item.trip, item.result.scenario, item.wave, matched.length > 1));
  });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function closeProcessModal() {
  const modal = document.getElementById("processModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  clearProcessTypingTimers();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function destroyRouteLeafletMap() {
  routeAmapMarkers.forEach((marker) => {
    try { marker.setMap?.(null); } catch {}
  });
  routeAmapMarkers = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (routeAmapPolyline) {
    try { routeAmapPolyline.setMap?.(null); } catch {}
    routeAmapPolyline = null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (routeAmapMap) {
    try { routeAmapMap.destroy?.(); } catch {}
    routeAmapMap = null;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (routeLeafletLayerGroup) {
    routeLeafletLayerGroup.clearLayers();
    routeLeafletLayerGroup = null;
  }
  if (routeLeafletMap) {
    routeLeafletMap.remove();
    routeLeafletMap = null;
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderInteractiveRouteMap(containerId, mapData) {
  const container = document.getElementById(containerId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!container) return false;
  destroyRouteLeafletMap();
  if (window.AMap?.Map) {
    routeAmapMap = new window.AMap.Map(containerId, {
      viewMode: "2D",
      zoom: 11,
      mapStyle: "amap://styles/normal",
      resizeEnable: true,
      dragEnable: true,
      zoomEnable: true,
      doubleClickZoom: true,
      scrollWheel: true,
      keyboardEnable: false,
    });
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    try {
      routeAmapMap.addControl(new window.AMap.ToolBar({ position: "RB" }));
      routeAmapMap.addControl(new window.AMap.Scale());
    } catch {}
    const lineLngLats = (mapData.polylinePoints || [])
      .filter((point) => Array.isArray(point) && point.length === 2)
      .map((point) => [Number(point[0]), Number(point[1])]);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (lineLngLats.length) {
      routeAmapPolyline = new window.AMap.Polyline({
        path: lineLngLats,
        strokeColor: "#2563eb",
        strokeWeight: 6,
        strokeOpacity: 0.95,
        lineJoin: "round",
        lineCap: "round",
      });
      routeAmapMap.add(routeAmapPolyline);
    }
    const boundsPoints = [];
    routeAmapMarkers = (mapData.markers || []).flatMap((item) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const lng = Number(item.lng);
      const lat = Number(item.lat);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!Number.isFinite(lng) || !Number.isFinite(lat)) return [];
      boundsPoints.push([lng, lat]);
      const labelHtml = `
        <div class="leaflet-route-marker ${item.label === "D" ? "is-depot" : ""}">
          <span class="leaflet-route-badge">${escapeHtml(item.label)}</span>
          <span class="leaflet-route-text">${escapeHtml(item.shortName || item.name)}</span>
        </div>
      `;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const marker = new window.AMap.Marker({
        position: [lng, lat],
        anchor: item.label === "D" ? "center" : "bottom-center",
        content: labelHtml,
        offset: new window.AMap.Pixel(item.label === "D" ? -22 : -22, item.label === "D" ? -17 : -30),
      });
      const infoWindow = new window.AMap.InfoWindow({
        content: `<strong>${escapeHtml(item.name)}</strong>`,
        offset: new window.AMap.Pixel(0, -18),
      });
      marker.on("click", () => infoWindow.open(routeAmapMap, [lng, lat]));
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return marker;
    });
    if (routeAmapMarkers.length) routeAmapMap.add(routeAmapMarkers);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (boundsPoints.length || lineLngLats.length) {
      routeAmapMap.setFitView([...(routeAmapMarkers || []), ...(routeAmapPolyline ? [routeAmapPolyline] : [])], false, [70, 90, 70, 90], 16);
    } else {
      routeAmapMap.setZoomAndCenter(11, [Number(DC.lng), Number(DC.lat)]);
    }
    setTimeout(() => routeAmapMap?.resize?.(), 60);
    return true;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (typeof window.L === "undefined") return false;
  routeLeafletMap = window.L.map(container, {
    zoomControl: true,
    scrollWheelZoom: true,
    dragging: true,
    doubleClickZoom: true,
    boxZoom: true,
    touchZoom: true,
  });
  window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap",
  }).addTo(routeLeafletMap);
  routeLeafletLayerGroup = window.L.layerGroup().addTo(routeLeafletMap);

  const lineLatLngs = (mapData.polylinePoints || [])
    .filter((point) => Array.isArray(point) && point.length === 2)
    .map((point) => [Number(point[1]), Number(point[0])]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (lineLatLngs.length) {
    window.L.polyline(lineLatLngs, {
      color: "#2563eb",
      weight: 5,
      opacity: 0.9,
    }).addTo(routeLeafletLayerGroup);
  }

  const markerLatLngs = [];
  (mapData.markers || []).forEach((item) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lat = Number(item.lat);
    const lng = Number(item.lng);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    markerLatLngs.push([lat, lng]);
    const markerHtml = `
      <div class="leaflet-route-marker ${item.label === "D" ? "is-depot" : ""}">
        <span class="leaflet-route-badge">${escapeHtml(item.label)}</span>
        <span class="leaflet-route-text">${escapeHtml(item.shortName || item.name)}</span>
      </div>
    `;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const icon = window.L.divIcon({
      className: "leaflet-route-divicon",
      html: markerHtml,
      iconSize: [110, 34],
      iconAnchor: item.label === "D" ? [22, 17] : [22, 30],
    });
    window.L.marker([lat, lng], { icon })
      .bindPopup(`<strong>${escapeHtml(item.name)}</strong>`)
      .addTo(routeLeafletLayerGroup);
  });

  const bounds = window.L.latLngBounds([...(lineLatLngs || []), ...markerLatLngs]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (bounds.isValid()) routeLeafletMap.fitBounds(bounds.pad(0.18));
  else routeLeafletMap.setView([Number(DC.lat), Number(DC.lng)], 11);
  setTimeout(() => routeLeafletMap?.invalidateSize(), 50);
  return true;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildFallbackRouteMapData(result, trip) {
  const nodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId) || { id: stop.storeId, name: stop.storeName, lng: 0, lat: 0 }), DC]
    .filter((node) => Number.isFinite(Number(node.lng)) && Number.isFinite(Number(node.lat)));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const markers = computeMapOverlayMarkers(nodes.slice(0, -1).map((node, index) => ({
    label: index === 0 ? "D" : String(index),
    lng: Number(node.lng),
    lat: Number(node.lat),
    name: node.id === DC.id ? L("depot") : `${node.id} ${node.name}`,
    shortName: node.id === DC.id ? L("depot") : shortenMapName(node.name || node.id, 12),
  })), nodes.map((node) => [Number(node.lng), Number(node.lat)]));
  return {
    title: "fallback",
    markers,
    polylinePoints: nodes.map((node) => [Number(node.lng), Number(node.lat)]),
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderFallbackRouteMap(containerId, mapData) {
  const container = document.getElementById(containerId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!container) return false;
  const points = (mapData.polylinePoints || []).filter((point) => Array.isArray(point) && point.length === 2);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const markers = mapData.markers || [];
  const allPoints = [...points, ...markers.map((item) => [Number(item.lng), Number(item.lat)])];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!allPoints.length) return false;
  container.innerHTML = `
    <div class="route-panzoom">
      <div class="route-panzoom-toolbar">
        <button type="button" class="mini route-zoom-in">+</button>
        <button type="button" class="mini route-zoom-out">-</button>
        <span class="muted">${lang() === "ja" ? "実地図ベース / ドラッグ・ホイール操作に対応" : "真实底图，可拖动 / 滚轮缩放"}</span>
      </div>
      <div class="route-panzoom-viewport">
        <div class="route-panzoom-content route-tilemap-content">
          <div class="route-tilemap-layer"></div>
          <svg class="route-tilemap-overlay" xmlns="http://www.w3.org/2000/svg"></svg>
        </div>
      </div>
    </div>
  `;
  const viewport = container.querySelector(".route-panzoom-viewport");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const content = container.querySelector(".route-panzoom-content");
  const tileLayer = container.querySelector(".route-tilemap-layer");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const overlay = container.querySelector(".route-tilemap-overlay");
  if (!viewport || !content || !tileLayer || !overlay) return false;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const tileSize = 256;
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const project = (lng, lat, zoom) => {
    const sinLat = Math.sin((Number(lat) * Math.PI) / 180);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const scale = tileSize * 2 ** zoom;
    const x = ((Number(lng) + 180) / 360) * scale;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const y = (0.5 - Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI)) * scale;
    return { x, y };
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const unproject = (x, y, zoom) => {
    const scale = tileSize * 2 ** zoom;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lng = (x / scale) * 360 - 180;
    const n = Math.PI - (2 * Math.PI * y) / scale;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const lat = (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
    return { lng, lat };
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const getViewportSize = () => ({
    width: Math.max(320, viewport.clientWidth || 960),
    height: Math.max(320, viewport.clientHeight || 640),
  });
  const chooseZoom = (width, height) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (let zoom = 17; zoom >= 3; zoom -= 1) {
      const projected = allPoints.map(([lng, lat]) => project(lng, lat, zoom));
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const xs = projected.map((item) => item.x);
      const ys = projected.map((item) => item.y);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const spanX = Math.max(...xs) - Math.min(...xs);
      const spanY = Math.max(...ys) - Math.min(...ys);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (spanX <= width - 140 && spanY <= height - 140) return zoom;
    }
    return 3;
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const initialSize = getViewportSize();
  const centerSeed = {
    lng: allPoints.reduce((sum, point) => sum + Number(point[0]), 0) / allPoints.length,
    lat: allPoints.reduce((sum, point) => sum + Number(point[1]), 0) / allPoints.length,
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let zoom = chooseZoom(initialSize.width, initialSize.height);
  let center = { ...centerSeed };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let dragging = false;
  let dragStartX = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let dragStartY = 0;
  let dragCenterWorld = null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wrapTileX = (x, maxTile) => ((x % maxTile) + maxTile) % maxTile;
  const buildMarkerSvg = (item, x, y) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const depot = item.label === "D";
    const labelWidth = Math.max(88, String(item.shortName || item.name).length * 12);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `
      <g transform="translate(${x.toFixed(1)},${y.toFixed(1)})">
        <circle r="${depot ? 18 : 15}" fill="${depot ? "#2563eb" : "#d97706"}" stroke="#ffffff" stroke-width="3"></circle>
        <text text-anchor="middle" dominant-baseline="central" font-size="12" font-weight="700" fill="#ffffff">${escapeHtml(item.label)}</text>
        <rect x="18" y="-16" rx="12" ry="12" width="${labelWidth}" height="28" fill="rgba(15,23,42,0.82)"></rect>
        <text x="28" y="2" font-size="13" fill="#f8fafc">${escapeHtml(item.shortName || item.name)}</text>
      </g>
    `;
  };
  const render = () => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const { width, height } = getViewportSize();
    overlay.setAttribute("viewBox", `0 0 ${width} ${height}`);
    const centerWorld = project(center.lng, center.lat, zoom);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const topLeftX = centerWorld.x - width / 2;
    const topLeftY = centerWorld.y - height / 2;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const scale = tileSize * 2 ** zoom;
    const maxTile = 2 ** zoom;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const startTileX = Math.floor(topLeftX / tileSize);
    const endTileX = Math.floor((topLeftX + width) / tileSize);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const startTileY = Math.floor(topLeftY / tileSize);
    const endTileY = Math.floor((topLeftY + height) / tileSize);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const tiles = [];
    for (let tileX = startTileX; tileX <= endTileX; tileX += 1) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      for (let tileY = startTileY; tileY <= endTileY; tileY += 1) {
        if (tileY < 0 || tileY >= maxTile) continue;
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        const drawX = tileX * tileSize - topLeftX;
        const drawY = tileY * tileSize - topLeftY;
        tiles.push(`
          <img
            class="route-tile"
            alt=""
            draggable="false"
            src="https://tile.openstreetmap.org/${zoom}/${wrapTileX(tileX, maxTile)}/${tileY}.png"
            style="left:${drawX.toFixed(1)}px;top:${drawY.toFixed(1)}px;width:${tileSize}px;height:${tileSize}px;"
          />
        `);
      }
    }
    tileLayer.innerHTML = tiles.join("");
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const polylinePath = points.map(([lng, lat], index) => {
      const projected = project(lng, lat, zoom);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const x = projected.x - topLeftX;
      const y = projected.y - topLeftY;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
    const markerSvg = markers.map((item) => {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const projected = project(item.lng, item.lat, zoom);
      return buildMarkerSvg(item, projected.x - topLeftX, projected.y - topLeftY);
    }).join("");
    overlay.innerHTML = `
      <path d="${polylinePath}" fill="none" stroke="#2563eb" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"></path>
      ${markerSvg}
    `;
  };
  viewport?.addEventListener("wheel", (event) => {
    event.preventDefault();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextZoom = clamp(zoom + (event.deltaY < 0 ? 1 : -1), 3, 18);
    if (nextZoom === zoom) return;
    zoom = nextZoom;
    render();
  }, { passive: false });
  viewport?.addEventListener("pointerdown", (event) => {
    dragging = true;
    dragStartX = event.clientX;
    dragStartY = event.clientY;
    dragCenterWorld = project(center.lng, center.lat, zoom);
    viewport.setPointerCapture?.(event.pointerId);
  });
  viewport?.addEventListener("pointermove", (event) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!dragging || !dragCenterWorld) return;
    const { width, height } = getViewportSize();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const nextWorldX = dragCenterWorld.x - (event.clientX - dragStartX);
    const nextWorldY = dragCenterWorld.y - (event.clientY - dragStartY);
    center = unproject(nextWorldX, nextWorldY, zoom);
    center.lat = clamp(center.lat, -85, 85);
    render();
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stopDragging = () => { dragging = false; };
  viewport?.addEventListener("pointerup", stopDragging);
  viewport?.addEventListener("pointerleave", stopDragging);
  container.querySelector(".route-zoom-in")?.addEventListener("click", () => { zoom = clamp(zoom + 1, 3, 18); render(); });
  container.querySelector(".route-zoom-out")?.addEventListener("click", () => { zoom = clamp(zoom - 1, 3, 18); render(); });
  window.requestAnimationFrame(render);
  return true;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildRouteStopScheduleTable(trip) {
  const rows = (trip?.stops || []).map((stop, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${escapeHtml(`${stop.storeId || ""} ${stop.storeName || ""}`.trim())}</td>
      <td>${formatTime(stop.arrival)}</td>
      <td>${escapeHtml(stop.desiredArrival || "--:--")}</td>
    </tr>
  `).join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `
    <div class="route-stop-card">
      <div class="route-stop-card-title">${lang() === "ja" ? "店舗到着計画" : "门店到店计划"}</div>
      <div class="table-wrap route-stop-table-wrap">
        <table class="route-stop-table">
          <tr>
            <th>${L("routeStopSeq")}</th>
            <th>${L("routeStopName")}</th>
            <th>${L("routePlanArrival")}</th>
            <th>${L("routeDesiredArrival")}</th>
          </tr>
          ${rows || `<tr><td colspan="4">${lang() === "ja" ? "店舗はありません" : "暂无门店"}</td></tr>`}
        </table>
      </div>
    </div>
  `;
}

async function openRouteMapModal(resultKey, waveId, plateNo, tripNo) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const result = state.lastResults.find((item) => item.key === resultKey);
  if (!result) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
  if (waveIndex < 0) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const wave = result.scenario.waves[waveIndex];
  const plan = result.solution[waveIndex].find((item) => item.vehicle.plateNo === plateNo);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const trip = plan?.trips.find((item) => String(item.tripNo) === String(tripNo));
  if (!plan || !trip) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const modal = document.getElementById("routeMapModal");
  const title = document.getElementById("routeMapModalTitle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const body = document.getElementById("routeMapModalBody");
  const fallbackMapData = buildFallbackRouteMapData(result, trip);
  title.textContent = `${plateNo} · ${waveId} · ${L("tripNo")}${tripNo}${L("tripSuffix")} · ${L("routeMap")}`;
  body.innerHTML = `
    <div class="route-map-shell">
        <div id="routeInteractiveMapFallback" class="route-map-interactive"></div>
        <div class="route-map-meta">
          <p class="muted">${L("route")}：${buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? "まずローカルの対話型ルート図を表示し、外部ルートデータが取れれば自動で更新します。" : "先展示本地交互线路图，若外部线路数据可用会自动升级成更完整的线路图。"}</p>
          <div class="route-map-legend">
            ${fallbackMapData.markers.map((item) => `<span class="route-map-chip"><strong>${escapeHtml(item.label)}</strong>${escapeHtml(item.name)}</span>`).join("")}
          </div>
          ${buildRouteStopScheduleTable(trip)}
        </div>
      </div>
    `;
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  renderFallbackRouteMap("routeInteractiveMapFallback", fallbackMapData);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    const mapData = await getTripRouteMapData(result, wave, plan, trip);
    body.innerHTML = `
      <div class="route-map-shell">
        <div id="routeInteractiveMap" class="route-map-interactive"></div>
        <div class="route-map-meta">
          <p class="muted">${L("route")}：${buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? `この地図はドラッグ・拡大縮小できます。底図は交互地図、順路は既存の実线路データを描画しています。` : `这张图现在可以拖动和缩放，底图可交互，顺路继续使用现有真实线路数据绘制。`}</p>
          <div class="route-map-legend">
            ${mapData.markers.map((item) => `<span class="route-map-chip"><strong>${escapeHtml(item.label)}</strong>${escapeHtml(item.name)}</span>`).join("")}
          </div>
          ${buildRouteStopScheduleTable(trip)}
        </div>
      </div>
    `;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!renderInteractiveRouteMap("routeInteractiveMap", mapData)) {
      body.innerHTML = `
        <div class="route-map-shell">
          <div id="routeInteractiveMapFallback" class="route-map-interactive"></div>
          <div class="route-map-meta">
            <p class="muted">${L("route")}：${buildRouteDisplay(trip.route, result.scenario)}</p>
            <p class="muted">${lang() === "ja" ? `交互底図の読み込みに失敗したため、ローカル交互线路图に切り替えました。` : `交互底图加载失败，已切换到本地交互线路图。`}</p>
            <div class="route-map-legend">
              ${mapData.markers.map((item) => `<span class="route-map-chip"><strong>${escapeHtml(item.label)}</strong>${escapeHtml(item.name)}</span>`).join("")}
            </div>
            ${buildRouteStopScheduleTable(trip)}
          </div>
        </div>
      `;
      renderFallbackRouteMap("routeInteractiveMapFallback", mapData);
    }
  } catch (error) {
    body.innerHTML = `
      <div class="route-map-shell">
        <div id="routeInteractiveMapFallback" class="route-map-interactive"></div>
        <div class="route-map-meta">
          <p class="muted">${L("route")}：${buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? "外部地図の読み込みに失敗したため、ローカルのルート図で表示しています。" : "外部地图加载失败，已改用本地线路图显示。"} ${escapeHtml(error?.message || "")}</p>
          <div class="route-map-legend">
            ${fallbackMapData.markers.map((item) => `<span class="route-map-chip"><strong>${escapeHtml(item.label)}</strong>${escapeHtml(item.name)}</span>`).join("")}
          </div>
          ${buildRouteStopScheduleTable(trip)}
        </div>
      </div>
    `;
    renderFallbackRouteMap("routeInteractiveMapFallback", fallbackMapData);
  }
}

function closeRouteMapModal() {
  destroyRouteLeafletMap();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const modal = document.getElementById("routeMapModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function openShowcaseModal() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const modal = document.getElementById("showcaseModal");
  const box = document.getElementById("showcaseTypewriter");
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  clearProcessTypingTimers();
  typeProcessText(box, buildShowcaseNarrative());
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function closeShowcaseModal() {
  const modal = document.getElementById("showcaseModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  clearProcessTypingTimers();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildAdjustControls(result, waveId, sourcePlate, stopId, plans) {
  const options = plans
    .filter((plan) => plan.vehicle.plateNo !== sourcePlate)
    .map((plan) => `<option value="${plan.vehicle.plateNo}">${plan.vehicle.plateNo}</option>`)
    .join("");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!options) return `<span class="adjust-note">${L("noOtherVehicle")}</span>`;
  return `
    <div class="adjust-box">
      <select class="adjust-target" data-result="${result.key}" data-wave="${waveId}" data-store="${stopId}" data-source="${sourcePlate}">
        <option value="">${L("adjustTo")}</option>
        ${options}
      </select>
      <button class="confirm-adjust apply-adjust" data-result="${result.key}" data-wave="${waveId}" data-store="${stopId}" data-source="${sourcePlate}">${L("confirmAdjust")}</button>
    </div>
  `;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderStopCards(result, wave, plan, trip, plans) {
  const adjustment = result.lastAdjustment;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return trip.stops.map((stop) => `
    <article class="stop-card ${adjustment && adjustment.waveId === wave.waveId && adjustment.targetPlate === plan.vehicle.plateNo && adjustment.storeId === stop.storeId ? "inserted-stop" : ""}">
      <div class="stop-main">
        <div class="stop-title">
          <strong>${stop.storeId}</strong>
          <span>${stop.storeName}</span>
          ${adjustment && adjustment.waveId === wave.waveId && adjustment.targetPlate === plan.vehicle.plateNo && adjustment.storeId === stop.storeId ? `<span class="chip bad">${L("inserted")}</span>` : ""}
        </div>
        <div class="stop-metrics">
          <span class="chip">${L("leg")} ${stop.distance.toFixed(1)} km</span>
          <span class="chip">${L("arrive")} ${formatTime(stop.arrival)}</span>
          <span class="chip">${L("unloadStart")} ${formatTime(stop.start)}</span>
          <span class="chip">${L("leave")} ${formatTime(stop.leave)}</span>
          <span class="chip">${L("desired")} ${stop.desiredArrival}</span>
          <span class="chip">${L("unloadMinutes")} ${formatMinutesValue(stop.serviceMinutes)} ${L("minutes")}</span>
          <span class="chip ${stop.onTime ? "" : "bad"}">${stop.onTime ? L("notLate") : L("late")}</span>
        </div>
      </div>
      <div class="stop-adjust">
        ${buildAdjustControls(result, wave.waveId, plan.vehicle.plateNo, stop.storeId, plans)}
      </div>
    </article>
  `).join("");
}

function escapeHtml(value) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function persistRelayConsoleLines() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    localStorage.setItem("vrptw-live-console", JSON.stringify(relayConsoleLines.slice(-400)));
  } catch {}
}

function flushRelayConsoleLogQueue() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (relayConsoleLogFlushTimer) {
    clearTimeout(relayConsoleLogFlushTimer);
    relayConsoleLogFlushTimer = null;
  }
  if (!relayConsolePendingLogLines.length) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const batch = relayConsolePendingLogLines.splice(0, relayConsolePendingLogLines.length);
  const payloadText = batch.join("\n");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  try {
    fetch(`${GA_BACKEND_URL}/sfrz/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ line: payloadText }),
    }).catch(() => {});
  } catch (_) {}
}

function scheduleRelayConsoleLogFlush() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (relayConsoleLogFlushTimer) return;
  relayConsoleLogFlushTimer = setTimeout(flushRelayConsoleLogQueue, 250);
}

function openRelayConsoleModal(title = "") {
  relayConsoleLines = [];
  relayConsolePendingLogLines = [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (relayConsoleLogFlushTimer) {
    clearTimeout(relayConsoleLogFlushTimer);
    relayConsoleLogFlushTimer = null;
  }
  const modal = document.getElementById("relayConsoleModal");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const body = document.getElementById("relayConsoleBody");
  const titleNode = document.getElementById("relayConsoleTitle");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (body) body.textContent = "";
  if (titleNode) titleNode.textContent = title || (lang() === "ja" ? "リレー最適化の過程" : "接力求解过程");
  persistRelayConsoleLines();
  modal?.classList.remove("hidden");
  modal?.setAttribute("aria-hidden", "false");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function appendRelayConsoleLine(text) {
  relayConsoleLines.push(text);
  relayConsolePendingLogLines.push(String(text || ""));
  persistRelayConsoleLines();
  scheduleRelayConsoleLogFlush();
  const body = document.getElementById("relayConsoleBody");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (body) {
    body.insertAdjacentText("beforeend", `${body.textContent ? "\n" : ""}${text}`);
    body.scrollTop = body.scrollHeight;
  }
}

function reportRelayStageProgress(text) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (typeof relayStageReporter === "function" && text) relayStageReporter(text);
}

function reportStrategyAuditToRelayConsole(strategyAudit, algoName = "算法") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!strategyAudit || typeof strategyAudit !== "object") return;
  const waveId = String(strategyAudit.waveId || "").trim() || "-";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const inputCount = Number(strategyAudit.inputStoreCount || 0);
  const outputCount = Number(strategyAudit.outputStoreCount || 0);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const zeroIds = Array.isArray(strategyAudit.filteredZeroLoadStoreIds)
    ? strategyAudit.filteredZeroLoadStoreIds.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const scopeIds = Array.isArray(strategyAudit.filteredWaveScopeStoreIds)
    ? strategyAudit.filteredWaveScopeStoreIds.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const emitStoreIdChunks = (title, ids) => {
    const list = Array.isArray(ids) ? ids : [];
    reportRelayStageProgress(`策略中心审计（${algoName} ${waveId}）：${title} ${list.length} 家。`);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!list.length) return;
    const chunkSize = 20;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (let i = 0; i < list.length; i += chunkSize) {
      const chunk = list.slice(i, i + chunkSize);
      reportRelayStageProgress(`策略中心审计（${algoName} ${waveId}）：${title}明细 ${i + 1}-${Math.min(i + chunkSize, list.length)}：${chunk.join("、")}。`);
    }
  };
  reportRelayStageProgress(`策略中心审计（${algoName} ${waveId}）：输入门店 ${inputCount} 家，策略后 ${outputCount} 家。`);
  emitStoreIdChunks("过滤零货量", zeroIds);
  emitStoreIdChunks("过滤非本波次", scopeIds);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveId === "W3") {
    reportRelayStageProgress(`策略中心审计（${algoName} ${waveId}）：W3按单程里程规则执行（由后端约束层判定）。`);
  }
}

function reportBackendUnscheduledToRelayConsole(unscheduledStores, algoName = "算法", waveId = "-") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rows = Array.isArray(unscheduledStores) ? unscheduledStores : [];
  if (!rows.length) {
    reportRelayStageProgress(`后端未分配诊断（${algoName} ${waveId}）：0 家。`);
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const groups = new Map();
  for (const row of rows) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const reason = String(row?.reasonText || row?.reason || "未知").trim() || "未知";
    const storeId = String(row?.storeId || "").trim();
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (!groups.has(reason)) groups.set(reason, []);
    if (storeId) groups.get(reason).push(storeId);
  }
  reportRelayStageProgress(`后端未分配诊断（${algoName} ${waveId}）：共 ${rows.length} 家。`);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const [reason, ids] of groups.entries()) {
    reportRelayStageProgress(`后端未分配诊断（${algoName} ${waveId}）：${reason} ${ids.length} 家。`);
    const chunkSize = 20;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      reportRelayStageProgress(`后端未分配诊断（${algoName} ${waveId}）：${reason}明细 ${i + 1}-${Math.min(i + chunkSize, ids.length)}：${chunk.join("、")}。`);
    }
  }
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function closeRelayConsoleModal() {
  flushRelayConsoleLogQueue();
  const modal = document.getElementById("relayConsoleModal");
  modal?.classList.add("hidden");
  modal?.setAttribute("aria-hidden", "true");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getRoutePalette(index) {
  const colors = ["#dc2626", "#2563eb", "#059669", "#d97706", "#7c3aed", "#0891b2", "#be123c", "#65a30d"];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return colors[index % colors.length];
}

function sortPlansByPreference(plans, resultMetrics) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const usageMap = getUsageMap(resultMetrics);
  return [...plans].sort((a, b) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const aUsage = usageMap.get(a.vehicle.plateNo) || { preferredMet: false, achievedRate: 0 };
    const bUsage = usageMap.get(b.vehicle.plateNo) || { preferredMet: false, achievedRate: 0 };
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return Number(bUsage.preferredMet) - Number(aUsage.preferredMet)
      || bUsage.achievedRate - aUsage.achievedRate
      || a.totalDistance - b.totalDistance
      || a.vehicle.plateNo.localeCompare(b.vehicle.plateNo);
  });
}

function renderWaveBlocks(result, selectedWaveIds = null) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const selectedSet = selectedWaveIds instanceof Set ? selectedWaveIds : null;
  return result.solution.map((plans, index) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const wave = result.scenario.waves[index];
    if (!wave) return "";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (selectedSet && selectedSet.size && !selectedSet.has(String(wave.waveId || ""))) return "";
    const showReturnTime = wave.waveId === "W1";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const used = sortPlansByPreference(plans.filter((plan) => plan.trips.length), result.metrics);
    const usageMap = getUsageMap(result.metrics);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const overtimeTrips = used.flatMap((plan) => plan.trips.filter((trip) => (trip.waveLateMinutes || 0) > 0).map((trip) => ({ plateNo: plan.vehicle.plateNo, trip })));
    return `
      <section class="wave-block">
        <h3>${wave.waveId} ${wave.start} - ${wave.end}</h3>
        <p class="muted">${L("dispatchStart")}：${wave.start}，${lang() === "ja" ? "締切ルール" : "截止规则"}：${(wave.endMode || "return") === "return" ? LT("waveModeReturn", { time: wave.end }) : LT("waveModeService", { time: wave.end })}${wave.singleWave ? `，${L("waveSingleHint")}` : `，${L("waveRegularHint")}`}</p>
        <p class="muted">${L("includedStores")}：${wave.storeList.join(",") || L("allStores")}</p>
        ${overtimeTrips.length ? `<div class="note overtime-note">${L("overtimeTrips")}：${overtimeTrips.map(({ plateNo, trip }) => `${plateNo} ${L("tripNo")}${trip.tripNo}${L("tripSuffix")}${L("overWave")} ${(trip.waveLateMinutes || 0).toFixed(0)} ${L("minutes")}`).join("；")}</div>` : ""}
        ${used.map((plan) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const usage = usageMap.get(plan.vehicle.plateNo);
          return `
            <div class="vehicle-card">
              <div class="meta">
                <span class="chip">${plan.vehicle.plateNo}</span>
                <span class="chip">${plan.vehicle.type}</span>
                <span class="chip">${L("storesCount")} ${flattenPlanStoreIds(plan).length}</span>
                <span class="chip">${L("totalLoad")} ${plan.totalLoad}</span>
                <span class="chip">${L("maxTrip")} ${Math.max(...plan.trips.map((trip) => trip.loadBoxes), 0)}/${getVehicleSolveCapacity(plan.vehicle)}</span>
                <span class="chip">${((plan.totalLoad / Math.max(getVehicleSolveCapacity(plan.vehicle), 1)) * 100).toFixed(1)}%</span>
                <span class="chip">${L("trips")} ${plan.tripCount}</span>
                <span class="chip">${L("totalRoundKm")} ${plan.totalDistance.toFixed(1)} km</span>
                <span class="chip ${usage && !usage.preferredMet ? "bad" : ""}">${usage?.preferredMet ? L("loadPreferredMet") : L("loadPreferredMiss")}</span>
              </div>
              ${plan.trips.map((trip) => `
                <div class="trip-block">
                  ${(() => {
                    // EN: Key step in this business flow.
                    // CN: 当前业务流程中的关键步骤。
                    const truck = getTripTruckVisual(trip);
                    return `<div class="trip-hero">
                    <p class="route">${L("tripNo")} ${trip.tripNo} ${L("tripSuffix")}：${buildRouteDisplay(trip.route, result.scenario)}</p>
                      <div class="trip-truck-row">
                        <div class="trip-truck trip-truck-${truck.cls}">
                          <img src="${truck.src}" alt="${escapeHtml(`${plan.vehicle.plateNo} ${truck.badge}`)}" onerror="this.style.display='none';this.parentElement.classList.add('trip-truck-fallback')">
                          <span class="trip-truck-badge">${truck.badge}</span>
                          <span class="trip-truck-fallback-text">${truck.badge}</span>
                        </div>
                      </div>
                      <p class="trip-actions"><button class="alert open-process" data-result="${result.key}" data-wave="${wave.waveId}" data-plate="${plan.vehicle.plateNo}" data-trip="${trip.tripNo}">${L("viewViz")}</button><button class="secondary open-route-map" data-result="${result.key}" data-wave="${wave.waveId}" data-plate="${plan.vehicle.plateNo}" data-trip="${trip.tripNo}">${L("routeMap")}</button></p>
                      <p class="trip-footer muted">${showReturnTime ? `${L("returnTime")}：${formatTime(trip.finish)}，` : ""}${L("backDistance")}：${trip.stops.length ? result.scenario.dist[trip.stops[trip.stops.length - 1].storeId][DC.id].toFixed(1) : "0.0"} km，${L("tripRoundKm")}：${trip.totalDistance.toFixed(1)} km，${L("tripLoadRate")}：${formatRate(trip.loadRate)}${(trip.waveLateMinutes || 0) > 0 ? `，<span class="status-bad">${L("overWave")} ${(trip.waveLateMinutes || 0).toFixed(0)} ${L("minutes")}</span>` : ""}</p>
                    </div>`;
                  })()}
                  <div class="stop-list">
                    ${renderStopCards(result, wave, plan, trip, plans)}
                  </div>
                </div>
              `).join("")}
            </div>
          `;
        }).join("")}
      </section>
    `;
  }).join("");
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderResults() {
  const box = document.getElementById("resultPanels");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!state.lastResults.length) { box.innerHTML = ""; return; }
  const activeKey = state.activeResultKey || state.lastResults[0]?.key;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const activeResult = state.lastResults.find((item) => item.key === activeKey) || state.lastResults[0];
  const detailExpanded = !!state.ui.resultDetailExpanded;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveIds = (activeResult?.scenario?.waves || []).map((wave) => String(wave.waveId || "")).filter(Boolean);
  const selectedWaveIds = new Set((state.ui.resultDetailWaveIds || []).map((id) => String(id || "")).filter((id) => waveIds.includes(id)));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.resultDetailWaveIds = [...selectedWaveIds];
  const allWaveSelected = waveIds.length > 0 && waveIds.every((id) => selectedWaveIds.has(id));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  document.getElementById("resultIntro").textContent = `${state.lastResults.length > 1 ? L("generatedCompare") : L("generatedSingle")} ${buildSolveModeSummary()}`;
  box.innerHTML = `
    <div class="result-switcher">
      ${state.lastResults.map((result) => `<button class="${result.key === activeResult.key ? "primary" : "secondary"} view-result-detail" data-result-key="${result.key}">${algoLabel(result.key)} ${L("detail")}</button>`).join("")}
    </div>
    ${activeResult ? `
    <article class="panel algorithm-panel">
      <div class="panel-head result-panel-head">
        <div>
          <h2>${algoLabel(activeResult.key)}</h2>
          <p>${algoDescription(activeResult.key)}</p>
        </div>
        <button class="${detailExpanded ? "secondary" : "primary"} toggle-result-detail" data-result-key="${activeResult.key}">
          ${lang() === "ja" ? (detailExpanded ? "詳細を閉じる" : "詳細を開く") : (detailExpanded ? "收起明细" : "展开明细")}
        </button>
      </div>
      <div class="algo-summary"><span class="chip">${L("score")} ${activeResult.metrics.score.toFixed(1)}${lang() === "ja" ? " / 100（高いほど良い）" : " / 100（越高越好）"}</span><span class="chip">${L("onTime")} ${activeResult.metrics.totalOnTime}/${activeResult.metrics.totalStops}</span><span class="chip">${L("totalDistance")} ${activeResult.metrics.totalDistance.toFixed(1)} km</span><span class="chip">${L("avgLoad")} ${formatRate(activeResult.metrics.loadRate)}</span><span class="chip">${L("fleetLoad")} ${formatRate(activeResult.metrics.fleetLoadRate)}</span><span class="chip">${lang() === "ja" ? `使用車両 ${activeResult.metrics.usedVehicleCount} 台` : `已用车辆 ${activeResult.metrics.usedVehicleCount} 辆`}</span><span class="chip">${lang() === "ja" ? `待機車両 ${activeResult.metrics.unusedVehicleCount} 台` : `未用车辆 ${activeResult.metrics.unusedVehicleCount} 辆`}</span><span class="chip">${lang() === "ja" ? `モード ${currentStrategyLabel()}` : `模式 ${currentStrategyLabel()}`}</span><span class="chip">${lang() === "ja" ? `方針 ${currentGoalLabel()}` : `目标 ${currentGoalLabel()}`}</span></div>
      <div class="result-wave-filter">
        <span class="muted">${lang() === "ja" ? "波次選択" : "波次选择"}</span>
        <button class="${allWaveSelected ? "primary" : "secondary"} toggle-result-wave-filter" data-wave-filter-mode="all">${lang() === "ja" ? "全部" : "全部"}</button>
        ${waveIds.map((waveId) => `<button class="${selectedWaveIds.has(waveId) ? "primary" : "secondary"} toggle-result-wave-filter" data-wave-id="${escapeHtml(waveId)}">${escapeHtml(waveId)}</button>`).join("")}
      </div>
      ${detailExpanded ? `
      <div class="result-detail-body">
        <p class="note">${lang() === "ja" ? `スコアは 100 点満点です。現在は「${currentGoalLabel()}」重視で並べつつ、定時性・距離・車両数・積載も合わせて評価しています。` : `评分说明：100分制。当前按“${currentGoalLabel()}”权重排序，同时综合参考准点、里程、车辆使用和装载表现。`}</p>
        ${activeResult.adjustMessage ? `<p class="note">${activeResult.adjustMessage}</p>` : ""}
        ${selectedWaveIds.size ? renderWaveBlocks(activeResult, selectedWaveIds) : `<div class="result-detail-collapsed">${lang() === "ja" ? "先に波次を選択してから詳細を開いてください。" : "请先选择要查看的波次，再展开明细。"}</div>`}
        <div class="result-detail-actions">
          <button class="secondary toggle-result-detail" data-result-key="${activeResult.key}">
            ${lang() === "ja" ? "詳細を閉じる" : "收起明细"}
          </button>
        </div>
      </div>
      ` : `
      <div class="result-detail-collapsed">
        ${lang() === "ja" ? "詳細は折りたたまれています。必要なときだけ開いて確認できます。" : "明细已收起。需要定位问题时再展开，这样页面更短，方便截图。"}
      </div>
      `}
    </article>
    ` : ""}
  `;
}

function sleep(ms) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function updateGenerationProgress(progress, text) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.generating = progress < 100;
  state.ui.progress = progress;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.progressText = text;
  renderGenerationProgress();
  await sleep(30);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function solveTwoVehicleTransfer(scenario, wave, sourceVehicle, targetVehicle, sourceIds, targetIds, movedStoreId) {
  const nextSourceIds = sourceIds.filter((id) => id !== movedStoreId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nextTargetIds = [...targetIds, movedStoreId];
  const sourcePlan = solveFixedMembershipPlan(sourceVehicle, nextSourceIds, scenario, wave, sourceVehicle);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const targetPlan = solveFixedMembershipPlan(targetVehicle, nextTargetIds, scenario, wave, targetVehicle);
  if (!sourcePlan || !targetPlan) return null;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return [sourcePlan, targetPlan];
}

function applyStoreTransfer(resultKey, waveId, sourcePlate, targetPlate, storeId) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const result = state.lastResults.find((item) => item.key === resultKey);
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!result) return;
  if (!targetPlate || targetPlate === sourcePlate) {
    result.adjustMessage = L("chooseDifferentVehicle");
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
  if (waveIndex < 0) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const nextSolution = clone(result.solution);
  const plans = nextSolution[waveIndex];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const sourceIndex = plans.findIndex((plan) => plan.vehicle.plateNo === sourcePlate);
  const targetIndex = plans.findIndex((plan) => plan.vehicle.plateNo === targetPlate);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (sourceIndex < 0 || targetIndex < 0) return;

  const sourceIds = flattenPlanStoreIds(plans[sourceIndex]);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const targetIds = flattenPlanStoreIds(plans[targetIndex]);
  if (!sourceIds.includes(storeId)) {
    result.adjustMessage = LT("storeNotOnVehicle", { plate: sourcePlate, store: storeId });
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }

  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const rebuilt = solveTwoVehicleTransfer(
    result.scenario,
    result.scenario.waves[waveIndex],
    { ...plans[sourceIndex].vehicle, priorRegularDistance: plans[sourceIndex].priorRegularDistance, priorWaveCount: plans[sourceIndex].priorWaveCount },
    { ...plans[targetIndex].vehicle, priorRegularDistance: plans[targetIndex].priorRegularDistance, priorWaveCount: plans[targetIndex].priorWaveCount },
    sourceIds,
    targetIds,
    storeId,
  );

  if (!rebuilt) {
    result.adjustMessage = LT("transferFailed", { store: storeId, source: sourcePlate, target: targetPlate });
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }

  plans[sourceIndex] = rebuilt[0];
  plans[targetIndex] = rebuilt[1];
  result.solution = nextSolution;
  applyFinalRuleToResult(result, result.scenario);
  result.lastAdjustment = { waveId, sourcePlate, targetPlate, storeId };
  result.adjustMessage = LT("transferSuccess", { store: storeId, source: sourcePlate, target: targetPlate });
  box.textContent = result.adjustMessage;
  renderVehicles();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderStoresTable();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getResultByKey(resultKey) {
  return state.lastResults.find((item) => item.key === resultKey) || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getWaveAndPlans(result, waveId) {
  const waveIndex = result?.scenario?.waves?.findIndex((wave) => wave.waveId === waveId) ?? -1;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (waveIndex < 0) return null;
  return {
    waveIndex,
    wave: result.scenario.waves[waveIndex],
    plans: result.solution[waveIndex],
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function syncResultAfterLocalAdjustment(result) {
  applyFinalRuleToResult(result, result.scenario);
  reportWaveCandidateAssignedPendingStats("局部调整后", result.solution || [], result.scenario);
}

function getLiveUnscheduledItems(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!result?.scenario) return [];
  return computeFinalPendingByWave(result.solution || [], result.scenario);
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getStoreAssignmentCoverage(store = {}, assignmentMap = new Map()) {
  const variants = buildStoreKeyVariants(store?.id);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const candidateWaves = ["W1", "W2", "W3", "W4"].filter((waveId) => isStoreCandidateForWaveRule(store, waveId));
  const assignedByWave = new Map();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  for (const waveId of candidateWaves) {
    let plateNo = "";
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    for (const variant of variants) {
      const key = buildStoreWaveAssignmentKey(variant, waveId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const hit = key ? assignmentMap.get(key) : null;
      if (hit && String(hit.plateNo || "").trim()) {
        plateNo = String(hit.plateNo || "").trim();
        break;
      }
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (plateNo) assignedByWave.set(waveId, plateNo);
  }
  const assignedWaves = [...assignedByWave.keys()];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const pendingWaves = candidateWaves.filter((waveId) => !assignedByWave.has(waveId));
  const isFullyAssigned = candidateWaves.length > 0 && pendingWaves.length === 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let displayPlate = "-";
  if (isFullyAssigned && assignedWaves.length) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const uniquePlates = new Set([...assignedByWave.values()]);
    if (uniquePlates.size === 1) {
      displayPlate = [...uniquePlates][0];
    } else {
      displayPlate = assignedWaves.map((waveId) => `${waveId}:${assignedByWave.get(waveId)}`).join(" / ");
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    candidateWaves,
    assignedWaves,
    pendingWaves,
    assignedByWave,
    isFullyAssigned,
    hasAnyAssigned: assignedWaves.length > 0,
    displayPlate,
  };
}

function computeStoreDetailUnscheduledCount(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!result?.solution?.length) return 0;
  const stores = Array.isArray(state.stores) ? state.stores : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const assignmentMap = result?.storeAssignmentMap instanceof Map
    ? result.storeAssignmentMap
    : buildStoreAssignmentMapFromSolution(result.solution || []);
  let count = 0;
  stores.forEach((store) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const hasAnyResolvedLoad = (
      Number(store?.resolvedWave1Load || 0) > 0
      || Number(store?.resolvedWave2Load || 0) > 0
      || Number(store?.resolvedWave3Load || 0) > 0
      || Number(store?.resolvedWave4Load || 0) > 0
    );
    if (!hasAnyResolvedLoad) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const coverage = getStoreAssignmentCoverage(store, assignmentMap);
    if (coverage.candidateWaves.length && !coverage.isFullyAssigned) count += 1;
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return count;
}

function exportLiveUnscheduledIds(resultKey = "") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const result = getResultByKey(resultKey) || state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!result) {
    if (box) box.textContent = L("exportNoResult");
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const items = getLiveUnscheduledItems(result);
  const stats = computeWaveCandidateAssignedPendingStats(result.solution || [], result.scenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lines = [];
  lines.push(`算法: ${result.label || result.key || "-"}`);
  lines.push(`导出时间: ${new Date().toLocaleString("zh-CN", { hour12: false })}`);
  lines.push(`未调度店-波次条数: ${items.length}`);
  lines.push(`未调度门店去重数: ${new Set(items.map((x) => normalizeStoreKey(x.storeId))).size}`);
  lines.push(`明细筛选未调度门店数: ${computeStoreDetailUnscheduledCount(result)}`);
  lines.push("");
  lines.push("=== 实时集合核对 ===");
  stats.forEach((row) => {
    lines.push(`${row.waveId}: candidate=${row.candidateCount}, assigned=${row.assignedCount}, pending=${row.pendingCount}`);
  });
  lines.push("");
  lines.push("=== 未调度店-波次明细 ===");
  items.forEach((item) => {
    lines.push(`${item.waveId},${normalizeStoreKey(item.storeId)},${String(item.storeName || "")}`);
  });
  const stamp = new Date();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  const fileName = `未调度实时集合_${result.key || "result"}_${timestamp}.txt`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const blob = new Blob([`\uFEFF${lines.join("\r\n")}`], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (box) box.textContent = `${L("exportDone")} ${fileName}`;
}

function collectCandidatePlansForUnscheduled(plans, preferredPlate = "") {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const sorted = [...plans].sort((a, b) => {
    const aEmpty = a.trips.length ? 1 : 0;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const bEmpty = b.trips.length ? 1 : 0;
    if (aEmpty !== bEmpty) return aEmpty - bEmpty;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (preferredPlate) {
      const aPreferred = a.vehicle.plateNo === preferredPlate ? 0 : 1;
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const bPreferred = b.vehicle.plateNo === preferredPlate ? 0 : 1;
      if (aPreferred !== bPreferred) return aPreferred - bPreferred;
    }
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return a.totalDistance - b.totalDistance || a.vehicle.plateNo.localeCompare(b.vehicle.plateNo);
  });
  const emptyOnly = sorted.filter((plan) => !plan.trips.length);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return emptyOnly.length ? emptyOnly : sorted;
}

function tryAssignStoreToSpecificPlan(result, waveId, targetPlate, storeId) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = getWaveAndPlans(result, waveId);
  if (!target) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const store = result.scenario.storeMap.get(storeId);
  if (!store) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const targetPlanIndex = target.plans.findIndex((plan) => plan.vehicle.plateNo === targetPlate);
  if (targetPlanIndex < 0) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const candidate = buildTripCandidate(target.plans[targetPlanIndex], store, result.scenario, target.wave, false, { allowToleranceBreak: false });
  if (!candidate?.nextPlan) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  target.plans[targetPlanIndex] = candidate.nextPlan;
  syncResultAfterLocalAdjustment(result);
  result.adjustMessage = LT("assignSuccess", { store: store.name || storeId, plate: targetPlate });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return { ok: true, message: result.adjustMessage };
}

function rescheduleUnscheduledStores(resultKey) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const result = getResultByKey(resultKey);
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!result) return;
  applyFinalRuleToResult(result, result.scenario);
  const pending = [...getLiveUnscheduledItems(result)];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!pending.length) {
    result.adjustMessage = L("noUnscheduled");
    box.textContent = result.adjustMessage;
    renderSummary();
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }
  let scheduledNow = 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let round = 0;
  while (round < 6) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const latestPending = computeFinalPendingByWave(result.solution || [], result.scenario);
    if (!latestPending.length) break;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    let roundScheduled = 0;
    for (const item of latestPending) {
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      const target = getWaveAndPlans(result, item.waveId);
      const store = result.scenario.storeMap.get(item.storeId);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      if (!target || !store) continue;
      const candidatePlans = collectCandidatePlansForUnscheduled(target.plans);
      // EN: Key step in this business flow.
      // CN: 当前业务流程中的关键步骤。
      for (const plan of candidatePlans) {
        const candidate = buildTripCandidate(plan, store, result.scenario, target.wave, false, { allowToleranceBreak: false });
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (!candidate?.nextPlan) continue;
        const planIndex = target.plans.findIndex((one) => one.vehicle.plateNo === plan.vehicle.plateNo);
        // EN: Key step in this business flow.
        // CN: 当前业务流程中的关键步骤。
        if (planIndex < 0) continue;
        target.plans[planIndex] = candidate.nextPlan;
        scheduledNow += 1;
        roundScheduled += 1;
        break;
      }
    }
    applyFinalRuleToResult(result, result.scenario);
    reportWaveCandidateAssignedPendingStats(`补排第${round + 1}轮后`, result.solution || [], result.scenario);
    if (roundScheduled <= 0) break;
    round += 1;
  }
  syncResultAfterLocalAdjustment(result);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const remainingCount = Number(result?.metrics?.unscheduledCount || 0);
  result.adjustMessage = scheduledNow > 0
    ? `${LT("rescheduleProgress", { count: scheduledNow })}，${lang() === "ja" ? `未割当残り ${remainingCount} 件` : `剩余未调度 ${remainingCount} 家`}`
    : `${L("rescheduleNoProgress")}，${lang() === "ja" ? `今回も ${pending.length} 件とも条件に合いませんでした。` : `本轮 ${pending.length} 家都未满足条件。`}`;
  box.textContent = result.adjustMessage;
  renderVehicles();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderStoresTable();
}

function buildUnscheduledAssignControls(result, item) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = getWaveAndPlans(result, item.waveId);
  if (!target) return `<span class="adjust-note">${L("noAssignableVehicle")}</span>`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const options = collectCandidatePlansForUnscheduled(target.plans)
    .map((plan) => `<option value="${plan.vehicle.plateNo}">${plan.vehicle.plateNo}</option>`)
    .join("");
  if (!options) return `<span class="adjust-note">${L("noAssignableVehicle")}</span>`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `
    <div class="adjust-box unscheduled-adjust-box">
      <select class="unscheduled-target" data-result="${result.key}" data-wave="${item.waveId}" data-store="${item.storeId}">
        <option value="">${L("manualAssignPlaceholder")}</option>
        ${options}
      </select>
      <button class="confirm-adjust apply-unscheduled-assign" data-result="${result.key}" data-wave="${item.waveId}" data-store="${item.storeId}">${L("confirmAssign")}</button>
    </div>
  `;
}

function getEarliestDirectArrivalInfo(result, item) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const target = getWaveAndPlans(result, item.waveId);
  const store = result?.scenario?.storeMap?.get(item.storeId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!target || !store) return null;
  const plans = Array.isArray(target?.plans) ? target.plans : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseDeparture = Math.max(
    Number(target.wave.startMin || 0),
    Number(target.wave.earliestDepartureMin || target.wave.startMin || 0),
    Number(result.scenario?.dispatchStartMin || 0)
  );
  let best = null;
  plans.forEach((plan) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const vehicle = plan?.vehicle;
    if (!vehicle) return;
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const departure = Math.max(baseDeparture, Number(plan.availableTime || 0), Number(plan.earliestDepartureMin || 0));
    const distance = Number(result.scenario?.dist?.[DC.id]?.[store.id] || 0);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const arrival = departure + getTravelMinutes(result.scenario, DC.id, store.id, Number(vehicle.speed || 38));
    if (!best || arrival < best.arrival) {
      best = {
        earliestArrival: arrival,
        distance,
        plateNo: vehicle.plateNo,
      };
    }
  });
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!best) return null;
  const timing = getStoreTimingForWave(store, target.wave, result.scenario.dispatchStartMin);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const desiredArrivalMin = Number(timing.desiredArrivalMin || best.earliestArrival);
  const latestAllowed = Number(timing.latestAllowedArrivalMin || desiredArrivalMin || best.earliestArrival);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const lateBy = Math.max(0, best.earliestArrival - latestAllowed);
  const lateByDisplay = lateBy > 0 ? Math.max(1, Math.ceil(lateBy)) : 0;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    ...best,
    desiredArrivalMin,
    latestAllowedArrivalMin: latestAllowed,
    allowedLateMinutes: Number(timing.allowedLateMinutes || 0),
    lateBy,
    lateByDisplay,
  };
}

function renderUnscheduledPanel(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const items = getLiveUnscheduledItems(result);
  if (!items.length) return "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const stats = computeWaveCandidateAssignedPendingStats(result.solution || [], result.scenario);
  const statsLine = stats.map((row) => `${row.waveId} c/a/p=${row.candidateCount}/${row.assignedCount}/${row.pendingCount}`).join(" | ");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const panelUniqueCount = new Set(items.map((item) => normalizeStoreKey(item.storeId))).size;
  const detailFilteredCount = computeStoreDetailUnscheduledCount(result);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const mismatch = panelUniqueCount !== detailFilteredCount;
  return `
    <section class="unscheduled-panel">
      <div class="unscheduled-head">
        <div>
          <h3>${L("rescheduleSection")}</h3>
          <p class="muted">${L("rescheduleHint")}</p>
          <p class="note">${escapeHtml(statsLine)}</p>
          ${mismatch ? `<p class="note" style="color:#b91c1c;font-weight:700;">${LT("unscheduledMismatch", { panel: panelUniqueCount, detail: detailFilteredCount })}</p>` : ""}
        </div>
        <div class="unscheduled-actions">
          <button class="secondary export-live-unscheduled" data-result="${result.key}">${L("exportLiveUnscheduled")}</button>
          <button class="secondary save-current-result" data-result="${result.key}">${L("saveScheduledResult")}</button>
          <button class="primary reschedule-again" data-result="${result.key}">${L("rescheduleAgain")}</button>
        </div>
      </div>
      <p class="note">${lang() === "ja" ? `現在未割当 店舗 ${panelUniqueCount} 件（店-波次 ${items.length} 件）。まず空車を優先し、1 店 1 車で補配を試します。` : `当前未调度门店 ${panelUniqueCount} 家（店-波次 ${items.length} 条）。系统会优先空闲车辆，先按一店一车尝试补调。`}</p>
      ${result.adjustMessage ? `<p class="note">${result.adjustMessage}</p>` : ""}
      <div class="unscheduled-list">
        ${items.map((item) => {
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          const timing = getEarliestDirectArrivalInfo(result, item);
          const timingText = timing
            ? (lang() === "ja"
              ? `最早直行到着 ${formatTime(timing.earliestArrival)} / 期望 ${formatTime(timing.desiredArrivalMin)} / 最終許容 ${formatTime(timing.latestAllowedArrivalMin)}（+${Number(timing.allowedLateMinutes || 0)}）/ 要求より ${Number(timing.lateByDisplay || 0)} 分遅れ`
              : `最早直达可到 ${formatTime(timing.earliestArrival)} / 期望 ${formatTime(timing.desiredArrivalMin)} / 最晚允许 ${formatTime(timing.latestAllowedArrivalMin)}（+${Number(timing.allowedLateMinutes || 0)}）/ 比要求晚 ${Number(timing.lateByDisplay || 0)} 分钟`)
            : "";
          // EN: Key step in this business flow.
          // CN: 当前业务流程中的关键步骤。
          return `
          <article class="unscheduled-card">
            <div class="unscheduled-meta">
              <div class="stop-title">
                <strong>${item.storeId}</strong>
                <span>${item.storeName}</span>
                <span class="chip">${item.waveId}</span>
              </div>
              <div class="stop-metrics">
                <span class="chip">${L("desired")} ${item.desiredArrival || "-"}</span>
                <span class="chip bad">${getDisplayUnscheduledReason(item)}</span>  
                ${timingText ? `<span class="chip warn">${timingText}</span>` : ""}
              </div>
            </div>
            <div class="unscheduled-assign">
              ${buildUnscheduledAssignControls(result, item)}
            </div>
          </article>
        `;
        }).join("")}
      </div>
    </section>
  `;
}

function renderStoreReschedulePanel() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const mount = document.getElementById("storeReschedulePanel");
  if (!mount) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const activeKey = state.activeResultKey || state.lastResults[0]?.key;
  const activeResult = state.lastResults.find((item) => item.key === activeKey) || state.lastResults[0];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (activeResult) applyFinalRuleToResult(activeResult, activeResult.scenario);
  mount.innerHTML = activeResult ? renderUnscheduledPanel(activeResult) : "";
}

function loadSavedPlans() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); } catch { return []; } }
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function loadRunArchives() { try { return JSON.parse(localStorage.getItem(RUN_ARCHIVE_KEY) || "[]"); } catch { return []; } }
function archiveStrategyLabel(key) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return ({
    quick: "快速初排",
    deep: "继续优化",
    relay: "接力求解",
    compare: "多方案对比",
    global: "全局搜索",
    manual: "手工模式",
  })[key] || key;
}
function archiveGoalLabel(key) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return ({
    balanced: "综合平衡",
    ontime: "最准时",
    vehicles: "少用车",
    distance: "省里程",
    load: "重装载",
  })[key] || key;
}
function snapshotResultForArchive(result) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return {
    key: result.key,
    label: result.label,
    description: result.description,
    solution: result.solution,
    metrics: result.metrics,
    traceLog: (result.traceLog || []).slice(-120),
    unscheduledStores: result.unscheduledStores || [],
    adjustMessage: result.adjustMessage || "",
  };
}
function snapshotRunArchive() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!state.lastResults.length) return null;
  return {
    id: `run-${Date.now()}`,
    createdAt: new Date().toLocaleString("zh-CN"),
    strategy: state.settings.solveStrategy || "quick",
    goal: state.settings.optimizeGoal || "balanced",
    activeResultKey: state.activeResultKey || state.lastResults[0]?.key || "",
    bestScore: Number(state.lastResults[0]?.metrics?.score || 0).toFixed(1),
    resultCount: state.lastResults.length,
    algorithms: state.lastResults.map((x) => x.label).join(" / "),
    stores: JSON.parse(JSON.stringify(state.stores)),
    vehicles: JSON.parse(JSON.stringify(state.vehicles)),
    waves: JSON.parse(JSON.stringify(state.waves)),
    settings: JSON.parse(JSON.stringify(state.settings)),
    results: state.lastResults.map(snapshotResultForArchive),
  };
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function autoArchiveCurrentRun() {
  const snapshot = snapshotRunArchive();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!snapshot) return;
  void saveRunArchiveToBackend(snapshot);
  const archives = loadRunArchives().filter((item) => item.id !== snapshot.id);
  archives.unshift(snapshot);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  let limit = Math.min(18, archives.length);
  while (limit >= 1) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    try {
      localStorage.setItem(RUN_ARCHIVE_KEY, JSON.stringify(archives.slice(0, limit)));
      return;
    } catch (error) {
      limit -= 1;
      if (limit < 1) {
        console.warn("run archive save failed", error);
      }
    }
  }
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function renderSavedPlans() {
  const mount = document.getElementById("savedPlans");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!mount) return;
  if (!state.ui.archiveDateFilter) state.ui.archiveDateFilter = todayDateKey();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dateFilter = state.ui.archiveDateFilter;
  const pageSize = 6;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const requestedPage = Math.max(1, state.ui.archivePage || 1);
  const shouldRefreshRemote = !archiveBackendCache.loading
    && (archiveBackendCache.date !== dateFilter || archiveBackendCache.page !== requestedPage);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (shouldRefreshRemote) {
    void refreshArchiveBackendCache(dateFilter, requestedPage, pageSize);
  }
  const localArchives = loadRunArchives().filter((item) => extractDateKey(item.createdAt) === dateFilter);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const useRemote = archiveBackendCache.date === dateFilter
    && archiveBackendCache.page === requestedPage
    && Array.isArray(archiveBackendCache.items);
  const archives = useRemote ? archiveBackendCache.items : localArchives;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const totalPages = useRemote
    ? Math.max(1, Number(archiveBackendCache.totalPages || 1))
    : Math.max(1, Math.ceil(archives.length / pageSize));
  state.ui.archivePage = Math.min(Math.max(1, state.ui.archivePage || 1), totalPages);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const start = (state.ui.archivePage - 1) * pageSize;
  const pageItems = useRemote ? archives : archives.slice(start, start + pageSize);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const saved = loadSavedPlans().filter((item) => extractDateKey(item.createdAt) === dateFilter);
  mount.innerHTML = `
    <section class="archive-shell">
      <div class="archive-toolbar">
        <div>
          <p class="archive-eyebrow">Solve Archive</p>
          <h3 class="archive-title">求解档案</h3>
        </div>
        <div class="archive-pager-wrap">
          <label class="archive-date-filter">
            <span>${lang() === "ja" ? "日付" : "日期"}</span>
            <input id="archiveDateFilterInput" type="date" value="${escapeHtml(dateFilter)}">
          </label>
          <div class="archive-pager">
          <button class="secondary archive-page-btn" data-archive-page="${Math.max(1, state.ui.archivePage - 1)}" ${state.ui.archivePage <= 1 ? "disabled" : ""}>上一页</button>
          <span class="archive-page-indicator">第 ${state.ui.archivePage} / ${totalPages} 页</span>
          <button class="secondary archive-page-btn" data-archive-page="${Math.min(totalPages, state.ui.archivePage + 1)}" ${state.ui.archivePage >= totalPages ? "disabled" : ""}>下一页</button>
          </div>
        </div>
      </div>
      ${archives.length ? `<div class="table-wrap">
        <table class="archive-table">
          <thead>
            <tr>
              <th>轮次</th>
              <th>${L("savedAt")}</th>
              <th>求解方式</th>
              <th>方案目标</th>
              <th>最佳分</th>
              <th>最佳方案</th>
              <th>算法链</th>
              <th>结果数</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${pageItems.map((item, index) => {
              const results = Array.isArray(item?.results) ? item.results : [];
              // EN: Key step in this business flow.
              // CN: 当前业务流程中的关键步骤。
              const bestResult = results.slice().sort((a, b) => Number(b?.metrics?.score || 0) - Number(a?.metrics?.score || 0))[0];
              return `
              <tr class="${state.ui.archiveCurrentId === item.id ? "archive-row-current" : ""}">
                <td>${start + index + 1}</td>
                <td>${item.createdAt}</td>
                <td>${archiveStrategyLabel(item.strategy)}</td>
                <td>${archiveGoalLabel(item.goal)}</td>
                <td><strong>${item.bestScore}</strong></td>
                <td>${bestResult?.label || "-"}</td>
                <td class="archive-algo-cell">${item.algorithms}</td>
                <td>${item.resultCount}</td>
                <td>
                  <div class="archive-action-group">
                    <button class="secondary preview-archive-btn" data-archive-id="${item.id}">查看</button>
                    <button class="primary adopt-archive-btn" data-archive-id="${item.id}">采用</button>
                  </div>
                </td>
              </tr>`;
            }).join("")}
          </tbody>
        </table>
      </div>` : `<div class="note">${lang() === "ja" ? "この日に該当する求解档案はまだありません。" : "这一天还没有求解档案。"}</div>`}
      ${saved.length ? `
        <div class="archive-legacy">
          <p class="archive-legacy-title">手工快照</p>
          <div class="saved-list">${saved.map((item) => `<article class="adjust-item"><strong>${item.algorithms}</strong><p>${L("savedAt")}：${item.createdAt}</p><p>${L("wavesLabel")}：${item.waves}</p></article>`).join("")}</div>
        </div>` : (!archives.length ? `<div class="note">${lang() === "ja" ? "この日に該当する手動快照もありません。" : "这一天也没有手工快照。"}</div>` : "")}
    </section>
  `;
}
// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function restoreArchivedRun(archiveId, mode = "preview") {
  let archive = await getRunArchiveFromBackend(archiveId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!archive) {
    archive = loadRunArchives().find((item) => item.id === archiveId);
  }
  if (!archive) return;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.stores = JSON.parse(JSON.stringify(archive.stores || []));
  state.vehicles = JSON.parse(JSON.stringify(archive.vehicles || []));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.waves = JSON.parse(JSON.stringify(archive.waves || []));
  state.settings = { ...state.settings, ...(archive.settings || {}) };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const baseScenario = await buildScenario();
  const selectedScenario = applySolveWaveSelectionToScenario(baseScenario);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (selectedScenario.error || !selectedScenario.scenario) {
    box.textContent = selectedScenario.error || (lang() === "ja" ? "波次筛选失败。" : "波次筛选失败。");
    return;
  }
  const scenario = selectedScenario.scenario;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.lastResults = (archive.results || []).map((result) => ({
    ...result,
    scenario,
    storeAssignmentMap: buildStoreAssignmentMapFromSolution(result.solution || []),
  }));
  state.activeResultKey = archive.activeResultKey || state.lastResults[0]?.key || "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.archiveCurrentId = archive.id;
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (box) {
    box.textContent = mode === "adopt"
      ? `已采用 ${archive.createdAt} 这轮求解档案。上方调度结果区已经切到这一轮，可以继续汇报、导出，或在此基础上继续优化。`
      : `正在查看 ${archive.createdAt} 这轮求解档案。上方调度结果区已经切到这一轮；如果老板认可，再点“采用”即可。`;
  }
  renderAll();
}
function saveCurrentPlan() { if (!state.lastResults.length) return; const saved = loadSavedPlans(); saved.unshift({ id: `plan-${Date.now()}`, createdAt: new Date().toLocaleString("zh-CN"), algorithms: state.lastResults.map((x) => x.label).join(" vs "), waves: state.settings.ignoreWaves ? "忽略波次（全天统一调度）" : state.waves.map((w) => `${w.waveId}(${w.start}-${w.end})`).join("，") }); localStorage.setItem(STORAGE_KEY, JSON.stringify(saved.slice(0, 12))); renderSavedPlans(); }

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function todayDateKey() {
  const now = new Date();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function extractDateKey(value) {
  const text = String(value || "");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const match = text.match(/(\d{4})[\/\-年](\d{1,2})[\/\-月](\d{1,2})/);
  if (!match) return "";
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return `${match[1]}-${String(match[2]).padStart(2, "0")}-${String(match[3]).padStart(2, "0")}`;
}

function getWaveSolverAlgorithmOptions() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return ["vrptw", "hybrid", "ga", "tabu", "lns", "savings", "sa", "aco", "pso", "vehicle"];
}

function getConfiguredWaveOptions() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (state.settings.ignoreWaves) {
    return [{
      value: "ALL",
      label: lang() === "ja" ? "ALL ・ 全店舗統合" : "ALL · 全部门店统一调度",
    }];
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const options = (state.waves || []).map((wave) => ({
    value: wave.waveId,
    label: `${wave.waveId} · ${wave.start}-${wave.end}`,
  }));
  const stores = enrichStores(state.stores || []);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (stores.length) {
    const dist = buildDistanceTable(stores);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const singleWaveIds = getSingleWaveStoreIds(stores, dist, Number(state.settings.singleWaveDistanceKm || 70));
    if (singleWaveIds.length) {
      options.push({
        value: "单波次",
        label: lang() === "ja"
          ? `単独波次 · ${state.settings.singleWaveStart || "19:10"}-${state.settings.singleWaveEnd || "05:30"}`
          : `单波次 · ${state.settings.singleWaveStart || "19:10"}-${state.settings.singleWaveEnd || "05:30"}`,
      });
    }
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return options;
}

function ensureWaveSolverPanel() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const panel = document.getElementById("waveSolverPanel");
  if (panel) panel.remove();
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return null;
}

function normalizeSolveWaveSelection(selectedIds = [], options = getConfiguredWaveOptions()) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const optionIds = new Set((options || []).map((item) => String(item.value || "").trim()).filter(Boolean).filter((id) => id !== "ALL"));
  const raw = Array.isArray(selectedIds) ? selectedIds.map((id) => String(id || "").trim()).filter(Boolean) : [];
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!raw.length || raw.includes("ALL")) return ["ALL"];
  const filtered = Array.from(new Set(raw.filter((id) => optionIds.has(id))));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return filtered.length ? filtered : ["ALL"];
}

function renderSolveWaveSelectionOptions() {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const allInput = document.getElementById("solveWaveAllInput");
  const optionsBox = document.getElementById("solveWaveOptions");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!allInput || !optionsBox) return;
  const options = getConfiguredWaveOptions().filter((item) => String(item.value || "").trim() !== "ALL");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  state.ui.solveWaveSelectedIds = normalizeSolveWaveSelection(state.ui.solveWaveSelectedIds, options);
  const isAll = state.ui.solveWaveSelectedIds.includes("ALL");
  allInput.checked = isAll;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!options.length) {
    optionsBox.innerHTML = `<span class="muted">${lang() === "ja" ? "可选波次なし" : "暂无可选波次"}</span>`;
    return;
  }
  const selectedSet = new Set(state.ui.solveWaveSelectedIds);
  optionsBox.innerHTML = options.map((item) => {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    const id = String(item.value || "").trim();
    const checked = !isAll && selectedSet.has(id);
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return `
      <label class="solve-wave-option">
        <input class="solve-wave-option-input" type="checkbox" data-solve-wave-id="${escapeHtml(id)}" ${checked ? "checked" : ""}>
        <span>${escapeHtml(String(item.label || id))}</span>
      </label>
    `;
  }).join("");
}

function renderWaveSolverPanel() {
  ensureWaveSolverPanel();
  renderSolveWaveSelectionOptions();
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function applySolveWaveSelectionToScenario(scenario) {
  if (!scenario || !Array.isArray(scenario.waves)) return { scenario, error: "波次数据不可用" };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (state.settings.ignoreWaves) return { scenario };
  const selected = normalizeSolveWaveSelection(state.ui.solveWaveSelectedIds, getConfiguredWaveOptions());
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (selected.includes("ALL")) return { scenario };
  const selectedSet = new Set(selected.map((id) => String(id || "").trim()).filter(Boolean));
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const filteredWaves = (scenario.waves || []).filter((wave) => selectedSet.has(String(wave?.waveId || "").trim()));
  if (!filteredWaves.length) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    return { scenario: null, error: lang() === "ja" ? "少なくとも1つの波次を選択してください。" : "请至少选择1个波次求解。" };
  }
  return { scenario: { ...scenario, waves: filteredWaves } };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function focusSolvedOutput() {
  const target = document.getElementById("resultPanels") || document.getElementById("analyticsPanel") || document.getElementById("summaryCards");
  target?.scrollIntoView?.({ behavior: "smooth", block: "start" });
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function buildSingleWaveScenario(baseScenario, waveId) {
  const wave = (baseScenario?.waves || []).find((item) => item.waveId === waveId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!wave) return null;
  return {
    ...baseScenario,
    waves: [clone(wave)],
  };
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
function getAlgorithmRunner(key) {
  const map = {
    vrptw: solveByVRPTW,
    hybrid: solveByHybrid,
    ga: solveByGA,
    tabu: solveByTabu,
    lns: solveByLNS,
    savings: solveBySavings,
    sa: solveBySA,
    aco: solveByACO,
    pso: solveByPSO,
    vehicle: solveByVehicle,
  };
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  return map[key] || null;
}

function getWaveOptimizer(key) {
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const map = {
    hybrid: optimizeWaveWithHybrid,
    ga: optimizeWaveWithGA,
    tabu: optimizeWaveWithTabu,
    lns: optimizeWaveWithLns,
    sa: optimizeWaveWithSA,
    aco: optimizeWaveWithACO,
    pso: optimizeWaveWithPSO,
    vehicle: optimizeWaveWithVehicleDrivenBackend,
  };
  return map[key] || null;
}

// EN: Key step in this business flow.
// CN: 当前业务流程中的关键步骤。
async function runSingleWaveInitialSolve() {
  syncSettingsFromUI();
  const box = document.getElementById("validationBox");
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const error = validateInput();
  if (error) {
    // EN: Key step in this business flow.
    // CN: 当前业务流程中的关键步骤。
    if (box) box.textContent = error;
    return;
  }
  const waveId = state.ui.waveSolverWaveId;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const algoKey = state.ui.waveSolverAlgo;
  const runner = getAlgorithmRunner(algoKey);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!runner) return;
  if (box) box.textContent = lang() === "ja" ? `波次 ${waveId} を ${algoLabel(algoKey)} で初期求解しています…` : `正在用 ${algoLabel(algoKey)} 对 ${waveId} 做初步求解…`;
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const scenario = await buildScenario();
  const singleScenario = buildSingleWaveScenario(scenario, waveId);
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  if (!singleScenario) {
    if (box) box.textContent = lang() === "ja" ? `波次 ${waveId} が見つかりません。` : `没有找到波次 ${waveId}。`;
    return;
  }
  // EN: Key step in this business flow.
  // CN: 当前业务流程中的关键步骤。
  const result = await runner(singleScenario);
  result.adjustMessage = lang() === "ja" ? `已切到 ${waveId} 的单独求解视图，当前算法是 ${algoLabel(algoKey)}。` : `已切到 ${waveId} 的单独求解视图，当前算法是 ${algoLabel(algoKey)}。`;
  state.lastResults = [result];
  state.activeResultKey = result.key;
  state.ui.generating = false;
  state.ui.progress = 100;
  state.ui.progressText = lang() === "ja" ? `${waveId} の初期求解が完了しました` : `${waveId} 的初步求解已完成`;
  if (box) box.textContent = lang() === "ja" ? `${waveId} の初期求解が完成しました。` : `${waveId} 的初步求解已完成。`;
  renderAll();
  document.getElementById("resultIntro").textContent = lang() === "ja"
    ? `${waveId} の単独波次ビューです。甘特图・驾驶舱・线路明细は下方に統一表示します。`
    : `当前是 ${waveId} 的单波次视图，甘特图、驾驶舱和线路明细都在下方统一展示。`;
  focusSolvedOutput();
}

async function runSingleWaveResolve() {
  const box = document.getElementById("validationBox");
  const result = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0] || null;
  if (!result) {
    if (box) box.textContent = lang() === "ja" ? "先に通常の调度结果を生成してください。" : "请先生成一版调度结果。";
    return;
  }
  const waveId = state.ui.waveSolverWaveId;
  const algoKey = state.ui.waveSolverAlgo;
  const target = getWaveAndPlans(result, waveId);
  if (!target) {
    if (box) box.textContent = lang() === "ja" ? `当前结果に波次 ${waveId} がありません。` : `当前结果里没有波次 ${waveId}。`;
    return;
  }
  if (box) box.textContent = lang() === "ja" ? `正在用 ${algoLabel(algoKey)} 对 ${waveId} 做再求解…` : `正在用 ${algoLabel(algoKey)} 对 ${waveId} 做再求解…`;
  const singleScenario = buildSingleWaveScenario(result.scenario, waveId);
  if (!singleScenario) return;
  let nextWavePlans = null;
  const optimizer = getWaveOptimizer(algoKey);
  if (optimizer && target.plans?.length) {
    const optimized = await optimizer(clone(target.plans), singleScenario, singleScenario.waves[0], 0);
    nextWavePlans = optimized?.plans || null;
  }
  if (!nextWavePlans) {
    const runner = getAlgorithmRunner(algoKey);
    const rerun = runner ? await runner(singleScenario) : null;
    nextWavePlans = rerun?.solution?.[0] || null;
  }
  if (!nextWavePlans) {
    if (box) box.textContent = lang() === "ja" ? `${waveId} の再求解に失敗しました。` : `${waveId} 的再求解失败了。`;
    return;
  }
  result.solution[target.waveIndex] = nextWavePlans;
  result.adjustMessage = lang() === "ja" ? `${waveId} を ${algoLabel(algoKey)} で再求解しました。` : `${waveId} 已按 ${algoLabel(algoKey)} 完成再求解。`;
  syncResultAfterLocalAdjustment(result);
  state.ui.generating = false;
  state.ui.progress = 100;
  state.ui.progressText = lang() === "ja" ? `${waveId} の再求解が完了しました` : `${waveId} 的再求解已完成`;
  if (box) box.textContent = result.adjustMessage;
  renderAll();
  document.getElementById("resultIntro").textContent = lang() === "ja"
    ? `${waveId} の波次だけを再求解したビューです。下方の甘特图・结果卡・线路明细を合わせて確認できます。`
    : `当前是只针对 ${waveId} 做再求解后的视图，下方会统一展示甘特图、结果卡和线路明细。`;
  focusSolvedOutput();
}

function buildExportText(result) {
  const lines = [];
  const metrics = result.metrics || {};
  const unscheduled = result.unscheduledStores || metrics.unscheduledStores || [];
  lines.push(`${L("exportFilePrefix")} - ${result.label}`);
  lines.push(`Time: ${new Date().toLocaleString()}`);
  lines.push("");
  lines.push(`${L("score")}: ${Number(metrics.score || 0).toFixed(1)} / 100`);
  lines.push(`${L("scheduledStores")}: ${metrics.scheduledCount || 0}`);
  lines.push(`${L("unscheduledStores")}: ${metrics.unscheduledCount || 0}`);
  lines.push(`${L("onTime")}: ${formatRate(metrics.onTimeRate || 0)}`);
  lines.push(`${L("totalDistance")}: ${Number(metrics.totalDistance || 0).toFixed(1)} km`);
  lines.push(`${L("avgLoad")}: ${formatRate(metrics.loadRate || 0)}`);
  lines.push(`${L("fleetLoad")}: ${formatRate(metrics.fleetLoadRate || 0)}`);
  lines.push(`${L("usedVehiclesShort")}: ${metrics.usedVehicleCount || 0}`);
  lines.push(`${L("idleVehiclesShort")}: ${metrics.unusedVehicleCount || 0}`);
  lines.push("");

  result.scenario.waves.forEach((wave, waveIndex) => {
    const plans = result.solution?.[waveIndex] || [];
    lines.push(`=== ${wave.waveId} ${wave.start} - ${wave.end} (${wave.endMode === "return" ? L("returnEnd") : L("serviceEnd")}) ===`);
    plans.filter((plan) => plan.trips?.length).forEach((plan) => {
      const planTripAvgLoad = (plan.trips || []).length ? (plan.trips || []).reduce((sum, trip) => sum + Number(trip.loadRate || 0), 0) / (plan.trips || []).length : 0;
      lines.push(`${plan.vehicle.plateNo} | ${L("storesCount")}: ${plan.totalStores || 0} | ${L("trips")}: ${plan.tripCount || 0} | ${L("totalRoundKm")}: ${Number(plan.totalDistance || 0).toFixed(1)} km | ${L("avgLoad")}: ${formatRate(planTripAvgLoad)}`);
      plan.trips.forEach((trip) => {
        lines.push(`  ${L("tripNo")}${trip.tripNo}${L("tripSuffix")}: ${buildRouteDisplay(trip.route, result.scenario)}`);
        lines.push(`    ${L("tripRoundKm")}: ${Number(trip.totalDistance || 0).toFixed(1)} km | ${L("tripLoadRate")}: ${formatRate(trip.loadRate || 0)}`);
        (trip.stops || []).forEach((stop) => {
          lines.push(`    - ${stop.storeId} ${stop.storeName || ""} | ${L("arrive")}: ${formatTime(stop.arrival)} | ${L("unloadStart")}: ${formatTime(stop.start)} | ${L("leave")}: ${formatTime(stop.leave)} | ${L("desired")}: ${stop.desiredArrival || "-"} | ${L("unloadMinutes")}: ${formatMinutesValue(stop.serviceMinutes)} ${L("minutes")} | ${stop.onTime ? L("notLate") : L("late")}`);
        });
      });
      lines.push("");
    });
  });

  lines.push(`=== ${L("unscheduledDetails")} ===`);
  if (!unscheduled.length) {
    lines.push(L("noUnscheduled"));
  } else {
    unscheduled.forEach((item) => {
      lines.push(`- ${item.storeId} ${item.storeName || ""} | ${item.waveId || "-"} | ${L("desired")}: ${item.desiredArrival || "-"} | ${getDisplayUnscheduledReason(item)}`);
    });
  }
  return lines.join("\r\n");
}

function exportCurrentResult() {
  const result = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  const box = document.getElementById("validationBox");
  if (!result) {
    if (box) box.textContent = L("exportNoResult");
    return;
  }
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  const fileName = `${L("exportFilePrefix")}_${result.key}_${timestamp}.txt`;
  const blob = new Blob([`\uFEFF${buildExportText(result)}`], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  if (box) box.textContent = `${L("exportDone")} ${fileName}`;
}

function escapeExcelHtml(value) {
  return escapeHtml(String(value ?? "")).replace(/\n/g, "<br>");
}

function downloadXlsHtml(fileName, title, headers, rows) {
  const thead = headers.map((header) => `<th>${escapeExcelHtml(header)}</th>`).join("");
  const tbody = rows.length
    ? rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeExcelHtml(cell)}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${Math.max(1, headers.length)}">无数据</td></tr>`;
  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    table { border-collapse: collapse; }
    th, td { border: 1px solid #999; padding: 4px 8px; }
    th { background: #f3f4f6; }
  </style>
</head>
<body>
  <h3>${escapeExcelHtml(title)}</h3>
  <table>
    <thead><tr>${thead}</tr></thead>
    <tbody>${tbody}</tbody>
  </table>
</body>
</html>`;
  const blob = new Blob([`\uFEFF${html}`], { type: "application/vnd.ms-excel;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function buildStoreExportRows(mode) {
  const stores = Array.isArray(state.stores) ? state.stores : [];
  const parseBelongs = (store) => parseWaveBelongs(store?.waveBelongs || "");
  return stores
    .filter((store) => {
      const belongs = parseBelongs(store);
      if (mode === "multi") return belongs.some((x) => x === "1" || x === "2") && !belongs.some((x) => x === "3" || x === "4");
      return belongs.some((x) => x === "3" || x === "4");
    })
    .map((store) => {
      const belongs = String(store?.waveBelongs || "").trim();
      const time1 = String(store?.waveArrivals?.w1 || "").trim();
      const time2 = String(store?.waveArrivals?.w2 || "").trim();
      const time3 = String(store?.waveArrivals?.w3 || "").trim();
      const time4 = String(store?.waveArrivals?.w4 || "").trim();
      const isMulti = mode === "multi";
      return isMulti
        ? [
          store.id || "",
          store.name || "",
          store.district || "",
          Number(store.tripCount || 1),
          Number(store.wave1TotalLoad || 0),
          Number(store.wave2TotalLoad || 0),
          time1,
          time2,
          belongs,
          Number(store.serviceMinutes || 15),
          Number(store.difficulty || 1),
          Number(store.parking || 10),
          store.status || "",
          store.plateNo || "",
        ]
        : [
          store.id || "",
          store.name || "",
          store.district || "",
          Number(store.tripCount || 1),
          Number(store.wave3TotalLoad || 0),
          Number(store.wave4TotalLoad || 0),
          time3,
          time4,
          belongs,
          Number(store.serviceMinutes || 15),
          Number(store.difficulty || 1),
          Number(store.parking || 10),
          store.status || "",
          store.plateNo || "",
        ];
    });
}

function exportMultiDailyStores() {
  const headers = ["编号", "名称", "区域", "次数", "一波次货量", "二波次货量", "一配时间", "二配时间", "所属波次", "卸货分钟", "难度", "允许偏差(分)", "状态", "车号"];
  const rows = buildStoreExportRows("multi");
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  downloadXlsHtml(`一日多配店铺明细_${timestamp}.xls`, "一日多配店铺明细", headers, rows);
}

function exportSingleDailyStores() {
  const headers = ["编号", "名称", "区域", "次数", "三波次货量", "四波次货量", "W3到店时间", "W4到店时间", "所属波次", "卸货分钟", "难度", "允许偏差(分)", "状态", "车号"];
  const rows = buildStoreExportRows("single");
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  downloadXlsHtml(`一日一配店铺明细_${timestamp}.xls`, "一日一配店铺明细", headers, rows);
}

function renderVehicles() {
  const usedSet = getUsedVehicleSet();
  const rows = state.vehicles.map((vehicle, index) => ({ vehicle, index, used: usedSet.has(vehicle.plateNo) }));
  const dir = state.ui.vehicleSortDir === "desc" ? -1 : 1;
  const field = state.ui.vehicleSortField || "plateNo";
  rows.sort((a, b) => {
    const toNum = (value) => Number(value || 0);
    if (field === "capacity" || field === "speed") {
      return (toNum(a.vehicle[field]) - toNum(b.vehicle[field])) * dir;
    }
    if (field === "status") {
      return ((a.used ? 1 : 0) - (b.used ? 1 : 0)) * dir;
    }
    const left = field === "type" ? vehicleTypeLabel(a.vehicle.type) : String(a.vehicle[field] || "");
    const right = field === "type" ? vehicleTypeLabel(b.vehicle.type) : String(b.vehicle[field] || "");
    return left.localeCompare(right, "zh-Hans-CN", { numeric: true }) * dir;
  });
  const columns = [
    { label: L("vehicleTableHeaders")[0], width: 140, sortable: true, sortField: "plateNo" },
    { label: L("vehicleTableHeaders")[1], width: 140, sortable: true, sortField: "driverName" },
    { label: L("vehicleTableHeaders")[2], width: 160, sortable: true, sortField: "type" },
    { label: L("vehicleTableHeaders")[3], width: 100, sortable: true, sortField: "capacity" },
    { label: L("vehicleTableHeaders")[4], width: 100, sortable: true, sortField: "speed" },
    { label: L("vehicleTableHeaders")[5], width: 110 },
    { label: L("vehicleTableHeaders")[6], width: 100, sortable: true, sortField: "status" },
    { label: L("vehicleTableHeaders")[7], width: 86 },
  ];
  const rowHtml = rows.map(({ vehicle: v, index: i, used }) => `
    <tr class="${used ? "vehicle-used-row" : ""} ${state.ui.vehicleLocatedIndex === i ? "located-row" : ""}">
      <td><input data-kind="vehicle" data-field="plateNo" data-index="${i}" value="${v.plateNo || ""}"></td>
      <td><input data-kind="vehicle" data-field="driverName" data-index="${i}" value="${v.driverName || ""}"></td>
      <td><input data-kind="vehicle" data-field="type" data-index="${i}" value="${vehicleTypeLabel(v.type)}"></td>
      <td><input data-kind="vehicle" data-field="capacity" data-index="${i}" type="number" value="${v.capacity || 0}"></td>
      <td><input type="number" value="38" readonly></td>
      <td><input data-kind="vehicle" data-field="canCarryCold" data-index="${i}" type="checkbox" ${v.canCarryCold ? "checked" : ""}></td>
      <td>${used ? `<span class="vehicle-used-tag">${L("used")}</span>` : `<span class="vehicle-idle-tag">${L("idle")}</span>`}</td>
      <td><button class="mini" data-remove="vehicle" data-index="${i}">${L("del")}</button></td>
    </tr>
  `);
  document.getElementById("vehicleTable").innerHTML = buildDataTableHtml({ tableKind: "vehicle", columns, rows: rowHtml, tableClass: "vehicle-data-table" });
}
function renderStoresTable() {
  renderStoreReschedulePanel();
  state.stores.forEach((store) => {
    syncStoreWaveLoads(store);
    const normalizedId = normalizeStoreCode(store.id);
    if (normalizedId !== store.id) store.id = normalizedId;
    if (!isAutoByW2Store(normalizedId)) return;
    if (!store.waveArrivals || typeof store.waveArrivals !== "object") store.waveArrivals = {};
    store.waveArrivals.w1 = "";
    if (!store.wave1Mode) store.wave1Mode = "AUTO_BY_W2";
    if (typeof store.desiredArrival === "string" && store.desiredArrival.trim()) store.desiredArrival = "";
    if (!store.waveArrivals.w2) {
      const preset = getStoreWavePreset(normalizedId);
      if (preset?.w2) store.waveArrivals.w2 = preset.w2;
    }
  });
  const assignmentMap = getActiveStoreAssignmentMap();
  const getResolvedWaveLoad = (store, field) => {
    const resolvedFieldMap = {
      wave1TotalLoad: "resolvedWave1Load",
      wave2TotalLoad: "resolvedWave2Load",
      wave3TotalLoad: "resolvedWave3Load",
      wave4TotalLoad: "resolvedWave4Load",
    };
    const resolvedField = resolvedFieldMap[field];
    if (resolvedField && store && store[resolvedField] !== undefined && store[resolvedField] !== null) {
      return Number(store[resolvedField] || 0);
    }
    return Number(store?.[field] || 0);
  };
  const rows = state.stores.map((store, index) => {
    const coverage = getStoreAssignmentCoverage(store, assignmentMap);
    const assignment = coverage.isFullyAssigned ? (getStoreAssignmentByRule(store, assignmentMap) || null) : null;
    return { store, index, assignment, coverage };
  });
  const dir = state.ui.storeSortDir === "desc" ? -1 : 1;
  const field = state.ui.storeSortField || "id";
  const parseTimeToSortValue = (v) => {
    const s = String(v || "").trim();
    if (!s) return Number.POSITIVE_INFINITY;
    const m = s.match(/^(\d{1,2}):(\d{2})/);
    if (!m) return Number.POSITIVE_INFINITY;
    return Number(m[1]) * 60 + Number(m[2]);
  };
  const parseWaveBelongsKey = (v) => {
    const ids = parseWaveBelongs(v || "");
    if (!ids.length) return Number.POSITIVE_INFINITY;
    return Number(ids[0] || 0) || Number.POSITIVE_INFINITY;
  };
  const getStoreSortValue = (row, sortField) => {
    const s = row.store || {};
    const plateNo = String(row.coverage?.displayPlate || row.assignment?.plateNo || s.plateNo || "");
    switch (sortField) {
      case "id": return String(s.id || "");
      case "name": return String(s.name || "");
      case "district": return String(s.district || "");
      case "tripCount": return Number(s.tripCount || 0);
      case "wave1TotalLoad": return getResolvedWaveLoad(s, "wave1TotalLoad");
      case "wave2TotalLoad": return getResolvedWaveLoad(s, "wave2TotalLoad");
      case "wave3TotalLoad": return getResolvedWaveLoad(s, "wave3TotalLoad");
      case "wave4TotalLoad": return getResolvedWaveLoad(s, "wave4TotalLoad");
      case "waveW1": return parseTimeToSortValue(s.waveArrivals?.w1 || s.desiredArrival || "");
      case "waveW2": return parseTimeToSortValue(s.waveArrivals?.w2 || "");
      case "waveW4": return parseTimeToSortValue(s.waveArrivals?.w4 || s.waveArrivals?.w3 || "");
      case "waveBelongs": return parseWaveBelongsKey(s.waveBelongs || "");
      case "serviceMinutes": return Number(s.serviceMinutes ?? 0);
      case "difficulty": return Number(s.difficulty || 0);
      case "parking": return Number(s.parking || 0);
      case "status": return row.coverage?.isFullyAssigned ? 1 : 0;
      case "plateNo": return plateNo;
      default: return String(s.id || "");
    }
  };
  const compareMixed = (left, right) => {
    const leftNum = Number(left);
    const rightNum = Number(right);
    const leftIsNum = Number.isFinite(leftNum) && String(left).trim() !== "";
    const rightIsNum = Number.isFinite(rightNum) && String(right).trim() !== "";
    if (leftIsNum && rightIsNum) return leftNum - rightNum;
    return String(left || "").localeCompare(String(right || ""), "zh-Hans-CN", { numeric: true });
  };
  rows.sort((a, b) => compareMixed(getStoreSortValue(a, field), getStoreSortValue(b, field)) * dir);
  const buildStoreWaveHeader = (label, total) => `
    <div class="store-wave-header">
      <span class="store-wave-header-label">${escapeHtml(label)}</span>
      <span class="store-wave-header-total">合计 ${formatLoadConvertValue(total)}</span>
    </div>
  `;
  const buildColumns = (waveLabelA, waveTotalA, waveLabelB, waveTotalB, opts = {}) => {
    const includeFirstTime = opts.includeFirstTime !== false;
    const secondTimeLabel = opts.secondTimeLabel || L("storeTableHeaders")[10];
    const firstWaveSortField = opts.firstWaveSortField || "wave1TotalLoad";
    const secondWaveSortField = opts.secondWaveSortField || "wave2TotalLoad";
    const secondTimeSortField = opts.secondTimeSortField || "waveW2";
    const columns = [
      { label: L("storeTableHeaders")[0], width: 140, sortable: true, sortField: "id" },
      { label: L("storeTableHeaders")[1], width: 200, sortable: true, sortField: "name" },
      { label: L("storeTableHeaders")[2], width: 120, sortable: true, sortField: "district" },
      { label: L("storeTableHeaders")[5], width: 100, sortable: true, sortField: "tripCount" },
      { headerHtml: buildStoreWaveHeader(waveLabelA, waveTotalA), width: 120, sortable: true, sortField: firstWaveSortField },
      { headerHtml: buildStoreWaveHeader(waveLabelB, waveTotalB), width: 120, sortable: true, sortField: secondWaveSortField },
    ];
    if (includeFirstTime) {
      columns.push({ label: L("storeTableHeaders")[9], width: 120, sortable: true, sortField: "waveW1" });
    }
    columns.push({ label: secondTimeLabel, width: 120, sortable: true, sortField: secondTimeSortField });
    columns.push(
      { label: L("storeTableHeaders")[11], width: 120, sortable: true, sortField: "waveBelongs" },
      { label: L("storeTableHeaders")[12], width: 100, sortable: true, sortField: "serviceMinutes" },
      { label: L("storeTableHeaders")[13], width: 100, sortable: true, sortField: "difficulty" },
      { label: L("storeTableHeaders")[14], width: 120, sortable: true, sortField: "parking" },
      { label: L("storeTableHeaders")[15], width: 100, sortable: true, sortField: "status" },
      { label: L("storeTableHeaders")[16], width: 140, sortable: true, sortField: "plateNo" },
      { label: L("storeTableHeaders")[17], width: 86 },
    );
    return columns;
  };
  const buildStoreRowHtml = (targetRows, firstWaveField, secondWaveField, firstWaveTitle, secondWaveTitle, opts = {}) => targetRows.map(({ store: s, index: i, assignment, coverage }) => `
    <tr class="${coverage?.isFullyAssigned ? "store-used-row" : ""} ${state.ui.storeLocatedIndex === i ? "located-row" : ""}">
      <td><input data-kind="store" data-field="id" data-index="${i}" value="${s.id || ""}" title="${escapeHtml(String(s.id || ""))}"></td>
      <td class="store-name-cell">
        <div class="store-name-stack">
          <span class="store-name-id" title="${escapeHtml(String(s.id || ""))}">店铺编号：${escapeHtml(String(s.id || ""))}</span>
          <input data-kind="store" data-field="name" data-index="${i}" value="${s.name || ""}" title="${escapeHtml(String(s.name || ""))}">
        </div>
      </td>
      <td><input data-kind="store" data-field="district" data-index="${i}" value="${s.district || ""}"></td>
      <td><input data-kind="store" data-field="tripCount" data-index="${i}" type="number" min="1" step="1" value="${Math.max(1, Number(s.tripCount || 1))}"></td>
      <td><input type="number" min="0" step="0.0001" value="${formatLoadConvertValue(s[firstWaveField] || 0)}" readonly title="${firstWaveTitle}"></td>
      <td><input type="number" min="0" step="0.0001" value="${formatLoadConvertValue(s[secondWaveField] || 0)}" readonly title="${secondWaveTitle}"></td>
      ${opts.includeFirstTime !== false ? `<td><input data-kind="store" data-field="waveW1" data-index="${i}" type="time" value="${isAutoByW2Store(s.id) ? "" : (s.waveArrivals?.w1 || "")}"></td>` : ""}
      <td><input data-kind="store" data-field="${opts.arrivalField || "waveW2"}" data-index="${i}" type="time" value="${opts.arrivalField === "waveW4" ? (s.waveArrivals?.w4 || s.waveArrivals?.w3 || "") : (s.waveArrivals?.w2 || "")}"></td>
      <td><input data-kind="store" data-field="waveBelongs" data-index="${i}" value="${normalizeWaveBelongsInput(s.waveBelongs || "")}" placeholder="1,2"></td>
      <td><input data-kind="store" data-field="serviceMinutes" data-index="${i}" type="number" min="1" value="${s.serviceMinutes ?? 15}"></td>
      <td><input data-kind="store" data-field="difficulty" data-index="${i}" type="number" step="0.1" value="${s.difficulty || 1}"></td>
      <td><input data-kind="store" data-field="parking" data-index="${i}" type="number" value="${s.parking ?? 10}"></td>
      <td>${coverage?.isFullyAssigned ? `<span class="store-used-tag">${L("scheduled")}</span>` : `<span class="store-idle-tag">${L("unscheduled")}</span>`}</td>
      <td><span class="store-plate-tag">${escapeHtml(String(coverage?.displayPlate || "-"))}</span></td>
      <td><button class="mini" data-remove="store" data-index="${i}">${L("del")}</button></td>
    </tr>
  `);
  const parseStoreWaveGroup = (store) => {
    const belongs = parseWaveBelongs(store?.waveBelongs || "");
    const has12 = belongs.some((waveId) => waveId === "1" || waveId === "2");
    const has34 = belongs.some((waveId) => waveId === "3" || waveId === "4");
    return { has12, has34 };
  };
  const multiDailyRows = rows.filter(({ store }) => {
    const { has12, has34 } = parseStoreWaveGroup(store);
    return has12 && !has34;
  });
  const singleDailyRows = rows.filter(({ store }) => {
    const { has34 } = parseStoreWaveGroup(store);
    return has34;
  });
  const multiWave1Total = multiDailyRows.reduce((sum, row) => sum + getResolvedWaveLoad(row.store, "wave1TotalLoad"), 0);
  const multiWave2Total = multiDailyRows.reduce((sum, row) => sum + getResolvedWaveLoad(row.store, "wave2TotalLoad"), 0);
  const singleWave3Total = singleDailyRows.reduce((sum, row) => sum + getResolvedWaveLoad(row.store, "wave3TotalLoad"), 0);
  const singleWave4Total = singleDailyRows.reduce((sum, row) => sum + getResolvedWaveLoad(row.store, "wave4TotalLoad"), 0);
  const storeTable = document.getElementById("storeTable");
  if (storeTable) {
    storeTable.innerHTML = buildDataTableHtml({
      tableKind: "store",
      columns: buildColumns("一波次货量", multiWave1Total, "二波次货量", multiWave2Total, {
        includeFirstTime: true,
        firstWaveSortField: "wave1TotalLoad",
        secondWaveSortField: "wave2TotalLoad",
        secondTimeSortField: "waveW2",
      }),
      rows: buildStoreRowHtml(
        multiDailyRows,
        "resolvedWave1Load",
        "resolvedWave2Load",
        "110：rpcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380",
        "210：apcs/350+apaper/380",
        { includeFirstTime: true, arrivalField: "waveW2" }
      ),
      tableClass: "store-data-table",
    });
  }
  const storeTableSingleDaily = document.getElementById("storeTableSingleDaily");
  if (storeTableSingleDaily) {
    storeTableSingleDaily.innerHTML = buildDataTableHtml({
      tableKind: "store",
      columns: buildColumns("三波次货量", singleWave3Total, "四波次货量", singleWave4Total, {
        includeFirstTime: false,
        secondTimeLabel: "到店时间",
        firstWaveSortField: "wave3TotalLoad",
        secondWaveSortField: "wave4TotalLoad",
        secondTimeSortField: "waveW4",
      }),
      rows: buildStoreRowHtml(
        singleDailyRows,
        "resolvedWave3Load",
        "resolvedWave4Load",
        "W3：rpcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380+apcs/350+apaper/380",
        "W4：rpcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380+apcs/350+apaper/380",
        { includeFirstTime: false, arrivalField: "waveW4" }
      ),
      tableClass: "store-data-table",
    });
  }
}
function renderWaves() {
  const left = document.getElementById("waveLeftList");
  const right = document.getElementById("waveRightEditor");
  if (!left || !right) return;
  const waveQuery = String(state.ui.waveSearchQuery || "").trim().toLowerCase();
  const allStoreOptions = state.stores.map((store) => ({
    id: normalizeStoreCode(store.id),
    name: store.name || normalizeStoreCode(store.id),
  })).filter((store) => store.id);
  const searched = state.waves.map((wave, index) => {
    const storeNames = getWaveStoreNameList(wave.storeIds).join(" ");
    const hay = `${wave.waveId || ""} ${storeNames}`.toLowerCase();
    return { wave, index, matched: !waveQuery || hay.includes(waveQuery) };
  });
  const leftItems = searched.filter((item) => item.matched);
  if (!leftItems.length) {
    left.innerHTML = `<div class="muted">无匹配波次</div>`;
  } else {
    if (!leftItems.some((item) => item.index === state.ui.waveSelectedIndex)) {
      state.ui.waveSelectedIndex = leftItems[0].index;
    }
    left.innerHTML = leftItems.map((item) => `
      <button type="button" class="wave-left-item ${item.index === state.ui.waveSelectedIndex ? "is-active" : ""}" data-wave-select-index="${item.index}">
        <div class="wave-left-name">${escapeHtml(item.wave.waveId || "--")}</div>
        <div class="wave-left-meta">${escapeHtml(item.wave.start || "--")} - ${escapeHtml(item.wave.end || "--")}</div>
        <div class="wave-left-meta">${renderWaveStoreNameTags(item.wave.storeIds, 2)}</div>
      </button>
    `).join("");
  }
  const selectedIndex = Math.max(0, Math.min(state.waves.length - 1, Number(state.ui.waveSelectedIndex || 0)));
  state.ui.waveSelectedIndex = selectedIndex;
  const wave = state.waves[selectedIndex];
  if (!wave) {
    right.innerHTML = `<div class="muted">暂无波次</div>`;
    return;
  }
  const selectedIds = new Set(parseStoreIds(wave.storeIds).map((id) => normalizeStoreCode(id)));
  const storeQuery = String(state.ui.waveStoreSearchQuery || "").trim().toLowerCase();
  const filteredStores = allStoreOptions.filter((store) => !storeQuery || `${store.id} ${store.name}`.toLowerCase().includes(storeQuery));
  right.innerHTML = `
    <div class="wave-edit-grid">
      <label>${L("waveTableHeaders")[0]}<input data-kind="wave" data-field="waveId" data-index="${selectedIndex}" value="${escapeHtml(wave.waveId || "")}"></label>
      <label>${L("waveTableHeaders")[1]}<input data-kind="wave" data-field="start" data-index="${selectedIndex}" type="time" value="${escapeHtml(wave.start || "06:30")}"></label>
      <label>${L("waveTableHeaders")[2]}<input data-kind="wave" data-field="end" data-index="${selectedIndex}" type="time" value="${escapeHtml(wave.end || "10:30")}"></label>
      <label>${L("waveTableHeaders")[3]}
        <select data-kind="wave" data-field="endMode" data-index="${selectedIndex}">
          <option value="return" ${(wave.endMode || "return") === "return" ? "selected" : ""}>${L("returnEnd")}</option>
          <option value="service" ${(wave.endMode || "return") === "service" ? "selected" : ""}>${L("serviceEnd")}</option>
        </select>
      </label>
    </div>
    <div class="wave-editor-toolbar">
      <input id="waveStoreSearchInput" type="text" placeholder="搜索门店名称" value="${escapeHtml(state.ui.waveStoreSearchQuery || "")}">
      <button class="secondary" data-wave-select-all="${selectedIndex}">全选当前检索</button>
      <button class="secondary" data-wave-clear-all="${selectedIndex}">清空当前波次</button>
      <button class="mini" data-remove="wave" data-index="${selectedIndex}">${L("del")}</button>
    </div>
    <div>${renderWaveStoreNameTags(wave.storeIds, 6)}</div>
    <div class="wave-store-list">
      ${filteredStores.map((store) => `
        <label class="wave-store-option">
          <input type="checkbox" data-wave-store-index="${selectedIndex}" data-store-id="${escapeHtml(store.id)}" ${selectedIds.has(store.id) ? "checked" : ""}>
          <span class="store-name">${escapeHtml(store.name)}</span>
        </label>
      `).join("") || `<div class="muted">无匹配门店</div>`}
    </div>
  `;
}
function renderAll() {
  syncRealtimeDispatchContext();
  renderVehicles();
  renderStoresTable();
  renderWaves();
  renderDataArchivePanels();
  renderSingleWaveInfo();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderRecommendedPlans();
  renderRunRegionList();
  renderSavedPlans();
  renderWaveSolverPanel();
  applyMainTabVisibility();
}
function normalizeCustomAlgorithms(keys) {
  const picked = [];
  const seen = new Set();
  for (const raw of Array.isArray(keys) ? keys : []) {
    const key = String(raw || "").trim();
    if (!ALGORITHM_ORDER.includes(key) || seen.has(key)) continue;
    picked.push(key);
    seen.add(key);
    if (picked.length >= MAX_FREE_SOLVE_ALGOS) break;
  }
  return picked;
}
function getDefaultFreeAlgorithms() {
  return ["vrptw", "savings"];
}
function getEffectiveFreeAlgorithms() {
  const uiPicked = normalizeCustomAlgorithms(getCheckedAlgorithmsFromUI());
  if (uiPicked.length) return uiPicked;
  return normalizeCustomAlgorithms(state.settings.customAlgorithms || []);
}
function setAlgorithmsChecked(keys) {
  const wanted = new Set(keys);
  document.querySelectorAll(".algo-check").forEach((input) => {
    input.checked = wanted.has(input.value);
  });
}
function getCheckedAlgorithmsFromUI() {
  return Array.from(document.querySelectorAll(".algo-check:checked")).map((input) => String(input.value || "").trim());
}
function syncAlgorithmControls() {
  const isFreeMode = (state.settings.solveStrategy || "manual") === "free";
  if (isFreeMode) {
    const normalized = getEffectiveFreeAlgorithms();
    state.settings.customAlgorithms = normalized;
    setAlgorithmsChecked(state.settings.customAlgorithms);
  }
  document.querySelectorAll(".algo-check").forEach((input) => {
    input.disabled = !isFreeMode;
    input.setAttribute("aria-disabled", isFreeMode ? "false" : "true");
    input.title = isFreeMode
      ? (lang() === "ja" ? `自由求解模式：最多选择 ${MAX_FREE_SOLVE_ALGOS} 个算法。` : `自由求解模式：最多选择 ${MAX_FREE_SOLVE_ALGOS} 种算法。`)
      : (lang() === "ja" ? "アルゴリズムの組み合わせは、現在の方針に応じてシステムが自動で選びます。" : "算法组合会由系统按当前策略自动选择。");
    input.closest(".algo-box")?.classList.toggle("is-locked", !isFreeMode);
  });
}
function renderAlgorithmPool() {
  const box = document.querySelector(".algo-box");
  if (!box) return;
  const currentStrategy = state.settings.solveStrategy || "quick";
  const currentPreset = strategyPreset(currentStrategy, state.settings.optimizeGoal || "balanced");
  const active = new Set(currentPreset.algorithms || []);
  let shell = box.querySelector(".algorithm-pool-shell");
  if (!shell) {
    shell = document.createElement("section");
    shell.className = "algorithm-pool-shell";
    box.appendChild(shell);
  }
  let pool = shell.querySelector(".algorithm-pool");
  if (!pool) {
    pool = document.createElement("div");
    pool.className = "algorithm-pool";
    shell.appendChild(pool);
  }
  const order = ["vrptw", "hybrid", "ga", "tabu", "lns", "savings", "sa", "aco", "pso", "vehicle"];
  shell.innerHTML = `
    <div class="algorithm-pool-head">
      <div>
        <h3>${lang() === "ja" ? "アルゴリズム中枢" : "算法中枢"}</h3>
      </div>
      <p class="algorithm-pool-summary">${currentStrategy === "free"
        ? (lang() === "ja"
          ? `自由求解です。下のカードを直接クリックしてアルゴリズムを選択できます（最大 ${MAX_FREE_SOLVE_ALGOS} 本）。現在 ${active.size} 本。`
          : `当前是自由求解，可直接点击下方卡片选择算法（最多 ${MAX_FREE_SOLVE_ALGOS} 种）。当前已选 ${active.size} 种。`)
        : (lang() === "ja"
          ? `今回の計算では ${active.size} 本のアルゴリズムが動作中です。実際に使われている中核だけを自動で点灯表示します。`
          : `本轮已调用 ${active.size} 套算法，系统会自动点亮当前正在参与求解的核心。`)}</p>
    </div>
    <div class="algorithm-pool"></div>
  `;
  pool = shell.querySelector(".algorithm-pool");
  pool.innerHTML = order.map((key) => `
    <div class="algorithm-card ${active.has(key) ? "is-active" : ""} ${currentStrategy === "free" ? "is-pickable" : ""}" data-algo-card="${key}" title="${currentStrategy === "free" ? (lang() === "ja" ? "クリックして選択/解除（最大5本）" : "点击选择/取消（最多5种）") : ""}">
      <div class="algorithm-card-head">
        <span class="algorithm-card-name">${algoLabel(key)}</span>
        <span class="algorithm-card-state">${active.has(key) ? (lang() === "ja" ? "稼働中" : "已启用") : (lang() === "ja" ? "待機中" : "待命中")}</span>
      </div>
      <div class="algorithm-card-body">${algoDescription(key)}</div>
    </div>
  `).join("");
}
function strategyPreset(strategy, goal = "") {
  const targetGoal = String(goal || state.settings.optimizeGoal || "balanced");
  if (strategy === "free") {
    const freeAlgos = normalizeCustomAlgorithms(state.settings.customAlgorithms || []);
    return {
      algorithms: freeAlgos.length ? freeAlgos : getDefaultFreeAlgorithms(),
      goal: targetGoal,
    };
  }
  const byStrategy = {
    manual: {
      balanced: { algorithms: ["vrptw", "savings"], goal: "balanced" },
      ontime: { algorithms: ["vrptw", "savings"], goal: "ontime" },
      distance: { algorithms: ["savings", "vrptw"], goal: "distance" },
      vehicles: { algorithms: ["savings", "vrptw"], goal: "vehicles" },
      load: { algorithms: ["vrptw", "savings"], goal: "load" },
    },
    quick: {
      balanced: { algorithms: ["vrptw", "savings"], goal: "balanced" },
      ontime: { algorithms: ["vrptw", "savings"], goal: "ontime" },
      distance: { algorithms: ["savings", "vrptw"], goal: "distance" },
      vehicles: { algorithms: ["savings", "vrptw"], goal: "vehicles" },
      load: { algorithms: ["vrptw", "savings"], goal: "load" },
    },
    deep: {
      balanced: { algorithms: ["hybrid", "tabu", "lns"], goal: "balanced" },
      ontime: { algorithms: ["hybrid", "tabu", "sa"], goal: "ontime" },
      distance: { algorithms: ["lns", "tabu", "sa"], goal: "distance" },
      vehicles: { algorithms: ["hybrid", "lns", "tabu"], goal: "vehicles" },
      load: { algorithms: ["hybrid", "tabu", "sa"], goal: "load" },
    },
    global: {
      balanced: { algorithms: ["pso", "ga", "aco"], goal: "balanced" },
      ontime: { algorithms: ["ga", "pso", "aco"], goal: "ontime" },
      distance: { algorithms: ["aco", "pso", "ga"], goal: "distance" },
      vehicles: { algorithms: ["pso", "ga", "aco"], goal: "vehicles" },
      load: { algorithms: ["pso", "ga", "aco"], goal: "load" },
    },
    relay: {
      balanced: { algorithms: ["vrptw", "pso", "hybrid", "tabu", "lns"], goal: "balanced" },
      ontime: { algorithms: ["vrptw", "ga", "hybrid", "tabu", "sa"], goal: "ontime" },
      distance: { algorithms: ["savings", "aco", "lns", "tabu", "sa"], goal: "distance" },
      vehicles: { algorithms: ["savings", "pso", "ga", "hybrid", "tabu"], goal: "vehicles" },
      load: { algorithms: ["vrptw", "pso", "hybrid", "ga", "sa"], goal: "load" },
    },
    compare: {
      balanced: { algorithms: ["vrptw", "hybrid", "tabu", "lns", "savings", "sa", "aco", "pso", "ga", "vehicle"], goal: "balanced" },
      ontime: { algorithms: ["vrptw", "hybrid", "tabu", "lns", "savings", "sa", "aco", "pso", "ga", "vehicle"], goal: "ontime" },
      distance: { algorithms: ["vrptw", "hybrid", "tabu", "lns", "savings", "sa", "aco", "pso", "ga", "vehicle"], goal: "distance" },
      vehicles: { algorithms: ["vrptw", "hybrid", "tabu", "lns", "savings", "sa", "aco", "pso", "ga", "vehicle"], goal: "vehicles" },
      load: { algorithms: ["vrptw", "hybrid", "tabu", "lns", "savings", "sa", "aco", "pso", "ga", "vehicle"], goal: "load" },
    },
  };
  const strategyMap = byStrategy[strategy] || byStrategy.manual;
  return strategyMap[targetGoal] || strategyMap.balanced;
}
function currentStrategyLabel() {
  const labels = {
    manual: L("strategyManual"),
    quick: L("strategyQuick"),
    deep: L("strategyDeep"),
    global: L("strategyGlobal"),
    relay: L("strategyRelay"),
    free: L("strategyFree"),
    compare: L("strategyCompare"),
  };
  return labels[state.settings.solveStrategy || "manual"] || labels.manual;
}
function currentGoalLabel() {
  const labels = {
    balanced: L("goalBalanced"),
    ontime: L("goalOnTime"),
    distance: L("goalDistance"),
    vehicles: L("goalVehicles"),
    load: L("goalLoad"),
  };
  return labels[state.settings.optimizeGoal || "balanced"] || labels.balanced;
}
function renderStrategyPreviewControls() {
  const groups = document.querySelectorAll(".strategy-shell .strategy-preview-group");
  if (groups[0]) {
    groups[0].innerHTML = `
      <div class="strategy-preview-label" id="solveModePreviewLabel">${lang() === "ja" ? "解法モード" : "求解方式"}</div>
      <div class="strategy-preview-row">
        <button type="button" class="strategy-pill strategy-preview-btn ${(state.settings.solveStrategy || "quick") === "quick" ? "is-active" : ""}" data-solve-preview="quick" id="solvePreviewQuick">${L("quickSolve")}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill strategy-preview-btn ${(state.settings.solveStrategy || "quick") === "deep" ? "is-active" : ""}" data-solve-preview="deep" id="solvePreviewDeep">${L("deepOptimize")}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill strategy-preview-btn ${(state.settings.solveStrategy || "quick") === "relay" ? "is-active" : ""}" data-solve-preview="relay" id="solvePreviewRelay">${L("relaySolve")}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill strategy-preview-btn ${(state.settings.solveStrategy || "quick") === "free" ? "is-active" : ""}" data-solve-preview="free" id="solvePreviewFree">${L("freeSolve")}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill strategy-preview-btn ${(state.settings.solveStrategy || "quick") === "compare" ? "is-active" : ""}" data-solve-preview="compare" id="solvePreviewCompare">${L("multiCompare")}</button>
      </div>`;
  }
  if (groups[1]) {
    groups[1].innerHTML = `
      <div class="strategy-preview-label" id="goalModePreviewLabel">${lang() === "ja" ? "目標方針" : "方案目标"}</div>
      <div class="strategy-preview-row">
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "ontime" ? "is-active" : ""}" data-goal-preview="ontime" id="goalPreviewOnTime">${lang() === "ja" ? "定時重視" : "最准时"}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "vehicles" ? "is-active" : ""}" data-goal-preview="vehicles" id="goalPreviewVehicles">${lang() === "ja" ? "少車両" : "少用车"}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "distance" ? "is-active" : ""}" data-goal-preview="distance" id="goalPreviewDistance">${lang() === "ja" ? "短距離" : "省里程"}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "balanced" ? "is-active" : ""}" data-goal-preview="balanced" id="goalPreviewBalanced">${L("goalBalanced")}</button>
      </div>`;
  }
}
function buildStrategyHint() {
  const strategy = state.settings.solveStrategy || "manual";
  const goal = state.settings.optimizeGoal || "balanced";
  const labels = {
    quick: lang() === "ja" ? "まず実用的な初期案を素早く作る構成です。" : "先用快速构造算法给出可行初稿。",
    deep: lang() === "ja" ? "現在の案を引き継いで、さらに詰めていく改善構成です。" : "主打在现有方案上继续深挖优化。",
    global: lang() === "ja" ? "大きく別のルート構造を探すためのグローバル探索構成です。" : "主打全局搜索，探索完全不同的排线结构。",
    relay: lang() === "ja" ? "初期案から改善までを段階的につなぐ、リレー型の求解構成です。" : "让初排和优化算法接力求解，更像真正的平台流程。",
    free: lang() === "ja" ? `自由求解です。アルゴリズムは手動で選択でき、最大 ${MAX_FREE_SOLVE_ALGOS} 本までです。` : `自由求解模式，可手动选择算法，最多 ${MAX_FREE_SOLVE_ALGOS} 种。`,
    compare: lang() === "ja" ? "複数の解法を並べて差を見比べる比較モードです。" : "并排跑多种思路，方便做算法对比。",
    manual: lang() === "ja" ? "現在は手動選択モードです。" : "当前是手动勾选模式。",
  };
  const goalTexts = {
    balanced: lang() === "ja" ? "目標は総合バランス重視です。" : "目标偏向综合平衡。",
    ontime: lang() === "ja" ? "目標は定時性重視です。" : "目标偏向准点率。",
    distance: lang() === "ja" ? "目標は総距離の圧縮です。" : "目标偏向总里程压缩。",
    vehicles: lang() === "ja" ? "目標は使用車両数の抑制です。" : "目标偏向少用车。",
    load: lang() === "ja" ? "目標は積載の集約です。" : "目标偏向装载集中。",
  };
  const baseHint = `${labels[strategy] || labels.manual} ${goalTexts[goal] || goalTexts.balanced}`;
  if (strategy === "free") {
    const selected = getEffectiveFreeAlgorithms();
    return `${baseHint} ${lang() === "ja" ? `現在 ${selected.length}/${MAX_FREE_SOLVE_ALGOS} 本を選択中です。` : `当前已选择 ${selected.length}/${MAX_FREE_SOLVE_ALGOS} 种算法。`}`;
  }
  return baseHint;
}
function updateStrategyActionButtons() {
  const mapping = {
    quickSolveBtn: "quick",
    deepOptimizeBtn: "deep",
    globalSearchBtn: "global",
    relaySolveBtn: "relay",
    freeSolveBtn: "free",
    multiCompareBtn: "compare",
  };
  Object.entries(mapping).forEach(([id, strategy]) => {
    const btn = document.getElementById(id);
    if (!btn) return;
    btn.classList.toggle("is-active", (state.settings.solveStrategy || "manual") === strategy);
  });
}
function applySolveStrategy(strategy, goalOverride = "") {
  const preset = strategyPreset(strategy, goalOverride || state.settings.optimizeGoal || "");
  state.settings.solveStrategy = strategy;
  state.settings.optimizeGoal = goalOverride || preset.goal || "balanced";
  if (strategy === "free") {
    const normalized = normalizeCustomAlgorithms(state.settings.customAlgorithms || preset.algorithms);
    state.settings.customAlgorithms = normalized.length ? normalized : getDefaultFreeAlgorithms();
    setAlgorithmsChecked(state.settings.customAlgorithms);
  } else {
    setAlgorithmsChecked(preset.algorithms);
  }
  syncAlgorithmControls();
  const strategySelect = document.getElementById("solveStrategySelect");
  const goalSelect = document.getElementById("optimizeGoalSelect");
  if (strategySelect) strategySelect.value = state.settings.solveStrategy;
  if (goalSelect) goalSelect.value = state.settings.optimizeGoal;
  const hint = document.getElementById("strategyHint");
  if (hint) hint.textContent = buildStrategyHint();
  renderStrategyPreviewControls();
  updateStrategyActionButtons();
  renderAlgorithmPool();
}
function buildSolveModeSummary() {
  const strategy = currentStrategyLabel();
  const goal = currentGoalLabel();
  const selectedCount = strategyPreset(state.settings.solveStrategy || "manual", state.settings.optimizeGoal || "balanced").algorithms.length;
  return lang() === "ja"
    ? `現在は「${strategy}」モードで計算し、目標方針は「${goal}」、参加アルゴリズムは ${selectedCount} 本です。`
    : `当前按“${strategy}”模式求解，优化目标为“${goal}”，参与算法 ${selectedCount} 种。`;
}

function relayMetricSummary(metrics) {
  const onTime = Math.round((metrics.totalOnTime / Math.max(metrics.totalStops || 0, 1)) * 100);
  return `评分 ${metrics.score.toFixed(1)}，已调度 ${metrics.scheduledCount || 0} 家，未调度 ${metrics.unscheduledCount || 0} 家，准点 ${onTime}%，总里程 ${(metrics.totalDistance || 0).toFixed(1)} km，用车 ${metrics.usedVehicleCount || 0} 辆`;
}

function describeRelayMetricDelta(beforeMetrics, afterMetrics) {
  const parts = [];
  const scoreDelta = (afterMetrics.score || 0) - (beforeMetrics.score || 0);
  const scheduledDelta = (afterMetrics.scheduledCount || 0) - (beforeMetrics.scheduledCount || 0);
  const unscheduledDelta = (afterMetrics.unscheduledCount || 0) - (beforeMetrics.unscheduledCount || 0);
  const onTimeBefore = (beforeMetrics.totalOnTime / Math.max(beforeMetrics.totalStops || 0, 1)) * 100;
  const onTimeAfter = (afterMetrics.totalOnTime / Math.max(afterMetrics.totalStops || 0, 1)) * 100;
  const onTimeDelta = onTimeAfter - onTimeBefore;
  const distanceDelta = (afterMetrics.totalDistance || 0) - (beforeMetrics.totalDistance || 0);
  const vehicleDelta = (afterMetrics.usedVehicleCount || 0) - (beforeMetrics.usedVehicleCount || 0);
  if (Math.abs(scoreDelta) > 0.05) parts.push(`总分 ${scoreDelta > 0 ? "提高" : "下降"} ${Math.abs(scoreDelta).toFixed(1)}`);
  if (scheduledDelta !== 0) parts.push(`已调度门店 ${scheduledDelta > 0 ? `增加 ${scheduledDelta}` : `减少 ${Math.abs(scheduledDelta)}`}`);
  if (unscheduledDelta !== 0) parts.push(`未调度门店 ${unscheduledDelta < 0 ? `减少 ${Math.abs(unscheduledDelta)}` : `增加 ${unscheduledDelta}`}`);
  if (Math.abs(onTimeDelta) >= 0.5) parts.push(`准点率 ${onTimeDelta > 0 ? "上升" : "下降"} ${Math.abs(onTimeDelta).toFixed(1)}%`);
  if (Math.abs(distanceDelta) >= 0.1) parts.push(`总里程 ${distanceDelta < 0 ? `减少 ${Math.abs(distanceDelta).toFixed(1)} km` : `增加 ${distanceDelta.toFixed(1)} km`}`);
  if (vehicleDelta !== 0) parts.push(`用车 ${vehicleDelta < 0 ? `减少 ${Math.abs(vehicleDelta)} 辆` : `增加 ${vehicleDelta} 辆`}`);
  return parts.length ? parts.join("，") : "主要指标几乎没有变化";
}
function syncSettingsFromUI() {
  state.settings.dispatchStartTime = document.getElementById("dispatchStartTimeInput").value || "19:10";
  state.settings.maxRouteKm = Number(document.getElementById("maxRouteKmInput").value || 220);
  state.settings.minLoadRate = Number(document.getElementById("minLoadRateInput").value || 0) / 100;
  state.settings.targetScore = Number(document.getElementById("targetScoreInput").value || 0);
  state.settings.ignoreWaves = Boolean(document.getElementById("ignoreWavesInput").checked);
  state.settings.concentrateLate = Boolean(document.getElementById("concentrateLateInput")?.checked);
  state.settings.singleWaveDistanceKm = Number(document.getElementById("singleWaveDistanceInput").value || 70);
  state.settings.singleWaveStart = document.getElementById("singleWaveStartInput").value || "19:10";
  state.settings.singleWaveEnd = document.getElementById("singleWaveEndInput").value || "05:30";
  state.settings.singleWaveEndMode = document.getElementById("singleWaveEndModeInput").value || "service";
  state.settings.solveStrategy = document.getElementById("solveStrategySelect")?.value || "manual";
  state.settings.optimizeGoal = document.getElementById("optimizeGoalSelect")?.value || "balanced";
  state.strategyConfig = normalizeStrategyConfig({
    ...state.strategyConfig,
    deliveryMode: document.getElementById("strategyDeliveryModeSelect")?.value || "singleDailyWave",
    optimizeGoal: document.getElementById("strategyOptimizeGoalSelect")?.value || "balanced",
    loadDistanceBias: Number(document.getElementById("strategyLoadDistanceBiasInput")?.value || 0),
    latenessTolerance: document.getElementById("strategyLatenessToleranceSelect")?.value || "medium",
    vehicleCostBias: Number(document.getElementById("strategyVehicleCostBiasInput")?.value || 50),
    dualWaveWeight: Number(document.getElementById("strategyDualWaveWeightInput")?.value || 50),
    crossRegionPenaltyWeight: Number(document.getElementById("strategyCrossRegionPenaltyWeightInput")?.value || 50),
    waveDelayPenalty: Number(document.getElementById("strategyWaveDelayPenaltyInput")?.value || 50),
    lateRouteStrength: document.getElementById("strategyLateRouteStrengthSelect")?.value || "medium",
    deliveryDifficultyMode: document.getElementById("strategyDifficultyModeSelect")?.value || "time",
    difficultyTier1Unlimited: Boolean(document.getElementById("strategyDifficultyTier1UnlimitedInput")?.checked),
    difficultyTier1Limit: Number(document.getElementById("strategyDifficultyTier1LimitInput")?.value || 1),
    difficultyTier2Unlimited: Boolean(document.getElementById("strategyDifficultyTier2UnlimitedInput")?.checked),
    difficultyTier2Limit: Number(document.getElementById("strategyDifficultyTier2LimitInput")?.value || 2),
    difficultyTier3Unlimited: Boolean(document.getElementById("strategyDifficultyTier3UnlimitedInput")?.checked),
    difficultyTier3Limit: Number(document.getElementById("strategyDifficultyTier3LimitInput")?.value || 0),
    difficultyScoreLimit: Number(document.getElementById("strategyDifficultyScoreLimitInput")?.value || 8),
  });
  state.strategyConfig = applyOptimizeGoalPreset(state.strategyConfig.optimizeGoal, state.strategyConfig);
  updateDifficultyTierLimitInputState(state.strategyConfig);
  updateDifficultyModeFieldVisibility(state.strategyConfig.deliveryDifficultyMode || "time");
  if ((state.settings.solveStrategy || "manual") === "free") {
    state.settings.customAlgorithms = normalizeCustomAlgorithms(getCheckedAlgorithmsFromUI());
  }
  const w1 = state.waves.find((wave) => wave.waveId === "W1");
  if (w1) w1.start = state.settings.dispatchStartTime;
  const hint = document.getElementById("strategyHint");
  if (hint) hint.textContent = buildStrategyHint();
  renderStrategyPreviewControls();
  renderAlgorithmPool();
  renderSingleWaveInfo();
}
async function triggerStrategySolve(strategy, goal) {
  if (state.ui.generating) return;
  applySolveStrategy(strategy, goal);
  const report = await buildSolveDiagnoseReport();
  const box = document.getElementById("validationBox");
  if (box) {
    box.textContent = `${buildSolveModeSummary()} ${lang() === "ja"
      ? "先打开求解前诊断窗口，确认字段是否齐全后再继续。"
      : "先弹出求解前诊断窗口，确认字段是否齐全后再继续。"}`
    ;
  }
  pendingSolveContinuation = async () => {
    pendingSolveContinuation = null;
    await generatePlans();
  };
  openSolveDiagnoseModal(report);
}

async function openSolveDiagnoseAndPause(strategy, goal) {
  if (state.ui.generating) return;
  applySolveStrategy(strategy, goal);
  const report = await buildSolveDiagnoseReport();
  const box = document.getElementById("validationBox");
  if (box) box.textContent = `${buildSolveModeSummary()} ${lang() === "ja" ? "先に診断を確認してください。" : "先看诊断报告，再决定是否继续求解。"}`;
  pendingSolveContinuation = async () => {
    pendingSolveContinuation = null;
    await generatePlans();
  };
  openSolveDiagnoseModal(report);
}
function autoBuildWavesFromCurrentStores() { state.waves = buildAutoWaves(state.stores); renderWaves(); document.getElementById("validationBox").textContent = LT("autoWaveBuilt", { count: state.waves.length }); }
function addVehicle() { state.vehicles.push({ plateNo: "", driverName: "", type: ENFORCED_VEHICLE_TYPE, capacity: 100, speed: 38, canCarryCold: false }); renderVehicles(); }
function addStore() {
  state.stores.push(syncStoreWaveLoads({
    id: "",
    name: "",
    district: "",
    lng: 0,
    lat: 0,
    address: "",
    tripCount: 1,
    waveBelongs: "1",
    rpcs: 0,
    rcase: 0,
    bpcs: 0,
    bpaper: 0,
    apcs: 0,
    apaper: 0,
    rpaper: 0,
    coldRatio: 0,
    desiredArrival: "02:45",
    waveArrivals: { w1: "02:45" },
    serviceMinutes: 15,
    difficulty: 1,
    parking: 10,
  }));
  renderStoresTable();
}
function addWave() {
  const nextIndex = state.waves.length + 1;
  const defaults = nextIndex === 1
    ? { start: "19:10", end: "23:59", endMode: "return" }
    : nextIndex === 2
      ? { start: "21:00", end: "07:00", endMode: "service" }
      : nextIndex === 3
        ? { start: "21:00", end: "07:00", endMode: "service" }
        : nextIndex === 4
          ? { start: "12:00", end: "18:00", endMode: "service" }
        : { start: "19:10", end: "23:59", endMode: "return" };
  state.waves.push({ waveId: `W${nextIndex}`, ...defaults, storeIds: "" });
  renderWaves();
}
function parseVehicleTxt(text) {
  const seen = new Set();
  return String(text || "")
    .replace(/\u0000/g, "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split(/[\t,，;；\s]+/).map((x) => x.trim()).filter(Boolean);
      return { plateNo: parts[0] || "", driverName: parts[1] || "" };
    })
    .filter((item) => item.plateNo)
    .filter((item) => {
      if (seen.has(item.plateNo)) return false;
      seen.add(item.plateNo);
      return true;
    })
    .map((item) => ({ ...item, type: ENFORCED_VEHICLE_TYPE, capacity: 100, speed: 38, canCarryCold: false }));
}

function decodeTextWithEncoding(buffer, encoding) {
  try {
    return new TextDecoder(encoding, { fatal: false }).decode(buffer);
  } catch {
    return "";
  }
}

async function readTextFileWithFallback(file) {
  const buffer = await file.arrayBuffer();
  const candidates = ["utf-8", "utf-16le", "utf-16be", "gb18030"];
  for (const encoding of candidates) {
    const text = decodeTextWithEncoding(buffer, encoding);
    const parsed = parseVehicleTxt(text);
    if (parsed.length) return { text, parsed, encoding };
  }
  const fallbackText = decodeTextWithEncoding(buffer, "utf-8");
  return { text: fallbackText, parsed: parseVehicleTxt(fallbackText), encoding: "utf-8" };
}

async function parseVehicleFileByBackend(file) {
  const available = await ensureGaBackendAvailable(true);
  if (!available) throw new Error("ga_backend_unavailable");
  const arrayBuffer = await file.arrayBuffer();
  let binary = "";
  const bytes = new Uint8Array(arrayBuffer);
  for (let i = 0; i < bytes.length; i += 1) binary += String.fromCharCode(bytes[i]);
  const contentBase64 = btoa(binary);
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/vehicles/parse`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fileName: file.name || "", contentBase64 }),
  }, 8000);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const payload = await response.json();
  if (!payload?.ok) throw new Error(payload?.error || "vehicle_parse_failed");
  return Array.isArray(payload.vehicles) ? payload.vehicles : [];
}

function setImportFileTag(kind, fileName = "") {
  const nameEl = document.getElementById(`${kind}FileName`);
  const tagEl = document.getElementById(`${kind}FileTag`);
  if (nameEl) nameEl.textContent = fileName || L("noFileChosen");
  if (tagEl) {
    tagEl.classList.toggle("is-empty", !fileName);
    tagEl.title = fileName || "";
  }
}

function splitSimpleCsvLine(line) {
  return String(line || "").split(/[\t,，]/).map((x) => x.trim());
}

function parseStoreFileText(text) {
  const source = String(text || "").trim();
  if (!source) return [];
  try {
    const parsed = JSON.parse(source);
    if (Array.isArray(parsed)) return parsed;
  } catch {}
  const lines = source.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = splitSimpleCsvLine(lines[0]);
  const indexOf = (...candidates) => candidates.map((name) => headers.indexOf(name)).find((idx) => idx >= 0) ?? -1;
  const idx = {
    id: indexOf("编号", "店铺代码", "id", "store_id"),
    name: indexOf("名称", "店铺名称", "name", "store_name"),
    district: indexOf("区域", "district"),
    lng: indexOf("经度", "lng", "longitude"),
    lat: indexOf("纬度", "lat", "latitude"),
    tripCount: indexOf("次数", "tripCount", "trip_count"),
    rpcs: indexOf("RPCS", "rpcs"),
    rcase: indexOf("RCASE", "rcase"),
    bpcs: indexOf("BPCS", "bpcs"),
    bpaper: indexOf("BPAPER", "bpaper"),
    apcs: indexOf("APCS", "apcs"),
    apaper: indexOf("APAPER", "apaper"),
    rpaper: indexOf("RPAPER", "rpaper"),
    coldRatio: indexOf("冷藏比例", "coldRatio", "cold_ratio"),
    waveBelongs: indexOf("所属波次", "waveBelongs", "wave_belongs"),
    wave1Time: indexOf("一配时间", "wave1_time", "first_wave_time"),
    wave2Time: indexOf("二配时间", "wave2_time", "second_wave_time"),
    wave3Time: indexOf("到店时间", "wave3_time", "arrival_time_w3"),
    wave4Time: indexOf("到店时间", "wave4_time", "arrival_time_w4", "arrival_time"),
    serviceMinutes: indexOf("卸货分钟", "serviceMinutes", "service_minutes"),
    difficulty: indexOf("难度", "difficulty"),
    parking: indexOf("允许偏差(分)", "停车", "parking"),
  };
  const getValue = (cells, key) => (idx[key] >= 0 ? cells[idx[key]] : "");
  return lines.slice(1).map((line) => {
    const cells = splitSimpleCsvLine(line);
    return {
      id: getValue(cells, "id"),
      name: getValue(cells, "name"),
      district: getValue(cells, "district"),
      lng: getValue(cells, "lng"),
      lat: getValue(cells, "lat"),
      tripCount: getValue(cells, "tripCount"),
      rpcs: getValue(cells, "rpcs"),
      rcase: getValue(cells, "rcase"),
      bpcs: getValue(cells, "bpcs"),
      bpaper: getValue(cells, "bpaper"),
      apcs: getValue(cells, "apcs"),
      apaper: getValue(cells, "apaper"),
      rpaper: getValue(cells, "rpaper"),
      coldRatio: getValue(cells, "coldRatio"),
      waveBelongs: getValue(cells, "waveBelongs"),
      wave1Time: getValue(cells, "wave1Time"),
      wave2Time: getValue(cells, "wave2Time"),
      wave3Time: getValue(cells, "wave3Time"),
      wave4Time: getValue(cells, "wave4Time"),
      serviceMinutes: getValue(cells, "serviceMinutes"),
      difficulty: getValue(cells, "difficulty"),
      parking: getValue(cells, "parking"),
    };
  });
}

function normalizeImportedStore(raw) {
  const id = normalizeStoreCode(raw?.id || raw?.store_id || raw?.storeId || "");
  const waveBelongs = normalizeWaveBelongsInput(raw?.waveBelongs || raw?.wave_belongs || "1");
  const waveArrivals = {};
  const belongsIds = parseWaveBelongs(waveBelongs);
  const w1 = String(raw?.wave1Time || raw?.wave1_time || raw?.first_wave_time || "").trim();
  const w2 = String(raw?.wave2Time || raw?.wave2_time || raw?.second_wave_time || "").trim();
  const w3 = String(raw?.wave3Time || raw?.wave3_time || raw?.arrival_time_w3 || "").trim();
  const w4 = String(raw?.wave4Time || raw?.wave4_time || raw?.arrival_time_w4 || raw?.arrival_time || "").trim();
  if (belongsIds.includes(1) && w1) waveArrivals.w1 = w1;
  if (belongsIds.includes(2) && w2) waveArrivals.w2 = w2;
  if (belongsIds.includes(3) && w3) waveArrivals.w3 = w3;
  if (belongsIds.includes(4) && w4) waveArrivals.w4 = w4;
  const normalized = {
    id,
    name: String(raw?.name || raw?.store_name || "").trim(),
    district: String(raw?.district || "").trim(),
    lng: Number(raw?.lng || raw?.longitude || 0),
    lat: Number(raw?.lat || raw?.latitude || 0),
    address: String(raw?.address || "").trim(),
    tripCount: Math.max(1, Number(raw?.tripCount || raw?.trip_count || (waveBelongs ? waveBelongs.split(",").length : 1) || 1)),
    waveBelongs,
    rpcs: Number(raw?.rpcs || 0),
    rcase: Number(raw?.rcase || 0),
    bpcs: Number(raw?.bpcs || 0),
    bpaper: Number(raw?.bpaper || 0),
    apcs: Number(raw?.apcs || 0),
    apaper: Number(raw?.apaper || 0),
    rpaper: Number(raw?.rpaper || 0),
    coldRatio: Number(raw?.coldRatio || raw?.cold_ratio || 0),
    desiredArrival: w1 || "",
    waveArrivals,
    serviceMinutes: Math.max(1, Number(raw?.serviceMinutes || raw?.service_minutes || 15)),
    difficulty: Number(raw?.difficulty || 1),
    parking: Number(raw?.parking || 10),
  };
  return syncStoreWaveLoads(normalized);
}

function clearImportSelection(kind) {
  const input = document.getElementById(`${kind}File`);
  if (input) input.value = "";
  setImportFileTag(kind, "");
}

function bindImports() {
  const box = document.getElementById("validationBox");
  const bindImportButton = (kind, onImport) => {
    const input = document.getElementById(`${kind}File`);
    const trigger = document.getElementById(`${kind}FileTrigger`);
    const clearBtn = document.getElementById(`${kind}FileClearBtn`);
    trigger?.addEventListener("click", () => input?.click());
    clearBtn?.addEventListener("click", () => clearImportSelection(kind));
    input?.addEventListener("change", async (event) => {
      const file = event.target.files?.[0];
      setImportFileTag(kind, file?.name || "");
      if (!file) return;
      try {
        await onImport(file);
      } finally {
        input.value = "";
      }
    });
  };
  bindImportButton("vehicle", async (file) => {
    try {
      let parsed = [];
      try {
        parsed = await parseVehicleFileByBackend(file);
      } catch {
        const fallback = await readTextFileWithFallback(file);
        parsed = fallback.parsed;
      }
      if (!parsed.length) {
        box.textContent = L("importVehicleFailed");
        return;
      }
      state.vehicles = parsed.map((v) => ({
        plateNo: String(v.plateNo || "").trim(),
        driverName: String(v.driverName || "").trim(),
        type: String(v.type || ENFORCED_VEHICLE_TYPE),
        capacity: Number(v.capacity || 100),
        speed: 38,
        canCarryCold: Boolean(v.canCarryCold),
      })).filter((v) => v.plateNo);
      state.lastResults = [];
      state.activeResultKey = "";
      renderAll();
      box.textContent = LT("importedVehicles", { count: state.vehicles.length });
    } catch (error) {
      box.textContent = `${L("importVehicleFailed")} ${error?.message || ""}`.trim();
    }
  });
  bindImportButton("store", async (file) => {
    try {
      const text = await file.text();
      const parsed = parseStoreFileText(text).map((item) => normalizeImportedStore(item)).filter((store) => store.id && store.name);
      if (!parsed.length) {
        box.textContent = lang() === "ja" ? "店舗ファイルの解析に失敗しました。" : "门店文件解析失败。";
        return;
      }
  const resolvedMap = await fetchStoreWaveResolvedLoadMap(parsed.map((item) => item?.id).filter(Boolean));
  state.stores = applyStoreWaveResolvedLoadsToStores(parsed, resolvedMap);
      state.waves = buildAutoWaves(state.stores);
      state.lastResults = [];
      state.activeResultKey = "";
      renderAll();
      box.textContent = lang() === "ja" ? `店舗 ${state.stores.length} 件を導入し、ローカル折算貨量を適用しました。` : `已导入门店 ${state.stores.length} 家，并已应用本地折算货量。`;
    } catch (error) {
      box.textContent = `${lang() === "ja" ? "门店導入失敗" : "门店导入失败"} ${error?.message || ""}`.trim();
    }
  });
}
function bindEditing() {
    document.body.addEventListener("click", (event) => {
      const storeWaveSortBtn = event.target.closest?.("[data-store-wave-sort]");
      if (storeWaveSortBtn) {
        toggleStoreWaveResolvedSort(storeWaveSortBtn.dataset.storeWaveSort || "");
        return;
      }
      const sortBtn = event.target.closest?.("[data-table-sort]");
      if (sortBtn) {
        toggleDataTableSort(sortBtn.dataset.tableKind || "store", sortBtn.dataset.tableSort);
        return;
      }
    });
    document.body.addEventListener("input", (event) => {
      const t = event.target;
      if (t?.id === "storeSearchInput") {
        state.ui.storeSearchQuery = t.value || "";
        return;
      }
      if (t?.id === "vehicleSearchInput") {
        state.ui.vehicleSearchQuery = t.value || "";
        return;
      }
      if (t?.id === "waveSearchInput") {
        state.ui.waveSearchQuery = t.value || "";
        renderWaves();
        return;
      }
      if (t?.id === "waveStoreSearchInput") {
        state.ui.waveStoreSearchQuery = t.value || "";
        renderWaves();
        return;
      }
      if (!t.dataset.kind) return;
    const list = t.dataset.kind === "vehicle" ? state.vehicles : t.dataset.kind === "store" ? state.stores : state.waves;
    const item = list[Number(t.dataset.index)];
    const value = t.type === "checkbox"
      ? t.checked
      : (t.tagName === "SELECT" && t.multiple
        ? Array.from(t.selectedOptions || []).map((opt) => normalizeStoreCode(opt.value)).filter(Boolean).join(",")
        : t.value);
    if (t.dataset.kind === "store" && (t.dataset.field === "waveW1" || t.dataset.field === "waveW2" || t.dataset.field === "waveW4")) {
      item.waveArrivals = { ...(item.waveArrivals || {}) };
      if (t.dataset.field === "waveW1") {
        item.waveArrivals.w1 = value || "";
        item.desiredArrival = value || item.desiredArrival;
      } else if (t.dataset.field === "waveW4") {
        if (value) item.waveArrivals.w4 = value;
        else delete item.waveArrivals.w4;
      } else if (value) {
        item.waveArrivals.w2 = value;
      } else {
        delete item.waveArrivals.w2;
      }
    } else if (t.dataset.kind === "store" && t.dataset.field === "waveBelongs") {
      item.waveBelongs = normalizeWaveBelongsInput(value);
      if (t.value !== item.waveBelongs) t.value = item.waveBelongs;
    } else if (t.dataset.kind === "store" && t.dataset.field === "tripCount") {
      item.tripCount = Math.max(1, Number(value || 1));
      syncStoreWaveLoads(item);
      if (Number(t.value || 0) !== item.tripCount) t.value = String(item.tripCount);
    } else {
      item[t.dataset.field] = value;
    }
    if (t.dataset.kind === "wave" && t.dataset.field === "storeIds") {
      renderWaves();
    }
    if (t.dataset.kind === "store") renderSingleWaveInfo();
  });
  document.body.addEventListener("change", (event) => {
    const t = event.target;
    if (t?.matches?.("[data-wave-store-index]")) {
      const waveIndex = Number(t.dataset.waveStoreIndex || 0);
      const storeId = normalizeStoreCode(t.dataset.storeId || "");
      const wave = state.waves[waveIndex];
      if (wave && storeId) {
        const picked = new Set(parseStoreIds(wave.storeIds).map((id) => normalizeStoreCode(id)));
        if (t.checked) picked.add(storeId);
        else picked.delete(storeId);
        wave.storeIds = Array.from(picked).join(",");
      }
      renderWaves();
      return;
    }
    if (!t?.dataset?.kind) return;
    if (t.tagName === "SELECT" && t.multiple) {
      t.dispatchEvent(new Event("input", { bubbles: true }));
    }
  });
  document.getElementById("dispatchStartTimeInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("maxRouteKmInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("minLoadRateInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("targetScoreInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("ignoreWavesInput").addEventListener("change", syncSettingsFromUI);
  document.getElementById("singleWaveDistanceInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("singleWaveStartInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("singleWaveEndInput").addEventListener("input", syncSettingsFromUI);
  document.getElementById("singleWaveEndModeInput").addEventListener("change", syncSettingsFromUI);
  const strategyInputBindings = [
    ["strategyDeliveryModeSelect", "change"],
    ["strategyOptimizeGoalSelect", "change"],
    ["strategyLoadDistanceBiasInput", "input"],
    ["strategyLatenessToleranceSelect", "change"],
    ["strategyVehicleCostBiasInput", "input"],
    ["strategyDualWaveWeightInput", "input"],
    ["strategyCrossRegionPenaltyWeightInput", "input"],
    ["strategyWaveDelayPenaltyInput", "input"],
    ["strategyLateRouteStrengthSelect", "change"],
    ["strategyDifficultyModeSelect", "change"],
    ["strategyDifficultyTier1UnlimitedInput", "change"],
    ["strategyDifficultyTier1LimitInput", "input"],
    ["strategyDifficultyTier2UnlimitedInput", "change"],
    ["strategyDifficultyTier2LimitInput", "input"],
    ["strategyDifficultyTier3UnlimitedInput", "change"],
    ["strategyDifficultyTier3LimitInput", "input"],
    ["strategyDifficultyScoreLimitInput", "input"],
  ];
  strategyInputBindings.forEach(([id, eventName]) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener(eventName, () => {
      if (id === "strategyLoadDistanceBiasInput") {
        state.strategyConfigTouched.loadDistanceBias = true;
      }
      syncSettingsFromUI();
    });
  });
  document.getElementById("solveStrategySelect")?.addEventListener("change", (event) => {
    applySolveStrategy(event.target.value || "manual");
  });
  document.getElementById("optimizeGoalSelect")?.addEventListener("change", (event) => {
    applySolveStrategy(state.settings.solveStrategy || "quick", event.target.value || "balanced");
    syncSettingsFromUI();
  });
  document.getElementById("languageSelect").addEventListener("change", (event) => {
    state.language = event.target.value || "zh";
    applyLanguage();
    renderAll();
  });
  document.getElementById("toggleStorePanelBtn").addEventListener("click", () => {
    const body = document.getElementById("storePanelBody");
    body.classList.toggle("collapsed");
    document.getElementById("toggleStorePanelBtn").textContent = body.classList.contains("collapsed") ? L("unfoldStore") : L("foldStore");
  });
  document.getElementById("toggleVehiclePanelBtn").addEventListener("click", () => {
    const body = document.getElementById("vehiclePanelBody");
    body.classList.toggle("collapsed");
    document.getElementById("toggleVehiclePanelBtn").textContent = body.classList.contains("collapsed") ? L("unfoldVehicle") : L("foldVehicle");
  });
  document.body.addEventListener("click", (event) => {
    const solvePreviewBtn = event.target.closest("[data-solve-preview]");
    if (solvePreviewBtn) {
      applySolveStrategy(solvePreviewBtn.dataset.solvePreview || "quick", state.settings.optimizeGoal || "balanced");
      return;
    }
    const goalPreviewBtn = event.target.closest("[data-goal-preview]");
    if (goalPreviewBtn) {
      applySolveStrategy(state.settings.solveStrategy || "quick", goalPreviewBtn.dataset.goalPreview || "balanced");
      return;
    }
    const showcaseBtn = event.target.closest("#openShowcaseBtn");
    if (showcaseBtn) {
      openShowcaseModal();
      return;
    }
    if (event.target.closest("#wmsFetchBtn")) {
      void triggerWmsFetch();
      return;
    }
    if (event.target.closest("#calcWaveLoadBtn")) {
      openLoadConvertModal();
      return;
    }
    if (event.target.closest("#saveDualStoreLoadsBtn")) {
      void (async () => {
        const box = document.getElementById("validationBox");
        try {
          const result = await saveDualTableLoadsToBackend();
          await queryStoreWaveResolvedTable();
          showSaveDualLoadsReport(result?.reportText || "");
          if (box) box.textContent = `双表货量已保存：${Number(result?.upserted || 0)} 条`;
        } catch (error) {
          if (box) box.textContent = `保存双表货量失败：${error?.message || ""}`.trim();
        }
      })();
      return;
    }
    const storeLocateBtn = event.target.closest("#storeLocateBtn");
    if (storeLocateBtn) {
      const query = String(document.getElementById("storeSearchInput")?.value || state.ui.storeSearchQuery || "").trim();
      const found = locateStoreRow(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `已定位门店：${query}` : `未找到门店：${query}`) : "请输入门店检索关键字。";
      return;
    }
    const vehicleLocateBtn = event.target.closest("#vehicleLocateBtn");
    if (vehicleLocateBtn) {
      const query = String(document.getElementById("vehicleSearchInput")?.value || state.ui.vehicleSearchQuery || "").trim();
      const found = locateVehicleRow(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `已定位车辆：${query}` : `未找到车辆：${query}`) : "请输入车辆检索关键字。";
      return;
    }
    const waveLocateBtn = event.target.closest("#waveLocateBtn");
    if (waveLocateBtn) {
      const query = String(document.getElementById("waveSearchInput")?.value || state.ui.waveSearchQuery || "").trim();
      const found = locateWaveItem(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `已定位波次：${query}` : `未找到波次：${query}`) : "请输入波次检索关键字。";
      return;
    }
    if (event.target.closest("#dataArchiveQueryBtn")) {
      void queryDataArchives();
      return;
    }
    const archiveViewBtn = event.target.closest("[data-data-archive-view]");
    if (archiveViewBtn) {
      dataArchiveCache.selectedId = String(archiveViewBtn.dataset.dataArchiveView || "");
      renderDataArchivePanels();
      return;
    }
    const waveSelectBtn = event.target.closest("[data-wave-select-index]");
    if (waveSelectBtn) {
      state.ui.waveSelectedIndex = Number(waveSelectBtn.dataset.waveSelectIndex || 0);
      renderWaves();
      return;
    }
    const waveSelectAllBtn = event.target.closest("[data-wave-select-all]");
    if (waveSelectAllBtn) {
      const waveIndex = Number(waveSelectAllBtn.dataset.waveSelectAll || 0);
      const wave = state.waves[waveIndex];
      if (wave) {
        const selected = new Set(parseStoreIds(wave.storeIds).map((id) => normalizeStoreCode(id)));
        const query = String(state.ui.waveStoreSearchQuery || "").trim().toLowerCase();
        state.stores.forEach((store) => {
          const storeId = normalizeStoreCode(store.id);
          if (!storeId) return;
          const hay = `${storeId} ${store.name || ""}`.toLowerCase();
          if (!query || hay.includes(query)) selected.add(storeId);
        });
        wave.storeIds = Array.from(selected).join(",");
      }
      renderWaves();
      return;
    }
    const waveClearAllBtn = event.target.closest("[data-wave-clear-all]");
    if (waveClearAllBtn) {
      const waveIndex = Number(waveClearAllBtn.dataset.waveClearAll || 0);
      if (state.waves[waveIndex]) state.waves[waveIndex].storeIds = "";
      renderWaves();
      return;
    }
    const saveStoreBtn = event.target.closest("#saveStoreDataBtn");
    if (saveStoreBtn) {
      syncSettingsFromUI();
      void (async () => {
        const box = document.getElementById("validationBox");
        try {
          const archiveId = await saveBaseDataSnapshot("store");
          if (box) box.textContent = `门店资料已保存到后台档案：${archiveId}`;
        } catch (error) {
          if (box) box.textContent = `门店资料保存失败：${error?.message || ""}`.trim();
        }
      })();
      return;
    }
    const saveVehicleBtn = event.target.closest("#saveVehicleDataBtn");
    if (saveVehicleBtn) {
      syncSettingsFromUI();
      void (async () => {
        const box = document.getElementById("validationBox");
        try {
          const archiveId = await saveBaseDataSnapshot("vehicle");
          if (box) box.textContent = `车辆资料已保存到后台档案：${archiveId}`;
        } catch (error) {
          if (box) box.textContent = `车辆资料保存失败：${error?.message || ""}`.trim();
        }
      })();
      return;
    }
    const saveWaveBtn = event.target.closest("#saveWaveDataBtn");
    if (saveWaveBtn) {
      syncSettingsFromUI();
      void (async () => {
        const box = document.getElementById("validationBox");
        try {
          const archiveId = await saveBaseDataSnapshot("wave");
          if (box) box.textContent = `波次资料已保存到后台档案：${archiveId}`;
        } catch (error) {
          if (box) box.textContent = `波次资料保存失败：${error?.message || ""}`.trim();
        }
      })();
      return;
    }
    const resultBtn = event.target.closest(".view-result-detail");
    if (resultBtn) {
      state.activeResultKey = resultBtn.dataset.resultKey || "";
      state.ui.resultDetailExpanded = false;
      state.ui.resultDetailWaveIds = [];
      renderSummary();
      renderAnalytics();
      renderResults();
      renderStoresTable();
      document.getElementById("resultPanels")?.scrollIntoView?.({ behavior: "smooth", block: "start" });
      return;
    }
    const toggleResultWaveBtn = event.target.closest(".toggle-result-wave-filter");
    if (toggleResultWaveBtn) {
      const result = state.lastResults.find((item) => item.key === (state.activeResultKey || state.lastResults[0]?.key)) || state.lastResults[0];
      const waveIds = (result?.scenario?.waves || []).map((wave) => String(wave.waveId || "")).filter(Boolean);
      const selected = new Set((state.ui.resultDetailWaveIds || []).map((id) => String(id || "")).filter((id) => waveIds.includes(id)));
      const mode = String(toggleResultWaveBtn.dataset.waveFilterMode || "");
      if (mode === "all") {
        const allSelected = waveIds.length > 0 && waveIds.every((id) => selected.has(id));
        state.ui.resultDetailWaveIds = allSelected ? [] : [...waveIds];
      } else {
        const waveId = String(toggleResultWaveBtn.dataset.waveId || "");
        if (waveId) {
          if (selected.has(waveId)) selected.delete(waveId);
          else selected.add(waveId);
        }
        state.ui.resultDetailWaveIds = [...selected];
      }
      renderResults();
      return;
    }
    const toggleResultDetailBtn = event.target.closest(".toggle-result-detail");
    if (toggleResultDetailBtn) {
      state.ui.resultDetailExpanded = !state.ui.resultDetailExpanded;
      renderResults();
      document.getElementById("resultPanels")?.scrollIntoView?.({ behavior: "smooth", block: "start" });
      return;
    }
    if (event.target.closest(".toggle-assistant-panel")) {
      state.ui.assistantExpanded = !state.ui.assistantExpanded;
      renderAnalytics();
      return;
    }
    const dockToggle = event.target.closest(".assistant-dock-toggle");
    if (dockToggle) {
      state.ui.assistantDockState = String(dockToggle.dataset.assistantDockState || "half");
      renderAnalytics();
      return;
    }
    const collapsedDock = event.target.closest("#assistantDock.state-collapsed");
    if (collapsedDock && !event.target.closest(".assistant-dock-toggle")) {
      state.ui.assistantDockState = "half";
      renderAnalytics();
      return;
    }
    const saveBtn = event.target.closest(".save-current-result");
    if (saveBtn) {
      saveCurrentPlan();
      return;
    }
    const archivePageBtn = event.target.closest(".archive-page-btn");
    if (archivePageBtn) {
      state.ui.archivePage = Number(archivePageBtn.dataset.archivePage || 1);
      renderSavedPlans();
      return;
    }
    if (event.target.closest("#waveInitialSolveBtn")) {
      void runSingleWaveInitialSolve();
      return;
    }
    if (event.target.closest("#waveResolveBtn")) {
      void runSingleWaveResolve();
      return;
    }
    const previewArchiveBtn = event.target.closest(".preview-archive-btn");
    if (previewArchiveBtn) {
      void restoreArchivedRun(previewArchiveBtn.dataset.archiveId, "preview");
      return;
    }
    const adoptArchiveBtn = event.target.closest(".adopt-archive-btn");
    if (adoptArchiveBtn) {
      void restoreArchivedRun(adoptArchiveBtn.dataset.archiveId, "adopt");
      return;
    }
    const rescheduleBtn = event.target.closest(".reschedule-again");
    if (rescheduleBtn) {
      rescheduleUnscheduledStores(rescheduleBtn.dataset.result);
      return;
    }
    const exportLiveUnscheduledBtn = event.target.closest(".export-live-unscheduled");
    if (exportLiveUnscheduledBtn) {
      exportLiveUnscheduledIds(exportLiveUnscheduledBtn.dataset.result);
      return;
    }
    const processBtn = event.target.closest(".open-process");
    if (processBtn) {
      openProcessModal(processBtn.dataset.result, processBtn.dataset.wave, processBtn.dataset.plate, processBtn.dataset.trip);
      return;
    }
    const algoCard = event.target.closest("[data-algo-card]");
    if (algoCard) {
      const strategy = state.settings.solveStrategy || "manual";
      if (strategy !== "free") return;
      const key = String(algoCard.dataset.algoCard || "").trim();
      if (!ALGORITHM_ORDER.includes(key)) return;
      const selected = getEffectiveFreeAlgorithms();
      const has = selected.includes(key);
      let next = [];
      if (has) {
        next = selected.filter((item) => item !== key);
      } else {
        if (selected.length >= MAX_FREE_SOLVE_ALGOS) {
          const box = document.getElementById("validationBox");
          if (box) {
            box.textContent = lang() === "ja"
              ? `自由求解では最大 ${MAX_FREE_SOLVE_ALGOS} 本まで選択できます。`
              : `自由求解最多选择 ${MAX_FREE_SOLVE_ALGOS} 种算法。`;
          }
          return;
        }
        next = [...selected, key];
      }
      state.settings.customAlgorithms = next;
      setAlgorithmsChecked(next);
      const hint = document.getElementById("strategyHint");
      if (hint) hint.textContent = buildStrategyHint();
      renderAlgorithmPool();
      return;
    }
    if (event.target.closest("#quickSolveBtn")) {
      void openSolveDiagnoseAndPause("quick", "balanced");
      return;
    }
    if (event.target.closest("#deepOptimizeBtn")) {
      void openSolveDiagnoseAndPause("deep", "ontime");
      return;
    }
    if (event.target.closest("#globalSearchBtn")) {
      void openSolveDiagnoseAndPause("global", "distance");
      return;
    }
    if (event.target.closest("#relaySolveBtn")) {
      void openSolveDiagnoseAndPause("relay", "balanced");
      return;
    }
    if (event.target.closest("#freeSolveBtn")) {
      void openSolveDiagnoseAndPause("free", state.settings.optimizeGoal || "balanced");
      return;
    }
    if (event.target.closest("#multiCompareBtn")) {
      void openSolveDiagnoseAndPause("compare", "balanced");
      return;
    }
    const routeMapBtn = event.target.closest(".open-route-map");
    if (routeMapBtn) {
      openRouteMapModal(routeMapBtn.dataset.result, routeMapBtn.dataset.wave, routeMapBtn.dataset.plate, routeMapBtn.dataset.trip);
      return;
    }
    const speechBtn = event.target.closest(".speech-trigger");
    if (speechBtn) {
      triggerSpeech(speechBtn.dataset.speech || "");
      return;
    }
    const listenBtn = event.target.closest(".speech-listen-trigger");
    if (listenBtn) {
      void startAssistantListening();
      return;
    }
    const saveDeepSeekBtn = event.target.closest(".save-deepseek-config");
    if (saveDeepSeekBtn) {
      state.ai.deepseekApiKey = String(document.getElementById("deepseekApiKeyInput")?.value || "").trim();
      state.ai.deepseekModel = String(document.getElementById("deepseekModelSelect")?.value || "deepseek-chat");
      state.ai.mode = String(document.getElementById("deepseekModeSelect")?.value || "dispatch");
      state.ai.answerStyle = String(document.getElementById("deepseekStyleSelect")?.value || "brief");
      saveDeepSeekSettings();
      const box = document.getElementById("validationBox");
      if (box) box.textContent = state.ai.deepseekApiKey ? L("deepseekSaved") : L("deepseekMissingKey");
      renderAnalytics();
      return;
    }
    const askDeepSeekBtn = event.target.closest(".ask-deepseek-btn");
    if (askDeepSeekBtn) {
      const question = String(document.getElementById("assistantQuestionInput")?.value || "").trim();
      state.ai.questionDraft = question;
      void submitAssistantQuestion(question);
      return;
    }
    if (event.target.closest("[data-close-modal]") || event.target.id === "closeProcessModalBtn") {
      closeProcessModal();
      return;
    }
    if (event.target.closest("[data-close-showcase]") || event.target.id === "closeShowcaseModalBtn") {
      closeShowcaseModal();
      return;
    }
    if (event.target.closest("[data-close-route-map]") || event.target.id === "closeRouteMapModalBtn") {
      closeRouteMapModal();
      return;
    }
    if (event.target.closest("[data-close-relay-console]") || event.target.id === "closeRelayConsoleBtn") {
      closeRelayConsoleModal();
      return;
    }
    if (event.target.closest("[data-close-load-convert]") || event.target.id === "closeLoadConvertModalBtn") {
      closeLoadConvertModal();
      return;
    }
    if (event.target.closest("[data-close-solve-diagnose]") || event.target.id === "closeSolveDiagnoseModalBtn" || event.target.id === "cancelSolveDiagnoseBtn") {
      pendingSolveContinuation = null;
      closeSolveDiagnoseModal();
      return;
    }
    if (event.target.id === "continueSolveDiagnoseBtn") {
      const continuation = pendingSolveContinuation;
      closeSolveDiagnoseModal();
      if (continuation) {
        void continuation();
      }
      return;
    }
    const adjustBtn = event.target.closest(".apply-adjust");
    if (adjustBtn) {
      const selector = document.querySelector(`.adjust-target[data-result="${adjustBtn.dataset.result}"][data-wave="${adjustBtn.dataset.wave}"][data-store="${adjustBtn.dataset.store}"][data-source="${adjustBtn.dataset.source}"]`);
      applyStoreTransfer(adjustBtn.dataset.result, adjustBtn.dataset.wave, adjustBtn.dataset.source, selector?.value || "", adjustBtn.dataset.store);
      return;
    }
    const assignBtn = event.target.closest(".apply-unscheduled-assign");
    if (assignBtn) {
      const selector = document.querySelector(`.unscheduled-target[data-result="${assignBtn.dataset.result}"][data-wave="${assignBtn.dataset.wave}"][data-store="${assignBtn.dataset.store}"]`);
      const plate = selector?.value || "";
      const result = getResultByKey(assignBtn.dataset.result);
      const box = document.getElementById("validationBox");
      if (!plate) {
        box.textContent = L("noAssignableVehicle");
        return;
      }
      const outcome = tryAssignStoreToSpecificPlan(result, assignBtn.dataset.wave, plate, assignBtn.dataset.store);
      box.textContent = outcome.message || "";
      renderVehicles();
      renderSummary();
      renderAnalytics();
      renderResults();
      renderStoresTable();
      return;
    }
    const btn = event.target.closest("[data-remove]");
    if (!btn) return;
    const list = btn.dataset.remove === "vehicle" ? state.vehicles : btn.dataset.remove === "store" ? state.stores : state.waves;
    list.splice(Number(btn.dataset.index), 1);
    renderAll();
  });
  document.body.addEventListener("input", (event) => {
    if (event.target.id === "assistantQuestionInput") {
      state.ai.questionDraft = event.target.value || "";
      return;
    }
    if (event.target.id === "deepseekApiKeyInput") {
      state.ai.deepseekApiKey = event.target.value || "";
      return;
    }
  });
  document.body.addEventListener("change", (event) => {
    if (event.target.matches(".algo-check")) {
      if ((state.settings.solveStrategy || "manual") !== "free") {
        syncAlgorithmControls();
        return;
      }
      const beforeCount = getCheckedAlgorithmsFromUI().length;
      if (beforeCount > MAX_FREE_SOLVE_ALGOS) {
        event.target.checked = false;
      }
      const selected = normalizeCustomAlgorithms(getCheckedAlgorithmsFromUI());
      state.settings.customAlgorithms = selected;
      setAlgorithmsChecked(selected);
      const hint = document.getElementById("strategyHint");
      if (hint) hint.textContent = buildStrategyHint();
      renderAlgorithmPool();
      const box = document.getElementById("validationBox");
      if (beforeCount > MAX_FREE_SOLVE_ALGOS && box) {
        box.textContent = lang() === "ja"
          ? `自由求解では最大 ${MAX_FREE_SOLVE_ALGOS} 本まで選択できます。`
          : `自由求解最多选择 ${MAX_FREE_SOLVE_ALGOS} 种算法。`;
      }
      return;
    }
    if (event.target.id === "archiveDateFilterInput") {
      state.ui.archiveDateFilter = event.target.value || todayDateKey();
      state.ui.archivePage = 1;
      renderSavedPlans();
      return;
    }
    if (event.target.id === "storeSourceSelect") {
      state.ui.storeDataSource = event.target.value === "real" ? "real" : "sample";
      localStorage.setItem("storeDataSource", state.ui.storeDataSource);
      void refreshBaseDataBySource();
      return;
    }
    if (event.target.id === "vehicleSourceSelect") {
      state.ui.vehicleDataSource = event.target.value === "real" ? "real" : "sample";
      localStorage.setItem("vehicleDataSource", state.ui.vehicleDataSource);
      void refreshBaseDataBySource();
      return;
    }
    if (event.target.id === "waveSolverWaveSelect") {
      state.ui.waveSolverWaveId = event.target.value || "";
      renderWaveSolverPanel();
      return;
    }
    if (event.target.id === "solveWaveAllInput") {
      state.ui.solveWaveSelectedIds = event.target.checked ? ["ALL"] : normalizeSolveWaveSelection([], getConfiguredWaveOptions());
      renderWaveSolverPanel();
      return;
    }
    if (event.target.matches(".solve-wave-option-input")) {
      const waveId = String(event.target.dataset.solveWaveId || "").trim();
      const current = normalizeSolveWaveSelection(state.ui.solveWaveSelectedIds, getConfiguredWaveOptions());
      const nextSet = new Set(current.includes("ALL") ? [] : current);
      if (event.target.checked) nextSet.add(waveId);
      else nextSet.delete(waveId);
      state.ui.solveWaveSelectedIds = nextSet.size ? Array.from(nextSet) : ["ALL"];
      renderWaveSolverPanel();
      return;
    }
    if (event.target.id === "waveSolverAlgoSelect") {
      state.ui.waveSolverAlgo = event.target.value || "hybrid";
      renderWaveSolverPanel();
      return;
    }
    if (event.target.id === "deepseekModelSelect") {
      state.ai.deepseekModel = event.target.value || "deepseek-chat";
      return;
    }
    if (event.target.id === "deepseekModeSelect") {
      state.ai.mode = event.target.value || "dispatch";
      return;
    }
    if (event.target.id === "deepseekStyleSelect") {
      state.ai.answerStyle = event.target.value || "brief";
    }
  });
  [
    { id: "storeSearchInput", locate: () => locateStoreRow(state.ui.storeSearchQuery || "") },
    { id: "vehicleSearchInput", locate: () => locateVehicleRow(state.ui.vehicleSearchQuery || "") },
    { id: "waveSearchInput", locate: () => locateWaveItem(state.ui.waveSearchQuery || "") },
  ].forEach((item) => {
    const input = document.getElementById(item.id);
    if (!input) return;
    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      item.locate();
    });
  });
  document.body.addEventListener("mousedown", startAssistantDockDrag);
  document.addEventListener("mousemove", moveAssistantDockDrag);
  document.addEventListener("mouseup", stopAssistantDockDrag);
  document.addEventListener("mouseleave", stopAssistantDockDrag);
  const archiveDateInput = document.getElementById("dataArchiveDateInput");
  if (archiveDateInput && !archiveDateInput.value) archiveDateInput.value = todayDateKey();
}
async function generatePlans() {
  if (state.ui.generating) {
    const box = document.getElementById("validationBox");
    if (box) {
      box.textContent = lang() === "ja"
        ? "既に求解中です。リレーウィンドウの進捗を確認してください。"
        : "当前已在求解中，请查看“求解过程可视化窗口”进度。";
    }
    return;
  }
  state.ui.generating = true;
  renderGenerationProgress();
  try {
    syncSettingsFromUI();
    const box = document.getElementById("validationBox");
    const strategy = state.settings.solveStrategy || "manual";
    let selected = strategyPreset(strategy, state.settings.optimizeGoal || "balanced").algorithms;
    if (strategy === "free") {
      state.settings.customAlgorithms = getEffectiveFreeAlgorithms();
      selected = [...state.settings.customAlgorithms];
    }
    const error = validateInput();
    if (error) {
      box.textContent = error;
      state.ui.generating = false;
      renderGenerationProgress();
      return;
    }
    if (!selected.length) {
      box.textContent = strategy === "free"
        ? (lang() === "ja" ? "自由求解では少なくとも 1 本のアルゴリズムを選択してください。" : "自由求解请至少勾选 1 种算法。")
        : (lang() === "ja" ? "現在の方針に対応するアルゴリズムチェーンが設定されていません。" : "请先为当前策略内置至少 1 条算法链。");
      state.ui.generating = false;
      renderGenerationProgress();
      return;
    }
  // EN: Business note for nearby logic.
  // CN: 附近逻辑的业务提示。

  openRelayConsoleModal(lang() === "ja" ? "求解过程可视化窗口" : "求解过程可视化窗口");
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  收到求解请求，开始同步前台设置与基础校验。`);
  relayStageReporter = (text) => appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${text}`);
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  当前策略：${state.settings.solveStrategy || "manual"}，目标：${state.settings.optimizeGoal || "balanced"}，算法：${selected.join("、") || "（空）"}。`);
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  正在构建场景（门店/车辆/波次/路网），这一步可能耗时较长。`);
  const scenario = await buildScenario();
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  场景构建完成：门店 ${scenario?.stores?.length || 0} 家，车辆 ${scenario?.vehicles?.length || 0} 台，波次 ${scenario?.waves?.length || 0} 个。`);
  const runners = { vrptw: solveByVRPTW, hybrid: solveByHybrid, ga: solveByGA, tabu: solveByTabu, lns: solveByLNS, savings: solveBySavings, sa: solveBySA, aco: solveByACO, pso: solveByPSO, vehicle: solveByVehicle };
  const carryResult = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0] || null;
  const requiredTimingIssues = [];
  for (const wave of scenario.waves || []) {
    for (const store of scenario.stores || []) {
      if (!isStoreInWave(store, wave)) continue;
      const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
      if (!timing?.desiredArrival) {
        requiredTimingIssues.push(`${store.id || "-"}@${wave.waveId || "-"}`);
      }
    }
  }
  if (requiredTimingIssues.length) {
    if (box) {
      box.textContent = `${lang() === "ja" ? "求解前检查未通过：" : "求解前检查未通过："}${requiredTimingIssues.slice(0, 15).join("、")}${requiredTimingIssues.length > 15 ? "..." : ""}`;
    }
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  求解前检查未通过：${requiredTimingIssues.slice(0, 30).join("、")}${requiredTimingIssues.length > 30 ? "..." : ""}`);
    state.ui.generating = false;
    renderGenerationProgress();
    return;
  }
  state.lastResults = [];
  state.activeResultKey = "";
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${lang() === "ja" ? "开始求解，正在准备场景与校验..." : "开始求解，正在准备场景与校验..."}`);
  await updateGenerationProgress(6, L("progressPreparing"));
  const built = [];
  const ALGO_TIMEOUT_MS = 120000;
  const runWithTimeout = async (algoName, task) => {
    let timer = null;
    try {
      return await Promise.race([
        task(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error(`ALGO_TIMEOUT:${algoName}:${ALGO_TIMEOUT_MS}`)), ALGO_TIMEOUT_MS);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  };

  try {
    if (strategy === "relay") {
      appendRelayConsoleLine(lang() === "ja" ? "リレー最適化のウィンドウを開きました。ここに各段階の動きが順次表示されます。" : "接力求解窗口已打开，下面会持续打印每一步动作。");
      await updateGenerationProgress(24, lang() === "ja" ? "リレー最適化を起動しています..." : "正在启动接力求解...");
      try {
        built.push({ ...(await runWithTimeout("relay", () => solveByRelay(scenario, selected, carryResult))), scenario, adjustMessage: "" });
      } catch (error) {
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  接力求解失败或超时：${error?.message || error}`);
        if (box) box.textContent = `接力求解失败或超时：${error?.message || error}`;
      }
      await cooperativeYield();
      await updateGenerationProgress(75, lang() === "ja" ? "リレー段階が完了し、結果をまとめています..." : "接力阶段已完成，正在汇总结果...");
    } else if (strategy === "compare") {
      appendRelayConsoleLine(lang() === "ja" ? "多算法対比を起動しました。ここに各算法の進行と剖析が順次表示されます。" : "多算法对比已启动。下面会持续打印每套算法的运行进度与剖析。");
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  开始运行 ${algoLabel(key)}。`);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let result = null;
        try {
          result = { ...(await runWithTimeout(key, () => runners[key](scenario))), scenario, adjustMessage: "" };
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 失败或超时：${error?.message || error}`);
          await cooperativeYield();
          await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
          continue;
        }
        built.push(result);
        state.lastResults = [...built].sort((a, b) => b.metrics.score - a.metrics.score);
        state.activeResultKey = state.lastResults[0]?.key || "";
        box.textContent = `${buildSolveModeSummary()} ${lang() === "ja" ? `現在已完成 ${built.length}/${selected.length} 本算法，最新完成的是 ${algoLabel(key)}。` : `当前已完成 ${built.length}/${selected.length} 套算法，最新完成的是 ${algoLabel(key)}。`}`;
        renderVehicles();
        renderSummary();
        renderAnalytics();
        renderResults();
        renderStoresTable();
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 已完成，当前评分 ${result.metrics.score.toFixed(1)}，总里程 ${result.metrics.totalDistance.toFixed(1)} km，用车 ${result.metrics.usedVehicleCount || 0} 辆。`);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    } else if (strategy === "deep" && carryResult?.solution?.length) {
      appendRelayConsoleLine(lang() === "ja" ? "深度最適化モード：既存解の波次改善を開始。" : "深度优化模式：开始在已有解上做波次优化。");
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        const optimizer = ({ hybrid: optimizeWaveWithHybrid, tabu: optimizeWaveWithTabu, lns: optimizeWaveWithLns, sa: optimizeWaveWithSA, ga: optimizeWaveWithGA, aco: optimizeWaveWithACO, pso: optimizeWaveWithPSO })[key];
        if (!optimizer) continue;
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  开始执行深度优化算法 ${algoLabel(key)}。`);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let improved = null;
        try {
          improved = await runWithTimeout(`deep-${key}`, () => improveSolutionByWaveOptimizer(carryResult.solution, scenario, optimizer, 40 + i));
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 深度优化失败或超时：${error?.message || error}`);
          await cooperativeYield();
          await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
          continue;
        }
        built.push(applyFinalRuleToResult({
          key,
          label: algoLabel(key),
          description: algoDescription(key),
          solution: improved.solution,
          traceLog: improved.traceLog || [],
          scenario,
          adjustMessage: "",
        }, scenario));
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 深度优化完成，评分 ${built[built.length - 1]?.metrics?.score?.toFixed?.(1) ?? "-"}`);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    } else {
      appendRelayConsoleLine(lang() === "ja" ? "标准求解模式：逐个算法执行。" : "标准求解模式：逐个算法执行。");
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  开始运行 ${algoLabel(key)}。`);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let result = null;
        try {
          result = { ...(await runWithTimeout(key, () => runners[key](scenario))), scenario, adjustMessage: "" };
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 失败或超时：${error?.message || error}`);
          await cooperativeYield();
          await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
          continue;
        }
        built.push(result);
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 已完成，评分 ${result.metrics.score.toFixed(1)}，总里程 ${result.metrics.totalDistance.toFixed(1)} km。`);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    }
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  求解计算阶段结束，开始汇总结果。`);
  } finally {
    relayStageReporter = null;
  }

  state.lastResults = built.sort((a, b) => b.metrics.score - a.metrics.score);
  state.activeResultKey = state.lastResults[0]?.key || "";
  if (!state.lastResults.length) {
    const issues = diagnoseScenarioFeasibility(scenario);
    box.textContent = issues.length
      ? `${buildSolveModeSummary()} ${state.settings.ignoreWaves ? (lang() === "ja" ? "現在は波次を分けずに計算しています。" : "当前按忽略波次模式求解") : (lang() === "ja" ? "現在は波次ごとに計算しています。" : "当前按波次求解")}，${lang() === "ja" ? "現在の条件では実行可能な案が見つかりません。想定される制約衝突：" : "暂未找到可行方案。可能的硬约束冲突："}${issues.join("；")}`
      : `${buildSolveModeSummary()} ${lang() === "ja" ? "現在の制約では実行可能な案が見つかりません。" : "当前约束下暂未找到可行方案。"}`;
    await updateGenerationProgress(100, L("progressDone"));
    renderVehicles();
    renderSummary();
    renderAnalytics();
    renderResults();
    renderStoresTable();
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  未找到可行解，已输出约束冲突提示。`);
    return;
  }

  autoArchiveCurrentRun();
  state.ui.archivePage = 1;
  await updateGenerationProgress(88, L("progressFinishing"));
  const bestResult = state.lastResults[0];
  const targetScore = Number(state.settings.targetScore || 0);
  const routeSourceMessage = scenario.distanceSource === "database-full"
    ? (lang() === "ja"
      ? ` 現在は数据库全量距离矩阵を使用中です。主数据源=${String(scenario?.distDbStats?.dbDominantSource || "unknown")}、欠損 0。`
      : ` 当前使用数据库全量距离矩阵，主数据源=${String(scenario?.distDbStats?.dbDominantSource || "unknown")}，缺失 0。`)
    : scenario.distanceSource === "amap"
    ? (lang() === "ja"
      ? ` 現在は高德の道路距離と所要時間を使用中です。キャッシュ命中 ${scenario.distanceCacheHitPairs || 0} 件、今回の新規取得 ${scenario.distanceFetchedPairs || 0} 件。`
      : ` 当前使用高德路网距离/时长，缓存命中 ${scenario.distanceCacheHitPairs || 0} 对，本轮新拉取 ${scenario.distanceFetchedPairs || 0} 对。`)
    : (lang() === "ja"
      ? " 現在は高德の道路情報が使えないため、直線距離の推定に自動で切り替えています。"
      : " 当前高德路网不可用，已自动回退到直线距离估算。");
  const baseMessage = state.settings.ignoreWaves
    ? (lang() === "ja" ? "可能な範囲で割り当てた結果を生成しました。現在は波次を分けずに計算しています。" : "已生成尽量调度结果。当前按忽略波次模式求解。")
    : (lang() === "ja" ? `可能な範囲で割り当てた結果を生成しました。現在は ${scenario.waves.length} 個の業務波次で計算しており、到着要件を強制約として扱います。` : `已生成尽量调度结果。当前按 ${scenario.waves.length} 个业务波次求解，并按到店要求做强约束。`);
  const unscheduledMessage = bestResult.metrics.unscheduledCount ? ` ${LT("unscheduledSummary", { count: bestResult.metrics.unscheduledCount, names: formatUnscheduledDetails(bestResult.metrics.unscheduledStores, 12) })}` : ` ${L("noUnscheduled")}`;
  const reasonMessage = bestResult.metrics.unscheduledCount ? ` ${lang() === "ja" ? `未割当の主因：${summarizeUnscheduledReasons(bestResult.metrics.unscheduledStores)}` : `未调度主因：${summarizeUnscheduledReasons(bestResult.metrics.unscheduledStores)}`}` : "";
  const targetMessage = targetScore > 0 ? ` ${buildTargetScoreAdvice(bestResult)}` : "";
  box.textContent = `${buildSolveModeSummary()} ${baseMessage}${routeSourceMessage}${unscheduledMessage}${reasonMessage}${targetMessage}`;
  renderVehicles();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderStoresTable();
  renderSavedPlans();
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  求解完成：最佳方案 ${bestResult.label}，评分 ${bestResult.metrics.score.toFixed(1)}。`);
    await updateGenerationProgress(100, L("progressDone"));
  } finally {
    state.ui.generating = false;
    renderGenerationProgress();
  }
}
async function loadSample() {
  const sample = sampleData();
  const storeSelect = document.getElementById("storeSourceSelect");
  const vehicleSelect = document.getElementById("vehicleSourceSelect");
  const storeSource = String(storeSelect?.value || localStorage.getItem("storeDataSource") || "sample");
  const vehicleSource = String(vehicleSelect?.value || localStorage.getItem("vehicleDataSource") || "sample");
  state.ui.storeDataSource = storeSource === "real" ? "real" : "sample";
  state.ui.vehicleDataSource = vehicleSource === "real" ? "real" : "sample";
  if (storeSelect) storeSelect.value = state.ui.storeDataSource;
  if (vehicleSelect) vehicleSelect.value = state.ui.vehicleDataSource;

  let stores = [];
  if (state.ui.storeDataSource === "real") {
    stores = await fetchWmsStoresFromBackend();
  } else {
    try {
      const dbStores = await fetchStoresFromBackend();
      if (dbStores.length) stores = dbStores;
    } catch {}
    if (!stores.length) stores = sample.stores;
  }

  let vehicles = [];
  if (state.ui.vehicleDataSource === "real") {
    vehicles = await fetchWmsVehiclesFromBackend();
  } else {
    vehicles = sample.vehicles;
  }

  const resolvedMap = await fetchStoreWaveResolvedLoadMap(stores.map((item) => item?.id).filter(Boolean));
  stores = applyStoreWaveResolvedLoadsToStores(stores, resolvedMap);

  state.vehicles = vehicles;
  state.stores = stores;
  state.waves = buildAutoWaves(state.stores);
  state.strategyConfigTouched = { loadDistanceBias: false };
  state.lastResults = [];
  state.activeResultKey = "";
  try {
    renderAll();
    updateStrategyConfigUI();
    applySolveStrategy(state.settings.solveStrategy || "quick", state.settings.optimizeGoal || "");
    applyLanguage();
    document.getElementById("dispatchStartTimeInput").value = state.settings.dispatchStartTime;
    document.getElementById("maxRouteKmInput").value = state.settings.maxRouteKm;
    document.getElementById("minLoadRateInput").value = Math.round(state.settings.minLoadRate * 100);
    const targetScoreInput = document.getElementById("targetScoreInput");
    targetScoreInput.value = state.settings.targetScore || 0;
    targetScoreInput.min = "0";
    targetScoreInput.max = "100";
    targetScoreInput.step = "0.1";
    document.getElementById("ignoreWavesInput").checked = state.settings.ignoreWaves;
    ensureConcentrateLateControl();
    if (document.getElementById("concentrateLateInput")) document.getElementById("concentrateLateInput").checked = state.settings.concentrateLate;
    document.getElementById("singleWaveDistanceInput").value = state.settings.singleWaveDistanceKm;
    document.getElementById("singleWaveStartInput").value = state.settings.singleWaveStart;
    document.getElementById("singleWaveEndInput").value = state.settings.singleWaveEnd;
    document.getElementById("singleWaveEndModeInput").value = state.settings.singleWaveEndMode;
    const solveStrategySelect = document.getElementById("solveStrategySelect");
    if (solveStrategySelect) solveStrategySelect.value = state.settings.solveStrategy || "manual";
    const optimizeGoalSelect = document.getElementById("optimizeGoalSelect");
    if (optimizeGoalSelect) optimizeGoalSelect.value = state.settings.optimizeGoal || "balanced";
    const strategyHint = document.getElementById("strategyHint");
    if (strategyHint) strategyHint.textContent = buildStrategyHint();
    updateStrategyActionButtons();
    setImportFileTag("store", "");
    setImportFileTag("vehicle", "");
    localStorage.setItem("storeDataSource", state.ui.storeDataSource);
    localStorage.setItem("vehicleDataSource", state.ui.vehicleDataSource);
    await refreshWmsStatus();
    await refreshRunRegionData(true);
    await queryStoreWaveResolvedTable({ needConfirm: false });
    document.getElementById("validationBox").textContent = lang() === "ja"
      ? `店舗 ${state.stores.length} 件・車両 ${state.vehicles.length} 台を読み込み、貨量はローカル折算値を適用しました。`
      : `已加载门店 ${state.stores.length} 家、车辆 ${state.vehicles.length} 台，并已将货量统一应用为本地折算值。`;
  } catch (error) {
    const box = document.getElementById("validationBox");
    if (box) {
      box.textContent = `${lang() === "ja" ? "画面文言の更新中にエラーが出ましたが、基本データは復元済みです。" : "界面文案刷新时发生错误，但基础数据已经恢复。"} ${error?.message || ""}`.trim();
    }
  }
}
try {
  setupHeroQuickActions();
  document.getElementById("storePanelBody")?.classList.remove("collapsed");
  renderWaveSolverPanel();
  recommendedPlanCache.taskDate = localStorage.getItem(RECOMMENDED_PLAN_TASK_DATE_KEY) || getCurrentTaskDate();
  renderRecommendedPlans();
  void refreshRecommendedPlanSelected(recommendedPlanCache.taskDate, true);
  document.getElementById("loadSampleBtn")?.addEventListener("click", () => { void loadSample(); });
  document.getElementById("savePlanBtn")?.addEventListener("click", saveCurrentPlan);
  const exportResultBtn = document.getElementById("exportResultBtn");
  if (exportResultBtn) exportResultBtn.addEventListener("click", exportCurrentResult);
  document.getElementById("exportMultiDailyStoreBtn")?.addEventListener("click", exportMultiDailyStores);
  document.getElementById("exportSingleDailyStoreBtn")?.addEventListener("click", exportSingleDailyStores);
  document.getElementById("clearDistanceCacheBtn")?.addEventListener("click", () => {
    const keys = Object.keys(localStorage || {});
    const targetKeys = keys.filter((k) => (
      k.startsWith("distance_matrix_")
      || k.startsWith("dispatch_amap_distance_cache")
      || k === AMAP_DISTANCE_CACHE_KEY
      || k === AMAP_ROUTE_CACHE_KEY
    ));
    targetKeys.forEach((k) => localStorage.removeItem(k));
    const box = document.getElementById("validationBox");
    if (box) box.textContent = `已清理距离缓存 ${targetKeys.length} 项，页面即将刷新。`;
    setTimeout(() => window.location.reload(), 120);
  });
  document.getElementById("addVehicleBtn")?.addEventListener("click", addVehicle);
  document.getElementById("addStoreBtn")?.addEventListener("click", addStore);
  document.getElementById("addWaveBtn")?.addEventListener("click", addWave);
  document.getElementById("autoWaveBtn")?.addEventListener("click", autoBuildWavesFromCurrentStores);
  const generateBtn = document.getElementById("generateBtn");
  if (generateBtn) {
    generateBtn.setAttribute("type", "button");
    generateBtn.addEventListener("click", (event) => {
      event.preventDefault();
      void generatePlans();
    });
  }
  document.getElementById("queryStoreWaveResolvedBtn")?.addEventListener("click", () => { void queryStoreWaveResolvedTable({ needConfirm: true }); });
  document.getElementById("storeWaveResolvedShopCodeInput")?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") void queryStoreWaveResolvedTable({ needConfirm: true });
  });
  document.getElementById("storeWaveResolvedWaveBelongsInput")?.addEventListener("change", () => { void queryStoreWaveResolvedTable({ needConfirm: false }); });
  bindMainTabs();
  setMainTab("basic");
  bindImports();
  bindEditing();
  bindRunRegionEvents();
  loadDeepSeekSettings();
  void loadSample();
  startGaBackendStatusMonitor();
} catch (error) {
  const box = document.getElementById("validationBox");
  if (box) {
    box.textContent = `${lang() === "ja" ? "初期化中にエラーが出ました。まず一度「固定店舗を再読込」を押してください。" : "初始化时发生错误，请先点一次“重新加载固定门店”。"} ${error?.message || ""}`.trim();
  }
}

window.addEventListener("beforeunload", () => {
  flushRelayConsoleLogQueue();
  if (gaBackendStatusTimer) {
    clearInterval(gaBackendStatusTimer);
    gaBackendStatusTimer = null;
  }
  if (runRegionMapRetryTimer) {
    clearTimeout(runRegionMapRetryTimer);
    runRegionMapRetryTimer = null;
  }
  if (runRegionPolygonEditor) {
    try { runRegionPolygonEditor.close(); } catch {}
    runRegionPolygonEditor = null;
  }
  if (runRegionMouseTool) {
    try { runRegionMouseTool.close(true); } catch {}
    runRegionMouseTool = null;
  }
  runRegionStoreMarkers.forEach((marker) => {
    try { marker.setMap?.(null); } catch {}
  });
  runRegionStoreMarkers = [];
  runRegionPolygons.forEach((polygon) => {
    try { polygon.setMap?.(null); } catch {}
  });
  runRegionPolygons.clear();
  if (runRegionMap) {
    try { runRegionMap.destroy?.(); } catch {}
    runRegionMap = null;
  }
});
// EN: Business note for nearby logic.
// CN: 附近逻辑的业务提示。
async function directVehicleSolve() {
  if (state.ui.generating) {
    const box = document.getElementById("validationBox");
    if (box) box.textContent = "正在求解中，请稍后";
    return;
  }
  const box = document.getElementById("validationBox");
  try {
    state.ui.generating = true;
    renderGenerationProgress();
    await updateGenerationProgress(5, "正在构建场景...");
    
    const scenario = await buildScenario();
    const selected = applySolveWaveSelectionToScenario(scenario);
    if (selected.error || !selected.scenario) {
      box.textContent = selected.error || "场景构建失败";
      return;
    }
    const finalScenario = selected.scenario;
    const strategyConfig = buildBackendStrategyConfig(state.strategyConfig);
    
    const solutions = [];
    let totalUnscheduled = [];
    
    // EN: Business note for nearby logic.
    // CN: 附近逻辑的业务提示。
    let usedVehicles = new Set();
    // EN: Business note for nearby logic.
    // CN: 附近逻辑的业务提示。
    let w1Assignments = {};   // { plateNo: [storeId, ...] }
    let w1RoutesByPlate = {}; // { plateNo: [[storeId, ...], ...] }
    let w1PriorStats = {};    // { plateNo: { finish_time, prior_round_distance, priorWaveCount } }
    
    for (let idx = 0; idx < finalScenario.waves.length; idx++) {
      const wave = finalScenario.waves[idx];
      const waveId = wave.waveId;
      box.textContent = `正在求解 ${waveId} (${idx+1}/${finalScenario.waves.length}) ...`;
      await updateGenerationProgress(20 + (idx / finalScenario.waves.length) * 70, `求解 ${waveId}`);
      
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      let payload = {
        algorithmKey: "vehicle",
        scenario: finalScenario,
        wave: wave,
        stores: scenario.stores,
        dist: finalScenario.dist,
        strategyConfig: strategyConfig,
      };
      
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      if (waveId === "W1") {
        // EN: Business note for nearby logic.
        // CN: 附近逻辑的业务提示。
        payload.vehicles = finalScenario.vehicles;
      } else if (waveId === "W2") {
        // EN: Business note for nearby logic.
        // CN: 附近逻辑的业务提示。
        const w1UsedVehicles = finalScenario.vehicles.filter(v => usedVehicles.has(v.plateNo));
        if (w1UsedVehicles.length === 0) {
          // EN: Business note for nearby logic.
          // CN: 附近逻辑的业务提示。
          payload.vehicles = finalScenario.vehicles;
        } else {
          payload.vehicles = w1UsedVehicles;
        }
        payload.w1_assignments = w1Assignments;   // 传递 W1 门店映射
        payload.w1_routes_by_plate = w1RoutesByPlate; // 传递 W1 完整趟次结构
        payload.w1_prior_stats = w1PriorStats; // 传递 W1 接力 prior stats
      } else if (waveId === "W3") {
        // EN: Business note for nearby logic.
        // CN: 附近逻辑的业务提示。
        if (strategyConfig.w3ExcludePriorVehicles && usedVehicles.size > 0) {
          payload.excluded_vehicles = Array.from(usedVehicles);
          console.log(`[directVehicleSolve] 排除车辆: ${payload.excluded_vehicles.join(", ")}`);
        }
        // EN: Business note for nearby logic.
        // CN: 附近逻辑的业务提示。
        payload.vehicles = finalScenario.vehicles.map((vehicle) => ({
          ...vehicle,
          speed: Number(strategyConfig.w3SpeedKmh || 48),
        }));
      } else if (waveId === "W4") {
        // EN: Business note for nearby logic.
        // CN: 附近逻辑的业务提示。
        payload.vehicles = finalScenario.vehicles;
      }
      
      const response = await fetch("http://127.0.0.1:8765/wave-optimize", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (!result.bestState) throw new Error(`波次 ${waveId} 求解失败`);
      
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      const waveUsedPlates = result.bestState
        .filter(item => item.routes && item.routes.length > 0)
        .map(item => item.plateNo);
      waveUsedPlates.forEach(plate => usedVehicles.add(plate));
      console.log(`[directVehicleSolve] ${waveId} 使用车辆: ${waveUsedPlates.join(", ")}`);
      
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      if (waveId === "W1") {
        w1RoutesByPlate = {};
        w1Assignments = {};
        w1PriorStats = {};
        for (const item of result.bestState) {
          const plate = item.plateNo;
          const routes = item.routes || [];
          w1RoutesByPlate[plate] = routes.map((route) => Array.isArray(route) ? [...route] : []);
          const allStores = [];
          for (const route of routes) {
            if (Array.isArray(route)) allStores.push(...route);
          }
          w1Assignments[plate] = allStores;
          w1PriorStats[plate] = {
            finish_time: Number(item.finish_time ?? item.finishTime ?? wave.startMin ?? 0),
            prior_round_distance: Number(item.prior_round_distance ?? item.priorRoundDistance ?? 0),
            priorWaveCount: Number(item.priorWaveCount ?? routes.length ?? 0),
          };
        }
      }
      
      // EN: Business note for nearby logic.
      // CN: 附近逻辑的业务提示。
      const plans = result.bestState.map(item => ({
        vehicle: { plateNo: item.plateNo, capacity: item.capacity, speed: item.speed, canCarryCold: item.canCarryCold || false },
        waveId: waveId,
        trips: (item.routes || []).map((route, tripIdx) => ({
          tripNo: tripIdx + 1,
          route: route,
          stops: [],
          totalDistance: 0,
          loadBoxes: 0,
          loadRate: 0,
        })),
        totalDistance: 0,
        totalLoad: 0,
        tripCount: (item.routes || []).length,
        availableTime: 0,
      }));
      solutions.push(plans);
      if (result.unscheduledStores) totalUnscheduled.push(...result.unscheduledStores);
    }
    
    const metrics = evaluateSolution(solutions, finalScenario, totalUnscheduled);
    const resultObj = {
      key: "vehicle_new",
      label: "车辆驱动构造（新）",
      description: "无硬约束，全门店必调度，日志见 C:\\Users\\laoj0\\Desktop\\123.txt",
      solution: solutions,
      metrics: metrics,
      unscheduledStores: totalUnscheduled,
      storeAssignmentMap: buildStoreAssignmentMapFromSolution(solutions),
      scenario: finalScenario,
    };
    
    state.lastResults = [resultObj];
    state.activeResultKey = resultObj.key;
    autoArchiveCurrentRun();
    renderAll();
    box.textContent = `车辆驱动构造（新）完成，评分 ${metrics.score.toFixed(1)}，已调度 ${metrics.scheduledCount} 家，未调度 ${metrics.unscheduledCount} 家。详情日志 C:\\Users\\laoj0\\Desktop\\123.txt`;
    await updateGenerationProgress(100, "完成");
  } catch (err) {
    console.error(err);
    if (box) box.textContent = `车辆驱动构造（新）失败：${err.message}`;
  } finally {
    state.ui.generating = false;
    renderGenerationProgress();
  }
}

function addDirectVehicleButton() {
  const generateBtn = document.getElementById("generateBtn");
  if (!generateBtn) return;
  const parent = generateBtn.parentNode;
  if (parent && !document.getElementById("directVehicleBtn")) {
    const newBtn = document.createElement("button");
    newBtn.id = "directVehicleBtn";
    newBtn.className = "secondary";
    newBtn.textContent = lang() === "ja" ? "車両駆動構築（新）" : "车辆驱动构造（新）";
    newBtn.style.marginLeft = "8px";
    newBtn.addEventListener("click", directVehicleSolve);
    parent.insertBefore(newBtn, generateBtn.nextSibling);
  }
}

// EN: Business note for nearby logic.
// CN: 附近逻辑的业务提示。
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", addDirectVehicleButton);
} else {
  addDirectVehicleButton();
}
