/*
 * 璋冨害绯荤粺鍓嶇涓昏剼鏈€? *
 * 杩欓噷璐熻矗涓昏皟搴﹂〉闈㈢殑娴忚鍣ㄤ晶閫昏緫锛? * - 缁勭粐璋冨害杈撳叆锛? * - 璋冪敤鍚庣鎺ュ彛锛? * - 灞曠ず璋冨害杩囩▼銆佹尝娆＄粨鏋滃拰淇濆瓨/褰掓。鑳藉姏锛? * - 绠＄悊鏈湴缂撳瓨涓庨〉闈㈢姸鎬佹仮澶嶃€? */
const STORAGE_KEY = "dispatch_saved_plans_v6";
const RUN_ARCHIVE_KEY = "dispatch_run_archive_v1";
const DEEPSEEK_SETTINGS_KEY = "dispatch_deepseek_settings_v1";
const AMAP_WEB_SERVICE_KEY = "3ba73c0e0906dcbe77eeb85f3a5c343d";
const AMAP_JS_WEB_KEY = "28741158a28eba2a5182757cf0d6c059";
const AMAP_DISTANCE_CACHE_KEY = "dispatch_amap_distance_cache_v1";
const AMAP_ROUTE_CACHE_KEY = "dispatch_amap_route_cache_v1";
const AMAP_ORIGIN_BATCH_SIZE = 20;
const RECOMMENDED_PLAN_TASK_DATE_KEY = "dispatch_recommended_task_date_v1";
const RUN_REGION_SCHEME_SELECTED_KEY = "dispatch_run_region_scheme_selected_v1";
const ENFORCED_VEHICLE_TYPE = "4.2绫冲帰寮忚揣杞?;
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
const LOAD_CONVERT_WAVE2_FIELDS = ["apcs", "apaper"];
const algorithmMeta = {
  vrptw: { label: "VRPTW璐績鎻掑叆鍚彂寮?, description: "绾墠绔椂闂寸淮搴︽彃鍏ュ紡璐績 VRPTW 鍚彂寮忋€? },
  hybrid: { label: "娣峰悎VRPTW鍚彂寮?, description: "鍦ㄨ椽蹇冩彃鍏ュ垵濮嬭В涓婂彔鍔犵蹇屾悳绱笌澶ч偦鍩熸悳绱㈢殑绾墠绔贩鍚堝惎鍙戝紡銆? },
  ga: { label: "閬椾紶绠楁硶锛圙A锛?, description: "鍩轰簬璐績鍒濆瑙ｅ仛杞婚噺鎵板姩銆? },
  tabu: { label: "绂佸繉鎼滅储", description: "鍥寸粫鍒濆瑙ｅ仛閭诲煙鏇挎崲銆? },
  lns: { label: "澶ч偦鍩熸悳绱?, description: "灞€閮ㄦ媶瑙ｅ悗閲嶆柊鎻掑叆闂ㄥ簵銆? },
  savings: { label: "Clark-Wright鑺傜害娉?, description: "鎸夎妭绾﹀€煎悎骞惰矾绾匡紝鍐嶅仛 2-opt 涓庤溅杈嗗垎閰嶃€? },
  sa: { label: "妯℃嫙閫€鐏?, description: "鐢ㄩ€€鐏帴鍙楀噯鍒欒烦鍑哄眬閮ㄦ渶浼橈紝鍐嶉厤鍚堝眬閮ㄤ紭鍖栨敹鏁涖€? },
  aco: { label: "铓佺兢绠楁硶锛圓CO锛?, description: "鐢ㄤ俊鎭礌鍜屽惎鍙戝紡鏋勯€犻棬搴楀簭鍒楋紝鍐嶅仛灞€閮ㄦ敼杩涖€? },
  pso: { label: "绮掑瓙缇ょ畻娉?, description: "鐢ㄤ紭鍏堢骇瀹炴暟缂栫爜銆侀€熷害鏇存柊鍜岀兢浣撴渶浼樺紩瀵兼悳绱€? },
  vehicle: { label: "杞﹁締椹卞姩鏋勯€?, description: "鎸夎溅杈嗛€愬彴瑁呭～闂ㄥ簵锛屽厛涓ユ牸鍙鍐嶅仛涓€娆℃澗寮涢噸璇曘€? },
  relay: { label: "鎺ュ姏姹傝В", description: "鍏堢敓鎴愬垵鎺掞紝鍐嶆寜闃舵涓茶仈鍏ㄥ眬鎼滅储涓庡眬閮ㄤ紭鍖栵紝閫愭鎺ユ鎵撶（鏈€缁堟柟妗堛€? },
};
function getAlgorithmQuickGuide(key) {
  const guides = {
    aco: "馃悳 铓佺兢绠楁硶(ACO)锛氭ā鎷熻殏铓佽椋燂紝淇℃伅绱犲紩瀵兼悳绱€傞€傚悎100-300瀹堕棬搴楋紝鑳芥壘鍒颁紭璐ㄨ矾寰勪絾鏀舵暃绋嶆參銆?,
    pso: "馃惁 绮掑瓙缇ょ畻娉?PSO)锛氭ā鎷熼笩缇ゆ崟椋燂紝缇や綋鏅鸿兘寮曞銆傞€傚悎50-150瀹堕棬搴楋紝鏀舵暃蹇絾鍙兘鏃╃啛銆?,
    vehicle: "馃殮 杞﹁締椹卞姩鏋勯€狅細鎸夎溅杈嗛€愬彴濉炲簵锛屼紭鍏堟彁鍗囪鐩栫巼锛岄€傚悎璋冭瘯鍜屼繚搴曟帓婊°€?,
    sa: "馃敟 妯℃嫙閫€鐏?SA)锛氭ā鎷熼噾灞為€€鐏紝姒傜巼鎬ф帴鍙楀樊瑙ｃ€傞€傚悎浠绘剰瑙勬ā锛岃兘璺冲嚭灞€閮ㄦ渶浼樸€?,
    tabu: "馃搵 绂佸繉鎼滅储(Tabu)锛氬甫璁板繂鐨勯偦鍩熸悳绱€傞€傚悎灏忚妯＄簿纭紭鍖栵紝閬垮厤璧板洖澶磋矾銆?,
    lns: "馃敤 澶ч偦鍩熸悳绱?LNS)锛氱牬鍧?淇妗嗘灦銆傞€傚悎200-500瀹堕棬搴楋紝鑳藉ぇ瑙勬ā閲嶆瀯瑙ｃ€?,
    hybrid: "馃幆 娣峰悎绠楁硶锛歋A+LNS+Tabu涓夐樁娈垫帴鍔涖€傝拷姹傛瀬鑷磋川閲忔椂浣跨敤锛岃€楁椂鏈€闀裤€?,
    ga: "馃К 閬椾紶绠楁硶(GA)锛氭ā鎷熻嚜鐒堕€夋嫨锛岃繘鍖栨悳绱€傚叏灞€鑳藉姏寮猴紝浣嗘敹鏁涙參銆?,
    savings: "馃挵 Clark-Wright鑺傜害娉曪細缁忓吀鏋勯€犳硶锛屽揩閫熺敓鎴愬垵濮嬭В銆傞€熷害蹇紝璐ㄩ噺绋冲畾銆?,
    vrptw: "鈴?VRPTW璐績鎻掑叆锛氭椂闂寸獥浼樺厛锛岄€愪釜鎻掑叆銆傚彲琛屾€уソ锛岄€傚悎蹇€熷垵鎺掋€?
  };
  return guides[key] || `${key}绠楁硶锛岃鎯呰鏌ョ湅绯荤粺鏂囨。銆俙;
}
const I18N = {
  zh: {
    algorithmLabels: { vrptw: "VRPTW璐績鎻掑叆鍚彂寮?, hybrid: "娣峰悎VRPTW鍚彂寮?, ga: "閬椾紶绠楁硶锛圙A锛?, tabu: "绂佸繉鎼滅储", lns: "澶ч偦鍩熸悳绱?, savings: "Clark-Wright鑺傜害娉?, sa: "妯℃嫙閫€鐏?, aco: "铓佺兢绠楁硶锛圓CO锛?, pso: "绮掑瓙缇ょ畻娉?, vehicle: "杞﹁締椹卞姩鏋勯€?, relay: "鎺ュ姏姹傝В" },
    algorithmDescriptions: {
      vrptw: "绾墠绔椂闂寸淮搴︽彃鍏ュ紡璐績 VRPTW 鍚彂寮忋€?,
      hybrid: "鍦ㄨ椽蹇冩彃鍏ュ垵濮嬭В涓婂彔鍔犵蹇屾悳绱笌澶ч偦鍩熸悳绱㈢殑绾墠绔贩鍚堝惎鍙戝紡銆?,
      ga: "鍩轰簬璐績鍒濆瑙ｅ仛杞婚噺鎵板姩銆?,
      tabu: "鍥寸粫鍒濆瑙ｅ仛閭诲煙鏇挎崲銆?,
      lns: "灞€閮ㄦ媶瑙ｅ悗閲嶆柊鎻掑叆闂ㄥ簵銆?,
      savings: "鎸夎妭绾﹀€煎悎骞惰矾绾匡紝鍐嶅仛 2-opt 涓庤溅杈嗗垎閰嶃€?,
      sa: "鐢ㄩ€€鐏帴鍙楀噯鍒欒烦鍑哄眬閮ㄦ渶浼橈紝鍐嶉厤鍚堝眬閮ㄤ紭鍖栨敹鏁涖€?,
      aco: "鐢ㄤ俊鎭礌鍜屽惎鍙戝紡鏋勯€犻棬搴楀簭鍒楋紝鍐嶅仛灞€閮ㄦ敼杩涖€?,
      pso: "鐢ㄤ紭鍏堢骇瀹炴暟缂栫爜銆侀€熷害鏇存柊鍜岀兢浣撴渶浼樺紩瀵兼悳绱€?,
      vehicle: "鎸夎溅杈嗛€愬彴瑁呭～闂ㄥ簵锛屽厛涓ユ牸鍙锛屽啀鍋氫竴娆¤蒋鏃堕棿绐楅噸璇曘€?,
      relay: "鍏堝嚭鍒濇帓锛屽啀鎸夐樁娈典覆鑱斿叏灞€鎼滅储涓庡眬閮ㄤ紭鍖栵紝鐢熸垚鍗曟潯鏈€缁堟帴鍔涜В銆?,
    },
  },
  ja: {
    algorithmLabels: { vrptw: "VRPTW璨鎸垮叆娉?, hybrid: "銉忋偆銉栥儶銉冦儔VRPTW", ga: "閬轰紳鐨勩偄銉偞銉偤銉?, tabu: "銈裤儢銉兼帰绱?, lns: "澶ц繎鍌嶆帰绱?, savings: "Clark-Wright绡€绱勬硶", sa: "鐒笺亶銇伨銇楁硶", aco: "锜汇偝銉儖銉兼渶閬╁寲", pso: "绮掑瓙缇ゆ渶閬╁寲", vehicle: "杌婁浮椐嗗嫊妲嬬瘔", relay: "銉儸銉兼渶閬╁寲" },
    algorithmDescriptions: {
      vrptw: "銉曘儹銉炽儓銈ㄣ兂銉夊畬绲愩伄鏅傞枔娆″厓鎸垮叆鍨嬨兓璨 VRPTW 銉掋儱銉笺儶銈广儐銈ｃ儍銈€?,
      hybrid: "璨鎸垮叆銇垵鏈熻В銇偪銉栥兗鎺㈢储銇ㄥぇ杩戝倣鎺㈢储銈掗噸銇倠绱斻儠銉兂銉堛偍銉炽儔娣峰悎銉掋儱銉笺儶銈广儐銈ｃ儍銈€?,
      ga: "璨鍒濇湡瑙ｃ倰銉欍兗銈广伀杌介噺銇憘鍕曘倰琛屻亜銇俱仚銆?,
      tabu: "鍒濇湡瑙ｃ伄杩戝倣銈掓帰绱仐銆佸弽寰┿倰閬裤亼銇俱仚銆?,
      lns: "涓€閮ㄥ簵鑸椼倰澶栥仐銇﹀啀鎸垮叆銇椼€佸眬鎵€鏀瑰杽銇椼伨銇欍€?,
      savings: "绡€绱勫€ら爢銇儷銉笺儓銈掔当鍚堛仐銆併仢銇緦 2-opt 銇ㄨ粖涓″壊褰撱倰琛屻亜銇俱仚銆?,
      sa: "鐒笺亶銇伨銇椼伄鍙楃悊瑕忓墖銇у眬鎵€鏈€閬┿亱銈夋姕銇戝嚭銇椼€佸眬鎵€鏀瑰杽銇у弾鏉熴仌銇涖伨銇欍€?,
      aco: "銉曘偋銉儮銉炽仺銉掋儱銉笺儶銈广儐銈ｃ儍銈仹瑷晱闋嗐倰妲嬬瘔銇椼€併仢銇緦銇眬鎵€鏀瑰杽銇椼伨銇欍€?,
      pso: "鍎厛搴︺伄瀹熸暟绗﹀彿鍖栥仺閫熷害鏇存柊銇с€佸€嬩綋鏈€鑹兓鍏ㄤ綋鏈€鑹伀鍚戙亼銇︽帰绱仐銇俱仚銆?,
      vehicle: "杌婁浮銇斻仺銇簵鑸椼倰瑭般倎銈嬫绡夋硶銇с€併伨銇氬幊瀵嗗彲琛屻€佹銇珐鍜屽啀瑭﹁銇椼伨銇欍€?,
      relay: "鍒濇湡妗堛倰浣滄垚銇椼仧寰屻€佹闅庛仈銇ㄣ伀鍏ㄥ煙鎺㈢储銇ㄥ眬鎵€鏀瑰杽銈掋仱銇亷銆佹渶绲傛銇哥（銇嶄笂銇掋伨銇欍€?,
    },
  },
};
function lang() { return state.language || "zh"; }
function algoLabel(key) { return I18N[lang()].algorithmLabels[key] || algorithmMeta[key]?.label || key; }
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
function cloneDefaultStrategyConfig() {
  return { ...DEFAULT_STRATEGY_CONFIG };
}
function normalizeStrategyConfig(input = {}) {
  const scoreLimit = Number(input.difficultyScoreLimit ?? DEFAULT_STRATEGY_CONFIG.difficultyScoreLimit);
  const tier1Limit = Number(input.difficultyTier1Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier1Limit);
  const tier2Limit = Number(input.difficultyTier2Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier2Limit);
  const tier3Limit = Number(input.difficultyTier3Limit ?? DEFAULT_STRATEGY_CONFIG.difficultyTier3Limit);
  const toBool = (value, defaultValue) => {
    if (value === undefined || value === null) return Boolean(defaultValue);
    if (typeof value === "string") {
      const v = value.trim().toLowerCase();
      if (["true", "1", "yes", "on"].includes(v)) return true;
      if (["false", "0", "no", "off"].includes(v)) return false;
    }
    return Boolean(value);
  };
  const maxSolveCapacityRaw = Number(input.maxSolveCapacity ?? DEFAULT_STRATEGY_CONFIG.maxSolveCapacity);
  const defaultSpeedRaw = Number(input.defaultSpeedKmh ?? DEFAULT_STRATEGY_CONFIG.defaultSpeedKmh);
  const w3SpeedRaw = Number(input.w3SpeedKmh ?? DEFAULT_STRATEGY_CONFIG.w3SpeedKmh);
  const relayMaxRaw = Number(input.w1w2RelayMaxKm ?? DEFAULT_STRATEGY_CONFIG.w1w2RelayMaxKm);
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
function applyOptimizeGoalPreset(goal, strategyConfig = state.strategyConfig, force = false) {
  const next = { ...(strategyConfig || cloneDefaultStrategyConfig()) };
  next.optimizeGoal = ["load", "balanced", "distance"].includes(goal) ? goal : "balanced";
  if (force || !state.strategyConfigTouched?.loadDistanceBias) {
    if (next.optimizeGoal === "load") {
      next.loadDistanceBias = -60;
    } else if (next.optimizeGoal === "distance") {
      next.loadDistanceBias = 60;
    } else {
      next.loadDistanceBias = 0;
    }
  }
  return normalizeStrategyConfig(next);
}
function updateStrategyConfigUI() {
  const cfg = state.strategyConfig || cloneDefaultStrategyConfig();
  const deliveryMode = document.getElementById("strategyDeliveryModeSelect");
  if (deliveryMode) deliveryMode.value = cfg.deliveryMode || "singleDailyWave";
  const goalSelect = document.getElementById("strategyOptimizeGoalSelect");
  if (goalSelect) goalSelect.value = cfg.optimizeGoal || "balanced";
  const loadDistanceBias = document.getElementById("strategyLoadDistanceBiasInput");
  if (loadDistanceBias) loadDistanceBias.value = String(cfg.loadDistanceBias ?? 0);
  const latenessTolerance = document.getElementById("strategyLatenessToleranceSelect");
  if (latenessTolerance) latenessTolerance.value = cfg.latenessTolerance || "medium";
  const vehicleCostBias = document.getElementById("strategyVehicleCostBiasInput");
  if (vehicleCostBias) vehicleCostBias.value = String(cfg.vehicleCostBias ?? 50);
  const dualWave = document.getElementById("strategyDualWaveWeightInput");
  if (dualWave) dualWave.value = String(cfg.dualWaveWeight ?? 50);
  const crossRegion = document.getElementById("strategyCrossRegionPenaltyWeightInput");
  if (crossRegion) crossRegion.value = String(cfg.crossRegionPenaltyWeight ?? 50);
  const waveDelay = document.getElementById("strategyWaveDelayPenaltyInput");
  if (waveDelay) waveDelay.value = String(cfg.waveDelayPenalty ?? 50);
  const lateRoute = document.getElementById("strategyLateRouteStrengthSelect");
  if (lateRoute) lateRoute.value = cfg.lateRouteStrength || "medium";
  const diffMode = document.getElementById("strategyDifficultyModeSelect");
  if (diffMode) diffMode.value = cfg.deliveryDifficultyMode || "time";
  const tier1Unlimited = document.getElementById("strategyDifficultyTier1UnlimitedInput");
  if (tier1Unlimited) tier1Unlimited.checked = Boolean(cfg.difficultyTier1Unlimited);
  const tier1Limit = document.getElementById("strategyDifficultyTier1LimitInput");
  if (tier1Limit) tier1Limit.value = String(cfg.difficultyTier1Limit ?? 1);
  const tier2Unlimited = document.getElementById("strategyDifficultyTier2UnlimitedInput");
  if (tier2Unlimited) tier2Unlimited.checked = Boolean(cfg.difficultyTier2Unlimited);
  const tier2Limit = document.getElementById("strategyDifficultyTier2LimitInput");
  if (tier2Limit) tier2Limit.value = String(cfg.difficultyTier2Limit ?? 2);
  const tier3Unlimited = document.getElementById("strategyDifficultyTier3UnlimitedInput");
  if (tier3Unlimited) tier3Unlimited.checked = Boolean(cfg.difficultyTier3Unlimited);
  const tier3Limit = document.getElementById("strategyDifficultyTier3LimitInput");
  if (tier3Limit) tier3Limit.value = String(cfg.difficultyTier3Limit ?? 0);
  const diffScoreLimit = document.getElementById("strategyDifficultyScoreLimitInput");
  if (diffScoreLimit) diffScoreLimit.value = String(cfg.difficultyScoreLimit ?? 8);
  updateDifficultyTierLimitInputState(cfg);
  updateDifficultyModeFieldVisibility(cfg.deliveryDifficultyMode || "time");
}

function updateDifficultyModeFieldVisibility(mode) {
  const countGroup = document.getElementById("strategyDifficultyCountGroup");
  const scoreGroup = document.getElementById("strategyDifficultyScoreGroup");
  if (countGroup) countGroup.style.display = mode === "count" ? "" : "none";
  if (scoreGroup) scoreGroup.style.display = mode === "score" ? "" : "none";
}

function updateDifficultyTierLimitInputState(strategyCfg = state.strategyConfig || cloneDefaultStrategyConfig()) {
  const cfg = strategyCfg || cloneDefaultStrategyConfig();
  const tier1Limit = document.getElementById("strategyDifficultyTier1LimitInput");
  const tier2Limit = document.getElementById("strategyDifficultyTier2LimitInput");
  const tier3Limit = document.getElementById("strategyDifficultyTier3LimitInput");
  if (tier1Limit) tier1Limit.disabled = Boolean(cfg.difficultyTier1Unlimited);
  if (tier2Limit) tier2Limit.disabled = Boolean(cfg.difficultyTier2Unlimited);
  if (tier3Limit) tier3Limit.disabled = Boolean(cfg.difficultyTier3Unlimited);
}
function buildBackendStrategyConfig(strategyConfigInput = state.strategyConfig) {
  const cfg = normalizeStrategyConfig(strategyConfigInput || {});
  const distanceWeight = Math.max(0, Math.min(100, Math.round((cfg.loadDistanceBias + 100) / 2)));
  const loadWeight = 100 - distanceWeight;
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
const UI_TEXT = {
  zh: {
    showcase: "绯荤粺浠嬬粛", reload: "閲嶆柊鍔犺浇鍥哄畾闂ㄥ簵", save: "淇濆瓨褰撳墠鏂规", exportResult: "瀵煎嚭褰撳墠缁撴灉",
    storeInfo: "闂ㄥ簵淇℃伅", vehicleInfo: "杞﹁締淇℃伅", waveConfig: "娉㈡閰嶇疆", algoRun: "绠楁硶涓庢墽琛?, result: "璋冨害缁撴灉", saved: "宸蹭繚瀛樻柟妗?,
    storeDesc: "闂ㄥ簵鏁版嵁宸插唴缃埌绯荤粺涓紝褰撳墠鍙洿鎺ョ紪杈戝熀纭€灞炴€с€?, vehicleDesc: "杞﹁締鍙€氳繃 TXT 瀵煎叆锛屼篃鍙互鎵嬪伐琛ュ厖銆?, waveDesc: "澹版槑鍙厤閫佹尝娆°€佸悇娉㈡寮€濮嬬粨鏉熸椂闂达紝浠ュ強璇ユ尝娆″寘鍚殑闂ㄥ簵缂栧彿銆?, algoDesc: "鍙崟鐙敓鎴愭柟妗堬紝涔熷彲鍚屾椂鍕鹃€変袱绉嶇畻娉曞仛瀵规瘮銆傛渶浣庤杞界巼浼氫綔涓烘帓搴忓亸濂斤紝鑰岄潪纭€у崱姝绘潯浠躲€?, resultDesc: "绯荤粺浼氬睍绀洪棬搴楄矾绾裤€佽窛绂汇€佽皟搴﹁繃绋嬶紝浠ュ強浜哄伐璋冭溅鍏ュ彛銆?, savedDesc: "鏂规淇濆瓨鍦ㄥ綋鍓嶆祻瑙堝櫒鏈湴锛屽彲浣滀负鍘嗗彶鎺掔嚎璁板綍銆?,
    foldStore: "鎶樺彔搴楅摵鏄庣粏", unfoldStore: "灞曞紑搴楅摵鏄庣粏", foldVehicle: "鎶樺彔杞﹁締鏄庣粏", unfoldVehicle: "灞曞紑杞﹁締鏄庣粏",
    addStore: "鏂板闂ㄥ簵", addVehicle: "鏂板杞﹁締", addWave: "鏂板娉㈡",
    dispatchStart: "棣栨鍙戣溅鏃堕棿", maxKm: "鍗曡溅寰€杩旀€婚噷绋嬩笂闄愶紙鍏噷锛?, minLoad: "鍗曡溅鏈€浣庤杞界巼锛?锛?, ignoreWaves: "蹇界暐娉㈡", autoWave: "鎸夊浐瀹氶棬搴楄嚜鍔ㄥ垎娉㈡", concentrateLate: "杩熷埌绾胯矾灏介噺闆嗕腑",
    targetScore: "鐩爣璇勫垎涓嬮檺",
    singleWaveDistance: "鍗曟尝娆¤窛绂婚槇鍊硷紙鍏噷锛?, singleWaveStart: "鍗曟尝娆″紑濮?, singleWaveEnd: "鍗曟尝娆＄粨鏉?, singleWaveMode: "鍗曟尝娆℃埅姝㈣鍒?,
    returnEnd: "鍥炲簱鎴", serviceEnd: "瀹屽簵鎴", generate: "鐢熸垚璋冨害缁撴灉", close: "鍏抽棴",
    processTitle: "璋冨害杩囩▼鍙鍖?, showcaseTitle: "绯荤粺鑳藉姏鎬昏",
    storeTableHeaders: ["缂栧彿", "鍚嶇О", "鍖哄煙", "缁忓害", "绾害", "娆℃暟", "涓€娉㈡璐ч噺", "浜屾尝娆¤揣閲?, "鍐疯棌姣斾緥", "涓€閰嶆椂闂?, "浜岄厤鏃堕棿", "鎵€灞炴尝娆?, "鍗歌揣鍒嗛挓", "闅惧害", "鍏佽鍋忓樊(鍒?", "鐘舵€?, "杞﹀彿", ""],
    vehicleTableHeaders: ["杞﹀彿", "鍙告満鍚?, "杞﹀瀷", "瀹归噺", "閫熷害", "鍙€佸喎钘?, "鐘舵€?, ""],
    waveTableHeaders: ["娉㈡", "寮€濮?, "缁撴潫", "鎴瑙勫垯", "鍖呭惈闂ㄥ簵", ""],
    used: "宸叉淳杞?, idle: "鏈娇鐢?, del: "鍒犻櫎", scheduled: "宸茶皟搴?, unscheduled: "鏈皟搴?, assignedVehicle: "瀵瑰簲杞﹀彿",
    chooseFile: "閫夋嫨鏂囦欢",
    noFileChosen: "鏈€夋嫨鏂囦欢",
    allDay: "鍏ㄥぉ缁熶竴璋冨害", route: "璺嚎", totalDistance: "鎬婚噷绋?, avgLoad: "骞冲潎鍗曡稛瑁呰浇鐜?, fleetLoad: "杞﹂槦鍒╃敤鐜?, fleetLoadHint: "鎸夊凡鐢ㄨ溅杈嗗閲忔姌绠楋紝澶氳稛鏃跺彲瓒呰繃100%", score: "缁煎悎寰楀垎", onTime: "鍑嗘椂鐜?,
    detail: "鏄庣粏", viewViz: "鏌ョ湅鍙鍖?, adjustTo: "璋冩暣鍒拌溅杈?..", confirmAdjust: "纭璋冩暣", noOtherVehicle: "褰撳墠娉㈡娌℃湁鍏朵粬杞﹁締鍙帴鎵?,
    inserted: "鎵嬪伐鎻掑叆", leg: "鏈", arrive: "鍒拌揪", unloadStart: "寮€濮嬪嵏璐?, leave: "绂诲紑", desired: "鏈熸湜鍒拌揪", unloadMinutes: "鍗歌揣", minutes: "鍒嗛挓", notLate: "鍑嗘椂", late: "瓒呮椂绐?,
    storesCount: "闂ㄥ簵鏁?, totalLoad: "绱瑁呰浇", maxTrip: "鏈€楂樺崟瓒?, trips: "瓒熸暟", totalRoundKm: "寰€杩旀€婚噷绋?, loadPreferredMet: "杈惧埌瑁呰浇鍋忓ソ", loadPreferredMiss: "鏈揪瑁呰浇鍋忓ソ",
    tripNo: "绗?, tripSuffix: "瓒?, returnTime: "鍥炲簱鏃堕棿", backDistance: "鍥炰粨璺濈", tripRoundKm: "鍗曡稛寰€杩旀€婚噷绋?, tripLoadRate: "鍗曡稛瑁呰浇鐜?, overWave: "瓒呮尝娆?,
    generatedSingle: "绯荤粺宸茬敓鎴愯皟搴︽柟妗堛€?, generatedCompare: "绯荤粺宸插畬鎴愬弻绠楁硶瀵规瘮銆?, savedEmpty: "杩樻病鏈変繚瀛樿繃璋冨害鏂规銆?,
    multiStoreDaily: "澶氶棬搴楀崟鏃ュ娆￠厤閫佽皟搴?,
    targetAchieved: "宸茶揪鍒扮洰鏍囪瘎鍒?,
    targetMissed: "鏈揪鍒扮洰鏍囪瘎鍒?,
    noVehicles: "璇峰厛褰曞叆鎴栧鍏ヨ溅杈嗕俊鎭€?,
    noStores: "褰撳墠鍥哄畾闂ㄥ簵鏁版嵁涓虹┖銆?,
    noWaves: "璇峰厛閰嶇疆鑷冲皯涓€涓尝娆°€?,
    regularMissing: "鏅€氭尝娆¤繕鏈?{count} 瀹堕棬搴楁湭鍒嗛厤锛歿names}",
    unscheduledStores: "鏈皟搴﹂棬搴?,
    scheduledStores: "宸茶皟搴﹂棬搴?,
    hardArrivalHint: "鍒板簵瑕佹眰鏃堕棿宸蹭綔涓哄己绾︽潫锛涜秴鍑哄厑璁稿埌搴楀亸宸殑闂ㄥ簵浼氫紭鍏堜繚鐣欎负鏈皟搴︺€?,
      lateFocusHint: "宸插惎鐢ㄨ繜鍒扮嚎璺泦涓瓥鐣ワ紝绯荤粺浼氫紭鍏堝噺灏戝嚭鐜拌繜鍒扮殑绾胯矾鏁般€?,
      noUnscheduled: "鍏ㄩ儴闂ㄥ簵宸茶皟搴︺€?,
      unscheduledSummary: "褰撳墠灏氭湁 {count} 瀹堕棬搴楁湭璋冨害锛歿names}",
    unscheduledReasonArrival: "鍒板簵鏃堕棿瓒呭嚭鍏佽鍋忓樊绐楀彛",
      unscheduledReasonWave: "娉㈡鎴鏃堕棿涓嶈冻",
      unscheduledReasonMileage: "閲岀▼绾︽潫涓嶈冻",
      unscheduledReasonCapacity: "杞﹁締瀹归噺涓嶈冻",
      unscheduledReasonSlot: "鐜版湁杞﹁締鏃跺簭宸叉弧锛屾棤娉曞啀鎻掑叆",
      unscheduledReasonMixed: "澶氶噸绾︽潫鍙犲姞锛屽綋鍓嶆棤娉曟彃鍏?,
      unscheduledReasonTitle: "鏈皟搴﹀師鍥?,
      unscheduledDetails: "鏈皟搴︽槑缁?,
      importedVehicles: "宸插鍏?{count} 杈嗚溅杈嗐€?,
    importVehicleFailed: "杞﹁締 TXT 鏈В鏋愬嚭鏈夋晥杞﹀彿锛岃妫€鏌ョ紪鐮佹垨姣忚涓€杈嗚溅鐨勬牸寮忋€?,
    autoWaveBuilt: "宸叉寜鍥哄畾闂ㄥ簵鑷姩鐢熸垚 {count} 涓尝娆°€?,
    chooseDifferentVehicle: "璇烽€夋嫨涓€涓笉鍚岀殑鐩爣杞﹁締銆?,
    storeNotOnVehicle: "鏈湪 {plate} 涓婃壘鍒伴棬搴?{store}銆?,
    transferFailed: "闂ㄥ簵 {store} 鏃犳硶浠?{source} 璋冩暣鍒?{target}锛岃妫€鏌ユ椂闂淬€侀噷绋嬫垨瀹归噺銆?,
    transferSuccess: "宸插皢闂ㄥ簵 {store} 浠?{source} 璋冩暣鍒?{target}锛屽苟浠呴噸绠楄繖涓よ締杞︺€?,
    rescheduleAgain: "鍐嶆璋冨害",
    saveScheduledResult: "淇濆瓨褰撳墠缁撴灉",
    rescheduleSection: "鏈皟搴﹂棬搴楄ˉ璋?,
    rescheduleHint: "淇濈暀宸茶皟搴︾嚎璺紝浼樺厛鐢ㄧ┖闂茶溅杈嗙户缁ˉ璋冩湭瀹夋帓鍑哄幓鐨勯棬搴椼€?,
    exportLiveUnscheduled: "瀵煎嚭褰撳墠鏈皟搴D",
    unscheduledMismatch: "鍙ｅ緞涓嶄竴鑷达細琛ヨ皟闈㈡澘={panel}锛屾槑缁嗙瓫閫?{detail}銆傝鍋滄浣跨敤璇ョ粨鏋滃苟鍙嶉銆?,
    manualAssignVehicle: "鎵嬪伐鎸囧畾杞﹀彿",
    manualAssignPlaceholder: "鎸囧畾杞﹀彿...",
    confirmAssign: "鎸囧畾璋冨害",
    rescheduleProgress: "鏈疆鏂板璋冨害 {count} 瀹堕棬搴椼€?,
    rescheduleNoProgress: "鏈疆鏈兘鏂板璋冨害闂ㄥ簵銆?,
    noAssignableVehicle: "褰撳墠娌℃湁鍙敤浜庤ˉ璋冪殑杞﹁締銆?,
    assignFailed: "闂ㄥ簵 {store} 鎸囧畾缁?{plate} 澶辫触锛岃妫€鏌ユ椂闂淬€侀噷绋嬫垨瀹归噺銆?,
    assignSuccess: "闂ㄥ簵 {store} 宸叉寚瀹氱粰 {plate}锛屽苟瀹屾垚灞€閮ㄩ噸绠椼€?,
    savedAt: "淇濆瓨鏃堕棿",
    wavesLabel: "娉㈡",
    waveModeReturn: "杞﹁締闇€鍦?{time} 鍓嶅洖搴?,
    waveModeService: "杞﹁締闇€鍦?{time} 鍓嶅畬鎴愭渶鍚庝竴瀹跺簵",
    waveSingleHint: "璇ョ粍浣滀负鍗曟尝娆″簵閾哄崟鍒楁眰瑙ｏ紝涓斿崟杞﹀彧璺戜竴瓒?,
    waveRegularHint: "鏅€氭尝娆℃寜姝ゆ椂闂寸‖绾︽潫鎵ц",
    includedStores: "鍖呭惈闂ㄥ簵",
    allStores: "鍏ㄩ儴闂ㄥ簵",
    overtimeTrips: "瓒呮尝娆＄嚎璺?,
    playback: "鍥炴斁",
    selectedAlgorithms: "褰撳墠宸插嬀閫夌畻娉?,
    noneSelected: "鏈€夋嫨",
    staticMap: "闈欐€佸尯鍩熷湴鍥?,
    routeLegend: "绾胯矾鍥句緥",
    depot: "搴撴埧",
    viewMap: "鏌ョ湅鍦板浘",
    routeMap: "鐪嬬嚎璺浘",
    routeMapHint: "鎸夊綋鍓嶇嚎璺『搴忓湪鍦板浘涓婃弿鐐硅繛绾匡紝浼樺厛浣跨敤楂樺痉椹捐溅璺緞缂撳瓨銆?,
    routeStopSeq: "椤哄簭",
    routePlanArrival: "璁″垝鍒板簵",
    routeDesiredArrival: "甯屾湜鍒板簵",
    routeStopName: "搴楅摵",
    analyticsTitle: "鏁板瓧椹鹃┒鑸?,
    analyticsDesc: "鐢ㄥ浘褰㈡柟寮忓睍绀鸿皟搴︽晥鐜囥€佺畻娉曞樊寮傘€佹尝娆¤礋鑽蜂笌鍖哄煙鍒嗗竷銆?,
    dashboard: "鏍稿績鎸囨爣",
    algoCompare: "绠楁硶瀵规瘮",
    gantt: "娉㈡鐢樼壒鍥?,
    loadCurve: "杞﹁締瑁呰浇鏇茬嚎",
    spatial: "鍖哄煙鏁ｇ偣鍒嗗眰",
    progressTitle: "鐢熸垚杩涘害",
    storesToday: "浠婃棩闂ㄥ簵",
    usedVehiclesShort: "宸茬敤杞?,
    idleVehiclesShort: "闂茬疆杞?,
    overtimeTripsShort: "瓒呮椂绾胯矾",
    routeDigest: "绾胯矾鎽樿",
    routeDigestHint: "姣忔潯绾垮睍绀洪棬搴楁暟銆佹€婚噷绋嬨€佽杞界巼锛屾柟渚跨洿鎺ユ瘮杈冪嚎璺粨鏋勩€?,
    algorithmScore: "绠楁硶寰楀垎",
    scoreBreakdownTitle: "璇勫垎鎷嗚В",
    onTimeScore: "鍑嗘椂寰楀垎",
    distanceScoreLabel: "璺濈寰楀垎",
    loadScoreLabel: "瑁呰浇寰楀垎",
    preferenceScoreLabel: "鍋忓ソ寰楀垎",
    progressIdle: "绛夊緟鐢熸垚鏂规",
    progressPreparing: "姝ｅ湪鏁寸悊闂ㄥ簵銆佽溅杈嗕笌娉㈡绾︽潫鈥?,
    progressRunning: "姝ｅ湪杩愯 {algo}鈥?,
    progressFinishing: "姝ｅ湪姹囨€诲浘褰㈠寲缁撴灉鈥?,
    progressDone: "鍥惧舰椹鹃┒鑸卞凡鍒锋柊",
    tripLabel: "瓒熸",
    singleWaveLabel: "鍗曟尝娆?,
    regularWaveLabel: "鏅€氭尝娆?,
    loadAxis: "瑁呰浇鐜?,
    timeAxis: "鏃堕棿",
    scatterNear: "杩戣窛闂ㄥ簵",
    scatterFar: "杩滆窛闂ㄥ簵",
    scatterSingle: "鍗曟尝娆″簵閾?,
    noChartData: "褰撳墠娌℃湁瓒冲鏁版嵁鐢熸垚鍥惧舰銆?,
    voiceBroadcast: "璇煶鎾姤",
    voiceAsk: "璇煶鎻愰棶",
    mascotTitle: "椴哥暐浣垮姪鎵?,
    mascotDesc: "鐢ㄦ洿娓╁拰銆佹洿娓呮櫚鐨勬柟寮忔挱鎶ュ綋鍓嶈皟搴︽憳瑕佷笌椋庨櫓鎻愮ず銆?,
    speechUnsupported: "褰撳墠娴忚鍣ㄤ笉鏀寔璇煶鎾姤銆?,
    speechListenUnsupported: "褰撳墠娴忚鍣ㄤ笉鏀寔璇煶璇嗗埆銆?,
    speechMicPreparing: "姝ｅ湪鐢宠楹﹀厠椋庢潈闄愶紝娴忚鍣ㄥ脊绐楄鐩存帴鐐瑰厑璁革紝鍏佽鍚庝細绔嬪埢寮€濮嬫敹闊炽€?,
    speechMicDenied: "楹﹀厠椋庢潈闄愭湭寮€鍚紝璇峰湪娴忚鍣ㄥ湴鍧€鏍忛噷鍏佽楹﹀厠椋庡悗鍐嶈瘯銆?,
    speechMicFailed: "楹﹀厠椋庡垵濮嬪寲澶辫触锛岃妫€鏌ユ祻瑙堝櫒鏄惁鍏佽褰撳墠椤甸潰浣跨敤楹﹀厠椋庛€?,
    speechListening: "姝ｅ湪鍚紝璇风洿鎺ユ彁闂€?,
    speechHeard: "宸插惉鍒帮細{text}",
    speechAnswerPrefix: "鏁板瓧鍔╃悊鍥炵瓟锛?,
    deepseekKeyLabel: "DeepSeek Key",
    deepseekModelLabel: "DeepSeek妯″瀷",
    deepseekModeLabel: "鍔╂墜妯″紡",
    deepseekModeDispatch: "璋冨害鍔╂墜",
    deepseekModeGeneral: "閫氱敤鍔╂墜",
    deepseekStyleLabel: "鍥炵瓟椋庢牸",
    deepseekStyleBrief: "绠€娲佸洖绛?,
    deepseekStyleDetailed: "璇︾粏鍒嗘瀽",
    deepseekSave: "淇濆瓨DeepSeek閰嶇疆",
    deepseekAsk: "闂瓺eepSeek",
    deepseekAskPlaceholder: "渚嬪锛氫负浠€涔堣繖娆?W2 鐢ㄨ溅鍋忓锛熻繕鑳芥€庝箞浼樺寲锛?,
    deepseekSaved: "DeepSeek 閰嶇疆宸蹭繚瀛樺湪褰撳墠娴忚鍣ㄣ€?,
    deepseekMissingKey: "璇峰厛濉叆 DeepSeek API Key銆?,
    deepseekThinking: "DeepSeek 姝ｅ湪缁撳悎褰撳墠璋冨害缁撴灉鎬濊€冣€?,
    deepseekFailed: "DeepSeek 璋冪敤澶辫触锛歿message}",
    deepseekReady: "DeepSeek 宸叉帴鍏ュ綋鍓嶉〉闈紝鍙洿鎺ユ枃瀛楁彁闂垨閰嶅悎璇煶鎻愰棶銆?,
    deepseekLocalFallback: "褰撳墠鏈厤缃?DeepSeek锛屼粛浣跨敤鏈湴鎸夐挳鍔╂墜鍥炵瓟銆?,
    deepseekAnswerPrefix: "DeepSeek鍥炵瓟锛?,
    exportNoResult: "褰撳墠杩樻病鏈夊彲瀵煎嚭鐨勮皟搴︾粨鏋溿€?,
    exportDone: "宸插鍑哄綋鍓嶈皟搴︾粨鏋溿€?,
    exportFilePrefix: "璋冨害缁撴灉",
    solveStrategy: "姹傝В绛栫暐",
    optimizeGoal: "浼樺寲鐩爣",
    strategyManual: "鎵嬪姩鍕鹃€?,
    strategyQuick: "蹇€熷垵鎺?,
    strategyDeep: "娣卞害浼樺寲",
    strategyGlobal: "鍏ㄥ眬鎼滅储",
    strategyRelay: "鎺ュ姏姹傝В",
    strategyFree: "鑷敱姹傝В",
    strategyCompare: "澶氱畻娉曞姣?,
    goalBalanced: "缁煎悎骞宠　",
    goalOnTime: "鍑嗙偣浼樺厛",
    goalDistance: "閲岀▼浼樺厛",
    goalVehicles: "鏈€灏戠敤杞?,
    goalLoad: "瑁呰浇浼樺厛",
    quickSolve: "蹇€熷垵鎺?,
    deepOptimize: "缁х画浼樺寲",
    globalSearch: "鍏ㄥ眬鎼滅储",
    relaySolve: "鎺ュ姏姹傝В",
    freeSolve: "鑷敱姹傝В",
    multiCompare: "澶氱畻娉曞姣?,
    gaBackendChecking: "GA鍚庡彴锛氭鏌ヤ腑鈥?,
    gaBackendOnline: "GA鍚庡彴锛氬凡杩為€氾紙Python锛?,
    gaBackendOffline: "GA鍚庡彴锛氭湭杩為€氾紙闇€鍏堟仮澶嶅悗绔級",
  },
  ja: {
    showcase: "銈枫偣銉嗐儬绱逛粙", reload: "鍥哄畾搴楄垪銈掑啀瑾炯", save: "鐝惧湪銇銈掍繚瀛?, exportResult: "鐝惧湪绲愭灉銈掑嚭鍔?,
    storeInfo: "搴楄垪鎯呭牨", vehicleInfo: "杌婁浮鎯呭牨", waveConfig: "娉㈡瑷畾", algoRun: "銈儷銈淬儶銈恒儬瀹熻", result: "閰嶉€佺祼鏋?, saved: "淇濆瓨娓堛伩妗?,
    storeDesc: "搴楄垪銉囥兗銈裤伅銈枫偣銉嗐儬銇唴钄点仌銈屻仸銇娿倞銆佸熀鏈睘鎬с倰鐩存帴绶ㄩ泦銇с亶銇俱仚銆?, vehicleDesc: "杌婁浮銇?TXT 鍙栬炯銇倐鎵嬪叆鍔涖伀銈傚蹇溿仐銇俱仚銆?, waveDesc: "閰嶉€佹尝娆°€佸悇娉㈡銇枊濮嬬祩浜嗘檪闁撱€佸璞″簵鑸楃暘鍙枫倰瀹氱京銇椼伨銇欍€?, algoDesc: "鍗樼嫭妗堢敓鎴愩伀銈傘€佽鏁般偄銉偞銉偤銉犳瘮杓冦伀銈傚蹇溿仐銇俱仚銆傛渶浣庣杓夌巼銇儚銉笺儔鍒剁磩銇с伅銇亸鍎厛搴︺仺銇椼仸鎵便亜銇俱仚銆?, resultDesc: "搴楄垪銉兗銉堛€佽窛闆€侀厤閫侀亷绋嬨€佹墜鍕曡粖涓¤鏁淬倰琛ㄧず銇椼伨銇欍€?, savedDesc: "妗堛伅鐝惧湪銇儢銉┿偊銈躲伀淇濆瓨銇曘倢銆佸饱姝淬仺銇椼仸鍙傜収銇с亶銇俱仚銆?,
    foldStore: "搴楄垪鏄庣窗銈掓姌銈娿仧銇熴個", unfoldStore: "搴楄垪鏄庣窗銈掑睍闁?, foldVehicle: "杌婁浮鏄庣窗銈掓姌銈娿仧銇熴個", unfoldVehicle: "杌婁浮鏄庣窗銈掑睍闁?,
    addStore: "搴楄垪杩藉姞", addVehicle: "杌婁浮杩藉姞", addWave: "娉㈡杩藉姞",
    dispatchStart: "鍒濆洖鍑虹櫤鏅傚埢", maxKm: "杌婁浮寰€寰╃窂璺濋洟涓婇檺锛坘m锛?, minLoad: "杌婁浮鏈€浣庣杓夌巼锛?锛?, ignoreWaves: "娉㈡銈掔劇瑕?, autoWave: "鍥哄畾搴楄垪銇ц嚜鍕曟尝娆″垎鍓?, concentrateLate: "閬呯潃銉┿偆銉炽倰闆嗕腑",
    targetScore: "鐩銈广偝銈笅闄?,
    singleWaveDistance: "鍗樼嫭娉㈡璺濋洟銇椼亶銇勫€わ紙km锛?, singleWaveStart: "鍗樼嫭娉㈡闁嬪", singleWaveEnd: "鍗樼嫭娉㈡绲備簡", singleWaveMode: "鍗樼嫭娉㈡绶犲垏銉兗銉?,
    returnEnd: "甯板韩绶犲垏", serviceEnd: "鏈€绲傚簵瀹屼簡绶犲垏", generate: "閰嶉€佺祼鏋溿倰鐢熸垚", close: "闁夈仒銈?,
    processTitle: "閰嶉€侀亷绋嬨伄鍙鍖?, showcaseTitle: "銈枫偣銉嗐儬鑳藉姏銇窂瑕?,
    storeTableHeaders: ["鐣彿", "鍚嶇О", "銈ㄣ儶銈?, "绲屽害", "绶害", "閰嶉€佸洖鏁?, "涓€娉㈡璨ㄩ噺", "浜屾尝娆¤波閲?, "鍐疯數姣旂巼", "涓€閰嶆檪鍒?, "浜岄厤鏅傚埢", "鎵€灞炴尝娆?, "鑽蜂笅銈嶃仐鍒?, "闆ｆ槗搴?, "瑷卞鍋忓樊(鍒?", "鐘舵厠", "杌婄暘", ""],
    vehicleTableHeaders: ["杌婄暘", "閬嬭虎鎵嬪悕", "杌婄ó", "瀹归噺", "閫熷害", "鍐疯數鍙?, "鐘舵厠", ""],
    waveTableHeaders: ["娉㈡", "闁嬪", "绲備簡", "绶犲垏銉兗銉?, "瀵捐薄搴楄垪", ""],
    used: "閰嶈粖娓堛伩", idle: "寰呮", scheduled: "鍓插綋娓堛伩", unscheduled: "鏈壊褰?, assignedVehicle: "鎷呭綋杌婄暘",
    chooseFile: "銉曘偂銈ゃ儷閬告姙",
    noFileChosen: "鏈伕鎶?,
    allDay: "绲傛棩涓€鎷厤閫?, route: "銉兗銉?, totalDistance: "绶忚窛闆?, avgLoad: "骞冲潎鍗樹究绌嶈級鐜?, fleetLoad: "杌婇殜绋煎儘鐜?, fleetLoadHint: "浣跨敤杌婁浮瀹归噺銉欍兗銈规彌绠椼€傝鏁颁究鏅傘伅100%瓒呫倐銇傘倞銇俱仚", score: "绶忓悎銈广偝銈?, onTime: "鏅傞枔鍐呯巼",
    detail: "鏄庣窗", viewViz: "鍙鍖栥倰瑕嬨倠", adjustTo: "绉诲嫊鍏堣粖涓?..", confirmAdjust: "瑾挎暣纰哄畾", noOtherVehicle: "銇撱伄娉㈡銇с伅浠栥伀寮曞彈杌婁浮銇屻亗銈娿伨銇涖倱",
    inserted: "鎵嬪嫊鎸垮叆", leg: "鍖洪枔", arrive: "鍒扮潃", unloadStart: "鑽蜂笅銈嶃仐闁嬪", leave: "鍑虹櫤", desired: "甯屾湜鍒扮潃", unloadMinutes: "鑽蜂笅銈嶃仐", minutes: "鍒?, notLate: "鏅傞枔鍐?, late: "鏅傞枔绐撹秴閬?,
    storesCount: "搴楄垪鏁?, totalLoad: "绱▓绌嶈級", maxTrip: "鍗樿稛鏈€澶?, trips: "渚挎暟", totalRoundKm: "寰€寰╃窂璺濋洟", loadPreferredMet: "绌嶈級鍎厛閬旀垚", loadPreferredMiss: "绌嶈級鍎厛鏈仈",
    tripNo: "绗?, tripSuffix: "渚?, returnTime: "甯板韩鏅傚埢", backDistance: "甯板韩璺濋洟", tripRoundKm: "鍗樹究寰€寰╄窛闆?, tripLoadRate: "鍗樹究绌嶈級鐜?, overWave: "娉㈡瓒呴亷",
    generatedSingle: "閰嶉€佹銈掔敓鎴愩仐銇俱仐銇熴€?, generatedCompare: "瑜囨暟銈儷銈淬儶銈恒儬姣旇純銈掔敓鎴愩仐銇俱仐銇熴€?, savedEmpty: "銇俱仩淇濆瓨娓堛伩妗堛伅銇傘倞銇俱仜銈撱€?,
    multiStoreDaily: "澶氬簵鑸楀崢鏃ヨ鏁板洖閰嶉€佽搴?,
    targetAchieved: "鐩銈广偝銈㈤仈鎴?,
    targetMissed: "鐩銈广偝銈㈡湭閬?,
    noVehicles: "鍏堛伀杌婁浮鎯呭牨銈掑叆鍔涖伨銇熴伅鍙栬炯銇椼仸銇忋仩銇曘亜銆?,
    noStores: "鐝惧湪銆佸浐瀹氬簵鑸椼儑銉笺偪銇屻亗銈娿伨銇涖倱銆?,
    noWaves: "灏戙仾銇忋仺銈?1 銇ゃ伄娉㈡銈掕ō瀹氥仐銇︺亸銇犮仌銇勩€?,
    regularMissing: "閫氬父娉㈡銇ф湭鍓插綋銇簵鑸椼亴 {count} 浠躲亗銈娿伨銇欙細{names}",
    unscheduledStores: "鏈壊褰撳簵鑸?,
    scheduledStores: "鍓插綋娓堝簵鑸?,
    hardArrivalHint: "鍒扮潃瑕佹眰鏅傚埢銇挤鍒剁磩銇ф壉銇勩€佽ū瀹瑰亸宸倰瓒呫亪銈嬪簵鑸椼伅鏈壊褰撱伀鍥炪仐銇俱仚銆?,
      lateFocusHint: "閬呯潃銉┿偆銉抽泦涓垿鐣ャ倰鏈夊姽鍖栥仐銇︺亜銇俱仚銆傞亝鐫€銇岀櫤鐢熴仚銈嬭粖绶氭暟銈掑劒鍏堢殑銇姂銇堛伨銇欍€?,
      noUnscheduled: "鍏ㄥ簵鑸椼倰鍓层倞褰撱仸娓堛伩銇с仚銆?,
      unscheduledSummary: "鐝惧湪銆佹湭鍓插綋搴楄垪銇?{count} 浠躲亗銈娿伨銇欙細{names}",
    unscheduledReasonArrival: "鍒扮潃瑕佹眰鏅傚埢銇ū瀹瑰亸宸伀鍙庛伨銈娿伨銇涖倱",
      unscheduledReasonWave: "娉㈡銇窢鍒囨檪闁撱亴涓嶈冻銇椼仸銇勩伨銇?,
      unscheduledReasonMileage: "璺濋洟鍒剁磩銇屼笉瓒炽仐銇︺亜銇俱仚",
      unscheduledReasonCapacity: "杌婁浮瀹归噺銇屼笉瓒炽仐銇︺亜銇俱仚",
      unscheduledReasonSlot: "鏃㈠瓨杌婁浮銇檪绯诲垪銇尶鍏ヤ綑鍦般亴銇傘倞銇俱仜銈?,
      unscheduledReasonMixed: "瑜囨暟鍒剁磩銇岄噸銇倞銆佺従鍦ㄣ伅鎸垮叆銇с亶銇俱仜銈?,
      unscheduledReasonTitle: "鏈壊褰撶悊鐢?,
      unscheduledDetails: "鏈壊褰撴槑绱?,
      importedVehicles: "{count} 鍙般伄杌婁浮銈掑彇杈笺仐銇俱仐銇熴€?,
    importVehicleFailed: "杌婁浮 TXT 銇嬨倝鏈夊姽銇粖鐣倰瑙ｆ瀽銇с亶銇俱仜銈撱仹銇椼仧銆傛枃瀛椼偝銉笺儔銇俱仧銇?1 琛?1 鍙般伄褰㈠紡銈掔⒑瑾嶃仐銇︺亸銇犮仌銇勩€?,
    autoWaveBuilt: "鍥哄畾搴楄垪銇熀銇ャ亶 {count} 鍊嬨伄娉㈡銈掕嚜鍕曠敓鎴愩仐銇俱仐銇熴€?,
    chooseDifferentVehicle: "鐣般仾銈嬬洰妯欒粖涓°倰閬告姙銇椼仸銇忋仩銇曘亜銆?,
    storeNotOnVehicle: "{plate} 涓娿伀搴楄垪 {store} 銇岃銇ゃ亱銈娿伨銇涖倱銆?,
    transferFailed: "搴楄垪 {store} 銈?{source} 銇嬨倝 {target} 銇哥Щ銇涖伨銇涖倱銆傛檪闁撱兓璺濋洟銉诲閲忋倰纰鸿獚銇椼仸銇忋仩銇曘亜銆?,
    transferSuccess: "搴楄垪 {store} 銈?{source} 銇嬨倝 {target} 銇Щ銇椼€併亾銇?2 鍙般伄銇垮啀瑷堢畻銇椼伨銇椼仧銆?,
    rescheduleAgain: "鍐嶅壊褰?,
    saveScheduledResult: "鐝惧湪绲愭灉銈掍繚瀛?,
    rescheduleSection: "鏈壊褰撳簵鑸椼伄鍐嶅壊褰?,
    rescheduleHint: "鏃㈠瓨銇壊褰撱儷銉笺儓銈掍繚鎸併仐銇ゃ仱銆佺┖銇嶈粖涓°倰鍎厛銇椼仸鏈壊褰撳簵鑸椼倰杩藉姞銇у壊銈婂綋銇︺伨銇欍€?,
    exportLiveUnscheduled: "鐝惧湪銇湭鍓插綋ID銈掑嚭鍔?,
    unscheduledMismatch: "闆嗚▓涓嶄竴鑷达細鍐嶅壊褰撱儜銉嶃儷={panel}銆佹槑绱般儠銈ｃ儷銈?{detail}銆傘亾銇祼鏋溿伄鍒╃敤銈掑仠姝仐銇﹀牨鍛娿仐銇︺亸銇犮仌銇勩€?,
    manualAssignVehicle: "鎵嬪嫊銇ц粖鐣寚瀹?,
    manualAssignPlaceholder: "杌婄暘銈掓寚瀹?..",
    confirmAssign: "鎸囧畾鍓插綋",
    rescheduleProgress: "浠婂洖銇啀鍓插綋銇?{count} 搴楄垪銈掕拷鍔犮仹鍓层倞褰撱仸銇俱仐銇熴€?,
    rescheduleNoProgress: "浠婂洖銇啀鍓插綋銇с伅杩藉姞鍓插綋銇屻亗銈娿伨銇涖倱銇с仐銇熴€?,
    noAssignableVehicle: "鐝惧湪銆佸啀鍓插綋銇娇銇堛倠杌婁浮銇屻亗銈娿伨銇涖倱銆?,
    assignFailed: "搴楄垪 {store} 銈?{plate} 銇寚瀹氥仹銇嶃伨銇涖倱銇с仐銇熴€傛檪闁撱兓璺濋洟銉诲閲忋倰纰鸿獚銇椼仸銇忋仩銇曘亜銆?,
    assignSuccess: "搴楄垪 {store} 銈?{plate} 銇寚瀹氥仐銆佸眬鎵€鍐嶈▓绠椼倰瀹屼簡銇椼伨銇椼仧銆?,
    savedAt: "淇濆瓨鏅傚埢",
    wavesLabel: "娉㈡",
    waveModeReturn: "杌婁浮銇?{time} 銇俱仹銇赴搴仚銈嬪繀瑕併亴銇傘倞銇俱仚",
    waveModeService: "杌婁浮銇?{time} 銇俱仹銇渶绲傚簵鑸椼倰瀹屼簡銇欍倠蹇呰銇屻亗銈娿伨銇?,
    waveSingleHint: "銇撱伄銈般儷銉笺儣銇崢鐙尝娆°仺銇椼仸鍒ュ嚘鐞嗐仌銈屻€? 鍙?1 渚裤伄銇裤仹銇?,
    waveRegularHint: "閫氬父娉㈡銇亾銇檪闁撳埗绱勩倰銉忋兗銉夐仼鐢ㄣ仐銇俱仚",
    includedStores: "瀵捐薄搴楄垪",
    allStores: "鍏ㄥ簵鑸?,
    overtimeTrips: "娉㈡瓒呴亷銉┿偆銉?,
    playback: "銉儣銉偆",
    selectedAlgorithms: "鐝惧湪鏈夊姽銇偄銉偞銉偤銉?,
    noneSelected: "鏈伕鎶?,
    staticMap: "闈欑殑銈ㄣ儶銈優銉冦儣",
    routeLegend: "銉兗銉堝嚒渚?,
    depot: "鍊夊韩",
    viewMap: "鍦板洺銈掕銈?,
    routeMap: "銉兗銉堝洺",
    routeMapHint: "鐝惧湪銇í鍟忛爢銇у湴鍥充笂銇儷銉笺儓銈掓弿銇嶃€侀珮寰枫伄璧拌銉兗銉堛偔銉ｃ儍銈枫儱銈掑劒鍏堢殑銇埄鐢ㄣ仐銇俱仚銆?,
    routeStopSeq: "闋嗙暘",
    routePlanArrival: "浜堝畾鍒板簵",
    routeDesiredArrival: "甯屾湜鍒板簵",
    routeStopName: "搴楄垪",
    analyticsTitle: "銉囥偢銈裤儷銉€銉冦偡銉ャ儨銉笺儔",
    analyticsDesc: "閰嶈粖鍔圭巼銆併偄銉偞銉偤銉犲樊銆佹尝娆¤矤鑽枫€併偍銉偄鍒嗗竷銈掋偘銉┿儠銇х⒑瑾嶃仹銇嶃伨銇欍€?,
    dashboard: "涓昏鎸囨",
    algoCompare: "銈儷銈淬儶銈恒儬姣旇純",
    gantt: "娉㈡銈裤偆銉犮儵銈ゃ兂",
    loadCurve: "杌婁浮绌嶈級鎺ㄧЩ",
    spatial: "銈ㄣ儶銈㈠垎甯?,
    progressTitle: "鐢熸垚閫叉崡",
    storesToday: "鏈棩搴楄垪",
    usedVehiclesShort: "浣跨敤杌?,
    idleVehiclesShort: "寰呮杌?,
    overtimeTripsShort: "瓒呮檪銉┿偆銉?,
    routeDigest: "銉兗銉堣绱?,
    routeDigestHint: "鍚勩儷銉笺儓銇簵鑸楁暟銉荤窂璺濋洟銉荤杓夌巼銈掍甫銇广仸銆併仢銇牬銇ф閫犲樊銈掕姣斻伖銈夈倢銇俱仚銆?,
    algorithmScore: "銈儷銈淬儶銈恒儬寰楃偣",
    scoreBreakdownTitle: "銈广偝銈㈠唴瑷?,
    onTimeScore: "瀹氭檪寰楃偣",
    distanceScoreLabel: "璺濋洟寰楃偣",
    loadScoreLabel: "绌嶈級寰楃偣",
    preferenceScoreLabel: "鍡滃ソ寰楃偣",
    progressIdle: "妗堛伄鐢熸垚寰呮涓仹銇?,
    progressPreparing: "搴楄垪銉昏粖涓°兓娉㈡鍒剁磩銈掓暣鐞嗐仐銇︺亜銇俱仚鈥?,
    progressRunning: "{algo} 銈掑疅琛屻仐銇︺亜銇俱仚鈥?,
    progressFinishing: "鍥冲舰銉€銉冦偡銉ャ儨銉笺儔銈掗泦瑷堛仐銇︺亜銇俱仚鈥?,
    progressDone: "銉€銉冦偡銉ャ儨銉笺儔銈掓洿鏂般仐銇俱仐銇?,
    tripLabel: "渚?,
    singleWaveLabel: "鍗樼嫭娉㈡",
    regularWaveLabel: "閫氬父娉㈡",
    loadAxis: "绌嶈級鐜?,
    timeAxis: "鏅傞枔",
    scatterNear: "杩戣窛闆㈠簵鑸?,
    scatterFar: "閬犺窛闆㈠簵鑸?,
    scatterSingle: "鍗樼嫭娉㈡搴楄垪",
    noChartData: "鐝惧湪銆佸洺褰倰鐢熸垚銇欍倠銇熴倎銇崄鍒嗐仾銉囥兗銈裤亴銇傘倞銇俱仜銈撱€?,
    voiceBroadcast: "闊冲０銉栥儶銉笺儠銈ｃ兂銈?,
    voiceAsk: "闊冲０璩晱",
    mascotTitle: "榀ㄧ暐浣裤偄銈枫偣銈裤兂銉?,
    mascotDesc: "鐝惧湪銇搴︾姸娉併仺娉ㄦ剰鐐广倰銆併倓銈忋倝銇嬨亸鍒嗐亱銈娿倓銇欍亸妗堝唴銇椼伨銇欍€?,
    speechUnsupported: "銇撱伄銉栥儵銈︺偠銇с伅闊冲０瑾伩涓娿亽銇蹇溿仐銇︺亜銇俱仜銈撱€?,
    speechListenUnsupported: "銇撱伄銉栥儵銈︺偠銇с伅闊冲０瑾嶈瓨銇蹇溿仐銇︺亜銇俱仜銈撱€?,
    speechMicPreparing: "銉炪偆銈ī闄愩倰鐢宠珛涓仹銇欍€傘儢銉┿偊銈躲伄銉濄儍銉椼偄銉冦儣銇цū鍙仚銈嬨仺銆併仢銇伨銇鹃煶澹板彈浠樸倰闁嬪銇椼伨銇欍€?,
    speechMicDenied: "銉炪偆銈ī闄愩亴鐒″姽銇с仚銆傘儢銉┿偊銈躲伄銈儔銉偣銉愩兗銇с優銈ゃ偗銈掕ū鍙仐銇︺亱銈夊啀瑭﹁銇椼仸銇忋仩銇曘亜銆?,
    speechMicFailed: "銉炪偆銈伄鍒濇湡鍖栥伀澶辨晽銇椼伨銇椼仧銆傘儢銉┿偊銈躲仹銇撱伄銉氥兗銈搞伄銉炪偆銈埄鐢ㄣ亴瑷卞彲銇曘倢銇︺亜銈嬨亱纰鸿獚銇椼仸銇忋仩銇曘亜銆?,
    speechListening: "璩晱銈掕仦銇勩仸銇勩伨銇欍€傜稓銇戙仸瑭便仐銇︺亸銇犮仌銇勩€?,
    speechHeard: "鑱炪亶鍙栥仯銇熷唴瀹癸細{text}",
    speechAnswerPrefix: "銉囥偢銈裤儷鍔╂墜銇洖绛旓細",
    deepseekKeyLabel: "DeepSeek Key",
    deepseekModelLabel: "DeepSeek銉儑銉?,
    deepseekModeLabel: "鍔╂墜銉兗銉?,
    deepseekModeDispatch: "閰嶈粖鍔╂墜",
    deepseekModeGeneral: "姹庣敤鍔╂墜",
    deepseekStyleLabel: "鍥炵瓟銈广偪銈ゃ儷",
    deepseekStyleBrief: "绨℃綌鍥炵瓟",
    deepseekStyleDetailed: "瑭崇窗鍒嗘瀽",
    deepseekSave: "DeepSeek瑷畾銈掍繚瀛?,
    deepseekAsk: "DeepSeek銇唱鍟?,
    deepseekAskPlaceholder: "渚嬶細浠婂洖銇?W2 銇т娇鐢ㄨ粖涓°亴澶氥亜鐞嗙敱銇ㄦ敼鍠勬柟鍚戙伅锛?,
    deepseekSaved: "DeepSeek 瑷畾銈掋亾銇儢銉┿偊銈躲伀淇濆瓨銇椼伨銇椼仧銆?,
    deepseekMissingKey: "鍏堛伀 DeepSeek API Key 銈掑叆鍔涖仐銇︺亸銇犮仌銇勩€?,
    deepseekThinking: "DeepSeek 銇岀従鍦ㄣ伄閰嶈粖绲愭灉銈掕笍銇俱亪銇﹁€冦亪銇︺亜銇俱仚鈥?,
    deepseekFailed: "DeepSeek 鍛笺伋鍑恒仐銇け鏁椼仐銇俱仐銇燂細{message}",
    deepseekReady: "DeepSeek 銇屻亾銇敾闈伀鎺ョ稓銇曘倢銇俱仐銇熴€傘儐銈偣銉堛仹銈傞煶澹般仹銈傝唱鍟忋仹銇嶃伨銇欍€?,
    deepseekLocalFallback: "DeepSeek 銇屾湭瑷畾銇仧銈併€佺従鍦ㄣ伅銉兗銈儷銇儨銈裤兂鍔╂墜銇у洖绛斻仐銇俱仚銆?,
    deepseekAnswerPrefix: "DeepSeek鍥炵瓟锛?,
    exportNoResult: "鐝惧湪銆佸嚭鍔涖仹銇嶃倠閰嶉€佺祼鏋溿亴銇傘倞銇俱仜銈撱€?,
    exportDone: "鐝惧湪銇厤閫佺祼鏋溿倰鍑哄姏銇椼伨銇椼仧銆?,
    exportFilePrefix: "閰嶉€佺祼鏋?,
    solveStrategy: "瑙ｆ硶銉兗銉?,
    optimizeGoal: "鐩鏂归嚌",
    strategyManual: "鎵嬪嫊閬告姙",
    strategyQuick: "銈偆銉冦偗鍒濇湡妗?,
    strategyDeep: "缍欑稓鏀瑰杽",
    strategyGlobal: "鍏ㄥ煙鎺㈢储",
    strategyRelay: "銉儸銉兼渶閬╁寲",
    strategyFree: "鑷敱姹傝В",
    strategyCompare: "瑜囨暟妗堟瘮杓?,
    goalBalanced: "绶忓悎銉愩儵銉炽偣",
    goalOnTime: "瀹氭檪鏈€鍎厛",
    goalDistance: "璺濋洟鏈€鍎厛",
    goalVehicles: "杌婁浮鏁版渶灏?,
    goalLoad: "绌嶈級鍎厛",
    quickSolve: "銈偆銉冦偗鍒濇湡妗?,
    deepOptimize: "缍欑稓鏀瑰杽",
    globalSearch: "銈般儹銉笺儛銉帰绱?,
    relaySolve: "銉儸銉兼渶閬╁寲",
    freeSolve: "鑷敱姹傝В",
    multiCompare: "瑜囨暟妗堟瘮杓?,
    gaBackendChecking: "GA銉愩儍銈偍銉炽儔锛氱⒑瑾嶄腑鈥?,
    gaBackendOnline: "GA銉愩儍銈偍銉炽儔锛氭帴缍氭笀銇匡紙Python锛?,
    gaBackendOffline: "GA銉愩儍銈偍銉炽儔锛氭湭鎺ョ稓锛堛儛銉冦偗銈ㄣ兂銉夊京鏃с亴蹇呰锛?,
  },
};
function L(key) { return UI_TEXT[lang()]?.[key] ?? key; }
function LT(key, vars = {}) {
  let text = L(key);
  Object.entries(vars).forEach(([name, value]) => {
    text = text.replaceAll(`{${name}}`, String(value));
  });
  return text;
}
function vehicleTypeLabel(type) {
  if (lang() !== "ja") return type || "";
  const map = {
    "甯告俯杞?: "甯告俯杌?,
    "甯告俯+鍐疯棌娣峰悎杞?: "甯告俯銉诲喎钄垫贩鍚堣粖",
  };
  return map[type] || type || "";
}

const DC = { id: "DC", name: "閰嶉€佷腑蹇?, address: "鍖椾含甯傞『涔夊尯澶╂煴璺?0鍙?, lng: 116.58848, lat: 40.04776 };
const MASCOT_IMAGE_SRC = "./assets/mascot-whale.png";
const TRUCK_IMAGE_LOW = "assets/truck-30-v2.jpg";
const TRUCK_IMAGE_MID = "assets/truck-60-v2.jpg";
const TRUCK_IMAGE_HIGH = "assets/truck-full-v2.jpg";
const ALGORITHM_ORDER = ["vrptw", "hybrid", "ga", "tabu", "lns", "savings", "sa", "aco", "pso", "vehicle"];
const MAX_FREE_SOLVE_ALGOS = 5;
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
let processTypingTimers = [];
let routeLeafletMap = null;
let routeLeafletLayerGroup = null;
let routeAmapMap = null;
let routeAmapMarkers = [];
let routeAmapPolyline = null;
let relayConsoleLines = [];
let relayStageReporter = null;
let relayConsolePendingLogLines = [];
let relayConsoleLogFlushTimer = null;
const GA_BACKEND_URL = "";                     
const USE_FULL_DISTANCE_MATRIX_FROM_BACKEND = true;
let gaBackendHealth = { available: null, checkedAt: 0 };
let gaBackendStatusTimer = null;
let archiveBackendCache = { date: "", page: 1, items: [], totalPages: 1, total: 0, loading: false };
let dataArchiveCache = { date: "", keyword: "", items: [], selectedId: "" };
let recommendedPlanCache = { taskDate: "", items: [], loading: false };
let recommendedPlanSelectedCache = { taskDate: "", selected: null, loading: false };
let storeWaveResolvedCache = { items: [], count: 0, total: 0, limit: 200 };
let runRegionSchemeCache = { items: [], loading: false };
let runRegionCache = { items: [], loading: false };
let runRegionStorePoints = [];
let runRegionMap = null;
let runRegionStoreMarkers = [];
let runRegionPolygons = new Map();
let runRegionDraftPolygon = null;
let runRegionEditingId = null;
let runRegionMouseTool = null;
let runRegionPolygonEditor = null;
let runRegionMapRetryTimer = null;
let runRegionStoreVisibilityMode = "show_all";
let runRegionTargetRegionId = "all";
let runRegionCheckedRegionIds = new Set();
let runRegionSelectedSchemeNo = "";
let runRegionScheme1AutoGenerated = false;
let loadConvertPreviewCache = null;
let assistantDockDragState = null;
let amapCacheSyncPending = false;
let amapCacheSyncTimer = null;
const STRICT_ALGO_TRUTH_MODE = false;
const BACKEND_ONLY_REAL_ALGOS = new Set(["vrptw", "savings", "ga", "hybrid", "tabu", "lns", "sa", "aco", "pso", "vehicle"]);
const IGNORE_CAPACITY_CONSTRAINT = false;
const MAIN_TAB_KEYS = new Set(["basic", "region", "strategy", "solve", "result"]);

function loadDeepSeekSettings() {
  try {
    const raw = localStorage.getItem(DEEPSEEK_SETTINGS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    state.ai.deepseekApiKey = String(parsed.deepseekApiKey || "");
    state.ai.deepseekModel = String(parsed.deepseekModel || "deepseek-chat");
    state.ai.mode = String(parsed.mode || "dispatch");
    state.ai.answerStyle = String(parsed.answerStyle || "brief");
  } catch {}
}

function saveDeepSeekSettings() {
  localStorage.setItem(DEEPSEEK_SETTINGS_KEY, JSON.stringify({
    deepseekApiKey: state.ai.deepseekApiKey || "",
    deepseekModel: state.ai.deepseekModel || "deepseek-chat",
    mode: state.ai.mode || "dispatch",
    answerStyle: state.ai.answerStyle || "brief",
  }));
}

function getTripTruckVisual(trip) {
  const rate = trip?.loadRate || 0;
  if (rate < 0.45) return {
    src: TRUCK_IMAGE_LOW,
    badge: lang() === "ja" ? "璧ゅ瓧" : "浜忎簡",
    cls: "low",
  };
  if (rate < 0.8) return {
    src: TRUCK_IMAGE_MID,
    badge: lang() === "ja" ? "閬旀" : "杈炬爣",
    cls: "mid",
  };
  return {
    src: TRUCK_IMAGE_HIGH,
    badge: lang() === "ja" ? "鐩堝埄" : "璧氫簡",
    cls: "high",
  };
}

const FIXED_STORES = [
  ["391001","鏈堝潧鍖楄",0.000000,0.000000,""],
  ["391012","閲戣瀺琛?,0.000000,0.000000,""],
  ["391028","閲戣瀺琛椾腑蹇?,0.000000,0.000000,""],
  ["391030","搴勮儨宕囧厜",0.000000,0.000000,""],
  ["391074","寰疯儨鍥介檯涓績",0.000000,0.000000,""],
  ["391094","绱噾鍗拌薄",0.000000,0.000000,""],
  ["391409","鍖椾含鍦伴搧榧撴ゼ澶ц绔欏簵",0.000000,0.000000,""],
  ["391705","鍖椾含鎬€鏌斿煄甯傚鍘呭簵",0.000000,0.000000,""],
  ["391709","鍖椾含澶ч挓瀵哄箍鍦哄簵",0.000000,0.000000,""],
  ["391712","鍖椾含閲戣瀺绉戞妧瀛﹂櫌搴?,0.000000,0.000000,""],
  ["391715","鐭虫櫙灞遍噾闅呯鎶€澶у帵搴?,0.000000,0.000000,""],
  ["391718","鍖椾含鍦伴搧12鍙风嚎椹兼埧钀ョ珯搴?,0.000000,0.000000,""],
  ["391721","鍖椾含涓芥辰骞冲畨閲戣瀺涓績搴?,0.000000,0.000000,""],
  ["391722","榧庡ソ鐢靛瓙澶у帵",0.000000,0.000000,""],
  ["392029","瀵屽崕澶у帵",0.000000,0.000000,""],
  ["392084","鍗楅敚榧撳贩鍦伴搧绔?,0.000000,0.000000,""],
  ["392144","鍓嶉棬澶ц",0.000000,0.000000,""],
  ["392179","涓滃洓鍖楀ぇ琛楀簵",0.000000,0.000000,""],
  ["392315","鍦伴搧鍜屽钩閲屽寳琛楃珯搴?,0.000000,0.000000,""],
  ["392394","鍖椾含鍦伴搧宕囨枃闂ㄧ珯搴?,0.000000,0.000000,""],
  ["392395","鍖椾含鍦伴搧涓滃洓绔欏簵",0.000000,0.000000,""],
  ["392408","鍖椾含鍦伴搧瀹夊痉閲屽寳琛楃珯搴?,0.000000,0.000000,""],
  ["392525","鍖椾含鐜悆璐告槗涓績搴?,0.000000,0.000000,""],
  ["392647","鍖椾含娉撴櫉鍥介檯涓績",0.000000,0.000000,""],
  ["392678","鍖椾含鍦伴搧闆嶅拰瀹珯搴?,0.000000,0.000000,""],
  ["392707","鍖椾含鍦伴搧3鍙风嚎鏈濋槼绔欏簵",0.000000,0.000000,""],
  ["392708","鍖椾含鍦伴搧3鍙风嚎涓滃潩绔欏簵",0.000000,0.000000,""],
  ["392710","鍖椾含鍦伴搧3鍙风嚎鍥㈢粨婀栫珯搴?,0.000000,0.000000,""],
  ["392712","鍖椾含鎭掓瘏澶у帵搴?,0.000000,0.000000,""],
  ["392713","鍖椾含鍦伴搧浠€鍒规捣绔欏簵",0.000000,0.000000,""],
  ["392715","鍖椾含鍦伴搧娆箰璋风珯搴?,0.000000,0.000000,""],
  ["392716","鍖椾含鍦伴搧瑙掗棬瑗跨珯搴?,0.000000,0.000000,""],
  ["392717","鍖椾含鍦伴搧涓板彴绔欏簵",0.000000,0.000000,""],
  ["392718","鑵捐鍖椾含鎬婚儴搴?,0.000000,0.000000,""],
  ["392719","閲戦殔鏅洪€犲伐鍦哄簵鏂板簵",0.000000,0.000000,""],
  ["392720","鏈濋槼楂橀搧绔欏簵",0.000000,0.000000,""],
  ["392721","寤跺簡鐜悆鏂版剰鐧捐揣搴?,0.000000,0.000000,""],
  ["392722","鍖椾含闀挎ス澶╄搴?,0.000000,0.000000,""],
  ["392723","鍖椾含鍥涙儬閲戝湴鍚嶄含搴?,0.000000,0.000000,""],
  ["393003","鐜悆閲戣瀺涓績",0.000000,0.000000,""],
  ["393009","姘戠敓澶у帵",0.000000,0.000000,""],
  ["393010","鍙屼簳棣栧煄鍥介檯",0.000000,0.000000,""],
  ["393016","濂ユ灄鍖瑰厠鍏洯",0.000000,0.000000,""],
  ["393032","闀垮瘜瀹?,0.000000,0.000000,""],
  ["393033","鍥涙柟妗ヤ笢鍖椾晶搴?,0.000000,0.000000,""],
  ["393034","鍔叉澗娴锋枃澶у帵",0.000000,0.000000,""],
  ["393035","绠″簞鏂板ぉ鍦?,0.000000,0.000000,""],
  ["393046","灏忓ぉ绔鸿矾",0.000000,0.000000,""],
  ["393067","鏈涗含鍖楁湜閲戣緣澶у帵",0.000000,0.000000,""],
  ["393082","鍏嗘嘲鍥介檯涓績",0.000000,0.000000,""],
  ["393117","鍗侀噷鍫″湴閾佺珯",0.000000,0.000000,""],
  ["393151","棣栭兘鏈哄満T3浜屽眰鍒拌揪鍖哄",0.000000,0.000000,""],
  ["393152","棣栭兘鏈哄満T3涓夊眰鍑哄彂鍖轰笢鎸囧粖",0.000000,0.000000,""],
  ["393153","棣栭兘鏈哄満T3涓夊眰鍑哄彂鍖鸿タ鎸囧粖",0.000000,0.000000,""],
  ["393155","棣栭兘鏈哄満T3鍥涘眰鍑哄彂鍖哄涓?,0.000000,0.000000,""],
  ["393156","棣栭兘鏈哄満T3D鍑哄彂鍖?,0.000000,0.000000,""],
  ["393167","濂ユ灄鍖瑰厠妫灄鍏洯锛堝崡鍥級搴?,0.000000,0.000000,""],
  ["393168","濂ユ灄鍖瑰厠妫灄鍏洯锛堝寳鍥級搴?,0.000000,0.000000,""],
  ["393188","鍙戝睍澶у帵搴?,0.000000,0.000000,""],
  ["393206","涓滀簹鏈涗含涓績搴?,0.000000,0.000000,""],
  ["393220","姊电煶涓績搴?,0.000000,0.000000,""],
  ["393244","鍒╂槦琛屼腑蹇冨簵",0.000000,0.000000,""],
  ["393303","鍒╂槦琛屼腑蹇冧簩搴?,0.000000,0.000000,""],
  ["393380","鍏夋槑澶у帵搴?,0.000000,0.000000,""],
  ["393399","鍖椾含鍦伴搧鑽夋埧绔欏簵",0.000000,0.000000,""],
  ["393416","鍖椾含鍦伴搧鍏亾鍙ｅ簵",0.000000,0.000000,""],
  ["393425","鍖椾含鍦伴搧瑗垮湡鍩庡簵",0.000000,0.000000,""],
  ["393426","鍖椾含鍦伴搧鏈涗含瑗跨珯搴?,0.000000,0.000000,""],
  ["393546","鍖椾含娴﹂」涓績搴?,0.000000,0.000000,""],
  ["393557","鍖椾含楦挎噵鍟嗗姟澶у帵搴?,0.000000,0.000000,""],
  ["393651","鍖椾含骞冲畨鍥介檯閲戣瀺涓績搴?,0.000000,0.000000,""],
  ["393675","鍖椾含闃块噷宸村反鎬婚儴搴?,0.000000,0.000000,""],
  ["393688","鍖椾含骞挎挱澶у帵閰掑簵搴?,0.000000,0.000000,""],
  ["393706","鍙ゅ寳姘撮晣涓夊彿搴?,0.000000,0.000000,""],
  ["393707","鍖椾含绉戞妧璐㈠瘜涓績搴?,0.000000,0.000000,""],
  ["393708","鍖椾含鎬€鏌斿晢涓氳搴?,0.000000,0.000000,""],
  ["393709","浜笢鎬婚儴2鍙烽櫌搴?,0.000000,0.000000,""],
  ["393710","鍖椾含鍦伴搧娼樺鍥珯搴?,0.000000,0.000000,""],
  ["393711","寰蒋涓浗鐮斿彂闆嗗洟鎬婚儴搴?,0.000000,0.000000,""],
  ["393715","棣栭兘鍥介檯鏈哄満T3鍥涘眰鍑哄彂灞傚瑗?,0.000000,0.000000,""],
  ["393717","鍖椾含鍗楃珯鍦伴搧灞?搴?,0.000000,0.000000,""],
  ["393718","鍖椾含鍗楃珯鍑哄彂灞?搴?,0.000000,0.000000,""],
  ["393720","鏄屽钩鏂版柊鍏瘬搴?,0.000000,0.000000,""],
  ["394024","铻嶇璧勮涓績",0.000000,0.000000,""],
  ["394038","鑱旀兂鏂板洯鍖?,0.000000,0.000000,""],
  ["394048","娴锋穩鏉忕煶鍙ｈ矾",0.000000,0.000000,""],
  ["394062","鐏櫒钀ュ湴閾佺珯",0.000000,0.000000,""],
  ["394069","浜掕仈缃戝垱鏂颁腑蹇?,0.000000,0.000000,""],
  ["394070","鏈ㄨ嵎璺崕涓哄洯鍖?,0.000000,0.000000,""],
  ["394072","瓒呭競鍙戜竾鏌?,0.000000,0.000000,""],
  ["394073","瓒呭競鍙戣姳鍥矾",0.000000,0.000000,""],
  ["394075","瓒呭競鍙戠緤鍧婂簵",0.000000,0.000000,""],
  ["394085","瓒呭競鍙戠敇瀹跺彛",0.000000,0.000000,""],
  ["394089","瓒呭競鍙戜笂鍦?,0.000000,0.000000,""],
  ["394096","瓒呭競鍙戝啘澶?,0.000000,0.000000,""],
  ["394103","瓒呭競鍙戠鍗楄矾",0.000000,0.000000,""],
  ["394104","瓒呭競鍙戠帀娉夎矾",0.000000,0.000000,""],
  ["394105","瓒呭競鍙戣タ闀垮畨琛?,0.000000,0.000000,""],
  ["394110","鏂版氮鎬婚儴澶у帵",0.000000,0.000000,""],
  ["394112","瓒呭競鍙戞捣娣€澶ц",0.000000,0.000000,""],
  ["394118","瓒呭競鍙戝闄㈣矾瓒呯綏",0.000000,0.000000,""],
  ["394122","鑱旀兂涓夋爣澶у帵",0.000000,0.000000,""],
  ["394123","鑱旀兂鍖楃爺鍥ゼ澶у帵",0.000000,0.000000,""],
  ["394142","瓒呭競鍙戝弻姒嗘爲瓒呯綏",0.000000,0.000000,""],
  ["394150","缃戞槗鐮斿彂涓績",0.000000,0.000000,""],
  ["394196","瓒呭競鍙戝箔鍗楄矾搴?,0.000000,0.000000,""],
  ["394234","瓒呭競鍙戠綏妫寳娓呰矾搴?,0.000000,0.000000,""],
  ["394258","瓒呭競鍙戠綏妫寳瀹夋渤搴?,0.000000,0.000000,""],
  ["394405","鍖椾含鍦伴搧鑲叉柊绔欏簵",0.000000,0.000000,""],
  ["394406","鍖椾含鍦伴搧骞冲畨閲岀珯搴?,0.000000,0.000000,""],
  ["394419","鍖椾含鍦伴搧杞﹂亾娌熺珯搴?,0.000000,0.000000,""],
  ["394437","瓒呭競鍙戠綏妫竻娌冲簵",0.000000,0.000000,""],
  ["394451","娓呭崕绉戞妧鍥簵",0.000000,0.000000,""],
  ["394485","姘告嘲鐢熸椿鏈嶅姟涓績搴?,0.000000,0.000000,""],
  ["394488","瓒呭競鍙戠綏妫お鑸熷潪搴?,0.000000,0.000000,""],
  ["394489","瓒呭競鍙戠綏妫叚閲屽悲搴?,0.000000,0.000000,""],
  ["394510","涓滃崌绉戞妧鍥簵",0.000000,0.000000,""],
  ["394512","瓒呭競鍙戠綏妫寳娓呰矾浜屽簵",0.000000,0.000000,""],
  ["394526","鍗庝负鍖楃爺鎵€K鍖哄簵",0.000000,0.000000,""],
  ["394560","瓒呭競鍙戠綏妫寳瀹夋渤浜屽簵",0.000000,0.000000,""],
  ["394620","鍖椾含蹇墜鍏冧腑蹇冨簵",0.000000,0.000000,""],
  ["394630","瓒呭競鍙戠綏妫洓瀛ｉ潚璺簵",0.000000,0.000000,""],
  ["394705","骞宠胺姝ヨ琛楀簵",0.000000,0.000000,""],
  ["394720","鍖椾含绔欏箍鍦轰笢渚у簵",0.000000,0.000000,""],
  ["395017","姹夊▉鍥介檯骞垮満",0.000000,0.000000,""],
  ["395092","娉ユ醇鍦伴搧绔?,0.000000,0.000000,""],
  ["395274","钂查粍姒嗗湴閾佺珯搴?,0.000000,0.000000,""],
  ["395302","涓芥辰骞冲畨骞哥涓績搴?,0.000000,0.000000,""],
  ["395411","鍖椾含鍦伴搧鍏噷妗ョ珯搴?,0.000000,0.000000,""],
  ["395412","鍖椾含鍦伴搧閮叕搴勭珯搴?,0.000000,0.000000,""],
  ["395413","鍖椾含鍦伴搧涓板彴绉戞妧鍥尯绔欏簵",0.000000,0.000000,""],
  ["395470","涓扮涓績搴?,0.000000,0.000000,""],
  ["395606","绂忔捣鍏瘬",0.000000,0.000000,""],
  ["395708","鍖椾含绔?鍙峰€欒溅瀹ゅ簵",0.000000,0.000000,""],
  ["395710","鍖椾含棣栭兘鏈哄満T3涓夊眰鍑哄彂鍖哄寳鎸囧粖搴?,0.000000,0.000000,""],
  ["396053","鎴垮北鍗楀ぇ琛?,0.000000,0.000000,""],
  ["396054","鑹埂鎷辫景鍗楀ぇ琛?,0.000000,0.000000,""],
  ["396058","鎴垮北缁垮煄鐧惧悎",0.000000,0.000000,""],
  ["396081","鎴垮北鐕曞寲鍖婚櫌",0.000000,0.000000,""],
  ["396141","澶у叴鏄熺墝鍏变韩闄?,0.000000,0.000000,""],
  ["396255","涓滄柟鑸┖鍏徃鎬婚儴搴?,0.000000,0.000000,""],
  ["396276","浜笢鎬婚儴1鍙峰簵",0.000000,0.000000,""],
  ["396388","鍖椾含楦垮潳骞垮満搴?,0.000000,0.000000,""],
  ["396449","澶у叴鍥介檯姘㈣兘绀鸿寖鍖哄簵",0.000000,0.000000,""],
  ["396495","澶у叴鍗楄埅鎬婚儴鍩哄湴搴?,0.000000,0.000000,""],
  ["396706","鍖椾含鍗楃珯鍑哄彂灞?搴?,0.000000,0.000000,""],
  ["397398","鍖椾含鍦伴搧鐗╄祫瀛﹂櫌璺珯搴?,0.000000,0.000000,""],
  ["397682","鍖椾含閫氬窞鏂板厜澶?,0.000000,0.000000,""],
  ["398090","瓒呭競鍙戝ぉ閫氳嫅瑗垮尯",0.000000,0.000000,""],
  ["398170","鏄屽钩鏈潵绉戝鍩庢湭鏉ヤ腑蹇冨簵",0.000000,0.000000,""],
  ["398439","鍖椾含鍦伴搧鏄屽钩绔欏簵",0.000000,0.000000,""],
  ["398558","鍖椾含鎬€鏌旈泚鏍栦汉鎵嶇ぞ鍖哄簵",0.000000,0.000000,""],
  ["398689","鍥為緳瑙備綋鑲插叕鍥簵",0.000000,0.000000,""],
  ["398708","鍖椾含绔?鍙峰€欒溅瀹ゅ簵",0.000000,0.000000,""],
  ["398718","鍖椾含鍖楃珯搴?,0.000000,0.000000,""],
  ["399519","鍙ゅ寳姘撮晣涓€鍙峰簵",0.000000,0.000000,""],
  ["399520","鍙ゅ寳姘撮晣浜屽彿搴?,0.000000,0.000000,""],
  ["399548","鍖椾含涓捣澶у帵搴?,0.000000,0.000000,""],
  ["399569","瑗块暱瀹変腑楠忎笘鐣屽煄搴?,0.000000,0.000000,""],
  ["399670","鍖椾含闂ㄥご娌熼暱瀹夊ぉ琛楀簵",0.000000,0.000000,""],
  ["399722","鍖椾含绁ヤ簯灏忛晣搴?,0.000000,0.000000,""],
  ["409392","澶╂触甯傝摕宸炲尯澶╀竴缁挎捣搴?,0.000000,0.000000,""],
  ["461717","寤婂潑鐕曢儕澶忓▉澶疯摑婀惧簵",0.000000,0.000000,""],
  ["462710","鎵垮痉甯傚痉姹囧ぇ鍘﹀簵",0.000000,0.000000,""],
  ["462717","鎵垮痉瀹藉箍鏃朵唬骞垮満搴?,0.000000,0.000000,""],
  ["466370","寤婂潑棣欐渤鏂板紑琛楀簵",0.000000,0.000000,""],
  ["466435","寤婂潑棣欐渤鏂板崕琛楀簵",0.000000,0.000000,""],
  ["466455","寤婂潑鐕曢儕灏氫含骞垮満搴?,0.000000,0.000000,""],
  ["466506","寤婂潑鐕曢儕璇轰簹澶у帵搴?,0.000000,0.000000,""],
  ["466653","寤婂潑鐕曢儕鐕曚含鐞嗗伐瀛﹂櫌搴?,0.000000,0.000000,""],
  ["466697","寤婂潑鐕曢儕33鍙疯鍖哄簵",0.000000,0.000000,""],
  ["467708","涓夋渤鏂囧寲鑹烘湳澶ц搴?,0.000000,0.000000,""],
  ["469475","鎵垮痉甯備簩浠欏眳澶ц搴?,0.000000,0.000000,""],
  ["469476","鎵垮痉甯傚皬浣熸矡璺簵",0.000000,0.000000,""],
  ["469517","鎵垮痉甯傜鍦板崕鍥簵",0.000000,0.000000,""],
  ["469559","鎵垮痉甯傞敠缁ｅぇ琛楀簵",0.000000,0.000000,""],
  ["469585","鎵垮痉寰￠緳鐎氬簻搴?,0.000000,0.000000,""],
  ["469588","鎵垮痉閾傛偊灞卞簵",0.000000,0.000000,""],
  ["469659","鎵垮痉鍏寸洓涓芥按搴?,0.000000,0.000000,""],
  ["469660","鎵垮痉閾舵槦涓借嫅搴?,0.000000,0.000000,""],
  ["469691","鎵垮痉甯傚槈鍜屽箍鍦哄簵",0.000000,0.000000,""],
  ["471708","寮犲鍙ｅ磭绀兼堡鍗板簵-瀵岄緳瀛?,0.000000,0.000000,""],
  ["478480","寮犲鍙ｅ杈板簞鍥簵",0.000000,0.000000,""],
  ["478481","寮犲鍙ｆ槑寰峰崡璺簵",0.000000,0.000000,""],
  ["478566","寮犲鍙ｅ鍖栧叓涓簵",0.000000,0.000000,""],
  ["478567","寮犲鍙ｅ鍖栧叴娉板簵",0.000000,0.000000,""],
  ["478571","寮犲鍙ｅ鍖栭紦妤煎簵",0.000000,0.000000,""],
  ["478572","寮犲鍙ｅ鍖栧厜澶у簵",0.000000,0.000000,""],
  ["478573","寮犲鍙ｅ鍖栫殗鍩庡簵",0.000000,0.000000,""],
  ["478581","寮犲鍙ｅ彜瀹忓ぇ琛楀簵",0.000000,0.000000,""],
  ["478582","寮犲鍙ｅ寳鏂瑰闄㈣タ鏍″尯搴?,0.000000,0.000000,""],
  ["478583","寮犲鍙ｆ€€鏉ユ柊鍩庝匠鑻戝簵",0.000000,0.000000,""],
  ["478584","寮犲鍙ｆ€€鏉ユ柊涓滄柟搴?,0.000000,0.000000,""],
  ["478610","寮犲鍙ｈ亴涓氭妧鏈闄㈠簵",0.000000,0.000000,""],
  ["478648","寮犲鍙ｄ箰浜煄搴?,0.000000,0.000000,""],
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

function normalizeStoreCode(id) {
  const raw = String(id || "").trim();
  if (raw === "93003") return "393003";
  return raw;
}

function isAutoByW2Store(id) {
  return AUTO_BY_W2_STORE_IDS.has(normalizeStoreCode(id));
}

function getStoreWavePreset(id) {
  const raw = normalizeStoreCode(id);
  if (STORE_WAVE_TIME_PRESETS[raw]) return STORE_WAVE_TIME_PRESETS[raw];
  const compatId = raw.length === 5 ? `3${raw}` : "";
  return compatId ? STORE_WAVE_TIME_PRESETS[compatId] || null : null;
}

function toMinutes(v) {
  const [h, m] = String(v || "00:00").split(":").map(Number);
  return (h || 0) * 60 + (m || 0);
}
function alignMinuteToDispatch(minute, dispatchStartMin) {
  return minute < dispatchStartMin ? minute + 1440 : minute;
}
function formatTime(v) {
  const day = 24 * 60;
  const n = ((Math.round(v) % day) + day) % day;
  return `${String(Math.floor(n / 60)).padStart(2, "0")}:${String(n % 60).padStart(2, "0")}`;
}
function formatRate(v) { return `${(v * 100).toFixed(1)}%`; }
function formatMinutesValue(v) {
  const n = Number(v || 0);
  if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
  return Number(n.toFixed(1)).toString();
}
function parseStoreIds(v) { return String(v || "").split(",").map((x) => x.trim()).filter(Boolean); }
function normalizeWaveBelongsInput(v) {
  return Array.from(new Set(
    String(v || "")
      .split(",")
      .map((x) => x.trim())
      .filter((x) => /^\d+$/.test(x))
  )).join(",");
}
function parseWaveBelongs(v) {
  const normalized = normalizeWaveBelongsInput(v);
  return normalized ? normalized.split(",") : [];
}
function clone(v) { return JSON.parse(JSON.stringify(v)); }

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
  try {
    localStorage.setItem(REALTIME_DISPATCH_CONTEXT_KEY, JSON.stringify(context));
  } catch (error) {
    console.warn("[simulate] sync realtime dispatch context failed:", error);
  }
}
function buildWaveSpan(start, end) { const startMin = toMinutes(start); let endMin = toMinutes(end); if (endMin <= startMin) endMin += 1440; return { startMin, endMin }; }

function buildStores() {
  return FIXED_STORES
    .filter(([id]) => getStoreWavePreset(id))
    .map(([id, name, lng, lat, district], index) => {
      const normalizedId = normalizeStoreCode(id);
      const preset = getStoreWavePreset(normalizedId) || {};
      const autoByW2 = isAutoByW2Store(normalizedId);
      const waveArrivals = {
        w1: autoByW2 ? "" : (preset.w1 || ""),
        w2: preset.w2 || "",
        ...(preset.w3 ? { w3: preset.w3 } : {}),
        ...(preset.w4 ? { w4: preset.w4 } : {}),
      };
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

function buildAutoWaves(stores) {
  const waveBuckets = { 1: [], 2: [], 3: [], 4: [] };
  stores.forEach((store) => {
    let belongs = parseWaveBelongs(store.waveBelongs || "");
    if (!belongs.length) {
      belongs = [];
      if (store.waveArrivals?.w1) belongs.push("1");
      if (store.waveArrivals?.w2) belongs.push("2");
    }
    const seen = new Set();
    belongs.forEach((waveNo) => {
      if (!["1", "2", "3", "4"].includes(waveNo)) return;
      if (seen.has(waveNo)) return;
      seen.add(waveNo);
      waveBuckets[Number(waveNo)].push(store.id);
    });
  });
  return [
    { waveId: "W1", start: "19:10", end: "23:59", endMode: "return", storeIds: waveBuckets[1].join(",") },
    { waveId: "W2", start: "21:00", end: "07:00", endMode: "service", storeIds: waveBuckets[2].join(",") },
    { waveId: "W3", start: "19:30", end: "07:00", endMode: "service", storeIds: waveBuckets[3].join(",") },
    { waveId: "W4", start: "12:00", end: "18:00", endMode: "service", storeIds: waveBuckets[4].join(",") },
  ];
}

function sampleData() {
  const stores = buildStores();
  return {
    vehicles: [
      { plateNo: "浜珹-1001", driverName: "", type: ENFORCED_VEHICLE_TYPE, capacity: 120, speed: 38, canCarryCold: true },
      { plateNo: "浜珹-1002", driverName: "", type: "4.2绫冲帰寮忚揣杞?, capacity: 100, speed: 38, canCarryCold: false },
      { plateNo: "浜珹-1003", driverName: "", type: "4.2绫冲帰寮忚揣杞?, capacity: 100, speed: 38, canCarryCold: false },
      { plateNo: "浜珹-1004", driverName: "", type: "4.2绫冲帰寮忚揣杞?, capacity: 100, speed: 38, canCarryCold: false },
    ],
    stores,
    waves: buildAutoWaves(stores),
  };
}

function toHHMMText(v) {
  const raw = String(v || "").trim();
  if (!raw) return "";
  const m = raw.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return raw;
  return `${String(Number(m[1])).padStart(2, "0")}:${m[2]}`;
}

function mapBackendShopRow(row) {
  const code = normalizeStoreCode(row.shop_code || "");
  const waveBelongs = normalizeWaveBelongsInput(row.wave_belongs || "");
  const w1 = toHHMMText(row.wave1_time);
  const w2 = toHHMMText(row.wave2_time);
  const w3 = toHHMMText(row.arrival_time_w3 || row.wave3_time || row.wave3_arrival || "");
  const w4 = toHHMMText(row.arrival_time_w4 || row.wave4_time || row.wave4_arrival || "");
  const autoByW2 = isAutoByW2Store(code);
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

async function fetchStoresFromBackend() {
  const available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/shops/list`, {}, 3000);
  if (!response.ok) return [];
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.shops)) return [];
  return payload.shops.map(mapBackendShopRow).filter((store) => store.id && store.name);
}

function mapWmsStoreRow(row) {
  const id = normalizeStoreCode(row?.shop_code || "");
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

function mapWmsVehicleRow(row) {
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
  const available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/stores`, {}, 8000);
  if (!response.ok) return [];
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return [];
  return payload.items.map((row) => mapWmsStoreRow(row)).filter((item) => item.id && item.name);
}

async function fetchWmsVehiclesFromBackend() {
  const available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/vehicles`, {}, 8000);
  if (!response.ok) return [];
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return [];
  return payload.items.map((row) => mapWmsVehicleRow(row)).filter((item) => item.plateNo);
}

async function fetchWmsCargoLatestMap() {
  return new Map();
}

function computeResolvedLoadsFromCleanCargoAndTiming(cargo = {}, timing = {}) {
  const belongsText = normalizeWaveBelongsInput(String(timing?.wave_belongs || ""));
  const has1 = /(^|,)\s*1\s*(,|$)/.test(belongsText);
  const has2 = /(^|,)\s*2\s*(,|$)/.test(belongsText);
  const has3 = /(^|,)\s*3\s*(,|$)/.test(belongsText);
  const has4 = /(^|,)\s*4\s*(,|$)/.test(belongsText);
  const only2 = belongsText === "2";
  const rpcs = Number(cargo?.rpcs || 0);
  const rcase = Number(cargo?.rcase || 0);
  const bpcs = Number(cargo?.bpcs || 0);
  const bpaper = Number(cargo?.bpaper || 0);
  const apcs = Number(cargo?.apcs || 0);
  const apaper = Number(cargo?.apaper || 0);
  const rpaper = Number(cargo?.rpaper || 0);
  const baseW1 = (rpcs / 207) + (rcase / 380) + (bpcs / 120) + (bpaper / 380) + (rpaper / 380);
  const baseW2 = (apcs / 350) + (apaper / 380);
  const total = baseW1 + baseW2;
  let w1 = 0;
  let w2 = 0;
  let w3 = 0;
  let w4 = 0;
  if (only2) {
    w2 = total;
  } else if (has3 || has4) {
    if (has3) w3 = total;
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

async function fetchCleanCargoRawMap(shopCodes = []) {
  const available = await ensureGaBackendAvailable(true);
  if (!available) return new Map();
  const codes = Array.isArray(shopCodes) ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean) : [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/clean-cargo-raw/list?limit=5000`, {}, 12000);
  if (!response.ok) return new Map();
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return new Map();
  const allowed = codes.length ? new Set(codes) : null;
  const map = new Map();
  payload.items.forEach((row) => {
    const key = normalizeStoreCode(row?.shop_code || "");
    if (!key) return;
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
  return map;
}

async function fetchStoreWaveTimingMap(shopCodes = []) {
  const available = await ensureGaBackendAvailable(true);
  if (!available) return new Map();
  const codes = Array.isArray(shopCodes) ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean) : [];
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/store-wave-timing-resolved/list?limit=5000`, {}, 12000);
  if (!response.ok) return new Map();
  const payload = await response.json();
  if (!payload?.ok || !Array.isArray(payload.items)) return new Map();
  const allowed = codes.length ? new Set(codes) : null;
  const map = new Map();
  payload.items.forEach((row) => {
    const key = normalizeStoreCode(row?.shop_code || "");
    if (!key) return;
    if (allowed && !allowed.has(key)) return;
    map.set(key, {
      wave_belongs: String(row?.wave_belongs || "").trim(),
      first_wave_time: toHHMMText(String(row?.first_wave_time || "").trim()),
      second_wave_time: toHHMMText(String(row?.second_wave_time || "").trim()),
      arrival_time_w3: toHHMMText(String(row?.arrival_time_w3 || "").trim()),
      arrival_time_w4: toHHMMText(String(row?.arrival_time_w4 || "").trim()),
    });
  });
  return map;
}

async function fetchStoreWaveResolvedLoadMap(shopCodes = []) {
  const codes = Array.isArray(shopCodes)
    ? shopCodes.map((code) => normalizeStoreCode(code)).filter(Boolean)
    : [];
  const [cargoMap, timingMap] = await Promise.all([
    fetchCleanCargoRawMap(codes),
    fetchStoreWaveTimingMap(codes),
  ]);
  const allKeys = new Set([...cargoMap.keys(), ...timingMap.keys()]);
  const map = new Map();
  allKeys.forEach((key) => {
    const resolved = computeResolvedLoadsFromCleanCargoAndTiming(cargoMap.get(key) || {}, timingMap.get(key) || {});
    map.set(key, { shop_code: key, ...resolved });
  });
  return map;
}

async function fetchStoreWaveResolvedList({ shopCode = "", waveBelongs = "", limit = 200 } = {}) {
  const available = await ensureGaBackendAvailable(true);
  if (!available) return { ok: false, items: [], count: 0, total: 0, error: "backend_unavailable" };
  const params = new URLSearchParams();
  if (shopCode) params.set("shopCode", String(shopCode).trim());
  if (waveBelongs) params.set("waveBelongs", String(waveBelongs).trim());
  params.set("limit", String(Math.max(1, Math.min(2000, Number(limit || 200) || 200))));
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/store-wave-load-resolved/list?${params.toString()}`, {}, 12000);
  if (!response.ok) return { ok: false, items: [], count: 0, total: 0, error: `http_${response.status}` };
  const payload = await response.json();
  return payload || { ok: false, items: [], count: 0, total: 0 };
}

function buildStoreWaveResolvedQueryConfirmText(payload = {}) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const byWave = { "1": [], "2": [], "3": [], "4": [] };
  items.forEach((row) => {
    const code = String(row?.shop_code || "").trim();
    const belongs = parseWaveBelongs(String(row?.wave_belongs || "").trim());
    if (belongs.includes("1")) byWave["1"].push(`${code} | 璐ч噺=${Number(row?.wave1_load || 0)} | 鏃堕棿=${String(row?.first_wave_time || "").trim() || "(绌?"}`);
    if (belongs.includes("2")) byWave["2"].push(`${code} | 璐ч噺=${Number(row?.wave2_load || 0)} | 鏃堕棿=${String(row?.second_wave_time || "").trim() || "(绌?"}`);
    if (belongs.includes("3")) byWave["3"].push(`${code} | 璐ч噺=${Number(row?.wave3_load || 0)} | 鏃堕棿=${String(row?.arrival_time_w3 || "").trim() || "(绌?"}`);
    if (belongs.includes("4")) byWave["4"].push(`${code} | 璐ч噺=${Number(row?.wave4_load || 0)} | 鏃堕棿=${String(row?.arrival_time_w4 || "").trim() || "(绌?"}`);
  });
  return [
    "鍗冲皢鍐欏叆鈥滄姌绠楄揣閲忔煡璇⑩€濆睍绀哄尯锛堟潵婧愶細store_wave_load_resolved锛?,
    `杩斿洖鏉℃暟锛?{Number(payload?.count || items.length || 0)} / 鎬昏锛?{Number(payload?.total || items.length || 0)}`,
    "",
    `W1 鏄庣粏(${byWave["1"].length})`,
    ...byWave["1"],
    "",
    `W2 鏄庣粏(${byWave["2"].length})`,
    ...byWave["2"],
    "",
    `W3 鏄庣粏(${byWave["3"].length})`,
    ...byWave["3"],
    "",
    `W4 鏄庣粏(${byWave["4"].length})`,
    ...byWave["4"],
    "",
    "鐐瑰嚮鈥滅‘瀹氣€濈户缁啓鍏ユ煡璇㈢粨鏋滐紝鐐瑰嚮鈥滃彇娑堚€濇斁寮冩湰娆″啓鍏ャ€?
  ].join("\n");
}

function showStoreWaveResolvedQueryConfirmDialog(reportText) {
  return new Promise((resolve) => {
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

    const title = document.createElement("div");
    title.textContent = "鏌ヨ纭锛坰tore_wave_load_resolved锛?;
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

    const footer = document.createElement("div");
    footer.style.display = "flex";
    footer.style.gap = "12px";
    footer.style.justifyContent = "flex-end";
    footer.style.padding = "12px 16px 16px";

    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.textContent = "鍙栨秷";
    cancelBtn.style.minWidth = "96px";
    cancelBtn.style.height = "40px";

    const okBtn = document.createElement("button");
    okBtn.type = "button";
    okBtn.textContent = "纭畾";
    okBtn.style.minWidth = "96px";
    okBtn.style.height = "40px";

    const finish = (ok) => {
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
    document.body.appendChild(overlay);
  });
}

function renderStoreWaveResolvedTable(payload = {}) {
  const summary = document.getElementById("storeWaveResolvedSummary");
  const body = document.getElementById("storeWaveResolvedTableBody");
  if (!summary || !body) return;
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const sortField = String(state.ui.storeWaveResolvedSortField || "shop_code");
  const sortDir = String(state.ui.storeWaveResolvedSortDir || "asc") === "desc" ? "desc" : "asc";
  const toSortValue = (row, field) => {
    if (field === "updated_at") return String(row?.updated_at || "");
    if (field === "wave_belongs" || field === "shop_code" || field === "first_wave_time" || field === "second_wave_time" || field === "arrival_time_w3" || field === "arrival_time_w4") {
      return String(row?.[field] || "");
    }
    return Number(row?.[field] || 0) || 0;
  };
  const items = rawItems.slice().sort((a, b) => {
    const av = toSortValue(a, sortField);
    const bv = toSortValue(b, sortField);
    let cmp = 0;
    if (typeof av === "number" && typeof bv === "number") cmp = av - bv;
    else cmp = String(av).localeCompare(String(bv), "zh-Hans-CN", { numeric: true });
    return sortDir === "asc" ? cmp : -cmp;
  });
  const count = Number(payload.count || items.length || 0);
  const total = Number(payload.total || count || 0);
  const limit = Number(payload.limit || 0);
  summary.textContent = `褰撳墠杩斿洖 ${count} 鏉?/ 鎬昏 ${total} 鏉?{limit ? `锛坙imit=${limit}锛塦 : ""}`;
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="12" class="muted">鏃犲尮閰嶆暟鎹?/td></tr>`;
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
  document.querySelectorAll("[data-store-wave-sort]").forEach((btn) => {
    const field = String(btn.getAttribute("data-store-wave-sort") || "");
    const active = field === String(state.ui.storeWaveResolvedSortField || "");
    const mark = active ? (state.ui.storeWaveResolvedSortDir === "desc" ? "鈻? : "鈻?) : "";
    const markNode = btn.querySelector(".store-wave-sort-mark");
    if (markNode) markNode.textContent = mark;
  });
}

function toggleStoreWaveResolvedSort(field) {
  const nextField = String(field || "").trim();
  if (!nextField) return;
  if (state.ui.storeWaveResolvedSortField === nextField) {
    state.ui.storeWaveResolvedSortDir = state.ui.storeWaveResolvedSortDir === "asc" ? "desc" : "asc";
  } else {
    state.ui.storeWaveResolvedSortField = nextField;
    state.ui.storeWaveResolvedSortDir = "asc";
  }
  renderStoreWaveResolvedTable(storeWaveResolvedCache);
  updateStoreWaveResolvedSortMarks();
}

async function queryStoreWaveResolvedTable({ needConfirm = false } = {}) {
  const shopCode = String(document.getElementById("storeWaveResolvedShopCodeInput")?.value || "").trim();
  const waveBelongs = String(document.getElementById("storeWaveResolvedWaveBelongsInput")?.value || "").trim();
  const limit = Number(document.getElementById("storeWaveResolvedLimitInput")?.value || 200);
  const payload = await fetchStoreWaveResolvedList({ shopCode, waveBelongs, limit });
  if (needConfirm) {
    const confirmText = buildStoreWaveResolvedQueryConfirmText(payload);
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

async function buildResolvedLoadRowsFromDualTables() {
  const storeIds = (Array.isArray(state.stores) ? state.stores : [])
    .map((store) => normalizeStoreCode(store?.id || store?.shopCode || ""))
    .filter(Boolean);
  const resolvedMap = await fetchStoreWaveResolvedLoadMap(storeIds);
  const rows = [];
  resolvedMap.forEach((resolved, shopCode) => {
    const belongs = String(resolved?.wave_belongs || "").trim();
    const belongsSet = new Set(parseWaveBelongs(belongs));
    const wave1 = belongsSet.has("1") ? (Number(resolved?.wave1_load || 0) || 0) : 0;
    const wave2 = belongsSet.has("2") ? (Number(resolved?.wave2_load || 0) || 0) : 0;
    const wave3 = belongsSet.has("3") ? (Number(resolved?.wave3_load || 0) || 0) : 0;
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
  return rows;
}

async function saveDualTableLoadsToBackend() {
  const available = await ensureGaBackendAvailable(true);
  if (!available) throw new Error("backend_unavailable");
  const rows = await buildResolvedLoadRowsFromDualTables();
  const formatWaveDetails = (waveNo, fieldLoad, fieldTime) => {
    const picked = rows.filter((row) => parseWaveBelongs(row.wave_belongs || "").includes(String(waveNo)));
    const lines = picked.map((row) => {
      const load = Number(row?.[fieldLoad] || 0);
      const time = String(row?.[fieldTime] || "").trim();
      return `${row.shop_code} | 璐ч噺=${load} | 鏃堕棿=${time || "(绌?"}`;
    });
    return { count: picked.length, lines };
  };
  const w1 = formatWaveDetails("1", "wave1_load", "first_wave_time");
  const w2 = formatWaveDetails("2", "wave2_load", "second_wave_time");
  const w3 = formatWaveDetails("3", "wave3_load", "arrival_time_w3");
  const w4 = formatWaveDetails("4", "wave4_load", "arrival_time_w4");
  const response = await fetchJsonWithTimeout(
    `${GA_BACKEND_URL}/store-wave-load-resolved/save`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ rows })
    },
    20000
  );
  if (!response.ok) {
    throw new Error(`http_${response.status}`);
  }
  const payload = await response.json();
  const reportText = [
    "淇濆瓨鍙岃〃璐ч噺缁撴灉锛堝啓鍏ョ洰鏍囪〃锛歴tore_wave_load_resolved锛?,
    `鎬昏鏁帮細${rows.length}`,
    `鍚庣杩斿洖 upserted锛?{Number(payload?.upserted || 0)}`,
    "",
    `W1 鍐欏叆 ${w1.count} 琛岋細`,
    ...w1.lines,
    "",
    `W2 鍐欏叆 ${w2.count} 琛岋細`,
    ...w2.lines,
    "",
    `W3 鍐欏叆 ${w3.count} 琛岋細`,
    ...w3.lines,
    "",
    `W4 鍐欏叆 ${w4.count} 琛岋細`,
    ...w4.lines,
  ].join("\n");
  return { ...(payload || {}), reportText };
}

function showSaveDualLoadsReport(reportText) {
  const text = String(reportText || "").trim();
  if (!text) {
    window.alert("淇濆瓨瀹屾垚");
    return;
  }
  const popup = window.open("", "_blank", "width=980,height=760");
  if (!popup) {
    window.alert(text);
    return;
  }
  const escaped = escapeHtml(text);
  popup.document.open();
  popup.document.write(`
    <!doctype html>
    <html lang="zh-CN">
    <head>
      <meta charset="utf-8" />
      <title>淇濆瓨鍙岃〃璐ч噺鏄庣粏</title>
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

function normalizeStoreWaveLoads(store = {}) {
  const details = normalizeStoreLoadDetails(store);
  const tripCount = Math.max(1, Number(store.tripCount || 1) || 1);
  const belongs = parseWaveBelongs(store.waveBelongs || "");
  const onlyWave2 = belongs.length === 1 && belongs[0] === "2";
  const hasWave3 = belongs.includes("3");
  const hasWave4 = belongs.includes("4");
  const wave1GroupLoad = details.rpcs + details.rcase + details.bpcs + details.bpaper + details.rpaper;
  const wave2GroupLoad = details.apcs + details.apaper;
  const rawTotal = wave1GroupLoad + wave2GroupLoad;
  let wave1TotalLoadBase = Math.max(0, Number(store.wave1TotalLoadBase || 0) || 0);
  let wave2TotalLoadBase = Math.max(0, Number(store.wave2TotalLoadBase || 0) || 0);
  // 浼樺厛瑙勫垯锛氫粎灞炰簬2娉㈡鐨勫簵锛屼竴娉㈡涓嶇畻璐ч噺锛涗簩娉㈡鎸夊叏閮ㄧ被鍨嬭绠椼€?
  // 鍏紡锛歳pcs/207 + rcase/380 + bpcs/120 + bpaper/380 + rpaper/380 + apcs/350 + apaper/380
  const wave1Load = onlyWave2 ? 0 : (tripCount === 2 ? wave1GroupLoad : (wave1GroupLoad + wave2GroupLoad));
  const wave2Load = onlyWave2 ? rawTotal : (tripCount === 2 ? wave2GroupLoad : 0);
  const wave1TotalLoad = onlyWave2 ? 0 : (tripCount === 2 ? wave1TotalLoadBase : (wave1TotalLoadBase + wave2TotalLoadBase));
  const wave2TotalLoad = onlyWave2 ? (wave1TotalLoadBase + wave2TotalLoadBase) : (tripCount === 2 ? wave2TotalLoadBase : 0);
  // W3/W4 鐨勫熀纭€鍙ｅ緞鐩稿悓锛堝叏閮ㄨ揣閲忎竴娆￠€佸畬锛夛紝浣嗘渶缁堝彧钀藉湪鎵€灞炴尝娆″垪
  const baseW3Load = rawTotal;
  const baseW4Load = rawTotal;
  const baseW3TotalLoad = wave1TotalLoadBase + wave2TotalLoadBase;
  const baseW4TotalLoad = wave1TotalLoadBase + wave2TotalLoadBase;
  const wave3Load = hasWave3 ? baseW3Load : 0;
  const wave4Load = hasWave4 ? baseW4Load : 0;
  const wave3TotalLoad = hasWave3 ? baseW3TotalLoad : 0;
  const wave4TotalLoad = hasWave4 ? baseW4TotalLoad : 0;
  const totalLoad = wave1TotalLoad + wave2TotalLoad;
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
  return store;
}

function applyWmsCargoToStores(stores, cargoMap) {
  if (!Array.isArray(stores)) return [];
  const map = cargoMap instanceof Map ? cargoMap : new Map();
  return stores.map((store) => {
    const key = normalizeStoreCode(store?.id || "");
    const cargo = map.get(key);
    if (!cargo) {
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

function enrichStores(stores) {
  return stores.map((s) => {
    const merged = { ...s };
    syncStoreWaveLoads(merged);
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

function getStoreTimingForWave(store, wave, dispatchStartMin) {
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
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
  if (!desiredArrival) {
    throw new Error(`missing_desired_arrival:${store?.id || ""}:${waveId || "UNKNOWN"}`);
  }
  const desiredArrivalMin = alignMinuteToDispatch(toMinutes(desiredArrival), dispatchStartMin);
  const allowedLateMinutes = Number(store.allowedLateMinutes ?? store.parking ?? 10);
  return {
    desiredArrival,
    desiredArrivalMin,
    allowedLateMinutes,
    latestAllowedArrivalMin: desiredArrivalMin + allowedLateMinutes,
  };
}

function isStoreInWave(store, wave) {
  const waveNo = String(wave?.waveId || "").trim().toUpperCase();
  const belongs = parseWaveBelongs(store?.waveBelongs || "");
  if (!belongs.length) return false;
  if (waveNo.includes("W1") || waveNo === "1" || waveNo === "FIRST") return belongs.includes(1);
  if (waveNo.includes("W2") || waveNo === "2" || waveNo === "SECOND") return belongs.includes(2);
  if (waveNo.includes("W3") || waveNo === "3" || waveNo === "THIRD") return belongs.includes(3);
  if (waveNo.includes("W4") || waveNo === "4" || waveNo === "FOURTH") return belongs.includes(4);
  return false;
}

function isSecondDeliveryWave(wave = {}) {
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
  return waveId === "W2" || waveId === "2" || waveId === "SECOND" || waveId.includes("W2");
}

function getVehicleSolveCapacity(vehicle = {}) {
  const cap = Number(vehicle?.solveCapacity || 1);
  return cap > 0 ? cap : 1;
}

function getStoreSolveLoadForWave(store = {}, wave = {}) {
  const waveId = String(wave?.waveId || "").trim().toUpperCase();
  const wave1 = Math.max(0, Number(store?.wave1TotalLoad || 0) || 0);
  const wave2 = Math.max(0, Number(store?.wave2TotalLoad || 0) || 0);
  const wave3 = Math.max(0, Number(store?.wave3TotalLoad || 0) || 0);
  const wave4 = Math.max(0, Number(store?.wave4TotalLoad || 0) || 0);
  if (waveId === "W4" || waveId === "4" || waveId === "FOURTH" || waveId.includes("W4")) return wave4;
  if (waveId === "W3" || waveId === "3" || waveId === "THIRD" || waveId.includes("W3")) return wave3;
  return isSecondDeliveryWave(wave) ? wave2 : wave1;
}

function getStoreWaveLoadByWaveId(store = {}, waveId = "") {
  const normalized = String(waveId || "").trim().toUpperCase();
  if (normalized === "W1" || normalized === "1" || normalized === "FIRST") {
    return Math.max(0, Number(store?.resolvedWave1Load ?? 0) || 0);
  }
  if (normalized === "W2" || normalized === "2" || normalized === "SECOND") {
    return Math.max(0, Number(store?.resolvedWave2Load ?? 0) || 0);
  }
  if (normalized === "W3" || normalized === "3" || normalized === "THIRD") {
    return Math.max(0, Number(store?.resolvedWave3Load ?? 0) || 0);
  }
  if (normalized === "W4" || normalized === "4" || normalized === "FOURTH") {
    return Math.max(0, Number(store?.resolvedWave4Load ?? 0) || 0);
  }
  return 0;
}

function isStoreCandidateForWaveRule(store = {}, waveId = "") {
  const waveText = String(waveId || "").trim().toUpperCase();
  const belongs = parseWaveBelongs(store?.waveBelongs || "");
  const belongsWave =
    (waveText === "W1" && belongs.includes("1"))
    || (waveText === "W2" && belongs.includes("2"))
    || (waveText === "W3" && belongs.includes("3"))
    || (waveText === "W4" && belongs.includes("4"));
  if (!belongsWave) return false;
  return getStoreWaveLoadByWaveId(store, waveText) > 0;
}

function calcLoadConvertTerm(details, field) {
  const quantity = Math.max(0, Number(details?.[field] || 0) || 0);
  const capacity = Math.max(0, Number(LOAD_CONVERT_CAPACITY_MAP[field] || 0) || 0);
  const value = capacity > 0 ? (quantity / capacity) : 0;
  return { field, quantity, capacity, value };
}

function buildStoreLoadConvertPreview(store = {}) {
  const details = normalizeStoreLoadDetails(store);
  const belongs = parseWaveBelongs(store.waveBelongs || "");
  const onlyWave2 = belongs.length === 1 && belongs[0] === "2";
  const hasWave3 = belongs.includes("3");
  const hasWave4 = belongs.includes("4");
  const tripCount = Math.max(1, Number(store.tripCount || 1) || 1);
  const wave1Terms = LOAD_CONVERT_WAVE1_FIELDS.map((field) => calcLoadConvertTerm(details, field));
  const wave2Terms = LOAD_CONVERT_WAVE2_FIELDS.map((field) => calcLoadConvertTerm(details, field));
  const wave1Base = wave1Terms.reduce((sum, term) => sum + term.value, 0);
  const wave2Base = wave2Terms.reduce((sum, term) => sum + term.value, 0);
  const wave34Base = wave1Base + wave2Base;
  const wave1Total = onlyWave2 ? 0 : (tripCount === 2 ? wave1Base : (wave1Base + wave2Base));
  const wave2Total = onlyWave2 ? (wave1Base + wave2Base) : (tripCount === 2 ? wave2Base : 0);
  const wave3Total = hasWave3 ? wave34Base : 0;
  const wave4Total = hasWave4 ? wave34Base : 0;
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

function buildLoadConvertPreview(stores = []) {
  const items = (Array.isArray(stores) ? stores : []).map((store, index) => ({
    index,
    ...buildStoreLoadConvertPreview(store),
  }));
  const wave1Sum = items.reduce((sum, item) => sum + Number(item.wave1Total || 0), 0);
  const wave2Sum = items.reduce((sum, item) => sum + Number(item.wave2Total || 0), 0);
  const wave3Sum = items.reduce((sum, item) => sum + Number(item.wave3Total || 0), 0);
  const wave4Sum = items.reduce((sum, item) => sum + Number(item.wave4Total || 0), 0);
  return { items, wave1Sum, wave2Sum, wave3Sum, wave4Sum };
}

function roundLoadConvertValue(value, digits = 2) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(digits));
}

function formatLoadConvertValue(value) {
  return roundLoadConvertValue(value, 2);
}

function renderLoadConvertModal(preview) {
  const summary = document.getElementById("loadConvertSummary");
  const table = document.getElementById("loadConvertTable");
  if (!summary || !table) return;
  const wave1Sum = formatLoadConvertValue(preview?.wave1Sum || 0);
  const wave2Sum = formatLoadConvertValue(preview?.wave2Sum || 0);
  const wave3Sum = formatLoadConvertValue(preview?.wave3Sum || 0);
  const wave4Sum = formatLoadConvertValue(preview?.wave4Sum || 0);
  summary.innerHTML = `
    <span class="chip">涓€娉㈡鎶樼畻鍚堣锛?{wave1Sum}</span>
    <span class="chip">浜屾尝娆℃姌绠楀悎璁★細${wave2Sum}</span>
    <span class="chip">涓夋尝娆℃姌绠楀悎璁★細${wave3Sum}</span>
    <span class="chip">鍥涙尝娆℃姌绠楀悎璁★細${wave4Sum}</span>
    <span class="chip">闂ㄥ簵鏁帮細${(preview?.items || []).length}</span>
  `;
  const rows = (preview?.items || []).map((item) => {
    const wave1Expr = item.onlyWave2
      ? "浠?娉㈡搴楋細涓€娉㈡=0"
      : (item.wave1Terms.map((term) => `${term.field}:${term.quantity}/${term.capacity || 0}=${formatLoadConvertValue(term.value)}`).join(" + ") || "0");
    const wave2ExprTerms = item.onlyWave2 ? [...item.wave1Terms, ...item.wave2Terms] : item.wave2Terms;
    const wave2Expr = wave2ExprTerms.map((term) => `${term.field}:${term.quantity}/${term.capacity || 0}=${formatLoadConvertValue(term.value)}`).join(" + ") || "0";
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
        <th>搴楅摵缂栧彿</th>
        <th>搴楅摵鍚嶇О</th>
        <th>娆℃暟</th>
        <th>涓€娉㈡鎶樼畻</th>
        <th>涓€娉㈡璁＄畻杩囩▼</th>
        <th>浜屾尝娆℃姌绠?/th>
        <th>浜屾尝娆¤绠楄繃绋?/th>
        <th>涓夋尝娆℃姌绠?/th>
        <th>鍥涙尝娆℃姌绠?/th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  `;
}

function applyLoadConvertPreviewToStores(preview) {
  if (!preview || !Array.isArray(preview.items)) return;
  preview.items.forEach((item) => {
    const store = state.stores[item.index];
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

function applyStoreWaveResolvedLoadsToStores(stores, resolvedMap) {
  if (!Array.isArray(stores)) return [];
  const map = resolvedMap instanceof Map ? resolvedMap : new Map();
  return stores.map((store) => {
    const key = normalizeStoreCode(store?.id || "");
    const resolved = map.get(key);
    if (!resolved) {
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
    const arrivals = {};
    if (belongs.includes("1")) arrivals.w1 = String(resolved.first_wave_time || "").trim();
    if (belongs.includes("2")) arrivals.w2 = String(resolved.second_wave_time || "").trim();
    if (belongs.includes("3")) arrivals.w3 = String(resolved.arrival_time_w3 || "").trim();
    if (belongs.includes("4")) arrivals.w4 = String(resolved.arrival_time_w4 || "").trim();
    next.waveArrivals = arrivals;
    return next;
  });
}

function openLoadConvertModal() {
  const modal = document.getElementById("loadConvertModal");
  if (!modal) return;
  loadConvertPreviewCache = buildLoadConvertPreview(state.stores);
  renderLoadConvertModal(loadConvertPreviewCache);
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

function closeLoadConvertModal() {
  const modal = document.getElementById("loadConvertModal");
  if (!modal) return;
  applyLoadConvertPreviewToStores(loadConvertPreviewCache);
  loadConvertPreviewCache = null;
  renderStoresTable();
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

let pendingSolveContinuation = null;

function describeSolveField(value, { required = false } = {}) {
  const text = value === null || value === undefined || String(value).trim() === "" ? "缂哄け" : String(value).trim();
  const ok = text !== "缂哄け";
  return {
    ok,
    text,
    label: ok ? "宸插彇寰? : (required ? "缂哄け锛堜細鍗′綇姹傝В锛? : "缂哄け"),
  };
}

async function buildSolveDiagnoseReport() {
  const stores = Array.isArray(state.stores) ? state.stores : [];
  const vehicles = Array.isArray(state.vehicles) ? state.vehicles : [];
  const waves = Array.isArray(state.waves) ? state.waves : [];
  const dispatchStartMin = toMinutes(state.settings.dispatchStartTime || "19:10");
  const resolvedPayload = await fetchStoreWaveResolvedList({ limit: 5000 });
  const resolvedItems = Array.isArray(resolvedPayload?.items) ? resolvedPayload.items : [];
  const resolvedByShop = new Map();
  resolvedItems.forEach((row) => {
    const key = normalizeStoreCode(row?.shop_code || "");
    if (!key) return;
    resolvedByShop.set(key, row);
  });
  const scenarioFields = [
    ["dispatchStartMin", describeSolveField(dispatchStartMin, { required: true })],
    ["maxRouteKm", describeSolveField(state.settings.maxRouteKm, { required: true })],
    ["concentrateLate", describeSolveField(state.settings.concentrateLate)],
    ["dist", { ok: false, text: "鏈湪寮圭獥闃舵鏋勫缓锛涙眰瑙ｆ椂 buildScenario 鎵嶄細鐢熸垚", label: "鏈洿鎺ュ彇寰? }],
    ["storeMap", { ok: stores.length > 0, text: `褰撳墠闂ㄥ簵鏁?${stores.length}`, label: stores.length > 0 ? "宸插彇寰? : "缂哄け锛堟棤闂ㄥ簵锛? }],
    ["vehicles", { ok: vehicles.length > 0, text: `褰撳墠杞﹁締鏁?${vehicles.length}`, label: vehicles.length > 0 ? "宸插彇寰? : "缂哄け锛堟棤杞﹁締锛? }],
  ];

  const storeRows = stores.map((store) => {
    const id = String(store?.id || "").trim();
    const row = resolvedByShop.get(normalizeStoreCode(id)) || {};
    const belongsText = String(row?.wave_belongs || "").trim();
    const belongs = parseWaveBelongs(belongsText);
    const wave1 = String(row?.first_wave_time || "").trim();
    const wave2 = String(row?.second_wave_time || "").trim();
    const wave3 = String(row?.arrival_time_w3 || "").trim();
    const wave4 = String(row?.arrival_time_w4 || "").trim();
    const waveTime = { 1: wave1, 2: wave2, 3: wave3, 4: wave4 };
    const requiredWaves = belongs.length ? belongs : [1];
    const timingIssues = requiredWaves.filter((waveNo) => !String(waveTime[waveNo] || "").trim()).map((waveNo) => `W${waveNo}缂篳);
    const nonRequiredPresent = [1, 2, 3, 4]
      .filter((waveNo) => !requiredWaves.includes(waveNo))
      .filter((waveNo) => String(waveTime[waveNo] || "").trim())
      .map((waveNo) => `W${waveNo}鏈夊€间絾鏈弬涓巂);
    return {
      id,
      name: String(store?.name || "").trim(),
      boxes: Number(row?.total_resolved_load || 0),
      waveBelongs: belongsText,
      desiredArrival: requiredWaves.map((waveNo) => `W${waveNo}=${String(waveTime[waveNo] || "").trim() || "缂?}`).join(" / "),
      latestAllowedArrivalMin: Number.isFinite(Number(store?.parking ?? store?.allowedLateMinutes)) ? Number(store?.parking ?? store?.allowedLateMinutes) : 0,
      actualServiceMinutes: Number(store?.serviceMinutes ?? 0),
      serviceMinutes: Number(store?.serviceMinutes || 0),
      coldRatio: Number(store?.coldRatio || 0),
      difficulty: Number(store?.difficulty || 0),
      parking: Number(store?.parking || 0),
      lng: Number(store?.lng || 0),
      lat: Number(store?.lat || 0),
      requiredWaves: requiredWaves.map((waveNo) => `W${waveNo}`).join("銆?),
      directStatus: (!resolvedByShop.has(normalizeStoreCode(id)))
        ? "褰撳墠闂ㄥ簵鍦?store_wave_load_resolved 涓棤璁板綍"
        : (timingIssues.length ? `褰撳墠姹傝В鐩稿叧娉㈡缂哄皯 ${timingIssues.join("锛?)}` : "褰撳墠姹傝В鐩稿叧娉㈡閮藉凡鍙栧緱"),
      fallbackStatus: nonRequiredPresent.length ? `鍏朵粬娉㈡鍑虹幇鏁板€间細姹℃煋涓氬姟锛?{nonRequiredPresent.join("锛?)}` : "鍏朵粬娉㈡涓虹┖姝ｅ父",
    };
  });

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

  const selectedAlgorithms = (state.settings.solveStrategy || "manual") === "free"
    ? getEffectiveFreeAlgorithms()
    : strategyPreset(state.settings.solveStrategy || "manual", state.settings.optimizeGoal || "balanced").algorithms;

  return {
    summary: {
      strategy: currentStrategyLabel(),
      goal: currentGoalLabel(),
      algorithms: selectedAlgorithms.map((key) => algoLabel(key)).join("銆?),
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

function renderSolveDiagnoseModal(report) {
  const summary = document.getElementById("solveDiagnoseSummary");
  const body = document.getElementById("solveDiagnoseBody");
  if (!summary || !body) return;
  const summaryParts = [
    `褰撳墠绛栫暐锛?{escapeHtml(report?.summary?.strategy || "-")}`,
    `鐩爣锛?{escapeHtml(report?.summary?.goal || "-")}`,
    `绠楁硶锛?{escapeHtml(report?.summary?.algorithms || "-")}`,
    `闂ㄥ簵 ${Number(report?.summary?.storeCount || 0)} 瀹禶,
    `杞﹁締 ${Number(report?.summary?.vehicleCount || 0)} 鍙癭,
    `娉㈡ ${Number(report?.summary?.waveCount || 0)} 涓猔,
  ];
  summary.innerHTML = summaryParts.map((text) => `<span class="chip">${text}</span>`).join("");

  const scenarioRows = (report?.scenarioFields || []).map(([key, item]) => `
    <tr>
      <td>${escapeHtml(key)}</td>
      <td>${escapeHtml(item.label || "")}</td>
      <td>${escapeHtml(item.text || "")}</td>
    </tr>
  `).join("");
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
      <td>${escapeHtml(row.canCarryCold ? "鏄? : "鍚?)}</td>
      <td>${escapeHtml(String(row.priorRegularDistance ?? 0))}</td>
      <td>${escapeHtml(String(row.priorWaveCount ?? 0))}</td>
      <td>${escapeHtml(String(row.earliestDepartureMin ?? 0))}</td>
      <td>${escapeHtml(String(row.routes ?? 0))}</td>
    </tr>
  `).join("");
  const waveRows = (report?.waveRows || []).map((row) => `
    <tr>
      <td>${escapeHtml(row.waveId || "")}</td>
      <td>${escapeHtml(String(row.startMin ?? 0))}</td>
      <td>${escapeHtml(String(row.endMin ?? 0))}</td>
      <td>${escapeHtml(row.endMode || "")}</td>
      <td>${escapeHtml(row.relaxEnd ? "鏄? : "鍚?)}</td>
      <td>${escapeHtml(row.singleWave ? "鏄? : "鍚?)}</td>
      <td>${escapeHtml(row.isNightWave ? "鏄? : "鍚?)}</td>
      <td>${escapeHtml(String(row.earliestDepartureMin ?? 0))}</td>
      <td>${escapeHtml(String(row.storeCount ?? 0))}</td>
    </tr>
  `).join("");
  body.innerHTML = `
    <div class="table-wrap" style="color:#111;">
      <table class="load-convert-table">
        <thead><tr><th>瀛楁</th><th>鐘舵€?/th><th>璇存槑</th></tr></thead>
        <tbody>${scenarioRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">闂ㄥ簵瀛楁璇婃柇锛氬彧鐪嬭闂ㄥ簵鎵€灞炴尝娆＄殑鏃堕棿銆傛湭鍙備笌娉㈡涓虹┖鏄甯镐笟鍔＄姸鎬侊紝鏈弬涓庢尝娆″鏋滄湁鍊间細琚涓烘薄鏌撱€?/div>
      <table class="load-convert-table">
        <thead><tr><th>闂ㄥ簵</th><th>鍚嶇О</th><th>娉㈡褰掑睘</th><th>搴旀鏌ユ尝娆?/th><th>boxes</th><th>鏃堕棿鐘舵€?/th><th>璇存槑</th><th>鏃堕棿鍐呭</th><th>瀹為檯鏈嶅姟</th><th>澶囩敤鏈嶅姟</th><th>parking</th></tr></thead>
        <tbody>${storeRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">杞﹁締瀛楁璇婃柇锛歝apacity / speed / 鍙惁鍐烽摼 / 鍘嗗彶閲岀▼ / 鍘嗗彶娉㈡ / routes 鏄惁鑳界洿鎺ュ彇寰椼€?/div>
      <table class="load-convert-table">
        <thead><tr><th>杞︾墝</th><th>鍙告満</th><th>杞﹀瀷</th><th>capacity</th><th>speed</th><th>鍐烽摼</th><th>鍘嗗彶閲岀▼</th><th>鍘嗗彶娉㈡</th><th>鏈€鏃╁嚭杞?/th><th>routes闀垮害</th></tr></thead>
        <tbody>${vehicleRows}</tbody>
      </table>
    </div>
    <div class="table-wrap" style="margin-top:12px;color:#111;">
      <div class="note" style="margin-bottom:8px;">娉㈡瀛楁璇婃柇锛氱湅鐪嬪綋鍓嶆尝娆℃槸鍚﹀畬鏁淬€佹槸鍚︽槸澶滄尝/鍗曟尝娆°€佷互鍙婂簵閾烘暟閲忔槸鍚﹀寰椾笂銆?/div>
      <table class="load-convert-table">
        <thead><tr><th>娉㈡</th><th>寮€濮?/th><th>缁撴潫</th><th>缁撴潫鍙ｅ緞</th><th>鏀惧缁撴潫</th><th>鍗曟尝娆?/th><th>澶滄尝</th><th>鏈€鏃╁嚭杞?/th><th>闂ㄥ簵鏁?/th></tr></thead>
        <tbody>${waveRows}</tbody>
      </table>
    </div>
  `;
}

function openSolveDiagnoseModal(report) {
  const modal = document.getElementById("solveDiagnoseModal");
  if (!modal) return;
  renderSolveDiagnoseModal(report);
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

function closeSolveDiagnoseModal() {
  const modal = document.getElementById("solveDiagnoseModal");
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function createEmptyMatrix(nodeIds) {
  const matrix = {};
  nodeIds.forEach((fromId) => {
    matrix[fromId] = {};
    nodeIds.forEach((toId) => {
      matrix[fromId][toId] = fromId === toId ? 0 : null;
    });
  });
  return matrix;
}

function estimateStraightDurationMinutes(distanceKm, speedKmh = 38) {
  return (Number(distanceKm || 0) / Math.max(1, Number(speedKmh || 38))) * 60;
}

function buildStraightDistanceData(stores) {
  const points = new Map([[DC.id, DC], ...stores.map((s) => [s.id, s])]);
  const nodes = [DC.id, ...stores.map((s) => s.id)];
  const dist = createEmptyMatrix(nodes);
  const duration = createEmptyMatrix(nodes);
  for (const a of nodes) {
    for (const b of nodes) {
      if (a === b) continue;
      const p1 = points.get(a);
      const p2 = points.get(b);
      const km = Math.sqrt(((p1.lng - p2.lng) * 92) ** 2 + ((p1.lat - p2.lat) * 111) ** 2);
      const distanceKm = Number(Math.max(1, km).toFixed(1));
      dist[a][b] = distanceKm;
      duration[a][b] = estimateStraightDurationMinutes(distanceKm);
    }
  }
  return { dist, duration, source: "straight" };
}

function buildDistanceTable(stores) {
  return buildStraightDistanceData(stores).dist;
}

function loadAmapDistanceCache() {
  try {
    return JSON.parse(localStorage.getItem(AMAP_DISTANCE_CACHE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveAmapDistanceCache(cache) {
  try {
    localStorage.setItem(AMAP_DISTANCE_CACHE_KEY, JSON.stringify(cache));
    scheduleAmapCacheSync();
  } catch {
    // Ignore storage failures and continue with in-memory data.
  }
}

function loadAmapRouteCache() {
  try {
    return JSON.parse(localStorage.getItem(AMAP_ROUTE_CACHE_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveAmapRouteCache(cache) {
  try {
    localStorage.setItem(AMAP_ROUTE_CACHE_KEY, JSON.stringify(cache));
    scheduleAmapCacheSync();
  } catch {
    // Ignore storage failures and continue with runtime data.
  }
}

function scheduleAmapCacheSync(delayMs = 1800) {
  amapCacheSyncPending = true;
  if (amapCacheSyncTimer) return;
  amapCacheSyncTimer = setTimeout(() => {
    amapCacheSyncTimer = null;
    void flushAmapCacheToBackend();
  }, delayMs);
}

function getAmapCacheKey(fromNode, toNode) {
  return `${fromNode.lng},${fromNode.lat}->${toNode.lng},${toNode.lat}`;
}

function parsePolylinePoints(polyline) {
  return String(polyline || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.split(",").map(Number))
    .filter((pair) => pair.length === 2 && Number.isFinite(pair[0]) && Number.isFinite(pair[1]));
}

function serializePolylinePoints(points) {
  return points.map((pair) => `${Number(pair[0]).toFixed(6)},${Number(pair[1]).toFixed(6)}`).join(";");
}

function simplifyPolylinePoints(points, maxPoints = 90) {
  if (points.length <= maxPoints) return points;
  const step = Math.ceil(points.length / maxPoints);
  const simplified = points.filter((_, index) => index % step === 0);
  const last = points[points.length - 1];
  if (!simplified.length || simplified[simplified.length - 1][0] !== last[0] || simplified[simplified.length - 1][1] !== last[1]) simplified.push(last);
  return simplified;
}

async function fetchAmapDrivingPolyline(originNode, destinationNode) {
  const origin = `${originNode.lng},${originNode.lat}`;
  const destination = `${destinationNode.lng},${destinationNode.lat}`;
  const url = `https://restapi.amap.com/v3/direction/driving?key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}&origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(destination)}&extensions=all&strategy=0`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`AMap route HTTP ${response.status}`);
  const payload = await response.json();
  if (String(payload.status) !== "1" || !Array.isArray(payload.route?.paths) || !payload.route.paths.length) {
    throw new Error(payload.info || "AMap driving route failed");
  }
  const steps = payload.route.paths[0]?.steps || [];
  const points = steps.flatMap((step) => parsePolylinePoints(step.polyline));
  if (!points.length) return [[originNode.lng, originNode.lat], [destinationNode.lng, destinationNode.lat]];
  return points;
}

async function fetchAmapDistanceBatch(originNodes, destinationNode) {
  if (!originNodes.length || !AMAP_WEB_SERVICE_KEY) return [];
  const origins = originNodes.map((node) => `${node.lng},${node.lat}`).join("|");
  const destination = `${destinationNode.lng},${destinationNode.lat}`;
  const url = `https://restapi.amap.com/v3/distance?key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}&origins=${encodeURIComponent(origins)}&destination=${encodeURIComponent(destination)}&type=1`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`AMap distance HTTP ${response.status}`);
  const payload = await response.json();
  if (String(payload.status) !== "1" || !Array.isArray(payload.results)) {
    throw new Error(payload.info || "AMap distance failed");
  }
  return payload.results.map((item) => ({
    distanceKm: Number(item.distance || 0) / 1000,
    durationMinutes: Number(item.duration || 0) / 60,
  }));
}

async function buildDistanceData(stores) {
  const DISTANCE_PROGRESS_VERBOSE = false;
  const emit = (text) => {
    if (!DISTANCE_PROGRESS_VERBOSE) return;
    if (!text) return;
    reportRelayStageProgress(String(text));
  };
  const emitChunkedPairs = (title, pairs, chunkSize = 8) => {
    if (!DISTANCE_PROGRESS_VERBOSE) return;
    const list = Array.isArray(pairs) ? pairs : [];
    if (!list.length) {
      emit(`${title}锛? 鏉°€俙);
      return;
    }
    emit(`${title}锛?{list.length} 鏉°€俙);
    for (let i = 0; i < list.length; i += chunkSize) {
      const chunk = list.slice(i, i + chunkSize);
      emit(`${title}鏄庣粏 ${i + 1}-${Math.min(i + chunkSize, list.length)}锛?{chunk.join(" | ")}`);
    }
  };
  if (USE_FULL_DISTANCE_MATRIX_FROM_BACKEND) {
    try {
      const storeIds = (stores || []).map((item) => String(item?.id || "").trim()).filter(Boolean);
      if (storeIds.length > 0) {
        const params = new URLSearchParams({
          storeIds: storeIds.join(","),
          includeDuration: "true",
          strict: "true",
        });
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 30000);
        try {
          const response = await fetch(`${GA_BACKEND_URL}/distance-matrix/full?${params.toString()}`, {
            method: "GET",
            signal: controller.signal,
          });
          if (response.ok) {
            const data = await response.json();
            const missingCount = Number(data?.missingCount || 0);
            if (data?.ok && missingCount === 0 && data?.dist && data?.duration) {
              const dominantSource = String(data?.dbDominantSource || "");
              const sourceCounts = data?.dbSourceCounts || {};
              console.log(`[buildDistanceData] matrix source=database-full nodeCount=${Number(data?.nodeCount || 0)} dbReadMs=${Number(data?.dbReadMs || 0)} dbDominantSource=${dominantSource}`);
              reportRelayStageProgress(`璺濈鐭╅樀鏉ユ簮纭锛歞atabase-full锛屼富鏁版嵁婧?${dominantSource || "unknown"}锛宮issing=0锛宯ode=${Number(data?.nodeCount || 0)}锛宒bReadMs=${Number(data?.dbReadMs || 0)}銆俙);
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
  const straight = buildStraightDistanceData(stores);
  const nodes = [DC, ...stores];
  const nodeIds = nodes.map((node) => node.id);
  const totalPairCount = Math.max(0, nodes.length * (nodes.length - 1));
  emit(`璺濈鐭╅樀鏋勫缓锛氳妭鐐?${nodes.length}锛堝簱鎴?闂ㄥ簵锛夛紝鐩爣鏈夊悜杈?${totalPairCount} 鏉°€俙);
  if (!AMAP_WEB_SERVICE_KEY || typeof fetch !== "function") {
    emit("璺濈鐭╅樀鏋勫缓锛氶珮寰疯兘鍔涗笉鍙敤锛岀洿鎺ュ洖閫€鐩寸嚎璺濈銆?);
    return { ...straight, source: "straight-fallback", cacheHitPairs: 0, fetchedPairs: 0, distDbStats: { fullMatrix: false, missingCount: -1 } };
  }

  const dist = createEmptyMatrix(nodeIds);
  const duration = createEmptyMatrix(nodeIds);
  const cache = loadAmapDistanceCache();
  let cacheHitPairs = 0;
  let fetchedPairs = 0;
  const cacheDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;

  nodes.forEach((fromNode) => {
    nodes.forEach((toNode) => {
      if (fromNode.id === toNode.id) return;
      const cached = cache[getAmapCacheKey(fromNode, toNode)];
      if (!cached) return;
      dist[fromNode.id][toNode.id] = Number(cached.distanceKm || 0);
      duration[fromNode.id][toNode.id] = Number(cached.durationMinutes || 0);
      cacheHitPairs += 1;
      if (DISTANCE_PROGRESS_VERBOSE) {
        cacheDetails.push(`${fromNode.id}->${toNode.id} ${Number(cached.distanceKm || 0).toFixed(2)}km/${Number(cached.durationMinutes || 0).toFixed(1)}m`);
      }
    });
  });
  emitChunkedPairs("璺濈缂撳瓨鍛戒腑", cacheDetails || [], 6);

  try {
    for (const destinationNode of nodes) {
      const pendingOrigins = nodes.filter((originNode) => originNode.id !== destinationNode.id && !(dist[originNode.id]?.[destinationNode.id] > 0));
      emit(`璺濈鎷夊彇锛氱洰鏍囩偣 ${destinationNode.id}锛屽緟鎷夊彇璧风偣 ${pendingOrigins.length}銆俙);
      for (let start = 0; start < pendingOrigins.length; start += AMAP_ORIGIN_BATCH_SIZE) {
        const batch = pendingOrigins.slice(start, start + AMAP_ORIGIN_BATCH_SIZE);
        emit(`璺濈鎷夊彇锛氱洰鏍囩偣 ${destinationNode.id}锛屾壒娆?${Math.floor(start / AMAP_ORIGIN_BATCH_SIZE) + 1}/${Math.max(1, Math.ceil(pendingOrigins.length / AMAP_ORIGIN_BATCH_SIZE))}锛岃捣鐐?${batch.map((x) => x.id).join("銆?)}銆俙);
        const results = await fetchAmapDistanceBatch(batch, destinationNode);
        const fetchedDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;
        batch.forEach((originNode, index) => {
          const result = results[index];
          if (!result) return;
          const distanceKm = Number(result.distanceKm || 0);
          const durationMinutes = Number(result.durationMinutes || 0);
          if (!(distanceKm > 0) || !(durationMinutes > 0)) return;
          dist[originNode.id][destinationNode.id] = distanceKm;
          duration[originNode.id][destinationNode.id] = durationMinutes;
          cache[getAmapCacheKey(originNode, destinationNode)] = { distanceKm, durationMinutes };
          fetchedPairs += 1;
          if (DISTANCE_PROGRESS_VERBOSE) {
            fetchedDetails.push(`${originNode.id}->${destinationNode.id} ${distanceKm.toFixed(2)}km/${durationMinutes.toFixed(1)}m`);
          }
        });
        emitChunkedPairs(`璺濈鎷夊彇缁撴灉锛堢洰鏍?{destinationNode.id}锛塦, fetchedDetails || [], 6);
      }
    }
    saveAmapDistanceCache(cache);
  } catch (error) {
    console.warn("AMap distance fetch failed, fallback to straight-line data.", error);
    emit(`璺濈鎷夊彇寮傚父锛?{error?.message || error}锛屽皢鑷姩琛ラ綈鐩寸嚎璺濈銆俙);
  }

  const fallbackDetails = DISTANCE_PROGRESS_VERBOSE ? [] : null;
  nodes.forEach((fromNode) => {
    nodes.forEach((toNode) => {
      if (fromNode.id === toNode.id) return;
      if (!(dist[fromNode.id][toNode.id] > 0)) {
        dist[fromNode.id][toNode.id] = straight.dist[fromNode.id][toNode.id];
        duration[fromNode.id][toNode.id] = straight.duration[fromNode.id][toNode.id];
        if (DISTANCE_PROGRESS_VERBOSE) {
          fallbackDetails.push(`${fromNode.id}->${toNode.id} ${Number(straight.dist[fromNode.id][toNode.id] || 0).toFixed(2)}km/${Number(straight.duration[fromNode.id][toNode.id] || 0).toFixed(1)}m`);
        }
        return;
      }
      if (!(duration[fromNode.id][toNode.id] > 0)) duration[fromNode.id][toNode.id] = straight.duration[fromNode.id][toNode.id];
    });
  });
  emitChunkedPairs("璺濈鐩寸嚎琛ラ綈", fallbackDetails || [], 6);
  const fallbackCount = DISTANCE_PROGRESS_VERBOSE ? fallbackDetails.length : Math.max(0, totalPairCount - cacheHitPairs - fetchedPairs);
  emit(`璺濈鐭╅樀瀹屾垚锛氱紦瀛樺懡涓?${cacheHitPairs}锛屾柊鎷夊彇 ${fetchedPairs}锛岀洿绾胯ˉ榻?${fallbackCount}锛屾渶缁堟€昏竟 ${totalPairCount}銆俙);

  return {
    dist,
    duration,
    source: fetchedPairs > 0 || cacheHitPairs > 0 ? "amap" : "straight-fallback",
    cacheHitPairs,
    fetchedPairs,
    distDbStats: { fullMatrix: false, missingCount: -1 },
  };
}

function getTravelMinutes(scenario, fromId, toId, speedKmh = 38) {
  const durationMinutes = Number(scenario?.duration?.[fromId]?.[toId] || 0);
  if (durationMinutes > 0) return durationMinutes;
  const distanceKm = Number(scenario?.dist?.[fromId]?.[toId] || 0);
  return estimateStraightDurationMinutes(distanceKm, speedKmh);
}

function getTravelMinutesSolverConsistent(scenario, fromId, toId, speedKmh = 38) {
  const distanceKm = Number(scenario?.dist?.[fromId]?.[toId] || 0);
  return distanceKm / Math.max(Number(speedKmh || 0), 1) * 60;
}

function routeNodeDisplay(id, scenario) {
  if (id === DC.id) return L("depot");
  const store = scenario?.storeMap?.get(id);
  return store ? `${store.id} ${store.name}` : String(id || "");
}

function buildRouteDisplay(routeIds = [], scenario) {
  return [DC.id, ...routeIds, DC.id].map((id) => routeNodeDisplay(id, scenario)).join(" 鈫?");
}

function shortenMapName(name, limit = 14) {
  const text = String(name || "");
  return text.length > limit ? `${text.slice(0, limit)}鈥 : text;
}

function computeMapOverlayMarkers(markers = [], polyline = []) {
  const allPoints = [...polyline, ...markers.map((item) => [Number(item.lng), Number(item.lat)])]
    .filter((point) => Array.isArray(point) && point.length === 2 && Number.isFinite(point[0]) && Number.isFinite(point[1]));
  if (!allPoints.length) return markers.map((item) => ({ ...item, xPercent: 50, yPercent: 50 }));
  const lngs = allPoints.map((point) => point[0]);
  const lats = allPoints.map((point) => point[1]);
  const minLng = Math.min(...lngs);
  const maxLng = Math.max(...lngs);
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const lngSpan = Math.max(0.00001, maxLng - minLng);
  const latSpan = Math.max(0.00001, maxLat - minLat);
  const padX = 9;
  const padY = 10;
  return markers.map((item) => {
    const ratioX = (Number(item.lng) - minLng) / lngSpan;
    const ratioY = (Number(item.lat) - minLat) / latSpan;
    return {
      ...item,
      xPercent: padX + ratioX * (100 - padX * 2),
      yPercent: 100 - (padY + ratioY * (100 - padY * 2)),
    };
  });
}

async function getTripRouteMapData(result, wave, plan, trip) {
  const routeCache = loadAmapRouteCache();
  const nodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId) || { id: stop.storeId, name: stop.storeName, lng: 0, lat: 0 }), DC]
    .filter((node) => Number.isFinite(Number(node.lng)) && Number.isFinite(Number(node.lat)));
  const combinedPoints = [];
  const legs = [];

  for (let index = 0; index < nodes.length - 1; index += 1) {
    const fromNode = nodes[index];
    const toNode = nodes[index + 1];
    const cacheKey = getAmapCacheKey(fromNode, toNode);
    let points = parsePolylinePoints(routeCache[cacheKey]?.polyline || "");
    let source = routeCache[cacheKey]?.source || "cache";
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
    const normalizedPoints = points.filter((pair) => pair.length === 2);
    if (combinedPoints.length && normalizedPoints.length) {
      const [firstLng, firstLat] = normalizedPoints[0];
      const [lastLng, lastLat] = combinedPoints[combinedPoints.length - 1];
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
  const markerNodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId)).filter(Boolean)];
  const markers = computeMapOverlayMarkers(markerNodes.map((node, index) => {
    const label = index === 0 ? "D" : String(index);
    const fullName = node.id === DC.id ? L("depot") : `${node.id} ${node.name}`;
    return {
      label,
      lng: Number(node.lng),
      lat: Number(node.lat),
      name: fullName,
      shortName: node.id === DC.id ? L("depot") : shortenMapName(node.name || node.id, 12),
    };
  }), simplified);

  const depotParam = `${DC.lng},${DC.lat}`;
  const stopParam = markers.slice(1).map((item) => `${item.lng},${item.lat}`).join(";");
  const pathParam = serializePolylinePoints(simplified);
  const markerSegments = [`mid,0x3366FF,D:${depotParam}`];
  if (stopParam) markerSegments.push(`small,0xD97706,:${stopParam}`);
  const staticMapUrl = `https://restapi.amap.com/v3/staticmap?size=1100*640&scale=2&traffic=1&markers=${encodeURIComponent(markerSegments.join("|"))}&paths=8,0x2563EB,0.95,,0:${encodeURIComponent(pathParam)}&key=${encodeURIComponent(AMAP_WEB_SERVICE_KEY)}`;

  return {
    title: `${plan.vehicle.plateNo} 路 ${wave.waveId} 路 ${L("tripNo")}${trip.tripNo}${L("tripSuffix")}`,
    staticMapUrl,
    markers,
    legs,
    pointCount: simplified.length,
    polylinePoints: simplified,
  };
}

function getSingleWaveStoreIds(stores, dist, thresholdKm) {
  // 褰诲簳鍋滅敤鈥滃崟娉㈡鑷姩鍒嗘祦鈥濓紝鎵€鏈夐棬搴椾粎鎸変笟鍔℃尝娆★紙W1/W2/W3/W4锛夊弬涓庢眰瑙ｃ€?
  return [];
}
function validateInput() {
  if (!state.vehicles.length) return L("noVehicles");
  if (!state.stores.length) return L("noStores");
  if (state.settings.ignoreWaves) return "";
  if (!state.waves.length) return L("noWaves");
  const stores = enrichStores(state.stores);
  // 鍋滅敤鈥滃崟娉㈡鍒嗘祦鈥濓細鎵€鏈夐棬搴楅兘鎸変笟鍔℃尝娆℃牎楠岃鐩栧叧绯?
  const regularIds = new Set(stores.map((store) => store.id));
  const covered = new Set();
  for (const wave of state.waves) {
    parseStoreIds(wave.storeIds).forEach((id) => {
      if (regularIds.has(id)) covered.add(id);
    });
  }
  const missing = [...regularIds].filter((id) => !covered.has(id));
  if (missing.length) return LT("regularMissing", { count: missing.length, names: `${missing.slice(0, 8).join(",")}${missing.length > 8 ? "..." : ""}` });
  return "";
}

async function buildScenario() {
  const stores = enrichStores(state.stores);
  reportRelayStageProgress(`鍦烘櫙鏋勫缓锛氬紑濮嬫暣鐞嗛棬搴椾富鏁版嵁锛屽叡 ${stores.length} 瀹躲€俙);
  const dispatchStartMin = toMinutes(state.settings.dispatchStartTime || "19:10");
  const normalizedStores = stores.map((store) => ({
      ...store,
      desiredArrivalMin: alignMinuteToDispatch(toMinutes(store.waveArrivals?.w1 || store.desiredArrival), dispatchStartMin),
      allowedLateMinutes: Number(store.parking || 10),
      latestAllowedArrivalMin: alignMinuteToDispatch(toMinutes(store.waveArrivals?.w1 || store.desiredArrival), dispatchStartMin) + Number(store.parking || 10),
      actualServiceMinutes: Number(store.actualServiceMinutes || (Number(store.serviceMinutes || 15) * Math.max(0.1, Number(store.difficulty || 1)))),
    }));
  const storeRows = normalizedStores.map((store) => {
    const desired = String(store.waveArrivals?.w1 || store.desiredArrival || "");
    return `${store.id}[娉㈡:${store.waveBelongs || "-"} 璐ч噺:${Number(store.totalResolvedLoad || 0).toFixed(6)} W1:${Number(store.resolvedWave1Load || 0).toFixed(6)} W2:${Number(store.resolvedWave2Load || 0).toFixed(6)} W3:${Number(store.resolvedWave3Load || 0).toFixed(6)} W4:${Number(store.resolvedWave4Load || 0).toFixed(6)} 鏈熸湜:${desired || "--:--"} 鍏佽鍋忓樊:${Number(store.parking || 0)} 鍗歌揣:${Number(store.actualServiceMinutes || 0)}]`;
  });
  for (let i = 0; i < storeRows.length; i += 15) {
    const chunk = storeRows.slice(i, i + 15);
    reportRelayStageProgress(`鍦烘櫙鏋勫缓锛氶棬搴楁槑缁?${i + 1}-${Math.min(i + 15, storeRows.length)}锛?{chunk.join(" | ")}`);
  }
  const distanceData = await buildDistanceData(normalizedStores);
  const dist = distanceData.dist;
  // 鍋滅敤鈥滃崟娉㈡鍒嗘祦鈥濓細涓嶅啀鎸夎窛绂婚槇鍊煎己鍒跺垏鍒扳€滃崟娉㈡鈥?
  const singleWaveThreshold = Number(state.settings.singleWaveDistanceKm || 70);
  const singleWaveStoreIds = [];
  const singleWaveIdSet = new Set();
  const regularStoreIds = normalizedStores.map((store) => store.id);
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
      const normalWaves = state.waves.map((w) => {
        const span = buildWaveSpan(w.start, w.end);
        let { startMin, endMin } = span;
        while (endMin < dispatchStartMin) {
          startMin += 1440;
          endMin += 1440;
        }
        const waveId = String(w.waveId || "").trim().toUpperCase();
        const isNightWave = waveId === "W1" || waveId === "W2";
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
          // 涓ユ牸鎸変笟鍔℃尝娆″弬涓庢眰瑙ｏ紝涓嶅仛鍗曟尝娆℃敼鍒嗘祦
          storeList: parseStoreIds(w.storeIds),
        };
      }).filter((wave) => wave.storeList.length);
      return normalWaves;
    })();
  reportRelayStageProgress(`鍦烘櫙鏋勫缓锛氭尝娆″叡 ${waves.length} 涓€俙);
  waves.forEach((wave) => {
    reportRelayStageProgress(`鍦烘櫙鏋勫缓锛?{wave.waveId} ${wave.start || "--:--"}-${wave.end || "--:--"}锛岄棬搴?${Array.isArray(wave.storeList) ? wave.storeList.length : 0} 瀹讹紝闂ㄥ簵娓呭崟锛?{(wave.storeList || []).join("銆?) || "-"}`);
  });
  reportRelayStageProgress(`鍦烘櫙鏋勫缓锛氳溅杈嗗叡 ${state.vehicles.length} 鍙般€俙);
  state.vehicles.forEach((v) => {
    reportRelayStageProgress(`鍦烘櫙鏋勫缓锛氳溅杈?${v.plateNo || "-"} 绫诲瀷=${ENFORCED_VEHICLE_TYPE} 瀹归噺=${Number(v.capacity || 0)} 閫熷害=${Number(v.speed || 0)} 鍐烽摼=${Boolean(v.canCarryCold)}`);
  });
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

function createVehiclePlan(vehicle, waveId, startMin, scenario, priorStats = {}) {
  const earliestDepartureMin = Number(priorStats.earliestDepartureMin || startMin);
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
  return plan.trips.flatMap((trip) => trip.stops.map((stop) => stop.storeId));
}

function getUsageMap(metrics) {
  return new Map((metrics?.usageList || []).map((item) => [item.plateNo, item]));
}

function getUsedVehicleSet() {
  const used = new Set();
  state.lastResults.forEach((result) => {
    result.solution.flat().forEach((plan) => {
      if (plan.trips?.length) used.add(plan.vehicle.plateNo);
    });
  });
  return used;
}

function normalizeStoreKey(id) {
  const raw = String(id ?? "").trim();
  if (!raw) return "";
  if (/^\d+$/.test(raw)) {
    if (raw.length === 5) return `3${raw}`;
    if (raw.length === 6 && raw.startsWith("3")) return raw;
  }
  return raw;
}

function buildStoreKeyVariants(id) {
  const raw = String(id ?? "").trim();
  const normalized = normalizeStoreKey(raw);
  const variants = new Set([raw, normalized]);
  if (/^\d{6}$/.test(normalized) && normalized.startsWith("3")) {
    variants.add(normalized.slice(1));
  }
  return [...variants].filter(Boolean);
}

function buildStoreWaveAssignmentKey(storeId, waveId) {
  const sid = normalizeStoreKey(storeId);
  const wid = String(waveId || "").trim().toUpperCase();
  if (!sid || !wid) return "";
  return `${sid}|${wid}`;
}

function buildStoreAssignmentMapFromSolution(solution = []) {
  const map = new Map();
  (solution || []).forEach((wavePlans) => {
    (wavePlans || []).forEach((plan) => {
      const plateNo = plan?.vehicle?.plateNo || "";
      const waveId = plan?.waveId || "";
      (plan?.trips || []).forEach((trip) => {
        const stopIds = [];
        (trip?.stops || []).forEach((stop) => {
          if (stop?.storeId != null) stopIds.push(stop.storeId);
        });
        (trip?.route || []).forEach((storeId) => {
          if (storeId != null) stopIds.push(storeId);
        });
        stopIds.forEach((storeId) => {
          buildStoreKeyVariants(storeId).forEach((storeKey) => {
            const compositeKey = buildStoreWaveAssignmentKey(storeKey, waveId);
            if (compositeKey) {
              map.set(compositeKey, {
                plateNo,
                waveId,
                tripNo: trip?.tripNo || 1,
              });
            }
            // 淇濈暀鍗曢敭浠呬綔鍏煎鍏滃簳锛屽墠绔樉绀烘煡璇㈠凡鍒囧埌澶嶅悎閿?            map.set(storeKey, {
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

function getActiveStoreAssignmentMap() {
  const result = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  if (result?.storeAssignmentMap instanceof Map && result.storeAssignmentMap.size) return result.storeAssignmentMap;
  if (result?.solution?.length) {
    result.storeAssignmentMap = buildStoreAssignmentMapFromSolution(result.solution);
    return result.storeAssignmentMap;
  }
  const fallback = new Map();
  state.lastResults.forEach((oneResult) => {
    const oneMap = oneResult?.storeAssignmentMap instanceof Map && oneResult.storeAssignmentMap.size
      ? oneResult.storeAssignmentMap
      : buildStoreAssignmentMapFromSolution(oneResult?.solution || []);
    oneMap.forEach((value, key) => {
      if (!fallback.has(key)) fallback.set(key, value);
    });
  });
  return fallback;
}

function getStoreAssignmentByRule(store = {}, assignmentMap = new Map()) {
  const variants = buildStoreKeyVariants(store?.id);
  const waveOrder = ["W1", "W2", "W3", "W4"];
  for (const waveId of waveOrder) {
    if (!isStoreCandidateForWaveRule(store, waveId)) continue;
    for (const variant of variants) {
      const key = buildStoreWaveAssignmentKey(variant, waveId);
      if (!key) continue;
      const hit = assignmentMap.get(key);
      if (hit && String(hit.plateNo || "").trim()) return hit;
    }
  }
  // 鍏滃簳鑰侀敭锛堥伩鍏嶅巻鍙插綊妗ｄ笉鍙锛?  for (const variant of variants) {
    const hit = assignmentMap.get(variant);
    if (hit && String(hit.plateNo || "").trim()) return hit;
  }
  return null;
}

function getSortConfig(kind) {
  if (kind === "vehicle") return { fieldKey: "vehicleSortField", dirKey: "vehicleSortDir" };
  if (kind === "wave") return { fieldKey: "waveSortField", dirKey: "waveSortDir" };
  return { fieldKey: "storeSortField", dirKey: "storeSortDir" };
}

function toggleDataTableSort(kind, field) {
  const cfg = getSortConfig(kind);
  if (state.ui[cfg.fieldKey] === field) {
    state.ui[cfg.dirKey] = state.ui[cfg.dirKey] === "asc" ? "desc" : "asc";
  } else {
    state.ui[cfg.fieldKey] = field;
    state.ui[cfg.dirKey] = "asc";
  }
  if (kind === "vehicle") renderVehicles();
  else if (kind === "wave") renderWaves();
  else renderStoresTable();
}

function buildSortMark(kind, field) {
  const cfg = getSortConfig(kind);
  if (state.ui[cfg.fieldKey] !== field) return "";
  return state.ui[cfg.dirKey] === "asc" ? " 鈻? : " 鈻?;
}

function buildDataTableHtml({ tableKind = "store", columns = [], rows = [], tableClass = "" } = {}) {
  const colgroup = columns.map((column) => `<col style="width:${column.width || 120}px;">`).join("");
  const thead = columns.map((column) => {
    if (column.sortable && column.sortField) {
      if (column.headerHtml) {
        return `<th><button class="data-table-sort data-table-sort-rich" data-table-kind="${tableKind}" data-table-sort="${column.sortField}">${column.headerHtml}<span class="data-table-sort-mark">${buildSortMark(tableKind, column.sortField).trim()}</span></button></th>`;
      }
      return `<th><button class="data-table-sort" data-table-kind="${tableKind}" data-table-sort="${column.sortField}">${escapeHtml(String(column.label || ""))}${buildSortMark(tableKind, column.sortField)}</button></th>`;
    }
    if (column.headerHtml) {
      return `<th>${column.headerHtml}</th>`;
    }
    return `<th>${escapeHtml(String(column.label || ""))}</th>`;
  }).join("");
  const body = rows.length ? rows.join("") : `<tr><td colspan="${columns.length}" class="muted">${L("noChartData")}</td></tr>`;
  return `<table class="data-table ${tableClass}"><colgroup>${colgroup}</colgroup><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table>`;
}

function getWaveStoreNameList(storeIdsText) {
  const ids = parseStoreIds(storeIdsText);
  const storeById = new Map(state.stores.map((store) => [normalizeStoreCode(store.id), store]));
  return ids.map((id) => storeById.get(normalizeStoreCode(id))?.name || id);
}

function renderWaveStoreNameTags(storeIdsText, previewCount = 3) {
  const names = getWaveStoreNameList(storeIdsText);
  if (!names.length) return `<span class="muted">-</span>`;
  const preview = names.slice(0, previewCount);
  const remain = Math.max(0, names.length - preview.length);
  const title = escapeHtml(names.join(" / "));
  const chips = preview.map((name) => `<span class="wave-store-chip">${escapeHtml(name)}</span>`).join("");
  const suffix = remain > 0 ? `<span class="wave-store-more">+${remain}</span>` : "";
  return `<div class="wave-store-chip-wrap" title="${title}">${chips}${suffix}</div>`;
}

function createStop(store, timing, legDistance, arrival, start, leave) {
  const lateMinutes = Math.max(0, arrival - timing.desiredArrivalMin);
  const overToleranceMinutes = Math.max(0, arrival - timing.latestAllowedArrivalMin);
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

function summarizePlan(plan) {
  plan.totalDistance = plan.trips.reduce((sum, trip) => sum + trip.totalDistance, 0);
  plan.totalLoad = plan.trips.reduce((sum, trip) => sum + trip.loadBoxes, 0);
  plan.tripCount = plan.trips.length;
  plan.availableTime = plan.trips.length ? plan.trips[plan.trips.length - 1].finish : plan.availableTime;
  plan.feasible = true;
  return plan;
}

function createEmptyTrip(plan) {
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
  const v = Number(scenario?.strategyConfigResolved?.w1w2RelayMaxKm ?? scenario?.strategyConfig?.w1w2RelayMaxKm ?? state?.strategyConfig?.w1w2RelayMaxKm ?? 240);
  return Number.isFinite(v) && v > 0 ? v : 240;
}

function getSolveW3OneWayMaxKm(scenario) {
  const v = Number(scenario?.strategyConfigResolved?.w3OneWayMaxKm ?? scenario?.strategyConfig?.w3OneWayMaxKm ?? state?.strategyConfig?.w3OneWayMaxKm ?? 260);
  return Number.isFinite(v) && v > 0 ? v : 260;
}

function isW3WaveForSolve(wave) {
  return String(wave?.waveId || "").trim().toUpperCase() === "W3";
}

function buildTripFromRoute(route, vehicle, scenario, wave, startTime, tripNo, options = {}) {
  let currentNode = DC.id;
  let currentTime = startTime;
  let loadBoxes = 0;
  let totalDistance = 0;
  let outboundDistance = 0;
  let onTimeCount = 0;
  let lateStoreCount = 0;
  let lateMinutes = 0;
  let overToleranceMinutes = 0;
  const w3OneWayMaxKm = getSolveW3OneWayMaxKm(scenario);
  const isW3Wave = isW3WaveForSolve(wave);
  const stops = [];

  for (const storeId of route) {
    const store = scenario.storeMap.get(storeId);
    if (!store) return null;
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    // 鍐烽摼绾︽潫宸插仠鐢細涓嶅啀鎸?coldRatio/canCarryCold 鎷︽埅闂ㄥ簵鍒嗛厤
    loadBoxes += getStoreSolveLoadForWave(store, wave);
    if (!IGNORE_CAPACITY_CONSTRAINT && loadBoxes > getVehicleSolveCapacity(vehicle)) return null;

    const legDistance = scenario.dist[currentNode][store.id];
    const travelMinutes = legDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
    const arrival = currentTime + travelMinutes;
    const start = arrival;
    const leave = start + store.actualServiceMinutes;
    totalDistance += legDistance;
    if (isW3Wave) {
      outboundDistance += legDistance;
      if (outboundDistance > w3OneWayMaxKm) return null;
    }

    const stop = createStop(store, timing, legDistance, arrival, start, leave);
    if (stop.onTime) onTimeCount += 1;
    else lateStoreCount += 1;
    lateMinutes += stop.lateMinutes || 0;
    overToleranceMinutes += stop.overToleranceMinutes || 0;
    if (!options.allowToleranceBreak && stop.overToleranceMinutes > 0) return null;
    stops.push(stop);
    currentNode = store.id;
    currentTime = leave;
  }

  const backDistance = route.length ? scenario.dist[currentNode][DC.id] : 0;
  totalDistance += backDistance;

  const backTravelMinutes = backDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
  const finish = currentTime + backTravelMinutes;
  const waveLateType = wave.endMode || "return";
  const serviceEnd = stops.length ? stops[stops.length - 1].leave : startTime;
  const waveLateMinutes = waveLateType === "return"
    ? Math.max(0, finish - wave.endMin)
    : Math.max(0, serviceEnd - wave.endMin);
  if (!wave.relaxEnd && waveLateMinutes > 0) return null;

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

function mapTripFailureLabel(code) {
  const key = String(code || "").trim();
  if (key === "capacity") return "瀹归噺";
  if (key === "arrival_window") return "鏃堕棿绐?;
  if (key === "wave_end") return "娉㈡鎴";
  if (key === "max_route_km" || key === "max_route_km_single" || key === "max_route_km_return" || key === "night_regular_distance") return "閲岀▼";
  if (key === "store_missing") return "闂ㄥ簵缂哄け";
  return "缁煎悎";
}

function diagnoseTripBuildFailure(route, vehicle, scenario, wave, startTime, options = {}) {
  let currentNode = DC.id;
  let currentTime = startTime;
  let loadBoxes = 0;
  let totalDistance = 0;
  let outboundDistance = 0;
  const w3OneWayMaxKm = getSolveW3OneWayMaxKm(scenario);
  const isW3Wave = isW3WaveForSolve(wave);
  for (const storeId of route) {
    const store = scenario.storeMap.get(storeId);
    if (!store) {
      if (isW3Wave) console.log(`[W3] 璺嚎鎷掔粷: store_missing, storeId=${storeId}`);
      return { code: "store_missing", label: mapTripFailureLabel("store_missing"), storeId };
    }
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    loadBoxes += getStoreSolveLoadForWave(store, wave);
    if (!IGNORE_CAPACITY_CONSTRAINT && loadBoxes > getVehicleSolveCapacity(vehicle)) {
      if (isW3Wave) {
        console.log(`[W3] 璺嚎鎷掔粷: capacity, storeId=${storeId}, loadBoxes=${loadBoxes}, capacity=${getVehicleSolveCapacity(vehicle)}`);
      }
      return { code: "capacity", label: mapTripFailureLabel("capacity"), storeId };
    }
    const legDistance = scenario.dist[currentNode][store.id];
    const travelMinutes = legDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
    const arrival = currentTime + travelMinutes;
    const leave = arrival + store.actualServiceMinutes;
    totalDistance += legDistance;
    if (isW3Wave) {
      outboundDistance += legDistance;
      console.log(`[W3] 妫€鏌ヨ矾绾? storeId=${storeId}, legDistance=${legDistance}, outboundDistance绱=${outboundDistance}, limit=${w3OneWayMaxKm}`);
      if (outboundDistance > w3OneWayMaxKm) {
        console.log(`[W3] 璺嚎鎷掔粷: max_route_km_single, storeId=${storeId}, outboundDistance=${outboundDistance}, limit=${w3OneWayMaxKm}`);
        return { code: "max_route_km_single", label: mapTripFailureLabel("max_route_km_single"), storeId };
      }
    }
    const overTolerance = Math.max(0, arrival - timing.latestAllowedArrivalMin);
    if (!options.allowToleranceBreak && overTolerance > 0) {
      if (isW3Wave) {
        console.log(`[W3] 璺嚎鎷掔粷: arrival_window, storeId=${storeId}, arrival=${arrival}, latestAllowed=${timing.latestAllowedArrivalMin}, overTolerance=${overTolerance}`);
      }
      return { code: "arrival_window", label: mapTripFailureLabel("arrival_window"), storeId };
    }
    currentNode = store.id;
    currentTime = leave;
  }
  const backDistance = route.length ? scenario.dist[currentNode][DC.id] : 0;
  totalDistance += backDistance;
  const backTravelMinutes = backDistance / Math.max(Number(vehicle.speed || 0), 1) * 60;
  const finish = currentTime + backTravelMinutes;
  const serviceEnd = currentTime;
  const waveLateMinutes = (wave.endMode || "return") === "return"
    ? Math.max(0, finish - wave.endMin)
    : Math.max(0, serviceEnd - wave.endMin);
  if (!wave.relaxEnd && waveLateMinutes > 0) {
    if (isW3Wave) {
      console.log(`[W3] 璺嚎鎷掔粷: wave_end, storeId=${route[route.length - 1]}, finish=${finish}, serviceEnd=${serviceEnd}, endMin=${wave.endMin}, endMode=${wave.endMode || "return"}`);
    }
    return { code: "wave_end", label: mapTripFailureLabel("wave_end"), storeId: route[route.length - 1] };
  }
  if (isW3Wave) console.log(`[W3] 璺嚎鎷掔粷: unknown, storeId=${route[0]}`);
  return { code: "unknown", label: mapTripFailureLabel("unknown"), storeId: route[0] };
}

function rebuildPlanFromRoutesWithReason(vehicle, routes, scenario, wave, priorStats = {}, options = {}) {
  const plan = createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, priorStats);
  const regularMileageCap = getSolveRelayMaxKm(scenario);
  let availableTime = plan.availableTime;
  for (const route of routes) {
    if (!route.length) continue;
    const trip = buildTripFromRoute(route, vehicle, scenario, wave, availableTime, plan.trips.length + 1, {
      ...options,
      solverConsistentTravel: true,
    });
    if (!trip) {
      const failure = diagnoseTripBuildFailure(route, vehicle, scenario, wave, availableTime, options);
      return { plan: null, failure };
    }
    plan.trips.push(trip);
    availableTime = trip.finish;
  }
  plan.availableTime = availableTime;
  summarizePlan(plan);
  if (wave.isNightWave && plan.priorRegularDistance + plan.totalDistance > regularMileageCap) {
    return {
      plan: null,
      failure: { code: "night_regular_distance", label: mapTripFailureLabel("night_regular_distance"), storeId: "" },
    };
  }
  return { plan, failure: null };
}

function rebuildPlanFromRoutes(vehicle, routes, scenario, wave, priorStats = {}, options = {}) {
  return rebuildPlanFromRoutesWithReason(vehicle, routes, scenario, wave, priorStats, options).plan;
}

function computePlanLateness(plan, scenario) {
  return plan.trips.reduce((sum, trip) => sum + trip.stops.reduce((tripSum, stop) => {
      const store = scenario.storeMap.get(stop.storeId);
      const timing = getStoreTimingForWave(store, { waveId: plan.waveId }, scenario.dispatchStartMin);
      return tripSum + Math.max(0, stop.arrival - (timing?.latestAllowedArrivalMin || 0));
    }, 0), 0);
}

function computePlanViolation(plan) {
  return plan.trips.reduce((sum, trip) => sum + (trip.overToleranceMinutes || 0), 0);
}

function computePlanCostBreakdown(plan, scenario, wave) {
  const vehicleBusyPenalty = wave.singleWave ? 0 : plan.priorRegularDistance * 1.2 + plan.priorWaveCount * 150;
  const latenessMinutes = computePlanLateness(plan, scenario);
  const arrivalViolationMinutes = computePlanViolation(plan);
  const latenessPenalty = latenessMinutes * 60;
  const arrivalViolationPenalty = arrivalViolationMinutes * 20000;
  const waveLateMinutes = plan.trips.reduce((sum, trip) => sum + (trip.waveLateMinutes || 0), 0);
  const waveLatePenalty = waveLateMinutes * 80;
  const extraTripCount = (!wave.singleWave && plan.tripCount > 1) ? (plan.tripCount - 1) : 0;
  const extraTripPenalty = extraTripCount * 180;
  const distanceCost = plan.totalDistance * 0.45;
  const loadBonus = plan.totalLoad * 0.08;
  const lateRouteCount = plan.trips.filter((trip) => (trip.overToleranceMinutes || 0) > 0).length;
  const lateRoutePenalty = scenario.concentrateLate ? lateRouteCount * 1600 : lateRouteCount * 240;
  const totalCost = arrivalViolationPenalty + latenessPenalty + waveLatePenalty + vehicleBusyPenalty + extraTripPenalty + lateRoutePenalty + distanceCost - loadBonus;
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

function computePlanCost(plan, scenario, wave) {
  return computePlanCostBreakdown(plan, scenario, wave).totalCost;
}

function computePlansCostBreakdown(plans, scenario, wave) {
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
  return total;
}

function formatWaveCostBreakdown(breakdown) {
  if (!breakdown) return "";
  const parts = [
    `閲岀▼ ${Number(breakdown.distanceCost || 0).toFixed(1)}`,
  ];
  if (Number(breakdown.latenessPenalty || 0) > 0) parts.push(`鏅氬埌 ${Number(breakdown.latenessPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.arrivalViolationPenalty || 0) > 0) parts.push(`瓒呭厑璁稿亸宸?${Number(breakdown.arrivalViolationPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.waveLatePenalty || 0) > 0) parts.push(`娉㈡瓒呮椂 ${Number(breakdown.waveLatePenalty || 0).toFixed(1)}`);
  if (Number(breakdown.vehicleBusyPenalty || 0) > 0) parts.push(`杞﹁締缁窇 ${Number(breakdown.vehicleBusyPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.extraTripPenalty || 0) > 0) parts.push(`澶氳稛娆?${Number(breakdown.extraTripPenalty || 0).toFixed(1)}`);
  if (Number(breakdown.lateRoutePenalty || 0) > 0) parts.push(`鏅氬埌绾胯矾 ${Number(breakdown.lateRoutePenalty || 0).toFixed(1)}`);
  if (Number(breakdown.loadBonus || 0) > 0) parts.push(`瑁呰浇鎶垫墸 -${Number(breakdown.loadBonus || 0).toFixed(1)}`);
  return parts.join("锛?);
}

function buildTripCandidate(plan, store, scenario, wave, debug = false, options = {}) {
  const vehicle = plan.vehicle;
  if (!IGNORE_CAPACITY_CONSTRAINT && getStoreSolveLoadForWave(store, wave) > getVehicleSolveCapacity(vehicle)) return null;

  const baseRoutes = plan.trips.map((trip) => [...trip.route]);
  const candidates = [];

  for (let tripIndex = 0; tripIndex < baseRoutes.length; tripIndex += 1) {
    const route = baseRoutes[tripIndex];
    for (let insertAt = 0; insertAt <= route.length; insertAt += 1) {
      const nextRoutes = baseRoutes.map((item) => [...item]);
      nextRoutes[tripIndex].splice(insertAt, 0, store.id);
      const nextPlan = rebuildPlanFromRoutes(vehicle, nextRoutes, scenario, wave, {
        priorRegularDistance: plan.priorRegularDistance,
        priorWaveCount: plan.priorWaveCount,
      }, options);
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
    const nextRoutes = baseRoutes.map((item) => [...item]);
    nextRoutes.push([store.id]);
    const nextPlan = rebuildPlanFromRoutes(vehicle, nextRoutes, scenario, wave, {
      priorRegularDistance: plan.priorRegularDistance,
      priorWaveCount: plan.priorWaveCount,
    }, options);
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

function buildPlanForStoreOrder(vehicle, orderedStores, scenario, wave, priorStats = {}, options = {}) {
  let plan = createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, priorStats);
  for (const store of orderedStores) {
    const candidate = buildTripCandidate(plan, store, scenario, wave, false, options);
    if (!candidate) return null;
    plan = candidate.nextPlan;
  }
  return plan;
}

function localizeUnscheduledReason(reason) {
  const map = {
    arrival: "unscheduledReasonArrival",
    wave: "unscheduledReasonWave",
    mileage: "unscheduledReasonMileage",
    capacity: "unscheduledReasonCapacity",
    slot: "unscheduledReasonSlot",
    mixed: "unscheduledReasonMixed",
  };
  return L(map[reason] || "unscheduledReasonMixed");
}

function getDisplayUnscheduledReason(item) {
  const reason = String(item?.reason || "").trim().toLowerCase();
  const reasonText = String(item?.reasonText || "").trim();
  const isCapacity = reason === "capacity" || /capacity|瀹归噺/.test(reasonText.toLowerCase());
  if (IGNORE_CAPACITY_CONSTRAINT && isCapacity) {
    return localizeUnscheduledReason("slot");
  }
  return reasonText || localizeUnscheduledReason(reason);
}

function diagnoseStoreVehicleConstraint(store, plan, scenario, wave) {
  const vehicle = plan.vehicle;
  const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
  if (!IGNORE_CAPACITY_CONSTRAINT && getStoreSolveLoadForWave(store, wave) > getVehicleSolveCapacity(vehicle)) return "capacity";
  const directDistance = scenario.dist[DC.id][store.id];
  const roundDistance = directDistance + scenario.dist[store.id][DC.id];
  if (isW3WaveForSolve(wave) && directDistance > getSolveW3OneWayMaxKm(scenario)) return "mileage";
  const regularMileageCap = getSolveRelayMaxKm(scenario);
  if (wave.isNightWave && Number(plan.priorRegularDistance || 0) + roundDistance > regularMileageCap) return "mileage";
  const depart = Math.max(plan.availableTime || wave.startMin, wave.startMin, scenario.dispatchStartMin);
  const arrival = depart + getTravelMinutes(scenario, DC.id, store.id, vehicle.speed);
  const leave = arrival + (store.actualServiceMinutes || store.serviceMinutes || 0);
  const finish = leave + getTravelMinutes(scenario, store.id, DC.id, vehicle.speed);
  if (arrival > timing.latestAllowedArrivalMin) return "arrival";
  if (!wave.relaxEnd && (((wave.endMode || "return") === "return" && finish > wave.endMin) || ((wave.endMode || "return") !== "return" && leave > wave.endMin))) {
      return "wave";
  }
  return "slot";
}

function diagnoseUnscheduledStore(store, plans, scenario, wave) {
  const stats = { arrival: 0, wave: 0, mileage: 0, capacity: 0, slot: 0 };
  for (const plan of plans) {
    const strictCandidate = buildTripCandidate(plan, store, scenario, wave, false, { allowToleranceBreak: false });
    if (strictCandidate) {
      return { reason: "slot", detail: localizeUnscheduledReason("slot"), stats };
    }
    const relaxedCandidate = buildTripCandidate(plan, store, scenario, wave, false, { allowToleranceBreak: true });
    if (relaxedCandidate) {
      stats.arrival += 1;
      continue;
    }
    const reason = diagnoseStoreVehicleConstraint(store, plan, scenario, wave);
    stats[reason] += 1;
  }
  const ranked = Object.entries(stats).sort((a, b) => b[1] - a[1]);
  const bestReason = (stats.arrival > 0 ? "arrival" : ranked[0]?.[0]) || "mixed";
  return { reason: bestReason, detail: localizeUnscheduledReason(bestReason), stats };
}

function formatUnscheduledDetails(unscheduledStores, limit = 8) {
  return (unscheduledStores || []).slice(0, limit).map((item) => `${item.storeName}锛?{getDisplayUnscheduledReason(item)}锛塦).join("銆?);
}

function summarizeUnscheduledReasons(unscheduledStores) {
  const buckets = new Map();
  for (const item of unscheduledStores || []) {
    const key = getDisplayUnscheduledReason(item);
    buckets.set(key, (buckets.get(key) || 0) + 1);
  }
  return [...buckets.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([label, count]) => `${label} 脳 ${count}`)
    .join("锛?);
}

function evaluateStoreInsertionChoices(plans, store, scenario, wave, traceMode = false) {
  const attemptModes = scenario.concentrateLate
    ? [{ allowToleranceBreak: false }, { allowToleranceBreak: true }]
    : [{ allowToleranceBreak: false }];
  const vehicleEvaluations = [];
  for (const mode of attemptModes) {
    const ranked = [];
    for (let i = 0; i < plans.length; i += 1) {
      const candidate = buildTripCandidate(plans[i], store, scenario, wave, traceMode, mode);
      if (!candidate) continue;
      const chosenTripNo = candidate.candidates?.[0]?.chosenTripNo || candidate.nextPlan.trips.find((trip) => trip.route.includes(store.id))?.tripNo || 1;
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
      return {
        best: ranked[0],
        second: ranked[1] || null,
        vehicleEvaluations,
      };
    }
  }
  return { best: null, second: null, vehicleEvaluations };
}

function computeRegretPriority(bestChoice, secondChoice, store, wave, scenario) {
  const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
  const regretBase = (secondChoice?.cost ?? (bestChoice.cost + 35)) - bestChoice.cost;
  const urgency = Math.max(0, (scenario.dispatchStartMin + 1440) - timing.latestAllowedArrivalMin);
  const remoteBonus = scenario.dist?.[DC.id]?.[store.id] || 0;
  return regretBase * 1.6 + remoteBonus * 0.05 + urgency * 0.02;
}

function greedySolve(scenario, seed = 0, traceMode = false) {
  const solution = [];
  const regularVehicleStats = new Map();
  const traceLog = [];
  const unscheduledStores = [];
  for (const wave of scenario.waves) {
    const plans = scenario.vehicles.map((vehicle) => {
      const prior = regularVehicleStats.get(vehicle.plateNo) || {};
      const earliestDepartureMin = wave.isNightWave
        ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(prior.nightAvailableMin || wave.startMin))
        : wave.startMin;
      return createVehiclePlan(vehicle, wave.waveId, wave.startMin, scenario, {
        ...prior,
        earliestDepartureMin,
      });
    });
    const stores = wave.storeList
      .map((id) => scenario.storeMap.get(id))
      .filter(Boolean)
      .sort((a, b) => {
        const timingA = getStoreTimingForWave(a, wave, scenario.dispatchStartMin);
        const timingB = getStoreTimingForWave(b, wave, scenario.dispatchStartMin);
        return timingA.desiredArrivalMin - timingB.desiredArrivalMin || a.id.localeCompare(b.id);
      });
    if (seed && stores.length > 1) {
      for (let i = stores.length - 1; i > 0; i -= 1) {
        const j = Math.floor(((i + 1) * ((seed % 11) + 1)) % stores.length);
        [stores[i], stores[j]] = [stores[j], stores[i]];
      }
    }
    const unrouted = [...stores];
    while (unrouted.length) {
      let bestDecision = null;
      const infeasible = [];
      for (const store of unrouted) {
        const { best, second, vehicleEvaluations } = evaluateStoreInsertionChoices(plans, store, scenario, wave, traceMode);
        if (!best) {
          infeasible.push({ store, vehicleEvaluations });
          continue;
        }
        const priority = computeRegretPriority(best, second, store, wave, scenario);
        if (!bestDecision || priority > bestDecision.priority || (priority === bestDecision.priority && best.cost < bestDecision.best.cost)) {
          bestDecision = { store, best, priority, vehicleEvaluations };
        }
      }
      if (!bestDecision) {
        for (const item of infeasible) {
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
      const removeIndex = unrouted.findIndex((item) => item.id === bestDecision.store.id);
      if (removeIndex >= 0) unrouted.splice(removeIndex, 1);
    }
    if (wave.isNightWave) {
      plans.forEach((plan) => {
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
    solution.push(plans);
  }
  return traceMode ? { solution, traceLog, unscheduledStores } : { solution, unscheduledStores };
}

function cloneWaveRouteState(plans) {
  return plans.map((plan) => ({
    vehicle: plan.vehicle,
    priorRegularDistance: plan.priorRegularDistance,
    priorWaveCount: plan.priorWaveCount,
    earliestDepartureMin: plan.earliestDepartureMin,
    routes: plan.trips.map((trip) => [...trip.route]),
  }));
}

function rebuildWavePlansFromState(routeState, scenario, wave) {
  const plans = [];
  const degradedVehicles = [];
  for (const stateItem of routeState) {
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
      if (!emptyPlan.plan) return null;
      const plate = String(stateItem?.vehicle?.plateNo || "").trim() || "-";
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
    const examples = degradedVehicles.slice(0, 8).map((item) => (
      item.storeId
        ? `${item.plate}[${item.reasonLabel}/${item.storeId}]`
        : `${item.plate}[${item.reasonLabel}]`
    ));
    reportRelayStageProgress(`鍚庣閲嶅缓闄嶇骇锛?{degradedVehicles.length} 鍙拌溅杈嗚矾绾夸笉鍚堟硶锛屽凡缃┖锛?{examples.join("銆?)}锛塦);
  }
  return plans;
}

function wavePlansCost(plans, scenario, wave) {
  return plans.reduce((sum, plan) => sum + computePlanCost(plan, scenario, wave), 0);
}

function approximateRouteDistance(route = [], scenario) {
  if (!route.length) return 0;
  let total = Number(scenario?.dist?.[DC.id]?.[route[0]] || 0);
  for (let i = 0; i < route.length - 1; i += 1) {
    total += Number(scenario?.dist?.[route[i]]?.[route[i + 1]] || 0);
  }
  total += Number(scenario?.dist?.[route[route.length - 1]]?.[DC.id] || 0);
  return total;
}

function approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes = []) {
  const targets = touchedVehicleIndexes.length
    ? touchedVehicleIndexes.map((index) => routeState[index]).filter(Boolean)
    : routeState;
  return targets.reduce((sum, item) => sum + item.routes.reduce((routeSum, route) => routeSum + approximateRouteDistance(route, scenario), 0), 0);
}

function normalizeRouteBuckets(routeState) {
  routeState.forEach((item) => {
    item.routes = item.routes.filter((route) => route.length);
  });
}

function createSeededRandom(seed = 42) {
  let value = seed % 2147483647;
  if (value <= 0) value += 2147483646;
  return () => {
    value = value * 16807 % 2147483647;
    return (value - 1) / 2147483646;
  };
}

function hashRouteState(routeState) {
  return routeState
    .map((item) => `${item.vehicle.plateNo}:${item.routes.map((route) => route.join(">")).join("|")}`)
    .join("||");
}

function pushTraceEvent(traceLog, event, limit = 160) {
  traceLog.push(event);
  if (traceLog.length > limit) traceLog.splice(0, traceLog.length - limit);
}

function mergeTraceLogs(baseTraceLog, extraTraceLog, limit = 220) {
  (extraTraceLog || []).forEach((event) => pushTraceEvent(baseTraceLog, event, limit));
  return baseTraceLog;
}

function summarizeRouteState(routeState) {
  return routeState
    .map((item) => `${item.vehicle.plateNo}[${item.routes.map((route) => route.join("->")).join(" | ") || "-"}]`)
    .join(" / ");
}

function flattenStoresFromRouteState(routeState) {
  return routeState.flatMap((item) => item.routes.flatMap((route) => route));
}

function evaluateRouteState(routeState, scenario, wave) {
  const plans = rebuildWavePlansFromState(routeState, scenario, wave);
  if (!plans) return null;
  const planCosts = plans.map((plan) => computePlanCost(plan, scenario, wave));
  return { plans, cost: planCosts.reduce((sum, value) => sum + value, 0), planCosts };
}

function evaluateRouteStateIncremental(routeState, scenario, wave, baselineEval = null, touchedVehicleIndexes = []) {
  if (!baselineEval?.plans?.length || !touchedVehicleIndexes.length) {
    return evaluateRouteState(routeState, scenario, wave);
  }
  const plans = baselineEval.plans.map((plan) => clone(plan));
  const planCosts = [...(baselineEval.planCosts || baselineEval.plans.map((plan) => computePlanCost(plan, scenario, wave)))];
  for (const vehicleIndex of [...new Set(touchedVehicleIndexes)].sort((a, b) => a - b)) {
    const stateItem = routeState[vehicleIndex];
    if (!stateItem) continue;
    const rebuilt = rebuildSingleStatePlan(stateItem, scenario, wave);
    if (!rebuilt) return null;
    plans[vehicleIndex] = rebuilt;
    planCosts[vehicleIndex] = computePlanCost(rebuilt, scenario, wave);
  }
  return { plans, cost: planCosts.reduce((sum, value) => sum + value, 0), planCosts };
}

function rebuildSingleStatePlan(stateItem, scenario, wave) {
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

function estimateRouteInsertDelta(route, storeId, scenario) {
  const fromDepot = Number(scenario?.dist?.[DC.id]?.[storeId] || 0);
  const toDepot = Number(scenario?.dist?.[storeId]?.[DC.id] || 0);
  if (!route.length) return fromDepot + toDepot;
  let best = Number.POSITIVE_INFINITY;
  for (let position = 0; position <= route.length; position += 1) {
    const prevId = position === 0 ? DC.id : route[position - 1];
    const nextId = position === route.length ? DC.id : route[position];
    const delta = Number(scenario?.dist?.[prevId]?.[storeId] || 0)
      + Number(scenario?.dist?.[storeId]?.[nextId] || 0)
      - Number(scenario?.dist?.[prevId]?.[nextId] || 0);
    if (delta < best) best = delta;
  }
  return best;
}

function rankVehicleIndexesForStoreInsertion(routeState, storeId, scenario, limit = 0) {
  const ranked = routeState
    .map((stateItem, vehicleIndex) => {
      const routeDeltas = stateItem.routes.map((route) => estimateRouteInsertDelta(route, storeId, scenario));
      const bestExistingDelta = routeDeltas.length ? Math.min(...routeDeltas) : Number.POSITIVE_INFINITY;
      const newTripDelta = Number(scenario?.dist?.[DC.id]?.[storeId] || 0) + Number(scenario?.dist?.[storeId]?.[DC.id] || 0);
      const bestDelta = Math.min(bestExistingDelta, newTripDelta);
      return {
        vehicleIndex,
        score: bestDelta + stateItem.routes.length * 1.5,
      };
    })
    .sort((a, b) => a.score - b.score)
    .map((item) => item.vehicleIndex);
  return limit > 0 ? ranked.slice(0, Math.min(limit, ranked.length)) : ranked;
}

function insertStoresIntoRouteState(baseState, storeOrder, scenario, wave, options = {}) {
  const states = clone(baseState);
  const candidateVehicleLimit = Number(options.candidateVehicleLimit || 0);
  for (const storeId of storeOrder) {
    const store = scenario.storeMap.get(storeId);
    if (!store) continue;
    let bestIndex = -1;
    let bestCost = Number.POSITIVE_INFINITY;
    let bestRoutes = null;
    const primaryIndexes = rankVehicleIndexesForStoreInsertion(states, storeId, scenario, candidateVehicleLimit);
    const fallbackIndexes = candidateVehicleLimit > 0
      ? Array.from({ length: states.length }, (_, index) => index).filter((index) => !primaryIndexes.includes(index))
      : [];
    const candidateIndexes = [...primaryIndexes, ...fallbackIndexes];
    for (const vehicleIndex of candidateIndexes) {
      const currentPlan = rebuildSingleStatePlan(states[vehicleIndex], scenario, wave);
      if (!currentPlan) continue;
      const candidate = buildTripCandidate(currentPlan, store, scenario, wave, false);
      if (!candidate || candidate.cost >= bestCost) continue;
      bestIndex = vehicleIndex;
      bestCost = candidate.cost;
      bestRoutes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
    }
    if (bestIndex < 0 || !bestRoutes) return null;
    states[bestIndex].routes = bestRoutes;
    normalizeRouteBuckets(states);
  }
  return states;
}

function collectStorePositions(routeState) {
  const positions = [];
  routeState.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        positions.push({ vehicleIndex, tripIndex, stopIndex, storeId, routeLength: route.length });
      });
    });
  });
  return positions;
}

function removeStoresFromRouteState(routeState, removedStoreIds) {
  const removedSet = new Set(removedStoreIds);
  const next = clone(routeState);
  next.forEach((stateItem) => {
    stateItem.routes = stateItem.routes
      .map((route) => route.filter((storeId) => !removedSet.has(storeId)))
      .filter((route) => route.length);
  });
  return next;
}

function computeStoreRelatedness(storeAId, storeBId, scenario, wave) {
  if (storeAId === storeBId) return 0;
  const storeA = scenario.storeMap.get(storeAId);
  const storeB = scenario.storeMap.get(storeBId);
  if (!storeA || !storeB) return Number.POSITIVE_INFINITY;
  const distance = scenario.dist?.[storeAId]?.[storeBId] || scenario.dist?.[storeBId]?.[storeAId] || 0;
  const timingA = getStoreTimingForWave(storeA, wave, scenario.dispatchStartMin);
  const timingB = getStoreTimingForWave(storeB, wave, scenario.dispatchStartMin);
  const timeGap = Math.abs(timingA.desiredArrivalMin - timingB.desiredArrivalMin);
  const loadGap = Math.abs(getStoreSolveLoadForWave(storeA, wave) - getStoreSolveLoadForWave(storeB, wave));
  return distance + timeGap * 0.25 + loadGap * 0.6;
}

function estimateRemovalSavings(routeState, scenario) {
  const scores = [];
  routeState.forEach((stateItem) => {
    stateItem.routes.forEach((route) => {
      route.forEach((storeId, stopIndex) => {
        const prevId = stopIndex === 0 ? DC.id : route[stopIndex - 1];
        const nextId = stopIndex === route.length - 1 ? DC.id : route[stopIndex + 1];
        const saving = (scenario.dist?.[prevId]?.[storeId] || 0) + (scenario.dist?.[storeId]?.[nextId] || 0) - (scenario.dist?.[prevId]?.[nextId] || 0);
        scores.push({ storeId, saving });
      });
    });
  });
  return scores.sort((a, b) => b.saving - a.saving);
}

function crossoverRouteStates(parentA, parentB, scenario, wave, random) {
  const isCompareMode = (state.settings.solveStrategy || "manual") === "compare";
  const child = cloneWaveRouteState(parentA);
  const inheritedVehicleIndexes = new Set();
  const inheritedStores = new Set();
  child.forEach((item, vehicleIndex) => {
    const takeParentA = random() < 0.5;
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
    if (!inheritedVehicleIndexes.has(vehicleIndex)) item.routes = [];
  });
  return insertStoresIntoRouteState(child, remainingOrder, scenario, wave, {
    candidateVehicleLimit: isCompareMode ? 8 : 0,
  });
}

function tournamentSelect(population, random, size = 3) {
  let best = null;
  for (let i = 0; i < size; i += 1) {
    const candidate = population[Math.floor(random() * population.length)];
    if (!best || candidate.cost < best.cost) best = candidate;
  }
  return best;
}

function pickRemovalCount(totalStops, random) {
  if (totalStops <= 3) return 1;
  const ratio = 0.12 + random() * 0.12;
  return Math.max(1, Math.min(Math.max(3, Math.floor(totalStops * 0.25)), Math.floor(totalStops * ratio)));
}

function randomRelocateNeighbor(routeState, wave, random) {
  const states = clone(routeState);
  const nonEmptySources = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        nonEmptySources.push({ vehicleIndex, tripIndex, stopIndex, storeId });
      });
    });
  });
  if (!nonEmptySources.length) return null;
  const source = nonEmptySources[Math.floor(random() * nonEmptySources.length)];
  const sourceRoute = states[source.vehicleIndex].routes[source.tripIndex];
  const [storeId] = sourceRoute.splice(source.stopIndex, 1);
  if (!sourceRoute.length) states[source.vehicleIndex].routes.splice(source.tripIndex, 1);
  const possibleTargets = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      for (let insertAt = 0; insertAt <= route.length; insertAt += 1) {
        possibleTargets.push({ vehicleIndex, tripIndex, insertAt, mode: "insert" });
      }
    });
    if (!(wave.singleWave && stateItem.routes.length)) {
      possibleTargets.push({ vehicleIndex, tripIndex: stateItem.routes.length, insertAt: 0, mode: "new-trip" });
    }
  });
  if (!possibleTargets.length) return null;
  const target = possibleTargets[Math.floor(random() * possibleTargets.length)];
  if (target.mode === "new-trip") states[target.vehicleIndex].routes.push([storeId]);
  else states[target.vehicleIndex].routes[target.tripIndex].splice(target.insertAt, 0, storeId);
  normalizeRouteBuckets(states);
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
  let current = clone(routeState);
  for (let step = 0; step < steps; step += 1) {
    const roll = random();
    let next = null;
    if (roll < 0.35) next = randomRelocateNeighbor(current, wave, random)?.state || null;
    else if (roll < 0.6) next = randomSwapNeighbor(current, random)?.state || null;
    else if (roll < 0.8) next = randomTwoOptNeighbor(current, random)?.state || null;
    else next = randomLnsNeighbor(current, scenario, wave, random)?.state || null;
    if (!next) continue;
    const rebuilt = rebuildWavePlansFromState(next, scenario, wave);
    if (!rebuilt) continue;
    current = cloneWaveRouteState(rebuilt);
  }
  return current;
}

function randomRelocateMove(routeState, wave, random) {
  return randomRelocateNeighbor(routeState, wave, random)?.state || null;
}

function randomSwapNeighbor(routeState, random) {
  const states = clone(routeState);
  const positions = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      route.forEach((storeId, stopIndex) => {
        positions.push({ vehicleIndex, tripIndex, stopIndex, storeId });
      });
    });
  });
  if (positions.length < 2) return null;
  const first = positions[Math.floor(random() * positions.length)];
  let second = positions[Math.floor(random() * positions.length)];
  let guard = 0;
  while (guard < 12 && first.vehicleIndex === second.vehicleIndex && first.tripIndex === second.tripIndex && first.stopIndex === second.stopIndex) {
    second = positions[Math.floor(random() * positions.length)];
    guard += 1;
  }
  if (first.vehicleIndex === second.vehicleIndex && first.tripIndex === second.tripIndex && first.stopIndex === second.stopIndex) return null;
  const a = states[first.vehicleIndex].routes[first.tripIndex][first.stopIndex];
  const b = states[second.vehicleIndex].routes[second.tripIndex][second.stopIndex];
  states[first.vehicleIndex].routes[first.tripIndex][first.stopIndex] = b;
  states[second.vehicleIndex].routes[second.tripIndex][second.stopIndex] = a;
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
  return randomSwapNeighbor(routeState, random)?.state || null;
}

function randomTwoOptNeighbor(routeState, random) {
  const states = clone(routeState);
  const longRoutes = [];
  states.forEach((stateItem, vehicleIndex) => {
    stateItem.routes.forEach((route, tripIndex) => {
      if (route.length >= 3) longRoutes.push({ vehicleIndex, tripIndex, route });
    });
  });
  if (!longRoutes.length) return null;
  const target = longRoutes[Math.floor(random() * longRoutes.length)];
  const route = states[target.vehicleIndex].routes[target.tripIndex];
  const i = Math.floor(random() * (route.length - 1));
  const j = i + 1 + Math.floor(random() * (route.length - i - 1));
  states[target.vehicleIndex].routes[target.tripIndex] = route.slice(0, i).concat(route.slice(i, j + 1).reverse(), route.slice(j + 1));
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
  return randomTwoOptNeighbor(routeState, random)?.state || null;
}

function lnsRepairGreedy(routeState, removedStoreIds, scenario, wave) {
  let states = clone(routeState);
  for (const storeId of removedStoreIds) {
    const store = scenario.storeMap.get(storeId);
    if (!store) continue;
    let bestState = null;
    let bestCost = Number.POSITIVE_INFINITY;
    states.forEach((stateItem, vehicleIndex) => {
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
      const candidate = buildTripCandidate(plan, store, scenario, wave, false);
      if (!candidate) return;
      const nextState = clone(states);
      nextState[vehicleIndex].routes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
      const rebuilt = rebuildWavePlansFromState(nextState, scenario, wave);
      if (!rebuilt) return;
      const cost = wavePlansCost(rebuilt, scenario, wave);
      if (cost < bestCost) {
        bestCost = cost;
        bestState = nextState;
      }
    });
    if (!bestState) return null;
    states = bestState;
  }
  return states;
}

function lnsRepair(routeState, removedStoreIds, scenario, wave) {
  return lnsRepairGreedy(routeState, removedStoreIds, scenario, wave);
}

function lnsRepairRegret(routeState, removedStoreIds, scenario, wave, regretK = 2) {
  let states = clone(routeState);
  const pending = [...removedStoreIds];
  while (pending.length) {
    let bestChoice = null;
    for (const storeId of pending) {
      const store = scenario.storeMap.get(storeId);
      if (!store) continue;
      const ranked = [];
      states.forEach((stateItem, vehicleIndex) => {
        const plan = rebuildSingleStatePlan(stateItem, scenario, wave);
        if (!plan) return;
        const candidate = buildTripCandidate(plan, store, scenario, wave, false);
        if (!candidate) return;
        const nextState = clone(states);
        nextState[vehicleIndex].routes = candidate.nextPlan.trips.map((trip) => [...trip.route]);
        const rebuilt = rebuildWavePlansFromState(nextState, scenario, wave);
        if (!rebuilt) return;
        ranked.push({
          storeId,
          nextState,
          cost: wavePlansCost(rebuilt, scenario, wave),
        });
      });
      ranked.sort((a, b) => a.cost - b.cost);
      if (!ranked.length) continue;
      const best = ranked[0];
      const compare = ranked[Math.min(regretK - 1, ranked.length - 1)];
      const regret = (compare?.cost ?? (best.cost + 30)) - best.cost;
      if (!bestChoice || regret > bestChoice.regret || (regret === bestChoice.regret && best.cost < bestChoice.best.cost)) {
        bestChoice = { storeId, best, regret };
      }
    }
    if (!bestChoice) return null;
    states = bestChoice.best.nextState;
    const index = pending.indexOf(bestChoice.storeId);
    if (index >= 0) pending.splice(index, 1);
  }
  return states;
}

function destroyRandom(routeState, scenario, wave, random, removeCount) {
  const storeRefs = collectStorePositions(routeState);
  if (!storeRefs.length) return null;
  const pool = [...storeRefs];
  const removed = [];
  while (removed.length < removeCount && pool.length) {
    const pickIndex = Math.floor(random() * pool.length);
    removed.push(pool.splice(pickIndex, 1)[0].storeId);
  }
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "random",
  };
}

function destroyShaw(routeState, scenario, wave, random, removeCount) {
  const storeRefs = collectStorePositions(routeState);
  if (!storeRefs.length) return null;
  const seed = storeRefs[Math.floor(random() * storeRefs.length)].storeId;
  const ranked = [...new Set(storeRefs.map((item) => item.storeId))]
    .map((storeId) => ({ storeId, score: computeStoreRelatedness(seed, storeId, scenario, wave) }))
    .sort((a, b) => a.score - b.score);
  const removed = ranked.slice(0, removeCount).map((item) => item.storeId);
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "shaw",
  };
}

function destroyWorst(routeState, scenario, wave, random, removeCount) {
  const ranked = estimateRemovalSavings(routeState, scenario);
  if (!ranked.length) return null;
  const jittered = ranked
    .map((item, index) => ({ ...item, rankScore: item.saving - random() * (8 + index * 0.1) }))
    .sort((a, b) => b.rankScore - a.rankScore);
  const removed = [...new Set(jittered.slice(0, removeCount).map((item) => item.storeId))];
  return {
    partialState: removeStoresFromRouteState(routeState, removed),
    removedStoreIds: removed,
    destroyKey: "worst",
  };
}

function runLnsIteration(routeState, scenario, wave, random) {
  const totalStops = flattenStoresFromRouteState(routeState).length;
  if (!totalStops) return null;
  const removeCount = pickRemovalCount(totalStops, random);
  const destroyRoll = random();
  let destroyed = null;
  if (destroyRoll < 0.34) destroyed = destroyRandom(routeState, scenario, wave, random, removeCount);
  else if (destroyRoll < 0.68) destroyed = destroyShaw(routeState, scenario, wave, random, removeCount);
  else destroyed = destroyWorst(routeState, scenario, wave, random, removeCount);
  if (!destroyed) return null;
  const repairRoll = random();
  const repairedState = repairRoll < 0.45
    ? lnsRepairGreedy(destroyed.partialState, destroyed.removedStoreIds, scenario, wave)
    : lnsRepairRegret(destroyed.partialState, destroyed.removedStoreIds, scenario, wave, repairRoll < 0.75 ? 2 : 3);
  if (!repairedState) return null;
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

function randomLnsNeighbor(routeState, scenario, wave, random) {
  return runLnsIteration(routeState, scenario, wave, random);
}

function randomLnsMove(routeState, scenario, wave, random) {
  return randomLnsNeighbor(routeState, scenario, wave, random)?.state || null;
}

function sampleNeighborhood(routeState, scenario, wave, random, sampleSize = 20, includeLns = true) {
  const sampled = [];
  const seen = new Set();
  const baselineEval = evaluateRouteState(routeState, scenario, wave);
  if (!baselineEval) return sampled;
  const baselineApprox = approximateTouchedDistance(routeState, scenario);
  for (let i = 0; i < sampleSize; i += 1) {
    const roll = random();
    let neighbor = null;
    if (roll < 0.4) neighbor = randomRelocateNeighbor(routeState, wave, random);
    else if (roll < 0.7) neighbor = randomSwapNeighbor(routeState, random);
    else if (roll < 0.9) neighbor = randomTwoOptNeighbor(routeState, random);
    else if (includeLns) neighbor = randomLnsNeighbor(routeState, scenario, wave, random);
    if (!neighbor) continue;
    const signature = hashRouteState(neighbor.state);
    if (seen.has(signature)) continue;
    const touchedVehicleIndexes = neighbor.meta?.touchedVehicleIndexes || [];
    const approxCandidate = approximateTouchedDistance(neighbor.state, scenario, touchedVehicleIndexes);
    const approxCurrent = touchedVehicleIndexes.length ? approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes) : baselineApprox;
    const allowExpensiveEval = approxCandidate <= approxCurrent + 8 || neighbor.meta?.type === "lns" || random() < 0.22;
    if (!allowExpensiveEval) continue;
    const evaluated = evaluateRouteStateIncremental(neighbor.state, scenario, wave, baselineEval, touchedVehicleIndexes);
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

function sampleSingleNeighbor(routeState, scenario, wave, random, includeLns = true, maxAttempts = 6) {
  const baselineEval = evaluateRouteState(routeState, scenario, wave);
  if (!baselineEval) return null;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const roll = random();
    let neighbor = null;
    if (roll < 0.46) neighbor = randomRelocateNeighbor(routeState, wave, random);
    else if (roll < 0.76) neighbor = randomSwapNeighbor(routeState, random);
    else if (roll < 0.94) neighbor = randomTwoOptNeighbor(routeState, random);
    else if (includeLns) neighbor = randomLnsNeighbor(routeState, scenario, wave, random);
    if (!neighbor) continue;
    const touchedVehicleIndexes = neighbor.meta?.touchedVehicleIndexes || [];
    const approxCandidate = approximateTouchedDistance(neighbor.state, scenario, touchedVehicleIndexes);
    const approxCurrent = approximateTouchedDistance(routeState, scenario, touchedVehicleIndexes);
    if (approxCandidate > approxCurrent + 12 && neighbor.meta?.type !== "lns" && attempt < maxAttempts - 1) continue;
    const evaluated = evaluateRouteStateIncremental(neighbor.state, scenario, wave, baselineEval, touchedVehicleIndexes);
    if (!evaluated) continue;
    return {
      ...neighbor,
      signature: hashRouteState(neighbor.state),
      plans: evaluated.plans,
      cost: evaluated.cost,
    };
  }
  return null;
}

function localImproveState(routeState, scenario, wave, random, rounds = 6) {
  let currentState = clone(routeState);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return null;
  if (rounds <= 0) return { state: currentState, plans: currentEval.plans, cost: currentEval.cost };
  for (let round = 0; round < rounds; round += 1) {
    const neighborhood = sampleNeighborhood(currentState, scenario, wave, random, 10, round % 2 === 0);
    const improving = neighborhood.find((item) => item.cost + 1e-6 < currentEval.cost);
    if (!improving) break;
    currentState = improving.state;
    currentEval = { plans: improving.plans, cost: improving.cost };
  }
  return { state: currentState, plans: currentEval.plans, cost: currentEval.cost };
}

async function cooperativeYield() {
  await sleep(0);
}

async function optimizeWaveWithVrptwBackend(initialPlans, scenario, wave, randomSeed = 7) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "vrptw", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("vrptw_BACKEND_REQUIRED:no_result");
}

async function optimizeWaveWithSavingsBackend(initialPlans, scenario, wave, randomSeed = 13) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "savings", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("savings_BACKEND_REQUIRED:no_result");
}

async function optimizeWaveWithVehicleDrivenBackend(initialPlans, scenario, wave, randomSeed = 17) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "vehicle", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  throw new Error("vehicle_BACKEND_REQUIRED:no_result");
}

async function optimizeWaveWithHybrid(initialPlans, scenario, wave, randomSeed = 42) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "hybrid", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  let currentPlans = currentEval.plans;
  let currentCost = currentEval.cost;
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  let noImproveRounds = 0;
  const traceLog = [
    {
      algorithmKey: "hybrid",
      scope: "wave",
      waveId: wave.waveId,
      stage: "hybrid-start",
      textZh: `娣峰悎闃舵浠庤椽蹇冩彃鍏ュ垵濮嬭В鍚姩锛屽垵濮嬬患鍚堟垚鏈?${currentCost.toFixed(1)}銆俙,
      textJa: `娣峰悎娈甸殠銇勃娆叉尶鍏ャ伄鍒濇湡瑙ｃ亱銈夐枊濮嬨仐銆佸垵鏈熺窂鍚堛偝銈广儓銇?${currentCost.toFixed(1)} 銇с仚銆俙,
      },
  ];
  reportRelayStageProgress(`${wave.waveId} 娣峰悎VRPTW宸插惎鍔紝褰撳墠娉㈡鍐呴儴浠ｄ环 ${currentCost.toFixed(1)}锛屽紑濮嬪仛娣峰悎閭诲煙杩唬銆俙);
  for (let iteration = 0; iteration < 110; iteration += 1) {
    if (iteration && iteration % 6 === 0) await cooperativeYield();
    const neighborhood = sampleNeighborhood(currentState, scenario, wave, random, 24, true);
    if (!neighborhood.length) continue;
    let candidate = neighborhood.find((item) => item.cost + 1e-6 < currentCost);
    if (!candidate && noImproveRounds >= 4) candidate = neighborhood[0];
    if (!candidate) {
      noImproveRounds += 1;
      continue;
    }
    const temperature = Math.max(0.03, 1 - iteration / 110);
    const accept = candidate.cost + 1e-6 < currentCost || random() < Math.exp((currentCost - candidate.cost) / Math.max(0.001, temperature));
    if (!accept) {
      noImproveRounds += 1;
      continue;
    }
    currentState = candidate.state;
    currentPlans = candidate.plans;
    currentCost = candidate.cost;
    noImproveRounds = candidate.cost + 1e-6 < bestCost ? 0 : noImproveRounds + 1;
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
      textZh: `绗?${iteration + 1} 杞帴鍙?${candidate.meta?.type || "mixed"} 鍔ㄤ綔锛屽€欓€夋尝娆″唴閮ㄤ唬浠?${candidate.cost.toFixed(1)}锛屽綋鍓嶆渶濂?${Math.min(bestCost, candidate.cost).toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇?${candidate.meta?.type || "mixed"} 鍕曚綔銈掓帯鐢ㄣ仐銆佸€欒銈炽偣銉?${candidate.cost.toFixed(1)}銆佺従鏅傜偣銇渶鑹伅 ${Math.min(bestCost, candidate.cost).toFixed(1)}銆俙,
      });
    }
    if (candidate.cost + 1e-6 < bestCost) {
      bestCost = candidate.cost;
      bestState = cloneWaveRouteState(candidate.plans);
      bestPlans = candidate.plans;
      reportRelayStageProgress(`${wave.waveId} 娣峰悎VRPTW鍦ㄧ ${iteration + 1} 杞埛鏂版渶浼橈紝娉㈡鍐呴儴浠ｄ环闄嶅埌 ${bestCost.toFixed(1)}銆俙);
      if (traceLog.length < 22) {
        pushTraceEvent(traceLog, {
          algorithmKey: "hybrid",
          scope: "wave",
          waveId: wave.waveId,
          stage: "hybrid-best",
          iteration,
          moveKey: candidate.meta?.type || "mixed",
          bestCost,
      textZh: `绗?${iteration + 1} 杞埛鏂版渶浼樿В锛屽姩浣?${candidate.meta?.type || "mixed"}锛屾柊鐨勬尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
          textJa: `${iteration + 1} 鍥炵洰銇ф渶鑹В銈掓洿鏂般仐銆佸嫊浣?${candidate.meta?.type || "mixed"}銆佹柊銇椼亜鏈€鑹偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
        });
      }
    }
    if ((iteration + 1) % 18 === 0) {
      reportRelayStageProgress(`${wave.waveId} 娣峰悎VRPTW宸茶窇鍒扮 ${iteration + 1}/110 杞紝褰撳墠鏈€濂芥尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
    }
    if (noImproveRounds >= 6) {
      const kick = localImproveState(bestState, scenario, wave, random, 3);
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
    textZh: `娣峰悎闃舵缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `娣峰悎娈甸殠銇岀祩浜嗐仐銆佹渶绲傛渶鑹窂鍚堛偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 娣峰悎VRPTW缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  return { plans: finalPlans, traceLog };
}

async function optimizeWaveWithTabu(initialPlans, scenario, wave, randomSeed = 77) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "tabu", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  let currentPlans = currentEval.plans;
  let currentCost = currentEval.cost;
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  const tabu = new Map();
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "tabu",
    scope: "wave",
    waveId: wave.waveId,
    stage: "tabu-start",
    bestCost,
    textZh: `绂佸繉鎼滅储浠庡綋鍓嶅彲琛岃В鍑哄彂锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `銈裤儢銉兼帰绱伅鐝惧湪銇彲琛岃В銇嬨倝闁嬪銇椼€佸垵鏈熴偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 绂佸繉鎼滅储宸插惎鍔紝鍒濆娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
  for (let iteration = 0; iteration < 120; iteration += 1) {
    if (iteration && iteration % 6 === 0) await cooperativeYield();
    let bestNeighbor = null;
    const candidates = sampleNeighborhood(currentState, scenario, wave, random, 28, true);
    for (const candidate of candidates) {
      const tabuBlocked = (candidate.meta?.tabuKeys || []).some((key) => iteration < (tabu.get(key) || -1));
      const aspiration = candidate.cost + 1e-6 < bestCost;
      if (tabuBlocked && !aspiration) continue;
      if (!bestNeighbor || candidate.cost < bestNeighbor.cost) bestNeighbor = candidate;
    }
    if (!bestNeighbor) continue;
    currentState = bestNeighbor.state;
    currentPlans = bestNeighbor.plans;
    currentCost = bestNeighbor.cost;
    (bestNeighbor.meta?.tabuKeys || [bestNeighbor.signature]).forEach((key, index) => {
      tabu.set(key, iteration + 8 + index);
    });
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
        textZh: `绗?${iteration + 1} 杞粠閭诲煙鏍锋湰涓€夊嚭褰撳墠鏈€浼橀偦灞咃紝鍔ㄤ綔 ${bestNeighbor.meta?.type || "mixed"}锛屾垚鏈?${currentCost.toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇ц繎鍌嶃偟銉炽儣銉亱銈夋渶鑹殻鎺ヨВ銈掓帯鐢ㄣ仐銆佸嫊浣?${bestNeighbor.meta?.type || "mixed"}銆併偝銈广儓銇?${currentCost.toFixed(1)} 銇с仚銆俙,
      });
    }
    if (currentCost < bestCost) {
      bestCost = currentCost;
      bestState = cloneWaveRouteState(currentPlans);
      bestPlans = currentPlans;
      reportRelayStageProgress(`${wave.waveId} 绂佸繉鎼滅储鍦ㄧ ${iteration + 1} 杞埛鏂版渶浼橈紝娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
      pushTraceEvent(traceLog, {
        algorithmKey: "tabu",
        scope: "wave",
        waveId: wave.waveId,
        stage: "tabu-best",
        iteration,
        bestCost,
      textZh: `绗?${iteration + 1} 杞埛鏂扮蹇屾悳绱㈡渶浼樿В锛屾柊鐨勬尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇с偪銉栥兗鎺㈢储銇渶鑹В銈掓洿鏂般仐銆佹柊銇椼亜鏈€鑹偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
      });
    }
    if ((iteration + 1) % 20 === 0) {
      reportRelayStageProgress(`${wave.waveId} 绂佸繉鎼滅储宸茶窇鍒扮 ${iteration + 1}/120 杞紝褰撳墠鏈€濂芥尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
    }
  }
  const finalPlans = rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans;
  pushTraceEvent(traceLog, {
    algorithmKey: "tabu",
    scope: "wave",
    waveId: wave.waveId,
    stage: "tabu-finish",
    bestCost,
    textZh: `绂佸繉鎼滅储缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `銈裤儢銉兼帰绱亴绲備簡銇椼€佹渶绲傛渶鑹偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 绂佸繉鎼滅储缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  return { plans: finalPlans, traceLog };
}

async function optimizeWaveWithLns(initialPlans, scenario, wave, randomSeed = 109) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "lns", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  let currentPlans = currentEval.plans;
  let currentCost = currentEval.cost;
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "lns",
    scope: "wave",
    waveId: wave.waveId,
    stage: "lns-start",
    bestCost,
    textZh: `澶ч偦鍩熸悳绱粠鍒濆瑙ｅ嚭鍙戯紝寮€濮嬪仛 destroy / repair 杩唬锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `澶ц繎鍌嶆帰绱伅鍒濇湡瑙ｃ亱銈夐枊濮嬨仐銆乨estroy / repair 銈掑弽寰┿仐銇俱仚銆傚垵鏈熴偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 澶ч偦鍩熸悳绱㈠凡鍚姩锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  for (let iteration = 0; iteration < 70; iteration += 1) {
    if (iteration && iteration % 5 === 0) await cooperativeYield();
    const candidate = runLnsIteration(currentState, scenario, wave, random);
    if (!candidate) continue;
    const candidatePlans = rebuildWavePlansFromState(candidate.state, scenario, wave);
    if (!candidatePlans) continue;
    const candidateCost = wavePlansCost(candidatePlans, scenario, wave);
    const improved = candidateCost < currentCost;
    const accepted = improved || random() < 0.10;
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
        textZh: `绗?${iteration + 1} 杞畬鎴愪竴娆?${candidate.meta?.destroyKey || "random"} destroy + ${candidate.meta?.repairKey || "greedy"} repair锛屽€欓€夋垚鏈?${candidateCost.toFixed(1)}${improved ? "锛屼紭浜庡綋鍓嶈В骞惰鎺ュ彈" : "锛屼綔涓烘壈鍔ㄨВ琚帴鍙?}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇?${candidate.meta?.destroyKey || "random"} destroy + ${candidate.meta?.repairKey || "greedy"} repair 銈掑畬浜嗐仐銆佸€欒銈炽偣銉?${candidateCost.toFixed(1)}${improved ? " 銇従瑙ｃ倛銈婅壇銇忔帯鐢ㄣ仌銈屻伨銇椼仧" : " 銇敧涔辫В銇ㄣ仐銇︽帯鐢ㄣ仌銈屻伨銇椼仧"}銆俙,
      });
    }
    if (candidateCost < bestCost) {
      bestCost = candidateCost;
      bestState = cloneWaveRouteState(candidatePlans);
      bestPlans = candidatePlans;
      reportRelayStageProgress(`${wave.waveId} 澶ч偦鍩熷湪绗?${iteration + 1} 杞埛鏂版渶浼橈紝娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
      pushTraceEvent(traceLog, {
        algorithmKey: "lns",
        scope: "wave",
        waveId: wave.waveId,
        stage: "lns-best",
        iteration,
        bestCost,
        textZh: `绗?${iteration + 1} 杞埛鏂?LNS 鏈€浼樿В锛屾尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇?LNS 銇渶鑹В銈掓洿鏂般仐銆佹渶鑹偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
      });
    }
    if ((iteration + 1) % 14 === 0) {
      reportRelayStageProgress(`${wave.waveId} 澶ч偦鍩熷凡璺戝埌绗?${iteration + 1}/70 杞紝褰撳墠鏈€濂芥尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
    }
  }
  const finalPlans = rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans;
  pushTraceEvent(traceLog, {
    algorithmKey: "lns",
    scope: "wave",
    waveId: wave.waveId,
    stage: "lns-finish",
    bestCost,
    textZh: `澶ч偦鍩熸悳绱㈢粨鏉燂紝鏈€缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `澶ц繎鍌嶆帰绱亴绲備簡銇椼€佹渶绲傛渶鑹偝銈广儓銇?${bestCost.toFixed(1)} 銇с仚銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 澶ч偦鍩熸悳绱㈢粨鏉燂紝鏈€缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  return { plans: finalPlans, traceLog };
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 2500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function ensureGaBackendAvailable(force = false) {
  const now = Date.now();
  if (!force && gaBackendHealth.available !== null && now - gaBackendHealth.checkedAt < 15000) {
    return gaBackendHealth.available;
  }
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/health`, {}, 1200);
    gaBackendHealth = { available: response.ok, checkedAt: now };
    return response.ok;
  } catch {
    gaBackendHealth = { available: false, checkedAt: now };
    return false;
  }
}

function renderGaBackendStatus() {
  const bar = document.getElementById("gaBackendStatusBar");
  if (!bar) return;
  bar.classList.remove("is-online", "is-offline", "is-checking");
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

async function refreshGaBackendStatus(force = false) {
  try {
    await ensureGaBackendAvailable(force);
  } catch {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
  } finally {
    renderGaBackendStatus();
  }
}

function startGaBackendStatusMonitor() {
  if (gaBackendStatusTimer) clearInterval(gaBackendStatusTimer);
  refreshGaBackendStatus(true);
  scheduleAmapCacheSync(2600);
  gaBackendStatusTimer = setInterval(() => {
    refreshGaBackendStatus(true);
  }, 6000);
}

async function flushAmapCacheToBackend() {
  if (!amapCacheSyncPending) return false;
  amapCacheSyncPending = false;
  let available = await ensureGaBackendAvailable();
  if (!available) {
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) {
    amapCacheSyncPending = true;
    scheduleAmapCacheSync(8000);
    return false;
  }
  try {
    const distanceCache = loadAmapDistanceCache();
    const routeCache = loadAmapRouteCache();
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/amap-cache/sync`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        distanceCache,
        routeCache,
        maxRows: 12000,
      }),
    }, 12000);
    if (!response.ok) {
      amapCacheSyncPending = true;
      scheduleAmapCacheSync(8000);
      return false;
    }
    const data = await response.json();
    return Boolean(data?.ok);
  } catch {
    amapCacheSyncPending = true;
    scheduleAmapCacheSync(8000);
    return false;
  }
}

async function saveRunArchiveToBackend(snapshot) {
  if (!snapshot?.id) return false;
  let available = await ensureGaBackendAvailable();
  if (!available) {
    await sleep(160);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) return false;
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ snapshot }),
    }, 8000);
    if (!response.ok) return false;
    const data = await response.json();
    return Boolean(data?.ok);
  } catch {
    return false;
  }
}

async function listRunArchivesFromBackend(date, page = 1, pageSize = 6) {
  let available = await ensureGaBackendAvailable();
  if (!available) {
    await sleep(160);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) return null;
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date, page, pageSize }),
    }, 10000);
    if (!response.ok) return null;
    const data = await response.json();
    if (!data?.ok) return null;
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

async function getRunArchiveFromBackend(archiveId) {
  if (!archiveId) return null;
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return null;
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/get`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: archiveId }),
    }, 8000);
    if (!response.ok) return null;
    const data = await response.json();
    if (!data?.ok || !data?.found) return null;
    return data.item || null;
  } catch {
    return null;
  }
}

function getCurrentTaskDate() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function formatRecommendedScore(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(4) : String(value);
}

function formatRecommendedScoreLabel(value) {
  if (value === null || value === undefined || value === "") return "寰呰绠?;
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(4) : String(value);
}

function renderRecommendedPlans() {
  const mount = document.getElementById("recommendedPlansSection");
  if (!mount) return;
  const taskDate = recommendedPlanCache.taskDate || getCurrentTaskDate();
  const items = Array.isArray(recommendedPlanCache.items) ? recommendedPlanCache.items : [];
  const selected = recommendedPlanSelectedCache.taskDate === taskDate ? recommendedPlanSelectedCache.selected : null;
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
          <button class="secondary select-recommended-plan" data-candidate-id="${item.id}">閫夋嫨姝ゆ柟妗?/button>
        </div>
      `).join("")
    : `<div class="empty-state">鏆傛棤鎺ㄨ崘鏂规</div>`;
  mount.innerHTML = `
    <div class="panel-head">
      <div>
        <h2>鍘嗗彶鏂规鎺ㄨ崘</h2>
        <p>褰撳墠浠诲姟鏃ユ湡锛?{taskDate}</p>
      </div>
      <div class="toolbar inline-toolbar">
        <button id="fetchRecommendedPlansBtn" class="primary">鑾峰彇鎺ㄨ崘鏂规</button>
      </div>
    </div>
    <div class="recommended-selected">
      <div class="panel-head">
        <div>
          <h3>褰撳墠宸查€夋柟妗?/h3>
          <p>${selected ? `sourceRunId: ${selected.sourceRunId || "-"} | created_at: ${selected.snapshot?.created_at || selected.createdAt || "-"} | strategy: ${selected.snapshot?.strategy || "-"} | goal: ${selected.snapshot?.goal || "-"} | best_score: ${formatRecommendedScore(selected.snapshot?.best_score)}` : "鏆傛棤宸查€夋柟妗?}</p>
        </div>
      </div>
    </div>
    <div id="recommendedPlansList" class="recommended-list">
      ${emptyState}
    </div>
  `;
  const fetchBtn = document.getElementById("fetchRecommendedPlansBtn");
  if (fetchBtn) {
    fetchBtn.onclick = async () => {
      await refreshRecommendedPlans(taskDate, true);
      await refreshRecommendedPlanSelected(taskDate, true);
    };
  }
  document.querySelectorAll(".select-recommended-plan").forEach((button) => {
    button.onclick = async () => {
      const candidateId = Number(button.dataset.candidateId || 0);
      await selectRecommendedPlan(taskDate, candidateId);
    };
  });
}

async function refreshRecommendedPlanSelected(taskDate = getCurrentTaskDate(), force = false) {
  if (!taskDate) return;
  if (recommendedPlanSelectedCache.loading && !force) return;
  recommendedPlanSelectedCache.loading = true;
  renderRecommendedPlans();
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/current?taskDate=${encodeURIComponent(taskDate)}`, {}, 5000);
    if (!response.ok) {
      recommendedPlanSelectedCache = { taskDate, selected: null, loading: false };
      renderRecommendedPlans();
      return;
    }
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
  if (!taskDate) return;
  if (recommendedPlanCache.loading && !force) return;
  recommendedPlanCache.loading = true;
  renderRecommendedPlans();
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/list?taskDate=${encodeURIComponent(taskDate)}`, {}, 5000);
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

async function selectRecommendedPlan(taskDate, candidateId) {
  if (!taskDate || !candidateId) return;
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/recommended-plans/select`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ taskDate, candidateId }),
    }, 5000);
    if (!response.ok) return;
    const data = await response.json();
    if (!data?.ok) return;
    await refreshRecommendedPlans(taskDate, true);
    await refreshRecommendedPlanSelected(taskDate, true);
  } catch {}
}

function setRunRegionStatus(text = "") {
  const el = document.getElementById("runRegionStatus");
  if (!el) return;
  el.textContent = text || "";
}

function getSelectedRunRegionSchemeNo() {
  return String(runRegionSelectedSchemeNo || "").trim();
}

function setSelectedRunRegionSchemeNo(value = "") {
  runRegionSelectedSchemeNo = String(value || "").trim();
  if (runRegionSelectedSchemeNo) {
    localStorage.setItem(RUN_REGION_SCHEME_SELECTED_KEY, runRegionSelectedSchemeNo);
  } else {
    localStorage.removeItem(RUN_REGION_SCHEME_SELECTED_KEY);
  }
  const regionSelect = document.getElementById("runRegionSchemeSelect");
  if (regionSelect) regionSelect.value = runRegionSelectedSchemeNo;
  const solveSelect = document.getElementById("solveRegionSchemeSelect");
  if (solveSelect) solveSelect.value = runRegionSelectedSchemeNo;
}

function normalizeRunRegionPath(path) {
  if (!Array.isArray(path)) return [];
  return path
    .map((point) => {
      if (Array.isArray(point) && point.length >= 2) {
        return [Number(point[0]), Number(point[1])];
      }
      if (point && typeof point === "object") {
        return [Number(point.lng ?? point.x), Number(point.lat ?? point.y)];
      }
      return null;
    })
    .filter((point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]));
}

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

function getPathFromRunRegionPolygon(polygon) {
  if (!polygon?.getPath) return [];
  const rawPath = polygon.getPath() || [];
  return rawPath
    .map((point) => {
      if (Array.isArray(point) && point.length >= 2) return [Number(point[0]), Number(point[1])];
      if (point && typeof point.getLng === "function" && typeof point.getLat === "function") {
        return [Number(point.getLng()), Number(point.getLat())];
      }
      if (point && typeof point === "object") return [Number(point.lng), Number(point.lat)];
      return null;
    })
    .filter((point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]));
}

function stopRunRegionDrawing(removeDraft = false) {
  if (runRegionPolygonEditor) {
    try { runRegionPolygonEditor.close(); } catch {}
    runRegionPolygonEditor = null;
  }
  if (runRegionMouseTool) {
    try { runRegionMouseTool.close(true); } catch {}
    runRegionMouseTool = null;
  }
  if (removeDraft && runRegionDraftPolygon && !runRegionEditingId) {
    try { runRegionDraftPolygon.setMap(null); } catch {}
  }
  runRegionDraftPolygon = null;
  runRegionEditingId = null;
  const saveBtn = document.getElementById("runRegionSaveBtn");
  if (saveBtn) saveBtn.disabled = true;
}

async function ensureRunRegionMapReady() {
  const container = document.getElementById("runRegionMap");
  if (!container) return false;
  if (runRegionMap) return true;
  if (!window.AMap?.Map) {
    container.innerHTML = `<div class="empty-state">楂樺痉鍦板浘鏈姞杞藉畬鎴愶紝璇风◢鍚庨噸璇曘€?/div>`;
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
  try {
    runRegionMap.addControl(new window.AMap.ToolBar({ position: "RB" }));
    runRegionMap.addControl(new window.AMap.Scale());
  } catch {}
  runRegionMap.setZoomAndCenter(11, [Number(DC.lng), Number(DC.lat)]);
  return true;
}

function normalizeRunRegionStoreVisibilityMode(mode) {
  const value = String(mode || "").trim();
  if (["show_all", "show_in_region", "hide_all", "hide_in_region"].includes(value)) return value;
  return "show_all";
}

function ensureRunRegionCheckedSelection(items) {
  const list = Array.isArray(items) ? items : [];
  const validIds = new Set(list.map((item) => String(item.id)));
  const next = new Set();
  runRegionCheckedRegionIds.forEach((id) => {
    if (validIds.has(String(id))) next.add(String(id));
  });
  if (!next.size && list.length) {
    list.forEach((item) => next.add(String(item.id)));
  }
  runRegionCheckedRegionIds = next;
}

function getSelectedRunRegionItems() {
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  if (!items.length) return [];
  return items.filter((item) => runRegionCheckedRegionIds.has(String(item.id)));
}

function getSelectedRunRegionStoreIdSet() {
  const set = new Set();
  getSelectedRunRegionItems().forEach((item) => {
    (item.storeIds || []).forEach((storeId) => {
      const sid = String(storeId || "").trim();
      if (sid) set.add(sid);
    });
  });
  return set;
}

function getRunRegionTargetPaths() {
  const items = getSelectedRunRegionItems();
  if (!items.length) return [];
  if (String(runRegionTargetRegionId || "all") === "all") {
    return items.map((item) => normalizeRunRegionPath(item.path)).filter((path) => path.length >= 3);
  }
  const target = items.find((item) => String(item.id) === String(runRegionTargetRegionId));
  if (!target) return [];
  const path = normalizeRunRegionPath(target.path);
  return path.length >= 3 ? [path] : [];
}

function isPointInRunRegionPath(point, path) {
  if (!Array.isArray(path) || path.length < 3 || !Array.isArray(point) || point.length < 2) return false;
  const x = Number(point[0]);
  const y = Number(point[1]);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
  let inside = false;
  for (let i = 0, j = path.length - 1; i < path.length; j = i, i += 1) {
    const xi = Number(path[i][0]);
    const yi = Number(path[i][1]);
    const xj = Number(path[j][0]);
    const yj = Number(path[j][1]);
    const intersects = ((yi > y) !== (yj > y))
      && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || 1e-12) + xi);
    if (intersects) inside = !inside;
  }
  return inside;
}

function isRunRegionStorePointInTarget(item, paths) {
  const lng = Number(item?.lng);
  const lat = Number(item?.lat);
  if (!Number.isFinite(lng) || !Number.isFinite(lat) || !paths.length) return false;
  const point = [lng, lat];
  return paths.some((path) => isPointInRunRegionPath(point, path));
}

function getVisibleRunRegionStorePoints() {
  const mode = normalizeRunRegionStoreVisibilityMode(runRegionStoreVisibilityMode);
  if (mode === "hide_all") return [];
  const points = Array.isArray(runRegionStorePoints) ? runRegionStorePoints : [];
  const selectedItems = getSelectedRunRegionItems();
  if (!selectedItems.length) return [];
  const selectedStoreIdSet = getSelectedRunRegionStoreIdSet();
  const selectedPaths = selectedItems.map((item) => normalizeRunRegionPath(item.path)).filter((path) => path.length >= 3);
  const scopedPoints = points.filter((item) => {
    const sid = String(item?.store_id || "").trim();
    if (sid && selectedStoreIdSet.has(sid)) return true;
    if (selectedPaths.length) return isRunRegionStorePointInTarget(item, selectedPaths);
    return false;
  });
  if (mode === "show_all") return scopedPoints;
  const paths = getRunRegionTargetPaths();
  if (!paths.length) return mode === "show_in_region" ? [] : scopedPoints;
  if (mode === "show_in_region") {
    return scopedPoints.filter((item) => isRunRegionStorePointInTarget(item, paths));
  }
  if (mode === "hide_in_region") {
    return scopedPoints.filter((item) => !isRunRegionStorePointInTarget(item, paths));
  }
  return scopedPoints;
}

function renderRunRegionStoreMarkers() {
  if (!runRegionMap || !window.AMap?.Marker) return;
  runRegionStoreMarkers.forEach((marker) => {
    try { marker.setMap(null); } catch {}
  });
  runRegionStoreMarkers = [];
  const visiblePoints = getVisibleRunRegionStorePoints();
  runRegionStoreMarkers = visiblePoints.flatMap((item) => {
    const lng = Number(item.lng);
    const lat = Number(item.lat);
    if (!Number.isFinite(lng) || !Number.isFinite(lat)) return [];
    const marker = new window.AMap.Marker({
      position: [lng, lat],
      anchor: "center",
      content: '<div class="run-region-store-point"></div>',
      offset: new window.AMap.Pixel(-6, -6),
      zIndex: 120,
      bubble: true,
    });
    const labelText = `${item.store_name || item.store_id || ""}`.trim();
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

function renderRunRegionPolygons() {
  if (!runRegionMap || !window.AMap?.Polygon) return;
  runRegionPolygons.forEach((polygon) => {
    try { polygon.setMap(null); } catch {}
  });
  runRegionPolygons.clear();
  getSelectedRunRegionItems().forEach((item, index) => {
    const path = normalizeRunRegionPath(item.path);
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

function fitRunRegionMapView() {
  if (!runRegionMap) return;
  const overlays = [...runRegionStoreMarkers, ...Array.from(runRegionPolygons.values())];
  if (!overlays.length) return;
  try {
    runRegionMap.setFitView(overlays, false, [56, 56, 56, 56], 16);
  } catch {}
}

function renderRunRegionSchemeOptions() {
  const items = Array.isArray(runRegionSchemeCache.items) ? runRegionSchemeCache.items : [];
  const options = [
    `<option value="">璇烽€夋嫨鍒嗗尯鏂规鍙?/option>`,
    ...items.map((item) => `<option value="${escapeHtml(item.schemeNo)}">${escapeHtml(`${item.schemeNo} | ${item.name}${item.enabled ? "" : "锛堝仠鐢級"}`)}</option>`),
  ];
  const regionSelect = document.getElementById("runRegionSchemeSelect");
  if (regionSelect) {
    regionSelect.innerHTML = options.join("");
    regionSelect.value = getSelectedRunRegionSchemeNo();
  }
  const solveSelect = document.getElementById("solveRegionSchemeSelect");
  if (solveSelect) {
    solveSelect.innerHTML = options.join("");
    solveSelect.value = getSelectedRunRegionSchemeNo();
  }
  const selected = items.find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
  const noInput = document.getElementById("runRegionSchemeNoInput");
  const nameInput = document.getElementById("runRegionSchemeNameInput");
  const enabledInput = document.getElementById("runRegionSchemeEnabledInput");
  if (selected) {
    if (noInput) noInput.value = selected.schemeNo;
    if (nameInput) nameInput.value = selected.name || "";
    if (enabledInput) enabledInput.checked = Boolean(selected.enabled);
  } else {
    if (nameInput) nameInput.value = "";
    if (enabledInput) enabledInput.checked = true;
  }
}

function renderRunRegionTargetRegionOptions() {
  const select = document.getElementById("runRegionTargetRegion");
  if (!select) return;
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  const previous = String(runRegionTargetRegionId || "all");
  const options = [
    `<option value="all">鍏ㄩ儴</option>`,
    ...items.map((item) => `<option value="${item.id}">${escapeHtml((item.regionCode ? `鏂规1-${item.regionCode}` : "") || item.name || `杩愯鍖?${item.id}`)}</option>`),
  ];
  select.innerHTML = options.join("");
  const keep = previous === "all" || items.some((item) => String(item.id) === previous);
  runRegionTargetRegionId = keep ? previous : "all";
  select.value = runRegionTargetRegionId;
}

function renderRunRegionList() {
  const mount = document.getElementById("runRegionList");
  if (!mount) return;
  renderRunRegionSchemeOptions();
  if (!getSelectedRunRegionSchemeNo()) {
    runRegionTargetRegionId = "all";
    renderRunRegionTargetRegionOptions();
    mount.innerHTML = `<div class="empty-state">璇峰厛閫夋嫨鍒嗗尯鏂规鍙枫€?/div>`;
    return;
  }
  const items = Array.isArray(runRegionCache.items) ? runRegionCache.items : [];
  ensureRunRegionCheckedSelection(items);
  renderRunRegionTargetRegionOptions();
  if (!items.length) {
    mount.innerHTML = `<div class="empty-state">鏆傛棤杩愯鍖猴紝璇峰厛缁樺埗骞朵繚瀛樸€?/div>`;
    return;
  }
  const checkedCount = items.filter((item) => runRegionCheckedRegionIds.has(String(item.id))).length;
  const allChecked = checkedCount > 0 && checkedCount === items.length;
  const listHeader = `
    <div class="run-region-check-toolbar">
      <label class="run-region-check-inline">
        <input type="checkbox" id="runRegionCheckAll" ${allChecked ? "checked" : ""}>
        <span>鍏ㄩ儴</span>
      </label>
      <span class="run-region-check-summary">宸查€?${checkedCount}/${items.length}</span>
    </div>
  `;
  mount.innerHTML = items.map((item) => `
    <article class="run-region-item" data-region-id="${item.id}">
      <div class="run-region-item-title">
        <label class="run-region-check-inline">
          <input type="checkbox" class="run-region-visibility-check" data-region-id="${item.id}" ${runRegionCheckedRegionIds.has(String(item.id)) ? "checked" : ""}>
          <span>${escapeHtml(item.name || `杩愯鍖?${item.id}`)}</span>
        </label>
      </div>
      <div class="run-region-item-meta">鏂规鍙? ${escapeHtml(item.schemeNo || "-")} | 鍒嗗尯鍙? ${escapeHtml(item.regionCode || "-")} | ID: ${item.id} | 鍥存爮鐐? ${(item.path || []).length}</div>
      <div class="run-region-item-meta">闂ㄥ簵闆嗗悎(${(item.storeIds || []).length}): ${escapeHtml((item.storeNames && item.storeNames.length ? item.storeNames : item.storeIds || []).join("銆?) || "-")}</div>
      <div class="run-region-item-meta">鏇存柊鏃堕棿: ${escapeHtml(String(item.updatedAt || item.createdAt || "-"))}</div>
      <div class="run-region-item-actions">
        <button class="secondary run-region-edit-btn" data-region-id="${item.id}">缂栬緫</button>
        <button class="alert run-region-delete-btn" data-region-id="${item.id}">鍒犻櫎</button>
      </div>
    </article>
  `).join("");
  mount.innerHTML = `${listHeader}${mount.innerHTML}`;
}

async function listRunRegionSchemesFromBackend() {
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/list`, {}, 6000);
    if (!response.ok) return [];
    const data = await response.json();
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
    return [];
  }
}

async function listRunRegionsFromBackend(schemeNo) {
  const scheme = String(schemeNo || "").trim();
  if (!scheme) return [];
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return [];
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/list?schemeNo=${encodeURIComponent(scheme)}`, {}, 6000);
    if (!response.ok) return [];
    const data = await response.json();
    if (!data?.ok || !Array.isArray(data.items)) return [];
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

async function generateRunRegionScheme1FromBackend() {
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return false;
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/generate-scheme1`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    }, 15000);
    if (!response.ok) return false;
    const data = await response.json();
    return Boolean(data?.ok);
  } catch {
    return false;
  }
}

async function listStorePointsForRunRegion() {
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) return fallbackRunRegionStorePoints();
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/stores/points`, {}, 6000);
    if (!response.ok) return fallbackRunRegionStorePoints();
    const data = await response.json();
    if (!data?.ok || !Array.isArray(data.items)) return fallbackRunRegionStorePoints();
    const points = data.items
      .map((item) => ({
        store_id: String(item.store_id || item.storeId || ""),
        store_name: String(item.store_name || item.storeName || ""),
        lng: Number(item.lng),
        lat: Number(item.lat),
      }))
      .filter((item) => item.store_id && Number.isFinite(item.lng) && Number.isFinite(item.lat));
    return points.length ? points : fallbackRunRegionStorePoints();
  } catch {
    return fallbackRunRegionStorePoints();
  }
}

async function createRunRegionOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  if (!response.ok) return null;
  const data = await response.json();
  if (!data?.ok) return null;
  return data.item || null;
}

async function createRunRegionSchemeOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  if (!response.ok) return null;
  const data = await response.json();
  if (!data?.ok) return null;
  return data.item || null;
}

async function updateRunRegionSchemeOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  if (!response.ok) return null;
  const data = await response.json();
  if (!data?.ok) return null;
  return data.item || null;
}

async function deleteRunRegionSchemeOnBackend(schemeId) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-region-schemes/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id: Number(schemeId || 0) }),
  }, 6000);
  if (!response.ok) return false;
  const data = await response.json();
  return Boolean(data?.ok);
}

async function updateRunRegionOnBackend(payload) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }, 6000);
  if (!response.ok) return null;
  const data = await response.json();
  if (!data?.ok) return null;
  return data.item || null;
}

async function deleteRunRegionOnBackend(regionId) {
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/run-regions/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ id: Number(regionId || 0) }),
  }, 6000);
  if (!response.ok) return false;
  const data = await response.json();
  return Boolean(data?.ok);
}

async function refreshRunRegionData(force = false) {
  if ((runRegionCache.loading || runRegionSchemeCache.loading) && !force) return;
  runRegionCache.loading = true;
  runRegionSchemeCache.loading = true;
  renderRunRegionList();
  const mapReady = await ensureRunRegionMapReady();
  if (!mapReady && !window.AMap?.Map) {
    if (runRegionMapRetryTimer) clearTimeout(runRegionMapRetryTimer);
    runRegionMapRetryTimer = setTimeout(() => {
      runRegionMapRetryTimer = null;
      void refreshRunRegionData(true);
    }, 1200);
  }
  try {
    const [rawSchemes, points] = await Promise.all([
      listRunRegionSchemesFromBackend(),
      listStorePointsForRunRegion(),
    ]);
    let schemes = Array.isArray(rawSchemes) ? rawSchemes : [];
    if (!runRegionScheme1AutoGenerated && !schemes.some((item) => item.schemeNo === "1")) {
      const generated = await generateRunRegionScheme1FromBackend();
      runRegionScheme1AutoGenerated = Boolean(generated);
      if (generated) {
        schemes = await listRunRegionSchemesFromBackend();
      }
    }
    runRegionSchemeCache = { items: schemes, loading: false };
    const savedSchemeNo = String(localStorage.getItem(RUN_REGION_SCHEME_SELECTED_KEY) || "").trim();
    const current = getSelectedRunRegionSchemeNo() || savedSchemeNo;
    const active = schemes.find((item) => item.schemeNo === current) || schemes.find((item) => item.enabled) || schemes[0] || null;
    setSelectedRunRegionSchemeNo(active?.schemeNo || "");
    let regions = await listRunRegionsFromBackend(getSelectedRunRegionSchemeNo());
    if (!runRegionScheme1AutoGenerated && String(active?.schemeNo || "") === "1" && !regions.length) {
      const generated = await generateRunRegionScheme1FromBackend();
      runRegionScheme1AutoGenerated = Boolean(generated);
      if (generated) {
        regions = await listRunRegionsFromBackend("1");
      }
    }
    runRegionCache = { items: regions, loading: false };
    ensureRunRegionCheckedSelection(regions);
    runRegionStorePoints = points;
    renderRunRegionList();
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
  if (!getSelectedRunRegionSchemeNo()) {
    setRunRegionStatus("璇峰厛閫夋嫨鍒嗗尯鏂规鍙枫€?);
    return;
  }
  const mapReady = await ensureRunRegionMapReady();
  if (!mapReady) {
    setRunRegionStatus("鍦板浘涓嶅彲鐢紝鏃犳硶缁樺埗鍥存爮銆?);
    return;
  }
  stopRunRegionDrawing(true);
  runRegionEditingId = null;
  const nameInput = document.getElementById("runRegionNameInput");
  if (nameInput) nameInput.focus();
  if (!window.AMap?.MouseTool) {
    setRunRegionStatus("缁樺浘鎻掍欢鏈姞杞斤紝璇峰埛鏂伴〉闈㈠悗閲嶈瘯銆?);
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
    if (runRegionMouseTool) {
      try { runRegionMouseTool.close(true); } catch {}
      runRegionMouseTool = null;
    }
    if (runRegionDraftPolygon && window.AMap?.PolygonEditor) {
      runRegionPolygonEditor = new window.AMap.PolygonEditor(runRegionMap, runRegionDraftPolygon);
      try { runRegionPolygonEditor.open(); } catch {}
    }
    const saveBtn = document.getElementById("runRegionSaveBtn");
    if (saveBtn) saveBtn.disabled = false;
    setRunRegionStatus("鍥存爮宸茬粯鍒讹紝鍙嫋鎷界紪杈戝悗淇濆瓨銆?);
  });
  setRunRegionStatus("璇峰湪鍦板浘涓婄偣鍑荤粯鍒惰繍琛屽尯鍥存爮銆?);
}

async function editRunRegion(regionId) {
  const id = String(regionId || "");
  if (!id) return;
  const item = (runRegionCache.items || []).find((x) => String(x.id) === id);
  if (!item) return;
  const mapReady = await ensureRunRegionMapReady();
  if (!mapReady) return;
  stopRunRegionDrawing(false);
  const polygon = runRegionPolygons.get(id);
  if (!polygon) return;
  runRegionEditingId = Number(id);
  runRegionDraftPolygon = polygon;
  if (window.AMap?.PolygonEditor) {
    runRegionPolygonEditor = new window.AMap.PolygonEditor(runRegionMap, polygon);
    try { runRegionPolygonEditor.open(); } catch {}
  }
  const nameInput = document.getElementById("runRegionNameInput");
  if (nameInput) nameInput.value = item.name || "";
  const saveBtn = document.getElementById("runRegionSaveBtn");
  if (saveBtn) saveBtn.disabled = false;
  try { runRegionMap.setFitView([polygon], false, [40, 40, 40, 40], 17); } catch {}
  setRunRegionStatus(`姝ｅ湪缂栬緫杩愯鍖猴細${item.name || item.id}`);
}

async function cancelRunRegionEdit() {
  stopRunRegionDrawing(true);
  const nameInput = document.getElementById("runRegionNameInput");
  if (nameInput) nameInput.value = "";
  setRunRegionStatus("宸插彇娑堢紪杈戙€?);
  await refreshRunRegionData(true);
}

async function saveRunRegionDraft() {
  const schemeNo = getSelectedRunRegionSchemeNo();
  if (!schemeNo) {
    setRunRegionStatus("璇峰厛閫夋嫨鍒嗗尯鏂规鍙枫€?);
    return;
  }
  const nameInput = document.getElementById("runRegionNameInput");
  const name = String(nameInput?.value || "").trim();
  if (!name) {
    setRunRegionStatus("璇峰厛濉啓杩愯鍖哄悕绉般€?);
    return;
  }
  const path = normalizeRunRegionPath(getPathFromRunRegionPolygon(runRegionDraftPolygon));
  if (path.length < 3) {
    setRunRegionStatus("鍥存爮鐐逛綅涓嶈冻锛岃嚦灏戦渶瑕?涓偣銆?);
    return;
  }
  let available = await ensureGaBackendAvailable();
  if (!available) available = await ensureGaBackendAvailable(true);
  if (!available) {
    setRunRegionStatus("鍚庣涓嶅彲鐢紝鏃犳硶淇濆瓨杩愯鍖恒€?);
    return;
  }
  let saved = null;
  if (runRegionEditingId) {
    saved = await updateRunRegionOnBackend({ id: runRegionEditingId, schemeNo, name, path });
  } else {
    saved = await createRunRegionOnBackend({ schemeNo, name, path });
  }
  if (!saved) {
    setRunRegionStatus("淇濆瓨澶辫触锛岃閲嶈瘯銆?);
    return;
  }
  stopRunRegionDrawing(true);
  if (nameInput) nameInput.value = "";
  setRunRegionStatus("杩愯鍖哄凡淇濆瓨銆?);
  await refreshRunRegionData(true);
}

async function removeRunRegion(regionId) {
  const id = Number(regionId || 0);
  if (id <= 0) return;
  const ok = await deleteRunRegionOnBackend(id);
  if (!ok) {
    setRunRegionStatus("鍒犻櫎澶辫触锛岃閲嶈瘯銆?);
    return;
  }
  if (runRegionEditingId === id) {
    stopRunRegionDrawing(true);
  }
  setRunRegionStatus("杩愯鍖哄凡鍒犻櫎銆?);
  await refreshRunRegionData(true);
}

function bindRunRegionEvents() {
  const schemeSelect = document.getElementById("runRegionSchemeSelect");
  if (schemeSelect) {
    schemeSelect.addEventListener("change", () => {
      setSelectedRunRegionSchemeNo(schemeSelect.value || "");
      runRegionTargetRegionId = "all";
      runRegionCheckedRegionIds = new Set();
      void refreshRunRegionData(true);
    });
  }
  const solveSchemeSelect = document.getElementById("solveRegionSchemeSelect");
  if (solveSchemeSelect) {
    solveSchemeSelect.addEventListener("change", () => {
      setSelectedRunRegionSchemeNo(solveSchemeSelect.value || "");
      runRegionTargetRegionId = "all";
      runRegionCheckedRegionIds = new Set();
      void refreshRunRegionData(true);
    });
  }
  document.getElementById("runRegionSchemeCreateBtn")?.addEventListener("click", async () => {
    const schemeNo = String(document.getElementById("runRegionSchemeNoInput")?.value || "").trim();
    const name = String(document.getElementById("runRegionSchemeNameInput")?.value || "").trim();
    const enabled = Boolean(document.getElementById("runRegionSchemeEnabledInput")?.checked);
    if (!schemeNo || !name) {
      setRunRegionStatus("璇峰～鍐欐柟妗堝彿鍜屾柟妗堝悕绉般€?);
      return;
    }
    const created = await createRunRegionSchemeOnBackend({ schemeNo, name, enabled });
    if (!created) {
      setRunRegionStatus("鍒嗗尯鏂规鏂板澶辫触锛岃閲嶈瘯銆?);
      return;
    }
    setSelectedRunRegionSchemeNo(created.schemeNo || schemeNo);
    setRunRegionStatus("鍒嗗尯鏂规宸叉柊澧炪€?);
    await refreshRunRegionData(true);
  });
  document.getElementById("runRegionSchemeUpdateBtn")?.addEventListener("click", async () => {
    const selected = (runRegionSchemeCache.items || []).find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
    if (!selected) {
      setRunRegionStatus("璇峰厛閫夋嫨瑕佹洿鏂扮殑鍒嗗尯鏂规銆?);
      return;
    }
    const name = String(document.getElementById("runRegionSchemeNameInput")?.value || "").trim();
    const enabled = Boolean(document.getElementById("runRegionSchemeEnabledInput")?.checked);
    if (!name) {
      setRunRegionStatus("璇峰～鍐欐柟妗堝悕绉般€?);
      return;
    }
    const updated = await updateRunRegionSchemeOnBackend({ id: selected.id, name, enabled });
    if (!updated) {
      setRunRegionStatus("鍒嗗尯鏂规鏇存柊澶辫触锛岃閲嶈瘯銆?);
      return;
    }
    setRunRegionStatus("鍒嗗尯鏂规宸叉洿鏂般€?);
    await refreshRunRegionData(true);
  });
  document.getElementById("runRegionSchemeDeleteBtn")?.addEventListener("click", async () => {
    const selected = (runRegionSchemeCache.items || []).find((item) => item.schemeNo === getSelectedRunRegionSchemeNo()) || null;
    if (!selected) {
      setRunRegionStatus("璇峰厛閫夋嫨瑕佸垹闄ょ殑鍒嗗尯鏂规銆?);
      return;
    }
    const ok = await deleteRunRegionSchemeOnBackend(selected.id);
    if (!ok) {
      setRunRegionStatus("鍒犻櫎鍒嗗尯鏂规澶辫触锛堝彲鑳戒粛鏈夊洿鏍忕粦瀹氾級銆?);
      return;
    }
    setSelectedRunRegionSchemeNo("");
    setRunRegionStatus("鍒嗗尯鏂规宸插垹闄ゃ€?);
    await refreshRunRegionData(true);
  });
  const modeSelect = document.getElementById("runRegionStoreVisibilityMode");
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
  if (targetRegionSelect) {
    runRegionTargetRegionId = String(targetRegionSelect.value || "all");
    targetRegionSelect.addEventListener("change", () => {
      runRegionTargetRegionId = String(targetRegionSelect.value || "all");
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
  document.getElementById("runRegionDrawBtn")?.addEventListener("click", () => {
    void startRunRegionDraw();
  });
  document.getElementById("runRegionSaveBtn")?.addEventListener("click", () => {
    void saveRunRegionDraft();
  });
  document.getElementById("runRegionCancelBtn")?.addEventListener("click", () => {
    void cancelRunRegionEdit();
  });
  document.getElementById("runRegionRefreshBtn")?.addEventListener("click", () => {
    void refreshRunRegionData(true);
  });
  document.getElementById("runRegionList")?.addEventListener("click", (event) => {
    const editBtn = event.target.closest(".run-region-edit-btn");
    if (editBtn) {
      void editRunRegion(editBtn.dataset.regionId);
      return;
    }
    const deleteBtn = event.target.closest(".run-region-delete-btn");
    if (deleteBtn) {
      void removeRunRegion(deleteBtn.dataset.regionId);
    }
  });
  document.getElementById("runRegionList")?.addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
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
    if (target.classList.contains("run-region-visibility-check")) {
      const regionId = String(target.dataset.regionId || "").trim();
      if (!regionId) return;
      if (target.checked) runRegionCheckedRegionIds.add(regionId);
      else runRegionCheckedRegionIds.delete(regionId);
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
  if (!date) return;
  if (archiveBackendCache.loading) return;
  archiveBackendCache.loading = true;
  try {
    const remote = await listRunArchivesFromBackend(date, page, pageSize);
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
  if (item && typeof item === "object") {
    const keys = ["storeId", "store_id", "id", "code", "shop_code", "shopCode", "storeCode"];
    for (const key of keys) {
      const value = item[key];
      if (value !== undefined && value !== null && String(value).trim()) return normalizeStoreCode(value);
    }
  }
  return normalizeStoreCode(item);
}

function buildGaBackendPayload(initialPlans, scenario, wave, randomSeed = 203) {
  const backendStrategyConfig = buildBackendStrategyConfig(state.strategyConfig);
  const requireNumberForBackend = (store, field) => {
    const value = Number(store?.[field]);
    if (!Number.isFinite(value)) {
      throw new Error(`missing_${field}:${store?.id || ""}:${wave?.waveId || ""}`);
    }
    return value;
  };
  const candidateStoreIds = (wave.storeList || []).filter((storeId) => {
    const store = scenario.storeMap.get(storeId);
    if (!store) return false;
    return isStoreCandidateForWaveRule(store, wave?.waveId || "");
  });
  const relevantIds = [DC.id, ...candidateStoreIds];
  const dist = {};
  relevantIds.forEach((fromId) => {
    dist[fromId] = {};
    relevantIds.forEach((toId) => {
      dist[fromId][toId] = Number(scenario?.dist?.[fromId]?.[toId] || 0);
    });
  });
  const stores = candidateStoreIds.map((storeId) => {
    const store = scenario.storeMap.get(storeId);
    if (!store) throw new Error(`missing_store:${storeId}:${wave?.waveId || ""}`);
    const timing = getStoreTimingForWave(store, wave, scenario.dispatchStartMin);
    return {
      id: store.id,
      name: store.name,
      boxes: requireNumberForBackend(store, "boxes"),
      // 鍚庣鎸夊綋鍓嶆尝娆″彛寰勬敹鏃堕棿绐楋紝閬垮厤璇敤闂ㄥ簵瀵硅薄涓婄殑璺ㄦ尝娆￠仐鐣欏瓧娈?
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
  const solveSpeedKmh = waveIdForSolve === "W3"
    ? Number(backendStrategyConfig.w3SpeedKmh || 48)
    : Number(backendStrategyConfig.defaultSpeedKmh || 38);
  const solveCapacity = Math.max(0.1, Number(backendStrategyConfig.maxSolveCapacity || 1));
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

function buildWaveOptimizerBackendPayload(initialPlans, scenario, wave, algorithmKey, randomSeed = 203) {
  return {
    ...buildGaBackendPayload(initialPlans, scenario, wave, randomSeed),
    algorithmKey,
  };
}

function sanitizeBackendRoutes(routes = [], allowedStoreSet = new Set()) {
  const seen = new Set();
  return (Array.isArray(routes) ? routes : [])
    .map((route) => (Array.isArray(route) ? route : []))
    .map((route) => {
      const removed = [];
      const sanitized = route.filter((storeId) => {
        const normalized = String(storeId ?? "").trim();
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
        return true;
      }).map((storeId) => String(storeId).trim());
      if (removed.length) console.log("[W3] sanitize杩囨护:", removed);
      return sanitized;
    })
    .filter((route) => route.length);
}

function buildAllowedStoreSetForBackendRebuild(wave, strategyAudit = null, scenario = null) {
  const storeMap = scenario?.storeMap instanceof Map ? scenario.storeMap : null;
  const base = new Set((wave?.storeList || []).map((storeId) => String(storeId).trim()).filter((storeId) => {
    if (!storeId) return false;
    if (!storeMap) return true;
    const store = storeMap.get(storeId);
    if (!store) return false;
    return isStoreCandidateForWaveRule(store, wave?.waveId || "");
  }));
  if (!strategyAudit || typeof strategyAudit !== "object") return base;
  const removed = new Set([
    ...(Array.isArray(strategyAudit.filteredZeroLoadStoreIds) ? strategyAudit.filteredZeroLoadStoreIds : []),
    ...(Array.isArray(strategyAudit.filteredWaveScopeStoreIds) ? strategyAudit.filteredWaveScopeStoreIds : []),
  ].map((storeId) => String(storeId || "").trim()).filter(Boolean));
  if (!removed.size) return base;
  const filtered = new Set();
  base.forEach((storeId) => {
    if (!removed.has(storeId)) filtered.add(storeId);
  });
  if (String(wave?.waveId || "").trim().toUpperCase() === "W3") {
    console.log("[W3] allowedStoreSet size:", filtered.size);
    console.log("[W3] allowedStoreSet sample:", [...filtered].slice(0, 20));
  }
  return filtered;
}

function collectAssignedStoreSetFromRouteState(routeState = [], allowedStoreSet = null) {
  const assigned = new Set();
  (Array.isArray(routeState) ? routeState : []).forEach((stateItem) => {
    (stateItem?.routes || []).forEach((route) => {
      (route || []).forEach((sid) => {
        const nsid = String(sid ?? "").trim();
        if (!nsid) return;
        if (allowedStoreSet instanceof Set && !allowedStoreSet.has(nsid)) return;
        assigned.add(nsid);
      });
    });
  });
  return assigned;
}

function collectAssignedStoreSetFromPlans(plans = [], allowedStoreSet = null) {
  const assigned = new Set();
  (Array.isArray(plans) ? plans : []).forEach((plan) => {
    (plan?.trips || []).forEach((trip) => {
      (trip?.route || []).forEach((sid) => {
        const nsid = String(sid ?? "").trim();
        if (!nsid) return;
        if (allowedStoreSet instanceof Set && !allowedStoreSet.has(nsid)) return;
        assigned.add(nsid);
      });
    });
  });
  return assigned;
}

function computePendingStoreIds(candidateStoreSet = new Set(), assignedStoreSet = new Set()) {
  return [...candidateStoreSet].filter((storeId) => !assignedStoreSet.has(storeId));
}

function reportRebuildSetStats(waveId, stage, candidateStoreSet, assignedStoreSet, pendingStoreIds) {
  const waveTag = String(waveId || "-");
  const candidateCount = candidateStoreSet instanceof Set ? candidateStoreSet.size : 0;
  const assignedCount = assignedStoreSet instanceof Set ? assignedStoreSet.size : 0;
  const pendingCount = Array.isArray(pendingStoreIds) ? pendingStoreIds.length : 0;
  reportRelayStageProgress(`闆嗗悎鏍稿锛?{waveTag}/${stage}锛夛細candidate=${candidateCount}锛宎ssigned=${assignedCount}锛宲ending=${pendingCount}銆俙);
}

function rebuildWavePlansFromBackendState(bestState, initialPlans, scenario, wave, strategyAudit = null) {
  if (!Array.isArray(bestState) || !bestState.length) return null;
  const normalizePlateNo = (plateNo) => String(plateNo || "").replace(/\s+/g, "").trim().toUpperCase();
  const backendByPlate = new Map(
    bestState
      .map((item) => ({ key: normalizePlateNo(item?.plateNo), item }))
      .filter((row) => row.key)
      .map((row) => [row.key, row.item]),
  );
  const allowedStoreSet = buildAllowedStoreSetForBackendRebuild(wave, strategyAudit, scenario);
  if (String(wave?.waveId || "").trim().toUpperCase() === "W3" && (!strategyAudit || typeof strategyAudit !== "object")) {
    console.log("[W3] allowedStoreSet size:", allowedStoreSet.size);
    console.log("[W3] allowedStoreSet sample:", [...allowedStoreSet].slice(0, 20));
  }
  const baselineState = cloneWaveRouteState(initialPlans);
  const fallbackFailures = [];
  let routeState = initialPlans.map((plan, index) => {
    const planPlate = normalizePlateNo(plan?.vehicle?.plateNo);
    const backendItem = (planPlate && backendByPlate.get(planPlate)) || {};
    if (!backendItem?.plateNo) {
      const hasBaselineRoutes = Array.isArray(baselineState[index]?.routes) && baselineState[index].routes.length > 0;
      if (hasBaselineRoutes) {
        reportRelayStageProgress(`鍚庣鍥炲～鎻愮ず锛?{wave?.waveId || "-"}锛夛細杞︾墝 ${plan?.vehicle?.plateNo || "-"} 鏈湪 bestState 鍖归厤锛屽洖閫€鍩虹嚎绾胯矾銆俙);
      }
    }
    const sanitizedRoutes = Array.isArray(backendItem.routes)
      ? sanitizeBackendRoutes(backendItem.routes, allowedStoreSet)
      : baselineState[index].routes.map((route) => [...route]);
    if (String(wave?.waveId || "").trim().toUpperCase() === "W3") {
      console.log("[W3] 杞﹁締", backendItem.plateNo || plan?.vehicle?.plateNo || "-", "鍘熷routes:", backendItem.routes);
      console.log("[W3] 杞﹁締", backendItem.plateNo || plan?.vehicle?.plateNo || "-", "sanitized鍚?", sanitizedRoutes);
    }
    const backendSpeed = Number(backendItem.speed);
    const vehicle = {
      ...plan.vehicle,
      speed: Number.isFinite(backendSpeed) && backendSpeed > 0 ? backendSpeed : plan.vehicle.speed,
    };
    return {
      vehicle,
      priorRegularDistance: Number(backendItem.priorRegularDistance ?? plan.priorRegularDistance ?? 0),
      priorWaveCount: Number(backendItem.priorWaveCount ?? plan.priorWaveCount ?? 0),
      earliestDepartureMin: Number(backendItem.earliestDepartureMin ?? plan.earliestDepartureMin ?? wave.startMin),
      routes: sanitizedRoutes,
    };
  });
  const seen = new Set();
  routeState.forEach((stateItem) => {
    stateItem.routes = (stateItem.routes || [])
      .map((route) => route.filter((storeId) => {
        if (!allowedStoreSet.has(storeId) || seen.has(storeId)) return false;
        seen.add(storeId);
        return true;
      }))
      .filter((route) => route.length);
  });
  routeState = routeState.map((stateItem, index) => {
    if (rebuildSingleStatePlan(stateItem, scenario, wave)) return stateItem;
    const plateNo = String(stateItem?.vehicle?.plateNo || baselineState[index]?.vehicle?.plateNo || "").trim();
    fallbackFailures.push(plateNo || `vehicle#${index + 1}`);
    return {
      ...baselineState[index],
      routes: sanitizeBackendRoutes(baselineState[index].routes.map((route) => [...route]), allowedStoreSet),
    };
  });
  let preRebuildAssigned = collectAssignedStoreSetFromRouteState(routeState, allowedStoreSet);
  let preRebuildPending = computePendingStoreIds(allowedStoreSet, preRebuildAssigned);
  reportRebuildSetStats(wave?.waveId || "-", "閲嶅缓鍓?, allowedStoreSet, preRebuildAssigned, preRebuildPending);

  let preRebuildRound = 0;
  while (preRebuildPending.length && preRebuildRound < 6) {
    const repaired = insertStoresIntoRouteState(routeState, preRebuildPending, scenario, wave, { candidateVehicleLimit: 8 });
    if (!repaired) {
      reportRelayStageProgress(`閲嶅缓琛ユ帓绗?{preRebuildRound + 1}杞棤鍙鎻掑叆浣嶏紝浠嶆湁 ${preRebuildPending.length} 瀹跺緟鎺掋€俙);
      break;
    }
    routeState = repaired;
    const nextAssigned = collectAssignedStoreSetFromRouteState(routeState, allowedStoreSet);
    const nextPending = computePendingStoreIds(allowedStoreSet, nextAssigned);
    reportRebuildSetStats(wave?.waveId || "-", `閲嶅缓琛ユ帓绗?{preRebuildRound + 1}杞悗`, allowedStoreSet, nextAssigned, nextPending);
    if (nextPending.length >= preRebuildPending.length) {
      reportRelayStageProgress(`閲嶅缓琛ユ帓绗?{preRebuildRound + 1}杞棤杩涘睍锛堝緟鎺?${preRebuildPending.length}鈫?{nextPending.length}锛夛紝鍋滄缁х画琛ユ帓銆俙);
      preRebuildAssigned = nextAssigned;
      preRebuildPending = nextPending;
      break;
    }
    preRebuildAssigned = nextAssigned;
    preRebuildPending = nextPending;
    preRebuildRound += 1;
  }

  let rebuiltPlans = rebuildWavePlansFromState(routeState, scenario, wave);
  if (!rebuiltPlans) {
    const fallbackNote = fallbackFailures.length
      ? `锛岄噸寤哄け璐ュ洖閫€杞﹁締 ${fallbackFailures.length} 鍙帮紙绀轰緥锛?{fallbackFailures.slice(0, 6).join("銆?)}锛塦
      : "";
    reportRelayStageProgress(`鍚庣缁撴灉閲嶅缓澶辫触锛歛llowed=${allowedStoreSet.size}锛屽凡鍒嗛厤=${preRebuildAssigned.size}锛屽緟琛?${preRebuildPending.length}${fallbackNote}銆俙);
    return null;
  }

  // 浜屾琛ユ帓锛氬鐞嗏€滈噸寤洪檷绾х疆绌哄悗鈥濆啀娆℃帀鍑烘潵鐨勯棬搴?  let secondRound = 0;
  while (secondRound < 6) {
    const secondAssigned = collectAssignedStoreSetFromPlans(rebuiltPlans, allowedStoreSet);
    const secondPending = computePendingStoreIds(allowedStoreSet, secondAssigned);
    reportRebuildSetStats(wave?.waveId || "-", secondRound === 0 ? "浜屾琛ユ帓鍓? : `浜屾琛ユ帓绗?{secondRound}杞墠`, allowedStoreSet, secondAssigned, secondPending);
    if (!secondPending.length) break;

    const retryState = cloneWaveRouteState(rebuiltPlans);
    const secondRepaired = insertStoresIntoRouteState(retryState, secondPending, scenario, wave, { candidateVehicleLimit: 8 });
    if (!secondRepaired) {
      reportRelayStageProgress(`浜屾琛ユ帓绗?{secondRound + 1}杞棤鍙鎻掑叆浣嶏紝浠嶆湁 ${secondPending.length} 瀹跺緟鎺掋€俙);
      break;
    }
    const secondPlans = rebuildWavePlansFromState(secondRepaired, scenario, wave);
    if (!secondPlans) {
      reportRelayStageProgress(`浜屾琛ユ帓绗?{secondRound + 1}杞湭閫氳繃閲嶅缓鏍￠獙锛屼粛鏈?${secondPending.length} 瀹跺緟鎺掋€俙);
      break;
    }
    const afterAssigned = collectAssignedStoreSetFromPlans(secondPlans, allowedStoreSet);
    const afterPending = computePendingStoreIds(allowedStoreSet, afterAssigned);
    reportRebuildSetStats(wave?.waveId || "-", `浜屾琛ユ帓绗?{secondRound + 1}杞悗`, allowedStoreSet, afterAssigned, afterPending);
    rebuiltPlans = secondPlans;
    if (afterPending.length >= secondPending.length) {
      reportRelayStageProgress(`浜屾琛ユ帓绗?{secondRound + 1}杞棤杩涘睍锛堝緟鎺?${secondPending.length}鈫?{afterPending.length}锛夛紝鍋滄缁х画琛ユ帓銆俙);
      break;
    }
    secondRound += 1;
  }
  return rebuiltPlans;
}

async function tryOptimizeWaveWithPythonGA(initialPlans, scenario, wave, randomSeed = 203) {
  let available = await ensureGaBackendAvailable();
  if (!available) {
    await sleep(180);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) {
    reportRelayStageProgress(`GA Python 鍚庡彴褰撳墠涓嶅彲杈撅紝${wave.waveId} 鍏堝洖閫€鍒板墠绔仐浼犵畻娉曘€俙);
    return null;
  }
  const payload = buildGaBackendPayload(initialPlans, scenario, wave, randomSeed);
  try {
    reportRelayStageProgress(`GA ${wave.waveId} 姝ｅ湪鍒囧埌 Python 鍚庡彴姹傝В锛屾祻瑙堝櫒涓荤嚎绋嬩笉鍐嶇‖鎵涜繖涓€妫掋€俙);
    let response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/ga-optimize-wave`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }, 600000);
    if (!response.ok) {
      await sleep(220);
      response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/ga-optimize-wave`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }, 600000);
    }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    reportStrategyAuditToRelayConsole(data.strategyAudit, "閬椾紶绠楁硶锛圙A锛?);
    reportBackendUnscheduledToRelayConsole(data.unscheduledStores, "閬椾紶绠楁硶锛圙A锛?, wave.waveId || "-");
    const plans = rebuildWavePlansFromBackendState(data.bestState || [], initialPlans, scenario, wave, data.strategyAudit || null);
    if (Array.isArray(data.traceLog) && data.traceLog.length) {
      data.traceLog.forEach((event) => {
        const message = (lang() === "ja" ? event?.textJa : event?.textZh) || event?.textZh || event?.textJa || "";
        if (message) reportRelayStageProgress(message);
      });
    }
    if (!plans) {
      const backendRouteCount = Array.isArray(data.bestState)
        ? data.bestState.reduce((sum, item) => sum + ((item?.routes || []).length || 0), 0)
        : 0;
      throw new Error(`Backend state rebuild failed (vehicles=${Array.isArray(data.bestState) ? data.bestState.length : 0}, routes=${backendRouteCount})`);
    }
    reportRelayStageProgress(`GA ${wave.waveId} 宸蹭粠 Python 鍚庡彴鎴愬姛鎺ュ洖锛屽綋鍓嶆尝娆′笉鐢ㄥ啀鍥為€€鍓嶇閬椾紶绠楁硶銆俙);
    return {
      plans,
      traceLog: Array.isArray(data.traceLog) ? data.traceLog : [],
      unscheduledStores: Array.isArray(data.unscheduledStores) ? data.unscheduledStores : [],
    };
  } catch (error) {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
    reportRelayStageProgress(`GA Python 鍚庡彴鏈疆澶辫触锛?{error?.message || "unknown"}锛夛紝鏈疆宸蹭腑姝紝涓嶅啀鍥為€€鍓嶇銆俙);
    throw new Error(`GA_BACKEND_REQUIRED:${error?.message || "unknown"}`);
  }
}

async function tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, algorithmKey, randomSeed = 203) {
  if (STRICT_ALGO_TRUTH_MODE) {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} 宸插垏鍥炲墠绔湡瀹炵畻娉曟眰瑙ｏ紙鐪熷疄鎬т紭鍏堟ā寮忥級锛屾湰杞笉璧?Python 杩戜技鍚庣銆俙);
    return null;
  }
  let available = await ensureGaBackendAvailable();
  if (!available) {
    await sleep(180);
    available = await ensureGaBackendAvailable(true);
  }
  if (!available) {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} Python 鍚庡彴褰撳墠涓嶅彲杈俱€俙);
    if (BACKEND_ONLY_REAL_ALGOS.has(String(algorithmKey || ""))) {
      throw new Error(`${algorithmKey}_BACKEND_REQUIRED:unreachable`);
    }
    return null;
  }
  const payload = buildWaveOptimizerBackendPayload(initialPlans, scenario, wave, algorithmKey, randomSeed);
  try {
    reportRelayStageProgress(`${algoLabel(algorithmKey)} ${wave.waveId} 姝ｅ湪鍒囧埌 Python 鍚庡彴姹傝В锛屾祻瑙堝櫒涓荤嚎绋嬩笉鍐嶇‖鎵涜繖涓€妫掋€俙);
    let response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wave-optimize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }, 600000);
    if (!response.ok) {
      await sleep(220);
      response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wave-optimize`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }, 600000);
    }
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    reportStrategyAuditToRelayConsole(data.strategyAudit, algoLabel(algorithmKey));
    reportBackendUnscheduledToRelayConsole(data.unscheduledStores, algoLabel(algorithmKey), wave.waveId || "-");
    const plans = rebuildWavePlansFromBackendState(data.bestState || [], initialPlans, scenario, wave, data.strategyAudit || null);
    if (Array.isArray(data.traceLog) && data.traceLog.length) {
      data.traceLog.forEach((event) => {
        const message = (lang() === "ja" ? event?.textJa : event?.textZh) || event?.textZh || event?.textJa || "";
        if (message) reportRelayStageProgress(message);
      });
    }
    if (["vrptw", "savings"].includes(String(algorithmKey || "").trim())) {
      const iterTag = `${String(algorithmKey || "").trim()}-python-iteration`;
      const hasConstructiveIteration = Array.isArray(data.traceLog)
        && data.traceLog.some((event) => String(event?.stage || "").trim() === iterTag);
      if (!hasConstructiveIteration) {
        throw new Error(`${algorithmKey}_BACKEND_STALE_OR_INVALID_TRACE`);
      }
    }
    if (!plans) throw new Error("Backend state rebuild failed");
    reportRelayStageProgress(`${algoLabel(algorithmKey)} ${wave.waveId} 宸蹭粠 Python 鍚庡彴鎴愬姛鎺ュ洖锛屽綋鍓嶆尝娆′笉鐢ㄥ啀鍥為€€鍓嶇 ${algoLabel(algorithmKey)}銆俙);
    return {
      plans,
      traceLog: Array.isArray(data.traceLog) ? data.traceLog : [],
      unscheduledStores: Array.isArray(data.unscheduledStores) ? data.unscheduledStores : [],
    };
  } catch (error) {
    gaBackendHealth = { available: false, checkedAt: Date.now() };
    const raw = String(error?.message || "unknown");
    const staleHint = raw.includes("_BACKEND_STALE_OR_INVALID_TRACE")
      ? "鍚庣杩斿洖鐨勬棩蹇楃粨鏋勬棤鏁堬紙澶ф鐜囨槸鏃ц繘绋嬫湭閲嶅惎锛夛紝璇烽噸鍚?GA 鍚庡彴鍚庨噸璇曘€?
      : raw;
    reportRelayStageProgress(`${algoLabel(algorithmKey)} Python 鍚庡彴鏈疆澶辫触锛?{staleHint}锛夈€俙);
    if (BACKEND_ONLY_REAL_ALGOS.has(String(algorithmKey || ""))) {
      throw new Error(`${algorithmKey}_BACKEND_REQUIRED:${error?.message || "unknown"}`);
    }
    return null;
  }
}

async function optimizeWaveWithGA(initialPlans, scenario, wave, randomSeed = 203) {
  const backendResult = await tryOptimizeWaveWithPythonGA(initialPlans, scenario, wave, randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now());
  const random = createSeededRandom(randomSeed);
  const initialState = cloneWaveRouteState(initialPlans);
  const traceLog = [];
  const isCompareMode = (state.settings.solveStrategy || "manual") === "compare";
  const populationSize = isCompareMode ? 6 : 12;
  const generations = isCompareMode ? 8 : 22;
  const eliteCount = isCompareMode ? 2 : 3;
  const stagnationLimit = isCompareMode ? 3 : 6;
  const improvementThreshold = isCompareMode ? 0.2 : 0.1;
  const seedImproveRounds = isCompareMode ? 0 : 2;
  const offspringImproveRounds = isCompareMode ? 0 : 2;
  const immigrantImproveRounds = isCompareMode ? 0 : 1;
  const population = [];
  const seen = new Set();
  const profile = { seedMs: 0, crossoverMs: 0, offspringEvalMs: 0, immigrantMs: 0 };

  function addIndividual(state) {
    const signature = hashRouteState(state);
    if (seen.has(signature)) return false;
    const improved = localImproveState(state, scenario, wave, random, seedImproveRounds) || evaluateRouteState(state, scenario, wave);
    if (!improved) return false;
    seen.add(signature);
    population.push({ state: cloneWaveRouteState(improved.plans), plans: improved.plans, cost: improved.cost, signature });
    return true;
  }

  const seedStart = now();
  addIndividual(initialState);
  for (let guard = 0; guard < (isCompareMode ? 24 : 80) && population.length < populationSize; guard += 1) {
    addIndividual(mutateRouteState(initialState, scenario, wave, random, 1 + Math.floor(random() * 4)));
  }
  profile.seedMs += now() - seedStart;
  if (!population.length) return { plans: initialPlans, traceLog };

  pushTraceEvent(traceLog, {
    algorithmKey: "ga",
    scope: "wave",
    waveId: wave.waveId,
    stage: "ga-start",
    bestCost: Math.min(...population.map((item) => item.cost)),
    textZh: isCompareMode ? `閬椾紶绠楁硶宸茬敓鎴愬姣旂缇?${population.length} 涓釜浣擄紝鎸夎交閲忓姣旈绠楀紑濮嬪仛閫夋嫨 / 浜ゅ弶 / 鍙樺紓銆俙 : `閬椾紶绠楁硶宸茬敓鎴愬垵濮嬬缇?${population.length} 涓釜浣擄紝寮€濮嬪仛閫夋嫨 / 浜ゅ弶 / 鍙樺紓銆俙,
    textJa: isCompareMode ? `閬轰紳鐨勩偄銉偞銉偤銉犮伅姣旇純鐢ㄣ伄杌介噺闆嗗洠 ${population.length} 鍊嬩綋銈掔敓鎴愩仐銆侀伕鎶?/ 浜ゅ弶 / 澶夌暟銈掗枊濮嬨仐銇俱仚銆俙 : `閬轰紳鐨勩偄銉偞銉偤銉犮伅鍒濇湡闆嗗洠 ${population.length} 鍊嬩綋銈掔敓鎴愩仐銆侀伕鎶?/ 浜ゅ弶 / 澶夌暟銈掗枊濮嬨仐銇俱仚銆俙,
  });
  if (isCompareMode) {
    pushTraceEvent(traceLog, {
      algorithmKey: "ga",
      scope: "wave",
      waveId: wave.waveId,
      stage: "ga-profile-seed",
      textZh: `GA 鎬ц兘鍓栨瀽宸插紑鍚細鍒濆绉嶇兢闃舵鑰楁椂绾?${(profile.seedMs / 1000).toFixed(2)} 绉掋€俙,
      textJa: `GA 銇€ц兘銉椼儹銉曘偂銈ゃ儷銈掓湁鍔瑰寲銇椼伨銇椼仧銆傚垵鏈熼泦鍥ｃ伄鎵€瑕佹檪闁撱伅绱?${(profile.seedMs / 1000).toFixed(2)} 绉掋仹銇欍€俙,
    });
    reportRelayStageProgress(`GA 鍓栨瀽鍚姩锛?{wave.waveId} 鍒濆绉嶇兢闃舵鑰楁椂绾?${(profile.seedMs / 1000).toFixed(2)} 绉掋€俙);
  }

  let bestCostSeen = Infinity;
  let stagnantGenerations = 0;
  for (let generation = 0; generation < generations; generation += 1) {
    const generationStart = now();
    let generationCrossoverMs = 0;
    let generationOffspringEvalMs = 0;
    let generationImmigrantMs = 0;
    if (generation && generation % 2 === 0) await cooperativeYield();
    population.sort((a, b) => a.cost - b.cost);
    const nextPopulation = population.slice(0, eliteCount).map((item) => ({ ...item, state: cloneWaveRouteState(item.state), plans: item.plans.map((plan) => clone(plan)) }));
    let offspringAttempts = 0;
    const maxOffspringAttempts = isCompareMode ? populationSize * 18 : populationSize * 36;
    while (nextPopulation.length < populationSize && offspringAttempts < maxOffspringAttempts) {
      offspringAttempts += 1;
      const parentA = tournamentSelect(population, random, 3);
      const parentB = tournamentSelect(population, random, 3);
      const crossoverStart = now();
      let childState = crossoverRouteStates(parentA.state, parentB.state, scenario, wave, random);
      if (!childState) childState = mutateRouteState(parentA.state, scenario, wave, random, 2);
      if (random() < 0.85) childState = mutateRouteState(childState, scenario, wave, random, random() < 0.6 ? 1 : 2);
      const crossoverSpent = now() - crossoverStart;
      generationCrossoverMs += crossoverSpent;
      profile.crossoverMs += crossoverSpent;
      const evalStart = now();
      const improved = localImproveState(childState, scenario, wave, random, offspringImproveRounds) || evaluateRouteState(childState, scenario, wave);
      const evalSpent = now() - evalStart;
      generationOffspringEvalMs += evalSpent;
      profile.offspringEvalMs += evalSpent;
      if (!improved) continue;
      nextPopulation.push({
        state: cloneWaveRouteState(improved.plans),
        plans: improved.plans,
        cost: improved.cost,
        signature: hashRouteState(childState),
      });
    }
    if (nextPopulation.length < populationSize) {
      const fallbackSource = nextPopulation[0] || population[0];
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
        textZh: `绗?${generation + 1} 浠ｅ悗浠ｇ敓鎴愬皾璇曡揪鍒颁笂闄?${maxOffspringAttempts} 娆★紝浠嶆湭濉弧绉嶇兢锛屾湰浠ｅ墿浣欎釜浣撴敼鐢ㄥ綋鍓嶆渶浼樹釜浣撹ˉ浣嶏紝閬垮厤闄峰叆鏃犱紤姝㈠皾璇曘€俙,
        textJa: `绗?${generation + 1} 涓栦唬銇с伅瀛愬€嬩綋鐢熸垚銇│琛屻亴涓婇檺 ${maxOffspringAttempts} 鍥炪伀閬斻仐銇︺倐闆嗗洠銈掑煁銈併亶銈屻仾銇嬨仯銇熴仧銈併€佹畫銈娿伅鐝炬檪鐐广伄鏈€鑹€嬩綋銇ц瀹屻仐銆佺劇闄愯│琛屻倰闃层亷銇俱仚銆俙,
      });
      if (isCompareMode) {
        reportRelayStageProgress(`GA ${wave.waveId} 绗?${generation + 1} 浠ｅ瓙浠ｇ敓鎴愬凡灏濊瘯 ${maxOffspringAttempts} 娆′粛鏈～婊＄缇わ紝宸叉敼鐢ㄥ綋鍓嶆渶浼樹釜浣撹ˉ浣嶏紝閬垮厤鍗℃銆俙);
      }
    }
    if (generation % 5 === 4 && population.length) {
      const immigrantStart = now();
      const immigrant = mutateRouteState(population[0].state, scenario, wave, random, 3);
      const immigrantEval = localImproveState(immigrant, scenario, wave, random, immigrantImproveRounds) || evaluateRouteState(immigrant, scenario, wave);
      const immigrantSpent = now() - immigrantStart;
      generationImmigrantMs += immigrantSpent;
      profile.immigrantMs += immigrantSpent;
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
        textZh: `绗?${generation + 1} 浠ｅ畬鎴愰€夋嫨銆佷氦鍙変笌鍙樺紓锛屽綋鍓嶇缇ゆ渶浼樻垚鏈?${generationBest.toFixed(1)}銆俙,
        textJa: `${generation + 1} 涓栦唬銇ч伕鎶炪兓浜ゅ弶銉诲鐣般倰瀹屼簡銇椼€佺従鍦ㄣ伄闆嗗洠鏈€鑹偝銈广儓銇?${generationBest.toFixed(1)} 銇с仚銆俙,
      });
    }
    if (isCompareMode) {
      const generationTotalMs = now() - generationStart;
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-profile-generation",
        generation,
        bestCost: generationBest,
        textZh: `GA 绗?${generation + 1} 浠ｈ€楁椂绾?${(generationTotalMs / 1000).toFixed(2)} 绉掞紝鍏朵腑浜ゅ弶/淇 ${(generationCrossoverMs / 1000).toFixed(2)} 绉掞紝瀛愪唬璇勪及 ${(generationOffspringEvalMs / 1000).toFixed(2)} 绉掞紝绉绘皯 ${(generationImmigrantMs / 1000).toFixed(2)} 绉掋€俙,
        textJa: `GA 绗?${generation + 1} 涓栦唬銇墍瑕佹檪闁撱伅绱?${(generationTotalMs / 1000).toFixed(2)} 绉掋仹銆佸唴瑷炽伅浜ゅ弶/淇京 ${(generationCrossoverMs / 1000).toFixed(2)} 绉掋€佸瓙浠ｈ渚?${(generationOffspringEvalMs / 1000).toFixed(2)} 绉掋€佺Щ姘?${(generationImmigrantMs / 1000).toFixed(2)} 绉掋仹銇欍€俙,
      });
      reportRelayStageProgress(`GA ${wave.waveId} 绗?${generation + 1} 浠ｈ€楁椂绾?${(generationTotalMs / 1000).toFixed(2)} 绉掞紝鍏朵腑浜ゅ弶/淇 ${(generationCrossoverMs / 1000).toFixed(2)} 绉掞紝瀛愪唬璇勪及 ${(generationOffspringEvalMs / 1000).toFixed(2)} 绉掞紝绉绘皯 ${(generationImmigrantMs / 1000).toFixed(2)} 绉掋€傚綋鍓嶆渶浼樹唬浠?${generationBest.toFixed(1)}銆俙);
    }
    if (isCompareMode && stagnantGenerations >= stagnationLimit) {
      pushTraceEvent(traceLog, {
        algorithmKey: "ga",
        scope: "wave",
        waveId: wave.waveId,
        stage: "ga-early-stop",
        generation,
        bestCost: generationBest,
        textZh: `杩炵画 ${stagnationLimit} 浠ｆ彁鍗囦笉瓒?${improvementThreshold.toFixed(1)}锛屽姣旀ā寮忎笅鎻愬墠鏀跺伐锛岄伩鍏嶇户缁┖杞€俙,
        textJa: `${stagnationLimit} 涓栦唬閫ｇ稓銇ф敼鍠勫箙銇?${improvementThreshold.toFixed(1)} 鏈簚銇仧銈併€佹瘮杓冦儮銉笺儔銇с伅鏃┿倎銇墦銇″垏銇ｃ仸绌鸿虎銈掗槻銇庛伨銇欍€俙,
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
    textZh: `閬椾紶绠楁硶缁撴潫锛屾渶缁堢缇ゆ渶浼樻垚鏈?${population[0].cost.toFixed(1)}銆俙,
    textJa: `閬轰紳鐨勩偄銉偞銉偤銉犮亴绲備簡銇椼€佹渶绲傞泦鍥ｃ伄鏈€鑹偝銈广儓銇?${population[0].cost.toFixed(1)} 銇с仚銆俙,
  });
  if (isCompareMode) {
    pushTraceEvent(traceLog, {
      algorithmKey: "ga",
      scope: "wave",
      waveId: wave.waveId,
      stage: "ga-profile-summary",
      bestCost: population[0].cost,
      textZh: `GA 鍓栨瀽姹囨€伙細鍒濆绉嶇兢 ${(profile.seedMs / 1000).toFixed(2)} 绉掞紝浜ゅ弶/淇 ${(profile.crossoverMs / 1000).toFixed(2)} 绉掞紝瀛愪唬璇勪及 ${(profile.offspringEvalMs / 1000).toFixed(2)} 绉掞紝绉绘皯 ${(profile.immigrantMs / 1000).toFixed(2)} 绉掋€俙,
      textJa: `GA 銉椼儹銉曘偂銈ゃ儷闆嗚▓锛氬垵鏈熼泦鍥?${(profile.seedMs / 1000).toFixed(2)} 绉掋€佷氦鍙?淇京 ${(profile.crossoverMs / 1000).toFixed(2)} 绉掋€佸瓙浠ｈ渚?${(profile.offspringEvalMs / 1000).toFixed(2)} 绉掋€佺Щ姘?${(profile.immigrantMs / 1000).toFixed(2)} 绉掋仹銇欍€俙,
    });
    reportRelayStageProgress(`GA 鍓栨瀽姹囨€伙細鍒濆绉嶇兢 ${(profile.seedMs / 1000).toFixed(2)} 绉掞紝浜ゅ弶/淇 ${(profile.crossoverMs / 1000).toFixed(2)} 绉掞紝瀛愪唬璇勪及 ${(profile.offspringEvalMs / 1000).toFixed(2)} 绉掞紝绉绘皯 ${(profile.immigrantMs / 1000).toFixed(2)} 绉掋€俙);
  }
  return { plans: population[0].plans, traceLog };
}

async function optimizeWaveWithSA(initialPlans, scenario, wave, randomSeed = 307) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "sa", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  let currentState = cloneWaveRouteState(initialPlans);
  let currentEval = evaluateRouteState(currentState, scenario, wave);
  if (!currentEval) return { plans: initialPlans, traceLog: [] };
  let currentCost = currentEval.cost;
  let currentPlans = currentEval.plans;
  let bestState = cloneWaveRouteState(currentPlans);
  let bestPlans = currentPlans;
  let bestCost = currentCost;
  let temperature = Math.max(12, currentCost * 0.12);
  const finalTemperature = 0.25;
  const coolingRate = 0.93;
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "sa",
    scope: "wave",
    waveId: wave.waveId,
    stage: "sa-start",
    bestCost,
    textZh: `妯℃嫙閫€鐏粠鍒濆瑙ｅ惎鍔紝鍒濆娓╁害 ${temperature.toFixed(1)}锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `銈枫儫銉ャ儸銉笺儐銉冦儔銈儖銉笺儶銉炽偘銈掑垵鏈熻В銇嬨倝闁嬪銇椼€佸垵鏈熸俯搴?${temperature.toFixed(1)}銆佸垵鏈熴偝銈广儓 ${bestCost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 妯℃嫙閫€鐏凡鍚姩锛屽垵濮嬫俯搴?${temperature.toFixed(1)}锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  let epoch = 0;
  while (temperature > finalTemperature && epoch < 28) {
    if (epoch) await cooperativeYield();
    for (let inner = 0; inner < 8; inner += 1) {
      const candidate = sampleSingleNeighbor(currentState, scenario, wave, random, inner === 0 || (epoch + inner) % 5 === 0, 5);
      if (!candidate) continue;
      const delta = candidate.cost - currentCost;
      const accepted = delta < 0 || random() < Math.exp(-delta / Math.max(0.001, temperature));
      if (!accepted) continue;
      currentState = candidate.state;
      currentPlans = candidate.plans;
      currentCost = candidate.cost;
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
          textZh: `娓╁害 ${temperature.toFixed(2)} 涓嬫帴鍙?${candidate.meta?.type || "mixed"} 閭诲煙锛屽€欓€夋垚鏈?${candidate.cost.toFixed(1)}銆俙,
          textJa: `娓╁害 ${temperature.toFixed(2)} 銇?${candidate.meta?.type || "mixed"} 杩戝倣銈掓帯鐢ㄣ仐銆佸€欒銈炽偣銉堛伅 ${candidate.cost.toFixed(1)}銆俙,
        });
      }
      if (candidate.cost + 1e-6 < bestCost) {
        bestCost = candidate.cost;
        bestState = cloneWaveRouteState(candidate.plans);
        bestPlans = candidate.plans;
        reportRelayStageProgress(`${wave.waveId} 妯℃嫙閫€鐏湪绗?${epoch + 1} 杞埛鏂版渶浼橈紝娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
        pushTraceEvent(traceLog, {
          algorithmKey: "sa",
          scope: "wave",
          waveId: wave.waveId,
          stage: "sa-best",
          epoch,
          bestCost,
      textZh: `妯℃嫙閫€鐏埛鏂版渶浼樿В锛屾柊鐨勬尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
          textJa: `鐒笺亶銇伨銇椼仹鏈€鑹В銈掓洿鏂般仐銆佹柊銇椼亜鏈€鑹偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
        });
      }
    }
    if ((epoch + 1) % 4 === 0) {
      reportRelayStageProgress(`${wave.waveId} 妯℃嫙閫€鐏凡璺戝埌绗?${epoch + 1}/28 杞紝褰撳墠娓╁害 ${temperature.toFixed(2)}锛屽綋鍓嶆渶濂芥尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
    }
    temperature *= coolingRate;
    epoch += 1;
  }
  const polished = localImproveState(bestState, scenario, wave, random, 4);
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
    textZh: `妯℃嫙閫€鐏粨鏉燂紝鏈€缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `銈枫儫銉ャ儸銉笺儐銉冦儔銈儖銉笺儶銉炽偘銇岀祩浜嗐仐銆佹渶绲傛渶鑹偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 妯℃嫙閫€鐏粨鏉燂紝鏈€缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  return { plans: rebuildWavePlansFromState(bestState, scenario, wave) || bestPlans, traceLog };
}

function buildPheromoneMapForWave(wave, initialValue = 0.2) {
  const pheromone = new Map();
  const ids = [DC.id, ...wave.storeList];
  ids.forEach((fromId) => {
    const row = new Map();
    ids.forEach((toId) => {
      if (fromId !== toId) row.set(toId, initialValue);
    });
    pheromone.set(fromId, row);
  });
  return pheromone;
}

function pheromoneValue(pheromone, fromId, toId, fallback = 0.2) {
  return pheromone.get(fromId)?.get(toId) ?? fallback;
}

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

function evaporatePheromone(pheromone, rho = 0.12) {
  pheromone.forEach((row) => {
    row.forEach((value, key) => {
      row.set(key, Math.max(0.01, value * (1 - rho)));
    });
  });
}

function selectAcoNextStore(currentId, remainingStoreIds, pheromone, scenario, wave, random, alpha = 1, beta = 2.2) {
  const weighted = remainingStoreIds.map((storeId) => {
    const pher = Math.pow(pheromoneValue(pheromone, currentId, storeId), alpha);
    const heuristic = 1 / Math.max(1, (scenario.dist?.[currentId]?.[storeId] || 1));
    const timing = getStoreTimingForWave(scenario.storeMap.get(storeId), wave, scenario.dispatchStartMin);
    const urgency = 1 + Math.max(0, (1440 - timing.latestAllowedArrivalMin) / 1440);
    const weight = pher * Math.pow(heuristic * urgency, beta);
    return { storeId, weight };
  });
  const total = weighted.reduce((sum, item) => sum + item.weight, 0);
  if (total <= 0) return remainingStoreIds[0];
  let cursor = random() * total;
  for (const item of weighted) {
    cursor -= item.weight;
    if (cursor <= 0) return item.storeId;
  }
  return weighted[weighted.length - 1]?.storeId || remainingStoreIds[0];
}

function constructAcoState(seedState, pheromone, scenario, wave, random) {
  const allStoreIds = [...new Set(flattenStoresFromRouteState(seedState))];
  if (!allStoreIds.length) return null;
  const order = [];
  const remaining = [...allStoreIds];
  let currentId = DC.id;
  while (remaining.length) {
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
  const inserted = insertStoresIntoRouteState(emptyState, order, scenario, wave);
  if (!inserted) return null;
  return inserted;
}

async function optimizeWaveWithACO(initialPlans, scenario, wave, randomSeed = 409) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "aco", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  const initialState = cloneWaveRouteState(initialPlans);
  const initialEval = evaluateRouteState(initialState, scenario, wave);
  if (!initialEval) return { plans: initialPlans, traceLog: [] };
  const pheromone = buildPheromoneMapForWave(wave, 0.18);
  let bestState = cloneWaveRouteState(initialEval.plans);
  let bestPlans = initialEval.plans;
  let bestCost = initialEval.cost;
  const traceLog = [];
  pushTraceEvent(traceLog, {
    algorithmKey: "aco",
    scope: "wave",
    waveId: wave.waveId,
    stage: "aco-start",
    bestCost,
    textZh: `铓佺兢绠楁硶鍚姩锛屽厛鍒濆鍖栦俊鎭礌锛屽啀鏋勯€犲€欓€夐棬搴楀簭鍒楋紝鍒濆娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙,
    textJa: `锜汇偝銉儖銉兼渶閬╁寲銈掗枊濮嬨仐銆併儠銈с儹銉兂銈掑垵鏈熷寲銇椼仸鍊欒闋嗗簭銈掓绡夈仐銇俱仚銆傚垵鏈熴偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 铓佺兢绠楁硶宸插惎鍔紝鍒濆娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
  for (let iteration = 0; iteration < 26; iteration += 1) {
    if (iteration) await cooperativeYield();
    const ants = [];
    for (let ant = 0; ant < 10; ant += 1) {
      const candidateState = constructAcoState(bestState, pheromone, scenario, wave, random);
      if (!candidateState) continue;
      const improved = localImproveState(candidateState, scenario, wave, random, 2) || evaluateRouteState(candidateState, scenario, wave);
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
    if (ants[0].cost + 1e-6 < bestCost) {
      bestCost = ants[0].cost;
      bestState = cloneWaveRouteState(ants[0].plans);
      bestPlans = ants[0].plans;
      reportRelayStageProgress(`${wave.waveId} 铓佺兢鍦ㄧ ${iteration + 1} 杞埛鏂版渶浼橈紝娉㈡鍐呴儴浠ｄ环 ${bestCost.toFixed(1)}銆俙);
      pushTraceEvent(traceLog, {
        algorithmKey: "aco",
        scope: "wave",
        waveId: wave.waveId,
        stage: "aco-best",
        iteration,
        bestCost,
        textZh: `绗?${iteration + 1} 杞殎缇ゅ埛鏂版渶浼樿В锛屾柊鐨勬尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇ц熁缇ゃ亴鏈€鑹В銈掓洿鏂般仐銆佹柊銇椼亜鏈€鑹偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
      });
    }
    if ((iteration + 1) % 5 === 0) {
      reportRelayStageProgress(`${wave.waveId} 铓佺兢宸茶窇鍒扮 ${iteration + 1}/26 杞紝鏈疆鏈€浼樺€欓€変唬浠?${ants[0].cost.toFixed(1)}锛屽叏灞€鏈€浼?${bestCost.toFixed(1)}銆俙);
    }
    if (traceLog.length < 18) {
      pushTraceEvent(traceLog, {
        algorithmKey: "aco",
        scope: "wave",
        waveId: wave.waveId,
        stage: "aco-iteration",
        iteration,
        bestCost: ants[0].cost,
        textZh: `绗?${iteration + 1} 杞俊鎭礌瀹屾垚鎸ュ彂涓庡己鍖栵紝鏈疆鏈€浼樺€欓€夋垚鏈?${ants[0].cost.toFixed(1)}銆俙,
        textJa: `${iteration + 1} 鍥炵洰銇с儠銈с儹銉兂銇捀鐧恒仺寮峰寲銈掑畬浜嗐仐銆併亾銇洖銇渶鑹€欒銈炽偣銉堛伅 ${ants[0].cost.toFixed(1)}銆俙,
      });
    }
  }
  pushTraceEvent(traceLog, {
    algorithmKey: "aco",
    scope: "wave",
    waveId: wave.waveId,
    stage: "aco-finish",
    bestCost,
    textZh: `铓佺兢绠楁硶缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙,
    textJa: `锜汇偝銉儖銉兼渶閬╁寲銇岀祩浜嗐仐銆佹渶绲傛渶鑹偝銈广儓銇?${bestCost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 铓佺兢绠楁硶缁撴潫锛屾渶缁堟尝娆″唴閮ㄤ唬浠?${bestCost.toFixed(1)}銆俙);
  return { plans: bestPlans, traceLog };
}

function decodePsoPriorityState(priorityVector, templateState, scenario, wave) {
  const storeIds = flattenStoresFromRouteState(templateState);
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
  return insertStoresIntoRouteState(emptyState, ordered, scenario, wave);
}

async function optimizeWaveWithPSO(initialPlans, scenario, wave, randomSeed = 503) {
  const backendResult = await tryOptimizeWaveWithPythonBackend(initialPlans, scenario, wave, "pso", randomSeed);
  if (backendResult?.plans?.length) return backendResult;
  const random = createSeededRandom(randomSeed);
  const templateState = cloneWaveRouteState(initialPlans);
  const storeIds = flattenStoresFromRouteState(templateState);
  if (!storeIds.length) return { plans: initialPlans, traceLog: [] };
  const dimension = storeIds.length;
  const particleCount = Math.min(18, Math.max(10, dimension));
  const iterations = 36;
  const c1 = 1.55;
  const c2 = 1.55;
  const vmax = 0.35;
  const traceLog = [];
  let globalBest = null;
  const particles = [];
  const describeCurrentWaveCost = (plans) => {
    const breakdown = computePlansCostBreakdown(plans, scenario, wave);
    return `褰撳墠杩欐尝娆＄殑鍐呴儴浠ｄ环绾?${Number(breakdown.totalCost || 0).toFixed(1)}锛岀粍鎴愭槸锛?{formatWaveCostBreakdown(breakdown)}銆俙;
  };

  function evaluateParticle(position) {
    const state = decodePsoPriorityState(position, templateState, scenario, wave);
    if (!state) return null;
    const improved = localImproveState(state, scenario, wave, random, 2) || evaluateRouteState(state, scenario, wave);
    if (!improved) return null;
    return {
      position: [...position],
      state: cloneWaveRouteState(improved.plans),
      plans: improved.plans,
      cost: improved.cost,
    };
  }

  for (let i = 0; i < particleCount; i += 1) {
    const position = Array.from({ length: dimension }, () => random());
    const velocity = Array.from({ length: dimension }, () => (random() - 0.5) * 0.2);
    const evaluated = evaluateParticle(position);
    if (!evaluated) continue;
    const particle = {
      position,
      velocity,
      bestPosition: [...position],
      bestCost: evaluated.cost,
      bestState: evaluated.state,
      bestPlans: evaluated.plans,
    };
    particles.push(particle);
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
    textZh: `绮掑瓙缇ょ畻娉曞惎鍔紝绮掑瓙鏁?${particles.length}锛岀淮搴?${dimension}锛屽垵濮嬫尝娆″唴閮ㄤ唬浠?${globalBest.cost.toFixed(1)}銆俙,
    textJa: `绮掑瓙缇ゆ渶閬╁寲銈掗枊濮嬨仐銆佺矑瀛愭暟 ${particles.length}銆佹鍏?${dimension}銆佸垵鏈熴伄鍏ㄤ綋鏈€鑹偝銈广儓銇?${globalBest.cost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 宸茶繘鍏ョ矑瀛愮兢鎼滅储锛屽綋鍓嶅鐞?${dimension} 瀹堕棬搴椼€?{particles.length} 涓矑瀛愩€傚厛鐢ㄧ幇鏈夋柟妗堝仛璧疯窇绾匡紝鍐嶈绮掑瓙缇ゅ弽澶嶈皟鏁撮棬搴椾紭鍏堢骇銆?{describeCurrentWaveCost(globalBest.plans)}`);

  let stagnation = 0;
  for (let iter = 0; iter < iterations; iter += 1) {
    if (iter) await cooperativeYield();
    const inertia = 0.78 - (0.45 * iter / iterations);
    let improvedThisRound = false;
    particles.forEach((particle, index) => {
      for (let d = 0; d < dimension; d += 1) {
        const r1 = random();
        const r2 = random();
        let velocity = inertia * particle.velocity[d]
          + c1 * r1 * (particle.bestPosition[d] - particle.position[d])
          + c2 * r2 * (globalBest.position[d] - particle.position[d]);
        velocity = Math.max(-vmax, Math.min(vmax, velocity));
        particle.velocity[d] = velocity;
        particle.position[d] = Math.max(0, Math.min(1, particle.position[d] + velocity));
      }
      const evaluated = evaluateParticle(particle.position);
      if (!evaluated) return;
      if (evaluated.cost + 1e-6 < particle.bestCost) {
        particle.bestCost = evaluated.cost;
        particle.bestPosition = [...particle.position];
        particle.bestState = evaluated.state;
        particle.bestPlans = evaluated.plans;
      }
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
          textZh: `绗?${iter + 1} 杞矑瀛?${index + 1} 鍒锋柊鍏ㄥ眬鏈€浼橈紝鏂扮殑娉㈡鍐呴儴浠ｄ环 ${globalBest.cost.toFixed(1)}銆俙,
          textJa: `${iter + 1} 鍥炵洰銇х矑瀛?${index + 1} 銇屽叏浣撴渶鑹倰鏇存柊銇椼€佹柊銇椼亜鏈€鑹偝銈广儓銇?${globalBest.cost.toFixed(1)}銆俙,
        });
        if ((iter + 1) <= 3 || (iter + 1) % 6 === 0) {
          reportRelayStageProgress(`${wave.waveId} 绗?${iter + 1} 杞噷锛岀矑瀛?${index + 1} 鍒锋柊浜嗚繖涓€娉㈡鐨勫綋鍓嶆渶浼樸€傝鏄庡畠纭疄鎵惧埌鏇撮『鐨勬帓娉曚簡銆?{describeCurrentWaveCost(globalBest.plans)}`);
        }
      }
    });
    if (traceLog.length < 20) {
      pushTraceEvent(traceLog, {
        algorithmKey: "pso",
        scope: "wave",
        waveId: wave.waveId,
        stage: "pso-iteration",
        iteration: iter,
        inertia,
        bestCost: globalBest.cost,
        textZh: `绗?${iter + 1} 杞畬鎴愰€熷害涓庝綅缃洿鏂帮紝鎯€ф潈閲?${inertia.toFixed(2)}锛屽綋鍓嶆尝娆″唴閮ㄤ唬浠?${globalBest.cost.toFixed(1)}銆俙,
        textJa: `${iter + 1} 鍥炵洰銇ч€熷害銇ㄤ綅缃伄鏇存柊銈掑畬浜嗐仐銆佹叄鎬ч噸銇?${inertia.toFixed(2)}銆佺従鍦ㄣ伄鍏ㄤ綋鏈€鑹偝銈广儓銇?${globalBest.cost.toFixed(1)}銆俙,
      });
    }
    if ((iter + 1) === 1 || (iter + 1) % 6 === 0 || iter === iterations - 1) {
      reportRelayStageProgress(`${wave.waveId} 绮掑瓙缇ゅ凡璺戝埌绗?${iter + 1}/${iterations} 杞€傝繖娈垫椂闂翠富瑕佸湪鍙嶅璋冩暣闂ㄥ簵鍏堝悗椤哄簭鍜岃溅杈嗗綊灞炪€?{describeCurrentWaveCost(globalBest.plans)}`);
    }
    stagnation = improvedThisRound ? 0 : stagnation + 1;
    if (stagnation >= 8) {
      const restartCount = Math.max(1, Math.floor(particles.length * 0.25));
      for (let i = particles.length - restartCount; i < particles.length; i += 1) {
        particles[i].position = Array.from({ length: dimension }, () => random());
        particles[i].velocity = Array.from({ length: dimension }, () => (random() - 0.5) * 0.2);
      }
      reportRelayStageProgress(`${wave.waveId} 杩炵画鍑犺疆娌℃湁鏄庢樉鎻愬崌锛岀矑瀛愮兢鍒氬垰閲嶅惎浜?${restartCount} 涓〃鐜版渶宸殑绮掑瓙锛岄伩鍏嶅ぇ瀹朵竴鐩村洿鐫€鍚屼竴鏉℃棫璺嚎绌鸿浆銆俙);
      stagnation = 0;
    }
  }
  const polished = localImproveState(globalBest.state, scenario, wave, random, 5);
  reportRelayStageProgress(`${wave.waveId} 鐨勭矑瀛愮兢涓讳綋鎼滅储璺戝畬浜嗭紝姝ｅ湪鍋氭渶鍚庝竴杞眬閮ㄥ井璋冿紝鎶婂凡缁忔壘鍒扮殑濂芥柟妗堝啀鎶涘厜涓€涓嬨€?{describeCurrentWaveCost(globalBest.plans)}`);
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
    textZh: `绮掑瓙缇ょ畻娉曠粨鏉燂紝鏈€缁堟尝娆″唴閮ㄤ唬浠?${globalBest.cost.toFixed(1)}銆俙,
    textJa: `绮掑瓙缇ゆ渶閬╁寲銇岀祩浜嗐仐銆佹渶绲傛渶鑹偝銈广儓銇?${globalBest.cost.toFixed(1)}銆俙,
  });
  reportRelayStageProgress(`${wave.waveId} 鐨勭矑瀛愮兢鎼滅储瀹屾垚銆傛帴涓嬫潵浼氬洖鍒版暣鐩樻柟妗堜笂鍒ゆ柇杩欎竴妫掑€间笉鍊煎緱姝ｅ紡鎺ヨ繃鍘汇€?{describeCurrentWaveCost(globalBest.plans)}`);
  return { plans: globalBest.plans, traceLog };
}

function buildSeedPlansForWave(scenario, wave, regularVehicleStats = new Map(), basePlans = []) {
  return scenario.vehicles.map((vehicle) => {
    const prior = regularVehicleStats.get(vehicle.plateNo) || {};
    const earliestDepartureMin = wave.isNightWave
      ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(prior.nightAvailableMin || wave.startMin))
      : wave.startMin;
    const basePlan = (basePlans || []).find((plan) => plan.vehicle.plateNo === vehicle.plateNo);
    if (!basePlan?.trips?.length) {
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

function calculateRouteDistanceFromIds(route, scenario) {
  if (!route.length) return 0;
  let total = scenario.dist?.[DC.id]?.[route[0]] || 0;
  for (let i = 0; i < route.length - 1; i += 1) total += scenario.dist?.[route[i]]?.[route[i + 1]] || 0;
  total += scenario.dist?.[route[route.length - 1]]?.[DC.id] || 0;
  return total;
}

function hasSavingsStandaloneFeasibility(route, scenario, wave) {
  return scenario.vehicles.some((vehicle) => {
    const plan = rebuildPlanFromRoutes(vehicle, [route], scenario, wave, {
      priorRegularDistance: 0,
      priorWaveCount: 0,
      earliestDepartureMin: wave.isNightWave ? Math.max(Number(wave.earliestDepartureMin || wave.startMin), Number(scenario.dispatchStartMin || wave.startMin)) : wave.startMin,
    });
    return !!plan;
  });
}

function buildSavingsCandidatesForWave(scenario, wave) {
  const savings = [];
  for (let i = 0; i < wave.storeList.length; i += 1) {
    for (let j = i + 1; j < wave.storeList.length; j += 1) {
      const storeAId = wave.storeList[i];
      const storeBId = wave.storeList[j];
      const saving = (scenario.dist?.[DC.id]?.[storeAId] || 0) + (scenario.dist?.[DC.id]?.[storeBId] || 0) - (scenario.dist?.[storeAId]?.[storeBId] || 0);
      if (saving <= 0) continue;
      savings.push({ storeAId, storeBId, saving });
    }
  }
  savings.sort((a, b) => b.saving - a.saving);
  return savings;
}

function buildSavingsMergeOptions(routeA, routeB, storeAId, storeBId) {
  const options = [];
  const aFront = routeA[0] === storeAId;
  const aBack = routeA[routeA.length - 1] === storeAId;
  const bFront = routeB[0] === storeBId;
  const bBack = routeB[routeB.length - 1] === storeBId;
  if (!(aFront || aBack) || !(bFront || bBack)) return options;
  if (aBack && bFront) options.push(routeA.concat(routeB));
  if (aFront && bBack) options.push(routeB.concat(routeA));
  if (aFront && bFront) options.push([...routeA].reverse().concat(routeB));
  if (aBack && bBack) options.push(routeA.concat([...routeB].reverse()));
  return options.filter((route, index, list) => {
    const key = route.join(">");
    return list.findIndex((item) => item.join(">") === key) === index;
  });
}

function twoOptRouteIds(route, scenario, wave) {
  let best = [...route];
  let bestDistance = calculateRouteDistanceFromIds(best, scenario);
  let improved = true;
  while (improved) {
    improved = false;
    for (let i = 0; i < best.length - 1; i += 1) {
      for (let j = i + 1; j < best.length; j += 1) {
        const candidate = best.slice(0, i).concat(best.slice(i, j + 1).reverse(), best.slice(j + 1));
        const candidateDistance = calculateRouteDistanceFromIds(candidate, scenario);
        if (candidateDistance + 1e-6 >= bestDistance) continue;
        if (!hasSavingsStandaloneFeasibility(candidate, scenario, wave)) continue;
        best = candidate;
        bestDistance = candidateDistance;
        improved = true;
        break;
      }
      if (improved) break;
    }
  }
  return best;
}

function assignSavingsRoutesToPlans(routes, seedPlans, scenario, wave, traceLog) {
  const plans = seedPlans.map((plan) => clone(plan));
  const unscheduledStores = [];
  const sortedRoutes = [...routes].sort((a, b) => (b.totalBoxes - a.totalBoxes) || (b.route.length - a.route.length));
  for (const item of sortedRoutes) {
    let best = null;
    for (let i = 0; i < plans.length; i += 1) {
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
      if (!nextPlan) continue;
      const cost = computePlanCost(nextPlan, scenario, wave);
      if (!best || cost < best.cost) best = { planIndex: i, nextPlan, cost };
    }
    if (best) {
      plans[best.planIndex] = best.nextPlan;
      if (traceLog.length < 48) {
        pushTraceEvent(traceLog, {
          algorithmKey: "savings",
          scope: "wave",
          waveId: wave.waveId,
          stage: "savings-assignment",
          textZh: `灏嗚妭绾︽硶鐢熸垚绾胯矾 ${item.route.join("->")} 鍒嗛厤缁?${plans[best.planIndex].vehicle.plateNo}锛岀患鍚堟垚鏈?${best.cost.toFixed(1)}銆俙,
          textJa: `绡€绱勬硶銇х敓鎴愩仐銇熴儷銉笺儓 ${item.route.join("->")} 銈?${plans[best.planIndex].vehicle.plateNo} 銇壊銈婂綋銇︺€佺窂鍚堛偝銈广儓銇?${best.cost.toFixed(1)}銆俙,
        });
      }
      continue;
    }
    for (const storeId of item.route) {
      const store = scenario.storeMap.get(storeId);
      if (!store) continue;
      let inserted = false;
      for (let i = 0; i < plans.length; i += 1) {
        const candidate = buildTripCandidate(plans[i], store, scenario, wave, false, { allowToleranceBreak: false });
        if (!candidate) continue;
        plans[i] = candidate.nextPlan;
        inserted = true;
        break;
      }
      if (!inserted) {
        const diagnosis = diagnoseUnscheduledStore(store, plans, scenario, wave);
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
  return { plans, unscheduledStores };
}

function solveWaveBySavings(scenario) {
  const optimizedSolution = [];
  const traceLog = [];
  const regularVehicleStats = new Map();
  const unscheduledStores = [];
  const maxVehicleCapacity = Math.max(...scenario.vehicles.map((vehicle) => Number(vehicle.capacity || 0)), 0);
  for (const wave of scenario.waves) {
    const seedPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, []);
    const routeRefs = new Map();
    let routes = wave.storeList
      .map((storeId) => scenario.storeMap.get(storeId))
      .filter(Boolean)
      .map((store) => {
        const item = {
          id: `cw_${wave.waveId}_${store.id}`,
          route: [store.id],
          totalBoxes: Number(getStoreSolveLoadForWave(store, wave) || 0),
        };
        routeRefs.set(store.id, item);
        return item;
      });
    const savings = buildSavingsCandidatesForWave(scenario, wave);
    pushTraceEvent(traceLog, {
      algorithmKey: "savings",
      scope: "wave",
      waveId: wave.waveId,
      stage: "savings-start",
      textZh: `Clark-Wright 鑺傜害娉曞惎鍔紝鍏堢敓鎴?${routes.length} 鏉″崟搴楃嚎璺紝鍐嶈绠?${savings.length} 涓妭绾﹀€笺€俙,
      textJa: `Clark-Wright 绡€绱勬硶銈掗枊濮嬨仐銆併伨銇?${routes.length} 鏈伄鍗樺簵鑸椼儷銉笺儓銈掍綔鎴愩仐銆併仢銇緦 ${savings.length} 鍊嬨伄绡€绱勫€ゃ倰瑷堢畻銇椼伨銇欍€俙,
    });
    for (const saving of savings) {
      const routeA = routeRefs.get(saving.storeAId);
      const routeB = routeRefs.get(saving.storeBId);
      if (!routeA || !routeB || routeA === routeB) continue;
      if (!IGNORE_CAPACITY_CONSTRAINT && routeA.totalBoxes + routeB.totalBoxes > maxVehicleCapacity) continue;
      const options = buildSavingsMergeOptions(routeA.route, routeB.route, saving.storeAId, saving.storeBId);
      let bestRoute = null;
      let bestDistance = Number.POSITIVE_INFINITY;
      for (const option of options) {
        if (!hasSavingsStandaloneFeasibility(option, scenario, wave)) continue;
        const distance = calculateRouteDistanceFromIds(option, scenario);
        if (distance < bestDistance) {
          bestDistance = distance;
          bestRoute = option;
        }
      }
      if (!bestRoute) continue;
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
          textZh: `鑺傜害鍊?${saving.saving.toFixed(1)} 椹卞姩鍚堝苟 ${saving.storeAId} 涓?${saving.storeBId}锛屽舰鎴愮嚎璺?${bestRoute.join("->")}銆俙,
          textJa: `绡€绱勫€?${saving.saving.toFixed(1)} 銇熀銇ャ亶 ${saving.storeAId} 銇?${saving.storeBId} 銈掔当鍚堛仐銆併儷銉笺儓 ${bestRoute.join("->")} 銈掑舰鎴愩€俙,
        });
      }
    }
    routes = routes.map((item) => ({
      ...item,
      route: twoOptRouteIds(item.route, scenario, wave),
    }));
    const assigned = assignSavingsRoutesToPlans(routes, seedPlans, scenario, wave, traceLog);
    optimizedSolution.push(assigned.plans);
    unscheduledStores.push(...assigned.unscheduledStores);
    if (wave.isNightWave) {
      assigned.plans.forEach((plan) => {
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
      textZh: `Clark-Wright 鑺傜害娉曞畬鎴?${wave.waveId}锛屾渶缁堢敓鎴?${assigned.plans.flatMap((plan) => plan.trips).length} 瓒熺嚎璺€俙,
      textJa: `Clark-Wright 绡€绱勬硶銇?${wave.waveId} 銈掑畬浜嗐仐銆佹渶绲傜殑銇?${assigned.plans.flatMap((plan) => plan.trips).length} 渚裤倰鐢熸垚銆俙,
    });
  }
  return { solution: optimizedSolution, traceLog, unscheduledStores };
}

async function solveWaveByWaveWithOptimizer(scenario, optimizer, baseSeed = 0, optimizerKey = "hybrid") {
  const baseRun = greedySolve(scenario, baseSeed, true);
  if (!baseRun?.solution?.length) return { solution: [], traceLog: [], unscheduledStores: baseRun?.unscheduledStores || [] };
  const optimizedSolution = [];
  let traceLog = [];
  const backendUnscheduledStores = [];
  let backendUnscheduledProvided = false;
  pushTraceEvent(traceLog, {
    algorithmKey: optimizerKey,
    scope: "wave",
    stage: `${optimizerKey}-seed`,
    textZh: `${algoLabel(optimizerKey)} 浠ュ揩閫熷垵鎺掔粨鏋滀綔涓鸿捣璺戝熀绾匡紝浠ヤ笅灞曠ず鐨勬槸璇ョ畻娉曡嚜韬殑缁х画浼樺寲杩囩▼銆俙,
    textJa: `${algoLabel(optimizerKey)} 銇垵鏈熼厤杌娿倰璧风偣銇渶閬╁寲銈掔稒缍氥仐銇俱仚銆備互涓嬨伅褰撹┎銈儷銈淬儶銈恒儬鑷韩銇儹銈般仹銇欍€俙,
  });
  const regularVehicleStats = new Map();
  for (let waveIndex = 0; waveIndex < scenario.waves.length; waveIndex += 1) {
    if (waveIndex) await cooperativeYield();
    const wave = scenario.waves[waveIndex];
    const seededPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, baseRun.solution[waveIndex] || []);
    const optimized = await optimizer(seededPlans, scenario, wave, baseSeed * 100 + 42 + waveIndex);
    optimizedSolution.push(optimized.plans);
    traceLog = traceLog.concat(optimized.traceLog || []);
    if (Array.isArray(optimized.unscheduledStores)) {
      backendUnscheduledProvided = true;
      backendUnscheduledStores.push(...optimized.unscheduledStores);
    }
    if (wave.isNightWave) {
      optimized.plans.forEach((plan) => {
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
  return {
    solution: optimizedSolution,
    traceLog,
    unscheduledStores: backendUnscheduledProvided ? backendUnscheduledStores : (baseRun.unscheduledStores || []),
  };
}

async function improveSolutionByWaveOptimizer(baseSolution, scenario, optimizer, baseSeed = 0) {
  if (!baseSolution?.length) return { solution: [], traceLog: [] };
  const optimizedSolution = [];
  let traceLog = [];
  const regularVehicleStats = new Map();
  const baseMetrics = evaluateSolution(baseSolution, scenario, []);
  const waveIssueScores = scenario.waves.map((wave, waveIndex) => {
    const plans = baseSolution[waveIndex] || [];
    const trips = plans.flatMap((plan) => plan.trips || []);
    const lateStops = trips.reduce((sum, trip) => sum + (trip.lateStoreCount || 0), 0);
    const overTolerance = trips.reduce((sum, trip) => sum + (trip.overToleranceMinutes || 0), 0);
    const lowLoadTrips = trips.filter((trip) => (trip.loadRate || 0) < 0.55).length;
    const mileageRef = isW3WaveForSolve(wave) ? getSolveW3OneWayMaxKm(scenario) : getSolveRelayMaxKm(scenario);
    const longTrips = trips.filter((trip) => (trip.totalDistance || 0) > Math.max(80, mileageRef * 0.45)).length;
    const score = lateStops * 8 + overTolerance * 0.2 + lowLoadTrips * 2 + longTrips * 2;
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
  if (!focusWaveIndexes.size) {
    waveIssueScores.forEach((item) => {
      if (item.hasTrips) focusWaveIndexes.add(item.waveIndex);
    });
  }
  reportRelayStageProgress(`杩欎竴妫掍笉浼氭暣鐩橀噸绠楋紝鑰屾槸浼樺厛鐩綇闂鏇撮噸鐨勬尝娆★細${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("銆?)}銆傚叾瀹冩尝娆″厛娌跨敤涓婁竴杞粨鏋溿€俙);
  for (let waveIndex = 0; waveIndex < scenario.waves.length; waveIndex += 1) {
    if (waveIndex) await cooperativeYield();
    const wave = scenario.waves[waveIndex];
    const seededPlans = buildSeedPlansForWave(scenario, wave, regularVehicleStats, baseSolution[waveIndex] || []);
    if (!focusWaveIndexes.has(waveIndex)) {
      reportRelayStageProgress(`${wave.waveId} 褰撳墠闂涓嶉噸锛岃繖涓€妫掑厛涓嶅姩瀹冿紝鐩存帴娌跨敤涓婁竴杞帓娉曘€俙);
      optimizedSolution.push(seededPlans);
      traceLog.push({
        algorithmKey: "focus",
        scope: "wave",
        waveId: wave.waveId,
        stage: "focus-skip",
        textZh: `${wave.waveId} 褰撳墠闂涓嶉噸锛岀户缁部鐢ㄤ笂涓€杞粨鏋滐紝涓嶅仛閲嶅閲嶇畻銆俙,
        textJa: `${wave.waveId} 銇従鏅傜偣銇у晱椤屻亴閲嶃亸銇亜銇熴倎銆佸墠杓祼鏋溿倰銇濄伄銇俱伨寮曘亶缍欍亷銇俱仚銆俙,
      });
      if (wave.isNightWave) {
        seededPlans.forEach((plan) => {
          const prev = regularVehicleStats.get(plan.vehicle.plateNo) || { priorRegularDistance: 0, priorWaveCount: 0, nightAvailableMin: wave.startMin };
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
    const tripCount = seededPlans.flatMap((plan) => plan.trips || []).length;
    reportRelayStageProgress(`寮€濮嬬粏淇?${wave.waveId}銆傝繖娉㈠綋鍓嶆湁 ${tripCount} 瓒熺嚎璺紝浼樺寲鍣ㄤ細鍙湪杩欓儴鍒嗛噷閲嶆帓锛岀湅鐪嬭兘涓嶈兘鍘嬮噷绋嬨€佸皯鐢ㄨ溅锛屾垨鑰呮妸璇勫垎鍐嶅線涓婇《銆俙);
    const optimized = await optimizer(seededPlans, scenario, wave, baseSeed * 100 + 91 + waveIndex);
    optimizedSolution.push(optimized.plans);
    traceLog = traceLog.concat(optimized.traceLog || []);
    const beforeDistance = seededPlans.reduce((sum, plan) => sum + (plan.totalDistance || 0), 0);
    const afterDistance = optimized.plans.reduce((sum, plan) => sum + (plan.totalDistance || 0), 0);
    const distanceDelta = afterDistance - beforeDistance;
    const distanceDeltaLabel = `${distanceDelta > 0 ? "+" : ""}${distanceDelta.toFixed(1)}`;
    reportRelayStageProgress(`${wave.waveId} 杩欐尝宸茬粡绠楀畬銆備紭鍖栧墠閲岀▼绾?${beforeDistance.toFixed(1)} km锛屼紭鍖栧悗绾?${afterDistance.toFixed(1)} km锛屽彉鍖?${distanceDeltaLabel} km銆俙);
    if (wave.isNightWave) {
      optimized.plans.forEach((plan) => {
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
  const optimizedMetrics = evaluateSolution(optimizedSolution, scenario, []);
  traceLog.push({
    algorithmKey: "focus",
    scope: "wave",
    stage: "focus-summary",
    textZh: `鏈疆浼樺寲浼樺厛澶勭悊浜?${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("銆?)}锛屽叾瀹冩尝娆＄洿鎺ユ壙鎺ヤ笂涓€杞粨鏋滐紝閬垮厤鏁寸洏閲嶅閲嶇畻銆備紭鍖栧墠璇勫垎 ${baseMetrics.score.toFixed(1)}锛屼紭鍖栧悗璇勫垎 ${optimizedMetrics.score.toFixed(1)}銆俙,
    textJa: `鏈吉銇?${[...focusWaveIndexes].map((index) => scenario.waves[index]?.waveId).filter(Boolean).join("銆?)} 銈掑劒鍏堢殑銇渶閬╁寲銇椼€佷粬銇尝娆°伅鍓嶈吉绲愭灉銈掋仢銇伨銇惧紩銇嶇稒銇勩仹鍏ㄩ潰鍐嶈▓绠椼倰閬裤亼銇俱仐銇熴€傛渶閬╁寲鍓嶃偣銈炽偄 ${baseMetrics.score.toFixed(1)}銆佹渶閬╁寲寰屻偣銈炽偄 ${optimizedMetrics.score.toFixed(1)}銆俙,
  });
  return { solution: optimizedSolution, traceLog };
}

function solveFixedMembershipPlan(vehicle, storeIds, scenario, wave, priorStats = {}) {
  const storesBase = storeIds.map((id) => scenario.storeMap.get(id)).filter(Boolean);
  let bestPlan = null;
  let bestScore = Number.POSITIVE_INFINITY;
  for (const seed of [0, 1, 2]) {
    const stores = [...storesBase].sort((a, b) => {
      const timingA = getStoreTimingForWave(a, wave, scenario.dispatchStartMin);
      const timingB = getStoreTimingForWave(b, wave, scenario.dispatchStartMin);
      return timingA.desiredArrivalMin - timingB.desiredArrivalMin || a.id.localeCompare(b.id);
    });
    if (seed) {
      for (let i = stores.length - 1; i > 0; i -= 1) {
        const j = (i + seed) % stores.length;
        [stores[i], stores[j]] = [stores[j], stores[i]];
      }
    }
    const plan = buildPlanForStoreOrder(vehicle, stores, scenario, wave, priorStats);
    if (!plan) continue;
    const score = computePlanCost(plan, scenario, wave);
    if (score < bestScore) {
      bestScore = score;
      bestPlan = plan;
    }
  }
  return bestPlan;
}

function evaluateSolution(solution, scenario = null, unscheduledStores = []) {
  if (!solution.length) return { feasible: false, score: 0, totalStops: 0, totalOnTime: 0, totalDistance: 0, loadRate: 0, fleetLoadRate: 0, totalLoadBoxes: 0, unscheduledStores, unscheduledCount: unscheduledStores.length, scheduledCount: 0, scheduledByWave: [] };
  const plans = solution.flat();
  const trips = plans.flatMap((p) => p.trips);
  const scheduledByWave = (scenario?.waves || []).map((wave) => ({ waveId: String(wave?.waveId || ""), count: 0 }));
  solution.forEach((wavePlans, waveIndex) => {
    const count = (wavePlans || []).reduce((sum, plan) => sum + (plan.trips || []).reduce((tripSum, trip) => tripSum + ((trip.stops || []).length || 0), 0), 0);
    if (scheduledByWave[waveIndex]) {
      scheduledByWave[waveIndex].count = count;
    } else {
      scheduledByWave.push({ waveId: `W${waveIndex + 1}`, count });
    }
  });
  const totalStops = trips.reduce((sum, t) => sum + t.stops.length, 0);
  const totalOnTime = trips.reduce((sum, t) => sum + (t.onTimeCount || 0), 0);
  const totalDistance = plans.reduce((sum, p) => sum + p.totalDistance, 0);
  const loadRate = trips.length ? trips.reduce((sum, t) => sum + (t.loadRate || 0), 0) / trips.length : 0;
  const totalLoadBoxes = plans.reduce((sum, p) => sum + Number(p.totalLoad || 0), 0);
  const lateStoreCount = trips.reduce((sum, t) => sum + (t.lateStoreCount || 0), 0);
  const lateRouteCount = trips.reduce((sum, t) => sum + ((t.overToleranceMinutes || 0) > 0 ? 1 : 0), 0);
  const overToleranceCount = trips.reduce((sum, t) => sum + t.stops.filter((stop) => (stop.overToleranceMinutes || 0) > 0).length, 0);
  const usage = new Map();
  for (const plan of plans) {
    if (!plan.trips.length) continue;
    const key = plan.vehicle.plateNo;
    if (!usage.has(key)) usage.set(key, { load: 0, cap: plan.vehicle.capacity, trips: [] });
    usage.get(key).load += plan.totalLoad;
    usage.get(key).trips.push(...plan.trips);
  }
  const preferredTarget = Number(state.settings.minLoadRate || 0);
  const usageList = [...usage.entries()].map(([plateNo, item]) => {
    const achievedRate = item.cap > 0 ? item.load / item.cap : 0;
    const maxTripLoad = item.trips.reduce((max, trip) => Math.max(max, trip.loadBoxes || 0), 0);
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
  const preferredShortfall = usageList.reduce((sum, item) => sum + Math.max(0, preferredTarget - item.achievedRate), 0);
  const usedVehicleCount = usageList.length;
  const unusedVehicleCount = Math.max(0, state.vehicles.length - usedVehicleCount);
  const usedVehicleCapacity = usageList.reduce((sum, item) => sum + Number(item.cap || 0), 0);
  const fleetLoadRate = usedVehicleCapacity ? totalLoadBoxes / usedVehicleCapacity : 0;
  const onTimeRatio = totalOnTime / Math.max(totalStops, 1);
  const completionRatio = totalStops / Math.max(totalStops + unscheduledStores.length, 1);
  const distanceScore = Math.max(0, Math.min(1, (3500 - totalDistance) / 3500));
  const loadScore = Math.max(0, Math.min(1, loadRate));
  const preferenceScoreRaw = (preferredMetCount / Math.max(usageList.length, 1)) - preferredShortfall * 0.02;
  const preferenceScore = Math.max(0, Math.min(1, preferenceScoreRaw));
  const vehicleScore = Math.max(0, Math.min(1, (Math.max(state.vehicles.length, 1) - usedVehicleCount + 1) / Math.max(state.vehicles.length, 1)));
  const goal = state.settings.optimizeGoal || "balanced";
  const goalWeights = {
    balanced: { completion: 0.40, onTime: 0.35, distance: 0.15, load: 0.10, vehicles: 0.00 },
    ontime: { completion: 0.34, onTime: 0.46, distance: 0.10, load: 0.04, vehicles: 0.06 },
    distance: { completion: 0.30, onTime: 0.26, distance: 0.30, load: 0.06, vehicles: 0.08 },
    vehicles: { completion: 0.30, onTime: 0.22, distance: 0.14, load: 0.04, vehicles: 0.30 },
    load: { completion: 0.30, onTime: 0.24, distance: 0.10, load: 0.28, vehicles: 0.08 },
  };
  const weights = goalWeights[goal] || goalWeights.balanced;
  const score = (
    weights.completion * completionRatio +
    weights.onTime * onTimeRatio +
    weights.distance * distanceScore +
    weights.load * loadScore +
    weights.vehicles * vehicleScore
  ) * 100;
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
  const unscheduled = [];
  if (!scenario || !Array.isArray(scenario.waves) || !Array.isArray(scenario.stores)) return unscheduled;
  const assignmentMap = buildStoreAssignmentMapFromSolution(solution || []);
  const sourceStoreMap = scenario.storeMap instanceof Map
    ? scenario.storeMap
    : new Map((scenario.stores || []).map((store) => [String(store?.id || "").trim(), store]).filter(([id]) => id));
  const storeMap = new Map();
  sourceStoreMap.forEach((store, id) => {
    const raw = String(id || "").trim();
    const normalized = normalizeStoreKey(raw);
    if (raw && !storeMap.has(raw)) storeMap.set(raw, store);
    if (normalized && !storeMap.has(normalized)) storeMap.set(normalized, store);
  });
  for (const wave of (scenario.waves || [])) {
    const waveId = String(wave?.waveId || "").trim().toUpperCase();
    const seen = new Set();
    for (const storeId of (wave?.storeList || [])) {
      const sid = normalizeStoreKey(storeId);
      if (!sid || seen.has(sid)) continue;
      seen.add(sid);
      const store = storeMap.get(sid);
      if (!store) continue;
      if (!isStoreCandidateForWaveRule(store, waveId)) continue;
      const variants = buildStoreKeyVariants(sid);
      let assigned = false;
      for (const variant of variants) {
        const key = buildStoreWaveAssignmentKey(variant, waveId);
        const hit = key ? assignmentMap.get(key) : null;
        if (hit && String(hit.plateNo || "").trim()) {
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        unscheduled.push({
          waveId,
          storeId: sid,
          storeName: String(store?.name || ""),
          reason: "no_plate",
          reasonText: "鏈尝娆℃湁璐т絾鏃犺溅鐗屽彿",
          source: "final_state",
        });
      }
    }
  }
  return unscheduled;
}

function computeWaveCandidateAssignedPendingStats(solution = [], scenario = null) {
  const stats = [];
  if (!scenario || !Array.isArray(scenario.waves) || !Array.isArray(scenario.stores)) return stats;
  const assignmentMap = buildStoreAssignmentMapFromSolution(solution || []);
  const sourceStoreMap = scenario.storeMap instanceof Map
    ? scenario.storeMap
    : new Map((scenario.stores || []).map((store) => [String(store?.id || "").trim(), store]).filter(([id]) => id));
  const storeMap = new Map();
  sourceStoreMap.forEach((store, id) => {
    const raw = String(id || "").trim();
    const normalized = normalizeStoreKey(raw);
    if (raw && !storeMap.has(raw)) storeMap.set(raw, store);
    if (normalized && !storeMap.has(normalized)) storeMap.set(normalized, store);
  });
  for (const wave of (scenario.waves || [])) {
    const waveId = String(wave?.waveId || "").trim().toUpperCase();
    const candidateSet = new Set();
    const assignedSet = new Set();
    for (const storeId of (wave?.storeList || [])) {
      const sid = normalizeStoreKey(storeId);
      if (!sid || candidateSet.has(sid)) continue;
      const store = storeMap.get(sid);
      if (!store) continue;
      if (!isStoreCandidateForWaveRule(store, waveId)) continue;
      candidateSet.add(sid);
      const variants = buildStoreKeyVariants(sid);
      for (const variant of variants) {
        const key = buildStoreWaveAssignmentKey(variant, waveId);
        const hit = key ? assignmentMap.get(key) : null;
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
  return stats;
}

function reportWaveCandidateAssignedPendingStats(tag = "缁撴灉", solution = [], scenario = null) {
  const rows = computeWaveCandidateAssignedPendingStats(solution, scenario);
  rows.forEach((row) => {
    reportRelayStageProgress(`闆嗗悎鏍稿锛?{row.waveId}/${tag}锛夛細candidate=${row.candidateCount}锛宎ssigned=${row.assignedCount}锛宲ending=${row.pendingCount}銆俙);
  });
  return rows;
}

function applyFinalRuleToResult(result, scenario) {
  if (!result || typeof result !== "object") return result;
  const unscheduled = computeFinalPendingByWave(result.solution || [], scenario);
  result.unscheduledStores = unscheduled;
  result.metrics = evaluateSolution(result.solution || [], scenario, unscheduled);
  result.storeAssignmentMap = buildStoreAssignmentMapFromSolution(result.solution || []);
  result.waveSetStats = computeWaveCandidateAssignedPendingStats(result.solution || [], scenario);
  return result;
}

function buildRelayStagePlan(selectedSet = [], optimizeGoal = "balanced", storeCount = 0) {
  const goal = String(optimizeGoal || "balanced");
  const initialCandidates = selectedSet.filter((key) => ["vrptw", "savings"].includes(key));
  const globalCandidates = selectedSet.filter((key) => ["ga", "aco", "pso"].includes(key));
  const localCandidates = selectedSet.filter((key) => ["hybrid", "lns", "tabu", "sa"].includes(key));
  const initialKeys = initialCandidates.length ? initialCandidates : ["vrptw", "savings"];
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
  const globalPriority = globalPriorityMap[goal] || globalPriorityMap.balanced;
  const localPriority = localPriorityMap[goal] || localPriorityMap.balanced;
  const maxGlobalStages = Math.min(1, globalCandidates.length);
  const maxLocalStages = storeCount >= 90 ? 1 : storeCount >= 50 ? 2 : 2;
  const pickedGlobals = globalPriority.filter((key) => globalCandidates.includes(key)).slice(0, maxGlobalStages);
  const pickedLocals = localPriority.filter((key) => localCandidates.includes(key)).slice(0, maxLocalStages);
  const stageKeys = [...pickedGlobals, ...pickedLocals];
  return {
    initialKeys,
    stageKeys: stageKeys.length ? stageKeys : ["hybrid"],
  };
}

async function solveByVRPTW(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithVrptwBackend, 0, "vrptw");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "vrptw", label: algoLabel("vrptw"), description: algoDescription("vrptw"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByHybrid(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithHybrid, 0, "hybrid");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "hybrid", label: algoLabel("hybrid"), description: algoDescription("hybrid"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByGA(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithGA, 1, "ga");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "ga", label: algoLabel("ga"), description: algoDescription("ga"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByTabu(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithTabu, 2, "tabu");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "tabu", label: algoLabel("tabu"), description: algoDescription("tabu"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByLNS(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithLns, 4, "lns");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "lns", label: algoLabel("lns"), description: algoDescription("lns"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveBySavings(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithSavingsBackend, 3, "savings");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "savings", label: algoLabel("savings"), description: algoDescription("savings"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveBySA(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithSA, 6, "sa");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "sa", label: algoLabel("sa"), description: algoDescription("sa"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByACO(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithACO, 8, "aco");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "aco", label: algoLabel("aco"), description: algoDescription("aco"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByPSO(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithPSO, 10, "pso");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "pso", label: algoLabel("pso"), description: algoDescription("pso"), solution: s, traceLog: run.traceLog || [] }, scenario);
}
async function solveByVehicle(scenario) {
  const run = await solveWaveByWaveWithOptimizer(scenario, optimizeWaveWithVehicleDrivenBackend, 12, "vehicle");
  const s = run.solution;
  return applyFinalRuleToResult({ key: "vehicle", label: algoLabel("vehicle"), description: algoDescription("vehicle"), solution: s, traceLog: run.traceLog || [] }, scenario);
}

async function solveByRelay(scenario, selectedKeys = [], baseResult = null) {
  const selectedSet = [...new Set((selectedKeys || []).filter(Boolean))];
  const relayPlan = buildRelayStagePlan(selectedSet, state.settings.optimizeGoal, scenario.stores.length);
  const initialKeys = relayPlan.initialKeys;
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
  const traceLog = [];
  const relayLog = (text) => appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${text}`);
  relayStageReporter = relayLog;
  try {
  pushTraceEvent(traceLog, {
    algorithmKey: "relay",
    scope: "wave",
    stage: "relay-start",
    textZh: `鎺ュ姏姹傝В鍚姩锛氬垵鎺掗樁娈?${initialKeys.map((key) => algoLabel(key)).join(" / ")}锛屽悗缁樁娈?${stageKeys.length ? stageKeys.map((key) => algoLabel(key)).join(" -> ") : "鏃? }銆俙,
    textJa: `銉儸銉兼眰瑙ｃ倰闁嬪銇椼伨銇欍€傚垵鏈熸闅庛伅 ${initialKeys.map((key) => algoLabel(key)).join(" / ")}銆佸緦缍氭闅庛伅 ${stageKeys.length ? stageKeys.map((key) => algoLabel(key)).join(" -> ") : "銇仐"} 銇с仚銆俙,
  });
  relayLog(`鎺ュ姏姹傝В鍚姩銆傜涓€闃舵鍏堜粠 ${initialKeys.map((key) => algoLabel(key)).join(" / ")} 閲屾寫涓€涓洿濂界殑鍒濇帓鏂规銆傚悗缁笉浼氭妸鎵€鏈夌畻娉曟満姊拌窇婊★紝鑰屾槸鎸夊綋鍓嶇洰鏍囨寫鏈€鍊煎緱鎺ユ鐨勫嚑妫掋€俙);
  relayLog(`涓嬮潰鏃ュ織閲屾彁鍒扮殑鈥滄尝娆″唴閮ㄤ唬浠封€濓紝涓嶆槸鏈€鍚庣粰瀹㈡埛鐪嬬殑缁煎悎璇勫垎锛岃€屾槸绠楁硶鍐呴儴姣旇緝璺嚎浼樺姡鐨勫昂瀛愩€傚畠涓昏鐢遍噷绋嬫垚鏈€佹櫄鍒扮綒鍒嗐€佽秴鍏佽鍋忓樊缃氬垎銆佹尝娆¤秴鏃剁綒鍒嗐€佽溅杈嗙画璺戠綒鍒嗐€侀澶栬稛娆＄綒鍒嗭紝鍐嶅噺鍘昏杞芥姷鎵ｇ粍鎴愩€俙);

  let current = null;
  if (baseResult?.solution?.length) {
    current = applyFinalRuleToResult({
      key: "relay-seed",
      label: algoLabel("relay"),
      description: algoDescription("relay"),
      solution: baseResult.solution,
      traceLog: baseResult.traceLog || [],
    }, scenario);
    relayLog(`妫€娴嬪埌浣犱笂涓€杞凡缁忔湁鍙敤鏂规锛屾墍浠ヨ繖涓€杞帴鍔涚洿鎺ヤ粠鐜版湁缁撴灉璧疯窇锛屼笉鍐嶉噸澶嶉噸寤哄垵鎺掋€傚綋鍓嶈捣璺戞寚鏍囷細${relayMetricSummary(current.metrics)}銆俙);
  } else {
    for (let index = 0; index < initialKeys.length; index += 1) {
      const key = initialKeys[index];
      relayLog(`姝ｅ湪鎵ц鍒濇帓鍊欓€?${algoLabel(key)}锛岀洰鐨勬槸鍏堟嬁鍒颁竴鐗堣兘鐢ㄧ殑鍩虹鏂规銆俙);
      const candidate = await ({ vrptw: solveByVRPTW, savings: solveBySavings }[key])(scenario);
      if (!current || candidate.metrics.score > current.metrics.score) current = candidate;
      relayLog(`${algoLabel(key)} 瀹屾垚銆?{relayMetricSummary(candidate.metrics)}銆俙);
      pushTraceEvent(traceLog, {
        algorithmKey: "relay",
        scope: "wave",
        stage: "relay-initial",
        waveId: scenario.waves[0]?.waveId || "ALL",
        textZh: `鍒濇帓鍊欓€?${algoLabel(key)} 瀹屾垚锛岃瘎鍒?${candidate.metrics.score.toFixed(1)}锛?{current.key === candidate.key ? "鏆傚垪褰撳墠鎺ュ姏棣栨" : "鏈秴杩囧綋鍓嶉妫?}銆俙,
        textJa: `鍒濇湡鍊欒 ${algoLabel(key)} 銇屽畬浜嗐仐銆併偣銈炽偄銇?${candidate.metrics.score.toFixed(1)}銆?{current.key === candidate.key ? "鐝惧湪銇涓€璧拌€呫仺銇椼仸鎺＄敤" : "鐝惧湪銇厛闋銇董鎸?}銆俙,
      });
      await cooperativeYield();
    }
  }
  if (!current) return applyFinalRuleToResult({ key: "relay", label: algoLabel("relay"), description: algoDescription("relay"), solution: [], traceLog }, scenario);
  relayLog(`鍒濇帓闃舵缁撴潫锛屽綋鍓嶆帴鍔涢妫掓槸 ${algoLabel(current.key)}銆傞妫掔殑鍏抽敭鎸囨爣鏄細${relayMetricSummary(current.metrics)}銆傛帴涓嬫潵杩涘叆缁х画浼樺寲闃舵锛屽叡璁″垝 ${stageKeys.length} 妫掋€俙);

  let noImproveStages = 0;
  for (let index = 0; index < stageKeys.length; index += 1) {
    const key = stageKeys[index];
    relayLog(`绗?${index + 2} 闃舵鐢?${algoLabel(key)} 鎺ユ銆傚畠浼氬湪褰撳墠鏈€濂芥柟妗堝熀纭€涓婄户缁壘鏇村ソ鐨勬帓娉曘€俙);
    const beforeMetrics = current.metrics;
    const improved = await improveSolutionByWaveOptimizer(current.solution, scenario, optimizerMap[key], 30 + index);
    const metrics = evaluateSolution(improved.solution, scenario, computeFinalPendingByWave(improved.solution, scenario));
    const scoreGain = metrics.score - current.metrics.score;
    const accepted = scoreGain > 1e-6;
    const materialGain = scoreGain >= 0.25
      || (metrics.unscheduledCount || 0) < (beforeMetrics.unscheduledCount || 0)
      || (metrics.totalDistance || 0) + 3 < (beforeMetrics.totalDistance || 0)
      || (metrics.totalOnTime || 0) > (beforeMetrics.totalOnTime || 0);
    relayLog(`${algoLabel(key)} 鏈疆缁撴灉锛?{relayMetricSummary(metrics)}銆傜浉姣斾笂涓€妫掞紝${describeRelayMetricDelta(beforeMetrics, metrics)}銆?{accepted ? `鍥犱负鏂板垎鏁拌秴杩囧綋鍓?${beforeMetrics.score.toFixed(1)}锛屾墍浠ヨ繖涓€妫掓寮忔帴妫掋€俙 : `鍥犱负鏂板垎鏁版病鏈夎秴杩囧綋鍓?${beforeMetrics.score.toFixed(1)}锛屾墍浠ヨ繖涓€妫掓病鏈夋帴杩囧幓銆俙}`);
    pushTraceEvent(traceLog, {
      algorithmKey: "relay",
      scope: "wave",
      stage: "relay-stage",
      waveId: scenario.waves[0]?.waveId || "ALL",
      textZh: `${index + 2} 闃舵鐢?${algoLabel(key)} 鎺ユ锛屽€欓€夎瘎鍒?${metrics.score.toFixed(1)}锛?{accepted ? `浼樹簬褰撳墠 ${current.metrics.score.toFixed(1)}锛屾寮忔帴妫抈 : `鏈秴杩囧綋鍓?${current.metrics.score.toFixed(1)}锛屼繚鐣欏師鏂规`}銆俙,
      textJa: `${index + 2} 娈甸殠銇?${algoLabel(key)} 銇屽紩銇嶇稒銇庛€佸€欒銈广偝銈?${metrics.score.toFixed(1)}銆?{accepted ? `鐝捐 ${current.metrics.score.toFixed(1)} 銈掍笂鍥炪仯銇熴仧銈佹帯鐢╜ : `鐝捐 ${current.metrics.score.toFixed(1)} 銈掕秴銇堛仛鍏冩銈掔董鎸乣}銆俙,
    });
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
        relayLog(`杩欎竴妫掕櫧鐒跺垎鏁扮暐鏈変笂鍗囷紝浣嗘彁鍗囧緢鏈夐檺銆傛垜浼氭妸瀹冩帴浣忥紝鍚屾椂寮€濮嬪叧娉ㄦ槸鍚﹁鎻愬墠鏀跺伐锛岄伩鍏嶇┖鑰楁椂闂淬€俙);
      }
    } else {
      noImproveStages += 1;
    }
    mergeTraceLogs(traceLog, improved.traceLog || []);
    if (noImproveStages >= 2 && index < stageKeys.length - 1) {
      relayLog(`杩炵画 ${noImproveStages} 妫掓病鏈夊甫鏉ュ疄璐ㄦ彁鍗囷紝鍚庨潰鐨勬帴鍔涘厛鎻愬墠鍋滄锛岄伩鍏嶇户缁┖杞€楁椂銆俙);
      pushTraceEvent(traceLog, {
        algorithmKey: "relay",
        scope: "wave",
        stage: "relay-early-stop",
        waveId: scenario.waves[0]?.waveId || "ALL",
        textZh: `杩炵画 ${noImproveStages} 涓樁娈垫湭甯︽潵瀹炶川鎻愬崌锛屾帴鍔涙彁鍓嶆敹宸ャ€俙,
        textJa: `${noImproveStages} 娈甸殠閫ｇ稓銇у疅璩敼鍠勩亴鍑恒仾銇嬨仯銇熴仧銈併€併儶銉兗銈掓棭鏈熺祩浜嗐仐銇俱仐銇熴€俙,
      });
      break;
    }
    await cooperativeYield();
  }

  pushTraceEvent(traceLog, {
    algorithmKey: "relay",
    scope: "wave",
    stage: "relay-finish",
    textZh: `鎺ュ姏姹傝В瀹屾垚锛屾渶缁堥噰鐢ㄦ柟妗堣瘎鍒?${current.metrics.score.toFixed(1)}銆俙,
    textJa: `銉儸銉兼眰瑙ｃ亴瀹屼簡銇椼€佹渶绲傛帯鐢ㄣ偣銈炽偄銇?${current.metrics.score.toFixed(1)} 銇с仚銆俙,
  });
  relayLog(`鎺ュ姏姹傝В瀹屾垚銆傛渶缁堟柟妗堣瘎鍒?${current.metrics.score.toFixed(1)}锛屽凡璋冨害 ${current.metrics.scheduledCount || 0} 瀹讹紝鏈皟搴?${(current.metrics.unscheduledCount || 0)} 瀹躲€俙);

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
  const issues = [];
  for (const wave of scenario.waves) {
    if (wave.relaxEnd) continue;
    for (const id of wave.storeList) {
      const store = scenario.storeMap.get(id); if (!store) continue;
      const fastest = scenario.vehicles[0]; if (!fastest) continue;
      const directDistance = scenario.dist[DC.id][store.id];
      const soloDistance = directDistance + scenario.dist[store.id][DC.id];
      const arrival = Math.max(wave.startMin, scenario.dispatchStartMin) + getTravelMinutes(scenario, DC.id, store.id, fastest.speed);
      const start = arrival;
      const leave = start + (store.actualServiceMinutes || store.serviceMinutes || 0);
      const finish = leave + getTravelMinutes(scenario, store.id, DC.id, fastest.speed);
      if (isW3WaveForSolve(wave) && directDistance > getSolveW3OneWayMaxKm(scenario)) {
        issues.push(`${wave.waveId} 鐨勯棬搴?${store.name} 鍗曠嫭鍒板簵灏辫 ${directDistance.toFixed(1)} km`);
      }
      else if ((wave.endMode || "return") === "return" ? finish > wave.endMin : leave > wave.endMin) {
        issues.push(`${wave.waveId} 鐨勯棬搴?${store.name} 鍗曠嫭鎵ц涔熶細鏅氫簬娉㈡${(wave.endMode || "return") === "return" ? "鍥炲簱" : "瀹屽簵"}鎴`);
      }
      if (issues.length >= 6) return issues;
    }
  }
  return issues;
}

function formatScheduledByWave(metrics = {}) {
  const list = Array.isArray(metrics.scheduledByWave) ? metrics.scheduledByWave : [];
  if (!list.length) return `${L("scheduledStores")} ${metrics.scheduledCount || 0}`;
  return `${L("scheduledStores")}锛?{list.map((item) => `${item.waveId || "-"}:${Number(item.count || 0)}`).join(" | ")}锛塦;
}

function renderSummary() {
  const best = [...state.lastResults].sort((a, b) => b.metrics.score - a.metrics.score)[0];
  document.getElementById("summaryCards").innerHTML = state.lastResults.map((result) => `
    <article class="metric-card ${state.activeResultKey === result.key ? "metric-card-active" : ""}">
      <p class="label">${algoLabel(result.key)}</p>
      <div class="value">${result.metrics.score.toFixed(1)}</div>
      ${Number(state.settings.targetScore || 0) > 0 ? `<p class="hint">${L("targetScore")} ${Number(state.settings.targetScore).toFixed(1)}锛?{result.metrics.score >= Number(state.settings.targetScore || 0) ? L("targetAchieved") : L("targetMissed")}</p>` : ""}
      <p class="hint">${lang() === "ja" ? "銈广偝銈伅绶忓悎瑭曚尽銇с€佹暟鍊ゃ亴楂樸亜銇汇仼鑹亜銇с仚銆? : "璇勫垎浠ｈ〃缁煎悎琛ㄧ幇锛屾暟鍊艰秺楂樿秺濂姐€?}</p>
      <p class="hint">${L("dispatchStart")} ${state.settings.dispatchStartTime}锛?{formatScheduledByWave(result.metrics)}锛?{L("unscheduledStores")} ${result.metrics.unscheduledCount}锛?{L("onTime")}鐜?${formatRate(result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1))}锛?{L("totalDistance")} ${result.metrics.totalDistance.toFixed(1)} km锛?{L("avgLoad")} ${formatRate(result.metrics.loadRate)}锛?{L("fleetLoad")} ${formatRate(result.metrics.fleetLoadRate)}锛?{lang() === "ja" ? `浣跨敤杌婁浮 ${result.metrics.usedVehicleCount} 鍙?/ 寰呮杌婁浮 ${result.metrics.unusedVehicleCount} 鍙癭 : `宸茬敤杞﹁締 ${result.metrics.usedVehicleCount} 杈?/ 鏈敤杞﹁締 ${result.metrics.unusedVehicleCount} 杈哷}</p>
      ${best && best.key !== result.key ? `<p class="hint">${lang() === "ja" ? `鐝惧湪銇渶鑹 ${algoLabel(best.key)} 銇ㄦ瘮銇广倠銇ㄣ€併偣銈炽偄宸?${Math.abs(best.metrics.score - result.metrics.score).toFixed(1)}銆佽窛闆㈠樊 ${Math.abs(best.metrics.totalDistance - result.metrics.totalDistance).toFixed(1)} km銆佷娇鐢ㄨ粖涓″樊 ${Math.abs((best.metrics.usedVehicleCount || 0) - (result.metrics.usedVehicleCount || 0))} 鍙般仹銇欍€俙 : `涓庡綋鍓嶆渶浣?${algoLabel(best.key)} 鐩告瘮锛氬垎鏁板樊 ${Math.abs(best.metrics.score - result.metrics.score).toFixed(1)}锛岄噷绋嬪樊 ${Math.abs(best.metrics.totalDistance - result.metrics.totalDistance).toFixed(1)} km锛岀敤杞﹀樊 ${Math.abs((best.metrics.usedVehicleCount || 0) - (result.metrics.usedVehicleCount || 0))} 杈嗐€俙}</p>` : ""}
      <p class="hint">${lang() === "ja" ? "100鐐瑰埗銆傛湭閬呯潃鐜?5% + 璺濋洟25% + 骞冲潎鍗樹究绌嶈級鐜?5% + 绌嶈級鍎厛閬旀垚鐜?5%銆傘倛銈婃檪闁撻€氥倞銇с€佽窛闆亴鐭亸銆佺杓夈亴鑹亜銇汇仼楂樺緱鐐广仹銇欍€? : "100鍒嗗埗銆傛湭鏅氬埌鐜?5% + 璺濈25% + 骞冲潎鍗曡稛瑁呰浇鐜?5% + 瑁呰浇鍋忓ソ杈炬垚鐜?5%銆備篃灏辨槸璇达紝瓒婂噯鏃躲€佽秺鐪侀噷绋嬨€佽秺鑳借锛屽垎鏁拌秺楂樸€?}</p>
      <p class="hint">${L("hardArrivalHint")}${state.settings.concentrateLate ? ` ${L("lateFocusHint")}` : ""}</p>
        <p class="hint">${result.metrics.unscheduledCount ? LT("unscheduledSummary", { count: result.metrics.unscheduledCount, names: formatUnscheduledDetails(result.metrics.unscheduledStores, 8) }) : L("noUnscheduled")}</p>
        ${result.metrics.unscheduledCount ? `<p class="hint">${lang() === "ja" ? `鏈壊褰撱伄涓诲洜锛?{summarizeUnscheduledReasons(result.metrics.unscheduledStores)}` : `鏈皟搴︿富鍥狅細${summarizeUnscheduledReasons(result.metrics.unscheduledStores)}`}${result.metrics.unusedVehicleCount > 0 ? (lang() === "ja" ? `銆傘仾銇婂緟姗熻粖涓°伅 ${result.metrics.unusedVehicleCount} 鍙般亗銈娿€佷富鍥犮伅杌婁浮涓嶈冻銇с伅銇亸鍒剁磩鍋淬仹銇欍€俙 : `銆傚綋鍓嶄粛鏈?${result.metrics.unusedVehicleCount} 杈嗛棽缃溅锛屼富鍥犱笉鏄溅涓嶅锛岃€屾槸绾︽潫涓嶆弧瓒炽€俙) : ""}</p>` : ""}
        <button class="secondary view-result-detail" data-result-key="${result.key}">${algoLabel(result.key)} ${L("detail")}</button>
      </article>
  `).join("");
}

function normalizeMainTab(tabKey) {
  const key = String(tabKey || "").trim();
  return MAIN_TAB_KEYS.has(key) ? key : "basic";
}

function applyMainTabVisibility() {
  const activeTab = normalizeMainTab(state.ui.mainTab);
  state.ui.mainTab = activeTab;
  document.querySelectorAll("[data-main-tab]").forEach((section) => {
    section.classList.toggle("tab-hidden", section.dataset.mainTab !== activeTab);
  });
  document.querySelectorAll("[data-main-tab-btn]").forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.mainTabBtn === activeTab);
  });
}

function setMainTab(tabKey) {
  state.ui.mainTab = normalizeMainTab(tabKey);
  applyMainTabVisibility();
}

function bindMainTabs() {
  const tabs = document.getElementById("mainTabs");
  if (!tabs) return;
  tabs.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-main-tab-btn]");
    if (!btn) return;
    setMainTab(btn.dataset.mainTabBtn);
  });
}

function ensureAnalyticsMount() {
  const summarySection = document.getElementById("summaryCards");
  const resultPanels = document.getElementById("resultPanels");
  const resultSection = resultPanels?.closest("section");
  if (!summarySection || !resultSection) return null;
  let panel = document.getElementById("analyticsPanel");
  if (panel) {
    panel.setAttribute("data-main-tab", "solve");
    if (panel.previousElementSibling !== summarySection) {
      summarySection.insertAdjacentElement("afterend", panel);
    }
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

function ensureGanttMount() {
  const resultPanels = document.getElementById("resultPanels");
  const resultSection = resultPanels?.closest("section");
  if (!resultSection) return null;
  let panel = document.getElementById("ganttPanel");
  if (panel) {
    panel.setAttribute("data-main-tab", "solve");
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
  return document.getElementById("ganttPanel");
}

function dockAssistantPanel() {
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  const assistantCard = document.querySelector("#analyticsContent .mascot-card");
  if (!assistantCard) {
    dock.innerHTML = "";
    dock.classList.add("is-empty");
    dock.classList.remove("state-collapsed", "state-half", "state-full");
    return;
  }
  const normalizeDockState = (value) => {
    const key = String(value || "").trim();
    if (key === "collapsed" || key === "half" || key === "full") return key;
    return "half";
  };
  state.ui.assistantDockState = normalizeDockState(state.ui.assistantDockState);
  dock.innerHTML = "";
  const controls = document.createElement("div");
  controls.className = "assistant-dock-controls";
  const labels = lang() === "ja"
    ? { collapsed: "鎶樸倞銇熴仧銈€", half: "绨＄増", full: "鍏ㄩ噺" }
    : { collapsed: "鎶樺彔", half: "绠€鐗?, full: "鍏ㄩ噺" };
  controls.innerHTML = `
    <span class="assistant-dock-move" title="${lang() === "ja" ? "銉夈儵銉冦偘銇椼仸绉诲嫊" : "鎷栨嫿绉诲姩"}">鈰嫯</span>
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
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  const pos = state.ui.assistantDockPosition;
  if (!pos || !Number.isFinite(pos.left) || !Number.isFinite(pos.top)) {
    dock.style.left = "";
    dock.style.top = "";
    dock.style.right = "";
    return;
  }
  const rect = dock.getBoundingClientRect();
  const maxLeft = Math.max(0, window.innerWidth - rect.width - 8);
  const maxTop = Math.max(0, window.innerHeight - rect.height - 8);
  const left = Math.max(0, Math.min(Math.round(pos.left), maxLeft));
  const top = Math.max(0, Math.min(Math.round(pos.top), maxTop));
  dock.style.left = `${left}px`;
  dock.style.top = `${top}px`;
  dock.style.right = "auto";
}

function startAssistantDockDrag(event) {
  if (!event || event.button !== 0) return;
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  const handle = event.target?.closest?.(".assistant-dock-controls, #assistantDock.state-collapsed");
  if (!handle) return;
  if (event.target?.closest?.(".assistant-dock-toggle")) return;
  const rect = dock.getBoundingClientRect();
  assistantDockDragState = {
    offsetX: event.clientX - rect.left,
    offsetY: event.clientY - rect.top,
  };
  state.ui.assistantDockPosition = { left: rect.left, top: rect.top };
  dock.classList.add("is-dragging");
  event.preventDefault();
}

function moveAssistantDockDrag(event) {
  if (!assistantDockDragState) return;
  const dock = document.getElementById("assistantDock");
  if (!dock) return;
  const rect = dock.getBoundingClientRect();
  const maxLeft = Math.max(0, window.innerWidth - rect.width - 8);
  const maxTop = Math.max(0, window.innerHeight - rect.height - 8);
  state.ui.assistantDockPosition = {
    left: Math.max(0, Math.min(event.clientX - assistantDockDragState.offsetX, maxLeft)),
    top: Math.max(0, Math.min(event.clientY - assistantDockDragState.offsetY, maxTop)),
  };
  applyAssistantDockPosition();
}

function stopAssistantDockDrag() {
  if (!assistantDockDragState) return;
  assistantDockDragState = null;
  document.getElementById("assistantDock")?.classList.remove("is-dragging");
}

function locateStoreRow(queryText) {
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return false;
  const assignmentMap = getActiveStoreAssignmentMap();
  const match = state.stores.findIndex((store) => {
    const assignment = getStoreAssignmentByRule(store, assignmentMap);
    const plate = String(assignment?.plateNo || store?.plateNo || "").toLowerCase();
    const hay = `${store.id || ""} ${store.name || ""} ${plate}`.toLowerCase();
    return hay.includes(query);
  });
  state.ui.storeLocatedIndex = match;
  renderStoresTable();
  const rowInput = document.querySelector(`input[data-kind="store"][data-index="${match}"]`);
  rowInput?.closest("tr")?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  rowInput?.focus?.();
  return match >= 0;
}

function locateVehicleRow(queryText) {
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return false;
  const match = state.vehicles.findIndex((vehicle) => {
    const hay = `${vehicle.plateNo || ""} ${vehicle.driverName || ""}`.toLowerCase();
    return hay.includes(query);
  });
  state.ui.vehicleLocatedIndex = match;
  renderVehicles();
  const rowInput = document.querySelector(`#vehicleTable input[data-kind="vehicle"][data-index="${match}"]`);
  rowInput?.closest("tr")?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  rowInput?.focus?.();
  return match >= 0;
}

function locateWaveItem(queryText) {
  const query = String(queryText || "").trim().toLowerCase();
  if (!query) return false;
  const match = state.waves.findIndex((wave) => {
    const names = getWaveStoreNameList(wave.storeIds).join(" ");
    const hay = `${wave.waveId || ""} ${names}`.toLowerCase();
    return hay.includes(query);
  });
  if (match < 0) return false;
  state.ui.waveSelectedIndex = match;
  renderWaves();
  const btn = document.querySelector(`.wave-left-item[data-wave-select-index="${match}"]`);
  btn?.scrollIntoView?.({ behavior: "smooth", block: "center" });
  return true;
}

async function saveBaseDataSnapshot(sourceModule) {
  const available = await ensureGaBackendAvailable(true);
  if (!available) {
    throw new Error("backend_unavailable");
  }
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
  const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/save`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ snapshot }),
  }, 10000);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const payload = await response.json();
  if (!payload?.ok) throw new Error(payload?.error || "archive_save_failed");
  return snapshot.id;
}

function renderDataArchivePanels() {
  const table = document.getElementById("dataArchiveTable");
  const storeTable = document.getElementById("dataArchiveStoreTable");
  const status = document.getElementById("dataArchiveQueryStatus");
  if (!table || !storeTable || !status) return;
  const items = Array.isArray(dataArchiveCache.items) ? dataArchiveCache.items : [];
  const columns = [
    { label: "妗ｆID", width: 260 },
    { label: "淇濆瓨鏃堕棿", width: 170 },
    { label: "鏉ユ簮妯″潡", width: 100 },
    { label: "闂ㄥ簵鏁?, width: 80 },
    { label: "杞﹁締鏁?, width: 80 },
    { label: "娉㈡鏁?, width: 80 },
    { label: "鎿嶄綔", width: 100 },
  ];
  const rows = items.map((item) => `
    <tr class="${dataArchiveCache.selectedId === item.id ? "located-row" : ""}">
      <td title="${escapeHtml(String(item.id || ""))}">${escapeHtml(String(item.id || ""))}</td>
      <td>${escapeHtml(String(item.createdAt || ""))}</td>
      <td>${escapeHtml(String(item.sourceModule || ""))}</td>
      <td>${Number(item.storeCount || 0)}</td>
      <td>${Number(item.vehicleCount || 0)}</td>
      <td>${Number(item.waveCount || 0)}</td>
      <td><button class="mini" data-data-archive-view="${escapeHtml(String(item.id || ""))}">鏌ョ湅</button></td>
    </tr>
  `);
  table.innerHTML = buildDataTableHtml({ tableKind: "dataArchive", columns, rows, tableClass: "data-archive-table" });
  const selected = items.find((item) => item.id === dataArchiveCache.selectedId) || items[0] || null;
  if (!selected) {
    status.textContent = "鏆傛棤鍩虹璧勬枡妗ｆ銆?;
    storeTable.innerHTML = "";
    return;
  }
  dataArchiveCache.selectedId = selected.id;
  status.textContent = `褰撳墠妗ｆ锛?{selected.id}锛岄棬搴?${selected.storeCount} 瀹躲€俙;
  const storeColumns = [
    { label: "缂栧彿", width: 110 },
    { label: "鍚嶇О", width: 220 },
    { label: "鍖哄煙", width: 120 },
    { label: "缁忓害", width: 140 },
    { label: "绾害", width: 140 },
    { label: "涓€娉㈡璐ч噺", width: 110 },
    { label: "浜屾尝娆¤揣閲?, width: 110 },
    { label: "鍐疯棌姣斾緥", width: 90 },
    { label: "鎵€灞炴尝娆?, width: 110 },
  ];
  const storeRows = (selected.stores || []).map((store) => {
    const loads = normalizeStoreWaveLoads(store || {});
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
  const dateInput = document.getElementById("dataArchiveDateInput");
  const keywordInput = document.getElementById("dataArchiveKeywordInput");
  const status = document.getElementById("dataArchiveQueryStatus");
  if (!dateInput || !keywordInput || !status) return;
  const dateValue = String(dateInput.value || todayDateKey());
  const keyword = String(keywordInput.value || "").trim().toLowerCase();
  dataArchiveCache.date = dateValue;
  dataArchiveCache.keyword = keyword;
  status.textContent = "姝ｅ湪鏌ヨ...";
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/archive/list`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: dateValue, page: 1, pageSize: 50 }),
    }, 8000);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
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
        if (!keyword) return true;
        const text = `${item.id} ${item.sourceModule} ${(item.stores || []).map((store) => `${store.id || ""} ${store.name || ""} ${store.plateNo || ""}`).join(" ")}`.toLowerCase();
        return text.includes(keyword);
      });
    dataArchiveCache.items = items;
    dataArchiveCache.selectedId = items[0]?.id || "";
    renderDataArchivePanels();
  } catch (error) {
    status.textContent = `鏌ヨ澶辫触锛?{error?.message || ""}`;
  }
}

function setWmsSyncStatus(text) {
  const el = document.getElementById("wmsSyncStatus");
  if (el) el.textContent = String(text || "");
}

function currentStoreSource() {
  const value = String(state.ui.storeDataSource || "sample").trim();
  return value === "real" ? "real" : "sample";
}

function currentVehicleSource() {
  const value = String(state.ui.vehicleDataSource || "sample").trim();
  return value === "real" ? "real" : "sample";
}

async function refreshBaseDataBySource() {
  const sample = sampleData();
  const storeSource = currentStoreSource();
  const vehicleSource = currentVehicleSource();
  let stores = [];
  let vehicles = [];

  if (storeSource === "real") {
    stores = await fetchWmsStoresFromBackend();
  } else {
    try {
      stores = await fetchStoresFromBackend();
    } catch {}
    if (!stores.length) stores = sample.stores;
  }

  if (vehicleSource === "real") {
    vehicles = await fetchWmsVehiclesFromBackend();
  } else {
    vehicles = sample.vehicles;
  }

  const resolvedMap = await fetchStoreWaveResolvedLoadMap(stores.map((item) => item?.id).filter(Boolean));
  stores = applyStoreWaveResolvedLoadsToStores(stores, resolvedMap);

  state.stores = stores;
  state.vehicles = vehicles;
  state.waves = buildAutoWaves(state.stores);
  state.lastResults = [];
  state.activeResultKey = "";
  renderAll();
  setWmsSyncStatus(`搴楅摵鏉ユ簮=${storeSource === "real" ? "鐪熷疄涓氬姟" : "鏍锋湰"}锛涜溅杈嗘潵婧?${vehicleSource === "real" ? "鐪熷疄涓氬姟" : "鏍锋湰"}锛涜揣閲?娉㈡/鏃堕棿鏉ユ簮=wms_cargo_raw_clean_snapshot + store_wave_timing_resolved(${resolvedMap.size}搴?銆俙);
}

async function refreshWmsStatus() {
  try {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/status`, {}, 5000);
    if (!response.ok) return;
    const payload = await response.json();
    if (!payload?.ok) return;
    const counts = payload.counts || {};
    const latestBatch = payload.latestBatch || {};
    const mode = latestBatch.mode || "-";
    const finishedAt = latestBatch.finished_at || latestBatch.finishedAt || "-";
    setWmsSyncStatus(`WMS鍚屾鐘舵€侊細搴楅摵${counts.shops || 0}锛岃溅杈?{counts.vehicles || 0}锛岃揣閲?{counts.cargo || 0}锛岃杞?{counts.carload || 0}锛屽埌搴?{counts.arrivaltime || 0}锛涙渶杩戞壒娆?${mode} ${finishedAt}`);
  } catch {}
}

async function triggerWmsFetch() {
  const box = document.getElementById("validationBox");
  setWmsSyncStatus("姝ｅ湪鎶撳彇WMS锛堝彧璇伙級...");
  let password = "";
  for (let i = 0; i < 2; i += 1) {
    const response = await fetchJsonWithTimeout(`${GA_BACKEND_URL}/wms/fetch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(password ? { password } : {}),
    }, 120000);
    const payload = response.ok ? await response.json() : { ok: false, error: `HTTP ${response.status}` };
    if (payload?.needPassword) {
      const promptText = i === 0 ? "璇疯緭鍏MS鏁版嵁搴撳瘑鐮侊紙浠呮湰鍦板姞瀵嗕繚瀛橈級锛? : "瀵嗙爜閿欒锛岃閲嶆柊杈撳叆WMS鏁版嵁搴撳瘑鐮侊細";
      const typed = window.prompt(promptText, "");
      if (!typed) {
        setWmsSyncStatus("宸插彇娑圵MS鎶撳彇銆?);
        return;
      }
      password = String(typed || "").trim();
      continue;
    }
    if (!payload?.ok) {
      const err = payload?.error || "wms_fetch_failed";
      setWmsSyncStatus(`WMS鎶撳彇澶辫触锛?{err}`);
      if (box) box.textContent = `WMS鎶撳彇澶辫触锛?{err}`;
      return;
    }
    const tableSummary = Array.isArray(payload?.tables) ? payload.tables : [];
    const inserted = tableSummary.reduce((sum, item) => sum + Number(item?.insertedRows || 0), 0);
    const skipped = tableSummary.reduce((sum, item) => sum + Number(item?.skippedRows || 0), 0);
    setWmsSyncStatus(`WMS鎶撳彇瀹屾垚锛氭壒娆?{payload.batchId || "-"}锛屾柊澧?{inserted}锛屽幓閲嶈烦杩?{skipped}銆俙);
    if (box) box.textContent = `WMS鍙鎶撳彇瀹屾垚锛氭柊澧?{inserted}锛岃烦杩?{skipped}銆俙;
    await refreshWmsStatus();
    await refreshBaseDataBySource();
    return;
  }
  setWmsSyncStatus("WMS鎶撳彇澶辫触锛氬瘑鐮佹牎楠屾湭閫氳繃銆?);
}

function miniKpiCard(label, value, sub = "", accent = "") {
  return `<article class="kpi-card ${accent}"><p class="kpi-label">${escapeHtml(label)}</p><div class="kpi-value" data-animate-number="${escapeHtml(String(value))}">${escapeHtml(String(value))}</div>${sub ? `<p class="kpi-sub">${escapeHtml(sub)}</p>` : ""}</article>`;
}

function polarPoint(cx, cy, r, angleDeg) {
  const rad = (angleDeg - 90) * Math.PI / 180;
  return [cx + Math.cos(rad) * r, cy + Math.sin(rad) * r];
}

function collectComparisonRouteRows(result) {
  const rows = [];
  (result.solution || []).forEach((plans, waveIndex) => {
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

function renderComparisonRouteDigest(result) {
  const rows = collectComparisonRouteRows(result);
  if (!rows.length) return `<p class="algo-route-empty">${L("noChartData")}</p>`;
  return `<div class="algo-route-digest">
    <div class="algo-route-digest-head">
      <strong>${L("routeDigest")}</strong>
      <span>${L("routeDigestHint")}</span>
    </div>
    <div class="algo-route-list">
      ${rows.map((row) => `
        <div class="algo-route-row">
          <div class="algo-route-line">${escapeHtml(`${row.waveId} 路 ${row.plateNo} 路 ${L("tripLabel")}${row.tripNo}`)}</div>
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

function renderExecutiveCompare(results) {
  if (!results.length) return `<div class="chart-card wide-card"><div class="chart-title">${L("algoCompare")}</div><div class="note">${L("noChartData")}</div></div>`;
  const ranked = [...results].sort((a, b) => b.metrics.score - a.metrics.score);
  return `<div class="chart-card wide-card executive-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("algoCompare")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "绶忓垎銉绘檪闁撻爢瀹堛兓璺濋洟銉荤杓夈伄4瑕栫偣銇ф瘮杓冦仐銇俱仚銆? : "浠庢€诲垎銆佹椂鏁堛€佽窛绂汇€佽杞藉洓涓搴︾湅绠楁硶宸紓銆?}</p>
      </div>
    </div>
    <div class="algo-board">
      ${ranked.map((result, idx) => {
        const score = result.metrics.score || 0;
        const onTime = (result.metrics.scoreBreakdown?.onTimeRatio || 0) * 100;
        const distance = (result.metrics.scoreBreakdown?.distanceScore || 0) * 100;
        const load = (result.metrics.scoreBreakdown?.loadScore || 0) * 100;
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

function renderComparisonBars(results) {
  if (!results.length) return "";
  const max = Math.max(...results.map((r) => r.metrics.score), 1);
  return `<div class="chart-card wide-card"><div class="chart-title">${L("algorithmScore")}</div>${results.map((result, idx) => `
    <div class="bar-row">
      <span class="bar-label">${escapeHtml(algoLabel(result.key))}</span>
      <div class="bar-track"><div class="bar-fill series-${idx % 5}" style="width:${(result.metrics.score / max) * 100}%"></div></div>
      <strong>${result.metrics.score.toFixed(1)}</strong>
    </div>
  `).join("")}</div>`;
}

function collectGanttRows(result) {
  const rows = [];
  result.solution.forEach((plans, waveIndex) => {
    const wave = result.scenario.waves[waveIndex];
    plans.forEach((plan) => {
      plan.trips.forEach((trip) => {
        const firstStop = trip.stops[0];
        const serviceEnd = trip.stops[trip.stops.length - 1]?.leave ?? trip.finish;
        rows.push({
          label: `${plan.vehicle.plateNo} 路 ${wave.waveId} 路 ${L("tripLabel")}${trip.tripNo}`,
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

function renderGantt(result) {
  const rows = collectGanttRows(result);
  if (!rows.length) return `<div class="chart-card"><div class="chart-title">${L("gantt")}</div><div class="note">${L("noChartData")}</div></div>`;
  const minTime = Math.min(...rows.map((r) => r.start));
  const maxTime = Math.max(...rows.map((r) => r.end));
  const span = Math.max(60, maxTime - minTime);
  const svgHeight = 60 + rows.length * 34;
  const rowSvg = rows.map((row, index) => {
    const y = 32 + index * 34;
    const x = 150 + ((row.start - minTime) / span) * 760;
    const w = Math.max(12, ((row.end - row.start) / span) * 760);
    return `<g>
      <text x="10" y="${y + 14}" class="gantt-label">${escapeHtml(row.label)}</text>
      <rect x="${x}" y="${y}" width="${w}" height="20" rx="8" class="gantt-bar ${row.waveType === "single" ? "single" : "regular"}" />
      ${row.overtime > 0 ? `<rect x="${x + w - 10}" y="${y}" width="10" height="20" rx="0" class="gantt-overtime"/>` : ""}
    </g>`;
  }).join("");
  const ticks = Array.from({ length: 6 }, (_, i) => {
    const value = minTime + (span / 5) * i;
    const x = 150 + (760 / 5) * i;
    return `<g><line x1="${x}" y1="12" x2="${x}" y2="${svgHeight - 10}" class="gantt-grid"/><text x="${x - 12}" y="12" class="gantt-tick">${formatTime(value)}</text></g>`;
  }).join("");
  const overtimeCount = rows.filter((row) => row.overtime > 0).length;
  const singleCount = rows.filter((row) => row.waveType === "single").length;
  return `<div class="chart-card wide-card gantt-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("gantt")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "杌婁浮脳娉㈡脳渚裤伄鏅傞枔閰嶇疆銈掍竴鐪笺仹纰鸿獚銇с亶銇俱仚銆? : "鎸夎溅杈?脳 娉㈡ 脳 瓒熸鏌ョ湅鏃堕棿鍗犵敤涓庤秴鏃朵綅缃€?}</p>
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
  const candidates = result.solution.flat().filter((plan) => plan.trips.length).slice(0, 8);
  if (!candidates.length) return `<div class="chart-card"><div class="chart-title">${L("loadCurve")}</div><div class="note">${L("noChartData")}</div></div>`;
  const series = candidates.map((plan, idx) => {
    const points = [{ x: 0, y: 0 }];
    let step = 1;
    plan.trips.forEach((trip) => {
      trip.stops.forEach((stop) => {
        const load = trip.stops.slice(0, trip.stops.indexOf(stop) + 1).reduce((sum, item) => {
          const stopStore = result.scenario.storeMap.get(item.storeId);
          return sum + getStoreSolveLoadForWave(stopStore, { waveId: plan.waveId });
        }, 0);
        points.push({ x: step, y: Math.min(1, load / Math.max(getVehicleSolveCapacity(plan.vehicle), 1)) });
        step += 1;
      });
    });
    return { label: plan.vehicle.plateNo, points, cls: `series-${idx % 5}` };
  });
  const maxX = Math.max(...series.map((s) => s.points[s.points.length - 1]?.x || 1), 1);
  const lines = series.map((s) => {
    const d = s.points.map((p, i) => `${i ? "L" : "M"} ${40 + (p.x / maxX) * 500} ${220 - p.y * 170}`).join(" ");
    return `<g><path d="${d}" class="load-path ${s.cls}"/><text x="560" y="${40 + series.indexOf(s) * 18}" class="load-legend ${s.cls}">${escapeHtml(s.label)}</text></g>`;
  }).join("");
  return `<div class="chart-card"><div class="chart-title">${L("loadCurve")}</div><svg viewBox="0 0 640 250" class="load-svg">
    <line x1="40" y1="220" x2="560" y2="220" class="axis-line"/><line x1="40" y1="20" x2="40" y2="220" class="axis-line"/>
    <text x="8" y="30" class="axis-text">100%</text><text x="12" y="220" class="axis-text">0%</text><text x="520" y="242" class="axis-text">${escapeHtml(L("timeAxis"))}</text>
    ${lines}
  </svg></div>`;
}

function renderSpatialScatter(result) {
  const stores = result.scenario.stores;
  if (!stores.length) return `<div class="chart-card"><div class="chart-title">${L("spatial")}</div><div class="note">${L("noChartData")}</div></div>`;
  const lngs = [DC.lng, ...stores.map((s) => s.lng)];
  const lats = [DC.lat, ...stores.map((s) => s.lat)];
  const minLng = Math.min(...lngs), maxLng = Math.max(...lngs), minLat = Math.min(...lats), maxLat = Math.max(...lats);
  const normX = (lng) => 40 + ((lng - minLng) / Math.max(0.0001, maxLng - minLng)) * 560;
  const normY = (lat) => 240 - ((lat - minLat) / Math.max(0.0001, maxLat - minLat)) * 180;
  const singleSet = new Set(result.scenario.singleWaveStoreIds || []);
  const points = stores.map((store) => {
    const cls = singleSet.has(store.id) ? "single" : ((result.scenario.dist[DC.id][store.id] || 0) > result.scenario.singleWaveThreshold ? "far" : "near");
    return `<g><circle cx="${normX(store.lng)}" cy="${normY(store.lat)}" r="${singleSet.has(store.id) ? 7 : 5}" class="scatter-point ${cls}"/><text x="${normX(store.lng) + 8}" y="${normY(store.lat) - 8}" class="scatter-label">${escapeHtml(store.id)}</text></g>`;
  }).join("");
  const dc = `<g><circle cx="${normX(DC.lng)}" cy="${normY(DC.lat)}" r="9" class="scatter-dc"/><text x="${normX(DC.lng) + 12}" y="${normY(DC.lat) - 10}" class="scatter-label">${escapeHtml(L("depot"))}</text></g>`;
  return `<div class="chart-card wide-card"><div class="chart-title">${L("spatial")}</div><svg viewBox="0 0 640 280" class="scatter-svg">
    <rect x="20" y="20" width="590" height="220" rx="18" class="scatter-bg"/>
    ${dc}${points}
  </svg>
  <div class="legend-row"><span class="legend-item"><i class="legend-dot near"></i>${L("scatterNear")}</span><span class="legend-item"><i class="legend-dot far"></i>${L("scatterFar")}</span><span class="legend-item"><i class="legend-dot single"></i>${L("scatterSingle")}</span></div>
  </div>`;
}

function renderDashboard(result) {
  const overtimeTrips = result.solution.flat().flatMap((plan) => plan.trips).filter((trip) => (trip.waveLateMinutes || 0) > 0).length;
  const onTimeRatio = result.metrics.totalOnTime / Math.max(result.metrics.totalStops, 1);
  const scoreState = result.metrics.score >= 80 ? "state-good" : result.metrics.score >= 60 ? "state-warn" : "state-bad";
  const onTimeState = onTimeRatio >= 0.98 ? "state-good" : onTimeRatio >= 0.9 ? "state-warn" : "state-bad";
  const distanceState = result.metrics.totalDistance <= 2300 ? "state-good" : result.metrics.totalDistance <= 2800 ? "state-warn" : "state-bad";
  const loadState = result.metrics.loadRate >= 0.65 ? "state-good" : result.metrics.loadRate >= 0.45 ? "state-warn" : "state-bad";
  const mascot = renderMascotAssistant(result, overtimeTrips);
  return `<div class="chart-card wide-card cockpit-card">
    <div class="chart-head">
      <div>
        <div class="chart-title">${L("dashboard")}</div>
        <p class="kpi-sub">${lang() === "ja" ? "褰撴棩銇厤杌婄姸娉併倰涓€鏋氥仹鎶婃彙銇欍倠绠＄悊銉撱儱銉笺仹銇欍€? : "涓€鐪兼煡鐪嬪綋鏃ヨ皟搴﹁川閲忋€佽祫婧愬崰鐢ㄥ拰鏃舵晥琛ㄧ幇銆?}</p>
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
        ${miniKpiCard(L("totalDistance"), `${result.metrics.totalDistance.toFixed(1)} km`, lang() === "ja" ? "鐭亜銇汇仼楂樿渚? : "瓒婄煭瓒婁紭", distanceState)}
        ${miniKpiCard(L("onTime"), formatRate(onTimeRatio), `${L("overtimeTripsShort")} ${overtimeTrips}`, onTimeState)}
        ${miniKpiCard(L("avgLoad"), formatRate(result.metrics.loadRate), `${algoLabel(result.key)}`, loadState)}
        ${miniKpiCard(L("fleetLoad"), formatRate(result.metrics.fleetLoadRate), L("fleetLoadHint"), result.metrics.fleetLoadRate >= 0.9 ? "state-good" : result.metrics.fleetLoadRate >= 0.65 ? "state-warn" : "state-bad")}
      </div>
      ${mascot}
    </div>
  </div>`;
}

function buildMascotSnapshot(result, overtimeTrips) {
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
        ? (lang() === "ja" ? `瓒呮檪銉┿偆銉?${overtimeTrips} 浠躲伀娉ㄦ剰銆俙 : `褰撳墠鏈?${overtimeTrips} 鏉¤秴鏃剁嚎璺渶瑕佸叧娉ㄣ€俙)
        : (lang() === "ja" ? "瓒呮檪銉┿偆銉炽伅銇傘倞銇俱仜銈撱€? : "褰撳墠娌℃湁瓒呮椂绾胯矾銆?),
      summaryChips: [
        `<span class="summary-chip">${escapeHtml(algoLabel(result.key))}</span>`,
        `<span class="summary-chip">${L("score")} ${result.metrics.score.toFixed(1)}</span>`,
      `<span class="summary-chip">${L("usedVehiclesShort")} ${result.metrics.usedVehicleCount}</span>`,
      `<span class="summary-chip">${L("fleetLoad")} ${formatRate(result.metrics.fleetLoadRate)}</span>`,
      ].join(""),
    };
  }
  return {
    score: 0,
    usedVehicleCount: state.vehicles.length,
    unusedVehicleCount: state.vehicles.length,
    totalDistance: 0,
    loadRate: 0,
    onTimeRatio: 1,
    overtimeTrips: 0,
    algorithm: lang() === "ja" ? "寰呮涓? : "寰呮満涓?,
    riskText: state.ui.micPriming
      ? L("speechMicPreparing")
      : (lang() === "ja"
          ? "銇俱仩姝ｅ紡銇皟搴︾粨鏋溿伅銇傘倞銇俱仜銈撱€傚厛銇唱鍟忋倰鑱炪亸銇撱仺銈傘€佺敓鎴愬緦銇绱勩倰鑱炪亸銇撱仺銈傘仹銇嶃伨銇欍€?
          : "褰撳墠杩樻病鏈夋寮忚皟搴︾粨鏋溿€備綘鍙互鍏堥棶鎸夐挳鐢ㄩ€旓紝鐢熸垚鍚庡啀鍚粨鏋滄憳瑕併€?),
    summaryChips: [
      `<span class="summary-chip">${escapeHtml(lang() === "ja" ? "寰呮涓? : "寰呮満涓?)}</span>`,
      `<span class="summary-chip">${L("storesToday")} ${state.stores.length}</span>`,
      `<span class="summary-chip">${L("usedVehiclesShort")} ${state.vehicles.length}</span>`,
    ].join(""),
  };
}

function renderMascotAssistant(result, overtimeTrips) {
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
  const deepseekReady = Boolean(String(state.ai.deepseekApiKey || "").trim());
  const assistantExpanded = !!state.ui.assistantExpanded;
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
            ${lang() === "ja" ? (assistantExpanded ? "鍔╂墜銈掗枆銇樸倠" : "鍔╂墜銈掗枊銇?) : (assistantExpanded ? "鏀惰捣鍔╂墜" : "鎵撳紑鍔╂墜")}
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
  if (lang() === "ja") {
    return `銉熴儎銈儕銈般優閰嶈粖瀹樸倛銈婂牨鍛娿仐銇俱仚銆傜従鍦ㄣ伄鎺ㄥエ銈儷銈淬儶銈恒儬銇?{payload.algorithm}銇с仚銆傜窂鍚堛偣銈炽偄銇?{payload.score.toFixed(1)}鐐广€備娇鐢ㄨ粖涓°伅${payload.usedVehicleCount}鍙般€佸緟姗熻粖涓°伅${payload.unusedVehicleCount}鍙般€佺窂璺濋洟銇?{payload.totalDistance.toFixed(1)}銈儹銆佸钩鍧囧崢渚跨杓夌巼銇?{Math.round(payload.loadRate * 100)}銉戙兗銈汇兂銉堛€佽粖闅婄鍍嶇巼銇?{Math.round((payload.fleetLoadRate || 0) * 100)}銉戙兗銈汇兂銉堛€佹湭閬呯潃鐜囥伅${Math.round(payload.onTimeRatio * 100)}銉戙兗銈汇兂銉堛仹銇欍€?{payload.overtimeTrips > 0 ? `銇亰銆佽秴鏅傘儵銈ゃ兂銇?{payload.overtimeTrips}浠躲亗銈娿伨銇欍€俙 : "瓒呮檪銉┿偆銉炽伅銇傘倞銇俱仜銈撱€?}`
  }
  return `椴哥暐浣垮姪鎵嬫眹鎶ャ€傚綋鍓嶆帹鑽愮畻娉曟槸${payload.algorithm}銆傜患鍚堣瘎鍒?{payload.score.toFixed(1)}鍒嗐€傚凡鐢ㄨ溅杈?{payload.usedVehicleCount}杈嗭紝闂茬疆杞﹁締${payload.unusedVehicleCount}杈嗭紝鎬婚噷绋?{payload.totalDistance.toFixed(1)}鍏噷锛屽钩鍧囧崟瓒熻杞界巼${Math.round(payload.loadRate * 100)}%锛岃溅闃熷埄鐢ㄧ巼${Math.round((payload.fleetLoadRate || 0) * 100)}%锛屾湭鏅氬埌鐜?{Math.round(payload.onTimeRatio * 100)}%銆?{payload.overtimeTrips > 0 ? `鍙﹀杩樻湁${payload.overtimeTrips}鏉¤秴鏃剁嚎璺渶瑕佸叧娉ㄣ€俙 : "褰撳墠娌℃湁瓒呮椂绾胯矾銆?}`
}

function assistantButtonCatalog() {
  return [
    {
      key: "generate",
      aliases: ["鐢熸垚璋冨害缁撴灉", "鐢熸垚缁撴灉", "寮€濮嬭皟搴?, "run", "generate", "閰嶈粖鐢熸垚", "鐢熸垚"],
      answerZh: "鈥滅敓鎴愯皟搴︾粨鏋溾€濅細鎸夊綋鍓嶉棬搴椼€佽溅杈嗐€佹尝娆°€佺畻娉曞拰鐩爣璁剧疆锛屾寮忓紑濮嬭绠楄皟搴︽柟妗堛€?,
      answerJa: "銆岀敓鎴愯皟搴︾粨鏋溿€嶃伅銆佺従鍦ㄣ伄搴楄垪銉昏粖涓°兓娉㈡銉汇偄銉偞銉偤銉犮兓鐩瑷畾銇熀銇ャ亜銇︺€佹湰鐣伄閰嶈粖瑷堢畻銈掗枊濮嬨仐銇俱仚銆?,
    },
    {
      key: "reload",
      aliases: ["閲嶆柊鍔犺浇鍥哄畾闂ㄥ簵", "閲嶆柊鍔犺浇", "鍔犺浇鏍蜂緥", "reload", "銈点兂銉椼儷鍐嶈杈?, "鍐嶅姞杞?],
      answerZh: "鈥滈噸鏂板姞杞藉浐瀹氶棬搴椻€濅細鎶婄郴缁熸仮澶嶅埌褰撳墠鍐呯疆鐨勫浐瀹氶棬搴楁牱渚嬫暟鎹€?,
      answerJa: "銆岄噸鏂板姞杞藉浐瀹氶棬搴椼€嶃伅銆佺従鍦ㄥ唴钄点仐銇︺亜銈嬪浐瀹氬簵鑸椼偟銉炽儣銉伀鎴汇仐銇俱仚銆?,
    },
    {
      key: "save",
      aliases: ["淇濆瓨褰撳墠鏂规", "淇濆瓨鏂规", "save", "淇濆瓨"],
      answerZh: "鈥滀繚瀛樺綋鍓嶆柟妗堚€濅細鎶婂綋鍓嶇粨鏋滃瓨鍒版祻瑙堝櫒鏈湴锛屾柟渚夸箣鍚庣户缁湅鎴栧姣斻€?,
      answerJa: "銆屼繚瀛樺綋鍓嶆柟妗堛€嶃伅銆佺従鍦ㄣ伄绲愭灉銈掋儢銉┿偊銈躲伄銉兗銈儷銇繚瀛樸仐銇︺€併亗銇ㄣ仹瑕嬬洿銇椼仧銈婃瘮杓冦仹銇嶃倠銈堛亞銇仐銇俱仚銆?,
    },
    {
      key: "export",
      aliases: ["瀵煎嚭褰撳墠缁撴灉", "瀵煎嚭缁撴灉", "export", "瀵煎嚭"],
      answerZh: "鈥滃鍑哄綋鍓嶇粨鏋溾€濅細鎶婂綋鍓嶉€変腑鐨勮皟搴︽柟妗堝鍑烘垚鏂囨湰缁撴灉銆?,
      answerJa: "銆屽鍑哄綋鍓嶇粨鏋溿€嶃伅銆佺従鍦ㄩ伕鎶炰腑銇厤杌婄祼鏋溿倰銉嗐偔銈广儓銇ㄣ仐銇︽浉銇嶅嚭銇椼伨銇欍€?,
    },
    {
      key: "autowave",
      aliases: ["鑷姩鍒嗘尝娆?, "鎸夊浐瀹氶棬搴楄嚜鍔ㄥ垎娉㈡", "鍒嗘尝娆?, "auto wave", "娉㈡鑷嫊鐢熸垚"],
      answerZh: "鈥滄寜鍥哄畾闂ㄥ簵鑷姩鍒嗘尝娆♀€濅細鏍规嵁褰撳墠闂ㄥ簵瑙勫垯鑷姩鐢熸垚涓€鐗堟尝娆￠厤缃€?,
      answerJa: "銆屾寜鍥哄畾闂ㄥ簵鑷姩鍒嗘尝娆°€嶃伅銆佺従鍦ㄣ伄搴楄垪銉兗銉伀鍩恒仴銇勩仸娉㈡瑷畾銈掕嚜鍕曠敓鎴愩仐銇俱仚銆?,
    },
    {
      key: "quick",
      aliases: ["蹇€熷垵鎺?, "蹇€?, "鍒濇帓", "quick"],
      answerZh: "鈥滃揩閫熷垵鎺掆€濅細鐩存帴鍒囧埌蹇€熸瀯閫犳ā寮忥紝骞堕┈涓婄敓鎴愪竴鐗堝彲鐢ㄥ垵绋裤€?,
      answerJa: "銆屽揩閫熷垵鎺掋€嶃伅銆佺礌鏃┿亜鍒濇湡妲嬬瘔銉兗銉夈伀鍒囥倞鏇裤亪銇︺€併仚銇愪娇銇堛倠鍒濈銈掍綔銈娿伨銇欍€?,
    },
    {
      key: "deep",
      aliases: ["缁х画浼樺寲", "娣卞害浼樺寲", "浼樺寲", "deep"],
      answerZh: "鈥滅户缁紭鍖栤€濅細鍒囧埌鏀硅繘鍨嬬畻娉曟ā寮忥紝鍦ㄧ幇鏈夋€濊矾涓婄户缁繁鎸栨洿濂界殑缁撴灉銆?,
      answerJa: "銆岀稒缍氭敼鍠勩€嶃伅銆佹敼鍠勭郴銈儷銈淬儶銈恒儬銇垏銈婃浛銇堛仸銆佺従鍦ㄣ伄妗堛倰銇曘倝銇（銇嶈炯銇裤伨銇欍€?,
    },
    {
      key: "global",
      aliases: ["鍏ㄥ眬鎼滅储", "鍏ㄥ眬", "global"],
      answerZh: "鈥滃叏灞€鎼滅储鈥濅細鍚敤鏇村亸鍏ㄥ眬鎺㈢储鐨勭畻娉曪紝灏濊瘯鎵惧埌瀹屽叏涓嶅悓鐨勬帓绾跨粨鏋勩€?,
      answerJa: "銆屽叏灞€鎼滅储銆嶃伅銆佸ぇ鍩熸帰绱㈠瘎銈娿伄銈儷銈淬儶銈恒儬銈掍娇銇ｃ仸銆併伨銇ｃ仧銇忛仌銇嗛厤绶氭閫犮倰鎺仐銇俱仚銆?,
    },
    {
      key: "relay",
      aliases: ["鎺ュ姏姹傝В", "鎺ュ姏", "relay"],
      answerZh: "鈥滄帴鍔涙眰瑙ｂ€濅細鍏堝仛鍒濇帓锛屽啀璁╁悗缁畻娉曚竴妫掍竴妫掓帴鐫€浼樺寲锛屽苟瀹炴椂鎵撳嵃姣忎竴闃舵鍦ㄥ仛浠€涔堛€?,
      answerJa: "銆屻儶銉兗鏈€閬╁寲銆嶃伅銆併伨銇氬垵鏈熸銈掍綔銈娿€併仢銇緦銇偄銉偞銉偤銉犮亴娈甸殠銇斻仺銇紩銇嶇稒銇勩仹鏀瑰杽銇椼€佸悇娈甸殠銇唴瀹广倐闋嗐伀琛ㄧず銇椼伨銇欍€?,
    },
    {
      key: "compare",
      aliases: ["澶氱畻娉曞姣?, "瀵规瘮", "compare", "姣旇純"],
      answerZh: "鈥滃绠楁硶瀵规瘮鈥濅細骞舵帓璺戝绉嶇畻娉曪紝鏂逛究浣犵湅鍚屼竴鎵规暟鎹笅璋佹洿濂姐€?,
      answerJa: "銆屽绠楁硶瀵规瘮銆嶃伅銆佽鏁般偄銉偞銉偤銉犮倰涓﹀垪銇蛋銈夈仜銇︺€佸悓銇樸儑銉笺偪銇с仼銈屻亴鑹亜銇嬫瘮杓冦仐銈勩仚銇忋仐銇俱仚銆?,
    },
    {
      key: "routeMap",
      aliases: ["鐪嬬嚎璺浘", "绾胯矾鍥?, "route map", "鍦板洺"],
      answerZh: "鈥滅湅绾胯矾鍥锯€濅細鎵撳紑鍙嫋鍔ㄣ€佸彲缂╂斁鐨勮矾绾垮湴鍥撅紝骞舵爣鍑洪『搴忓彿鍜屽簵鍚嶃€?,
      answerJa: "銆屻儷銉笺儓鍥炽€嶃伅銆併儔銉┿儍銈般倓鎷″ぇ绺皬銇屻仹銇嶃倠鍦板洺銈掗枊銇嶃€佽í鍟忛爢銇ㄥ簵鑸楀悕銈掕〃绀恒仐銇俱仚銆?,
    },
    {
      key: "viz",
      aliases: ["鏌ョ湅鍙鍖?, "鍙鍖?, "鍥炴斁", "playback"],
      answerZh: "鈥滄煡鐪嬪彲瑙嗗寲鈥濅細鎵撳紑杩囩▼鍥炴斁锛屽憡璇変綘绠楁硶鏄€庝箞涓€姝ヤ竴姝ユ帓鍑烘潵鐨勩€?,
      answerJa: "銆屽彲瑕栧寲銈掕銈嬨€嶃伅銆佽▓绠椼伄娴併倢銈掗枊銇嶃€併偄銉偞銉偤銉犮亴銇┿亞绲勩伩绔嬨仸銇熴亱銈掗爢銇⒑瑾嶃仹銇嶃伨銇欍€?,
    },
  ];
}

function normalizeAssistantQuery(text) {
  return String(text || "").toLowerCase().replace(/\s+/g, "");
}

function buildAssistantAnswer(queryText) {
  const normalized = normalizeAssistantQuery(queryText);
  const catalog = assistantButtonCatalog();
  const askAll = ["姣忎釜鎸夐挳", "鎵€鏈夋寜閽?, "鎸夐挳鍚嶅瓧", "鎸夐挳鍚嶇О", "鏈夊摢浜涙寜閽?, "鍏ㄩ儴鎸夐挳", "銉溿偪銉?, "銇欍伖銇︺伄銉溿偪銉?, "銉溿偪銉冲悕"].some((token) => normalized.includes(normalizeAssistantQuery(token)));
  if (askAll) {
    const lines = catalog.map((item) => lang() === "ja" ? item.answerJa : item.answerZh);
    return `${lang() === "ja" ? "銇撱伄鐢婚潰銇с倛銇忎娇銇嗐儨銈裤兂銇富銇銇€氥倞銇с仚銆? : "杩欓〉甯哥敤鎸夐挳涓昏鏈夎繖浜涖€?} ${lines.join(" ")}`;
  }
  const matched = catalog.find((item) => item.aliases.some((alias) => normalized.includes(normalizeAssistantQuery(alias))));
  if (matched) return lang() === "ja" ? matched.answerJa : matched.answerZh;
  return lang() === "ja"
    ? "璩晱銇仦銇嶅彇銈屻伨銇椼仧銇屻€併伨銇犵壒瀹氥伄銉溿偪銉炽伀绲愩伋浠樸亜銇︺亜銇俱仜銈撱€傘仧銇ㄣ亪銇般€屻偗銈ゃ儍銈垵鏈熸銇綍銈掋仚銈嬨伄銆嶃倓銆屻亾銇敾闈伄銉溿偪銉炽伅浣曘亴銇с亶銈嬨伄銆嶃仺鑱炪亜銇︺亸銇犮仌銇勩€?
    : "鎴戝惉鍒颁簡浣犵殑闂锛屼絾杩樻病鍖归厤鍒板叿浣撴寜閽€備綘鍙互鐩存帴闂紝姣斿鈥滃揩閫熷垵鎺掓槸骞蹭粈涔堢殑鈥濇垨鑰呪€滄瘡涓寜閽兘鏈変粈涔堢敤鈥濄€?;
}

function buildDeepSeekDispatchContext(result) {
  const now = new Date();
  const nowZh = now.toLocaleString("zh-CN", { hour12: false });
  const briefMode = (state.ai.answerStyle || "brief") !== "detailed";
  
  const algorithmKnowledge = {
    aco: "铓佺兢绠楁硶(ACO)锛氭ā鎷熻殏铓佽椋熺殑淇℃伅绱犳鍙嶉鏈哄埗銆傞€傚悎100-300瀹堕棬搴楃殑涓瓑瑙勬ā闂锛岃兘鑷姩鍙戠幇浼樿川璺緞妯″紡銆傜壒鐐规槸锛氫俊鎭礌鎸ュ彂鐜?.25銆?鍙殏铓併€佺簿鑻辩瓥鐣ャ€備紭鐐规槸鑳芥壘鍒板叏灞€杈冧紭瑙ｏ紝缂虹偣鏄墠鏈熸敹鏁涜緝鎱€?,
    pso: "绮掑瓙缇ょ畻娉?PSO)锛氭ā鎷熼笩缇ゆ崟椋熺殑绀句細瀛︿範琛屼负銆傞€傚悎50-150瀹堕棬搴椼€傜壒鐐规槸锛?0涓矑瀛愩€佹儻鎬ф潈閲?.7閫掑噺銆佷釜浣撴渶浼?缇や綋鏈€浼樺紩瀵笺€備紭鐐规槸鏀舵暃蹇€侀€傚悎杩炵画浼樺寲锛岀己鐐规槸鍙兘鏃╃啛銆?,
    sa: "妯℃嫙閫€鐏?SA)锛氭ā鎷熼噾灞為€€鐏繃绋嬬殑姒傜巼鎬ф悳绱€傞€傚悎浠绘剰瑙勬ā銆傜壒鐐规槸锛氬垵濮嬫俯搴?000銆侀檷娓╅€熺巼0.97銆丮etropolis鎺ュ彈鍑嗗垯銆佽嚜閫傚簲閲嶅惎銆備紭鐐规槸鑳借烦鍑哄眬閮ㄦ渶浼橈紝缂虹偣鏄弬鏁版晱鎰熴€?,
    tabu: "绂佸繉鎼滅储(Tabu)锛氬甫鐭湡璁板繂鐨勯偦鍩熸悳绱€傞€傚悎灏忚妯＄簿纭紭鍖栥€傜壒鐐规槸锛氱蹇岃〃闀垮害12銆佺壒璧﹀噯鍒欍€?0涓偦鍩熷€欓€夈€備紭鐐规槸鑳介伩鍏嶅惊鐜悳绱紝缂虹偣鏄蹇嗗紑閿€澶с€?,
    lns: "澶ч偦鍩熸悳绱?LNS)锛氱牬鍧?淇鍙岄樁娈典紭鍖栥€傞€傚悎200-500瀹堕棬搴椼€傜壒鐐规槸锛氱牬鍧忔瘮渚?.35銆佹敮鎸侀殢鏈?鏈€宸牬鍧忋€佽椽濠?閬楁喚淇銆備紭鐐规槸鑳藉ぇ瑙勬ā閲嶆瀯瑙ｏ紝缂虹偣鏄慨澶嶈川閲忎緷璧栫牬鍧忕瓥鐣ャ€?,
    hybrid: "娣峰悎绠楁硶锛氫覆鑱擲A+LNS+Tabu涓夐樁娈点€傚厛鍏ㄥ眬鎺㈢储锛屽啀灞€閮ㄧ簿淇紝鏈€鍚庢敹鍙ｃ€傞€傚悎杩芥眰鏋佽嚧璐ㄩ噺鏃朵娇鐢ㄣ€備紭鐐规槸缁煎悎鍚勭畻娉曚紭鐐癸紝缂虹偣鏄€楁椂鏈€闀裤€?,
    ga: "閬椾紶绠楁硶(GA)锛氭ā鎷熻嚜鐒堕€夋嫨鐨勮繘鍖栫畻娉曘€傞€傚悎涓瓑瑙勬ā銆傜壒鐐规槸锛氶敠鏍囪禌閫夋嫨銆侀儴鍒嗘槧灏勪氦鍙夈€佷笁绉嶅彉寮傜畻瀛愩€佺簿鑻变繚鐣欍€備紭鐐规槸鍏ㄥ眬鎼滅储鑳藉姏寮猴紝缂虹偣鏄敹鏁涙參銆?,
    savings: "Clark-Wright鑺傜害娉曪細缁忓吀鏋勯€犲惎鍙戝紡銆傛寜鑺傜害鍊煎悎骞惰矾绾匡紝蹇€熺敓鎴愬垵濮嬭В銆備紭鐐规槸閫熷害蹇€佽В璐ㄩ噺绋冲畾锛岀己鐐规槸闅剧獊鐮村眬閮ㄦ渶浼樸€?,
    vrptw: "VRPTW璐績鎻掑叆锛氭寜鏃堕棿绐椾紭鍏堟帓搴忥紝閫愪釜鎻掑叆闂ㄥ簵銆備紭鐐规槸鍙鎬уソ銆佺害鏉熷鐞嗕弗鏍硷紝缂虹偣鏄В璐ㄩ噺渚濊禆鎺掑簭銆?
  };
  
  const costFormula = `鎴愭湰鍏紡 = 鍒板簵瓒呭亸宸綒鍒?瓒?鍒嗛挓+20000) + 鏅氬埌缃氬垎(瓒?鍒嗛挓+60) + 瓒呮尝娆＄綒鍒?瓒?鍒嗛挓+80) + 杞﹁締缁窇缃氬垎(鍓嶆尝娆￠噷绋嬅?.2 + 鍓嶆尝娆¤稛鏁懊?50) + 棰濆瓒熸缃氬垎(姣忓1瓒?180) + 瓒呮椂璺嚎缃氬垎(姣忔潯+1600鎴?40) + 璺濈鎴愭湰(姣忓叕閲?.45) - 瑁呰浇濂栧姳(姣忕0.08)

瑙ｈ锛氬埌搴楄秴鍋忓樊缃氬垎鏋侀珮(20000)鎰忓懗鐫€绯荤粺浼樺厛淇濊瘉闂ㄥ簵鍑嗘椂锛涙暟鍊艰秺灏忚秺濂斤紝0鏈€鐞嗘兂銆俙;

  if (!result?.metrics) {
    return `銆愮郴缁熺姸鎬併€戝綋鍓嶆病鏈夎皟搴︾粨鏋溿€?
銆愬彲鐢ㄥ姛鑳姐€戜綘鍙互闂垜锛氳繖9绉嶇畻娉曟湁浠€涔堝尯鍒紵VRPTW鍜岃妭绾︽硶鍝釜濂斤紵铓佺兢绠楁硶鎬庝箞宸ヤ綔鐨勶紵鎴愭湰鏄€庝箞绠楃殑锛熸湭璋冨害闂ㄥ簵鎬庝箞鍔烇紵
銆愬洖绛旈鏍笺€?{briefMode ? "绠€娲佸洖绛旓紝3-6琛屽嵆鍙€? : "鍙互璇︾粏鍒嗘瀽锛屼絾瑕佹姄浣忛噸鐐广€?}
銆愬綋鍓嶆椂闂淬€?{nowZh}锛屼互姝や负鍑嗐€俙;
  }

  const waveSummaries = result.solution.map((plans, idx) => {
    const wave = result.scenario.waves[idx];
    const totalTrips = plans.reduce((s, p) => s + (p.trips?.length || 0), 0);
    const totalDist = plans.reduce((s, p) => s + (p.totalDistance || 0), 0);
    const usedVehicles = plans.filter(p => p.trips?.length).length;
    const lateTrips = plans.flatMap(p => p.trips || []).filter(t => (t.waveLateMinutes || 0) > 0).length;
    return `銆?{wave.waveId}銆?{wave.start}-${wave.end}锛氱敤杞?{usedVehicles}杈嗭紝${totalTrips}瓒燂紝閲岀▼${totalDist.toFixed(1)}km锛岃秴鏃剁嚎璺?{lateTrips}鏉°€俙;
  }).join("\n");

  const algorithmKey = result.key;
  const algorithmDesc = algorithmKnowledge[algorithmKey] || `${algorithmKey}绠楁硶锛岃鎯呰鏌ョ湅绯荤粺鏂囨。銆俙;

  return `銆愯鑹层€戜綘鏄哺鐣ヤ娇璋冨害姹傝В鍣ㄧ殑鏅鸿兘璋冨害鍔╂墜锛屼笓绮句簬VRPTW绠楁硶瑙ｈ鍜岃皟搴︾粨鏋滃垎鏋愩€?

銆愪綘鑳藉仛鐨勪簨銆?.瑙ｉ噴9绉嶈皟搴︾畻娉曠殑鍘熺悊銆佷紭缂虹偣鍜岄€傜敤鍦烘櫙 2.鍒嗘瀽褰撳墠璋冨害缁撴灉鐨勮川閲忓拰闂 3.瑙ｈ鎴愭湰鎷嗗垎鐨勫惈涔?4.寤鸿濡備綍鏀瑰杽鏈皟搴﹂棬搴?5.鍥炵瓟绠楁硶瀵规瘮銆佸弬鏁拌皟浼樼瓑闂

銆愬綋鍓嶈皟搴︾粨鏋溿€?
- 浣跨敤鐨勭畻娉? ${result.label}
- 缁煎悎璇勫垎: ${result.metrics.score.toFixed(1)}/100
- 宸茶皟搴﹂棬搴? ${result.metrics.scheduledCount || 0}瀹?
- 鏈皟搴﹂棬搴? ${result.metrics.unscheduledCount || 0}瀹?
- 鍑嗘椂鐜? ${((result.metrics.totalOnTime || 0) / Math.max(result.metrics.totalStops || 1, 1) * 100).toFixed(1)}%
- 鎬婚噷绋? ${(result.metrics.totalDistance || 0).toFixed(1)} km
- 骞冲潎瑁呰浇鐜? ${((result.metrics.loadRate || 0) * 100).toFixed(1)}%
- 杞﹂槦鍒╃敤鐜? ${((result.metrics.fleetLoadRate || 0) * 100).toFixed(1)}%
- 宸茬敤杞﹁締: ${result.metrics.usedVehicleCount || 0}杈?
- 闂茬疆杞﹁締: ${result.metrics.unusedVehicleCount || 0}杈?

銆愬悇娉㈡璇︽儏銆?
${waveSummaries}

銆愭垚鏈叕寮忋€?
${costFormula}

銆愮畻娉曠煡璇嗐€?
褰撳墠浣跨敤鐨?{result.label}锛?{algorithmDesc}

銆愭湭璋冨害闂ㄥ簵銆?
${result.metrics.unscheduledCount ? (result.metrics.unscheduledStores || []).slice(0, 10).map(s => `- ${s.storeName}(${s.storeId})锛?{s.reasonText || '绾︽潫涓嶆弧瓒?}`).join("\n") : "鏃犳湭璋冨害闂ㄥ簵"}

銆愬洖绛旇姹傘€?
1. 鍩轰簬浠ヤ笂鐪熷疄鏁版嵁鍥炵瓟锛屼笉瑕佺紪閫?
2. ${briefMode ? "鍥炵瓟瑕佺畝娲侊紝3-6琛岃娓呮牳蹇冮棶棰? : "鍙互璇︾粏鍒嗘瀽锛屼絾瑕佺粨鏋勬竻鏅?}
3. 閬囧埌鏈皟搴﹂棬搴楋紝缁欏嚭鍏蜂綋鏀瑰杽寤鸿锛堟斁瀹芥椂闂寸獥銆佽皟鏁存尝娆°€佸鍔犺溅杈嗙瓑锛?
4. 閬囧埌绠楁硶闂锛岀粨鍚堢畻娉曠壒鐐硅В閲?
5. 浣跨敤涓枃锛岃姘斾笓涓氫絾涓嶇敓纭?

銆愬綋鍓嶆椂闂淬€?{nowZh}

鐢ㄦ埛闂锛歚;
}

function buildDeepSeekGeneralContext() {
  const now = new Date();
  const nowZh = now.toLocaleString("zh-CN", { hour12: false });
  const nowJa = now.toLocaleString("ja-JP", { hour12: false });
  const briefMode = (state.ai.answerStyle || "brief") !== "detailed";
  return lang() === "ja"
    ? [
        "銇傘仾銇熴伅銇撱伄銉氥兗銈稿唴銇у嫊浣溿仚銈嬫睅鐢ㄣ偄銈枫偣銈裤兂銉堛仹銇欍€?,
        `鐝惧湪銇儹銉笺偒銉棩鏅傘伅 ${nowJa} 銇с仚銆傛棩浠樸倓鏅傚埢銇枹銇欍倠璩晱銇繀銇氥亾銈屻倰鍩烘簴銇仐銇︺亸銇犮仌銇勩€俙,
        "涓€鑸殑銇唱鍟忋伀銈傜瓟銇堛仸銈堛亜銇с仚銇屻€併亾銇儦銉笺偢涓娿伄閰嶈粖绲愭灉銇屻亗銈嬪牬鍚堛伅銆併仢銇枃鑴堛倐娲荤敤銇椼仸銇忋仩銇曘亜銆?,
        "涓嶇⒑銇嬨仾銇撱仺銇柇瀹氥仜銇氥€佺啊娼斻亱銇ゅ疅鐢ㄧ殑銇瓟銇堛仸銇忋仩銇曘亜銆?,
        briefMode
          ? "鏃㈠畾銇с伅 4 銆?8 琛岀▼搴︺伄鐭亜鍥炵瓟銇仐銆丮arkdown 銇〃銈勯暦銇勭畤鏉℃浉銇嶃伅閬裤亼銇︺亸銇犮仌銇勩€?
          : "浠婂洖銇倓銈勮┏銇椼亸绛斻亪銇︺倛銇勩仹銇欍亴銆丮arkdown 銇〃銇娇銈忋仛銆佽銇裤倓銇欍仌銈掑劒鍏堛仐銇︺亸銇犮仌銇勩€?,
      ].join("\n")
    : [
        "浣犳槸褰撳墠椤甸潰閲岀殑閫氱敤鍔╂墜銆?,
        `褰撳墠鏈湴鏃ユ湡鏃堕棿鏄?${nowZh}锛屽嚒鏄秹鍙婁粖澶╁嚑鍙枫€佺幇鍦ㄥ嚑鐐广€佹槦鏈熷嚑锛岄兘蹇呴』浠ヨ繖涓椂闂翠负鍑嗐€俙,
        "浣犲彲浠ュ洖绛斾竴鑸棶棰橈紱濡傛灉闂涓庡綋鍓嶉〉闈㈣皟搴︾粨鏋滄湁鍏筹紝涔熷彲浠ョ粨鍚堥〉闈笂涓嬫枃銆?,
        "涓嶇‘瀹氱殑浜嬩笉瑕佺‖缂栵紝鍥炵瓟灏介噺绠€娲併€佸疄鐢ㄣ€?,
        briefMode
          ? "榛樿鍙粰 4 鍒?8 琛岀殑鐭洖绛旓紝涓嶈杈撳嚭 Markdown 琛ㄦ牸鎴栬秴闀垮ぇ娈点€?
          : "杩欐鍏佽璇︾粏涓€鐐癸紝浣嗕笉瑕佽緭鍑?Markdown 琛ㄦ牸锛屼篃涓嶈杩囧害鍐楅暱銆?,
      ].join("\n");
}

function detectAssistantPageName() {
  const path = String(window.location.pathname || "").toLowerCase();
  if (path.includes("dengtang")) return "鐧诲爞";
  if (path.includes("rengong-boundary")) return "缂樿捣路鎬х┖";
  if (path.includes("rengong")) return "鍧愮収";
  if (path.includes("chaos-jianwei")) return "瑙佸井";
  if (path.includes("kuijing")) return "绐ュ緞";
  if (path.includes("tuiyan") || path.includes("chaos")) return "娣锋矊";
  const chaosPanel = document.getElementById("tuiyanPage");
  if (chaosPanel && chaosPanel.style.display !== "none" && !chaosPanel.hidden) return "娣锋矊";
  return "鍧愮収";
}

function detectAssistantDate() {
  const candidateIds = [
    "dateInput",
    "routeDateStart",
    "routeDateEnd",
    "dataArchiveDateInput",
    "archiveDateFilterInput",
  ];
  for (const id of candidateIds) {
    const el = document.getElementById(id);
    if (el && String(el.value || "").trim()) return String(el.value).trim();
  }
  const firstDateInput = document.querySelector('input[type="date"][value]:not([value=""])');
  if (firstDateInput && String(firstDateInput.value || "").trim()) return String(firstDateInput.value).trim();
  return "";
}

function detectAssistantRouteId() {
  const byState =
    (state && state.ui && (state.ui.selectedRouteId || state.ui.routeId || state.ui.currentRouteId)) || "";
  if (String(byState || "").trim()) return String(byState).trim();
  const activeRouteNode = document.querySelector("[data-route-id].active, [data-route-id].is-active, tr[data-route-id].selected");
  if (activeRouteNode) {
    const rid = activeRouteNode.getAttribute("data-route-id");
    if (rid && String(rid).trim()) return String(rid).trim();
  }
  return "";
}

function detectAssistantFilters() {
  const parts = [];
  const ids = ["routeStatusFilter", "routeCategoryFilter", "rdGroupFilter", "rdTypeFilter", "rdRouteNameFilter"];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    if (el.tagName === "SELECT" && el.multiple) {
      const values = Array.from(el.selectedOptions || [])
        .map((o) => String(o.textContent || o.value || "").trim())
        .filter(Boolean);
      if (values.length) parts.push(`${id}:${values.join("|")}`);
      return;
    }
    const value = String(el.value || "").trim();
    if (value) parts.push(`${id}:${value}`);
  });
  const activeFilterBtns = Array.from(document.querySelectorAll(".dt-filter-btn.active, .bd-filter-btn.active"))
    .map((el) => String(el.textContent || "").trim())
    .filter(Boolean);
  if (activeFilterBtns.length) parts.push(`active:${activeFilterBtns.join("|")}`);
  return parts.join("; ");
}

function buildAssistantContextBlock(questionText) {
  const q = String(questionText || "");
  const debug = /debug\s*=\s*true/i.test(q);
  const ctx = {
    page: detectAssistantPageName() || "鍧愮収",
    date: detectAssistantDate() || "",
    route_id: detectAssistantRouteId() || "",
    filters: detectAssistantFilters() || "鍏ㄩ儴",
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

async function askDeepSeekAssistant(questionText, result) {
  const apiKey = String(state.ai.deepseekApiKey || "").trim();
  if (!apiKey) throw new Error(L("deepseekMissingKey"));
  const systemPrompt = state.ai.mode === "general"
    ? buildDeepSeekGeneralContext()
    : buildDeepSeekDispatchContext(result);
  
  let enhancedQuestion = questionText;
  const lowerQuestion = questionText.toLowerCase();
  
  if (lowerQuestion.includes("绠楁硶") || lowerQuestion.includes("鍖哄埆") || lowerQuestion.includes("鍝釜濂?)) {
    enhancedQuestion = `鍏充簬绠楁硶瀵规瘮鐨勯棶棰橈細${questionText}\n\n璇风粨鍚堟垜涓婇潰鎻愪緵鐨?绉嶇畻娉曠煡璇嗗簱鏉ュ洖绛旓紝璇存槑鍚勮嚜鐗圭偣鍜岄€傜敤鍦烘櫙銆俙;
  } else if (lowerQuestion.includes("涓轰粈涔?) || lowerQuestion.includes("鍘熷洜")) {
    enhancedQuestion = `鍏充簬鍘熷洜鍒嗘瀽鐨勯棶棰橈細${questionText}\n\n璇峰熀浜庝笂闈㈡彁渚涚殑璋冨害缁撴灉鏁版嵁鏉ュ垎鏋愬師鍥狅紝缁欏嚭鍏蜂綋瑙ｉ噴銆俙;
  } else if (lowerQuestion.includes("鎬庝箞") || lowerQuestion.includes("濡備綍") || lowerQuestion.includes("寤鸿")) {
    enhancedQuestion = `鍏充簬鏀硅繘寤鸿鐨勯棶棰橈細${questionText}\n\n璇峰熀浜庡綋鍓嶆湭璋冨害闂ㄥ簵鍜岀害鏉熸儏鍐碉紝缁欏嚭鍏蜂綋鍙搷浣滅殑寤鸿銆俙;
  } else if (lowerQuestion.includes("鎴愭湰") || lowerQuestion.includes("浠ｄ环") || lowerQuestion.includes("璇勫垎")) {
    enhancedQuestion = `鍏充簬鎴愭湰璇勫垎鐨勯棶棰橈細${questionText}\n\n璇峰弬鑰冧笂闈㈢殑鎴愭湰鍏紡鏉ュ洖绛旓紝瑙ｉ噴鍚勯」缃氬垎鐨勫惈涔夈€俙;
  }
  
  const ctxPrefix = buildAssistantContextBlock(questionText);
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
    const text = await response.text();
    throw new Error(`HTTP ${response.status}${text ? ` - ${text.slice(0, 180)}` : ""}`);
  }
  const payload = await response.json();
  if (!payload?.ok) throw new Error(payload?.error || "deepseek_proxy_failed");
  let answer = String(payload?.content || "").trim();
  
  if (answer.length < 30 && (answer.includes("sorry") || answer.includes("鏃犳硶") || answer.includes("涓嶈兘"))) {
    answer = `鎶辨瓑锛屾垜鐞嗚В杩欎釜闂鍙兘鏈夊洶闅俱€俓n\n鎮ㄥ彲浠ヨ繖鏍烽棶鎴戯細\n- "涓轰粈涔堣繖娆＄敤浜?{result?.label || '褰撳墠'}绠楁硶锛?\n- "褰撳墠缁撴灉鏈変粈涔堥棶棰橈紵"\n- "鏈皟搴﹂棬搴楁€庝箞鏀瑰杽锛?\n- "ACO鍜孭SO绠楁硶鏈変粈涔堝尯鍒紵"\n\n褰撳墠璋冨害璇勫垎${result?.metrics?.score?.toFixed(1) || '?'}鍒嗭紝${result?.metrics?.unscheduledCount ? `鏈?{result.metrics.unscheduledCount}瀹堕棬搴楁湭璋冨害` : '鍏ㄩ儴闂ㄥ簵宸茶皟搴?}銆俙;
  }
  
  return answer;
}

async function submitAssistantQuestion(questionText, { speak = false } = {}) {
  const text = String(questionText || "").trim();
  const box = document.getElementById("validationBox");
  if (!text) {
    if (box) box.textContent = lang() === "ja" ? "鍏堛伀璩晱銈掑叆鍔涖仐銇︺亸銇犮仌銇勩€? : "璇峰厛杈撳叆闂銆?;
    return;
  }
  const activeResult = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0] || null;
  state.ai.lastQuestion = text;
  if (!state.ai.deepseekApiKey) {
    const localAnswer = buildAssistantAnswer(text);
    state.ai.lastAnswer = localAnswer;
    if (box) box.textContent = `${L("deepseekLocalFallback")} ${L("speechAnswerPrefix")} ${localAnswer}`;
    renderAnalytics();
    if (speak) speakAssistantAnswer(localAnswer);
    return;
  }
  state.ai.loading = true;
  state.ai.lastAnswer = "";
  renderAnalytics();
  if (box) box.textContent = L("deepseekThinking");
  try {
    const answer = await askDeepSeekAssistant(text, activeResult);
    state.ai.lastAnswer = answer || (lang() === "ja" ? "鍥炵瓟銇岀┖銇с仐銇熴€? : "杩欐鍥炵瓟鍐呭涓虹┖銆?);
    if (box) box.textContent = `${L("deepseekAnswerPrefix")} ${state.ai.lastAnswer}`;
    if (speak) speakAssistantAnswer(state.ai.lastAnswer);
  } catch (error) {
    const message = error?.message || (lang() === "ja" ? "涓嶆槑銇偍銉┿兗" : "鏈煡閿欒");
    if (box) box.textContent = LT("deepseekFailed", { message });
  } finally {
    state.ai.loading = false;
    renderAnalytics();
  }
}

function speakAssistantAnswer(text) {
  if (!("speechSynthesis" in window) || typeof SpeechSynthesisUtterance === "undefined") {
    document.getElementById("validationBox").textContent = L("speechUnsupported");
    return;
  }
  window.speechSynthesis.cancel();
  state.ui.speaking = true;
  document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.add("is-speaking"));
  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  utterance.rate = lang() === "ja" ? 0.98 : 0.92;
  utterance.pitch = 1;
  utterance.onend = () => {
    state.ui.speaking = false;
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  utterance.onerror = () => {
    state.ui.speaking = false;
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  window.speechSynthesis.speak(utterance);
}

async function ensureAssistantMicPermission() {
  if (state.ui.micPermission === "granted") return true;
  if (state.ui.micPermission === "denied") {
    const deniedBox = document.getElementById("validationBox");
    if (deniedBox) deniedBox.textContent = L("speechMicDenied");
    return false;
  }
  if (!navigator.mediaDevices?.getUserMedia) {
    return true;
  }
  if (state.ui.micPriming) return false;
  const box = document.getElementById("validationBox");
  state.ui.micPriming = true;
  renderAnalytics();
  if (box) box.textContent = L("speechMicPreparing");
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    stream.getTracks().forEach((track) => track.stop());
    state.ui.micPermission = "granted";
    return true;
  } catch (error) {
    const denied = ["NotAllowedError", "PermissionDeniedError", "SecurityError"].includes(error?.name || "");
    state.ui.micPermission = denied ? "denied" : "error";
    if (box) box.textContent = denied ? L("speechMicDenied") : L("speechMicFailed");
    return false;
  } finally {
    state.ui.micPriming = false;
    renderAnalytics();
  }
}

async function startAssistantListening() {
  const Recognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!Recognition) {
    document.getElementById("validationBox").textContent = L("speechListenUnsupported");
    return;
  }
  if (state.ui.listening || state.ui.micPriming) return;
  const micReady = await ensureAssistantMicPermission();
  if (!micReady) return;
  const box = document.getElementById("validationBox");
  state.ui.listening = true;
  renderAnalytics();
  if (box) box.textContent = L("speechListening");
  const recognition = new Recognition();
  recognition.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  recognition.interimResults = false;
  recognition.maxAlternatives = 1;
  recognition.onresult = (event) => {
    const text = event.results?.[0]?.[0]?.transcript?.trim() || "";
    state.ai.questionDraft = text;
    if (box) box.textContent = LT("speechHeard", { text });
    void submitAssistantQuestion(text, { speak: true });
  };
  recognition.onerror = (event) => {
    if (event?.error === "not-allowed" || event?.error === "service-not-allowed") {
      state.ui.micPermission = "denied";
      if (box) box.textContent = L("speechMicDenied");
      return;
    }
    if (event?.error === "aborted") {
      if (box) box.textContent = lang() === "ja" ? "銉栥儵銈︺偠銇ī闄愩儩銉冦儣銈儍銉椼仹涓€搴︿腑鏂仌銈屻伨銇椼仧銆? 绉掋伝銇╁緟銇ｃ仸銇嬨倝銈傘亞涓€搴﹁┍銇椼亱銇戙仸銇忋仩銇曘亜銆? : "鍒氬垰琚祻瑙堝櫒鏉冮檺寮圭獥鎵撴柇浜嗭紝璇风◢绛変竴绉掑悗鍐嶉棶銆?;
      return;
    }
    if (box) box.textContent = lang() === "ja" ? "闊冲０瑾嶈瓨銇け鏁椼仐銇俱仐銇熴€傚皯銇楀緟銇ｃ仸銇嬨倝銈傘亞涓€搴︺亰瑭︺仐銇忋仩銇曘亜銆? : "璇煶璇嗗埆澶辫触锛岃绋嶇瓑鐗囧埢鍚庡啀璇曘€?;
  };
  recognition.onend = () => {
    state.ui.listening = false;
    renderAnalytics();
  };
  recognition.start();
}

function triggerSpeech(payloadText) {
  if (!("speechSynthesis" in window) || typeof SpeechSynthesisUtterance === "undefined") {
    document.getElementById("validationBox").textContent = L("speechUnsupported");
    return;
  }
  let payload;
  try {
    payload = JSON.parse(payloadText);
  } catch {
    return;
  }
  window.speechSynthesis.cancel();
  state.ui.speaking = true;
  document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.add("is-speaking"));
  const utterance = new SpeechSynthesisUtterance(buildSpeechText(payload));
  utterance.lang = lang() === "ja" ? "ja-JP" : "zh-CN";
  utterance.rate = lang() === "ja" ? 0.95 : 0.9;
  utterance.pitch = 1;
  utterance.onend = () => {
    state.ui.speaking = false;
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  utterance.onerror = () => {
    state.ui.speaking = false;
    document.querySelectorAll(".mascot-avatar").forEach((node) => node.classList.remove("is-speaking"));
  };
  window.speechSynthesis.speak(utterance);
}

function animateDashboardNumbers() {
  document.querySelectorAll("[data-animate-number]").forEach((node) => {
    const raw = node.dataset.animateNumber;
    if (!raw || node.dataset.animated === raw) return;
    node.dataset.animated = raw;
    const match = String(raw).match(/-?\d+(\.\d+)?/);
    if (!match) {
      node.textContent = raw;
      return;
    }
    const target = Number(match[0]);
    const suffix = String(raw).replace(match[0], "");
    const duration = 700;
    const start = performance.now();
    const decimals = match[0].includes(".") ? match[0].split(".")[1].length : 0;
    const step = (now) => {
      const progress = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - progress, 3);
      node.textContent = `${(target * eased).toFixed(decimals)}${suffix}`;
      if (progress < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  });
}

function renderGenerationProgress() {
  const mount = document.getElementById("generationProgress");
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

function renderAnalytics() {
  const panel = ensureAnalyticsMount();
  const ganttPanel = ensureGanttMount();
  if (!panel || !ganttPanel) return;
  applyMainTabVisibility();
  document.getElementById("analyticsTitle").textContent = L("analyticsTitle");
  document.getElementById("analyticsDesc").textContent = L("analyticsDesc");
  document.getElementById("ganttTitle").textContent = L("gantt");
  document.getElementById("ganttDesc").textContent = lang() === "ja"
    ? "杌婁浮 脳 娉㈡ 脳 渚裤伄鏅傞枔閰嶇疆銈掍笅娈点仹纰鸿獚銇с亶銇俱仚銆?
    : "鍦ㄤ笅鏂瑰崟鐙煡鐪嬭溅杈?脳 娉㈡ 脳 瓒熸鐨勬椂闂村崰鐢ㄣ€?;
  renderGenerationProgress();
  const mount = document.getElementById("analyticsContent");
  const ganttMount = document.getElementById("ganttContent");
  if (!mount) return;
  if (!ganttMount) return;
  const activeResult = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  if (!activeResult) {
    mount.innerHTML = `
      <div class="chart-card wide-card cockpit-card">
        <div class="chart-head">
          <div>
            <div class="chart-title">${L("dashboard")}</div>
            <p class="kpi-sub">${lang() === "ja" ? "銇俱仩绲愭灉銇屻仾銇忋仸銈傘€併亾銇撱仹榀ㄧ暐浣裤偄銈枫偣銈裤兂銉堛倰璧峰嫊銇с亶銇俱仚銆? : "鍗充娇杩樻病鐢熸垚璋冨害缁撴灉锛屼篃鍙互鍏堝湪杩欓噷鍞よ捣椴哥暐浣垮姪鎵嬨€?}</p>
          </div>
          <div class="cockpit-highlight state-warn">
            <span>${lang() === "ja" ? "鐘舵厠" : "鐘舵€?}</span>
            <strong>${lang() === "ja" ? "寰呮涓? : "寰呮満涓?}</strong>
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

function buildTargetScoreAdvice(bestResult) {
  const target = Number(state.settings.targetScore || 0);
  if (!target || !bestResult?.metrics?.feasible) return "";
  const gap = target - bestResult.metrics.score;
  if (gap <= 0) {
    return lang() === "ja"
      ? `鐝惧湪銇渶鑹 ${algoLabel(bestResult.key)} 銇洰妯欍偣銈炽偄 ${target.toFixed(1)} 銈掗仈鎴愩仐銇︺亜銇俱仚銆俙
      : `褰撳墠鏈€浣虫柟妗?${algoLabel(bestResult.key)} 宸茶揪鍒扮洰鏍囪瘎鍒?${target.toFixed(1)}銆俙;
  }

  const assumptions = [];
  if (bestResult.metrics.totalDistance > 260) {
    assumptions.push(lang() === "ja"
      ? "閫氬父娉㈡銇窛闆笂闄愩倰灏戙仐绶┿倎銈嬨€併伨銇熴伅閬犺窛闆㈠簵鑸椼倰鍗樼嫭娉㈡銇稿銇忕Щ銇?
      : "閫傚害鏀惧鏅€氭尝娆＄殑閲岀▼涓婇檺锛屾垨鎶婃洿澶氳繙璺濈闂ㄥ簵鍒掑叆鍗曟尝娆?);
  }
  if ((bestResult.metrics.totalOnTime / Math.max(bestResult.metrics.totalStops, 1)) < 0.98) {
    assumptions.push(lang() === "ja"
      ? "娉㈡銇檪闁撳腐銈掑簝銇掋倠銆併伨銇熴伅甯屾湜鍒扮潃鏅傚埢銈掑垎鏁ｃ仌銇涖倠"
      : "鎷夊娉㈡鏃舵锛屾垨鎶婃湡鏈涘埌杈炬椂闂村垎鏁ｄ竴浜?);
  }
  if (bestResult.metrics.loadRate < 0.65) {
    assumptions.push(lang() === "ja"
      ? "杌婁浮銈掑皯銇楁笡銈夈仚銇嬨€佹墜鍕曡杌娿仹绌嶈級銈掑瘎銇涖倠"
      : "閫傚害鍑忓皯鍚敤杞﹁締锛屾垨閫氳繃鎵嬪伐璋冭溅鎶婅杞介泦涓竴浜?);
  }
  if ((bestResult.metrics.unusedVehicleCount || 0) === 0) {
    assumptions.push(lang() === "ja"
      ? "杌婁浮鏁般倰杩藉姞銇椼仸銆侀€氬父娉㈡銇亸銈娿倰銇曘倝銇钩婧栧寲銇欍倠"
      : "澧炲姞鍙敤杞﹁締锛岃繘涓€姝ユ憡骞虫櫘閫氭尝娆″帇鍔?);
  }
  if (!assumptions.length) {
    assumptions.push(lang() === "ja"
      ? "涓€閮ㄥ簵鑸椼伄甯屾湜鍒扮潃鏅傚埢銉绘尝娆°兓杌婁浮鍓插綋銈掍汉鎵嬨仹瑾挎暣銇欍倢銇般€佺洰妯欍伀杩戙仴銇戙倠鍙兘鎬с亴銇傘倞銇俱仚"
      : "閫氳繃浜哄伐寰皟閮ㄥ垎闂ㄥ簵鐨勬湡鏈涘埌杈俱€佹尝娆℃垨杞﹁締褰掑睘锛岃瘎鍒嗚繕鏈夌户缁笂鎺㈢殑绌洪棿");
  }

  return lang() === "ja"
    ? `鐝惧湪銇渶鑹銇?${algoLabel(bestResult.key)} 銇?${bestResult.metrics.score.toFixed(1)} 銇с€佺洰妯?${target.toFixed(1)} 銇俱仹銇傘仺 ${gap.toFixed(1)} 銇с仚銆傛キ鍕欏墠鎻愩倰娆°伄鏂瑰悜銇ф敼鍠勩仚銈屻伆鍒伴仈銇椼倓銇欍亸銇倞銇俱仚锛?{assumptions.join("锛?)}銆俙
    : `褰撳墠鏈€浣虫柟妗堟槸 ${algoLabel(bestResult.key)}锛岃瘎鍒?${bestResult.metrics.score.toFixed(1)}锛岃窛绂荤洰鏍?${target.toFixed(1)} 杩樺樊 ${gap.toFixed(1)}銆傚鏋滀笟鍔″墠鎻愬厑璁革紝鏈濊繖浜涙柟鍚戜紭鍖栨洿瀹规槗瀹炵幇鐩爣锛?{assumptions.join("锛?)}銆俙;
}

function renderSingleWaveInfo() {
  const info = document.getElementById("singleWaveInfo");
  if (!info) return;
  const stores = enrichStores(state.stores);
  if (!stores.length) {
    info.textContent = lang() === "ja" ? "鐝惧湪銆佸簵鑸椼儑銉笺偪銇屻亗銈娿伨銇涖倱銆? : "褰撳墠娌℃湁闂ㄥ簵鏁版嵁銆?;
    return;
  }
  info.textContent = lang() === "ja"
    ? "鍗樼嫭娉㈡銇嚜鍕曞垎娴併伅鍋滄涓仹銇欍€傘仚銇广仸妤嫏娉㈡锛圵1/W2/W3/W4锛夈仹姹傝В銇椼伨銇欍€?
    : "鍗曟尝娆¤嚜鍔ㄥ垎娴佸凡鍋滅敤銆傚綋鍓嶆墍鏈夐棬搴楅兘鎸変笟鍔℃尝娆★紙W1/W2/W3/W4锛夋眰瑙ｃ€?;
}

function applyLanguage() {
  document.documentElement.lang = lang() === "ja" ? "ja" : "zh-CN";
  document.title = lang() === "ja" ? "榀ㄧ暐浣裤偑銉椼儐銈ｃ優銈ゃ偠銉? : "椴哥暐浣胯皟搴︽眰瑙ｅ櫒";
  const tabTitles = lang() === "ja"
    ? { basic: "鍩虹璩囨枡", region: "閬嬭鍖哄畾缇?, strategy: "瑾垮害鎴︾暐", solve: "姹傝В銇ㄦ瘮杓?, result: "瑾垮害绲愭灉" }
    : { basic: "鍩虹璧勬枡", region: "杩愯鍖哄畾涔?, strategy: "璋冨害绛栫暐", solve: "姹傝В涓庡姣?, result: "璋冨害缁撴灉" };
  const tabBasicBtn = document.getElementById("mainTabBasicBtn");
  const tabRegionBtn = document.getElementById("mainTabRegionBtn");
  const tabStrategyBtn = document.getElementById("mainTabStrategyBtn");
  const tabSolveBtn = document.getElementById("mainTabSolveBtn");
  const tabResultBtn = document.getElementById("mainTabResultBtn");
  if (tabBasicBtn) tabBasicBtn.textContent = tabTitles.basic;
  if (tabRegionBtn) tabRegionBtn.textContent = tabTitles.region;
  if (tabStrategyBtn) tabStrategyBtn.textContent = tabTitles.strategy;
  if (tabSolveBtn) tabSolveBtn.textContent = tabTitles.solve;
  if (tabResultBtn) tabResultBtn.textContent = tabTitles.result;
  const hero = document.querySelector(".hero");
  if (hero) {
    const eyebrow = hero.querySelector(".eyebrow");
    if (eyebrow) eyebrow.textContent = "Cetacean Optimizer";
    hero.querySelector("h1").textContent = lang() === "ja" ? "榀ㄧ暐浣裤偑銉椼儐銈ｃ優銈ゃ偠銉? : "椴哥暐浣胯皟搴︽眰瑙ｅ櫒";
    hero.querySelector(".lead").textContent = lang() === "ja" ? "鐝惧湪銇増銇疅鐢ㄦ€с倰鍎厛銇椼€佸杌婁浮銉诲娉㈡銉诲搴楄垪鍗樻棩瑜囨暟鍥為厤閫併€佹墜鍕曡粖涓¤鏁淬€佸崢鐙尝娆″簵鑸椼€侀厤閫佸彲瑕栧寲銇蹇溿仐銇︺亜銇俱仚銆? : "褰撳墠鐗堟湰浼樺厛杩芥眰鍙敤锛屾敮鎸佸杞︺€佸娉㈡銆佸闂ㄥ簵鍗曟棩澶氭閰嶉€佽皟搴︺€佹墜宸ヨ皟杞︺€佸崟娉㈡搴楅摵锛屼互鍙婅皟搴﹁繃绋嬪彲瑙嗗寲銆?;
  }
  document.getElementById("openShowcaseBtn").textContent = L("showcase");
  document.getElementById("loadSampleBtn").textContent = L("reload");
  document.getElementById("savePlanBtn").textContent = L("save");
  const exportResultBtn = document.getElementById("exportResultBtn");
  if (exportResultBtn) exportResultBtn.textContent = L("exportResult");
  document.getElementById("toggleStorePanelBtn").textContent = document.getElementById("storePanelBody").classList.contains("collapsed") ? L("unfoldStore") : L("foldStore");
  document.getElementById("toggleVehiclePanelBtn").textContent = document.getElementById("vehiclePanelBody").classList.contains("collapsed") ? L("unfoldVehicle") : L("foldVehicle");
  document.getElementById("addStoreBtn").textContent = L("addStore");
  document.getElementById("addVehicleBtn").textContent = L("addVehicle");
  const locateText = lang() === "ja" ? "妞滅储" : "瀹氫綅";
  const saveDataText = lang() === "ja" ? "璩囨枡淇濆瓨" : "淇濆瓨璧勬枡";
  const storeImportText = lang() === "ja" ? "搴楄垪灏庡叆" : "瀵煎叆闂ㄥ簵";
  const vehicleImportText = lang() === "ja" ? "杌婁浮灏庡叆" : "瀵煎叆杞﹁締";
  const batchReserveText = lang() === "ja" ? "涓€鎷搷浣滐紙浜堢磩锛? : "鎵归噺鎿嶄綔锛堥鐣欙級";
  const waveImportText = lang() === "ja" ? "娉㈡灏庡叆" : "瀵煎叆娉㈡";
  const storeFileTrigger = document.getElementById("storeFileTrigger");
  const vehicleFileTrigger = document.getElementById("vehicleFileTrigger");
  const waveImportBtn = document.getElementById("waveImportBtn");
  const storeBatchBtn = document.getElementById("storeBatchBtn");
  const vehicleBatchBtn = document.getElementById("vehicleBatchBtn");
  const waveBatchBtn = document.getElementById("waveBatchBtn");
  const storeLocateBtn = document.getElementById("storeLocateBtn");
  const vehicleLocateBtn = document.getElementById("vehicleLocateBtn");
  const waveLocateBtn = document.getElementById("waveLocateBtn");
  const saveStoreDataBtn = document.getElementById("saveStoreDataBtn");
  const saveVehicleDataBtn = document.getElementById("saveVehicleDataBtn");
  const saveWaveDataBtn = document.getElementById("saveWaveDataBtn");
  const dataArchiveQueryBtn = document.getElementById("dataArchiveQueryBtn");
  if (storeFileTrigger) storeFileTrigger.textContent = storeImportText;
  if (vehicleFileTrigger) vehicleFileTrigger.textContent = vehicleImportText;
  if (waveImportBtn) waveImportBtn.textContent = waveImportText;
  if (storeBatchBtn) storeBatchBtn.textContent = batchReserveText;
  if (vehicleBatchBtn) vehicleBatchBtn.textContent = batchReserveText;
  if (waveBatchBtn) waveBatchBtn.textContent = batchReserveText;
  if (storeLocateBtn) storeLocateBtn.textContent = locateText;
  if (vehicleLocateBtn) vehicleLocateBtn.textContent = locateText;
  if (waveLocateBtn) waveLocateBtn.textContent = locateText;
  if (saveStoreDataBtn) saveStoreDataBtn.textContent = saveDataText;
  if (saveVehicleDataBtn) saveVehicleDataBtn.textContent = saveDataText;
  if (saveWaveDataBtn) saveWaveDataBtn.textContent = saveDataText;
  if (dataArchiveQueryBtn) dataArchiveQueryBtn.textContent = lang() === "ja" ? "鐓т細" : "鏌ヨ";
  const calcWaveLoadBtn = document.getElementById("calcWaveLoadBtn");
  if (calcWaveLoadBtn) calcWaveLoadBtn.textContent = lang() === "ja" ? "璨ㄩ噺鎻涚畻" : "璐ч噺鎶樼畻";
  const saveDualStoreLoadsBtn = document.getElementById("saveDualStoreLoadsBtn");
  if (saveDualStoreLoadsBtn) saveDualStoreLoadsBtn.textContent = lang() === "ja" ? "淇濆瓨鍙岃〃璨ㄩ噺" : "淇濆瓨鍙岃〃璐ч噺";
  const closeLoadConvertBtn = document.getElementById("closeLoadConvertModalBtn");
  if (closeLoadConvertBtn) closeLoadConvertBtn.textContent = lang() === "ja" ? "閬╃敤銇椼仸闁夈仒銈? : "搴旂敤骞跺叧闂?;
  const loadConvertTitle = document.getElementById("loadConvertModalTitle");
  if (loadConvertTitle) loadConvertTitle.textContent = lang() === "ja" ? "璨ㄩ噺鎻涚畻绲愭灉" : "璐ч噺鎶樼畻缁撴灉";
  renderWaveSolverPanel();
  const storeFile = document.getElementById("storeFile");
  const vehicleFile = document.getElementById("vehicleFile");
  setImportFileTag("store", storeFile?.files?.[0]?.name || "");
  setImportFileTag("vehicle", vehicleFile?.files?.[0]?.name || "");
  document.getElementById("addWaveBtn").textContent = L("addWave");
  document.getElementById("dispatchStartLabel").textContent = L("dispatchStart");
  document.getElementById("maxRouteKmLabel").textContent = L("maxKm");
  document.getElementById("minLoadRateLabel").textContent = L("minLoad");
  document.getElementById("targetScoreLabel").textContent = L("targetScore");
  const targetScoreInput = document.getElementById("targetScoreInput");
  if (targetScoreInput) {
    targetScoreInput.min = "0";
    targetScoreInput.max = "100";
    targetScoreInput.step = "0.1";
  }
  document.getElementById("ignoreWavesText").textContent = L("ignoreWaves");
  const concentrateLateText = document.getElementById("concentrateLateText");
  if (concentrateLateText) concentrateLateText.textContent = L("concentrateLate");
  document.getElementById("autoWaveBtn").textContent = L("autoWave");
  document.getElementById("singleWaveDistanceLabel").textContent = L("singleWaveDistance");
  document.getElementById("singleWaveStartLabel").textContent = L("singleWaveStart");
  document.getElementById("singleWaveEndLabel").textContent = L("singleWaveEnd");
  document.getElementById("singleWaveModeLabel").textContent = L("singleWaveMode");
  const solveStrategyLabel = document.getElementById("solveStrategyLabel");
  if (solveStrategyLabel) solveStrategyLabel.textContent = L("solveStrategy");
  const optimizeGoalLabel = document.getElementById("optimizeGoalLabel");
  if (optimizeGoalLabel) optimizeGoalLabel.textContent = L("optimizeGoal");
  const solveRegionSchemeLabel = document.getElementById("solveRegionSchemeLabel");
  if (solveRegionSchemeLabel) solveRegionSchemeLabel.textContent = lang() === "ja" ? "鍒嗗尯鏂规鐣彿" : "鍒嗗尯鏂规鍙?;
  document.getElementById("generateBtn").textContent = L("generate");
  document.getElementById("closeProcessModalBtn").textContent = L("close");
  document.getElementById("closeShowcaseModalBtn").textContent = L("close");
  document.getElementById("processModalTitle").textContent = L("processTitle");
  document.getElementById("showcaseModalTitle").textContent = L("showcaseTitle");
  const solveDiagnoseTitle = document.getElementById("solveDiagnoseModalTitle");
  if (solveDiagnoseTitle) solveDiagnoseTitle.textContent = lang() === "ja" ? "姹傝В鍓嶈ê鏂? : "姹傝В鍓嶈瘖鏂?;
  const closeSolveDiagnoseBtn = document.getElementById("closeSolveDiagnoseModalBtn");
  if (closeSolveDiagnoseBtn) closeSolveDiagnoseBtn.textContent = lang() === "ja" ? "闁夈仒銈? : "鍏抽棴";
  const cancelSolveDiagnoseBtn = document.getElementById("cancelSolveDiagnoseBtn");
  if (cancelSolveDiagnoseBtn) cancelSolveDiagnoseBtn.textContent = lang() === "ja" ? "姹傝В銈掋倓銈併倠" : "鍙栨秷姹傝В";
  const continueSolveDiagnoseBtn = document.getElementById("continueSolveDiagnoseBtn");
  if (continueSolveDiagnoseBtn) continueSolveDiagnoseBtn.textContent = lang() === "ja" ? "缍氥亼銇︽眰瑙? : "缁х画姹傝В";
  document.getElementById("languageSelect").value = lang();
  const setPanelHeadText = (panelId, titleText, descText = null) => {
    const panel = document.getElementById(panelId);
    if (!panel) return;
    const titleEl = panel.querySelector(".panel-head h2");
    if (titleEl && titleText) titleEl.textContent = titleText;
    if (descText !== null) {
      const descEl = panel.querySelector(".panel-head p");
      if (descEl) descEl.textContent = descText;
    }
  };
  setPanelHeadText("storeInfoPanel", L("storeInfo"), L("storeDesc"));
  setPanelHeadText("vehicleInfoPanel", L("vehicleInfo"), L("vehicleDesc"));
  setPanelHeadText("waveConfigPanel", L("waveConfig"), L("waveDesc"));
  setPanelHeadText("wmsSyncPanel", lang() === "ja" ? "WMS妤嫏鍚屾湡" : "WMS涓氬姟鎶撳彇", lang() === "ja" ? "閬犵WMS浜旇〃銈掕銇垮彇銈婂皞鐢ㄣ仹鍚屾湡銆傚簵鑸?杌婁浮銇牱鏈兓瀹熶笟鍔°倰鍒囨浛銇с亶銆佽波閲忋伅CargoQTY銈掑挤鍒堕仼鐢ㄣ仐銇俱仚銆? : "涓€閿彧璇绘姄鍙栬繙绔疻MS浜旇〃銆傚簵閾哄拰杞﹁締鍙€夋牱鏈?鐪熷疄涓氬姟锛岃揣閲忕粺涓€浣跨敤鎶撳彇鐨凜argoQTY銆?);
  setPanelHeadText("dataArchivePanel", lang() === "ja" ? "妗ｆ鐓т細" : "妗ｆ鏌ヨ", lang() === "ja" ? "鍩虹璧勬枡妗ｆ銈掔収浼氥仐銆佺矾搴︾祵搴︺倰鍚個瀹屽叏銉囥兗銈裤倰纰鸿獚銇с亶銇俱仚銆? : "鏌ヨ鍩虹璧勬枡妗ｆ锛屽苟鏌ョ湅瀹屾暣缁忕含搴﹂棬搴楁暟鎹€?);
  setPanelHeadText("strategyPanel", L("algoRun"), L("algoDesc"));
  setPanelHeadText("solveComparePanel", tabTitles.solve, lang() === "ja" ? "姹傝В瀹熻銉婚€叉崡銉汇偄銉偞銉偤銉犳瘮杓冦倰銇撱伄銈汇偗銈枫儳銉炽仹鎵便亜銇俱仚銆? : "鍦ㄦ湰鍖鸿繘琛屾眰瑙ｆ墽琛屻€佽繘搴﹁窡韪笌绠楁硶瀵规瘮銆?);
  setPanelHeadText("dispatchResultPanel", L("result"));
  const savePanel = document.getElementById("savedPlans")?.closest(".panel");
  if (savePanel) {
    const saveTitle = savePanel.querySelector(".panel-head h2");
    const saveDesc = savePanel.querySelector(".panel-head p");
    if (saveTitle) saveTitle.textContent = L("saved");
    if (saveDesc) saveDesc.textContent = L("savedDesc");
  }
  const singleWaveModeSelect = document.getElementById("singleWaveEndModeInput");
  if (singleWaveModeSelect?.options[0]) singleWaveModeSelect.options[0].text = L("returnEnd");
  if (singleWaveModeSelect?.options[1]) singleWaveModeSelect.options[1].text = L("serviceEnd");
  const algoLabels = document.querySelectorAll(".algo-box label");
  if (algoLabels[0]) algoLabels[0].lastChild.textContent = ` ${algoLabel("vrptw")}`;
  if (algoLabels[1]) algoLabels[1].lastChild.textContent = ` ${algoLabel("hybrid")}`;
  if (algoLabels[2]) algoLabels[2].lastChild.textContent = ` ${algoLabel("ga")}`;
  if (algoLabels[3]) algoLabels[3].lastChild.textContent = ` ${algoLabel("tabu")}`;
  if (algoLabels[4]) algoLabels[4].lastChild.textContent = ` ${algoLabel("lns")}`;
  if (algoLabels[5]) algoLabels[5].lastChild.textContent = ` ${algoLabel("savings")}`;
  if (algoLabels[6]) algoLabels[6].lastChild.textContent = ` ${algoLabel("sa")}`;
  if (algoLabels[7]) algoLabels[7].lastChild.textContent = ` ${algoLabel("aco")}`;
  if (algoLabels[8]) algoLabels[8].lastChild.textContent = ` ${algoLabel("pso")}`;
  if (algoLabels[9]) algoLabels[9].lastChild.textContent = ` ${algoLabel("vehicle")}`;
  const solveStrategySelect = document.getElementById("solveStrategySelect");
  if (solveStrategySelect?.options[0]) solveStrategySelect.options[0].text = L("strategyManual");
  if (solveStrategySelect?.options[1]) solveStrategySelect.options[1].text = L("strategyQuick");
  if (solveStrategySelect?.options[2]) solveStrategySelect.options[2].text = L("strategyDeep");
  if (solveStrategySelect?.options[3]) solveStrategySelect.options[3].text = L("strategyGlobal");
  if (solveStrategySelect?.options[4]) solveStrategySelect.options[4].text = L("strategyRelay");
  if (solveStrategySelect?.options[5]) solveStrategySelect.options[5].text = L("strategyFree");
  if (solveStrategySelect?.options[6]) solveStrategySelect.options[6].text = L("strategyCompare");
  if (solveStrategySelect) solveStrategySelect.value = state.settings.solveStrategy || "manual";
  const optimizeGoalSelect = document.getElementById("optimizeGoalSelect");
  if (optimizeGoalSelect?.options[0]) optimizeGoalSelect.options[0].text = L("goalBalanced");
  if (optimizeGoalSelect?.options[1]) optimizeGoalSelect.options[1].text = L("goalOnTime");
  if (optimizeGoalSelect?.options[2]) optimizeGoalSelect.options[2].text = L("goalDistance");
  if (optimizeGoalSelect?.options[3]) optimizeGoalSelect.options[3].text = L("goalVehicles");
  if (optimizeGoalSelect?.options[4]) optimizeGoalSelect.options[4].text = L("goalLoad");
  if (optimizeGoalSelect) optimizeGoalSelect.value = state.settings.optimizeGoal || "balanced";
  const quickSolveBtn = document.getElementById("quickSolveBtn");
  if (quickSolveBtn) quickSolveBtn.textContent = L("quickSolve");
  const deepOptimizeBtn = document.getElementById("deepOptimizeBtn");
  if (deepOptimizeBtn) deepOptimizeBtn.textContent = L("deepOptimize");
  const globalSearchBtn = document.getElementById("globalSearchBtn");
  if (globalSearchBtn) globalSearchBtn.textContent = L("globalSearch");
  const wmsFetchBtn = document.getElementById("wmsFetchBtn");
  if (wmsFetchBtn) wmsFetchBtn.textContent = lang() === "ja" ? "WMS鍚屾湡" : "涓€閿姄鍙朩MS";
  const runRegionSchemeCreateBtn = document.getElementById("runRegionSchemeCreateBtn");
  if (runRegionSchemeCreateBtn) runRegionSchemeCreateBtn.textContent = lang() === "ja" ? "鏂规杩藉姞" : "鏂板鏂规";
  const runRegionSchemeUpdateBtn = document.getElementById("runRegionSchemeUpdateBtn");
  if (runRegionSchemeUpdateBtn) runRegionSchemeUpdateBtn.textContent = lang() === "ja" ? "鏂规鏇存柊" : "鏇存柊鏂规";
  const runRegionSchemeDeleteBtn = document.getElementById("runRegionSchemeDeleteBtn");
  if (runRegionSchemeDeleteBtn) runRegionSchemeDeleteBtn.textContent = lang() === "ja" ? "鏂规鍓婇櫎" : "鍒犻櫎鏂规";
  const storeSourceSelect = document.getElementById("storeSourceSelect");
  if (storeSourceSelect?.options?.[0]) storeSourceSelect.options[0].text = lang() === "ja" ? "鏍锋湰" : "鏍锋湰";
  if (storeSourceSelect?.options?.[1]) storeSourceSelect.options[1].text = lang() === "ja" ? "瀹熶笟鍔? : "鐪熷疄涓氬姟";
  const vehicleSourceSelect = document.getElementById("vehicleSourceSelect");
  if (vehicleSourceSelect?.options?.[0]) vehicleSourceSelect.options[0].text = lang() === "ja" ? "鏍锋湰" : "鏍锋湰";
  if (vehicleSourceSelect?.options?.[1]) vehicleSourceSelect.options[1].text = lang() === "ja" ? "瀹熶笟鍔? : "鐪熷疄涓氬姟";
  const relaySolveBtn = document.getElementById("relaySolveBtn");
  if (relaySolveBtn) relaySolveBtn.textContent = L("relaySolve");
  const freeSolveBtn = document.getElementById("freeSolveBtn");
  if (freeSolveBtn) freeSolveBtn.textContent = L("freeSolve");
  const multiCompareBtn = document.getElementById("multiCompareBtn");
  if (multiCompareBtn) multiCompareBtn.textContent = L("multiCompare");
  const strategyHint = document.getElementById("strategyHint");
  if (strategyHint) strategyHint.textContent = buildStrategyHint();
  renderStrategyPreviewControls();
  syncAlgorithmControls();
  renderAlgorithmPool();
  renderGaBackendStatus();
  if (!state.lastResults.length && state.stores.length) {
    document.getElementById("validationBox").textContent = lang() === "ja"
      ? `鍥哄畾搴楄垪 ${state.stores.length} 浠躲倰瑾伩杈笺伩銆侀€氬父娉㈡ ${state.waves.length} 浠躲倰鐢熸垚銇椼伨銇椼仧銆俙
      : `宸插姞杞藉浐瀹氶棬搴?${state.stores.length} 瀹讹紝骞惰嚜鍔ㄥ垎鎴?${state.waves.length} 涓櫘閫氭尝娆°€俙;
  }
}

function setupHeroQuickActions() {
  const hero = document.querySelector(".hero");
  const loadBtn = document.getElementById("loadSampleBtn");
  const saveBtn = document.getElementById("savePlanBtn");
  if (!hero || !loadBtn || !saveBtn) return;
  let strip = document.querySelector(".quick-actions");
  if (!strip) {
    strip = document.createElement("section");
    strip.className = "quick-actions";
    hero.insertAdjacentElement("afterend", strip);
  }
  strip.append(loadBtn, saveBtn);
}

function ensureConcentrateLateControl() {
  if (document.getElementById("concentrateLateInput")) return;
  const ignoreWrap = document.getElementById("ignoreWavesLabel");
  if (!ignoreWrap?.parentElement) return;
  const label = document.createElement("label");
  label.id = "concentrateLateLabel";
  label.innerHTML = `<input id="concentrateLateInput" type="checkbox"> <span id="concentrateLateText"></span>`;
  ignoreWrap.insertAdjacentElement("afterend", label);
  document.getElementById("concentrateLateInput").checked = Boolean(state.settings.concentrateLate);
  const text = document.getElementById("concentrateLateText");
  if (text) text.textContent = L("concentrateLate");
}

function renderTripLegs(trip, scenario) {
  const legs = [];
  let from = DC.id;
  for (const stop of trip.stops) {
    legs.push(`${from}鈫?{stop.storeId} ${stop.distance.toFixed(1)}km`);
    from = stop.storeId;
  }
  if (trip.stops.length) legs.push(`${from}鈫?{DC.id} ${scenario.dist[from][DC.id].toFixed(1)}km`);
  return legs.join(" / ");
}

function buildProcessSteps(plan, trip, scenario) {
  const storeMap = scenario.storeMap;
  const steps = [];
  let cumulativeLoad = 0;
  let cumulativeDistance = 0;
  for (let index = 0; index < trip.stops.length; index += 1) {
    const stop = trip.stops[index];
    const store = storeMap.get(stop.storeId);
    cumulativeLoad += getStoreSolveLoadForWave(store, { waveId: plan.waveId });
    cumulativeDistance += stop.distance;
    const returnDistance = scenario.dist[stop.storeId][DC.id];
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
      route: [DC.id, ...trip.stops.slice(0, index + 1).map((item) => item.storeId)].join(" 鈫?"),
    });
  }
  return steps;
}

function traceAlgorithmKeysForResult(result) {
  const key = String(result?.key || "").trim();
  if (!key) return new Set();
  if (key === "relay") return new Set(["relay"]);
  return new Set([key]);
}

function buildAlgorithmNarrativeHint(result) {
  const key = String(result?.key || "");
  const hintsZh = {
    hybrid: "娣峰悎鍚彂寮忎細鍏堝仛鎵板姩鎺㈢储锛屽啀鍋氬眬閮ㄧ簿淇紝鏃ュ織閲嶇偣鐪嬧€滃姩浣溾€濆拰鈥滃埛鏂版渶浼樷€濊疆娆°€?,
    tabu: "绂佸繉鎼滅储浼氳褰曠蹇屽姩浣滀笌鏈€浼樺埛鏂帮紝鏃ュ織閲嶇偣鐪嬧€滈伩鍏嶅洖澶磋矾鈥濆悗鐨勬敼杩涜建杩广€?,
    lns: "澶ч偦鍩熸悳绱細鍙嶅 destroy/repair锛屾棩蹇楅噸鐐圭湅澶ф壈鍔ㄥ悗鏄惁鍑虹幇鏄捐憲闄嶆湰銆?,
    sa: "妯℃嫙閫€鐏細鍑虹幇鈥滄帴鍙楀樊瑙ｂ€濈殑闃舵锛屾棩蹇楅噸鐐圭湅娓╁害涓嬮檷鍚庡浣曢€愭鏀舵暃銆?,
    aco: "铓佺兢绠楁硶浼氭寜杞寮哄寲浼樿矾寰勶紝鏃ュ織閲嶇偣鐪嬭疆娆″唴鏈€浼樹笌鍏ㄥ眬鏈€浼樺彉鍖栥€?,
    pso: "绮掑瓙缇や細璁板綍绮掑瓙鍒锋柊鏈€浼樹笌閲嶅惎锛屾棩蹇楅噸鐐圭湅缇や綋鏈€浼樹綍鏃惰绐佺牬銆?,
    ga: "閬椾紶绠楁硶浼氳褰曚笘浠ｈ凯浠ｃ€佹敼杩涘箙搴﹀拰鎻愬墠鏀舵暃锛屾棩蹇楅噸鐐圭湅浠ｉ檯鏈€浼樺彉鍖栥€?,
    savings: "鑺傜害娉曟槸鏋勯€犲瀷绠楁硶锛屾棩蹇楅噸鐐圭湅鑺傜害鍊煎悎骞朵笌杞﹁締鍒嗛厤銆?,
    vrptw: "VRPTW 鍒濇帓鏄彲琛屾€т紭鍏堬紝鏃ュ織閲嶇偣鐪嬮棬搴楁彃鍏ユ瘮杈冧笌绾︽潫鍙鎬с€?,
  };
  const hintsJa = {
    hybrid: "銉忋偆銉栥儶銉冦儔銇簝鍩熸帰绱仺灞€鎵€鏀瑰杽銈掔祫銇垮悎銈忋仜銇俱仚銆傚嫊浣滅ó鍒ャ仺鏈€鑹洿鏂板洖銈掗噸鐐圭⒑瑾嶃仐銇俱仚銆?,
    tabu: "銈裤儢銉兼帰绱伅鎴汇倞鎵嬨倰绂併仒銇俱仚銆傛渶鑹洿鏂般伄杌岃贰銇ф敼鍠勩伄璩倰纰鸿獚銇椼伨銇欍€?,
    lns: "LNS 銇?destroy/repair 銈掔拱銈婅繑銇椼伨銇欍€傚ぇ銇嶃仾鍐嶆鎴愬緦銇敼鍠勫箙銈掗噸鐐圭⒑瑾嶃仐銇俱仚銆?,
    sa: "SA 銇竴鏅傜殑銇姡瑙ｅ彈鐞嗐亴鐧虹敓銇椼伨銇欍€傛俯搴︿綆涓嬪緦銇弾鏉熼亷绋嬨倰纰鸿獚銇椼伨銇欍€?,
    aco: "ACO 銇弽寰┿仹鑹祵璺倰寮峰寲銇椼伨銇欍€傚悇鍙嶅京銇渶鑹€ゆ帹绉汇倰纰鸿獚銇椼伨銇欍€?,
    pso: "PSO 銇矑瀛愮兢銇ф渶鑹В銈掓洿鏂般仐銇俱仚銆傜兢鏈€鑹亴鏇存柊銇曘倢銈嬪眬闈倰纰鸿獚銇椼伨銇欍€?,
    ga: "GA 銇笘浠ｃ仈銇ㄣ伀閫插寲銇椼伨銇欍€備笘浠ｆ渶鑹仺鏃╂湡鍙庢潫銇櫤鐢熺偣銈掔⒑瑾嶃仐銇俱仚銆?,
    savings: "绡€绱勬硶銇鎴愬瀷銇с仚銆傜瘈绱勫€ゃ伀銈堛倠绲卞悎銇ㄩ厤杌婄祼鏋溿倰纰鸿獚銇椼伨銇欍€?,
    vrptw: "VRPTW 鍒濇湡閰嶈粖銇彲琛屾€у劒鍏堛仹銇欍€傛尶鍏ユ瘮杓冦仺鍒剁磩鍏呰冻銈掔⒑瑾嶃仐銇俱仚銆?,
  };
  return lang() === "ja" ? (hintsJa[key] || "") : (hintsZh[key] || "");
}

function buildTraceNarrative(result, plan, trip, wave) {
  const breakdown = computePlanCostBreakdown(plan, result.scenario, wave);
  const algorithmKey = String(result?.key || "").trim();
  const supportsStoreInsertionTrace = new Set(["vrptw", "savings", "relay", "focus"]).has(algorithmKey);
  const keys = traceAlgorithmKeysForResult(result);
  const hasKeyFilter = keys.size > 0;
  const waveLogs = (result.traceLog || []).filter((entry) => {
    if (entry.waveId !== wave.waveId || entry.scope !== "wave") return false;
    if (!hasKeyFilter) return true;
    return keys.has(String(entry.algorithmKey || ""));
  });
  const traces = (result.traceLog || []).filter((entry) => {
    if (entry.waveId !== wave.waveId) return false;
    if (!(entry.chosenPlate === plan.vehicle.plateNo && entry.chosenTripNo === trip.tripNo)) return false;
    if (!hasKeyFilter) return true;
    return keys.has(String(entry.algorithmKey || ""));
  });
  const lines = [
    lang() === "ja" ? "> 鐪熴伄鎺㈢储銉偘" : "> 鐪熷疄绠楁硶鎼滅储鏃ュ織",
    lang() === "ja"
      ? "> 涓婃銇偄銉偞銉偤銉犮亴瀹熼殯銇彈鐞嗐仐銇熸帰绱偆銉欍兂銉堛€佷笅娈点伅銇撱伄渚裤伀鍏ャ倠搴楄垪銇壊褰撴瘮杓冦仹銇欍€?
      : "> 涓婇潰鍏堝睍绀虹畻娉曠湡姝ｆ墽琛屽苟鎺ュ彈鐨勬悳绱簨浠讹紝涓嬮潰鍐嶅睍绀鸿繖鏉＄嚎璺唴闂ㄥ簵鐨勬彃鍏ユ瘮杈冦€?,
    lang() === "ja"
      ? "> 銈炽偣銉堝紡: 閬呯潃銉氥儕銉儐銈?+ 娉㈡瓒呴亷銉氥儕銉儐銈?+ 杌婁浮绻佸繖銉氥儕銉儐銈?+ 杩藉姞渚裤儦銉娿儷銉嗐偅 + 璺濋洟銈炽偣銉?- 绌嶈級銉溿兗銉娿偣銆傚€ゃ亴浣庛亜銇汇仼鑹亜銇с仚銆?
      : "> 鎴愭湰鍏紡: 鏅氬埌缃氬垎 + 瓒呮尝娆＄綒鍒?+ 杞﹁締绻佸繖缃氬垎 + 棰濆瓒熸缃氬垎 + 璺濈鎴愭湰 - 瑁呰浇濂栧姳銆傛暟鍊艰秺浣庤秺濂姐€?,
    lang() === "ja"
      ? `> 鐝惧湪銉兗銉堛伄銈炽偣銉堝唴瑷? 閬呯潃銉氥儕銉儐銈?${breakdown.latenessPenalty.toFixed(1)} / 娉㈡瓒呴亷銉氥儕銉儐銈?${breakdown.waveLatePenalty.toFixed(1)} / 璺濋洟銈炽偣銉?${breakdown.distanceCost.toFixed(1)} / 绌嶈級銉溿兗銉娿偣 ${breakdown.loadBonus.toFixed(1)} / 绻佸繖銉氥儕銉儐銈?${breakdown.vehicleBusyPenalty.toFixed(1)} / 杩藉姞渚裤儦銉娿儷銉嗐偅 ${breakdown.extraTripPenalty.toFixed(1)} / 绶忋偝銈广儓 ${breakdown.totalCost.toFixed(1)}`
      : `> 褰撳墠绾胯矾鎴愭湰鎷嗗垎: 鏅氬埌缃氬垎 ${breakdown.latenessPenalty.toFixed(1)} / 瓒呮尝娆＄綒鍒?${breakdown.waveLatePenalty.toFixed(1)} / 璺濈鎴愭湰 ${breakdown.distanceCost.toFixed(1)} / 瑁呰浇濂栧姳 ${breakdown.loadBonus.toFixed(1)} / 绻佸繖缃氬垎 ${breakdown.vehicleBusyPenalty.toFixed(1)} / 棰濆瓒熸缃氬垎 ${breakdown.extraTripPenalty.toFixed(1)} / 鎬绘垚鏈?${breakdown.totalCost.toFixed(1)}`,
    "",
  ];
  const fallbackWaveLogs = (result.traceLog || []).filter((entry) => {
    if (entry.scope !== "wave") return false;
    if (!hasKeyFilter) return true;
    return keys.has(String(entry.algorithmKey || ""));
  });
  if (waveLogs.length) {
    lines.push(lang() === "ja" ? "[娉㈡銉儥銉帰绱紙褰撹┎娉㈡锛塢" : "[娉㈡绾ф悳绱紙褰撳墠娉㈡锛塢");
    waveLogs.slice(0, 28).forEach((entry) => {
      const stage = String(entry.stage || "").trim();
      const stageTag = stage ? `[${stage}] ` : "";
      const body = (lang() === "ja" ? entry.textJa : entry.textZh) || entry.textZh || entry.textJa || "";
      lines.push(`${stageTag}${body}`);
      const cb = entry?.costBreakdown || null;
      if (cb && typeof cb === "object") {
        const line = lang() === "ja"
          ? `  cost = 鍒板簵瓒呭亸宸綒鍒?${Number(cb.arrivalViolationPenalty || 0).toFixed(1)} + 鏅氬埌缃氬垎 ${Number(cb.latenessPenalty || 0).toFixed(1)} + 瓒呮尝娆＄綒鍒?${Number(cb.waveLatePenalty || 0).toFixed(1)} + 杞﹁締缁窇缃氬垎 ${Number(cb.vehicleBusyPenalty || 0).toFixed(1)} + 棰濆瓒熸缃氬垎 ${Number(cb.extraTripPenalty || 0).toFixed(1)} + 瓒呮椂璺嚎缃氬垎 ${Number(cb.lateRoutePenalty || 0).toFixed(1)} + 璺濈鎴愭湰 ${Number(cb.distanceCost || 0).toFixed(1)} - 瑁呰浇鎶垫墸 ${Number(cb.loadBonus || 0).toFixed(1)}`
          : `  cost = 鍒板簵瓒呭亸宸綒鍒?${Number(cb.arrivalViolationPenalty || 0).toFixed(1)} + 鏅氬埌缃氬垎 ${Number(cb.latenessPenalty || 0).toFixed(1)} + 瓒呮尝娆＄綒鍒?${Number(cb.waveLatePenalty || 0).toFixed(1)} + 杞﹁締缁窇缃氬垎 ${Number(cb.vehicleBusyPenalty || 0).toFixed(1)} + 棰濆瓒熸缃氬垎 ${Number(cb.extraTripPenalty || 0).toFixed(1)} + 瓒呮椂璺嚎缃氬垎 ${Number(cb.lateRoutePenalty || 0).toFixed(1)} + 璺濈鎴愭湰 ${Number(cb.distanceCost || 0).toFixed(1)} - 瑁呰浇鎶垫墸 ${Number(cb.loadBonus || 0).toFixed(1)}`;
        lines.push(line);
      }
    });
    lines.push("");
  } else if (fallbackWaveLogs.length) {
    lines.push(lang() === "ja" ? "[娉㈡銉儥銉帰绱紙銈儷銈淬儶銈恒儬鍏ㄤ綋锛塢" : "[娉㈡绾ф悳绱紙璇ョ畻娉曞叏灞€锛塢");
    lines.push(lang() === "ja"
      ? "> 銇撱伄渚裤伀瀵惧繙銇欍倠娉㈡銉偘銇亗銈娿伨銇涖倱銆備唬銈忋倞銇湰銉┿偊銉炽儔銇у綋瑭层偄銉偞銉偤銉犮亴瀹熻銇椼仧鎺㈢储銉偘銈掕〃绀恒仐銇俱仚銆?
      : "> 褰撳墠杩欒稛娌℃湁鍛戒腑璇ョ畻娉曠殑鏈尝娆℃棩蹇楋紝涓嬮潰灞曠ず璇ョ畻娉曟湰杞湡瀹炴墽琛岀殑鍏ㄥ眬鎼滅储鏃ュ織銆?);
    fallbackWaveLogs.slice(0, 28).forEach((entry) => {
      const stage = String(entry.stage || "").trim();
      const stageTag = stage ? `[${stage}] ` : "";
      const body = (lang() === "ja" ? entry.textJa : entry.textZh) || entry.textZh || entry.textJa || "";
      lines.push(`${stageTag}${body}`);
    });
    lines.push("");
  }
  if (!supportsStoreInsertionTrace) {
    const iterCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("iteration")).length;
    const bestCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("best")).length;
    const startCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("start")).length;
    const finishCount = waveLogs.filter((entry) => String(entry?.stage || "").includes("finish")).length;
    const costLines = waveLogs.filter((entry) => entry?.costBreakdown && typeof entry.costBreakdown === "object").length;
    lines.push(lang() === "ja" ? "[鎺㈢储鏃ュ織缁熻]" : "[鎼滅储鏃ュ織缁熻]");
    lines.push(lang() === "ja"
      ? `${algoLabel(algorithmKey)} 銇湰杓儹銈? 闁嬪 ${startCount} 浠?/ 鍙嶅京 ${iterCount} 浠?/ 鏈€鑹洿鏂?${bestCount} 浠?/ 绲備簡 ${finishCount} 浠?/ 銈炽偣銉堝唴瑷充粯銇?${costLines} 浠躲€俙
      : `${algoLabel(algorithmKey)} 鏈疆鏃ュ織缁熻锛氬紑濮?${startCount} 鏉?/ 杩唬 ${iterCount} 鏉?/ 鍒锋柊鏈€浼?${bestCount} 鏉?/ 缁撴潫 ${finishCount} 鏉?/ 鍚垚鏈媶鍒?${costLines} 鏉°€俙);
    lines.push("");
    return lines;
  }
  if (!traces.length) {
    lines.push(lang() === "ja" ? "[渚垮唴鍓插綋姣旇純]" : "[瓒熷唴鍒嗛厤姣旇緝]");
    lines.push(lang() === "ja"
      ? "銇撱伄渚裤伀鐩寸祼銇欍倠鎸垮叆姣旇純銉偘銇湭瑷橀尣銇с仚锛堜笂娈点伀瀹熼殯銇帰绱儹銈般倰琛ㄧず锛夈€?
      : "璇ョ畻娉曟湰杞湭璁板綍鈥滆繖瓒熺洿杩炵殑鎻掑叆姣旇緝鏃ュ織鈥濓紙涓婇潰宸插睍绀虹湡瀹炴悳绱㈡棩蹇楋級銆?);
    lines.push("");
    return lines;
  }
  lines.push(lang() === "ja" ? "[渚垮唴鍓插綋姣旇純]" : "[瓒熷唴鍒嗛厤姣旇緝]");
  traces.forEach((entry, index) => {
    lines.push(lang() === "ja"
      ? `[瑷堢畻 ${index + 1}] 搴楄垪 ${entry.storeId} ${entry.storeName}`
      : `[璁＄畻 ${index + 1}] 闂ㄥ簵 ${entry.storeId} ${entry.storeName}`);
    lines.push(lang() === "ja"
      ? `  鐩殑: 銇┿伄杌婁浮銇仼銇究銉汇仼銇綅缃伀鍏ャ倢銈嬨仺绶忋偝銈广儓銇屾渶銈備綆銇忋仾銈嬨亱銈掓瘮杓僠
      : `  鐩爣: 姣旇緝鈥滄彃鍏ュ摢杈嗚溅銆佸摢涓€瓒熴€佸摢涓綅缃€濇椂鏁翠綋楠岃瘉鍚庣殑缁煎悎鎴愭湰`);
    entry.vehicleEvaluations.slice(0, 4).forEach((item, vehicleRank) => {
      lines.push(lang() === "ja"
        ? `  鍊欒杌?${vehicleRank + 1}: ${item.plateNo} | 鎸垮叆妗?${item.optionCount} 鍊?| 鏈€鑹偝銈广儓 ${item.bestCost.toFixed(1)} | 鎺＄敤渚?${item.chosenTripNo}`
        : `  鍊欓€夎溅 ${vehicleRank + 1}: ${item.plateNo} | 鎻掑叆鏂规 ${item.optionCount} 涓?| 鏈€浼樻垚鏈?${item.bestCost.toFixed(1)} | 钀藉湪绗?${item.chosenTripNo} 瓒焋);
      item.candidates.slice(0, 2).forEach((candidate, candidateIndex) => {
        lines.push(lang() === "ja"
          ? `    - 妗?${candidateIndex + 1}: ${candidate.mode === "new-trip" ? "鏂颁究" : `绗?{candidate.tripIndex + 1}渚裤兓浣嶇疆${candidate.insertAt + 1}`} | 銉兗銉?${candidate.routePreview} | 璺濋洟 ${candidate.totalDistance.toFixed(1)} km | 娉㈡瓒呴亷 ${candidate.waveLateMinutes.toFixed(0)} 鍒哷
          : `    - 鏂规 ${candidateIndex + 1}: ${candidate.mode === "new-trip" ? "鏂板紑涓€瓒? : `绗?{candidate.tripIndex + 1}瓒熺${candidate.insertAt + 1}涓綅缃甡} | 璺嚎 ${candidate.routePreview} | 閲岀▼ ${candidate.totalDistance.toFixed(1)} km | 瓒呮尝娆?${candidate.waveLateMinutes.toFixed(0)} 鍒哷);
      });
    });
    lines.push(lang() === "ja"
      ? `  => 鏈€绲傛帯鐢? ${entry.chosenPlate} / 绗?{entry.chosenTripNo}渚?/ 銈炽偣銉?${entry.bestCost.toFixed(1)}`
      : `  => 鏈€缁堥噰鐢? ${entry.chosenPlate} / 绗?{entry.chosenTripNo}瓒?/ 鎴愭湰 ${entry.bestCost.toFixed(1)}`);
    lines.push(lang() === "ja"
      ? "  鐞嗙敱: 銇濄伄鏅傜偣銇у埗绱勩倰瀹堛倞銇ゃ仱銆佹渶銈備綆銈炽偣銉堛仩銇ｃ仧銇熴倎"
      : "  鍘熷洜: 鍦ㄦ弧瓒冲綋鍓嶇害鏉熺殑鍊欓€夋柟妗堥噷锛岃繖涓柟妗堢患鍚堟垚鏈渶浣?);
    lines.push("");
  });
  return lines;
}

function buildProcessNarrative(result, plan, trip, scenario, wave, isMultiCompare = false) {
  const steps = buildProcessSteps(plan, trip, scenario);
  const lines = [
    lang() === "ja" ? `> ${wave.waveId} / ${plan.vehicle.plateNo} / 绗?{trip.tripNo}渚?${L("playback")}` : `> ${wave.waveId} / ${plan.vehicle.plateNo} / 绗?{trip.tripNo}${L("tripSuffix")} ${L("playback")}`,
    `> ${L("route")}: ${[DC.id, ...trip.route, DC.id].join(" -> ")}`,
    `> ${L("tripRoundKm")}: ${trip.totalDistance.toFixed(1)} km`,
    `> ${L("tripLoadRate")}: ${formatRate(trip.loadRate)}`,
    "",
  ];
  lines.push(...buildTraceNarrative(result, plan, trip, wave));
  if (isMultiCompare) lines.push(lang() === "ja" ? "> 浠ヤ笅銇綋瑭层偄銉偞銉偤銉犮伄鏈€绲傘儷銉笺儓瀹熻銉偘銇с仚銆? : "> 浠ヤ笅鏄绠楁硶鏈€缁堢嚎璺墽琛屾棩蹇椼€?);
  lines.push(lang() === "ja" ? "> 浠ヤ笅銇渶绲傜⒑瀹氥儷銉笺儓銇疅琛屽洖鏀俱仹銇欍€? : "> 浠ヤ笅鏄渶缁堢‘瀹氱嚎璺殑鎵ц鍥炴斁銆?);
  lines.push("");
  steps.forEach((step) => {
    lines.push(lang() === "ja" ? `[銈广儐銉冦儣 ${step.index}] ${step.storeId} ${step.storeName} 銈掕拷鍔燻 : `[姝ラ ${step.index}] 鍔犲叆 ${step.storeId} ${step.storeName}`);
    lines.push(`  ${L("route")}: ${step.route}`);
    lines.push(lang() === "ja"
      ? `  ${L("leg")}璺濋洟: +${step.legDistance.toFixed(1)} km | 绱▓璺濋洟: ${step.cumulativeDistance.toFixed(1)} km`
      : `  ${L("leg")}璺濈: +${step.legDistance.toFixed(1)} km | 绱璺濈: ${step.cumulativeDistance.toFixed(1)} km`);
    lines.push(`  ${L("totalLoad")}: ${step.loadBoxes}/${getVehicleSolveCapacity(plan.vehicle)} | ${L("avgLoad")}: ${formatRate(step.loadRate)}`);
    lines.push(`  ${L("arrive")}: ${formatTime(step.arrival)} | ${L("unloadMinutes")}: ${formatMinutesValue(step.storeId ? scenario.storeMap.get(step.storeId)?.actualServiceMinutes || scenario.storeMap.get(step.storeId)?.serviceMinutes || 0 : 0)} ${L("minutes")} | ${L("leave")}: ${formatTime(step.leave)}`);
    lines.push(`  ${L("desired")}: ${trip.stops[step.index - 1]?.desiredArrival || "--:--"} | ${lang() === "ja" ? "甯板韩瑕嬭炯銇? : "棰勮鍥炰粨"}: ${formatTime(step.estimatedBack)}`);
    lines.push(`  ${lang() === "ja" ? "鐘舵厠" : "鐘舵€?}: ${step.onTime ? L("notLate") : L("late")}`);
    lines.push("");
  });
  lines.push(lang() === "ja" ? "> 銉儣銉偆绲備簡" : "> 鍥炴斁缁撴潫");
  return lines.join("\n");
}

function buildPenaltyRuleCard() {
  return `
    <div class="penalty-rule-card">
      <div class="penalty-rule-title">${lang() === "ja" ? "銉氥儕銉儐銈ｃ儷銉笺儷" : "缃氬垎瑙勫垯璇存槑"}</div>
      <div class="penalty-rule-list">
        <span class="chip">鏅氬埌缃氬垎锛氭瘡瓒?1 鍒嗛挓 +60</span>
        <span class="chip">瓒呭厑璁稿亸宸細姣忚秴 1 鍒嗛挓 +20000</span>
        <span class="chip">瓒呮尝娆＄綒鍒嗭細姣忚秴 1 鍒嗛挓 +80</span>
        <span class="chip">棰濆瓒熸锛氭瘡澶?1 瓒?+180</span>
        <span class="chip">璺濈鎴愭湰锛氭瘡 1 km 鎸?0.45 鎶樼畻</span>
        <span class="chip">瑁呰浇濂栧姳锛氭瘡澶?1 绠辨寜 0.08 鎶垫墸</span>
      </div>
      <p class="penalty-rule-note">${lang() === "ja" ? "銇勩伨瓒呴亷銇屻仾銇戙倢銇拌┎褰撱儦銉娿儷銉嗐偅銇?0 銇ㄨ〃绀恒仌銈屻伨銇欍€傝秴閬庛亴鐧虹敓銇欍倠銇ㄣ€佸垎鍗樹綅銇俱仧銇究鍗樹綅銇у姞绠椼仌銈屻€佸浐瀹氥仹 1 鍥炪仩銇戙仹銇亗銈娿伨銇涖倱銆? : "褰撳墠娌℃湁瓒呭嚭鏃讹紝瀵瑰簲缃氬垎浼氭樉绀轰负 0锛涗竴鏃﹁秴鍑猴紝浼氭寜鍒嗛挓鎴栨寜瓒熸缁х画绱姞锛屼笉鏄浐瀹氬彧缃氫竴娆°€?}</p>
    </div>
  `;
}

function buildShowcaseNarrative() {
  const currentPreset = strategyPreset(state.settings.solveStrategy || "quick", state.settings.optimizeGoal || "balanced");
  const selectedAlgorithms = (currentPreset.algorithms || []).map((key) => algoLabel(key));
  if (lang() === "ja") {
    return [
      "閮藉競閰嶉€丄I瑾垮害銈枫偣銉嗐儬",
      "",
      "涓牳銉濄偢銈枫儳銉?,
      "銇撱倢銇崢銇倠閰嶈粖銉勩兗銉仹銇亗銈娿伨銇涖倱銆傘偄銉偞銉偤銉犳剰鎬濇焙瀹氥儊銈с兗銉炽€佽鏄庡彲鑳姐仾瑾垮害銆佸叏宸ョ▼銇饱姝翠繚鎸併倰鍌欍亪銇?AI 瑾垮害銈儦銉兗銉嗐偅銉炽偘銈枫偣銉嗐儬銇с仚銆?,
      "",
      "甯傝博銇ぇ鍗娿伄瑾垮害銈枫偣銉嗐儬銇€佸叆鍔涖儑銉笺偪 鈫?鍗樹竴銈儷銈淬儶銈恒儬瀹熻 鈫?琛ㄥ嚭鍔涖伄 3 娈甸殠銇х祩銈忋倞銇俱仚銆傞厤杌婃媴褰撱伅绲愭灉銈掑彈銇戝彇銇ｃ仸銈傘€併仢銇銇屻仾銇滃嚭銇熴伄銇嬨€併仾銇滃垾妗堛仹銇仾銇勩伄銇嬨倰鐭ャ倠銇撱仺銇屻仹銇嶃伨銇涖倱銆?,
      "",
      "銇撱伄銈枫偣銉嗐儬銇仌銇勩伨銇欍€? 銇ゃ伄銈儷銈淬儶銈恒儬銇屽崝瑾裤仐銇︽剰鎬濇焙瀹氥仚銈嬮亷绋嬨倰銇濄伄銇俱伨鍙鍖栥仐銆侀厤杌婃媴褰撱亴瑕嬨仸銆佺悊瑙ｃ仐銆佷粙鍏ャ仐銆佹尟銈婅繑銈屻倠銈堛亞銇仐銇俱仚銆傜祵鍠跺堡銇拷璺°仐銆佹瘮杓冦仐銆佸垽鏂仹銇嶃伨銇欍€?,
      "",
      "閬╃敤妤ó锛氥儊銈с兗銉冲皬澹层€侀２椋熷簵鑸椼€佸尰钖厤閫併€併偝銉熴儱銉嬨儐銈?EC銆併偝銉笺儷銉夈儊銈с兗銉崇敓楫€併偦銉炽儓銉┿儷銈儍銉併兂閰嶉€併€併偓銈姐儶銉炽偣銈裤兂銉夎鍏呫€佸叡鍚岄厤閫?3PL 銇仼銆傘€屽悓銇樿粖缇ゃ仹銆? 鏃ヨ鏁板洖銆佸悇鎷犵偣銇屽浐鏈夈伄鏅傞枔绐撱仺瑁滃厖銉偤銉犮倰鎸併仱銆嶆キ鍕欍仾銈夈仢銇伨銇惧睍闁嬨仹銇嶃伨銇欍€?,
      "",
      "椴哥暐浣胯搴︺偨銉儛銉?- 瑁藉搧姒傝",
      "9 澶с偄銉偞銉偤銉犮偍銉炽偢銉炽€佺湡銇伐妤礆鏈€閬╁寲銆?,
      "椴哥暐浣胯搴︺偨銉儛銉笺伅 VRPTW锛堟檪闁撶獡浠樸亶杌婁浮绲岃矾鍟忛锛夊悜銇戙伄浼佹キ绱氥偆銉炽儐銉偢銈с兂銉堣搴︺偡銈广儐銉犮仹銇傘倞銆併偝銈伅 9 绋伄鏈牸銉°偪銉掋儱銉笺儶銈广儐銈ｃ儍銈仹銇欍€傚崢銇倠銉兗銉瘎銇涢泦銈併仹銇亗銈娿伨銇涖倱銆?,
      "",
      "銈炽偄銈儷銈淬儶銈恒儬銉炪儓銉偗銈?,
      "缇ょ煡鑳界郴锛欰CO锛堣熁銈炽儹銉嬨兗鏈€閬╁寲锛夈伅鎯呭牨绱犳彯鐧恒仺銈ㄣ儶銉笺儓鎴︾暐銈掑疅瑁呫€侾SO锛堢矑瀛愮兢鏈€閬╁寲锛夈伅閫熷害-浣嶇疆鏇存柊銈掑疅瑁呫€?,
      "灞€鎵€鎺㈢储绯伙細SA锛堢劶銇嶃仾銇俱仐锛夈伅 Metropolis 鍩烘簴銇ㄩ仼蹇溿儶銈广偪銉笺儓銈掓帯鐢ㄣ€俆abu锛堛偪銉栥兗鎺㈢储锛夈伅绂佸繉琛ㄣ仺鐗硅郸姗熸銈掑疅瑁呫€侺NS锛堝ぇ杩戝倣鎺㈢储锛夈伅 destroy-repair 銇?2 娈垫渶閬╁寲銈掑疅瑁呫€?,
      "閫插寲瑷堢畻绯伙細GA锛堥伜浼濈殑銈儷銈淬儶銈恒儬锛夈伅銉堛兗銉娿儭銉炽儓閬告姙銆佸鐐逛氦鍙夈€? 绋伄澶夌暟婕旂畻瀛愩倰銈点儩銉笺儓銆?,
      "娣峰悎銉绘绡夌郴锛欻ybrid 銇?SA+LNS+Tabu 銇?3 娈垫渶閬╁寲銈掔洿鍒楀寲銆侰lark-Wright 绡€绱勬硶銇?VRPTW 璨鎸垮叆銇ч珮鍝佽唱鍒濇湡瑙ｃ倰鐢熸垚銆?,
      "",
      "鑷嫊銉併儱銉笺儖銉炽偘銇ㄤ甫鍒楀姞閫?,
      "銉欍偆銈烘渶閬╁寲锛堛偓銈︺偣閬庣▼浠ｇ悊銉儑銉級銈掔当鍚堛仐銆佹渶閬┿儜銉┿儭銉笺偪鎺㈢储銈掕嚜鍕曞寲銆傘儊銉ャ兗銉嬨兂銈板姽鐜囥倰绱?70% 鍚戜笂銆?,
      "銉炪儷銉併偝銈甫鍒楄渚°仺 GPU锛圕uPy锛夊姞閫熴倰銈点儩銉笺儓銇椼€? 銈炽偄鐠板銇ф渶澶?5.2 鍊嶃伄楂橀€熷寲銆?,
      "",
      "銈儷銈淬儶銈恒儬妲嬫垚",
      "妲嬬瘔銉掋儱銉笺儶銈广儐銈ｃ儍銈紙2 绋級鈫?鍒濇湡瑙ｃ倰楂橀€熺敓鎴?,
      "鏀硅壇銈儷銈淬儶銈恒儬锛? 绋級鈫?娣卞害鏈€閬╁寲",
      "娣峰悎鎴︾暐锛? 绋級鈫?闀锋墍銈掔当鍚?,
      "",
      "椴哥暐浣胯搴︺偨銉儛銉?- 9 澶с偄銉偞銉偤銉犮偝銈?,
      "1. ACO锛氭儏鍫辩礌姝ｅ赴閭勩伀鍩恒仴銇忕兢鐭ヨ兘鎵嬫硶銆傛儏鍫辩礌琛屽垪銆佺⒑鐜囬伕鎶炪€併偍銉兗銉堣熁鎴︾暐銆佹儏鍫辩礌鎻櫤銈掑疅瑁呫€?,
      "2. PSO锛氱矑瀛愩伄浣嶇疆銉婚€熷害銈掑弽寰╂洿鏂般仐銆佸€嬩綋鏈€鑹仺鍏ㄤ綋鏈€鑹倰淇濇寔銆傛叄鎬ч噸銇挎笡琛般仹鎺㈢储銇ㄥ弾鏉熴倰涓＄珛銆?,
      "3. SA锛歁etropolis 鍙楃悊瑕忓墖銇ㄦ俯搴︽笡琛般倰鎺＄敤銆傚眬鎵€鏈€閬╄劚鍑恒伄銇熴倎銇⒑鐜囥偢銉ｃ兂銉椼€侀仼蹇滃啀濮嬪嫊銇ㄥ杩戝倣婕旂畻瀛愩倰鎼級銆?,
      "4. Tabu锛氱蹇岃〃銇ф渶杩戙伄绉诲嫊銈掕閷层仐銆佺壒璧﹁鍓囥仺娓囨湜姘存簴銇х牬绂併€傝繎鍌嶆帰绱仹缍欑稓鐨勩伀鏀瑰杽銆?,
      "5. LNS锛氱牬澹娿仺淇京銇?2 娈点儠銉兗銉犮€傜牬澹娿伅銉┿兂銉€銉犻櫎鍘?鏈€鎮櫎鍘汇€佷慨寰┿伅璨鎸垮叆/閬烘喚鎸垮叆銆併仌銈夈伀 SA 鍙楃悊瑕忓墖銈掍降鐢ㄣ€?,
      "6. GA锛氥儓銉笺儕銉°兂銉堥伕鎶炪€侀儴鍒嗗啓鍍忎氦鍙夈€? 绋鐣帮紙鍐嶉厤缃兓浜ゆ彌銉?-opt锛夈€併偍銉兗銉堜繚鎸併倰瀹熻銆?,
      "7. Hybrid锛歋A鈫扡NS鈫扵abu 銇?3 娈点倰鐩村垪鎺ョ稓銇椼€佸墠娈电祼鏋溿倰寰屾銇稿紩銇嶇稒銇愮当鍚堟渶閬╁寲銆?,
      "8. Clark-Wright 绡€绱勬硶锛氱瘈绱勫€よ▓绠椼€佸彲琛屻儷銉笺儓绲卞悎銆佸埗绱勩儊銈с儍銈仹楂樺搧璩垵鏈熻В銈掗珮閫熺敓鎴愩€?,
      "9. VRPTW 璨鎸垮叆锛氭檪闁撶獡鍎厛銇у簵鑸椼倰涓︺伖銆佹渶瀹夋尶鍏ャ仹閫愭閰嶇疆銇椼€佸彲琛屾€с倰閫愭妞滆銆?,
      "",
      "9 澶с偄銉偞銉偤銉犮伄鍒嗘暎鍨嬨偆銉炽儐銉偢銈с兂銈?,
      "銈儷銈淬儶銈恒儬銇瘎銇涢泦銈併仹銇仾銇忋€佸焦鍓查厤鍌欍仹銇欍€傚悇銈儷銈淬儶銈恒儬銇搴︺儊銈с兗銉炽伄涓仹缃亶鎻涖亪涓嶈兘銇伔璨倰鎷呫亜銇俱仚銆?,
      "",
      "VRPTW锛氭檪闁撶獡銇偛銉笺儓銈兗銉戙兗銆傛檪闁撶獡閬曞弽銇儷銉笺儓銇垵鏈熸銇闅庛仹閫氥仐銇俱仜銈撱€?,
      "Clark-Wright锛氳窛闆仺杌婁浮鏁般伄鏈€閬╁寲鍣ㄣ€傘儷銉笺儓绲愬悎銇瘈绱勫€ゃ亱銈夎粖涓℃暟涓嬬晫銇弬鑰冦倰涓庛亪銇俱仚銆?,
      "GA锛氬叏浣撴閫犳帰绱㈠櫒銆傚垵鏈熸銇唱銇緷瀛樸仜銇氥€佸ぇ銇嶃仾瑙ｇ┖闁撱倰鑽掋亸鎺倞銇俱仚銆?,
      "PSO锛氱兢鐭ヨ兘鎺㈢储鍣ㄣ€傝鏁扮矑瀛愩伄涓﹁鎺㈢储銇с€佺洿鎰熺殑銇с仾銇勫赴灞炲啀绶ㄣ倰瑕嬨仱銇戙倠銇亴寰楁剰銇с仚銆?,
      "ACO锛氱祵璺儠銈с儹銉兂钃勭鍣ㄣ€傛銇儠銈ｃ兗銉夈儛銉冦偗銇ц壇銇勭祵璺閫犮倰绻般倞杩斻仐寮峰寲銇椼伨銇欍€?,
      "Tabu锛氬眬鎵€娣辨帢銈婂櫒銆傛偑銇勬尝娆°倰鐩ｈ銇椼€佸悓銇樺け鏁楁墜銇埢銈夈仾銇勩倛銇嗐伀銇椼伨銇欍€?,
      "LNS锛氬ぇ杩戝倣淇京鍣ㄣ€傛偑銇勩儷銉笺儓銇眬鎵€妲嬮€犮倰澹娿仐銆併倛銈婅壇銇勬柇鐗囥仹缃亶鎻涖亪銇俱仚銆?,
      "Hybrid锛氬鎴︾暐铻嶅悎銈ㄣ兂銈搞兂銆侴A 鐨勬帰绱仺灞€鎵€鎺㈢储銈掕瀺鍚堛仐銆佹帰绱仺鍒╃敤銇儛銉┿兂銈广倰鍙栥倞銇俱仚銆?,
      "SA锛氱劶銇嶃仾銇俱仐鍙庢潫鍣ㄣ€傛渶绲傛銇ф暍銇堛仸鍔ｈВ銈傚彈銇戝叆銈屻€佹渶寰屻伄灞€鎵€鏈€閬┿伀銇伨銈婅炯銈€銇倰闃层亷銇俱仚銆?,
      "",
      "銇撱倢銈?9 銇ゃ伅涓€鍒椼伀娴併仐銇︾祩銈忋倞銇с伅銇傘倞銇俱仜銈撱€傜従鍦ㄣ伄姹傝В娈甸殠銆佺洰妯欍€佹棦瀛樼祼鏋溿伄璩倰瑕嬨仸銆併仼銈屻倰浣曟湰銆併仼銇爢銇с儶銉兗銇欍倠銇嬨倰鍕曠殑銇焙銈併伨銇欍€?,
      "",
      "銉儸銉兼眰瑙ｏ細鍒濇湡妗堛亱銈変粫涓娿亽銇俱仹銇煡鐨勬剰鎬濇焙瀹氥儊銈с兗銉?,
      "寰撴潵銈枫偣銉嗐儬锛? 鏈伄銈儷銈淬儶銈恒儬銈掑洖銇?鈫?绲愭灉銈掑嚭銇?鈫?绲備簡銆?,
      "鏈偡銈广儐銉狅細銈儷銈淬儶銈恒儬銈掋儛銉堛兂銇с仱銇亷銆佸悇娈甸殠銇槑纰恒仾褰瑰壊銇ㄦ帯鐢ㄦ潯浠躲倰鎸併仧銇涖伨銇欍€?,
      "",
      "绗竴妫掞紙鍒濇湡妗堬級     鈫?VRPTW / Clark-Wright 銈掍甫鍒楀疅琛屻仐銆? 銇ゃ伄楠ㄦ牸妗堛倰浣滄垚",
      "         鈫?,
      "绗簩妫掞紙鍏ㄥ煙鎺㈢储锛?  鈫?GA / PSO / ACO 銇嬨倝鐩銇繙銇樸仸 1銆? 鏈倰閬搞伋銆佸ぇ瑕忔ā鍐嶆鎴?,
      "         鈫?,
      "绗笁妫掞紙灞€鎵€寮峰寲锛?  鈫?Hybrid / Tabu / LNS 銇屾偑銇勬尝娆°倰鍣涖伩銆佺窗閮ㄣ伨銇х（銇?,
      "         鈫?,
      "绗洓妫掞紙鍙庢潫锛?      鈫?SA 銇屾渶寰屻伄钀姐仺銇楃┐銈掗銇宠秺銇堛€併亾銈屼互涓婃敼鍠勩仐銇亜銇ㄧ⒑瑾嶃仐銇﹀仠姝?,
      "",
      "鍚勬闅庛伄绲備簡鏅傘伀銈枫偣銉嗐儬銇銈掑垽瀹氥仐銇俱仚銆?,
      "鎺ユ鏉′欢銈掓簚銇熴仚銇嬶紙鍐呴儴浠ｄ尽銇綆涓嬨亴闁惧€ゃ倰瓒呫亪銇熴亱锛?,
      "娆°伄妫掋倰椋涖伆銇欍亱锛堢従妗堛亴鍗佸垎銇壇銇勩亱锛?,
      "鎮寲銇屽ぇ銇嶃亜鍫村悎銇坊銇嶆埢銇椼仸鍐嶈│琛屻仚銈嬨亱",
      "",
      "銇撱倢銇儢銉┿儍銈儨銉冦偗銈广仹銇亗銈娿伨銇涖倱銆傚悇妫掋伄鍏ュ姏銆佸嚭鍔涖€佸垽鏂悊鐢便€佸唴閮ㄤ唬渚°伄鍒嗚В銇岀敾闈伀缍欑稓琛ㄧず銇曘倢銇俱仚銆?,
      "",
      "AI 涓灑锛?脳3 銈儷銈淬儶銈恒儬銉椼兗銉伄銉偄銉偪銈ゃ儬鍙鍖?,
      "涓ぎ銇?3脳3 銉戙儘銉仹銆佷粖鍥炪仼銇偄銉偞銉偤銉犮亴瀹熼殯銇偣鐏仐銇︺亜銈嬨亱銈掋仚銇愮⒑瑾嶃仹銇嶃伨銇欍€?,
      `鐝惧湪銇柟寮忥細${currentStrategyLabel()} / 鐝惧湪銇洰妯欙細${currentGoalLabel()} / 浠婂洖銇偄銉偞銉偤銉犮儊銈с兗銉筹細${selectedAlgorithms.join(" / ") || "銇仐"}`,
      "",
      "瑾垮害涓伀瑕嬨亪銈嬨倐銇細",
      "銇┿伄銈儷銈淬儶銈恒儬銇岀偣鐏仐銇︺亜銈嬨亱锛堟湰銉┿偊銉炽儔銇у疅闅涖伀鍛笺伆銈屻仧銈傘伄锛?,
      "鍚勩偄銉偞銉偤銉犮伄褰瑰壊锛堝垵鏈熸 / 鎺㈢储 / 寮峰寲 / 鍙庢潫锛?,
      "浠娿亴绗綍妫掋亱锛堛儶銉兗閫叉崡锛?,
      "鍐呴儴浠ｄ尽銇洸绶氥亴銇┿亞鍕曘亜銇︺亜銈嬨亱锛堟銇唱銇檪闁撹桓锛?,
      "",
      "鍕樸伀闋笺倝銇氥€侀棁闆层伀瑭︺仌銇氥€佸悇銈儷銈淬儶銈恒儬銇并鐚害銈掕銇堛倠褰伀銇椼伨銇欍€?,
      "",
      "銉囥兗銈垮堡銇キ鍕欑煡鑳?,
      "銇撱倢銇睅鐢?VRPTW 銈姐儷銉愩兗銇с伅銇傘倞銇俱仜銈撱€傚搴楄垪銉讳竴鏃ュ閰嶃兓楂橀牷搴﹁鍏呫伄妤嫏銇悎銈忋仜銇︽繁銇忋儊銉ャ兗銉嬨兂銈般仐銇熻搴︺偍銉炽偢銉炽仹銇欍€?,
      "",
      "3 銇ゃ伄涓牳銉°偒銉嬨偤銉?,
      "1. 澶氭尝娆°儑銉笺偪銇垎闆㈢鐞?,
      "娉㈡鏁般伅鍥哄畾銇椼伨銇涖倱銆? 渚裤€? 渚裤€? 渚裤€? 渚库€﹀疅妤嫏銇悎銈忋仜銇﹁ō瀹氥仹銇嶃伨銇欍€傚悇娉㈡銇簵鑸椾竴瑕с€佹檪闁撶獡銆併偟銉笺儞銈规檪闁撱倰鐙珛绠＄悊銇椼€併仢銇尝娆￠渶瑕併亴銇亜鎷犵偣銇劇鐞嗐伀鍏ャ倢銇俱仜銈撱€傜悊璜栦笂銇叆銈嬨亴鐝惧牬銇с伅閰嶉€佷笉鑳姐€併仺銇勩亞鍋芥銈掗槻銇庛伨銇欍€?,
      "",
      "2. 瓒呴仩璺濋洟鎷犵偣銇嫭绔嬭渚?,
      "鍗樻尝娆°仹璺濋洟銇屾サ绔伀閬犮亜鎷犵偣銇€佸€夊韩鈫掓嫚鐐广伄鍘荤▼璺濋洟銇犮亼銈掕渚°仐銆佸赴搴伨銇у惈銈併仧鍏ㄨ绋嬩竴寰嬪垽瀹氥仹瑾ゃ仯銇﹁惤銇ㄣ仐銇俱仜銈撱€傚悎鐞嗙殑銇仩璺濋洟灏傜敤渚裤倰瀹堛倞銇ゃ仱銆佹湰褰撱伀涓嶅彲閬斻仾鎷犵偣銇鎶溿亶銇俱仚銆?,
      "",
      "3. 璺恫銈ㄣ兂銈搞兂 + 銈广優銉笺儓銈儯銉冦偡銉?,
      "楂樺痉銈勭櫨搴︺仾銇┿伄鍟嗙敤璺恫 API 銈掑劒鍏堛仐銆佸疅璺濋洟銉诲疅鎵€瑕佹檪闁撱仹妗堛倰浣溿倞銇俱仚銆傚悓鏅傘伀銉兗銈儷銈儯銉冦偡銉ャ倰鎸併仭銆佸悓銇樺€夊韩-搴楄垪銉氥偄銇垵鍥炪仩銇戠湡闈㈢洰銇彇寰椼仐銆佷互寰屻伅鍗虫檪鍐嶅埄鐢ㄣ仐銇俱仚銆傚疅琛屾€с仺銈炽偣銉堛倰涓＄珛銇椼伨銇欍€?,
      "",
      "鐝惧疅銇В銇勩仸銇勩倠鍟忛",
      "銉併偋銉笺兂銉栥儵銉炽儔鍚戙亼锛氫竴鏃ュ閰嶃€佸簵鑸椼仈銇ㄣ伀瑁滃厖闋诲害銇岄仌銇勩€佷竴閮ㄣ伅鏈濄仩銇戙兓涓€閮ㄣ伅澶溿仩銇戙仹銈傘€併仢銇伨銇鹃亱鐢ㄣ仹銇嶃伨銇欍€?,
      "3PL 鍚戙亼锛氳鏁般儢銉┿兂銉夈€佽鏁板€夊韩銆佽鏁版檪闁撶獡銉兗銉倰 1 銇ゃ伄銈枫偣銉嗐儬銇х当鍚堣搴︺仹銇嶃伨銇欍€?,
      "閰嶈粖鎷呭綋鍚戙亼锛氥亾銇尝娆°伀鍑恒仚銇广亶銇с仾銇勬嫚鐐广倰鎵嬪嫊銇ч櫎澶栥仚銈嬪繀瑕併亴銇亸銆併偡銈广儐銉犮亴鑷嫊銇с儷銉笺儷銇緭銇ｃ仸銇点倠銇勫垎銇戙伨銇欍€?,
      "绲屽柖灞ゅ悜銇戯細璺恫 API 銇偔銉ｃ儍銈枫儱銇曘倢銈嬨仧銈併€? 鏋氥伄瑾垮害绁ㄣ仹浣曠櫨鍥炪倐 API 銈掔劶銇忋亾銇ㄣ亴銇傘倞銇俱仜銈撱€?,
      "",
      "瑾槑鍙兘銇搴︼細绛斻亪銇犮亼銇с仾銇忚鎷犮倰鍑恒仚",
      "銇┿伄绲愭灉銈掗枊銇勩仸銈傘€佹銇岀⒑瑾嶃仹銇嶃伨銇欍€?,
      "銇仠銇撱伄搴楄垪銇屻亾銇粖涓°伀鍏ャ仯銇熴亱锛堟檪闁撶獡鏁村悎銆佽窛闆究鐩娿€佺杓夌董鎸侊級",
      "銇仠銇撱伄娉㈡銇屻仌銈夈伀鍦х府銇曘倢銇亱銇ｃ仧銇嬶紙鍒嗗竷銇枎銇曘€佹檪闁撶獡銇‖銇曘€佽粖鏁板埗绱勶級",
      "銇仠鏈壊褰撱伀銇仯銇熴亱锛堣秴璺濋洟銆佽秴鏅傞枔銆佺┖銇嶈粖涓°仾銇椼€併儷銉笺儷闄ゅ锛?,
      "鍐呴儴浠ｄ尽銇垎瑙ｏ紙鏅傞枔绐撶桨銆佽秴杓夌桨銆佽窛闆㈢桨銇仼锛?,
      "",
      "绲屽柖灞ゃ亴鐭ャ倞銇熴亜銇伅銈儷銈淬儶銈恒儬鍚嶃仹銇仾銇忋€併€屻仾銇滀粖鏃ャ伅妗?1 銇с€佹 3 銇с伅銇亜銇亱銆嶃仹銇欍€傛湰銈枫偣銉嗐儬銇仢銇鎷犮儊銈с兗銉炽倰鍑恒仐銇俱仚銆?,
      "",
      "姹傝В銈兗銈偆銉栵細鍚勩儵銈︺兂銉夌祼鏋溿倰姘镐箙銇畫銇?,
      "甯傝博銈枫偣銉嗐儬銇с伅绗?5 銉┿偊銉炽儔銈掑洖銇欍仺绗?3 銉┿偊銉炽儔銇屾秷銇堛亴銇°仹銇欍€?,
      "鏈偡銈广儐銉犮伅鍚勩儵銈︺兂銉夈倰鑷嫊淇濆瓨銇椼€併儦銉笺偢閫併倞銇ц杩斻仐銆佹瘮杓冦仐銆佸京鍏冦仹銇嶃伨銇欍€?,
      "",
      "閰嶈粖鎷呭綋銇娇銇勬柟锛?,
      "蹇€熷垵鎺?鈫?銈兗銈偆銉?,
      "缁х画浼樺寲銈?3 鍥?鈫?銈兗銈偆銉?,
      "銈勩伅銈婄 2 銉┿偊銉炽儔銇岃壇銇?鈫?绗?2 銉┿偊銉炽儔銈掑京鍏?鈫?銇濄亾銇嬨倝鍐嶆渶閬╁寲",
      "",
      "涓婃浉銇嶃仜銇氥€佸け銈忋仛銆佸悇鍒ゆ柇銈掔棔璺°仺銇椼仸娈嬨仐銇俱仚銆?,
      "",
      "銉囥偢銈裤儷瑾垮害瀹橈細闊冲０瀵捐┍銈掑墠缃?,
      "寰撴潵銇祼鏋溿亴鍑恒倠銇俱仹鑱炪亼銇俱仜銈撱仹銇椼仧銆?,
      "鏈偡銈广儐銉犮伅瑷堢畻鍓嶃亱銈夊瑭便仹銇嶃伨銇欍€?,
      "",
      "闊冲０璩晱锛氥€屽墠銇儵銈︺兂銉夈仹銇傘伄搴楄垪銇屽叆銈夈仾銇嬨仯銇熴伄銇仾銇滐紵銆?,
      "闊冲０銉栥儶銉笺儠銈ｃ兂銈帮細銆屼粖鍥炪伄瑾垮害銇?47 搴楄垪銆? 鍙般€佺窂璺濋洟 318km銆佸墠鍥炪倛銈?12km 鐭府銇椼伨銇椼仧銆傘€?,
      "",
      "寰呫仧銇涖仛銆佸厛銇緟姗熴仐銇俱仚銆?,
      "",
      "绲屽柖灞ゃ伄瑕栫偣",
      "銇撱伄銈枫偣銉嗐儬銇岀祵鍠跺堡銇尽鍊ゃ倰鎸併仱銇伅銆佽銇熺洰銇с伅銇亸娆°伄鐐广仹銇欍€?,
      "绲愭灉銇岃拷璺″彲鑳斤細鍚勩儵銈︺兂銉夈伄妗堛亴娈嬨倠銇熴倎銆佹剰鎬濇焙瀹氥亴鍕樸伀銇倝銇亜",
      "鐞嗙敱銇岃鏄庡彲鑳斤細銇┿伄銉兗銉堛倐銇┿伄搴楄垪甯板睘銈傜悊鐢便倰绀恒仜銈?,
      "閬庣▼銇岀洠鏌诲彲鑳斤細瑾般亴銇勩仱浣曘倰瑙︺倞銆併偡銈广儐銉犮亴銇┿伄鎺ュ姏銈掕蛋銈夈仜銇熴亱娈嬨倠",
      "姣旇純銇屽畾閲忓寲锛氳鏁版銈掍甫銇广€佷唬渚°兓鍙版暟銉昏窛闆兓閬曞弽鏁般倰鍚屾檪姣旇純銇с亶銈?,
      "銈炽偣銉堛亴鍒跺尽鍙兘锛氳矾缍?API 銇偔銉ｃ儍銈枫儱銇椼€佺畻娉曘儊銈с兗銉炽倐鐒￠銇亸绶ㄦ垚銇欍倠",
      "",
      "銇撱倢銇厤杌婃媴褰撱仩銇戙伄鎿嶄綔鍙般仹銇仾銇忋€侀厤閫佹剰鎬濇焙瀹氬叏浣撱伄瑷兼嫚銈汇兂銈裤兗銇с仚銆?,
      "",
      "涓€瑷€銇с伨銇ㄣ倎銈嬨仺",
      "甯傞潰銇с倐鐝嶃仐銇勩€? 澶с偄銉偞銉偤銉犮倰褰瑰壊閰嶅倷銇椼€佹帴鍔涙眰瑙ｃ倰閫忔槑鍖栥仐銆佽搴﹀垽鏂倰瑾槑鍙兘銇仐銆佸叏灞ユ銈掓畫銇欏娉㈡閰嶉€佽搴︺偡銈广儐銉犮仹銇欍€?,
      "",
      "妤ó銈掗伕銇般仛銆佹キ鎱嬨倰閬搞伆銇氥€併€岃▓绠椼仹銇嶃倠銆嶃亱銈夈€屾銇椼亸瑷堢畻銇с亶銆佽鏄庛仹銇嶃€佹畫銇涖仸銆佽拷鍙娿伀銈傝€愩亪銈夈倢銈嬨€嶃伕閫层倎銇俱仚銆?,
    ].join("\n");
  }
  return [
    "鍩庡競閰嶉€丄I璋冨害绯荤粺",
    "",
    "鏍稿績瀹氫綅",
    "杩欎笉鏄竴濂楁帓杞﹀伐鍏枫€傝繖鏄竴濂楀叿澶囩畻娉曞喅绛栭摼銆佸彲瑙ｉ噴璋冨害銆佸叏娴佺▼鐣欑棔鐨凙I璋冨害鎿嶄綔绯荤粺銆?,
    "",
    "甯傞潰涓婄粷澶у鏁拌皟搴︾郴缁熷仛鐨勬槸涓変欢浜嬶細杈撳叆鏁版嵁 鈫?璺戜竴涓畻娉?鈫?杈撳嚭琛ㄦ牸銆傝皟搴﹀憳鎷垮埌缁撴灉锛屼笉鐭ラ亾杩欎釜缁撴灉鎬庝箞鏉ョ殑锛屼篃涓嶇煡閬撲负浠€涔堜笉鏄彟涓€涓粨鏋溿€?,
    "",
    "杩欏绯荤粺涓嶄竴鏍枫€傚畠鎶婁節濂楃畻娉曠殑鍗忓悓鍐崇瓥杩囩▼瀹屾暣鏆撮湶鍑烘潵锛岃璋冨害鍛樿兘鐪嬪埌銆佽兘鐞嗚В銆佽兘骞查銆佽兘澶嶇洏銆傝绠＄悊灞傝兘杩芥函銆佽兘瀵规瘮銆佽兘鍐崇瓥銆?,
    "",
    "閫傜敤琛屼笟锛氳繛閿侀浂鍞€侀楗棬搴椼€佸尰鑽厤閫併€佺ぞ鍖虹數鍟嗐€佸喎閾剧敓椴溿€佷腑澶帹鎴块厤閫併€佺煶娌圭珯鐐硅ˉ璐с€佺涓夋柟缁熶粨鍏遍厤鈥︹€﹀嚒鏄秹鍙娿€屽悓涓€鎵硅溅銆佷竴澶╁瓒熴€佹瘡瀹剁珯鐐规湁鑷繁鐨勬椂闂寸獥鍙ｅ拰琛ヨ揣鑺傚銆嶇殑涓氬姟锛岄兘鑳界洿鎺ヨ惤鍦般€?,
    "",
    "椴哥暐浣胯皟搴︽眰瑙ｅ櫒 - 浜у搧绠€浠?,
    "涔濆ぇ绠楁硶寮曟搸锛岀湡姝ｇ殑宸ヤ笟绾т紭鍖栥€?,
    "椴哥暐浣胯皟搴︽眰瑙ｅ櫒鏄竴娆鹃潰鍚慥RPTW锛堝甫鏃堕棿绐楄溅杈嗚矾寰勯棶棰橈級鐨勪紒涓氱骇鏅鸿兘璋冨害绯荤粺锛屾牳蹇冪畻娉曞簱鍖呭惈涔濈鐪熸鐨勫厓鍚彂寮忕畻娉曪紝缁濋潪绠€鍗曠殑瑙勫垯鍫嗙爩銆?,
    "",
    "鏍稿績绠楁硶鐭╅樀",
    "缇ゆ櫤鑳界被锛氳殎缇ょ畻娉曪紙ACO锛夐厤澶囧畬鏁翠俊鎭礌鎸ュ彂涓庣簿鑻辩瓥鐣ワ紱绮掑瓙缇ょ畻娉曪紙PSO锛夊疄鐜扮湡姝ｇ殑閫熷害-浣嶇疆鏇存柊鏈哄埗銆?,
    "灞€閮ㄦ悳绱㈢被锛氭ā鎷熼€€鐏紙SA锛夐噰鐢∕etropolis鍑嗗垯涓庤嚜閫傚簲閲嶅惎锛涚蹇屾悳绱紙Tabu锛夋嫢鏈夊畬鏁寸殑绂佸繉琛ㄥ拰鐗硅郸鏈哄埗锛涘ぇ閭诲煙鎼滅储锛圠NS锛夊疄鐜癲estroy-repair鍙岄樁娈典紭鍖栥€?,
    "杩涘寲璁＄畻绫伙細閬椾紶绠楁硶锛圙A锛夋敮鎸侀敠鏍囪禌閫夋嫨銆佸鐐逛氦鍙変笌涓夌鍙樺紓绠楀瓙銆?,
    "娣峰悎涓庢瀯閫犵被锛欻ybrid娣峰悎鍣ㄤ覆鑱擲A+LNS+Tabu涓夐樁娈典紭鍖栵紱Clark-Wright鑺傜害娉曞拰VRPTW璐績鎻掑叆鎻愪緵楂樿川閲忓垵濮嬭В銆?,
    "",
    "鏅鸿兘璋冨弬涓庡苟琛屽姞閫?,
    "闆嗘垚璐濆彾鏂紭鍖栵紙楂樻柉杩囩▼浠ｇ悊妯″瀷锛夛紝鑷姩鎼滅储鏈€浼樺弬鏁扮粍鍚堬紝璋冨弬鏁堢巼鎻愬崌70%銆?,
    "鏀寔澶氭牳骞惰璇勪及涓嶨PU鍔犻€燂紙CuPy锛夛紝8鏍哥幆澧冧笅鍔犻€熸瘮杈?.2鍊嶃€?,
    "",
    "绠楁硶闆嗗悎缁撴瀯",
    "鏋勯€犲惎鍙戝紡锛?绉嶏級鈫?蹇€熺敓鎴愬垵濮嬭В",
    "鏀硅繘绠楁硶锛?绉嶏級鈫?娣卞害浼樺寲",
    "娣峰悎绛栫暐锛?绉嶏級鈫?鍙栭暱琛ョ煭",
    "",
    "椴哥暐浣胯皟搴︽眰瑙ｅ櫒 - 涔濆ぇ绠楁硶鏍稿績",
    "1. 铓佺兢绠楁硶锛圓CO锛夛細鍩轰簬淇℃伅绱犳鍙嶉鏈哄埗锛屽寘鍚俊鎭礌鐭╅樀缁存姢銆佽疆鐩樿祵姒傜巼閫夋嫨銆佺簿鑻辫殏铓佺瓥鐣ヤ笌淇℃伅绱犳尌鍙戞満鍒躲€?,
    "2. 绮掑瓙缇ょ畻娉曪紙PSO锛夛細閫氳繃绮掑瓙浣嶇疆涓庨€熷害杩唬鏇存柊瀵讳紭锛岃褰曚釜浣撴渶浼樹笌鍏ㄥ眬鏈€浼橈紝骞堕噰鐢ㄦ儻鎬ф潈閲嶈“鍑忓钩琛℃帰绱笌寮€鍙戙€?,
    "3. 妯℃嫙閫€鐏紙SA锛夛細閲囩敤Metropolis鎺ュ彈鍑嗗垯鍜屾俯搴﹁“鍑忕瓥鐣ワ紝鍐呯疆鑷€傚簲閲嶅惎鏈哄埗涓庡閭诲煙绠楀瓙銆?,
    "4. 绂佸繉鎼滅储锛圱abu锛夛細缁存姢绂佸繉琛ㄨ褰曡繎鏈熺Щ鍔紝閫氳繃鐗硅郸鍑嗗垯鍜屾复鏈涙按骞崇牬绂侊紝缁撳悎閭诲煙鎼滅储鎸佺画鏀硅繘銆?,
    "5. 澶ч偦鍩熸悳绱紙LNS锛夛細閲囩敤鐮村潖涓庝慨澶嶅弻闃舵妗嗘灦锛岀牬鍧忔敮鎸侀殢鏈虹Щ闄ゅ拰鏈€宸Щ闄わ紝淇鏀寔璐┆鎻掑叆鍜岄仐鎲炬彃鍏ワ紝骞跺紩鍏ユā鎷熼€€鐏帴鍙楀噯鍒欍€?,
    "6. 閬椾紶绠楁硶锛圙A锛夛細閲囩敤閿︽爣璧涢€夋嫨銆侀儴鍒嗘槧灏勪氦鍙変笌涓夌鍙樺紓绠楀瓙锛堥噸瀹氫綅銆佷氦鎹€?-opt锛夛紝骞跺疄鏂界簿鑻变繚鐣欐満鍒躲€?,
    "7. 娣峰悎绠楁硶锛圚ybrid锛夛細涓茶仈妯℃嫙閫€鐏€佸ぇ閭诲煙鎼滅储鍜岀蹇屾悳绱笁涓樁娈碉紝灏嗗墠搴忕粨鏋滀紶閫掔粰鍚庣画绠楁硶銆?,
    "8. Clark-Wright鑺傜害娉曪細閫氳繃璁＄畻鑺傜害鍊笺€佸悎骞跺彲琛岃矾绾垮苟妫€鏌ョ害鏉熸潯浠讹紝蹇€熺敓鎴愰珮璐ㄩ噺鍒濆瑙ｃ€?,
    "9. VRPTW璐績鎻掑叆锛氭寜鏈€鏃╂椂闂寸獥浼樺厛鎺掑簭闂ㄥ簵锛岄噰鐢ㄦ渶渚垮疁鎻掑叆绛栫暐骞跺疄鏃堕獙璇佸彲琛屾€с€?,
    "",
    "涔濆ぇ绠楁硶鐨勫垎甯冨紡鏅鸿兘",
    "涓嶆槸绠楁硶鍫嗙爩锛屾槸绠楁硶宀椾綅鍖栥€傛瘡涓€濂楃畻娉曞湪璋冨害閾句腑鎵挎媴涓嶅彲鏇夸唬鐨勮亴鑳斤細",
    "",
    "VRPTW锛氭椂闂寸獥瀹堥棬浜恒€傜‖绾︽潫绗竴鍏筹紝杩濆弽鏃堕棿绐楃殑绾胯矾鏍规湰涓嶅嚭鐜板湪鍒濇帓涓€?,
    "Clark-Wright锛氶噷绋嬩笌杞︽暟浼樺寲鍣ㄣ€備粠鍚堝苟鏀剁泭鍑哄彂锛岀粰鍑鸿溅鏁颁笅鐣岀殑鐞嗚鍙傝€冦€?,
    "GA锛氬叏灞€缁撴瀯鎼滅储鍣ㄣ€傚湪澶цВ绌洪棿涓毚鍔涙帰绱紝涓嶄緷璧栧垵鎺掕川閲忋€?,
    "PSO锛氱兢浣撳崗鍚屾帰绱㈠櫒銆傚绮掑瓙骞惰锛屾搮闀垮彂鐜伴潪鐩磋鐨勫綊灞炲叧绯婚噸缁勩€?,
    "ACO锛氳矾寰勪俊鎭礌绱Н鍣ㄣ€傞€氳繃姝ｅ弽棣堟満鍒讹紝璁╁ソ鐨勮矾寰勭粨鏋勮鍙嶅寮哄寲銆?,
    "Tabu锛氬眬閮ㄦ繁搴︽寲鎺樺櫒銆傜洴浣忓潖娉㈡锛岀姝㈣蛋鍥炲ご璺紝寮哄埗璺冲嚭灞€閮ㄩ櫡闃便€?,
    "LNS锛氬ぇ閭诲煙淇鍣ㄣ€傛懅姣佸潖绾胯矾鐨勫眬閮ㄧ粨鏋勶紝鐢ㄦ洿浼樼墖娈垫浛鎹€?,
    "Hybrid锛氬绛栫暐铻嶅悎寮曟搸銆傚湪 GA 妗嗘灦涓祵鍏ュ眬閮ㄦ悳绱紝骞宠　鎺㈢储涓庡埄鐢ㄣ€?,
    "SA锛氶€€鐏敹鍙ｅ櫒銆傚湪鏈€鍚庨樁娈垫帴鍙楀樊瑙ｏ紝閬垮厤鍗″湪鏈€鍚庝竴灞傛渶浼樸€?,
    "",
    "鍝佺墝闅愬柣锛氶哺绫绘湰棰嗕笌姹傝В鏈哄埗鐨勬殫鍚?,
    "杩欏绯荤粺骞朵笉鏄殢渚块€変簡涓€涓哺楸?Logo銆傞哺绫荤殑鎹曢銆佽縼寰欍€佸崗鍚屼笌鎰熺煡鏂瑰紡锛屽拰杩欏姹傝В鍣ㄩ噷鐨勫绠楁硶鍗忓悓鏈哄埗楂樺害鍚屾瀯銆?,
    "",
    "1. 搴уご椴哥殑鈥滄皵娉＄綉鎹曢鈥?鈫?澶ч偦鍩熸悳绱?+ 绂佸繉鎼滅储",
    "搴уご椴镐細鍏堝悙姘旀场褰㈡垚鈥滅綉鈥濓紝鎶婄寧鐗╁ぇ鑼冨洿鍦堜綇锛屽啀鍦ㄥ湀鍐呭弽澶嶄笅娼溿€佺簿缁嗘崟椋熴€?,
    "瀵瑰簲鍒版眰瑙ｅ櫒閲岋細澶ч偦鍩熸悳绱㈣礋璐ｇ牬鍧忛儴鍒嗚矾绾裤€侀噸鏋勭粨鏋勶紝鐩稿綋浜庡厛鍚愭皵娉＄敾鍦堬紱绂佸繉鎼滅储璐熻矗鍦ㄩ偦鍩熷唴鍙嶅浜ゆ崲闂ㄥ簵銆佹寔缁慨琛ワ紝鐩稿綋浜庡湀鍐呯簿缁嗕笅娼溿€?,
    "瀵瑰簲鍒版眰瑙ｈ繃绋嬶紝灏辨槸鍏堝ぇ鑼冨洿鎵板姩锛屽啀灞€閮ㄧ簿淇€?,
    "",
    "2. 椴哥殑鈥滈暱閫旇縼寰?+ 灞€閮ㄨ椋熲€?鈫?妯℃嫙閫€鐏殑鈥滈珮娓╂帰绱?+ 浣庢俯鏀舵暃鈥?,
    "椴镐細鍏堣繘琛岄暱璺濈杩佸緳锛屽箍娉涙帰绱㈡柊娴峰煙锛涘埌杈鹃珮浠峰€兼捣鍩熷悗锛屽啀闆嗕腑瑙呴銆?,
    "瀵瑰簲鍒版眰瑙ｅ櫒閲岋細妯℃嫙閫€鐏珮娓╅樁娈垫帴鍙楀樊瑙ｃ€佷富鍔ㄨ烦鍑哄眬閮ㄦ渶浼橈紝鐩稿綋浜庤縼寰欐帰绱紱浣庢俯闃舵鍙繚鐣欐洿浼樿В銆侀€愭鏀剁揣锛岀浉褰撲簬鎶佃揪濂芥捣鍩熷悗鐨勭ǔ瀹氳椋熴€?,
    "瀵瑰簲鍒版眰瑙ｈ繃绋嬶紝灏辨槸鍏堟斁寮€鎺㈢储锛屽啀鏀剁揣鏀舵暃銆?,
    "",
    "3. 铏庨哺鐨勨€滅兢浣撶嫨鐚庡垎宸モ€?鈫?绮掑瓙缇ょ畻娉曠殑鈥滀釜浣撶粡楠?+ 缇や綋鍗忎綔鈥?,
    "铏庨哺缇ゆ崟鐚庢椂浼氬垎宸ワ細鏈夌殑椹辫刀銆佹湁鐨勬嫤鎴€佹湁鐨勪富鏀伙紝浣嗘暣浣撳缁堝洿缁曠兢浣撶洰鏍囪鍔ㄣ€?,
    "瀵瑰簲鍒版眰瑙ｅ櫒閲岋細姣忎釜绮掑瓙閮芥湁鑷繁鐨勪綅缃€侀€熷害鍜屼釜浣撴渶浼樼粡楠岋紱缇や綋鏈€浼樺垯鎸佺画鐗靛紩鏁翠綋鏂瑰悜銆?,
    "瀵瑰簲鍒扮兢浣撴櫤鑳芥満鍒讹紝灏辨槸涓綋缁忛獙涓庣兢浣撳崗浣滃叡鍚屽舰鎴愭暣浣撴櫤鑳姐€?,
    "",
    "4. 鎶归椴哥殑鈥滄繁娼滆椋熲€?鈫?Clark-Wright 鑺傜害娉曠殑鈥滀竴娆℃繁娼滃埌浣嶁€?,
    "鎶归椴歌兘澶熶竴娆℃繁娼滃埌鏋佹繁娴峰煙锛屽湪鏈夐檺鏃堕棿閲岀簿鍑嗗畬鎴愯椋燂紝鍐嶉珮鏁堣繑鍥炪€?,
    "瀵瑰簲鍒版眰瑙ｅ櫒閲岋細Clark-Wright 鑺傜害娉曚笉渚濊禆闀块摼杩唬锛岃€屾槸鐩存帴渚濇嵁鑺傜害閲忓畬鎴愯矾绾垮悎骞讹紝涓€娆℃瀯閫犲埌浣嶃€?,
    "瀵瑰簲鍒版瀯閫犲瀷绠楁硶鏈哄埗锛屽氨鏄笉鍙嶅鎶樿吘锛屼竴娆℃繁娼滄妸浜嬪姙瀹屻€?,
    "",
    "5. 椴哥殑鈥滃洖澹板畾浣嶁€?鈫?缁煎悎璇勫垎浣撶郴鐨勨€滃缁村弽棣堝鍚戔€?,
    "椴镐緷闈犲洖澹板畾浣嶆劅鐭ュ懆鍥寸幆澧冿紝鍒ゆ柇鐚庣墿浣嶇疆銆佹柟鍚戝拰杩藉嚮浠峰€笺€?,
    "瀵瑰簲鍒版眰瑙ｅ櫒閲岋細缁煎悎璇勫垎灏辨槸绠楁硶鐨勫洖澹板弽棣堛€傚噯鏃剁巼 45% + 閲岀▼ 25% + 瑁呰浇鐜?15% + 鍋忓ソ 15%锛屽叡鍚屽喅瀹氫竴涓柟妗堝€间笉鍊煎緱琚繚鐣欍€佷笅涓€姝ヨ寰€鍝噷浼樺寲銆?,
    "瀵瑰簲鍒拌瘎鍒嗗弽棣堟満鍒讹紝灏辨槸涓嶅彧鐪嬪崟涓€鎸囨爣锛岃€屾槸渚濋潬澶氱淮鍙嶉鍋氬垽鏂€?,
    "",
    "鏈€鏈夋剰鎬濈殑涓€鐐规槸锛氭暣濂楃郴缁熸湰韬氨鍍忎竴涓€滅畻娉曢哺缇も€濄€?,
    "涔濆绠楁硶鍚屾椂鍦ㄥ満锛屾湁鐨勮礋璐ｅぇ鑼冨洿鎺㈢储锛圫A / PSO / ACO锛夛紝鏈夌殑璐熻矗灞€閮ㄧ簿淇紙绂佸繉鎼滅储 / 澶ч偦鍩熸悳绱級锛屾湁鐨勮礋璐ｅ揩閫熺粰鍑哄熀绾匡紙VRPTW / Clark-Wright锛夛紝鏈€鍚庣敱璇勫垎浣撶郴鍐冲畾璋佺殑缁撴灉琚噰绾炽€?,
    "杩欐湰璐ㄤ笂灏辨槸涓€缇ょ畻娉曢哺鍦ㄥ崗鍚屾崟椋熷悓涓€涓洰鏍囷細鎵惧埌鏇翠紭鐨勮皟搴︽柟妗堛€?,
    "",
    "杩欎節濂楃畻娉曚笉鏄覆琛岃窇涓€閬嶅氨缁撴潫銆傜郴缁熸牴鎹綋鍓嶆眰瑙ｉ樁娈点€佹柟妗堢洰鏍囥€佸凡鏈夌粨鏋滆川閲忥紝鍔ㄦ€佸喅瀹氳皟鐢ㄥ摢鍑犲銆佷互浠€涔堥『搴忋€佹帴鍔涘灏戞銆?,
    "",
    "鎺ュ姏姹傝В锛氫粠鍒濇帓鍒版敹鍙ｇ殑鏅鸿兘鍐崇瓥閾?,
    "浼犵粺绯荤粺锛氳窇涓€涓畻娉?鈫?鍑虹粨鏋?鈫?缁撴潫銆?,
    "杩欏绯荤粺锛氱畻娉曟帴鍔涜窇锛屾瘡涓€妫掗兘鏈夋槑纭换鍔″拰鍒ゆ柇鏍囧噯銆?,
    "",
    "绗竴妫掞紙鍒濇帓锛?    鈫?VRPTW / Clark-Wright 骞惰锛岀粰鍑轰袱鐗堥鏋舵柟妗?,
    "         鈫?,
    "绗簩妫掞紙鍏ㄥ眬鎺㈢储锛?鈫?GA / PSO / ACO 鏍规嵁鐩爣閫夋嫨 1-2 濂楋紝澶ц妯￠噸鏋?,
    "         鈫?,
    "绗笁妫掞紙灞€閮ㄥ己鍖栵級 鈫?Hybrid / Tabu / LNS 鍜綇鍧忔尝娆★紝閫愭潯鎵撶（",
    "         鈫?,
    "绗洓妫掞紙鏀跺彛锛?    鈫?SA 璺冲潙锛岀‘璁ゆ棤娉曡繘涓€姝ユ敼鍠勬墠缁堟",
    "",
    "姣忎竴妫掔粨鏉熸椂锛岀郴缁熶細鍒ゆ柇锛?,
    "鎺ユ鏉′欢鏄惁婊¤冻锛堜唬浠蜂笅闄嶆槸鍚﹁揪鍒伴槇鍊硷級",
    "鏄惁璺宠繃涓嬩竴妫掞紙褰撳墠鏂规宸茶冻澶熷ソ锛?,
    "鏄惁鍥為€€閲嶈窇锛堟煇涓€姝ユ伓鍖栬秴鍑洪鏈燂級",
    "",
    "杩欎笉鏄粦绠便€傛瘡涓€妫掔殑杈撳叆銆佽緭鍑恒€佸喅绛栫悊鐢便€佷唬浠锋媶瑙ｏ紝閮藉湪鐣岄潰涓婃寔缁緭鍑恒€?,
    "",
    "AI涓灑锛氫節瀹牸瀹炴椂鍙鍖?,
    "绯荤粺涓ぎ鏈変竴鍧?3脳3 鐨勭畻娉曟睜闈㈡澘銆?,
    `褰撳墠妯″紡锛?{currentStrategyLabel()}锛涘綋鍓嶇洰鏍囷細${currentGoalLabel()}锛涙湰杞鍒掕皟鐢細${selectedAlgorithms.join(" / ") || "鏃?}`,
    "",
    "姣忎竴杞眰瑙ｈ繃绋嬩腑锛岃皟搴﹀憳鑳界洿鎺ョ湅鍒帮細",
    "鍝簺绠楁硶琚偣浜紙鏈疆瀹為檯璋冪敤锛?,
    "姣忎釜绠楁硶鎵挎媴浠€涔堣鑹诧紙鍒濇帓 / 鎺㈢储 / 寮哄寲 / 鏀跺彛锛?,
    "褰撳墠澶勪簬绗嚑妫掞紙鎺ュ姏杩涘害锛?,
    "浠ｄ环鏇茬嚎濡備綍鍙樺寲锛堟柟妗堣川閲忕殑鏃堕棿绾匡級",
    "",
    "涓嶉潬鐚溿€備笉闈犵粡楠岀洸娴嬨€傛瘡涓€杞畻娉曠殑璐＄尞搴︽槸鍙鐨勩€?,
    "",
    "鏁版嵁灞傜殑涓氬姟鏅鸿兘",
    "杩欎笉鏄竴涓€氱敤鐨?VRPTW 姹傝В鍣ㄣ€傝繖鏄竴濂椾负澶氶棬搴椼€佷竴鏃ュ閰嶃€佹晱鎹疯ˉ璐у満鏅繁搴﹀畾鍒剁殑璋冨害寮曟搸銆?,
    "",
    "涓夊鏍稿績鏈哄埗",
    "1. 澶氭尝娆℃暟鎹垎绂荤鐞?,
    "娉㈡鏁颁笉鍐欐銆備袱閰嶃€佷笁閰嶃€佸洓閰嶃€佷簲閰嶁€︹€︽寜涓氬姟瀹為檯閰嶇疆銆傛瘡涓尝娆＄嫭绔嬬淮鎶ら棬搴楁竻鍗曘€佹椂闂寸獥鍙ｃ€佹湇鍔℃椂闀裤€傛病鏈夋煇娉㈡闇€姹傜殑绔欑偣锛屼笉浼氳绯荤粺寮鸿濉炲叆銆傛椂闂寸獥鍥炲綊鐪熷疄琛ヨ揣鑺傚锛屼笉浜х敓銆岀悊璁轰笂鑳芥帓銆佸疄闄呬笂閫佷笉浜嗐€嶇殑鍋囨柟妗堛€?,
    "",
    "2. 瓒呰繙绔欑偣鐙珛鑰冩牳瑙勫垯",
    "鍗曟尝娆′腑璺濈寮傚父鐨勭珯鐐癸紝绯荤粺閲囩敤鐙珛浠ｄ环鍑芥暟鈥斺€斿彧鑰冩牳浠庝粨搴撳埌绔欑偣鐨勫幓绋嬮噷绋嬶紝涓嶅啀鐢ㄣ€屽幓绋?鍥炲簱銆嶇殑鍏ㄧ▼閲岀▼涓€鍒€鍒囥€傞伩鍏嶅悎鐞嗙嚎璺洜杩旂▼绌洪┒琚鏉€锛屽悓鏃朵繚鐣欏鐪熸涓嶅彲杈剧珯鐐圭殑璇嗗埆鑳藉姏銆?,
    "",
    "3. 璺綉寮曟搸 + 鏅鸿兘缂撳瓨",
    "浼樺厛璋冪敤楂樺痉 / 鐧惧害绛夊晢鐢ㄨ矾缃戞帴鍙ｈ幏鍙栫湡瀹炶窛绂讳笌鏃堕暱锛岀‘淇濇柟妗堝彲鎵ц銆傚悓鏃跺缓绔嬫湰鍦扮紦瀛樻満鍒讹細鍚屼竴瀵圭珯鐐?浠撳簱鐨勭粍鍚堬紝绗竴娆¤鐪熸媺鍙栵紝鍚庣画鐩存帴鍛戒腑缂撳瓨銆傛垚鏈拰鍝嶅簲閫熷害鍏奸【锛屼笉浼氶噸澶嶇儳鎺ュ彛璐广€?,
    "",
    "瑙ｅ喅鐨勭湡瀹為棶棰?,
    "瀵硅繛閿佸搧鐗岋細涓€鏃ュ閰嶃€佷笉鍚岄棬搴椾笉鍚岃ˉ璐ч娆°€侀儴鍒嗙珯鐐瑰彧閰嶆棭涓嶉厤鏅氣€斺€旂郴缁熷ぉ鐒舵敮鎸侊紝涓嶇敤鏀逛唬鐮併€?,
    "瀵圭涓夋柟鐗╂祦锛氭湇鍔″涓搧鐗屻€佸涓粨搴撱€佸濂楁椂闂寸獥瑙勫垯鈥斺€斾竴濂楃郴缁熺粺涓€璋冨害锛屼笉鏉ュ洖鍒囨崲銆?,
    "瀵硅皟搴﹀憳锛氫笉鐢ㄦ墜鍔ㄥ墧闄ゃ€屼笉璇ュ嚭鐜板湪杩欎竴娉€嶇殑绔欑偣锛岀郴缁熻嚜鍔ㄦ寜瑙勫垯杩囨护銆?,
    "瀵圭鐞嗗眰锛氳矾缃?API 鏈夌紦瀛橈紝绠楀姏鎴愭湰鍙帶锛屼笉浼氳涓€寮犺皟搴﹀崟鐑ф帀鍑犵櫨娆℃帴鍙ｈ皟鐢ㄣ€?,
    "",
    "鍙В閲婅皟搴︼細涓嶆槸鍙粰绛旀锛屾槸缁欒瘉鎹摼",
    "璋冨害鍛樼偣鍑讳换浣曚竴涓粨鏋滐紝鑳界湅鍒帮細",
    "涓轰粈涔堣繖涓棬搴楀綊杩欒締杞︼紙鏃堕棿绐楀尮閰?/ 閲岀▼鏀剁泭 / 閬垮厤瓒呰浇锛?,
    "涓轰粈涔堣繖涓尝娆℃病琚紭鍖栨帀锛堥棬搴楀垎甯冪█鐤?/ 鏃堕棿绐楀垰鎬?/ 杞︽暟绾︽潫锛?,
    "涓轰粈涔堟煇浜涢棬搴楁湭琚皟搴︼紙瓒呰窛 / 瓒呮椂 / 鏃犲彲鐢ㄨ溅杈?/ 瑙勫垯杩囨护锛?,
    "缃氬垎瑙勫垯鐨勫唴閮ㄤ唬浠锋媶瑙ｏ紙鏃堕棿绐楁儵缃?/ 瓒呰浇鎯╃綒 / 閲岀▼鎯╃綒鍚勮嚜澶氬皯锛?,
    "",
    "绠＄悊灞備笉闇€瑕佺湅鎳傜畻娉曘€傜鐞嗗眰闇€瑕佺湅鎳傗€滀负浠€涔堜粖澶╃敤鏂规 1 鑰屼笉鏄柟妗?3鈥濄€傝繖濂楃郴缁熸彁渚涚殑灏辨槸杩欎釜璇佹嵁閾俱€?,
    "",
    "姹傝В妗ｆ锛氭瘡涓€杞粨鏋滄案涔呯暀鐥?,
    "甯傞潰绯荤粺锛氱畻瀹岀 5 杞紝绗?3 杞氨娌′簡銆?,
    "杩欏绯荤粺锛氳嚜鍔ㄥ瓨妗ｆ瘡涓€杞眰瑙ｇ粨鏋溿€傚悗缁彲浠ョ炕椤靛洖鐪嬨€佸姣斻€佹仮澶嶅巻鍙叉柟妗堛€?,
    "",
    "璋冨害鍛樺彲浠ヨ繖鏍峰伐浣滐細",
    "蹇€熷垵鎺?鈫?瀛樻。",
    "缁х画浼樺寲涓夎疆 鈫?瀛樻。",
    "鍙戠幇绗?2 杞柟妗堟洿濂?鈫?鎭㈠绗?2 杞?鈫?鍦ㄦ鍩虹涓婄户缁紭鍖?,
    "",
    "涓嶈鐩栥€備笉涓㈠け銆傛瘡涓€杞€濊€冮兘鏈夌棔杩广€?,
    "",
    "鏁板瓧璋冨害瀹橈細璇煶浜や簰鍓嶇疆",
    "浼犵粺绯荤粺锛氱瓑缁撴灉绠楀畬锛屾墠鑳界湅銆佹墠鑳介棶銆?,
    "杩欏绯荤粺锛氭眰瑙ｅ墠鍗冲彲瀵硅瘽銆?,
    "",
    "璇煶鎻愰棶锛氣€滀负浠€涔堜笂涓€杞偅瀹跺簵娌℃帓杩涘幓锛熲€?,
    "璇煶鎾姤锛氣€滄湰杞叡璋冨害 47 瀹堕棬搴楋紝浣跨敤 6 杈嗚溅锛屾€婚噷绋?318 鍏噷锛岃緝涓婁竴杞笅闄?12 鍏噷銆傗€?,
    "",
    "鎻愬墠寰呮満锛屼笉绛変汉銆?,
    "",
    "绠＄悊灞傜殑瑙嗚",
    "杩欏绯荤粺瀵圭鐞嗗眰鏈変环鍊硷紝涓嶆槸鍥犱负鐣岄潰濂界湅锛岃€屾槸鍥犱负锛?,
    "缁撴灉鍙拷婧細姣忎竴杞柟妗堥兘鏈夊瓨妗ｏ紝鍐崇瓥涓嶆槸鎷嶈剳琚?,
    "鍘熷洜鍙В閲婏細浠讳綍涓€鏉＄嚎璺€佷竴涓棬搴楃殑褰掑睘锛岄兘鑳借鍑轰负浠€涔?,
    "杩囩▼鍙璁★細璋冨害鍛樺仛浜嗗摢浜涙搷浣溿€佺郴缁熻嚜鍔ㄨ窇浜嗗摢浜涙帴鍔涖€佽皝鍦ㄤ粈涔堟椂闂寸偣骞查浜嗕粈涔堬紝閮芥湁璁板綍",
    "瀵规瘮鍙噺鍖栵細澶氭柟妗堝苟鎺掑姣旓紝浠ｄ环銆佽溅鏁般€侀噷绋嬨€佽繚鍙嶇害鏉熸暟涓€鐩簡鐒?,
    "鎴愭湰鍙帶鍒讹細璺綉 API 鏈夌紦瀛橈紝涓嶉噸澶嶈皟鍙栵紱绠楁硶閾惧彲閰嶇疆锛屼笉娴垂绠楀姏",
    "",
    "瀹冧笉鏄皟搴﹀憳涓€涓汉鐨勬搷浣滃彴銆傚畠鏄暣涓厤閫佸喅绛栫殑璇佹嵁涓績銆?,
    "",
    "涓€鍙ヨ瘽鎬荤粨",
    "甯傞潰鍞竴涓€濂楀皢涔濆ぇ绠楁硶宀椾綅鍖栥€佹帴鍔涙眰瑙ｉ€忔槑鍖栥€佽皟搴﹀喅绛栧彲瑙ｉ噴鍖栥€佸叏娴佺▼鐣欑棔姘镐箙鍖栫殑澶氭尝娆￠厤閫佽皟搴︾郴缁熴€?,
    "",
    "涓嶉檺琛屼笟锛屼笉闄愪笟鎬併€備粠鈥滅畻寰楀嚭鈥濆埌鈥滅畻寰楀銆佽寰楁竻銆佺暀寰椾綇銆佺粡寰楄捣杩介棶鈥濄€?,
  ].join("\n");
}

function clearProcessTypingTimers() {
  processTypingTimers.forEach((timer) => clearTimeout(timer));
  processTypingTimers = [];
}

function typeProcessText(box, text) {
  box.textContent = "";
  let index = 0;
  const punctuationPauses = new Set(["锛?, "銆?, "锛?, "锛?, "锛?, "锛?, ",", ".", ":", ";", "!", "?", "\n"]);
  const tick = () => {
    if (index >= text.length) {
      return;
    }
    const nextChar = text[index];
    const step = /[a-zA-Z0-9]/.test(nextChar) ? 2 : 1;
    index = Math.min(text.length, index + step);
    box.textContent = text.slice(0, index);
    box.scrollTop = box.scrollHeight;
    const pause = punctuationPauses.has(text[index - 1]) ? 220 : 95;
    const timer = setTimeout(tick, pause);
    processTypingTimers.push(timer);
  };
  tick();
}

function openProcessModal(resultKey, waveId, plateNo, tripNo) {
  const matched = state.lastResults.map((result) => {
    const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
    if (waveIndex < 0) return null;
    const plan = result.solution[waveIndex].find((item) => item.vehicle.plateNo === plateNo);
    const trip = plan?.trips.find((item) => String(item.tripNo) === String(tripNo));
    if (!plan || !trip) return null;
    return { result, wave: result.scenario.waves[waveIndex], plan, trip };
  }).filter(Boolean);
  if (!matched.length) return;
  const modal = document.getElementById("processModal");
  const preferredIndex = matched.findIndex((item) => item.result.key === resultKey);
  if (preferredIndex > 0) {
    const [preferred] = matched.splice(preferredIndex, 1);
    matched.unshift(preferred);
  }
  document.getElementById("processModalTitle").textContent = lang() === "ja" ? `${plateNo} 路 ${waveId} 路 绗?${tripNo} 渚?路 瑜囨暟銈儷銈淬儶銈恒儬${L("playback")}` : `${plateNo} 路 ${waveId} 路 绗?${tripNo} ${L("tripSuffix")} 路 澶氱畻娉?{L("playback")}`;
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
    const box = document.getElementById(`processTypewriter-${index}`);
    if (box) typeProcessText(box, buildProcessNarrative(item.result, item.plan, item.trip, item.result.scenario, item.wave, matched.length > 1));
  });
}

function closeProcessModal() {
  const modal = document.getElementById("processModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  clearProcessTypingTimers();
}

function destroyRouteLeafletMap() {
  routeAmapMarkers.forEach((marker) => {
    try { marker.setMap?.(null); } catch {}
  });
  routeAmapMarkers = [];
  if (routeAmapPolyline) {
    try { routeAmapPolyline.setMap?.(null); } catch {}
    routeAmapPolyline = null;
  }
  if (routeAmapMap) {
    try { routeAmapMap.destroy?.(); } catch {}
    routeAmapMap = null;
  }
  if (routeLeafletLayerGroup) {
    routeLeafletLayerGroup.clearLayers();
    routeLeafletLayerGroup = null;
  }
  if (routeLeafletMap) {
    routeLeafletMap.remove();
    routeLeafletMap = null;
  }
}

function renderInteractiveRouteMap(containerId, mapData) {
  const container = document.getElementById(containerId);
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
    try {
      routeAmapMap.addControl(new window.AMap.ToolBar({ position: "RB" }));
      routeAmapMap.addControl(new window.AMap.Scale());
    } catch {}
    const lineLngLats = (mapData.polylinePoints || [])
      .filter((point) => Array.isArray(point) && point.length === 2)
      .map((point) => [Number(point[0]), Number(point[1])]);
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
      const lng = Number(item.lng);
      const lat = Number(item.lat);
      if (!Number.isFinite(lng) || !Number.isFinite(lat)) return [];
      boundsPoints.push([lng, lat]);
      const labelHtml = `
        <div class="leaflet-route-marker ${item.label === "D" ? "is-depot" : ""}">
          <span class="leaflet-route-badge">${escapeHtml(item.label)}</span>
          <span class="leaflet-route-text">${escapeHtml(item.shortName || item.name)}</span>
        </div>
      `;
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
      return marker;
    });
    if (routeAmapMarkers.length) routeAmapMap.add(routeAmapMarkers);
    if (boundsPoints.length || lineLngLats.length) {
      routeAmapMap.setFitView([...(routeAmapMarkers || []), ...(routeAmapPolyline ? [routeAmapPolyline] : [])], false, [70, 90, 70, 90], 16);
    } else {
      routeAmapMap.setZoomAndCenter(11, [Number(DC.lng), Number(DC.lat)]);
    }
    setTimeout(() => routeAmapMap?.resize?.(), 60);
    return true;
  }
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
  if (lineLatLngs.length) {
    window.L.polyline(lineLatLngs, {
      color: "#2563eb",
      weight: 5,
      opacity: 0.9,
    }).addTo(routeLeafletLayerGroup);
  }

  const markerLatLngs = [];
  (mapData.markers || []).forEach((item) => {
    const lat = Number(item.lat);
    const lng = Number(item.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    markerLatLngs.push([lat, lng]);
    const markerHtml = `
      <div class="leaflet-route-marker ${item.label === "D" ? "is-depot" : ""}">
        <span class="leaflet-route-badge">${escapeHtml(item.label)}</span>
        <span class="leaflet-route-text">${escapeHtml(item.shortName || item.name)}</span>
      </div>
    `;
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
  if (bounds.isValid()) routeLeafletMap.fitBounds(bounds.pad(0.18));
  else routeLeafletMap.setView([Number(DC.lat), Number(DC.lng)], 11);
  setTimeout(() => routeLeafletMap?.invalidateSize(), 50);
  return true;
}

function buildFallbackRouteMapData(result, trip) {
  const nodes = [DC, ...trip.stops.map((stop) => result.scenario.storeMap.get(stop.storeId) || { id: stop.storeId, name: stop.storeName, lng: 0, lat: 0 }), DC]
    .filter((node) => Number.isFinite(Number(node.lng)) && Number.isFinite(Number(node.lat)));
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

function renderFallbackRouteMap(containerId, mapData) {
  const container = document.getElementById(containerId);
  if (!container) return false;
  const points = (mapData.polylinePoints || []).filter((point) => Array.isArray(point) && point.length === 2);
  const markers = mapData.markers || [];
  const allPoints = [...points, ...markers.map((item) => [Number(item.lng), Number(item.lat)])];
  if (!allPoints.length) return false;
  container.innerHTML = `
    <div class="route-panzoom">
      <div class="route-panzoom-toolbar">
        <button type="button" class="mini route-zoom-in">+</button>
        <button type="button" class="mini route-zoom-out">-</button>
        <span class="muted">${lang() === "ja" ? "瀹熷湴鍥炽儥銉笺偣 / 銉夈儵銉冦偘銉汇儧銈ゃ兗銉搷浣溿伀瀵惧繙" : "鐪熷疄搴曞浘锛屽彲鎷栧姩 / 婊氳疆缂╂斁"}</span>
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
  const content = container.querySelector(".route-panzoom-content");
  const tileLayer = container.querySelector(".route-tilemap-layer");
  const overlay = container.querySelector(".route-tilemap-overlay");
  if (!viewport || !content || !tileLayer || !overlay) return false;
  const tileSize = 256;
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));
  const project = (lng, lat, zoom) => {
    const sinLat = Math.sin((Number(lat) * Math.PI) / 180);
    const scale = tileSize * 2 ** zoom;
    const x = ((Number(lng) + 180) / 360) * scale;
    const y = (0.5 - Math.log((1 + sinLat) / (1 - sinLat)) / (4 * Math.PI)) * scale;
    return { x, y };
  };
  const unproject = (x, y, zoom) => {
    const scale = tileSize * 2 ** zoom;
    const lng = (x / scale) * 360 - 180;
    const n = Math.PI - (2 * Math.PI * y) / scale;
    const lat = (180 / Math.PI) * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
    return { lng, lat };
  };
  const getViewportSize = () => ({
    width: Math.max(320, viewport.clientWidth || 960),
    height: Math.max(320, viewport.clientHeight || 640),
  });
  const chooseZoom = (width, height) => {
    for (let zoom = 17; zoom >= 3; zoom -= 1) {
      const projected = allPoints.map(([lng, lat]) => project(lng, lat, zoom));
      const xs = projected.map((item) => item.x);
      const ys = projected.map((item) => item.y);
      const spanX = Math.max(...xs) - Math.min(...xs);
      const spanY = Math.max(...ys) - Math.min(...ys);
      if (spanX <= width - 140 && spanY <= height - 140) return zoom;
    }
    return 3;
  };
  const initialSize = getViewportSize();
  const centerSeed = {
    lng: allPoints.reduce((sum, point) => sum + Number(point[0]), 0) / allPoints.length,
    lat: allPoints.reduce((sum, point) => sum + Number(point[1]), 0) / allPoints.length,
  };
  let zoom = chooseZoom(initialSize.width, initialSize.height);
  let center = { ...centerSeed };
  let dragging = false;
  let dragStartX = 0;
  let dragStartY = 0;
  let dragCenterWorld = null;
  const wrapTileX = (x, maxTile) => ((x % maxTile) + maxTile) % maxTile;
  const buildMarkerSvg = (item, x, y) => {
    const depot = item.label === "D";
    const labelWidth = Math.max(88, String(item.shortName || item.name).length * 12);
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
    const { width, height } = getViewportSize();
    overlay.setAttribute("viewBox", `0 0 ${width} ${height}`);
    const centerWorld = project(center.lng, center.lat, zoom);
    const topLeftX = centerWorld.x - width / 2;
    const topLeftY = centerWorld.y - height / 2;
    const scale = tileSize * 2 ** zoom;
    const maxTile = 2 ** zoom;
    const startTileX = Math.floor(topLeftX / tileSize);
    const endTileX = Math.floor((topLeftX + width) / tileSize);
    const startTileY = Math.floor(topLeftY / tileSize);
    const endTileY = Math.floor((topLeftY + height) / tileSize);
    const tiles = [];
    for (let tileX = startTileX; tileX <= endTileX; tileX += 1) {
      for (let tileY = startTileY; tileY <= endTileY; tileY += 1) {
        if (tileY < 0 || tileY >= maxTile) continue;
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
    const polylinePath = points.map(([lng, lat], index) => {
      const projected = project(lng, lat, zoom);
      const x = projected.x - topLeftX;
      const y = projected.y - topLeftY;
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
    const markerSvg = markers.map((item) => {
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
    if (!dragging || !dragCenterWorld) return;
    const { width, height } = getViewportSize();
    const nextWorldX = dragCenterWorld.x - (event.clientX - dragStartX);
    const nextWorldY = dragCenterWorld.y - (event.clientY - dragStartY);
    center = unproject(nextWorldX, nextWorldY, zoom);
    center.lat = clamp(center.lat, -85, 85);
    render();
  });
  const stopDragging = () => { dragging = false; };
  viewport?.addEventListener("pointerup", stopDragging);
  viewport?.addEventListener("pointerleave", stopDragging);
  container.querySelector(".route-zoom-in")?.addEventListener("click", () => { zoom = clamp(zoom + 1, 3, 18); render(); });
  container.querySelector(".route-zoom-out")?.addEventListener("click", () => { zoom = clamp(zoom - 1, 3, 18); render(); });
  window.requestAnimationFrame(render);
  return true;
}

function buildRouteStopScheduleTable(trip) {
  const rows = (trip?.stops || []).map((stop, index) => `
    <tr>
      <td>${index + 1}</td>
      <td>${escapeHtml(`${stop.storeId || ""} ${stop.storeName || ""}`.trim())}</td>
      <td>${formatTime(stop.arrival)}</td>
      <td>${escapeHtml(stop.desiredArrival || "--:--")}</td>
    </tr>
  `).join("");
  return `
    <div class="route-stop-card">
      <div class="route-stop-card-title">${lang() === "ja" ? "搴楄垪鍒扮潃瑷堢敾" : "闂ㄥ簵鍒板簵璁″垝"}</div>
      <div class="table-wrap route-stop-table-wrap">
        <table class="route-stop-table">
          <tr>
            <th>${L("routeStopSeq")}</th>
            <th>${L("routeStopName")}</th>
            <th>${L("routePlanArrival")}</th>
            <th>${L("routeDesiredArrival")}</th>
          </tr>
          ${rows || `<tr><td colspan="4">${lang() === "ja" ? "搴楄垪銇亗銈娿伨銇涖倱" : "鏆傛棤闂ㄥ簵"}</td></tr>`}
        </table>
      </div>
    </div>
  `;
}

async function openRouteMapModal(resultKey, waveId, plateNo, tripNo) {
  const result = state.lastResults.find((item) => item.key === resultKey);
  if (!result) return;
  const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
  if (waveIndex < 0) return;
  const wave = result.scenario.waves[waveIndex];
  const plan = result.solution[waveIndex].find((item) => item.vehicle.plateNo === plateNo);
  const trip = plan?.trips.find((item) => String(item.tripNo) === String(tripNo));
  if (!plan || !trip) return;
  const modal = document.getElementById("routeMapModal");
  const title = document.getElementById("routeMapModalTitle");
  const body = document.getElementById("routeMapModalBody");
  const fallbackMapData = buildFallbackRouteMapData(result, trip);
  title.textContent = `${plateNo} 路 ${waveId} 路 ${L("tripNo")}${tripNo}${L("tripSuffix")} 路 ${L("routeMap")}`;
  body.innerHTML = `
    <div class="route-map-shell">
        <div id="routeInteractiveMapFallback" class="route-map-interactive"></div>
        <div class="route-map-meta">
          <p class="muted">${L("route")}锛?{buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? "銇俱仛銉兗銈儷銇瑭卞瀷銉兗銉堝洺銈掕〃绀恒仐銆佸閮ㄣ儷銉笺儓銉囥兗銈裤亴鍙栥倢銈屻伆鑷嫊銇ф洿鏂般仐銇俱仚銆? : "鍏堝睍绀烘湰鍦颁氦浜掔嚎璺浘锛岃嫢澶栭儴绾胯矾鏁版嵁鍙敤浼氳嚜鍔ㄥ崌绾ф垚鏇村畬鏁寸殑绾胯矾鍥俱€?}</p>
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
  try {
    const mapData = await getTripRouteMapData(result, wave, plan, trip);
    body.innerHTML = `
      <div class="route-map-shell">
        <div id="routeInteractiveMap" class="route-map-interactive"></div>
        <div class="route-map-meta">
          <p class="muted">${L("route")}锛?{buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? `銇撱伄鍦板洺銇儔銉┿儍銈般兓鎷″ぇ绺皬銇с亶銇俱仚銆傚簳鍥炽伅浜や簰鍦板洺銆侀爢璺伅鏃㈠瓨銇疅绾胯矾銉囥兗銈裤倰鎻忕敾銇椼仸銇勩伨銇欍€俙 : `杩欏紶鍥剧幇鍦ㄥ彲浠ユ嫋鍔ㄥ拰缂╂斁锛屽簳鍥惧彲浜や簰锛岄『璺户缁娇鐢ㄧ幇鏈夌湡瀹炵嚎璺暟鎹粯鍒躲€俙}</p>
          <div class="route-map-legend">
            ${mapData.markers.map((item) => `<span class="route-map-chip"><strong>${escapeHtml(item.label)}</strong>${escapeHtml(item.name)}</span>`).join("")}
          </div>
          ${buildRouteStopScheduleTable(trip)}
        </div>
      </div>
    `;
    if (!renderInteractiveRouteMap("routeInteractiveMap", mapData)) {
      body.innerHTML = `
        <div class="route-map-shell">
          <div id="routeInteractiveMapFallback" class="route-map-interactive"></div>
          <div class="route-map-meta">
            <p class="muted">${L("route")}锛?{buildRouteDisplay(trip.route, result.scenario)}</p>
            <p class="muted">${lang() === "ja" ? `浜や簰搴曞洺銇銇胯炯銇裤伀澶辨晽銇椼仧銇熴倎銆併儹銉笺偒銉氦浜掔嚎璺浘銇垏銈婃浛銇堛伨銇椼仧銆俙 : `浜や簰搴曞浘鍔犺浇澶辫触锛屽凡鍒囨崲鍒版湰鍦颁氦浜掔嚎璺浘銆俙}</p>
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
          <p class="muted">${L("route")}锛?{buildRouteDisplay(trip.route, result.scenario)}</p>
          <p class="muted">${lang() === "ja" ? "澶栭儴鍦板洺銇銇胯炯銇裤伀澶辨晽銇椼仧銇熴倎銆併儹銉笺偒銉伄銉兗銉堝洺銇ц〃绀恒仐銇︺亜銇俱仚銆? : "澶栭儴鍦板浘鍔犺浇澶辫触锛屽凡鏀圭敤鏈湴绾胯矾鍥炬樉绀恒€?} ${escapeHtml(error?.message || "")}</p>
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
  const modal = document.getElementById("routeMapModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function openShowcaseModal() {
  const modal = document.getElementById("showcaseModal");
  const box = document.getElementById("showcaseTypewriter");
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  clearProcessTypingTimers();
  typeProcessText(box, buildShowcaseNarrative());
}

function closeShowcaseModal() {
  const modal = document.getElementById("showcaseModal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  clearProcessTypingTimers();
}

function buildAdjustControls(result, waveId, sourcePlate, stopId, plans) {
  const options = plans
    .filter((plan) => plan.vehicle.plateNo !== sourcePlate)
    .map((plan) => `<option value="${plan.vehicle.plateNo}">${plan.vehicle.plateNo}</option>`)
    .join("");
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

function renderStopCards(result, wave, plan, trip, plans) {
  const adjustment = result.lastAdjustment;
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
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function persistRelayConsoleLines() {
  try {
    localStorage.setItem("vrptw-live-console", JSON.stringify(relayConsoleLines.slice(-400)));
  } catch {}
}

function flushRelayConsoleLogQueue() {
  if (relayConsoleLogFlushTimer) {
    clearTimeout(relayConsoleLogFlushTimer);
    relayConsoleLogFlushTimer = null;
  }
  if (!relayConsolePendingLogLines.length) return;
  const batch = relayConsolePendingLogLines.splice(0, relayConsolePendingLogLines.length);
  const payloadText = batch.join("\n");
  try {
    fetch(`${GA_BACKEND_URL}/sfrz/log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ line: payloadText }),
    }).catch(() => {});
  } catch (_) {}
}

function scheduleRelayConsoleLogFlush() {
  if (relayConsoleLogFlushTimer) return;
  relayConsoleLogFlushTimer = setTimeout(flushRelayConsoleLogQueue, 250);
}

function openRelayConsoleModal(title = "") {
  relayConsoleLines = [];
  relayConsolePendingLogLines = [];
  if (relayConsoleLogFlushTimer) {
    clearTimeout(relayConsoleLogFlushTimer);
    relayConsoleLogFlushTimer = null;
  }
  const modal = document.getElementById("relayConsoleModal");
  const body = document.getElementById("relayConsoleBody");
  const titleNode = document.getElementById("relayConsoleTitle");
  if (body) body.textContent = "";
  if (titleNode) titleNode.textContent = title || (lang() === "ja" ? "銉儸銉兼渶閬╁寲銇亷绋? : "鎺ュ姏姹傝В杩囩▼");
  persistRelayConsoleLines();
  modal?.classList.remove("hidden");
  modal?.setAttribute("aria-hidden", "false");
}

function appendRelayConsoleLine(text) {
  relayConsoleLines.push(text);
  relayConsolePendingLogLines.push(String(text || ""));
  persistRelayConsoleLines();
  scheduleRelayConsoleLogFlush();
  const body = document.getElementById("relayConsoleBody");
  if (body) {
    body.insertAdjacentText("beforeend", `${body.textContent ? "\n" : ""}${text}`);
    body.scrollTop = body.scrollHeight;
  }
}

function reportRelayStageProgress(text) {
  if (typeof relayStageReporter === "function" && text) relayStageReporter(text);
}

function reportStrategyAuditToRelayConsole(strategyAudit, algoName = "绠楁硶") {
  if (!strategyAudit || typeof strategyAudit !== "object") return;
  const waveId = String(strategyAudit.waveId || "").trim() || "-";
  const inputCount = Number(strategyAudit.inputStoreCount || 0);
  const outputCount = Number(strategyAudit.outputStoreCount || 0);
  const zeroIds = Array.isArray(strategyAudit.filteredZeroLoadStoreIds)
    ? strategyAudit.filteredZeroLoadStoreIds.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const scopeIds = Array.isArray(strategyAudit.filteredWaveScopeStoreIds)
    ? strategyAudit.filteredWaveScopeStoreIds.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const emitStoreIdChunks = (title, ids) => {
    const list = Array.isArray(ids) ? ids : [];
    reportRelayStageProgress(`绛栫暐涓績瀹¤锛?{algoName} ${waveId}锛夛細${title} ${list.length} 瀹躲€俙);
    if (!list.length) return;
    const chunkSize = 20;
    for (let i = 0; i < list.length; i += chunkSize) {
      const chunk = list.slice(i, i + chunkSize);
      reportRelayStageProgress(`绛栫暐涓績瀹¤锛?{algoName} ${waveId}锛夛細${title}鏄庣粏 ${i + 1}-${Math.min(i + chunkSize, list.length)}锛?{chunk.join("銆?)}銆俙);
    }
  };
  reportRelayStageProgress(`绛栫暐涓績瀹¤锛?{algoName} ${waveId}锛夛細杈撳叆闂ㄥ簵 ${inputCount} 瀹讹紝绛栫暐鍚?${outputCount} 瀹躲€俙);
  emitStoreIdChunks("杩囨护闆惰揣閲?, zeroIds);
  emitStoreIdChunks("杩囨护闈炴湰娉㈡", scopeIds);
  if (waveId === "W3") {
    reportRelayStageProgress(`绛栫暐涓績瀹¤锛?{algoName} ${waveId}锛夛細W3鎸夊崟绋嬮噷绋嬭鍒欐墽琛岋紙鐢卞悗绔害鏉熷眰鍒ゅ畾锛夈€俙);
  }
}

function reportBackendUnscheduledToRelayConsole(unscheduledStores, algoName = "绠楁硶", waveId = "-") {
  const rows = Array.isArray(unscheduledStores) ? unscheduledStores : [];
  if (!rows.length) {
    reportRelayStageProgress(`鍚庣鏈垎閰嶈瘖鏂紙${algoName} ${waveId}锛夛細0 瀹躲€俙);
    return;
  }
  const groups = new Map();
  for (const row of rows) {
    const reason = String(row?.reasonText || row?.reason || "鏈煡").trim() || "鏈煡";
    const storeId = String(row?.storeId || "").trim();
    if (!groups.has(reason)) groups.set(reason, []);
    if (storeId) groups.get(reason).push(storeId);
  }
  reportRelayStageProgress(`鍚庣鏈垎閰嶈瘖鏂紙${algoName} ${waveId}锛夛細鍏?${rows.length} 瀹躲€俙);
  for (const [reason, ids] of groups.entries()) {
    reportRelayStageProgress(`鍚庣鏈垎閰嶈瘖鏂紙${algoName} ${waveId}锛夛細${reason} ${ids.length} 瀹躲€俙);
    const chunkSize = 20;
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      reportRelayStageProgress(`鍚庣鏈垎閰嶈瘖鏂紙${algoName} ${waveId}锛夛細${reason}鏄庣粏 ${i + 1}-${Math.min(i + chunkSize, ids.length)}锛?{chunk.join("銆?)}銆俙);
    }
  }
}

function closeRelayConsoleModal() {
  flushRelayConsoleLogQueue();
  const modal = document.getElementById("relayConsoleModal");
  modal?.classList.add("hidden");
  modal?.setAttribute("aria-hidden", "true");
}

function getRoutePalette(index) {
  const colors = ["#dc2626", "#2563eb", "#059669", "#d97706", "#7c3aed", "#0891b2", "#be123c", "#65a30d"];
  return colors[index % colors.length];
}

function sortPlansByPreference(plans, resultMetrics) {
  const usageMap = getUsageMap(resultMetrics);
  return [...plans].sort((a, b) => {
    const aUsage = usageMap.get(a.vehicle.plateNo) || { preferredMet: false, achievedRate: 0 };
    const bUsage = usageMap.get(b.vehicle.plateNo) || { preferredMet: false, achievedRate: 0 };
    return Number(bUsage.preferredMet) - Number(aUsage.preferredMet)
      || bUsage.achievedRate - aUsage.achievedRate
      || a.totalDistance - b.totalDistance
      || a.vehicle.plateNo.localeCompare(b.vehicle.plateNo);
  });
}

function renderWaveBlocks(result, selectedWaveIds = null) {
  const selectedSet = selectedWaveIds instanceof Set ? selectedWaveIds : null;
  return result.solution.map((plans, index) => {
    const wave = result.scenario.waves[index];
    if (!wave) return "";
    if (selectedSet && selectedSet.size && !selectedSet.has(String(wave.waveId || ""))) return "";
    const showReturnTime = wave.waveId === "W1";
    const used = sortPlansByPreference(plans.filter((plan) => plan.trips.length), result.metrics);
    const usageMap = getUsageMap(result.metrics);
    const overtimeTrips = used.flatMap((plan) => plan.trips.filter((trip) => (trip.waveLateMinutes || 0) > 0).map((trip) => ({ plateNo: plan.vehicle.plateNo, trip })));
    return `
      <section class="wave-block">
        <h3>${wave.waveId} ${wave.start} - ${wave.end}</h3>
        <p class="muted">${L("dispatchStart")}锛?{wave.start}锛?{lang() === "ja" ? "绶犲垏銉兗銉? : "鎴瑙勫垯"}锛?{(wave.endMode || "return") === "return" ? LT("waveModeReturn", { time: wave.end }) : LT("waveModeService", { time: wave.end })}${wave.singleWave ? `锛?{L("waveSingleHint")}` : `锛?{L("waveRegularHint")}`}</p>
        <p class="muted">${L("includedStores")}锛?{wave.storeList.join(",") || L("allStores")}</p>
        ${overtimeTrips.length ? `<div class="note overtime-note">${L("overtimeTrips")}锛?{overtimeTrips.map(({ plateNo, trip }) => `${plateNo} ${L("tripNo")}${trip.tripNo}${L("tripSuffix")}${L("overWave")} ${(trip.waveLateMinutes || 0).toFixed(0)} ${L("minutes")}`).join("锛?)}</div>` : ""}
        ${used.map((plan) => {
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
                    const truck = getTripTruckVisual(trip);
                    return `<div class="trip-hero">
                    <p class="route">${L("tripNo")} ${trip.tripNo} ${L("tripSuffix")}锛?{buildRouteDisplay(trip.route, result.scenario)}</p>
                      <div class="trip-truck-row">
                        <div class="trip-truck trip-truck-${truck.cls}">
                          <img src="${truck.src}" alt="${escapeHtml(`${plan.vehicle.plateNo} ${truck.badge}`)}" onerror="this.style.display='none';this.parentElement.classList.add('trip-truck-fallback')">
                          <span class="trip-truck-badge">${truck.badge}</span>
                          <span class="trip-truck-fallback-text">${truck.badge}</span>
                        </div>
                      </div>
                      <p class="trip-actions"><button class="alert open-process" data-result="${result.key}" data-wave="${wave.waveId}" data-plate="${plan.vehicle.plateNo}" data-trip="${trip.tripNo}">${L("viewViz")}</button><button class="secondary open-route-map" data-result="${result.key}" data-wave="${wave.waveId}" data-plate="${plan.vehicle.plateNo}" data-trip="${trip.tripNo}">${L("routeMap")}</button></p>
                      <p class="trip-footer muted">${showReturnTime ? `${L("returnTime")}锛?{formatTime(trip.finish)}锛宍 : ""}${L("backDistance")}锛?{trip.stops.length ? result.scenario.dist[trip.stops[trip.stops.length - 1].storeId][DC.id].toFixed(1) : "0.0"} km锛?{L("tripRoundKm")}锛?{trip.totalDistance.toFixed(1)} km锛?{L("tripLoadRate")}锛?{formatRate(trip.loadRate)}${(trip.waveLateMinutes || 0) > 0 ? `锛?span class="status-bad">${L("overWave")} ${(trip.waveLateMinutes || 0).toFixed(0)} ${L("minutes")}</span>` : ""}</p>
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

function renderResults() {
  const box = document.getElementById("resultPanels");
  if (!state.lastResults.length) { box.innerHTML = ""; return; }
  const activeKey = state.activeResultKey || state.lastResults[0]?.key;
  const activeResult = state.lastResults.find((item) => item.key === activeKey) || state.lastResults[0];
  const detailExpanded = !!state.ui.resultDetailExpanded;
  const waveIds = (activeResult?.scenario?.waves || []).map((wave) => String(wave.waveId || "")).filter(Boolean);
  const selectedWaveIds = new Set((state.ui.resultDetailWaveIds || []).map((id) => String(id || "")).filter((id) => waveIds.includes(id)));
  state.ui.resultDetailWaveIds = [...selectedWaveIds];
  const allWaveSelected = waveIds.length > 0 && waveIds.every((id) => selectedWaveIds.has(id));
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
          ${lang() === "ja" ? (detailExpanded ? "瑭崇窗銈掗枆銇樸倠" : "瑭崇窗銈掗枊銇?) : (detailExpanded ? "鏀惰捣鏄庣粏" : "灞曞紑鏄庣粏")}
        </button>
      </div>
      <div class="algo-summary"><span class="chip">${L("score")} ${activeResult.metrics.score.toFixed(1)}${lang() === "ja" ? " / 100锛堥珮銇勩伝銇╄壇銇勶級" : " / 100锛堣秺楂樿秺濂斤級"}</span><span class="chip">${L("onTime")} ${activeResult.metrics.totalOnTime}/${activeResult.metrics.totalStops}</span><span class="chip">${L("totalDistance")} ${activeResult.metrics.totalDistance.toFixed(1)} km</span><span class="chip">${L("avgLoad")} ${formatRate(activeResult.metrics.loadRate)}</span><span class="chip">${L("fleetLoad")} ${formatRate(activeResult.metrics.fleetLoadRate)}</span><span class="chip">${lang() === "ja" ? `浣跨敤杌婁浮 ${activeResult.metrics.usedVehicleCount} 鍙癭 : `宸茬敤杞﹁締 ${activeResult.metrics.usedVehicleCount} 杈哷}</span><span class="chip">${lang() === "ja" ? `寰呮杌婁浮 ${activeResult.metrics.unusedVehicleCount} 鍙癭 : `鏈敤杞﹁締 ${activeResult.metrics.unusedVehicleCount} 杈哷}</span><span class="chip">${lang() === "ja" ? `銉兗銉?${currentStrategyLabel()}` : `妯″紡 ${currentStrategyLabel()}`}</span><span class="chip">${lang() === "ja" ? `鏂归嚌 ${currentGoalLabel()}` : `鐩爣 ${currentGoalLabel()}`}</span></div>
      <div class="result-wave-filter">
        <span class="muted">${lang() === "ja" ? "娉㈡閬告姙" : "娉㈡閫夋嫨"}</span>
        <button class="${allWaveSelected ? "primary" : "secondary"} toggle-result-wave-filter" data-wave-filter-mode="all">${lang() === "ja" ? "鍏ㄩ儴" : "鍏ㄩ儴"}</button>
        ${waveIds.map((waveId) => `<button class="${selectedWaveIds.has(waveId) ? "primary" : "secondary"} toggle-result-wave-filter" data-wave-id="${escapeHtml(waveId)}">${escapeHtml(waveId)}</button>`).join("")}
      </div>
      ${detailExpanded ? `
      <div class="result-detail-body">
        <p class="note">${lang() === "ja" ? `銈广偝銈伅 100 鐐规簚鐐广仹銇欍€傜従鍦ㄣ伅銆?{currentGoalLabel()}銆嶉噸瑕栥仹涓︺伖銇ゃ仱銆佸畾鏅傛€с兓璺濋洟銉昏粖涓℃暟銉荤杓夈倐鍚堛倧銇涖仸瑭曚尽銇椼仸銇勩伨銇欍€俙 : `璇勫垎璇存槑锛?00鍒嗗埗銆傚綋鍓嶆寜鈥?{currentGoalLabel()}鈥濇潈閲嶆帓搴忥紝鍚屾椂缁煎悎鍙傝€冨噯鐐广€侀噷绋嬨€佽溅杈嗕娇鐢ㄥ拰瑁呰浇琛ㄧ幇銆俙}</p>
        ${activeResult.adjustMessage ? `<p class="note">${activeResult.adjustMessage}</p>` : ""}
        ${selectedWaveIds.size ? renderWaveBlocks(activeResult, selectedWaveIds) : `<div class="result-detail-collapsed">${lang() === "ja" ? "鍏堛伀娉㈡銈掗伕鎶炪仐銇︺亱銈夎┏绱般倰闁嬨亜銇︺亸銇犮仌銇勩€? : "璇峰厛閫夋嫨瑕佹煡鐪嬬殑娉㈡锛屽啀灞曞紑鏄庣粏銆?}</div>`}
        <div class="result-detail-actions">
          <button class="secondary toggle-result-detail" data-result-key="${activeResult.key}">
            ${lang() === "ja" ? "瑭崇窗銈掗枆銇樸倠" : "鏀惰捣鏄庣粏"}
          </button>
        </div>
      </div>
      ` : `
      <div class="result-detail-collapsed">
        ${lang() === "ja" ? "瑭崇窗銇姌銈娿仧銇熴伨銈屻仸銇勩伨銇欍€傚繀瑕併仾銇ㄣ亶銇犮亼闁嬨亜銇︾⒑瑾嶃仹銇嶃伨銇欍€? : "鏄庣粏宸叉敹璧枫€傞渶瑕佸畾浣嶉棶棰樻椂鍐嶅睍寮€锛岃繖鏍烽〉闈㈡洿鐭紝鏂逛究鎴浘銆?}
      </div>
      `}
    </article>
    ` : ""}
  `;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function updateGenerationProgress(progress, text) {
  state.ui.generating = progress < 100;
  state.ui.progress = progress;
  state.ui.progressText = text;
  renderGenerationProgress();
  await sleep(30);
}

function solveTwoVehicleTransfer(scenario, wave, sourceVehicle, targetVehicle, sourceIds, targetIds, movedStoreId) {
  const nextSourceIds = sourceIds.filter((id) => id !== movedStoreId);
  const nextTargetIds = [...targetIds, movedStoreId];
  const sourcePlan = solveFixedMembershipPlan(sourceVehicle, nextSourceIds, scenario, wave, sourceVehicle);
  const targetPlan = solveFixedMembershipPlan(targetVehicle, nextTargetIds, scenario, wave, targetVehicle);
  if (!sourcePlan || !targetPlan) return null;
  return [sourcePlan, targetPlan];
}

function applyStoreTransfer(resultKey, waveId, sourcePlate, targetPlate, storeId) {
  const result = state.lastResults.find((item) => item.key === resultKey);
  const box = document.getElementById("validationBox");
  if (!result) return;
  if (!targetPlate || targetPlate === sourcePlate) {
    result.adjustMessage = L("chooseDifferentVehicle");
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }

  const waveIndex = result.scenario.waves.findIndex((wave) => wave.waveId === waveId);
  if (waveIndex < 0) return;
  const nextSolution = clone(result.solution);
  const plans = nextSolution[waveIndex];
  const sourceIndex = plans.findIndex((plan) => plan.vehicle.plateNo === sourcePlate);
  const targetIndex = plans.findIndex((plan) => plan.vehicle.plateNo === targetPlate);
  if (sourceIndex < 0 || targetIndex < 0) return;

  const sourceIds = flattenPlanStoreIds(plans[sourceIndex]);
  const targetIds = flattenPlanStoreIds(plans[targetIndex]);
  if (!sourceIds.includes(storeId)) {
    result.adjustMessage = LT("storeNotOnVehicle", { plate: sourcePlate, store: storeId });
    renderAnalytics();
    renderResults();
    renderStoresTable();
    return;
  }

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

function getResultByKey(resultKey) {
  return state.lastResults.find((item) => item.key === resultKey) || null;
}

function getWaveAndPlans(result, waveId) {
  const waveIndex = result?.scenario?.waves?.findIndex((wave) => wave.waveId === waveId) ?? -1;
  if (waveIndex < 0) return null;
  return {
    waveIndex,
    wave: result.scenario.waves[waveIndex],
    plans: result.solution[waveIndex],
  };
}

function syncResultAfterLocalAdjustment(result) {
  applyFinalRuleToResult(result, result.scenario);
  reportWaveCandidateAssignedPendingStats("灞€閮ㄨ皟鏁村悗", result.solution || [], result.scenario);
}

function getLiveUnscheduledItems(result) {
  if (!result?.scenario) return [];
  return computeFinalPendingByWave(result.solution || [], result.scenario);
}

function getStoreAssignmentCoverage(store = {}, assignmentMap = new Map()) {
  const variants = buildStoreKeyVariants(store?.id);
  const candidateWaves = ["W1", "W2", "W3", "W4"].filter((waveId) => isStoreCandidateForWaveRule(store, waveId));
  const assignedByWave = new Map();
  for (const waveId of candidateWaves) {
    let plateNo = "";
    for (const variant of variants) {
      const key = buildStoreWaveAssignmentKey(variant, waveId);
      const hit = key ? assignmentMap.get(key) : null;
      if (hit && String(hit.plateNo || "").trim()) {
        plateNo = String(hit.plateNo || "").trim();
        break;
      }
    }
    if (plateNo) assignedByWave.set(waveId, plateNo);
  }
  const assignedWaves = [...assignedByWave.keys()];
  const pendingWaves = candidateWaves.filter((waveId) => !assignedByWave.has(waveId));
  const isFullyAssigned = candidateWaves.length > 0 && pendingWaves.length === 0;
  let displayPlate = "-";
  if (isFullyAssigned && assignedWaves.length) {
    const uniquePlates = new Set([...assignedByWave.values()]);
    if (uniquePlates.size === 1) {
      displayPlate = [...uniquePlates][0];
    } else {
      displayPlate = assignedWaves.map((waveId) => `${waveId}:${assignedByWave.get(waveId)}`).join(" / ");
    }
  }
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
  if (!result?.solution?.length) return 0;
  const stores = Array.isArray(state.stores) ? state.stores : [];
  const assignmentMap = result?.storeAssignmentMap instanceof Map
    ? result.storeAssignmentMap
    : buildStoreAssignmentMapFromSolution(result.solution || []);
  let count = 0;
  stores.forEach((store) => {
    const hasAnyResolvedLoad = (
      Number(store?.resolvedWave1Load || 0) > 0
      || Number(store?.resolvedWave2Load || 0) > 0
      || Number(store?.resolvedWave3Load || 0) > 0
      || Number(store?.resolvedWave4Load || 0) > 0
    );
    if (!hasAnyResolvedLoad) return;
    const coverage = getStoreAssignmentCoverage(store, assignmentMap);
    if (coverage.candidateWaves.length && !coverage.isFullyAssigned) count += 1;
  });
  return count;
}

function exportLiveUnscheduledIds(resultKey = "") {
  const result = getResultByKey(resultKey) || state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0];
  const box = document.getElementById("validationBox");
  if (!result) {
    if (box) box.textContent = L("exportNoResult");
    return;
  }
  const items = getLiveUnscheduledItems(result);
  const stats = computeWaveCandidateAssignedPendingStats(result.solution || [], result.scenario);
  const lines = [];
  lines.push(`绠楁硶: ${result.label || result.key || "-"}`);
  lines.push(`瀵煎嚭鏃堕棿: ${new Date().toLocaleString("zh-CN", { hour12: false })}`);
  lines.push(`鏈皟搴﹀簵-娉㈡鏉℃暟: ${items.length}`);
  lines.push(`鏈皟搴﹂棬搴楀幓閲嶆暟: ${new Set(items.map((x) => normalizeStoreKey(x.storeId))).size}`);
  lines.push(`鏄庣粏绛涢€夋湭璋冨害闂ㄥ簵鏁? ${computeStoreDetailUnscheduledCount(result)}`);
  lines.push("");
  lines.push("=== 瀹炴椂闆嗗悎鏍稿 ===");
  stats.forEach((row) => {
    lines.push(`${row.waveId}: candidate=${row.candidateCount}, assigned=${row.assignedCount}, pending=${row.pendingCount}`);
  });
  lines.push("");
  lines.push("=== 鏈皟搴﹀簵-娉㈡鏄庣粏 ===");
  items.forEach((item) => {
    lines.push(`${item.waveId},${normalizeStoreKey(item.storeId)},${String(item.storeName || "")}`);
  });
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  const fileName = `鏈皟搴﹀疄鏃堕泦鍚坃${result.key || "result"}_${timestamp}.txt`;
  const blob = new Blob([`\uFEFF${lines.join("\r\n")}`], { type: "text/plain;charset=utf-8" });
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

function collectCandidatePlansForUnscheduled(plans, preferredPlate = "") {
  const sorted = [...plans].sort((a, b) => {
    const aEmpty = a.trips.length ? 1 : 0;
    const bEmpty = b.trips.length ? 1 : 0;
    if (aEmpty !== bEmpty) return aEmpty - bEmpty;
    if (preferredPlate) {
      const aPreferred = a.vehicle.plateNo === preferredPlate ? 0 : 1;
      const bPreferred = b.vehicle.plateNo === preferredPlate ? 0 : 1;
      if (aPreferred !== bPreferred) return aPreferred - bPreferred;
    }
    return a.totalDistance - b.totalDistance || a.vehicle.plateNo.localeCompare(b.vehicle.plateNo);
  });
  const emptyOnly = sorted.filter((plan) => !plan.trips.length);
  return emptyOnly.length ? emptyOnly : sorted;
}

function tryAssignStoreToSpecificPlan(result, waveId, targetPlate, storeId) {
  const target = getWaveAndPlans(result, waveId);
  if (!target) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  const store = result.scenario.storeMap.get(storeId);
  if (!store) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  const targetPlanIndex = target.plans.findIndex((plan) => plan.vehicle.plateNo === targetPlate);
  if (targetPlanIndex < 0) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  const candidate = buildTripCandidate(target.plans[targetPlanIndex], store, result.scenario, target.wave, false, { allowToleranceBreak: false });
  if (!candidate?.nextPlan) return { ok: false, message: LT("assignFailed", { store: storeId, plate: targetPlate }) };
  target.plans[targetPlanIndex] = candidate.nextPlan;
  syncResultAfterLocalAdjustment(result);
  result.adjustMessage = LT("assignSuccess", { store: store.name || storeId, plate: targetPlate });
  return { ok: true, message: result.adjustMessage };
}

function rescheduleUnscheduledStores(resultKey) {
  const result = getResultByKey(resultKey);
  const box = document.getElementById("validationBox");
  if (!result) return;
  applyFinalRuleToResult(result, result.scenario);
  const pending = [...getLiveUnscheduledItems(result)];
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
  let round = 0;
  while (round < 6) {
    const latestPending = computeFinalPendingByWave(result.solution || [], result.scenario);
    if (!latestPending.length) break;
    let roundScheduled = 0;
    for (const item of latestPending) {
      const target = getWaveAndPlans(result, item.waveId);
      const store = result.scenario.storeMap.get(item.storeId);
      if (!target || !store) continue;
      const candidatePlans = collectCandidatePlansForUnscheduled(target.plans);
      for (const plan of candidatePlans) {
        const candidate = buildTripCandidate(plan, store, result.scenario, target.wave, false, { allowToleranceBreak: false });
        if (!candidate?.nextPlan) continue;
        const planIndex = target.plans.findIndex((one) => one.vehicle.plateNo === plan.vehicle.plateNo);
        if (planIndex < 0) continue;
        target.plans[planIndex] = candidate.nextPlan;
        scheduledNow += 1;
        roundScheduled += 1;
        break;
      }
    }
    applyFinalRuleToResult(result, result.scenario);
    reportWaveCandidateAssignedPendingStats(`琛ユ帓绗?{round + 1}杞悗`, result.solution || [], result.scenario);
    if (roundScheduled <= 0) break;
    round += 1;
  }
  syncResultAfterLocalAdjustment(result);
  const remainingCount = Number(result?.metrics?.unscheduledCount || 0);
  result.adjustMessage = scheduledNow > 0
    ? `${LT("rescheduleProgress", { count: scheduledNow })}锛?{lang() === "ja" ? `鏈壊褰撴畫銈?${remainingCount} 浠禶 : `鍓╀綑鏈皟搴?${remainingCount} 瀹禶}`
    : `${L("rescheduleNoProgress")}锛?{lang() === "ja" ? `浠婂洖銈?${pending.length} 浠躲仺銈傛潯浠躲伀鍚堛亜銇俱仜銈撱仹銇椼仧銆俙 : `鏈疆 ${pending.length} 瀹堕兘鏈弧瓒虫潯浠躲€俙}`;
  box.textContent = result.adjustMessage;
  renderVehicles();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderStoresTable();
}

function buildUnscheduledAssignControls(result, item) {
  const target = getWaveAndPlans(result, item.waveId);
  if (!target) return `<span class="adjust-note">${L("noAssignableVehicle")}</span>`;
  const options = collectCandidatePlansForUnscheduled(target.plans)
    .map((plan) => `<option value="${plan.vehicle.plateNo}">${plan.vehicle.plateNo}</option>`)
    .join("");
  if (!options) return `<span class="adjust-note">${L("noAssignableVehicle")}</span>`;
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
  const target = getWaveAndPlans(result, item.waveId);
  const store = result?.scenario?.storeMap?.get(item.storeId);
  if (!target || !store) return null;
  const plans = Array.isArray(target?.plans) ? target.plans : [];
  const baseDeparture = Math.max(
    Number(target.wave.startMin || 0),
    Number(target.wave.earliestDepartureMin || target.wave.startMin || 0),
    Number(result.scenario?.dispatchStartMin || 0)
  );
  let best = null;
  plans.forEach((plan) => {
    const vehicle = plan?.vehicle;
    if (!vehicle) return;
    const departure = Math.max(baseDeparture, Number(plan.availableTime || 0), Number(plan.earliestDepartureMin || 0));
    const distance = Number(result.scenario?.dist?.[DC.id]?.[store.id] || 0);
    const arrival = departure + getTravelMinutes(result.scenario, DC.id, store.id, Number(vehicle.speed || 38));
    if (!best || arrival < best.arrival) {
      best = {
        earliestArrival: arrival,
        distance,
        plateNo: vehicle.plateNo,
      };
    }
  });
  if (!best) return null;
  const timing = getStoreTimingForWave(store, target.wave, result.scenario.dispatchStartMin);
  const desiredArrivalMin = Number(timing.desiredArrivalMin || best.earliestArrival);
  const latestAllowed = Number(timing.latestAllowedArrivalMin || desiredArrivalMin || best.earliestArrival);
  const lateBy = Math.max(0, best.earliestArrival - latestAllowed);
  const lateByDisplay = lateBy > 0 ? Math.max(1, Math.ceil(lateBy)) : 0;
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
  const items = getLiveUnscheduledItems(result);
  if (!items.length) return "";
  const stats = computeWaveCandidateAssignedPendingStats(result.solution || [], result.scenario);
  const statsLine = stats.map((row) => `${row.waveId} c/a/p=${row.candidateCount}/${row.assignedCount}/${row.pendingCount}`).join(" | ");
  const panelUniqueCount = new Set(items.map((item) => normalizeStoreKey(item.storeId))).size;
  const detailFilteredCount = computeStoreDetailUnscheduledCount(result);
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
      <p class="note">${lang() === "ja" ? `鐝惧湪鏈壊褰?搴楄垪 ${panelUniqueCount} 浠讹紙搴?娉㈡ ${items.length} 浠讹級銆傘伨銇氱┖杌娿倰鍎厛銇椼€? 搴?1 杌娿仹瑁滈厤銈掕│銇椼伨銇欍€俙 : `褰撳墠鏈皟搴﹂棬搴?${panelUniqueCount} 瀹讹紙搴?娉㈡ ${items.length} 鏉★級銆傜郴缁熶細浼樺厛绌洪棽杞﹁締锛屽厛鎸変竴搴椾竴杞﹀皾璇曡ˉ璋冦€俙}</p>
      ${result.adjustMessage ? `<p class="note">${result.adjustMessage}</p>` : ""}
      <div class="unscheduled-list">
        ${items.map((item) => {
          const timing = getEarliestDirectArrivalInfo(result, item);
          const timingText = timing
            ? (lang() === "ja"
              ? `鏈€鏃╃洿琛屽埌鐫€ ${formatTime(timing.earliestArrival)} / 鏈熸湜 ${formatTime(timing.desiredArrivalMin)} / 鏈€绲傝ū瀹?${formatTime(timing.latestAllowedArrivalMin)}锛?${Number(timing.allowedLateMinutes || 0)}锛? 瑕佹眰銈堛倞 ${Number(timing.lateByDisplay || 0)} 鍒嗛亝銈宍
              : `鏈€鏃╃洿杈惧彲鍒?${formatTime(timing.earliestArrival)} / 鏈熸湜 ${formatTime(timing.desiredArrivalMin)} / 鏈€鏅氬厑璁?${formatTime(timing.latestAllowedArrivalMin)}锛?${Number(timing.allowedLateMinutes || 0)}锛? 姣旇姹傛櫄 ${Number(timing.lateByDisplay || 0)} 鍒嗛挓`)
            : "";
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
  const mount = document.getElementById("storeReschedulePanel");
  if (!mount) return;
  const activeKey = state.activeResultKey || state.lastResults[0]?.key;
  const activeResult = state.lastResults.find((item) => item.key === activeKey) || state.lastResults[0];
  if (activeResult) applyFinalRuleToResult(activeResult, activeResult.scenario);
  mount.innerHTML = activeResult ? renderUnscheduledPanel(activeResult) : "";
}

function loadSavedPlans() { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); } catch { return []; } }
function loadRunArchives() { try { return JSON.parse(localStorage.getItem(RUN_ARCHIVE_KEY) || "[]"); } catch { return []; } }
function archiveStrategyLabel(key) {
  return ({
    quick: "蹇€熷垵鎺?,
    deep: "缁х画浼樺寲",
    relay: "鎺ュ姏姹傝В",
    compare: "澶氭柟妗堝姣?,
    global: "鍏ㄥ眬鎼滅储",
    manual: "鎵嬪伐妯″紡",
  })[key] || key;
}
function archiveGoalLabel(key) {
  return ({
    balanced: "缁煎悎骞宠　",
    ontime: "鏈€鍑嗘椂",
    vehicles: "灏戠敤杞?,
    distance: "鐪侀噷绋?,
    load: "閲嶈杞?,
  })[key] || key;
}
function snapshotResultForArchive(result) {
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
function autoArchiveCurrentRun() {
  const snapshot = snapshotRunArchive();
  if (!snapshot) return;
  void saveRunArchiveToBackend(snapshot);
  const archives = loadRunArchives().filter((item) => item.id !== snapshot.id);
  archives.unshift(snapshot);
  let limit = Math.min(18, archives.length);
  while (limit >= 1) {
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
function renderSavedPlans() {
  const mount = document.getElementById("savedPlans");
  if (!mount) return;
  if (!state.ui.archiveDateFilter) state.ui.archiveDateFilter = todayDateKey();
  const dateFilter = state.ui.archiveDateFilter;
  const pageSize = 6;
  const requestedPage = Math.max(1, state.ui.archivePage || 1);
  const shouldRefreshRemote = !archiveBackendCache.loading
    && (archiveBackendCache.date !== dateFilter || archiveBackendCache.page !== requestedPage);
  if (shouldRefreshRemote) {
    void refreshArchiveBackendCache(dateFilter, requestedPage, pageSize);
  }
  const localArchives = loadRunArchives().filter((item) => extractDateKey(item.createdAt) === dateFilter);
  const useRemote = archiveBackendCache.date === dateFilter
    && archiveBackendCache.page === requestedPage
    && Array.isArray(archiveBackendCache.items);
  const archives = useRemote ? archiveBackendCache.items : localArchives;
  const totalPages = useRemote
    ? Math.max(1, Number(archiveBackendCache.totalPages || 1))
    : Math.max(1, Math.ceil(archives.length / pageSize));
  state.ui.archivePage = Math.min(Math.max(1, state.ui.archivePage || 1), totalPages);
  const start = (state.ui.archivePage - 1) * pageSize;
  const pageItems = useRemote ? archives : archives.slice(start, start + pageSize);
  const saved = loadSavedPlans().filter((item) => extractDateKey(item.createdAt) === dateFilter);
  mount.innerHTML = `
    <section class="archive-shell">
      <div class="archive-toolbar">
        <div>
          <p class="archive-eyebrow">Solve Archive</p>
          <h3 class="archive-title">姹傝В妗ｆ</h3>
        </div>
        <div class="archive-pager-wrap">
          <label class="archive-date-filter">
            <span>${lang() === "ja" ? "鏃ヤ粯" : "鏃ユ湡"}</span>
            <input id="archiveDateFilterInput" type="date" value="${escapeHtml(dateFilter)}">
          </label>
          <div class="archive-pager">
          <button class="secondary archive-page-btn" data-archive-page="${Math.max(1, state.ui.archivePage - 1)}" ${state.ui.archivePage <= 1 ? "disabled" : ""}>涓婁竴椤?/button>
          <span class="archive-page-indicator">绗?${state.ui.archivePage} / ${totalPages} 椤?/span>
          <button class="secondary archive-page-btn" data-archive-page="${Math.min(totalPages, state.ui.archivePage + 1)}" ${state.ui.archivePage >= totalPages ? "disabled" : ""}>涓嬩竴椤?/button>
          </div>
        </div>
      </div>
      ${archives.length ? `<div class="table-wrap">
        <table class="archive-table">
          <thead>
            <tr>
              <th>杞</th>
              <th>${L("savedAt")}</th>
              <th>姹傝В鏂瑰紡</th>
              <th>鏂规鐩爣</th>
              <th>鏈€浣冲垎</th>
              <th>鏈€浣虫柟妗?/th>
              <th>绠楁硶閾?/th>
              <th>缁撴灉鏁?/th>
              <th>鎿嶄綔</th>
            </tr>
          </thead>
          <tbody>
            ${pageItems.map((item, index) => {
              const results = Array.isArray(item?.results) ? item.results : [];
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
                    <button class="secondary preview-archive-btn" data-archive-id="${item.id}">鏌ョ湅</button>
                    <button class="primary adopt-archive-btn" data-archive-id="${item.id}">閲囩敤</button>
                  </div>
                </td>
              </tr>`;
            }).join("")}
          </tbody>
        </table>
      </div>` : `<div class="note">${lang() === "ja" ? "銇撱伄鏃ャ伀瑭插綋銇欍倠姹傝В妗ｆ銇伨銇犮亗銈娿伨銇涖倱銆? : "杩欎竴澶╄繕娌℃湁姹傝В妗ｆ銆?}</div>`}
      ${saved.length ? `
        <div class="archive-legacy">
          <p class="archive-legacy-title">鎵嬪伐蹇収</p>
          <div class="saved-list">${saved.map((item) => `<article class="adjust-item"><strong>${item.algorithms}</strong><p>${L("savedAt")}锛?{item.createdAt}</p><p>${L("wavesLabel")}锛?{item.waves}</p></article>`).join("")}</div>
        </div>` : (!archives.length ? `<div class="note">${lang() === "ja" ? "銇撱伄鏃ャ伀瑭插綋銇欍倠鎵嬪嫊蹇収銈傘亗銈娿伨銇涖倱銆? : "杩欎竴澶╀篃娌℃湁鎵嬪伐蹇収銆?}</div>` : "")}
    </section>
  `;
}
async function restoreArchivedRun(archiveId, mode = "preview") {
  let archive = await getRunArchiveFromBackend(archiveId);
  if (!archive) {
    archive = loadRunArchives().find((item) => item.id === archiveId);
  }
  if (!archive) return;
  state.stores = JSON.parse(JSON.stringify(archive.stores || []));
  state.vehicles = JSON.parse(JSON.stringify(archive.vehicles || []));
  state.waves = JSON.parse(JSON.stringify(archive.waves || []));
  state.settings = { ...state.settings, ...(archive.settings || {}) };
  const baseScenario = await buildScenario();
  const selectedScenario = applySolveWaveSelectionToScenario(baseScenario);
  if (selectedScenario.error || !selectedScenario.scenario) {
    box.textContent = selectedScenario.error || (lang() === "ja" ? "娉㈡绛涢€夊け璐ャ€? : "娉㈡绛涢€夊け璐ャ€?);
    return;
  }
  const scenario = selectedScenario.scenario;
  state.lastResults = (archive.results || []).map((result) => ({
    ...result,
    scenario,
    storeAssignmentMap: buildStoreAssignmentMapFromSolution(result.solution || []),
  }));
  state.activeResultKey = archive.activeResultKey || state.lastResults[0]?.key || "";
  state.ui.archiveCurrentId = archive.id;
  const box = document.getElementById("validationBox");
  if (box) {
    box.textContent = mode === "adopt"
      ? `宸查噰鐢?${archive.createdAt} 杩欒疆姹傝В妗ｆ銆備笂鏂硅皟搴︾粨鏋滃尯宸茬粡鍒囧埌杩欎竴杞紝鍙互缁х画姹囨姤銆佸鍑猴紝鎴栧湪姝ゅ熀纭€涓婄户缁紭鍖栥€俙
      : `姝ｅ湪鏌ョ湅 ${archive.createdAt} 杩欒疆姹傝В妗ｆ銆備笂鏂硅皟搴︾粨鏋滃尯宸茬粡鍒囧埌杩欎竴杞紱濡傛灉鑰佹澘璁ゅ彲锛屽啀鐐光€滈噰鐢ㄢ€濆嵆鍙€俙;
  }
  renderAll();
}
function saveCurrentPlan() { if (!state.lastResults.length) return; const saved = loadSavedPlans(); saved.unshift({ id: `plan-${Date.now()}`, createdAt: new Date().toLocaleString("zh-CN"), algorithms: state.lastResults.map((x) => x.label).join(" vs "), waves: state.settings.ignoreWaves ? "蹇界暐娉㈡锛堝叏澶╃粺涓€璋冨害锛? : state.waves.map((w) => `${w.waveId}(${w.start}-${w.end})`).join("锛?) }); localStorage.setItem(STORAGE_KEY, JSON.stringify(saved.slice(0, 12))); renderSavedPlans(); }

function todayDateKey() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function extractDateKey(value) {
  const text = String(value || "");
  const match = text.match(/(\d{4})[\/\-骞碷(\d{1,2})[\/\-鏈圿(\d{1,2})/);
  if (!match) return "";
  return `${match[1]}-${String(match[2]).padStart(2, "0")}-${String(match[3]).padStart(2, "0")}`;
}

function getWaveSolverAlgorithmOptions() {
  return ["vrptw", "hybrid", "ga", "tabu", "lns", "savings", "sa", "aco", "pso", "vehicle"];
}

function getConfiguredWaveOptions() {
  if (state.settings.ignoreWaves) {
    return [{
      value: "ALL",
      label: lang() === "ja" ? "ALL 銉?鍏ㄥ簵鑸楃当鍚? : "ALL 路 鍏ㄩ儴闂ㄥ簵缁熶竴璋冨害",
    }];
  }
  const options = (state.waves || []).map((wave) => ({
    value: wave.waveId,
    label: `${wave.waveId} 路 ${wave.start}-${wave.end}`,
  }));
  const stores = enrichStores(state.stores || []);
  if (stores.length) {
    const dist = buildDistanceTable(stores);
    const singleWaveIds = getSingleWaveStoreIds(stores, dist, Number(state.settings.singleWaveDistanceKm || 70));
    if (singleWaveIds.length) {
      options.push({
        value: "鍗曟尝娆?,
        label: lang() === "ja"
          ? `鍗樼嫭娉㈡ 路 ${state.settings.singleWaveStart || "19:10"}-${state.settings.singleWaveEnd || "05:30"}`
          : `鍗曟尝娆?路 ${state.settings.singleWaveStart || "19:10"}-${state.settings.singleWaveEnd || "05:30"}`,
      });
    }
  }
  return options;
}

function ensureWaveSolverPanel() {
  const panel = document.getElementById("waveSolverPanel");
  if (panel) panel.remove();
  return null;
}

function normalizeSolveWaveSelection(selectedIds = [], options = getConfiguredWaveOptions()) {
  const optionIds = new Set((options || []).map((item) => String(item.value || "").trim()).filter(Boolean).filter((id) => id !== "ALL"));
  const raw = Array.isArray(selectedIds) ? selectedIds.map((id) => String(id || "").trim()).filter(Boolean) : [];
  if (!raw.length || raw.includes("ALL")) return ["ALL"];
  const filtered = Array.from(new Set(raw.filter((id) => optionIds.has(id))));
  return filtered.length ? filtered : ["ALL"];
}

function renderSolveWaveSelectionOptions() {
  const allInput = document.getElementById("solveWaveAllInput");
  const optionsBox = document.getElementById("solveWaveOptions");
  if (!allInput || !optionsBox) return;
  const options = getConfiguredWaveOptions().filter((item) => String(item.value || "").trim() !== "ALL");
  state.ui.solveWaveSelectedIds = normalizeSolveWaveSelection(state.ui.solveWaveSelectedIds, options);
  const isAll = state.ui.solveWaveSelectedIds.includes("ALL");
  allInput.checked = isAll;
  if (!options.length) {
    optionsBox.innerHTML = `<span class="muted">${lang() === "ja" ? "鍙€夋尝娆°仾銇? : "鏆傛棤鍙€夋尝娆?}</span>`;
    return;
  }
  const selectedSet = new Set(state.ui.solveWaveSelectedIds);
  optionsBox.innerHTML = options.map((item) => {
    const id = String(item.value || "").trim();
    const checked = !isAll && selectedSet.has(id);
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

function applySolveWaveSelectionToScenario(scenario) {
  if (!scenario || !Array.isArray(scenario.waves)) return { scenario, error: "娉㈡鏁版嵁涓嶅彲鐢? };
  if (state.settings.ignoreWaves) return { scenario };
  const selected = normalizeSolveWaveSelection(state.ui.solveWaveSelectedIds, getConfiguredWaveOptions());
  if (selected.includes("ALL")) return { scenario };
  const selectedSet = new Set(selected.map((id) => String(id || "").trim()).filter(Boolean));
  const filteredWaves = (scenario.waves || []).filter((wave) => selectedSet.has(String(wave?.waveId || "").trim()));
  if (!filteredWaves.length) {
    return { scenario: null, error: lang() === "ja" ? "灏戙仾銇忋仺銈?銇ゃ伄娉㈡銈掗伕鎶炪仐銇︺亸銇犮仌銇勩€? : "璇疯嚦灏戦€夋嫨1涓尝娆℃眰瑙ｃ€? };
  }
  return { scenario: { ...scenario, waves: filteredWaves } };
}

function focusSolvedOutput() {
  const target = document.getElementById("resultPanels") || document.getElementById("analyticsPanel") || document.getElementById("summaryCards");
  target?.scrollIntoView?.({ behavior: "smooth", block: "start" });
}

function buildSingleWaveScenario(baseScenario, waveId) {
  const wave = (baseScenario?.waves || []).find((item) => item.waveId === waveId);
  if (!wave) return null;
  return {
    ...baseScenario,
    waves: [clone(wave)],
  };
}

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
  return map[key] || null;
}

function getWaveOptimizer(key) {
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

async function runSingleWaveInitialSolve() {
  syncSettingsFromUI();
  const box = document.getElementById("validationBox");
  const error = validateInput();
  if (error) {
    if (box) box.textContent = error;
    return;
  }
  const waveId = state.ui.waveSolverWaveId;
  const algoKey = state.ui.waveSolverAlgo;
  const runner = getAlgorithmRunner(algoKey);
  if (!runner) return;
  if (box) box.textContent = lang() === "ja" ? `娉㈡ ${waveId} 銈?${algoLabel(algoKey)} 銇у垵鏈熸眰瑙ｃ仐銇︺亜銇俱仚鈥 : `姝ｅ湪鐢?${algoLabel(algoKey)} 瀵?${waveId} 鍋氬垵姝ユ眰瑙ｂ€;
  const scenario = await buildScenario();
  const singleScenario = buildSingleWaveScenario(scenario, waveId);
  if (!singleScenario) {
    if (box) box.textContent = lang() === "ja" ? `娉㈡ ${waveId} 銇岃銇ゃ亱銈娿伨銇涖倱銆俙 : `娌℃湁鎵惧埌娉㈡ ${waveId}銆俙;
    return;
  }
  const result = await runner(singleScenario);
  result.adjustMessage = lang() === "ja" ? `宸插垏鍒?${waveId} 鐨勫崟鐙眰瑙ｈ鍥撅紝褰撳墠绠楁硶鏄?${algoLabel(algoKey)}銆俙 : `宸插垏鍒?${waveId} 鐨勫崟鐙眰瑙ｈ鍥撅紝褰撳墠绠楁硶鏄?${algoLabel(algoKey)}銆俙;
  state.lastResults = [result];
  state.activeResultKey = result.key;
  state.ui.generating = false;
  state.ui.progress = 100;
  state.ui.progressText = lang() === "ja" ? `${waveId} 銇垵鏈熸眰瑙ｃ亴瀹屼簡銇椼伨銇椼仧` : `${waveId} 鐨勫垵姝ユ眰瑙ｅ凡瀹屾垚`;
  if (box) box.textContent = lang() === "ja" ? `${waveId} 銇垵鏈熸眰瑙ｃ亴瀹屾垚銇椼伨銇椼仧銆俙 : `${waveId} 鐨勫垵姝ユ眰瑙ｅ凡瀹屾垚銆俙;
  renderAll();
  document.getElementById("resultIntro").textContent = lang() === "ja"
    ? `${waveId} 銇崢鐙尝娆°儞銉ャ兗銇с仚銆傜敇鐗瑰浘銉婚┚椹惰埍銉荤嚎璺槑缁嗐伅涓嬫柟銇当涓€琛ㄧず銇椼伨銇欍€俙
    : `褰撳墠鏄?${waveId} 鐨勫崟娉㈡瑙嗗浘锛岀敇鐗瑰浘銆侀┚椹惰埍鍜岀嚎璺槑缁嗛兘鍦ㄤ笅鏂圭粺涓€灞曠ず銆俙;
  focusSolvedOutput();
}

async function runSingleWaveResolve() {
  const box = document.getElementById("validationBox");
  const result = state.lastResults.find((item) => item.key === state.activeResultKey) || state.lastResults[0] || null;
  if (!result) {
    if (box) box.textContent = lang() === "ja" ? "鍏堛伀閫氬父銇皟搴︾粨鏋溿倰鐢熸垚銇椼仸銇忋仩銇曘亜銆? : "璇峰厛鐢熸垚涓€鐗堣皟搴︾粨鏋溿€?;
    return;
  }
  const waveId = state.ui.waveSolverWaveId;
  const algoKey = state.ui.waveSolverAlgo;
  const target = getWaveAndPlans(result, waveId);
  if (!target) {
    if (box) box.textContent = lang() === "ja" ? `褰撳墠缁撴灉銇尝娆?${waveId} 銇屻亗銈娿伨銇涖倱銆俙 : `褰撳墠缁撴灉閲屾病鏈夋尝娆?${waveId}銆俙;
    return;
  }
  if (box) box.textContent = lang() === "ja" ? `姝ｅ湪鐢?${algoLabel(algoKey)} 瀵?${waveId} 鍋氬啀姹傝В鈥 : `姝ｅ湪鐢?${algoLabel(algoKey)} 瀵?${waveId} 鍋氬啀姹傝В鈥;
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
    if (box) box.textContent = lang() === "ja" ? `${waveId} 銇啀姹傝В銇け鏁椼仐銇俱仐銇熴€俙 : `${waveId} 鐨勫啀姹傝В澶辫触浜嗐€俙;
    return;
  }
  result.solution[target.waveIndex] = nextWavePlans;
  result.adjustMessage = lang() === "ja" ? `${waveId} 銈?${algoLabel(algoKey)} 銇у啀姹傝В銇椼伨銇椼仧銆俙 : `${waveId} 宸叉寜 ${algoLabel(algoKey)} 瀹屾垚鍐嶆眰瑙ｃ€俙;
  syncResultAfterLocalAdjustment(result);
  state.ui.generating = false;
  state.ui.progress = 100;
  state.ui.progressText = lang() === "ja" ? `${waveId} 銇啀姹傝В銇屽畬浜嗐仐銇俱仐銇焋 : `${waveId} 鐨勫啀姹傝В宸插畬鎴恅;
  if (box) box.textContent = result.adjustMessage;
  renderAll();
  document.getElementById("resultIntro").textContent = lang() === "ja"
    ? `${waveId} 銇尝娆°仩銇戙倰鍐嶆眰瑙ｃ仐銇熴儞銉ャ兗銇с仚銆備笅鏂广伄鐢樼壒鍥俱兓缁撴灉鍗°兓绾胯矾鏄庣粏銈掑悎銈忋仜銇︾⒑瑾嶃仹銇嶃伨銇欍€俙
    : `褰撳墠鏄彧閽堝 ${waveId} 鍋氬啀姹傝В鍚庣殑瑙嗗浘锛屼笅鏂逛細缁熶竴灞曠ず鐢樼壒鍥俱€佺粨鏋滃崱鍜岀嚎璺槑缁嗐€俙;
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
    : `<tr><td colspan="${Math.max(1, headers.length)}">鏃犳暟鎹?/td></tr>`;
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
  const headers = ["缂栧彿", "鍚嶇О", "鍖哄煙", "娆℃暟", "涓€娉㈡璐ч噺", "浜屾尝娆¤揣閲?, "涓€閰嶆椂闂?, "浜岄厤鏃堕棿", "鎵€灞炴尝娆?, "鍗歌揣鍒嗛挓", "闅惧害", "鍏佽鍋忓樊(鍒?", "鐘舵€?, "杞﹀彿"];
  const rows = buildStoreExportRows("multi");
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  downloadXlsHtml(`涓€鏃ュ閰嶅簵閾烘槑缁哶${timestamp}.xls`, "涓€鏃ュ閰嶅簵閾烘槑缁?, headers, rows);
}

function exportSingleDailyStores() {
  const headers = ["缂栧彿", "鍚嶇О", "鍖哄煙", "娆℃暟", "涓夋尝娆¤揣閲?, "鍥涙尝娆¤揣閲?, "W3鍒板簵鏃堕棿", "W4鍒板簵鏃堕棿", "鎵€灞炴尝娆?, "鍗歌揣鍒嗛挓", "闅惧害", "鍏佽鍋忓樊(鍒?", "鐘舵€?, "杞﹀彿"];
  const rows = buildStoreExportRows("single");
  const stamp = new Date();
  const timestamp = `${stamp.getFullYear()}${String(stamp.getMonth() + 1).padStart(2, "0")}${String(stamp.getDate()).padStart(2, "0")}_${String(stamp.getHours()).padStart(2, "0")}${String(stamp.getMinutes()).padStart(2, "0")}${String(stamp.getSeconds()).padStart(2, "0")}`;
  downloadXlsHtml(`涓€鏃ヤ竴閰嶅簵閾烘槑缁哶${timestamp}.xls`, "涓€鏃ヤ竴閰嶅簵閾烘槑缁?, headers, rows);
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
      <span class="store-wave-header-total">鍚堣 ${formatLoadConvertValue(total)}</span>
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
          <span class="store-name-id" title="${escapeHtml(String(s.id || ""))}">搴楅摵缂栧彿锛?{escapeHtml(String(s.id || ""))}</span>
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
      columns: buildColumns("涓€娉㈡璐ч噺", multiWave1Total, "浜屾尝娆¤揣閲?, multiWave2Total, {
        includeFirstTime: true,
        firstWaveSortField: "wave1TotalLoad",
        secondWaveSortField: "wave2TotalLoad",
        secondTimeSortField: "waveW2",
      }),
      rows: buildStoreRowHtml(
        multiDailyRows,
        "resolvedWave1Load",
        "resolvedWave2Load",
        "110锛歳pcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380",
        "210锛歛pcs/350+apaper/380",
        { includeFirstTime: true, arrivalField: "waveW2" }
      ),
      tableClass: "store-data-table",
    });
  }
  const storeTableSingleDaily = document.getElementById("storeTableSingleDaily");
  if (storeTableSingleDaily) {
    storeTableSingleDaily.innerHTML = buildDataTableHtml({
      tableKind: "store",
      columns: buildColumns("涓夋尝娆¤揣閲?, singleWave3Total, "鍥涙尝娆¤揣閲?, singleWave4Total, {
        includeFirstTime: false,
        secondTimeLabel: "鍒板簵鏃堕棿",
        firstWaveSortField: "wave3TotalLoad",
        secondWaveSortField: "wave4TotalLoad",
        secondTimeSortField: "waveW4",
      }),
      rows: buildStoreRowHtml(
        singleDailyRows,
        "resolvedWave3Load",
        "resolvedWave4Load",
        "W3锛歳pcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380+apcs/350+apaper/380",
        "W4锛歳pcs/207+rcase/380+bpcs/120+bpaper/380+rpaper/380+apcs/350+apaper/380",
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
    left.innerHTML = `<div class="muted">鏃犲尮閰嶆尝娆?/div>`;
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
    right.innerHTML = `<div class="muted">鏆傛棤娉㈡</div>`;
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
      <input id="waveStoreSearchInput" type="text" placeholder="鎼滅储闂ㄥ簵鍚嶇О" value="${escapeHtml(state.ui.waveStoreSearchQuery || "")}">
      <button class="secondary" data-wave-select-all="${selectedIndex}">鍏ㄩ€夊綋鍓嶆绱?/button>
      <button class="secondary" data-wave-clear-all="${selectedIndex}">娓呯┖褰撳墠娉㈡</button>
      <button class="mini" data-remove="wave" data-index="${selectedIndex}">${L("del")}</button>
    </div>
    <div>${renderWaveStoreNameTags(wave.storeIds, 6)}</div>
    <div class="wave-store-list">
      ${filteredStores.map((store) => `
        <label class="wave-store-option">
          <input type="checkbox" data-wave-store-index="${selectedIndex}" data-store-id="${escapeHtml(store.id)}" ${selectedIds.has(store.id) ? "checked" : ""}>
          <span class="store-name">${escapeHtml(store.name)}</span>
        </label>
      `).join("") || `<div class="muted">鏃犲尮閰嶉棬搴?/div>`}
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
      ? (lang() === "ja" ? `鑷敱姹傝В妯″紡锛氭渶澶氶€夋嫨 ${MAX_FREE_SOLVE_ALGOS} 涓畻娉曘€俙 : `鑷敱姹傝В妯″紡锛氭渶澶氶€夋嫨 ${MAX_FREE_SOLVE_ALGOS} 绉嶇畻娉曘€俙)
      : (lang() === "ja" ? "銈儷銈淬儶銈恒儬銇祫銇垮悎銈忋仜銇€佺従鍦ㄣ伄鏂归嚌銇繙銇樸仸銈枫偣銉嗐儬銇岃嚜鍕曘仹閬搞伋銇俱仚銆? : "绠楁硶缁勫悎浼氱敱绯荤粺鎸夊綋鍓嶇瓥鐣ヨ嚜鍔ㄩ€夋嫨銆?);
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
        <h3>${lang() === "ja" ? "銈儷銈淬儶銈恒儬涓灑" : "绠楁硶涓灑"}</h3>
      </div>
      <p class="algorithm-pool-summary">${currentStrategy === "free"
        ? (lang() === "ja"
          ? `鑷敱姹傝В銇с仚銆備笅銇偒銉笺儔銈掔洿鎺ャ偗銉儍銈仐銇︺偄銉偞銉偤銉犮倰閬告姙銇с亶銇俱仚锛堟渶澶?${MAX_FREE_SOLVE_ALGOS} 鏈級銆傜従鍦?${active.size} 鏈€俙
          : `褰撳墠鏄嚜鐢辨眰瑙ｏ紝鍙洿鎺ョ偣鍑讳笅鏂瑰崱鐗囬€夋嫨绠楁硶锛堟渶澶?${MAX_FREE_SOLVE_ALGOS} 绉嶏級銆傚綋鍓嶅凡閫?${active.size} 绉嶃€俙)
        : (lang() === "ja"
          ? `浠婂洖銇▓绠椼仹銇?${active.size} 鏈伄銈儷銈淬儶銈恒儬銇屽嫊浣滀腑銇с仚銆傚疅闅涖伀浣裤倧銈屻仸銇勩倠涓牳銇犮亼銈掕嚜鍕曘仹鐐圭伅琛ㄧず銇椼伨銇欍€俙
          : `鏈疆宸茶皟鐢?${active.size} 濂楃畻娉曪紝绯荤粺浼氳嚜鍔ㄧ偣浜綋鍓嶆鍦ㄥ弬涓庢眰瑙ｇ殑鏍稿績銆俙)}</p>
    </div>
    <div class="algorithm-pool"></div>
  `;
  pool = shell.querySelector(".algorithm-pool");
  pool.innerHTML = order.map((key) => `
    <div class="algorithm-card ${active.has(key) ? "is-active" : ""} ${currentStrategy === "free" ? "is-pickable" : ""}" data-algo-card="${key}" title="${currentStrategy === "free" ? (lang() === "ja" ? "銈儶銉冦偗銇椼仸閬告姙/瑙ｉ櫎锛堟渶澶?鏈級" : "鐐瑰嚮閫夋嫨/鍙栨秷锛堟渶澶?绉嶏級") : ""}">
      <div class="algorithm-card-head">
        <span class="algorithm-card-name">${algoLabel(key)}</span>
        <span class="algorithm-card-state">${active.has(key) ? (lang() === "ja" ? "绋煎儘涓? : "宸插惎鐢?) : (lang() === "ja" ? "寰呮涓? : "寰呭懡涓?)}</span>
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
      <div class="strategy-preview-label" id="solveModePreviewLabel">${lang() === "ja" ? "瑙ｆ硶銉兗銉? : "姹傝В鏂瑰紡"}</div>
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
      <div class="strategy-preview-label" id="goalModePreviewLabel">${lang() === "ja" ? "鐩鏂归嚌" : "鏂规鐩爣"}</div>
      <div class="strategy-preview-row">
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "ontime" ? "is-active" : ""}" data-goal-preview="ontime" id="goalPreviewOnTime">${lang() === "ja" ? "瀹氭檪閲嶈" : "鏈€鍑嗘椂"}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "vehicles" ? "is-active" : ""}" data-goal-preview="vehicles" id="goalPreviewVehicles">${lang() === "ja" ? "灏戣粖涓? : "灏戠敤杞?}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "distance" ? "is-active" : ""}" data-goal-preview="distance" id="goalPreviewDistance">${lang() === "ja" ? "鐭窛闆? : "鐪侀噷绋?}</button>
        <span class="strategy-divider">/</span>
        <button type="button" class="strategy-pill goal-preview-btn ${(state.settings.optimizeGoal || "balanced") === "balanced" ? "is-active" : ""}" data-goal-preview="balanced" id="goalPreviewBalanced">${L("goalBalanced")}</button>
      </div>`;
  }
}
function buildStrategyHint() {
  const strategy = state.settings.solveStrategy || "manual";
  const goal = state.settings.optimizeGoal || "balanced";
  const labels = {
    quick: lang() === "ja" ? "銇俱仛瀹熺敤鐨勩仾鍒濇湡妗堛倰绱犳棭銇忎綔銈嬫鎴愩仹銇欍€? : "鍏堢敤蹇€熸瀯閫犵畻娉曠粰鍑哄彲琛屽垵绋裤€?,
    deep: lang() === "ja" ? "鐝惧湪銇銈掑紩銇嶇稒銇勩仹銆併仌銈夈伀瑭般倎銇︺亜銇忔敼鍠勬鎴愩仹銇欍€? : "涓绘墦鍦ㄧ幇鏈夋柟妗堜笂缁х画娣辨寲浼樺寲銆?,
    global: lang() === "ja" ? "澶с亶銇忓垾銇儷銉笺儓妲嬮€犮倰鎺仚銇熴倎銇偘銉兗銉愩儷鎺㈢储妲嬫垚銇с仚銆? : "涓绘墦鍏ㄥ眬鎼滅储锛屾帰绱㈠畬鍏ㄤ笉鍚岀殑鎺掔嚎缁撴瀯銆?,
    relay: lang() === "ja" ? "鍒濇湡妗堛亱銈夋敼鍠勩伨銇с倰娈甸殠鐨勩伀銇ゃ仾銇愩€併儶銉兗鍨嬨伄姹傝В妲嬫垚銇с仚銆? : "璁╁垵鎺掑拰浼樺寲绠楁硶鎺ュ姏姹傝В锛屾洿鍍忕湡姝ｇ殑骞冲彴娴佺▼銆?,
    free: lang() === "ja" ? `鑷敱姹傝В銇с仚銆傘偄銉偞銉偤銉犮伅鎵嬪嫊銇ч伕鎶炪仹銇嶃€佹渶澶?${MAX_FREE_SOLVE_ALGOS} 鏈伨銇с仹銇欍€俙 : `鑷敱姹傝В妯″紡锛屽彲鎵嬪姩閫夋嫨绠楁硶锛屾渶澶?${MAX_FREE_SOLVE_ALGOS} 绉嶃€俙,
    compare: lang() === "ja" ? "瑜囨暟銇В娉曘倰涓︺伖銇﹀樊銈掕姣斻伖銈嬫瘮杓冦儮銉笺儔銇с仚銆? : "骞舵帓璺戝绉嶆€濊矾锛屾柟渚垮仛绠楁硶瀵规瘮銆?,
    manual: lang() === "ja" ? "鐝惧湪銇墜鍕曢伕鎶炪儮銉笺儔銇с仚銆? : "褰撳墠鏄墜鍔ㄥ嬀閫夋ā寮忋€?,
  };
  const goalTexts = {
    balanced: lang() === "ja" ? "鐩銇窂鍚堛儛銉┿兂銈归噸瑕栥仹銇欍€? : "鐩爣鍋忓悜缁煎悎骞宠　銆?,
    ontime: lang() === "ja" ? "鐩銇畾鏅傛€ч噸瑕栥仹銇欍€? : "鐩爣鍋忓悜鍑嗙偣鐜囥€?,
    distance: lang() === "ja" ? "鐩銇窂璺濋洟銇湩绺仹銇欍€? : "鐩爣鍋忓悜鎬婚噷绋嬪帇缂┿€?,
    vehicles: lang() === "ja" ? "鐩銇娇鐢ㄨ粖涓℃暟銇姂鍒躲仹銇欍€? : "鐩爣鍋忓悜灏戠敤杞︺€?,
    load: lang() === "ja" ? "鐩銇杓夈伄闆嗙磩銇с仚銆? : "鐩爣鍋忓悜瑁呰浇闆嗕腑銆?,
  };
  const baseHint = `${labels[strategy] || labels.manual} ${goalTexts[goal] || goalTexts.balanced}`;
  if (strategy === "free") {
    const selected = getEffectiveFreeAlgorithms();
    return `${baseHint} ${lang() === "ja" ? `鐝惧湪 ${selected.length}/${MAX_FREE_SOLVE_ALGOS} 鏈倰閬告姙涓仹銇欍€俙 : `褰撳墠宸查€夋嫨 ${selected.length}/${MAX_FREE_SOLVE_ALGOS} 绉嶇畻娉曘€俙}`;
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
    ? `鐝惧湪銇€?{strategy}銆嶃儮銉笺儔銇ц▓绠椼仐銆佺洰妯欐柟閲濄伅銆?{goal}銆嶃€佸弬鍔犮偄銉偞銉偤銉犮伅 ${selectedCount} 鏈仹銇欍€俙
    : `褰撳墠鎸夆€?{strategy}鈥濇ā寮忔眰瑙ｏ紝浼樺寲鐩爣涓衡€?{goal}鈥濓紝鍙備笌绠楁硶 ${selectedCount} 绉嶃€俙;
}

function relayMetricSummary(metrics) {
  const onTime = Math.round((metrics.totalOnTime / Math.max(metrics.totalStops || 0, 1)) * 100);
  return `璇勫垎 ${metrics.score.toFixed(1)}锛屽凡璋冨害 ${metrics.scheduledCount || 0} 瀹讹紝鏈皟搴?${metrics.unscheduledCount || 0} 瀹讹紝鍑嗙偣 ${onTime}%锛屾€婚噷绋?${(metrics.totalDistance || 0).toFixed(1)} km锛岀敤杞?${metrics.usedVehicleCount || 0} 杈哷;
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
  if (Math.abs(scoreDelta) > 0.05) parts.push(`鎬诲垎 ${scoreDelta > 0 ? "鎻愰珮" : "涓嬮檷"} ${Math.abs(scoreDelta).toFixed(1)}`);
  if (scheduledDelta !== 0) parts.push(`宸茶皟搴﹂棬搴?${scheduledDelta > 0 ? `澧炲姞 ${scheduledDelta}` : `鍑忓皯 ${Math.abs(scheduledDelta)}`}`);
  if (unscheduledDelta !== 0) parts.push(`鏈皟搴﹂棬搴?${unscheduledDelta < 0 ? `鍑忓皯 ${Math.abs(unscheduledDelta)}` : `澧炲姞 ${unscheduledDelta}`}`);
  if (Math.abs(onTimeDelta) >= 0.5) parts.push(`鍑嗙偣鐜?${onTimeDelta > 0 ? "涓婂崌" : "涓嬮檷"} ${Math.abs(onTimeDelta).toFixed(1)}%`);
  if (Math.abs(distanceDelta) >= 0.1) parts.push(`鎬婚噷绋?${distanceDelta < 0 ? `鍑忓皯 ${Math.abs(distanceDelta).toFixed(1)} km` : `澧炲姞 ${distanceDelta.toFixed(1)} km`}`);
  if (vehicleDelta !== 0) parts.push(`鐢ㄨ溅 ${vehicleDelta < 0 ? `鍑忓皯 ${Math.abs(vehicleDelta)} 杈哷 : `澧炲姞 ${vehicleDelta} 杈哷}`);
  return parts.length ? parts.join("锛?) : "涓昏鎸囨爣鍑犱箮娌℃湁鍙樺寲";
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
      ? "鍏堟墦寮€姹傝В鍓嶈瘖鏂獥鍙ｏ紝纭瀛楁鏄惁榻愬叏鍚庡啀缁х画銆?
      : "鍏堝脊鍑烘眰瑙ｅ墠璇婃柇绐楀彛锛岀‘璁ゅ瓧娈垫槸鍚﹂綈鍏ㄥ悗鍐嶇户缁€?}`
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
  if (box) box.textContent = `${buildSolveModeSummary()} ${lang() === "ja" ? "鍏堛伀瑷烘柇銈掔⒑瑾嶃仐銇︺亸銇犮仌銇勩€? : "鍏堢湅璇婃柇鎶ュ憡锛屽啀鍐冲畾鏄惁缁х画姹傝В銆?}`;
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
      const parts = line.split(/[\t,锛?锛沑s]+/).map((x) => x.trim()).filter(Boolean);
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
  return String(line || "").split(/[\t,锛宂/).map((x) => x.trim());
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
    id: indexOf("缂栧彿", "搴楅摵浠ｇ爜", "id", "store_id"),
    name: indexOf("鍚嶇О", "搴楅摵鍚嶇О", "name", "store_name"),
    district: indexOf("鍖哄煙", "district"),
    lng: indexOf("缁忓害", "lng", "longitude"),
    lat: indexOf("绾害", "lat", "latitude"),
    tripCount: indexOf("娆℃暟", "tripCount", "trip_count"),
    rpcs: indexOf("RPCS", "rpcs"),
    rcase: indexOf("RCASE", "rcase"),
    bpcs: indexOf("BPCS", "bpcs"),
    bpaper: indexOf("BPAPER", "bpaper"),
    apcs: indexOf("APCS", "apcs"),
    apaper: indexOf("APAPER", "apaper"),
    rpaper: indexOf("RPAPER", "rpaper"),
    coldRatio: indexOf("鍐疯棌姣斾緥", "coldRatio", "cold_ratio"),
    waveBelongs: indexOf("鎵€灞炴尝娆?, "waveBelongs", "wave_belongs"),
    wave1Time: indexOf("涓€閰嶆椂闂?, "wave1_time", "first_wave_time"),
    wave2Time: indexOf("浜岄厤鏃堕棿", "wave2_time", "second_wave_time"),
    wave3Time: indexOf("鍒板簵鏃堕棿", "wave3_time", "arrival_time_w3"),
    wave4Time: indexOf("鍒板簵鏃堕棿", "wave4_time", "arrival_time_w4", "arrival_time"),
    serviceMinutes: indexOf("鍗歌揣鍒嗛挓", "serviceMinutes", "service_minutes"),
    difficulty: indexOf("闅惧害", "difficulty"),
    parking: indexOf("鍏佽鍋忓樊(鍒?", "鍋滆溅", "parking"),
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
        box.textContent = lang() === "ja" ? "搴楄垪銉曘偂銈ゃ儷銇В鏋愩伀澶辨晽銇椼伨銇椼仧銆? : "闂ㄥ簵鏂囦欢瑙ｆ瀽澶辫触銆?;
        return;
      }
  const resolvedMap = await fetchStoreWaveResolvedLoadMap(parsed.map((item) => item?.id).filter(Boolean));
  state.stores = applyStoreWaveResolvedLoadsToStores(parsed, resolvedMap);
      state.waves = buildAutoWaves(state.stores);
      state.lastResults = [];
      state.activeResultKey = "";
      renderAll();
      box.textContent = lang() === "ja" ? `搴楄垪 ${state.stores.length} 浠躲倰灏庡叆銇椼€併儹銉笺偒銉姌绠楄波閲忋倰閬╃敤銇椼伨銇椼仧銆俙 : `宸插鍏ラ棬搴?${state.stores.length} 瀹讹紝骞跺凡搴旂敤鏈湴鎶樼畻璐ч噺銆俙;
    } catch (error) {
      box.textContent = `${lang() === "ja" ? "闂ㄥ簵灏庡叆澶辨晽" : "闂ㄥ簵瀵煎叆澶辫触"} ${error?.message || ""}`.trim();
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
          if (box) box.textContent = `鍙岃〃璐ч噺宸蹭繚瀛橈細${Number(result?.upserted || 0)} 鏉;
        } catch (error) {
          if (box) box.textContent = `淇濆瓨鍙岃〃璐ч噺澶辫触锛?{error?.message || ""}`.trim();
        }
      })();
      return;
    }
    const storeLocateBtn = event.target.closest("#storeLocateBtn");
    if (storeLocateBtn) {
      const query = String(document.getElementById("storeSearchInput")?.value || state.ui.storeSearchQuery || "").trim();
      const found = locateStoreRow(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `宸插畾浣嶉棬搴楋細${query}` : `鏈壘鍒伴棬搴楋細${query}`) : "璇疯緭鍏ラ棬搴楁绱㈠叧閿瓧銆?;
      return;
    }
    const vehicleLocateBtn = event.target.closest("#vehicleLocateBtn");
    if (vehicleLocateBtn) {
      const query = String(document.getElementById("vehicleSearchInput")?.value || state.ui.vehicleSearchQuery || "").trim();
      const found = locateVehicleRow(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `宸插畾浣嶈溅杈嗭細${query}` : `鏈壘鍒拌溅杈嗭細${query}`) : "璇疯緭鍏ヨ溅杈嗘绱㈠叧閿瓧銆?;
      return;
    }
    const waveLocateBtn = event.target.closest("#waveLocateBtn");
    if (waveLocateBtn) {
      const query = String(document.getElementById("waveSearchInput")?.value || state.ui.waveSearchQuery || "").trim();
      const found = locateWaveItem(query);
      const box = document.getElementById("validationBox");
      if (box) box.textContent = query ? (found ? `宸插畾浣嶆尝娆★細${query}` : `鏈壘鍒版尝娆★細${query}`) : "璇疯緭鍏ユ尝娆℃绱㈠叧閿瓧銆?;
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
          if (box) box.textContent = `闂ㄥ簵璧勬枡宸蹭繚瀛樺埌鍚庡彴妗ｆ锛?{archiveId}`;
        } catch (error) {
          if (box) box.textContent = `闂ㄥ簵璧勬枡淇濆瓨澶辫触锛?{error?.message || ""}`.trim();
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
          if (box) box.textContent = `杞﹁締璧勬枡宸蹭繚瀛樺埌鍚庡彴妗ｆ锛?{archiveId}`;
        } catch (error) {
          if (box) box.textContent = `杞﹁締璧勬枡淇濆瓨澶辫触锛?{error?.message || ""}`.trim();
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
          if (box) box.textContent = `娉㈡璧勬枡宸蹭繚瀛樺埌鍚庡彴妗ｆ锛?{archiveId}`;
        } catch (error) {
          if (box) box.textContent = `娉㈡璧勬枡淇濆瓨澶辫触锛?{error?.message || ""}`.trim();
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
              ? `鑷敱姹傝В銇с伅鏈€澶?${MAX_FREE_SOLVE_ALGOS} 鏈伨銇ч伕鎶炪仹銇嶃伨銇欍€俙
              : `鑷敱姹傝В鏈€澶氶€夋嫨 ${MAX_FREE_SOLVE_ALGOS} 绉嶇畻娉曘€俙;
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
          ? `鑷敱姹傝В銇с伅鏈€澶?${MAX_FREE_SOLVE_ALGOS} 鏈伨銇ч伕鎶炪仹銇嶃伨銇欍€俙
          : `鑷敱姹傝В鏈€澶氶€夋嫨 ${MAX_FREE_SOLVE_ALGOS} 绉嶇畻娉曘€俙;
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
        ? "鏃伀姹傝В涓仹銇欍€傘儶銉兗銈︺偅銉炽儔銈︺伄閫叉崡銈掔⒑瑾嶃仐銇︺亸銇犮仌銇勩€?
        : "褰撳墠宸插湪姹傝В涓紝璇锋煡鐪嬧€滄眰瑙ｈ繃绋嬪彲瑙嗗寲绐楀彛鈥濊繘搴︺€?;
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
        ? (lang() === "ja" ? "鑷敱姹傝В銇с伅灏戙仾銇忋仺銈?1 鏈伄銈儷銈淬儶銈恒儬銈掗伕鎶炪仐銇︺亸銇犮仌銇勩€? : "鑷敱姹傝В璇疯嚦灏戝嬀閫?1 绉嶇畻娉曘€?)
        : (lang() === "ja" ? "鐝惧湪銇柟閲濄伀瀵惧繙銇欍倠銈儷銈淬儶銈恒儬銉併偋銉笺兂銇岃ō瀹氥仌銈屻仸銇勩伨銇涖倱銆? : "璇峰厛涓哄綋鍓嶇瓥鐣ュ唴缃嚦灏?1 鏉＄畻娉曢摼銆?);
      state.ui.generating = false;
      renderGenerationProgress();
      return;
    }
  // 涓存椂鏀惧紑锛氬睆钄藉垎鍖烘柟妗堝彿蹇呴€夋牎楠?

  openRelayConsoleModal(lang() === "ja" ? "姹傝В杩囩▼鍙鍖栫獥鍙? : "姹傝В杩囩▼鍙鍖栫獥鍙?);
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  鏀跺埌姹傝В璇锋眰锛屽紑濮嬪悓姝ュ墠鍙拌缃笌鍩虹鏍￠獙銆俙);
  relayStageReporter = (text) => appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${text}`);
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  褰撳墠绛栫暐锛?{state.settings.solveStrategy || "manual"}锛岀洰鏍囷細${state.settings.optimizeGoal || "balanced"}锛岀畻娉曪細${selected.join("銆?) || "锛堢┖锛?}銆俙);
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  姝ｅ湪鏋勫缓鍦烘櫙锛堥棬搴?杞﹁締/娉㈡/璺綉锛夛紝杩欎竴姝ュ彲鑳借€楁椂杈冮暱銆俙);
  const scenario = await buildScenario();
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  鍦烘櫙鏋勫缓瀹屾垚锛氶棬搴?${scenario?.stores?.length || 0} 瀹讹紝杞﹁締 ${scenario?.vehicles?.length || 0} 鍙帮紝娉㈡ ${scenario?.waves?.length || 0} 涓€俙);
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
      box.textContent = `${lang() === "ja" ? "姹傝В鍓嶆鏌ユ湭閫氳繃锛? : "姹傝В鍓嶆鏌ユ湭閫氳繃锛?}${requiredTimingIssues.slice(0, 15).join("銆?)}${requiredTimingIssues.length > 15 ? "..." : ""}`;
    }
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  姹傝В鍓嶆鏌ユ湭閫氳繃锛?{requiredTimingIssues.slice(0, 30).join("銆?)}${requiredTimingIssues.length > 30 ? "..." : ""}`);
    state.ui.generating = false;
    renderGenerationProgress();
    return;
  }
  state.lastResults = [];
  state.activeResultKey = "";
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${lang() === "ja" ? "寮€濮嬫眰瑙ｏ紝姝ｅ湪鍑嗗鍦烘櫙涓庢牎楠?.." : "寮€濮嬫眰瑙ｏ紝姝ｅ湪鍑嗗鍦烘櫙涓庢牎楠?.."}`);
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
      appendRelayConsoleLine(lang() === "ja" ? "銉儸銉兼渶閬╁寲銇偊銈ｃ兂銉夈偊銈掗枊銇嶃伨銇椼仧銆傘亾銇撱伀鍚勬闅庛伄鍕曘亶銇岄爢娆¤〃绀恒仌銈屻伨銇欍€? : "鎺ュ姏姹傝В绐楀彛宸叉墦寮€锛屼笅闈細鎸佺画鎵撳嵃姣忎竴姝ュ姩浣溿€?);
      await updateGenerationProgress(24, lang() === "ja" ? "銉儸銉兼渶閬╁寲銈掕捣鍕曘仐銇︺亜銇俱仚..." : "姝ｅ湪鍚姩鎺ュ姏姹傝В...");
      try {
        built.push({ ...(await runWithTimeout("relay", () => solveByRelay(scenario, selected, carryResult))), scenario, adjustMessage: "" });
      } catch (error) {
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  鎺ュ姏姹傝В澶辫触鎴栬秴鏃讹細${error?.message || error}`);
        if (box) box.textContent = `鎺ュ姏姹傝В澶辫触鎴栬秴鏃讹細${error?.message || error}`;
      }
      await cooperativeYield();
      await updateGenerationProgress(75, lang() === "ja" ? "銉儸銉兼闅庛亴瀹屼簡銇椼€佺祼鏋溿倰銇俱仺銈併仸銇勩伨銇?.." : "鎺ュ姏闃舵宸插畬鎴愶紝姝ｅ湪姹囨€荤粨鏋?..");
    } else if (strategy === "compare") {
      appendRelayConsoleLine(lang() === "ja" ? "澶氱畻娉曞姣斻倰璧峰嫊銇椼伨銇椼仧銆傘亾銇撱伀鍚勭畻娉曘伄閫茶銇ㄥ墫鏋愩亴闋嗘琛ㄧず銇曘倢銇俱仚銆? : "澶氱畻娉曞姣斿凡鍚姩銆備笅闈細鎸佺画鎵撳嵃姣忓绠楁硶鐨勮繍琛岃繘搴︿笌鍓栨瀽銆?);
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  寮€濮嬭繍琛?${algoLabel(key)}銆俙);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let result = null;
        try {
          result = { ...(await runWithTimeout(key, () => runners[key](scenario))), scenario, adjustMessage: "" };
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 澶辫触鎴栬秴鏃讹細${error?.message || error}`);
          await cooperativeYield();
          await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
          continue;
        }
        built.push(result);
        state.lastResults = [...built].sort((a, b) => b.metrics.score - a.metrics.score);
        state.activeResultKey = state.lastResults[0]?.key || "";
        box.textContent = `${buildSolveModeSummary()} ${lang() === "ja" ? `鐝惧湪宸插畬鎴?${built.length}/${selected.length} 鏈畻娉曪紝鏈€鏂板畬鎴愮殑鏄?${algoLabel(key)}銆俙 : `褰撳墠宸插畬鎴?${built.length}/${selected.length} 濂楃畻娉曪紝鏈€鏂板畬鎴愮殑鏄?${algoLabel(key)}銆俙}`;
        renderVehicles();
        renderSummary();
        renderAnalytics();
        renderResults();
        renderStoresTable();
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 宸插畬鎴愶紝褰撳墠璇勫垎 ${result.metrics.score.toFixed(1)}锛屾€婚噷绋?${result.metrics.totalDistance.toFixed(1)} km锛岀敤杞?${result.metrics.usedVehicleCount || 0} 杈嗐€俙);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    } else if (strategy === "deep" && carryResult?.solution?.length) {
      appendRelayConsoleLine(lang() === "ja" ? "娣卞害鏈€閬╁寲銉兗銉夛細鏃㈠瓨瑙ｃ伄娉㈡鏀瑰杽銈掗枊濮嬨€? : "娣卞害浼樺寲妯″紡锛氬紑濮嬪湪宸叉湁瑙ｄ笂鍋氭尝娆′紭鍖栥€?);
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        const optimizer = ({ hybrid: optimizeWaveWithHybrid, tabu: optimizeWaveWithTabu, lns: optimizeWaveWithLns, sa: optimizeWaveWithSA, ga: optimizeWaveWithGA, aco: optimizeWaveWithACO, pso: optimizeWaveWithPSO })[key];
        if (!optimizer) continue;
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  寮€濮嬫墽琛屾繁搴︿紭鍖栫畻娉?${algoLabel(key)}銆俙);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let improved = null;
        try {
          improved = await runWithTimeout(`deep-${key}`, () => improveSolutionByWaveOptimizer(carryResult.solution, scenario, optimizer, 40 + i));
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 娣卞害浼樺寲澶辫触鎴栬秴鏃讹細${error?.message || error}`);
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
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 娣卞害浼樺寲瀹屾垚锛岃瘎鍒?${built[built.length - 1]?.metrics?.score?.toFixed?.(1) ?? "-"}`);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    } else {
      appendRelayConsoleLine(lang() === "ja" ? "鏍囧噯姹傝В妯″紡锛氶€愪釜绠楁硶鎵ц銆? : "鏍囧噯姹傝В妯″紡锛氶€愪釜绠楁硶鎵ц銆?);
      for (let i = 0; i < selected.length; i += 1) {
        const key = selected[i];
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  寮€濮嬭繍琛?${algoLabel(key)}銆俙);
        await updateGenerationProgress(20 + (i / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
        let result = null;
        try {
          result = { ...(await runWithTimeout(key, () => runners[key](scenario))), scenario, adjustMessage: "" };
        } catch (error) {
          appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 澶辫触鎴栬秴鏃讹細${error?.message || error}`);
          await cooperativeYield();
          await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
          continue;
        }
        built.push(result);
        appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  ${algoLabel(key)} 宸插畬鎴愶紝璇勫垎 ${result.metrics.score.toFixed(1)}锛屾€婚噷绋?${result.metrics.totalDistance.toFixed(1)} km銆俙);
        await cooperativeYield();
        await updateGenerationProgress(20 + ((i + 1) / Math.max(selected.length, 1)) * 55, LT("progressRunning", { algo: algoLabel(key) }));
      }
    }
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  姹傝В璁＄畻闃舵缁撴潫锛屽紑濮嬫眹鎬荤粨鏋溿€俙);
  } finally {
    relayStageReporter = null;
  }

  state.lastResults = built.sort((a, b) => b.metrics.score - a.metrics.score);
  state.activeResultKey = state.lastResults[0]?.key || "";
  if (!state.lastResults.length) {
    const issues = diagnoseScenarioFeasibility(scenario);
    box.textContent = issues.length
      ? `${buildSolveModeSummary()} ${state.settings.ignoreWaves ? (lang() === "ja" ? "鐝惧湪銇尝娆°倰鍒嗐亼銇氥伀瑷堢畻銇椼仸銇勩伨銇欍€? : "褰撳墠鎸夊拷鐣ユ尝娆℃ā寮忔眰瑙?) : (lang() === "ja" ? "鐝惧湪銇尝娆°仈銇ㄣ伀瑷堢畻銇椼仸銇勩伨銇欍€? : "褰撳墠鎸夋尝娆℃眰瑙?)}锛?{lang() === "ja" ? "鐝惧湪銇潯浠躲仹銇疅琛屽彲鑳姐仾妗堛亴瑕嬨仱銇嬨倞銇俱仜銈撱€傛兂瀹氥仌銈屻倠鍒剁磩琛濈獊锛? : "鏆傛湭鎵惧埌鍙鏂规銆傚彲鑳界殑纭害鏉熷啿绐侊細"}${issues.join("锛?)}`
      : `${buildSolveModeSummary()} ${lang() === "ja" ? "鐝惧湪銇埗绱勩仹銇疅琛屽彲鑳姐仾妗堛亴瑕嬨仱銇嬨倞銇俱仜銈撱€? : "褰撳墠绾︽潫涓嬫殏鏈壘鍒板彲琛屾柟妗堛€?}`;
    await updateGenerationProgress(100, L("progressDone"));
    renderVehicles();
    renderSummary();
    renderAnalytics();
    renderResults();
    renderStoresTable();
    appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  鏈壘鍒板彲琛岃В锛屽凡杈撳嚭绾︽潫鍐茬獊鎻愮ず銆俙);
    return;
  }

  autoArchiveCurrentRun();
  state.ui.archivePage = 1;
  await updateGenerationProgress(88, L("progressFinishing"));
  const bestResult = state.lastResults[0];
  const targetScore = Number(state.settings.targetScore || 0);
  const routeSourceMessage = scenario.distanceSource === "database-full"
    ? (lang() === "ja"
      ? ` 鐝惧湪銇暟鎹簱鍏ㄩ噺璺濈鐭╅樀銈掍娇鐢ㄤ腑銇с仚銆備富鏁版嵁婧?${String(scenario?.distDbStats?.dbDominantSource || "unknown")}銆佹瑺鎼?0銆俙
      : ` 褰撳墠浣跨敤鏁版嵁搴撳叏閲忚窛绂荤煩闃碉紝涓绘暟鎹簮=${String(scenario?.distDbStats?.dbDominantSource || "unknown")}锛岀己澶?0銆俙)
    : scenario.distanceSource === "amap"
    ? (lang() === "ja"
      ? ` 鐝惧湪銇珮寰枫伄閬撹矾璺濋洟銇ㄦ墍瑕佹檪闁撱倰浣跨敤涓仹銇欍€傘偔銉ｃ儍銈枫儱鍛戒腑 ${scenario.distanceCacheHitPairs || 0} 浠躲€佷粖鍥炪伄鏂拌鍙栧緱 ${scenario.distanceFetchedPairs || 0} 浠躲€俙
      : ` 褰撳墠浣跨敤楂樺痉璺綉璺濈/鏃堕暱锛岀紦瀛樺懡涓?${scenario.distanceCacheHitPairs || 0} 瀵癸紝鏈疆鏂版媺鍙?${scenario.distanceFetchedPairs || 0} 瀵广€俙)
    : (lang() === "ja"
      ? " 鐝惧湪銇珮寰枫伄閬撹矾鎯呭牨銇屼娇銇堛仾銇勩仧銈併€佺洿绶氳窛闆伄鎺ㄥ畾銇嚜鍕曘仹鍒囥倞鏇裤亪銇︺亜銇俱仚銆?
      : " 褰撳墠楂樺痉璺綉涓嶅彲鐢紝宸茶嚜鍔ㄥ洖閫€鍒扮洿绾胯窛绂讳及绠椼€?);
  const baseMessage = state.settings.ignoreWaves
    ? (lang() === "ja" ? "鍙兘銇瘎鍥层仹鍓层倞褰撱仸銇熺祼鏋溿倰鐢熸垚銇椼伨銇椼仧銆傜従鍦ㄣ伅娉㈡銈掑垎銇戙仛銇▓绠椼仐銇︺亜銇俱仚銆? : "宸茬敓鎴愬敖閲忚皟搴︾粨鏋溿€傚綋鍓嶆寜蹇界暐娉㈡妯″紡姹傝В銆?)
    : (lang() === "ja" ? `鍙兘銇瘎鍥层仹鍓层倞褰撱仸銇熺祼鏋溿倰鐢熸垚銇椼伨銇椼仧銆傜従鍦ㄣ伅 ${scenario.waves.length} 鍊嬨伄妤嫏娉㈡銇ц▓绠椼仐銇︺亰銈娿€佸埌鐫€瑕佷欢銈掑挤鍒剁磩銇ㄣ仐銇︽壉銇勩伨銇欍€俙 : `宸茬敓鎴愬敖閲忚皟搴︾粨鏋溿€傚綋鍓嶆寜 ${scenario.waves.length} 涓笟鍔℃尝娆℃眰瑙ｏ紝骞舵寜鍒板簵瑕佹眰鍋氬己绾︽潫銆俙);
  const unscheduledMessage = bestResult.metrics.unscheduledCount ? ` ${LT("unscheduledSummary", { count: bestResult.metrics.unscheduledCount, names: formatUnscheduledDetails(bestResult.metrics.unscheduledStores, 12) })}` : ` ${L("noUnscheduled")}`;
  const reasonMessage = bestResult.metrics.unscheduledCount ? ` ${lang() === "ja" ? `鏈壊褰撱伄涓诲洜锛?{summarizeUnscheduledReasons(bestResult.metrics.unscheduledStores)}` : `鏈皟搴︿富鍥狅細${summarizeUnscheduledReasons(bestResult.metrics.unscheduledStores)}`}` : "";
  const targetMessage = targetScore > 0 ? ` ${buildTargetScoreAdvice(bestResult)}` : "";
  box.textContent = `${buildSolveModeSummary()} ${baseMessage}${routeSourceMessage}${unscheduledMessage}${reasonMessage}${targetMessage}`;
  renderVehicles();
  renderSummary();
  renderAnalytics();
  renderResults();
  renderStoresTable();
  renderSavedPlans();
  appendRelayConsoleLine(`${new Date().toLocaleTimeString("zh-CN", { hour12: false })}  姹傝В瀹屾垚锛氭渶浣虫柟妗?${bestResult.label}锛岃瘎鍒?${bestResult.metrics.score.toFixed(1)}銆俙);
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
      ? `搴楄垪 ${state.stores.length} 浠躲兓杌婁浮 ${state.vehicles.length} 鍙般倰瑾伩杈笺伩銆佽波閲忋伅銉兗銈儷鎶樼畻鍊ゃ倰閬╃敤銇椼伨銇椼仧銆俙
      : `宸插姞杞介棬搴?${state.stores.length} 瀹躲€佽溅杈?${state.vehicles.length} 鍙帮紝骞跺凡灏嗚揣閲忕粺涓€搴旂敤涓烘湰鍦版姌绠楀€笺€俙;
  } catch (error) {
    const box = document.getElementById("validationBox");
    if (box) {
      box.textContent = `${lang() === "ja" ? "鐢婚潰鏂囪█銇洿鏂颁腑銇偍銉┿兗銇屽嚭銇俱仐銇熴亴銆佸熀鏈儑銉笺偪銇京鍏冩笀銇裤仹銇欍€? : "鐣岄潰鏂囨鍒锋柊鏃跺彂鐢熼敊璇紝浣嗗熀纭€鏁版嵁宸茬粡鎭㈠銆?} ${error?.message || ""}`.trim();
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
    if (box) box.textContent = `宸叉竻鐞嗚窛绂荤紦瀛?${targetKeys.length} 椤癸紝椤甸潰鍗冲皢鍒锋柊銆俙;
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
    box.textContent = `${lang() === "ja" ? "鍒濇湡鍖栦腑銇偍銉┿兗銇屽嚭銇俱仐銇熴€傘伨銇氫竴搴︺€屽浐瀹氬簵鑸椼倰鍐嶈杈笺€嶃倰鎶笺仐銇︺亸銇犮仌銇勩€? : "鍒濆鍖栨椂鍙戠敓閿欒锛岃鍏堢偣涓€娆♀€滈噸鏂板姞杞藉浐瀹氶棬搴椻€濄€?} ${error?.message || ""}`.trim();
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
// ========== 鏂板锛氱洿鎺ヨ皟鐢ㄨ溅杈嗛┍鍔ㄦ瀯閫犵畻娉曪紙鏂帮級 ==========
async function directVehicleSolve() {
  if (state.ui.generating) {
    const box = document.getElementById("validationBox");
    if (box) box.textContent = "姝ｅ湪姹傝В涓紝璇风◢鍚?;
    return;
  }
  const box = document.getElementById("validationBox");
  try {
    state.ui.generating = true;
    renderGenerationProgress();
    await updateGenerationProgress(5, "姝ｅ湪鏋勫缓鍦烘櫙...");
    
    const scenario = await buildScenario();
    const selected = applySolveWaveSelectionToScenario(scenario);
    if (selected.error || !selected.scenario) {
      box.textContent = selected.error || "鍦烘櫙鏋勫缓澶辫触";
      return;
    }
    const finalScenario = selected.scenario;
    const strategyConfig = buildBackendStrategyConfig(state.strategyConfig);
    
    const solutions = [];
    let totalUnscheduled = [];
    
    // 璁板綍宸蹭娇鐢ㄧ殑杞﹁締锛堣溅鐗屽彿闆嗗悎锛?    let usedVehicles = new Set();
    // 璁板綍 W1 姣忎釜杞﹁締鐨勯棬搴楀垪琛紙鎸夐『搴忥級
    let w1Assignments = {};   // { plateNo: [storeId, ...] }
    let w1RoutesByPlate = {}; // { plateNo: [[storeId, ...], ...] }
    let w1PriorStats = {};    // { plateNo: { finish_time, prior_round_distance, priorWaveCount } }
    
    for (let idx = 0; idx < finalScenario.waves.length; idx++) {
      const wave = finalScenario.waves[idx];
      const waveId = wave.waveId;
      box.textContent = `姝ｅ湪姹傝В ${waveId} (${idx+1}/${finalScenario.waves.length}) ...`;
      await updateGenerationProgress(20 + (idx / finalScenario.waves.length) * 70, `姹傝В ${waveId}`);
      
      // 鏋勫缓鍩虹 payload
      let payload = {
        algorithmKey: "vehicle",
        scenario: finalScenario,
        wave: wave,
        stores: scenario.stores,
        dist: finalScenario.dist,
        strategyConfig: strategyConfig,
      };
      
      // 鐗规畩澶勭悊涓嶅悓娉㈡
      if (waveId === "W1") {
        // W1: 浣跨敤鍏ㄩ儴杞﹁締锛屼笉鎺掗櫎浠讳綍杞︼紝涔熶笉浼犻€?assignments
        payload.vehicles = finalScenario.vehicles;
      } else if (waveId === "W2") {
        // W2: 鍙娇鐢?W1 涓敤杩囩殑杞﹁締锛屽苟浼犻€?W1 鐨?assignments
        const w1UsedVehicles = finalScenario.vehicles.filter(v => usedVehicles.has(v.plateNo));
        if (w1UsedVehicles.length === 0) {
          // 濡傛灉 W1 娌℃湁杞﹁締锛堜笉鍙兘锛夛紝鍥為€€鍏ㄩ儴杞﹁締
          payload.vehicles = finalScenario.vehicles;
        } else {
          payload.vehicles = w1UsedVehicles;
        }
        payload.w1_assignments = w1Assignments;   // 浼犻€?W1 闂ㄥ簵鏄犲皠
        payload.w1_routes_by_plate = w1RoutesByPlate; // 浼犻€?W1 瀹屾暣瓒熸缁撴瀯
        payload.w1_prior_stats = w1PriorStats; // 浼犻€?W1 鎺ュ姏 prior stats
      } else if (waveId === "W3") {
        // W3: 鎺掗櫎 W1 鍜?W2 鐢ㄨ繃鐨勮溅杈?        if (strategyConfig.w3ExcludePriorVehicles && usedVehicles.size > 0) {
          payload.excluded_vehicles = Array.from(usedVehicles);
          console.log(`[directVehicleSolve] 鎺掗櫎杞﹁締: ${payload.excluded_vehicles.join(", ")}`);
        }
        // W3 浣跨敤鍏ㄩ儴杞﹁締锛堟帓闄ゅ悗鍚庣浼氳繃婊わ級
        payload.vehicles = finalScenario.vehicles.map((vehicle) => ({
          ...vehicle,
          speed: Number(strategyConfig.w3SpeedKmh || 48),
        }));
      } else if (waveId === "W4") {
        // W4: 涓嶆帓闄わ紙鍙牴鎹渶瑕佽皟鏁达級
        payload.vehicles = finalScenario.vehicles;
      }
      
      const response = await fetch("/wave-optimize", {                     
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const result = await response.json();
      if (!result.bestState) throw new Error(`娉㈡ ${waveId} 姹傝В澶辫触`);
      
      // 璁板綍鏈尝娆′娇鐢ㄧ殑杞﹁締
      const waveUsedPlates = result.bestState
        .filter(item => item.routes && item.routes.length > 0)
        .map(item => item.plateNo);
      waveUsedPlates.forEach(plate => usedVehicles.add(plate));
      console.log(`[directVehicleSolve] ${waveId} 浣跨敤杞﹁締: ${waveUsedPlates.join(", ")}`);
      
      // 濡傛灉鏄?W1锛岃褰曟瘡涓溅杈嗙殑闂ㄥ簵鍒楄〃锛堟寜璺嚎椤哄簭锛?      if (waveId === "W1") {
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
      
      // 杞崲涓哄墠绔渶瑕佺殑 plan 缁撴瀯
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
      label: "杞﹁締椹卞姩鏋勯€狅紙鏂帮級",
      description: "鏃犵‖绾︽潫锛屽叏闂ㄥ簵蹇呰皟搴︼紝鏃ュ織瑙?C:\\Users\\laoj0\\Desktop\\123.txt",
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
    box.textContent = `杞﹁締椹卞姩鏋勯€狅紙鏂帮級瀹屾垚锛岃瘎鍒?${metrics.score.toFixed(1)}锛屽凡璋冨害 ${metrics.scheduledCount} 瀹讹紝鏈皟搴?${metrics.unscheduledCount} 瀹躲€傝鎯呮棩蹇?C:\\Users\\laoj0\\Desktop\\123.txt`;
    await updateGenerationProgress(100, "瀹屾垚");
  } catch (err) {
    console.error(err);
    if (box) box.textContent = `杞﹁締椹卞姩鏋勯€狅紙鏂帮級澶辫触锛?{err.message}`;
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
    newBtn.textContent = lang() === "ja" ? "杌婁浮椐嗗嫊妲嬬瘔锛堟柊锛? : "杞﹁締椹卞姩鏋勯€狅紙鏂帮級";
    newBtn.style.marginLeft = "8px";
    newBtn.addEventListener("click", directVehicleSolve);
    parent.insertBefore(newBtn, generateBtn.nextSibling);
  }
}

// 纭繚鎸夐挳鍦?DOM 鍔犺浇鍚庢坊鍔?
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", addDirectVehicleButton);
} else {
  addDirectVehicleButton();
}
