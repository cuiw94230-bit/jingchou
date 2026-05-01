# 人工经验提取

## 1. 文档目的
本文档用于完整说明“经验模块（rengong）”近两天形成的最终成果，覆盖：

- 业务目标与边界
- 数据来源与抽取规则
- 数据库落库结构（表/字段）
- 后端函数与接口
- 前端展示结构
- 已实现的统计模板与规律识别
- 批处理落库与验收口径
- 常见问题与排查方法

本文档仅描述“经验模块”，不涉及调度主系统算法改造，不改变 solver / evaluator / 调度页面。

---

## 2. 业务目标与边界

### 2.1 目标
从人工调度历史结果（`human_dispatch_routes`）中，提取可复用的“结构经验”，包括：

1. 双配结构（X / X+100）
2. 单配线路结构
3. 模板化统计（结构分布，不输出具体调度解）
4. 经验结果按天落库，支持跨日期批处理与回溯

### 2.2 边界（严格）

- 不改 solver
- 不改 evaluator
- 不改调度主流程
- 不把人工某天方案直接当“调度初始解”
- 只做分析抽取、模板统计、经验展示与经验落库

---

## 3. 数据来源与核心口径

## 3.1 主数据来源

- 人工调度明细：`human_dispatch_routes`
- 经验页求解对齐明细：`human_dispatch_solver_ready`
- 门店档案补齐：`human_dispatch_solver_profile`（及部分门店基础表）

## 3.2 双配识别口径

按**同一天 + 同车**分组：

- 若存在 `route_id = X` 与 `route_id = X + 100`
- 则定义为一个双配门店组

并计算：

- `first_stores`（X）
- `second_stores`（X+100）
- `core_stores = first ∩ second`
- `extra_stores = second - first`
- `union_stores = first ∪ second`

## 3.3 单配识别口径

按**同一天 + 同车**分组：

1. 先识别并标记全部双配使用的 route_id（X 与 X+100）
2. 剩余 route_id 记为单配线路（single route）

注意：单配不是“无数据”，而是“不参与 X/X+100 对关系”的线路。

---

## 4. 数据库结构（经验落库三表）

> 以下三表均属于经验模块，不写入调度结果表，不影响主调度。

## 4.1 `rengong_store_groups`（双配结构层）

每条记录表示一个双配门店组。

核心字段：

- `delivery_date`：日期
- `vehicle_id`：车牌/车辆标识
- `base_route_id`：基线路号（较小 route_id）
- `first_route_id`：X
- `second_route_id`：X+100
- `first_store_ids`：一配门店集合（JSON）
- `second_store_ids`：二配门店集合（JSON）
- `core_store_ids`：重合门店集合（JSON）
- `extra_store_ids`：二配新增门店集合（JSON）
- `first_store_count`：一配店数
- `second_store_count`：二配店数
- `core_store_count`：重合店数
- `extra_store_count`：二配新增店数
- `overlap_ratio`：重合率
- `extra_ratio`：二配新增比例
- `created_at`：写入时间

## 4.2 `rengong_single_routes`（单配结构层）

每条记录表示一个单配线路。

核心字段：

- `delivery_date`
- `vehicle_id`
- `route_id`
- `route_name`
- `store_ids`（JSON）
- `store_count`
- `single_type`（`airport / station / metro / other`）
- `created_at`

## 4.3 `rengong_templates`（模板层）

每条记录表示某天的聚合模板（不是具体调度方案）。

核心字段：

- `delivery_date`
- `day_type`（`NORMAL` / `COLD_ONLY`）
- `group_count`
- `avg_first_store_count`
- `avg_second_store_count`
- `avg_extra_ratio`
- `avg_overlap_ratio`
- `single_route_count`
- `airport_count`
- `station_count`
- `metro_count`
- `other_count`
- `single_store_count`
- `small_route_count`
- `medium_route_count`
- `large_route_count`
- `created_at`

### day_type 规则

- 周日：`COLD_ONLY`
- 其余：`NORMAL`

Python 判定口径：`weekday() == 6`（周日）
MySQL 回填口径：`DAYOFWEEK(delivery_date)=1`（周日）

---

## 5. 后端函数清单（经验模块）

以下函数位于：

- `tools/ga_backend_server.py`

## 5.1 结构抽取函数

1. `_rengong_extract_store_groups(delivery_date)`
   - 输入：日期
   - 输出：
     - `storeGroupStats`
     - `storeGroups[]`
   - 作用：按 X / X+100 提取双配门店组，并生成重合/新增指标

2. `_rengong_extract_single_routes(delivery_date)`
   - 输入：日期
   - 输出：
     - `singleRouteStats`
     - `singleRoutes[]`
   - 作用：提取未参与双配的单配线路

3. `_rengong_build_dispatch_input_simulation(groups)`
   - 输入：双配门店组
   - 输出：
     - `dispatchInputSimStats`
     - `dispatchInputSimulations[]`
   - 作用：构造“调度输入模拟（结构层）”
   - 规则：
     - `wave1_stores = core_stores（一配）`
     - `wave2_stores = core + extra（二配）`
   - 仅用于分析展示，不接调度求解

## 5.2 画像与模板函数

4. `_rengong_build_single_route_portrait(single_routes)`
   - 对单配做画像统计：
     - 分类：机场 / 高铁车站 / 地铁 / 其他
     - 分布：单店、小线(<=3)、中线(4~6)、大线(>=7)

5. `_rengong_build_human_structure_template(...)`
   - 输出三类模板：
     - 双配模板
     - 单配模板
     - 车辆使用模板
   - 仅输出聚合统计，不输出具体车牌绑定方案

## 5.3 落库函数

6. `_ensure_rengong_template_tables()`
   - 作用：确保三张经验表存在；兼容补齐 `day_type` 字段

7. `_rengong_persist_day_templates(delivery_date, store_groups, single_routes, human_structure_template)`
   - 作用：按天“先删后写”落库三张经验表
   - 同时写入 `day_type`

8. `run_rengong_template_batch()`
   - 作用：全量日期批处理
   - 流程：
     1. 读取 `human_dispatch_routes` 全部日期
     2. 逐天提取双配/单配/模板并落库
     3. 逐天做覆盖率校验（human vs experience）
   - 输出：
     - `processed_days`
     - `daily[]`（每天 group/single/missing/extra）
     - `missing_dates[]`

## 5.4 经验接口函数

9. `rengong_day_view(query)`
   - 作用：经验页当天展示主接口
   - 返回包含：
     - 双配、单配、画像、模板、输入模拟、门店双表、车辆表等数据

10. `rengong_summary(query)` / `rengong_list(query)`
   - 作用：经验页摘要与列表支撑

---

## 6. 接口清单（经验模块）

## 6.1 GET `/rengong/day-view`

用途：加载某天经验页全量视图数据（含双表、双配、单配、画像、模板）

主要返回字段（核心）：

- `daySummary`
- `waveBreakdown`
- `storeGroupStats`, `storeGroups`
- `dispatchInputSimStats`, `dispatchInputSimulations`
- `singleRouteStats`, `singleRoutes`
- `singleRoutePortrait`
- `humanStructureTemplate`
- `multiDeliveryStores`, `singleDeliveryStores`

## 6.2 GET `/rengong/summary`

用途：经验模块汇总统计

## 6.3 GET `/rengong/list`

用途：经验列表查询

## 6.4 POST `/rengong/template-batch`

用途：全量日期批处理并落库

返回：

- `processed_days`
- `daily[]`
- `missing_dates[]`
- `has_missing_dates`

---

## 7. 前端页面成果（经验页）

文件：

- `rengong.html`
- `rengong.js`
- `rengong.css`

已实现模块（经验页）：

1. 日期选择与当天加载
2. KPI 摘要区（当日门店数、待补数、原始货量、折算货量）
3. 波次概览
4. 门店组（双配结构：X / X+100）
5. 调度输入模拟（基于人工门店组）
6. 单配线路（人工结构）
7. 单配线路画像（分类统计 + 店数分布）
8. 人工结构模板（双配模板 + 单配模板 + 车辆模板）
9. 一日多配门店明细
10. 一日一配门店明细
11. 车辆信息（按当天车号去重展示，默认未使用）

说明：

- 经验页是独立页面，不修改 `app.js`
- 入口按钮已在主页面增加“经验”

---

## 8. 关键业务规则与规律沉淀

## 8.1 双配规律

- 双配核心模式：`X / X+100`
- 二配通常是“一配核心 + 少量新增”
- 用 `second_extra_ratio` 与 `jaccard`（重合率）衡量稳定性

## 8.2 单配规律

- 单配是“未配对线路”，不是“空线路”
- 单配可按线路名语义分型（机场/高铁车站/地铁/其他）
- 单配可按店数分布判断任务形态（单店、小线、中线、大线）

## 8.3 日类型规律

- 周日标记为 `COLD_ONLY`
- 非周日标记为 `NORMAL`
- 可直接用于后续分日型建模与策略隔离

---

## 9. 验收口径与已验证结果

## 9.1 覆盖率验收口径

按天比较：

- `human_dispatch_routes` 当天 `DISTINCT store_id`（人工全集）
- 经验集合：
  - 双配门店组 `store_ids` 并集
  - 单配线路 `store_ids` 并集

检查：

- `missing_count = all_stores - experience_stores`
- `extra_count = experience_stores - all_stores`

## 9.2 已验证状态

全量批处理已可运行，并生成：

- `processed_days`
- 逐日 `group_count/single_count/missing_count/extra_count`

在最新验收中，`missing_dates` 为 0（无缺失日期）。

---

## 10. 运行与维护建议

1. 每次导入/刷新人工数据后，执行一次全量批处理：
   - `POST /rengong/template-batch`

2. 日常查看某天经验结果：
   - 打开经验页，选日期查询

3. 如发现某天结构异常，优先核查：
   - `human_dispatch_routes` 当天是否缺数据
   - 车号/route_id 是否存在脏值
   - 是否存在异常 route_id 非整数

4. 不建议在经验模块中引入调度求解行为，保持“分析层”职责纯净。

---

## 11. 非目标声明（避免误解）

以下不属于本模块当前职责：

- 不生成可直接执行的调度解
- 不替代 solver
- 不回写调度结果表
- 不修改主调度策略中心与算法参数
- 不输出“具体车牌绑定具体门店”的可执行方案

本模块定位：**经验结构抽取 + 聚合模板沉淀 + 可视化验证 + 数据资产落库**。

---

## 12. 术语表

- 双配门店组：同一天同车下，route_id 存在 X 与 X+100 的组
- 单配线路：同一天同车下，不参与 X/X+100 配对的线路
- 核心店（core）：一配与二配共同出现的门店
- 二配新增店（extra）：仅在二配出现、未在一配出现的门店
- 重合率（jaccard）：`|A∩B| / |A∪B|`
- 二配新增比例：`|extra| / |first|`
- 日类型（day_type）：`NORMAL` 或 `COLD_ONLY`

---

## 13. 结论

经验模块当前已完成从人工调度数据到“结构层 + 模板层 + 落库层 + 展示层”的闭环：

- 有规则可追溯（X/X+100、single）
- 有数据可复算（按天先删后写）
- 有模板可比较（双配/单配/车辆）
- 有批处理可扩展（全量日期）

并与主调度系统解耦，满足“只做经验提取，不扰动调度核心”的要求。


---

## 12. 2026-04-27 20:08 之后的索引/入口链路改动补录（经验/混沌）

> 本节仅补录 `2026-04-27 20:08` 之后发生的“索引（入口/跳转/页面组织）相关改动”，口径与本文一致：
> - 不改调度主链路
> - 不接 solver/evaluator/objective
> - 以经验/混沌模块只读展示为主

### 12.1 变更范围

本轮索引相关改动涉及文件：

- `index.html`（混沌页入口区新增“见微”按钮）
- `tuiyan.js`（从“见微”回跳后自动打开混沌页）
- `chaos-jianwei.html`（见微页面新增/文案与按钮）
- `chaos-jianwei.js`（见微页面只读数据加载、筛选、排序、摘要映射）
- `chaos-jianwei.css`（见微页面样式）
- `tools/ga_backend_server.py`（只读接口：`/chaos/replay-task-view`）

### 12.2 新增页面与入口（索引层）

#### 12.2.1 混沌二级页“见微”新增

新增页面：

- `chaos-jianwei.html`
- `chaos-jianwei.js`
- `chaos-jianwei.css`

定位：

- 属于混沌模块下的二级只读页面
- 用于展示“人工调度历史反推的每日待调度任务输入”
- 不触发求解，不写入调度表

#### 12.2.2 混沌页新增“见微”入口

在 `index.html` 的混沌操作区新增按钮：

- 按钮名：`见微`
- 跳转：`./chaos-jianwei.html`

作用：

- 从混沌首页可直接进入“见微”复盘页

### 12.3 回跳链路修复（索引行为修复）

问题：

- 在“见微”页点击“混沌”按钮时，出现“回到主页而非混沌首页”的体验问题。

原因：

- 主页面混沌区是单页内隐藏区（`#tuiyanPage`），不是独立 `chaos.html` 文件。
- 仅靠普通 URL 跳转无法保证自动展开混沌区。

修复：

- `chaos-jianwei.html` 的“混沌”按钮改为：
  1. 先写 `sessionStorage.openChaosOnLanding = '1'`
  2. 再跳 `index.html`
- `tuiyan.js` 在 `initPageSwitch()` 中新增一次性判断：
  - 若检测到该标记，则自动执行 `openPage()` 打开混沌区
  - 打开后立即清理标记

结果：

- 从“见微”点击“混沌”，可直接回到混沌首页视图，不再停留主页普通区。

### 12.4 见微页面只读接口（供索引页加载）

新增只读接口：

- `GET /chaos/replay-task-view?date=YYYY-MM-DD`

数据来源（只读）：

- `chaos_replay_tasks`
- `chaos_replay_task_stores`
- `chaos_replay_task_vehicles`

默认行为：

- 未传 `date` 时，自动取 `chaos_replay_tasks` 在 `calc_version='chaos_replay_task_v1'` 下的最新日期。

返回结构：

- `task`: 当日摘要
- `stores`: 店铺明细
- `vehicles`: 车辆明细

### 12.5 见微页面展示增强（仅前端）

#### 12.5.1 摘要字段中文化

- `delivery_date -> 日期`
- `day_type -> 日类型`（前端映射：`NORMAL->平日`，`COLD_ONLY->周日`）
- `store_count -> 店铺数`
- `vehicle_count -> 车辆数`
- `route_count -> 线路数`
- `total_load_w1 -> W1装载`
- `total_load_w2 -> W2装载`
- `total_load -> 总装载`

#### 12.5.2 店铺表增强

- 新增“序号”列（按当前筛选+排序后的顺序重编号）
- 新增“原始货量”列（直接展示接口返回 `sum_*` 字段，不前端重算）
- 保持搜索：店铺编号、店铺名称
- 保持排序：店铺编号、店铺名称、W1货量、W2货量、总货量

#### 12.5.3 车辆表增强

- 新增“序号”列（按当前筛选+排序后的顺序重编号）
- 保持搜索：车牌、司机
- 新增排序：车牌、司机、线路数

### 12.6 原始货量空值问题排查与修复

问题：

- 前端“原始货量”列读取 `sum_rpcs/sum_rcase/...`，但接口最初未返回这些字段，导致展示为空。

后端修复（只读接口层）：

- `chaos_replay_task_view()` 的 `stores` 查询新增返回：
  - `sum_rpcs`
  - `sum_rcase`
  - `sum_rpaper`
  - `sum_bpcs`
  - `sum_bpaper`
  - `sum_apcs`
  - `sum_apaper`

注意：

- 字段来自已落库表 `chaos_replay_task_stores`，未改表结构。
- 接口代码改后需重启后端进程，前端才能拿到新字段。

### 12.7 索引改动验收口径

本段改动验收重点：

1. 入口可达
   - `index.html` 可进入“见微”
   - “见微”可回“混沌首页”

2. 只读性
   - 全部为 GET 拉取，不写调度主表，不触发求解

3. 功能不回归
   - 见微页搜索/排序/刷新可用
   - 混沌页原有推演链路不受影响

4. 红线符合
   - 未修改 solver/evaluator/objective
   - 未修改调度主接口 `/wave-optimize` 行为

### 12.8 关键时间点（补录）

- `2026-04-28 ~ 2026-04-29`
  - 完成“见微”页面新增与入口接入
  - 完成 `/chaos/replay-task-view` 只读接口
  - 修复“见微->混沌”回跳链路
  - 修复见微原始货量字段返回
  - 完成 `day_type` 中文映射展示


---

## 2026-04-30 当日17阶段工作总览（完整审计版）

> 范围说明：本文仅记录**经验/坐照链路**与**混沌只读复盘链路**的建设结果。
> 严格遵守红线：未改 solver/evaluator/objective/主调度接口行为；未接自动调度；未写 C_SHOP_MAIN。

### 0. 当日总体目标

1. 把“人工调度经验”从数据抽取推进到**可解释、可复核、可展示**。
2. 建立三层经验结构：
   - 结构层：今天线路 vs 前一同类日线路（店铺/顺序/变化）
   - 负载层：每条线路历史负载分布与风险分区（slack）
   - 参考层：不同负载位置下，人工更可能参考谁（reference weight）
3. 形成前台演示页“登堂”：只读解释，不参与求解，不改调度结果。

---

## 1) 17阶段逐阶段结果（做了什么）

### 第1-5阶段（只读分析）

- 完成“人工复用规律”基础分析：
  - 同类日口径：NORMAL 对比前一个 NORMAL，COLD_ONLY 对比前一个 COLD_ONLY
  - 结构复用类型：EXACT_COPY / SAME_STORES_DIFF_ORDER / PARTIAL_CHANGE / NEW_ROUTE / MISSING_PREV_ROUTE / UNKNOWN_CHANGE
- 完成变化原因拆解：
  - LOAD_DRIVEN_CHANGE / STRUCTURE_SPLIT / STRUCTURE_MERGE / ROUTE_REASSIGN / ORDER_ONLY_CHANGE / ZERO_OR_TINY_LOAD_ROUTE / DATA_ISSUE / UNKNOWN_REASON
- 完成负载弹性（slack）提取前置验证：
  - EXACT_COPY 负载波动分布
  - 负载位置（相对 p90）与结构变化率关系

### 第6阶段（建表+样例写入）

- 新建经验表：`rengong_route_slack_profiles`
- 完成样例写入（小批）和字段规则验证

### 第7阶段（全量前校验）

- 校验了 `rengong_route_slack_profiles` 样例数据：
  - 分位数单调性
  - 风险区公式
  - low_sample_flag 正确性
  - 唯一键冲突检查

### 第8阶段（全量幂等写入）

- 对 `rengong_route_slack_profiles` 做全量 UPSERT（幂等）
- 口径：只剔除 p99 极端样本，不剔除 812 条敏感样本

### 第9阶段（reference source 只读建模）

- 参考来源类型确立：
  - PREV_MATCH
  - RECENT_STABLE_MATCH
  - ANY_HISTORY_MATCH
  - NO_REFERENCE
- 优先级定义：PREV > RECENT_STABLE > ANY_HISTORY > NO_REFERENCE

### 第10阶段（reference_weight 离散统计）

- 按 `load_vs_p90` 分桶统计四类参考来源权重（全局 + 分层）

### 第10.5阶段（权重函数拟合）

- 对比两种落地函数：
  - piecewise_constant
  - piecewise_linear
- 给出可落库版本（采用分段权重口径）

### 第11阶段（建表+落库）

- 新建经验表：`rengong_reference_weight_profiles`
- 写入 7 条全局分段策略（reference_weight_v1）

### 第12/12.5阶段（解释引擎样例）

- 基于 slack + reference_weight 输出中文解释样例
- 解释话术从“模型语言”改为“调度员可读语言”

### 第13阶段（评审样本导出）

- 生成调度解释评审样本（近7日，多类型覆盖）
- 形成人工可评审字段（认同/误导/备注）

### 第14阶段（全链路审计）

- 核查数据库一致性、字段完整性、唯一键、公式、乱码
- 结论：核心经验表结构与公式通过；导出物存在历史乱码问题需重生

### 第14.5阶段（clean导出重生）

- 重生 `dispatch_explain_review_clean.xlsx`（仅评审必要字段）
- 修复中文输出链路（Unicode escape 方案）

### 第15阶段（启动与静态链路修复）

- 修复后台启动链路与静态访问阻断点（确保前端能通过 HTTP 访问）
- 识别 `file://` 导致 fetch 失败的根因，完成 `http://127.0.0.1:8765` 入口闭环

### 第16阶段（登堂页面首版）

- 新增页面：`dengtang.html/js/css`
- 新增接口：`GET /rengong/dengtang-explain?date=YYYY-MM-DD`
- 前端功能：日期、KPI、筛选、线路卡片解释（只读）

### 第17阶段（登堂参考日还原层）

- 在 `routes[]` 增加：reference/current_snapshot/reference_snapshot/diff_summary
- 前端新增显示：今日线路、参考日线路、变化摘要（店铺/顺序/货量/里程）
- 强约束落地：无单店里程字段时明确返回 `store_distance_available=false` + `store_distance_note`

---

## 2) 今日新增/使用的核心表（建了什么表）

> 注：仅列当日17阶段核心结果链路涉及。

### A. `rengong_route_slack_profiles`（经验核心画像表）

用途：保存每条线路（route_id + day_type）的历史负载分布、风险区间、变化率画像。

关键字段与含义：

- `delivery_scope`：固定 `HUMAN_ROUTE_REUSE`，表示“人工复用经验域”
- `day_type`：NORMAL/COLD_ONLY
- `route_id`：线路号
- `route_group`：W1/W2/OTHER
- `route_type`：市区/地铁/周边城市/机场/郊区周边等
- `is_city`：是否市区
- `sample_count`：样本天数
- `load_min/p10/p25/p50/p75/p90/load_max`：该线路历史负载分布
- `stable_zone_upper`：稳定上界（=p75）
- `watch_zone_upper`：观察上界（=p90）
- `soft_risk_upper`：软风险上界（=p90*1.1）
- `high_risk_upper`：高风险上界（=p90*1.3）
- `normal_load_limit`：普通线路硬上限（如适用）
- `change_rate_below_p75`：低负载区变化率
- `change_rate_p75_p90`：观察区变化率
- `change_rate_above_p90`：超过历史上界变化率
- `change_rate_above_1_1_p90 / change_rate_above_1_3_p90`：高区段变化率
- `change_rate_limit_exceeded`：超过 normal_limit 时变化率
- `data_issue_count`：p99 极端样本计数
- `low_sample_flag`：样本不足标记（sample_count<15）
- `calc_version`：`route_slack_profile_v1`
- `source_date_start/source_date_end`：来源日期范围

唯一键：`calc_version + day_type + route_id`

### B. `rengong_reference_weight_profiles`（经验参考权重表）

用途：给定 `x=route_load/load_p90`，输出人工参考来源权重（prev/recent/any/no_ref）。

关键字段与含义：

- `calc_version`：`reference_weight_v1`
- `day_type_scope/route_group_scope/route_type_scope/is_city_scope`：当前版本全局范围（ALL）
- `bucket_min/bucket_max/bucket_label`：分段区间
- `prev_weight`：参考前一同类日权重
- `recent_weight`：参考最近稳定日权重
- `any_history_weight`：参考任意历史相似日权重
- `no_reference_weight`：放弃参考、重构权重
- `sample_count`：桶内样本数
- `low_sample_flag`：桶内样本不足标记
- `source_date_start/source_date_end`

固定 7 桶：
- LT_0_6
- B_0_6_0_8
- B_0_8_0_9
- B_0_9_1_0
- B_1_0_1_1
- B_1_1_1_3
- GT_1_3

### C. 复盘任务表（此前建立，今日持续使用）

1. `chaos_replay_tasks`
2. `chaos_replay_task_stores`
3. `chaos_replay_task_vehicles`

用途：反向抽取“每日待调度输入题面”（只读复盘），用于解释样本与同类日判定辅助。

---

## 3) 今日接口与页面能力（做了什么功能）

### 后端接口

#### `GET /rengong/dengtang-explain?date=YYYY-MM-DD`

功能：
- 返回当天线路解释结果（只读）
- 口径：前一同类日参考（prev comparable day）
- 输出 summary + routes

`daySummary` 字段：
- `delivery_date`
- `day_type`
- `route_count`
- `exact_copy_count`
- `changed_count`
- `stable_count`
- `risk_count`
- `review_count`

`routes[]` 字段：
- `route_id`
- `route_load`
- `load_status`
- `reuse_type`
- `decision_hint`
- `explanation_text`
- `review_level`

第17阶段新增：

`reference`：
- `reference_date`
- `reference_type`（当前固定 `PREV_COMPARABLE_DAY`）
- `reference_label`

`current_snapshot`：
- `route_id`
- `route_load`
- `store_count`
- `distance`（线路级）
- `distance_available`
- `distance_note`
- `stores[]`
- `store_distance_available`
- `store_distance_note`

`reference_snapshot`：同上（参考日快照）

`diff_summary`：
- `added_store_count`
- `removed_store_count`
- `same_store_count`
- `order_changed`
- `route_load_delta`
- `route_load_delta_ratio`
- `distance_delta`
- `distance_delta_ratio`

`stores[]` 字段：
- `stop_sequence`
- `store_id`
- `store_name`
- `store_load`
- `distance_from_prev`
- `distance_cumulative`

单店里程强约束：
- 若无字段，不虚构，返回：
  - `store_distance_available=false`
  - `store_distance_note="当前数据无单店里程字段"`

### 前端页面

#### `dengtang.html` / `dengtang.js` / `dengtang.css`

定位：
- 只读解释展示
- 不出调度方案
- 不触发求解

能力：
1. 日期选择
2. KPI（线路数/稳定复用/发生变化/高风险/需复核）
3. 筛选（全部/稳定复用/发生变化/高风险/需复核）
4. 线路卡片（建议+原因）
5. 参考日还原层：
   - 今日线路快照
   - 参考日线路快照
   - 变化摘要（新增/移出/顺序/货量/里程）

入口：
- 已在 `rengong.html`（坐照页顶部按钮区）增加“登堂”入口
- 未挂混沌模块

---

## 4) 今日形成的模型（形成了什么模型）

### 模型一：结构复用判定模型（规则型）

输入：同 route_id 的“今日 vs 前一同类日”店铺序列

输出：
- EXACT_COPY
- SAME_STORES_DIFF_ORDER
- PARTIAL_CHANGE
- NEW_ROUTE
- MISSING_PREV_ROUTE
- UNKNOWN_CHANGE

用途：
- 识别“是否复用前一同类日结构”
- 提供解释引擎中的“实际行为”

### 模型二：线路负载弹性模型（slack profile）

输入：route_id + day_type 历史负载样本

核心输出：
- p75/p90 等分布点
- 稳定/观察/软风险/高风险边界
- 各区段变化率

用途：
- 回答“这条线在什么负载范围内通常不变”
- 回答“什么时候结构变化概率开始抬升”

### 模型三：参考来源权重模型（reference weight）

输入：x=route_load/load_p90

输出：
- P(prev)
- P(recent)
- P(any_history)
- P(no_reference)

用途：
- 回答“当前负载下，人工更可能模仿谁”
- 提供解释中的“行为倾向依据”

---

## 5) 模型怎么用（业务使用方式）

### 用法A：解释某天每条线路为什么保持/变化

1. 选日期（如 2026-04-08）
2. 接口返回每条线路：
   - 今日快照
   - 参考日快照
   - diff_summary
   - 建议与原因
3. 调度员可快速判断：
   - 今天是否“该动就动、该稳就稳”
   - 哪些线路需要人工复核

### 用法B：沉淀线路经验边界

- 通过 `rengong_route_slack_profiles` 看线路长期稳定区间
- 对异常线路进行“结构变化触发条件”排查

### 用法C：做经验评审闭环

- 使用 clean 评审样本表（xlsx）
- 人工填写“认同/误导/备注”
- 反哺解释文案，不改调度核心

---

## 6) 当前模型能回答什么问题

1. 这条线路今天有没有参考前一同类日？
2. 如果变了，主要是店铺变了还是顺序变了？
3. 今天相对参考日新增/移出多少店？
4. 货量变化了多少？（绝对值、比例）
5. 线路级里程相对参考日变化了多少？
6. 当前负载处于线路历史的哪个风险区？
7. 这类负载位置下，人工通常更偏向复用还是重构？
8. 哪些线路属于“高风险但未调整”或“低负载却调整”，需要重点复核？

---

## 7) 当日红线执行与边界说明

- 未改 solver/evaluator/objective
- 未改主调度接口行为
- 未接自动调度
- 未写 C_SHOP_MAIN
- 未新增调度核心落库
- 经验链路均为只读分析或经验表落库

里程口径：
- 路线级里程优先取已有真实里程字段（如 `rengong_route_distance_load_profiles.one_way_distance_km`）
- 单店里程若无字段，明确提示缺失，不做虚构

---

## 8) 当前遗留与下一步建议（非强制）

1. 单店里程字段目前缺失（接口已明确提示）
   - 若后续希望展示“逐店段里程”，需补齐可追溯来源字段（仍需高德真实链路）
2. 参考来源目前固定 prev comparable day
   - 后续可在不改主调度前提下，增量扩展 recent/any history 对照视图
3. 解释文案已可读
   - 后续可接人工评审结果做话术分层（仍只限前端展示层）

---

## 9) 本节关键词索引

- 经验主表：`rengong_route_slack_profiles`
- 参考权重表：`rengong_reference_weight_profiles`
- 解释接口：`/rengong/dengtang-explain`
- 页面：`dengtang.html/js/css`
- 参考口径：`PREV_COMPARABLE_DAY`
- 版本：`route_slack_profile_v1` / `reference_weight_v1`
