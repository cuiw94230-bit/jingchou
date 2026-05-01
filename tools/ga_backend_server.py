"""
GA 鍚庣鎬诲叆鍙ｃ€?

杩欐槸椤圭洰鐨勭患鍚堝悗鍙帮細
1. 鏆撮湶璋冨害銆佹帹婕斻€佸綊妗ｃ€乄MS銆佸尯鍩熸柟妗堛€佽窛绂荤紦瀛樼瓑 HTTP 鎺ュ彛锛?
2. 璐熻矗鏁版嵁搴撳缓琛ㄣ€佽惤搴撱€佺姸鎬佹祦銆佹棩蹇楁祦锛?
3. 璐熻矗缁勮姹傝В payload锛屽苟璋冪敤 solver_core 涓殑绠楁硶瀹炵幇銆?
"""
import json
import math
import os

import random
import re
import time
import traceback
import mimetypes
import base64
import io
import csv
import hashlib
import socket
import threading
import contextlib
from urllib.parse import urlparse, parse_qs
from decimal import Decimal
from copy import deepcopy
from datetime import datetime, timedelta, date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import error as urllib_error
from urllib import request as urllib_request
from functools import lru_cache
import importlib

try:
    import pymysql
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
except Exception:
    pymysql = None

# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
try:
    import cupy as cp
    GPU_AVAILABLE = True
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
except Exception:
    GPU_AVAILABLE = False

# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
try:
    from skopt import gp_minimize
    from skopt.space import Real, Integer
    SKOPT_AVAILABLE = True
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
except Exception:
    SKOPT_AVAILABLE = False

try:
    import pandas as pd
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
except Exception:
    pd = None

# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
try:
    import pyodbc
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
except Exception:
    pyodbc = None

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import settings
from solver_core.runner import run_wave_optimize, run_ga_optimize
from solver_core.constraints import check_plan_constraints
from solver_core.integrity_guard import verify_algorithm_file
from solver_core.simulation import build_failure_records as build_simulation_failure_records
from tools.strategy_center import apply_strategy_center

import os

HOST = settings.HOST
PORT = int(os.environ.get("PORT", settings.PORT))
DC_ID = "DC"
DEEPSEEK_API_URL = settings.DEEPSEEK_API_URL
MYSQL_HOST = settings.MYSQL_HOST
MYSQL_PORT = settings.MYSQL_PORT
MYSQL_USER = settings.MYSQL_USER
MYSQL_PASSWORD = settings.MYSQL_PASSWORD
MYSQL_DATABASE = settings.MYSQL_DATABASE
ALLOW_REMOTE_DB = settings.ALLOW_REMOTE_DB

ARCHIVE_TABLE = settings.ARCHIVE_TABLE
ARCHIVE_ROUTE_TABLE = settings.ARCHIVE_ROUTE_TABLE
RECOMMENDED_PLAN_TABLE = "recommended_plan_candidates"
AMAP_DISTANCE_TABLE = settings.AMAP_DISTANCE_TABLE
AMAP_ROUTE_TABLE = settings.AMAP_ROUTE_TABLE
STORE_DISTANCE_TABLE = "store_distance_matrix"

RUN_REGION_TABLE = "dispatch_run_regions"
RUN_REGION_SCHEME_TABLE = "dispatch_region_schemes"
RUN_REGION_MEMBER_TABLE = "dispatch_region_members"
WMS_CRED_TABLE = "wms_remote_credential"
WMS_BATCH_TABLE = "wms_sync_batches"
WMS_RAW_TABLE = "wms_raw_records"
WMS_SHOP_TABLE = "wms_shop_snapshot"
WMS_VEHICLE_TABLE = "wms_vehicle_snapshot"
WMS_CARGO_TABLE = "wms_cargoqty_snapshot"
WMS_CARGO_RAW_TABLE = "wms_cargo_raw_snapshot"
WMS_CARGO_RAW_CLEAN_TABLE = "wms_cargo_raw_clean_snapshot"
WMS_CARLOAD_TABLE = "wms_carload_snapshot"
WMS_ARRIVAL_TABLE = "wms_arrivaltime_snapshot"
LOCAL_LOAD_TABLE = "local_store_total_load"
LOCAL_LOAD_FACTOR_TABLE = "local_load_factor_rows"
HUMAN_DISPATCH_SOLVER_PROFILE_TABLE = "human_dispatch_solver_profile"
RENGONG_STORE_GROUP_TABLE = "rengong_store_groups"
RENGONG_SINGLE_ROUTE_TABLE = "rengong_single_routes"
RENGONG_TEMPLATE_TABLE = "rengong_templates"
SIMULATION_FAILURE_LOG_TABLE = "simulation_failure_log"
SIMULATION_TASK_TABLE = "simulation_task"
SIMULATION_STORE_ATTEMPT_TABLE = "simulation_store_attempt"
SIMULATION_INSERT_TRIAL_TABLE = "simulation_insert_trial"
SIMULATION_SINGLE_TASK_TABLE = "simulation_single_task"
SIMULATION_SINGLE_SNAPSHOT_TABLE = "simulation_single_snapshot"
SIMULATION_SINGLE_ROUTE_TABLE = "simulation_single_route"
SIMULATION_SINGLE_ROUTE_STOP_TABLE = "simulation_single_route_stop"
SIMULATION_SINGLE_EVENT_LOG_TABLE = "simulation_single_event_log"
SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE = "simulation_single_confirmed_route"
SIMULATION_SINGLE_REMAINING_STATE_TABLE = "simulation_single_remaining_state"
SIMULATION_SINGLE_STATE_TRANSITION_TABLE = "simulation_single_state_transition"
LOCAL_CARGO_DEFAULT_FILE = r"C:\x\鍓湰搴楅摵璐ч噺.xlsx"
LOCAL_CARLOAD_DEFAULT_FILE = r"C:\x\瑁呰浇鑳藉姏琛?xlsx"
DEFAULT_VEHICLE_TYPE = "4.2绫冲帰寮忚揣杞?
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SFRZ_LOG_FILE = os.path.join(PROJECT_ROOT, "sfrz.txt")
_SFRZ_LOG_LOCK = threading.Lock()
_SIMULATION_TASKS = {}
_SIMULATION_TASKS_LOCK = threading.Lock()
_SIMULATION_TASK_TTL_SECONDS = 3600
_SIMULATION_STDIO_LOCK = threading.Lock()


def _append_sfrz_log(text):
    line = str(text or "").rstrip("\r\n")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not line:
        return
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = f"{timestamp} {line}\n"
    try:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with _SFRZ_LOG_LOCK:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            with open(SFRZ_LOG_FILE, "a", encoding="utf-8") as fp:
                fp.write(row)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        pass


def _resolve_algorithm_module_file(algorithm_key):
    key = str(algorithm_key or "").strip().lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if key in ("vehicle", "vehicle_driven"):
        mod = importlib.import_module("solver_core.vehicle_driven")
        return str(getattr(mod, "__file__", "") or "")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if key in ("vrptw", "savings", "hybrid", "tabu", "lns", "sa", "aco", "pso", "ga"):
        mod = importlib.import_module(f"solver_core.algorithms.{key}")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return str(getattr(mod, "__file__", "") or "")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return ""


def _assert_expected_algorithm_module(algorithm_key, module_file):
    key = str(algorithm_key or "").strip().lower()
    path = str(module_file or "").replace("\\", "/").lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if key == "aco" and not path.endswith("/solver_core/algorithms/aco.py"):
        raise ValueError(f"algorithm_module_mismatch:aco:{module_file}")
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if key in ("vehicle", "vehicle_driven") and not path.endswith("/solver_core/vehicle_driven.py"):
        raise ValueError(f"algorithm_module_mismatch:vehicle:{module_file}")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _read_sfrz_log_tail(limit=200):
    limit = max(1, min(2000, int(limit or 200)))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not os.path.exists(SFRZ_LOG_FILE):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return []
    with _SFRZ_LOG_LOCK:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with open(SFRZ_LOG_FILE, "r", encoding="utf-8", errors="ignore") as fp:
            lines = fp.readlines()
    return [str(x).rstrip("\r\n") for x in lines[-limit:]]


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _cleanup_simulation_tasks():
    now_ts = time.time()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with _SIMULATION_TASKS_LOCK:
        expired = [
            key for key, value in _SIMULATION_TASKS.items()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if now_ts - float((value or {}).get("updated_at") or 0) > _SIMULATION_TASK_TTL_SECONDS
        ]
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        for key in expired:
            _SIMULATION_TASKS.pop(key, None)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_task_init(task_id):
    task_key = str(task_id or "").strip()
    if not task_key:
        return
    _cleanup_simulation_tasks()
    now_ts = time.time()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with _SIMULATION_TASKS_LOCK:
        _SIMULATION_TASKS[task_key] = {
            "task_id": task_key,
            "status": "running",
            "done": False,
            "lines": [],
            "created_at": now_ts,
            "updated_at": now_ts,
            "error_message": "",
        }


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_task_append(task_id, text):
    task_key = str(task_id or "").strip()
    line = str(text or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not task_key or not line:
        return
    now_ts = time.time()
    stamped = f"{datetime.now().strftime('%H:%M:%S')}  {line}"
    with _SIMULATION_TASKS_LOCK:
        task = _SIMULATION_TASKS.setdefault(task_key, {
            "task_id": task_key,
            "status": "running",
            "done": False,
            "lines": [],
            "created_at": now_ts,
            "updated_at": now_ts,
            "error_message": "",
        })
        task["lines"].append(stamped)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if len(task["lines"]) > 2000:
            task["lines"] = task["lines"][-2000:]
        task["updated_at"] = now_ts


def _simulation_task_finish(task_id, status="success", error_message=""):
    task_key = str(task_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not task_key:
        return
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    with _SIMULATION_TASKS_LOCK:
        task = _SIMULATION_TASKS.get(task_key)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not task:
            return
        task["status"] = str(status or "success")
        task["done"] = True
        task["updated_at"] = time.time()
        task["error_message"] = str(error_message or "")


def _simulation_task_snapshot(task_id, cursor=0):
    task_key = str(task_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key:
        return {"found": False}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    with _SIMULATION_TASKS_LOCK:
        task = deepcopy(_SIMULATION_TASKS.get(task_key) or {})
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {"found": False}
    start = max(0, int(cursor or 0))
    lines = task.get("lines") or []
    return {
        "found": True,
        "task_id": task_key,
        "status": str(task.get("status") or "running"),
        "done": bool(task.get("done")),
        "lines": lines[start:],
        "next_cursor": len(lines),
        "error_message": str(task.get("error_message") or ""),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
class _SimulationTaskLogWriter:
    def __init__(self, task_id, original_stream):
        self.task_id = str(task_id or "").strip()
        self.original_stream = original_stream
        self._buffer = ""

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    def write(self, text):
        chunk = str(text or "")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not chunk:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return 0
        try:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.original_stream:
                self.original_stream.write(chunk)
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        except Exception:
            pass
        self._buffer += chunk
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            clean = str(line).rstrip("\r")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if clean and not _should_skip_simulation_task_line(clean):
                _simulation_task_append(self.task_id, clean)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return len(chunk)

    def flush(self):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        try:
            if self.original_stream:
                self.original_stream.flush()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            pass
        clean = str(self._buffer).rstrip("\r")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if clean and not _should_skip_simulation_task_line(clean):
            _simulation_task_append(self.task_id, clean)
        self._buffer = ""


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _should_skip_simulation_task_line(text):
    line = str(text or "").strip()
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return True
    if 'HTTP/1.1"' in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return True
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "GET /simulate/task-log" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return True
    if "OPTIONS /sfrz/log" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return True
    if "POST /sfrz/log" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return True
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if "GET /health" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return True
    return False


@contextlib.contextmanager
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _capture_simulation_task_stream(task_id):
    writer_out = _SimulationTaskLogWriter(task_id, sys.stdout)
    writer_err = _SimulationTaskLogWriter(task_id, sys.stderr)
    with _SIMULATION_STDIO_LOCK:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with contextlib.redirect_stdout(writer_out), contextlib.redirect_stderr(writer_err):
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            try:
                yield
            finally:
                writer_out.flush()
                writer_err.flush()
WMS_REMOTE_SERVER = "39.107.122.91"
WMS_REMOTE_DATABASE = "smartroute"
WMS_REMOTE_UID = "sa"
_archive_tables_ready = False
_rengong_template_tables_ready = False
DEFAULT_DC_LNG = 116.568327
DEFAULT_DC_LAT = 40.082845

# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def call_deepseek(payload):
    api_key = str(payload.get("apiKey", "")).strip()
    if not api_key:
        raise ValueError("missing_api_key")
    
    request_payload = {
        "model": payload.get("model") or "deepseek-chat",
        "temperature": payload.get("temperature", 0.3),
        "messages": payload.get("messages") or [],
        "max_tokens": payload.get("max_tokens", 4096),
    }
    
    req = urllib_request.Request(
        DEEPSEEK_API_URL,
        data=json.dumps(request_payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    
    last_error = None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for attempt in range(3):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        try:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            with urllib_request.urlopen(req, timeout=180) as response:
                raw = response.read().decode("utf-8")
                data = json.loads(raw)
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if len(content.strip()) < 10 and attempt < 2:
                    time.sleep(1)
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                return {
                    "content": content,
                    "raw": data,
                }
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            last_error = f"http_{exc.code}:{detail}"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if exc.code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(last_error) from exc
        except (urllib_error.URLError, TimeoutError) as exc:
            last_error = f"url_error:{exc.reason if hasattr(exc, 'reason') else str(exc)}"
            time.sleep(2 ** attempt)
            continue
    
    raise RuntimeError(f"闁插秷鐦?濞嗏€虫倵娴犲秴銇戠拹? {last_error}")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def mysql_connection():
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if pymysql is None:
        raise RuntimeError("pymysql_not_installed")
    host = str(MYSQL_HOST or "").strip().lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if (not ALLOW_REMOTE_DB) and host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError(f"remote_db_blocked:{MYSQL_HOST}")
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


# 鍚庣绗竴娆″惎鍔ㄦ椂浼氬湪杩欓噷闆嗕腑寤鸿〃銆?
# 涓昏皟搴︺€佸綊妗ｃ€乄MS銆佸噺杞︽帹婕斻€佸崟搴楁帹婕旂浉鍏崇殑琛ㄩ兘鍦ㄨ繖閲岀粺涓€缁存姢锛岄伩鍏嶈剼鏈悇鑷伔鍋峰缓琛ㄣ€?
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def ensure_archive_tables():
    global _archive_tables_ready
    if _archive_tables_ready:
        return
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ARCHIVE_TABLE} (
                    id VARCHAR(64) PRIMARY KEY,
                    created_at VARCHAR(40) NOT NULL,
                    created_date DATE NOT NULL,
                    strategy VARCHAR(32) NOT NULL,
                    goal VARCHAR(32) NOT NULL,
                    active_result_key VARCHAR(64) NULL,
                    best_score DECIMAL(8,2) NOT NULL DEFAULT 0,
                    result_count INT NOT NULL DEFAULT 0,
                    algorithms TEXT NULL,
                    payload_json LONGTEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_created_date (created_date),
                    INDEX idx_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ARCHIVE_ROUTE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    run_id VARCHAR(64) NOT NULL,
                    result_key VARCHAR(64) NULL,
                    result_label VARCHAR(128) NULL,
                    wave_id VARCHAR(32) NULL,
                    vehicle_plate VARCHAR(64) NULL,
                    trip_no INT NOT NULL DEFAULT 0,
                    store_count INT NOT NULL DEFAULT 0,
                    route_distance_km DECIMAL(10,2) NOT NULL DEFAULT 0,
                    load_rate DECIMAL(8,4) NOT NULL DEFAULT 0,
                    route_text TEXT NULL,
                    INDEX idx_run_id (run_id),
                    INDEX idx_wave_id (wave_id),
                    CONSTRAINT fk_archive_route_run FOREIGN KEY (run_id) REFERENCES {ARCHIVE_TABLE}(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RECOMMENDED_PLAN_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_date DATE NOT NULL,
                    source_run_id VARCHAR(64) NOT NULL,
                    source_type VARCHAR(32) NOT NULL DEFAULT 'archive',
                    similarity_score DECIMAL(10,4) NULL,
                    rank_no INT NOT NULL DEFAULT 0,
                    snapshot_json LONGTEXT NOT NULL,
                    full_snapshot_json LONGTEXT NULL,
                    selected_flag TINYINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_task_date (task_date),
                    INDEX idx_source_run_id (source_run_id),
                    INDEX idx_selected_flag (selected_flag)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                """
                SELECT COUNT(1) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s
                  AND TABLE_NAME=%s
                  AND COLUMN_NAME='full_snapshot_json'
                """,
                (MYSQL_DATABASE, RECOMMENDED_PLAN_TABLE),
            )
            col_exists = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not col_exists:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                cursor.execute(
                    f"ALTER TABLE {RECOMMENDED_PLAN_TABLE} ADD COLUMN full_snapshot_json LONGTEXT NULL"
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {AMAP_DISTANCE_TABLE} (
                    cache_key VARCHAR(255) PRIMARY KEY,
                    distance_km DECIMAL(10,3) NOT NULL DEFAULT 0,
                    duration_minutes DECIMAL(10,3) NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {AMAP_ROUTE_TABLE} (
                    cache_key VARCHAR(255) PRIMARY KEY,
                    polyline MEDIUMTEXT NULL,
                    source VARCHAR(32) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STORE_DISTANCE_TABLE} (
                    from_store_id VARCHAR(64) NOT NULL,
                    to_store_id VARCHAR(64) NOT NULL,
                    distance_km DECIMAL(10,3) NOT NULL DEFAULT 0,
                    duration_minutes DECIMAL(10,3) NOT NULL DEFAULT 0,
                    source VARCHAR(32) NOT NULL DEFAULT 'fallback',
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (from_store_id, to_store_id),
                    INDEX idx_store_distance_to (to_store_id),
                    INDEX idx_store_distance_updated (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RUN_REGION_SCHEME_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    scheme_no VARCHAR(64) NOT NULL,
                    name VARCHAR(128) NOT NULL,
                    enabled TINYINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_scheme_no (scheme_no),
                    INDEX idx_enabled_updated (enabled, updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RUN_REGION_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    scheme_no VARCHAR(64) NOT NULL,
                    region_code VARCHAR(128) NULL,
                    name VARCHAR(128) NOT NULL,
                    polygon_path LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_scheme_no (scheme_no),
                    INDEX idx_name (name),
                    INDEX idx_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RUN_REGION_MEMBER_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    scheme_no VARCHAR(64) NOT NULL,
                    region_code VARCHAR(128) NOT NULL,
                    delivery_date DATE NULL,
                    vehicle_id VARCHAR(64) NULL,
                    store_id VARCHAR(64) NOT NULL,
                    store_name VARCHAR(255) NULL,
                    lng DECIMAL(12,6) NULL,
                    lat DECIMAL(12,6) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_scheme_region_store_day_vehicle (scheme_no, region_code, delivery_date, vehicle_id, store_id),
                    INDEX idx_scheme_region (scheme_no, region_code),
                    INDEX idx_store (store_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='scheme_no'
                """,
                (MYSQL_DATABASE, RUN_REGION_TABLE),
            )
            region_scheme_exists = int((cursor.fetchone() or {}).get("cnt") or 0)
            if not region_scheme_exists:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    f"ALTER TABLE {RUN_REGION_TABLE} ADD COLUMN scheme_no VARCHAR(64) NOT NULL DEFAULT 'default'"
                )
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                cursor.execute(
                    f"ALTER TABLE {RUN_REGION_TABLE} ADD INDEX idx_scheme_no (scheme_no)"
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='region_code'
                """,
                (MYSQL_DATABASE, RUN_REGION_TABLE),
            )
            region_code_exists = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not region_code_exists:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(
                    f"ALTER TABLE {RUN_REGION_TABLE} ADD COLUMN region_code VARCHAR(128) NULL AFTER scheme_no"
                )
            cursor.execute(
                f"""
                INSERT INTO {RUN_REGION_SCHEME_TABLE} (scheme_no, name, enabled)
                VALUES ('default', '榛樿鏂规', 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name)
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_CRED_TABLE} (
                    id TINYINT PRIMARY KEY,
                    pwd_cipher VARCHAR(1024) NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_BATCH_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    batch_id VARCHAR(64) NOT NULL,
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP NULL,
                    mode VARCHAR(16) NOT NULL DEFAULT 'incremental',
                    success_flag TINYINT NOT NULL DEFAULT 0,
                    summary_json LONGTEXT NULL,
                    error_text TEXT NULL,
                    INDEX idx_batch_id (batch_id),
                    INDEX idx_started_at (started_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_RAW_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    batch_id VARCHAR(64) NOT NULL,
                    source_table VARCHAR(64) NOT NULL,
                    business_date DATE NULL,
                    shop_code VARCHAR(64) NULL,
                    plate_no VARCHAR(64) NULL,
                    row_hash VARCHAR(64) NOT NULL,
                    payload_json LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_wms_raw_table_hash (source_table, row_hash),
                    INDEX idx_wms_raw_batch (batch_id),
                    INDEX idx_wms_raw_date (business_date),
                    INDEX idx_wms_raw_shop (shop_code),
                    INDEX idx_wms_raw_plate (plate_no)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_SHOP_TABLE} (
                    shop_code VARCHAR(64) PRIMARY KEY,
                    shop_name VARCHAR(255) NULL,
                    district VARCHAR(128) NULL,
                    lng DECIMAL(12,6) NULL,
                    lat DECIMAL(12,6) NULL,
                    latest_business_date DATE NULL,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'C_SHOP_MAIN',
                    payload_json LONGTEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_VEHICLE_TABLE} (
                    plate_no VARCHAR(64) PRIMARY KEY,
                    driver_name VARCHAR(128) NULL,
                    latest_business_date DATE NULL,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'C_Route_Driver',
                    payload_json LONGTEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_CARGO_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    business_date DATE NULL,
                    shop_code VARCHAR(64) NOT NULL,
                    ambient_qty DECIMAL(14,3) NOT NULL DEFAULT 0,
                    cold_qty DECIMAL(14,3) NOT NULL DEFAULT 0,
                    frozen_qty DECIMAL(14,3) NOT NULL DEFAULT 0,
                    total_boxes DECIMAL(14,3) NOT NULL DEFAULT 0,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'CargoQTY',
                    row_hash VARCHAR(64) NOT NULL,
                    payload_json LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_wms_cargo_hash (row_hash),
                    INDEX idx_wms_cargo_shop_date (shop_code, business_date),
                    INDEX idx_wms_cargo_date (business_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_CARGO_RAW_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    shop_code VARCHAR(64) NOT NULL,
                    business_date DATE NULL,
                    rpcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    rcase DECIMAL(14,3) NOT NULL DEFAULT 0,
                    bpcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    bpaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    apcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    apaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    rpaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'CargoQTY',
                    row_hash VARCHAR(64) NOT NULL,
                    payload_json LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_wms_cargo_raw_hash (row_hash),
                    INDEX idx_wms_cargo_raw_shop_date (shop_code, business_date),
                    INDEX idx_wms_cargo_raw_date (business_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_CARLOAD_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    business_date DATE NULL,
                    plate_no VARCHAR(64) NOT NULL,
                    load_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'CarLoad',
                    row_hash VARCHAR(64) NOT NULL,
                    payload_json LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_wms_carload_hash (row_hash),
                    INDEX idx_wms_carload_plate_date (plate_no, business_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {WMS_ARRIVAL_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    business_date DATE NULL,
                    shop_code VARCHAR(64) NULL,
                    plate_no VARCHAR(64) NULL,
                    route_id VARCHAR(64) NULL,
                    arrival_time VARCHAR(32) NULL,
                    source_table VARCHAR(64) NOT NULL DEFAULT 'arrivaltime',
                    row_hash VARCHAR(64) NOT NULL,
                    payload_json LONGTEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_wms_arrival_hash (row_hash),
                    INDEX idx_wms_arrival_date (business_date),
                    INDEX idx_wms_arrival_shop (shop_code),
                    INDEX idx_wms_arrival_plate (plate_no)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_LOAD_FACTOR_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    vehicle_key VARCHAR(128) NOT NULL,
                    load_type_key VARCHAR(128) NOT NULL,
                    rpcs_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    rcase_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    bpcs_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    bpaper_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    apcs_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    apaper_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    rpaper_factor DECIMAL(14,6) NOT NULL DEFAULT 0,
                    source_file VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_local_factor_key (vehicle_key, load_type_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LOCAL_LOAD_TABLE} (
                    shop_code VARCHAR(64) PRIMARY KEY,
                    rpcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    rcase DECIMAL(14,3) NOT NULL DEFAULT 0,
                    bpcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    bpaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    apcs DECIMAL(14,3) NOT NULL DEFAULT 0,
                    apaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    rpaper DECIMAL(14,3) NOT NULL DEFAULT 0,
                    wave1_total_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                    wave2_total_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                    total_load DECIMAL(14,3) NOT NULL DEFAULT 0,
                    source_file VARCHAR(255) NULL,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_local_load_updated_at (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_FAILURE_LOG_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    batch_id VARCHAR(100) NOT NULL,
                    wave_id VARCHAR(5) NULL,
                    shop_code VARCHAR(20) NOT NULL,
                    target_vehicle VARCHAR(20) NULL,
                    failure_type VARCHAR(50) NULL,
                    current_time_text VARCHAR(10) NULL,
                    expected_arrival VARCHAR(10) NULL,
                    latest_allowed VARCHAR(10) NULL,
                    late_minutes INT NOT NULL DEFAULT 0,
                    route_stops LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_fail_batch (batch_id),
                    INDEX idx_sim_fail_wave (wave_id),
                    INDEX idx_sim_fail_shop (shop_code),
                    INDEX idx_sim_fail_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_TASK_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    batch_id VARCHAR(100) NOT NULL,
                    target VARCHAR(32) NOT NULL DEFAULT 'min_vehicles',
                    status VARCHAR(20) NOT NULL DEFAULT 'running',
                    error_message TEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at DATETIME NULL,
                    UNIQUE KEY uk_sim_task_id (task_id),
                    INDEX idx_sim_task_batch (batch_id),
                    INDEX idx_sim_task_status (status),
                    INDEX idx_sim_task_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_STORE_ATTEMPT_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    batch_id VARCHAR(100) NOT NULL,
                    wave_id VARCHAR(5) NOT NULL,
                    shop_code VARCHAR(32) NOT NULL,
                    shop_name VARCHAR(255) NULL,
                    final_status VARCHAR(20) NOT NULL DEFAULT 'failed',
                    best_failure_type VARCHAR(50) NULL,
                    best_violation_type VARCHAR(64) NULL,
                    trial_count INT NOT NULL DEFAULT 0,
                    success_count INT NOT NULL DEFAULT 0,
                    failed_count INT NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_attempt_task (task_id),
                    INDEX idx_sim_attempt_batch (batch_id),
                    INDEX idx_sim_attempt_wave (wave_id),
                    INDEX idx_sim_attempt_shop (shop_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_INSERT_TRIAL_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    batch_id VARCHAR(100) NOT NULL,
                    wave_id VARCHAR(5) NOT NULL,
                    shop_code VARCHAR(32) NOT NULL,
                    shop_name VARCHAR(255) NULL,
                    trial_no INT NOT NULL DEFAULT 0,
                    target_vehicle VARCHAR(64) NULL,
                    route_index INT NULL,
                    insert_pos INT NULL,
                    result_status VARCHAR(20) NOT NULL DEFAULT 'failed',
                    failure_type VARCHAR(50) NULL,
                    violation_type VARCHAR(64) NULL,
                    expected_arrival VARCHAR(32) NULL,
                    latest_allowed VARCHAR(32) NULL,
                    late_minutes INT NOT NULL DEFAULT 0,
                    route_distance_km DECIMAL(12,3) NULL,
                    route_load DECIMAL(12,3) NULL,
                    route_before LONGTEXT NULL,
                    route_after LONGTEXT NULL,
                    violation_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_trial_task (task_id),
                    INDEX idx_sim_trial_batch (batch_id),
                    INDEX idx_sim_trial_wave (wave_id),
                    INDEX idx_sim_trial_shop (shop_code),
                    INDEX idx_sim_trial_status (result_status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_TASK_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    source_batch_id VARCHAR(100) NOT NULL,
                    task_date DATE NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'running',
                    shop_code VARCHAR(32) NOT NULL,
                    shop_name VARCHAR(255) NULL,
                    original_wave VARCHAR(16) NULL,
                    simulated_wave VARCHAR(16) NULL,
                    original_time VARCHAR(32) NULL,
                    simulated_time VARCHAR(32) NULL,
                    target VARCHAR(32) NOT NULL DEFAULT 'min_vehicles',
                    algorithm_key VARCHAR(32) NULL,
                    affected_wave VARCHAR(16) NULL,
                    vehicle_count_before INT NOT NULL DEFAULT 0,
                    vehicle_count_after INT NOT NULL DEFAULT 0,
                    mileage_before DECIMAL(14,3) NOT NULL DEFAULT 0,
                    mileage_after DECIMAL(14,3) NOT NULL DEFAULT 0,
                    summary_text TEXT NULL,
                    payload_json LONGTEXT NULL,
                    result_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at DATETIME NULL,
                    UNIQUE KEY uk_sim_single_task_id (task_id),
                    INDEX idx_sim_single_task_batch (source_batch_id),
                    INDEX idx_sim_single_task_shop (shop_code),
                    INDEX idx_sim_single_task_status (status),
                    INDEX idx_sim_single_task_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_SNAPSHOT_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    snapshot_type VARCHAR(20) NOT NULL,
                    wave_id VARCHAR(16) NULL,
                    candidate_store_count INT NOT NULL DEFAULT 0,
                    assigned_store_count INT NOT NULL DEFAULT 0,
                    pending_store_count INT NOT NULL DEFAULT 0,
                    vehicle_count INT NOT NULL DEFAULT 0,
                    total_mileage DECIMAL(14,3) NOT NULL DEFAULT 0,
                    solve_status VARCHAR(20) NOT NULL DEFAULT 'success',
                    solver_source VARCHAR(32) NULL,
                    raw_result_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_snapshot_task (task_id),
                    INDEX idx_sim_single_snapshot_type (snapshot_type),
                    INDEX idx_sim_single_snapshot_wave (wave_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_ROUTE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    snapshot_type VARCHAR(20) NOT NULL,
                    wave_id VARCHAR(16) NULL,
                    vehicle_no VARCHAR(64) NULL,
                    trip_no INT NOT NULL DEFAULT 1,
                    route_seq INT NOT NULL DEFAULT 0,
                    store_count INT NOT NULL DEFAULT 0,
                    route_distance_km DECIMAL(14,3) NOT NULL DEFAULT 0,
                    solver_load_value DECIMAL(14,6) NULL,
                    display_load_percent DECIMAL(14,3) NULL,
                    start_time VARCHAR(32) NULL,
                    end_time VARCHAR(32) NULL,
                    route_status VARCHAR(20) NOT NULL DEFAULT 'active',
                    route_payload_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_route_task (task_id),
                    INDEX idx_sim_single_route_type (snapshot_type),
                    INDEX idx_sim_single_route_wave (wave_id),
                    INDEX idx_sim_single_route_vehicle (vehicle_no)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_ROUTE_STOP_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    snapshot_type VARCHAR(20) NOT NULL,
                    wave_id VARCHAR(16) NULL,
                    vehicle_no VARCHAR(64) NULL,
                    trip_no INT NOT NULL DEFAULT 1,
                    stop_seq INT NOT NULL DEFAULT 0,
                    shop_code VARCHAR(32) NOT NULL,
                    shop_name VARCHAR(255) NULL,
                    expected_arrival_time VARCHAR(32) NULL,
                    scheduled_arrival_time VARCHAR(32) NULL,
                    scheduled_leave_time VARCHAR(32) NULL,
                    boxes DECIMAL(14,3) NULL,
                    volume DECIMAL(14,6) NULL,
                    load_after_stop DECIMAL(14,6) NULL,
                    is_target_shop TINYINT(1) NOT NULL DEFAULT 0,
                    arrival_source VARCHAR(32) NULL,
                    stop_payload_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_stop_task (task_id),
                    INDEX idx_sim_single_stop_type (snapshot_type),
                    INDEX idx_sim_single_stop_wave (wave_id),
                    INDEX idx_sim_single_stop_vehicle (vehicle_no),
                    INDEX idx_sim_single_stop_shop (shop_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_EVENT_LOG_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    stage VARCHAR(64) NULL,
                    event_type VARCHAR(64) NOT NULL,
                    wave_id VARCHAR(16) NULL,
                    vehicle_no VARCHAR(64) NULL,
                    shop_code VARCHAR(32) NULL,
                    message TEXT NULL,
                    detail_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_event_task (task_id),
                    INDEX idx_sim_single_event_type (event_type),
                    INDEX idx_sim_single_event_wave (wave_id),
                    INDEX idx_sim_single_event_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    snapshot_type VARCHAR(20) NOT NULL,
                    wave_id VARCHAR(16) NULL,
                    vehicle_no VARCHAR(64) NULL,
                    trip_no INT NOT NULL DEFAULT 1,
                    route_distance_km DECIMAL(14,3) NOT NULL DEFAULT 0,
                    route_load_value DECIMAL(14,6) NULL,
                    store_count INT NOT NULL DEFAULT 0,
                    route_payload_json LONGTEXT NULL,
                    confirmed_by VARCHAR(64) NULL,
                    confirmed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_confirm_task (task_id),
                    INDEX idx_sim_single_confirm_wave (wave_id),
                    INDEX idx_sim_single_confirm_vehicle (vehicle_no)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_REMAINING_STATE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    parent_state_id BIGINT NULL,
                    state_seq INT NOT NULL DEFAULT 0,
                    wave_id VARCHAR(16) NULL,
                    remaining_vehicle_json LONGTEXT NULL,
                    remaining_store_json LONGTEXT NULL,
                    remaining_cargo_json LONGTEXT NULL,
                    remaining_dist_matrix_ref VARCHAR(255) NULL,
                    locked_route_ids_json LONGTEXT NULL,
                    state_summary TEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_state_task (task_id),
                    INDEX idx_sim_single_state_parent (parent_state_id),
                    INDEX idx_sim_single_state_wave (wave_id),
                    INDEX idx_sim_single_state_seq (state_seq)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SIMULATION_SINGLE_STATE_TRANSITION_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    task_id VARCHAR(128) NOT NULL,
                    from_state_id BIGINT NULL,
                    to_state_id BIGINT NULL,
                    action_type VARCHAR(64) NOT NULL,
                    action_payload_json LONGTEXT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sim_single_transition_task (task_id),
                    INDEX idx_sim_single_transition_from (from_state_id),
                    INDEX idx_sim_single_transition_to (to_state_id),
                    INDEX idx_sim_single_transition_action (action_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # 鑰佺幆澧冨吋瀹癸細琛ㄥ凡瀛樺湪鏃惰ˉ鍒?
            try:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(f"ALTER TABLE {LOCAL_LOAD_TABLE} ADD COLUMN wave1_total_load DECIMAL(14,6) NOT NULL DEFAULT 0")
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            except Exception:
                pass
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            try:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(f"ALTER TABLE {LOCAL_LOAD_TABLE} ADD COLUMN wave2_total_load DECIMAL(14,6) NOT NULL DEFAULT 0")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            except Exception:
                pass
    _archive_tables_ready = True


def recommended_plan_list(payload):
    ensure_archive_tables()
    task_date = str(payload.get("taskDate") or payload.get("task_date") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not task_date:
        task_date = time.strftime("%Y-%m-%d")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"DELETE FROM {RECOMMENDED_PLAN_TABLE} WHERE task_date=%s",
                (task_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT id, payload_json, created_at, best_score, result_count, strategy, goal
                FROM {ARCHIVE_TABLE}
                WHERE payload_json IS NOT NULL AND payload_json <> ''
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 3
                """
            )
            archive_rows = cursor.fetchall() or []
            candidate_rows = []
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for rank_no, row in enumerate(archive_rows, start=1):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                try:
                    snapshot = json.loads(row.get("payload_json") or "{}")
                except Exception:
                    snapshot = {}
                source_run_id = str(row.get("id") or snapshot.get("id") or "")
                candidate_snapshot = {
                    "run_id": source_run_id,
                    "created_at": str(snapshot.get("createdAt") or row.get("created_at") or ""),
                    "result_count": int(snapshot.get("resultCount") or row.get("result_count") or 0),
                    "best_score": snapshot.get("bestScore") if snapshot.get("bestScore") is not None else row.get("best_score"),
                    "strategy": str(snapshot.get("strategy") or row.get("strategy") or ""),
                    "goal": str(snapshot.get("goal") or row.get("goal") or ""),
                }
                full_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                candidate_rows.append(
                    (
                        task_date,
                        source_run_id,
                        "archive",
                        None,
                        rank_no,
                        json.dumps(candidate_snapshot, ensure_ascii=False),
                        full_snapshot_json,
                        0,
                    )
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if candidate_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {RECOMMENDED_PLAN_TABLE}
                    (task_date, source_run_id, source_type, similarity_score, rank_no, snapshot_json, full_snapshot_json, selected_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    candidate_rows,
                )
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT id, task_date, source_run_id, source_type, similarity_score, rank_no, snapshot_json, selected_flag, created_at, updated_at
                FROM {RECOMMENDED_PLAN_TABLE}
                WHERE task_date=%s
                ORDER BY selected_flag DESC, rank_no ASC, id ASC
                """,
                (task_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    items = []
    for row in rows:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            snapshot = json.loads(row.get("snapshot_json") or "{}")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            snapshot = {}
        items.append(
            {
                "id": int(row.get("id") or 0),
                "taskDate": str(row.get("task_date") or task_date),
                "sourceRunId": str(row.get("source_run_id") or ""),
                "sourceType": str(row.get("source_type") or "archive"),
                "similarityScore": row.get("similarity_score"),
                "rankNo": int(row.get("rank_no") or 0),
                "snapshot": snapshot,
                "fullSnapshot": json.loads(row.get("full_snapshot_json") or "{}") if row.get("full_snapshot_json") else snapshot,
                "selectedFlag": int(row.get("selected_flag") or 0),
                "createdAt": row.get("created_at"),
                "updatedAt": row.get("updated_at"),
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return {"taskDate": task_date, "items": items}


def recommended_plan_current(payload):
    ensure_archive_tables()
    task_date = str(payload.get("taskDate") or payload.get("task_date") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_date:
        task_date = time.strftime("%Y-%m-%d")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                SELECT id, source_run_id, snapshot_json, full_snapshot_json, created_at
                FROM {RECOMMENDED_PLAN_TABLE}
                WHERE task_date=%s AND selected_flag=1
                ORDER BY rank_no ASC, id ASC
                LIMIT 1
                """,
                (task_date,),
            )
            row = cursor.fetchone()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not row:
        return {"taskDate": task_date, "selected": None}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        snapshot = json.loads(row.get("snapshot_json") or "{}")
    except Exception:
        snapshot = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    try:
        full_snapshot = json.loads(row.get("full_snapshot_json") or "{}")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception:
        full_snapshot = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "taskDate": task_date,
        "selected": {
            "id": int(row.get("id") or 0),
            "sourceRunId": str(row.get("source_run_id") or ""),
            "snapshot": snapshot,
            "fullSnapshot": full_snapshot,
            "createdAt": row.get("created_at"),
        },
    }


def _extract_recommended_solution_vehicles(full_snapshot):
    results = full_snapshot.get("results") if isinstance(full_snapshot, dict) else None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(results, list) or not results:
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return []
    active_key = str(full_snapshot.get("activeResultKey") or "").strip()
    chosen = None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if active_key:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for result in results:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if isinstance(result, dict) and str(result.get("key") or "").strip() == active_key:
                chosen = result
                break
    if chosen is None:
        chosen = results[0] if isinstance(results[0], dict) else None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not isinstance(chosen, dict):
        return []
    solution = chosen.get("solution") or []
    wave_groups = solution if isinstance(solution, list) else []
    vehicles = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for wave in wave_groups:
        plans = wave if isinstance(wave, list) else [wave]
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for plan in plans:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(plan, dict):
                continue
            vehicle = plan.get("vehicle") or {}
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if not isinstance(vehicle, dict):
                vehicle = {}
            routes = []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for trip in (plan.get("trips") or []):
                if not isinstance(trip, dict):
                    continue
                route = trip.get("route") or []
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if isinstance(route, list) and route:
                    routes.append([str(store_id) for store_id in route if str(store_id).strip()])
            vehicles.append(
                {
                    "plateNo": str(vehicle.get("plateNo") or ""),
                    "routes": routes,
                    "priorRegularDistance": float(plan.get("priorRegularDistance") or 0.0),
                    "priorWaveCount": int(plan.get("priorWaveCount") or 0),
                    "earliestDepartureMin": int(plan.get("earliestDepartureMin") or 0),
                }
            )
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return vehicles


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _validate_solver_payload_fields(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload_must_be_dict")
    algorithm_key = str(payload.get("algorithmKey") or "").strip().lower()
    is_vehicle_algorithm = algorithm_key == "vehicle"
    scenario = payload.get("scenario")
    wave = payload.get("wave")
    stores = payload.get("stores")
    vehicles = payload.get("vehicles")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(scenario, dict):
        raise ValueError("scenario_required")
    if not isinstance(wave, dict):
        raise ValueError("wave_required")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not is_vehicle_algorithm and not isinstance(stores, list):
        raise ValueError("stores_required")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not isinstance(vehicles, list) or not vehicles:
        raise ValueError("vehicles_required")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if (not is_vehicle_algorithm) and (not stores):
        raise ValueError("stores_empty")
    required_wave_keys = ("waveId", "startMin", "endMin", "singleWave", "endMode", "isNightWave", "relaxEnd")
    for key in required_wave_keys:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if key not in wave:
            raise ValueError(f"wave_field_missing:{key}")
    if "dispatchStartMin" not in scenario:
        raise ValueError("scenario_field_missing:dispatchStartMin")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if "concentrateLate" not in scenario:
        raise ValueError("scenario_field_missing:concentrateLate")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return payload


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _normalize_strategy_config(payload):
    default_config = {
        "deliveryMode": "singleDailyWave",
        "optimizeGoal": "balanced",
        "allowLate": True,
        "latenessStrength": "medium",
        "allowMultiTrip": True,
        "vehicleBusyCostWeight": 50,
        "dualWaveWeight": 50,
        "crossRegionPenaltyWeight": 50,
        "loadWeight": 50,
        "distanceWeight": 50,
        "waveDelayPenalty": 50,
        "lateRouteStrength": "medium",
        "deliveryDifficultyMode": "time",
        "difficultyTier1Unlimited": False,
        "difficultyTier1Limit": 1,
        "difficultyTier2Unlimited": False,
        "difficultyTier2Limit": 2,
        "difficultyTier3Unlimited": True,
        "difficultyTier3Limit": 0,
        "difficultyScoreLimit": 8,
        "maxSolveCapacity": 1.2,
        "defaultSpeedKmh": 38,
        "w3SpeedKmh": 48,
        "w1w2RelayMaxKm": 240,
        "w3OneWayMaxKm": 260,
        "w3ExcludePriorVehicles": True,
    }
    source = payload.get("strategyConfig")
    if not isinstance(source, dict):
        source = {}

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def clamp_int(value, default_value):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        try:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return max(0, min(100, int(float(value))))
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return int(default_value)

    def clamp_float(value, default_value, min_value, max_value):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        try:
            parsed = float(value)
        except Exception:
            parsed = float(default_value)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return max(float(min_value), min(float(max_value), parsed))

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def clamp_int_range(value, default_value, min_value, max_value):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            parsed = int(float(value))
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        except Exception:
            parsed = int(default_value)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return max(int(min_value), min(int(max_value), parsed))

    def parse_bool(value, default_value):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if value is None:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return bool(default_value)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1", "yes", "on"}:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return True
            if text in {"false", "0", "no", "off"}:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                return False
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return bool(value)

    cfg = {
        "deliveryMode": str(source.get("deliveryMode") or default_config["deliveryMode"]).strip() or default_config["deliveryMode"],
        "optimizeGoal": str(source.get("optimizeGoal") or default_config["optimizeGoal"]).strip() or default_config["optimizeGoal"],
        "allowLate": bool(source.get("allowLate", default_config["allowLate"])),
        "latenessStrength": str(source.get("latenessStrength") or default_config["latenessStrength"]).strip() or default_config["latenessStrength"],
        "allowMultiTrip": bool(source.get("allowMultiTrip", default_config["allowMultiTrip"])),
        "vehicleBusyCostWeight": clamp_int(source.get("vehicleBusyCostWeight"), default_config["vehicleBusyCostWeight"]),
        "dualWaveWeight": clamp_int(source.get("dualWaveWeight"), default_config["dualWaveWeight"]),
        "crossRegionPenaltyWeight": clamp_int(source.get("crossRegionPenaltyWeight"), default_config["crossRegionPenaltyWeight"]),
        "loadWeight": clamp_int(source.get("loadWeight"), default_config["loadWeight"]),
        "distanceWeight": clamp_int(source.get("distanceWeight"), default_config["distanceWeight"]),
        "waveDelayPenalty": clamp_int(source.get("waveDelayPenalty"), default_config["waveDelayPenalty"]),
        "lateRouteStrength": str(source.get("lateRouteStrength") or default_config["lateRouteStrength"]).strip() or default_config["lateRouteStrength"],
        "deliveryDifficultyMode": str(source.get("deliveryDifficultyMode") or default_config["deliveryDifficultyMode"]).strip() or default_config["deliveryDifficultyMode"],
        "difficultyTier1Unlimited": parse_bool(source.get("difficultyTier1Unlimited"), default_config["difficultyTier1Unlimited"]),
        "difficultyTier1Limit": clamp_int_range(source.get("difficultyTier1Limit"), default_config["difficultyTier1Limit"], 0, 999),
        "difficultyTier2Unlimited": parse_bool(source.get("difficultyTier2Unlimited"), default_config["difficultyTier2Unlimited"]),
        "difficultyTier2Limit": clamp_int_range(source.get("difficultyTier2Limit"), default_config["difficultyTier2Limit"], 0, 999),
        "difficultyTier3Unlimited": parse_bool(source.get("difficultyTier3Unlimited"), default_config["difficultyTier3Unlimited"]),
        "difficultyTier3Limit": clamp_int_range(source.get("difficultyTier3Limit"), default_config["difficultyTier3Limit"], 0, 999),
        "difficultyScoreLimit": clamp_int_range(source.get("difficultyScoreLimit"), default_config["difficultyScoreLimit"], 1, 999),
        "maxSolveCapacity": clamp_float(source.get("maxSolveCapacity"), default_config["maxSolveCapacity"], 0.1, 9.99),
        "defaultSpeedKmh": clamp_float(source.get("defaultSpeedKmh"), default_config["defaultSpeedKmh"], 1.0, 120.0),
        "w3SpeedKmh": clamp_float(source.get("w3SpeedKmh"), default_config["w3SpeedKmh"], 1.0, 120.0),
        "w1w2RelayMaxKm": clamp_float(source.get("w1w2RelayMaxKm"), default_config["w1w2RelayMaxKm"], 1.0, 2000.0),
        "w3OneWayMaxKm": clamp_float(source.get("w3OneWayMaxKm"), default_config["w3OneWayMaxKm"], 1.0, 2000.0),
        "w3ExcludePriorVehicles": parse_bool(source.get("w3ExcludePriorVehicles"), default_config["w3ExcludePriorVehicles"]),
        # 鍏煎鏃ц姹傦紝闈炲綋鍓嶄富閫昏緫
        "difficultyCountLimitLegacy": clamp_int_range(source.get("difficultyCountLimit"), 3, 0, 999),
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if cfg["optimizeGoal"] not in {"load", "balanced", "distance"}:
        cfg["optimizeGoal"] = default_config["optimizeGoal"]
    if cfg["deliveryMode"] not in {"singleDailyWave", "doubleDailyWave"}:
        cfg["deliveryMode"] = default_config["deliveryMode"]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if cfg["latenessStrength"] not in {"low", "medium", "high"}:
        cfg["latenessStrength"] = default_config["latenessStrength"]
    if cfg["lateRouteStrength"] not in {"low", "medium", "high"}:
        cfg["lateRouteStrength"] = default_config["lateRouteStrength"]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if cfg["deliveryDifficultyMode"] not in {"time", "count", "score"}:
        cfg["deliveryDifficultyMode"] = default_config["deliveryDifficultyMode"]

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if cfg["optimizeGoal"] == "load":
        preset = {"loadWeight": 70, "distanceWeight": 30}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    elif cfg["optimizeGoal"] == "distance":
        preset = {"loadWeight": 30, "distanceWeight": 70}
    else:
        preset = {"loadWeight": 50, "distanceWeight": 50}
    cfg["optimizeGoalPreset"] = preset
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if cfg["deliveryMode"] == "doubleDailyWave":
        cfg["dualWaveActive"] = True
        cfg["dualWaveCoeff"] = 1.0
        cfg["crossRegionToleranceLevel"] = "low"
    else:
        cfg["dualWaveActive"] = False
        cfg["dualWaveCoeff"] = 0.0
        cfg["crossRegionToleranceLevel"] = "high"
    payload["strategyConfig"] = cfg
    payload["strategyConfigResolved"] = {
        "deliveryMode": cfg["deliveryMode"],
        "allowLate": cfg["allowLate"],
        "latenessStrength": cfg["latenessStrength"],
        "allowMultiTrip": cfg["allowMultiTrip"],
        "vehicleBusyCostWeight": cfg["vehicleBusyCostWeight"],
        "loadWeight": cfg["loadWeight"],
        "distanceWeight": cfg["distanceWeight"],
        "waveDelayPenalty": cfg["waveDelayPenalty"],
        "lateRouteStrength": cfg["lateRouteStrength"],
        # 棰勭暀椤癸細鏈疆涓嶈繘鍏ョ洰鏍囧嚱鏁?
        "dualWaveWeight": cfg["dualWaveWeight"],
        "crossRegionPenaltyWeight": cfg["crossRegionPenaltyWeight"],
        "dualWaveActive": cfg["dualWaveActive"],
        "dualWaveCoeff": cfg["dualWaveCoeff"],
        "crossRegionToleranceLevel": cfg["crossRegionToleranceLevel"],
        "deliveryDifficultyMode": cfg["deliveryDifficultyMode"],
        "difficultyTier1Unlimited": cfg["difficultyTier1Unlimited"],
        "difficultyTier1Limit": cfg["difficultyTier1Limit"],
        "difficultyTier2Unlimited": cfg["difficultyTier2Unlimited"],
        "difficultyTier2Limit": cfg["difficultyTier2Limit"],
        "difficultyTier3Unlimited": cfg["difficultyTier3Unlimited"],
        "difficultyTier3Limit": cfg["difficultyTier3Limit"],
        "difficultyScoreLimit": cfg["difficultyScoreLimit"],
        "difficultyCountLimitLegacy": cfg["difficultyCountLimitLegacy"],
        "maxSolveCapacity": cfg["maxSolveCapacity"],
        "defaultSpeedKmh": cfg["defaultSpeedKmh"],
        "w3SpeedKmh": cfg["w3SpeedKmh"],
        "w1w2RelayMaxKm": cfg["w1w2RelayMaxKm"],
        "w3OneWayMaxKm": cfg["w3OneWayMaxKm"],
        "w3ExcludePriorVehicles": cfg["w3ExcludePriorVehicles"],
    }
    scenario = payload.get("scenario")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(scenario, dict):
        scenario["strategyConfigResolved"] = payload["strategyConfigResolved"]
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return payload


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _apply_operational_strategy_overrides(payload):
    if not isinstance(payload, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return payload
    cfg = payload.get("strategyConfigResolved")
    if not isinstance(cfg, dict):
        cfg = payload.get("strategyConfig") if isinstance(payload.get("strategyConfig"), dict) else {}
    wave = payload.get("wave") if isinstance(payload.get("wave"), dict) else {}
    wave_id = str(wave.get("waveId") or "").strip().upper()
    max_capacity = _to_float_safe(cfg.get("maxSolveCapacity"), 1.2)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if max_capacity <= 0:
        max_capacity = 1.2
    default_speed = _to_float_safe(cfg.get("defaultSpeedKmh"), 38.0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if default_speed <= 0:
        default_speed = 38.0
    w3_speed = _to_float_safe(cfg.get("w3SpeedKmh"), 48.0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if w3_speed <= 0:
        w3_speed = 48.0
    solve_speed = w3_speed if wave_id == "W3" else default_speed
    vehicles = payload.get("vehicles")
    if isinstance(vehicles, list):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for item in vehicles:
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if not isinstance(item, dict):
                continue
            item["capacity"] = max_capacity
            item["solveCapacity"] = max_capacity
            item["speed"] = solve_speed
    scenario = payload.get("scenario")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(scenario, dict):
        scenario["strategyConfigResolved"] = cfg
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return payload


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def apply_recommended_plan_warm_start(payload):
    if not isinstance(payload, dict) or not payload.get("useRecommendedPlan"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return payload
    task_date = str(payload.get("recommendedPlanTaskDate") or payload.get("taskDate") or "").strip()
    if not task_date:
        task_date = time.strftime("%Y-%m-%d")
    print(f"[RECOMMENDED] useRecommendedPlan=true taskDate={task_date}")
    selected = recommended_plan_current({"taskDate": task_date}).get("selected")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not selected:
        print("[RECOMMENDED] no selected plan found, fallback to default initialization")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return payload
    full_snapshot = selected.get("fullSnapshot") or {}
    print(f"[RECOMMENDED] selected sourceRunId={selected.get('sourceRunId') or ''}")
    recommended_vehicles = _extract_recommended_solution_vehicles(full_snapshot)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not recommended_vehicles:
        print("[RECOMMENDED] full_snapshot_json parse failed or solution missing, fallback to default initialization")
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return payload
    existing_vehicles = payload.get("vehicles") or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(existing_vehicles, list) or not existing_vehicles:
        print("[RECOMMENDED] vehicles missing, fallback to default initialization")
        return payload
    route_map = {item.get("plateNo"): item for item in recommended_vehicles if isinstance(item, dict)}
    updated_vehicles = []
    injected_count = 0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for vehicle in existing_vehicles:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(vehicle, dict):
            continue
        plate_no = str(vehicle.get("plateNo") or "")
        seed = route_map.get(plate_no)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if seed:
            updated = dict(vehicle)
            updated["routes"] = seed.get("routes") or []
            updated["priorRegularDistance"] = seed.get("priorRegularDistance", updated.get("priorRegularDistance", 0))
            updated["priorWaveCount"] = seed.get("priorWaveCount", updated.get("priorWaveCount", 0))
            updated["earliestDepartureMin"] = seed.get("earliestDepartureMin", updated.get("earliestDepartureMin", 0))
            updated_vehicles.append(updated)
            injected_count += len(updated["routes"])
        else:
            updated_vehicles.append(vehicle)
    payload["vehicles"] = updated_vehicles
    payload["recommendedPlanSourceRunId"] = str(selected.get("sourceRunId") or "")
    payload["recommendedPlanApplied"] = True
    print(f"[RECOMMENDED] solution extracted ok, injected_routes={injected_count}")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return payload


def recommended_plan_select(payload):
    ensure_archive_tables()
    task_date = str(payload.get("taskDate") or payload.get("task_date") or "").strip()
    candidate_id = int(payload.get("candidateId") or payload.get("candidate_id") or 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not task_date or candidate_id <= 0:
        raise ValueError("missing_task_date_or_candidate_id")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {RECOMMENDED_PLAN_TABLE}
                SET selected_flag=CASE WHEN id=%s THEN 1 ELSE 0 END
                WHERE task_date=%s
                """,
                (candidate_id, task_date),
            )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"ok": True}


def extract_date_key(value):
    text = str(value or "")
    matched = re.search(r"(\d{4})[\/\-骞碷(\d{1,2})[\/\-鏈圿(\d{1,2})", text)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not matched:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return time.strftime("%Y-%m-%d")
    yyyy = matched.group(1)
    mm = matched.group(2).zfill(2)
    dd = matched.group(3).zfill(2)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return f"{yyyy}-{mm}-{dd}"


def flatten_route_rows(snapshot):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    rows = []
    if not isinstance(snapshot, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return rows
    
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for result in (snapshot.get("results") or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not isinstance(result, dict):
            continue
        result_key = str(result.get("key") or "")
        result_label = str(result.get("label") or "")
        solution = result.get("solution") or []
        wave_groups = solution if isinstance(solution, list) else []
        for wave in wave_groups:
            plans = wave if isinstance(wave, list) else [wave]
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for plan in plans:
                if not isinstance(plan, dict):
                    continue
                vehicle = plan.get("vehicle")
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if isinstance(vehicle, dict):
                    plate_no = str(vehicle.get("plateNo") or "")
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    plate_no = ""
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for index, trip in enumerate(plan.get("trips") or []):
                    if not isinstance(trip, dict):
                        continue
                    route = [str(store_id) for store_id in (trip.get("route") or [])]
                    rows.append(
                        {
                            "result_key": result_key,
                            "result_label": result_label,
                            "wave_id": str(trip.get("waveId") or plan.get("waveId") or ""),
                            "vehicle_plate": plate_no,
                            "trip_no": index + 1,
                            "store_count": len(route),
                            "route_distance_km": float(
                                trip.get("routeDistanceKm")
                                or trip.get("roundTripKm")
                                or trip.get("distanceKm")
                                or 0.0
                            ),
                            "load_rate": float(trip.get("loadRate") or 0.0),
                            "route_text": " -> ".join(route),
                        }
                    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return rows


# 鎶婁竴娆″畬鏁磋皟搴︾粨鏋滀繚瀛樹负鈥滄壒娆″綊妗ｂ€濄€?
# 鍓嶅彴淇濆瓨鏂规銆佹帹婕旇鍙栧熀鍑嗘壒娆°€佹帹鑽愭柟妗堟娊鏍烽兘渚濊禆褰掓。缁撴灉銆?
def archive_save(payload):
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if isinstance(payload, list):
        snapshot = payload[0] if payload else {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    else:
        snapshot = payload.get("snapshot") or payload
    
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(snapshot, dict):
        snapshot = {}
    
    run_id = str(snapshot.get("id") or "").strip()
    if not run_id:
        raise ValueError("missing_archive_id")
    created_at = str(snapshot.get("createdAt") or time.strftime("%Y/%m/%d %H:%M:%S"))
    created_date = extract_date_key(created_at)
    strategy = str(snapshot.get("strategy") or "quick")
    goal = str(snapshot.get("goal") or "balanced")
    active_result_key = str(snapshot.get("activeResultKey") or "")
    best_score = float(snapshot.get("bestScore") or 0.0)
    result_count = int(snapshot.get("resultCount") or 0)
    algorithms = str(snapshot.get("algorithms") or "")
    payload_json = json.dumps(snapshot, ensure_ascii=False)
    route_rows = flatten_route_rows(snapshot)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {ARCHIVE_TABLE}
                (id, created_at, created_date, strategy, goal, active_result_key, best_score, result_count, algorithms, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    created_at = VALUES(created_at),
                    created_date = VALUES(created_date),
                    strategy = VALUES(strategy),
                    goal = VALUES(goal),
                    active_result_key = VALUES(active_result_key),
                    best_score = VALUES(best_score),
                    result_count = VALUES(result_count),
                    algorithms = VALUES(algorithms),
                    payload_json = VALUES(payload_json)
                """,
                (
                    run_id,
                    created_at,
                    created_date,
                    strategy,
                    goal,
                    active_result_key,
                    best_score,
                    result_count,
                    algorithms,
                    payload_json,
                ),
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {ARCHIVE_ROUTE_TABLE} WHERE run_id=%s", (run_id,))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if route_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {ARCHIVE_ROUTE_TABLE}
                    (run_id, result_key, result_label, wave_id, vehicle_plate, trip_no, store_count, route_distance_km, load_rate, route_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            run_id,
                            row["result_key"],
                            row["result_label"],
                            row["wave_id"],
                            row["vehicle_plate"],
                            row["trip_no"],
                            row["store_count"],
                            row["route_distance_km"],
                            row["load_rate"],
                            row["route_text"],
                        )
                        for row in route_rows
                    ],
                )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"saved": True, "id": run_id, "routeRows": len(route_rows)}


# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def archive_list(payload):
    ensure_archive_tables()
    date_key = str(payload.get("date") or extract_date_key(time.strftime("%Y/%m/%d")))
    page = max(1, int(payload.get("page") or 1))
    page_size = max(1, min(50, int(payload.get("pageSize") or 12)))
    offset = (page - 1) * page_size
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"SELECT COUNT(1) AS total FROM {ARCHIVE_TABLE} WHERE created_date=%s", (date_key,))
            total = int((cursor.fetchone() or {}).get("total") or 0)
            cursor.execute(
                f"""
                SELECT id, payload_json
                FROM {ARCHIVE_TABLE}
                WHERE created_date=%s
                ORDER BY updated_at DESC, id DESC
                LIMIT %s OFFSET %s
                """,
                (date_key, page_size, offset),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    items = []
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for row in rows:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            snapshot = json.loads(row.get("payload_json") or "{}")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if snapshot and snapshot.get("id"):
                items.append(snapshot)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            continue
    total_pages = max(1, (total + page_size - 1) // page_size)
    return {"items": items, "total": total, "page": page, "pageSize": page_size, "totalPages": total_pages}


# 璇诲彇鎸囧畾鎵规鐨勫畬鏁村綊妗ｅ揩鐓с€?
# 鎺ㄦ紨涓嶈鑷繁鎷煎巻鍙茬粨鏋滐紝鑰岃閫氳繃杩欓噷鎷垮埌鍘熷鎵规鐨勭粨鏋勫寲蹇収銆?
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def archive_get(payload):
    ensure_archive_tables()
    run_id = str(payload.get("id") or "").strip()
    if not run_id:
        raise ValueError("missing_archive_id")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"SELECT payload_json FROM {ARCHIVE_TABLE} WHERE id=%s LIMIT 1", (run_id,))
            row = cursor.fetchone()
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not row:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return {"found": False}
    try:
        snapshot = json.loads(row.get("payload_json") or "{}")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        snapshot = {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"found": True, "item": snapshot}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def simulate_config_get():
    return {
        "success": True,
        "data": {
            "max_late_minutes": 60,
            "time_step_minutes": 30,
            "default_target": "min_vehicles",
            "wave_adjust_enable": True,
            "wave_rules": {
                "W1": {"start": "19:10", "end": "23:59", "allow_cross_wave": True},
                "W2": {"start": "21:00", "end": "07:00", "allow_cross_wave": True},
                "W3": {"start": "21:00", "end": "07:00", "allow_cross_wave": False},
                "W4": {"start": "12:00", "end": "18:00", "allow_cross_wave": False},
            },
        },
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def simulate_task_log_get(payload):
    task_id = str((payload or {}).get("taskId") or (payload or {}).get("task_id") or "").strip()
    cursor = int((payload or {}).get("cursor") or 0)
    if not task_id:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return 400, {"success": False, "error_code": "TASK_NOT_FOUND", "message": "task_id涓嶈兘涓虹┖"}
    snapshot = _simulation_task_snapshot(task_id, cursor)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not snapshot.get("found"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return 404, {"success": False, "error_code": "TASK_NOT_FOUND", "message": f"浠诲姟涓嶅瓨鍦? {task_id}"}
    return 200, {"success": True, "data": snapshot}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulate_error(error_code, message, status=400):
    return status, {"success": False, "error_code": str(error_code or ""), "message": str(message or "")}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_extract_store_code(store):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(store, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return ""
    for key in ("id", "shop_code", "shopCode", "store_id", "storeId", "code"):
        value = str(store.get(key) or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if value:
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            return value
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return ""


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulate_extract_snapshot_stores(snapshot):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(snapshot, dict):
        return []
    candidate_keys = ("stores", "store_list", "items", "shops")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for key in candidate_keys:
        value = snapshot.get(key)
        if isinstance(value, list) and value:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return deepcopy(value)
    result_obj = snapshot.get("result")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if isinstance(result_obj, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for key in candidate_keys:
            value = result_obj.get(key)
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if isinstance(value, list) and value:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return deepcopy(value)
    item_obj = snapshot.get("item")
    if isinstance(item_obj, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for key in candidate_keys:
            value = item_obj.get(key)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if isinstance(value, list) and value:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                return deepcopy(value)
    return []


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulate_load_live_stores():
    ensure_archive_tables()
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                """
                SELECT
                  r.shop_code,
                  r.wave_belongs,
                  r.wave1_load,
                  r.wave2_load,
                  r.wave3_load,
                  r.wave4_load,
                  r.total_resolved_load,
                  r.first_wave_time,
                  r.second_wave_time,
                  r.arrival_time_w3,
                  r.arrival_time_w4,
                  s.shop_name,
                  s.district,
                  s.lng,
                  s.lat,
                  s.address,
                  s.detailed_address,
                  s.trip_count,
                  s.cold_ratio,
                  s.service_minutes,
                  s.difficulty,
                  s.allowed_late_minutes,
                  s.schedule_status,
                  s.plate_no,
                  t.first_wave_time AS timing_first_wave_time,
                  t.second_wave_time AS timing_second_wave_time,
                  t.arrival_time_w3 AS timing_arrival_time_w3,
                  t.arrival_time_w4 AS timing_arrival_time_w4
                FROM store_wave_load_resolved r
                LEFT JOIN c_shop_main s ON s.shop_code = r.shop_code
                LEFT JOIN store_wave_timing_resolved t ON t.shop_code = r.shop_code
                ORDER BY r.shop_code
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall() or []
    stores = []
    for row in rows:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(row, dict):
            continue
        shop_code = str(row.get("shop_code") or "").strip()
        if not shop_code:
            continue
        first_wave_time = str(row.get("timing_first_wave_time") or row.get("first_wave_time") or "").strip()
        second_wave_time = str(row.get("timing_second_wave_time") or row.get("second_wave_time") or "").strip()
        arrival_time_w3 = str(row.get("timing_arrival_time_w3") or row.get("arrival_time_w3") or "").strip()
        arrival_time_w4 = str(row.get("timing_arrival_time_w4") or row.get("arrival_time_w4") or "").strip()
        stores.append(
            {
                "id": shop_code,
                "name": str(row.get("shop_name") or "").strip(),
                "district": str(row.get("district") or "").strip(),
                "lng": _safe_float(row.get("lng"), 0.0),
                "lat": _safe_float(row.get("lat"), 0.0),
                "address": str(row.get("address") or "").strip(),
                "detailedAddress": str(row.get("detailed_address") or "").strip(),
                "tripCount": int(_safe_float(row.get("trip_count"), 1)),
                "waveBelongs": str(row.get("wave_belongs") or "").strip(),
                "coldRatio": _safe_float(row.get("cold_ratio"), 0.0),
                "desiredArrival": first_wave_time,
                "waveArrivals": {
                    "w1": first_wave_time,
                    "w2": second_wave_time,
                    "w3": arrival_time_w3,
                    "w4": arrival_time_w4,
                },
                "serviceMinutes": int(_safe_float(row.get("service_minutes"), 15)),
                "difficulty": _safe_float(row.get("difficulty"), 1.0),
                "parking": int(_safe_float(row.get("allowed_late_minutes"), 10)),
                "allowedLateMinutes": int(_safe_float(row.get("allowed_late_minutes"), 10)),
                "status": str(row.get("schedule_status") or "").strip(),
                "plateNo": str(row.get("plate_no") or "").strip(),
                "wave1Load": _safe_float(row.get("wave1_load"), 0.0),
                "wave2Load": _safe_float(row.get("wave2_load"), 0.0),
                "wave3Load": _safe_float(row.get("wave3_load"), 0.0),
                "wave4Load": _safe_float(row.get("wave4_load"), 0.0),
                "boxes": _safe_float(row.get("total_resolved_load"), 0.0),
                "wave1TotalLoad": _safe_float(row.get("wave1_load"), 0.0),
                "wave2TotalLoad": _safe_float(row.get("wave2_load"), 0.0),
                "wave3TotalLoad": _safe_float(row.get("wave3_load"), 0.0),
                "wave4TotalLoad": _safe_float(row.get("wave4_load"), 0.0),
                "totalLoad": _safe_float(row.get("total_resolved_load"), 0.0),
                "resolvedWave1Load": _safe_float(row.get("wave1_load"), 0.0),
                "resolvedWave2Load": _safe_float(row.get("wave2_load"), 0.0),
                "resolvedWave3Load": _safe_float(row.get("wave3_load"), 0.0),
                "resolvedWave4Load": _safe_float(row.get("wave4_load"), 0.0),
                "resolvedTotalLoad": _safe_float(row.get("total_resolved_load"), 0.0),
                "first_wave_time": first_wave_time,
                "second_wave_time": second_wave_time,
                "arrival_time_w3": arrival_time_w3,
                "arrival_time_w4": arrival_time_w4,
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return stores


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_store_wave_belongs_text(store):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(store, dict):
        return ""
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return str(store.get("waveBelongs") or store.get("wave_belongs") or "").strip()


def _simulate_normalize_wave_key(value):
    text = str(value or "").strip().upper()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not text:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return ""
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if text in ("1", "W1", "FIRST"):
        return "W1"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if text in ("2", "W2", "SECOND"):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return "W2"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if text in ("3", "W3", "THIRD"):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "W3"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if text in ("4", "W4", "FOURTH"):
        return "W4"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return text


def _simulate_wave_no(wave_id):
    wid = _simulate_normalize_wave_key(wave_id)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W1":
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "1"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W2":
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return "2"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wid == "W3":
        return "3"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W4":
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "4"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return ""


def _simulate_hhmm_to_minutes(text):
    value = str(text or "").strip()
    matched = re.fullmatch(r"(\d{1,2}):(\d{2})", value)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not matched:
        return None
    hours = int(matched.group(1))
    minutes = int(matched.group(2))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if hours < 0 or hours > 23 or minutes < 0 or minutes > 59:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return hours * 60 + minutes


def _simulate_minutes_to_hhmm(value):
    minute = int(round(_to_float_safe(value, 0.0)))
    minute = minute % 1440
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if minute < 0:
        minute += 1440
    return f"{minute // 60:02d}:{minute % 60:02d}"


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_time_in_wave(new_time, wave_rule):
    target = _simulate_hhmm_to_minutes(new_time)
    start = _simulate_hhmm_to_minutes((wave_rule or {}).get("start"))
    end = _simulate_hhmm_to_minutes((wave_rule or {}).get("end"))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if target is None or start is None or end is None:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return False
    if start <= end:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return start <= target <= end
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return target >= start or target <= end


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_get_active_result(snapshot):
    results = snapshot.get("results") if isinstance(snapshot, dict) else None
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(results, list) or not results:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return None
    active_key = str(snapshot.get("activeResultKey") or "").strip()
    if active_key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        for result in results:
            if isinstance(result, dict) and str(result.get("key") or "").strip() == active_key:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                return result
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return results[0] if isinstance(results[0], dict) else None


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_flatten_solution_plans(solution):
    flattened = []
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not isinstance(solution, list):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return flattened
    for wave_group in solution:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(wave_group, list):
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for plan in wave_group:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if isinstance(plan, dict):
                    flattened.append(plan)
        elif isinstance(wave_group, dict):
            flattened.append(wave_group)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return flattened


def _simulate_route_distance(route_ids, dist):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(dist, dict):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return 0.0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _extract_route_store_id(item):
        if isinstance(item, dict):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return str(
                item.get("shop_code")
                or item.get("storeId")
                or item.get("id")
                or item.get("code")
                or ""
            ).strip()
        return str(item or "").strip()
    route = [_extract_route_store_id(item) for item in (route_ids or [])]
    route = [sid for sid in route if sid]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not route:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return 0.0
    total = 0.0
    current = DC_ID
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for store_id in route:
        total += float(((dist.get(current) or {}).get(store_id) or 0.0))
        current = store_id
    total += float(((dist.get(current) or {}).get(DC_ID) or 0.0))
    return total


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulate_plan_distance(plan, dist):
    explicit = plan.get("totalDistance")
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if explicit is not None and str(explicit) != "":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return float(explicit)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            pass
    total = 0.0
    for trip in (plan.get("trips") or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(trip, dict):
            continue
        trip_distance = trip.get("routeDistanceKm")
        if trip_distance is None or str(trip_distance) == "":
            trip_distance = trip.get("roundTripKm")
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if trip_distance is None or str(trip_distance) == "":
            trip_distance = trip.get("distanceKm")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if trip_distance is None or str(trip_distance) == "":
            trip_distance = _simulate_route_distance(trip.get("route") or [], dist)
        total += float(trip_distance or 0.0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return total


# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_wave_stats_from_plans(plans, dist=None):
    stats = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for wave_id in ("W1", "W2", "W3", "W4"):
        wave_plans = [plan for plan in (plans or []) if _simulate_normalize_wave_key(plan.get("waveId")) == wave_id]
        store_ids = set()
        vehicle_ids = set()
        mileage = 0.0
        for plan in wave_plans:
            vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
            plate_no = str(vehicle.get("plateNo") or "").strip()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if plate_no and (plan.get("trips") or []):
                vehicle_ids.add(plate_no)
            mileage += _simulate_plan_distance(plan, dist)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for trip in (plan.get("trips") or []):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if not isinstance(trip, dict):
                    continue
                for store_id in (trip.get("route") or []):
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if isinstance(store_id, dict):
                        sid = str(
                            store_id.get("shop_code")
                            or store_id.get("storeId")
                            or store_id.get("id")
                            or store_id.get("code")
                            or ""
                        ).strip()
                    else:
                        sid = str(store_id or "").strip()
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if sid:
                        store_ids.add(sid)
        stats[wave_id] = {
            "vehicles": len(vehicle_ids),
            "mileage": round(mileage, 1),
            "stores": len(store_ids),
        }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return stats


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_total_unique_vehicles(plans):
    used = set()
    for plan in (plans or []):
        vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
        plate_no = str(vehicle.get("plateNo") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if plate_no and (plan.get("trips") or []):
            used.add(plate_no)
    return len(used)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_filter_plans_by_waves(plans, wave_ids):
    scoped = {str(_simulate_normalize_wave_key(wid) or "").strip() for wid in (wave_ids or []) if str(wid or "").strip()}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not scoped:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return list(plans or [])
    return [
        plan
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for plan in (plans or [])
        if _simulate_normalize_wave_key((plan or {}).get("waveId")) in scoped
    ]


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_summarize_snapshot(snapshot):
    active_result = _simulate_get_active_result(snapshot) or {}
    plans = _simulate_flatten_solution_plans(active_result.get("solution") or [])
    metrics = active_result.get("metrics") if isinstance(active_result.get("metrics"), dict) else {}
    total_mileage = metrics.get("totalDistance")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if total_mileage is None or str(total_mileage) == "":
        total_mileage = sum(item["mileage"] for item in _simulate_wave_stats_from_plans(plans).values())
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "plans": plans,
        "summary": {
            "total_vehicles": _simulate_total_unique_vehicles(plans),
            "total_mileage": round(float(total_mileage or 0.0), 1),
            "wave_stats": _simulate_wave_stats_from_plans(plans),
        },
    }


def _simulate_get_store_time(store, wave_id):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(store, dict):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return ""
    wid = _simulate_normalize_wave_key(wave_id)
    wave_arrivals = store.get("waveArrivals") if isinstance(store.get("waveArrivals"), dict) else {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W1":
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return str(wave_arrivals.get("w1") or store.get("desiredArrival") or store.get("first_wave_time") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W2":
        return str(wave_arrivals.get("w2") or store.get("second_wave_time") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if wid == "W3":
        return str(wave_arrivals.get("w3") or store.get("arrival_time_w3") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wid == "W4":
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return str(wave_arrivals.get("w4") or store.get("arrival_time_w4") or store.get("arrival_time") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return ""


# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_get_store_load(store):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(store, dict):
        return 0.0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for key in ("total_resolved_load", "resolved_load", "boxes", "demand", "load"):
        value = store.get(key)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if value is None or str(value).strip() == "":
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return float(_to_float_safe(value, 0.0))
    return 0.0


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulate_align_arrival_minutes(time_text, dispatch_start_min):
    minutes = _simulate_hhmm_to_minutes(time_text)
    if minutes is None:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return int(dispatch_start_min or 0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if minutes < int(dispatch_start_min or 0):
        minutes += 1440
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return int(minutes)


def _simulate_store_boxes_for_wave(store, wave_id):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(store, dict):
        return 0.0
    wid = _simulate_normalize_wave_key(wave_id)
    field_candidates = {
        "W1": ("wave1Load", "wave1_load", "resolvedWave1Load", "wave1TotalLoad", "wave1TotalLoadBase"),
        "W2": ("wave2Load", "wave2_load", "resolvedWave2Load", "wave2TotalLoad", "wave2TotalLoadBase"),
        "W3": ("wave3Load", "wave3_load", "resolvedWave3Load", "wave3TotalLoad", "wave3TotalLoadBase"),
        "W4": ("wave4Load", "wave4_load", "resolvedWave4Load", "wave4TotalLoad", "wave4TotalLoadBase"),
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for field in field_candidates.get(wid, ()):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        try:
            value = float(store.get(field) or 0.0)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            value = 0.0
        if value > 0:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return value
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    try:
        fallback = float(store.get("boxes") or store.get("totalResolvedLoad") or store.get("total_resolved_load") or 0.0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        fallback = 0.0
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return fallback


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_prepare_solver_stores(stores, wave_id, dispatch_start_min):
    prepared = []
    for store in (stores or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(store, dict):
            continue
        store_id = _simulate_extract_store_code(store)
        if not store_id:
            continue
        belongs_text = _simulate_store_wave_belongs_text(store)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not belongs_text:
            belongs_text = _simulate_wave_no(wave_id)
        boxes = _simulate_store_boxes_for_wave(store, wave_id)
        desired_time = _simulate_get_store_time(store, wave_id)
        normalized_wave_no = _simulate_wave_no(wave_id)
        belongs_tokens = [token.strip() for token in str(belongs_text or "").split(",") if token.strip()]
        supports_wave = (not belongs_tokens) or (normalized_wave_no in belongs_tokens)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if boxes <= 0 and not desired_time:
            supports_wave = False
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not supports_wave:
            continue
        desired_arrival_min = _simulate_align_arrival_minutes(desired_time, dispatch_start_min)
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        try:
            allowed_late = int(store.get("allowedLateMinutes") or store.get("allowed_late_minutes") or store.get("parking") or 10)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            allowed_late = 10
        prepared.append(
            {
                "id": store_id,
                "name": str(store.get("name") or store.get("shop_name") or "").strip(),
                "waveBelongs": belongs_text,
                "boxes": float(boxes or 0.0),
                "desiredArrivalMin": desired_arrival_min,
                "latestAllowedArrivalMin": desired_arrival_min + max(0, allowed_late),
                "actualServiceMinutes": float(store.get("actualServiceMinutes") or store.get("serviceMinutes") or store.get("service_minutes") or 15),
                "serviceMinutes": float(store.get("serviceMinutes") or store.get("service_minutes") or 15),
                "coldRatio": float(store.get("coldRatio") or store.get("cold_ratio") or 0.0),
                "difficulty": float(store.get("difficulty") or 1.0),
                "parking": float(allowed_late),
            }
        )
    return prepared


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_apply_store_adjustment(store, new_wave, new_time, new_allowed_late=None):
    wave_no = _simulate_wave_no(new_wave)
    store["waveBelongs"] = wave_no
    store["wave_belongs"] = wave_no
    wave_arrivals = store.get("waveArrivals") if isinstance(store.get("waveArrivals"), dict) else {}
    wave_arrivals = dict(wave_arrivals)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if new_wave == "W1":
        wave_arrivals["w1"] = new_time
        store["desiredArrival"] = new_time
        store["first_wave_time"] = new_time
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    elif new_wave == "W2":
        wave_arrivals["w2"] = new_time
        store["second_wave_time"] = new_time
    elif new_wave == "W3":
        wave_arrivals["w3"] = new_time
        store["arrival_time_w3"] = new_time
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    elif new_wave == "W4":
        wave_arrivals["w4"] = new_time
        store["arrival_time_w4"] = new_time
        store["arrival_time"] = new_time
    store["waveArrivals"] = wave_arrivals
    if new_allowed_late is not None:
        store["parking"] = int(new_allowed_late)
        store["allowedLateMinutes"] = int(new_allowed_late)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_find_wave(snapshot, wave_id):
    target = _simulate_normalize_wave_key(wave_id)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for wave in (snapshot.get("waves") or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if _simulate_normalize_wave_key(wave.get("waveId")) == target:
            found = deepcopy(wave)
            found["waveId"] = target
            start_text = str(found.get("start") or "").strip()
            end_text = str(found.get("end") or "").strip()
            start_min = _simulate_hhmm_to_minutes(start_text)
            end_min = _simulate_hhmm_to_minutes(end_text)
            if start_min is not None:
                found["startMin"] = start_min
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if end_min is not None:
                found["endMin"] = end_min
            if "endMode" not in found:
                found["endMode"] = "return" if target == "W1" else "service"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if "relaxEnd" not in found:
                found["relaxEnd"] = False
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if "singleWave" not in found:
                found["singleWave"] = False
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "isNightWave" not in found:
                found["isNightWave"] = target in ("W1", "W2", "W3")
            return found
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return None


# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulate_build_scenario(snapshot):
    source = snapshot.get("scenario") if isinstance(snapshot.get("scenario"), dict) else {}
    settings = snapshot.get("settings") if isinstance(snapshot.get("settings"), dict) else {}
    dispatch_start = source.get("dispatchStartMin")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if dispatch_start is None:
        start_candidates = []
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for wave in (snapshot.get("waves") or []):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(wave, dict):
                continue
            start_min = wave.get("startMin")
            if start_min is None:
                start_min = _simulate_hhmm_to_minutes(wave.get("start"))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if start_min is not None:
                start_candidates.append(int(start_min))
        dispatch_start = min(start_candidates) if start_candidates else 0
    return {
        "dispatchStartMin": int(dispatch_start or 0),
        "maxRouteKm": float(source.get("maxRouteKm") or settings.get("maxRouteKm") or 220),
        "concentrateLate": bool(source.get("concentrateLate") or settings.get("concentrateLate") or False),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_extract_realtime_context(payload):
    context = (payload or {}).get("realtime_context")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return context if isinstance(context, dict) else {}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_build_wave_payload(snapshot, stores, wave_id, target):
    scenario = _simulate_build_scenario(snapshot)
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    payload = {
        "algorithmKey": str(snapshot.get("activeResultKey") or "vehicle").strip().lower() or "vehicle",
        "scenario": scenario,
        "stores": _simulate_prepare_solver_stores(stores, wave_id, scenario.get("dispatchStartMin")),
        "vehicles": deepcopy(snapshot.get("vehicles") or []),
        "waves": deepcopy(snapshot.get("waves") or []),
        "settings": deepcopy(snapshot.get("settings") or {}),
        "dist": deepcopy(snapshot.get("dist") or {}),
        "wave": _simulate_find_wave(snapshot, wave_id),
    }
    payload["settings"]["optimizeGoal"] = str(target or payload["settings"].get("optimizeGoal") or "min_vehicles")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not payload.get("wave"):
        raise ValueError(f"missing_wave:{wave_id}")
    return payload


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulate_prepare_wave_payload(payload):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    payload = _normalize_strategy_config(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    payload = _normalize_numeric_types_for_solver(payload)
    payload, _ = apply_strategy_center(payload)
    payload = _apply_operational_strategy_overrides(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not _should_skip_dist_hydrate(payload):
        payload = _hydrate_payload_dist_from_db(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    payload = _validate_solver_payload_fields(payload)
    _enforce_and_validate_vehicle_type_for_solve(payload)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return payload


# 涓烘帹婕斿拰涓昏皟搴﹀鐢ㄥ悓涓€鏉￠澶勭悊閾俱€?
# 杩欐牱鎺ㄦ紨璋冪敤鍜屼富璋冨害璋冪敤鐨勬眰瑙ｅ墠澶勭悊灏介噺淇濇寔涓€鑷达紝鍑忓皯鈥滀富璋冨害鑳借窇銆佹帹婕斿彛寰勪笉鍚屸€濈殑闂銆?
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _run_wave_optimize_with_same_logic(payload):
    payload = _normalize_strategy_config(payload)
    algorithm_key = str(payload.get("algorithmKey") or "").strip().lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if algorithm_key not in ("vehicle", "vehicle_driven") and not bool(payload.get("skipResolvedStoreOverlay")):
        payload = _overlay_payload_stores_from_resolved_table(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    payload = _normalize_numeric_types_for_solver(payload)
    payload, strategy_audit = apply_strategy_center(payload)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    payload = _apply_operational_strategy_overrides(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not _should_skip_dist_hydrate(payload):
        payload = _hydrate_payload_dist_from_db(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    payload = _validate_solver_payload_fields(payload)
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if payload.get("useRecommendedPlan"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        payload = apply_recommended_plan_warm_start(payload)
    _enforce_and_validate_vehicle_type_for_solve(payload)
    algorithm_key = str(payload.get("algorithmKey") or "").strip().lower()
    module_file = _resolve_algorithm_module_file(algorithm_key)
    _append_sfrz_log(f"[SIMULATE_SOLVER_PATH] key={algorithm_key} module={module_file}")
    _assert_expected_algorithm_module(algorithm_key, module_file)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    result = run_wave_optimize(payload)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return result, payload, strategy_audit


def _simulate_convert_best_state_to_plans(best_state, wave_id, dist, solved_payload=None, solve_result=None):
    plans = []
    solved_payload = solved_payload if isinstance(solved_payload, dict) else {}
    scenario = solved_payload.get("scenario") if isinstance(solved_payload.get("scenario"), dict) else {}
    settings = solved_payload.get("settings") if isinstance(solved_payload.get("settings"), dict) else {}
    dispatch_start_min = _to_float_safe(scenario.get("dispatchStartMin"), None)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if dispatch_start_min is None:
        dispatch_start_min = _to_float_safe((solved_payload.get("wave") or {}).get("startMin"), 0.0)
    speed_kmh = _to_float_safe(scenario.get("speed"), 38.0)
    if str(wave_id or "").upper() == "W3":
        speed_kmh = _to_float_safe(scenario.get("w3Speed"), speed_kmh)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if speed_kmh <= 0:
        speed_kmh = 38.0
    service_minutes = _to_float_safe(settings.get("stopServiceMinutes"), 8.0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if service_minutes < 0:
        service_minutes = 0.0
    detailed_state = solve_result.get("bestStateDetailed") if isinstance(solve_result, dict) and isinstance(solve_result.get("bestStateDetailed"), list) else []
    detailed_route_map = {}
    detailed_vehicle_capacity_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for d_vehicle in detailed_state:
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(d_vehicle, dict):
            continue
        plate_key = str(d_vehicle.get("plateNo") or "").strip()
        detailed_vehicle_capacity_map[plate_key] = float(_to_float_safe(d_vehicle.get("capacity"), 0.0))
        d_routes = d_vehicle.get("routes") if isinstance(d_vehicle.get("routes"), list) else []
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for d_index, d_route in enumerate(d_routes):
            if not isinstance(d_route, dict):
                continue
            trip_no = int(_to_float_safe(d_route.get("tripNo"), d_index + 1))
            detailed_route_map[(plate_key, trip_no)] = d_route

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _extract_stop_code(item):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if isinstance(item, dict):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            return str(
                item.get("shop_code")
                or item.get("storeId")
                or item.get("id")
                or item.get("code")
                or ""
            ).strip()
        return str(item or "").strip()

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _extract_stop_arrival(item):
        if not isinstance(item, dict):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return None
        raw = (
            item.get("planned_arrival_time")
            or item.get("plannedArrivalTime")
            or item.get("arrival_time")
            or item.get("arrival")
        )
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if raw not in (None, ""):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return str(raw).strip()
        arrival_min = item.get("arrivalMin")
        if arrival_min is not None and str(arrival_min) != "":
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return _simulate_minutes_to_hhmm(_to_float_safe(arrival_min, 0.0))
        return None

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    def _calc_travel_minutes(from_id, to_id):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(dist, dict):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return 0.0
        from_key = str(from_id or "").strip()
        to_key = str(to_id or "").strip()
        if not from_key or not to_key:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return 0.0
        distance_km = _to_float_safe(((dist.get(from_key) or {}).get(to_key)), 0.0)
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if distance_km <= 0:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return 0.0
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return (distance_km / speed_kmh) * 60.0

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for item in (best_state or []):
        if not isinstance(item, dict):
            continue
        routes = item.get("routes") or []
        trips = []
        total_distance = 0.0
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        for index, route in enumerate(routes):
            plate_key = str(item.get("plateNo") or "").strip()
            trip_no = index + 1
            detail_route = detailed_route_map.get((plate_key, trip_no))
            route_ids = []
            route_stops = []
            if isinstance(detail_route, dict):
                detail_stops = detail_route.get("stops") if isinstance(detail_route.get("stops"), list) else []
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for stop in detail_stops:
                    stop_code = _extract_stop_code(stop)
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if not stop_code:
                        continue
                    route_ids.append(stop_code)
                    route_stops.append(
                        {
                            "shop_code": stop_code,
                            "planned_arrival_time": _extract_stop_arrival(stop),
                            "scheduled_source": "solver" if _extract_stop_arrival(stop) else "none",
                        }
                    )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            elif isinstance(route, dict):
                raw_stops = route.get("stops")
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if isinstance(raw_stops, list):
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    for stop in raw_stops:
                        stop_code = _extract_stop_code(stop)
                        if not stop_code:
                            continue
                        planned = _extract_stop_arrival(stop)
                        route_ids.append(stop_code)
                        route_stops.append(
                            {
                                "shop_code": stop_code,
                                "planned_arrival_time": planned,
                                "scheduled_source": "solver" if planned else "none",
                            }
                        )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                else:
                    raw_route = route.get("route") if isinstance(route.get("route"), list) else []
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    for stop in raw_route:
                        stop_code = _extract_stop_code(stop)
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if not stop_code:
                            continue
                        planned = _extract_stop_arrival(stop) if isinstance(stop, dict) else None
                        route_ids.append(stop_code)
                        route_stops.append(
                            {
                                "shop_code": stop_code,
                                "planned_arrival_time": planned,
                                "scheduled_source": "solver" if planned else "none",
                            }
                        )
            else:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                for stop in (route or []):
                    stop_code = _extract_stop_code(stop)
                    if not stop_code:
                        continue
                    planned = _extract_stop_arrival(stop) if isinstance(stop, dict) else None
                    route_ids.append(stop_code)
                    route_stops.append(
                        {
                            "shop_code": stop_code,
                            "planned_arrival_time": planned,
                            "scheduled_source": "solver" if planned else "none",
                        }
                    )

            # 鑻ユ眰瑙ｅ櫒鏈樉寮忚繑鍥炲埌搴楁椂闂达紝鍒欐寜绾胯矾椤哄簭涓庤窛绂绘帹绠椾竴浠藉彲鏍搁獙鍒板簵鏃堕棿
            cursor_min = float(dispatch_start_min or 0.0)
            prev_id = DC_ID
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for stop in route_stops:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not isinstance(stop, dict):
                    continue
                travel_min = _calc_travel_minutes(prev_id, stop.get("shop_code"))
                cursor_min += max(0.0, float(travel_min or 0.0))
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not stop.get("planned_arrival_time"):
                    stop["planned_arrival_time"] = _simulate_minutes_to_hhmm(cursor_min)
                    stop["scheduled_source"] = "reconstructed"
                cursor_min += service_minutes
                prev_id = str(stop.get("shop_code") or "").strip() or prev_id
            trip_distance = _simulate_route_distance(route_ids, dist)
            total_distance += trip_distance
            raw_route_distance = detail_route.get("routeDistanceKm") if isinstance(detail_route, dict) else None
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if raw_route_distance is None or str(raw_route_distance) == "":
                raw_route_distance = route.get("routeDistanceKm") if isinstance(route, dict) else None
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if raw_route_distance is None or str(raw_route_distance) == "":
                raw_route_distance = route.get("distanceKm") if isinstance(route, dict) else None
            if raw_route_distance is None or str(raw_route_distance) == "":
                raw_route_distance = trip_distance
            trips.append(
                {
                    "tripNo": index + 1,
                    "waveId": wave_id,
                    "route": route_stops if route_stops else route_ids,
                    "routeDistanceKm": float(raw_route_distance or 0.0),
                }
            )
        plans.append(
            {
                "waveId": wave_id,
                "vehicle": {
                    "plateNo": str(item.get("plateNo") or "").strip(),
                    "capacity": float(
                        _to_float_safe(
                            detailed_vehicle_capacity_map.get(str(item.get("plateNo") or "").strip()),
                            _to_float_safe(item.get("capacity"), 0.0),
                        )
                    ),
                },
                "trips": trips,
                "totalDistance": total_distance,
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return plans


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _build_route_comparison(
    old_solution,
    new_solution,
    shop_code,
    shop_name_map,
    store_load_map,
    old_expected_time_map=None,
    new_expected_time_map=None,
    preferred_wave=None,
):
    """瀵规瘮鏂版棫鏂规涓寚瀹氶棬搴楃殑璺嚎銆?""
    target_shop = str(shop_code or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    result = {
        "shop_code": target_shop,
        "old_routes": [],
        "new_route": None,
    }
    if not target_shop:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return result

    old_expected_time_map = old_expected_time_map or {}
    new_expected_time_map = new_expected_time_map or {}

    preferred_wave = _simulate_normalize_wave_key(preferred_wave)

    def _normalize_stop(stop, expected_time_map):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(stop, dict):
            code = str(
                stop.get("shop_code")
                or stop.get("storeId")
                or stop.get("id")
                or stop.get("code")
                or ""
            ).strip()
            name = str(stop.get("shop_name") or stop.get("name") or shop_name_map.get(code) or "").strip()
            arrival = (
                stop.get("planned_arrival_time")
                or stop.get("plannedArrivalTime")
                or stop.get("arrival_time")
                or stop.get("arrival")
                or None
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            return {
                "shop_code": code,
                "shop_name": name,
                "arrival": str(arrival).strip() if arrival not in (None, "") else None,
                "scheduled_time": str(arrival).strip() if arrival not in (None, "") else "姹傝В鍣ㄦ湭杩斿洖鍒板簵鏃堕棿",
                "scheduled_source": str(stop.get("scheduled_source") or ("solver" if arrival not in (None, "") else "none")),
                "expected_time": str(expected_time_map.get(code) or "--:--"),
                "boxes": round(float(store_load_map.get(code) or 0.0), 1),
            }
        code = str(stop or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {
            "shop_code": code,
            "shop_name": str(shop_name_map.get(code) or "").strip(),
            "arrival": None,
            "scheduled_time": "姹傝В鍣ㄦ湭杩斿洖鍒板簵鏃堕棿",
            "scheduled_source": "none",
            "expected_time": str(expected_time_map.get(code) or "--:--"),
            "boxes": round(float(store_load_map.get(code) or 0.0), 1),
        }

    def _collect_routes(plans, expected_time_map):
        matches = []
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for plan in (plans or []):
            if not isinstance(plan, dict):
                continue
            wave_id = _simulate_normalize_wave_key(plan.get("waveId"))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if preferred_wave and wave_id != preferred_wave:
                continue
            vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
            vehicle_plate = str(vehicle.get("plateNo") or "").strip()
            capacity = float(vehicle.get("capacity") or vehicle.get("solveCapacity") or 1.2)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if capacity <= 0:
                capacity = 1.2
            trips = plan.get("trips") if isinstance(plan.get("trips"), list) else []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for trip_idx, trip in enumerate(trips):
                if not isinstance(trip, dict):
                    continue
                route = trip.get("route") if isinstance(trip.get("route"), list) else []
                normalized_stops = [_normalize_stop(stop, expected_time_map) for stop in route]
                route_codes = [str(stop.get("shop_code") or "").strip() for stop in normalized_stops if str(stop.get("shop_code") or "").strip()]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if target_shop not in route_codes:
                    continue
                total_boxes = sum(float(store_load_map.get(code) or 0.0) for code in route_codes)
                matches.append(
                    {
                        "vehicle": vehicle_plate,
                        "wave": wave_id,
                        "trip": trip_idx + 1,
                        "stops": normalized_stops,
                        "total_distance": round(float(trip.get("routeDistanceKm") or plan.get("totalDistance") or 0.0), 1),
                        "total_load": round((total_boxes / capacity) * 100.0, 1),
                    }
                )
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return matches

    old_routes = _collect_routes(old_solution, old_expected_time_map)
    new_routes = _collect_routes(new_solution, new_expected_time_map)
    result["old_routes"] = old_routes
    result["new_route"] = new_routes[0] if new_routes else None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return result


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_json_dumps(value):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        return json.dumps(value, ensure_ascii=False)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    except Exception:
        return "[]"


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_json_loads(value, default=None):
    fallback = {} if default is None else default
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if isinstance(value, (dict, list)):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return value
    text = str(value or "").strip()
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not text:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return fallback
    try:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return json.loads(text)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return fallback


def _simulation_persist_task_start(task_id, batch_id, target):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    batch_key = str(batch_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key or not batch_key:
        return
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {SIMULATION_TASK_TABLE}
                (task_id, batch_id, target, status, error_message, created_at, finished_at)
                VALUES (%s, %s, %s, 'running', NULL, NOW(), NULL)
                ON DUPLICATE KEY UPDATE
                    batch_id=VALUES(batch_id),
                    target=VALUES(target),
                    status='running',
                    error_message=NULL,
                    created_at=NOW(),
                    finished_at=NULL
                """,
                (task_key, batch_key, str(target or "min_vehicles").strip() or "min_vehicles"),
            )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_persist_task_finish(task_id, status="success", error_message=""):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    if not task_key:
        return
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                UPDATE {SIMULATION_TASK_TABLE}
                SET status=%s, error_message=%s, finished_at=NOW()
                WHERE task_id=%s
                """,
                (
                    str(status or "success").strip() or "success",
                    str(error_message or "").strip() or None,
                    task_key,
                ),
            )


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_clear_task_rows(task_id):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not task_key:
        return
    tables = [
        SIMULATION_SINGLE_ROUTE_STOP_TABLE,
        SIMULATION_SINGLE_ROUTE_TABLE,
        SIMULATION_SINGLE_SNAPSHOT_TABLE,
        SIMULATION_SINGLE_EVENT_LOG_TABLE,
        SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE,
        SIMULATION_SINGLE_STATE_TRANSITION_TABLE,
        SIMULATION_SINGLE_REMAINING_STATE_TABLE,
        SIMULATION_SINGLE_TASK_TABLE,
    ]
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for table in tables:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(f"DELETE FROM {table} WHERE task_id=%s", (task_key,))


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_persist_task(
    task_id,
    batch_id,
    target_adjust,
    target,
    algorithm_key,
    affected_wave,
    payload,
    status="running",
    before_summary=None,
    after_summary=None,
    summary_text="",
    result_data=None,
    error_message="",
):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    batch_key = str(batch_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not task_key or not batch_key:
        return
    adjust = target_adjust if isinstance(target_adjust, dict) else {}
    before_summary = before_summary if isinstance(before_summary, dict) else {}
    after_summary = after_summary if isinstance(after_summary, dict) else {}
    task_date = None
    try:
        task_date = datetime.now().strftime("%Y-%m-%d")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    except Exception:
        task_date = None
    payload_json = _simulation_json_dumps(payload or {})
    result_json = _simulation_json_dumps(result_data or {})
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {SIMULATION_SINGLE_TASK_TABLE}
                (task_id, source_batch_id, task_date, status, shop_code, shop_name, original_wave,
                 simulated_wave, original_time, simulated_time, target, algorithm_key, affected_wave,
                 vehicle_count_before, vehicle_count_after, mileage_before, mileage_after, summary_text,
                 payload_json, result_json, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        CASE WHEN %s IN ('success','failed') THEN NOW() ELSE NULL END)
                ON DUPLICATE KEY UPDATE
                    source_batch_id=VALUES(source_batch_id),
                    task_date=VALUES(task_date),
                    status=VALUES(status),
                    shop_code=VALUES(shop_code),
                    shop_name=VALUES(shop_name),
                    original_wave=VALUES(original_wave),
                    simulated_wave=VALUES(simulated_wave),
                    original_time=VALUES(original_time),
                    simulated_time=VALUES(simulated_time),
                    target=VALUES(target),
                    algorithm_key=VALUES(algorithm_key),
                    affected_wave=VALUES(affected_wave),
                    vehicle_count_before=VALUES(vehicle_count_before),
                    vehicle_count_after=VALUES(vehicle_count_after),
                    mileage_before=VALUES(mileage_before),
                    mileage_after=VALUES(mileage_after),
                    summary_text=VALUES(summary_text),
                    payload_json=VALUES(payload_json),
                    result_json=VALUES(result_json),
                    finished_at=CASE WHEN VALUES(status) IN ('success','failed') THEN NOW() ELSE finished_at END
                """,
                (
                    task_key,
                    batch_key,
                    task_date,
                    str(status or "running").strip() or "running",
                    str(adjust.get("shop_code") or "").strip(),
                    str(adjust.get("shop_name") or "").strip() or None,
                    str(adjust.get("old_wave") or "").strip() or None,
                    str(adjust.get("new_wave") or "").strip() or None,
                    str(adjust.get("old_time") or "").strip() or None,
                    str(adjust.get("new_time") or "").strip() or None,
                    str(target or "min_vehicles").strip() or "min_vehicles",
                    str(algorithm_key or "").strip() or None,
                    str(affected_wave or "").strip() or None,
                    int(before_summary.get("total_vehicles") or 0),
                    int(after_summary.get("total_vehicles") or 0),
                    float(before_summary.get("total_mileage") or 0.0),
                    float(after_summary.get("total_mileage") or 0.0),
                    str(summary_text or error_message or "").strip() or None,
                    payload_json,
                    result_json,
                    str(status or "running").strip() or "running",
                ),
            )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_single_stage_for_line(text):
    line = str(text or "").strip()
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "general", "log", None
    wave_match = re.search(r"\b(W[1-4])\b", line)
    wave_id = wave_match.group(1) if wave_match else None
    if "鍩虹嚎-" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "baseline", "baseline_log", wave_id
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "A/B" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return "ab_compare", "ab_compare", wave_id
    if "鎬讳綋缁撴灉" in line or "A/B涓枃鎶ュ憡" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "summary", "summary", wave_id
    if "澶辫触鏍锋湰" in line or "寤鸿A锛? in line or "渚濇嵁锛? in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "diagnosis", "diagnosis", wave_id
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if "寮€濮嬫眰瑙? in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "solve", "solve_started", wave_id
    if "姹傝В瀹屾垚" in line:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "solve", "solve_finished", wave_id
    return "general", "log", wave_id


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulation_single_persist_event_logs(task_id):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key:
        return
    snapshot = _simulation_task_snapshot(task_key, 0) or {}
    lines = snapshot.get("lines") if isinstance(snapshot.get("lines"), list) else []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    rows = []
    for line in lines:
        clean = str(line or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not clean:
            continue
        stage, event_type, wave_id = _simulation_single_stage_for_line(clean)
        rows.append((task_key, stage, event_type, wave_id, None, None, clean, None))
    if not rows:
        return
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_EVENT_LOG_TABLE} WHERE task_id=%s", (task_key,))
            cursor.executemany(
                f"""
                INSERT INTO {SIMULATION_SINGLE_EVENT_LOG_TABLE}
                (task_id, stage, event_type, wave_id, vehicle_no, shop_code, message, detail_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )


def _simulation_single_collect_plan_rows(task_id, snapshot_type, plans, store_map, target_shop_code=""):
    route_rows = []
    stop_rows = []
    task_key = str(task_id or "").strip()
    target_key = str(target_shop_code or "").strip()
    route_seq = 0
    stores = store_map if isinstance(store_map, dict) else {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for plan in (plans or []):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(plan, dict):
            continue
        wave_id = _simulate_normalize_wave_key(plan.get("waveId"))
        vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
        vehicle_no = str(vehicle.get("plateNo") or "").strip() or None
        capacity = _to_float_safe(vehicle.get("capacity"), 1.2)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if capacity <= 0:
            capacity = 1.2
        trips = plan.get("trips") if isinstance(plan.get("trips"), list) else []
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for trip in trips:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(trip, dict):
                continue
            route_seq += 1
            trip_no = int(_to_float_safe(trip.get("tripNo"), 1))
            route = trip.get("route") if isinstance(trip.get("route"), list) else []
            route_distance = round(float(trip.get("routeDistanceKm") or 0.0), 3)
            store_codes = []
            total_boxes = 0.0
            start_time = None
            end_time = None
            for stop_idx, stop in enumerate(route, start=1):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if isinstance(stop, dict):
                    shop_code = str(stop.get("shop_code") or stop.get("storeId") or stop.get("id") or stop.get("code") or "").strip()
                    scheduled_arrival = str(stop.get("planned_arrival_time") or stop.get("plannedArrivalTime") or stop.get("arrival_time") or stop.get("arrival") or "").strip() or None
                    scheduled_source = str(stop.get("scheduled_source") or ("solver_returned" if scheduled_arrival else "missing")).strip() or "missing"
                else:
                    shop_code = str(stop or "").strip()
                    scheduled_arrival = None
                    scheduled_source = "missing"
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not shop_code:
                    continue
                store = stores.get(shop_code) or {}
                expected_time = _simulate_get_store_time(store, wave_id)
                boxes = round(float(_simulate_store_boxes_for_wave(store, wave_id) or 0.0), 3)
                total_boxes += boxes
                store_codes.append(shop_code)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if scheduled_arrival and not start_time:
                    start_time = scheduled_arrival
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if scheduled_arrival:
                    end_time = scheduled_arrival
                stop_rows.append(
                    (
                        task_key,
                        str(snapshot_type or "").strip(),
                        wave_id,
                        vehicle_no,
                        trip_no,
                        stop_idx,
                        shop_code,
                        str(store.get("shop_name") or store.get("name") or "").strip() or None,
                        str(expected_time or "").strip() or None,
                        scheduled_arrival,
                        None,
                        boxes,
                        boxes,
                        round(total_boxes, 6),
                        1 if target_key and shop_code == target_key else 0,
                        scheduled_source,
                        _simulation_json_dumps(stop if isinstance(stop, dict) else {"shop_code": shop_code}),
                    )
                )
            solver_load_value = round((total_boxes / capacity), 6) if capacity > 0 else None
            display_load_percent = round((total_boxes / capacity) * 100.0, 3) if capacity > 0 else None
            route_payload = {
                "trip": trip if isinstance(trip, dict) else {},
                "capacity_limit": float(capacity),
                "actual_load_value": round(float(total_boxes), 6),
                "display_load_percent": display_load_percent,
            }
            route_rows.append(
                (
                    task_key,
                    str(snapshot_type or "").strip(),
                    wave_id,
                    vehicle_no,
                    trip_no,
                    route_seq,
                    len(store_codes),
                    route_distance,
                    solver_load_value,
                    display_load_percent,
                    start_time,
                    end_time,
                    "active",
                    _simulation_json_dumps(route_payload),
                )
            )
    return route_rows, stop_rows


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulation_single_manual_route_key(snapshot_type, wave_id, vehicle_no, trip_no):
    return "|".join(
        [
            str(snapshot_type or "").strip(),
            str(wave_id or "").strip(),
            str(vehicle_no or "").strip(),
            str(int(_to_float_safe(trip_no, 1))),
        ]
    )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_single_persist_snapshots(
    task_id,
    baseline_wave,
    baseline_compare,
    simulated_compare,
    baseline_plans,
    simulated_plans,
    baseline_store_map,
    simulated_store_map,
    target_shop_code="",
):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key:
        return
    base_cmp = baseline_compare if isinstance(baseline_compare, dict) else {}
    sim_cmp = simulated_compare if isinstance(simulated_compare, dict) else {}
    snapshot_rows = [
        (
            task_key,
            "baseline",
            str(baseline_wave or "").strip() or None,
            int(base_cmp.get("candidate") or 0),
            int(base_cmp.get("assigned") or 0),
            int(base_cmp.get("pending") or 0),
            int(base_cmp.get("vehicles") or 0),
            float(base_cmp.get("mileage") or 0.0),
            "success",
            "solver",
            _simulation_json_dumps({"compare": base_cmp}),
        ),
        (
            task_key,
            "simulated",
            str(baseline_wave or "").strip() or None,
            int(sim_cmp.get("candidate") or 0),
            int(sim_cmp.get("assigned") or 0),
            int(sim_cmp.get("pending") or 0),
            int(sim_cmp.get("vehicles") or 0),
            float(sim_cmp.get("mileage") or 0.0),
            "success",
            "solver",
            _simulation_json_dumps({"compare": sim_cmp}),
        ),
    ]
    baseline_route_rows, baseline_stop_rows = _simulation_single_collect_plan_rows(
        task_key, "baseline", baseline_plans, baseline_store_map, target_shop_code
    )
    simulated_route_rows, simulated_stop_rows = _simulation_single_collect_plan_rows(
        task_key, "simulated", simulated_plans, simulated_store_map, target_shop_code
    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_ROUTE_STOP_TABLE} WHERE task_id=%s", (task_key,))
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_ROUTE_TABLE} WHERE task_id=%s", (task_key,))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_SNAPSHOT_TABLE} WHERE task_id=%s", (task_key,))
            cursor.executemany(
                f"""
                INSERT INTO {SIMULATION_SINGLE_SNAPSHOT_TABLE}
                (task_id, snapshot_type, wave_id, candidate_store_count, assigned_store_count,
                 pending_store_count, vehicle_count, total_mileage, solve_status, solver_source, raw_result_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                snapshot_rows,
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if baseline_route_rows or simulated_route_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_ROUTE_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, route_seq, store_count,
                     route_distance_km, solver_load_value, display_load_percent, start_time, end_time,
                     route_status, route_payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    baseline_route_rows + simulated_route_rows,
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if baseline_stop_rows or simulated_stop_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_ROUTE_STOP_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, stop_seq, shop_code, shop_name,
                     expected_arrival_time, scheduled_arrival_time, scheduled_leave_time, boxes, volume,
                     load_after_stop, is_target_shop, arrival_source, stop_payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    baseline_stop_rows + simulated_stop_rows,
                )


def _simulation_single_persist_iteration_snapshot(task_id, wave_id, compare_row, plans, store_map, target_shop_code=""):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key:
        return
    route_rows, stop_rows = _simulation_single_collect_plan_rows(
        task_key,
        "iterative",
        plans,
        store_map,
        target_shop_code,
    )
    snapshot_row = (
        task_key,
        "iterative",
        str(wave_id or "").strip() or None,
        int((compare_row or {}).get("candidate") or 0),
        int((compare_row or {}).get("assigned") or 0),
        int((compare_row or {}).get("pending") or 0),
        int((compare_row or {}).get("vehicles") or 0),
        float((compare_row or {}).get("mileage") or 0.0),
        "success",
        "solver",
        _simulation_json_dumps({"compare": compare_row or {}}),
    )
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"DELETE FROM {SIMULATION_SINGLE_ROUTE_STOP_TABLE} WHERE task_id=%s AND snapshot_type='iterative'",
                (task_key,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"DELETE FROM {SIMULATION_SINGLE_ROUTE_TABLE} WHERE task_id=%s AND snapshot_type='iterative'",
                (task_key,),
            )
            cursor.execute(
                f"DELETE FROM {SIMULATION_SINGLE_SNAPSHOT_TABLE} WHERE task_id=%s AND snapshot_type='iterative'",
                (task_key,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {SIMULATION_SINGLE_SNAPSHOT_TABLE}
                (task_id, snapshot_type, wave_id, candidate_store_count, assigned_store_count,
                 pending_store_count, vehicle_count, total_mileage, solve_status, solver_source, raw_result_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                snapshot_row,
            )
            if route_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_ROUTE_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, route_seq, store_count,
                     route_distance_km, solver_load_value, display_load_percent, start_time, end_time,
                     route_status, route_payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    route_rows,
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if stop_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_ROUTE_STOP_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, stop_seq, shop_code, shop_name,
                     expected_arrival_time, scheduled_arrival_time, scheduled_leave_time, boxes, volume,
                     load_after_stop, is_target_shop, arrival_source, stop_payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    stop_rows,
                )


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_route_key(wave_id, vehicle_no, trip_no):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return f"{str(wave_id or '').strip()}|{str(vehicle_no or '').strip()}|{int(_to_float_safe(trip_no, 1))}"


def _simulation_single_collect_assigned_codes(plans):
    assigned = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for plan in (plans or []):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(plan, dict):
            continue
        trips = plan.get("trips") if isinstance(plan.get("trips"), list) else []
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for trip in trips:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(trip, dict):
                continue
            route = trip.get("route") if isinstance(trip.get("route"), list) else []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for stop in route:
                if isinstance(stop, dict):
                    code = str(stop.get("shop_code") or stop.get("storeId") or stop.get("id") or stop.get("code") or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    code = str(stop or "").strip()
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if code:
                    assigned.add(code)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return assigned


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_collect_used_vehicles(plans):
    used = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for plan in (plans or []):
        if not isinstance(plan, dict):
            continue
        vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
        plate = str(vehicle.get("plateNo") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if plate:
            used.add(plate)
    return used


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_single_collect_locked_route_ids(plans):
    locked = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for plan in (plans or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not isinstance(plan, dict):
            continue
        wave_id = _simulate_normalize_wave_key(plan.get("waveId"))
        vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
        plate = str(vehicle.get("plateNo") or "").strip()
        trips = plan.get("trips") if isinstance(plan.get("trips"), list) else []
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        for trip in trips:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(trip, dict):
                continue
            locked.append(_simulation_single_route_key(wave_id, plate, trip.get("tripNo")))
    return locked


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_single_persist_confirmed_routes(task_id, route_comparison):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key or not isinstance(route_comparison, dict):
        return
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    rows = []
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for route in (route_comparison.get("old_routes") or []):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(route, dict):
            continue
        rows.append(
            (
                task_key,
                "baseline",
                str(route.get("wave") or "").strip() or None,
                str(route.get("vehicle") or "").strip() or None,
                int(_to_float_safe(route.get("trip"), 1)),
                float(route.get("total_distance") or 0.0),
                round(float(route.get("total_load") or 0.0) / 100.0, 6),
                len(route.get("stops") or []),
                _simulation_json_dumps(route),
                "system_ab_capture",
            )
        )
    new_route = route_comparison.get("new_route")
    if isinstance(new_route, dict):
        rows.append(
            (
                task_key,
                "simulated",
                str(new_route.get("wave") or "").strip() or None,
                str(new_route.get("vehicle") or "").strip() or None,
                int(_to_float_safe(new_route.get("trip"), 1)),
                float(new_route.get("total_distance") or 0.0),
                round(float(new_route.get("total_load") or 0.0) / 100.0, 6),
                len(new_route.get("stops") or []),
                _simulation_json_dumps(new_route),
                "system_ab_capture",
            )
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE} WHERE task_id=%s", (task_key,))
            if rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, route_distance_km,
                     route_load_value, store_count, route_payload_json, confirmed_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_persist_states(
    task_id,
    wave_id,
    source_vehicles,
    baseline_plans,
    simulated_plans,
    baseline_store_map,
    simulated_store_map,
):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    wave_key = str(wave_id or "").strip() or None
    if not task_key:
        return
    all_vehicle_rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for item in (source_vehicles or []):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(item, dict):
            continue
        plate = str(item.get("plateNo") or item.get("vehicleId") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if plate:
            all_vehicle_rows.append(item)
    baseline_used = _simulation_single_collect_used_vehicles(baseline_plans)
    simulated_used = _simulation_single_collect_used_vehicles(simulated_plans)
    baseline_assigned = _simulation_single_collect_assigned_codes(baseline_plans)
    simulated_assigned = _simulation_single_collect_assigned_codes(simulated_plans)
    baseline_store_rows = list((baseline_store_map or {}).values())
    simulated_store_rows = list((simulated_store_map or {}).values())

    initial_state = {
        "label": "initial_candidate_pool",
        "remaining_vehicle_json": all_vehicle_rows,
        "remaining_store_json": baseline_store_rows,
        "remaining_cargo_json": [
            {
                "shop_code": _simulate_extract_store_code(store),
                "boxes": _simulate_get_store_load(store),
            }
            for store in baseline_store_rows
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if _simulate_extract_store_code(store)
        ],
        "locked_route_ids_json": [],
        "state_summary": f"鍒濆寰呮眰瑙ｉ泦锛氳溅杈?{len(all_vehicle_rows)} 鍙帮紝鍊欓€夐棬搴?{len(baseline_store_rows)} 瀹躲€?,
    }
    baseline_remaining_vehicles = [
        item for item in all_vehicle_rows
        if str(item.get("plateNo") or item.get("vehicleId") or "").strip() not in baseline_used
    ]
    baseline_remaining_stores = [
        store for store in baseline_store_rows
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if _simulate_extract_store_code(store) not in baseline_assigned
    ]
    baseline_state = {
        "label": "baseline_remaining_after_solve",
        "remaining_vehicle_json": baseline_remaining_vehicles,
        "remaining_store_json": baseline_remaining_stores,
        "remaining_cargo_json": [
            {
                "shop_code": _simulate_extract_store_code(store),
                "boxes": _simulate_get_store_load(store),
            }
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for store in baseline_remaining_stores
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if _simulate_extract_store_code(store)
        ],
        "locked_route_ids_json": _simulation_single_collect_locked_route_ids(baseline_plans),
        "state_summary": f"鍩虹嚎姹傝В鍚庯細鍓╀綑杞﹁締 {len(baseline_remaining_vehicles)} 鍙帮紝鏈畨鎺掗棬搴?{len(baseline_remaining_stores)} 瀹躲€?,
    }
    simulated_remaining_vehicles = [
        item for item in all_vehicle_rows
        if str(item.get("plateNo") or item.get("vehicleId") or "").strip() not in simulated_used
    ]
    simulated_remaining_stores = [
        store for store in simulated_store_rows
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if _simulate_extract_store_code(store) not in simulated_assigned
    ]
    simulated_state = {
        "label": "simulated_remaining_after_solve",
        "remaining_vehicle_json": simulated_remaining_vehicles,
        "remaining_store_json": simulated_remaining_stores,
        "remaining_cargo_json": [
            {
                "shop_code": _simulate_extract_store_code(store),
                "boxes": _simulate_get_store_load(store),
            }
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for store in simulated_remaining_stores
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if _simulate_extract_store_code(store)
        ],
        "locked_route_ids_json": _simulation_single_collect_locked_route_ids(simulated_plans),
        "state_summary": f"妯℃嫙姹傝В鍚庯細鍓╀綑杞﹁締 {len(simulated_remaining_vehicles)} 鍙帮紝鏈畨鎺掗棬搴?{len(simulated_remaining_stores)} 瀹躲€?,
    }
    state_rows = [
        (task_key, None, 1, wave_key, _simulation_json_dumps(initial_state["remaining_vehicle_json"]), _simulation_json_dumps(initial_state["remaining_store_json"]), _simulation_json_dumps(initial_state["remaining_cargo_json"]), None, _simulation_json_dumps(initial_state["locked_route_ids_json"]), initial_state["state_summary"]),
        (task_key, None, 2, wave_key, _simulation_json_dumps(baseline_state["remaining_vehicle_json"]), _simulation_json_dumps(baseline_state["remaining_store_json"]), _simulation_json_dumps(baseline_state["remaining_cargo_json"]), None, _simulation_json_dumps(baseline_state["locked_route_ids_json"]), baseline_state["state_summary"]),
        (task_key, None, 3, wave_key, _simulation_json_dumps(simulated_state["remaining_vehicle_json"]), _simulation_json_dumps(simulated_state["remaining_store_json"]), _simulation_json_dumps(simulated_state["remaining_cargo_json"]), None, _simulation_json_dumps(simulated_state["locked_route_ids_json"]), simulated_state["state_summary"]),
    ]
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_STATE_TRANSITION_TABLE} WHERE task_id=%s", (task_key,))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(f"DELETE FROM {SIMULATION_SINGLE_REMAINING_STATE_TABLE} WHERE task_id=%s", (task_key,))
            cursor.executemany(
                f"""
                INSERT INTO {SIMULATION_SINGLE_REMAINING_STATE_TABLE}
                (task_id, parent_state_id, state_seq, wave_id, remaining_vehicle_json, remaining_store_json,
                 remaining_cargo_json, remaining_dist_matrix_ref, locked_route_ids_json, state_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                state_rows,
            )
            cursor.execute(
                f"SELECT id, state_seq FROM {SIMULATION_SINGLE_REMAINING_STATE_TABLE} WHERE task_id=%s ORDER BY state_seq",
                (task_key,),
            )
            id_rows = cursor.fetchall() or []
            id_by_seq = {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for row in id_rows:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if isinstance(row, dict):
                    id_by_seq[int(row.get("state_seq") or 0)] = int(row.get("id") or 0)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                elif isinstance(row, (list, tuple)) and len(row) >= 2:
                    id_by_seq[int(row[1] or 0)] = int(row[0] or 0)
            transition_rows = []
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if id_by_seq.get(1) and id_by_seq.get(2):
                transition_rows.append(
                    (
                        task_key,
                        id_by_seq.get(1),
                        id_by_seq.get(2),
                        "baseline_solve",
                        _simulation_json_dumps({"wave_id": wave_key}),
                    )
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if id_by_seq.get(2) and id_by_seq.get(3):
                transition_rows.append(
                    (
                        task_key,
                        id_by_seq.get(2),
                        id_by_seq.get(3),
                        "adjust_store_time",
                        _simulation_json_dumps({"wave_id": wave_key}),
                    )
                )
            if transition_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_STATE_TRANSITION_TABLE}
                    (task_id, from_state_id, to_state_id, action_type, action_payload_json)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    transition_rows,
                )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_single_load_route_detail(cursor, task_id, snapshot_type, vehicle_no, trip_no):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    cursor.execute(
        f"""
        SELECT shop_code
        FROM {SIMULATION_SINGLE_ROUTE_STOP_TABLE}
        WHERE task_id=%s AND snapshot_type=%s AND vehicle_no=%s AND trip_no=%s
        ORDER BY stop_seq ASC, id ASC
        """,
        (task_id, snapshot_type, vehicle_no, int(_to_float_safe(trip_no, 1))),
    )
    stop_codes = [str((row or {}).get("shop_code") or "").strip() for row in (cursor.fetchall() or []) if str((row or {}).get("shop_code") or "").strip()]
    dist = _simulation_single_load_distance_matrix(stop_codes)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    cursor.execute(
        f"""
        SELECT *
        FROM {SIMULATION_SINGLE_ROUTE_TABLE}
        WHERE task_id=%s AND snapshot_type=%s AND vehicle_no=%s AND trip_no=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (task_id, snapshot_type, vehicle_no, int(_to_float_safe(trip_no, 1))),
    )
    route_row = cursor.fetchone() or {}
    route_payload = _simulation_json_loads(route_row.get("route_payload_json"), {})
    if not isinstance(route_payload, dict):
        route_payload = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    cursor.execute(
        f"""
        SELECT *
        FROM {SIMULATION_SINGLE_ROUTE_STOP_TABLE}
        WHERE task_id=%s AND snapshot_type=%s AND vehicle_no=%s AND trip_no=%s
        ORDER BY stop_seq ASC, id ASC
        """,
        (task_id, snapshot_type, vehicle_no, int(_to_float_safe(trip_no, 1))),
    )
    stop_rows = cursor.fetchall() or []
    stops = []
    previous_stop_id = DC_ID
    max_load_after_stop = 0.0
    for stop in stop_rows:
        stop_id = str(stop.get("shop_code") or "").strip()
        leg_km = _simulation_get_leg_distance(dist, previous_stop_id, stop_id)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if stop_id:
            previous_stop_id = stop_id
        load_after_stop = float(_to_float_safe(stop.get("load_after_stop"), 0.0))
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if load_after_stop > max_load_after_stop:
            max_load_after_stop = load_after_stop
        stops.append(
            {
                "shop_code": stop_id,
                "shop_name": str(stop.get("shop_name") or "").strip(),
                "expected_time": str(stop.get("expected_arrival_time") or "--:--").strip() or "--:--",
                "scheduled_time": str(stop.get("scheduled_arrival_time") or "--:--").strip() or "--:--",
                "scheduled_leave_time": str(stop.get("scheduled_leave_time") or "--:--").strip() or "--:--",
                "arrival_source": str(stop.get("arrival_source") or "").strip() or None,
                "boxes": float(_to_float_safe(stop.get("boxes"), 0.0)),
                "volume": float(_to_float_safe(stop.get("volume"), 0.0)),
                "load_after_stop": load_after_stop,
                "is_target_shop": bool(stop.get("is_target_shop")),
                "route_leg_km": float(leg_km) if leg_km is not None else None,
                "route_leg_text": f"+{float(leg_km):.1f} km" if leg_km is not None else "--",
            }
        )
    actual_load_value = float(_to_float_safe(route_payload.get("actual_load_value"), 0.0))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if actual_load_value <= 0 and max_load_after_stop > 0:
        actual_load_value = max_load_after_stop
    return {
        "route_key": _simulation_single_manual_route_key(
            snapshot_type,
            str(route_row.get("wave_id") or "").strip(),
            str(route_row.get("vehicle_no") or vehicle_no or "").strip(),
            int(_to_float_safe(route_row.get("trip_no"), trip_no)),
        ),
        "vehicle": str(route_row.get("vehicle_no") or vehicle_no or "").strip(),
        "wave": str(route_row.get("wave_id") or "").strip(),
        "trip": int(_to_float_safe(route_row.get("trip_no"), trip_no)),
        "store_count": int(_to_float_safe(route_row.get("store_count"), len(stops))),
        "total_distance": float(_to_float_safe(route_row.get("route_distance_km"), 0.0)),
        "solver_load_value": float(_to_float_safe(route_row.get("solver_load_value"), 0.0)),
        "total_load": float(_to_float_safe(route_row.get("display_load_percent"), 0.0)),
        "actual_load_value": actual_load_value,
        "capacity_limit": float(_to_float_safe(route_payload.get("capacity_limit"), 1.2)),
        "start_time": str(route_row.get("start_time") or "--:--").strip() or "--:--",
        "end_time": str(route_row.get("end_time") or "--:--").strip() or "--:--",
        "stops": stops,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _simulation_single_load_distance_matrix(store_ids):
    node_ids = [DC_ID] + sorted({str(item or "").strip() for item in (store_ids or []) if str(item or "").strip()})
    dist = {node: {} for node in node_ids}
    if len(node_ids) <= 1:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return dist
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(node_ids))
            cursor.execute(
                f"""
                SELECT from_store_id, to_store_id, distance_km
                FROM {STORE_DISTANCE_TABLE}
                WHERE from_store_id IN ({placeholders}) AND to_store_id IN ({placeholders})
                """,
                tuple(node_ids + node_ids),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    for row in rows:
        src = str((row or {}).get("from_store_id") or "").strip()
        dst = str((row or {}).get("to_store_id") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if src not in dist or dst not in dist:
            continue
        km = _to_float_safe((row or {}).get("distance_km"), 0.0)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if km > 0:
            dist[src][dst] = float(km)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return dist


# 鍗曞簵鏃堕棿鎺ㄦ紨鐨勬暟鎹簱鍥炶鎺ュ彛銆?
# 鍓嶅彴寮圭獥銆佹姤鍛婂尯銆佸悗缁汉宸ョ‘璁?鍐嶄紭鍖栵紝閮戒紭鍏堜粠杩欓噷璇诲彇鐪熷疄钀藉簱缁撴灉锛岃€屼笉鏄彧鐪嬮娆¤繑鍥炲€笺€?
def simulate_single_report_get(payload):
    ensure_archive_tables()
    task_id = str((payload or {}).get("taskId") or (payload or {}).get("task_id") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_id:
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return 400, {"success": False, "error_code": "TASK_NOT_FOUND", "message": "task_id涓嶈兘涓虹┖"}

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_TASK_TABLE}
                WHERE task_id=%s
                LIMIT 1
                """,
                (task_id,),
            )
            task_row = cursor.fetchone() or {}
            if not task_row:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                return 404, {"success": False, "error_code": "TASK_NOT_FOUND", "message": f"鍗曞簵鎺ㄦ紨浠诲姟涓嶅瓨鍦? {task_id}"}

            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_SNAPSHOT_TABLE}
                WHERE task_id=%s
                ORDER BY id ASC
                """,
                (task_id,),
            )
            snapshot_rows = cursor.fetchall() or []

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE}
                WHERE task_id=%s
                ORDER BY snapshot_type ASC, id ASC
                """,
                (task_id,),
            )
            confirmed_rows = cursor.fetchall() or []

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_REMAINING_STATE_TABLE}
                WHERE task_id=%s
                ORDER BY state_seq DESC, id DESC
                LIMIT 1
                """,
                (task_id,),
            )
            latest_state_row = cursor.fetchone() or {}

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_ROUTE_TABLE}
                WHERE task_id=%s
                ORDER BY snapshot_type ASC, route_seq ASC, id ASC
                """,
                (task_id,),
            )
            route_rows = cursor.fetchall() or []

            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"""
                SELECT message
                FROM {SIMULATION_SINGLE_EVENT_LOG_TABLE}
                WHERE task_id=%s
                ORDER BY id ASC
                """,
                (task_id,),
            )
            event_rows = cursor.fetchall() or []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT *
                FROM {SIMULATION_SINGLE_STATE_TRANSITION_TABLE}
                WHERE task_id=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (task_id,),
            )
            latest_transition_row = cursor.fetchone() or {}

            snapshot_map = {}
            for row in snapshot_rows:
                snapshot_type = str(row.get("snapshot_type") or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if snapshot_type:
                    snapshot_map[snapshot_type] = row

            old_routes = []
            new_route = None
            anchor_keys = {"baseline": set(), "simulated": set()}
            route_decisions = {}
            manual_rows = []
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for row in confirmed_rows:
                confirmed_by = str(row.get("confirmed_by") or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if confirmed_by.startswith("manual_"):
                    manual_rows.append(row)
                    continue
                snapshot_type = str(row.get("snapshot_type") or "").strip()
                vehicle_no = str(row.get("vehicle_no") or "").strip()
                trip_no = int(_to_float_safe(row.get("trip_no"), 1))
                anchor_keys.setdefault(snapshot_type, set()).add((vehicle_no, trip_no))
                route_detail = _simulation_single_load_route_detail(cursor, task_id, snapshot_type, vehicle_no, trip_no)
                if snapshot_type == "baseline":
                    old_routes.append(route_detail)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                elif snapshot_type == "simulated" and not new_route:
                    new_route = route_detail

            for row in manual_rows:
                payload_json = _simulation_json_loads(row.get("route_payload_json"), {})
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not isinstance(payload_json, dict):
                    payload_json = {}
                route_key = _simulation_single_manual_route_key(
                    str(row.get("snapshot_type") or "").strip(),
                    str(row.get("wave_id") or "").strip(),
                    str(row.get("vehicle_no") or "").strip(),
                    int(_to_float_safe(row.get("trip_no"), 1)),
                )
                route_decisions[route_key] = {
                    "action": str(payload_json.get("decision_action") or "").strip() or ("confirm" if str(row.get("confirmed_by") or "").strip() == "manual_confirm" else "reoptimize"),
                    "checked": True,
                    "confirmed_by": str(row.get("confirmed_by") or "").strip(),
                }

            route_row_map = {}
            other_routes = {"baseline": [], "simulated": []}
            current_candidate_routes = []
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for row in route_rows:
                snapshot_type = str(row.get("snapshot_type") or "").strip()
                vehicle_no = str(row.get("vehicle_no") or "").strip()
                trip_no = int(_to_float_safe(row.get("trip_no"), 1))
                route_key = _simulation_single_manual_route_key(
                    snapshot_type,
                    str(row.get("wave_id") or "").strip(),
                    vehicle_no,
                    trip_no,
                )
                route_row_map[route_key] = row
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if (vehicle_no, trip_no) in anchor_keys.get(snapshot_type, set()):
                    continue
                if snapshot_type == "iterative":
                    route_detail = _simulation_single_load_route_detail(cursor, task_id, snapshot_type, vehicle_no, trip_no)
                    route_detail["route_decision"] = route_decisions.get(route_detail.get("route_key") or "", {})
                    current_candidate_routes.append(route_detail)
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if snapshot_type not in other_routes:
                    continue
                route_detail = _simulation_single_load_route_detail(cursor, task_id, snapshot_type, vehicle_no, trip_no)
                route_detail["route_decision"] = route_decisions.get(route_detail.get("route_key") or "", {})
                other_routes[snapshot_type].append(route_detail)

    stored_result = _simulation_json_loads(task_row.get("result_json"), {})
    result_data = deepcopy(stored_result if isinstance(stored_result, dict) else {})
    wave_id = str(
        (snapshot_map.get("simulated") or {}).get("wave_id")
        or (snapshot_map.get("baseline") or {}).get("wave_id")
        or task_row.get("affected_wave")
        or ""
    ).strip()
    baseline_row = snapshot_map.get("baseline") or {}
    simulated_row = snapshot_map.get("simulated") or {}
    ab_compare = {
        "wave_id": wave_id or None,
        "baseline": {
            "candidate": int(_to_float_safe(baseline_row.get("candidate_store_count"), 0)),
            "assigned": int(_to_float_safe(baseline_row.get("assigned_store_count"), 0)),
            "pending": int(_to_float_safe(baseline_row.get("pending_store_count"), 0)),
            "vehicles": int(_to_float_safe(baseline_row.get("vehicle_count"), 0)),
            "mileage": float(_to_float_safe(baseline_row.get("total_mileage"), 0.0)),
        },
        "simulated": {
            "candidate": int(_to_float_safe(simulated_row.get("candidate_store_count"), 0)),
            "assigned": int(_to_float_safe(simulated_row.get("assigned_store_count"), 0)),
            "pending": int(_to_float_safe(simulated_row.get("pending_store_count"), 0)),
            "vehicles": int(_to_float_safe(simulated_row.get("vehicle_count"), 0)),
            "mileage": float(_to_float_safe(simulated_row.get("total_mileage"), 0.0)),
        },
    }
    ab_compare["delta"] = {
        "candidate": ab_compare["simulated"]["candidate"] - ab_compare["baseline"]["candidate"],
        "assigned": ab_compare["simulated"]["assigned"] - ab_compare["baseline"]["assigned"],
        "pending": ab_compare["simulated"]["pending"] - ab_compare["baseline"]["pending"],
        "vehicles": ab_compare["simulated"]["vehicles"] - ab_compare["baseline"]["vehicles"],
        "mileage": round(ab_compare["simulated"]["mileage"] - ab_compare["baseline"]["mileage"], 3),
    }

    result_data["before"] = result_data.get("before") if isinstance(result_data.get("before"), dict) else {
        "total_vehicles": int(_to_float_safe(task_row.get("vehicle_count_before"), 0)),
        "total_mileage": float(_to_float_safe(task_row.get("mileage_before"), 0.0)),
    }
    result_data["after"] = result_data.get("after") if isinstance(result_data.get("after"), dict) else {
        "total_vehicles": int(_to_float_safe(task_row.get("vehicle_count_after"), 0)),
        "total_mileage": float(_to_float_safe(task_row.get("mileage_after"), 0.0)),
    }
    result_data["improvements"] = result_data.get("improvements") if isinstance(result_data.get("improvements"), dict) else {}
    result_data["ab_compare"] = ab_compare
    if isinstance(new_route, dict):
        new_route["route_decision"] = route_decisions.get(new_route.get("route_key") or "", {})
    result_data["route_comparison"] = {
        "shop_code": str(task_row.get("shop_code") or "").strip(),
        "old_routes": old_routes,
        "new_route": new_route,
        "other_routes": other_routes,
    }
    result_data["current_candidate_routes"] = current_candidate_routes
    result_data["route_decisions"] = route_decisions
    result_data["remaining_state_summary"] = {
        "state_seq": int(_to_float_safe(latest_state_row.get("state_seq"), 0)),
        "summary": str(latest_state_row.get("state_summary") or "").strip(),
        "locked_route_ids": _simulation_json_loads(latest_state_row.get("locked_route_ids_json"), []),
        "remaining_vehicle_count": len(_simulation_json_loads(latest_state_row.get("remaining_vehicle_json"), [])),
        "remaining_store_count": len(_simulation_json_loads(latest_state_row.get("remaining_store_json"), [])),
    }
    remaining_store_rows = _simulation_json_loads(latest_state_row.get("remaining_store_json"), [])
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not isinstance(remaining_store_rows, list):
        remaining_store_rows = []
    result_data["remaining_state_summary"]["remaining_stores"] = [
        {
            "shop_code": _simulate_extract_store_code(store),
            "shop_name": str((store or {}).get("shop_name") or (store or {}).get("name") or "").strip(),
            "expected_time": _simulate_get_store_time(store, wave_id),
        }
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for store in remaining_store_rows
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if _simulate_extract_store_code(store)
    ]
    latest_transition_payload = _simulation_json_loads(latest_transition_row.get("action_payload_json"), {})
    if not isinstance(latest_transition_payload, dict):
        latest_transition_payload = {}
    confirm_route_keys = latest_transition_payload.get("confirm_route_keys") or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(confirm_route_keys, list):
        confirm_route_keys = []
    reoptimize_route_keys = latest_transition_payload.get("reoptimize_route_keys") or []
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not isinstance(reoptimize_route_keys, list):
        reoptimize_route_keys = []
    released_vehicle_keys = latest_transition_payload.get("released_vehicle_keys") or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(released_vehicle_keys, list):
        released_vehicle_keys = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if str(latest_transition_row.get("action_type") or "").strip() == "manual_route_selection":
        hidden_route_keys = {str(item or "").strip() for item in (confirm_route_keys + reoptimize_route_keys) if str(item or "").strip()}
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if hidden_route_keys:
            current_candidate_routes = [
                item for item in current_candidate_routes
                if str((item or {}).get("route_key") or "").strip() not in hidden_route_keys
            ]
    result_data["manual_decision_summary"] = {
        "action_type": str(latest_transition_row.get("action_type") or "").strip(),
        "selected_count": int(latest_transition_payload.get("selected_count") or 0),
        "confirm_count": int(latest_transition_payload.get("confirm_count") or 0),
        "reoptimize_count": int(latest_transition_payload.get("reoptimize_count") or 0),
        "confirm_route_keys": confirm_route_keys,
        "released_vehicle_count": len(released_vehicle_keys),
        "released_vehicle_keys": released_vehicle_keys,
        "reoptimize_route_keys": reoptimize_route_keys,
    }
    result_data["task_audit"] = {
        "task_id": task_id,
        "status": str(task_row.get("status") or "").strip(),
        "source_batch_id": str(task_row.get("source_batch_id") or "").strip(),
        "shop_code": str(task_row.get("shop_code") or "").strip(),
        "shop_name": str(task_row.get("shop_name") or "").strip(),
        "original_wave": str(task_row.get("original_wave") or "").strip(),
        "simulated_wave": str(task_row.get("simulated_wave") or "").strip(),
        "original_time": str(task_row.get("original_time") or "").strip(),
        "simulated_time": str(task_row.get("simulated_time") or "").strip(),
        "affected_wave": str(task_row.get("affected_wave") or "").strip(),
        "algorithm_key": str(task_row.get("algorithm_key") or "").strip(),
        "summary_text": str(task_row.get("summary_text") or "").strip(),
        "created_at": task_row.get("created_at"),
        "finished_at": task_row.get("finished_at"),
    }
    result_data["event_log_lines"] = [str(row.get("message") or "") for row in event_rows]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return 200, {"success": True, "task_id": task_id, "data": result_data}


def _simulation_get_leg_distance(dist, from_id, to_id):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _safe_num(value):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        try:
            parsed = float(value)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if math.isfinite(parsed):
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                return parsed
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            pass
        return None

    fkey = str(from_id or "").strip()
    tkey = str(to_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(dist, dict):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return None
    row = dist.get(fkey)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if isinstance(row, dict):
        if tkey in row:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return _safe_num(row.get(tkey))
        if str(tkey) in row:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return _safe_num(row.get(str(tkey)))
    row = dist.get(str(fkey))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if isinstance(row, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if tkey in row:
            return _safe_num(row.get(tkey))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if str(tkey) in row:
            return _safe_num(row.get(str(tkey)))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return None


# 淇濆瓨鈥滅‘瀹?/ 鍐嶄紭鍖栤€濅汉宸ュ喅绛栥€?
# 纭畾鐨勭嚎璺細琚喕缁擄紱鍐嶄紭鍖栫殑绾胯矾浼氭媶鍥炲簵閾哄苟閲婃斁杞﹁締锛岀敓鎴愭柊鐨勫墿浣欐眰瑙ｉ泦銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def simulate_single_route_decisions_save(payload):
    ensure_archive_tables()
    task_id = str((payload or {}).get("taskId") or (payload or {}).get("task_id") or "").strip()
    decisions = (payload or {}).get("decisions") or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not task_id:
        return 400, {"success": False, "error_code": "TASK_NOT_FOUND", "message": "task_id涓嶈兘涓虹┖"}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(decisions, list):
        decisions = []

    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"SELECT * FROM {SIMULATION_SINGLE_TASK_TABLE} WHERE task_id=%s LIMIT 1", (task_id,))
            task_row = cursor.fetchone() or {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not task_row:
                return 404, {"success": False, "error_code": "TASK_NOT_FOUND", "message": f"鍗曞簵鎺ㄦ紨浠诲姟涓嶅瓨鍦? {task_id}"}

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            cursor.execute(
                f"SELECT * FROM {SIMULATION_SINGLE_ROUTE_TABLE} WHERE task_id=%s AND snapshot_type IN ('simulated','iterative') ORDER BY snapshot_type ASC, route_seq ASC, id ASC",
                (task_id,),
            )
            simulated_route_rows = cursor.fetchall() or []
            route_row_map = {}
            for row in simulated_route_rows:
                key = _simulation_single_manual_route_key(
                    str(row.get("snapshot_type") or "").strip(),
                    str(row.get("wave_id") or "").strip(),
                    str(row.get("vehicle_no") or "").strip(),
                    int(_to_float_safe(row.get("trip_no"), 1)),
                )
                route_row_map[key] = row

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"SELECT * FROM {SIMULATION_SINGLE_ROUTE_STOP_TABLE} WHERE task_id=%s AND snapshot_type IN ('simulated','iterative') ORDER BY snapshot_type ASC, trip_no ASC, stop_seq ASC, id ASC",
                (task_id,),
            )
            stop_rows = cursor.fetchall() or []
            stops_by_route = {}
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for row in stop_rows:
                key = _simulation_single_manual_route_key(
                    str(row.get("snapshot_type") or "").strip(),
                    str(row.get("wave_id") or "").strip(),
                    str(row.get("vehicle_no") or "").strip(),
                    int(_to_float_safe(row.get("trip_no"), 1)),
                )
                stops_by_route.setdefault(key, []).append(row)

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"SELECT * FROM {SIMULATION_SINGLE_REMAINING_STATE_TABLE} WHERE task_id=%s ORDER BY state_seq ASC, id ASC",
                (task_id,),
            )
            state_rows = cursor.fetchall() or []
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if not state_rows:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return 500, {"success": False, "error_code": "STATE_NOT_FOUND", "message": "鏈壘鍒板崟搴楁帹婕斿墿浣欓泦鐘舵€?}

            initial_state = state_rows[0]
            latest_state = state_rows[-1]
            base_state = latest_state if str(latest_state.get("state_summary") or "").startswith("浜哄伐鍐崇瓥鍚?) else initial_state
            latest_vehicles = _simulation_json_loads(base_state.get("remaining_vehicle_json"), [])
            latest_stores = _simulation_json_loads(base_state.get("remaining_store_json"), [])
            wave_id = str(base_state.get("wave_id") or task_row.get("affected_wave") or "").strip()

            decision_rows = []
            confirm_route_keys = []
            confirm_vehicle_keys = set()
            confirm_store_codes = set()
            reoptimize_route_keys = []
            reoptimize_vehicle_keys = set()
            reoptimize_store_codes = set()
            selected_count = 0
            confirm_count = 0
            reoptimize_count = 0
            for item in decisions:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not isinstance(item, dict):
                    continue
                checked = bool(item.get("checked"))
                route_key = str(item.get("route_key") or "").strip()
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not checked or not route_key or route_key not in route_row_map:
                    continue
                action = str(item.get("action") or "confirm").strip().lower()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if action not in ("confirm", "reoptimize"):
                    action = "confirm"
                selected_count += 1
                if action == "confirm":
                    confirm_count += 1
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    reoptimize_count += 1
                route_row = route_row_map[route_key]
                route_payload = _simulation_json_loads(route_row.get("route_payload_json"), {})
                if not isinstance(route_payload, dict):
                    route_payload = {}
                route_payload["decision_action"] = action
                route_payload["route_key"] = route_key
                decision_rows.append(
                    (
                        task_id,
                        str(route_row.get("snapshot_type") or "").strip() or "simulated",
                        str(route_row.get("wave_id") or "").strip() or None,
                        str(route_row.get("vehicle_no") or "").strip() or None,
                        int(_to_float_safe(route_row.get("trip_no"), 1)),
                        float(_to_float_safe(route_row.get("route_distance_km"), 0.0)),
                        float(_to_float_safe(route_row.get("solver_load_value"), 0.0)),
                        int(_to_float_safe(route_row.get("store_count"), 0)),
                        _simulation_json_dumps(route_payload),
                        f"manual_{action}",
                    )
                )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if action == "confirm":
                    confirm_route_keys.append(route_key)
                    confirm_vehicle_keys.add(str(route_row.get("vehicle_no") or "").strip())
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    for stop in (stops_by_route.get(route_key) or []):
                        code = str(stop.get("shop_code") or "").strip()
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        # EN: Verification point for backend data flow.
                        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                        if code:
                            confirm_store_codes.add(code)
                else:
                    reoptimize_route_keys.append(route_key)
                    vehicle_key = str(route_row.get("vehicle_no") or "").strip()
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if vehicle_key:
                        reoptimize_vehicle_keys.add(vehicle_key)
                    for stop in (stops_by_route.get(route_key) or []):
                        code = str(stop.get("shop_code") or "").strip()
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if code:
                            reoptimize_store_codes.add(code)

            vehicle_map = {}
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for item in (latest_vehicles or []):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if not isinstance(item, dict):
                    continue
                vehicle_key = str((item or {}).get("plateNo") or (item or {}).get("vehicleId") or "").strip()
                if vehicle_key:
                    vehicle_map[vehicle_key] = deepcopy(item)

            store_map = {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for store in (latest_stores or []):
                if not isinstance(store, dict):
                    continue
                shop_code = _simulate_extract_store_code(store)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if shop_code:
                    store_map[shop_code] = deepcopy(store)

            remaining_vehicles = [
                deepcopy(vehicle_map[vehicle_key])
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                for vehicle_key in sorted(reoptimize_vehicle_keys)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if vehicle_key in vehicle_map
            ]
            remaining_stores = [
                deepcopy(store_map[shop_code])
                for shop_code in sorted(reoptimize_store_codes)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if shop_code in store_map
            ]
            remaining_cargo = [
                {
                    "shop_code": _simulate_extract_store_code(store),
                    "boxes": _simulate_store_boxes_for_wave(store, wave_id),
                }
                for store in remaining_stores
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if _simulate_extract_store_code(store)
            ]
            state_summary = (
                f"浜哄伐鍐崇瓥鍚庯細宸查€?{selected_count} 鏉＄嚎璺紝鍏朵腑纭畾 {confirm_count} 鏉°€佸啀浼樺寲 {reoptimize_count} 鏉★紱"
                f"鍐荤粨杞﹁締 {len(confirm_vehicle_keys)} 鍙帮紝閲婃斁杞﹁締 {len(reoptimize_vehicle_keys)} 鍙帮紱"
                f"寰呭啀浼樺寲搴楅摵 {len(remaining_stores)} 瀹躲€?
            )

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"DELETE FROM {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE} WHERE task_id=%s AND confirmed_by LIKE 'manual_%%'",
                (task_id,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if decision_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_SINGLE_CONFIRMED_ROUTE_TABLE}
                    (task_id, snapshot_type, wave_id, vehicle_no, trip_no, route_distance_km,
                     route_load_value, store_count, route_payload_json, confirmed_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    decision_rows,
                )

            next_state_seq = int(_to_float_safe(latest_state.get("state_seq"), 0)) + 1
            parent_state_id = int(_to_float_safe(base_state.get("id"), 0)) or None
            cursor.execute(
                f"""
                INSERT INTO {SIMULATION_SINGLE_REMAINING_STATE_TABLE}
                (task_id, parent_state_id, state_seq, wave_id, remaining_vehicle_json, remaining_store_json,
                 remaining_cargo_json, remaining_dist_matrix_ref, locked_route_ids_json, state_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id,
                    parent_state_id,
                    next_state_seq,
                    wave_id or None,
                    _simulation_json_dumps(remaining_vehicles),
                    _simulation_json_dumps(remaining_stores),
                    _simulation_json_dumps(remaining_cargo),
                    None,
                    _simulation_json_dumps(confirm_route_keys),
                    state_summary,
                ),
            )
            new_state_id = cursor.lastrowid
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {SIMULATION_SINGLE_STATE_TRANSITION_TABLE}
                (task_id, from_state_id, to_state_id, action_type, action_payload_json)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    task_id,
                    parent_state_id,
                    new_state_id,
                    "manual_route_selection",
                    _simulation_json_dumps(
                        {
                            "selected_count": selected_count,
                            "confirm_count": confirm_count,
                            "reoptimize_count": reoptimize_count,
                            "confirm_route_keys": confirm_route_keys,
                            "reoptimize_route_keys": reoptimize_route_keys,
                            "released_vehicle_keys": sorted(list(reoptimize_vehicle_keys)),
                            "reoptimize_store_codes": sorted(list(reoptimize_store_codes)),
                        }
                    ),
                ),
            )

    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    return 200, {
        "success": True,
        "task_id": task_id,
        "data": {
            "selected_count": selected_count,
            "confirm_count": confirm_count,
            "reoptimize_count": reoptimize_count,
            "remaining_vehicle_count": len(remaining_vehicles),
            "remaining_store_count": len(remaining_stores),
            "released_vehicle_count": len(reoptimize_vehicle_keys),
            "state_summary": state_summary,
        },
    }


# 鍩轰簬浜哄伐鍐崇瓥鍚庣殑鍓╀綑闆嗙户缁湡姹傝В銆?
# 杩欎竴杞眰瑙ｄ笉鍐嶄娇鐢ㄦ棫鍊欓€夌嚎璺紝鑰屾槸浣跨敤鈥滃墿浣欏簵閾?+ 鍙啀鍒╃敤杞﹁締 + 鏂扮洰鏍団€濋噸鏂版眰瑙ｃ€?
# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def simulate_single_route_continue(payload):
    ensure_archive_tables()
    task_id = str((payload or {}).get("taskId") or (payload or {}).get("task_id") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_id:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return 400, {"success": False, "error_code": "TASK_NOT_FOUND", "message": "task_id涓嶈兘涓虹┖"}

    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {SIMULATION_SINGLE_TASK_TABLE} WHERE task_id=%s LIMIT 1", (task_id,))
            task_row = cursor.fetchone() or {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not task_row:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return 404, {"success": False, "error_code": "TASK_NOT_FOUND", "message": f"鍗曞簵鎺ㄦ紨浠诲姟涓嶅瓨鍦? {task_id}"}

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"SELECT * FROM {SIMULATION_SINGLE_REMAINING_STATE_TABLE} WHERE task_id=%s ORDER BY state_seq DESC, id DESC LIMIT 1",
                (task_id,),
            )
            latest_state = cursor.fetchone() or {}
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if not latest_state:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return 500, {"success": False, "error_code": "STATE_NOT_FOUND", "message": "鏈壘鍒板墿浣欐眰瑙ｉ泦鐘舵€?}

    task_payload = _simulation_json_loads(task_row.get("payload_json"), {})
    if not isinstance(task_payload, dict):
        task_payload = {}
    solve_target = (payload or {}).get("solve_target") or {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(solve_target, dict):
        solve_target = {}
    realtime_context = _simulate_extract_realtime_context(task_payload)
    batch_id = str(task_row.get("source_batch_id") or task_payload.get("batch_id") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not batch_id:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return 400, {"success": False, "error_code": "INVALID_BATCH_ID", "message": "缂哄皯婧愭壒娆D"}

    archive = archive_get({"id": batch_id})
    if not archive.get("found") or not isinstance(archive.get("item"), dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return 404, {"success": False, "error_code": "INVALID_BATCH_ID", "message": f"鎵规ID涓嶅瓨鍦? {batch_id}"}
    snapshot = deepcopy(archive.get("item") or {})

    remaining_stores = _simulation_json_loads(latest_state.get("remaining_store_json"), [])
    remaining_vehicles = _simulation_json_loads(latest_state.get("remaining_vehicle_json"), [])
    if not isinstance(remaining_stores, list):
        remaining_stores = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(remaining_vehicles, list):
        remaining_vehicles = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not remaining_stores:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return 200, {
            "success": True,
            "task_id": task_id,
            "data": {
                "message": "褰撳墠宸叉棤寰呰皟搴﹂棬搴楋紝鏃犻渶缁х画姹傝В",
                "remaining_vehicle_count": len(remaining_vehicles),
                "remaining_store_count": 0,
            },
        }
    if not remaining_vehicles:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return 400, {"success": False, "error_code": "VEHICLES_EMPTY", "message": "褰撳墠鍓╀綑闆嗗凡鏃犲彲鐢ㄨ溅杈?}

    reduce_vehicle_count = int(_to_float_safe(solve_target.get("reduce_vehicle_count"), 0))
    min_stores_per_vehicle = int(_to_float_safe(solve_target.get("min_stores_per_vehicle"), 0))
    min_load_rate = float(_to_float_safe(solve_target.get("min_load_rate"), 0.0))
    if reduce_vehicle_count > 0:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if reduce_vehicle_count >= len(remaining_vehicles):
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return 400, {"success": False, "error_code": "INVALID_VEHICLE_COUNT", "message": "鍑忓皯杞﹁締鏁颁笉鑳藉ぇ浜庣瓑浜庡綋鍓嶅彲鐢ㄨ溅杈嗘暟"}
        remaining_vehicles = remaining_vehicles[: max(1, len(remaining_vehicles) - reduce_vehicle_count)]

    wave_id = _simulate_normalize_wave_key(latest_state.get("wave_id") or task_row.get("affected_wave"))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not wave_id:
        return 400, {"success": False, "error_code": "INVALID_WAVE", "message": "褰撳墠鍓╀綑闆嗙己灏戞尝娆′俊鎭?}

    source_waves = deepcopy(realtime_context.get("waves") or snapshot.get("waves") or [])
    source_settings = deepcopy(realtime_context.get("settings") or snapshot.get("settings") or {})
    source_strategy_config = deepcopy(realtime_context.get("strategyConfig") or {})
    source_algorithm_key = str(
        realtime_context.get("algorithmKey")
        or realtime_context.get("activeResultKey")
        or task_row.get("algorithm_key")
        or snapshot.get("activeResultKey")
        or "vehicle"
    ).strip().lower() or "vehicle"
    target = str(task_payload.get("target") or task_row.get("target") or "min_vehicles").strip() or "min_vehicles"

    wave_obj = _simulate_find_wave({"waves": source_waves}, wave_id)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(wave_obj, dict):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return 400, {"success": False, "error_code": "INVALID_WAVE", "message": f"娉㈡涓嶅瓨鍦? {wave_id}"}

    scenario = _simulate_build_scenario(
        {
            "scenario": realtime_context.get("scenario") if isinstance(realtime_context.get("scenario"), dict) else {},
            "settings": source_settings,
            "waves": source_waves,
        }
    )
    prepared_stores = _simulate_prepare_solver_stores(remaining_stores, wave_id, scenario.get("dispatchStartMin"))
    wave_payload = {
        "algorithmKey": source_algorithm_key,
        "scenario": scenario,
        "stores": prepared_stores,
        "vehicles": deepcopy(remaining_vehicles),
        "waves": [deepcopy(wave_obj)],
        "settings": deepcopy(source_settings),
        "strategyConfig": deepcopy(source_strategy_config),
        "dist": {},
        "wave": wave_obj,
        "goal": target,
        "skipResolvedStoreOverlay": True,
    }
    wave_payload["settings"]["optimizeGoal"] = str(target or wave_payload["settings"].get("optimizeGoal") or "min_vehicles")

    _simulation_task_append(
        task_id,
        f"缁х画姹傝В鍓╀綑闆嗭細娉㈡ {wave_id}锛屽墿浣欒溅杈?{len(remaining_vehicles)} 鍙帮紝寰呰皟搴﹂棬搴?{len(prepared_stores)} 瀹讹紝鐩爣 鍑忚溅 {reduce_vehicle_count} 鍙?/ 姣忚溅鑷冲皯 {min_stores_per_vehicle} 搴?/ 瑁呰浇鐜囦笉浣庝簬 {min_load_rate:.1f}% 銆?
    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with _capture_simulation_task_stream(task_id):
            solve_result, solved_payload, strategy_audit = _run_wave_optimize_with_same_logic(wave_payload)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(strategy_audit, dict):
            _simulation_task_append(
                task_id,
                f"缁х画姹傝В-绛栫暐涓績瀹¤锛坽source_algorithm_key} {wave_id}锛夛細杈撳叆闂ㄥ簵 {int(strategy_audit.get('inputStoreCount') or 0)} 瀹讹紝绛栫暐鍚?{int(strategy_audit.get('outputStoreCount') or 0)} 瀹躲€?
            )
        best_state = solve_result.get("bestState") if isinstance(solve_result, dict) else None
        if not isinstance(best_state, list):
            raise ValueError(f"{wave_id} 鍓╀綑闆嗙户缁眰瑙ｅけ璐?)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    except Exception as error:
        traceback.print_exc()
        return 500, {"success": False, "error_code": "RESCHEDULE_FAILED", "message": f"缁х画姹傝В澶辫触: {error}"}

    assigned_codes = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for state_row in best_state:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(state_row, dict):
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for route in (state_row.get("routes") or []):
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for store_id in (route or []):
                code = str(store_id or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if code:
                    assigned_codes.add(code)
    candidate_codes = {
        _simulate_extract_store_code(store)
        for store in prepared_stores
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if _simulate_extract_store_code(store)
    }
    pending_codes = {sid for sid in candidate_codes if sid not in assigned_codes}
    compare_row = {
        "candidate": int(len(candidate_codes)),
        "assigned": int(len(assigned_codes)),
        "pending": int(len(pending_codes)),
        "vehicles": int(len({str((item or {}).get('plateNo') or '').strip() for item in best_state if isinstance(item, dict) and str((item or {}).get('plateNo') or '').strip()})),
        "mileage": round(sum(item["mileage"] for item in _simulate_wave_stats_from_plans(
            _simulate_convert_best_state_to_plans(best_state, wave_id, solved_payload.get("dist") or {}, solved_payload, solve_result)
        ).values()), 1),
    }
    iterative_plans = _simulate_convert_best_state_to_plans(
        best_state,
        wave_id,
        solved_payload.get("dist") or {},
        solved_payload,
        solve_result,
    )
    remaining_store_map = {
        _simulate_extract_store_code(store): store
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for store in (remaining_stores or [])
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if _simulate_extract_store_code(store)
    }
    target_shop_code = str(task_row.get("shop_code") or "").strip()
    _simulation_single_persist_iteration_snapshot(
        task_id,
        wave_id,
        compare_row,
        iterative_plans,
        remaining_store_map,
        target_shop_code,
    )
    _simulation_single_persist_event_logs(task_id)
    _simulation_task_append(
        task_id,
        f"缁х画姹傝В瀹屾垚锛歝andidate={compare_row['candidate']}锛宎ssigned={compare_row['assigned']}锛宲ending={compare_row['pending']}锛岃溅杈?{compare_row['vehicles']} 鍙帮紝閲岀▼ {compare_row['mileage']:.1f} km銆?
    )
    if min_stores_per_vehicle > 0 or min_load_rate > 0:
        low_quality_count = 0
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for plan in (iterative_plans or []):
            trips = plan.get("trips") if isinstance(plan.get("trips"), list) else []
            vehicle = plan.get("vehicle") if isinstance(plan.get("vehicle"), dict) else {}
            capacity = float(_to_float_safe(vehicle.get("capacity"), 1.2))
            for trip in trips:
                route = trip.get("route") if isinstance(trip.get("route"), list) else []
                stop_count = len(route)
                total_boxes = 0.0
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for stop in route:
                    code = str((stop or {}).get("shop_code") or stop or "").strip() if isinstance(stop, dict) else str(stop or "").strip()
                    store_obj = remaining_store_map.get(code) or {}
                    total_boxes += float(_simulate_store_boxes_for_wave(store_obj, wave_id) or 0.0)
                load_percent = (total_boxes / capacity) * 100.0 if capacity > 0 else 0.0
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if (min_stores_per_vehicle > 0 and stop_count < min_stores_per_vehicle) or (min_load_rate > 0 and load_percent < min_load_rate):
                    low_quality_count += 1
        _simulation_task_append(
            task_id,
            f"缁х画姹傝В鍚庣殑鐩爣鏍￠獙锛氭湭杈剧洰鏍囩嚎璺?{low_quality_count} 鏉★紙闂ㄥ簵鏁颁笅闄?{min_stores_per_vehicle}锛岃杞界巼涓嬮檺 {min_load_rate:.1f}%锛夈€?
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return 200, {
        "success": True,
        "task_id": task_id,
        "data": {
            "wave_id": wave_id,
            "candidate": compare_row["candidate"],
            "assigned": compare_row["assigned"],
            "pending": compare_row["pending"],
            "vehicles": compare_row["vehicles"],
            "mileage": compare_row["mileage"],
            "solve_target": {
                "reduce_vehicle_count": reduce_vehicle_count,
                "min_stores_per_vehicle": min_stores_per_vehicle,
                "min_load_rate": min_load_rate,
            },
        },
    }


def _simulation_route_distance(route, dist):
    route_ids = [str(x or "").strip() for x in (route or []) if str(x or "").strip()]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not route_ids:
        return 0.0
    total = 0.0
    current = "DC"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for sid in route_ids:
        leg = _simulation_get_leg_distance(dist, current, sid)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if leg is None:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return 0.0
        total += float(leg)
        current = sid
    back = _simulation_get_leg_distance(dist, current, "DC")
    if back is not None:
        total += float(back)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return round(total, 1)


# EN: Verification point for backend data flow.
# CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
def _simulation_route_load(route, stores):
    total = 0.0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for sid in (route or []):
        store = stores.get(str(sid or "").strip()) or {}
        total += _to_float_safe(store.get("boxes"), 0.0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return round(total, 3)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _simulation_collect_insert_trials(task_id, batch_id, wave_id, payload, solve_result):
    stores_list = payload.get("stores") if isinstance(payload.get("stores"), list) else []
    stores = {}
    for item in stores_list:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(item, dict):
            continue
        sid = _debug_norm_store_id(item.get("id"))
        if sid:
            stores[sid] = _normalize_numeric_types_for_solver(item)
    best_state = solve_result.get("bestState") if isinstance(solve_result, dict) else None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if best_state is None and isinstance(solve_result, dict):
        best_state = solve_result.get("best_state")
    vehicles_state = best_state if isinstance(best_state, list) else []
    assigned = _extract_assigned_store_ids(vehicles_state)
    missing_ids = [sid for sid in stores.keys() if _debug_norm_store_id(sid) not in assigned]
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not missing_ids:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return [], []

    wave = _normalize_numeric_types_for_solver(payload.get("wave")) if isinstance(payload.get("wave"), dict) else {}
    scenario = _normalize_numeric_types_for_solver(payload.get("scenario")) if isinstance(payload.get("scenario"), dict) else {}
    dist = _normalize_numeric_types_for_solver(payload.get("dist")) if isinstance(payload.get("dist"), dict) else {}
    valid_store_ids = set(stores.keys())
    trial_rows = []
    store_rows = []

    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    def _sanitize_routes(routes):
        cleaned_routes = []
        seen = set()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for route in routes if isinstance(routes, list) else []:
            if not isinstance(route, list):
                continue
            cleaned = []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for raw_sid in route:
                sid2 = _debug_norm_store_id(raw_sid)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not sid2 or sid2 not in valid_store_ids or sid2 in seen:
                    continue
                seen.add(sid2)
                cleaned.append(sid2)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if cleaned:
                cleaned_routes.append(cleaned)
        return cleaned_routes

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _map_failure(vtype):
        reason = _violation_to_reason(vtype)
        if reason == "arrival":
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return "arrival_window"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if reason == "wave":
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return "wave_end"
        return reason

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _failure_type_zh(ftype):
        mapping = {
            "arrival_window": "鏃堕棿绐楄秴鏃?,
            "wave_end": "娉㈡缁撴潫瓒呮椂",
            "mileage": "閲岀▼瓒呴檺",
            "capacity": "瑁呰浇瓒呴檺",
            "max_stops": "闂ㄥ簵鏁拌秴闄?,
            "slot": "鏃犲彲琛屾彃鍏ヤ綅",
        }
        return mapping.get(str(ftype or "").strip(), "绾︽潫涓嶆弧瓒?)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    def _violation_explain(ftype, violation_obj, route_distance_km, route_load_val):
        v = violation_obj if isinstance(violation_obj, dict) else {}
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if ftype == "arrival_window":
            arr = _simulate_minutes_to_hhmm(_to_float_safe(v.get("arrivalMin"), 0.0)) if v.get("arrivalMin") is not None else "--:--"
            lat = _simulate_minutes_to_hhmm(_to_float_safe(v.get("latestAllowedMin"), 0.0)) if v.get("latestAllowedMin") is not None else "--:--"
            late = int(_to_float_safe(v.get("lateMinutes"), 0.0)) if v.get("lateMinutes") is not None else 0
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return f"棰勮鍒拌揪 {arr}锛屾渶鏅氬厑璁?{lat}锛岃秴鏃?{late} 鍒嗛挓"
        if ftype == "wave_end":
            over = _calc_wave_end_over_minutes(v)
            finish_txt = "--:--"
            end_txt = "--:--"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if v.get("finishMin") is not None:
                finish_txt = _simulate_minutes_to_hhmm(_to_float_safe(v.get("serviceEndMin"), _to_float_safe(v.get("finishMin"), 0.0)))
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            if v.get("waveEndMin") is not None:
                end_txt = _simulate_minutes_to_hhmm(_to_float_safe(v.get("waveEndMin"), 0.0))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return f"褰撳墠缁撴潫 {finish_txt}锛屾尝娆℃埅姝?{end_txt}锛岃秴鍑?{over} 鍒嗛挓"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if ftype == "mileage":
            current_km = _to_float_safe(v.get("routeKm"), route_distance_km)
            max_km = _to_float_safe(v.get("maxKm"), 240.0)
            exceed = max(0.0, round(current_km - max_km, 1))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return f"绾胯矾閲岀▼ {current_km:.1f} km锛岄檺鍒?{max_km:.1f} km锛岃秴鍑?{exceed:.1f} km"
        if ftype == "capacity":
            current_load = _to_float_safe(v.get("currentLoad"), route_load_val)
            limit_load = _to_float_safe(v.get("capacityLimit"), 1.0)
            exceed = max(0.0, round(current_load - limit_load, 3))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            return f"瑁呰浇 {current_load:.3f}锛岄檺鍒?{limit_load:.3f}锛岃秴鍑?{exceed:.3f}"
        if ftype == "max_stops":
            current_stops = int(_to_float_safe(v.get("currentStops"), 0.0))
            limit_stops = int(_to_float_safe(v.get("maxStops"), 0.0))
            exceed = max(0, current_stops - limit_stops)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return f"鍋滈潬闂ㄥ簵 {current_stops} 涓紝闄愬埗 {limit_stops} 涓紝瓒呭嚭 {exceed} 涓?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "璇ユ彃鍏ヤ綅缃笉婊¤冻绾︽潫"

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _calc_wave_end_over_minutes(violation_obj):
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if not isinstance(violation_obj, dict):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return 0
        start_min = _to_float_safe(wave.get("startMin"), 0.0)
        end_min = _to_float_safe(violation_obj.get("waveEndMin"), _to_float_safe(wave.get("endMin"), 0.0))
        finish_min = _to_float_safe(
            violation_obj.get("serviceEndMin"),
            _to_float_safe(violation_obj.get("finishMin"), 0.0),
        )
        overnight = end_min < start_min
        normalized_end = end_min + 1440.0 if overnight else end_min
        normalized_finish = finish_min
        if overnight and normalized_finish < start_min:
            normalized_finish += 1440.0
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return max(0, int(round(normalized_finish - normalized_end)))

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for sid in missing_ids:
        store = stores.get(sid) or {}
        shop_name = str(store.get("name") or store.get("shop_name") or "").strip()
        trial_no = 0
        failed_count = 0
        success_count = 0
        best_failure_type = ""
        best_violation_type = ""
        _simulation_task_append(
            task_id,
            f"[{wave_id}] 搴梴sid} 寮€濮嬪皾璇曟彃鍏ワ紝褰撳墠鏃堕棿 {str(_simulate_minutes_to_hhmm(_to_float_safe(store.get('desiredArrivalMin'), 0.0)) or '--')}锛屾渶鏅?{str(_simulate_minutes_to_hhmm(_to_float_safe(store.get('latestAllowedArrivalMin'), 0.0)) or '--')}銆?,
        )

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        for vehicle in vehicles_state:
            if not isinstance(vehicle, dict):
                continue
            base_routes = _sanitize_routes(vehicle.get("routes") if isinstance(vehicle.get("routes"), list) else [])
            plate = str(vehicle.get("plateNo") or vehicle.get("vehicleId") or "").strip() or "--"

            candidate_variants = [(-1, 0, base_routes + [[sid]])]
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for ridx, route in enumerate(base_routes):
                if not isinstance(route, list):
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for pos in range(len(route) + 1):
                    new_routes = [list(r) if isinstance(r, list) else [] for r in base_routes]
                    new_routes[ridx].insert(pos, sid)
                    candidate_variants.append((ridx, pos, new_routes))

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            for route_index, insert_pos, variant_routes in candidate_variants:
                trial_no += 1
                probe = dict(vehicle)
                probe["routes"] = variant_routes
                checked = check_plan_constraints(probe, stores, dist, wave, scenario)
                feasible = bool(checked.get("feasible"))
                violations = checked.get("violations") or []
                violation = violations[0] if isinstance(violations, list) and violations else {}
                violation_type = str((violation or {}).get("type") or "").strip().lower()
                failure_type = "success" if feasible else _map_failure(violation_type)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if feasible:
                    success_count += 1
                else:
                    failed_count += 1
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if not best_failure_type:
                        best_failure_type = failure_type
                        best_violation_type = violation_type

                candidate_route = []
                route_before = []
                if route_index >= 0 and route_index < len(base_routes):
                    route_before = base_routes[route_index]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if route_index >= 0 and route_index < len(variant_routes):
                    candidate_route = variant_routes[route_index]
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                elif route_index < 0 and len(variant_routes) > len(base_routes):
                    candidate_route = variant_routes[-1]

                route_distance = _simulation_route_distance(candidate_route, dist)
                route_load = _simulation_route_load(candidate_route, stores)
                expected_arrival = None
                latest_allowed = _simulate_minutes_to_hhmm(_to_float_safe(store.get("latestAllowedArrivalMin"), 0.0))
                late_minutes = 0
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if isinstance(violation, dict):
                    expected_arrival = _simulate_minutes_to_hhmm(_to_float_safe(violation.get("arrivalMin"), 0.0)) if violation.get("arrivalMin") is not None else None
                    if violation.get("latestAllowedMin") is not None:
                        latest_allowed = _simulate_minutes_to_hhmm(_to_float_safe(violation.get("latestAllowedMin"), 0.0))
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if violation.get("lateMinutes") is not None:
                        late_minutes = int(_to_float_safe(violation.get("lateMinutes"), 0.0))
                    elif violation.get("waveEndMin") is not None and violation.get("finishMin") is not None:
                        late_minutes = _calc_wave_end_over_minutes(violation)

                trial_rows.append(
                    {
                        "task_id": task_id,
                        "batch_id": batch_id,
                        "wave_id": wave_id,
                        "shop_code": sid,
                        "shop_name": shop_name,
                        "trial_no": trial_no,
                        "target_vehicle": plate,
                        "route_index": route_index if route_index >= 0 else None,
                        "insert_pos": insert_pos if route_index >= 0 else None,
                        "result_status": "success" if feasible else "failed",
                        "failure_type": None if feasible else failure_type,
                        "violation_type": None if feasible else violation_type,
                        "expected_arrival": expected_arrival,
                        "latest_allowed": latest_allowed,
                        "late_minutes": late_minutes,
                        "route_distance_km": route_distance,
                        "route_load": route_load,
                        "route_before": route_before,
                        "route_after": candidate_route,
                        "violation_json": violation if isinstance(violation, dict) else {},
                    }
                )

                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                if feasible:
                    _simulation_task_append(
                        task_id,
                        f"[{wave_id}] 搴梴sid} 灏濊瘯#{trial_no} 鎴愬姛 -> 杞﹁締 {plate}锛岀嚎璺?{(route_index + 1) if route_index >= 0 else '鏂板缓'}锛屾彃鍏ヤ綅 {insert_pos if route_index >= 0 else 0}锛岄噷绋?{route_distance} km锛岃杞?{route_load}銆?,
                    )
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    _simulation_task_append(
                        task_id,
                        f"[{wave_id}] 搴梴sid} 灏濊瘯#{trial_no} 澶辫触 -> 杞﹁締 {plate}锛寋_failure_type_zh(failure_type)}锛泏_violation_explain(failure_type, violation, route_distance, route_load)}銆?,
                    )

        store_rows.append(
            {
                "task_id": task_id,
                "batch_id": batch_id,
                "wave_id": wave_id,
                "shop_code": sid,
                "shop_name": shop_name,
                "final_status": "failed" if failed_count > 0 and success_count == 0 else "partial",
                "best_failure_type": best_failure_type or None,
                "best_violation_type": best_violation_type or None,
                "trial_count": trial_no,
                "success_count": success_count,
                "failed_count": failed_count,
            }
        )
        _simulation_task_append(
            task_id,
            f"[{wave_id}] 搴梴sid} 灏濊瘯缁撴潫锛氭€诲皾璇?{trial_no}锛屾垚鍔?{success_count}锛屽け璐?{failed_count}锛屼富绾︽潫 {_failure_type_zh(best_failure_type) if best_failure_type else '--'}銆?,
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return store_rows, trial_rows


def _save_simulation_insert_trials(task_id, batch_id, wave_id, store_rows, trial_rows):
    ensure_archive_tables()
    task_key = str(task_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not task_key:
        return
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {SIMULATION_STORE_ATTEMPT_TABLE} WHERE task_id=%s AND wave_id=%s", (task_key, str(wave_id or "").strip()))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {SIMULATION_INSERT_TRIAL_TABLE} WHERE task_id=%s AND wave_id=%s", (task_key, str(wave_id or "").strip()))

            if store_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_STORE_ATTEMPT_TABLE}
                    (task_id, batch_id, wave_id, shop_code, shop_name, final_status, best_failure_type,
                     best_violation_type, trial_count, success_count, failed_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            task_key,
                            str(batch_id or "").strip(),
                            str(row.get("wave_id") or "").strip(),
                            str(row.get("shop_code") or "").strip(),
                            str(row.get("shop_name") or "").strip() or None,
                            str(row.get("final_status") or "failed").strip(),
                            str(row.get("best_failure_type") or "").strip() or None,
                            str(row.get("best_violation_type") or "").strip() or None,
                            int(row.get("trial_count") or 0),
                            int(row.get("success_count") or 0),
                            int(row.get("failed_count") or 0),
                        )
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        for row in store_rows
                        # EN: Verification point for backend data flow.
                        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                        if isinstance(row, dict)
                    ],
                )

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if trial_rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {SIMULATION_INSERT_TRIAL_TABLE}
                    (task_id, batch_id, wave_id, shop_code, shop_name, trial_no, target_vehicle, route_index, insert_pos,
                     result_status, failure_type, violation_type, expected_arrival, latest_allowed, late_minutes,
                     route_distance_km, route_load, route_before, route_after, violation_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            task_key,
                            str(batch_id or "").strip(),
                            str(row.get("wave_id") or "").strip(),
                            str(row.get("shop_code") or "").strip(),
                            str(row.get("shop_name") or "").strip() or None,
                            int(row.get("trial_no") or 0),
                            str(row.get("target_vehicle") or "").strip() or None,
                            row.get("route_index"),
                            row.get("insert_pos"),
                            str(row.get("result_status") or "failed").strip(),
                            str(row.get("failure_type") or "").strip() or None,
                            str(row.get("violation_type") or "").strip() or None,
                            str(row.get("expected_arrival") or "").strip() or None,
                            str(row.get("latest_allowed") or "").strip() or None,
                            int(row.get("late_minutes") or 0),
                            float(row.get("route_distance_km") or 0.0),
                            float(row.get("route_load") or 0.0),
                            _simulation_json_dumps(row.get("route_before") or []),
                            _simulation_json_dumps(row.get("route_after") or []),
                            _simulation_json_dumps(row.get("violation_json") or {}),
                        )
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        for row in trial_rows
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if isinstance(row, dict)
                    ],
                )


def _clear_simulation_failure_logs(batch_id):
    ensure_archive_tables()
    batch_key = str(batch_id or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not batch_key:
        return
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {SIMULATION_FAILURE_LOG_TABLE} WHERE batch_id=%s", (batch_key,))


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _save_simulation_failure_logs(batch_id, rows):
    ensure_archive_tables()
    batch_key = str(batch_id or "").strip()
    normalized_rows = []
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    for row in rows if isinstance(rows, list) else []:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(row, dict):
            continue
        shop_code = str(row.get("shop_code") or "").strip()
        if not shop_code:
            continue
        route_stops = row.get("route_stops")
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            route_stops_json = json.dumps(route_stops or [], ensure_ascii=False)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            route_stops_json = "[]"
        normalized_rows.append(
            (
                batch_key,
                str(row.get("wave_id") or "").strip() or None,
                shop_code,
                str(row.get("target_vehicle") or "").strip() or None,
                str(row.get("failure_type") or "").strip() or None,
                str(row.get("current_time") or "").strip() or None,
                str(row.get("expected_arrival") or "").strip() or None,
                str(row.get("latest_allowed") or "").strip() or None,
                int(row.get("late_minutes") or 0),
                route_stops_json,
            )
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not normalized_rows:
        return
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {SIMULATION_FAILURE_LOG_TABLE}
                (batch_id, wave_id, shop_code, target_vehicle, failure_type, current_time_text,
                 expected_arrival, latest_allowed, late_minutes, route_stops)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                normalized_rows,
            )


# 鍗曞簵鏃堕棿鎺ㄦ紨涓诲叆鍙ｃ€?
# 璇ユ帴鍙ｄ細瀵瑰悓涓€娉㈡鍋氬熀绾挎眰瑙ｄ笌妯℃嫙姹傝В锛屽啀鎶?A/B 宸紓銆佺嚎璺鎯呫€佹棩蹇楀拰钀藉簱缁撴灉涓€璧疯繑鍥炪€?
def simulate_optimize_time(payload):
    task_id = str((payload or {}).get("task_id") or f"sim_{int(time.time() * 1000)}").strip()
    _simulation_task_init(task_id)
    batch_id = str((payload or {}).get("batch_id") or "").strip()
    mode = str((payload or {}).get("mode") or "single_store").strip().lower() or "single_store"
    adjustments = (payload or {}).get("adjustments") or []
    target = str((payload or {}).get("target") or "min_vehicles").strip() or "min_vehicles"
    target_vehicle_count = int(_to_float_safe((payload or {}).get("target_vehicle_count"), 0.0))
    selected_waves_raw = (payload or {}).get("selected_waves") or []
    selected_waves = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for wave_item in selected_waves_raw if isinstance(selected_waves_raw, list) else []:
        wave_id = _simulate_normalize_wave_key(wave_item)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        if wave_id and wave_id not in selected_waves:
            selected_waves.append(wave_id)
    include_route_details = bool((payload or {}).get("include_route_details"))
    single_task_seed = {}

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _task_error(code, message, status=400):
        _simulation_task_append(task_id, f"鎺ㄦ紨澶辫触: {message}")
        if mode == "single_store":
            seed = single_task_seed if isinstance(single_task_seed, dict) else {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not seed and isinstance(adjustments, list) and adjustments:
                first_adjust = adjustments[0] if isinstance(adjustments[0], dict) else {}
                seed = {
                    "shop_code": str(first_adjust.get("shop_code") or "").strip(),
                    "old_wave": str(first_adjust.get("old_wave") or "").strip(),
                    "new_wave": str(first_adjust.get("new_wave") or "").strip(),
                    "old_time": str(first_adjust.get("old_time") or "").strip(),
                    "new_time": str(first_adjust.get("new_time") or "").strip(),
                }
            _simulation_single_clear_task_rows(task_id)
            _simulation_single_persist_task(
                task_id,
                batch_id,
                seed,
                target,
                None,
                str(seed.get("new_wave") or "").strip() if isinstance(seed, dict) else None,
                payload,
                status="failed",
                summary_text=str(message or "").strip(),
                result_data={"success": False, "error_code": code, "message": message},
                error_message=message,
            )
            _simulation_single_persist_event_logs(task_id)
        _simulation_task_finish(task_id, "failed", message)
        _simulation_persist_task_finish(task_id, "failed", message)
        error_status, error_payload = _simulate_error(code, message, status)
        error_payload["task_id"] = task_id
        return error_status, error_payload

    _simulation_task_append(task_id, "寮€濮嬫帹婕旓紝姝ｅ湪鍑嗗鍦烘櫙涓庢牎楠?..")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    if not batch_id:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return _task_error("INVALID_BATCH_ID", "鎵规ID涓嶈兘涓虹┖")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if mode == "fleet_feasibility":
        if target_vehicle_count <= 0:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return _task_error("INVALID_VEHICLE_COUNT", "鐩爣杞﹁締鏁板繀椤诲ぇ浜?")
    # EN: Verification point for backend data flow.
    # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
    elif not isinstance(adjustments, list) or not adjustments:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return _task_error("INVALID_SHOP_CODE", "adjustments 涓嶈兘涓虹┖")

    archive = archive_get({"id": batch_id})
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not archive.get("found") or not isinstance(archive.get("item"), dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return _task_error("INVALID_BATCH_ID", f"鎵规ID涓嶅瓨鍦? {batch_id}")

    snapshot = deepcopy(archive.get("item") or {})
    _append_sfrz_log(f"[SIMULATE] source_batch_keys={list(snapshot.keys())}")
    realtime_context = _simulate_extract_realtime_context(payload)
    live_stores = _simulate_load_live_stores()
    stores = live_stores or _simulate_extract_snapshot_stores(snapshot)
    if not stores:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        # EN: Verification point for backend data flow.
        # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
        return _task_error("STORES_EMPTY", "褰撳墠姹傝В璋冪敤琛ㄦ棤闂ㄥ簵鏁版嵁")
    stores_before_adjust = deepcopy(stores)
    source_vehicles = deepcopy(realtime_context.get("vehicles") or [])
    if not source_vehicles:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return _task_error("VEHICLES_EMPTY", "鏈娴嬪埌褰撳墠瀵煎叆杞﹁締鍒楄〃锛岃鍏堝洖璋冨害椤靛鍏ヨ溅杈?)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if mode == "fleet_feasibility":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if target_vehicle_count > len(source_vehicles):
            # EN: Verification point for backend data flow.
            # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
            return _task_error("INVALID_VEHICLE_COUNT", f"鐩爣杞﹁締鏁?{target_vehicle_count} 瓒呰繃褰撳墠鍙敤杞﹁締 {len(source_vehicles)}")
        source_vehicles = source_vehicles[:target_vehicle_count]
    source_waves = deepcopy(realtime_context.get("waves") or snapshot.get("waves") or [])
    source_settings = deepcopy(realtime_context.get("settings") or snapshot.get("settings") or {})
    source_strategy_config = deepcopy(realtime_context.get("strategyConfig") or {})
    source_algorithm_key = str(
        realtime_context.get("algorithmKey")
        or realtime_context.get("activeResultKey")
        or snapshot.get("activeResultKey")
        or "vehicle"
    ).strip().lower() or "vehicle"
    _simulation_task_append(task_id, f"鍦烘櫙鏋勫缓瀹屾垚锛氶棬搴?{len(stores)} 瀹讹紝杞﹁締 {len(source_vehicles)} 鍙帮紝娉㈡ {len(source_waves)} 涓€?)
    _simulation_task_append(task_id, f"鍩哄噯鏂规锛歿batch_id}锛岀畻娉?{source_algorithm_key}銆?)
    _simulation_persist_task_start(task_id, batch_id, target)
    _clear_simulation_failure_logs(batch_id)

    wave_rules = (simulate_config_get().get("data") or {}).get("wave_rules") or {}
    store_map = {_simulate_extract_store_code(store): store for store in stores if _simulate_extract_store_code(store)}
    affected_waves = set()
    affected_shops = []
    persisted_failure_logs = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if mode == "fleet_feasibility":
        for wave_id in (selected_waves or ["W1", "W2"]):
            affected_waves.add(_simulate_normalize_wave_key(wave_id))
        _simulation_task_append(
            task_id,
            f"鍑忚溅鍙鎬фā寮忥細鐩爣杞﹁締 {target_vehicle_count} 鍙帮紝鐩爣娉㈡ {('/'.join(sorted([w for w in affected_waves if w])) or '--')}銆?,
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    else:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for item in adjustments:
            shop_code = str((item or {}).get("shop_code") or "").strip()
            new_wave = _simulate_normalize_wave_key((item or {}).get("new_wave"))
            new_time = str((item or {}).get("new_time") or "").strip()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not shop_code or shop_code not in store_map:
                # EN: Verification point for backend data flow.
                # CN: 鍚庣鏁版嵁娴佺殑鏍搁獙鐐广€?
                return _task_error("INVALID_SHOP_CODE", f"搴楅摵缂栧彿涓嶅瓨鍦? {shop_code}")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if new_wave not in wave_rules:
                return _task_error("INVALID_WAVE", f"娉㈡涓嶅瓨鍦ㄦ垨涓嶅彲鐢? {new_wave}")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not _simulate_time_in_wave(new_time, wave_rules.get(new_wave) or {}):
                rule = wave_rules.get(new_wave) or {}
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return _task_error(
                    "INVALID_TIME",
                    f"鏂版椂闂?{new_time} 瓒呭嚭娉㈡ {new_wave} 鑼冨洿锛坽rule.get('start')}-{rule.get('end')}锛?,
                )
            store = store_map[shop_code]
            old_wave = _simulate_normalize_wave_key((item or {}).get("old_wave")) or _simulate_normalize_wave_key(store.get("waveBelongs"))
            old_time = str((item or {}).get("old_time") or _simulate_get_store_time(store, old_wave)).strip()
            new_allowed_late = (item or {}).get("new_allowed_late")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if new_allowed_late in ("", None):
                new_allowed_late = None
            else:
                new_allowed_late = int(new_allowed_late)
            _simulate_apply_store_adjustment(store, new_wave, new_time, new_allowed_late)
            affected_waves.add(new_wave)
            affected_shops.append(
                {
                    "shop_code": shop_code,
                    "shop_name": str(store.get("shop_name") or store.get("name") or "").strip(),
                    "old_wave": old_wave,
                    "old_time": old_time,
                    "new_wave": new_wave,
                    "new_time": new_time,
                    "reason": "閲嶆柊璋冨害瀹屾垚",
                }
            )
            _simulation_task_append(task_id, f"宸插簲鐢ㄨ皟鏁达細搴楅摵 {shop_code}锛屾尝娆?{old_wave or '--'} -> {new_wave}锛屾椂闂?{old_time or '--'} -> {new_time}銆?)
            _simulation_task_append(task_id, f"鏃堕棿娉ㄥ叆鏍￠獙锛氬簵閾?{shop_code} 鍦?{new_wave} 鐨勬眰瑙ｈ緭鍏ユ椂闂村凡璁剧疆涓?{new_time}銆?)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if affected_shops:
            single_task_seed = deepcopy(affected_shops[0] or {})
            _simulation_single_clear_task_rows(task_id)
            _simulation_single_persist_task(
                task_id,
                batch_id,
                single_task_seed,
                target,
                source_algorithm_key,
                _simulate_normalize_wave_key((single_task_seed or {}).get("new_wave")),
                payload,
                status="running",
                summary_text="鍗曞簵鏃堕棿鎺ㄦ紨浠诲姟宸插惎鍔?,
                result_data={"mode": "single_store", "task_id": task_id},
            )

    before_info = _simulate_summarize_snapshot(snapshot)
    before_plans = before_info["plans"]
    merged_plans = []
    rebuilt_plan_by_wave = {}
    baseline_plan_by_wave = {}
    total_store_attempt_rows = 0
    total_insert_trial_rows = 0
    valid_waves = {"W1", "W2", "W3", "W4"}
    normalized_waves = set()
    for wid in affected_waves:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not wid:
            continue
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if "," in str(wid):
            first_wave = str(wid).split(",")[0].strip()
            wid = _simulate_normalize_wave_key(first_wave)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        else:
            wid = _simulate_normalize_wave_key(wid)
        if wid in valid_waves:
            normalized_waves.add(wid)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _solve_selected_waves(stores_for_run, phase_label, capture_failures=False, capture_trials=False):
        solved_plan_by_wave = {}
        local_failure_logs = []
        local_store_attempt_rows = 0
        local_insert_trial_rows = 0
        local_wave_stats = {}
        for wave_id in sorted(normalized_waves, key=lambda x: ("W1", "W2", "W3", "W4").index(x)):
            wave_obj = _simulate_find_wave({"waves": source_waves}, wave_id)
            scenario = _simulate_build_scenario(
                {
                    "scenario": realtime_context.get("scenario") if isinstance(realtime_context.get("scenario"), dict) else {},
                    "settings": source_settings,
                    "waves": source_waves,
                }
            )
            wave_payload = {
                "algorithmKey": source_algorithm_key,
                "scenario": scenario,
                "stores": _simulate_prepare_solver_stores(stores_for_run, wave_id, scenario.get("dispatchStartMin")),
                "vehicles": deepcopy(source_vehicles),
                "waves": [deepcopy(wave_obj)] if isinstance(wave_obj, dict) else [],
                "settings": deepcopy(source_settings),
                "strategyConfig": deepcopy(source_strategy_config),
                "dist": {},
                "wave": wave_obj,
                "goal": target,
                "skipResolvedStoreOverlay": True,
            }
            wave_payload["settings"]["optimizeGoal"] = str(target or wave_payload["settings"].get("optimizeGoal") or "min_vehicles")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not wave_payload.get("wave"):
                raise ValueError(f"missing_wave:{wave_id}")
            _simulation_task_append(task_id, f"{phase_label}{wave_id} 寮€濮嬫眰瑙ｏ細鍊欓€夐棬搴?{len(wave_payload.get('stores') or [])} 瀹讹紝杞﹁締 {len(wave_payload.get('vehicles') or [])} 鍙般€?)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            with _capture_simulation_task_stream(task_id):
                solve_result, solved_payload, strategy_audit = _run_wave_optimize_with_same_logic(wave_payload)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if isinstance(strategy_audit, dict):
                _simulation_task_append(
                    task_id,
                    f"{phase_label}绛栫暐涓績瀹¤锛坽source_algorithm_key} {wave_id}锛夛細杈撳叆闂ㄥ簵 {int(strategy_audit.get('inputStoreCount') or 0)} 瀹讹紝绛栫暐鍚?{int(strategy_audit.get('outputStoreCount') or 0)} 瀹躲€?
                )
            if capture_failures:
                wave_failure_logs = build_simulation_failure_records(batch_id, wave_id, solved_payload, solve_result)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if wave_failure_logs:
                    _save_simulation_failure_logs(batch_id, wave_failure_logs)
                    local_failure_logs.extend(wave_failure_logs)
                    _simulation_task_append(task_id, f"{phase_label}{wave_id} 澶辫触鏍锋湰 {len(wave_failure_logs)} 鏉★紝宸茶褰曡缁嗗け璐ュ缓璁€?)
            if capture_trials:
                store_attempt_rows, insert_trial_rows = _simulation_collect_insert_trials(
                    task_id,
                    batch_id,
                    wave_id,
                    solved_payload,
                    solve_result,
                )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if store_attempt_rows or insert_trial_rows:
                    _save_simulation_insert_trials(task_id, batch_id, wave_id, store_attempt_rows, insert_trial_rows)
                    local_store_attempt_rows += len(store_attempt_rows)
                    local_insert_trial_rows += len(insert_trial_rows)
                    _simulation_task_append(
                        task_id,
                        f"{phase_label}{wave_id} 鎻掑叆鍥炴斁宸茶惤搴擄細搴楅摵 {len(store_attempt_rows)} 瀹讹紝璇曟彃鏄庣粏 {len(insert_trial_rows)} 鏉°€?,
                    )
            best_state = solve_result.get("bestState") if isinstance(solve_result, dict) else None
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(best_state, list):
                raise ValueError(f"{wave_id} 閲嶆柊璋冨害澶辫触")
            assigned_codes = set()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for state_row in best_state:
                if not isinstance(state_row, dict):
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                for route in (state_row.get("routes") or []):
                    for store_id in (route or []):
                        code = str(store_id or "").strip()
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if code:
                            assigned_codes.add(code)
            candidate_codes = {
                _simulate_extract_store_code(store)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                for store in (wave_payload.get("stores") or [])
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if _simulate_extract_store_code(store)
            }
            pending_codes = {sid for sid in candidate_codes if sid not in assigned_codes}
            pending_count = len(pending_codes)
            _simulation_task_append(task_id, f"{phase_label}{wave_id} 姹傝В瀹屾垚锛歝andidate={len(candidate_codes)}锛宎ssigned={len(assigned_codes)}锛宲ending={pending_count}銆?)
            solved_plan_by_wave[wave_id] = _simulate_convert_best_state_to_plans(
                best_state,
                wave_id,
                solved_payload.get("dist") or {},
                solved_payload,
                solve_result,
            )
            local_wave_stats[wave_id] = {
                "candidate": int(len(candidate_codes)),
                "assigned": int(len(assigned_codes)),
                "pending": int(pending_count),
                "pending_store_ids": sorted(list(pending_codes)),
            }
        return solved_plan_by_wave, local_failure_logs, local_store_attempt_rows, local_insert_trial_rows, local_wave_stats

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        baseline_wave_solve_stats = {}
        simulated_wave_solve_stats = {}
        if mode == "single_store":
            baseline_plan_by_wave, _, _, _, baseline_wave_solve_stats = _solve_selected_waves(stores_before_adjust, "鍩虹嚎-", capture_failures=False, capture_trials=False)
        rebuilt_plan_by_wave, captured_failures, attempt_row_count, trial_row_count, simulated_wave_solve_stats = _solve_selected_waves(stores, "", capture_failures=True, capture_trials=True)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if captured_failures:
            persisted_failure_logs.extend(captured_failures)
        total_store_attempt_rows += int(attempt_row_count or 0)
        total_insert_trial_rows += int(trial_row_count or 0)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for plan in before_plans:
            wave_id = _simulate_normalize_wave_key(plan.get("waveId"))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if wave_id in rebuilt_plan_by_wave:
                continue
            merged_plans.append(plan)
        for wave_id in ("W1", "W2", "W3", "W4"):
            merged_plans.extend(rebuilt_plan_by_wave.get(wave_id) or [])
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except ValueError as error:
        if str(error).startswith("missing_wave:"):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return _task_error("INVALID_WAVE", f"娉㈡涓嶅瓨鍦? {str(error).split(':', 1)[1]}")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return _task_error("RESCHEDULE_FAILED", str(error), status=500)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception as error:
        traceback.print_exc()
        return _task_error("RESCHEDULE_FAILED", f"閲嶆柊璋冨害澶辫触: {error}", status=500)

    summary_scope_waves = sorted([w for w in normalized_waves if w])
    before_plans_source = before_plans
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if mode == "single_store" and baseline_plan_by_wave:
        before_plans_source = []
        for wid in ("W1", "W2", "W3", "W4"):
            before_plans_source.extend(baseline_plan_by_wave.get(wid) or [])
    before_plans_for_summary = _simulate_filter_plans_by_waves(before_plans_source, summary_scope_waves)
    after_plans_for_summary = _simulate_filter_plans_by_waves(merged_plans, summary_scope_waves)
    after_wave_stats = _simulate_wave_stats_from_plans(after_plans_for_summary)
    after_summary = {
        "total_vehicles": _simulate_total_unique_vehicles(after_plans_for_summary),
        "total_mileage": round(sum(item["mileage"] for item in after_wave_stats.values()), 1),
        "wave_stats": after_wave_stats,
    }
    before_wave_stats_for_summary = _simulate_wave_stats_from_plans(before_plans_for_summary)
    before_summary_for_response = {
        "total_vehicles": _simulate_total_unique_vehicles(before_plans_for_summary),
        "total_mileage": round(sum(item["mileage"] for item in before_wave_stats_for_summary.values()), 1),
        "wave_stats": before_wave_stats_for_summary,
    }
    route_comparison_shop_code = str((adjustments[0] or {}).get("shop_code") or "").strip() if adjustments else ""
    shop_name_map = {}
    store_load_map = {}
    old_expected_time_map = {}
    new_expected_time_map = {}
    compare_wave_for_expected = None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if mode == "single_store" and affected_shops:
        compare_wave_for_expected = _simulate_normalize_wave_key((affected_shops[0] or {}).get("new_wave"))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    elif summary_scope_waves:
        compare_wave_for_expected = summary_scope_waves[0]
    before_store_map = {
        _simulate_extract_store_code(store): store
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for store in (stores_before_adjust or [])
        if _simulate_extract_store_code(store)
    }
    after_store_map = {
        _simulate_extract_store_code(store): store
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for store in (stores or [])
        if _simulate_extract_store_code(store)
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for store in (stores or []):
        code = _simulate_extract_store_code(store)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not code:
            continue
        shop_name_map[code] = str(store.get("shop_name") or store.get("name") or "").strip()
        store_load_map[code] = _simulate_get_store_load(store)
        old_store = before_store_map.get(code) or {}
        new_store = after_store_map.get(code) or {}
        old_expected_time_map[code] = _simulate_get_store_time(old_store, compare_wave_for_expected)
        new_expected_time_map[code] = _simulate_get_store_time(new_store, compare_wave_for_expected)
    route_comparison = _build_route_comparison(
        before_plans_source,
        merged_plans,
        route_comparison_shop_code,
        shop_name_map,
        store_load_map,
        old_expected_time_map,
        new_expected_time_map,
        compare_wave_for_expected,
    )
    schedule_diagnostics = None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if mode == "single_store":
        def _collect_route_schedule_diag(route):
            diag = {
                "solver_count": 0,
                "reconstructed_count": 0,
                "missing_count": 0,
                "total_stops": 0,
            }
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(route, dict):
                return diag
            stops = route.get("stops") if isinstance(route.get("stops"), list) else []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for stop in stops:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not isinstance(stop, dict):
                    continue
                diag["total_stops"] += 1
                source = str(stop.get("scheduled_source") or "").strip().lower()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if source == "solver":
                    diag["solver_count"] += 1
                elif source == "reconstructed":
                    diag["reconstructed_count"] += 1
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    diag["missing_count"] += 1
            return diag

        old_routes_chk = route_comparison.get("old_routes") if isinstance(route_comparison, dict) else []
        old_route_chk = old_routes_chk[0] if old_routes_chk else None
        new_route_chk = route_comparison.get("new_route") if isinstance(route_comparison, dict) else None
        old_diag = _collect_route_schedule_diag(old_route_chk)
        new_diag = _collect_route_schedule_diag(new_route_chk)
        schedule_diagnostics = {
            "old_route": old_diag,
            "new_route": new_diag,
            "summary": (
                "璋冨害鍒板簵鏃堕棿鏉ユ簮锛?
                f"鏀瑰墠(姹傝В鍣ㄧ洿鎺ヨ繑鍥?{old_diag['solver_count']} / 闈炵洿鎺ヨ繑鍥?{old_diag['reconstructed_count']} / 缂哄け {old_diag['missing_count']})锛?
                f"鏀瑰悗(姹傝В鍣ㄧ洿鎺ヨ繑鍥?{new_diag['solver_count']} / 闈炵洿鎺ヨ繑鍥?{new_diag['reconstructed_count']} / 缂哄け {new_diag['missing_count']})銆?
            ),
            "reason": (
                "鏈〉浠ユ眰瑙ｅ櫒鐩存帴杩斿洖鐨勯€愬簵鍒板簵鏃跺埢涓哄噯銆傝嫢鍑虹幇鈥滈潪鐩存帴杩斿洖鈥濇垨鈥滅己澶扁€濓紝"
                "璇存槑璇ユ缁撴灉浠嶆湭婊¤冻鐪熻皟搴﹀璁¤姹傦紝闇€瑕佺户缁ˉ榻愮畻娉曚晶閫愬簵鍒板簵鏃跺埢杈撳嚭銆?
            ),
        }
    single_store_ab_compare = None
    single_store_audit = None
    single_store_report = None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if mode == "single_store" and affected_shops:
        target_adjust = affected_shops[0]
        compare_wave = _simulate_normalize_wave_key(target_adjust.get("new_wave"))
        baseline_wave_metrics = baseline_wave_solve_stats.get(compare_wave, {}) if isinstance(baseline_wave_solve_stats, dict) else {}
        simulated_wave_metrics = simulated_wave_solve_stats.get(compare_wave, {}) if isinstance(simulated_wave_solve_stats, dict) else {}
        baseline_wave_summary = (before_summary_for_response.get("wave_stats") or {}).get(compare_wave, {})
        simulated_wave_summary = (after_summary.get("wave_stats") or {}).get(compare_wave, {})
        single_store_ab_compare = {
            "wave_id": compare_wave,
            "baseline": {
                "candidate": int(baseline_wave_metrics.get("candidate") or 0),
                "assigned": int(baseline_wave_metrics.get("assigned") or 0),
                "pending": int(baseline_wave_metrics.get("pending") or 0),
                "vehicles": int(baseline_wave_summary.get("vehicles") or 0),
                "mileage": float(baseline_wave_summary.get("mileage") or 0.0),
            },
            "simulated": {
                "candidate": int(simulated_wave_metrics.get("candidate") or 0),
                "assigned": int(simulated_wave_metrics.get("assigned") or 0),
                "pending": int(simulated_wave_metrics.get("pending") or 0),
                "vehicles": int(simulated_wave_summary.get("vehicles") or 0),
                "mileage": float(simulated_wave_summary.get("mileage") or 0.0),
            },
        }
        single_store_ab_compare["delta"] = {
            "candidate": int(single_store_ab_compare["simulated"]["candidate"] - single_store_ab_compare["baseline"]["candidate"]),
            "assigned": int(single_store_ab_compare["simulated"]["assigned"] - single_store_ab_compare["baseline"]["assigned"]),
            "pending": int(single_store_ab_compare["simulated"]["pending"] - single_store_ab_compare["baseline"]["pending"]),
            "vehicles": int(single_store_ab_compare["simulated"]["vehicles"] - single_store_ab_compare["baseline"]["vehicles"]),
            "mileage": round(float(single_store_ab_compare["simulated"]["mileage"] - single_store_ab_compare["baseline"]["mileage"]), 1),
        }
        base_cmp = single_store_ab_compare["baseline"]
        sim_cmp = single_store_ab_compare["simulated"]
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if (
            int(base_cmp.get("candidate") or 0) == 0
            and int(sim_cmp.get("candidate") or 0) == 0
            and int(base_cmp.get("assigned") or 0) == 0
            and int(sim_cmp.get("assigned") or 0) == 0
            and int(base_cmp.get("vehicles") or 0) == 0
            and int(sim_cmp.get("vehicles") or 0) == 0
            and float(base_cmp.get("mileage") or 0.0) == 0.0
            and float(sim_cmp.get("mileage") or 0.0) == 0.0
        ):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return _task_error("AB_COMPARE_EMPTY", f"鏈敓鎴愭湁鏁圓/B瀵圭収缁撴灉锛坽compare_wave or '--'}锛?, status=500)
        fixed_payload = {
            "mode": mode,
            "wave_id": compare_wave,
            "algorithm": source_algorithm_key,
            "vehicle_count": len(source_vehicles),
            "store_count": len(_simulate_prepare_solver_stores(stores_before_adjust, compare_wave, _simulate_build_scenario({"waves": source_waves, "settings": source_settings, "scenario": realtime_context.get("scenario") if isinstance(realtime_context.get("scenario"), dict) else {}}).get("dispatchStartMin"))),
            "target": target,
        }
        fixed_hash = hashlib.sha1(json.dumps(fixed_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
        old_route_count = len(route_comparison.get("old_routes") or []) if isinstance(route_comparison, dict) else 0
        new_route = route_comparison.get("new_route") if isinstance(route_comparison, dict) else {}
        single_store_audit = {
            "only_change": {
                "shop_code": str(target_adjust.get("shop_code") or ""),
                "wave": str(target_adjust.get("new_wave") or ""),
                "old_time": str(target_adjust.get("old_time") or ""),
                "new_time": str(target_adjust.get("new_time") or ""),
            },
            "fixed_input_fingerprint": fixed_hash,
            "fixed_inputs": fixed_payload,
            "route_diff": {
                "old_route_count": int(old_route_count),
                "new_route_vehicle": str((new_route or {}).get("vehicle") or ""),
                "new_route_wave": str((new_route or {}).get("wave") or ""),
                "new_route_stop_count": len((new_route or {}).get("stops") or []),
            },
            "proof_text": f"浠呭彉鏇村簵閾?{target_adjust.get('shop_code')} 鍦?{target_adjust.get('new_wave')} 鐨勫埌搴楁椂闂?{target_adjust.get('old_time') or '--'} -> {target_adjust.get('new_time') or '--'}锛屽叾浣欒緭鍏ヤ繚鎸佷竴鑷淬€?,
        }
        delta_cmp = single_store_ab_compare["delta"]
        vehicles_change = int(delta_cmp.get("vehicles") or 0)
        mileage_change = float(delta_cmp.get("mileage") or 0.0)
        assigned_change = int(delta_cmp.get("assigned") or 0)
        pending_change = int(delta_cmp.get("pending") or 0)
        if vehicles_change < 0 or mileage_change < 0 or assigned_change > 0 or pending_change < 0:
            result_sentence = (
                f"杩欐鏀瑰崟搴楁椂闂村甫鏉ョ殑鍑€鍙樺寲鏄細澶氬畨鎺?{max(0, assigned_change)} 瀹跺簵锛?
                f"灏戠敤 {max(0, -vehicles_change)} 鍙拌溅锛屽皯璺?{max(0.0, -mileage_change):.1f} 鍏噷銆?
            )
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif vehicles_change == 0 and mileage_change == 0 and assigned_change == 0 and pending_change == 0:
            result_sentence = "鏀瑰墠鏀瑰悗闂ㄥ簵瀹夋帓鏁般€佽溅杈嗘暟銆佹€婚噷绋嬪潎涓€鑷达紝鏈鏃堕棿璋冩暣鏈骇鐢熷彲瑙佹敹鐩娿€?
        else:
            result_sentence = (
                f"杩欐鏀瑰崟搴楁椂闂村悗鐨勫彉鍖栦负锛氬畨鎺掗棬搴楀彉鍖?{assigned_change:+d} 瀹讹紝"
                f"杞﹁締鍙樺寲 {vehicles_change:+d} 鍙帮紝鎬婚噷绋嬪彉鍖?{mileage_change:+.1f} 鍏噷銆?
            )
        single_store_report = {
            "wave_id": compare_wave,
            "baseline_text": (
                f"鍩虹嚎鏂规锛堟敼鍓嶏級锛氭湰娉㈡鍙備笌鎺掔嚎闂ㄥ簵 {int(base_cmp.get('candidate') or 0)} 瀹讹紝"
                f"鎴愬姛瀹夋帓 {int(base_cmp.get('assigned') or 0)} 瀹讹紝鏈畨鎺?{int(base_cmp.get('pending') or 0)} 瀹讹紱"
                f"浣跨敤杞﹁締 {int(base_cmp.get('vehicles') or 0)} 鍙帮紝鎬婚噷绋?{float(base_cmp.get('mileage') or 0.0):.1f} 鍏噷銆?
            ),
            "simulated_text": (
                f"鎺ㄦ紨鏂规锛堟敼鍚庯級锛氫粎灏嗗簵閾?{target_adjust.get('shop_code') or '--'} 鐨勫埌搴楁椂闂翠粠 "
                f"{target_adjust.get('old_time') or '--'} 璋冩暣鍒?{target_adjust.get('new_time') or '--'} 鍚庯紝"
                f"鏈尝娆″弬涓庢帓绾块棬搴?{int(sim_cmp.get('candidate') or 0)} 瀹讹紝鎴愬姛瀹夋帓 {int(sim_cmp.get('assigned') or 0)} 瀹讹紝"
                f"鏈畨鎺?{int(sim_cmp.get('pending') or 0)} 瀹讹紱浣跨敤杞﹁締 {int(sim_cmp.get('vehicles') or 0)} 鍙帮紝"
                f"鎬婚噷绋?{float(sim_cmp.get('mileage') or 0.0):.1f} 鍏噷銆?
            ),
            "delta_text": result_sentence,
            "variable_proof": str(single_store_audit.get("proof_text") or ""),
        }
    data = {
        "mode": mode,
        "target_vehicle_count": target_vehicle_count if mode == "fleet_feasibility" else None,
        "selected_waves": sorted([w for w in normalized_waves if w]),
        "before": before_summary_for_response,
        "after": after_summary,
        "improvements": {
            "vehicles_saved": before_summary_for_response["total_vehicles"] - after_summary["total_vehicles"],
            "mileage_saved": round(before_summary_for_response["total_mileage"] - after_summary["total_mileage"], 1),
            "affected_shops": affected_shops,
        },
        "route_comparison": route_comparison,
        "schedule_diagnostics": schedule_diagnostics,
        "ab_compare": single_store_ab_compare,
        "audit_proof": single_store_audit,
        "report": single_store_report,
        "route_changes": {
            "affected_waves": sorted(normalized_waves, key=lambda x: ("W1", "W2", "W3", "W4").index(x)),
            "details": [],
        },
        "attempt_trace_summary": {
            "store_attempt_rows": total_store_attempt_rows,
            "insert_trial_rows": total_insert_trial_rows,
        },
        "failure_logs": sorted(
            [
                {
                    "shop_code": str(item.get("shop_code") or "").strip(),
                    "wave_id": str(item.get("wave_id") or "").strip(),
                    "failure_type": str(item.get("failure_type") or "").strip(),
                    "violation_type": str(item.get("violation_type") or "").strip(),
                    "current_time": item.get("current_time"),
                    "expected_arrival": item.get("expected_arrival"),
                    "latest_allowed": item.get("latest_allowed"),
                    "late_minutes": int(item.get("late_minutes") or 0),
                    "target_vehicle": str(item.get("target_vehicle") or "").strip(),
                    "suggested_time": item.get("suggested_time"),
                    "suggestion_type": str(item.get("suggestion_type") or "").strip(),
                    "suggestion_text": str(item.get("suggestion_text") or "").strip(),
                    "current_limit_km": item.get("current_limit_km"),
                    "required_limit_km": item.get("required_limit_km"),
                    "exceed_km": item.get("exceed_km"),
                    "current_capacity_limit": item.get("current_capacity_limit"),
                    "required_capacity_limit": item.get("required_capacity_limit"),
                    "exceed_load": item.get("exceed_load"),
                    "current_stops_limit": item.get("current_stops_limit"),
                    "required_stops_limit": item.get("required_stops_limit"),
                    "wave_end_time": item.get("wave_end_time"),
                    "actual_finish_time": item.get("actual_finish_time"),
                    "over_minutes": int(item.get("over_minutes") or 0),
                    "suggested_wave_end_time": item.get("suggested_wave_end_time"),
                }
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for item in persisted_failure_logs
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if isinstance(item, dict)
            ],
            key=lambda item: (-int(item.get("late_minutes") or 0), str(item.get("shop_code") or "")),
        ),
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if include_route_details:
        data["route_changes"]["details"] = [
            {
                    "wave_id": wave_id,
                    "vehicles_before": before_summary_for_response["wave_stats"].get(wave_id, {}).get("vehicles", 0),
                    "vehicles_after": after_summary["wave_stats"].get(wave_id, {}).get("vehicles", 0),
                    "mileage_before": before_summary_for_response["wave_stats"].get(wave_id, {}).get("mileage", 0.0),
                    "mileage_after": after_summary["wave_stats"].get(wave_id, {}).get("mileage", 0.0),
                }
                for wave_id in data["route_changes"]["affected_waves"]
        ]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if mode == "fleet_feasibility":
        _simulation_task_append(
            task_id,
            f"鎬讳綋缁撴灉锛堢洰鏍囨尝娆?{'/'.join(summary_scope_waves) or '--'}锛夛細鐢ㄨ溅 {before_summary_for_response['total_vehicles']} -> {after_summary['total_vehicles']}锛岄噷绋?{round(before_summary_for_response['total_mileage'], 1)} -> {round(after_summary['total_mileage'], 1)} km銆?
        )
    else:
        _simulation_task_append(task_id, f"鎬讳綋缁撴灉锛氱敤杞?{before_summary_for_response['total_vehicles']} -> {after_summary['total_vehicles']}锛岄噷绋?{round(before_summary_for_response['total_mileage'], 1)} -> {round(after_summary['total_mileage'], 1)} km銆?)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(single_store_audit, dict):
            _simulation_task_append(task_id, f"A/B鍞竴鍙橀噺璇佹槑锛歿single_store_audit.get('proof_text')}")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if isinstance(single_store_ab_compare, dict):
            base = single_store_ab_compare.get("baseline") or {}
            sim = single_store_ab_compare.get("simulated") or {}
            delta = single_store_ab_compare.get("delta") or {}
            _simulation_task_append(
                task_id,
                f"A/B瀵圭収锛坽single_store_ab_compare.get('wave_id') or '--'}锛夛細candidate {base.get('candidate', 0)}->{sim.get('candidate', 0)}锛宎ssigned {base.get('assigned', 0)}->{sim.get('assigned', 0)}锛宲ending {base.get('pending', 0)}->{sim.get('pending', 0)}锛岃溅杈?{base.get('vehicles', 0)}->{sim.get('vehicles', 0)}锛岄噷绋?{float(base.get('mileage') or 0.0):.1f}->{float(sim.get('mileage') or 0.0):.1f} km锛埼攞float(delta.get('mileage') or 0.0):.1f}锛?
            )
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(single_store_report, dict):
            _simulation_task_append(task_id, f"A/B涓枃鎶ュ憡锛歿single_store_report.get('baseline_text')}")
            _simulation_task_append(task_id, f"A/B涓枃鎶ュ憡锛歿single_store_report.get('simulated_text')}")
            _simulation_task_append(task_id, f"A/B涓枃鎶ュ憡锛歿single_store_report.get('delta_text')}")
        if isinstance(schedule_diagnostics, dict):
            _simulation_task_append(task_id, str(schedule_diagnostics.get("summary") or ""))
            _simulation_task_append(task_id, str(schedule_diagnostics.get("reason") or ""))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if persisted_failure_logs:
        for item in persisted_failure_logs:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(item, dict):
                continue
            failure_type = str(item.get("failure_type") or "--")
            violation_type = str(item.get("violation_type") or "").strip()
            failure_zh = {
                "arrival_window": "鏃堕棿绐楄秴鏃?,
                "wave_end": "娉㈡缁撴潫瓒呮椂",
                "mileage": "閲岀▼瓒呴檺",
                "capacity": "瑁呰浇瓒呴檺",
                "max_stops": "闂ㄥ簵鏁拌秴闄?,
            }.get(failure_type, "绾︽潫鍐茬獊")
            violation_zh = {
                "arrival_window": "鍒板簵鏃堕棿绐楁鏌?,
                "wave_end": "娉㈡缁撴潫鏃跺埢妫€鏌?,
                "max_route_km_single": "鍗曠▼閲岀▼妫€鏌?,
                "night_regular_distance": "澶滄尝娆℃帴鍔涢噷绋嬫鏌?,
                "capacity": "杞﹁締瀹归噺妫€鏌?,
                "slot": "鎻掑叆浣嶇疆妫€鏌?,
            }.get(violation_type, "姹傝В鍣ㄧ害鏉熸鏌?)
            reason_label = failure_type
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if failure_type == "wave_end":
                reason_label = "wave_end(娉㈡缁撴潫鏃堕棿)"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            elif failure_type == "arrival_window":
                reason_label = "arrival_window(鏃堕棿绐?"
            elif failure_type == "mileage":
                reason_label = "mileage(閲岀▼)"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            elif failure_type == "capacity":
                reason_label = "capacity(瀹归噺)"
            elif failure_type == "max_stops":
                reason_label = "max_stops(闂ㄥ簵鏁?"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if failure_type == "arrival_window":
                detail_line = (
                    f"{str(item.get('wave_id') or '--')} | 搴梴str(item.get('shop_code') or '--')} | 鏃堕棿绐楄秴鏃?{int(item.get('late_minutes') or 0)} 鍒嗛挓 | "
                    f"棰勮鍒拌揪 {str(item.get('expected_arrival') or '--')} > 鏈€鏅氬厑璁?{str(item.get('latest_allowed') or '--')}"
                )
                action_line = (
                    f"寤鸿A锛氶棬搴楁椂闂磋皟鍒?{str(item.get('suggested_time') or '--')}锛?
                    f"寤鸿B锛氳嚦灏戞彁鍓?{int(item.get('late_minutes') or 0)} 鍒嗛挓锛?
                    "寤鸿C锛氬皾璇曟敼娲剧浉閭绘尝娆★紙鑻ュ厑璁歌法娉㈡锛夈€?
                )
                evidence_line = (
                    f"渚濇嵁锛氬け璐ョ被鍨嬩负{failure_zh}锛岃Е鍙戣鍒欎负{violation_zh}锛?
                    f"鍏宠仈杞﹁締 {str(item.get('target_vehicle') or '--')}銆?
                )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            elif failure_type == "wave_end":
                over_minutes = int(item.get("over_minutes") or item.get("late_minutes") or 0)
                detail_line = (
                    f"{str(item.get('wave_id') or '--')} | 搴梴str(item.get('shop_code') or '--')} | 娉㈡缁撴潫瓒呮椂 {over_minutes} 鍒嗛挓 | "
                    f"褰撳墠缁撴潫 {str(item.get('actual_finish_time') or '--')} > 鎴 {str(item.get('wave_end_time') or '--')}"
                )
                action_line = (
                    f"寤鸿A锛氬皢 {str(item.get('wave_id') or '--')} 鎴浠?{str(item.get('wave_end_time') or '--')} 鏀惧鍒?{str(item.get('suggested_wave_end_time') or '--')}锛?{over_minutes}鍒嗛挓锛夛紱"
                    f"寤鸿B锛氬簵閾烘彁鍓嶅埌 {str(item.get('suggested_time') or '--')}锛?{over_minutes}鍒嗛挓锛夛紱"
                    "寤鸿C锛氬皾璇曟敼娲剧浉閭绘尝娆★紙鑻ュ厑璁歌法娉㈡锛夈€?
                )
                evidence_line = (
                    f"渚濇嵁锛氬け璐ョ被鍨嬩负{failure_zh}锛岃Е鍙戣鍒欎负{violation_zh}锛?
                    f"鍏宠仈杞﹁締 {str(item.get('target_vehicle') or '--')}銆?
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            else:
                suggestion_text = str(item.get("suggestion_text") or "褰撳墠澶辫触绫诲瀷鏆傛湭鐢熸垚鑷姩寤鸿銆?).strip()
                detail_line = (
                    f"{str(item.get('wave_id') or '--')} | 搴梴str(item.get('shop_code') or '--')} | 瑙﹀彂绾︽潫 {reason_label} | "
                    f"褰撳墠鏃堕棿 {str(item.get('current_time') or '--')}锛屾渶鏅氬厑璁?{str(item.get('latest_allowed') or '--')}"
                )
                action_line = suggestion_text
                evidence_line = (
                    f"渚濇嵁锛氬け璐ョ被鍨嬩负{failure_zh}锛岃Е鍙戣鍒欎负{violation_zh}锛?
                    f"鍏宠仈杞﹁締 {str(item.get('target_vehicle') or '--')}銆?
                )
            _simulation_task_append(task_id, detail_line)
            _simulation_task_append(task_id, action_line)
            _simulation_task_append(task_id, evidence_line)
    else:
        _simulation_task_append(task_id, "鏈鎺ㄦ紨鏈骇鐢熷け璐ラ棬搴楁牱鏈紱褰撳墠闂ㄥ簵璋冩暣鍚庝粛鍙畬鎴愭眰瑙ｃ€?)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if mode == "single_store" and affected_shops and isinstance(single_store_ab_compare, dict):
        compare_wave = _simulate_normalize_wave_key((affected_shops[0] or {}).get("new_wave"))
        baseline_plans_for_persist = _simulate_filter_plans_by_waves(before_plans_source, [compare_wave] if compare_wave else [])
        simulated_plans_for_persist = _simulate_filter_plans_by_waves(merged_plans, [compare_wave] if compare_wave else [])
        _simulation_single_persist_task(
            task_id,
            batch_id,
            affected_shops[0],
            target,
            source_algorithm_key,
            compare_wave,
            payload,
            status="success",
            before_summary=before_summary_for_response,
            after_summary=after_summary,
            summary_text=str((single_store_report or {}).get("delta_text") or "").strip(),
            result_data=data,
        )
        _simulation_single_persist_snapshots(
            task_id,
            compare_wave,
            single_store_ab_compare.get("baseline") or {},
            single_store_ab_compare.get("simulated") or {},
            baseline_plans_for_persist,
            simulated_plans_for_persist,
            before_store_map,
            after_store_map,
            route_comparison_shop_code,
        )
        _simulation_single_persist_confirmed_routes(task_id, route_comparison)
        _simulation_single_persist_states(
            task_id,
            compare_wave,
            source_vehicles,
            baseline_plans_for_persist,
            simulated_plans_for_persist,
            before_store_map,
            after_store_map,
        )
        _simulation_single_persist_event_logs(task_id)
    _simulation_task_finish(task_id, "success")
    _simulation_persist_task_finish(task_id, "success", "")
    return 200, {"success": True, "task_id": task_id, "data": data}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def amap_cache_sync(payload):
    ensure_archive_tables()
    distance_cache = payload.get("distanceCache") or {}
    route_cache = payload.get("routeCache") or {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(distance_cache, dict):
        distance_cache = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(route_cache, dict):
        route_cache = {}
    max_rows = max(1, min(50000, int(payload.get("maxRows") or 12000)))
    distance_items = list(distance_cache.items())[:max_rows]
    route_items = list(route_cache.items())[:max_rows]
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            if distance_items:
                cursor.executemany(
                    f"""
                    INSERT INTO {AMAP_DISTANCE_TABLE}
                    (cache_key, distance_km, duration_minutes)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        distance_km = VALUES(distance_km),
                        duration_minutes = VALUES(duration_minutes)
                    """,
                    [
                        (
                            str(key)[:255],
                            float((value or {}).get("distanceKm") or 0.0),
                            float((value or {}).get("durationMinutes") or 0.0),
                        )
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        for key, value in distance_items
                    ],
                )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if route_items:
                cursor.executemany(
                    f"""
                    INSERT INTO {AMAP_ROUTE_TABLE}
                    (cache_key, polyline, source)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        polyline = VALUES(polyline),
                        source = VALUES(source)
                    """,
                    [
                        (
                            str(key)[:255],
                            str((value or {}).get("polyline") or ""),
                            str((value or {}).get("source") or ""),
                        )
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        for key, value in route_items
                    ],
                )
    return {
        "synced": True,
        "distanceRows": len(distance_items),
        "routeRows": len(route_items),
        "truncated": len(distance_cache) > len(distance_items) or len(route_cache) > len(route_items),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _norm_store_id(value):
    text = str(value or "").strip()
    if text.endswith(".0") and text[:-2].isdigit():
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return text[:-2]
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return text


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _to_float_safe(value, default=0.0):
    try:
        parsed = float(value)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception:
        return float(default)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not math.isfinite(parsed):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return float(default)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return float(parsed)


def _to_bool_safe(value, default=False):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if value is None:
        return bool(default)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(value, bool):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return value
    text = str(value).strip().lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if text in {"1", "true", "yes", "on"}:
        return True
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if text in {"0", "false", "no", "off"}:
        return False
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return bool(default)


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _normalize_store_ids_for_matrix(raw):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if raw is None:
        return []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if isinstance(raw, str):
        items = raw.split(",")
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    else:
        items = [raw]
    out = []
    seen = set()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in items:
        sid = _norm_store_id(item)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not sid or sid == DC_ID or sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _get_distance_matrix_full(payload):
    started = time.time()
    req = payload if isinstance(payload, dict) else {}
    store_ids = _normalize_store_ids_for_matrix(req.get("storeIds") or req.get("store_ids"))
    include_duration = _to_bool_safe(req.get("includeDuration"), True)
    strict = _to_bool_safe(req.get("strict"), False)

    if not store_ids:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {"ok": False, "error": "store_ids_required", "missingCount": 0, "missingPairs": None}

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(store_ids))
            cursor.execute(
                f"SELECT DISTINCT shop_code FROM c_shop_main WHERE shop_code IN ({placeholders})",
                tuple(store_ids),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
            valid_store_ids = sorted(
                {_norm_store_id(row.get("shop_code")) for row in rows if _norm_store_id(row.get("shop_code"))}
            )

    if not valid_store_ids:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {"ok": False, "error": "no_valid_store_ids", "missingCount": 0, "missingPairs": None}

    node_ids = [DC_ID] + valid_store_ids
    node_count = len(node_ids)
    dist = {node: {} for node in node_ids}
    duration = {node: {} for node in node_ids} if include_duration else None

    latest_updated_at = None
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            placeholders = ",".join(["%s"] * len(node_ids))
            cursor.execute(
                f"""
                SELECT from_store_id, to_store_id, distance_km, duration_minutes, source, updated_at
                FROM {STORE_DISTANCE_TABLE}
                WHERE from_store_id IN ({placeholders}) AND to_store_id IN ({placeholders})
                """,
                tuple(node_ids + node_ids),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
            source_counts = {}
            for row in rows:
                src = _norm_store_id(row.get("from_store_id"))
                dst = _norm_store_id(row.get("to_store_id"))
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if src not in dist or dst not in dist:
                    continue
                km = _to_float_safe(row.get("distance_km"), 0.0)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if km <= 0:
                    continue
                dist[src][dst] = float(km)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if include_duration:
                    duration[src][dst] = float(max(0.0, _to_float_safe(row.get("duration_minutes"), 0.0)))
                src_tag = str(row.get("source") or "").strip() or "unknown"
                source_counts[src_tag] = int(source_counts.get(src_tag, 0)) + 1
                updated_at = row.get("updated_at")
                if updated_at and (latest_updated_at is None or updated_at > latest_updated_at):
                    latest_updated_at = updated_at

    missing_pairs = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for src in node_ids:
        for dst in node_ids:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if src == dst:
                dist[src][dst] = 0.0
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if include_duration:
                    duration[src][dst] = 0.0
                continue
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if dst not in dist[src]:
                missing_pairs.append([src, dst])

    db_read_ms = int((time.time() - started) * 1000)
    result = {
        "ok": True,
        "source": "database",
        "dbSourceCounts": source_counts,
        "dbDominantSource": max(source_counts, key=lambda k: source_counts[k]) if source_counts else "",
        "dist": dist,
        "duration": duration if include_duration else None,
        "nodeCount": node_count,
        "pairCount": node_count * node_count,
        "missingCount": len(missing_pairs),
        "missingPairs": missing_pairs if missing_pairs else None,
        "dbReadMs": db_read_ms,
        "updatedAt": str(latest_updated_at) if latest_updated_at else "",
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if strict and missing_pairs:
        return {
            "ok": False,
            "error": "missing_distance_pairs",
            "source": "database",
            "dbSourceCounts": source_counts,
            "dbDominantSource": max(source_counts, key=lambda k: source_counts[k]) if source_counts else "",
            "nodeCount": node_count,
            "pairCount": node_count * node_count,
            "missingCount": len(missing_pairs),
            "missingPairs": missing_pairs,
            "dbReadMs": db_read_ms,
            "updatedAt": str(latest_updated_at) if latest_updated_at else "",
        }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return result


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _should_skip_dist_hydrate(payload):
    stats = payload.get("distDbStats") if isinstance(payload.get("distDbStats"), dict) else {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not stats:
        return False
    full_matrix = _to_bool_safe(stats.get("fullMatrix"), False)
    missing_count = int(_to_float_safe(stats.get("missingCount"), 0.0))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return full_matrix and missing_count == 0


def _haversine_km(lng1, lat1, lng2, lat2):
    lon1 = math.radians(_to_float_safe(lng1))
    lat1r = math.radians(_to_float_safe(lat1))
    lon2 = math.radians(_to_float_safe(lng2))
    lat2r = math.radians(_to_float_safe(lat2))
    dlon = lon2 - lon1
    dlat = lat2r - lat1r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1r) * math.cos(lat2r) * (math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(max(0.0, a)), math.sqrt(max(0.0, 1 - a)))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return 6371.0 * c


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _store_distance_fetch_points(store_ids):
    ids = [_norm_store_id(x) for x in (store_ids or []) if _norm_store_id(x) and _norm_store_id(x) != DC_ID]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not ids:
        return {}
    points = {}
    chunk_size = 500
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for start in range(0, len(ids), chunk_size):
                chunk = ids[start:start + chunk_size]
                placeholders = ",".join(["%s"] * len(chunk))
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    f"""
                    SELECT shop_code AS store_id, lng, lat
                    FROM c_shop_main
                    WHERE shop_code IN ({placeholders})
                    """,
                    tuple(chunk),
                )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for row in (cursor.fetchall() or []):
                    sid = _norm_store_id(row.get("store_id"))
                    if not sid:
                        continue
                    lng = _to_float_safe(row.get("lng"), 0.0)
                    lat = _to_float_safe(row.get("lat"), 0.0)
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if abs(lng) < 1e-9 and abs(lat) < 1e-9:
                        continue
                    points[sid] = {"lng": round(lng, 6), "lat": round(lat, 6)}
    return points


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _hydrate_payload_dist_from_db(payload):
    ensure_archive_tables()
    stores = payload.get("stores") if isinstance(payload.get("stores"), list) else []
    ids = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in stores:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not isinstance(item, dict):
            continue
        sid = _norm_store_id(item.get("id"))
        if sid:
            ids.append(sid)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not ids:
        payload["dist"] = {}
        payload["distDbStats"] = {"storeCount": 0, "dbHitPairs": 0, "dbWritePairs": 0, "fallbackPairs": 0}
        return payload
    node_ids = [DC_ID] + sorted(set(ids))
    points = {DC_ID: {"lng": DEFAULT_DC_LNG, "lat": DEFAULT_DC_LAT}}
    points.update(_store_distance_fetch_points(node_ids))

    incoming_dist = payload.get("dist") if isinstance(payload.get("dist"), dict) else {}
    matrix = {src: {} for src in node_ids}
    cache_rows = {}
    db_hit_pairs = 0
    chunk_size = 300
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for start in range(0, len(node_ids), chunk_size):
                chunk = node_ids[start:start + chunk_size]
                placeholders = ",".join(["%s"] * len(chunk))
                sql = f"""
                    SELECT from_store_id, to_store_id, distance_km, duration_minutes
                    FROM {STORE_DISTANCE_TABLE}
                    WHERE from_store_id IN ({placeholders})
                      AND to_store_id IN ({placeholders})
                """
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(sql, tuple(chunk + chunk))
                for row in (cursor.fetchall() or []):
                    src = _norm_store_id(row.get("from_store_id"))
                    dst = _norm_store_id(row.get("to_store_id"))
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if not src or not dst:
                        continue
                    dist_km = _to_float_safe(row.get("distance_km"), 0.0)
                    dur_min = _to_float_safe(row.get("duration_minutes"), 0.0)
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if dist_km <= 0:
                        continue
                    cache_rows[(src, dst)] = {"distance_km": dist_km, "duration_minutes": max(0.0, dur_min)}

    upsert_rows = []
    fallback_pairs = 0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for src in node_ids:
        for dst in node_ids:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if src == dst:
                matrix[src][dst] = 0.0
                continue
            from_db = cache_rows.get((src, dst))
            if from_db:
                matrix[src][dst] = float(from_db["distance_km"])
                db_hit_pairs += 1
                continue
            candidate = _to_float_safe(((incoming_dist.get(src) or {}).get(dst)), 0.0)
            source = "payload"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if candidate <= 0:
                p1 = points.get(src)
                p2 = points.get(dst)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if p1 and p2:
                    # 鐩寸嚎璺濈鍔犵郴鏁颁綔涓哄厹搴曪紝閬垮厤鏃犻敭瀵艰嚧鎻掑叆澶辫触銆?
                    candidate = max(0.1, _haversine_km(p1["lng"], p1["lat"], p2["lng"], p2["lat"]) * 1.18)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                else:
                    candidate = 1.0
                source = "fallback"
                fallback_pairs += 1
            matrix[src][dst] = float(candidate)
            duration = max(1.0, (float(candidate) / 35.0) * 60.0)
            upsert_rows.append((src, dst, float(candidate), duration, source))

    if upsert_rows:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with mysql_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    f"""
                    INSERT INTO {STORE_DISTANCE_TABLE}
                    (from_store_id, to_store_id, distance_km, duration_minutes, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        distance_km = VALUES(distance_km),
                        duration_minutes = VALUES(duration_minutes),
                        source = VALUES(source)
                    """,
                    upsert_rows,
                )
    payload["dist"] = matrix
    payload["distDbStats"] = {
        "storeCount": len(ids),
        "nodeCount": len(node_ids),
        "dbHitPairs": db_hit_pairs,
        "dbWritePairs": len(upsert_rows),
        "fallbackPairs": fallback_pairs,
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return payload


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _normalize_region_path(raw_path):
    data = raw_path
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(data, str):
        try:
            data = json.loads(data)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        except Exception:
            data = []
    points = []
    if not isinstance(data, list):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return points
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in data:
        lng = None
        lat = None
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            lng, lat = item[0], item[1]
        elif isinstance(item, dict):
            lng = item.get("lng", item.get("x"))
            lat = item.get("lat", item.get("y"))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if lng is None or lat is None:
            continue
        try:
            lng_val = float(lng)
            lat_val = float(lat)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            continue
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not math.isfinite(lng_val) or not math.isfinite(lat_val):
            continue
        points.append([round(lng_val, 6), round(lat_val, 6)])
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return points


def _shape_run_region_row(row):
    path = _normalize_region_path((row or {}).get("polygon_path"))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "id": int((row or {}).get("id") or 0),
        "schemeNo": str((row or {}).get("scheme_no") or ""),
        "regionCode": str((row or {}).get("region_code") or ""),
        "name": str((row or {}).get("name") or ""),
        "path": path,
        "storeIds": list((row or {}).get("store_ids") or []),
        "storeNames": list((row or {}).get("store_names") or []),
        "createdAt": (row or {}).get("created_at"),
        "updatedAt": (row or {}).get("updated_at"),
    }


def _shape_run_region_scheme_row(row):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "id": int((row or {}).get("id") or 0),
        "schemeNo": str((row or {}).get("scheme_no") or ""),
        "name": str((row or {}).get("name") or ""),
        "enabled": bool(int((row or {}).get("enabled") or 0)),
        "createdAt": (row or {}).get("created_at"),
        "updatedAt": (row or {}).get("updated_at"),
    }


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def run_region_schemes_list():
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT id, scheme_no, name, enabled, created_at, updated_at
                FROM {RUN_REGION_SCHEME_TABLE}
                ORDER BY enabled DESC, updated_at DESC, id DESC
                """
            )
            rows = cursor.fetchall() or []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT DISTINCT scheme_no
                FROM {RUN_REGION_TABLE}
                WHERE scheme_no IS NOT NULL AND scheme_no <> ''
                ORDER BY scheme_no ASC
                """
            )
            region_scheme_rows = cursor.fetchall() or []
    items = [_shape_run_region_scheme_row(row) for row in rows]
    known_scheme_no = {item.get("schemeNo") for item in items}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in region_scheme_rows:
        scheme_no = str((row or {}).get("scheme_no") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not scheme_no or scheme_no in known_scheme_no:
            continue
        items.append(
            {
                "id": 0,
                "schemeNo": scheme_no,
                "name": f"鏂规{scheme_no}",
                "enabled": True,
                "createdAt": None,
                "updatedAt": None,
            }
        )
    items.sort(key=lambda x: (0 if x.get("enabled") else 1, str(x.get("schemeNo") or "")))
    return {"items": items, "count": len(items)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def run_region_schemes_create(payload):
    ensure_archive_tables()
    scheme_no = str((payload or {}).get("schemeNo") or (payload or {}).get("scheme_no") or "").strip()
    name = str((payload or {}).get("name") or "").strip()
    enabled = 1 if bool((payload or {}).get("enabled", True)) else 0
    if not scheme_no:
        raise ValueError("missing_scheme_no")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not name:
        raise ValueError("missing_scheme_name")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {RUN_REGION_SCHEME_TABLE}
                (scheme_no, name, enabled)
                VALUES (%s, %s, %s)
                """,
                (scheme_no, name, enabled),
            )
            scheme_id = int(cursor.lastrowid or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT id, scheme_no, name, enabled, created_at, updated_at
                FROM {RUN_REGION_SCHEME_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (scheme_id,),
            )
            row = cursor.fetchone() or {}
    return {"item": _shape_run_region_scheme_row(row)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def run_region_schemes_update(payload):
    ensure_archive_tables()
    scheme_id = int((payload or {}).get("id") or 0)
    name = str((payload or {}).get("name") or "").strip()
    enabled = 1 if bool((payload or {}).get("enabled", True)) else 0
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if scheme_id <= 0:
        raise ValueError("missing_scheme_id")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not name:
        raise ValueError("missing_scheme_name")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {RUN_REGION_SCHEME_TABLE}
                SET name=%s, enabled=%s
                WHERE id=%s
                """,
                (name, enabled, scheme_id),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT id, scheme_no, name, enabled, created_at, updated_at
                FROM {RUN_REGION_SCHEME_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (scheme_id,),
            )
            row = cursor.fetchone()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not row:
        raise ValueError("scheme_not_found")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"item": _shape_run_region_scheme_row(row)}


def run_region_schemes_delete(payload):
    ensure_archive_tables()
    scheme_id = int((payload or {}).get("id") or 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if scheme_id <= 0:
        raise ValueError("missing_scheme_id")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"SELECT scheme_no FROM {RUN_REGION_SCHEME_TABLE} WHERE id=%s LIMIT 1",
                (scheme_id,),
            )
            row = cursor.fetchone() or {}
            scheme_no = str(row.get("scheme_no") or "").strip()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not scheme_no:
                raise ValueError("scheme_not_found")
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM {RUN_REGION_TABLE} WHERE scheme_no=%s",
                (scheme_no,),
            )
            region_count = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if region_count > 0:
                raise ValueError("scheme_in_use")
            cursor.execute(
                f"DELETE FROM {RUN_REGION_SCHEME_TABLE} WHERE id=%s",
                (scheme_id,),
            )
            deleted = int(cursor.rowcount or 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"ok": True, "deleted": deleted > 0}


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def run_regions_list(scheme_no=""):
    ensure_archive_tables()
    scheme_no = str(scheme_no or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not scheme_no:
        return {"items": [], "count": 0}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT id, scheme_no, region_code, name, polygon_path, created_at, updated_at
                FROM {RUN_REGION_TABLE}
                WHERE scheme_no=%s
                ORDER BY id DESC
                """
                ,
                (scheme_no,),
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT region_code, store_id, store_name
                FROM {RUN_REGION_MEMBER_TABLE}
                WHERE scheme_no=%s
                """
                ,
                (scheme_no,),
            )
            member_rows = cursor.fetchall() or []

    members_by_code = {}
    for member in member_rows:
        region_code = str((member or {}).get("region_code") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not region_code:
            continue
        bucket = members_by_code.setdefault(region_code, {"storeIds": [], "storeNames": []})
        store_id = str((member or {}).get("store_id") or "").strip()
        store_name = str((member or {}).get("store_name") or "").strip()
        if store_id and store_id not in bucket["storeIds"]:
            bucket["storeIds"].append(store_id)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if store_name and store_name not in bucket["storeNames"]:
            bucket["storeNames"].append(store_name)

    items = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in rows:
        region_code = str((row or {}).get("region_code") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not region_code:
            name = str((row or {}).get("name") or "").strip()
            if name.startswith("鏂规1-"):
                region_code = name.split("鏂规1-", 1)[1].strip()
        bucket = members_by_code.get(region_code, {"storeIds": [], "storeNames": []})
        items.append(
            _shape_run_region_row(
                {
                    **(row or {}),
                    "region_code": region_code,
                    "store_ids": bucket.get("storeIds") or [],
                    "store_names": bucket.get("storeNames") or [],
                }
            )
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"items": items, "count": len(items)}


def run_regions_create(payload):
    ensure_archive_tables()
    scheme_no = str((payload or {}).get("schemeNo") or (payload or {}).get("scheme_no") or "").strip()
    name = str((payload or {}).get("name") or "").strip()
    path = _normalize_region_path((payload or {}).get("path"))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not scheme_no:
        raise ValueError("missing_scheme_no")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not name:
        raise ValueError("missing_region_name")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if len(path) < 3:
        raise ValueError("invalid_region_path")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {RUN_REGION_SCHEME_TABLE} WHERE scheme_no=%s LIMIT 1",
                (scheme_no,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not cursor.fetchone():
                raise ValueError("scheme_not_found")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {RUN_REGION_TABLE}
                (scheme_no, region_code, name, polygon_path)
                VALUES (%s, %s, %s, %s)
                """,
                (scheme_no, "", name, json.dumps(path, ensure_ascii=False)),
            )
            region_id = int(cursor.lastrowid or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT id, scheme_no, region_code, name, polygon_path, created_at, updated_at
                FROM {RUN_REGION_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (region_id,),
            )
            row = cursor.fetchone() or {}
    return {"item": _shape_run_region_row(row)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def run_regions_update(payload):
    ensure_archive_tables()
    region_id = int((payload or {}).get("id") or 0)
    scheme_no = str((payload or {}).get("schemeNo") or (payload or {}).get("scheme_no") or "").strip()
    name = str((payload or {}).get("name") or "").strip()
    path = _normalize_region_path((payload or {}).get("path"))
    if region_id <= 0:
        raise ValueError("missing_region_id")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not scheme_no:
        raise ValueError("missing_scheme_no")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not name:
        raise ValueError("missing_region_name")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if len(path) < 3:
        raise ValueError("invalid_region_path")
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {RUN_REGION_SCHEME_TABLE} WHERE scheme_no=%s LIMIT 1",
                (scheme_no,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not cursor.fetchone():
                raise ValueError("scheme_not_found")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                UPDATE {RUN_REGION_TABLE}
                SET scheme_no=%s, region_code=%s, name=%s, polygon_path=%s
                WHERE id=%s
                """,
                (scheme_no, "", name, json.dumps(path, ensure_ascii=False), region_id),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT id, scheme_no, region_code, name, polygon_path, created_at, updated_at
                FROM {RUN_REGION_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (region_id,),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError("region_not_found")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"item": _shape_run_region_row(row)}


def run_regions_delete(payload):
    ensure_archive_tables()
    region_id = int((payload or {}).get("id") or 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if region_id <= 0:
        raise ValueError("missing_region_id")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {RUN_REGION_TABLE} WHERE id=%s",
                (region_id,),
            )
            deleted = int(cursor.rowcount or 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"ok": True, "deleted": deleted > 0}


def _safe_int(value, default=0):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if value is None:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return int(default)
        text = str(value).strip()
        if not text:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return int(default)
        return int(float(text))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return int(default)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _safe_float(value, default=0.0):
    try:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if value is None:
            return float(default)
        text = str(value).strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not text:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return float(default)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return float(text)
    except Exception:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return float(default)


def _normalize_store_id(value):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return str(value or "").strip()


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _normalize_vehicle_id(value):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return str(value or "").strip()


def _normalize_route_id(value):
    rid = _safe_int(value, 0)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return rid if rid > 0 else 0


def _jaccard_related(set_a, set_b):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not set_a or not set_b:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return False
    inter = len(set_a & set_b)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if inter <= 0:
        return False
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if set_a.issubset(set_b) or set_b.issubset(set_a):
        return True
    union = len(set_a | set_b)
    jaccard = (inter / union) if union else 0.0
    overlap = inter / max(1, min(len(set_a), len(set_b)))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return jaccard >= 0.6 or overlap >= 0.8


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _route_components(route_store_map):
    route_ids = sorted(route_store_map.keys())
    adj = {rid: set() for rid in route_ids}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for i in range(len(route_ids)):
        for j in range(i + 1, len(route_ids)):
            a = route_ids[i]
            b = route_ids[j]
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if abs(a - b) != 100:
                continue
            if _jaccard_related(route_store_map.get(a, set()), route_store_map.get(b, set())):
                adj[a].add(b)
                adj[b].add(a)
    visited = set()
    groups = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for rid in route_ids:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if rid in visited:
            continue
        stack = [rid]
        comp = []
        visited.add(rid)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        while stack:
            cur = stack.pop()
            comp.append(cur)
            for nxt in adj.get(cur, set()):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if nxt in visited:
                    continue
                visited.add(nxt)
                stack.append(nxt)
        groups.append(sorted(comp))
    return groups


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _convex_hull(points):
    uniq = sorted(set((round(float(x), 6), round(float(y), 6)) for x, y in points))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if len(uniq) <= 2:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return [[p[0], p[1]] for p in uniq]

    def cross(o, a, b):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    lower = []
    for p in uniq:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for p in reversed(uniq):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    hull = lower[:-1] + upper[:-1]
    return [[p[0], p[1]] for p in hull]


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _polygon_centroid(path):
    if not path:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return [116.4, 39.9]
    sx = 0.0
    sy = 0.0
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for p in path:
        sx += float(p[0])
        sy += float(p[1])
    n = max(1, len(path))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return [sx / n, sy / n]


def _shrink_polygon(path, ratio=0.9):
    c = _polygon_centroid(path)
    out = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for p in path:
        out.append([
            c[0] + (float(p[0]) - c[0]) * ratio,
            c[1] + (float(p[1]) - c[1]) * ratio,
        ])
    return out


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _point_in_polygon(point, polygon):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not polygon or len(polygon) < 3:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return False
    x = float(point[0])
    y = float(point[1])
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        xi, yi = float(polygon[i][0]), float(polygon[i][1])
        xj, yj = float(polygon[j][0]), float(polygon[j][1])
        intersects = ((yi > y) != (yj > y)) and (x < ((xj - xi) * (y - yi)) / ((yj - yi) or 1e-12) + xi)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if intersects:
            inside = not inside
        j = i
    return inside


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _segments_intersect(a, b, c, d):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def orient(p, q, r):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])

    o1 = orient(a, b, c)
    o2 = orient(a, b, d)
    o3 = orient(c, d, a)
    o4 = orient(c, d, b)
    return (o1 * o2 < 0) and (o3 * o4 < 0)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _polygons_overlap(poly_a, poly_b):
    if len(poly_a) < 3 or len(poly_b) < 3:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return False
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for i in range(len(poly_a)):
        a1 = poly_a[i]
        a2 = poly_a[(i + 1) % len(poly_a)]
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for j in range(len(poly_b)):
            b1 = poly_b[j]
            b2 = poly_b[(j + 1) % len(poly_b)]
            if _segments_intersect(a1, a2, b1, b2):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return True
    if _point_in_polygon(poly_a[0], poly_b):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return True
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if _point_in_polygon(poly_b[0], poly_a):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return True
    return False


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _fallback_polygon(points):
    pts = list(points or [])
    if not pts:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if len(pts) == 1:
        x, y = pts[0]
        d = 0.01
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return [[x - d, y - d], [x + d, y - d], [x + d, y + d], [x - d, y + d]]
    if len(pts) == 2:
        x1, y1 = pts[0]
        x2, y2 = pts[1]
        d = 0.006
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return [[x1 - d, y1 - d], [x2 + d, y2 - d], [x2 + d, y2 + d], [x1 - d, y1 + d]]
    return _convex_hull(pts)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _shape_non_overlap(path, exists):
    candidate = [list(p) for p in path]
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for _ in range(8):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if all(not _polygons_overlap(candidate, other) for other in exists):
            return candidate
        candidate = _shrink_polygon(candidate, 0.86)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return candidate


def _build_scheme1_regions_from_human_rows(rows, store_points_map=None):
    groups = {}
    store_meta = {}
    point_map = store_points_map or {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in rows or []:
        delivery_date = str(row.get("delivery_date") or "").strip()
        vehicle_id = _normalize_vehicle_id(row.get("vehicle_id"))
        route_id = _normalize_route_id(row.get("route_id"))
        store_id = _normalize_store_id(row.get("store_id"))
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not delivery_date or not vehicle_id or route_id <= 0 or not store_id:
            continue
        store_name = str(row.get("store_name") or "").strip()
        lng = row.get("lng")
        lat = row.get("lat")
        lng_val = _safe_float(lng, 0.0)
        lat_val = _safe_float(lat, 0.0)
        valid_lnglat = math.isfinite(lng_val) and math.isfinite(lat_val) and (abs(lng_val) > 1e-9 or abs(lat_val) > 1e-9)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if (not valid_lnglat) and store_id in point_map:
            fallback = point_map.get(store_id) or {}
            lng_val = _safe_float(fallback.get("lng"), 0.0)
            lat_val = _safe_float(fallback.get("lat"), 0.0)
            valid_lnglat = math.isfinite(lng_val) and math.isfinite(lat_val) and (abs(lng_val) > 1e-9 or abs(lat_val) > 1e-9)
        if store_id not in store_meta:
            store_meta[store_id] = {"storeName": store_name, "lng": None, "lat": None}
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if store_name and not store_meta[store_id].get("storeName"):
            store_meta[store_id]["storeName"] = store_name
        if valid_lnglat:
            store_meta[store_id]["lng"] = round(lng_val, 6)
            store_meta[store_id]["lat"] = round(lat_val, 6)
        gkey = (delivery_date, vehicle_id)
        g = groups.setdefault(gkey, {})
        g.setdefault(route_id, set()).add(store_id)

    region_agg = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for (delivery_date, vehicle_id), route_map in groups.items():
        comps = _route_components(route_map)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for comp in comps:
            route_ids = sorted(comp)
            region_code = "_".join(str(x) for x in route_ids)
            stores = set()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for rid in route_ids:
                stores.update(route_map.get(rid, set()))
            key = region_code
            if key not in region_agg:
                region_agg[key] = {
                    "regionCode": region_code,
                    "routeIds": route_ids,
                    "storeIds": set(),
                    "occurs": 0,
                    "records": [],
                }
            region_agg[key]["storeIds"].update(stores)
            region_agg[key]["occurs"] += 1
            region_agg[key]["records"].append({"deliveryDate": delivery_date, "vehicleId": vehicle_id})

    existing_paths = []
    regions = []
    members = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for region_code in sorted(region_agg.keys()):
        data = region_agg[region_code]
        points = []
        for sid in sorted(data["storeIds"]):
            meta = store_meta.get(sid) or {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if meta.get("lng") is None or meta.get("lat") is None:
                continue
            points.append((meta["lng"], meta["lat"]))
            members.append(
                {
                    "regionCode": region_code,
                    "storeId": sid,
                    "storeName": str(meta.get("storeName") or ""),
                    "lng": meta.get("lng"),
                    "lat": meta.get("lat"),
                }
            )
        path = _convex_hull(points) if len(points) >= 3 else _fallback_polygon(points)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if len(path) >= 3:
            path = _shape_non_overlap(path, existing_paths)
            existing_paths.append(path)
        regions.append(
            {
                "regionCode": region_code,
                "name": f"鏂规1-{region_code}",
                "routeIds": data["routeIds"],
                "storeIds": sorted(data["storeIds"]),
                "path": path,
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "regions": regions,
        "members": members,
        "groupCount": len(groups),
        "storeMetaCount": len(store_meta),
    }


def run_regions_generate_scheme1(payload=None):
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='human_dispatch_routes'
                """,
                (MYSQL_DATABASE,),
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if int((cursor.fetchone() or {}).get("cnt") or 0) <= 0:
                raise ValueError("human_dispatch_routes_not_found")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='human_dispatch_routes' AND COLUMN_NAME='delivery_date'
                """,
                (MYSQL_DATABASE,),
            )
            if int((cursor.fetchone() or {}).get("cnt") or 0) <= 0:
                raise ValueError("human_dispatch_routes_missing_required_columns")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='human_dispatch_routes'
                """,
                (MYSQL_DATABASE,),
            )
            cols = [str((x or {}).get("COLUMN_NAME") or "") for x in (cursor.fetchall() or [])]
            norm = {re.sub(r"[\s_\-]+", "", c.lower()): c for c in cols}
            lng_col = norm.get("lng") or norm.get("longitude") or norm.get("缁忓害")
            lat_col = norm.get("lat") or norm.get("latitude") or norm.get("绾害")
            select_parts = ["delivery_date", "vehicle_id", "route_id", "store_id", "store_name"]
            if lng_col:
                select_parts.append(f"`{lng_col}` AS lng")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            else:
                select_parts.append("NULL AS lng")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if lat_col:
                select_parts.append(f"`{lat_col}` AS lat")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            else:
                select_parts.append("NULL AS lat")
            cursor.execute(
                f"""
                SELECT {", ".join(select_parts)}
                FROM human_dispatch_routes
                WHERE delivery_date IS NOT NULL AND vehicle_id IS NOT NULL AND route_id IS NOT NULL AND store_id IS NOT NULL
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
            store_ids = sorted(
                {
                    _normalize_store_id((r or {}).get("store_id"))
                    for r in rows
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if _normalize_store_id((r or {}).get("store_id"))
                }
            )
            store_points_map = {}
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if store_ids:
                placeholders = ",".join(["%s"] * len(store_ids))
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                try:
                    cursor.execute(
                        f"""
                        SELECT shop_code AS store_id, shop_name AS store_name, lng, lat
                        FROM c_shop_main
                        WHERE shop_code IN ({placeholders})
                        """,
                        tuple(store_ids),
                    )
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    for p in cursor.fetchall() or []:
                        sid = _normalize_store_id((p or {}).get("store_id"))
                        if not sid:
                            continue
                        lng_val = _safe_float((p or {}).get("lng"), 0.0)
                        lat_val = _safe_float((p or {}).get("lat"), 0.0)
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if not (math.isfinite(lng_val) and math.isfinite(lat_val)):
                            continue
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        if abs(lng_val) <= 1e-9 and abs(lat_val) <= 1e-9:
                            continue
                        store_points_map[sid] = {
                            "lng": round(lng_val, 6),
                            "lat": round(lat_val, 6),
                            "storeName": str((p or {}).get("store_name") or "").strip(),
                        }
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                except Exception:
                    store_points_map = {}

    built = _build_scheme1_regions_from_human_rows(rows, store_points_map)
    regions = built["regions"]
    members = built["members"]

    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {RUN_REGION_SCHEME_TABLE} (scheme_no, name, enabled)
                VALUES ('1', '涓氬姟鍒嗗尯鏂规1', 1)
                ON DUPLICATE KEY UPDATE name=VALUES(name), enabled=1
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {RUN_REGION_TABLE} WHERE scheme_no='1'")
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {RUN_REGION_MEMBER_TABLE} WHERE scheme_no='1'")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for item in regions:
                path = _normalize_region_path(item.get("path"))
                if len(path) < 3:
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    f"""
                    INSERT INTO {RUN_REGION_TABLE} (scheme_no, region_code, name, polygon_path)
                    VALUES (%s, %s, %s, %s)
                    """,
                    ("1", str(item.get("regionCode") or ""), str(item.get("name") or ""), json.dumps(path, ensure_ascii=False)),
                )
            for m in members:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(
                    f"""
                    INSERT INTO {RUN_REGION_MEMBER_TABLE}
                    (scheme_no, region_code, delivery_date, vehicle_id, store_id, store_name, lng, lat)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      store_name=VALUES(store_name), lng=VALUES(lng), lat=VALUES(lat)
                    """,
                    (
                        "1",
                        str(m.get("regionCode") or ""),
                        None,
                        None,
                        str(m.get("storeId") or ""),
                        str(m.get("storeName") or ""),
                        m.get("lng"),
                        m.get("lat"),
                    ),
                )
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "ok": True,
        "schemeNo": "1",
        "regionCount": len([x for x in regions if len(_normalize_region_path(x.get("path"))) >= 3]),
        "rawRegionCount": len(regions),
        "memberCount": len(members),
        "groupCount": int(built.get("groupCount") or 0),
        "sourceRowCount": len(rows),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def shops_list():
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS c_shop_main (
                    shop_code VARCHAR(16) PRIMARY KEY,
                    shop_name VARCHAR(100) NOT NULL,
                    district VARCHAR(64) NULL,
                    lng DECIMAL(12,6) NOT NULL DEFAULT 0,
                    lat DECIMAL(12,6) NOT NULL DEFAULT 0,
                    trip_count INT NOT NULL DEFAULT 1,
                    boxes INT NOT NULL DEFAULT 0,
                    cold_ratio DECIMAL(8,4) NOT NULL DEFAULT 0,
                    wave1_time VARCHAR(8) NULL,
                    wave2_time VARCHAR(8) NULL,
                    wave_belongs VARCHAR(64) NULL,
                    service_minutes INT NOT NULL DEFAULT 15,
                    difficulty DECIMAL(6,2) NOT NULL DEFAULT 1.00,
                    allowed_late_minutes INT NOT NULL DEFAULT 10,
                    schedule_status VARCHAR(20) NULL,
                    plate_no VARCHAR(32) NULL,
                    address VARCHAR(255) NULL,
                    detailed_address VARCHAR(512) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                "SHOW COLUMNS FROM c_shop_main"
            )
            columns = {row["Field"] for row in cursor.fetchall()}

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            def pick(field, alias=None, default_sql="''"):
                out = alias or field
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if field in columns:
                    return f"`{field}` AS `{out}`"
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return f"{default_sql} AS `{out}`"

            select_sql = f"""
                SELECT
                    {pick('shop_code')},
                    {pick('shop_name')},
                    {pick('district')},
                    {pick('lng', default_sql='0')},
                    {pick('lat', default_sql='0')},
                    {pick('trip_count', default_sql='1')},
                    {pick('boxes', default_sql='0')},
                    {pick('cold_ratio', default_sql='0')},
                    {pick('wave1_time')},
                    {pick('wave2_time')},
                    {pick('wave_belongs')},
                    {pick('service_minutes', default_sql='15')},
                    {pick('difficulty', default_sql='1')},
                    {pick('allowed_late_minutes', default_sql='10')},
                    {pick('schedule_status')},
                    {pick('plate_no')},
                    {pick('address')},
                    {pick('detailed_address')}
                FROM c_shop_main
                ORDER BY shop_code
            """
            cursor.execute(select_sql)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"shops": rows, "count": len(rows)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def store_points_list():
    data = shops_list()
    points = []
    for item in (data or {}).get("shops") or []:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        try:
            lng = float(item.get("lng") or 0)
            lat = float(item.get("lat") or 0)
        except Exception:
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if (not math.isfinite(lng)) or (not math.isfinite(lat)):
            continue
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if abs(lng) < 1e-9 and abs(lat) < 1e-9:
            continue
        points.append(
            {
                "store_id": str(item.get("shop_code") or ""),
                "store_name": str(item.get("shop_name") or ""),
                "lng": round(lng, 6),
                "lat": round(lat, 6),
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"items": points, "count": len(points)}


def _normalize_col_name(value):
    text = str(value or "").replace("\ufeff", "").strip().lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return re.sub(r"[\s_\-閿涘牞绱?)锛堬級]+", "", text)


def _wms_secret_key():
    seed = f"{socket.gethostname()}|{MYSQL_DATABASE}|wms-local-secret".encode("utf-8")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return hashlib.sha256(seed).digest()


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _encrypt_local_password(plain_text):
    raw = str(plain_text or "").encode("utf-8")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not raw:
        return ""
    key = _wms_secret_key()
    enc = bytes([b ^ key[i % len(key)] for i, b in enumerate(raw)])
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return base64.b64encode(enc).decode("ascii")


def _decrypt_local_password(cipher_text):
    cipher = str(cipher_text or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not cipher:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return ""
    key = _wms_secret_key()
    raw = base64.b64decode(cipher.encode("ascii"))
    dec = bytes([b ^ key[i % len(key)] for i, b in enumerate(raw)])
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return dec.decode("utf-8", errors="ignore")


def _load_saved_wms_password():
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"SELECT pwd_cipher FROM {WMS_CRED_TABLE} WHERE id=1 LIMIT 1"
            )
            row = cursor.fetchone() or {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return _decrypt_local_password(row.get("pwd_cipher") or "")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _save_wms_password(password):
    ensure_archive_tables()
    cipher = _encrypt_local_password(password)
    if not cipher:
        return
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                INSERT INTO {WMS_CRED_TABLE}(id, pwd_cipher)
                VALUES (1, %s)
                ON DUPLICATE KEY UPDATE pwd_cipher=VALUES(pwd_cipher)
                """,
                (cipher,),
            )


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _clear_wms_password():
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {WMS_CRED_TABLE} WHERE id=1")


def _to_jsonable(value):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(value, (datetime, date)):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return value.isoformat()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(value, Decimal):
        try:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return float(value)
        except Exception:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return str(value)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if isinstance(value, bytes):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return value.decode("utf-8", errors="ignore")
    return value


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _normalize_numeric_types_for_solver(value):
    if isinstance(value, Decimal):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return float(value)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if isinstance(value, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {k: _normalize_numeric_types_for_solver(v) for k, v in value.items()}
    if isinstance(value, list):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return [_normalize_numeric_types_for_solver(v) for v in value]
    return value


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _to_date(value):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if value is None:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return None
    if isinstance(value, datetime):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return value.date()
    if isinstance(value, date):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return value
    text = str(value).strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not text:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        try:
            return datetime.strptime(text[:19], fmt).date()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception:
            pass
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return datetime.fromisoformat(text[:19]).date()
    except Exception:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return None


def _to_float(value, default=0.0):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if value is None or str(value).strip() == "":
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return float(default)
        return float(value)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception:
        return float(default)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _pick_by_candidates(row, candidates, default=""):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(row, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return default
    lookup = {_normalize_col_name(k): v for k, v in row.items()}
    for key in candidates:
        nk = _normalize_col_name(key)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if nk in lookup:
            return lookup[nk]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return default


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _detect_remote_date_column(columns, candidates):
    normalized = {_normalize_col_name(c): c for c in (columns or [])}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for key in candidates:
        nk = _normalize_col_name(key)
        if nk in normalized:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return normalized[nk]
    return None


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _fetch_rows_sqlserver(cursor, table_name, date_column=None, start_date=None, end_date=None):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if date_column and start_date and end_date:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        sql = f"SELECT * FROM [{table_name}] WITH (NOLOCK) WHERE CAST([{date_column}] AS DATE) >= ? AND CAST([{date_column}] AS DATE) <= ?"
        cursor.execute(sql, start_date, end_date)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    else:
        cursor.execute(f"SELECT * FROM [{table_name}] WITH (NOLOCK)")
    columns = [str(col[0]) for col in (cursor.description or [])]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    rows = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in cursor.fetchall() or []:
        row = {}
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for idx, col in enumerate(columns):
            row[col] = _to_jsonable(item[idx])
        rows.append(row)
    return rows, columns


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _wms_sqlserver_connection(password):
    if pyodbc is None:
        raise RuntimeError("pyodbc_not_installed")
    conn_str = (
        f"Driver={{SQL Server}};"
        f"Server={WMS_REMOTE_SERVER};"
        f"Database={WMS_REMOTE_DATABASE};"
        f"UID={WMS_REMOTE_UID};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "ApplicationIntent=ReadOnly;"
    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return pyodbc.connect(conn_str, timeout=12, autocommit=True)


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _table_last_date(table_name):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"SELECT MAX(business_date) AS max_date FROM {WMS_RAW_TABLE} WHERE source_table=%s",
                (table_name,),
            )
            row = cursor.fetchone() or {}
    return _to_date(row.get("max_date"))


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _insert_wms_batch_start(batch_id, mode):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {WMS_BATCH_TABLE}(batch_id, mode, success_flag)
                VALUES (%s, %s, 0)
                """,
                (batch_id, mode),
            )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _finish_wms_batch(batch_id, success_flag, summary=None, error_text=""):
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                UPDATE {WMS_BATCH_TABLE}
                SET finished_at=NOW(), success_flag=%s, summary_json=%s, error_text=%s
                WHERE batch_id=%s
                """,
                (
                    1 if success_flag else 0,
                    json.dumps(summary or {}, ensure_ascii=False),
                    str(error_text or ""),
                    batch_id,
                ),
            )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _upsert_wms_snapshots(batch_id, source_table, row, business_date, row_hash, payload_json):
    shop_code = str(_pick_by_candidates(row, ["shop_code", "搴楅摵浠ｇ爜", "闂ㄥ簵浠ｇ爜", "store_id", "shopid"], "")).strip()
    shop_name = str(_pick_by_candidates(row, ["shop_name", "搴楅摵鍚嶇О", "闂ㄥ簵鍚嶇О", "store_name"], "")).strip()
    district = str(_pick_by_candidates(row, ["district", "鍖哄煙", "澶у尯"], "")).strip()
    lng = _to_float(_pick_by_candidates(row, ["lng", "longitude", "缁忓害", "lon"], None), 0.0)
    lat = _to_float(_pick_by_candidates(row, ["lat", "latitude", "绾害"], None), 0.0)
    plate_no = str(_pick_by_candidates(row, ["plate_no", "杞︾墝鍙?, "杞﹀彿", "vehicle_id", "truck_no"], "")).strip()
    driver_name = str(_pick_by_candidates(row, ["driver_name", "鍙告満", "driver"], "")).strip()
    ambient_qty = _to_float(_pick_by_candidates(row, ["ambient_qty", "甯告俯", "甯告俯绠辨暟", "normal_qty"], None), 0.0)
    cold_qty = _to_float(_pick_by_candidates(row, ["cold_qty", "鍐疯棌", "鍐疯棌绠辨暟"], None), 0.0)
    frozen_qty = _to_float(_pick_by_candidates(row, ["frozen_qty", "鍐峰喕", "鍐峰喕绠辨暟"], None), 0.0)
    total_boxes = _to_float(_pick_by_candidates(row, ["total_boxes", "鎬荤鏁?, "绠辨暟", "qty", "cargo_qty"], None), ambient_qty + cold_qty + frozen_qty)
    load_factor = _to_float(_pick_by_candidates(row, ["load_factor", "瑁呰浇鑳藉姏", "瑁呰浇绯绘暟", "capacity_factor"], None), 0.0)
    route_id = str(_pick_by_candidates(row, ["route_id", "绾胯矾鍙?, "route_no"], "")).strip()
    arrival_time = str(_pick_by_candidates(row, ["arrival_time", "鍒板簵鏃堕棿", "arrive_time", "eta"], "")).strip()

    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT IGNORE INTO {WMS_RAW_TABLE}
                (batch_id, source_table, business_date, shop_code, plate_no, row_hash, payload_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (batch_id, source_table, business_date, shop_code or None, plate_no or None, row_hash, payload_json),
            )
            inserted_raw = int(cursor.rowcount or 0) > 0
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not inserted_raw:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return False
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if source_table == "C_SHOP_MAIN" and shop_code:
                cursor.execute(
                    f"""
                    INSERT INTO {WMS_SHOP_TABLE}(shop_code, shop_name, district, lng, lat, latest_business_date, source_table, payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        shop_name=VALUES(shop_name),
                        district=VALUES(district),
                        lng=VALUES(lng),
                        lat=VALUES(lat),
                        latest_business_date=VALUES(latest_business_date),
                        payload_json=VALUES(payload_json)
                    """,
                    (shop_code, shop_name, district, lng, lat, business_date, source_table, payload_json),
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if source_table == "C_Route_Driver" and plate_no:
                cursor.execute(
                    f"""
                    INSERT INTO {WMS_VEHICLE_TABLE}(plate_no, driver_name, latest_business_date, source_table, payload_json)
                    VALUES (%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        driver_name=VALUES(driver_name),
                        latest_business_date=VALUES(latest_business_date),
                        payload_json=VALUES(payload_json)
                    """,
                    (plate_no, driver_name, business_date, source_table, payload_json),
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if source_table == "CargoQTY" and shop_code:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    f"""
                    INSERT IGNORE INTO {WMS_CARGO_TABLE}
                    (business_date, shop_code, ambient_qty, cold_qty, frozen_qty, total_boxes, source_table, row_hash, payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (business_date, shop_code, ambient_qty, cold_qty, frozen_qty, total_boxes, source_table, row_hash, payload_json),
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if source_table == "CarLoad" and plate_no:
                cursor.execute(
                    f"""
                    INSERT IGNORE INTO {WMS_CARLOAD_TABLE}
                    (business_date, plate_no, load_factor, source_table, row_hash, payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (business_date, plate_no, load_factor, source_table, row_hash, payload_json),
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if source_table == "arrivaltime":
                cursor.execute(
                    f"""
                    INSERT IGNORE INTO {WMS_ARRIVAL_TABLE}
                    (business_date, shop_code, plate_no, route_id, arrival_time, source_table, row_hash, payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (business_date, shop_code or None, plate_no or None, route_id or None, arrival_time or None, source_table, row_hash, payload_json),
                )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return True


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _wms_table_configs():
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return [
        {"name": "C_Route_Driver", "date_cols": ["delivery_date", "涓氬姟鏃ユ湡", "date", "dispatch_date"]},
        {"name": "C_SHOP_MAIN", "date_cols": ["delivery_date", "涓氬姟鏃ユ湡", "date", "updated_at"]},
        {"name": "CargoQTY", "date_cols": ["delivery_date", "涓氬姟鏃ユ湡", "date", "order_date"]},
        {"name": "CarLoad", "date_cols": ["delivery_date", "涓氬姟鏃ユ湡", "date", "updated_at"]},
        {"name": "arrivaltime", "date_cols": ["delivery_date", "涓氬姟鏃ユ湡", "date", "arrive_date"]},
    ]


def wms_fetch(payload):
    ensure_archive_tables()
    raw_password = str((payload or {}).get("password") or "").strip()
    force_full = bool((payload or {}).get("forceFull"))
    password = raw_password or _load_saved_wms_password()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not password:
        return {"ok": False, "needPassword": True, "error": "missing_password"}

    batch_id = f"wms-{int(time.time() * 1000)}"
    mode = "incremental"
    _insert_wms_batch_start(batch_id, "incremental")
    summary = {"batchId": batch_id, "tables": [], "mode": "incremental"}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with mysql_connection() as conn:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(1) AS cnt FROM {WMS_RAW_TABLE}")
                has_data = int((cursor.fetchone() or {}).get("cnt") or 0) > 0
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if force_full or (not has_data):
            mode = "full_5_months"
        summary["mode"] = mode

        end_date = date.today()
        start_five_months = end_date - timedelta(days=153)

        with _wms_sqlserver_connection(password) as remote_conn:
            remote_cursor = remote_conn.cursor()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for item in _wms_table_configs():
                table = item["name"]
                local_last = _table_last_date(table)
                start_date = (local_last + timedelta(days=1)) if (local_last and mode == "incremental") else start_five_months
                remote_cursor.execute(f"SELECT TOP 0 * FROM [{table}]")
                columns = [str(col[0]) for col in (remote_cursor.description or [])]
                date_col = _detect_remote_date_column(columns, item.get("date_cols") or [])
                rows, _ = _fetch_rows_sqlserver(remote_cursor, table, date_col, start_date if date_col else None, end_date if date_col else None)

                inserted = 0
                skipped = 0
                max_date = local_last
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                for row in rows:
                    business_date = _to_date(_pick_by_candidates(row, [date_col] if date_col else []))
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if business_date and (max_date is None or business_date > max_date):
                        max_date = business_date
                    payload_json = json.dumps(row, ensure_ascii=False, sort_keys=True, default=_to_jsonable)
                    row_hash = hashlib.sha256(f"{table}|{payload_json}".encode("utf-8")).hexdigest()
                    if _upsert_wms_snapshots(batch_id, table, row, business_date, row_hash, payload_json):
                        inserted += 1
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    else:
                        skipped += 1

                summary["tables"].append(
                    {
                        "table": table,
                        "dateColumn": date_col or "",
                        "startDate": start_date.isoformat() if isinstance(start_date, date) else "",
                        "endDate": end_date.isoformat(),
                        "readRows": len(rows),
                        "insertedRows": inserted,
                        "skippedRows": skipped,
                        "maxBusinessDate": max_date.isoformat() if isinstance(max_date, date) else "",
                    }
                )

        if raw_password:
            _save_wms_password(raw_password)
        _finish_wms_batch(batch_id, True, summary=summary, error_text="")
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {"ok": True, "needPassword": False, **summary}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception as exc:
        msg = str(exc)
        auth_failed = ("login failed" in msg.lower()) or ("08001" in msg.lower()) or ("password" in msg.lower())
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if auth_failed:
            _clear_wms_password()
        _finish_wms_batch(batch_id, False, summary=summary, error_text=msg)
        return {"ok": False, "needPassword": auth_failed, "error": msg, **summary}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def wms_status():
    ensure_archive_tables()
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT batch_id, started_at, finished_at, mode, success_flag, summary_json, error_text
                FROM {WMS_BATCH_TABLE}
                ORDER BY id DESC
                LIMIT 1
                """
            )
            latest = cursor.fetchone() or {}
            counts = {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for table_name, alias in [
                (WMS_SHOP_TABLE, "shops"),
                (WMS_VEHICLE_TABLE, "vehicles"),
                (WMS_CARGO_TABLE, "cargo"),
                (WMS_CARLOAD_TABLE, "carload"),
                (WMS_ARRIVAL_TABLE, "arrivaltime"),
            ]:
                cursor.execute(f"SELECT COUNT(1) AS cnt FROM {table_name}")
                counts[alias] = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"SELECT COUNT(1) AS cnt FROM {WMS_CRED_TABLE} WHERE id=1")
            has_cred = int((cursor.fetchone() or {}).get("cnt") or 0) > 0
    return {
        "hasSavedCredential": has_cred,
        "latestBatch": latest,
        "counts": counts,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def wms_stores_list():
    ensure_archive_tables()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT shop_code, shop_name, district, lng, lat, latest_business_date
                FROM {WMS_SHOP_TABLE}
                ORDER BY shop_code
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    return {"items": rows, "count": len(rows)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def wms_vehicles_list():
    ensure_archive_tables()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT plate_no, driver_name, latest_business_date
                FROM {WMS_VEHICLE_TABLE}
                ORDER BY plate_no
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    return {"items": rows, "count": len(rows)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def wms_cargo_latest():
    ensure_archive_tables()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT c.shop_code, c.business_date, c.ambient_qty, c.cold_qty, c.frozen_qty, c.total_boxes
                FROM {WMS_CARGO_TABLE} c
                INNER JOIN (
                    SELECT shop_code, MAX(id) AS max_id
                    FROM {WMS_CARGO_TABLE}
                    GROUP BY shop_code
                ) t ON t.max_id = c.id
                ORDER BY c.shop_code
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    return {"items": rows, "count": len(rows)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _normalize_cargo_raw_row(row, source_table="CargoQTY"):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(row, dict):
        raise ValueError("cargo_raw_row_must_be_dict")
    source_table = str(source_table or "CargoQTY").strip() or "CargoQTY"
    shop_code = str(_pick_by_candidates(row, ["shop_code", "搴楅摵浠ｇ爜", "闂ㄥ簵浠ｇ爜", "store_id", "shopid"], "")).strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not shop_code:
        raise ValueError("shop_code_required")
    business_date = _to_date(_pick_by_candidates(row, ["business_date", "涓氬姟鏃ユ湡", "date", "day"], None))
    rpcs = _to_float(_pick_by_candidates(row, ["rpcs", "甯告俯鏁寸悊绠?], 0.0), 0.0)
    rcase = _to_float(_pick_by_candidates(row, ["rcase", "甯告俯鏁寸", "甯告俯鏁寸锛堟按锛?], 0.0), 0.0)
    bpcs = _to_float(_pick_by_candidates(row, ["bpcs", "鍐峰喕鍖?], 0.0), 0.0)
    bpaper = _to_float(_pick_by_candidates(row, ["bpaper", "鍐峰喕绾哥"], 0.0), 0.0)
    apcs = _to_float(_pick_by_candidates(row, ["apcs", "鍐疯棌绛?], 0.0), 0.0)
    apaper = _to_float(_pick_by_candidates(row, ["apaper", "鍐疯棌绾哥"], 0.0), 0.0)
    rpaper = _to_float(_pick_by_candidates(row, ["rpaper", "甯告俯绾哥"], 0.0), 0.0)
    payload = {
        "shop_code": shop_code,
        "business_date": business_date.isoformat() if isinstance(business_date, date) else None,
        "rpcs": rpcs,
        "rcase": rcase,
        "bpcs": bpcs,
        "bpaper": bpaper,
        "apcs": apcs,
        "apaper": apaper,
        "rpaper": rpaper,
        "source_table": source_table,
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _cargo_raw_json_default(value):
        if isinstance(value, Decimal):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if value == value.to_integral_value():
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return int(value)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return float(value)
        if isinstance(value, (datetime, date)):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return value.isoformat()
        return str(value)

    payload_json = json.dumps(row, ensure_ascii=False, sort_keys=True, default=_cargo_raw_json_default)
    row_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_cargo_raw_json_default).encode("utf-8")
    ).hexdigest()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        **payload,
        "row_hash": row_hash,
        "payload_json": payload_json,
    }


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def wms_cargo_raw_save(payload):
    ensure_archive_tables()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError("rows_required")
    source_table = str((payload or {}).get("sourceTable") or (payload or {}).get("source_table") or "CargoQTY").strip() or "CargoQTY"
    normalized_rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in rows:
        normalized_rows.append(_normalize_cargo_raw_row(item, source_table=source_table))
    inserted = 0
    skipped = 0
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for item in normalized_rows:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(
                    f"""
                    INSERT IGNORE INTO {WMS_CARGO_RAW_TABLE}
                    (shop_code, business_date, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper, source_table, row_hash, payload_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        item["shop_code"],
                        item["business_date"],
                        item["rpcs"],
                        item["rcase"],
                        item["bpcs"],
                        item["bpaper"],
                        item["apcs"],
                        item["apaper"],
                        item["rpaper"],
                        item["source_table"],
                        item["row_hash"],
                        item["payload_json"],
                    ),
                )
                if int(cursor.rowcount or 0) > 0:
                    inserted += 1
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    skipped += 1
    return {
        "ok": True,
        "sourceTable": source_table,
        "insertedRows": inserted,
        "skippedRows": skipped,
        "count": len(normalized_rows),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _ensure_clean_cargo_table(cursor):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {WMS_CARGO_RAW_CLEAN_TABLE} (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          shop_code VARCHAR(32) NOT NULL,
          rpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          rcase DECIMAL(18,6) NOT NULL DEFAULT 0,
          bpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          bpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          apcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          apaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          rpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          source_tag VARCHAR(64) NULL,
          batch_tag VARCHAR(64) NULL,
          payload_json LONGTEXT NULL,
          row_hash VARCHAR(64) NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_clean_cargo_row_hash (row_hash),
          KEY idx_clean_cargo_shop (shop_code),
          KEY idx_clean_cargo_batch (batch_tag)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _normalize_clean_cargo_row(item):
    row = item if isinstance(item, dict) else {}
    shop_code = str(
        row.get("shop_code")
        or row.get("shopCode")
        or row.get("SHOPCODE")
        or ""
    ).strip()
    if not shop_code:
        raise ValueError("clean_cargo_shop_code_required")
    payload_json = json.dumps(row, ensure_ascii=False)
    source_tag = str(row.get("source_tag") or row.get("sourceTag") or "manual").strip() or "manual"
    batch_tag = str(row.get("batch_tag") or row.get("batchTag") or "").strip() or None
    normalized = {
        "shop_code": shop_code,
        "rpcs": _safe_float(row.get("rpcs", row.get("RPCS")), 0.0),
        "rcase": _safe_float(row.get("rcase", row.get("RCASE")), 0.0),
        "bpcs": _safe_float(row.get("bpcs", row.get("BPCS")), 0.0),
        "bpaper": _safe_float(row.get("bpaper", row.get("BPAPER")), 0.0),
        "apcs": _safe_float(row.get("apcs", row.get("APCS")), 0.0),
        "apaper": _safe_float(row.get("apaper", row.get("APAPER")), 0.0),
        "rpaper": _safe_float(row.get("rpaper", row.get("RPAPER")), 0.0),
        "source_tag": source_tag,
        "batch_tag": batch_tag,
        "payload_json": payload_json,
    }
    hash_base = "|".join(
        [
            normalized["shop_code"],
            str(normalized["rpcs"]),
            str(normalized["rcase"]),
            str(normalized["bpcs"]),
            str(normalized["bpaper"]),
            str(normalized["apcs"]),
            str(normalized["apaper"]),
            str(normalized["rpaper"]),
            str(normalized["source_tag"] or ""),
            str(normalized["batch_tag"] or ""),
        ]
    )
    normalized["row_hash"] = hashlib.sha256(hash_base.encode("utf-8")).hexdigest()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return normalized


def clean_cargo_raw_save(payload):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    rows = (payload or {}).get("rows") if isinstance(payload, dict) else payload
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(rows, list):
        raise ValueError("rows_required")
    normalized_rows = [_normalize_clean_cargo_row(item) for item in rows]
    inserted = 0
    skipped = 0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            _ensure_clean_cargo_table(cursor)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for item in normalized_rows:
                cursor.execute(
                    f"""
                    INSERT IGNORE INTO {WMS_CARGO_RAW_CLEAN_TABLE}
                    (shop_code, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper, source_tag, batch_tag, payload_json, row_hash)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        item["shop_code"],
                        item["rpcs"],
                        item["rcase"],
                        item["bpcs"],
                        item["bpaper"],
                        item["apcs"],
                        item["apaper"],
                        item["rpaper"],
                        item["source_tag"],
                        item["batch_tag"],
                        item["payload_json"],
                        item["row_hash"],
                    ),
                )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if int(cursor.rowcount or 0) > 0:
                    inserted += 1
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                else:
                    skipped += 1
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"ok": True, "insertedRows": inserted, "skippedRows": skipped, "count": len(normalized_rows)}


def clean_cargo_raw_list(shop_code="", limit=500):
    shop_code = str(shop_code or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        limit = int(limit)
    except Exception:
        limit = 500
    limit = max(1, min(limit, 5000))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            _ensure_clean_cargo_table(cursor)
            where_sql = " WHERE 1=1 "
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            params = []
            if shop_code:
                where_sql += " AND shop_code = %s "
                params.append(shop_code)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM {WMS_CARGO_RAW_CLEAN_TABLE} {where_sql}",
                params,
            )
            total = int((cursor.fetchone() or {}).get("cnt") or 0)
            cursor.execute(
                f"""
                SELECT
                  shop_code, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper,
                  source_tag, batch_tag, created_at, updated_at
                FROM {WMS_CARGO_RAW_CLEAN_TABLE}
                {where_sql}
                ORDER BY id DESC
                LIMIT %s
                """,
                [*params, limit],
            )
            items = cursor.fetchall() or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"ok": True, "items": items, "count": len(items), "total": total, "limit": limit}


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _extract_raw_cargo_meta(payload_json):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not payload_json:
        return {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        data = json.loads(payload_json)
    except Exception:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(data, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return {}
    wave_belongs = str(_pick_by_candidates(data, ["wave_belongs", "鎵€灞炴尝娆?, "waveBelongs"], "")).strip()
    first_wave_time = str(_pick_by_candidates(data, ["first_wave_time", "涓€閰嶆椂闂?, "wave1_time"], "")).strip()
    second_wave_time = str(_pick_by_candidates(data, ["second_wave_time", "浜岄厤鏃堕棿", "wave2_time"], "")).strip()
    arrival_time = str(_pick_by_candidates(data, ["arrival_time", "鍒板簵鏃堕棿", "wave4_time"], "")).strip()
    return {
        "wave_belongs": wave_belongs,
        "first_wave_time": first_wave_time,
        "second_wave_time": second_wave_time,
        "arrival_time": arrival_time,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _compute_resolved_loads_from_raw_row(row):
    shop_code = str((row or {}).get("shop_code") or "").strip()
    if not shop_code:
        raise ValueError("raw_shop_code_required")
    raw_meta = _extract_raw_cargo_meta((row or {}).get("payload_json") or "")
    wave_belongs = str(raw_meta.get("wave_belongs") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not wave_belongs:
        raise ValueError(f"raw_wave_belongs_required:{shop_code}")
    norm_belongs = re.sub(r"[锛屻€乗s]+", ",", wave_belongs).replace(",,", ",").strip(",")
    rpcs = _safe_float((row or {}).get("rpcs"), 0.0)
    rcase = _safe_float((row or {}).get("rcase"), 0.0)
    bpcs = _safe_float((row or {}).get("bpcs"), 0.0)
    bpaper = _safe_float((row or {}).get("bpaper"), 0.0)
    apcs = _safe_float((row or {}).get("apcs"), 0.0)
    apaper = _safe_float((row or {}).get("apaper"), 0.0)
    rpaper = _safe_float((row or {}).get("rpaper"), 0.0)

    base_w1 = (rpcs / 207.0) + (rcase / 380.0) + (bpcs / 120.0) + (bpaper / 380.0) + (rpaper / 380.0)
    base_w2 = (apcs / 350.0) + (apaper / 380.0)
    total_full = base_w1 + base_w2

    has_1 = bool(re.search(r"(^|,)\s*1\s*(,|$)", norm_belongs))
    has_2 = bool(re.search(r"(^|,)\s*2\s*(,|$)", norm_belongs))
    has_3 = bool(re.search(r"(^|,)\s*3\s*(,|$)", norm_belongs))
    has_4 = bool(re.search(r"(^|,)\s*4\s*(,|$)", norm_belongs))

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if norm_belongs == "2":
        wave1_load = 0.0
        wave2_load = total_full
        wave3_load = 0.0
        wave4_load = 0.0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    elif has_3 or has_4:
        wave1_load = 0.0
        wave2_load = 0.0
        wave3_load = total_full if has_3 else 0.0
        wave4_load = total_full if has_4 else 0.0
    elif has_1 or has_2:
        wave1_load = base_w1
        wave2_load = base_w2
        wave3_load = 0.0
        wave4_load = 0.0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    else:
        raise ValueError(f"unsupported_wave_belongs:{shop_code}:{wave_belongs}")

    return {
        "shop_code": shop_code,
        "wave_belongs": wave_belongs,
        "wave1_load": round(wave1_load, 6),
        "wave2_load": round(wave2_load, 6),
        "wave3_load": round(wave3_load, 6),
        "wave4_load": round(wave4_load, 6),
        "total_resolved_load": round(wave1_load + wave2_load + wave3_load + wave4_load, 6),
        "first_wave_time": raw_meta.get("first_wave_time") or None,
        "second_wave_time": raw_meta.get("second_wave_time") or None,
        "arrival_time_w3": raw_meta.get("arrival_time") or None,
        "arrival_time_w4": raw_meta.get("arrival_time") or None,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def build_resolved_load_rows(raw_rows):
    # Pure compute path: input raw rows, output resolved rows. No DB side effects.
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    rows = raw_rows if isinstance(raw_rows, list) else []
    latest_by_shop = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in rows:
        shop_code = str((row or {}).get("shop_code") or "").strip()
        if not shop_code or shop_code in latest_by_shop:
            continue
        latest_by_shop[shop_code] = row
    resolved_rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in latest_by_shop.values():
        resolved_rows.append(_compute_resolved_loads_from_raw_row(row))
    return resolved_rows


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def wms_cargo_raw_resolve_only(payload=None):
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    rows = (payload or {}).get("rows") if isinstance(payload, dict) else None
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(rows, list):
        raise ValueError("rows_required")
    resolved_rows = build_resolved_load_rows(rows)
    return {"ok": True, "resolvedRows": len(resolved_rows), "items": resolved_rows}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def wms_cargo_raw_rebuild_resolved(payload=None):
    ensure_archive_tables()
    source_table = str((payload or {}).get("sourceTable") or (payload or {}).get("source_table") or "CargoQTY").strip() or "CargoQTY"
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT shop_code, business_date, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper, source_table, row_hash, payload_json, created_at
                FROM {WMS_CARGO_RAW_TABLE}
                WHERE source_table = %s
                ORDER BY shop_code, business_date DESC, created_at DESC, id DESC
                """,
                (source_table,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall() or []
    if not rows:
        raise ValueError("raw_cargo_rows_required")

    resolved_rows = build_resolved_load_rows(rows)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS store_wave_load_resolved (
                  shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
                  wave_belongs VARCHAR(32) NULL,
                  wave1_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave2_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave3_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave4_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  total_resolved_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  first_wave_time VARCHAR(16) NULL,
                  second_wave_time VARCHAR(16) NULL,
                  arrival_time_w3 VARCHAR(16) NULL,
                  arrival_time_w4 VARCHAR(16) NULL,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  KEY idx_swlr_wave_belongs (wave_belongs)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute("TRUNCATE TABLE store_wave_load_resolved")
            cursor.executemany(
                """
                INSERT INTO store_wave_load_resolved (
                  shop_code, wave_belongs,
                  wave1_load, wave2_load, wave3_load, wave4_load,
                  total_resolved_load, first_wave_time, second_wave_time, arrival_time
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        item["shop_code"],
                        item["wave_belongs"],
                        item["wave1_load"],
                        item["wave2_load"],
                        item["wave3_load"],
                        item["wave4_load"],
                        item["total_resolved_load"],
                        item["first_wave_time"],
                        item["second_wave_time"],
                        item["arrival_time_w3"],
                        item["arrival_time_w4"],
                    )
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    for item in resolved_rows
                ],
            )
    return {"ok": True, "sourceTable": source_table, "resolvedRows": len(resolved_rows)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _find_local_file_under_x(prefix_name):
    root = r"C:\x"
    if not os.path.isdir(root):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return ""
    candidates = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for name in os.listdir(root):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if str(name).startswith("~$"):
            continue
        path = os.path.join(root, name)
        if not os.path.isfile(path):
            continue
        base = str(name)
        stem = os.path.splitext(base)[0]
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if stem == prefix_name or base.startswith(prefix_name):
            candidates.append(path)
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0] if candidates else ""


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _resolve_local_source_file(preferred_path, fallback_prefix):
    path = str(preferred_path or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if path and os.path.isfile(path):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return path
    return _find_local_file_under_x(fallback_prefix)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _read_local_table(file_path):
    if pd is None:
        raise RuntimeError("pandas_not_installed")
    ext = os.path.splitext(file_path)[1].lower()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if ext in {".xlsx", ".xls", ".xlsm"}:
        df = pd.read_excel(file_path, sheet_name=0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    elif ext in {".csv", ".txt"}:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except Exception:
            df = pd.read_csv(file_path, encoding="gb18030")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    else:
        raise RuntimeError(f"unsupported_file_ext:{ext}")
    if df is None or df.empty:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return []
    df = df.fillna("")
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for _, row in df.iterrows():
        obj = {}
        for col in df.columns:
            key = str(col)
            obj[key] = _to_jsonable(row.get(col))
        rows.append(obj)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return rows


def _row_value(row, candidates, default=""):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return _pick_by_candidates(row or {}, candidates, default)


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _resolve_numeric(value, default=0.0):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return _to_float(value, default)


def _resolve_shopcode(row):
    code = _row_value(
        row,
        ["SHOPCODE", "shop_code", "搴楅摵浠ｇ爜", "闂ㄥ簵浠ｇ爜", "搴楀彿", "shopid", "store_code", "store_id"],
        "",
    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return str(code or "").strip()


LOAD_FIELD_KEYS = ("RPCS", "RCASE", "BPCS", "BPAPER", "APCS", "APAPER", "RPAPER")


def _normalize_text_key(value):
    text = str(value or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not text:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return ""
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return re.sub(r"[\s_\-()锛堬級:锛歖+", "", text).lower()


def _resolve_vehicle_type_key(row):
    raw = _row_value(
        row,
        [
            "vehicle_type",
            "VEHICLE_TYPE",
            "vehicleType",
            "杞﹀瀷",
            "杞﹁締绫诲瀷",
            "杞︾",
            "杞︾粍绫诲瀷",
            "瑁呰浇杞﹀瀷",
            "杞﹁締绫诲埆",
        ],
        "",
    )
    key = _normalize_text_key(raw)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if key:
        return key
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return _normalize_text_key(DEFAULT_VEHICLE_TYPE)


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _resolve_load_fields(row):
    values = {}
    issues = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for key in LOAD_FIELD_KEYS:
        raw = _row_value(
            row,
            [
                key,
                f"{key}_MAX",
                f"{key}鏈€澶ц杞?,
                f"{key}鏈€澶у€?,
                f"{key}瀹归噺",
                f"{key}涓婇檺",
                f"{key}鑳藉姏",
            ],
            None,
        )
        if raw in (None, ""):
            values[key] = 0.0
            issues.append(f"{key}:missing")
            continue
        max_capacity = _resolve_numeric(raw, 0.0)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if max_capacity <= 0:
            values[key] = 0.0
            issues.append(f"{key}:non_positive")
            continue
        values[key] = 1.0 / max_capacity
    return values, issues


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _resolve_loadtype_field_key(raw_load_type):
    key = _normalize_text_key(raw_load_type)
    mapping = {
        "甯告俯鏁寸悊绠?: "RPCS",
        "甯告俯鏁寸悊": "RPCS",
        "rpcs": "RPCS",
        "甯告俯鏁寸姘?: "RCASE",
        "鏁寸姘?: "RCASE",
        "甯告俯鏁寸": "RCASE",
        "rcase": "RCASE",
        "鍐峰喕鍖?: "BPCS",
        "bpcs": "BPCS",
        "鍐峰喕绾哥": "BPAPER",
        "bpaper": "BPAPER",
        "鍐疯棌绛?: "APCS",
        "apcs": "APCS",
        "鍐疯棌绾哥": "APAPER",
        "apaper": "APAPER",
        "甯告俯绾哥": "RPAPER",
        "rpaper": "RPAPER",
    }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if key in mapping:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return mapping[key]
    # 鍏煎鈥渞pcs:甯告俯鏁寸悊绠扁€濊繖绫绘贩鍚堝啓娉?
    if "rpcs" in key or "甯告俯鏁寸悊绠? in key or "甯告俯鏁寸悊" in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "RPCS"
    if "rcase" in key or "鏁寸姘? in key or "甯告俯鏁寸姘? in key or "甯告俯鏁寸" in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "RCASE"
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "bpcs" in key or "鍐峰喕鍖? in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "BPCS"
    if "bpaper" in key or "鍐峰喕绾哥" in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "BPAPER"
    if "apcs" in key or "鍐疯棌绛? in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "APCS"
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "apaper" in key or "鍐疯棌绾哥" in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "APAPER"
    if "rpaper" in key or "甯告俯绾哥" in key:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "RPAPER"
    return ""


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _build_default_factors_from_loadtype_rows(rows):
    values = {k: 0.0 for k in LOAD_FIELD_KEYS}
    logs = []
    hit = 0
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for idx, row in enumerate(rows or [], start=1):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not isinstance(row, dict):
            continue
        load_type = _row_value(row, ["LOADTYPE", "loadtype", "瑁呰浇绫诲瀷", "绫诲瀷", "璐х墿绫诲瀷"], "")
        load_value = _row_value(row, ["LOADVALUE", "loadvalue", "瑁呰浇鑳藉姏", "鑳藉姏鍊?, "鏈€澶ц杞?], None)
        field_key = _resolve_loadtype_field_key(load_type)
        if not field_key:
            continue
        hit += 1
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if load_value in (None, ""):
            values[field_key] = 0.0
            logs.append(f"瑁呰浇鑳藉姏琛ㄧ{idx}琛孾{field_key}]缂哄皯LOADVALUE锛屾寜0澶勭悊")
            continue
        max_capacity = _resolve_numeric(load_value, 0.0)
        if max_capacity <= 0:
            values[field_key] = 0.0
            logs.append(f"瑁呰浇鑳藉姏琛ㄧ{idx}琛孾{field_key}]鑳藉姏<=0锛屾寜0澶勭悊")
            continue
        values[field_key] = 1.0 / max_capacity
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return (values if hit > 0 else None), logs


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _hardcoded_default_factors():
    # 鐢ㄦ埛纭鐨勫敮涓€榛樿杞﹀瀷瑁呰浇鑳藉姏锛?
    # rpcs=207, rcase=380, bpcs=120, bpaper=380, apcs=350, apaper=380, rpaper=380
    caps = {
        "RPCS": 207.0,
        "RCASE": 380.0,
        "BPCS": 120.0,
        "BPAPER": 380.0,
        "APCS": 350.0,
        "APAPER": 380.0,
        "RPAPER": 380.0,
    }
    factors = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for key in LOAD_FIELD_KEYS:
        cap = float(caps.get(key, 0.0) or 0.0)
        factors[key] = (1.0 / cap) if cap > 0 else 0.0
    return factors


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _build_factor_map_from_carload_rows(rows):
    factor_map = {}
    logs = []
    default_type_key = _normalize_text_key(DEFAULT_VEHICLE_TYPE)
    vertical_default, vertical_logs = _build_default_factors_from_loadtype_rows(rows)
    if vertical_default is not None:
        all_zero = all(float(vertical_default.get(k, 0.0) or 0.0) <= 0 for k in LOAD_FIELD_KEYS)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if all_zero:
            raise ValueError("瑁呰浇鑳藉姏琛ㄨ瘑鍒埌LOADTYPE浣?绫荤郴鏁板叏涓?锛岀姝㈠洖閫€榛樿鑳藉姏")
        factor_map[default_type_key] = vertical_default
        logs.append("瑁呰浇鑳藉姏琛ㄨ瘑鍒负 LOADTYPE/LOADVALUE 缁撴瀯锛屽凡鎸夎溅鍨媖ey鍖归厤鍔犺浇")
        logs.extend(vertical_logs)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for missing_key in LOAD_FIELD_KEYS:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if vertical_default.get(missing_key, 0.0) == 0.0:
                logs.append(f"榛樿杞﹀瀷鑳藉姏缂哄皯{missing_key}锛屾寜0澶勭悊")
        return factor_map, logs
    raise ValueError("瑁呰浇鑳藉姏琛ㄦ湭璇嗗埆鍒版湁鏁圠OADTYPE鏄犲皠锛岀姝㈠洖閫€榛樿鑳藉姏")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _print_vehicle_type_carload_mapping(cargo_vehicle_keys, carload_keys, prefix="[LOCAL_LOAD_MATCH]"):
    cargo_keys = sorted({str(k or "").strip() for k in (cargo_vehicle_keys or []) if str(k or "").strip()})
    factor_keys = sorted({str(k or "").strip() for k in (carload_keys or []) if str(k or "").strip()})
    print(f"{prefix} cargo_vehicle_type_keys={cargo_keys}")
    print(f"{prefix} carload_vehicle_keys={factor_keys}")
    for key in cargo_keys:
        print(f"{prefix} vehicle_type={key} -> {'MATCH' if key in factor_keys else 'MISS'}")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _log_solve_block(reason):
    msg = str(reason or "").strip() or "鏈煡鍘熷洜"
    print(f"[SOLVE_BLOCK] {msg}")


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _log_backend_state_summary(tag, result):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def emit(line):
        print(line)
        _append_sfrz_log(line)

    data = result if isinstance(result, dict) else {}
    best_state = data.get("bestState")
    if best_state is None:
        best_state = data.get("best_state")
    trace_log = data.get("traceLog")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if trace_log is None:
        trace_log = data.get("trace")

    exists = best_state is not None
    state_type = type(best_state).__name__ if exists else "NoneType"
    state_len = len(best_state) if isinstance(best_state, list) else 0
    emit(f"[{tag}] bestState_exists={exists}")
    emit(f"[{tag}] bestState_type={state_type}")
    emit(f"[{tag}] bestState_len={state_len}")
    if not exists or state_len == 0:
        emit(f"[{tag}] bestState is empty")

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(best_state, list):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for vehicle in best_state:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(vehicle, dict):
                continue
            plate = vehicle.get("plateNo")
            if plate is None:
                plate = vehicle.get("plate_no")
            routes = vehicle.get("routes")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(routes, list):
                routes = []
            route_sizes = []
            for route in routes:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if isinstance(route, dict):
                    stops = route.get("stops")
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if isinstance(stops, list):
                        route_sizes.append(len(stops))
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    elif isinstance(route.get("route"), list):
                        route_sizes.append(len(route.get("route") or []))
                    else:
                        route_sizes.append(0)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                elif isinstance(route, list):
                    route_sizes.append(len(route))
                else:
                    route_sizes.append(0)
            emit(f"[{tag}] vehicle={plate} routes={len(routes)} route_sizes={route_sizes}")

    trace_len = len(trace_log) if isinstance(trace_log, list) else 0
    emit(f"[{tag}] traceLog_len={trace_len}")


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _reason_text_from_code(reason):
    mapping = {
        "capacity": "杞﹁締瀹归噺涓嶈冻",
        "arrival": "鍒板簵鏃堕棿涓嶆弧瓒?,
        "wave": "娉㈡鎴鏃堕棿涓嶈冻",
        "mileage": "绾胯矾閲岀▼瓒呴檺",
        "slot": "褰撳墠娉㈡鏃犲彲琛屾彃鍏?,
        "mixed": "缁煎悎绾︽潫鍐茬獊",
    }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return mapping.get(str(reason or "").strip(), mapping["mixed"])


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _violation_to_reason(vtype):
    t = str(vtype or "").strip().lower()
    if t == "capacity":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "capacity"
    if t in {"arrival_window"}:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "arrival"
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if t in {"wave_end"}:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "wave"
    if t in {"max_route_km", "max_route_km_single", "max_route_km_return", "night_regular_distance"}:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "mileage"
    return "slot"


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _extract_assigned_store_ids(best_state):
    assigned = set()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(best_state, list):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return assigned
    for item in best_state:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(item, dict):
            continue
        routes = item.get("routes") if isinstance(item.get("routes"), list) else []
        for route in routes:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if isinstance(route, list):
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                for sid in route:
                    s = _debug_norm_store_id(sid)
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if s:
                        assigned.add(s)
    return assigned


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _build_backend_unscheduled_stores(payload, result):
    stores_list = payload.get("stores") if isinstance(payload.get("stores"), list) else []
    stores = {
        str(item.get("id")): _normalize_numeric_types_for_solver(item)
        for item in stores_list
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    best_state = result.get("bestState") if isinstance(result, dict) else None
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if best_state is None and isinstance(result, dict):
        best_state = result.get("best_state")
    assigned = _extract_assigned_store_ids(best_state)
    missing_ids = [sid for sid in stores.keys() if _debug_norm_store_id(sid) not in assigned]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not missing_ids:
        return []

    wave = _normalize_numeric_types_for_solver(payload.get("wave")) if isinstance(payload.get("wave"), dict) else {}
    scenario = _normalize_numeric_types_for_solver(payload.get("scenario")) if isinstance(payload.get("scenario"), dict) else {}
    dist = _normalize_numeric_types_for_solver(payload.get("dist")) if isinstance(payload.get("dist"), dict) else {}
    vehicles_state = best_state if isinstance(best_state, list) else []

    valid_store_ids = set(stores.keys())

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def _sanitize_candidate_routes(routes):
        cleaned_routes = []
        seen = set()
        for route in routes if isinstance(routes, list) else []:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not isinstance(route, list):
                continue
            cleaned = []
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for raw_sid in route:
                sid2 = _debug_norm_store_id(raw_sid)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not sid2 or sid2 not in valid_store_ids or sid2 in seen:
                    continue
                seen.add(sid2)
                cleaned.append(sid2)
            if cleaned:
                cleaned_routes.append(cleaned)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return cleaned_routes

    unscheduled = []
    for sid in missing_ids:
        store = stores.get(sid) or {}
        reason_counts = {}
        feasible_found = False
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for item in vehicles_state:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not isinstance(item, dict):
                continue
            base_routes = _sanitize_candidate_routes(item.get("routes") if isinstance(item.get("routes"), list) else [])
            candidate_variants = []
            candidate_variants.append(base_routes + [[sid]])
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for ridx, route in enumerate(base_routes):
                if not isinstance(route, list):
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                for pos in range(len(route) + 1):
                    nr = [list(r) if isinstance(r, list) else [] for r in base_routes]
                    nr[ridx].insert(pos, sid)
                    candidate_variants.append(nr)
            checked_any = False
            for routes in candidate_variants:
                checked_any = True
                probe = dict(item)
                probe["routes"] = routes
                checked = check_plan_constraints(probe, stores, dist, wave, scenario)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if checked.get("feasible"):
                    feasible_found = True
                    break
                violations = checked.get("violations") or []
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if violations:
                    vtype = (violations[0] or {}).get("type")
                    reason = _violation_to_reason(vtype)
                    reason_counts[reason] = int(reason_counts.get(reason) or 0) + 1
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                else:
                    reason_counts["slot"] = int(reason_counts.get("slot") or 0) + 1
            if not checked_any:
                reason_counts["slot"] = int(reason_counts.get("slot") or 0) + 1
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if feasible_found:
                break
        if feasible_found:
            final_reason = "slot"
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif reason_counts:
            final_reason = sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        else:
            final_reason = "mixed"
        unscheduled.append(
            {
                "waveId": wave.get("waveId"),
                "storeId": sid,
                "storeName": store.get("name", ""),
                "reason": final_reason,
                "reasonText": _reason_text_from_code(final_reason),
                "source": "backend_intermediate",
            }
        )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return unscheduled


def _debug_norm_store_id(value):
    text = str(value or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return text


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _debug_extract_store_id(item):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if isinstance(item, dict):
        for key in ("storeId", "store_id", "id", "code", "shop_code", "shopCode", "storeCode"):
            value = item.get(key)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if str(value or "").strip():
                return _debug_norm_store_id(value)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return ""
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return _debug_norm_store_id(item)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _debug_extract_capacity_unscheduled_store_ids(result, wave_id="", limit=5):
    ids = []
    rows = result.get("unscheduledStores") if isinstance(result, dict) else []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or row.get("reasonText") or "").strip().lower()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if "capacity" not in reason and "瀹归噺" not in reason:
            continue
        row_wave = str(row.get("waveId") or row.get("wave_id") or "").strip().upper()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if wave_id and row_wave and row_wave != wave_id:
            continue
        sid = _debug_extract_store_id(row)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if sid and sid not in ids:
            ids.append(sid)
        if len(ids) >= int(limit):
            break
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return ids


def _enforce_and_validate_vehicle_type_for_solve(payload):
    vehicles = payload.get("vehicles") if isinstance(payload, dict) and isinstance(payload.get("vehicles"), list) else []
    missing_vehicle_type = []
    payload_vehicle_type_keys = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for idx, v in enumerate(vehicles, start=1):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(v, dict):
            continue
        raw_type = str(v.get("type") or v.get("vehicle_type") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not raw_type:
            plate = str(v.get("plateNo") or "").strip()
            missing_vehicle_type.append(plate or f"row#{idx}")
            continue
        key = _normalize_text_key(raw_type)
        if key:
            payload_vehicle_type_keys.add(key)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if missing_vehicle_type:
        details = ",".join(missing_vehicle_type)
        _log_solve_block(f"妫€娴嬪埌杞﹁締vehicle_type涓虹┖锛岀姝㈡眰瑙? {details}")
        raise ValueError(f"妫€娴嬪埌杞﹁締vehicle_type涓虹┖锛岀姝㈡眰瑙? {details}")
    print(f"[SOLVE_VEHICLE_CHECK] vehicle_type_keys={sorted(payload_vehicle_type_keys)}")


def store_wave_load_resolved_list(shop_code="", wave_belongs="", limit=200):
    ensure_archive_tables()
    shop_code = str(shop_code or "").strip()
    wave_belongs = str(wave_belongs or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    try:
        limit = int(limit)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    except Exception:
        limit = 200
    limit = max(1, min(limit, 2000))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS store_wave_load_resolved (
                  shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
                  wave_belongs VARCHAR(32) NULL,
                  wave1_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave2_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave3_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave4_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  total_resolved_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  first_wave_time VARCHAR(16) NULL,
                  second_wave_time VARCHAR(16) NULL,
                  arrival_time_w3 VARCHAR(16) NULL,
                  arrival_time_w4 VARCHAR(16) NULL,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  KEY idx_swlr_wave_belongs (wave_belongs)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute("SHOW COLUMNS FROM store_wave_load_resolved")
            cols = {str((r or {}).get("Field") or "") for r in (cursor.fetchall() or [])}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "first_wave_time" not in cols:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN first_wave_time VARCHAR(16) NULL AFTER total_resolved_load")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "second_wave_time" not in cols:
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN second_wave_time VARCHAR(16) NULL AFTER first_wave_time")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if "arrival_time_w3" not in cols:
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN arrival_time_w3 VARCHAR(16) NULL AFTER second_wave_time")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "arrival_time_w4" not in cols:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN arrival_time_w4 VARCHAR(16) NULL AFTER arrival_time_w3")
            where_sql = " WHERE 1=1 "
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            params = []
            if shop_code:
                where_sql += " AND shop_code = %s "
                params.append(shop_code)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if wave_belongs:
                where_sql += " AND wave_belongs = %s "
                params.append(wave_belongs)
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM store_wave_load_resolved {where_sql}",
                params,
            )
            total = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT
                  shop_code, wave_belongs,
                  wave1_load, wave2_load, wave3_load, wave4_load,
                  total_resolved_load, first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4, updated_at
                FROM store_wave_load_resolved
                {where_sql}
                ORDER BY shop_code
                LIMIT %s
                """,
                [*params, limit],
            )
            items = cursor.fetchall() or []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"items": items, "count": len(items), "total": total, "limit": limit}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def store_wave_load_resolved_get_one(shop_code=""):
    code = str(shop_code or "").strip()
    if not code:
        raise ValueError("shop_code_required")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT
                  shop_code, wave_belongs,
                  wave1_load, wave2_load, wave3_load, wave4_load,
                  first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4, updated_at
                FROM store_wave_load_resolved
                WHERE shop_code = %s
                LIMIT 1
                """,
                (code,),
            )
            item = cursor.fetchone()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"item": item}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def store_wave_load_resolved_get_batch(shop_codes):
    if not isinstance(shop_codes, list):
        raise ValueError("shop_codes_must_be_list")
    codes = [str(x or "").strip() for x in shop_codes if str(x or "").strip()]
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not codes:
        return {"items": [], "count": 0}
    placeholders = ",".join(["%s"] * len(codes))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT
                  shop_code, wave_belongs,
                  wave1_load, wave2_load, wave3_load, wave4_load,
                  first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4, updated_at
                FROM store_wave_load_resolved
                WHERE shop_code IN ({placeholders})
                ORDER BY shop_code
                """,
                codes,
            )
            items = cursor.fetchall() or []
    return {"items": items, "count": len(items)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_parse_int(value, default=0, minimum=None, maximum=None):
    try:
        parsed = int(str(value or "").strip())
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        parsed = int(default)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if minimum is not None:
        parsed = max(int(minimum), parsed)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if maximum is not None:
        parsed = min(int(maximum), parsed)
    return parsed


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_query_filters(query):
    q = str((query.get("q") or [""])[0] or "").strip()
    wave = str((query.get("wave") or [""])[0] or "").strip()
    solver_ready = str((query.get("solverReady") or query.get("solver_ready") or ["all"])[0] or "all").strip().lower()
    date_from = str((query.get("dateFrom") or query.get("date_from") or [""])[0] or "").strip()
    date_to = str((query.get("dateTo") or query.get("date_to") or [""])[0] or "").strip()

    where_sql = []
    params = []

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if q:
        where_sql.append(
            "("
            "shop_code LIKE %s OR "
            "store_name LIKE %s OR "
            "source_vehicle_ids LIKE %s OR "
            "source_route_ids LIKE %s OR "
            "source_route_names LIKE %s"
            ")"
        )
        like = f"%{q}%"
        params.extend([like, like, like, like, like])
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wave:
        where_sql.append("wave_belongs = %s")
        params.append(wave)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if solver_ready in {"0", "1"}:
        where_sql.append("solver_ready_flag = %s")
        params.append(int(solver_ready))
    if date_from:
        where_sql.append("delivery_date >= %s")
        params.append(date_from)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if date_to:
        where_sql.append("delivery_date <= %s")
        params.append(date_to)

    where_clause = (" WHERE " + " AND ".join(where_sql)) if where_sql else ""
    return where_clause, params


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def rengong_summary(query):
    where_clause, params = _rengong_query_filters(query)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  COUNT(*) AS total_rows,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows,
                  SUM(CASE WHEN solver_ready_flag = 0 THEN 1 ELSE 0 END) AS not_ready_rows,
                  COUNT(DISTINCT delivery_date) AS date_count,
                  MIN(delivery_date) AS date_min,
                  MAX(delivery_date) AS date_max,
                  ROUND(SUM(original_load), 6) AS original_load_sum,
                  ROUND(SUM(total_resolved_load), 6) AS resolved_load_sum
                FROM human_dispatch_solver_ready
                {where_clause}
                """,
                params,
            )
            overview = cursor.fetchone() or {}

            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT
                  COALESCE(NULLIF(wave_belongs, ''), '鏈槧灏?) AS wave_belongs,
                  COUNT(*) AS row_count,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows,
                  ROUND(SUM(original_load), 6) AS original_load_sum,
                  ROUND(SUM(total_resolved_load), 6) AS resolved_load_sum
                FROM human_dispatch_solver_ready
                {where_clause}
                GROUP BY COALESCE(NULLIF(wave_belongs, ''), '鏈槧灏?)
                ORDER BY row_count DESC, wave_belongs ASC
                """,
                params,
            )
            wave_breakdown = cursor.fetchall() or []

            cursor.execute(
                f"""
                SELECT
                  delivery_date,
                  COUNT(*) AS row_count,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows
                FROM human_dispatch_solver_ready
                {where_clause}
                GROUP BY delivery_date
                ORDER BY delivery_date DESC
                LIMIT 31
                """,
                params,
            )
            recent_dates = cursor.fetchall() or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"overview": overview, "waveBreakdown": wave_breakdown, "recentDates": recent_dates}


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def rengong_list(query):
    where_clause, params = _rengong_query_filters(query)
    page = _rengong_parse_int((query.get("page") or ["1"])[0], default=1, minimum=1)
    page_size = _rengong_parse_int((query.get("pageSize") or query.get("page_size") or ["50"])[0], default=50, minimum=1, maximum=200)
    offset = (page - 1) * page_size

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"SELECT COUNT(*) AS total FROM human_dispatch_solver_ready {where_clause}", params)
            total = int((cursor.fetchone() or {}).get("total") or 0)
            cursor.execute(
                f"""
                SELECT
                  id,
                  delivery_date,
                  shop_code,
                  store_name,
                  source_record_count,
                  source_vehicle_ids,
                  source_route_ids,
                  source_route_names,
                  original_ambient_turnover_boxes,
                  original_ambient_cartons,
                  original_ambient_full_cases,
                  original_frozen_bags,
                  original_frozen_cartons,
                  original_cold_bins,
                  original_cold_cartons,
                  original_load,
                  rpcs,
                  rcase,
                  bpcs,
                  bpaper,
                  apcs,
                  apaper,
                  rpaper,
                  wave_belongs,
                  first_wave_time,
                  second_wave_time,
                  arrival_time_w3,
                  arrival_time_w4,
                  wave1_load,
                  wave2_load,
                  wave3_load,
                  wave4_load,
                  total_resolved_load,
                  boxes,
                  timing_source,
                  solver_ready_flag,
                  missing_fields,
                  conversion_rule_version,
                  updated_at
                FROM human_dispatch_solver_ready
                {where_clause}
                ORDER BY delivery_date DESC, shop_code ASC
                LIMIT %s OFFSET %s
                """,
                [*params, page_size, offset],
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall() or []

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "items": rows,
        "total": total,
        "page": page,
        "pageSize": page_size,
        "hasMore": (offset + len(rows)) < total,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_expected_time_for_row(row):
    wave_belongs = str((row or {}).get("wave_belongs") or "").strip()
    if "," in wave_belongs:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return ""
    if wave_belongs == "1":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str((row or {}).get("first_wave_time") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wave_belongs == "2":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str((row or {}).get("second_wave_time") or "").strip()
    if wave_belongs == "3":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return str((row or {}).get("arrival_time_w3") or "").strip()
    if wave_belongs == "4":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str((row or {}).get("arrival_time_w4") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return ""


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_active_wave_load_for_row(row):
    wave_belongs = str((row or {}).get("wave_belongs") or "").strip()
    if wave_belongs == "1":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return _safe_float((row or {}).get("wave1_load"), 0.0)
    if wave_belongs == "2":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return _safe_float((row or {}).get("wave2_load"), 0.0)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wave_belongs == "3":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return _safe_float((row or {}).get("wave3_load"), 0.0)
    if wave_belongs == "4":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return _safe_float((row or {}).get("wave4_load"), 0.0)
    return _safe_float((row or {}).get("total_resolved_load"), 0.0)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_extract_store_groups(delivery_date):
    """
    浠?human_dispatch_routes 鎶藉彇鈥滃弻閰嶉棬搴楃粍鈥濓細
    鍚屼竴澶?+ 鍚岃溅锛宺oute_id = X 涓?X+100 瑙嗕负鍚屼竴闂ㄥ簵缁勩€?
    """
    route_store_map = {}
    by_vehicle_routes = {}

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                  delivery_date,
                  vehicle_id,
                  route_id,
                  store_id
                FROM human_dispatch_routes
                WHERE delivery_date = %s
                ORDER BY vehicle_id ASC, route_id ASC, store_id ASC
                """,
                (delivery_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []

    for row in rows:
        vehicle_id = str(row.get("vehicle_id") or "").strip()
        route_id = int(_safe_float(row.get("route_id"), 0))
        store_id = str(row.get("store_id") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not vehicle_id or route_id <= 0 or not store_id:
            continue
        key = (vehicle_id, route_id)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if key not in route_store_map:
            route_store_map[key] = set()
        route_store_map[key].add(store_id)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if vehicle_id not in by_vehicle_routes:
            by_vehicle_routes[vehicle_id] = set()
        by_vehicle_routes[vehicle_id].add(route_id)

    groups = []
    for vehicle_id, route_ids in by_vehicle_routes.items():
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for base_route_id in sorted(route_ids):
            pair_route_id = base_route_id + 100
            if pair_route_id not in route_ids:
                continue
            stores_a = route_store_map.get((vehicle_id, base_route_id), set())
            stores_b = route_store_map.get((vehicle_id, pair_route_id), set())
            union_stores = stores_a.union(stores_b)
            overlap_stores = stores_a.intersection(stores_b)
            store_ids = sorted(
                union_stores,
                key=lambda x: (int(x) if str(x).isdigit() else str(x)),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not store_ids:
                continue
            first_store_count = len(stores_a)
            second_store_count = len(stores_b)
            overlap_count = len(overlap_stores)
            union_store_count = len(union_stores)
            core_store_count = overlap_count
            first_store_ids = sorted(
                stores_a,
                key=lambda x: (int(x) if str(x).isdigit() else str(x)),
            )
            second_store_ids = sorted(
                stores_b,
                key=lambda x: (int(x) if str(x).isdigit() else str(x)),
            )
            extra_stores = stores_b.difference(stores_a)
            extra_store_count = len(extra_stores)
            extra_store_ids = sorted(
                extra_stores,
                key=lambda x: (int(x) if str(x).isdigit() else str(x)),
            )
            jaccard = round((overlap_count / union_store_count), 4) if union_store_count else 0.0
            second_extra_ratio = round((extra_store_count / first_store_count), 4) if first_store_count else 0.0
            second_vs_first_ratio = round((second_store_count / first_store_count), 4) if first_store_count else 0.0
            groups.append(
                {
                    "delivery_date": str(delivery_date),
                    "vehicle_id": vehicle_id,
                    "base_route_id": int(base_route_id),
                    "first_route_id": int(base_route_id),
                    "second_route_id": int(pair_route_id),
                    "pair_label": f"{int(base_route_id)} / {int(pair_route_id)}",
                    "first_store_count": first_store_count,
                    "second_store_count": second_store_count,
                    "overlap_count": overlap_count,
                    "union_store_count": union_store_count,
                    "core_store_count": core_store_count,
                    "first_store_ids": first_store_ids,
                    "second_store_ids": second_store_ids,
                    "extra_store_count": extra_store_count,
                    "extra_store_ids": extra_store_ids,
                    "second_extra_ratio": second_extra_ratio,
                    "second_vs_first_ratio": second_vs_first_ratio,
                    "jaccard": jaccard,
                    "store_ids": store_ids,
                    "store_count": union_store_count,
                }
            )

    groups.sort(key=lambda item: (item["delivery_date"], item["vehicle_id"], item["base_route_id"]))
    counts = [int(item.get("store_count") or 0) for item in groups]
    second_extra_ratios = [float(item.get("second_extra_ratio") or 0.0) for item in groups]
    total_groups = len(groups)
    stats = {
        "total_groups": total_groups,
        "avg_store_count": round((sum(counts) / total_groups), 2) if total_groups else 0.0,
        "max_store_count": max(counts) if counts else 0,
        "min_store_count": min(counts) if counts else 0,
        "avg_second_extra_ratio": round((sum(second_extra_ratios) / total_groups), 4) if total_groups else 0.0,
        "second_extra_over_20_count": sum(1 for item in groups if float(item.get("second_extra_ratio") or 0.0) > 0.2),
        "second_equals_first_count": sum(
            1 for item in groups if int(item.get("second_store_count") or 0) == int(item.get("first_store_count") or 0)
        ),
    }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return stats, groups


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_build_dispatch_input_simulation(groups):
    simulations = []
    for item in groups or []:
        vehicle_id = str(item.get("vehicle_id") or "").strip()
        first_route_id = int(_safe_float(item.get("first_route_id"), 0))
        second_route_id = int(_safe_float(item.get("second_route_id"), 0))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not vehicle_id or first_route_id <= 0 or second_route_id <= 0:
            continue

        core_stores = sorted(
            [str(x).strip() for x in (item.get("first_store_ids") or []) if str(x).strip()],
            key=lambda x: (int(x) if str(x).isdigit() else str(x)),
        )
        extra_stores = sorted(
            [str(x).strip() for x in (item.get("extra_store_ids") or []) if str(x).strip()],
            key=lambda x: (int(x) if str(x).isdigit() else str(x)),
        )
        wave1_stores = core_stores
        wave2_stores = sorted(
            set(wave1_stores).union(extra_stores),
            key=lambda x: (int(x) if str(x).isdigit() else str(x)),
        )
        total_store_count = len(set(wave1_stores).union(wave2_stores))
        pair_label = str(item.get("pair_label") or f"{first_route_id} / {second_route_id}")
        group_id = f"{item.get('delivery_date') or ''}|{vehicle_id}|{pair_label}"

        simulations.append(
            {
                "vehicle_id": vehicle_id,
                "group_id": group_id,
                "wave1_stores": wave1_stores,
                "wave2_stores": wave2_stores,
                "total_store_count": total_store_count,
                "pair_label": pair_label,
                "wave1_store_count": len(wave1_stores),
                "wave2_store_count": len(wave2_stores),
            }
        )

    stats = {
        "total_groups": len(simulations),
        "avg_total_store_count": round(
            sum(int(item.get("total_store_count") or 0) for item in simulations) / len(simulations), 2
        )
        if simulations
        else 0.0,
        "max_total_store_count": max((int(item.get("total_store_count") or 0) for item in simulations), default=0),
        "min_total_store_count": min((int(item.get("total_store_count") or 0) for item in simulations), default=0),
    }
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return stats, simulations


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_extract_single_routes(delivery_date):
    """
    浠?human_dispatch_routes 鎶藉彇鈥滃崟閰嶇嚎璺紙single routes锛夆€濓細
    鍚屼竴澶?+ 鍚岃溅涓嬶紝鍏堣瘑鍒?X / X+100 鍙岄厤骞舵爣璁板凡浣跨敤 route_id锛?
    鍓╀綑鏈弬涓庡弻閰嶇殑 route_id 瑙嗕负鍗曢厤绾胯矾銆?
    """
    route_store_map = {}
    route_name_map = {}
    by_vehicle_routes = {}

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='human_dispatch_routes'
                """,
                (MYSQL_DATABASE,),
            )
            cols = [str((x or {}).get("COLUMN_NAME") or "") for x in (cursor.fetchall() or [])]
            norm = {re.sub(r"[\s_\-]+", "", c.lower()): c for c in cols}
            route_name_col = (
                norm.get("routename")
                or norm.get("linename")
                or norm.get("绾胯矾鍚?)
                or norm.get("绾胯矾鍚嶇О")
            )
            route_name_select = f"`{route_name_col}` AS route_name" if route_name_col else "NULL AS route_name"

            cursor.execute(
                f"""
                SELECT
                  delivery_date,
                  vehicle_id,
                  route_id,
                  store_id,
                  {route_name_select}
                FROM human_dispatch_routes
                WHERE delivery_date = %s
                ORDER BY vehicle_id ASC, route_id ASC, store_id ASC
                """,
                (delivery_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall() or []

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in rows:
        vehicle_id = str(row.get("vehicle_id") or "").strip()
        route_id = int(_safe_float(row.get("route_id"), 0))
        store_id = str(row.get("store_id") or "").strip()
        route_name = str(row.get("route_name") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not vehicle_id or route_id <= 0 or not store_id:
            continue

        key = (vehicle_id, route_id)
        if key not in route_store_map:
            route_store_map[key] = set()
        route_store_map[key].add(store_id)

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if key not in route_name_map and route_name:
            route_name_map[key] = route_name

        if vehicle_id not in by_vehicle_routes:
            by_vehicle_routes[vehicle_id] = set()
        by_vehicle_routes[vehicle_id].add(route_id)

    singles = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for vehicle_id, route_ids in by_vehicle_routes.items():
        sorted_ids = sorted(route_ids)
        used_route_ids = set()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for route_id in sorted_ids:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if (route_id + 100) in route_ids:
                used_route_ids.add(route_id)
                used_route_ids.add(route_id + 100)

        single_route_ids = [rid for rid in sorted_ids if rid not in used_route_ids]
        for route_id in single_route_ids:
            stores = route_store_map.get((vehicle_id, route_id), set())
            store_ids = sorted(
                stores,
                key=lambda x: (int(x) if str(x).isdigit() else str(x)),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not store_ids:
                continue
            route_name = str(route_name_map.get((vehicle_id, route_id)) or "").strip() or f"璺嚎{route_id}"
            singles.append(
                {
                    "delivery_date": str(delivery_date),
                    "vehicle_id": vehicle_id,
                    "route_id": int(route_id),
                    "route_name": route_name,
                    "store_ids": store_ids,
                    "store_count": len(store_ids),
                }
            )

    singles.sort(key=lambda item: (item["delivery_date"], item["vehicle_id"], item["route_id"]))
    counts = [int(item.get("store_count") or 0) for item in singles]
    total_routes = len(singles)
    stats = {
        "total_routes": total_routes,
        "avg_store_count": round((sum(counts) / total_routes), 2) if total_routes else 0.0,
        "max_store_count": max(counts) if counts else 0,
        "min_store_count": min(counts) if counts else 0,
    }
    return stats, singles


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_build_dispatch_type_map(delivery_date):
    """
    鏋勫缓鍚屾棩鍚岃溅鐨勯厤閫佺被鍨嬫槧灏勶細
    - first: route_id = X 涓斿瓨鍦?X+100
    - second: route_id = X+100 涓斿瓨鍦?X
    - single: 鏈弬涓?X/X+100 閰嶅
    """
    by_vehicle_routes = {}
    dispatch_type_map = {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT vehicle_id, route_id
                FROM human_dispatch_routes
                WHERE delivery_date = %s
                """,
                (delivery_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    for row in rows:
        vehicle_id = str((row or {}).get("vehicle_id") or "").strip()
        route_id = int(_safe_float((row or {}).get("route_id"), 0))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not vehicle_id or route_id <= 0:
            continue
        by_vehicle_routes.setdefault(vehicle_id, set()).add(route_id)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for vehicle_id, route_ids in by_vehicle_routes.items():
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        for route_id in route_ids:
            if (route_id + 100) in route_ids:
                dispatch_type_map[(vehicle_id, route_id)] = "first"
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            elif (route_id - 100) in route_ids:
                dispatch_type_map[(vehicle_id, route_id)] = "second"
            else:
                dispatch_type_map[(vehicle_id, route_id)] = "single"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return dispatch_type_map


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_get_distance_duration_map(store_ids):
    """
    璇诲彇搴楀埌搴楄窛绂?鏃堕暱缂撳瓨锛屽彧鐢ㄦ湰鍦扮紦瀛樿〃锛屼笉璋冪敤澶栭儴 API銆?
    杩斿洖:
      {
        (src, dst): {"distance_km": x, "duration_minutes": y, "source": "..."}
      }
    """
    normalized = sorted({str(x or "").strip() for x in (store_ids or []) if str(x or "").strip()})
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not normalized:
        return {}
    ids = [DC_ID] + normalized
    placeholders = ",".join(["%s"] * len(ids))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    params = ids + ids
    mapping = {}
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT
                  from_store_id, to_store_id,
                  distance_km, duration_minutes, source
                FROM {STORE_DISTANCE_TABLE}
                WHERE from_store_id IN ({placeholders})
                  AND to_store_id IN ({placeholders})
                """,
                params,
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            rows = cursor.fetchall() or []
    for row in rows:
        src = str((row or {}).get("from_store_id") or "").strip()
        dst = str((row or {}).get("to_store_id") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not src or not dst:
            continue
        mapping[(src, dst)] = {
            "distance_km": float(_safe_float((row or {}).get("distance_km"), 0.0)),
            "duration_minutes": float(_safe_float((row or {}).get("duration_minutes"), 0.0)),
            "source": str((row or {}).get("source") or "matrix").strip() or "matrix",
        }
    return mapping


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_load_profile_map():
    """
    浠?human_dispatch_solver_profile 璇诲彇搴楅摵琛ュ厖灞炴€с€?
    浠呯敤浜庣粡楠屽垎鏋愶紝涓嶅奖鍝嶈皟搴﹂摼璺€?
    """
    profile = {}
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            _sync_human_dispatch_solver_profile(cursor)
            cursor.execute(
                f"""
                SELECT
                  shop_code, wave_belongs,
                  first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4,
                  service_minutes
                FROM {HUMAN_DISPATCH_SOLVER_PROFILE_TABLE}
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    for row in rows:
        shop_code = str((row or {}).get("shop_code") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not shop_code:
            continue
        profile[shop_code] = {
            "wave_belongs": str((row or {}).get("wave_belongs") or "").strip(),
            "first_wave_time": str((row or {}).get("first_wave_time") or "").strip(),
            "second_wave_time": str((row or {}).get("second_wave_time") or "").strip(),
            "arrival_time_w3": str((row or {}).get("arrival_time_w3") or "").strip(),
            "arrival_time_w4": str((row or {}).get("arrival_time_w4") or "").strip(),
            "service_minutes": int(_safe_float((row or {}).get("service_minutes"), 15)),
        }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return profile


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_expected_time_for_wave(profile_row, dispatch_type):
    if not isinstance(profile_row, dict):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return ""
    if dispatch_type == "first":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str(profile_row.get("first_wave_time") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if dispatch_type == "second":
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str(profile_row.get("second_wave_time") or "").strip()
    wave_text = str(profile_row.get("wave_belongs") or "").strip()
    wave_tokens = [x.strip() for x in wave_text.split(",") if x.strip()]
    if "3" in wave_tokens:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return str(profile_row.get("arrival_time_w3") or "").strip()
    if "4" in wave_tokens:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str(profile_row.get("arrival_time_w4") or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "2" in wave_tokens and "1" not in wave_tokens:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return str(profile_row.get("second_wave_time") or "").strip()
    return str(profile_row.get("first_wave_time") or "").strip()


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_extract_human_route_stops(delivery_date):
    """
    鎻愬彇浜哄伐绾胯矾鏄庣粏锛堥€愬仠闈狅級銆?
    """
    rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME='human_dispatch_routes'
                """,
                (MYSQL_DATABASE,),
            )
            cols = [str((x or {}).get("COLUMN_NAME") or "") for x in (cursor.fetchall() or [])]
            norm = {re.sub(r"[\s_\-]+", "", c.lower()): c for c in cols}
            route_name_col = (
                norm.get("routename")
                or norm.get("linename")
                or norm.get("绾胯矾鍚?)
                or norm.get("绾胯矾鍚嶇О")
            )
            route_name_select = f"`{route_name_col}` AS route_name" if route_name_col else "NULL AS route_name"
            cargo_map = {
                "ambient_turnover_boxes": norm.get("ambientturnoverboxes") or "ambient_turnover_boxes",
                "ambient_cartons": norm.get("ambientcartons") or "ambient_cartons",
                "ambient_full_cases": norm.get("ambientfullcases") or "ambient_full_cases",
                "frozen_bags": norm.get("frozenbags") or "frozen_bags",
                "frozen_cartons": norm.get("frozencartons") or "frozen_cartons",
                "cold_bins": norm.get("coldbins") or "cold_bins",
                "cold_cartons": norm.get("coldcartons") or "cold_cartons",
            }
            cursor.execute(
                f"""
                SELECT
                  delivery_date,
                  vehicle_id,
                  route_id,
                  {route_name_select},
                  stop_sequence,
                  store_id,
                  store_name,
                  CAST(COALESCE(`{cargo_map['ambient_turnover_boxes']}`, 0) AS DECIMAL(14,6)) AS ambient_turnover_boxes,
                  CAST(COALESCE(`{cargo_map['ambient_cartons']}`, 0) AS DECIMAL(14,6)) AS ambient_cartons,
                  CAST(COALESCE(`{cargo_map['ambient_full_cases']}`, 0) AS DECIMAL(14,6)) AS ambient_full_cases,
                  CAST(COALESCE(`{cargo_map['frozen_bags']}`, 0) AS DECIMAL(14,6)) AS frozen_bags,
                  CAST(COALESCE(`{cargo_map['frozen_cartons']}`, 0) AS DECIMAL(14,6)) AS frozen_cartons,
                  CAST(COALESCE(`{cargo_map['cold_bins']}`, 0) AS DECIMAL(14,6)) AS cold_bins,
                  CAST(COALESCE(`{cargo_map['cold_cartons']}`, 0) AS DECIMAL(14,6)) AS cold_cartons
                FROM human_dispatch_routes
                WHERE delivery_date = %s
                ORDER BY vehicle_id ASC, route_id ASC, stop_sequence ASC
                """,
                (delivery_date,),
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            rows = cursor.fetchall() or []
    return rows


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_extract_mileage_details(delivery_date):
    stops = _rengong_extract_human_route_stops(delivery_date)
    route_map = {}
    all_store_ids = set()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in stops:
        vehicle_id = str((row or {}).get("vehicle_id") or "").strip()
        route_id = int(_safe_float((row or {}).get("route_id"), 0))
        store_id = str((row or {}).get("store_id") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not vehicle_id or route_id <= 0 or not store_id:
            continue
        key = (vehicle_id, route_id)
        route_map.setdefault(
            key,
            {
                "delivery_date": str(delivery_date),
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "route_name": str((row or {}).get("route_name") or "").strip() or f"绾胯矾{route_id}",
                "stores": [],
            },
        )
        route_map[key]["stores"].append(store_id)
        all_store_ids.add(store_id)
    dist_map = _rengong_get_distance_duration_map(all_store_ids)
    mileage_details = []
    day_vehicle_map = {}
    for (_, _), route in route_map.items():
        stores = [x for x in (route.get("stores") or []) if x]
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not stores:
            continue
        segment_km_list = []
        source_set = set()
        outbound_km = 0.0
        cursor_id = DC_ID
        for sid in stores:
            pair = dist_map.get((cursor_id, sid)) or {}
            seg_km = float(_safe_float(pair.get("distance_km"), 0.0))
            outbound_km += seg_km
            segment_km_list.append(round(seg_km, 3))
            source_set.add(str(pair.get("source") or "matrix").strip() or "matrix")
            cursor_id = sid
        back_pair = dist_map.get((cursor_id, DC_ID)) or {}
        return_km = float(_safe_float(back_pair.get("distance_km"), 0.0))
        outbound_km = round(outbound_km, 3)
        return_km = round(return_km, 3)
        route_total_km = round(outbound_km + return_km, 3)
        source_set.add(str(back_pair.get("source") or "matrix").strip() or "matrix")
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not source_set:
            distance_source = "matrix"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif len(source_set) == 1:
            distance_source = next(iter(source_set))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        else:
            distance_source = "mixed"
        item = {
            "delivery_date": route.get("delivery_date"),
            "vehicle_id": route.get("vehicle_id"),
            "route_id": route.get("route_id"),
            "route_name": route.get("route_name"),
            "store_count": len(stores),
            "route_total_km": route_total_km,
            "outbound_km": outbound_km,
            "return_km": return_km,
            "segment_km_list": segment_km_list,
            "distance_source": distance_source,
        }
        mileage_details.append(item)
        day_key = (route.get("delivery_date"), route.get("vehicle_id"))
        day_vehicle_map[day_key] = day_vehicle_map.get(day_key, 0.0) + route_total_km
    day_vehicle_totals = [
        {
            "delivery_date": k[0],
            "vehicle_id": k[1],
            "day_total_km": round(v, 3),
        }
        for k, v in sorted(day_vehicle_map.items(), key=lambda x: (x[0][0], x[0][1]))
    ]
    mileage_details.sort(key=lambda x: (x["delivery_date"], x["vehicle_id"], int(_safe_float(x["route_id"], 0))))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "mileage_details": mileage_details,
        "day_vehicle_totals": day_vehicle_totals,
    }


def _rengong_extract_arrival_details(delivery_date):
    stops = _rengong_extract_human_route_stops(delivery_date)
    dispatch_type_map = _rengong_build_dispatch_type_map(delivery_date)
    profile_map = _rengong_load_profile_map()
    all_store_ids = {str((x or {}).get("store_id") or "").strip() for x in stops}
    all_store_ids = {x for x in all_store_ids if x}
    dist_map = _rengong_get_distance_duration_map(all_store_ids)

    by_route = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in stops:
        vehicle_id = str((row or {}).get("vehicle_id") or "").strip()
        route_id = int(_safe_float((row or {}).get("route_id"), 0))
        store_id = str((row or {}).get("store_id") or "").strip()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not vehicle_id or route_id <= 0 or not store_id:
            continue
        key = (vehicle_id, route_id)
        by_route.setdefault(key, []).append(row)

    details = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for (vehicle_id, route_id), route_stops in by_route.items():
        route_stops_sorted = sorted(route_stops, key=lambda r: int(_safe_float((r or {}).get("stop_sequence"), 0)))
        dispatch_type = dispatch_type_map.get((vehicle_id, route_id), "single")
        first_store_id = str((route_stops_sorted[0] or {}).get("store_id") or "").strip() if route_stops_sorted else ""
        first_profile = profile_map.get(first_store_id) or {}
        start_anchor_text = _rengong_expected_time_for_wave(first_profile, dispatch_type)
        cursor_min = _simulate_hhmm_to_minutes(start_anchor_text)
        if cursor_min is None:
            cursor_min = 0.0
        prev_store_id = DC_ID
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for row in route_stops_sorted:
            store_id = str((row or {}).get("store_id") or "").strip()
            store_profile = profile_map.get(store_id) or {}
            pair = dist_map.get((prev_store_id, store_id)) or {}
            seg_min = float(_safe_float(pair.get("duration_minutes"), 0.0))
            seg_km = float(_safe_float(pair.get("distance_km"), 0.0))
            cursor_min += seg_min
            arrival_min = cursor_min
            service_minutes_raw = store_profile.get("service_minutes")
            service_minutes_defaulted = service_minutes_raw in (None, "")
            service_minutes = int(_safe_float(service_minutes_raw, 15))
            if service_minutes <= 0:
                service_minutes = 15
                service_minutes_defaulted = True
            departure_min = arrival_min + service_minutes
            cursor_min = departure_min
            details.append(
                {
                    "delivery_date": str(delivery_date),
                    "vehicle_id": vehicle_id,
                    "route_id": route_id,
                    "route_name": str((row or {}).get("route_name") or "").strip() or f"绾胯矾{route_id}",
                    "stop_sequence": int(_safe_float((row or {}).get("stop_sequence"), 0)),
                    "store_id": store_id,
                    "store_name": str((row or {}).get("store_name") or "").strip(),
                    "dispatch_type": dispatch_type,
                    "computed_arrival_time": _simulate_minutes_to_hhmm(arrival_min),
                    "departure_time": _simulate_minutes_to_hhmm(departure_min),
                    "segment_minutes_from_prev": round(seg_min, 3),
                    "segment_km_from_prev": round(seg_km, 3),
                    "service_minutes": service_minutes,
                    "service_minutes_defaulted": bool(service_minutes_defaulted),
                }
            )
            prev_store_id = store_id
    details.sort(
        key=lambda x: (
            x["delivery_date"],
            x["vehicle_id"],
            int(_safe_float(x["route_id"], 0)),
            int(_safe_float(x["stop_sequence"], 0)),
        )
    )
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return details


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _rengong_extract_load_details(delivery_date):
    stops = _rengong_extract_human_route_stops(delivery_date)
    route_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in stops:
        vehicle_id = str((row or {}).get("vehicle_id") or "").strip()
        route_id = int(_safe_float((row or {}).get("route_id"), 0))
        store_id = str((row or {}).get("store_id") or "").strip()
        if not vehicle_id or route_id <= 0 or not store_id:
            continue
        key = (vehicle_id, route_id)
        route_map.setdefault(
            key,
            {
                "delivery_date": str(delivery_date),
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "route_name": str((row or {}).get("route_name") or "").strip() or f"绾胯矾{route_id}",
                "raw": {
                    "ambient_turnover_boxes": 0.0,
                    "ambient_cartons": 0.0,
                    "ambient_full_cases": 0.0,
                    "frozen_bags": 0.0,
                    "frozen_cartons": 0.0,
                    "cold_bins": 0.0,
                    "cold_cartons": 0.0,
                },
            },
        )
        bucket = route_map[key]["raw"]
        bucket["ambient_turnover_boxes"] += float(_safe_float((row or {}).get("ambient_turnover_boxes"), 0.0))
        bucket["ambient_cartons"] += float(_safe_float((row or {}).get("ambient_cartons"), 0.0))
        bucket["ambient_full_cases"] += float(_safe_float((row or {}).get("ambient_full_cases"), 0.0))
        bucket["frozen_bags"] += float(_safe_float((row or {}).get("frozen_bags"), 0.0))
        bucket["frozen_cartons"] += float(_safe_float((row or {}).get("frozen_cartons"), 0.0))
        bucket["cold_bins"] += float(_safe_float((row or {}).get("cold_bins"), 0.0))
        bucket["cold_cartons"] += float(_safe_float((row or {}).get("cold_cartons"), 0.0))

    load_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                """
                SELECT shop_code, wave1_load, wave2_load, wave3_load, wave4_load, total_resolved_load
                FROM store_wave_load_resolved
                """
            )
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for row in (cursor.fetchall() or []):
                shop_code = str((row or {}).get("shop_code") or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if shop_code:
                    load_map[shop_code] = row

            cursor.execute(
                f"""
                SELECT plate_no, vehicle_type, capacity, speed, can_cold
                FROM {WMS_VEHICLE_TABLE}
                """
            )
            vehicle_rows = cursor.fetchall() or []
    vehicle_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in vehicle_rows:
        plate = str((row or {}).get("plate_no") or "").strip()
        if not plate:
            continue
        vehicle_map[plate] = {
            "vehicle_type": str((row or {}).get("vehicle_type") or "").strip() or DEFAULT_VEHICLE_TYPE,
            "capacity": float(_safe_float((row or {}).get("capacity"), 0.0)),
            "speed": float(_safe_float((row or {}).get("speed"), 38.0)),
            "can_cold": int(_safe_float((row or {}).get("can_cold"), 0)),
        }

    dispatch_type_map = _rengong_build_dispatch_type_map(delivery_date)
    by_route_stores = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in stops:
        vehicle_id = str((row or {}).get("vehicle_id") or "").strip()
        route_id = int(_safe_float((row or {}).get("route_id"), 0))
        store_id = str((row or {}).get("store_id") or "").strip()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if vehicle_id and route_id > 0 and store_id:
            by_route_stores.setdefault((vehicle_id, route_id), set()).add(store_id)

    load_details = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for (vehicle_id, route_id), payload in sorted(route_map.items(), key=lambda x: (x[0][0], x[0][1])):
        dispatch_type = dispatch_type_map.get((vehicle_id, route_id), "single")
        route_stores = by_route_stores.get((vehicle_id, route_id), set())
        resolved_wave_sum = 0.0
        resolved_total_sum = 0.0
        for store_id in route_stores:
            row = load_map.get(store_id) or {}
            resolved_total_sum += float(_safe_float((row or {}).get("total_resolved_load"), 0.0))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if dispatch_type == "first":
                resolved_wave_sum += float(_safe_float((row or {}).get("wave1_load"), 0.0))
            elif dispatch_type == "second":
                resolved_wave_sum += float(_safe_float((row or {}).get("wave2_load"), 0.0))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            else:
                resolved_wave_sum += float(_safe_float((row or {}).get("total_resolved_load"), 0.0))

        vehicle_info = vehicle_map.get(vehicle_id) or {}
        capacity = float(_safe_float(vehicle_info.get("capacity"), 0.0))
        capacity_missing = capacity <= 0
        detail = {
            "delivery_date": payload.get("delivery_date"),
            "vehicle_id": vehicle_id,
            "route_id": route_id,
            "route_name": payload.get("route_name"),
            "dispatch_type": dispatch_type,
            "raw_load": {
                "ambient_turnover_boxes": round(payload["raw"]["ambient_turnover_boxes"], 6),
                "ambient_cartons": round(payload["raw"]["ambient_cartons"], 6),
                "ambient_full_cases": round(payload["raw"]["ambient_full_cases"], 6),
                "frozen_bags": round(payload["raw"]["frozen_bags"], 6),
                "frozen_cartons": round(payload["raw"]["frozen_cartons"], 6),
                "cold_bins": round(payload["raw"]["cold_bins"], 6),
                "cold_cartons": round(payload["raw"]["cold_cartons"], 6),
            },
            "resolved_load": {
                "wave_resolved_load": round(resolved_wave_sum, 6),
                "total_resolved_load": round(resolved_total_sum, 6),
            },
            "vehicle_capacity": {
                "capacity": round(capacity, 6),
                "vehicle_type": str(vehicle_info.get("vehicle_type") or DEFAULT_VEHICLE_TYPE),
                "speed": float(_safe_float(vehicle_info.get("speed"), 38.0)),
                "can_cold": int(_safe_float(vehicle_info.get("can_cold"), 0)),
            },
            "capacity_missing": bool(capacity_missing),
        }
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not capacity_missing:
            detail["load_ratio"] = {
                "total_ratio": round(resolved_total_sum / capacity, 6),
                "wave_ratio": round(resolved_wave_sum / capacity, 6),
            }
        load_details.append(detail)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return load_details


def rengong_boundary_details(query):
    """
    缁忛獙杈圭晫鏄庣粏鍘熷鏁版嵁浜у嚭锛堝彧璇汇€佸彧鍒嗘瀽锛夛細
    - mileage_details
    - arrival_details
    - single_route_profiles
    - load_details
    """
    delivery_date = str((query.get("date") or [""])[0] or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not delivery_date:
        raise ValueError("date_required")
    mileage_pack = _rengong_extract_mileage_details(delivery_date)
    arrival_details = _rengong_extract_arrival_details(delivery_date)
    _, single_routes = _rengong_extract_single_routes(delivery_date)
    load_details = _rengong_extract_load_details(delivery_date)
    return {
        "delivery_date": delivery_date,
        "mileage_details": mileage_pack.get("mileage_details") or [],
        "day_vehicle_totals": mileage_pack.get("day_vehicle_totals") or [],
        "arrival_details": arrival_details,
        "single_route_profiles": single_routes,
        "load_details": load_details,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def rengong_boundary_page_data(query):
    wave = str((query.get("wave") or ["W1"])[0] or "W1").strip().upper()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wave not in ("W1", "W2", "W3", "W4"):
        wave = "W1"

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wave != "W1":
        return {
            "wave": wave,
            "message": "璇ユ尝娆＄粡楠岃竟鐣屽皻鏈敓鎴?,
            "store_arrival_windows": [],
            "route_load_profiles": [],
            "load_rules": [],
        }

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT
                  store_id, store_name,
                  earliest_arrival_time, avg_arrival_time, latest_arrival_time,
                  early_allowed_minutes, late_allowed_minutes,
                  valid_sample_count
                FROM rengong_store_arrival_windows
                WHERE dispatch_scope=%s AND calc_version=%s
                ORDER BY store_id
                """,
                ("weekday_city_dual_w1", "w1_elapsed_p10_p90_v2"),
            )
            store_rows = c.fetchall() or []

            c.execute(
                """
                SELECT
                  delivery_date, vehicle_id, route_id, route_name, route_category,
                  store_count, route_load_w1, normal_hard_limit,
                  is_airport_route, is_over_normal_limit, is_dirty_candidate
                FROM rengong_route_load_profiles
                WHERE calc_version=%s
                ORDER BY delivery_date, route_id
                """,
                ("route_load_w1_v1",),
            )
            route_rows = c.fetchall() or []

            c.execute(
                """
                SELECT
                  delivery_date, vehicle_id, route_id, route_name,
                  route_group, route_type, route_type_detail,
                  store_count, one_way_distance_km, route_load,
                  is_airport, is_over_normal_limit, is_dirty_candidate
                FROM rengong_route_distance_load_profiles
                WHERE calc_version=%s
                ORDER BY route_type, one_way_distance_km DESC, delivery_date, route_id
                """,
                ("route_distance_load_profiles_v1",),
            )
            route_distance_rows = c.fetchall() or []

            c.execute(
                """
                SELECT
                  rule_scope, rule_type, route_category, hard_load_limit, ignore_load_limit,
                  vehicle_rule_type, preferred_vehicle_ids, allow_multi_trip,
                  dirty_load_threshold, dirty_exclude_airport, rule_desc, is_active
                FROM rengong_load_rules
                WHERE rule_scope=%s AND calc_version=%s
                ORDER BY id
                """,
                ("weekday_city_dual_w1", "load_rules_v1"),
            )
            rule_rows = c.fetchall() or []

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for r in store_rows:
        r["early_allowed_minutes"] = round(_safe_float(r.get("early_allowed_minutes"), 0.0), 2)
        r["late_allowed_minutes"] = round(_safe_float(r.get("late_allowed_minutes"), 0.0), 2)
        r["valid_sample_count"] = int(_safe_float(r.get("valid_sample_count"), 0))

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for r in route_rows:
        r["route_load_w1"] = round(_safe_float(r.get("route_load_w1"), 0.0), 6)
        r["normal_hard_limit"] = round(_safe_float(r.get("normal_hard_limit"), 1.25), 4)
        r["store_count"] = int(_safe_float(r.get("store_count"), 0))
        r["is_airport_route"] = bool(r.get("is_airport_route"))
        r["is_over_normal_limit"] = bool(r.get("is_over_normal_limit"))
        r["is_dirty_candidate"] = bool(r.get("is_dirty_candidate"))
        r["delivery_date"] = str(r.get("delivery_date") or "")

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for r in rule_rows:
        pref = r.get("preferred_vehicle_ids")
        if isinstance(pref, str):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            try:
                r["preferred_vehicle_ids"] = json.loads(pref)
            except Exception:
                r["preferred_vehicle_ids"] = []
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif pref is None:
            r["preferred_vehicle_ids"] = []
        r["hard_load_limit"] = None if r.get("hard_load_limit") in (None, "") else round(_safe_float(r.get("hard_load_limit"), 0.0), 4)
        r["dirty_load_threshold"] = None if r.get("dirty_load_threshold") in (None, "") else round(_safe_float(r.get("dirty_load_threshold"), 0.0), 4)
        r["ignore_load_limit"] = None if r.get("ignore_load_limit") is None else bool(r.get("ignore_load_limit"))
        r["allow_multi_trip"] = None if r.get("allow_multi_trip") is None else bool(r.get("allow_multi_trip"))
        r["dirty_exclude_airport"] = None if r.get("dirty_exclude_airport") is None else bool(r.get("dirty_exclude_airport"))
        r["is_active"] = bool(r.get("is_active"))

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "wave": wave,
        "message": "",
        "store_arrival_windows": store_rows,
        "route_load_profiles": route_rows,
        "route_distance_load_profiles": route_distance_rows,
        "load_rules": rule_rows,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def rengong_route_distance_load_detail(query):
    date_text = str((query.get("date") or [""])[0] or "").strip()
    route_id_text = str((query.get("route_id") or [""])[0] or "").strip()
    route_group = str((query.get("route_group") or [""])[0] or "").strip().upper()
    vehicle_id_q = str((query.get("vehicle_id") or [""])[0] or "").strip()

    if not date_text or not route_id_text or route_group not in ("W1", "W2", "OTHER"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return {
            "error": "invalid_params",
            "message": "闇€瑕佸弬鏁? date, route_id, route_group(W1/W2/OTHER)",
            "items": [],
        }

    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as c:
            # 璺嚎姹囨€讳俊鎭紙鐢ㄤ簬寮圭獥澶撮儴锛?
            c.execute(
                """
                SELECT
                  delivery_date, vehicle_id, route_id, route_name,
                  route_group, route_type, one_way_distance_km, route_load,
                  is_airport, is_dirty_candidate, store_count, calc_version
                FROM rengong_route_distance_load_profiles
                WHERE delivery_date=%s AND route_id=%s AND route_group=%s
                  AND calc_version=%s
                ORDER BY id DESC
                LIMIT 1
                """,
                (date_text, route_id_text, route_group, "route_distance_load_profiles_v1"),
            )
            summary = c.fetchone() or {}

            # 绾胯矾闂ㄥ簵椤哄簭
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if vehicle_id_q:
                c.execute(
                    """
                    SELECT
                      stop_sequence, store_id, store_name,
                      ambient_turnover_boxes, ambient_full_cases, ambient_cartons,
                      frozen_bags, frozen_cartons, cold_bins, cold_cartons
                    FROM human_dispatch_routes
                    WHERE delivery_date=%s AND route_id=%s AND vehicle_id=%s
                    ORDER BY stop_sequence
                    """,
                    (date_text, route_id_text, vehicle_id_q),
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            else:
                c.execute(
                    """
                    SELECT
                      stop_sequence, store_id, store_name, vehicle_id,
                      ambient_turnover_boxes, ambient_full_cases, ambient_cartons,
                      frozen_bags, frozen_cartons, cold_bins, cold_cartons
                    FROM human_dispatch_routes
                    WHERE delivery_date=%s AND route_id=%s
                    ORDER BY stop_sequence
                    """,
                    (date_text, route_id_text),
                )
            route_rows = c.fetchall() or []

            # 鍘婚噸锛堝悓搴忓彿+搴楋級
            dedup = {}
            for r in route_rows:
                sk = (int(_safe_float((r or {}).get("stop_sequence"), 0)), str((r or {}).get("store_id") or "").strip())
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if sk not in dedup:
                    dedup[sk] = r
            stops = [dedup[k] for k in sorted(dedup.keys(), key=lambda x: x[0])]

            # 璺濈鐭╅樀瀛楁鑷€傞厤
            c.execute("SHOW COLUMNS FROM store_distance_matrix")
            cols = [str((x or {}).get("Field") or "") for x in (c.fetchall() or [])]
            from_col = next((x for x in ("from_store_id", "from_store_code", "from_shop_code", "from_code") if x in cols), None)
            to_col = next((x for x in ("to_store_id", "to_store_code", "to_shop_code", "to_code") if x in cols), None)
            dist_col = next((x for x in ("distance_km", "distance_m", "distance", "distance_meter", "amap_distance_m") if x in cols), None)

            dist_map = {}
            if from_col and to_col and dist_col:
                c.execute(
                    f"SELECT `{from_col}` AS f, `{to_col}` AS t, `{dist_col}` AS d FROM store_distance_matrix"
                )
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                for r in c.fetchall() or []:
                    f = str((r or {}).get("f") or "").strip()
                    t = str((r or {}).get("t") or "").strip()
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if not f or not t:
                        continue
                    d = _safe_float((r or {}).get("d"), 0.0)
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if dist_col != "distance_km":
                        d = d / 1000.0
                    dist_map[(f, t)] = d

    items = []
    cum_load = 0.0
    for i, r in enumerate(stops):
        sid = str((r or {}).get("store_id") or "").strip()
        prev_sid = "DC" if i == 0 else str((stops[i - 1] or {}).get("store_id") or "").strip()
        next_sid = "" if i + 1 >= len(stops) else str((stops[i + 1] or {}).get("store_id") or "").strip()

        rpcs = _safe_float((r or {}).get("ambient_turnover_boxes"), 0.0)
        rcase = _safe_float((r or {}).get("ambient_full_cases"), 0.0)
        rpaper = _safe_float((r or {}).get("ambient_cartons"), 0.0)
        bpcs = _safe_float((r or {}).get("frozen_bags"), 0.0)
        bpaper = _safe_float((r or {}).get("frozen_cartons"), 0.0)
        apcs = _safe_float((r or {}).get("cold_bins"), 0.0)
        apaper = _safe_float((r or {}).get("cold_cartons"), 0.0)

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if route_group == "W1":
            store_load = rpcs / 207.0 + rcase / 380.0 + rpaper / 380.0 + bpcs / 120.0 + bpaper / 380.0
        elif route_group == "W2":
            store_load = apcs / 350.0 + apaper / 380.0
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        else:
            store_load = None

        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if store_load is not None:
            cum_load += store_load

        items.append(
            {
                "stop_sequence": int(_safe_float((r or {}).get("stop_sequence"), 0)),
                "store_id": sid,
                "store_name": str((r or {}).get("store_name") or "").strip(),
                "distance_from_prev_km": round(_safe_float(dist_map.get((prev_sid, sid), 0.0), 0.0), 3)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if sid
                else None,
                "distance_to_next_km": round(_safe_float(dist_map.get((sid, next_sid), 0.0), 0.0), 3)
                if sid and next_sid
                else None,
                "rpcs": round(rpcs, 3),
                "rcase": round(rcase, 3),
                "rpaper": round(rpaper, 3),
                "bpcs": round(bpcs, 3),
                "bpaper": round(bpaper, 3),
                "apcs": round(apcs, 3),
                "apaper": round(apaper, 3),
                "store_load_resolved": (round(store_load, 6) if store_load is not None else None),
                "cumulative_load": (round(cum_load, 6) if store_load is not None else None),
            }
        )

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "date": date_text,
        "route_id": route_id_text,
        "route_group": route_group,
        "summary": summary,
        "items": items,
    }


def chaos_replay_task_view(query):
    date_text = str((query.get("date") or [""])[0] or "").strip()
    calc_version = "chaos_replay_task_v1"

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as c:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not date_text:
                c.execute(
                    """
                    SELECT MAX(delivery_date) AS latest_date
                    FROM chaos_replay_tasks
                    WHERE calc_version=%s
                    """,
                    (calc_version,),
                )
                latest = c.fetchone() or {}
                date_text = str(latest.get("latest_date") or "").strip()

            if not date_text:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                return {"date": "", "task": {}, "stores": [], "vehicles": []}

            c.execute(
                """
                SELECT
                  delivery_date, day_type, store_count, vehicle_count, route_count,
                  total_load_w1, total_load_w2, total_load, source, calc_version
                FROM chaos_replay_tasks
                WHERE calc_version=%s AND delivery_date=%s
                LIMIT 1
                """,
                (calc_version, date_text),
            )
            task = c.fetchone() or {}

            c.execute(
                """
                SELECT
                  store_id, store_name,
                  sum_rpcs, sum_rcase, sum_rpaper,
                  sum_bpcs, sum_bpaper, sum_apcs, sum_apaper,
                  load_w1, load_w2, total_load,
                  original_route_ids, original_vehicle_ids
                FROM chaos_replay_task_stores
                WHERE calc_version=%s AND delivery_date=%s
                ORDER BY total_load DESC, store_id
                """,
                (calc_version, date_text),
            )
            stores = c.fetchall() or []

            c.execute(
                """
                SELECT
                  vehicle_id, driver_name, route_count, original_route_ids
                FROM chaos_replay_task_vehicles
                WHERE calc_version=%s AND delivery_date=%s
                ORDER BY vehicle_id
                """,
                (calc_version, date_text),
            )
            vehicles = c.fetchall() or []

    if task:
        task["delivery_date"] = str(task.get("delivery_date") or "")
        task["store_count"] = int(_safe_float(task.get("store_count"), 0))
        task["vehicle_count"] = int(_safe_float(task.get("vehicle_count"), 0))
        task["route_count"] = int(_safe_float(task.get("route_count"), 0))
        task["total_load_w1"] = round(_safe_float(task.get("total_load_w1"), 0.0), 6)
        task["total_load_w2"] = round(_safe_float(task.get("total_load_w2"), 0.0), 6)
        task["total_load"] = round(_safe_float(task.get("total_load"), 0.0), 6)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in stores:
        row["sum_rpcs"] = round(_safe_float(row.get("sum_rpcs"), 0.0), 6)
        row["sum_rcase"] = round(_safe_float(row.get("sum_rcase"), 0.0), 6)
        row["sum_rpaper"] = round(_safe_float(row.get("sum_rpaper"), 0.0), 6)
        row["sum_bpcs"] = round(_safe_float(row.get("sum_bpcs"), 0.0), 6)
        row["sum_bpaper"] = round(_safe_float(row.get("sum_bpaper"), 0.0), 6)
        row["sum_apcs"] = round(_safe_float(row.get("sum_apcs"), 0.0), 6)
        row["sum_apaper"] = round(_safe_float(row.get("sum_apaper"), 0.0), 6)
        row["load_w1"] = round(_safe_float(row.get("load_w1"), 0.0), 6)
        row["load_w2"] = round(_safe_float(row.get("load_w2"), 0.0), 6)
        row["total_load"] = round(_safe_float(row.get("total_load"), 0.0), 6)

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in vehicles:
        row["route_count"] = int(_safe_float(row.get("route_count"), 0))

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "date": date_text,
        "task": task,
        "stores": stores,
        "vehicles": vehicles,
    }


def rengong_dengtang_explain(query):
    date_text = str((query.get("date") or [""])[0] or "").strip()
    calc_version = "route_slack_profile_v1"

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as c:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if not date_text:
                c.execute("SELECT MAX(delivery_date) AS latest_date FROM chaos_replay_tasks")
                latest = c.fetchone() or {}
                date_text = str(latest.get("latest_date") or "").strip()
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if not date_text:
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                return {"daySummary": {}, "routes": []}

            c.execute(
                """
                SELECT day_type
                FROM chaos_replay_tasks
                WHERE delivery_date=%s
                LIMIT 1
                """,
                (date_text,),
            )
            day_row = c.fetchone() or {}
            day_type = str(day_row.get("day_type") or "").strip() or "NORMAL"

            c.execute(
                """
                SELECT delivery_date
                FROM chaos_replay_tasks
                WHERE day_type=%s AND delivery_date < %s
                ORDER BY delivery_date DESC
                LIMIT 1
                """,
                (day_type, date_text),
            )
            prev_row = c.fetchone() or {}
            prev_date_text = str(prev_row.get("delivery_date") or "").strip()

            c.execute(
                """
                SELECT
                  route_id, stop_sequence, store_id, store_name,
                  ambient_turnover_boxes, ambient_cartons, ambient_full_cases,
                  frozen_bags, frozen_cartons, cold_bins, cold_cartons
                FROM human_dispatch_routes
                WHERE delivery_date=%s
                ORDER BY route_id, stop_sequence
                """,
                (date_text,),
            )
            curr_rows = c.fetchall() or []

            prev_rows = []
            if prev_date_text:
                c.execute(
                    """
                    SELECT
                      route_id, stop_sequence, store_id, store_name,
                      ambient_turnover_boxes, ambient_cartons, ambient_full_cases,
                      frozen_bags, frozen_cartons, cold_bins, cold_cartons
                    FROM human_dispatch_routes
                    WHERE delivery_date=%s
                    ORDER BY route_id, stop_sequence
                    """,
                    (prev_date_text,),
                )
                prev_rows = c.fetchall() or []

            c.execute("SHOW COLUMNS FROM human_dispatch_routes")
            hdr_cols = [str((x or {}).get("Field") or "") for x in (c.fetchall() or [])]
            per_store_dist_col = next((x for x in ("distance_from_prev_km", "segment_km_from_prev", "distance_from_prev", "segment_distance_km") if x in hdr_cols), None)

            curr_dist_rows = []
            prev_dist_rows = []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if per_store_dist_col:
                c.execute(
                    f"""
                    SELECT route_id, stop_sequence, store_id, `{per_store_dist_col}` AS seg_dist
                    FROM human_dispatch_routes
                    WHERE delivery_date=%s
                    ORDER BY route_id, stop_sequence
                    """,
                    (date_text,),
                )
                curr_dist_rows = c.fetchall() or []
                if prev_date_text:
                    c.execute(
                        f"""
                        SELECT route_id, stop_sequence, store_id, `{per_store_dist_col}` AS seg_dist
                        FROM human_dispatch_routes
                        WHERE delivery_date=%s
                        ORDER BY route_id, stop_sequence
                        """,
                        (prev_date_text,),
                    )
                    prev_dist_rows = c.fetchall() or []

            c.execute(
                """
                SELECT delivery_date, route_id, one_way_distance_km
                FROM rengong_route_distance_load_profiles
                WHERE calc_version=%s AND delivery_date IN (%s, %s)
                """,
                ("route_distance_load_profiles_v1", date_text, prev_date_text or date_text),
            )
            route_distance_rows = c.fetchall() or []

            c.execute(
                """
                SELECT route_id, load_p75, load_p90, normal_load_limit
                FROM rengong_route_slack_profiles
                WHERE calc_version=%s AND day_type=%s
                """,
                (calc_version, day_type),
            )
            slack_rows = c.fetchall() or []

    curr_by_route = {}
    curr_seen = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for r in curr_rows:
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        seq = int(_safe_float((r or {}).get("stop_sequence"), 0))
        sid = str((r or {}).get("store_id") or "").strip()
        sname = str((r or {}).get("store_name") or "").strip()
        rpcs = _safe_float((r or {}).get("ambient_turnover_boxes"), 0.0)
        rcarton = _safe_float((r or {}).get("ambient_cartons"), 0.0)
        rcase = _safe_float((r or {}).get("ambient_full_cases"), 0.0)
        bpcs = _safe_float((r or {}).get("frozen_bags"), 0.0)
        bpaper = _safe_float((r or {}).get("frozen_cartons"), 0.0)
        apcs = _safe_float((r or {}).get("cold_bins"), 0.0)
        apaper = _safe_float((r or {}).get("cold_cartons"), 0.0)
        row_key = (rid, seq, sid)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if sid and row_key in curr_seen:
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if sid:
            curr_seen.add(row_key)
        load = rpcs / 207.0 + rcase / 380.0 + rcarton / 380.0 + bpcs / 120.0 + bpaper / 380.0 + apcs / 350.0 + apaper / 380.0
        bucket = curr_by_route.setdefault(rid, {"seq": [], "set": set(), "load": 0.0})
        if sid:
            bucket["seq"].append(sid)
            bucket["set"].add(sid)
            bucket.setdefault("stores", []).append(
                {
                    "stop_sequence": seq,
                    "store_id": sid,
                    "store_name": sname or sid,
                    "store_load": round(load, 6),
                }
            )
        bucket["load"] += load

    prev_by_route = {}
    prev_seen = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for r in prev_rows:
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        seq = int(_safe_float((r or {}).get("stop_sequence"), 0))
        sid = str((r or {}).get("store_id") or "").strip()
        sname = str((r or {}).get("store_name") or "").strip()
        rpcs = _safe_float((r or {}).get("ambient_turnover_boxes"), 0.0)
        rcarton = _safe_float((r or {}).get("ambient_cartons"), 0.0)
        rcase = _safe_float((r or {}).get("ambient_full_cases"), 0.0)
        bpcs = _safe_float((r or {}).get("frozen_bags"), 0.0)
        bpaper = _safe_float((r or {}).get("frozen_cartons"), 0.0)
        apcs = _safe_float((r or {}).get("cold_bins"), 0.0)
        apaper = _safe_float((r or {}).get("cold_cartons"), 0.0)
        row_key = (rid, seq, sid)
        if sid and row_key in prev_seen:
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if sid:
            prev_seen.add(row_key)
        load = rpcs / 207.0 + rcase / 380.0 + rcarton / 380.0 + bpcs / 120.0 + bpaper / 380.0 + apcs / 350.0 + apaper / 380.0
        bucket = prev_by_route.setdefault(rid, {"seq": [], "set": set()})
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if sid:
            bucket["seq"].append(sid)
            bucket["set"].add(sid)
            bucket.setdefault("stores", []).append(
                {
                    "stop_sequence": seq,
                    "store_id": sid,
                    "store_name": sname or sid,
                    "store_load": round(load, 6),
                }
            )
        bucket["load"] = _safe_float(bucket.get("load"), 0.0) + load

    curr_store_dist_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for r in curr_dist_rows:
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        seq = int(_safe_float((r or {}).get("stop_sequence"), 0))
        sid = str((r or {}).get("store_id") or "").strip()
        seg = _safe_float((r or {}).get("seg_dist"), 0.0)
        curr_store_dist_map[(rid, seq, sid)] = seg

    prev_store_dist_map = {}
    for r in prev_dist_rows:
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        seq = int(_safe_float((r or {}).get("stop_sequence"), 0))
        sid = str((r or {}).get("store_id") or "").strip()
        seg = _safe_float((r or {}).get("seg_dist"), 0.0)
        prev_store_dist_map[(rid, seq, sid)] = seg

    route_distance_map = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for r in route_distance_rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        km = _safe_float((r or {}).get("one_way_distance_km"), 0.0)
        route_distance_map[(d, rid)] = km

    slack_map = {}
    for r in slack_rows:
        rid = int(_safe_float((r or {}).get("route_id"), 0))
        slack_map[rid] = {
            "p75": _safe_float((r or {}).get("load_p75"), 0.0),
            "p90": _safe_float((r or {}).get("load_p90"), 0.0),
            "limit": _safe_float((r or {}).get("normal_load_limit"), 0.0),
        }

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _reuse_type(route_id: int):
        curr = curr_by_route.get(route_id) or {"seq": [], "set": set()}
        prev = prev_by_route.get(route_id)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not prev_date_text:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return "MISSING_PREV_ROUTE"
        if not prev:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return "NEW_ROUTE"
        if curr["seq"] == prev["seq"]:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return "EXACT_COPY"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if curr["set"] == prev["set"]:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            return "SAME_STORES_DIFF_ORDER"
        union = len(curr["set"] | prev["set"])
        inter = len(curr["set"] & prev["set"])
        overlap = inter / union if union else 0.0
        if overlap >= 0.2:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            return "PARTIAL_CHANGE"
        return "UNKNOWN_CHANGE"

    routes = []
    stable_count = 0
    risk_count = 0
    exact_copy_count = 0
    changed_count = 0
    review_count = 0

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _build_snapshot(date_key: str, route_id: int, route_bucket: dict, store_dist_map: dict):
        stores_src = sorted((route_bucket or {}).get("stores") or [], key=lambda x: int(_safe_float(x.get("stop_sequence"), 0)))
        stores = []
        cum = 0.0
        has_store_dist = bool(per_store_dist_col)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for s in stores_src:
            seq = int(_safe_float((s or {}).get("stop_sequence"), 0))
            sid = str((s or {}).get("store_id") or "").strip()
            seg = None
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if has_store_dist:
                seg = _safe_float(store_dist_map.get((route_id, seq, sid)), 0.0)
                cum += seg
            stores.append(
                {
                    "stop_sequence": seq,
                    "store_id": sid,
                    "store_name": str((s or {}).get("store_name") or sid).strip(),
                    "store_load": round(_safe_float((s or {}).get("store_load"), 0.0), 6),
                    "distance_from_prev": (round(seg, 3) if seg is not None else None),
                    "distance_cumulative": (round(cum, 3) if seg is not None else None),
                }
            )
        total_load = round(_safe_float((route_bucket or {}).get("load"), 0.0), 6)
        line_km = route_distance_map.get((date_key, route_id))
        return {
            "route_id": route_id,
            "route_load": total_load,
            "store_count": len(stores),
            "distance": (round(_safe_float(line_km, 0.0), 3) if line_km is not None else None),
            "distance_available": bool(line_km is not None),
            "distance_note": ("" if line_km is not None else "褰撳墠鏁版嵁鏃犵嚎璺噷绋嬪瓧娈?),
            "stores": stores,
            "store_distance_available": bool(per_store_dist_col),
            "store_distance_note": ("" if per_store_dist_col else "褰撳墠鏁版嵁鏃犲崟搴楅噷绋嬪瓧娈?),
        }

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for rid in sorted(curr_by_route.keys()):
        curr = curr_by_route[rid]
        load = _safe_float(curr.get("load"), 0.0)
        reuse = _reuse_type(rid)
        slack = slack_map.get(rid) or {"p75": 0.0, "p90": 0.0, "limit": 0.0}
        p75 = _safe_float(slack.get("p75"), 0.0)
        p90 = _safe_float(slack.get("p90"), 0.0)
        nlimit = _safe_float(slack.get("limit"), 0.0)

        if p90 <= 0:
            load_status = "缂哄皯鍘嗗彶涓婇檺鍙傝€?
            risk_flag = True
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif load <= p75:
            load_status = "璐熻浇鍋忎綆锛岀ǔ瀹氬尯"
            risk_flag = False
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif load <= p90:
            load_status = "璐熻浇姝ｅ父锛岃瀵熷尯"
            risk_flag = False
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif load <= p90 * 1.1:
            load_status = "璐熻浇鎺ヨ繎涓婇檺锛岃蒋椋庨櫓鍖?
            risk_flag = True
        elif load <= p90 * 1.3:
            load_status = "璐熻浇鏄庢樉鍋忛珮锛岄珮椋庨櫓鍖?
            risk_flag = True
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        else:
            load_status = "璐熻浇瓒呭嚭甯告€侊紝閲嶆瀯椋庨櫓鍖?
            risk_flag = True

        if reuse == "EXACT_COPY":
            decision_hint = "寤剁画鍓嶄竴鍚岀被鏃ュ畨鎺?
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif reuse == "SAME_STORES_DIFF_ORDER":
            decision_hint = "闂ㄥ簵闆嗗悎鏈彉锛岄『搴忔湁璋冩暣"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif reuse in ("PARTIAL_CHANGE", "UNKNOWN_CHANGE"):
            decision_hint = "缁撴瀯鏈夊彉鍖栵紝寤鸿澶嶆牳鍙樺寲鍘熷洜"
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif reuse == "NEW_ROUTE":
            decision_hint = "鍓嶄竴鍚岀被鏃ユ棤鍚屽彿绾胯矾锛屾寜鏂扮嚎澶嶆牳"
        else:
            decision_hint = "缂哄皯鍓嶄竴鍚岀被鏃ュ弬鑰冿紝寤鸿浜哄伐澶嶆牳"

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if risk_flag and reuse == "EXACT_COPY":
            review_level = "闇€澶嶆牳"
        elif risk_flag:
            review_level = "楂橀闄?
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif reuse in ("PARTIAL_CHANGE", "UNKNOWN_CHANGE", "NEW_ROUTE", "MISSING_PREV_ROUTE"):
            review_level = "闇€澶嶆牳"
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        else:
            review_level = "绋冲畾澶嶇敤"

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if review_level in ("楂橀闄?, "闇€澶嶆牳"):
            review_count += 1
        if risk_flag:
            risk_count += 1
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if reuse == "EXACT_COPY":
            exact_copy_count += 1
            stable_count += 1
        else:
            changed_count += 1

        extra = ""
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if nlimit > 0 and load >= nlimit:
            extra = " 宸茶秴杩囨櫘閫氳杞戒笂闄愶紝闇€閲嶇偣鍏虫敞銆?

        explanation_text = (
            f"璇ョ嚎璺綋鍓嶈揣閲忕害 {round(load, 3)}銆?
            f"{load_status}銆?
            f"鏈琛屼负涓?{reuse}锛寋decision_hint}銆?
            f"{extra}"
        ).strip()

        current_snapshot = _build_snapshot(date_text, rid, curr_by_route.get(rid) or {}, curr_store_dist_map)
        reference_bucket = prev_by_route.get(rid) or {}
        reference_snapshot = _build_snapshot(prev_date_text, rid, reference_bucket, prev_store_dist_map) if prev_date_text and reference_bucket else {
            "route_id": rid,
            "route_load": 0.0,
            "store_count": 0,
            "distance": None,
            "distance_available": False,
            "distance_note": "鏃犲墠涓€鍚岀被鏃ョ嚎璺揩鐓?,
            "stores": [],
            "store_distance_available": bool(per_store_dist_col),
            "store_distance_note": ("" if per_store_dist_col else "褰撳墠鏁版嵁鏃犲崟搴楅噷绋嬪瓧娈?),
        }

        curr_set = set((curr_by_route.get(rid) or {}).get("set") or set())
        prev_set = set((reference_bucket or {}).get("set") or set())
        added = len(curr_set - prev_set)
        removed = len(prev_set - curr_set)
        same = len(curr_set & prev_set)
        order_changed = bool((curr_by_route.get(rid) or {}).get("seq") != (reference_bucket or {}).get("seq")) if prev_date_text and reference_bucket else False
        prev_load = _safe_float((reference_bucket or {}).get("load"), 0.0)
        delta_load = load - prev_load
        delta_load_ratio = None if prev_load == 0 else (delta_load / prev_load)
        curr_dist = current_snapshot.get("distance")
        prev_dist = reference_snapshot.get("distance")
        dist_delta = None if curr_dist is None or prev_dist is None else (curr_dist - prev_dist)
        dist_delta_ratio = None if dist_delta is None or prev_dist == 0 else (dist_delta / prev_dist)

        routes.append(
            {
                "route_id": rid,
                "route_load": round(load, 3),
                "load_p90": (round(p90, 6) if p90 > 0 else None),
                "load_vs_p90": (round(load / p90, 6) if p90 > 0 else None),
                "load_status": load_status,
                "reuse_type": reuse,
                "decision_hint": decision_hint,
                "explanation_text": explanation_text,
                "review_level": review_level,
                "reference": {
                    "reference_date": prev_date_text or "",
                    "reference_type": "PREV_COMPARABLE_DAY",
                    "reference_label": ("鍓嶄竴鍚岀被鏃? if prev_date_text else "鏃犲墠涓€鍚岀被鏃?),
                },
                "current_snapshot": current_snapshot,
                "reference_snapshot": reference_snapshot,
                "diff_summary": {
                    "added_store_count": int(added),
                    "removed_store_count": int(removed),
                    "same_store_count": int(same),
                    "order_changed": bool(order_changed),
                    "route_load_delta": round(delta_load, 6),
                    "route_load_delta_ratio": (round(delta_load_ratio, 6) if delta_load_ratio is not None else None),
                    "distance_delta": (round(dist_delta, 6) if dist_delta is not None else None),
                    "distance_delta_ratio": (round(dist_delta_ratio, 6) if dist_delta_ratio is not None else None),
                },
            }
        )

    day_summary = {
        "delivery_date": date_text,
        "day_type": day_type,
        "route_count": len(routes),
        "exact_copy_count": exact_copy_count,
        "changed_count": changed_count,
        "stable_count": stable_count,
        "risk_count": risk_count,
        "review_count": review_count,
    }
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"daySummary": day_summary, "routes": routes}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _rengong_build_single_route_portrait(single_routes):
    """瀵瑰崟閰嶇嚎璺仛鐢诲儚缁熻锛堜粎缁忛獙椤靛垎鏋愬睍绀猴紝涓嶈惤搴擄級銆?""
    categories = {
        "鏈哄満": [],
        "楂橀搧/杞︾珯": [],
        "鍦伴搧": [],
        "鍏朵粬": [],
    }

    for item in (single_routes or []):
        route_name = str((item or {}).get("route_name") or "").strip()
        store_count = int(_safe_float((item or {}).get("store_count"), 0))
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if store_count <= 0:
            continue
        if "鏈哄満" in route_name:
            categories["鏈哄満"].append(store_count)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif ("楂橀搧" in route_name) or ("鍗楃珯" in route_name) or ("鍖楃珯" in route_name):
            categories["楂橀搧/杞︾珯"].append(store_count)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif "鍦伴搧" in route_name:
            categories["鍦伴搧"].append(store_count)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        else:
            categories["鍏朵粬"].append(store_count)

    category_rows = []
    for category_name in ("鏈哄満", "楂橀搧/杞︾珯", "鍦伴搧", "鍏朵粬"):
        values = categories.get(category_name) or []
        category_rows.append(
            {
                "category": category_name,
                "count": len(values),
                "avg_store_count": round((sum(values) / len(values)), 2) if values else 0.0,
                "max_store_count": max(values) if values else 0,
                "min_store_count": min(values) if values else 0,
            }
        )

    all_counts = [int(_safe_float((x or {}).get("store_count"), 0)) for x in (single_routes or [])]
    all_counts = [x for x in all_counts if x > 0]
    distribution = {
        "single_store_routes": sum(1 for x in all_counts if x == 1),
        "small_routes": sum(1 for x in all_counts if x <= 3),
        "medium_routes": sum(1 for x in all_counts if 4 <= x <= 6),
        "large_routes": sum(1 for x in all_counts if x >= 7),
        "total_routes": len(all_counts),
    }

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "categories": category_rows,
        "distribution": distribution,
    }


def _rengong_build_human_structure_template(
    store_group_stats,
    store_groups,
    single_route_stats,
    single_route_portrait,
    total_vehicle_count,
):
    """缁忛獙妯″潡锛氫汉宸ョ粨鏋勬ā鏉挎彁鍙栵紙绾仛鍚堬紝涓嶈緭鍑哄叿浣撹溅鐗岀粦瀹氭柟妗堬級銆?""

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    def _distribution(values):
        buckets = {}
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for raw in (values or []):
            v = int(_safe_float(raw, 0))
            key = str(v)
            buckets[key] = buckets.get(key, 0) + 1
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return [
            {"value": int(k), "count": int(buckets[k])}
            for k in sorted(buckets.keys(), key=lambda x: int(_safe_float(x, 0)))
        ]

    groups = list(store_groups or [])
    first_counts = [int(_safe_float(item.get("first_store_count"), 0)) for item in groups]
    second_counts = [int(_safe_float(item.get("second_store_count"), 0)) for item in groups]
    overlap_rates = [float(item.get("jaccard") or 0.0) for item in groups]
    second_extra_ratios = [float(item.get("second_extra_ratio") or 0.0) for item in groups]
    pair_count = len(groups)

    overlap_rate_distribution = [
        {
            "range": "0%~20%",
            "count": sum(1 for x in overlap_rates if 0.0 <= x < 0.2),
        },
        {
            "range": "20%~40%",
            "count": sum(1 for x in overlap_rates if 0.2 <= x < 0.4),
        },
        {
            "range": "40%~60%",
            "count": sum(1 for x in overlap_rates if 0.4 <= x < 0.6),
        },
        {
            "range": "60%~80%",
            "count": sum(1 for x in overlap_rates if 0.6 <= x < 0.8),
        },
        {
            "range": "80%~100%",
            "count": sum(1 for x in overlap_rates if 0.8 <= x <= 1.0),
        },
    ]

    portrait = single_route_portrait or {}
    categories = portrait.get("categories") or []
    distribution = portrait.get("distribution") or {}
    total_single_routes = int(_safe_float((single_route_stats or {}).get("total_routes"), 0))
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if total_single_routes <= 0:
        total_single_routes = int(_safe_float(distribution.get("total_routes"), 0))

    category_ratios = []
    for item in categories:
        count = int(_safe_float(item.get("count"), 0))
        ratio = (count / total_single_routes) if total_single_routes else 0.0
        category_ratios.append(
            {
                "category": str(item.get("category") or "鍏朵粬"),
                "count": count,
                "ratio": round(ratio, 4),
                "avg_store_count": float(item.get("avg_store_count") or 0.0),
            }
        )

    double_group_count = int(_safe_float((store_group_stats or {}).get("total_groups"), 0))
    single_route_count = total_single_routes
    total_vehicles = max(0, int(_safe_float(total_vehicle_count, 0)))
    total_tasks = double_group_count + single_route_count

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if total_tasks > 0 and total_vehicles > 0:
        suggested_double = int(round(total_vehicles * (double_group_count / total_tasks)))
        suggested_single = total_vehicles - suggested_double
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if double_group_count > 0 and suggested_double <= 0:
            suggested_double = 1
            suggested_single = max(0, total_vehicles - suggested_double)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if single_route_count > 0 and suggested_single <= 0:
            suggested_single = 1
            suggested_double = max(0, total_vehicles - suggested_single)
    else:
        suggested_double = 0
        suggested_single = 0

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "doubleTemplate": {
            "avg_first_store_count": round((sum(first_counts) / pair_count), 2) if pair_count else 0.0,
            "avg_second_store_count": round((sum(second_counts) / pair_count), 2) if pair_count else 0.0,
            "avg_second_extra_ratio": round((sum(second_extra_ratios) / pair_count), 4) if pair_count else 0.0,
            "first_store_count_distribution": _distribution(first_counts),
            "second_store_count_distribution": _distribution(second_counts),
            "overlap_rate_distribution": overlap_rate_distribution,
        },
        "singleTemplate": {
            "category_share": category_ratios,
            "size_distribution": {
                "single_store_routes": int(_safe_float(distribution.get("single_store_routes"), 0)),
                "small_routes": int(_safe_float(distribution.get("small_routes"), 0)),
                "medium_routes": int(_safe_float(distribution.get("medium_routes"), 0)),
                "large_routes": int(_safe_float(distribution.get("large_routes"), 0)),
                "total_routes": int(_safe_float(distribution.get("total_routes"), total_single_routes)),
            },
        },
        "vehicleUsageTemplate": {
            "double_group_count": double_group_count,
            "single_route_count": single_route_count,
            "total_vehicle_count": total_vehicles,
            "suggested_vehicle_for_double_tasks": suggested_double,
            "suggested_vehicle_for_single_tasks": suggested_single,
        },
    }


def _ensure_rengong_template_tables():
    """缁忛獙妯″潡钀藉簱琛細鍙岄厤缁撴瀯銆佸崟閰嶇粨鏋勩€佺粺璁℃ā鏉裤€?""
    global _rengong_template_tables_ready
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if _rengong_template_tables_ready:
        return
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RENGONG_STORE_GROUP_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    delivery_date DATE NOT NULL,
                    vehicle_id VARCHAR(50) NOT NULL,
                    base_route_id INT NOT NULL,
                    first_route_id INT NOT NULL,
                    second_route_id INT NOT NULL,
                    first_store_ids JSON NOT NULL,
                    second_store_ids JSON NOT NULL,
                    core_store_ids JSON NOT NULL,
                    extra_store_ids JSON NOT NULL,
                    first_store_count INT NOT NULL DEFAULT 0,
                    second_store_count INT NOT NULL DEFAULT 0,
                    core_store_count INT NOT NULL DEFAULT 0,
                    extra_store_count INT NOT NULL DEFAULT 0,
                    overlap_ratio DECIMAL(5,2) NOT NULL DEFAULT 0,
                    extra_ratio DECIMAL(5,2) NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_rsg_date (delivery_date),
                    INDEX idx_rsg_vehicle (vehicle_id),
                    INDEX idx_rsg_base_route (base_route_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RENGONG_SINGLE_ROUTE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    delivery_date DATE NOT NULL,
                    vehicle_id VARCHAR(50) NOT NULL,
                    route_id INT NOT NULL,
                    route_name VARCHAR(100) NULL,
                    store_ids JSON NOT NULL,
                    store_count INT NOT NULL DEFAULT 0,
                    single_type VARCHAR(20) NOT NULL DEFAULT 'other',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_rsr_date (delivery_date),
                    INDEX idx_rsr_vehicle (vehicle_id),
                    INDEX idx_rsr_route (route_id),
                    INDEX idx_rsr_type (single_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RENGONG_TEMPLATE_TABLE} (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    delivery_date DATE NOT NULL,
                    day_type VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
                    group_count INT NOT NULL DEFAULT 0,
                    avg_first_store_count DECIMAL(5,2) NOT NULL DEFAULT 0,
                    avg_second_store_count DECIMAL(5,2) NOT NULL DEFAULT 0,
                    avg_extra_ratio DECIMAL(5,2) NOT NULL DEFAULT 0,
                    avg_overlap_ratio DECIMAL(5,2) NOT NULL DEFAULT 0,
                    single_route_count INT NOT NULL DEFAULT 0,
                    airport_count INT NOT NULL DEFAULT 0,
                    station_count INT NOT NULL DEFAULT 0,
                    metro_count INT NOT NULL DEFAULT 0,
                    other_count INT NOT NULL DEFAULT 0,
                    single_store_count INT NOT NULL DEFAULT 0,
                    small_route_count INT NOT NULL DEFAULT 0,
                    medium_route_count INT NOT NULL DEFAULT 0,
                    large_route_count INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_rt_date (delivery_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # 瀛橀噺搴撳吋瀹癸細鑰佽〃鍙兘涓嶅瓨鍦?day_type锛岃ˉ榻愬嵆鍙紝涓嶅奖鍝嶅叾浠栭€昏緫
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            try:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    f"ALTER TABLE {RENGONG_TEMPLATE_TABLE} "
                    "ADD COLUMN day_type VARCHAR(20) NOT NULL DEFAULT 'NORMAL' AFTER delivery_date"
                )
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            except Exception:
                pass
    _rengong_template_tables_ready = True


def _rengong_single_type_from_route_name(route_name):
    name = str(route_name or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if "鏈哄満" in name:
        return "airport"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if ("楂橀搧" in name) or ("鍗楃珯" in name) or ("鍖楃珯" in name):
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "station"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if "鍦伴搧" in name:
        return "metro"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return "other"


def _rengong_persist_day_templates(delivery_date, store_groups, single_routes, human_structure_template):
    """缁忛獙妯″潡缁撴灉钀藉簱锛堟寜澶╁厛鍒犲悗鎻掞級锛氱粨鏋勫眰 + 妯℃澘灞傘€?""
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not str(delivery_date or "").strip():
        return
    _ensure_rengong_template_tables()
    day_type = "NORMAL"
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    try:
        day_obj = datetime.strptime(str(delivery_date), "%Y-%m-%d").date()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if day_obj.weekday() == 6:  # Python: Monday=0 ... Sunday=6
            day_type = "COLD_ONLY"
    except Exception:
        day_type = "NORMAL"

    double_template = (human_structure_template or {}).get("doubleTemplate") or {}
    single_template = (human_structure_template or {}).get("singleTemplate") or {}
    single_distribution = single_template.get("size_distribution") or {}
    category_share = single_template.get("category_share") or []

    category_count_map = {"airport": 0, "station": 0, "metro": 0, "other": 0}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for item in category_share:
        category = str((item or {}).get("category") or "").strip()
        count = int(_safe_float((item or {}).get("count"), 0))
        if category == "鏈哄満":
            category_count_map["airport"] = count
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif category in ("楂橀搧/杞︾珯", "楂橀搧/鍗楃珯/鍖楃珯"):
            category_count_map["station"] = count
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        elif category == "鍦伴搧":
            category_count_map["metro"] = count
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        elif category == "鍏朵粬":
            category_count_map["other"] = count

    avg_overlap_ratio = 0.0
    groups = list(store_groups or [])
    if groups:
        avg_overlap_ratio = sum(float(item.get("jaccard") or 0.0) for item in groups) / len(groups)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {RENGONG_STORE_GROUP_TABLE} WHERE delivery_date=%s", (delivery_date,))
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(f"DELETE FROM {RENGONG_SINGLE_ROUTE_TABLE} WHERE delivery_date=%s", (delivery_date,))
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(f"DELETE FROM {RENGONG_TEMPLATE_TABLE} WHERE delivery_date=%s", (delivery_date,))

            if groups:
                cursor.executemany(
                    f"""
                    INSERT INTO {RENGONG_STORE_GROUP_TABLE} (
                        delivery_date, vehicle_id, base_route_id, first_route_id, second_route_id,
                        first_store_ids, second_store_ids, core_store_ids, extra_store_ids,
                        first_store_count, second_store_count, core_store_count, extra_store_count,
                        overlap_ratio, extra_ratio
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s
                    )
                    """,
                    [
                        (
                            delivery_date,
                            str(item.get("vehicle_id") or ""),
                            int(_safe_float(item.get("base_route_id"), 0)),
                            int(_safe_float(item.get("first_route_id"), 0)),
                            int(_safe_float(item.get("second_route_id"), 0)),
                            json.dumps(item.get("first_store_ids") or [], ensure_ascii=False),
                            json.dumps(item.get("second_store_ids") or [], ensure_ascii=False),
                            json.dumps(item.get("first_store_ids") or [], ensure_ascii=False),
                            json.dumps(item.get("extra_store_ids") or [], ensure_ascii=False),
                            int(_safe_float(item.get("first_store_count"), 0)),
                            int(_safe_float(item.get("second_store_count"), 0)),
                            int(_safe_float(item.get("core_store_count"), 0)),
                            int(_safe_float(item.get("extra_store_count"), 0)),
                            round(float(item.get("jaccard") or 0.0), 2),
                            round(float(item.get("second_extra_ratio") or 0.0), 2),
                        )
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        for item in groups
                    ],
                )

            singles = list(single_routes or [])
            if singles:
                cursor.executemany(
                    f"""
                    INSERT INTO {RENGONG_SINGLE_ROUTE_TABLE} (
                        delivery_date, vehicle_id, route_id, route_name,
                        store_ids, store_count, single_type
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    """,
                    [
                        (
                            delivery_date,
                            str(item.get("vehicle_id") or ""),
                            int(_safe_float(item.get("route_id"), 0)),
                            str(item.get("route_name") or "")[:100],
                            json.dumps(item.get("store_ids") or [], ensure_ascii=False),
                            int(_safe_float(item.get("store_count"), 0)),
                            _rengong_single_type_from_route_name(item.get("route_name")),
                        )
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        for item in singles
                    ],
                )

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                INSERT INTO {RENGONG_TEMPLATE_TABLE} (
                    delivery_date,
                    day_type,
                    group_count,
                    avg_first_store_count,
                    avg_second_store_count,
                    avg_extra_ratio,
                    avg_overlap_ratio,
                    single_route_count,
                    airport_count,
                    station_count,
                    metro_count,
                    other_count,
                    single_store_count,
                    small_route_count,
                    medium_route_count,
                    large_route_count
                ) VALUES (
                    %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    delivery_date,
                    day_type,
                    int(_safe_float((human_structure_template or {}).get("vehicleUsageTemplate", {}).get("double_group_count"), 0)),
                    round(float(double_template.get("avg_first_store_count") or 0.0), 2),
                    round(float(double_template.get("avg_second_store_count") or 0.0), 2),
                    round(float(double_template.get("avg_second_extra_ratio") or 0.0), 2),
                    round(float(avg_overlap_ratio or 0.0), 2),
                    int(_safe_float(single_distribution.get("total_routes"), 0)),
                    int(_safe_float(category_count_map.get("airport"), 0)),
                    int(_safe_float(category_count_map.get("station"), 0)),
                    int(_safe_float(category_count_map.get("metro"), 0)),
                    int(_safe_float(category_count_map.get("other"), 0)),
                    int(_safe_float(single_distribution.get("single_store_routes"), 0)),
                    int(_safe_float(single_distribution.get("small_routes"), 0)),
                    int(_safe_float(single_distribution.get("medium_routes"), 0)),
                    int(_safe_float(single_distribution.get("large_routes"), 0)),
                ),
            )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def run_rengong_template_batch():
    """
    缁忛獙妯″潡鍏ㄩ噺鎵瑰鐞嗗叆鍙ｏ細
    - 閬嶅巻 human_dispatch_routes 鍏ㄩ儴 delivery_date
    - 閫愬ぉ鎶藉彇鍙岄厤闂ㄥ簵缁勩€佸崟閰嶇嚎璺€佹ā鏉跨粺璁″苟钀藉簱
    - 杈撳嚭閫愬ぉ group/single 缁熻涓庤鐩栫巼 missing/extra 鏍￠獙
    """
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT delivery_date
                FROM human_dispatch_routes
                ORDER BY delivery_date
                """
            )
            date_rows = cursor.fetchall() or []

    dates = [str((row or {}).get("delivery_date") or "").strip() for row in date_rows]
    dates = [d for d in dates if d]

    daily_results = []
    missing_dates = []

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for delivery_date in dates:
        store_group_stats, store_groups = _rengong_extract_store_groups(delivery_date)
        single_route_stats, single_routes = _rengong_extract_single_routes(delivery_date)
        single_route_portrait = _rengong_build_single_route_portrait(single_routes)

        vehicle_set = set()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for item in (store_groups or []):
            vehicle_id = str((item or {}).get("vehicle_id") or "").strip()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if vehicle_id:
                vehicle_set.add(vehicle_id)
        for item in (single_routes or []):
            vehicle_id = str((item or {}).get("vehicle_id") or "").strip()
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if vehicle_id:
                vehicle_set.add(vehicle_id)

        human_structure_template = _rengong_build_human_structure_template(
            store_group_stats=store_group_stats,
            store_groups=store_groups,
            single_route_stats=single_route_stats,
            single_route_portrait=single_route_portrait,
            total_vehicle_count=len(vehicle_set),
        )

        _rengong_persist_day_templates(
            delivery_date=delivery_date,
            store_groups=store_groups,
            single_routes=single_routes,
            human_structure_template=human_structure_template,
        )

        # 瑕嗙洊鐜囨牎楠岋細human_dispatch_routes 褰撳ぉ store_id 鍘婚噸闆嗗悎
        with mysql_connection() as conn:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            with conn.cursor() as cursor:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    """
                    SELECT DISTINCT CAST(store_id AS CHAR) AS sid
                    FROM human_dispatch_routes
                    WHERE delivery_date=%s
                    """,
                    (delivery_date,),
                )
                human_store_set = {
                    str((row or {}).get("sid") or "").strip()
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    for row in (cursor.fetchall() or [])
                    if str((row or {}).get("sid") or "").strip()
                }

        exp_store_set = set()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for group_item in (store_groups or []):
            for sid in (group_item.get("store_ids") or []):
                sid_str = str(sid or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if sid_str:
                    exp_store_set.add(sid_str)
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for single_item in (single_routes or []):
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for sid in (single_item.get("store_ids") or []):
                sid_str = str(sid or "").strip()
                if sid_str:
                    exp_store_set.add(sid_str)

        missing_count = len(human_store_set - exp_store_set)
        extra_count = len(exp_store_set - human_store_set)

        day_result = {
            "delivery_date": delivery_date,
            "group_count": int(_safe_float((store_group_stats or {}).get("total_groups"), 0)),
            "single_count": int(_safe_float((single_route_stats or {}).get("total_routes"), 0)),
            "total_human_stores": len(human_store_set),
            "total_experience_stores": len(exp_store_set),
            "missing_count": missing_count,
            "extra_count": extra_count,
        }
        daily_results.append(day_result)

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if missing_count > 0:
            missing_dates.append(
                {
                    "delivery_date": delivery_date,
                    "missing_count": missing_count,
                }
            )

    return {
        "processed_days": len(dates),
        "daily": daily_results,
        "missing_dates": missing_dates,
        "has_missing_dates": bool(missing_dates),
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def rengong_day_view(query):
    delivery_date = str((query.get("date") or [""])[0] or "").strip()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not delivery_date:
        raise ValueError("date_required")

    q = str((query.get("q") or [""])[0] or "").strip()
    solver_ready = str((query.get("solverReady") or query.get("solver_ready") or ["all"])[0] or "all").strip().lower()

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                """
                SELECT
                  COUNT(*) AS total_rows,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows,
                  SUM(CASE WHEN solver_ready_flag = 0 THEN 1 ELSE 0 END) AS not_ready_rows,
                  COUNT(DISTINCT delivery_date) AS date_count,
                  MIN(delivery_date) AS date_min,
                  MAX(delivery_date) AS date_max,
                  ROUND(SUM(original_load), 6) AS original_load_sum,
                  ROUND(SUM(total_resolved_load), 6) AS resolved_load_sum
                FROM human_dispatch_solver_ready
                """
            )
            overview = cursor.fetchone() or {}

            where_parts = ["h.delivery_date = %s"]
            params = [delivery_date]
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if q:
                like_text = f"%{q}%"
                where_parts.append("(h.shop_code LIKE %s OR h.store_name LIKE %s OR h.source_vehicle_ids LIKE %s OR h.source_route_ids LIKE %s)")
                params.extend([like_text, like_text, like_text, like_text])
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if solver_ready in ("0", "1"):
                where_parts.append("h.solver_ready_flag = %s")
                params.append(int(solver_ready))
            where_clause = " WHERE " + " AND ".join(where_parts)

            _sync_human_dispatch_solver_profile(cursor)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT
                  h.delivery_date,
                  h.shop_code,
                  COALESCE(NULLIF(p.store_name, ''), h.store_name) AS store_name,
                  COALESCE(NULLIF(p.wave_belongs, ''), h.wave_belongs) AS wave_belongs,
                  COALESCE(NULLIF(p.first_wave_time, ''), h.first_wave_time) AS first_wave_time,
                  COALESCE(NULLIF(p.second_wave_time, ''), h.second_wave_time) AS second_wave_time,
                  COALESCE(NULLIF(p.arrival_time_w3, ''), h.arrival_time_w3) AS arrival_time_w3,
                  COALESCE(NULLIF(p.arrival_time_w4, ''), h.arrival_time_w4) AS arrival_time_w4,
                  h.wave1_load,
                  h.wave2_load,
                  h.wave3_load,
                  h.wave4_load,
                  h.original_load,
                  h.total_resolved_load,
                  h.solver_ready_flag,
                  h.source_vehicle_ids,
                  h.source_drivers,
                  h.source_route_ids,
                  h.source_route_names,
                  h.timing_source,
                  h.missing_fields,
                  h.source_record_count,
                  COALESCE(NULLIF(p.district, ''), s.district) AS district,
                  p.trip_count,
                  p.service_minutes,
                  p.difficulty,
                  p.allowed_late_minutes,
                  p.schedule_status,
                  p.plate_no,
                  p.cold_ratio
                FROM human_dispatch_solver_ready h
                LEFT JOIN c_shop_main s ON s.shop_code = h.shop_code
                LEFT JOIN {HUMAN_DISPATCH_SOLVER_PROFILE_TABLE} p ON p.shop_code = h.shop_code
                {where_clause}
                ORDER BY h.shop_code ASC
                """,
                params,
            )
            rows = cursor.fetchall() or []

    multi_rows = []
    single_rows = []
    day_summary = {
        "delivery_date": delivery_date,
        "total_rows": len(rows),
        "ready_rows": sum(1 for row in rows if int(_safe_float(row.get("solver_ready_flag"), 0)) == 1),
        "not_ready_rows": sum(1 for row in rows if int(_safe_float(row.get("solver_ready_flag"), 0)) == 0),
        "original_load_sum": round(sum(_safe_float(row.get("original_load"), 0.0) for row in rows), 6),
        "resolved_load_sum": round(sum(_safe_float(row.get("total_resolved_load"), 0.0) for row in rows), 6),
        "multi_wave1_sum": 0.0,
        "multi_wave2_sum": 0.0,
        "single_resolved_sum": 0.0,
    }
    wave_stats = {}

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in rows:
        normalized = {
            "delivery_date": str(row.get("delivery_date") or ""),
            "shop_code": str(row.get("shop_code") or "").strip(),
            "store_name": str(row.get("store_name") or "").strip(),
            "district": str(row.get("district") or "").strip(),
            "wave_belongs": str(row.get("wave_belongs") or "").strip(),
            "first_wave_time": str(row.get("first_wave_time") or "").strip(),
            "second_wave_time": str(row.get("second_wave_time") or "").strip(),
            "arrival_time_w3": str(row.get("arrival_time_w3") or "").strip(),
            "arrival_time_w4": str(row.get("arrival_time_w4") or "").strip(),
            "wave1_load": _safe_float(row.get("wave1_load"), 0.0),
            "wave2_load": _safe_float(row.get("wave2_load"), 0.0),
            "wave3_load": _safe_float(row.get("wave3_load"), 0.0),
            "wave4_load": _safe_float(row.get("wave4_load"), 0.0),
            "original_load": _safe_float(row.get("original_load"), 0.0),
            "total_resolved_load": _safe_float(row.get("total_resolved_load"), 0.0),
            "solver_ready_flag": int(_safe_float(row.get("solver_ready_flag"), 0)),
            "source_vehicle_ids": str(row.get("source_vehicle_ids") or "").strip(),
            "source_drivers": str(row.get("source_drivers") or "").strip(),
            "source_route_ids": str(row.get("source_route_ids") or "").strip(),
            "source_route_names": str(row.get("source_route_names") or "").strip(),
            "timing_source": str(row.get("timing_source") or "").strip(),
            "missing_fields": str(row.get("missing_fields") or "").strip(),
            "source_record_count": int(_safe_float(row.get("source_record_count"), 0)),
            "trip_count": int(_safe_float(row.get("trip_count"), 1)),
            "unload_minutes": int(_safe_float(row.get("service_minutes"), 15)),
            "difficulty": _safe_float(row.get("difficulty"), 1.0),
            "tolerate_minutes": int(_safe_float(row.get("allowed_late_minutes"), 15)),
            "schedule_status": str(row.get("schedule_status") or "").strip(),
            "plate_no": str(row.get("plate_no") or "").strip(),
            "cold_ratio": _safe_float(row.get("cold_ratio"), 0.0),
        }
        wave_tokens = [token.strip() for token in normalized["wave_belongs"].split(",") if token.strip()]
        normalized["delivery_count"] = max(1, len(wave_tokens)) if wave_tokens else 1
        normalized["expected_time"] = _rengong_expected_time_for_row(normalized)
        normalized["active_wave_load"] = _rengong_active_wave_load_for_row(normalized)

        wave_key = normalized["wave_belongs"] or "鏈槧灏?
        stat = wave_stats.setdefault(
            wave_key,
            {"wave_belongs": wave_key, "row_count": 0, "ready_rows": 0, "original_load_sum": 0.0, "resolved_load_sum": 0.0},
        )
        stat["row_count"] += 1
        stat["ready_rows"] += 1 if normalized["solver_ready_flag"] == 1 else 0
        stat["original_load_sum"] += normalized["original_load"]
        stat["resolved_load_sum"] += normalized["total_resolved_load"]

        day_summary["multi_wave1_sum"] += normalized["wave1_load"]
        day_summary["multi_wave2_sum"] += normalized["wave2_load"]
        if "," in normalized["wave_belongs"]:
            multi_rows.append(normalized)
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        else:
            day_summary["single_resolved_sum"] += normalized["total_resolved_load"]
            single_rows.append(normalized)

    wave_breakdown = []
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for stat in wave_stats.values():
        wave_breakdown.append(
            {
                "wave_belongs": stat["wave_belongs"],
                "row_count": stat["row_count"],
                "ready_rows": stat["ready_rows"],
                "original_load_sum": round(stat["original_load_sum"], 6),
                "resolved_load_sum": round(stat["resolved_load_sum"], 6),
            }
        )
    wave_breakdown.sort(key=lambda item: (0 if item["wave_belongs"] == "2,1" else 1, item["wave_belongs"]))

    day_summary["multi_wave1_sum"] = round(day_summary["multi_wave1_sum"], 6)
    day_summary["multi_wave2_sum"] = round(day_summary["multi_wave2_sum"], 6)
    day_summary["single_resolved_sum"] = round(day_summary["single_resolved_sum"], 6)
    store_group_stats, store_groups = _rengong_extract_store_groups(delivery_date)
    sim_stats, sim_inputs = _rengong_build_dispatch_input_simulation(store_groups)
    single_route_stats, single_routes = _rengong_extract_single_routes(delivery_date)
    single_route_portrait = _rengong_build_single_route_portrait(single_routes)
    vehicle_set = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in rows:
        plate_no = str(row.get("plate_no") or "").strip()
        if plate_no:
            vehicle_set.add(plate_no)
        source_vehicle_ids = str(row.get("source_vehicle_ids") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if source_vehicle_ids:
            for token in re.split(r"[,锛屻€乗s|]+", source_vehicle_ids):
                token = str(token or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if token:
                    vehicle_set.add(token)
    human_structure_template = _rengong_build_human_structure_template(
        store_group_stats=store_group_stats,
        store_groups=store_groups,
        single_route_stats=single_route_stats,
        single_route_portrait=single_route_portrait,
        total_vehicle_count=len(vehicle_set),
    )
    _rengong_persist_day_templates(
        delivery_date=delivery_date,
        store_groups=store_groups,
        single_routes=single_routes,
        human_structure_template=human_structure_template,
    )

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {
        "overview": overview,
        "daySummary": day_summary,
        "waveBreakdown": wave_breakdown,
        "storeGroupStats": store_group_stats,
        "storeGroups": store_groups[:20],
        "dispatchInputSimStats": sim_stats,
        "dispatchInputSimulations": sim_inputs[:20],
        "singleRouteStats": single_route_stats,
        "singleRoutes": single_routes[:20],
        "singleRoutePortrait": single_route_portrait,
        "humanStructureTemplate": human_structure_template,
        "multiDeliveryStores": multi_rows,
        "singleDeliveryStores": single_rows,
    }


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def rengong_summary(query):
    where_clause, params = _rengong_query_filters(query)
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  COUNT(*) AS total_rows,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows,
                  SUM(CASE WHEN solver_ready_flag = 0 THEN 1 ELSE 0 END) AS not_ready_rows,
                  COUNT(DISTINCT delivery_date) AS date_count,
                  MIN(delivery_date) AS date_min,
                  MAX(delivery_date) AS date_max,
                  ROUND(SUM(original_load), 6) AS original_load_sum,
                  ROUND(SUM(total_resolved_load), 6) AS resolved_load_sum
                FROM human_dispatch_solver_ready
                {where_clause}
                """,
                params,
            )
            overview = cursor.fetchone() or {}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"""
                SELECT
                  COALESCE(NULLIF(wave_belongs, ''), '鏈槧灏?) AS wave_belongs,
                  COUNT(*) AS row_count,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows,
                  ROUND(SUM(original_load), 6) AS original_load_sum,
                  ROUND(SUM(total_resolved_load), 6) AS resolved_load_sum
                FROM human_dispatch_solver_ready
                {where_clause}
                GROUP BY COALESCE(NULLIF(wave_belongs, ''), '鏈槧灏?)
                ORDER BY row_count DESC, wave_belongs ASC
                """,
                params,
            )
            wave_breakdown = cursor.fetchall() or []
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT
                  delivery_date,
                  COUNT(*) AS row_count,
                  SUM(CASE WHEN solver_ready_flag = 1 THEN 1 ELSE 0 END) AS ready_rows
                FROM human_dispatch_solver_ready
                {where_clause}
                GROUP BY delivery_date
                ORDER BY delivery_date DESC
                LIMIT 31
                """,
                params,
            )
            recent_dates = cursor.fetchall() or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {
        "overview": overview,
        "waveBreakdown": wave_breakdown,
        "recentDates": recent_dates,
    }


def _overlay_payload_stores_from_resolved_table(payload):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if not isinstance(payload, dict):
        raise ValueError("payload_must_be_dict")

    stores = payload.get("stores")
    if not isinstance(stores, list) or not stores:
        raise ValueError("stores_required")

    wave = payload.get("wave")
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not isinstance(wave, dict):
        raise ValueError("wave_required")

    wave_id = str(wave.get("waveId") or "").strip().upper()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if wave_id == "W1":
        load_field = "wave1_load"
        time_field = "first_wave_time"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    elif wave_id == "W2":
        load_field = "wave2_load"
        time_field = "second_wave_time"
    elif wave_id == "W3":
        load_field = "wave3_load"
        time_field = "arrival_time_w3"
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    elif wave_id == "W4":
        load_field = "wave4_load"
        time_field = "arrival_time_w4"
    else:
        raise ValueError(f"unsupported_wave_id:{wave_id}")

    shop_codes = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for store in stores:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(store, dict):
            raise ValueError("store_item_must_be_dict")
        code = str(store.get("id") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not code:
            raise ValueError("store_id_required")
        shop_codes.append(code)

    result = store_wave_load_resolved_get_batch(shop_codes)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    rows = result.get("items")
    if not isinstance(rows, list):
        raise ValueError("resolved_rows_invalid")

    row_by_code = {}
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for row in rows:
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not isinstance(row, dict):
            raise ValueError("resolved_row_must_be_dict")
        code = str(row.get("shop_code") or "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if not code:
            raise ValueError("resolved_shop_code_required")
        row_by_code[code] = row

    for code in shop_codes:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if code not in row_by_code:
            raise ValueError(f"resolved_row_missing:{code}")

    for store in stores:
        code = str(store.get("id") or "").strip()
        row = row_by_code[code]

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if "wave_belongs" not in row:
            raise ValueError(f"wave_belongs_missing:{code}")
        wave_belongs_value = row.get("wave_belongs")
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if wave_belongs_value is None or str(wave_belongs_value).strip() == "":
            raise ValueError(f"wave_belongs_null:{code}")
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if load_field not in row:
            raise ValueError(f"{load_field}_missing:{code}")
        if time_field not in row:
            raise ValueError(f"{time_field}_missing:{code}")

        load_value = row.get(load_field)
        time_value = row.get(time_field)

        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if load_value is None:
            raise ValueError(f"{load_field}_null:{code}")
        if time_value is None or str(time_value).strip() == "":
            raise ValueError(f"{time_field}_null:{code}")

        store["waveBelongs"] = row.get("wave_belongs")
        store["boxes"] = load_value
        store["first_wave_time"] = row.get("first_wave_time")
        store["second_wave_time"] = row.get("second_wave_time")
        store["arrival_time_w3"] = row.get("arrival_time_w3")
        store["arrival_time_w4"] = row.get("arrival_time_w4")

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return payload


# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def store_wave_load_resolved_save(rows):
    ensure_archive_tables()
    safe_rows = rows if isinstance(rows, list) else []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS store_wave_load_resolved (
                  shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
                  wave_belongs VARCHAR(32) NULL,
                  wave1_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave2_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave3_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  wave4_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  total_resolved_load DECIMAL(14,6) NOT NULL DEFAULT 0,
                  first_wave_time VARCHAR(16) NULL,
                  second_wave_time VARCHAR(16) NULL,
                  arrival_time_w3 VARCHAR(16) NULL,
                  arrival_time_w4 VARCHAR(16) NULL,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  KEY idx_swlr_wave_belongs (wave_belongs)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cursor.execute("SHOW COLUMNS FROM store_wave_load_resolved")
            cols = {str((r or {}).get("Field") or "") for r in (cursor.fetchall() or [])}
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "first_wave_time" not in cols:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN first_wave_time VARCHAR(16) NULL AFTER total_resolved_load")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "second_wave_time" not in cols:
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN second_wave_time VARCHAR(16) NULL AFTER first_wave_time")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if "arrival_time_w3" not in cols:
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN arrival_time_w3 VARCHAR(16) NULL AFTER second_wave_time")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if "arrival_time_w4" not in cols:
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute("ALTER TABLE store_wave_load_resolved ADD COLUMN arrival_time_w4 VARCHAR(16) NULL AFTER arrival_time_w3")
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute("TRUNCATE TABLE store_wave_load_resolved")
            sql = """
                INSERT INTO store_wave_load_resolved (
                  shop_code, wave_belongs,
                  wave1_load, wave2_load, wave3_load, wave4_load,
                  total_resolved_load, first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            upserted = 0
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            for item in safe_rows:
                shop_code = str((item or {}).get("shop_code") or "").strip()
                if not shop_code:
                    continue
                wave_belongs = str((item or {}).get("wave_belongs") or "").strip()
                wave1_load = float((item or {}).get("wave1_load") or 0)
                wave2_load = float((item or {}).get("wave2_load") or 0)
                wave3_load = float((item or {}).get("wave3_load") or 0)
                wave4_load = float((item or {}).get("wave4_load") or 0)
                total_resolved_load = float((item or {}).get("total_resolved_load") or 0)
                first_wave_time = str((item or {}).get("first_wave_time") or "").strip() or None
                second_wave_time = str((item or {}).get("second_wave_time") or "").strip() or None
                arrival_time_w3 = str((item or {}).get("arrival_time_w3") or "").strip() or None
                arrival_time_w4 = str((item or {}).get("arrival_time_w4") or "").strip() or None
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(
                    sql,
                    (
                        shop_code,
                        wave_belongs,
                        wave1_load,
                        wave2_load,
                        wave3_load,
                        wave4_load,
                        total_resolved_load,
                        first_wave_time,
                        second_wave_time,
                        arrival_time_w3,
                        arrival_time_w4,
                    ),
                )
                upserted += 1
        conn.commit()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"upserted": upserted}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _ensure_store_wave_timing_resolved_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS store_wave_timing_resolved (
          shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
          wave_belongs VARCHAR(32) NULL,
          first_wave_time VARCHAR(16) NULL,
          second_wave_time VARCHAR(16) NULL,
          arrival_time_w3 VARCHAR(16) NULL,
          arrival_time_w4 VARCHAR(16) NULL,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          KEY idx_swtr_wave_belongs (wave_belongs)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def _ensure_human_dispatch_solver_profile_table(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HUMAN_DISPATCH_SOLVER_PROFILE_TABLE} (
          shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
          store_name VARCHAR(255) NULL,
          district VARCHAR(64) NULL,
          wave_belongs VARCHAR(32) NULL,
          first_wave_time VARCHAR(16) NULL,
          second_wave_time VARCHAR(16) NULL,
          arrival_time_w3 VARCHAR(16) NULL,
          arrival_time_w4 VARCHAR(16) NULL,
          trip_count INT NOT NULL DEFAULT 1,
          service_minutes INT NOT NULL DEFAULT 15,
          difficulty DECIMAL(8,3) NOT NULL DEFAULT 1.000,
          allowed_late_minutes INT NOT NULL DEFAULT 15,
          schedule_status VARCHAR(32) NULL,
          plate_no VARCHAR(64) NULL,
          cold_ratio DECIMAL(10,6) NOT NULL DEFAULT 0,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          KEY idx_hdsp_wave_belongs (wave_belongs),
          KEY idx_hdsp_district (district),
          KEY idx_hdsp_updated_at (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _sync_human_dispatch_solver_profile(cursor):
    _ensure_store_wave_timing_resolved_table(cursor)
    _ensure_human_dispatch_solver_profile_table(cursor)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    cursor.execute(
        f"""
        INSERT INTO {HUMAN_DISPATCH_SOLVER_PROFILE_TABLE} (
          shop_code, store_name, district, wave_belongs,
          first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4,
          trip_count, service_minutes, difficulty, allowed_late_minutes,
          schedule_status, plate_no, cold_ratio
        )
        SELECT
          r.shop_code,
          NULLIF(TRIM(s.shop_name), '') AS store_name,
          NULLIF(TRIM(s.district), '') AS district,
          COALESCE(NULLIF(TRIM(t.wave_belongs), ''), NULLIF(TRIM(r.wave_belongs), '')) AS wave_belongs,
          COALESCE(NULLIF(TRIM(t.first_wave_time), ''), NULLIF(TRIM(r.first_wave_time), '')) AS first_wave_time,
          COALESCE(NULLIF(TRIM(t.second_wave_time), ''), NULLIF(TRIM(r.second_wave_time), '')) AS second_wave_time,
          COALESCE(NULLIF(TRIM(t.arrival_time_w3), ''), NULLIF(TRIM(r.arrival_time_w3), '')) AS arrival_time_w3,
          COALESCE(NULLIF(TRIM(t.arrival_time_w4), ''), NULLIF(TRIM(r.arrival_time_w4), '')) AS arrival_time_w4,
          GREATEST(1, CAST(COALESCE(s.trip_count, 1) AS SIGNED)) AS trip_count,
          GREATEST(1, CAST(COALESCE(s.service_minutes, 15) AS SIGNED)) AS service_minutes,
          CAST(COALESCE(s.difficulty, 1) AS DECIMAL(8,3)) AS difficulty,
          GREATEST(0, CAST(COALESCE(s.allowed_late_minutes, 15) AS SIGNED)) AS allowed_late_minutes,
          NULLIF(TRIM(s.schedule_status), '') AS schedule_status,
          NULLIF(TRIM(s.plate_no), '') AS plate_no,
          CAST(COALESCE(s.cold_ratio, 0) AS DECIMAL(10,6)) AS cold_ratio
        FROM store_wave_load_resolved r
        LEFT JOIN c_shop_main s ON s.shop_code = r.shop_code
        LEFT JOIN store_wave_timing_resolved t ON t.shop_code = r.shop_code
        ON DUPLICATE KEY UPDATE
          store_name = VALUES(store_name),
          district = VALUES(district),
          wave_belongs = VALUES(wave_belongs),
          first_wave_time = VALUES(first_wave_time),
          second_wave_time = VALUES(second_wave_time),
          arrival_time_w3 = VALUES(arrival_time_w3),
          arrival_time_w4 = VALUES(arrival_time_w4),
          trip_count = VALUES(trip_count),
          service_minutes = VALUES(service_minutes),
          difficulty = VALUES(difficulty),
          allowed_late_minutes = VALUES(allowed_late_minutes),
          schedule_status = VALUES(schedule_status),
          plate_no = VALUES(plate_no),
          cold_ratio = VALUES(cold_ratio)
        """
    )


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _normalize_wave_id_for_timing(value):
    text = str(value or "").strip().upper()
    if text in ("1", "W1"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "1"
    if text in ("2", "W2"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "2"
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if text in ("3", "W3"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return "3"
    if text in ("4", "W4"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        return "4"
    raise ValueError("wave_id_must_be_1_2_3_4_or_w1_w2_w3_w4")


def _merge_wave_belongs_text(old_value, wave_no):
    exists = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    for token in str(old_value or "").split(","):
        t = token.strip()
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if t in ("1", "2", "3", "4") and t not in exists:
            exists.append(t)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if wave_no not in exists:
        exists.append(wave_no)
    return ",".join(exists)


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
# EN: Backend control point for this logic branch.
# CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
def store_wave_timing_resolved_list(shop_code="", wave_belongs="", limit=200):
    shop_code = str(shop_code or "").strip()
    wave_belongs = str(wave_belongs or "").strip()
    try:
        limit = int(limit)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    except Exception:
        limit = 200
    limit = max(1, min(limit, 5000))
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        with conn.cursor() as cursor:
            _ensure_store_wave_timing_resolved_table(cursor)
            where_sql = " WHERE 1=1 "
            params = []
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if shop_code:
                where_sql += " AND shop_code = %s "
                params.append(shop_code)
            if wave_belongs:
                wave_no = _normalize_wave_id_for_timing(wave_belongs)
                where_sql += " AND FIND_IN_SET(%s, wave_belongs) > 0 "
                params.append(wave_no)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM store_wave_timing_resolved {where_sql}",
                params,
            )
            total = int((cursor.fetchone() or {}).get("cnt") or 0)
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            cursor.execute(
                f"""
                SELECT
                  shop_code, wave_belongs,
                  first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4,
                  updated_at
                FROM store_wave_timing_resolved
                {where_sql}
                ORDER BY shop_code
                LIMIT %s
                """,
                [*params, limit],
            )
            items = cursor.fetchall() or []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"items": items, "count": len(items), "total": total, "limit": limit}


def store_wave_timing_resolved_save_wave(wave, rows):
    wave_no = _normalize_wave_id_for_timing(wave)
    safe_rows = rows if isinstance(rows, list) else []
    time_field = {
        "1": "first_wave_time",
        "2": "second_wave_time",
        "3": "arrival_time_w3",
        "4": "arrival_time_w4",
    }[wave_no]
    upserted = 0
    ignored = 0
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            _ensure_store_wave_timing_resolved_table(cursor)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            for item in safe_rows:
                shop_code = str((item or {}).get("shop_code") or (item or {}).get("shopCode") or "").strip()
                time_text = str((item or {}).get("time") or (item or {}).get(time_field) or "").strip()
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not shop_code or not time_text:
                    ignored += 1
                    continue
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                cursor.execute(
                    """
                    SELECT wave_belongs, first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4
                    FROM store_wave_timing_resolved
                    WHERE shop_code = %s
                    LIMIT 1
                    """,
                    (shop_code,),
                )
                existing = cursor.fetchone() or {}
                merged_wave_belongs = _merge_wave_belongs_text(existing.get("wave_belongs"), wave_no)
                first_wave_time = existing.get("first_wave_time")
                second_wave_time = existing.get("second_wave_time")
                arrival_time_w3 = existing.get("arrival_time_w3")
                arrival_time_w4 = existing.get("arrival_time_w4")
                if wave_no == "1":
                    first_wave_time = time_text
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                elif wave_no == "2":
                    second_wave_time = time_text
                elif wave_no == "3":
                    arrival_time_w3 = time_text
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                else:
                    arrival_time_w4 = time_text
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                cursor.execute(
                    """
                    INSERT INTO store_wave_timing_resolved (
                      shop_code, wave_belongs, first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      wave_belongs = VALUES(wave_belongs),
                      first_wave_time = VALUES(first_wave_time),
                      second_wave_time = VALUES(second_wave_time),
                      arrival_time_w3 = VALUES(arrival_time_w3),
                      arrival_time_w4 = VALUES(arrival_time_w4)
                    """,
                    (
                        shop_code,
                        merged_wave_belongs,
                        first_wave_time,
                        second_wave_time,
                        arrival_time_w3,
                        arrival_time_w4,
                    ),
                )
                upserted += 1
        conn.commit()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return {"wave": wave_no, "upserted": upserted, "ignored": ignored}


def _extract_vehicles_from_rows(rows):
    vehicles = []
    seen = set()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for row in rows:
        plate = str(row.get("plateNo") or "").strip()
        driver = str(row.get("driverName") or "").strip()
        if not plate:
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if plate in seen:
            continue
        seen.add(plate)
        vehicles.append(
            {
                "plateNo": plate,
                "driverName": driver,
                "type": DEFAULT_VEHICLE_TYPE,
                "capacity": 100,
                "speed": 38,
                "canCarryCold": False,
            }
        )
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return vehicles


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
def _parse_vehicle_text(raw_text):
    rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for line in str(raw_text or "").replace("\u0000", "").splitlines():
        s = line.strip()
        if not s:
            continue
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if s.startswith("#"):
            continue
        parts = re.split(r"[\t,閿?閿涙矐s]+", s)
        parts = [p.strip() for p in parts if p and p.strip()]
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if not parts:
            continue
        plate = parts[0]
        driver = parts[1] if len(parts) > 1 else ""
        rows.append({"plateNo": plate, "driverName": driver})
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    return _extract_vehicles_from_rows(rows)


def _parse_vehicle_excel(binary):
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if pd is None:
        raise RuntimeError("pandas_not_installed")
    df = pd.read_excel(io.BytesIO(binary), sheet_name=0)
    if df is None or df.empty:
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        return []
    cols = list(df.columns)
    norm_map = {_normalize_col_name(c): c for c in cols}

    plate_col = None
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for key in ("杞﹀彿", "杞︾墝鍙?, "杞︾墝", "plate", "plateno", "vehicleno"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if key in norm_map:
            plate_col = norm_map[key]
            break
    driver_col = None
    for key in ("鍙告満", "鍙告満鍚?, "鍙告満濮撳悕", "driver", "drivername"):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        if key in norm_map:
            driver_col = norm_map[key]
            break

    rows = []
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if plate_col is None:
        # fallback: first column plate, second column driver
        plate_col = cols[0]
        driver_col = cols[1] if len(cols) > 1 else None

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    for _, r in df.iterrows():
        plate = str(r.get(plate_col) if plate_col is not None else "").strip()
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if plate.lower() in ("nan", "none"):
            plate = ""
        driver = str(r.get(driver_col) if driver_col is not None else "").strip()
        if driver.lower() in ("nan", "none"):
            driver = ""
        rows.append({"plateNo": plate, "driverName": driver})
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return _extract_vehicles_from_rows(rows)


def parse_vehicle_file(payload):
    filename = str(payload.get("fileName") or "").strip()
    content_b64 = str(payload.get("contentBase64") or "").strip()
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    if not content_b64:
        raise ValueError("missing_file_content")
    binary = base64.b64decode(content_b64)
    lower = filename.lower()
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        vehicles = _parse_vehicle_excel(binary)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    elif lower.endswith(".csv"):
        text = binary.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        rows = []
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        for row in reader:
            if not row:
                continue
            plate = str(row[0] if len(row) > 0 else "").strip()
            driver = str(row[1] if len(row) > 1 else "").strip()
            rows.append({"plateNo": plate, "driverName": driver})
        vehicles = _extract_vehicles_from_rows(rows)
    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    else:
        text = binary.decode("utf-8", errors="ignore")
        vehicles = _parse_vehicle_text(text)
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    return {"vehicles": vehicles, "count": len(vehicles)}


# EN: Key backend step in this flow.
# CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        # EN: Backend control point for this logic branch.
        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
        def _json_default(value):
            if isinstance(value, Decimal):
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if value == value.to_integral_value():
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    return int(value)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                return float(value)
            return str(value)

        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.end_headers()
        self.wfile.write(body)

    # EN: Key backend step in this flow.
    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def do_OPTIONS(self):
        self._send(204, {})

    def do_GET(self):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            query = parse_qs(parsed.query)

            # 闈欐€佹枃浠剁洿鍑猴細鐢ㄤ簬 /index.html銆?dengtang.html銆?dengtang.js銆?dengtang.css 绛夊墠绔〉闈㈣祫婧?
            static_path = "/index.html" if path == "/" else path
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if static_path.startswith("/"):
                rel_path = static_path.lstrip("/")
                abs_path = os.path.abspath(os.path.join(PROJECT_ROOT, rel_path))
                project_root_abs = os.path.abspath(PROJECT_ROOT)
                if abs_path.startswith(project_root_abs + os.sep) or abs_path == project_root_abs:
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    if os.path.isfile(abs_path):
                        ctype, _ = mimetypes.guess_type(abs_path)
                        if not ctype:
                            ctype = "application/octet-stream"
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        with open(abs_path, "rb") as fp:
                            content = fp.read()
                        self.send_response(200)
                        self.send_header("Content-Type", ctype)
                        self.send_header("Content-Length", str(len(content)))
                        self.send_header("Access-Control-Allow-Origin", "*")
                        self.send_header("Access-Control-Allow-Headers", "Content-Type")
                        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
                        self.send_header("Access-Control-Allow-Private-Network", "true")
                        self.end_headers()
                        self.wfile.write(content)
                        return

            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/health":
                mysql_ok = False
                mysql_error = ""
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                try:
                    ensure_archive_tables()
                    mysql_ok = True
                except Exception as exc:
                    mysql_error = str(exc)
                self._send(
                    200,
                    {
                        "ok": True,
                        "service": "ga-backend",
                        "port": PORT,
                        "mysql": {"ok": mysql_ok, "database": MYSQL_DATABASE, "error": mysql_error},
                    },
                )
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/distance-matrix/full":
                store_ids_param = query.get("storeIds") or query.get("store_ids")
                store_ids = None
                if store_ids_param and len(store_ids_param):
                    store_ids = str(store_ids_param[0]).split(",")
                include_duration = _to_bool_safe((query.get("includeDuration") or ["true"])[0], True)
                strict = _to_bool_safe((query.get("strict") or ["false"])[0], False)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = _get_distance_matrix_full(
                    {"storeIds": store_ids, "includeDuration": include_duration, "strict": strict}
                )
                self._send(200, result)
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/recommended-plans/list":
                task_date = (query.get("taskDate") or query.get("task_date") or [""])[0]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = recommended_plan_list({"taskDate": task_date})
                self._send(200, {"ok": True, **result})
                return
            if path == "/recommended-plans/current":
                task_date = (query.get("taskDate") or query.get("task_date") or [""])[0]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = recommended_plan_current({"taskDate": task_date})
                self._send(200, {"ok": True, **result})
                return
            if path == "/run-regions/list":
                scheme_no = (query.get("schemeNo") or query.get("scheme_no") or [""])[0]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_regions_list(scheme_no)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/run-region-schemes/list":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_region_schemes_list()
                self._send(200, {"ok": True, **result})
                return
            if path == "/stores/points":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = store_points_list()
                self._send(200, {"ok": True, **result})
                return
            if path == "/wms/status":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_status()
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/wms/stores":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_stores_list()
                self._send(200, {"ok": True, **result})
                return
            if path == "/wms/vehicles":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = wms_vehicles_list()
                self._send(200, {"ok": True, **result})
                return
            if path == "/wms/cargo-latest":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_cargo_latest()
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/wms/cargo-raw/list":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                with mysql_connection() as conn:
                    with conn.cursor() as cursor:
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        cursor.execute(
                            f"""
                            SELECT
                              shop_code, business_date, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper,
                              source_table, row_hash, created_at, updated_at
                            FROM {WMS_CARGO_RAW_TABLE}
                            ORDER BY id DESC
                            LIMIT 500
                            """
                        )
                        rows = cursor.fetchall() or []
                self._send(200, {"ok": True, "items": rows, "count": len(rows)})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/clean-cargo-raw/list":
                shop_code = (query.get("shopCode") or query.get("shop_code") or [""])[0]
                limit = (query.get("limit") or ["500"])[0]
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = clean_cargo_raw_list(shop_code=shop_code, limit=limit)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/store-wave-load-resolved/list":
                shop_code = (query.get("shopCode") or query.get("shop_code") or [""])[0]
                wave_belongs = (query.get("waveBelongs") or query.get("wave_belongs") or [""])[0]
                limit = (query.get("limit") or ["200"])[0]
                result = store_wave_load_resolved_list(shop_code=shop_code, wave_belongs=wave_belongs, limit=limit)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/store-wave-load-resolved/item":
                shop_code = (query.get("shopCode") or query.get("shop_code") or [""])[0]
                result = store_wave_load_resolved_get_one(shop_code=shop_code)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/store-wave-timing-resolved/list":
                shop_code = (query.get("shopCode") or query.get("shop_code") or [""])[0]
                wave_belongs = (query.get("waveBelongs") or query.get("wave_belongs") or [""])[0]
                limit = (query.get("limit") or ["200"])[0]
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = store_wave_timing_resolved_list(shop_code=shop_code, wave_belongs=wave_belongs, limit=limit)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/rengong/summary":
                result = rengong_summary(query)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/rengong/list":
                result = rengong_list(query)
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/rengong/day-view":
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = rengong_day_view(query)
                # 缁忛獙椤靛弻閰嶉棬搴楃粍鍏滃簳锛氳嫢鑰佺増鏈繑鍥炰綋缂哄瓧娈碉紝杩欓噷琛ョ畻涓€娆★紝閬垮厤鍓嶇鏄剧ず 0 缁?
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if "storeGroupStats" not in result or "storeGroups" not in result:
                    _delivery_date = str((query.get("date") or [""])[0] or "").strip()
                    if _delivery_date:
                        _stats, _groups = _rengong_extract_store_groups(_delivery_date)
                        result["storeGroupStats"] = _stats
                        result["storeGroups"] = _groups[:20]
                        _sim_stats, _sim_inputs = _rengong_build_dispatch_input_simulation(_groups)
                        result["dispatchInputSimStats"] = _sim_stats
                        result["dispatchInputSimulations"] = _sim_inputs[:20]
                        _single_stats, _single_routes = _rengong_extract_single_routes(_delivery_date)
                        result["singleRouteStats"] = _single_stats
                        result["singleRoutes"] = _single_routes[:20]
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                elif "dispatchInputSimStats" not in result or "dispatchInputSimulations" not in result:
                    _sim_stats, _sim_inputs = _rengong_build_dispatch_input_simulation(result.get("storeGroups") or [])
                    result["dispatchInputSimStats"] = _sim_stats
                    result["dispatchInputSimulations"] = _sim_inputs[:20]
                if "singleRouteStats" not in result or "singleRoutes" not in result:
                    _delivery_date = str((query.get("date") or [""])[0] or "").strip()
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if _delivery_date:
                        _single_stats, _single_routes = _rengong_extract_single_routes(_delivery_date)
                        result["singleRouteStats"] = _single_stats
                        result["singleRoutes"] = _single_routes[:20]
                        result["singleRoutePortrait"] = _rengong_build_single_route_portrait(_single_routes)
                _single_src = result.get("singleRoutes") or []
                _portrait = result.get("singleRoutePortrait")
                _need_rebuild_portrait = not isinstance(_portrait, dict)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if not _need_rebuild_portrait:
                    _dist = _portrait.get("distribution") if isinstance(_portrait, dict) else None
                    _total_routes = 0
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    if isinstance(_dist, dict):
                        _total_routes = int(_safe_float(_dist.get("total_routes"), 0))
                    # 鍙褰撳ぉ瀛樺湪鍗曢厤绾胯矾锛岀敾鍍忓氨涓嶅簲涓虹┖锛涗负绌烘椂閲嶇畻涓€娆?
                    if _single_src and _total_routes <= 0:
                        _need_rebuild_portrait = True
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if _need_rebuild_portrait:
                    result["singleRoutePortrait"] = _rengong_build_single_route_portrait(_single_src)
                if "humanStructureTemplate" not in result:
                    _sg_stats = result.get("storeGroupStats") or {}
                    _sg = result.get("storeGroups") or []
                    _sr_stats = result.get("singleRouteStats") or {}
                    _sr_portrait = result.get("singleRoutePortrait") or {}
                    _mv = result.get("multiDeliveryStores") or []
                    _sv = result.get("singleDeliveryStores") or []
                    _vehicle_set = set()
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    for _row in (_mv + _sv):
                        _plate = str((_row or {}).get("plate_no") or "").strip()
                        # EN: Backend control point for this logic branch.
                        # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                        if _plate:
                            _vehicle_set.add(_plate)
                        _src_vehicles = str((_row or {}).get("source_vehicle_ids") or "").strip()
                        # EN: Key backend step in this flow.
                        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                        if _src_vehicles:
                            for _token in re.split(r"[,锛屻€乗s|]+", _src_vehicles):
                                _token = str(_token or "").strip()
                                # EN: Key backend step in this flow.
                                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                                # EN: Backend control point for this logic branch.
                                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                                if _token:
                                    _vehicle_set.add(_token)
                    result["humanStructureTemplate"] = _rengong_build_human_structure_template(
                        store_group_stats=_sg_stats,
                        store_groups=_sg,
                        single_route_stats=_sr_stats,
                        single_route_portrait=_sr_portrait,
                        total_vehicle_count=len(_vehicle_set),
                    )
                self._send(200, {"ok": True, **result})
                return
            if path == "/rengong/boundary-details":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = rengong_boundary_details(query)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/rengong/boundary-page-data":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = rengong_boundary_page_data(query)
                self._send(200, {"ok": True, **result})
                return
            if path == "/rengong/dengtang-explain":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = rengong_dengtang_explain(query)
                self._send(200, {"ok": True, **result})
                return
            if path == "/rengong/route-distance-load-detail":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = rengong_route_distance_load_detail(query)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                if result.get("error"):
                    self._send(400, {"ok": False, **result})
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                else:
                    self._send(200, {"ok": True, **result})
                return
            if path == "/chaos/replay-task-view":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = chaos_replay_task_view(query)
                self._send(200, {"ok": True, **result})
                return
            if path == "/simulate/config":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = simulate_config_get()
                self._send(200, result)
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path == "/simulate/task-log":
                task_id = (query.get("taskId") or query.get("task_id") or [""])[0]
                cursor = (query.get("cursor") or ["0"])[0]
                status, result = simulate_task_log_get({"taskId": task_id, "cursor": cursor})
                self._send(status, result)
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if path == "/simulate/single-report":
                task_id = (query.get("taskId") or query.get("task_id") or [""])[0]
                status, result = simulate_single_report_get({"taskId": task_id})
                self._send(status, result)
                return
            if path == "/sfrz/log":
                limit = (query.get("limit") or ["200"])[0]
                lines = _read_sfrz_log_tail(limit=limit)
                self._send(200, {"ok": True, "items": lines, "count": len(lines), "file": SFRZ_LOG_FILE})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if path.startswith("/shops/list"):
                result = shops_list()
                self._send(200, {"ok": True, **result})
                return
            self._send(404, {"ok": False, "error": "not_found"})
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception as error:
            print(f"[ERROR] {self.path}: {error}")
            traceback.print_exc()
            _append_sfrz_log(f"[GET][ERROR] {self.path}: {error}")
            self._send(500, {"ok": False, "error": str(error)})

    # EN: Backend control point for this logic branch.
    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
    def do_POST(self):
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        if self.path not in (
            "/ga-optimize-wave",
            "/wave-optimize",
            "/simulate/optimize-time",
            "/simulate/single-route-decisions",
            "/simulate/single-route-continue",
            "/distance-matrix/full",
            "/deepseek-chat",
            "/archive/save",
            "/archive/list",
            "/archive/get",
            "/amap-cache/sync",
            "/vehicles/parse",
            "/recommended-plans/select",
            "/run-regions/create",
            "/run-regions/update",
            "/run-regions/delete",
            "/run-regions/generate-scheme1",
            "/run-region-schemes/create",
            "/run-region-schemes/update",
            "/run-region-schemes/delete",
            "/wms/fetch",
            "/wms/cargo-raw/save",
            "/wms/cargo-raw/rebuild-resolved",
            "/wms/cargo-raw/resolve-only",
            "/clean-cargo-raw/save",
            "/store-wave-load-resolved/save",
            "/store-wave-load-resolved/batch",
            "/rengong/template-batch",
            "/sfrz/log",
        ):
            self._send(404, {"ok": False, "error": "not_found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            payload = json.loads(raw.decode("utf-8"))
            if self.path == "/sfrz/log":
                line = str((payload or {}).get("line") or "").strip()
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if line:
                    _append_sfrz_log(line)
                self._send(200, {"ok": True, "written": bool(line), "file": SFRZ_LOG_FILE})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/distance-matrix/full":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = _get_distance_matrix_full(payload)
                self._send(200, result)
                return
            if self.path == "/ga-optimize-wave":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                payload = _normalize_strategy_config(payload)
                payload = _overlay_payload_stores_from_resolved_table(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                payload = _normalize_numeric_types_for_solver(payload)
                payload, strategy_audit = apply_strategy_center(payload)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                payload = _apply_operational_strategy_overrides(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if not _should_skip_dist_hydrate(payload):
                    payload = _hydrate_payload_dist_from_db(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                payload = _validate_solver_payload_fields(payload)
                if payload.get("useRecommendedPlan"):
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    payload = apply_recommended_plan_warm_start(payload)
                _enforce_and_validate_vehicle_type_for_solve(payload)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = run_ga_optimize(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if isinstance(result, dict):
                    result["unscheduledStores"] = _build_backend_unscheduled_stores(payload, result)
                    result["strategyAudit"] = strategy_audit
                    result["distDbStats"] = payload.get("distDbStats") or {}
                _log_backend_state_summary("GA_OPTIMIZE_RESULT", result)
                result["usedRecommendedPlanWarmStart"] = bool(payload.get("recommendedPlanApplied"))
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/wave-optimize":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                payload = _normalize_strategy_config(payload)
                # 浠呭綋绠楁硶涓嶆槸 vehicle 鏃舵墠鎵ц overlay锛堝師鏈夌▼搴忎笉鍙橈級
                if payload.get("algorithmKey") != "vehicle":
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    payload = _overlay_payload_stores_from_resolved_table(payload)
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                payload = _normalize_numeric_types_for_solver(payload)
                payload, strategy_audit = apply_strategy_center(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                payload = _apply_operational_strategy_overrides(payload)
                if not _should_skip_dist_hydrate(payload):
                    # EN: Key backend step in this flow.
                    # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    payload = _hydrate_payload_dist_from_db(payload)
                payload = _validate_solver_payload_fields(payload)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                if payload.get("useRecommendedPlan"):
                    # EN: Backend control point for this logic branch.
                    # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                    payload = apply_recommended_plan_warm_start(payload)
                _enforce_and_validate_vehicle_type_for_solve(payload)
                algorithm_key = str(payload.get("algorithmKey") or "").strip().lower()
                module_file = _resolve_algorithm_module_file(algorithm_key)
                _append_sfrz_log(f"[SOLVER_PATH] key={algorithm_key} module={module_file}")
                _assert_expected_algorithm_module(algorithm_key, module_file)
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_wave_optimize(payload)
                if isinstance(result, dict):
                    result["unscheduledStores"] = _build_backend_unscheduled_stores(payload, result)
                    result["strategyAudit"] = strategy_audit
                    result["distDbStats"] = payload.get("distDbStats") or {}
                    result["solverDebug"] = {"algorithmKey": algorithm_key, "moduleFile": module_file}
                _log_backend_state_summary("WAVE_OPTIMIZE_RESULT", result)
                result["usedRecommendedPlanWarmStart"] = bool(payload.get("recommendedPlanApplied"))
                self._send(200, {"ok": True, **result})
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/simulate/optimize-time":
                status, result = simulate_optimize_time(payload)
                self._send(status, result)
                return
            if self.path == "/simulate/single-route-decisions":
                status, result = simulate_single_route_decisions_save(payload)
                self._send(status, result)
                return
            # EN: Key backend step in this flow.
            # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
            if self.path == "/simulate/single-route-continue":
                status, result = simulate_single_route_continue(payload)
                self._send(status, result)
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/rengong/template-batch":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_rengong_template_batch()
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/archive/save":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = archive_save(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/archive/list":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = archive_list(payload)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/archive/get":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = archive_get(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/amap-cache/sync":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = amap_cache_sync(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/vehicles/parse":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = parse_vehicle_file(payload)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/recommended-plans/select":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = recommended_plan_select(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/run-regions/create":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = run_regions_create(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/run-regions/update":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_regions_update(payload)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/run-regions/delete":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_regions_delete(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/run-regions/generate-scheme1":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = run_regions_generate_scheme1(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/run-region-schemes/create":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_region_schemes_create(payload)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/run-region-schemes/update":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = run_region_schemes_update(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/run-region-schemes/delete":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = run_region_schemes_delete(payload)
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/wms/fetch":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_fetch(payload)
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/wms/cargo-raw/save":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_cargo_raw_save(payload or {})
                self._send(200, result)
                return
            if self.path == "/wms/cargo-raw/rebuild-resolved":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = wms_cargo_raw_rebuild_resolved(payload or {})
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/wms/cargo-raw/resolve-only":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = wms_cargo_raw_resolve_only(payload or {})
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            if self.path == "/clean-cargo-raw/save":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = clean_cargo_raw_save(payload or {})
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/store-wave-load-resolved/save":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                # EN: Backend control point for this logic branch.
                # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
                result = store_wave_load_resolved_save((payload or {}).get("rows") or [])
                self._send(200, {"ok": True, **result})
                return
            if self.path == "/store-wave-load-resolved/batch":
                # EN: Key backend step in this flow.
                # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
                result = store_wave_load_resolved_get_batch((payload or {}).get("shopCodes") or [])
                self._send(200, {"ok": True, **result})
                return
            # EN: Backend control point for this logic branch.
            # CN: 褰撳墠閫昏緫鍒嗘敮鐨勫悗绔帶鍒惰妭鐐广€?
            result = call_deepseek(payload)
            self._send(200, {"ok": True, **result})
        # EN: Key backend step in this flow.
        # CN: 褰撳墠鍚庣娴佺▼涓殑鍏抽敭姝ラ銆?
        except Exception as error:
            print(f"[ERROR] {self.path}: {error}")
            traceback.print_exc()
            _append_sfrz_log(f"[POST][ERROR] {self.path}: {error}")
            self._send(500, {"ok": False, "error": str(error)})


if __name__ == "__main__":
    # verify_algorithm_file(strict=True)
    print(f"GA backend listening on http://{HOST}:{PORT}")
    print(f"骞惰璁＄畻: CPU鏍稿績鏁?{os.cpu_count()}")
    print(f"GPU鍙敤: {GPU_AVAILABLE}")
    print(f"鑷姩璋冨弬鍙敤: {SKOPT_AVAILABLE}")
    print(f"澶氱畻娉曠嫭绔嬪垵濮嬭В绛栫暐宸插惎鐢?")
    print(f"  - SA/Tabu: 闅忔満椤哄簭")
    print(f"  - LNS: 璺濈浼樺厛")
    print(f"  - ACO: 鏃堕棿绐椾紭鍏?)
    print(f"  - PSO: 瀹屽叏闅忔満")
    print(f"  - Hybrid: Clark-Wright鑺傜害娉?)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
