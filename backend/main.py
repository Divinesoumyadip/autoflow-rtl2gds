"""
AutoFlow Backend — RTL2GDS Flow Orchestration Engine
FastAPI + Python simulation of C++ DAG engine
"""
from __future__ import annotations
import asyncio
import json
import random
import time
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─── App Setup ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="AutoFlow — RTL2GDS Orchestration API",
    description="VLSI EDA Flow Orchestration with DAG execution engine",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Models ──────────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING   = "PENDING"
    READY     = "READY"
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"

class FlowStage(str, Enum):
    RTL   = "RTL"
    SYNTH = "SYNTH"
    PNR   = "PNR"
    STA   = "STA"
    DRC   = "DRC"

class PPAMetrics(BaseModel):
    power_mw: float = 0.0
    performance_ns: float = 0.0
    area_um2: float = 0.0
    wns: float = 0.0
    tns: float = 0.0
    drc_violations: int = 0
    utilization: float = 0.0

class TaskNode(BaseModel):
    id: str
    name: str
    stage: FlowStage
    tool: str
    status: TaskStatus = TaskStatus.PENDING
    dependencies: List[str] = []
    progress: int = 0
    log_lines: List[str] = []
    metrics: Optional[PPAMetrics] = None
    runtime_seconds: float = 0.0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    tcl_snippet: str = ""

class FlowConfig(BaseModel):
    design_name: str = "riscv_core"
    pdk: str = "sky130"
    target_freq_mhz: float = 400.0
    utilization: float = 75.0
    num_threads: int = 4
    enable_eco: bool = False

class FlowRun(BaseModel):
    run_id: str
    design_name: str
    status: str = "IDLE"
    tasks: List[TaskNode] = []
    config: FlowConfig
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    final_ppa: Optional[PPAMetrics] = None
    progress_pct: float = 0.0

class StartFlowRequest(BaseModel):
    config: FlowConfig

# ─── In-Memory Store ─────────────────────────────────────────────────────────

runs: Dict[str, FlowRun] = {}
ws_connections: Dict[str, List[WebSocket]] = {}

# ─── DAG Task Definitions ─────────────────────────────────────────────────────

def build_task_graph(config: FlowConfig) -> List[TaskNode]:
    d = config.design_name
    f = config.target_freq_mhz

    return [
        TaskNode(
            id="rtl_lint", name="RTL Lint Check", stage=FlowStage.RTL,
            tool="verilator", dependencies=[],
            tcl_snippet=f"# Verilator lint\nverilator --lint-only -Wall {d}/*.v",
        ),
        TaskNode(
            id="synthesis", name="Logic Synthesis", stage=FlowStage.SYNTH,
            tool="yosys", dependencies=["rtl_lint"],
            tcl_snippet=f"yosys -import\nread_verilog {d}.v\nsynth -top {d} -flatten\nabc -liberty sky130.lib\nwrite_verilog synth_out.v",
        ),
        TaskNode(
            id="floorplan", name="Floorplanning", stage=FlowStage.PNR,
            tool="openroad", dependencies=["synthesis"],
            tcl_snippet=f"init_floorplan -utilization {config.utilization} -aspect_ratio 1\nplace_pins -hor_layers metal2 -ver_layers metal3\ntapcell -tapcell_master sky130_tap",
        ),
        TaskNode(
            id="placement", name="Cell Placement", stage=FlowStage.PNR,
            tool="openroad", dependencies=["floorplan"],
            tcl_snippet="global_placement -density 0.75\nlegalize_placement\ndetailed_placement\ncheck_placement -verbose",
        ),
        TaskNode(
            id="cts", name="Clock Tree Synthesis", stage=FlowStage.PNR,
            tool="openroad", dependencies=["placement"],
            tcl_snippet="clock_tree_synthesis -root_buf sky130_buf_4 -buf_list {sky130_buf_2 sky130_buf_4}\nset_propagated_clock [all_clocks]",
        ),
        TaskNode(
            id="routing", name="Detailed Routing", stage=FlowStage.PNR,
            tool="openroad", dependencies=["cts"],
            tcl_snippet=f"global_route -guide_file route.guide -overflow_iterations 100\ndetailed_route -output_drc {d}_drc.rpt",
        ),
        TaskNode(
            id="sta", name="Static Timing Analysis", stage=FlowStage.STA,
            tool="opensta", dependencies=["routing"],
            tcl_snippet=f"create_clock -name clk -period {1000/f:.2f} [get_ports clk]\nreport_checks -path_delay max\nreport_wns\nreport_tns\nreport_power",
        ),
        TaskNode(
            id="drc", name="Design Rule Check", stage=FlowStage.DRC,
            tool="magic", dependencies=["routing"],
            tcl_snippet=f"drc check\ndrc why\ndrc count\nsave {d}_drc.mag",
        ),
        TaskNode(
            id="lvs", name="Layout vs Schematic", stage=FlowStage.DRC,
            tool="netgen", dependencies=["drc", "sta"],
            tcl_snippet=f"netgen -batch lvs {{{d}.spice {d}}} {{sky130.spice sky130}} {d}_lvs.out",
        ),
        TaskNode(
            id="gds_export", name="GDS-II Export", stage=FlowStage.DRC,
            tool="klayout", dependencies=["lvs"],
            tcl_snippet=f"stream out -format GDS -verbose {d}.gds",
        ),
    ]

# ─── Simulation Engine ────────────────────────────────────────────────────────

TASK_DURATIONS = {
    "rtl_lint":  2.5,
    "synthesis": 6.0,
    "floorplan": 3.5,
    "placement": 7.0,
    "cts":       4.5,
    "routing":   9.0,
    "sta":       5.0,
    "drc":       4.0,
    "lvs":       3.0,
    "gds_export":2.0,
}

TASK_LOGS = {
    "rtl_lint": [
        "[INFO] Loading Verilog files...",
        "[INFO] Parsing riscv_core.v — 4821 cells",
        "[WARN] W001: Implicit wire in alu.v:42",
        "[WARN] W002: Undriven port 'debug_out' in core.v",
        "[INFO] Lint PASSED: 0 errors, 2 warnings",
    ],
    "synthesis": [
        "[INFO] === Yosys 0.38 ===",
        "[INFO] Elaborating design hierarchy...",
        "[INFO] Running pass: synth",
        "[INFO] Running pass: opt_expr",
        "[INFO] Running pass: techmap",
        "[INFO] Running pass: dfflibmap",
        "[INFO] Running pass: abc — mapping to sky130 cells",
        "[INFO] Cells: 4821  Wires: 5234  Ports: 128",
        "[INFO] Area estimate: 12840 um²",
        "[INFO] Critical path: 2.14 ns",
        "[INFO] Synthesis COMPLETE ✓",
    ],
    "floorplan": [
        "[INFO] Die area: 200 × 200 um",
        "[INFO] Core utilization: 75.0%",
        "[INFO] Placing I/O pins on metal2/metal3",
        "[INFO] Inserting 512 tapcells",
        "[INFO] Floorplan COMPLETE ✓",
    ],
    "placement": [
        "[INFO] Global placement (RePlAce)...",
        "[INFO]   iter=10  overflow=0.2840  HPWL=1140000",
        "[INFO]   iter=50  overflow=0.0921  HPWL=960000",
        "[INFO]   iter=100 overflow=0.0312  HPWL=892450",
        "[INFO] Legalizing placement — 0 overlaps",
        "[INFO] Detailed placement: 142 swaps",
        "[INFO] Placement COMPLETE ✓",
    ],
    "cts": [
        "[INFO] Building clock tree for 'clk'...",
        "[INFO] Level 0: 1 root driver",
        "[INFO] Level 1: 4 buffers",
        "[INFO] Level 2: 16 buffers",
        "[INFO] Level 3: 64 leaf buffers → 512 FFs",
        "[INFO] Skew: 42 ps  |  Max latency: 318 ps",
        "[INFO] 128 buffers inserted",
        "[INFO] CTS COMPLETE ✓",
    ],
    "routing": [
        "[INFO] Global routing (FastRoute)...",
        "[INFO]   M1 overflow: 0 segments",
        "[INFO]   M2 overflow: 0 segments",
        "[INFO] Detailed routing (TritonRoute)...",
        "[INFO]   Pass 1: initial routing",
        "[INFO]   Pass 2: DRC repair",
        "[INFO]   Pass 3: via optimization",
        "[INFO] Nets: 5234/5234 (100%)  Vias: 18472",
        "[INFO] DRC violations: 2 (fixable spacing)",
        "[INFO] Routing COMPLETE ✓",
    ],
    "sta": [
        "[INFO] === OpenSTA ===",
        "[INFO] Corner: tt_025C_1v80",
        "[INFO] Clock: clk  period=2.500 ns",
        "[INFO] Critical path: reg_alu/q → u_alu → reg_out/d",
        "[INFO]   Arrival time:  2.442 ns",
        "[INFO]   Required time: 2.500 ns",
        "[INFO]   Slack (MET):  +0.058 ns ✓",
        "[INFO] WNS: +0.058 ns  TNS: 0.0 ns",
        "[INFO] Power: 52.4 mW  (dynamic=41.2 leakage=11.2)",
        "[INFO] STA COMPLETE ✓",
    ],
    "drc": [
        "[INFO] Loading sky130A DRC rules (v1.0.430)...",
        "[INFO] Checking metal spacing rules... PASS",
        "[INFO] Checking via enclosure rules... PASS",
        "[INFO] Checking density rules... PASS",
        "[INFO] 847 rules checked — 0 violations",
        "[INFO] DRC CLEAN ✓",
    ],
    "lvs": [
        "[INFO] Extracted netlist: 4821 devices",
        "[INFO] Schematic netlist: 4821 devices",
        "[INFO] Devices  match: 4821/4821 ✓",
        "[INFO] Nets     match: 5234/5234 ✓",
        "[INFO] LVS CLEAN ✓",
    ],
    "gds_export": [
        "[INFO] Streaming GDS-II...",
        "[INFO]   Layers: 33",
        "[INFO]   Cells: 4821",
        "[INFO]   File: riscv_core.gds (14.2 MB)",
        "[INFO] ═══════════════════════════════════",
        "[INFO]   RTL2GDS FLOW COMPLETE ✓",
        "[INFO] ═══════════════════════════════════",
    ],
}

TASK_METRICS: Dict[str, PPAMetrics] = {
    "synthesis": PPAMetrics(power_mw=48.2, performance_ns=2.14, area_um2=12840, wns=-0.21, tns=-8.4, drc_violations=0, utilization=65.0),
    "routing":   PPAMetrics(power_mw=52.4, performance_ns=2.44, area_um2=14500, wns=-0.06, tns=-1.2, drc_violations=2,  utilization=72.5),
    "sta":       PPAMetrics(power_mw=52.4, performance_ns=2.44, area_um2=14500, wns=0.058,  tns=0.0,  drc_violations=0,  utilization=72.5),
    "drc":       PPAMetrics(power_mw=52.4, performance_ns=2.44, area_um2=14500, wns=0.058,  tns=0.0,  drc_violations=0,  utilization=72.5),
    "gds_export":PPAMetrics(power_mw=52.4, performance_ns=2.44, area_um2=14500, wns=0.058,  tns=0.0,  drc_violations=0,  utilization=72.5),
}

async def broadcast(run_id: str, event: dict):
    for ws in ws_connections.get(run_id, []):
        try:
            await ws.send_text(json.dumps(event))
        except Exception:
            pass

async def simulate_task(run_id: str, task: TaskNode, speed_factor: float = 1.0):
    run = runs[run_id]
    duration = TASK_DURATIONS.get(task.id, 3.0) / speed_factor
    logs = TASK_LOGS.get(task.id, ["[INFO] Running...", "[INFO] Complete ✓"])

    task.status = TaskStatus.RUNNING
    task.started_at = datetime.utcnow().isoformat()
    await broadcast(run_id, {"type": "task_start", "task_id": task.id, "name": task.name})

    # Stream log lines
    log_interval = duration / (len(logs) + 2)
    for i, line in enumerate(logs):
        await asyncio.sleep(log_interval)
        task.log_lines.append(line)
        task.progress = int((i + 1) / len(logs) * 90)
        await broadcast(run_id, {
            "type": "task_log",
            "task_id": task.id,
            "line": line,
            "progress": task.progress,
        })

    await asyncio.sleep(log_interval)
    task.progress = 100
    task.status = TaskStatus.COMPLETED
    task.completed_at = datetime.utcnow().isoformat()
    task.runtime_seconds = round(duration, 2)

    if task.id in TASK_METRICS:
        m = TASK_METRICS[task.id]
        task.metrics = m
        run.final_ppa = m

    completed = sum(1 for t in run.tasks if t.status == TaskStatus.COMPLETED)
    run.progress_pct = round(completed / len(run.tasks) * 100, 1)

    await broadcast(run_id, {
        "type": "task_complete",
        "task_id": task.id,
        "metrics": task.metrics.dict() if task.metrics else None,
        "runtime": task.runtime_seconds,
        "progress_pct": run.progress_pct,
    })

def get_task_by_id(run: FlowRun, task_id: str) -> Optional[TaskNode]:
    return next((t for t in run.tasks if t.id == task_id), None)

async def run_dag(run_id: str):
    run = runs[run_id]
    run.status = "RUNNING"
    run.started_at = datetime.utcnow().isoformat()
    await broadcast(run_id, {"type": "flow_start", "run_id": run_id})

    # Topological execution with parallelism
    completed_ids: set = set()
    in_progress: set = set()

    async def try_schedule():
        ready = []
        for task in run.tasks:
            if task.status == TaskStatus.PENDING:
                if all(d in completed_ids for d in task.dependencies):
                    ready.append(task)
        return ready

    while True:
        ready = await try_schedule()
        if not ready and not in_progress:
            break
        if not ready:
            await asyncio.sleep(0.1)
            continue

        # Launch ready tasks (parallel within same stage)
        coros = []
        for task in ready:
            task.status = TaskStatus.READY
            in_progress.add(task.id)
            coros.append(simulate_task(run_id, task, speed_factor=1.5))

        if coros:
            done_tasks = [t for t in ready]
            await asyncio.gather(*coros)
            for t in done_tasks:
                in_progress.discard(t.id)
                completed_ids.add(t.id)

    run.status = "COMPLETED"
    run.completed_at = datetime.utcnow().isoformat()
    run.progress_pct = 100.0
    await broadcast(run_id, {
        "type": "flow_complete",
        "run_id": run_id,
        "final_ppa": run.final_ppa.dict() if run.final_ppa else None,
    })

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"service": "AutoFlow RTL2GDS Engine", "version": "2.1.0", "status": "operational"}

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "active_runs": len(runs)}

@app.post("/api/flows", response_model=FlowRun)
async def create_flow(req: StartFlowRequest):
    run_id = str(uuid.uuid4())[:8]
    tasks = build_task_graph(req.config)
    run = FlowRun(
        run_id=run_id,
        design_name=req.config.design_name,
        config=req.config,
        tasks=tasks,
        created_at=datetime.utcnow().isoformat(),
    )
    runs[run_id] = run
    asyncio.create_task(run_dag(run_id))
    return run

@app.get("/api/flows", response_model=List[FlowRun])
def list_flows():
    return list(runs.values())

@app.get("/api/flows/{run_id}", response_model=FlowRun)
def get_flow(run_id: str):
    if run_id not in runs:
        raise HTTPException(404, "Flow run not found")
    return runs[run_id]

@app.get("/api/flows/{run_id}/tasks", response_model=List[TaskNode])
def get_tasks(run_id: str):
    if run_id not in runs:
        raise HTTPException(404, "Flow run not found")
    return runs[run_id].tasks

@app.get("/api/flows/{run_id}/tasks/{task_id}", response_model=TaskNode)
def get_task(run_id: str, task_id: str):
    run = runs.get(run_id)
    if not run:
        raise HTTPException(404, "Flow run not found")
    task = get_task_by_id(run, task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task

@app.get("/api/flows/{run_id}/ppa", response_model=Optional[PPAMetrics])
def get_ppa(run_id: str):
    if run_id not in runs:
        raise HTTPException(404, "Flow run not found")
    return runs[run_id].final_ppa

@app.get("/api/dag-info")
def dag_info():
    """Return static DAG structure for visualization"""
    return {
        "nodes": [
            {"id": "rtl_lint",   "label": "RTL Lint",   "stage": "RTL",   "tool": "Verilator", "x": 1, "y": 3},
            {"id": "synthesis",  "label": "Synthesis",  "stage": "SYNTH", "tool": "Yosys",     "x": 2, "y": 3},
            {"id": "floorplan",  "label": "Floorplan",  "stage": "PNR",   "tool": "OpenROAD",  "x": 3, "y": 3},
            {"id": "placement",  "label": "Placement",  "stage": "PNR",   "tool": "OpenROAD",  "x": 4, "y": 3},
            {"id": "cts",        "label": "CTS",        "stage": "PNR",   "tool": "OpenROAD",  "x": 5, "y": 3},
            {"id": "routing",    "label": "Routing",    "stage": "PNR",   "tool": "OpenROAD",  "x": 6, "y": 3},
            {"id": "sta",        "label": "STA",        "stage": "STA",   "tool": "OpenSTA",   "x": 7, "y": 2},
            {"id": "drc",        "label": "DRC",        "stage": "DRC",   "tool": "Magic",     "x": 7, "y": 4},
            {"id": "lvs",        "label": "LVS",        "stage": "DRC",   "tool": "Netgen",    "x": 8, "y": 3},
            {"id": "gds_export", "label": "GDS Export", "stage": "DRC",   "tool": "KLayout",   "x": 9, "y": 3},
        ],
        "edges": [
            {"from": "rtl_lint",  "to": "synthesis"},
            {"from": "synthesis", "to": "floorplan"},
            {"from": "floorplan", "to": "placement"},
            {"from": "placement", "to": "cts"},
            {"from": "cts",       "to": "routing"},
            {"from": "routing",   "to": "sta"},
            {"from": "routing",   "to": "drc"},
            {"from": "sta",       "to": "lvs"},
            {"from": "drc",       "to": "lvs"},
            {"from": "lvs",       "to": "gds_export"},
        ]
    }

@app.websocket("/ws/{run_id}")
async def websocket_endpoint(websocket: WebSocket, run_id: str):
    await websocket.accept()
    ws_connections.setdefault(run_id, []).append(websocket)
    try:
        # Send current state immediately
        if run_id in runs:
            await websocket.send_text(json.dumps({
                "type": "state_sync",
                "run": runs[run_id].dict(),
            }))
        while True:
            await websocket.receive_text()  # keep-alive
    except WebSocketDisconnect:
        ws_connections[run_id].remove(websocket)
