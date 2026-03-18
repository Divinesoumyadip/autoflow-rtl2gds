# AutoFlow ⚡ — RTL2GDS Flow Orchestration Engine

> **Hero project for NVIDIA CAD/EDA Team applications**
> C++ DAG Engine · Python FastAPI · TCL Scripting · React 18 · WebSocket

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AutoFlow System                           │
│                                                                  │
│  ┌──────────────┐    REST/WS     ┌──────────────────────────┐   │
│  │  React 18    │◄──────────────►│   FastAPI Backend        │   │
│  │  Frontend    │                │   (Python 3.12)          │   │
│  │              │                │                          │   │
│  │  • DAG Viz   │                │  ┌────────────────────┐  │   │
│  │  • Live Logs │                │  │  DAG Engine (C++)  │  │   │
│  │  • PPA Cards │                │  │                    │  │   │
│  │  • TCL View  │                │  │  • Topo sort       │  │   │
│  └──────────────┘                │  │  • Thread pool     │  │   │
│                                  │  │  • Cycle detect    │  │   │
│                                  │  │  • Retry logic     │  │   │
│                                  │  └────────────────────┘  │   │
│                                  │                          │   │
│                                  │  ┌────────────────────┐  │   │
│                                  │  │   TCL Stub Layer   │  │   │
│                                  │  │                    │  │   │
│                                  │  │  • Yosys API       │  │   │
│                                  │  │  • OpenROAD API    │  │   │
│                                  │  │  • OpenSTA API     │  │   │
│                                  │  │  • Magic DRC API   │  │   │
│                                  │  └────────────────────┘  │   │
│                                  └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔷 C++ DAG Engine (`dag_engine/`)

The core scheduling primitive — this is the "Candidate Master level" component.

### Key Algorithms

**Cycle Detection** — DFS with recursion stack
```cpp
bool has_cycle_dfs(const string& node,
                   unordered_set<string>& visited,
                   unordered_set<string>& rec_stack);
```

**Topological Sort** — Kahn's algorithm (BFS-based)
```cpp
vector<string> topological_sort();
// O(V + E) — processes in_degree map
```

**Parallel Execution** — Thread pool with condition variable
```cpp
// Worker threads wait on CV, pick tasks from ready_queue
// Multiple independent tasks run concurrently
// Dependency tracking via in_degree counters
```

**Failure Propagation**
```cpp
// When task fails: mark all downstream as SKIPPED
// DFS traversal of adjacency list from failed node
```

### RTL2GDS Pipeline Tasks

| Task | Stage | Tool | Dependencies |
|------|-------|------|-------------|
| `rtl_lint` | RTL | Verilator | — |
| `synthesis` | SYNTH | Yosys | rtl_lint |
| `floorplan` | PNR | OpenROAD | synthesis |
| `placement` | PNR | OpenROAD | floorplan |
| `cts` | PNR | OpenROAD | placement |
| `routing` | PNR | OpenROAD | cts |
| `sta` | STA | OpenSTA | routing |
| `drc` | DRC | Magic | routing |
| `lvs` | DRC | Netgen | drc + sta |
| `gds_export` | DRC | KLayout | lvs |

**Parallelism**: `sta` and `drc` run concurrently after `routing` — this is the DAG's only parallel branch.

---

## 🐍 Python FastAPI Backend (`backend/`)

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/flows` | Launch a new RTL2GDS flow |
| `GET`  | `/api/flows` | List all flow runs |
| `GET`  | `/api/flows/{id}` | Get flow status + PPA |
| `GET`  | `/api/flows/{id}/tasks` | Get task list with logs |
| `GET`  | `/api/dag-info` | DAG structure for viz |
| `WS`   | `/ws/{run_id}` | Live streaming events |

### WebSocket Events

```json
{"type": "task_start",    "task_id": "synthesis", "name": "Logic Synthesis"}
{"type": "task_log",      "task_id": "synthesis", "line": "[INFO] Running abc...", "progress": 60}
{"type": "task_complete", "task_id": "synthesis", "metrics": {...}, "runtime": 6.2}
{"type": "flow_complete", "run_id": "abc123",     "final_ppa": {...}}
```

---

## 📜 TCL Scripting Layer (`tcl_stubs/`)

Full TCL namespace with EDA tool API stubs:

```tcl
namespace eval autoflow {
    proc synth_yosys  {design liberty_file target_freq} {...}
    proc floorplan    {utilization aspect_ratio core_space} {...}
    proc run_cts      {root_buf buf_list} {...}
    proc run_sta      {clock_period} {...}
    proc run_drc      {} {...}
    proc export_gds   {design} {...}
    proc run_full_flow {design pdk freq_mhz} {...}  # Master runner
}
```

---

## ⚡ PPA Metrics Tracked

| Metric | Description | Target (sky130) |
|--------|-------------|-----------------|
| **Power** | Total switching + leakage | < 60 mW |
| **Performance** | Critical path delay | < 2.5 ns |
| **Area** | Core cell footprint | < 15,000 μm² |
| **WNS** | Worst Negative Slack | ≥ 0 (timing met) |
| **TNS** | Total Negative Slack | = 0 |
| **DRC Violations** | Layout rule checks | = 0 |
| **Utilization** | Cell density | 70–80% |

---

## 🚀 Running Locally

### Backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend
```bash
# Open frontend/autoflow.html directly in browser
# OR serve with any static server:
npx serve frontend/
```

### Docker (Full Stack)
```bash
cd docker
docker-compose up --build
# Frontend: http://localhost:3000
# API:      http://localhost:8000/docs
```

---

## 🐳 Deployment

| Component | Platform | URL |
|-----------|----------|-----|
| Frontend | Vercel | `autoflow.vercel.app` |
| Backend | Render | `autoflow-api.onrender.com` |
| Container | Docker Hub | `autoflow/rtl2gds:latest` |

### Vercel (`vercel.json`)
```json
{
  "builds": [{"src": "frontend/autoflow.html", "use": "@vercel/static"}],
  "routes": [{"src": "/(.*)", "dest": "/frontend/autoflow.html"}]
}
```

### Render (`render.yaml`)
```yaml
services:
  - type: web
    name: autoflow-api
    env: python
    buildCommand: pip install -r requirements.txt
    startCommand: uvicorn main:app --host 0.0.0.0 --port $PORT
```

---

## 📐 Keywords for NVIDIA Interview

- **Dependency Graph** — DAG with topological ordering for EDA stage sequencing
- **Parallel Execution** — Thread pool executes independent tasks (STA ∥ DRC) concurrently
- **Resource Management** — Configurable thread count, retry policies, failure propagation
- **TCL Scripting** — Full Tcl namespace API mirroring Yosys/OpenROAD/OpenSTA interfaces
- **RTL-to-GDS** — Complete 10-stage VLSI physical design flow simulation
- **PPA Tracking** — Power/Performance/Area metrics aggregated across flow stages

---

*Built to demonstrate EDA toolchain knowledge, systems programming, and full-stack engineering for NVIDIA CAD team.*
