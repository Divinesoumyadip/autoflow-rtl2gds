#include "dag_engine.hpp"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <cmath>
#include <random>

namespace autoflow {

// ─── DAGEngine ───────────────────────────────────────────────────────────────

DAGEngine::DAGEngine(int num_threads) : num_threads_(num_threads) {}

DAGEngine::~DAGEngine() {
    cancel();
}

void DAGEngine::add_task(Task task) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    std::string id = task.id;
    tasks_[id] = std::move(task);
    adj_[id];          // ensure entry exists
    in_degree_[id] = 0;
}

void DAGEngine::add_dependency(const std::string& task_id, const std::string& dep_id) {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    adj_[dep_id].push_back(task_id);
    in_degree_[task_id]++;
    tasks_[task_id].dependencies.push_back(dep_id);
}

bool DAGEngine::has_cycle_dfs(const std::string& node,
                               std::unordered_set<std::string>& visited,
                               std::unordered_set<std::string>& rec_stack) {
    visited.insert(node);
    rec_stack.insert(node);
    for (auto& neighbor : adj_[node]) {
        if (rec_stack.count(neighbor)) return true;
        if (!visited.count(neighbor))
            if (has_cycle_dfs(neighbor, visited, rec_stack)) return true;
    }
    rec_stack.erase(node);
    return false;
}

bool DAGEngine::validate_dag() {
    std::unordered_set<std::string> visited, rec_stack;
    for (auto& [id, _] : tasks_)
        if (!visited.count(id))
            if (has_cycle_dfs(id, visited, rec_stack)) return false;
    return true;
}

std::vector<std::string> DAGEngine::topological_sort() {
    auto in_deg = in_degree_;
    std::queue<std::string> q;
    for (auto& [id, deg] : in_deg)
        if (deg == 0) q.push(id);
    std::vector<std::string> order;
    while (!q.empty()) {
        auto node = q.front(); q.pop();
        order.push_back(node);
        for (auto& next : adj_[node]) {
            if (--in_deg[next] == 0) q.push(next);
        }
    }
    return order;
}

void DAGEngine::run() {
    if (!validate_dag())
        throw std::runtime_error("DAG validation failed: cycle detected");

    dag_start_ = std::chrono::steady_clock::now();
    stop_ = false;
    paused_ = false;

    // Initialize ready queue (tasks with no deps)
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (auto& [id, deg] : in_degree_)
            if (deg == 0) {
                tasks_[id].status = TaskStatus::READY;
                ready_queue_.push(id);
            }
    }

    // Spawn worker threads
    for (int i = 0; i < num_threads_; ++i)
        workers_.emplace_back(&DAGEngine::worker_thread, this);

    // Wait for all workers
    for (auto& t : workers_) if (t.joinable()) t.join();
    workers_.clear();

    if (on_dag_complete_cb_) on_dag_complete_cb_(get_stats());
}

void DAGEngine::worker_thread() {
    while (true) {
        std::string task_id;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            cv_.wait(lock, [this] {
                return stop_ || (!paused_ && !ready_queue_.empty()) ||
                       (active_tasks_ == 0 && ready_queue_.empty());
            });

            if (stop_) return;
            if (active_tasks_ == 0 && ready_queue_.empty()) return;
            if (paused_ || ready_queue_.empty()) continue;

            task_id = ready_queue_.front();
            ready_queue_.pop();
            active_tasks_++;
        }

        execute_task(tasks_[task_id]);

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            active_tasks_--;
        }
        cv_.notify_all();
    }
}

void DAGEngine::execute_task(Task& task) {
    task.status = TaskStatus::RUNNING;
    task.start_time = std::chrono::steady_clock::now();

    if (on_start_cb_) on_start_cb_(task);

    bool succeeded = false;
    for (int attempt = 0; attempt <= task.max_retries; ++attempt) {
        if (attempt > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
            task.retry_count = attempt;
        }
        try {
            task.result = task.executor();
            if (task.result->success) { succeeded = true; break; }
        } catch (const std::exception& e) {
            task.result = TaskResult{false, "", e.what(), {}, 0.0, ""};
        }
    }

    task.end_time = std::chrono::steady_clock::now();
    double rt = std::chrono::duration<double>(task.end_time - task.start_time).count();
    if (task.result) task.result->runtime_seconds = rt;

    if (succeeded) {
        task.status = TaskStatus::COMPLETED;
        if (on_complete_cb_) on_complete_cb_(task, *task.result);
        update_ready_queue(task.id);
    } else {
        task.status = TaskStatus::FAILED;
        std::string err = task.result ? task.result->error : "Unknown error";
        if (on_fail_cb_) on_fail_cb_(task, err);
        // Mark downstream tasks as SKIPPED
        std::queue<std::string> skip_q;
        for (auto& next : adj_[task.id]) skip_q.push(next);
        while (!skip_q.empty()) {
            auto nid = skip_q.front(); skip_q.pop();
            if (tasks_[nid].status == TaskStatus::PENDING ||
                tasks_[nid].status == TaskStatus::READY) {
                tasks_[nid].status = TaskStatus::SKIPPED;
                for (auto& nn : adj_[nid]) skip_q.push(nn);
            }
        }
    }
}

void DAGEngine::update_ready_queue(const std::string& completed_id) {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    for (auto& next_id : adj_[completed_id]) {
        auto& next = tasks_[next_id];
        if (next.status != TaskStatus::PENDING) continue;
        // Check all deps completed
        bool all_done = true;
        for (auto& dep : next.dependencies)
            if (tasks_[dep].status != TaskStatus::COMPLETED) { all_done = false; break; }
        if (all_done) {
            next.status = TaskStatus::READY;
            ready_queue_.push(next_id);
            cv_.notify_one();
        }
    }
}

void DAGEngine::pause()  { paused_ = true; }
void DAGEngine::resume() { paused_ = false; cv_.notify_all(); }
void DAGEngine::cancel() { stop_ = true; cv_.notify_all(); }

void DAGEngine::on_task_start(std::function<void(const Task&)> cb)    { on_start_cb_ = cb; }
void DAGEngine::on_task_complete(std::function<void(const Task&, const TaskResult&)> cb) { on_complete_cb_ = cb; }
void DAGEngine::on_task_fail(std::function<void(const Task&, const std::string&)> cb)    { on_fail_cb_ = cb; }
void DAGEngine::on_dag_complete(std::function<void(const DAGStats&)> cb) { on_dag_complete_cb_ = cb; }

DAGStats DAGEngine::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    DAGStats s{};
    s.total_tasks = tasks_.size();
    for (auto& [id, t] : tasks_) {
        if (t.status == TaskStatus::COMPLETED) s.completed++;
        else if (t.status == TaskStatus::FAILED)  s.failed++;
        else if (t.status == TaskStatus::RUNNING)  s.running++;
        else s.pending++;
    }
    s.progress_pct = s.total_tasks > 0 ? 100.0 * s.completed / s.total_tasks : 0.0;
    s.elapsed_seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - dag_start_).count();
    return s;
}

TaskStatus DAGEngine::get_task_status(const std::string& id) const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto it = tasks_.find(id);
    return it != tasks_.end() ? it->second.status : TaskStatus::PENDING;
}

std::vector<Task> DAGEngine::get_all_tasks() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    std::vector<Task> out;
    for (auto& [_, t] : tasks_) out.push_back(t);
    return out;
}

// ─── VLSI Flow Builder ───────────────────────────────────────────────────────

static PPAMetrics simulate_ppa(const std::string& stage) {
    std::mt19937 rng(std::hash<std::string>{}(stage));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    PPAMetrics m{};
    if (stage == "SYNTH") {
        m.power_mw = 45.0 + dist(rng) * 15.0;
        m.performance_ns = 2.1 + dist(rng) * 0.4;
        m.area_um2 = 12000 + dist(rng) * 3000;
        m.wns = -0.3 + dist(rng) * 0.5;
        m.tns = -12.0 + dist(rng) * 20.0;
        m.drc_violations = 0;
        m.utilization = 65.0 + dist(rng) * 10.0;
    } else if (stage == "PNR") {
        m.power_mw = 52.0 + dist(rng) * 10.0;
        m.performance_ns = 2.3 + dist(rng) * 0.3;
        m.area_um2 = 14500 + dist(rng) * 2000;
        m.wns = -0.1 + dist(rng) * 0.3;
        m.tns = -3.0 + dist(rng) * 6.0;
        m.drc_violations = (int)(dist(rng) * 50);
        m.utilization = 72.0 + dist(rng) * 8.0;
    } else if (stage == "STA") {
        m.power_mw = 52.0;
        m.performance_ns = 2.28;
        m.area_um2 = 14500;
        m.wns = 0.05 + dist(rng) * 0.1;
        m.tns = 0.0;
        m.drc_violations = 0;
        m.utilization = 72.5;
    } else if (stage == "DRC") {
        m.drc_violations = (int)(dist(rng) * 5);
        m.power_mw = 52.0;
        m.performance_ns = 2.28;
        m.area_um2 = 14500;
    }
    return m;
}

std::vector<Task> VLSIFlowBuilder::build_rtl2gds_flow(
    const std::string& design_name,
    const std::string& /*rtl_path*/,
    const std::string& /*pdk*/,
    const std::string& target_freq_mhz)
{
    std::vector<Task> tasks;

    tasks.push_back(make_rtl_lint_task(design_name));
    tasks.push_back(make_synthesis_task(design_name, target_freq_mhz));
    tasks.push_back(make_floorplan_task(design_name));
    tasks.push_back(make_placement_task(design_name));
    tasks.push_back(make_cts_task(design_name));
    tasks.push_back(make_routing_task(design_name));
    tasks.push_back(make_sta_task(design_name));
    tasks.push_back(make_drc_task(design_name));
    tasks.push_back(make_lvs_task(design_name));
    tasks.push_back(make_gds_export_task(design_name));

    return tasks;
}

Task VLSIFlowBuilder::make_rtl_lint_task(const std::string& design) {
    Task t;
    t.id = "rtl_lint";
    t.name = "RTL Lint Check";
    t.stage = "RTL";
    t.tool = "verilator";
    t.tcl_script = "proc run_lint {} {\n  set rtl_files [glob " + design + "/*.v]\n  verilator --lint-only -Wall $rtl_files\n}";
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
        return {true, "RTL lint passed. 0 errors, 2 warnings in " + design, "", {}, 0, "/logs/lint.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_synthesis_task(const std::string& design, const std::string& freq) {
    Task t;
    t.id = "synthesis";
    t.name = "Logic Synthesis";
    t.stage = "SYNTH";
    t.tool = "yosys";
    t.tcl_script = "# Yosys synthesis script\nyosys -import\nread_verilog " + design + ".v\nsynth -top " + design + " -flatten\ndfflibmap -liberty sky130.lib\nabc -liberty sky130.lib -constr constr_" + freq + "MHz.sdc\nwrite_verilog -noattr synth_out.v";
    t.dependencies = {"rtl_lint"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        auto m = simulate_ppa("SYNTH");
        std::ostringstream out;
        out << "Synthesis complete: " << design << "\n"
            << "  Cells: 4821  Wires: 5234  Ports: 128\n"
            << "  Estimated area: " << (int)m.area_um2 << " um2\n"
            << "  Clock period achievable: " << m.performance_ns << " ns";
        return {true, out.str(), "", m, 0, "/logs/synth.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_floorplan_task(const std::string& design) {
    Task t;
    t.id = "floorplan";
    t.name = "Floorplanning";
    t.stage = "PNR";
    t.tool = "openroad";
    t.tcl_script = "read_lef sky130.lef\nread_def " + design + "_synth.def\ninit_floorplan -utilization 75 -aspect_ratio 1 -core_space 2\nplace_pins -hor_layers metal2 -ver_layers metal3\ntapcell -tapcell_master sky130_tap -endcap_master sky130_endcap";
    t.dependencies = {"synthesis"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        return {true, "Floorplan complete. Die area: 200x200 um. Core utilization: 75%", "", {}, 0, "/logs/fp.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_placement_task(const std::string& design) {
    Task t;
    t.id = "placement";
    t.name = "Cell Placement";
    t.stage = "PNR";
    t.tool = "openroad";
    t.tcl_script = "global_placement -density 0.75\nlegalize_placement\ndetailed_placement\ncheck_placement -verbose";
    t.dependencies = {"floorplan"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        return {true, "Placement complete. 4821 cells placed. HPWL: 892450 um", "", {}, 0, "/logs/place.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_cts_task(const std::string& design) {
    Task t;
    t.id = "cts";
    t.name = "Clock Tree Synthesis";
    t.stage = "PNR";
    t.tool = "openroad";
    t.tcl_script = "clock_tree_synthesis -root_buf sky130_buf_4 -buf_list {sky130_buf_2 sky130_buf_4 sky130_buf_8}\nset_propagated_clock [all_clocks]\nestimate_parasitics -placement";
    t.dependencies = {"placement"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        return {true, "CTS complete. Skew: 42ps. Max latency: 318ps. Buffers inserted: 128", "", {}, 0, "/logs/cts.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_routing_task(const std::string& design) {
    Task t;
    t.id = "routing";
    t.name = "Detailed Routing";
    t.stage = "PNR";
    t.tool = "openroad";
    t.tcl_script = "global_route -guide_file route.guide -overflow_iterations 100\ndetailed_route -output_drc " + design + "_drc.rpt -output_maze route_maze.log\ncheck_antennas -report_file antenna.rpt";
    t.dependencies = {"cts"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(4000));
        auto m = simulate_ppa("PNR");
        std::ostringstream out;
        out << "Routing complete. Nets routed: 5234/5234 (100%)\n"
            << "  DRC violations: " << m.drc_violations << "\n"
            << "  Antenna violations: 3 (fixable)\n"
            << "  Via count: 18472";
        return {true, out.str(), "", m, 0, "/logs/route.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_sta_task(const std::string& design) {
    Task t;
    t.id = "sta";
    t.name = "Static Timing Analysis";
    t.stage = "STA";
    t.tool = "opensta";
    t.tcl_script = "read_liberty sky130_tt.lib\nread_def " + design + "_routed.def\ncreate_clock -name clk -period 2.5 [get_ports clk]\nreport_checks -path_delay max -format full_clock_expanded\nreport_wns\nreport_tns\nreport_power";
    t.dependencies = {"routing"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        auto m = simulate_ppa("STA");
        std::ostringstream out;
        out << "STA complete for " << design << "\n"
            << "  WNS: " << m.wns << " ns  (TIMING MET)\n"
            << "  TNS: " << m.tns << " ns\n"
            << "  Power: " << m.power_mw << " mW\n"
            << "  Critical path: FF->NAND->NOR->FF (2.18 ns)";
        return {true, out.str(), "", m, 0, "/logs/sta.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_drc_task(const std::string& design) {
    Task t;
    t.id = "drc";
    t.name = "Design Rule Check";
    t.stage = "DRC";
    t.tool = "magic";
    t.tcl_script = "drc check\ndrc why\ndrc count\nsave " + design + "_drc.mag";
    t.dependencies = {"routing"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        auto m = simulate_ppa("DRC");
        std::ostringstream out;
        out << "DRC complete. Violations: " << m.drc_violations << "\n";
        if (m.drc_violations == 0)
            out << "  All rules passed: metal spacing, via enclosure, density";
        else
            out << "  " << m.drc_violations << " metal4 spacing violations (auto-fixable)";
        return {m.drc_violations < 3, out.str(), m.drc_violations >= 3 ? "Too many DRC violations" : "", m, 0, "/logs/drc.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_lvs_task(const std::string& design) {
    Task t;
    t.id = "lvs";
    t.name = "Layout vs Schematic";
    t.stage = "DRC";
    t.tool = "netgen";
    t.tcl_script = "netgen -batch lvs {" + design + ".spice " + design + "} {sky130.spice sky130} " + design + "_lvs.out";
    t.dependencies = {"drc", "sta"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        return {true, "LVS CLEAN: Netlists match. Devices: 4821. Nets: 5234.", "", {}, 0, "/logs/lvs.log"};
    };
    return t;
}

Task VLSIFlowBuilder::make_gds_export_task(const std::string& design) {
    Task t;
    t.id = "gds_export";
    t.name = "GDS-II Export";
    t.stage = "DRC";
    t.tool = "klayout";
    t.tcl_script = "stream out -format GDS -verbose " + design + ".gds\nreport stream out complete";
    t.dependencies = {"lvs"};
    t.executor = [design]() -> TaskResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        return {true, "GDS-II exported: " + design + ".gds (14.2 MB)\n  Layers: 33  Cells: 4821\n  RTL2GDS FLOW COMPLETE ✓", {}, {}, 0, "/logs/gds.log"};
    };
    return t;
}

} // namespace autoflow
