#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <memory>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include <optional>

namespace autoflow {

enum class TaskStatus {
    PENDING,
    READY,
    RUNNING,
    COMPLETED,
    FAILED,
    SKIPPED
};

enum class TaskPriority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2,
    CRITICAL = 3
};

struct PPAMetrics {
    double power_mw;       // Power in milliwatts
    double performance_ns; // Critical path delay in nanoseconds
    double area_um2;       // Area in square micrometers
    double wns;            // Worst Negative Slack
    double tns;            // Total Negative Slack
    int    drc_violations; // Design Rule Check violations
    double utilization;    // Cell utilization %
};

struct TaskResult {
    bool        success;
    std::string output;
    std::string error;
    PPAMetrics  metrics;
    double      runtime_seconds;
    std::string log_path;
};

struct Task {
    std::string              id;
    std::string              name;
    std::string              stage;        // RTL, SYNTH, PNR, STA, DRC
    std::vector<std::string> dependencies;
    std::function<TaskResult()> executor;
    TaskStatus               status{TaskStatus::PENDING};
    TaskPriority             priority{TaskPriority::NORMAL};
    std::optional<TaskResult> result;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
    int                      retry_count{0};
    int                      max_retries{2};
    std::string              tcl_script;
    std::string              tool;         // yosys, openroad, etc.
};

struct DAGStats {
    int total_tasks;
    int completed;
    int failed;
    int running;
    int pending;
    double progress_pct;
    double elapsed_seconds;
    PPAMetrics current_ppa;
};

class DAGEngine {
public:
    explicit DAGEngine(int num_threads = 4);
    ~DAGEngine();

    // Task management
    void add_task(Task task);
    void add_dependency(const std::string& task_id, const std::string& dep_id);
    bool validate_dag(); // Check for cycles using DFS

    // Execution control
    void run();
    void pause();
    void resume();
    void cancel();

    // Callbacks
    void on_task_start(std::function<void(const Task&)> cb);
    void on_task_complete(std::function<void(const Task&, const TaskResult&)> cb);
    void on_task_fail(std::function<void(const Task&, const std::string&)> cb);
    void on_dag_complete(std::function<void(const DAGStats&)> cb);

    // Status
    DAGStats get_stats() const;
    TaskStatus get_task_status(const std::string& task_id) const;
    std::vector<Task> get_all_tasks() const;
    std::vector<std::string> get_critical_path() const;

private:
    std::unordered_map<std::string, Task>              tasks_;
    std::unordered_map<std::string, std::vector<std::string>> adj_; // adjacency list
    std::unordered_map<std::string, int>               in_degree_;

    std::vector<std::thread>                           workers_;
    std::queue<std::string>                            ready_queue_;
    std::mutex                                         queue_mutex_;
    std::condition_variable                            cv_;
    std::atomic<bool>                                  stop_{false};
    std::atomic<bool>                                  paused_{false};
    std::atomic<int>                                   active_tasks_{0};
    int                                                num_threads_;

    std::function<void(const Task&)>                             on_start_cb_;
    std::function<void(const Task&, const TaskResult&)>         on_complete_cb_;
    std::function<void(const Task&, const std::string&)>        on_fail_cb_;
    std::function<void(const DAGStats&)>                        on_dag_complete_cb_;

    std::chrono::steady_clock::time_point              dag_start_;
    mutable std::mutex                                 stats_mutex_;

    void worker_thread();
    void execute_task(Task& task);
    void update_ready_queue(const std::string& completed_id);
    std::vector<std::string> topological_sort();
    bool has_cycle_dfs(const std::string& node,
                       std::unordered_set<std::string>& visited,
                       std::unordered_set<std::string>& rec_stack);
};

// ─── VLSI Stage Executors ────────────────────────────────────────────────────

class VLSIFlowBuilder {
public:
    static std::vector<Task> build_rtl2gds_flow(
        const std::string& design_name,
        const std::string& rtl_path,
        const std::string& pdk,
        const std::string& target_freq_mhz
    );

private:
    static Task make_rtl_lint_task(const std::string& design);
    static Task make_synthesis_task(const std::string& design, const std::string& freq);
    static Task make_floorplan_task(const std::string& design);
    static Task make_placement_task(const std::string& design);
    static Task make_cts_task(const std::string& design); // Clock Tree Synthesis
    static Task make_routing_task(const std::string& design);
    static Task make_sta_task(const std::string& design);  // Static Timing Analysis
    static Task make_drc_task(const std::string& design);  // Design Rule Check
    static Task make_lvs_task(const std::string& design);  // Layout vs Schematic
    static Task make_gds_export_task(const std::string& design);
};

} // namespace autoflow
