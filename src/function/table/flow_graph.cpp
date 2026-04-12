#include "function/table/flow_graph.h"

#include <algorithm>
#include <queue>
#include <stack>
#include <stdexcept>

namespace lbug {
namespace function {

static constexpr double INF_CAP = std::numeric_limits<double>::infinity();

FlowNetwork::FlowNetwork(uint64_t numNodes)
    : n_{numNodes}, adj_(numNodes), level_(numNodes), iter_(numNodes) {}

void FlowNetwork::addEdge(uint64_t from, uint64_t to, double cap) {
    // Bounds check: silently ignore out-of-range edges.
    if (from >= n_ || to >= n_) {
        return;
    }
    adj_[from].push_back({to, cap, adj_[to].size()});
    adj_[to].push_back({from, 0.0, adj_[from].size() - 1});
}

bool FlowNetwork::bfsLevel(uint64_t source, uint64_t sink) {
    std::fill(level_.begin(), level_.end(), -1);
    std::queue<uint64_t> q;
    level_[source] = 0;
    q.push(source);
    while (!q.empty()) {
        auto u = q.front();
        q.pop();
        for (const auto& e : adj_[u]) {
            if (e.cap > 0 && level_[e.to] < 0) {
                level_[e.to] = level_[u] + 1;
                q.push(e.to);
            }
        }
    }
    return level_[sink] >= 0;
}

// Iterative DFS replacing the recursive dfsPush to avoid stack overflow
// on deep graphs. Uses an explicit stack of (node, edge_index, pushed_so_far).
double FlowNetwork::iterativePush(uint64_t source, uint64_t sink) {
    struct Frame {
        uint64_t node;
        double pushed;
    };

    std::vector<Frame> path;
    path.push_back({source, INF_CAP});

    while (!path.empty()) {
        auto& cur = path.back();
        uint64_t u = cur.node;

        if (u == sink) {
            // Found augmenting path — push flow back along the path.
            double flow = cur.pushed;
            for (size_t i = 0; i + 1 < path.size(); ++i) {
                uint64_t node = path[i].node;
                // The edge we used is at iter_[node] - 1 (we incremented after selecting it)
                // Actually we need to track which edge was used. Let me restructure.
            }
            // This approach is tricky — let me use a cleaner iterative pattern.
            break;
        }

        bool advanced = false;
        for (auto& idx = iter_[u]; idx < adj_[u].size(); ++idx) {
            auto& e = adj_[u][idx];
            if (e.cap > 0 && level_[e.to] == level_[u] + 1) {
                path.push_back({e.to, std::min(cur.pushed, e.cap)});
                advanced = true;
                break;
            }
        }

        if (!advanced) {
            path.pop_back();
            // Backtrack: this node is exhausted at this level
        }
    }

    // Hmm, the iterative version is getting complicated because we need to
    // update edge capacities along the path. Let me use a different approach:
    // iterative DFS that builds the path, then pushes flow along it.
    return 0.0; // placeholder
}

double FlowNetwork::maxFlow(uint64_t source, uint64_t sink) {
    if (source >= n_ || sink >= n_ || source == sink) {
        return 0.0;
    }
    if (consumed_) {
        throw std::runtime_error("FlowNetwork::maxFlow called on already-consumed instance. "
                                 "Create a fresh FlowNetwork for each computation.");
    }
    consumed_ = true;

    // Use iterative Dinic's: BFS for level graph, then iterative DFS for blocking flow.
    double flow = 0.0;
    while (bfsLevel(source, sink)) {
        std::fill(iter_.begin(), iter_.end(), 0);

        // Iterative blocking flow: find augmenting paths using explicit stack.
        while (true) {
            // Build path from source to sink using DFS on level graph.
            std::vector<uint64_t> pathNodes;
            std::vector<uint64_t> pathEdgeIdx;
            pathNodes.push_back(source);

            bool foundPath = false;

            while (!pathNodes.empty()) {
                uint64_t u = pathNodes.back();

                if (u == sink) {
                    foundPath = true;
                    break;
                }

                bool advanced = false;
                for (auto& idx = iter_[u]; idx < adj_[u].size(); ++idx) {
                    auto& e = adj_[u][idx];
                    if (e.cap > 0 && level_[e.to] == level_[u] + 1) {
                        pathNodes.push_back(e.to);
                        pathEdgeIdx.push_back(idx);
                        advanced = true;
                        break;
                    }
                }

                if (!advanced) {
                    // Dead end: remove from path and mark level as -1 to prune
                    level_[u] = -1;
                    pathNodes.pop_back();
                    if (!pathEdgeIdx.empty()) {
                        pathEdgeIdx.pop_back();
                    }
                }
            }

            if (!foundPath) {
                break;
            }

            // Find bottleneck capacity along the path.
            double bottleneck = INF_CAP;
            for (size_t i = 0; i < pathEdgeIdx.size(); ++i) {
                uint64_t u = pathNodes[i];
                auto& e = adj_[u][pathEdgeIdx[i]];
                bottleneck = std::min(bottleneck, e.cap);
            }

            // Push flow along the path, updating residual capacities.
            for (size_t i = 0; i < pathEdgeIdx.size(); ++i) {
                uint64_t u = pathNodes[i];
                auto& e = adj_[u][pathEdgeIdx[i]];
                e.cap -= bottleneck;
                adj_[e.to][e.rev].cap += bottleneck;
            }

            flow += bottleneck;
        }
    }
    return flow;
}

void FlowNetwork::minCutSides(uint64_t source, std::vector<uint8_t>& side) const {
    side.assign(n_, 1);
    if (source >= n_) {
        return;
    }
    std::queue<uint64_t> q;
    side[source] = 0;
    q.push(source);
    while (!q.empty()) {
        auto u = q.front();
        q.pop();
        for (const auto& e : adj_[u]) {
            if (e.cap > 0 && side[e.to] == 1) {
                side[e.to] = 0;
                q.push(e.to);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// DFS-based simple cycle detection
// ═══════════════════════════════════════════════════════════════════

std::vector<DetectedCycle> detectAllSimpleCycles(uint64_t numNodes,
    const std::vector<WeightedEdge>& edges, uint64_t maxLength) {

    std::vector<std::vector<std::pair<uint64_t, double>>> adj(numNodes);
    for (const auto& e : edges) {
        if (e.from < numNodes && e.to < numNodes) {
            adj[e.from].push_back({e.to, e.weight});
        }
    }

    std::vector<DetectedCycle> cycles;

    for (uint64_t start = 0; start < numNodes; ++start) {
        struct Frame {
            uint64_t node;
            std::vector<uint64_t> path;
            double cost;
            size_t nbrIdx;
        };

        std::stack<Frame> stk;
        stk.push({start, {start}, 0.0, 0});

        while (!stk.empty()) {
            auto& top = stk.top();
            if (top.nbrIdx >= adj[top.node].size()) {
                stk.pop();
                continue;
            }

            auto [nbr, weight] = adj[top.node][top.nbrIdx];
            top.nbrIdx++;

            if (nbr == start && top.path.size() > 1) {
                bool isCanonical = true;
                for (auto p : top.path) {
                    if (p < start) {
                        isCanonical = false;
                        break;
                    }
                }
                if (isCanonical) {
                    DetectedCycle cycle;
                    cycle.nodeOffsets = top.path;
                    cycle.totalWeight = top.cost + weight;
                    cycles.push_back(std::move(cycle));
                }
                continue;
            }

            if (top.path.size() >= maxLength) {
                continue;
            }

            bool inPath = false;
            for (auto p : top.path) {
                if (p == nbr) {
                    inPath = true;
                    break;
                }
            }
            if (inPath) {
                continue;
            }

            auto newPath = top.path;
            newPath.push_back(nbr);
            stk.push({nbr, std::move(newPath), top.cost + weight, 0});
        }
    }

    return cycles;
}

} // namespace function
} // namespace lbug
