#include "function/table/flow_graph.h"

#include <algorithm>
#include <queue>
#include <stack>

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════════
// FlowNetwork — Dinic's Algorithm
// ═══════════════════════════════════════════════════════════════════

static constexpr double INF_CAP = std::numeric_limits<double>::infinity();

FlowNetwork::FlowNetwork(uint64_t numNodes)
    : n_{numNodes}, adj_(numNodes), level_(numNodes), iter_(numNodes) {}

void FlowNetwork::addEdge(uint64_t from, uint64_t to, double cap) {
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

double FlowNetwork::dfsPush(uint64_t u, uint64_t sink, double pushed) {
    if (u == sink) {
        return pushed;
    }
    for (auto& i = iter_[u]; i < adj_[u].size(); ++i) {
        auto& e = adj_[u][i];
        if (e.cap > 0 && level_[e.to] == level_[u] + 1) {
            double d = dfsPush(e.to, sink, std::min(pushed, e.cap));
            if (d > 0) {
                e.cap -= d;
                adj_[e.to][e.rev].cap += d;
                return d;
            }
        }
    }
    return 0.0;
}

double FlowNetwork::maxFlow(uint64_t source, uint64_t sink) {
    if (source == sink) {
        return 0.0;
    }
    double flow = 0.0;
    while (bfsLevel(source, sink)) {
        std::fill(iter_.begin(), iter_.end(), 0);
        while (true) {
            double f = dfsPush(source, sink, INF_CAP);
            if (f <= 0) {
                break;
            }
            flow += f;
        }
    }
    return flow;
}

void FlowNetwork::minCutSides(uint64_t source, std::vector<uint8_t>& side) const {
    side.assign(n_, 1); // default: sink side
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

    // Build adjacency list
    std::vector<std::vector<std::pair<uint64_t, double>>> adj(numNodes);
    for (const auto& e : edges) {
        if (e.from < numNodes && e.to < numNodes) {
            adj[e.from].push_back({e.to, e.weight});
        }
    }

    std::vector<DetectedCycle> cycles;

    for (uint64_t start = 0; start < numNodes; ++start) {
        // DFS from start, looking for paths back to start.
        // Only report cycles where start is the smallest node ID to avoid duplicates.
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
                // Found a cycle. Only report if start is the minimum node in the cycle
                // to avoid reporting the same cycle from every starting node.
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

            // Check not already in path
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
