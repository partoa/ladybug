#include "function/table/graph_algorithms.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <queue>
#include <stack>

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════
// 1. DOMINATORS — Cooper-Harvey-Kennedy iterative
// ═══════════════════════════════════════════════════════════════

DominatorResult computeDominators(const AdjGraph& graph, uint64_t entry) {
    uint64_t n = graph.numNodes;
    DominatorResult result;
    result.root = entry;
    result.idom.assign(n, -1);
    if (entry >= n) return result;

    // DFS for reverse postorder
    std::vector<bool> visited(n, false);
    std::vector<uint64_t> postorder;
    {
        std::stack<std::pair<uint64_t, size_t>> stk;
        stk.push({entry, 0});
        visited[entry] = true;
        while (!stk.empty()) {
            auto& [node, idx] = stk.top();
            if (idx < graph.fwd[node].size()) {
                auto next = graph.fwd[node][idx].to;
                idx++;
                if (!visited[next]) {
                    visited[next] = true;
                    stk.push({next, 0});
                }
            } else {
                postorder.push_back(node);
                stk.pop();
            }
        }
    }

    std::vector<uint64_t> rpo(postorder.rbegin(), postorder.rend());
    std::vector<int64_t> rpoNum(n, -1);
    for (uint64_t i = 0; i < rpo.size(); ++i) {
        rpoNum[rpo[i]] = static_cast<int64_t>(i);
    }

    std::vector<int64_t> doms(n, -1);
    doms[entry] = static_cast<int64_t>(entry);

    auto intersect = [&](int64_t b1, int64_t b2) -> int64_t {
        while (b1 != b2) {
            while (rpoNum[b1] > rpoNum[b2]) b1 = doms[b1];
            while (rpoNum[b2] > rpoNum[b1]) b2 = doms[b2];
        }
        return b1;
    };

    bool changed = true;
    while (changed) {
        changed = false;
        for (auto b : rpo) {
            if (b == entry) continue;
            int64_t newIdom = -1;
            for (const auto& pred : graph.bwd[b]) {
                auto p = static_cast<int64_t>(pred.to);
                if (doms[p] != -1) {
                    newIdom = (newIdom == -1) ? p : intersect(newIdom, p);
                }
            }
            if (newIdom != -1 && doms[b] != newIdom) {
                doms[b] = newIdom;
                changed = true;
            }
        }
    }

    result.idom = doms;
    return result;
}

// ═══════════════════════════════════════════════════════════════
// 2. FEEDBACK ARC SET — Eades-Lin-Smyth
// ═══════════════════════════════════════════════════════════════

FASResult computeFeedbackArcSet(const AdjGraph& graph) {
    uint64_t n = graph.numNodes;
    FASResult result;
    if (n == 0) return result;

    std::vector<int64_t> inDeg(n, 0), outDeg(n, 0);
    std::vector<bool> removed(n, false);

    for (uint64_t u = 0; u < n; ++u) {
        outDeg[u] = static_cast<int64_t>(graph.fwd[u].size());
        for (const auto& e : graph.fwd[u]) inDeg[e.to]++;
    }

    std::vector<uint64_t> leftList, rightList;
    uint64_t remaining = n;

    while (remaining > 0) {
        bool found = true;
        while (found) {
            found = false;
            for (uint64_t u = 0; u < n; ++u) {
                if (!removed[u] && outDeg[u] == 0) {
                    rightList.push_back(u);
                    removed[u] = true;
                    remaining--;
                    found = true;
                    for (const auto& e : graph.bwd[u])
                        if (!removed[e.to]) outDeg[e.to]--;
                }
            }
        }
        found = true;
        while (found) {
            found = false;
            for (uint64_t u = 0; u < n; ++u) {
                if (!removed[u] && inDeg[u] == 0) {
                    leftList.push_back(u);
                    removed[u] = true;
                    remaining--;
                    found = true;
                    for (const auto& e : graph.fwd[u])
                        if (!removed[e.to]) inDeg[e.to]--;
                }
            }
        }
        if (remaining > 0) {
            int64_t bestDelta = std::numeric_limits<int64_t>::min();
            uint64_t bestNode = 0;
            for (uint64_t u = 0; u < n; ++u) {
                if (!removed[u] && outDeg[u] - inDeg[u] > bestDelta) {
                    bestDelta = outDeg[u] - inDeg[u];
                    bestNode = u;
                }
            }
            leftList.push_back(bestNode);
            removed[bestNode] = true;
            remaining--;
            for (const auto& e : graph.fwd[bestNode])
                if (!removed[e.to]) inDeg[e.to]--;
            for (const auto& e : graph.bwd[bestNode])
                if (!removed[e.to]) outDeg[e.to]--;
        }
    }

    std::reverse(rightList.begin(), rightList.end());
    result.topoOrder.insert(result.topoOrder.end(), leftList.begin(), leftList.end());
    result.topoOrder.insert(result.topoOrder.end(), rightList.begin(), rightList.end());

    std::vector<uint64_t> pos(n);
    for (uint64_t i = 0; i < result.topoOrder.size(); ++i)
        pos[result.topoOrder[i]] = i;

    for (uint64_t u = 0; u < n; ++u)
        for (const auto& e : graph.fwd[u])
            if (pos[u] >= pos[e.to])
                result.feedbackArcs.push_back({u, e.to});

    return result;
}

// ═══════════════════════════════════════════════════════════════
// 3. ARTICULATION POINTS & BRIDGES — Tarjan's (iterative)
// ═══════════════════════════════════════════════════════════════

BiconnectivityResult computeArticulationAndBridges(const AdjGraph& graph) {
    uint64_t n = graph.numNodes;
    BiconnectivityResult result;
    if (n == 0) return result;

    std::vector<int64_t> disc(n, -1), low(n, -1), parent(n, -1);
    int64_t timer = 0;

    for (uint64_t start = 0; start < n; ++start) {
        if (disc[start] >= 0) continue;

        struct Frame {
            uint64_t node;
            size_t edgeIdx;
            int64_t childCount;
        };
        std::stack<Frame> stk;
        disc[start] = low[start] = timer++;
        stk.push({start, 0, 0});

        while (!stk.empty()) {
            auto& [u, idx, childCount] = stk.top();
            bool pushed = false;

            while (idx < graph.fwd[u].size()) {
                auto v = graph.fwd[u][idx].to;
                idx++;
                if (disc[v] < 0) {
                    parent[v] = static_cast<int64_t>(u);
                    disc[v] = low[v] = timer++;
                    childCount++;
                    stk.push({v, 0, 0});
                    pushed = true;
                    break;
                } else if (v != static_cast<uint64_t>(parent[u])) {
                    low[u] = std::min(low[u], disc[v]);
                }
            }

            if (!pushed) {
                stk.pop();
                if (!stk.empty()) {
                    auto& par = stk.top();
                    low[par.node] = std::min(low[par.node], low[u]);
                    if (low[u] > disc[par.node])
                        result.bridges.push_back({par.node, u});
                    if (parent[par.node] == -1 && par.childCount > 1)
                        result.articulationPoints.push_back(par.node);
                    else if (parent[par.node] != -1 && low[u] >= disc[par.node])
                        result.articulationPoints.push_back(par.node);
                }
            }
        }
    }

    std::sort(result.articulationPoints.begin(), result.articulationPoints.end());
    result.articulationPoints.erase(
        std::unique(result.articulationPoints.begin(), result.articulationPoints.end()),
        result.articulationPoints.end());

    return result;
}

// ═══════════════════════════════════════════════════════════════
// 4. LEVENSHTEIN EDIT DISTANCE
// ═══════════════════════════════════════════════════════════════

int64_t levenshteinDistance(const std::string& a, const std::string& b) {
    size_t m = a.size(), n = b.size();
    std::vector<int64_t> prev(n + 1), curr(n + 1);
    for (size_t j = 0; j <= n; ++j) prev[j] = static_cast<int64_t>(j);
    for (size_t i = 1; i <= m; ++i) {
        curr[0] = static_cast<int64_t>(i);
        for (size_t j = 1; j <= n; ++j) {
            int64_t cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
            curr[j] = std::min({prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost});
        }
        std::swap(prev, curr);
    }
    return prev[n];
}

double structuralSimilarity(const std::string& a, const std::string& b) {
    if (a.empty() && b.empty()) return 1.0;
    auto dist = levenshteinDistance(a, b);
    auto maxLen = static_cast<int64_t>(std::max(a.size(), b.size()));
    return 1.0 - static_cast<double>(dist) / static_cast<double>(maxLen);
}

// ═══════════════════════════════════════════════════════════════
// 5. K-HOP SUBGRAPH — BFS
// ═══════════════════════════════════════════════════════════════

KHopResult extractKHop(const AdjGraph& graph, uint64_t center, int64_t k) {
    KHopResult result;
    if (center >= graph.numNodes) return result;

    std::vector<int64_t> dist(graph.numNodes, -1);
    std::queue<uint64_t> q;
    dist[center] = 0;
    q.push(center);

    while (!q.empty()) {
        auto u = q.front(); q.pop();
        if (dist[u] >= k) continue;
        for (const auto& e : graph.fwd[u]) {
            if (dist[e.to] < 0) {
                dist[e.to] = dist[u] + 1;
                q.push(e.to);
            }
        }
        for (const auto& e : graph.bwd[u]) {
            if (dist[e.to] < 0) {
                dist[e.to] = dist[u] + 1;
                q.push(e.to);
            }
        }
    }

    for (uint64_t i = 0; i < graph.numNodes; ++i) {
        if (dist[i] >= 0) {
            result.nodes.push_back(i);
            result.distances.push_back(dist[i]);
        }
    }
    return result;
}

// ═══════════════════════════════════════════════════════════════
// 6. CALL PATH ENUMERATION — bounded DFS
// ═══════════════════════════════════════════════════════════════

std::vector<CallPath> enumerateCallPaths(const AdjGraph& graph,
    uint64_t source, uint64_t target, uint64_t maxDepth) {
    std::vector<CallPath> results;
    if (source >= graph.numNodes || target >= graph.numNodes) return results;

    struct Frame {
        uint64_t node;
        std::vector<uint64_t> path;
        size_t edgeIdx;
    };

    std::stack<Frame> stk;
    stk.push({source, {source}, 0});

    while (!stk.empty()) {
        auto& top = stk.top();

        if (top.node == target && top.path.size() > 1) {
            results.push_back({top.path});
            stk.pop();
            continue;
        }

        if (top.path.size() > maxDepth || top.edgeIdx >= graph.fwd[top.node].size()) {
            stk.pop();
            continue;
        }

        auto nextNode = graph.fwd[top.node][top.edgeIdx].to;
        top.edgeIdx++;

        bool inPath = false;
        for (auto p : top.path) {
            if (p == nextNode && nextNode != target) {
                inPath = true;
                break;
            }
        }

        if (!inPath) {
            auto newPath = top.path;
            newPath.push_back(nextNode);
            stk.push({nextNode, std::move(newPath), 0});
        }
    }

    return results;
}

// ═══════════════════════════════════════════════════════════════
// 7. CONSTRAINED REACHABILITY — 3x BFS
// ═══════════════════════════════════════════════════════════════

ReachabilityResult constrainedReachability(const AdjGraph& graph,
    uint64_t source, uint64_t target, uint64_t mustPass) {
    ReachabilityResult result = {false, false, -1, -1};
    uint64_t n = graph.numNodes;
    if (source >= n || target >= n || mustPass >= n) return result;

    auto bfs = [&](uint64_t start) -> std::vector<int64_t> {
        std::vector<int64_t> dist(n, -1);
        std::queue<uint64_t> q;
        dist[start] = 0;
        q.push(start);
        while (!q.empty()) {
            auto u = q.front(); q.pop();
            for (const auto& e : graph.fwd[u]) {
                if (dist[e.to] < 0) {
                    dist[e.to] = dist[u] + 1;
                    q.push(e.to);
                }
            }
        }
        return dist;
    };

    auto distFromSource = bfs(source);
    if (distFromSource[target] >= 0) {
        result.reachable = true;
        result.shortestPathLength = distFromSource[target];
    }

    if (distFromSource[mustPass] < 0) return result;

    auto distFromMustPass = bfs(mustPass);
    if (distFromMustPass[target] >= 0) {
        result.reachableThrough = true;
        result.shortestConstrainedPathLength =
            distFromSource[mustPass] + distFromMustPass[target];
    }

    return result;
}

} // namespace function
} // namespace lbug
