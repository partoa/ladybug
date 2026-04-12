#pragma once

#include <cstdint>
#include <limits>
#include <vector>

namespace lbug {
namespace function {

// ═══════════════════════════════════════════════════════════════════
// FlowNetwork — Dinic's max-flow algorithm
//
// Time complexity: O(V^2 * E) general, O(E * sqrt(V)) unit capacity
// ═══════════════════════════════════════════════════════════════════

struct FlowEdge {
    uint64_t to;
    double cap;   // residual capacity
    uint64_t rev; // index of reverse edge in adj[to]
};

class FlowNetwork {
public:
    explicit FlowNetwork(uint64_t numNodes);

    void addEdge(uint64_t from, uint64_t to, double cap);
    double maxFlow(uint64_t source, uint64_t sink);

    // After maxFlow(): side[i] = 0 if reachable from source in residual, 1 otherwise.
    void minCutSides(uint64_t source, std::vector<uint8_t>& side) const;

    uint64_t numNodes() const { return n_; }

private:
    bool bfsLevel(uint64_t source, uint64_t sink);
    double dfsPush(uint64_t u, uint64_t sink, double pushed);

    uint64_t n_;
    std::vector<std::vector<FlowEdge>> adj_;
    std::vector<int64_t> level_;
    std::vector<uint64_t> iter_;
};

// ═══════════════════════════════════════════════════════════════════
// Cycle detection
// ═══════════════════════════════════════════════════════════════════

struct WeightedEdge {
    uint64_t from;
    uint64_t to;
    double weight;
};

struct DetectedCycle {
    std::vector<uint64_t> nodeOffsets;
    double totalWeight;
};

// DFS-based: finds all simple cycles up to maxLength.
// For CPG dependency detection where any cycle is a finding.
std::vector<DetectedCycle> detectAllSimpleCycles(uint64_t numNodes,
    const std::vector<WeightedEdge>& edges, uint64_t maxLength = 10);

} // namespace function
} // namespace lbug
