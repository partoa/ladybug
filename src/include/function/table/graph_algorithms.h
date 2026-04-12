#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace lbug {
namespace function {

// Directed graph with forward and reverse adjacency lists.
struct AdjEdge {
    uint64_t to;
    double weight;
};

struct AdjGraph {
    uint64_t numNodes = 0;
    std::vector<std::vector<AdjEdge>> fwd;
    std::vector<std::vector<AdjEdge>> bwd;

    explicit AdjGraph(uint64_t n) : numNodes(n), fwd(n), bwd(n) {}

    void addEdge(uint64_t from, uint64_t to, double w = 1.0) {
        if (from < numNodes && to < numNodes) {
            fwd[from].push_back({to, w});
            bwd[to].push_back({from, w});
        }
    }
};

// 1. Dominator tree (Cooper-Harvey-Kennedy iterative algorithm)
struct DominatorResult {
    std::vector<int64_t> idom; // idom[v] = immediate dominator, -1 for root/unreachable
    uint64_t root;
};
DominatorResult computeDominators(const AdjGraph& graph, uint64_t entry);

// 2. Feedback arc set (Eades-Lin-Smyth greedy heuristic)
struct FASResult {
    std::vector<std::pair<uint64_t, uint64_t>> feedbackArcs;
    std::vector<uint64_t> topoOrder;
};
FASResult computeFeedbackArcSet(const AdjGraph& graph);

// 3. Articulation points and bridges (Tarjan's, iterative)
struct BiconnectivityResult {
    std::vector<uint64_t> articulationPoints;
    std::vector<std::pair<uint64_t, uint64_t>> bridges;
};
BiconnectivityResult computeArticulationAndBridges(const AdjGraph& graph);

// 4. Levenshtein edit distance
int64_t levenshteinDistance(const std::string& a, const std::string& b);
double structuralSimilarity(const std::string& a, const std::string& b);

// 5. k-hop subgraph extraction (BFS)
struct KHopResult {
    std::vector<uint64_t> nodes;
    std::vector<int64_t> distances;
};
KHopResult extractKHop(const AdjGraph& graph, uint64_t center, int64_t k);

// 6. Call path enumeration (bounded DFS)
struct CallPath {
    std::vector<uint64_t> nodes;
};
std::vector<CallPath> enumerateCallPaths(const AdjGraph& graph,
    uint64_t source, uint64_t target, uint64_t maxDepth = 15);

// 7. Constrained reachability (3x BFS)
struct ReachabilityResult {
    bool reachable;
    bool reachableThrough;
    int64_t shortestPathLength;
    int64_t shortestConstrainedPathLength;
};
ReachabilityResult constrainedReachability(const AdjGraph& graph,
    uint64_t source, uint64_t target, uint64_t mustPass);

} // namespace function
} // namespace lbug
