#include "function/table/graph_algorithms.h"
#include <cassert>
#include <cmath>
#include <iostream>

using namespace lbug::function;

// ═══════════════════════════════════════════════════════════════
// DOMINATORS
// ═══════════════════════════════════════════════════════════════

void testDomSimpleChain() {
    // 0→1→2→3: idom(1)=0, idom(2)=1, idom(3)=2
    AdjGraph g(4);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 3);
    auto r = computeDominators(g, 0);
    assert(r.idom[0] == 0);
    assert(r.idom[1] == 0);
    assert(r.idom[2] == 1);
    assert(r.idom[3] == 2);
    std::cout << "PASS: testDomSimpleChain\n";
}

void testDomDiamond() {
    // 0→1, 0→2, 1→3, 2→3: idom(3)=0 (both paths merge)
    AdjGraph g(4);
    g.addEdge(0, 1); g.addEdge(0, 2); g.addEdge(1, 3); g.addEdge(2, 3);
    auto r = computeDominators(g, 0);
    assert(r.idom[3] == 0);
    std::cout << "PASS: testDomDiamond\n";
}

void testDomUnreachable() {
    AdjGraph g(4);
    g.addEdge(0, 1);
    // 2, 3 unreachable from 0
    auto r = computeDominators(g, 0);
    assert(r.idom[2] == -1);
    assert(r.idom[3] == -1);
    std::cout << "PASS: testDomUnreachable\n";
}

void testDomEmpty() {
    AdjGraph g(0);
    auto r = computeDominators(g, 0);
    assert(r.idom.empty());
    std::cout << "PASS: testDomEmpty\n";
}

// ═══════════════════════════════════════════════════════════════
// FEEDBACK ARC SET
// ═══════════════════════════════════════════════════════════════

void testFASAcyclic() {
    AdjGraph g(3);
    g.addEdge(0, 1); g.addEdge(1, 2);
    auto r = computeFeedbackArcSet(g);
    assert(r.feedbackArcs.empty());
    std::cout << "PASS: testFASAcyclic\n";
}

void testFASTriangleCycle() {
    AdjGraph g(3);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 0);
    auto r = computeFeedbackArcSet(g);
    assert(r.feedbackArcs.size() == 1); // need to remove 1 edge to break cycle
    std::cout << "PASS: testFASTriangleCycle\n";
}

void testFASEmpty() {
    AdjGraph g(0);
    auto r = computeFeedbackArcSet(g);
    assert(r.feedbackArcs.empty());
    assert(r.topoOrder.empty());
    std::cout << "PASS: testFASEmpty\n";
}

void testFASSelfLoop() {
    AdjGraph g(2);
    g.addEdge(0, 0); // self-loop
    g.addEdge(0, 1);
    auto r = computeFeedbackArcSet(g);
    // Self-loop is a feedback arc
    bool hasSelfLoop = false;
    for (auto& [u, v] : r.feedbackArcs) {
        if (u == 0 && v == 0) hasSelfLoop = true;
    }
    assert(hasSelfLoop);
    std::cout << "PASS: testFASSelfLoop\n";
}

// ═══════════════════════════════════════════════════════════════
// ARTICULATION POINTS & BRIDGES
// ═══════════════════════════════════════════════════════════════

void testAPSimpleBridge() {
    // Path: 0→1→2→3. Node 1 and 2 are articulation points.
    // All edges are bridges.
    AdjGraph g(4);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 3);
    auto r = computeArticulationAndBridges(g);
    assert(!r.articulationPoints.empty());
    assert(!r.bridges.empty());
    std::cout << "PASS: testAPSimpleBridge (ap=" << r.articulationPoints.size()
              << ", bridges=" << r.bridges.size() << ")\n";
}

void testAPTriangleNoBridge() {
    // Triangle: no bridges, no articulation points (biconnected)
    AdjGraph g(3);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 0);
    auto r = computeArticulationAndBridges(g);
    assert(r.articulationPoints.empty());
    assert(r.bridges.empty());
    std::cout << "PASS: testAPTriangleNoBridge\n";
}

void testAPEmpty() {
    AdjGraph g(0);
    auto r = computeArticulationAndBridges(g);
    assert(r.articulationPoints.empty());
    assert(r.bridges.empty());
    std::cout << "PASS: testAPEmpty\n";
}

// ═══════════════════════════════════════════════════════════════
// LEVENSHTEIN DISTANCE
// ═══════════════════════════════════════════════════════════════

void testLevenshteinIdentical() {
    assert(levenshteinDistance("ABCDEF", "ABCDEF") == 0);
    std::cout << "PASS: testLevenshteinIdentical\n";
}

void testLevenshteinOneDiff() {
    assert(levenshteinDistance("ABCDEF", "ABCXEF") == 1);
    std::cout << "PASS: testLevenshteinOneDiff\n";
}

void testLevenshteinEmpty() {
    assert(levenshteinDistance("", "") == 0);
    assert(levenshteinDistance("ABC", "") == 3);
    assert(levenshteinDistance("", "XYZ") == 3);
    std::cout << "PASS: testLevenshteinEmpty\n";
}

void testLevenshteinInsertion() {
    assert(levenshteinDistance("ABC", "ABXC") == 1);
    std::cout << "PASS: testLevenshteinInsertion\n";
}

void testSimilarity() {
    assert(structuralSimilarity("ABCDEF", "ABCDEF") == 1.0);
    assert(structuralSimilarity("", "") == 1.0);
    assert(std::fabs(structuralSimilarity("ABC", "XYZ") - 0.0) < 1e-9);
    std::cout << "PASS: testSimilarity\n";
}

// ═══════════════════════════════════════════════════════════════
// K-HOP SUBGRAPH
// ═══════════════════════════════════════════════════════════════

void testKHopSimple() {
    // Star: 0 → {1,2,3,4}
    AdjGraph g(5);
    g.addEdge(0, 1); g.addEdge(0, 2); g.addEdge(0, 3); g.addEdge(0, 4);
    auto r = extractKHop(g, 0, 1);
    assert(r.nodes.size() == 5); // all reachable in 1 hop (fwd + bwd)
    std::cout << "PASS: testKHopSimple\n";
}

void testKHopZero() {
    AdjGraph g(3);
    g.addEdge(0, 1); g.addEdge(1, 2);
    auto r = extractKHop(g, 0, 0);
    assert(r.nodes.size() == 1); // only center
    assert(r.nodes[0] == 0);
    std::cout << "PASS: testKHopZero\n";
}

void testKHopDisconnected() {
    AdjGraph g(4);
    g.addEdge(0, 1);
    // 2, 3 disconnected
    auto r = extractKHop(g, 0, 10);
    assert(r.nodes.size() == 2); // only 0 and 1
    std::cout << "PASS: testKHopDisconnected\n";
}

// ═══════════════════════════════════════════════════════════════
// CALL PATH ENUMERATION
// ═══════════════════════════════════════════════════════════════

void testCallPathsSimple() {
    // 0→1→3, 0→2→3: two paths from 0 to 3
    AdjGraph g(4);
    g.addEdge(0, 1); g.addEdge(0, 2); g.addEdge(1, 3); g.addEdge(2, 3);
    auto paths = enumerateCallPaths(g, 0, 3, 5);
    assert(paths.size() == 2);
    std::cout << "PASS: testCallPathsSimple\n";
}

void testCallPathsNoPath() {
    AdjGraph g(3);
    g.addEdge(0, 1);
    auto paths = enumerateCallPaths(g, 0, 2, 5);
    assert(paths.empty());
    std::cout << "PASS: testCallPathsNoPath\n";
}

void testCallPathsDepthLimit() {
    // Chain 0→1→2→3→4. maxDepth=2 means max path length 3 nodes.
    AdjGraph g(5);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 3); g.addEdge(3, 4);
    auto paths = enumerateCallPaths(g, 0, 4, 2);
    assert(paths.empty()); // can't reach 4 in 2 hops
    auto paths2 = enumerateCallPaths(g, 0, 4, 5);
    assert(paths2.size() == 1);
    std::cout << "PASS: testCallPathsDepthLimit\n";
}

// ═══════════════════════════════════════════════════════════════
// CONSTRAINED REACHABILITY
// ═══════════════════════════════════════════════════════════════

void testConstrainedReachable() {
    // 0→1→2→3, 0→3 (shortcut)
    AdjGraph g(4);
    g.addEdge(0, 1); g.addEdge(1, 2); g.addEdge(2, 3); g.addEdge(0, 3);
    auto r = constrainedReachability(g, 0, 3, 2);
    assert(r.reachable);
    assert(r.reachableThrough); // 0→1→2→3 passes through 2
    assert(r.shortestPathLength == 1); // direct 0→3
    assert(r.shortestConstrainedPathLength == 3); // 0→1→2→3
    std::cout << "PASS: testConstrainedReachable\n";
}

void testConstrainedUnreachable() {
    AdjGraph g(4);
    g.addEdge(0, 1);
    auto r = constrainedReachability(g, 0, 3, 2);
    assert(!r.reachable);
    assert(!r.reachableThrough);
    std::cout << "PASS: testConstrainedUnreachable\n";
}

void testConstrainedMustPassUnreachable() {
    // 0→3 exists but 0 can't reach 2
    AdjGraph g(4);
    g.addEdge(0, 3);
    auto r = constrainedReachability(g, 0, 3, 2);
    assert(r.reachable);
    assert(!r.reachableThrough);
    std::cout << "PASS: testConstrainedMustPassUnreachable\n";
}

int main() {
    std::cout << "=== Dominators ===\n";
    testDomSimpleChain();
    testDomDiamond();
    testDomUnreachable();
    testDomEmpty();

    std::cout << "\n=== Feedback Arc Set ===\n";
    testFASAcyclic();
    testFASTriangleCycle();
    testFASEmpty();
    testFASSelfLoop();

    std::cout << "\n=== Articulation Points & Bridges ===\n";
    testAPSimpleBridge();
    testAPTriangleNoBridge();
    testAPEmpty();

    std::cout << "\n=== Levenshtein ===\n";
    testLevenshteinIdentical();
    testLevenshteinOneDiff();
    testLevenshteinEmpty();
    testLevenshteinInsertion();
    testSimilarity();

    std::cout << "\n=== K-Hop Subgraph ===\n";
    testKHopSimple();
    testKHopZero();
    testKHopDisconnected();

    std::cout << "\n=== Call Path Enumeration ===\n";
    testCallPathsSimple();
    testCallPathsNoPath();
    testCallPathsDepthLimit();

    std::cout << "\n=== Constrained Reachability ===\n";
    testConstrainedReachable();
    testConstrainedUnreachable();
    testConstrainedMustPassUnreachable();

    std::cout << "\nAll 26 tests passed.\n";
    return 0;
}
