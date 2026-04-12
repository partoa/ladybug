#include "function/table/flow_graph.h"
#include <cassert>
#include <chrono>
#include <cmath>
#include <iostream>

using namespace lbug::function;

// ═══════════════════════════════════════════════════════════════════
// BASIC CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

void testTwoPaths() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 2, 1.0);
    net.addEdge(1, 3, 1.0);
    net.addEdge(2, 3, 1.0);
    assert(net.maxFlow(0, 3) == 2.0);
    std::cout << "PASS: testTwoPaths\n";
}

void testBottleneck() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 5.0);
    net.addEdge(1, 2, 1.0);
    net.addEdge(2, 3, 5.0);
    assert(net.maxFlow(0, 3) == 1.0);
    std::cout << "PASS: testBottleneck\n";
}

void testDiamond() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 3.0);
    net.addEdge(0, 2, 3.0);
    net.addEdge(1, 3, 2.0);
    net.addEdge(2, 3, 2.0);
    net.addEdge(1, 2, 1.0);
    assert(net.maxFlow(0, 3) == 4.0);
    std::cout << "PASS: testDiamond\n";
}

void testMinCutPartition() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 5.0);
    net.addEdge(1, 2, 1.0);
    net.addEdge(2, 3, 5.0);
    net.maxFlow(0, 3);
    std::vector<uint8_t> side;
    net.minCutSides(0, side);
    assert(side[0] == 0);
    assert(side[1] == 0);
    assert(side[2] == 1);
    assert(side[3] == 1);
    std::cout << "PASS: testMinCutPartition\n";
}

// ═══════════════════════════════════════════════════════════════════
// EDGE CASES: EMPTY AND TRIVIAL GRAPHS
// ═══════════════════════════════════════════════════════════════════

void testZeroNodeNetwork() {
    FlowNetwork net(0);
    assert(net.numNodes() == 0);
    std::cout << "PASS: testZeroNodeNetwork\n";
}

void testSingleNode() {
    FlowNetwork net(1);
    assert(net.maxFlow(0, 0) == 0.0);
    std::vector<uint8_t> side;
    net.minCutSides(0, side);
    assert(side.size() == 1);
    assert(side[0] == 0);
    std::cout << "PASS: testSingleNode\n";
}

void testTwoNodesNoEdge() {
    FlowNetwork net(2);
    assert(net.maxFlow(0, 1) == 0.0);
    std::vector<uint8_t> side;
    net.minCutSides(0, side);
    assert(side[0] == 0);
    assert(side[1] == 1);
    std::cout << "PASS: testTwoNodesNoEdge\n";
}

void testReverseEdgeOnly() {
    FlowNetwork net(2);
    net.addEdge(1, 0, 1.0);
    assert(net.maxFlow(0, 1) == 0.0);
    std::cout << "PASS: testReverseEdgeOnly\n";
}

// ═══════════════════════════════════════════════════════════════════
// PARALLEL EDGES (multi-edges between same pair)
// ═══════════════════════════════════════════════════════════════════

void testParallelEdges() {
    FlowNetwork net(2);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 1, 1.0);
    assert(net.maxFlow(0, 1) == 3.0);
    std::cout << "PASS: testParallelEdges\n";
}

void testParallelEdgesReverse() {
    FlowNetwork net(3);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 1, 1.0);
    net.addEdge(1, 2, 2.0);
    assert(net.maxFlow(0, 2) == 2.0);
    std::cout << "PASS: testParallelEdgesReverse\n";
}

// ═══════════════════════════════════════════════════════════════════
// SELF-LOOPS
// ═══════════════════════════════════════════════════════════════════

void testSelfLoop() {
    FlowNetwork net(3);
    net.addEdge(0, 0, 10.0);
    net.addEdge(0, 1, 1.0);
    net.addEdge(1, 1, 10.0);
    net.addEdge(1, 2, 1.0);
    assert(net.maxFlow(0, 2) == 1.0);
    std::cout << "PASS: testSelfLoop\n";
}

// ═══════════════════════════════════════════════════════════════════
// DISCONNECTED COMPONENTS
// ═══════════════════════════════════════════════════════════════════

void testDisconnectedComponents() {
    FlowNetwork net(6);
    net.addEdge(0, 1, 1.0);
    net.addEdge(1, 2, 1.0);
    net.addEdge(3, 4, 1.0);
    net.addEdge(4, 5, 1.0);
    assert(net.maxFlow(0, 5) == 0.0);
    assert(net.maxFlow(0, 3) == 0.0);
    assert(net.maxFlow(0, 2) == 1.0);
    std::cout << "PASS: testDisconnectedComponents\n";
}

void testMinCutDisconnected() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 1.0);
    net.maxFlow(0, 3);
    std::vector<uint8_t> side;
    net.minCutSides(0, side);
    assert(side[0] == 0);
    assert(side[1] == 0);
    assert(side[2] == 1);
    assert(side[3] == 1);
    std::cout << "PASS: testMinCutDisconnected\n";
}

// ═══════════════════════════════════════════════════════════════════
// LARGE GRAPHS
// ═══════════════════════════════════════════════════════════════════

void testLongChain() {
    const uint64_t N = 1001;
    FlowNetwork net(N);
    for (uint64_t i = 0; i < N - 1; ++i) {
        net.addEdge(i, i + 1, 1.0);
    }
    assert(net.maxFlow(0, N - 1) == 1.0);
    std::cout << "PASS: testLongChain (depth " << N << ")\n";
}

void testWideGraph() {
    const uint64_t WIDTH = 100;
    FlowNetwork net(WIDTH + 2);
    for (uint64_t i = 1; i <= WIDTH; ++i) {
        net.addEdge(0, i, 1.0);
        net.addEdge(i, WIDTH + 1, 1.0);
    }
    assert(net.maxFlow(0, WIDTH + 1) == static_cast<double>(WIDTH));
    std::cout << "PASS: testWideGraph (width " << WIDTH << ")\n";
}

// ═══════════════════════════════════════════════════════════════════
// CAPACITY EDGE CASES
// ═══════════════════════════════════════════════════════════════════

void testFractionalCapacity() {
    FlowNetwork net(2);
    net.addEdge(0, 1, 0.5);
    assert(net.maxFlow(0, 1) == 0.5);
    std::cout << "PASS: testFractionalCapacity\n";
}

void testZeroCapacity() {
    FlowNetwork net(2);
    net.addEdge(0, 1, 0.0);
    assert(net.maxFlow(0, 1) == 0.0);
    std::cout << "PASS: testZeroCapacity\n";
}

// ═══════════════════════════════════════════════════════════════════
// GUSFIELD VERIFICATION (all-pairs via direct max-flow)
// ═══════════════════════════════════════════════════════════════════

void testGusfieldTriangle() {
    auto makeNet = []() {
        FlowNetwork net(3);
        net.addEdge(0, 1, 1.0); net.addEdge(1, 0, 1.0);
        net.addEdge(1, 2, 1.0); net.addEdge(2, 1, 1.0);
        net.addEdge(0, 2, 1.0); net.addEdge(2, 0, 1.0);
        return net;
    };
    auto n1 = makeNet(); assert(n1.maxFlow(0, 1) == 2.0);
    auto n2 = makeNet(); assert(n2.maxFlow(0, 2) == 2.0);
    auto n3 = makeNet(); assert(n3.maxFlow(1, 2) == 2.0);
    std::cout << "PASS: testGusfieldTriangle\n";
}

void testGusfieldPath() {
    auto makeNet = []() {
        FlowNetwork net(4);
        net.addEdge(0, 1, 1.0); net.addEdge(1, 0, 1.0);
        net.addEdge(1, 2, 1.0); net.addEdge(2, 1, 1.0);
        net.addEdge(2, 3, 1.0); net.addEdge(3, 2, 1.0);
        return net;
    };
    auto n1 = makeNet(); assert(n1.maxFlow(0, 3) == 1.0);
    auto n2 = makeNet(); assert(n2.maxFlow(0, 2) == 1.0);
    auto n3 = makeNet(); assert(n3.maxFlow(1, 3) == 1.0);
    std::cout << "PASS: testGusfieldPath\n";
}

void testGusfieldStar() {
    auto makeNet = []() {
        FlowNetwork net(5);
        for (uint64_t i = 1; i <= 4; ++i) {
            net.addEdge(0, i, 1.0);
            net.addEdge(i, 0, 1.0);
        }
        return net;
    };
    // Center has degree 4 (one edge to each leaf).
    // Flow center-to-leaf = 1 (single edge).
    // Flow leaf-to-leaf = 1 (only path goes through center, one edge-disjoint path).
    auto n1 = makeNet(); assert(n1.maxFlow(0, 1) == 1.0);
    auto n2 = makeNet(); assert(n2.maxFlow(1, 2) == 1.0);
    auto n3 = makeNet(); assert(n3.maxFlow(1, 4) == 1.0);
    std::cout << "PASS: testGusfieldStar\n";
}

// ═══════════════════════════════════════════════════════════════════
// CYCLE DETECTION
// ═══════════════════════════════════════════════════════════════════

void testCycleSimple() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,2,1.0}, {2,0,1.0}, {2,3,1.0}};
    auto cycles = detectAllSimpleCycles(4, edges, 5);
    assert(cycles.size() == 1);
    assert(cycles[0].nodeOffsets.size() == 3);
    std::cout << "PASS: testCycleSimple\n";
}

void testCycleMultiple() {
    std::vector<WeightedEdge> edges = {
        {0,1,1.0}, {1,2,1.0}, {2,0,1.0},
        {2,3,1.0}, {3,4,1.0}, {4,3,1.0}
    };
    assert(detectAllSimpleCycles(5, edges, 5).size() == 2);
    std::cout << "PASS: testCycleMultiple\n";
}

void testCycleAcyclic() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,2,1.0}, {2,3,1.0}};
    assert(detectAllSimpleCycles(4, edges, 5).empty());
    std::cout << "PASS: testCycleAcyclic\n";
}

void testCycleEmptyGraph() {
    assert(detectAllSimpleCycles(0, {}, 5).empty());
    assert(detectAllSimpleCycles(5, {}, 5).empty());
    std::cout << "PASS: testCycleEmptyGraph\n";
}

void testCycleSelfLoop() {
    std::vector<WeightedEdge> edges = {{0, 0, 1.0}};
    assert(detectAllSimpleCycles(1, edges, 5).empty());
    std::cout << "PASS: testCycleSelfLoop\n";
}

void testCycleMaxLengthZero() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,0,1.0}};
    assert(detectAllSimpleCycles(2, edges, 0).empty());
    std::cout << "PASS: testCycleMaxLengthZero\n";
}

void testCycleMaxLengthOne() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,0,1.0}};
    assert(detectAllSimpleCycles(2, edges, 1).empty());
    std::cout << "PASS: testCycleMaxLengthOne\n";
}

void testCycleMaxLengthTwo() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,0,1.0}};
    auto cycles = detectAllSimpleCycles(2, edges, 2);
    assert(cycles.size() == 1);
    assert(cycles[0].nodeOffsets.size() == 2);
    std::cout << "PASS: testCycleMaxLengthTwo\n";
}

void testCycleNoDuplicates() {
    std::vector<WeightedEdge> edges = {{0,1,1.0}, {1,2,1.0}, {2,0,1.0}};
    assert(detectAllSimpleCycles(3, edges, 5).size() == 1);
    std::cout << "PASS: testCycleNoDuplicates\n";
}

void testCycleWeightAccumulation() {
    std::vector<WeightedEdge> edges = {{0,1,2.5}, {1,2,3.7}, {2,0,1.3}};
    auto cycles = detectAllSimpleCycles(3, edges, 5);
    assert(cycles.size() == 1);
    assert(std::fabs(cycles[0].totalWeight - 7.5) < 1e-9);
    std::cout << "PASS: testCycleWeightAccumulation\n";
}

// ═══════════════════════════════════════════════════════════════════
// DENSE/SPARSE GRAPHS
// ═══════════════════════════════════════════════════════════════════

void testCycleDenseGraph() {
    const uint64_t N = 5;
    std::vector<WeightedEdge> edges;
    for (uint64_t i = 0; i < N; ++i)
        for (uint64_t j = 0; j < N; ++j)
            if (i != j) edges.push_back({i, j, 1.0});

    auto start = std::chrono::steady_clock::now();
    auto cycles = detectAllSimpleCycles(N, edges, 3);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    assert(ms < 1000);
    assert(!cycles.empty());
    std::cout << "PASS: testCycleDenseGraph (" << cycles.size() << " cycles, " << ms << "ms)\n";
}

void testCycleDenseGraphBounded() {
    const uint64_t N = 8;
    std::vector<WeightedEdge> edges;
    for (uint64_t i = 0; i < N; ++i)
        for (uint64_t j = 0; j < N; ++j)
            if (i != j) edges.push_back({i, j, 1.0});

    auto start = std::chrono::steady_clock::now();
    auto cycles = detectAllSimpleCycles(N, edges, 4);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    std::cout << "  K8 depth-4: " << cycles.size() << " cycles in " << ms << "ms\n";
    assert(ms < 5000);
    std::cout << "PASS: testCycleDenseGraphBounded\n";
}

void testSparseNodeIds() {
    FlowNetwork net(1001);
    net.addEdge(0, 1000, 5.0);
    assert(net.maxFlow(0, 1000) == 5.0);
    // Note: can't reuse network after maxFlow — residual graph is modified.
    FlowNetwork net2(1001);
    net2.addEdge(0, 1000, 5.0);
    assert(net2.maxFlow(1000, 0) == 0.0);
    std::cout << "PASS: testSparseNodeIds\n";
}

// ═══════════════════════════════════════════════════════════════════
// INVARIANTS
// ═══════════════════════════════════════════════════════════════════

void testFlowInvariant() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 2, 1.0);
    net.addEdge(0, 3, 1.0);
    net.addEdge(1, 3, 1.0);
    net.addEdge(2, 3, 1.0);
    assert(net.maxFlow(0, 3) == 3.0);
    std::cout << "PASS: testFlowInvariant\n";
}

void testBidirectionalEdges() {
    // Two nodes, each with an edge to the other (cap 1 each).
    // Flow from 0->1: only the forward edge contributes. Flow = 1.
    FlowNetwork net(2);
    net.addEdge(0, 1, 1.0);
    net.addEdge(1, 0, 1.0);
    assert(net.maxFlow(0, 1) == 1.0);

    // With an intermediate node, flow cancellation enables both directions.
    // 0->1->2 and 0<->1 bidirectional:
    // 0->1 (cap 1), 1->0 (cap 1), 0->2 (cap 1), 1->2 (cap 1)
    // Paths: 0->1->2 (flow 1) and 0->2 (flow 1). Total = 2.
    FlowNetwork net2(3);
    net2.addEdge(0, 1, 1.0); net2.addEdge(1, 0, 1.0);
    net2.addEdge(0, 2, 1.0); net2.addEdge(1, 2, 1.0);
    assert(net2.maxFlow(0, 2) == 2.0);
    std::cout << "PASS: testBidirectionalEdges\n";
}

// ═══════════════════════════════════════════════════════════════════
// PERFORMANCE
// ═══════════════════════════════════════════════════════════════════

void testPerformanceLargeGrid() {
    const uint64_t W = 50;
    const uint64_t N = W * W;
    FlowNetwork net(N);
    for (uint64_t r = 0; r < W; ++r)
        for (uint64_t c = 0; c < W; ++c) {
            uint64_t id = r * W + c;
            if (c + 1 < W) net.addEdge(id, id + 1, 1.0);
            if (r + 1 < W) net.addEdge(id, id + W, 1.0);
        }

    auto start = std::chrono::steady_clock::now();
    double f = net.maxFlow(0, N - 1);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    assert(f > 0);
    std::cout << "PASS: testPerformanceLargeGrid (flow=" << f << ", " << ms << "ms)\n";
}

int main() {
    std::cout << "=== Basic Correctness ===\n";
    testTwoPaths();
    testBottleneck();
    testDiamond();
    testMinCutPartition();

    std::cout << "\n=== Edge Cases: Empty/Trivial ===\n";
    testZeroNodeNetwork();
    testSingleNode();
    testTwoNodesNoEdge();
    testReverseEdgeOnly();

    std::cout << "\n=== Parallel Edges ===\n";
    testParallelEdges();
    testParallelEdgesReverse();

    std::cout << "\n=== Self Loops ===\n";
    testSelfLoop();

    std::cout << "\n=== Disconnected Components ===\n";
    testDisconnectedComponents();
    testMinCutDisconnected();

    std::cout << "\n=== Large Graphs ===\n";
    testLongChain();
    testWideGraph();

    std::cout << "\n=== Capacity Edge Cases ===\n";
    testFractionalCapacity();
    testZeroCapacity();

    std::cout << "\n=== Gusfield Verification ===\n";
    testGusfieldTriangle();
    testGusfieldPath();
    testGusfieldStar();

    std::cout << "\n=== Cycle Detection ===\n";
    testCycleSimple();
    testCycleMultiple();
    testCycleAcyclic();
    testCycleEmptyGraph();
    testCycleSelfLoop();
    testCycleMaxLengthZero();
    testCycleMaxLengthOne();
    testCycleMaxLengthTwo();
    testCycleNoDuplicates();
    testCycleWeightAccumulation();

    std::cout << "\n=== Dense/Sparse Graphs ===\n";
    testCycleDenseGraph();
    testCycleDenseGraphBounded();
    testSparseNodeIds();

    std::cout << "\n=== Invariants ===\n";
    testFlowInvariant();
    testBidirectionalEdges();

    std::cout << "\n=== Performance ===\n";
    testPerformanceLargeGrid();

    std::cout << "\nAll " << 32 << " tests passed.\n";
    return 0;
}
