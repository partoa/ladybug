#include "function/table/flow_graph.h"
#include <cassert>
#include <cmath>
#include <iostream>

using namespace lbug::function;

void testDinicSimpleTwoPaths() {
    // 0 --1--> 1 --1--> 3
    // 0 --1--> 2 --1--> 3
    // Max flow = 2
    FlowNetwork net(4);
    net.addEdge(0, 1, 1.0);
    net.addEdge(0, 2, 1.0);
    net.addEdge(1, 3, 1.0);
    net.addEdge(2, 3, 1.0);
    assert(net.maxFlow(0, 3) == 2.0);
    std::cout << "PASS: testDinicSimpleTwoPaths\n";
}

void testDinicBottleneck() {
    // 0 --5--> 1 --1--> 2 --5--> 3
    // Max flow = 1 (bottleneck at edge 1->2)
    FlowNetwork net(4);
    net.addEdge(0, 1, 5.0);
    net.addEdge(1, 2, 1.0);
    net.addEdge(2, 3, 5.0);
    assert(net.maxFlow(0, 3) == 1.0);
    std::cout << "PASS: testDinicBottleneck\n";
}

void testDinicDiamond() {
    // 0 --3--> 1 --2--> 3
    // 0 --3--> 2 --2--> 3
    // 1 --1--> 2
    // Max flow = 4
    FlowNetwork net(4);
    net.addEdge(0, 1, 3.0);
    net.addEdge(0, 2, 3.0);
    net.addEdge(1, 3, 2.0);
    net.addEdge(2, 3, 2.0);
    net.addEdge(1, 2, 1.0);
    assert(net.maxFlow(0, 3) == 4.0);
    std::cout << "PASS: testDinicDiamond\n";
}

void testDinicNoPath() {
    FlowNetwork net(2);
    net.addEdge(1, 0, 1.0); // wrong direction
    assert(net.maxFlow(0, 1) == 0.0);
    std::cout << "PASS: testDinicNoPath\n";
}

void testMinCutPartition() {
    FlowNetwork net(4);
    net.addEdge(0, 1, 5.0);
    net.addEdge(1, 2, 1.0);
    net.addEdge(2, 3, 5.0);
    net.maxFlow(0, 3);
    std::vector<uint8_t> side;
    net.minCutSides(0, side);
    assert(side[0] == 0); // source side
    assert(side[3] == 1); // sink side
    // Bottleneck is 1->2, so 0 and 1 on source side, 2 and 3 on sink side
    assert(side[1] == 0);
    assert(side[2] == 1);
    std::cout << "PASS: testMinCutPartition\n";
}

void testGusfield() {
    // Simple graph: 0-1 cap 3, 0-2 cap 2, 1-2 cap 1
    // Undirected: add both directions
    // Gomory-Hu tree should have 2 edges (n-1 = 2)
    FlowNetwork net(3);
    // Undirected edges represented as two directed edges
    net.addEdge(0, 1, 3.0); net.addEdge(1, 0, 3.0);
    net.addEdge(0, 2, 2.0); net.addEdge(2, 0, 2.0);
    net.addEdge(1, 2, 1.0); net.addEdge(2, 1, 1.0);

    // Verify max flows
    // 0-1: flow = 3+min(2,1) = 4? Let's compute:
    // 0->1 (cap 3), 0->2->1 (cap min(2,1)=1) = 4
    double f01 = net.maxFlow(0, 1);
    std::cout << "  flow(0,1) = " << f01 << "\n";
    assert(f01 == 4.0);

    // Need fresh network for each flow computation
    FlowNetwork net2(3);
    net2.addEdge(0, 1, 3.0); net2.addEdge(1, 0, 3.0);
    net2.addEdge(0, 2, 2.0); net2.addEdge(2, 0, 2.0);
    net2.addEdge(1, 2, 1.0); net2.addEdge(2, 1, 1.0);
    double f02 = net2.maxFlow(0, 2);
    std::cout << "  flow(0,2) = " << f02 << "\n";
    assert(f02 == 3.0); // 0->2 (2) + 0->1->2 (1) = 3

    FlowNetwork net3(3);
    net3.addEdge(0, 1, 3.0); net3.addEdge(1, 0, 3.0);
    net3.addEdge(0, 2, 2.0); net3.addEdge(2, 0, 2.0);
    net3.addEdge(1, 2, 1.0); net3.addEdge(2, 1, 1.0);
    double f12 = net3.maxFlow(1, 2);
    std::cout << "  flow(1,2) = " << f12 << "\n";
    assert(f12 == 3.0); // 1->2 (1) + 1->0->2 (2) = 3

    std::cout << "PASS: testGusfield (flow computations verified)\n";
}

void testCycleDetectionSimple() {
    // 0 -> 1 -> 2 -> 0 (cycle)
    // 2 -> 3 (no cycle through 3)
    std::vector<WeightedEdge> edges = {
        {0, 1, 1.0}, {1, 2, 1.0}, {2, 0, 1.0}, {2, 3, 1.0}
    };
    auto cycles = detectAllSimpleCycles(4, edges, 5);
    assert(cycles.size() == 1);
    assert(cycles[0].nodeOffsets.size() == 3);
    assert(std::fabs(cycles[0].totalWeight - 3.0) < 1e-9);
    std::cout << "PASS: testCycleDetectionSimple\n";
}

void testCycleDetectionMultiple() {
    // Two cycles: 0->1->2->0 and 3->4->3
    std::vector<WeightedEdge> edges = {
        {0, 1, 1.0}, {1, 2, 1.0}, {2, 0, 1.0},
        {2, 3, 1.0}, {3, 4, 1.0}, {4, 3, 1.0}
    };
    auto cycles = detectAllSimpleCycles(5, edges, 5);
    assert(cycles.size() == 2);
    std::cout << "PASS: testCycleDetectionMultiple\n";
}

void testCycleDetectionAcyclic() {
    std::vector<WeightedEdge> edges = {
        {0, 1, 1.0}, {1, 2, 1.0}, {2, 3, 1.0}
    };
    auto cycles = detectAllSimpleCycles(4, edges, 5);
    assert(cycles.size() == 0);
    std::cout << "PASS: testCycleDetectionAcyclic\n";
}

void testCycleDetectionReentrancy() {
    // deposit(0) -> update(1) -> transfer(2) -> callback(3) -> deposit(0)
    std::vector<WeightedEdge> edges = {
        {0, 1, 1.0}, {1, 2, 1.0}, {2, 3, 1.0}, {3, 0, 1.0}, {4, 1, 1.0}
    };
    auto cycles = detectAllSimpleCycles(5, edges, 5);
    assert(cycles.size() == 1);
    assert(cycles[0].nodeOffsets.size() == 4);
    std::cout << "PASS: testCycleDetectionReentrancy\n";
}

void testSelfLoop() {
    std::vector<WeightedEdge> edges = {{0, 0, 1.0}};
    auto cycles = detectAllSimpleCycles(1, edges, 5);
    // Self-loops: path size must be > 1, so single-node self-loops aren't detected.
    // This is correct — self-recursion is detected by checking CallsTo edges directly.
    std::cout << "PASS: testSelfLoop (no crash)\n";
}

int main() {
    testDinicSimpleTwoPaths();
    testDinicBottleneck();
    testDinicDiamond();
    testDinicNoPath();
    testMinCutPartition();
    testGusfield();
    testCycleDetectionSimple();
    testCycleDetectionMultiple();
    testCycleDetectionAcyclic();
    testCycleDetectionReentrancy();
    testSelfLoop();
    std::cout << "\nAll tests passed.\n";
    return 0;
}
