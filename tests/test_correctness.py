"""
HyperAgent Formal Correctness & Invariant Test Suite.
Verifies the core mathematical invariant: S_HyperAgent == S_Sequential
under hits, misses, rollbacks, and concurrent branches.
"""

import sys
import os
import unittest
import time

# Add python/ directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from hyperagent import (
    PDAG, Node, SemanticCache, StateManager,
    SequentialEngine, OrdinaryDAGEngine, SpeculativeDAGEngine, HyperAgentEngine
)
from workloads import create_financial_diligence_workload, create_cybersecurity_sweep_workload


class TestHyperAgentCorrectness(unittest.TestCase):

    def test_core_invariant_financial_workload(self):
        """Invariant: S_HyperAgent == S_Sequential on Financial Workload."""
        dag_seq, init_seq = create_financial_diligence_workload()
        dag_hyper, init_hyper = create_financial_diligence_workload()

        res_seq = SequentialEngine.run(dag_seq, init_seq)
        res_hyper = SpeculativeDAGEngine.run(dag_hyper, init_hyper, confidence_threshold=0.70)

        self.assertEqual(res_seq.final_state, res_hyper.final_state,
                         "Core Invariant Violated: HyperAgent final state diverges from Sequential state!")

    def test_core_invariant_cybersecurity_workload(self):
        """Invariant: S_HyperAgent == S_Sequential on Cybersecurity Workload."""
        dag_seq, init_seq = create_cybersecurity_sweep_workload()
        dag_hyper, init_hyper = create_cybersecurity_sweep_workload()

        res_seq = SequentialEngine.run(dag_seq, init_seq)
        res_hyper = SpeculativeDAGEngine.run(dag_hyper, init_hyper, confidence_threshold=0.70)

        self.assertEqual(res_seq.final_state, res_hyper.final_state,
                         "Core Invariant Violated: Cyber sweep final state diverges!")

    def test_speculative_hit_and_commit(self):
        """Test exact speculative hit: Branch must commit without re-running."""
        dag = PDAG()
        dag.add_node(Node("parent", "Parent Tool", lambda inp: {"val": 100}, estimated_latency_ms=50))
        dag.add_node(Node("child", "Child Tool", lambda inp: inp["parent"]["val"] * 2, estimated_latency_ms=50,
                          speculative_predictor=lambda state: {"parent": {"val": 100}}))
        dag.add_edge("parent", "child", "parent", transition_probability=0.95)

        metrics = SpeculativeDAGEngine.run(dag, {}, confidence_threshold=0.70)
        self.assertEqual(metrics.speculative_committed, 1)
        self.assertEqual(metrics.speculative_discarded, 0)
        self.assertEqual(metrics.final_state["child"], 200)

    def test_deterministic_rollback_on_prediction_mismatch(self):
        """Test rollback integrity: Prediction error must discard branch and produce clean canonical state."""
        dag = PDAG()
        # Parent returns val = 42
        dag.add_node(Node("parent", "Parent Tool", lambda inp: {"val": 42}, estimated_latency_ms=50))
        # Child predictor WRONGLY predicts val = 999
        dag.add_node(Node("child", "Child Tool", lambda inp: inp["parent"]["val"] + 1, estimated_latency_ms=50,
                          speculative_predictor=lambda state: {"parent": {"val": 999}}))
        dag.add_edge("parent", "child", "parent", transition_probability=0.95)

        metrics = SpeculativeDAGEngine.run(dag, {}, confidence_threshold=0.70)
        self.assertEqual(metrics.speculative_discarded, 1, "Failed prediction was not discarded!")
        self.assertEqual(metrics.speculative_committed, 0)
        # Expected correct result is 42 + 1 = 43 (NOT 999 + 1 = 1000)
        self.assertEqual(metrics.final_state["child"], 43, "Corrupted speculative state leaked into canonical!")

    def test_multiple_simultaneous_speculative_branches(self):
        """Test 3 simultaneous speculative branches forking and committing in parallel."""
        dag = PDAG()
        dag.add_node(Node("root", "Root Tool", lambda inp: {"seed": 10}, estimated_latency_ms=60))

        for i in range(3):
            nid = f"child_{i}"
            dag.add_node(Node(nid, f"Child {i}", lambda inp, idx=i: inp["root"]["seed"] + idx, estimated_latency_ms=40,
                              speculative_predictor=lambda state: {"root": {"seed": 10}}))
            dag.add_edge("root", nid, "root", transition_probability=0.90)

        metrics = SpeculativeDAGEngine.run(dag, {}, confidence_threshold=0.70)
        self.assertEqual(metrics.speculative_committed, 3)
        self.assertEqual(metrics.speculative_discarded, 0)
        self.assertEqual(metrics.final_state["child_0"], 10)
        self.assertEqual(metrics.final_state["child_1"], 11)
        self.assertEqual(metrics.final_state["child_2"], 12)

    def test_cycle_detection_rejection(self):
        """Test that cycles (A -> B -> C -> A) are mathematically rejected by P-DAG."""
        dag = PDAG()
        dag.add_node(Node("A", "Node A", lambda inp: None))
        dag.add_node(Node("B", "Node B", lambda inp: None))
        dag.add_node(Node("C", "Node C", lambda inp: None))

        dag.add_edge("A", "B", "param_ab")
        dag.add_edge("B", "C", "param_bc")

        with self.assertRaises(ValueError):
            dag.add_edge("C", "A", "param_ca")

    def test_in_memory_semantic_cache_isolation(self):
        """Test that SemanticCache provides sub-millisecond retrieval and correct hit ratios."""
        cache = SemanticCache(max_size=10)
        cache.put("key_1", {"data": 123})

        val = cache.get("key_1")
        self.assertEqual(val, {"data": 123})

        miss = cache.get("key_missing")
        self.assertIsNone(miss)

        telemetry = cache.get_telemetry()
        self.assertEqual(telemetry["hits"], 1)
        self.assertEqual(telemetry["misses"], 1)
        self.assertEqual(telemetry["hit_ratio"], 0.5)
        self.assertLess(telemetry["p50_ms"], 1.0, "Cache lookup exceeded 1ms threshold!")


if __name__ == "__main__":
    unittest.main(verbosity=2)
