"""
HyperAgent Automated Ablation & Benchmarking Suite.
Executes 4-Stage Ablation across deterministic workloads and logs empirical metrics.
"""

import sys
import os
import json
import time

# Add python/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from hyperagent import (
    PDAG, SemanticCache, 
    SequentialEngine, OrdinaryDAGEngine, SpeculativeDAGEngine, HyperAgentEngine
)
from workloads import create_financial_diligence_workload, create_cybersecurity_sweep_workload


def run_experiment(name: str, dag_factory):
    print(f"\n=======================================================")
    print(f" RUNNING BENCHMARK: {name}")
    print(f"=======================================================")

    results = {}

    # Stage 1: Sequential Baseline
    dag1, s1 = dag_factory()
    m_seq = SequentialEngine.run(dag1, s1)
    results["Sequential"] = m_seq.to_dict()
    print(f" [1/4] Sequential Baseline:      {m_seq.wall_clock_ms:8.2f} ms")

    # Stage 2: Ordinary DAG Parallelism
    dag2, s2 = dag_factory()
    m_dag = OrdinaryDAGEngine.run(dag2, s2)
    results["Ordinary_DAG"] = m_dag.to_dict()
    speedup_dag = m_seq.wall_clock_ms / m_dag.wall_clock_ms
    print(f" [2/4] Ordinary DAG Parallel:    {m_dag.wall_clock_ms:8.2f} ms (Speedup: {speedup_dag:4.2f}x)")

    # Stage 3: DAG + Speculative Dispatch
    dag3, s3 = dag_factory()
    m_spec = SpeculativeDAGEngine.run(dag3, s3, confidence_threshold=0.70)
    results["Speculative_DAG"] = m_spec.to_dict()
    speedup_spec = m_seq.wall_clock_ms / m_spec.wall_clock_ms
    print(f" [3/4] Speculative DAG Engine:   {m_spec.wall_clock_ms:8.2f} ms (Speedup: {speedup_spec:4.2f}x | SpecPrecision: {m_spec.speculative_precision:4.2f})")

    # Stage 4: HyperAgent Complete (DAG + Speculation + Cache)
    # Prime cache by running once then measuring second pass
    cache = SemanticCache()
    dag4_warmup, s4_warmup = dag_factory()
    HyperAgentEngine.run(dag4_warmup, s4_warmup, cache)
    
    dag4, s4 = dag_factory()
    m_hyper = HyperAgentEngine.run(dag4, s4, cache)
    results["HyperAgent_Complete"] = m_hyper.to_dict()
    speedup_hyper = m_seq.wall_clock_ms / m_hyper.wall_clock_ms
    print(f" [4/4] HyperAgent (with Cache):   {m_hyper.wall_clock_ms:8.2f} ms (Speedup: {speedup_hyper:4.2f}x | SpecPrecision: {m_hyper.speculative_precision:4.2f})")

    # Compute comparative delta table
    print(f"\n--- Comparative Performance Summary ---")
    print(f" Baseline (Sequential):          1.00x")
    print(f" Ordinary DAG Delta:             +{speedup_dag - 1.0:4.2f}x ({speedup_dag:4.2f}x)")
    print(f" Speculative Scheduling Delta:   +{speedup_spec - speedup_dag:4.2f}x ({speedup_spec:4.2f}x)")
    print(f" Semantic Cache Delta:           +{speedup_hyper - speedup_spec:4.2f}x ({speedup_hyper:4.2f}x)")
    print(f" Speculative Precision:          {m_spec.speculative_precision * 100:.1f}%")
    print(f" Cache Telemetry:                Hit Ratio: {m_hyper.cache_telemetry['hit_ratio']*100:.1f}% | p50: {m_hyper.cache_telemetry['p50_ms']}ms | p99: {m_hyper.cache_telemetry['p99_ms']}ms")

    return results


def main():
    all_results = {}
    all_results["Financial_Diligence"] = run_experiment("Financial Due Diligence Intelligence", create_financial_diligence_workload)
    all_results["Cybersecurity_Sweep"] = run_experiment("Autonomous Cybersecurity Threat Sweep", create_cybersecurity_sweep_workload)

    # Save to JSON
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "benchmark_results")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "ablation_benchmark.json")

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nSaved empirical benchmark metrics to: {out_file}")

if __name__ == "__main__":
    main()
