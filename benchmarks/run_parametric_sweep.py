"""
Parametric Threshold Sweep Experiment.
Sweeps theta in [0.50, 0.60, 0.70, 0.80, 0.90, 0.95] to plot:
- Speedup(theta)
- SpecPrecision(theta)
- Discard Count vs Commit Count
"""

import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from hyperagent import SequentialEngine, SpeculativeDAGEngine, AdaptiveSpeculativeEngine
from workloads import create_financial_diligence_workload

def main():
    print("=================================================================")
    print(" PARAMETRIC THRESHOLD SWEEP EXPERIMENT: Speedup(theta) & SpecPrecision(theta)")
    print("=================================================================")

    # Get sequential baseline
    dag_base, s_base = create_financial_diligence_workload()
    base_metrics = SequentialEngine.run(dag_base, s_base)
    t_seq = base_metrics.wall_clock_ms
    print(f"Sequential Baseline Latency: {t_seq:.2f} ms\n")

    thresholds = [0.50, 0.60, 0.70, 0.80, 0.90, 0.95]
    sweep_results = []

    print(f"{'Theta':<8} | {'Latency (ms)':<14} | {'Speedup':<10} | {'Dispatched':<12} | {'Committed':<10} | {'Discarded':<10} | {'Precision':<10}")
    print("-" * 88)

    for theta in thresholds:
        dag, init_s = create_financial_diligence_workload()
        m = SpeculativeDAGEngine.run(dag, init_s, confidence_threshold=theta)
        speedup = t_seq / m.wall_clock_ms
        precision = m.speculative_precision

        res = {
            "theta": theta,
            "wall_clock_ms": round(m.wall_clock_ms, 2),
            "speedup": round(speedup, 2),
            "speculative_dispatched": m.total_speculative_dispatched,
            "speculative_committed": m.speculative_committed,
            "speculative_discarded": m.speculative_discarded,
            "speculative_precision": round(precision, 4)
        }
        sweep_results.append(res)

        print(f"{theta:<8.2f} | {m.wall_clock_ms:<14.2f} | {speedup:<10.2f}x | {m.total_speculative_dispatched:<12} | {m.speculative_committed:<10} | {m.speculative_discarded:<10} | {precision * 100:<9.1f}%")

    # Run Adaptive Cost-Utility Engine for comparison
    print("-" * 88)
    dag_adapt, s_adapt = create_financial_diligence_workload()
    m_adapt = AdaptiveSpeculativeEngine.run(dag_adapt, s_adapt)
    speedup_adapt = t_seq / m_adapt.wall_clock_ms
    print(f"{'ADAPTIVE':<8} | {m_adapt.wall_clock_ms:<14.2f} | {speedup_adapt:<10.2f}x | {m_adapt.total_speculative_dispatched:<12} | {m_adapt.speculative_committed:<10} | {m_adapt.speculative_discarded:<10} | {m_adapt.speculative_precision * 100:<9.1f}%")

    out_file = os.path.join(os.path.dirname(__file__), "..", "data", "benchmark_results", "parametric_sweep_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "sequential_baseline_ms": t_seq,
            "sweep": sweep_results,
            "adaptive_break_even": {
                "wall_clock_ms": round(m_adapt.wall_clock_ms, 2),
                "speedup": round(speedup_adapt, 2),
                "speculative_precision": round(m_adapt.speculative_precision, 4)
            }
        }, f, indent=2)

    print(f"\nSaved parametric sweep curve to: {out_file}")

if __name__ == "__main__":
    main()
