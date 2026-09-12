import sys
import os
import json
import random
import statistics
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "python"))

from hyperagent import PDAG, Node, SequentialEngine, OrdinaryDAGEngine, SpeculativeDAGEngine, AdaptiveSpeculativeEngine

def create_accuracy_benchmark_workload(accuracy_rate: float, latency_scale: float = 0.1):
    dag = PDAG()

    def simulate_tool(name: str, base_ms: float, result_val: dict):
        def fn(inputs):
            time.sleep((base_ms * latency_scale) / 1000.0)
            return result_val
        return fn

    sec_out = {"revenue_m": 4200, "debt_m": 1200, "ebitda_m": 850}
    news_out = {"macro_sentiment_score": 0.78, "volatility": "low"}
    ratio_out = {"debt_to_ebitda": 1.41, "current_ratio": 2.1}
    insider_out = {"insider_buys": 4, "insider_sells": 1, "net_signal": "bullish"}
    risk_out = {"composite_risk_rating": "AA-", "default_probability_bps": 18}
    synth_out = {"final_verdict": "APPROVED", "confidence": 0.94}

    def make_predictor(true_inputs, perturb_key, perturb_val):
        def pred(state):
            if random.random() < accuracy_rate:
                return dict(true_inputs)
            else:
                p = dict(true_inputs)
                p[perturb_key] = perturb_val
                return p
        return pred

    n1 = Node("sec_parser", "SEC 10-K Parser", 
              simulate_tool("sec_parser", 800, sec_out), 800 * latency_scale)
    n2 = Node("news_sentiment", "News Macro Sentiment", 
              simulate_tool("news_sentiment", 600, news_out), 600 * latency_scale)
    
    n3 = Node("ratio_engine", "Financial Ratio Engine", 
              simulate_tool("ratio_engine", 500, ratio_out), 500 * latency_scale,
              speculative_predictor=make_predictor({"sec_parser": sec_out}, "sec_parser", {"revenue_m": 9999}))
    
    n4 = Node("insider_scan", "Insider Trading Scanner", 
              simulate_tool("insider_scan", 450, insider_out), 450 * latency_scale,
              speculative_predictor=make_predictor({"sec_parser": sec_out}, "sec_parser", {"revenue_m": 8888}))
    
    true_risk_in = {"ratio_engine": ratio_out, "news_sentiment": news_out, "insider_scan": insider_out}
    n5 = Node("risk_scorer", "Multi-Factor Risk Scorer", 
              simulate_tool("risk_scorer", 700, risk_out), 700 * latency_scale,
              speculative_predictor=make_predictor(true_risk_in, "ratio_engine", {"debt_to_ebitda": 9.99}))
    
    true_synth_in = {"risk_scorer": risk_out}
    n6 = Node("synthesis", "Executive Diligence Synthesizer", 
              simulate_tool("synthesis", 400, synth_out), 400 * latency_scale,
              speculative_predictor=make_predictor(true_synth_in, "risk_scorer", {"composite_risk_rating": "F"}))

    for n in [n1, n2, n3, n4, n5, n6]:
        dag.add_node(n)

    dag.add_edge("sec_parser", "ratio_engine", "sec_parser", 0.95)
    dag.add_edge("sec_parser", "insider_scan", "sec_parser", 0.95)
    dag.add_edge("ratio_engine", "risk_scorer", "ratio_engine", 0.95)
    dag.add_edge("news_sentiment", "risk_scorer", "news_sentiment", 0.95)
    dag.add_edge("insider_scan", "risk_scorer", "insider_scan", 0.95)
    dag.add_edge("risk_scorer", "synthesis", "risk_scorer", 0.95)

    init_state = {"target_ticker": "ACME_CORP", "fiscal_year": 2025}
    return dag, init_state

def run_experiment(repetitions=5):
    print("=================================================================")
    print(" EMPIRICAL BREAK-EVEN ANALYSIS: PREDICTION ACCURACY SWEEP")
    print(" Cache: STRICTLY DISABLED (COLD) | Repetitions: " + str(repetitions) + " per point")
    print("=================================================================\n")

    seq_times = []
    seq_tools = []
    for _ in range(repetitions):
        d, s = create_accuracy_benchmark_workload(1.0)
        m = SequentialEngine.run(d, s)
        seq_times.append(m.wall_clock_ms)
        seq_tools.append(m.total_tool_time_ms)
    
    base_seq_wall = statistics.mean(seq_times)
    base_seq_tool = statistics.mean(seq_tools)

    dag_times = []
    for _ in range(repetitions):
        d, s = create_accuracy_benchmark_workload(1.0)
        m = OrdinaryDAGEngine.run(d, s)
        dag_times.append(m.wall_clock_ms)
    base_dag_wall = statistics.mean(dag_times)

    dag_speedup = base_seq_wall / base_dag_wall
    print(f"Sequential Baseline: {base_seq_wall:.1f} ms | Tool Time: {base_seq_tool:.1f} ms")
    print(f"Ordinary DAG Baseline: {base_dag_wall:.1f} ms ({dag_speedup:.2f}x speedup over sequential)\n")

    accuracies = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    sweep_data = []

    hdr = f"{'Accuracy':<9} | {'Latency (ms)':<15} | {'Speedup(Seq)':<13} | {'Speedup(DAG)':<13} | {'SpecPrecision':<14} | {'RollbackRate':<13} | {'Overhead':<10}"
    print(hdr)
    print("-" * 95)

    break_even_dag = None
    break_even_seq = None

    for acc in accuracies:
        trial_walls = []
        trial_tools = []
        trial_precisions = []
        trial_rollbacks = []

        for r in range(repetitions):
            random.seed(42 + int(acc * 100) * 17 + r * 31)
            d, s = create_accuracy_benchmark_workload(acc)
            m = SpeculativeDAGEngine.run(d, s, confidence_threshold=0.70)
            trial_walls.append(m.wall_clock_ms)
            trial_tools.append(m.total_tool_time_ms)
            trial_precisions.append(m.speculative_precision)
            total_disp = m.total_speculative_dispatched
            rb_rate = (m.speculative_discarded / total_disp) if total_disp > 0 else 0.0
            trial_rollbacks.append(rb_rate)

        mean_wall = statistics.mean(trial_walls)
        std_wall = statistics.stdev(trial_walls) if repetitions > 1 else 0.0
        mean_tool = statistics.mean(trial_tools)
        mean_prec = statistics.mean(trial_precisions)
        mean_rb = statistics.mean(trial_rollbacks)
        speedup_seq = base_seq_wall / mean_wall
        speedup_dag = base_dag_wall / mean_wall
        overhead = mean_tool / base_seq_tool

        if speedup_dag >= 1.0 and break_even_dag is None:
            break_even_dag = acc
        if speedup_seq >= 1.0 and break_even_seq is None:
            break_even_seq = acc

        entry = {
            "prediction_accuracy": acc,
            "mean_wall_clock_ms": round(mean_wall, 2),
            "std_wall_clock_ms": round(std_wall, 2),
            "speedup_over_sequential": round(speedup_seq, 2),
            "speedup_over_ordinary_dag": round(speedup_dag, 2),
            "speculative_precision": round(mean_prec, 4),
            "rollback_rate": round(mean_rb, 4),
            "compute_overhead_ratio": round(overhead, 2)
        }
        sweep_data.append(entry)

        row = f"{acc:<9.1f} | {mean_wall:<6.1f} +/- {std_wall:<5.1f} | {speedup_seq:<13.2f}x | {speedup_dag:<13.2f}x | {mean_prec*100:<13.1f}% | {mean_rb*100:<12.1f}% | {overhead:<10.2f}x"
        print(row)

    print("-" * 95)
    msg_seq = f"Empirical Break-Even Accuracy (vs Sequential): {break_even_seq*100:.0f}%" if break_even_seq is not None else "Break-Even vs Seq: Not reached"
    msg_dag = f"Empirical Break-Even Accuracy (vs Ordinary DAG): {break_even_dag*100:.0f}%" if break_even_dag is not None else "Break-Even vs DAG: Not reached"
    print(msg_seq)
    print(msg_dag)

    results_dir = os.path.join(os.path.dirname(__file__), "..", "data", "benchmark_results")
    os.makedirs(results_dir, exist_ok=True)
    out_file = os.path.join(results_dir, "accuracy_sweep_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({
            "experiment": "prediction_accuracy_sweep",
            "cache_status": "strictly_disabled_cold",
            "repetitions": repetitions,
            "baseline_sequential_ms": round(base_seq_wall, 2),
            "baseline_ordinary_dag_ms": round(base_dag_wall, 2),
            "break_even_vs_sequential": break_even_seq,
            "break_even_vs_ordinary_dag": break_even_dag,
            "sweep": sweep_data
        }, f, indent=2)
    print(f"\nSaved accuracy sweep results to: {out_file}")

if __name__ == "__main__":
    run_experiment(repetitions=5)
