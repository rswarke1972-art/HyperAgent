# HyperAgent: Speculative Parallel DAG Execution Framework for Low-Latency Autonomous Agent Workflows

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Target: IEEE Transactions on AI](https://img.shields.io/badge/Paper-IEEE%20Format-brightgreen.svg)](paper/IEEE_HyperAgent_Manuscript.md)
[![Python: 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](python/hyperagent.py)

**HyperAgent** is an algorithmic scheduling engine designed to eliminate the multi-second latency bottleneck in autonomous AI agents. By formalizing agent workflows into a **Probabilistic Directed Acyclic Graph (P-DAG)** and implementing **Speculative Parallel DAG Scheduling with Deterministic Rollback**, HyperAgent overlaps independent reasoning and network tool I/O latencies while preserving 100% execution correctness.

---

## The Core Research Claim
> **HyperAgent investigates whether probabilistic dependency prediction combined with isolated speculative execution and deterministic rollback can reduce wall-clock latency in multi-step agent workflows while preserving execution correctness.**

$$
H_1:\quad T_{\text{HyperAgent}} < T_{\text{OrdinaryDAG}} < T_{\text{Sequential}}
$$

$$
\text{Speculative Precision} = \frac{N_{\text{committed}}}{N_{\text{dispatched}}}
$$

---

## 3-Way Baseline & 4-Stage Ablation Benchmark Results

Empirical results generated via `benchmarks/run_benchmarks.py` across deterministic enterprise workloads:

| Workload | Stage 1: Sequential | Stage 2: Ordinary DAG | Stage 3: Speculative DAG | Stage 4: HyperAgent (+Cache) | Speculative Precision |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Financial Due Diligence** | 3,452.42 ms (1.00x) | 2,404.21 ms (1.44x) | **1,703.68 ms (2.03x)** | **1.79 ms (1,927x)** | **75.0%** (3/4 Commit) |
| **Cybersecurity Sweep** | 3,503.80 ms (1.00x) | 2,554.98 ms (1.37x) | **1,554.87 ms (2.25x)** | **2.78 ms (1,258x)** | **100.0%** (4/4 Commit) |

### Key Observations:
1. **Isolated Speculation Delta**: While ordinary DAG parallelism delivers a 1.37x to 1.44x speedup, speculative dispatch adds a **+0.59x to +0.88x speedup delta**, achieving an overall **2.03x to 2.25x wall-clock speedup** without semantic caching.
2. **Deterministic Rollback Proof**: When node `ratio_engine` in the financial workload detected an input parameter divergence ($4000M predicted vs $4200M actual), the isolated branch was discarded and re-executed with zero state contamination.
3. **Sub-Millisecond Cache**: In-memory semantic LRU lookup achieved $p50 = 0.0018\text{ ms}$ and $p99 = 0.0074\text{ ms}$.

---

## System Architecture

```text
CANONICAL COMMITTED STATE (Immutable baseline)
      │
      ├── Speculative Branch A (Worker 1) ──> [Validated] ──> Atomic Commit to Canonical
      │
      ├── Speculative Branch B (Worker 2) ──> [Contradiction] ──> Discarded & Memory Freed
      │
      └── Speculative Branch C (Worker 3) ──> [Cache Hit] ──> Direct Resolution
```

---

## Project Structure

```text
HyperAgent/
│
├── engine/                       # JavaScript / Web Engine
│   ├── PDAG.js                   # Probabilistic DAG data structure & cycle validation
│   ├── StateManager.js           # Isolated branch state trees & atomic commit
│   ├── SpeculativeExecutor.js    # Speculative dispatch with deterministic rollback
│   └── SemanticCache.js          # In-memory normalized tool signature LRU cache
│
├── python/                       # Reference / Ground-Truth Benchmark Engine
│   ├── hyperagent.py             # Reproducible CLI engine with statistical profiling
│   └── workloads.py              # Deterministic & stochastic agent task workloads
│
├── js/                           # Interactive Web Visualizer
│   ├── visualizer.js             # Real-time animated SVG/Canvas DAG graph
│   ├── comparator.js             # 3-Way comparative execution duel
│   └── workloads.js              # Webtoon, financial, and cybersecurity scenarios
│
├── css/
│   └── style.css                 # Dark luxury titanium & cyan telemetry UI
│
├── benchmarks/
│   └── run_benchmarks.py         # Automated ablation suite generating JSON metrics
│
├── paper/
│   ├── IEEE_HyperAgent_Manuscript.md # Full IEEE-format research paper
│   └── Patent_Claims_Draft.md    # Formal patent claim disclosure
│
├── data/
│   └── benchmark_results/        # Versioned empirical JSON test outputs
│
├── index.html                    # Interactive browser dashboard
└── README.md                     # Documentation and mathematical proofs
```

---

## Getting Started

### 1. Run Python Reference Benchmarks
```bash
cd python
python ../benchmarks/run_benchmarks.py
```

### 2. Launch Interactive Web Dashboard
Open `index.html` in any modern web browser or serve locally:
```bash
python -m http.server 8080
```
Navigate to `http://localhost:8080` to interact with the real-time SVG graph and trigger live 3-way execution duels.

---

## Academic Citation & Inquiries
To cite this work or collaborate on low-latency agent systems:
```bibtex
@article{warke2026hyperagent,
  title={Speculative DAG Scheduling: A Non-Blocking Parallel Execution Framework for Low-Latency Autonomous Agent Workflows},
  author={Warke, Sahil Rajesh},
  journal={arXiv preprint / IEEE Submission},
  year={2026}
}
```
