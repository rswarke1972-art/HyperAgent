import { SpeculativeExecutor } from '../engine/SpeculativeExecutor.js';
import { SemanticCache } from '../engine/SemanticCache.js';

export class ExecutionComparator {
  constructor(visualizer, telemetryCallback, logCallback) {
    this.visualizer = visualizer;
    this.telemetryCallback = telemetryCallback;
    this.logCallback = logCallback;
    this.cache = new SemanticCache();
    this.nodeStates = {};
  }

  log(tag, message) {
    if (this.logCallback) {
      this.logCallback(tag, message);
    }
  }

  updateNodeState(nid, state, dag) {
    this.nodeStates[nid] = state;
    this.visualizer.render(dag, this.nodeStates);
  }

  async run3WayDuel(workload, enableCache = true) {
    const { dag, initialState } = workload;
    
    // Reset states
    this.nodeStates = {};
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);
    this.log('DUEL_START', `=== Initiating 3-Way Algorithmic Benchmark Duel ===`);

    // 1. Run Sequential
    this.log('STAGE_1', `Starting Stage 1: Sequential serialized execution loop...`);
    const seqMetrics = await SpeculativeExecutor.runSequential(
      dag, initialState, 
      (nid, state) => this.updateNodeState(nid, state, dag),
      (tag, msg) => this.log(`[SEQ] ${tag}`, msg)
    );

    // Reset for Ordinary DAG
    await new Promise(r => setTimeout(r, 600));
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);

    // 2. Run Ordinary DAG
    this.log('STAGE_2', `Starting Stage 2: Ordinary DAG wave concurrency (Kahn's wave barriers)...`);
    const dagMetrics = await SpeculativeExecutor.runOrdinaryDAG(
      dag, initialState, 
      (nid, state) => this.updateNodeState(nid, state, dag),
      (tag, msg) => this.log(`[DAG] ${tag}`, msg)
    );

    // Reset for HyperAgent
    await new Promise(r => setTimeout(r, 600));
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);

    // 3. Run HyperAgent Speculative
    this.log('STAGE_3', `Starting Stage 3: HyperAgent Speculative Parallel Scheduling with Deterministic Rollback...`);
    const activeCache = enableCache ? this.cache : null;
    const hyperMetrics = await SpeculativeExecutor.runHyperAgent(
      dag, initialState, activeCache, 0.70, 
      (nid, state) => this.updateNodeState(nid, state, dag),
      (tag, msg) => this.log(`[HYPER] ${tag}`, msg)
    );

    // Compute Speedup Deltas
    const speedupDAG = seqMetrics.wallClockMs / dagMetrics.wallClockMs;
    const speedupHyper = seqMetrics.wallClockMs / hyperMetrics.wallClockMs;
    const specDelta = speedupHyper - speedupDAG;

    const summary = {
      sequential: seqMetrics,
      ordinaryDAG: dagMetrics,
      hyperAgent: hyperMetrics,
      speedupDAG: Number(speedupDAG.toFixed(2)),
      speedupHyper: Number(speedupHyper.toFixed(2)),
      specDelta: Number(specDelta.toFixed(2)),
      specPrecision: hyperMetrics.specPrecision,
      cacheTelemetry: hyperMetrics.cacheTelemetry
    };

    this.log('SUMMARY', `Duel complete! Speedup: DAG ${summary.speedupDAG}x | HyperAgent ${summary.speedupHyper}x (Net speculation delta: +${summary.specDelta}x)`);

    if (this.telemetryCallback) {
      this.telemetryCallback(summary);
    }

    return summary;
  }
}
