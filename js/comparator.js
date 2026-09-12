import { SpeculativeExecutor } from '../engine/SpeculativeExecutor.js';
import { SemanticCache } from '../engine/SemanticCache.js';

export class ExecutionComparator {
  constructor(visualizer, telemetryCallback) {
    this.visualizer = visualizer;
    this.telemetryCallback = telemetryCallback;
    this.cache = new SemanticCache();
    this.nodeStates = {};
  }

  updateNodeState(nid, state, dag) {
    this.nodeStates[nid] = state;
    this.visualizer.render(dag, this.nodeStates);
  }

  async run3WayDuel(workload) {
    const { dag, initialState } = workload;
    
    // Reset states
    this.nodeStates = {};
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);

    // 1. Run Sequential
    const seqMetrics = await SpeculativeExecutor.runSequential(dag, initialState, (nid, state) => {
      this.updateNodeState(nid, state, dag);
    });

    // Reset for Ordinary DAG
    await new Promise(r => setTimeout(r, 600));
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);

    const dagMetrics = await SpeculativeExecutor.runOrdinaryDAG(dag, initialState, (nid, state) => {
      this.updateNodeState(nid, state, dag);
    });

    // Reset for HyperAgent
    await new Promise(r => setTimeout(r, 600));
    for (const nid of dag.nodes.keys()) this.nodeStates[nid] = 'QUEUED';
    this.visualizer.render(dag, this.nodeStates);

    const hyperMetrics = await SpeculativeExecutor.runHyperAgent(dag, initialState, this.cache, 0.70, (nid, state) => {
      this.updateNodeState(nid, state, dag);
    });

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

    if (this.telemetryCallback) {
      this.telemetryCallback(summary);
    }

    return summary;
  }
}
