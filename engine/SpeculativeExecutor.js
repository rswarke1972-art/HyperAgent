import { StateManager } from './StateManager.js';

export class SpeculativeExecutor {
  static async runSequential(dag, initialState, onNodeStateChange) {
    const stateMgr = new StateManager(initialState);
    const waves = dag.computeConcurrencyWaves();
    const orderedNodes = waves.flat();
    
    const t0 = performance.now();
    let accumulatedToolTime = 0;

    for (const nid of orderedNodes) {
      const node = dag.nodes.get(nid);
      if (onNodeStateChange) onNodeStateChange(nid, 'RUNNING');
      
      const currState = stateMgr.getCanonical();
      const nodeInputs = {};
      for (const e of dag.inEdges.get(nid)) {
        nodeInputs[e.paramKey] = currState[e.paramKey];
      }

      const toolT0 = performance.now();
      const res = await node.executeFn(nodeInputs);
      const toolTime = performance.now() - toolT0;
      accumulatedToolTime += toolTime;

      stateMgr.commitSpeculativeBranch(nid, nid, res);
      if (onNodeStateChange) onNodeStateChange(nid, 'RESOLVED');
    }

    const wallClockMs = performance.now() - t0;
    return {
      mode: 'Sequential',
      wallClockMs,
      accumulatedToolTime,
      finalState: stateMgr.getCanonical()
    };
  }

  static async runOrdinaryDAG(dag, initialState, onNodeStateChange) {
    const stateMgr = new StateManager(initialState);
    const waves = dag.computeConcurrencyWaves();
    
    const t0 = performance.now();
    let accumulatedToolTime = 0;

    for (const wave of waves) {
      if (onNodeStateChange) {
        wave.forEach(nid => onNodeStateChange(nid, 'RUNNING'));
      }

      const currState = stateMgr.getCanonical();
      const wavePromises = wave.map(async (nid) => {
        const node = dag.nodes.get(nid);
        const nodeInputs = {};
        for (const e of dag.inEdges.get(nid)) {
          nodeInputs[e.paramKey] = currState[e.paramKey];
        }
        const toolT0 = performance.now();
        const res = await node.executeFn(nodeInputs);
        const toolTime = performance.now() - toolT0;
        return { nid, res, toolTime };
      });

      const results = await Promise.all(wavePromises);
      for (const { nid, res, toolTime } of results) {
        accumulatedToolTime += toolTime;
        stateMgr.commitSpeculativeBranch(nid, nid, res);
        if (onNodeStateChange) onNodeStateChange(nid, 'RESOLVED');
      }
    }

    const wallClockMs = performance.now() - t0;
    return {
      mode: 'Ordinary_DAG',
      wallClockMs,
      accumulatedToolTime,
      finalState: stateMgr.getCanonical()
    };
  }

  static async runHyperAgent(dag, initialState, cache, confidenceThreshold = 0.70, onNodeStateChange) {
    const stateMgr = new StateManager(initialState);
    const waves = dag.computeConcurrencyWaves();
    
    const t0 = performance.now();
    let accumulatedToolTime = 0;
    let speculativeDispatched = 0;
    let speculativeCommitted = 0;
    let speculativeDiscarded = 0;

    const speculativePool = new Map();

    const executeToolWithCache = async (node, inputs, isSpec = false) => {
      const cacheKey = `${node.id}:${JSON.stringify(inputs)}`;
      const cached = cache.get(cacheKey);
      if (cached !== null) {
        return { res: cached, toolTime: 0.5, cacheHit: true };
      }

      const toolT0 = performance.now();
      const res = await node.executeFn(inputs);
      const toolTime = performance.now() - toolT0;
      cache.put(cacheKey, res);
      return { res, toolTime, cacheHit: false };
    };

    for (let waveIdx = 0; waveIdx < waves.length; waveIdx++) {
      const wave = waves[waveIdx];

      // Speculative pre-dispatch for next wave
      if (waveIdx + 1 < waves.length) {
        const nextWave = waves[waveIdx + 1];
        for (const nextNid of nextWave) {
          const nextNode = dag.nodes.get(nextNid);
          let conf = 1.0;
          for (const e of dag.inEdges.get(nextNid)) {
            conf *= e.transitionProbability;
          }

          if (conf >= confidenceThreshold && nextNode.speculativePredictor) {
            const branchId = `spec_${nextNid}_${Math.random().toString(36).substring(2, 7)}`;
            const predictedInputs = nextNode.speculativePredictor(stateMgr.getCanonical());
            stateMgr.createSpeculativeBranch(branchId, predictedInputs);

            speculativeDispatched++;
            if (onNodeStateChange) onNodeStateChange(nextNid, 'SPECULATIVE_RUNNING');

            const promise = executeToolWithCache(nextNode, predictedInputs, true);
            speculativePool.set(nextNid, { promise, branchId, predictedInputs });
          }
        }
      }

      // Current wave execution
      const currState = stateMgr.getCanonical();
      const wavePromises = [];

      for (const nid of wave) {
        const node = dag.nodes.get(nid);
        const actualInputs = {};
        for (const e of dag.inEdges.get(nid)) {
          actualInputs[e.paramKey] = currState[e.paramKey];
        }

        if (speculativePool.has(nid)) {
          const { promise, branchId, predictedInputs } = speculativePool.get(nid);
          speculativePool.delete(nid);

          wavePromises.push((async () => {
            const { res, toolTime, cacheHit } = await promise;
            accumulatedToolTime += toolTime;

            const predStr = JSON.stringify(predictedInputs);
            const actStr = JSON.stringify(actualInputs);

            if (predStr === actStr) {
              speculativeCommitted++;
              stateMgr.commitSpeculativeBranch(branchId, nid, res);
              if (onNodeStateChange) onNodeStateChange(nid, cacheHit ? 'CACHE_HIT' : 'RESOLVED');
            } else {
              speculativeDiscarded++;
              stateMgr.discardSpeculativeBranch(branchId);
              if (onNodeStateChange) onNodeStateChange(nid, 'ROLLED_BACK');

              // Re-run with valid inputs
              const retryRes = await executeToolWithCache(node, actualInputs, false);
              accumulatedToolTime += retryRes.toolTime;
              stateMgr.commitSpeculativeBranch(nid, nid, retryRes.res);
              if (onNodeStateChange) onNodeStateChange(nid, 'RESOLVED');
            }
          })());
        } else {
          if (onNodeStateChange) onNodeStateChange(nid, 'RUNNING');
          wavePromises.push((async () => {
            const { res, toolTime, cacheHit } = await executeToolWithCache(node, actualInputs, false);
            accumulatedToolTime += toolTime;
            stateMgr.commitSpeculativeBranch(nid, nid, res);
            if (onNodeStateChange) onNodeStateChange(nid, cacheHit ? 'CACHE_HIT' : 'RESOLVED');
          })());
        }
      }

      await Promise.all(wavePromises);
    }

    const wallClockMs = performance.now() - t0;
    const specPrecision = speculativeDispatched > 0 ? speculativeCommitted / speculativeDispatched : 1.0;

    return {
      mode: 'HyperAgent_Complete',
      wallClockMs,
      accumulatedToolTime,
      speculativeDispatched,
      speculativeCommitted,
      speculativeDiscarded,
      speculativePrecision: Number(specPrecision.toFixed(4)),
      cacheTelemetry: cache.getTelemetry(),
      finalState: stateMgr.getCanonical()
    };
  }
}
