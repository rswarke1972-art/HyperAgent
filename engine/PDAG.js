/**
 * Probabilistic Directed Acyclic Graph (P-DAG) Data Structure
 */
export class PDAG {
  constructor() {
    this.nodes = new Map();
    this.edges = [];
    this.adjList = new Map();
    this.inEdges = new Map();
  }

  addNode(id, name, executeFn, estimatedLatencyMs = 500, speculativePredictor = null) {
    if (this.nodes.has(id)) {
      throw new Error(`Duplicate node ID: ${id}`);
    }
    const node = { id, name, executeFn, estimatedLatencyMs, speculativePredictor };
    this.nodes.set(id, node);
    this.adjList.set(id, []);
    this.inEdges.set(id, []);
    return node;
  }

  addEdge(sourceId, targetId, paramKey, transitionProbability = 1.0) {
    if (!this.nodes.has(sourceId) || !this.nodes.has(targetId)) {
      throw new Error(`Invalid edge connection: ${sourceId} -> ${targetId}`);
    }
    const edge = { sourceId, targetId, paramKey, transitionProbability };
    this.edges.push(edge);
    this.adjList.get(sourceId).push(targetId);
    this.inEdges.get(targetId).push(edge);

    if (this.hasCycle()) {
      this.edges.pop();
      this.adjList.get(sourceId).pop();
      this.inEdges.get(targetId).pop();
      throw new Error(`Cycle detected when connecting ${sourceId} -> ${targetId}`);
    }
    return edge;
  }

  hasCycle() {
    const visited = new Map();
    for (const id of this.nodes.keys()) visited.set(id, 0); // 0: unvisited, 1: visiting, 2: visited

    const dfs = (u) => {
      visited.set(u, 1);
      for (const v of this.adjList.get(u) || []) {
        if (visited.get(v) === 1) return true;
        if (visited.get(v) === 0 && dfs(v)) return true;
      }
      visited.set(u, 2);
      return false;
    };

    for (const id of this.nodes.keys()) {
      if (visited.get(id) === 0 && dfs(id)) return true;
    }
    return false;
  }

  computeConcurrencyWaves() {
    const inDegrees = new Map();
    for (const id of this.nodes.keys()) {
      inDegrees.set(id, this.inEdges.get(id).length);
    }

    const waves = [];
    let currentWave = Array.from(this.nodes.keys()).filter(id => inDegrees.get(id) === 0);

    while (currentWave.length > 0) {
      waves.push([...currentWave].sort());
      const nextWave = [];
      for (const u of currentWave) {
        for (const v of this.adjList.get(u) || []) {
          const newDeg = inDegrees.get(v) - 1;
          inDegrees.set(v, newDeg);
          if (newDeg === 0) nextWave.push(v);
        }
      }
      currentWave = nextWave;
    }

    const totalProcessed = waves.reduce((sum, w) => sum + w.length, 0);
    if (totalProcessed !== this.nodes.size) {
      throw new Error("Unresolved dependency or graph cycle detected.");
    }
    return waves;
  }
}
