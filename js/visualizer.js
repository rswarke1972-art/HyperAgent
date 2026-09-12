/**
 * Interactive Real-Time SVG Graph Visualizer for P-DAG
 */
export class DAGVisualizer {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.nodePositions = new Map();
  }

  render(dag, nodeStates = {}) {
    if (!this.container) return;
    this.container.innerHTML = '';

    const waves = dag.computeConcurrencyWaves();
    const width = this.container.clientWidth || 800;
    const height = this.container.clientHeight || 450;

    const waveCount = waves.length;
    const colWidth = width / (waveCount + 1);

    this.nodePositions.clear();

    // Compute coordinates
    waves.forEach((wave, waveIdx) => {
      const x = colWidth * (waveIdx + 1);
      const rowHeight = height / (wave.length + 1);

      wave.forEach((nid, nodeIdx) => {
        const y = rowHeight * (nodeIdx + 1);
        this.nodePositions.set(nid, { x, y });
      });
    });

    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '100%');
    svg.setAttribute('height', '100%');
    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);

    // Render Edges
    dag.edges.forEach(edge => {
      const src = this.nodePositions.get(edge.sourceId);
      const tgt = this.nodePositions.get(edge.targetId);
      if (src && tgt) {
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        const dx = (tgt.x - src.x) * 0.5;
        const d = `M ${src.x + 80} ${src.y} C ${src.x + 80 + dx} ${src.y}, ${tgt.x - 80 - dx} ${tgt.y}, ${tgt.x - 80} ${tgt.y}`;
        path.setAttribute('d', d);
        path.setAttribute('fill', 'none');
        path.setAttribute('stroke', 'rgba(148, 163, 184, 0.3)');
        path.setAttribute('stroke-width', '2');
        path.setAttribute('stroke-dasharray', '4,4');
        svg.appendChild(path);
      }
    });

    // Render Nodes
    for (const [nid, node] of dag.nodes.entries()) {
      const pos = this.nodePositions.get(nid);
      if (!pos) continue;

      const state = nodeStates[nid] || 'QUEUED';
      
      const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      g.setAttribute('transform', `translate(${pos.x}, ${pos.y})`);
      g.setAttribute('class', `dag-node-group node-state-${state.toLowerCase()}`);

      // Node card rect
      const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      rect.setAttribute('x', '-75');
      rect.setAttribute('y', '-26');
      rect.setAttribute('width', '150');
      rect.setAttribute('height', '52');
      rect.setAttribute('rx', '8');
      rect.setAttribute('class', 'dag-node-rect');
      g.appendChild(rect);

      // Node Name
      const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      text.setAttribute('x', '0');
      text.setAttribute('y', '-6');
      text.setAttribute('text-anchor', 'middle');
      text.setAttribute('class', 'dag-node-title');
      text.textContent = node.name.length > 18 ? node.name.substring(0, 16) + '..' : node.name;
      g.appendChild(text);

      // Node State & Latency Badge
      const sub = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      sub.setAttribute('x', '0');
      sub.setAttribute('y', '12');
      sub.setAttribute('text-anchor', 'middle');
      sub.setAttribute('class', 'dag-node-meta');
      sub.textContent = `${state} · ${node.estimatedLatencyMs}ms`;
      g.appendChild(sub);

      svg.appendChild(g);
    }

    this.container.appendChild(svg);
  }
}
