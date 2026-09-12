/**
 * SemanticCache: In-Memory LRU with empirical p50/p95/p99 telemetry
 */
export class SemanticCache {
  constructor(maxSize = 256) {
    this.maxSize = maxSize;
    this.cache = new Map();
    this.accessTimesMs = [];
    this.hits = 0;
    this.misses = 0;
  }

  get(key) {
    const t0 = performance.now();
    const val = this.cache.get(key);
    const elapsed = performance.now() - t0;
    this.accessTimesMs.push(elapsed);

    if (val !== undefined) {
      this.hits++;
      return JSON.parse(JSON.stringify(val));
    }
    this.misses++;
    return null;
  }

  put(key, val) {
    if (this.cache.size >= this.maxSize && !this.cache.has(key)) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, JSON.parse(JSON.stringify(val)));
  }

  getTelemetry() {
    const total = this.hits + this.misses;
    const hitRatio = total > 0 ? this.hits / total : 0.0;
    const times = [...this.accessTimesMs].sort((a, b) => a - b);
    
    let p50 = 0, p95 = 0, p99 = 0;
    if (times.length > 0) {
      p50 = times[Math.floor(times.length * 0.50)];
      p95 = times[Math.floor(times.length * 0.95)] || times[times.length - 1];
      p99 = times[Math.floor(times.length * 0.99)] || times[times.length - 1];
    }

    return {
      hits: this.hits,
      misses: this.misses,
      hitRatio: Number(hitRatio.toFixed(4)),
      p50_ms: Number(p50.toFixed(4)),
      p95_ms: Number(p95.toFixed(4)),
      p99_ms: Number(p99.toFixed(4))
    };
  }
}
