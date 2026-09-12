"""
HyperAgent: Speculative Parallel DAG Framework for Low-Latency Autonomous Agent Workflows
Reference Benchmark Implementation in Python 3.
"""

import time
import uuid
import copy
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Any, Optional, Tuple, Callable
import statistics


class Node:
    def __init__(self, node_id: str, name: str, execute_fn: Callable[[Dict[str, Any]], Any], 
                 estimated_latency_ms: float = 100.0, 
                 speculative_predictor: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None):
        self.id = node_id
        self.name = name
        self.execute_fn = execute_fn
        self.estimated_latency_ms = estimated_latency_ms
        self.speculative_predictor = speculative_predictor


class Edge:
    def __init__(self, source_id: str, target_id: str, param_key: str, 
                 transition_probability: float = 1.0):
        self.source_id = source_id
        self.target_id = target_id
        self.param_key = param_key
        self.transition_probability = transition_probability


class PDAG:
    """Probabilistic Directed Acyclic Graph with cycle detection and wave partitioning."""
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.edges: List[Edge] = []
        self.adj_list: Dict[str, List[str]] = {}
        self.in_edges: Dict[str, List[Edge]] = {}

    def add_node(self, node: Node):
        if node.id in self.nodes:
            raise ValueError(f"Duplicate node ID: {node.id}")
        self.nodes[node.id] = node
        self.adj_list[node.id] = []
        self.in_edges[node.id] = []

    def add_edge(self, source_id: str, target_id: str, param_key: str, 
                 transition_probability: float = 1.0):
        if source_id not in self.nodes or target_id not in self.nodes:
            raise ValueError(f"Invalid edge: {source_id} -> {target_id}")
        
        edge = Edge(source_id, target_id, param_key, transition_probability)
        self.edges.append(edge)
        self.adj_list[source_id].append(target_id)
        self.in_edges[target_id].append(edge)

        if self.has_cycle():
            self.edges.pop()
            self.adj_list[source_id].pop()
            self.in_edges[target_id].pop()
            raise ValueError(f"Cycle detected when adding edge {source_id} -> {target_id}")

    def has_cycle(self) -> bool:
        visited: Dict[str, int] = {nid: 0 for nid in self.nodes}

        def dfs(u: str) -> bool:
            visited[u] = 1
            for v in self.adj_list.get(u, []):
                if visited[v] == 1:
                    return True
                if visited[v] == 0:
                    if dfs(v):
                        return True
            visited[u] = 2
            return False

        for nid in self.nodes:
            if visited[nid] == 0:
                if dfs(nid):
                    return True
        return False

    def compute_concurrency_waves(self) -> List[List[str]]:
        """Kahn's Algorithm extended to compute ordered concurrent execution waves."""
        in_degrees: Dict[str, int] = {nid: len(self.in_edges[nid]) for nid in self.nodes}
        waves: List[List[str]] = []

        current_wave = [nid for nid, deg in in_degrees.items() if deg == 0]

        while current_wave:
            waves.append(sorted(current_wave))
            next_wave = []
            for u in current_wave:
                for v in self.adj_list[u]:
                    in_degrees[v] -= 1
                    if in_degrees[v] == 0:
                        next_wave.append(v)
            current_wave = next_wave

        total_processed = sum(len(w) for w in waves)
        if total_processed != len(self.nodes):
            raise RuntimeError("Graph has an unresolved dependency or cycle.")

        return waves


class StateManager:
    """Manages Canonical Committed State and Isolated Speculative Branches."""
    def __init__(self, initial_state: Optional[Dict[str, Any]] = None):
        self.lock = threading.Lock()
        self.canonical_state: Dict[str, Any] = copy.deepcopy(initial_state or {})
        self.speculative_branches: Dict[str, Dict[str, Any]] = {}

    def create_speculative_branch(self, branch_id: str, predicted_state: Dict[str, Any]) -> Dict[str, Any]:
        with self.lock:
            branch = copy.deepcopy(self.canonical_state)
            branch.update(predicted_state)
            self.speculative_branches[branch_id] = branch
            return branch

    def commit_speculative_branch(self, branch_id: str, result_key: str, result_val: Any) -> None:
        with self.lock:
            self.canonical_state[result_key] = result_val
            if branch_id in self.speculative_branches:
                del self.speculative_branches[branch_id]

    def discard_speculative_branch(self, branch_id: str) -> None:
        with self.lock:
            if branch_id in self.speculative_branches:
                del self.speculative_branches[branch_id]

    def get_canonical(self) -> Dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.canonical_state)


class SemanticCache:
    """In-memory thread-safe LRU cache with empirical latency telemetry."""
    def __init__(self, max_size: int = 256):
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.access_times_ns: List[int] = []
        self.hits = 0
        self.misses = 0
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        t0 = time.perf_counter_ns()
        with self.lock:
            val = self.cache.get(key)
            t1 = time.perf_counter_ns()
            self.access_times_ns.append(t1 - t0)
            if val is not None:
                self.hits += 1
                return copy.deepcopy(val)
            self.misses += 1
            return None

    def put(self, key: str, val: Any) -> None:
        with self.lock:
            if len(self.cache) >= self.max_size and key not in self.cache:
                first_key = next(iter(self.cache))
                del self.cache[first_key]
            self.cache[key] = copy.deepcopy(val)

    def get_telemetry(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hits + self.misses
            hit_ratio = (self.hits / total) if total > 0 else 0.0
            times_ms = [ns / 1_000_000.0 for ns in self.access_times_ns]
            if times_ms:
                times_ms.sort()
                p50 = statistics.median(times_ms)
                p95 = times_ms[int(len(times_ms) * 0.95)] if len(times_ms) > 1 else times_ms[0]
                p99 = times_ms[int(len(times_ms) * 0.99)] if len(times_ms) > 1 else times_ms[-1]
            else:
                p50 = p95 = p99 = 0.0
            
            return {
                "hits": self.hits,
                "misses": self.misses,
                "hit_ratio": round(hit_ratio, 4),
                "p50_ms": round(p50, 4),
                "p95_ms": round(p95, 4),
                "p99_ms": round(p99, 4),
            }


class ExecutionMetrics:
    def __init__(self, mode: str):
        self.mode = mode
        self.wall_clock_ms: float = 0.0
        self.total_tool_time_ms: float = 0.0
        self.total_speculative_dispatched: int = 0
        self.speculative_committed: int = 0
        self.speculative_discarded: int = 0
        self.cache_telemetry: Dict[str, Any] = {}
        self.final_state: Dict[str, Any] = {}

    @property
    def speculative_precision(self) -> float:
        if self.total_speculative_dispatched == 0:
            return 1.0
        return self.speculative_committed / self.total_speculative_dispatched

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "wall_clock_ms": round(self.wall_clock_ms, 2),
            "total_tool_time_ms": round(self.total_tool_time_ms, 2),
            "speculative_dispatched": self.total_speculative_dispatched,
            "speculative_committed": self.speculative_committed,
            "speculative_discarded": self.speculative_discarded,
            "speculative_precision": round(self.speculative_precision, 4),
            "cache": self.cache_telemetry,
        }


class SequentialEngine:
    @staticmethod
    def run(dag: PDAG, initial_state: Dict[str, Any]) -> ExecutionMetrics:
        metrics = ExecutionMetrics(mode="Sequential")
        state_mgr = StateManager(initial_state)
        waves = dag.compute_concurrency_waves()
        ordered_nodes = [nid for wave in waves for nid in wave]
        
        t0 = time.perf_counter()
        accumulated_tool_time = 0.0

        for nid in ordered_nodes:
            node = dag.nodes[nid]
            curr_state = state_mgr.get_canonical()
            node_inputs = {e.param_key: curr_state.get(e.param_key) for e in dag.in_edges[nid]}
            
            tool_t0 = time.perf_counter()
            result = node.execute_fn(node_inputs)
            tool_t1 = time.perf_counter()
            accumulated_tool_time += (tool_t1 - tool_t0) * 1000.0
            
            state_mgr.commit_speculative_branch(nid, nid, result)

        t1 = time.perf_counter()
        metrics.wall_clock_ms = (t1 - t0) * 1000.0
        metrics.total_tool_time_ms = accumulated_tool_time
        metrics.final_state = state_mgr.get_canonical()
        return metrics


class OrdinaryDAGEngine:
    @staticmethod
    def run(dag: PDAG, initial_state: Dict[str, Any], max_workers: int = 8) -> ExecutionMetrics:
        metrics = ExecutionMetrics(mode="Ordinary_DAG")
        state_mgr = StateManager(initial_state)
        waves = dag.compute_concurrency_waves()
        
        t0 = time.perf_counter()
        accumulated_tool_time = 0.0
        tool_time_lock = threading.Lock()

        def execute_node(nid: str, curr_state: Dict[str, Any]) -> Tuple[str, Any, float]:
            node = dag.nodes[nid]
            node_inputs = {e.param_key: curr_state.get(e.param_key) for e in dag.in_edges[nid]}
            start_t = time.perf_counter()
            res = node.execute_fn(node_inputs)
            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            return nid, res, elapsed_ms

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for wave in waves:
                curr_state = state_mgr.get_canonical()
                futures = [pool.submit(execute_node, nid, curr_state) for nid in wave]
                
                for f in as_completed(futures):
                    nid, res, tool_ms = f.result()
                    with tool_time_lock:
                        accumulated_tool_time += tool_ms
                    state_mgr.commit_speculative_branch(nid, nid, res)

        t1 = time.perf_counter()
        metrics.wall_clock_ms = (t1 - t0) * 1000.0
        metrics.total_tool_time_ms = accumulated_tool_time
        metrics.final_state = state_mgr.get_canonical()
        return metrics


class SpeculativeDAGEngine:
    @staticmethod
    def run(dag: PDAG, initial_state: Dict[str, Any], 
            confidence_threshold: float = 0.70, max_workers: int = 8) -> ExecutionMetrics:
        metrics = ExecutionMetrics(mode="Speculative_DAG")
        state_mgr = StateManager(initial_state)
        waves = dag.compute_concurrency_waves()
        
        t0 = time.perf_counter()
        accumulated_tool_time = 0.0
        tool_time_lock = threading.Lock()

        speculative_pool: Dict[str, Tuple[Any, str, Dict[str, Any]]] = {}

        def run_tool(node: Node, inputs: Dict[str, Any]) -> Tuple[Any, float]:
            start_t = time.perf_counter()
            res = node.execute_fn(inputs)
            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            return res, elapsed_ms

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for wave_idx, wave in enumerate(waves):
                if wave_idx + 1 < len(waves):
                    next_wave = waves[wave_idx + 1]
                    for next_nid in next_wave:
                        next_node = dag.nodes[next_nid]
                        conf = 1.0
                        for e in dag.in_edges[next_nid]:
                            conf *= e.transition_probability
                        
                        if conf >= confidence_threshold and next_node.speculative_predictor:
                            branch_id = f"spec_{next_nid}_{uuid.uuid4().hex[:6]}"
                            predicted_inputs = next_node.speculative_predictor(state_mgr.get_canonical())
                            state_mgr.create_speculative_branch(branch_id, predicted_inputs)
                            
                            metrics.total_speculative_dispatched += 1
                            fut = pool.submit(run_tool, next_node, predicted_inputs)
                            speculative_pool[next_nid] = (fut, branch_id, predicted_inputs)

                curr_state = state_mgr.get_canonical()
                wave_futures = []

                for nid in wave:
                    node = dag.nodes[nid]
                    actual_inputs = {e.param_key: curr_state.get(e.param_key) for e in dag.in_edges[nid]}

                    if nid in speculative_pool:
                        fut, branch_id, pred_inputs = speculative_pool.pop(nid)
                        spec_res, tool_ms = fut.result()
                        with tool_time_lock:
                            accumulated_tool_time += tool_ms
                        
                        if pred_inputs == actual_inputs:
                            metrics.speculative_committed += 1
                            state_mgr.commit_speculative_branch(branch_id, nid, spec_res)
                        else:
                            metrics.speculative_discarded += 1
                            state_mgr.discard_speculative_branch(branch_id)
                            wave_futures.append((nid, pool.submit(run_tool, node, actual_inputs)))
                    else:
                        wave_futures.append((nid, pool.submit(run_tool, node, actual_inputs)))

                for nid, fut in wave_futures:
                    res, tool_ms = fut.result()
                    with tool_time_lock:
                        accumulated_tool_time += tool_ms
                    state_mgr.commit_speculative_branch(nid, nid, res)

        t1 = time.perf_counter()
        metrics.wall_clock_ms = (t1 - t0) * 1000.0
        metrics.total_tool_time_ms = accumulated_tool_time
        metrics.final_state = state_mgr.get_canonical()
        return metrics


class HyperAgentEngine:
    @staticmethod
    def run(dag: PDAG, initial_state: Dict[str, Any], cache: SemanticCache,
            confidence_threshold: float = 0.70, max_workers: int = 8) -> ExecutionMetrics:
        metrics = ExecutionMetrics(mode="HyperAgent_Complete")
        state_mgr = StateManager(initial_state)
        waves = dag.compute_concurrency_waves()
        
        t0 = time.perf_counter()
        accumulated_tool_time = 0.0
        tool_time_lock = threading.Lock()

        speculative_pool: Dict[str, Tuple[Any, str, Dict[str, Any]]] = {}

        def run_tool_with_cache(node: Node, inputs: Dict[str, Any]) -> Tuple[Any, float]:
            cache_key = f"{node.id}:{str(sorted(inputs.items()))}"
            cached_val = cache.get(cache_key)
            if cached_val is not None:
                return cached_val, 0.5
            
            start_t = time.perf_counter()
            res = node.execute_fn(inputs)
            elapsed_ms = (time.perf_counter() - start_t) * 1000.0
            cache.put(cache_key, res)
            return res, elapsed_ms

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for wave_idx, wave in enumerate(waves):
                if wave_idx + 1 < len(waves):
                    next_wave = waves[wave_idx + 1]
                    for next_nid in next_wave:
                        next_node = dag.nodes[next_nid]
                        conf = 1.0
                        for e in dag.in_edges[next_nid]:
                            conf *= e.transition_probability
                        
                        if conf >= confidence_threshold and next_node.speculative_predictor:
                            branch_id = f"spec_{next_nid}_{uuid.uuid4().hex[:6]}"
                            predicted_inputs = next_node.speculative_predictor(state_mgr.get_canonical())
                            state_mgr.create_speculative_branch(branch_id, predicted_inputs)
                            
                            metrics.total_speculative_dispatched += 1
                            fut = pool.submit(run_tool_with_cache, next_node, predicted_inputs)
                            speculative_pool[next_nid] = (fut, branch_id, predicted_inputs)

                curr_state = state_mgr.get_canonical()
                wave_futures = []

                for nid in wave:
                    node = dag.nodes[nid]
                    actual_inputs = {e.param_key: curr_state.get(e.param_key) for e in dag.in_edges[nid]}

                    if nid in speculative_pool:
                        fut, branch_id, pred_inputs = speculative_pool.pop(nid)
                        spec_res, tool_ms = fut.result()
                        with tool_time_lock:
                            accumulated_tool_time += tool_ms
                        
                        if pred_inputs == actual_inputs:
                            metrics.speculative_committed += 1
                            state_mgr.commit_speculative_branch(branch_id, nid, spec_res)
                        else:
                            metrics.speculative_discarded += 1
                            state_mgr.discard_speculative_branch(branch_id)
                            wave_futures.append((nid, pool.submit(run_tool_with_cache, node, actual_inputs)))
                    else:
                        wave_futures.append((nid, pool.submit(run_tool_with_cache, node, actual_inputs)))

                for nid, fut in wave_futures:
                    res, tool_ms = fut.result()
                    with tool_time_lock:
                        accumulated_tool_time += tool_ms
                    state_mgr.commit_speculative_branch(nid, nid, res)

        t1 = time.perf_counter()
        metrics.wall_clock_ms = (t1 - t0) * 1000.0
        metrics.total_tool_time_ms = accumulated_tool_time
        metrics.final_state = state_mgr.get_canonical()
        metrics.cache_telemetry = cache.get_telemetry()
        return metrics
