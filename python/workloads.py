"""
Deterministic & Stochastic Agentic Workloads for HyperAgent Benchmarking.
"""

import time
from typing import Dict, Any, Tuple
from hyperagent import PDAG, Node

def create_financial_diligence_workload(deterministic: bool = True) -> Tuple[PDAG, Dict[str, Any]]:
    """
    Workload 1: Financial Due Diligence Intelligence Pipeline
    6 Nodes, 3 Concurrency Waves.
    """
    dag = PDAG()

    def simulate_tool(name: str, latency_ms: float, result_val: Any):
        def fn(inputs: Dict[str, Any]) -> Any:
            time.sleep(latency_ms / 1000.0)
            return result_val
        return fn

    # 1. SEC Parser (800ms)
    n1 = Node(
        node_id="sec_parser",
        name="SEC 10-K Parser",
        execute_fn=simulate_tool("sec_parser", 800, {"revenue_m": 4200, "debt_m": 1200, "ebitda_m": 850}),
        estimated_latency_ms=800
    )

    # 2. News Macro Sentiment (600ms, independent of SEC)
    n2 = Node(
        node_id="news_sentiment",
        name="Global News Macro Sentiment",
        execute_fn=simulate_tool("news_sentiment", 600, {"macro_sentiment_score": 0.78, "volatility": "low"}),
        estimated_latency_ms=600
    )

    # 3. Ratio Engine (500ms, depends on sec_parser)
    # Predictor predicts standard ratio format before SEC parse finishes
    n3 = Node(
        node_id="ratio_engine",
        name="Financial Ratio Engine",
        execute_fn=simulate_tool("ratio_engine", 500, {"debt_to_ebitda": 1.41, "current_ratio": 2.1}),
        estimated_latency_ms=500,
        speculative_predictor=lambda state: {"sec_parser": {"revenue_m": 4000, "debt_m": 1200, "ebitda_m": 850}}
    )

    # 4. Insider Trading Scan (450ms, depends on sec_parser)
    n4 = Node(
        node_id="insider_scan",
        name="Form 4 Insider Trading Scanner",
        execute_fn=simulate_tool("insider_scan", 450, {"insider_buys": 4, "insider_sells": 1, "net_signal": "bullish"}),
        estimated_latency_ms=450,
        speculative_predictor=lambda state: {"sec_parser": {"revenue_m": 4200, "debt_m": 1200, "ebitda_m": 850}}
    )

    # 5. Risk Scorer (700ms, depends on ratio_engine, news_sentiment, insider_scan)
    n5 = Node(
        node_id="risk_scorer",
        name="Multi-Factor Risk Scorer",
        execute_fn=simulate_tool("risk_scorer", 700, {"composite_risk_rating": "AA-", "default_probability_bps": 18}),
        estimated_latency_ms=700,
        speculative_predictor=lambda state: {
            "ratio_engine": {"debt_to_ebitda": 1.41, "current_ratio": 2.1},
            "news_sentiment": {"macro_sentiment_score": 0.78, "volatility": "low"},
            "insider_scan": {"insider_buys": 4, "insider_sells": 1, "net_signal": "bullish"}
        }
    )

    # 6. Executive Synthesis (400ms, depends on risk_scorer)
    n6 = Node(
        node_id="synthesis",
        name="Executive Diligence Synthesizer",
        execute_fn=simulate_tool("synthesis", 400, {"final_verdict": "APPROVED", "confidence": 0.94}),
        estimated_latency_ms=400,
        speculative_predictor=lambda state: {
            "risk_scorer": {"composite_risk_rating": "AA-", "default_probability_bps": 18}
        }
    )

    # Add nodes to DAG
    for n in [n1, n2, n3, n4, n5, n6]:
        dag.add_node(n)

    # Add edges
    dag.add_edge("sec_parser", "ratio_engine", "sec_parser", transition_probability=0.85)
    dag.add_edge("sec_parser", "insider_scan", "sec_parser", transition_probability=0.95)
    dag.add_edge("ratio_engine", "risk_scorer", "ratio_engine", transition_probability=0.90)
    dag.add_edge("news_sentiment", "risk_scorer", "news_sentiment", transition_probability=0.92)
    dag.add_edge("insider_scan", "risk_scorer", "insider_scan", transition_probability=0.88)
    dag.add_edge("risk_scorer", "synthesis", "risk_scorer", transition_probability=0.96)

    initial_state = {"target_ticker": "ACME_CORP", "fiscal_year": 2025}
    return dag, initial_state


def create_cybersecurity_sweep_workload() -> Tuple[PDAG, Dict[str, Any]]:
    """
    Workload 2: Autonomous Cybersecurity Threat Sweep Pipeline
    6 Nodes, 3 Concurrency Waves.
    """
    dag = PDAG()

    def simulate_tool(name: str, latency_ms: float, result_val: Any):
        def fn(inputs: Dict[str, Any]) -> Any:
            time.sleep(latency_ms / 1000.0)
            return result_val
        return fn

    n1 = Node("dns_telemetry", "DNS Telemetry Trie Scanner", 
              simulate_tool("dns", 750, {"flagged_c2_queries": 3, "entropy_score": 4.12}), 750)
    n2 = Node("auth_log_scan", "Auth Log Anomaly Scanner", 
              simulate_tool("auth", 550, {"failed_logins": 14, "priv_escalation": False}), 550)
    
    n3 = Node("ip_reputation", "Threat Intel IP Reputation", 
              simulate_tool("ip", 400, {"bad_ips": ["198.51.100.23"], "asn_threat_score": 85}), 400,
              speculative_predictor=lambda state: {"dns_telemetry": {"flagged_c2_queries": 3, "entropy_score": 4.12}})
    
    n4 = Node("lateral_movement", "Lateral Movement Graph Tracker", 
              simulate_tool("lateral", 650, {"hops": 2, "target_host": "dc01.corp"}), 650,
              speculative_predictor=lambda state: {"auth_log_scan": {"failed_logins": 14, "priv_escalation": False}})
    
    n5 = Node("threat_correlator", "Multi-Source Threat Correlator", 
              simulate_tool("correlator", 800, {"threat_actor": "APT-41_HEURISTIC", "severity": "HIGH"}), 800,
              speculative_predictor=lambda state: {
                  "ip_reputation": {"bad_ips": ["198.51.100.23"], "asn_threat_score": 85},
                  "lateral_movement": {"hops": 2, "target_host": "dc01.corp"}
              })
    
    n6 = Node("mitigation_generator", "Automated Firewall ACL Generator", 
              simulate_tool("mitigation", 350, {"rules_pushed": ["DROP 198.51.100.23"], "quarantine_host": "ws-09"}), 350,
              speculative_predictor=lambda state: {
                  "threat_correlator": {"threat_actor": "APT-41_HEURISTIC", "severity": "HIGH"}
              })

    for n in [n1, n2, n3, n4, n5, n6]:
        dag.add_node(n)

    dag.add_edge("dns_telemetry", "ip_reputation", "dns_telemetry", 0.92)
    dag.add_edge("auth_log_scan", "lateral_movement", "auth_log_scan", 0.88)
    dag.add_edge("ip_reputation", "threat_correlator", "ip_reputation", 0.90)
    dag.add_edge("lateral_movement", "threat_correlator", "lateral_movement", 0.94)
    dag.add_edge("threat_correlator", "mitigation_generator", "threat_correlator", 0.98)

    initial_state = {"subnet": "10.0.0.0/16", "timeframe": "last_15m"}
    return dag, initial_state
