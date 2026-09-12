import { PDAG } from '../engine/PDAG.js';

export function createFinancialWorkload() {
  const dag = new PDAG();

  const simulateTool = (latencyMs, resultVal) => {
    return async () => {
      await new Promise(r => setTimeout(r, latencyMs));
      return resultVal;
    };
  };

  dag.addNode('sec_parser', 'SEC 10-K Parser', simulateTool(700, { revenue_m: 4200, debt_m: 1200, ebitda_m: 850 }), 700);
  dag.addNode('news_sentiment', 'News Macro Sentiment', simulateTool(500, { macro_score: 0.78, volatility: 'low' }), 500);
  
  dag.addNode('ratio_engine', 'Financial Ratio Engine', simulateTool(400, { debt_to_ebitda: 1.41, current_ratio: 2.1 }), 400,
    (state) => ({ sec_parser: { revenue_m: 4200, debt_m: 1200, ebitda_m: 850 } }));
  
  dag.addNode('insider_scan', 'Insider Trading Scanner', simulateTool(350, { insider_buys: 4, net_signal: 'bullish' }), 350,
    (state) => ({ sec_parser: { revenue_m: 4200, debt_m: 1200, ebitda_m: 850 } }));
  
  dag.addNode('risk_scorer', 'Multi-Factor Risk Scorer', simulateTool(600, { composite_risk: 'AA-', default_bps: 18 }), 600,
    (state) => ({
      ratio_engine: { debt_to_ebitda: 1.41, current_ratio: 2.1 },
      news_sentiment: { macro_score: 0.78, volatility: 'low' },
      insider_scan: { insider_buys: 4, net_signal: 'bullish' }
    }));
  
  dag.addNode('synthesis', 'Executive Diligence Synthesizer', simulateTool(300, { verdict: 'APPROVED', confidence: 0.95 }), 300,
    (state) => ({ risk_scorer: { composite_risk: 'AA-', default_bps: 18 } }));

  dag.addEdge('sec_parser', 'ratio_engine', 'sec_parser', 0.90);
  dag.addEdge('sec_parser', 'insider_scan', 'sec_parser', 0.95);
  dag.addEdge('ratio_engine', 'risk_scorer', 'ratio_engine', 0.92);
  dag.addEdge('news_sentiment', 'risk_scorer', 'news_sentiment', 0.88);
  dag.addEdge('insider_scan', 'risk_scorer', 'insider_scan', 0.91);
  dag.addEdge('risk_scorer', 'synthesis', 'risk_scorer', 0.98);

  const initialState = { target_ticker: 'NVDA_SYNTH', fiscal_year: 2025 };
  return { dag, initialState, title: 'Financial Due Diligence Intelligence' };
}

export function createCybersecurityWorkload() {
  const dag = new PDAG();

  const simulateTool = (latencyMs, resultVal) => {
    return async () => {
      await new Promise(r => setTimeout(r, latencyMs));
      return resultVal;
    };
  };

  dag.addNode('dns_telemetry', 'DNS Trie Telemetry Scanner', simulateTool(650, { c2_queries: 3, entropy: 4.12 }), 650);
  dag.addNode('auth_scan', 'Auth Log Anomaly Scanner', simulateTool(450, { failed_logins: 14, priv_escalation: false }), 450);
  
  dag.addNode('ip_intel', 'Threat Intel IP Reputation', simulateTool(350, { bad_ips: ['198.51.100.23'], threat_score: 85 }), 350,
    (state) => ({ dns_telemetry: { c2_queries: 3, entropy: 4.12 } }));
  
  dag.addNode('lateral_detector', 'Lateral Movement Tracker', simulateTool(550, { hops: 2, target: 'dc01.corp' }), 550,
    (state) => ({ auth_scan: { failed_logins: 14, priv_escalation: false } }));
  
  dag.addNode('correlator', 'Multi-Source Threat Correlator', simulateTool(650, { actor: 'APT-41_HEURISTIC', severity: 'HIGH' }), 650,
    (state) => ({
      ip_intel: { bad_ips: ['198.51.100.23'], threat_score: 85 },
      lateral_detector: { hops: 2, target: 'dc01.corp' }
    }));
  
  dag.addNode('mitigation', 'Firewall ACL Auto-Generator', simulateTool(300, { action: 'DROP 198.51.100.23', quarantine: 'ws-09' }), 300,
    (state) => ({ correlator: { actor: 'APT-41_HEURISTIC', severity: 'HIGH' } }));

  dag.addEdge('dns_telemetry', 'ip_intel', 'dns_telemetry', 0.95);
  dag.addEdge('auth_scan', 'lateral_detector', 'auth_scan', 0.90);
  dag.addEdge('ip_intel', 'correlator', 'ip_intel', 0.92);
  dag.addEdge('lateral_detector', 'correlator', 'lateral_detector', 0.94);
  dag.addEdge('correlator', 'mitigation', 'correlator', 0.98);

  const initialState = { subnet: '10.0.0.0/16', window: 'last_15m' };
  return { dag, initialState, title: 'Autonomous Cybersecurity Threat Sweep' };
}
