import React, { useState, useEffect, useRef } from 'react';
import {
  View, Text, StyleSheet, ScrollView, TouchableOpacity,
  SafeAreaView, RefreshControl, Animated, Dimensions, StatusBar
} from 'react-native';

const { width } = Dimensions.get('window');
const API_URL = 'http://YOUR_VPS_IP:8000/api';
const WS_URL = 'ws://YOUR_VPS_IP:8000/ws';

// Classic trading color palette
const COLORS = {
  bg: '#0d1117',
  card: '#161b22',
  cardBorder: '#30363d',
  gold: '#f0b90b',
  goldDim: 'rgba(240, 185, 11, 0.15)',
  cyan: '#00d4aa',
  cyanDim: 'rgba(0, 212, 170, 0.15)',
  purple: '#a855f7',
  purpleDim: 'rgba(168, 85, 247, 0.15)',
  red: '#ef4444',
  redDim: 'rgba(239, 68, 68, 0.15)',
  green: '#22c55e',
  greenDim: 'rgba(34, 197, 94, 0.15)',
  orange: '#f97316',
  text: '#e6edf3',
  textSecondary: '#8b949e',
  textMuted: '#484f58',
};

export default function App() {
  const [dashboard, setDashboard] = useState(null);
  const [history, setHistory] = useState([]);
  const [aiDecisions, setAiDecisions] = useState([]);
  const [connected, setConnected] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [activeTab, setActiveTab] = useState('dashboard');
  const pulseAnim = useRef(new Animated.Value(1)).current;

  // Pulse animation for live indicator
  useEffect(() => {
    Animated.loop(
      Animated.sequence([
        Animated.timing(pulseAnim, { toValue: 1.3, duration: 800, useNativeDriver: true }),
        Animated.timing(pulseAnim, { toValue: 1, duration: 800, useNativeDriver: true }),
      ])
    ).start();
  }, []);

  const fetchDashboard = async () => {
    try {
      const res = await fetch(`${API_URL}/dashboard`);
      const data = await res.json();
      setDashboard(data);
    } catch (e) { console.error(e); }
  };

  const fetchHistory = async () => {
    try {
      const res = await fetch(`${API_URL}/hedge-history?limit=10`);
      const data = await res.json();
      setHistory(data);
    } catch (e) { console.error(e); }
  };

  const fetchAiDecisions = async () => {
    try {
      const res = await fetch(`${API_URL}/ai-decisions?limit=10`);
      const data = await res.json();
      setAiDecisions(data);
    } catch (e) { console.error(e); }
  };

  const triggerKillSwitch = async () => {
    try {
      const res = await fetch(`${API_URL}/kill-switch`, { method: 'POST' });
      const data = await res.json();
      alert(data.message);
    } catch (e) { alert('Failed to trigger kill switch'); }
  };

  useEffect(() => {
    fetchDashboard(); fetchHistory(); fetchAiDecisions();
    const interval = setInterval(fetchDashboard, 3000);

    const ws = new WebSocket(WS_URL);
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.channel === 'rubaih:greeks') {
        setDashboard(prev => prev ? {
          ...prev,
          delta: msg.data.delta,
          gamma: msg.data.gamma,
          vega: msg.data.vega,
          theta: msg.data.theta,
          spot_price: msg.data.spot,
          timestamp: new Date(msg.data.timestamp * 1000).toISOString()
        } : null);
      }
    };

    return () => { clearInterval(interval); ws.close(); };
  }, []);

  const onRefresh = async () => {
    setRefreshing(true);
    await Promise.all([fetchDashboard(), fetchHistory(), fetchAiDecisions()]);
    setRefreshing(false);
  };

  const formatNumber = (n, digits = 4) => {
    if (n === undefined || n === null) return '—';
    return typeof n === 'number' ? n.toFixed(digits) : n;
  };

  const formatCurrency = (n) => {
    if (n === undefined || n === null) return '—';
    return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  return (
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="light-content" backgroundColor={COLORS.bg} />

      {/* Header */}
      <View style={styles.header}>
        <View style={styles.headerLeft}>
          <Text style={styles.headerTitle}>🤖 Rubaih</Text>
          <Text style={styles.headerSubtitle}>Delta Exchange India · Delta-Hedge Bot</Text>
        </View>
        <View style={styles.headerRight}>
          <Animated.View style={[styles.pulseDot, { transform: [{ scale: pulseAnim }] }]}>
            <View style={[styles.statusDot, { backgroundColor: connected ? COLORS.green : COLORS.red }]} />
          </Animated.View>
          <Text style={[styles.statusText, { color: connected ? COLORS.green : COLORS.red }]}>
            {connected ? 'LIVE' : 'OFFLINE'}
          </Text>
        </View>
      </View>

      {/* Tab Bar */}
      <View style={styles.tabBar}>
        {['dashboard', 'hedges', 'ai', 'settings'].map(tab => (
          <TouchableOpacity
            key={tab}
            style={[styles.tab, activeTab === tab && styles.tabActive]}
            onPress={() => setActiveTab(tab)}
          >
            <Text style={[styles.tabText, activeTab === tab && styles.tabTextActive]}>
              {tab === 'dashboard' ? '📊 Dashboard' : tab === 'hedges' ? '⚡ Hedges' : tab === 'ai' ? '🧠 AI' : '⚙️ Settings'}
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      <ScrollView
        style={styles.scroll}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={COLORS.gold} />}
      >
        {activeTab === 'dashboard' && (
          <>
            {/* Kill Switch */}
            <TouchableOpacity style={styles.killButton} onPress={triggerKillSwitch} activeOpacity={0.7}>
              <View style={styles.killButtonInner}>
                <Text style={styles.killButtonIcon}>☠️</Text>
                <Text style={styles.killButtonText}>EMERGENCY KILL SWITCH</Text>
                <Text style={styles.killButtonSub}>Instantly halt all trading</Text>
              </View>
            </TouchableOpacity>

            {/* Spot Price Hero */}
            {dashboard && (
              <View style={styles.heroCard}>
                <Text style={styles.heroLabel}>BTC Spot Price</Text>
                <Text style={styles.heroPrice}>{formatCurrency(dashboard.spot_price)}</Text>
                <View style={styles.heroRow}>
                  <View style={[styles.badge, { backgroundColor: COLORS.goldDim, borderColor: COLORS.gold }]}>
                    <Text style={[styles.badgeText, { color: COLORS.gold }]}>
                      {dashboard.ai_enabled ? '🧠 AI ON' : '🧠 AI OFF'}
                    </Text>
                  </View>
                  <View style={[styles.badge, { backgroundColor: COLORS.cyanDim, borderColor: COLORS.cyan }]}>
                    <Text style={[styles.badgeText, { color: COLORS.cyan }]}>
                      {dashboard.status?.toUpperCase()}
                    </Text>
                  </View>
                </View>
              </View>
            )}

            {/* Greeks Grid */}
            {dashboard && (
              <View style={styles.greeksGrid}>
                <GreekCard
                  label="Delta"
                  value={formatNumber(dashboard.delta, 4)}
                  unit=" BTC"
                  color={dashboard.delta > 0.1 ? COLORS.red : dashboard.delta < -0.1 ? COLORS.red : COLORS.green}
                  icon="Δ"
                />
                <GreekCard
                  label="Gamma"
                  value={formatNumber(dashboard.gamma, 6)}
                  color={COLORS.purple}
                  icon="Γ"
                />
                <GreekCard
                  label="Vega"
                  value={formatNumber(dashboard.vega, 2)}
                  unit=" $/vol"
                  color={COLORS.cyan}
                  icon="V"
                />
                <GreekCard
                  label="Theta"
                  value={formatNumber(dashboard.theta, 2)}
                  unit=" $/day"
                  color={COLORS.orange}
                  icon="Θ"
                />
              </View>
            )}

            {/* AI Decision Card */}
            {dashboard?.ai_last_action && (
              <View style={[styles.aiCard, { borderColor: COLORS.purple }]}>
                <View style={styles.aiCardHeader}>
                  <Text style={styles.aiCardIcon}>🧠</Text>
                  <Text style={styles.aiCardTitle}>Latest AI Decision</Text>
                </View>
                <View style={styles.aiCardRow}>
                  <Text style={[styles.aiAction, { color: getActionColor(dashboard.ai_last_action) }]}>
                    {dashboard.ai_last_action}
                  </Text>
                  <View style={[styles.confidenceBadge, { backgroundColor: COLORS.purpleDim }]}>
                    <Text style={[styles.confidenceText, { color: COLORS.purple }]}>
                      {(dashboard.ai_confidence * 100).toFixed(0)}% confidence
                    </Text>
                  </View>
                </View>
              </View>
            )}
          </>
        )}

        {activeTab === 'hedges' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>⚡ Recent Hedge Trades</Text>
            {history.length === 0 && <Text style={styles.empty}>No hedges yet</Text>}
            {history.map((h, i) => (
              <View key={h.id} style={[styles.tradeRow, i === 0 && styles.tradeRowFirst]}>
                <View style={[styles.tradeSideBadge, { backgroundColor: h.side === 'buy' ? COLORS.greenDim : COLORS.redDim }]}>
                  <Text style={[styles.tradeSideText, { color: h.side === 'buy' ? COLORS.green : COLORS.red }]}>
                    {h.side.toUpperCase()}
                  </Text>
                </View>
                <Text style={styles.tradeSize}>{h.size.toFixed(4)} BTC</Text>
                <Text style={styles.tradePrice}>{formatCurrency(h.price)}</Text>
                {h.ai_augmented && <Text style={styles.aiBadge}>🧠</Text>}
              </View>
            ))}
          </View>
        )}

        {activeTab === 'ai' && (
          <>
            <View style={[styles.card, { borderColor: COLORS.purple }]}>
              <Text style={[styles.cardTitle, { color: COLORS.purple }]}>🧠 AI Decision History</Text>
              {aiDecisions.length === 0 && <Text style={styles.empty}>No AI decisions yet</Text>}
              {aiDecisions.map((d, i) => (
                <View key={d.id} style={[styles.aiDecisionRow, i === 0 && styles.tradeRowFirst]}>
                  <View style={styles.aiDecisionLeft}>
                    <Text style={[styles.aiDecisionAction, { color: getActionColor(d.action) }]}>
                      {d.action}
                    </Text>
                    <Text style={styles.aiDecisionModel}>{d.model.split('/').pop()}</Text>
                  </View>
                  <View style={styles.aiDecisionRight}>
                    <View style={[styles.confidenceBadge, { backgroundColor: COLORS.purpleDim }]}>
                      <Text style={[styles.confidenceText, { color: COLORS.purple }]}>
                        {(d.confidence * 100).toFixed(0)}%
                      </Text>
                    </View>
                    <Text style={styles.aiDecisionRisk}>{d.risk_assessment}</Text>
                  </View>
                </View>
              ))}
            </View>
          </>
        )}

        {activeTab === 'settings' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>⚙️ Bot Settings</Text>
            <SettingsRow label="Delta Threshold" value="0.05 BTC" />
            <SettingsRow label="Max Delta" value="0.50 BTC" />
            <SettingsRow label="Max Vega" value="5,000 $/vol" />
            <SettingsRow label="Max Drawdown" value="5.0%" />
            <SettingsRow label="Min Hedge Interval" value="30 sec" />
            <SettingsRow label="Taker Fee" value="0.059%" />
            <SettingsRow label="Exchange" value="Delta Exchange India" />
            <SettingsRow label="Mode" value={USE_TESTNET ? 'TESTNET' : 'LIVE'} />
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

function GreekCard({ label, value, unit = '', color, icon }) {
  return (
    <View style={[styles.greekCard, { borderColor: color }]}>
      <View style={[styles.greekIconBox, { backgroundColor: color + '22' }]}>
        <Text style={[styles.greekIcon, { color }]}>{icon}</Text>
      </View>
      <Text style={styles.greekLabel}>{label}</Text>
      <Text style={[styles.greekValue, { color }]}>{value}{unit}</Text>
    </View>
  );
}

function SettingsRow({ label, value }) {
  return (
    <View style={styles.settingsRow}>
      <Text style={styles.settingsLabel}>{label}</Text>
      <Text style={styles.settingsValue}>{value}</Text>
    </View>
  );
}

function getActionColor(action) {
  if (action === 'HEDGE') return COLORS.cyan;
  if (action === 'HOLD') return COLORS.green;
  if (action === 'EMERGENCY') return COLORS.red;
  if (action === 'ADJUST_THRESHOLD') return COLORS.orange;
  return COLORS.text;
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bg },

  header: {
    flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
    paddingHorizontal: 20, paddingTop: 16, paddingBottom: 12,
    borderBottomWidth: 1, borderBottomColor: COLORS.cardBorder,
  },
  headerLeft: { flex: 1 },
  headerTitle: { fontSize: 24, fontWeight: 'bold', color: COLORS.gold, letterSpacing: 0.5 },
  headerSubtitle: { fontSize: 11, color: COLORS.textMuted, marginTop: 2 },
  headerRight: { flexDirection: 'row', alignItems: 'center' },
  pulseDot: { marginRight: 6 },
  statusDot: { width: 8, height: 8, borderRadius: 4 },
  statusText: { fontSize: 11, fontWeight: '600' },

  tabBar: {
    flexDirection: 'row', paddingHorizontal: 12, paddingVertical: 10,
    borderBottomWidth: 1, borderBottomColor: COLORS.cardBorder,
  },
  tab: { flex: 1, paddingVertical: 8, alignItems: 'center', borderRadius: 8 },
  tabActive: { backgroundColor: COLORS.goldDim },
  tabText: { fontSize: 11, color: COLORS.textSecondary, fontWeight: '500' },
  tabTextActive: { color: COLORS.gold, fontWeight: '600' },

  scroll: { flex: 1, paddingHorizontal: 16, paddingTop: 16 },

  killButton: {
    backgroundColor: COLORS.red,
    borderRadius: 16,
    marginBottom: 16,
    shadowColor: COLORS.red,
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.3,
    shadowRadius: 12,
    elevation: 8,
  },
  killButtonInner: { padding: 18, alignItems: 'center' },
  killButtonIcon: { fontSize: 28, marginBottom: 4 },
  killButtonText: { color: '#fff', fontSize: 16, fontWeight: 'bold', letterSpacing: 1 },
  killButtonSub: { color: 'rgba(255,255,255,0.7)', fontSize: 11, marginTop: 2 },

  heroCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.gold + '33',
    alignItems: 'center',
  },
  heroLabel: { fontSize: 12, color: COLORS.textSecondary, textTransform: 'uppercase', letterSpacing: 2 },
  heroPrice: { fontSize: 36, fontWeight: 'bold', color: COLORS.gold, marginVertical: 8, fontVariant: ['tabular-nums'] },
  heroRow: { flexDirection: 'row', gap: 8, marginTop: 4 },
  badge: { paddingHorizontal: 12, paddingVertical: 4, borderRadius: 20, borderWidth: 1 },
  badgeText: { fontSize: 10, fontWeight: '600' },

  greeksGrid: {
    flexDirection: 'row', flexWrap: 'wrap',
    justifyContent: 'space-between', marginBottom: 16,
  },
  greekCard: {
    width: (width - 48) / 2,
    backgroundColor: COLORS.card,
    borderRadius: 14,
    padding: 16,
    marginBottom: 12,
    borderWidth: 1,
    borderLeftWidth: 3,
  },
  greekIconBox: { width: 32, height: 32, borderRadius: 8, justifyContent: 'center', alignItems: 'center', marginBottom: 8 },
  greekIcon: { fontSize: 16, fontWeight: 'bold' },
  greekLabel: { fontSize: 11, color: COLORS.textSecondary, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 },
  greekValue: { fontSize: 18, fontWeight: 'bold', fontVariant: ['tabular-nums'] },

  aiCard: {
    backgroundColor: COLORS.card,
    borderRadius: 14,
    padding: 16,
    marginBottom: 16,
    borderWidth: 1,
    borderLeftWidth: 3,
  },
  aiCardHeader: { flexDirection: 'row', alignItems: 'center', marginBottom: 10 },
  aiCardIcon: { fontSize: 18, marginRight: 8 },
  aiCardTitle: { fontSize: 14, fontWeight: '600', color: COLORS.purple },
  aiCardRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  aiAction: { fontSize: 20, fontWeight: 'bold' },
  confidenceBadge: { paddingHorizontal: 10, paddingVertical: 4, borderRadius: 12 },
  confidenceText: { fontSize: 11, fontWeight: '600' },

  card: {
    backgroundColor: COLORS.card,
    borderRadius: 14,
    padding: 16,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.cardBorder,
  },
  cardTitle: { fontSize: 14, fontWeight: '600', color: COLORS.text, marginBottom: 12 },
  empty: { color: COLORS.textMuted, textAlign: 'center', padding: 20 },

  tradeRow: {
    flexDirection: 'row', alignItems: 'center',
    paddingVertical: 10, borderTopWidth: 1, borderTopColor: COLORS.cardBorder,
  },
  tradeRowFirst: { borderTopWidth: 0 },
  tradeSideBadge: { paddingHorizontal: 10, paddingVertical: 3, borderRadius: 6, marginRight: 10 },
  tradeSideText: { fontSize: 10, fontWeight: 'bold' },
  tradeSize: { color: COLORS.text, fontSize: 13, width: 90, fontVariant: ['tabular-nums'] },
  tradePrice: { color: COLORS.textSecondary, fontSize: 12, flex: 1, fontVariant: ['tabular-nums'] },
  aiBadge: { fontSize: 14 },

  aiDecisionRow: {
    flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
    paddingVertical: 12, borderTopWidth: 1, borderTopColor: COLORS.cardBorder,
  },
  aiDecisionLeft: { flex: 1 },
  aiDecisionAction: { fontSize: 16, fontWeight: 'bold', marginBottom: 2 },
  aiDecisionModel: { fontSize: 10, color: COLORS.textMuted },
  aiDecisionRight: { alignItems: 'flex-end' },
  aiDecisionRisk: { fontSize: 10, color: COLORS.textSecondary, marginTop: 4 },

  settingsRow: {
    flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
    paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: COLORS.cardBorder,
  },
  settingsLabel: { fontSize: 13, color: COLORS.textSecondary },
  settingsValue: { fontSize: 13, color: COLORS.gold, fontWeight: '500', fontVariant: ['tabular-nums'] },
});
