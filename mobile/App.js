import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  View, Text, StyleSheet, ScrollView, TouchableOpacity,
  SafeAreaView, RefreshControl, Animated, Dimensions, StatusBar,
  Alert, AppState, TextInput, Modal, Switch,
} from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { DEFAULT_API_HOST, DEFAULT_API_TOKEN, buildUrls } from './config';

const { width } = Dimensions.get('window');
const STORAGE_KEY = 'rubaih.connection.v1';
const THEME_KEY = 'rubaih.theme.v1';

const DARK = {
  bg: '#0f1419',
  card: '#1a222d',
  border: '#2a3544',
  text: '#f0f3f6',
  muted: '#8b98a8',
  faint: '#5a6878',
  accent: '#c9a227',
  accentDim: 'rgba(201,162,39,0.15)',
  good: '#1db954',
  goodDim: 'rgba(29,185,84,0.15)',
  bad: '#e74c3c',
  badDim: 'rgba(231,76,60,0.15)',
  info: '#3b9eff',
  infoDim: 'rgba(59,158,255,0.15)',
  inputBg: '#121820',
  logBg: '#0a0e12',
};

const LIGHT = {
  bg: '#f4f6f8',
  card: '#ffffff',
  border: '#dce3ea',
  text: '#1a2330',
  muted: '#5c6b7a',
  faint: '#8a97a5',
  accent: '#b8860b',
  accentDim: 'rgba(184,134,11,0.12)',
  good: '#0d8a3f',
  goodDim: 'rgba(13,138,63,0.12)',
  bad: '#c0392b',
  badDim: 'rgba(192,57,43,0.12)',
  info: '#1a73e8',
  infoDim: 'rgba(26,115,232,0.12)',
  inputBg: '#ffffff',
  logBg: '#eef2f6',
};

const TABS = [
  { id: 'dashboard', label: 'Home' },
  { id: 'coins', label: 'Coins' },
  { id: 'logs', label: 'Logs' },
  { id: 'trades', label: 'Trades' },
  { id: 'settings', label: 'Setup' },
];

export default function App() {
  const [ready, setReady] = useState(false);
  const [dark, setDark] = useState(true);
  const C = dark ? DARK : LIGHT;

  const [hostInput, setHostInput] = useState(DEFAULT_API_HOST);
  const [tokenInput, setTokenInput] = useState(DEFAULT_API_TOKEN);
  const [conn, setConn] = useState(() => buildUrls(DEFAULT_API_HOST, DEFAULT_API_TOKEN));

  const [dashboard, setDashboard] = useState(null);
  const [history, setHistory] = useState([]);
  const [settings, setSettings] = useState(null);
  const [scanPairs, setScanPairs] = useState([]);
  const [logs, setLogs] = useState([]);
  const [connected, setConnected] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [activeTab, setActiveTab] = useState('settings');
  const [saving, setSaving] = useState(false);
  const [editOpen, setEditOpen] = useState(true);

  const pulseAnim = useRef(new Animated.Value(1)).current;
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);
  const aliveRef = useRef(true);
  const connRef = useRef(conn);

  useEffect(() => { connRef.current = conn; }, [conn]);

  useEffect(() => {
    Animated.loop(
      Animated.sequence([
        Animated.timing(pulseAnim, { toValue: 1.25, duration: 900, useNativeDriver: true }),
        Animated.timing(pulseAnim, { toValue: 1, duration: 900, useNativeDriver: true }),
      ])
    ).start();
  }, [pulseAnim]);

  useEffect(() => {
    (async () => {
      try {
        const theme = await AsyncStorage.getItem(THEME_KEY);
        if (theme === 'light') setDark(false);
        const raw = await AsyncStorage.getItem(STORAGE_KEY);
        if (raw) {
          const saved = JSON.parse(raw);
          const next = buildUrls(saved.host || DEFAULT_API_HOST, saved.token || DEFAULT_API_TOKEN);
          setHostInput(next.apiHost || DEFAULT_API_HOST);
          setTokenInput(next.apiToken || DEFAULT_API_TOKEN);
          setConn(next);
          if (next.configured) {
            setActiveTab('dashboard');
            setEditOpen(false);
          }
        }
      } catch (e) {
        console.error(e);
      } finally {
        setReady(true);
      }
    })();
  }, []);

  const toggleTheme = async () => {
    const next = !dark;
    setDark(next);
    await AsyncStorage.setItem(THEME_KEY, next ? 'dark' : 'light');
  };

  const apiFetch = useCallback(async (path, options = {}) => {
    const c = connRef.current;
    if (!c.configured) throw new Error('Set VPS IP and API token in Setup first');
    const res = await fetch(`${c.apiUrl}${path}`, {
      ...options,
      headers: { ...c.authHeaders, ...(options.headers || {}) },
    });
    if (res.status === 401) throw new Error('Unauthorized — bad API token');
    if (!res.ok) throw new Error((await res.text()) || `HTTP ${res.status}`);
    return res.json();
  }, []);

  const fetchDashboard = useCallback(async () => {
    try { setDashboard(await apiFetch('/dashboard')); } catch (e) { console.error(e); }
  }, [apiFetch]);
  const fetchHistory = useCallback(async () => {
    try { setHistory(await apiFetch('/hedge-history?limit=20')); } catch (e) { console.error(e); }
  }, [apiFetch]);
  const fetchSettings = useCallback(async () => {
    try { setSettings(await apiFetch('/settings')); } catch (e) { console.error(e); }
  }, [apiFetch]);
  const fetchScan = useCallback(async () => {
    try {
      const data = await apiFetch('/scan');
      setScanPairs(data.pairs || []);
    } catch (e) { console.error(e); }
  }, [apiFetch]);
  const fetchLogs = useCallback(async () => {
    try { setLogs(await apiFetch('/logs?limit=100')); } catch (e) { console.error(e); }
  }, [apiFetch]);

  const connectWs = useCallback(() => {
    if (!aliveRef.current) return;
    const c = connRef.current;
    if (!c.configured) return;
    if (wsRef.current) { try { wsRef.current.close(); } catch (_) {} }
    const ws = new WebSocket(c.wsUrl);
    wsRef.current = ws;
    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      if (!aliveRef.current) return;
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = setTimeout(connectWs, 3000);
    };
    ws.onerror = () => { try { ws.close(); } catch (_) {} };
    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.channel === 'rubaih:greeks') {
          const posSize = msg.data.position_size;
          const posSide = msg.data.position_side;
          let delta = msg.data.delta;
          if (posSize != null && Number(posSize) > 0) {
            delta = posSide === 'short' ? -Number(posSize) : Number(posSize);
          }
          setDashboard((prev) => ({
            ...(prev || {}),
            delta,
            spot_price: msg.data.spot,
            session_pnl: msg.data.session_pnl ?? prev?.session_pnl,
            active_pair: msg.data.active_pair || prev?.active_pair,
            position_size: posSize != null ? Number(posSize) : (prev?.position_size ?? 0),
            position_side: posSide || prev?.position_side || 'flat',
            status: prev?.status || 'running',
          }));
        }
        if (msg.channel === 'rubaih:status' && msg.data?.status) {
          setDashboard((prev) => (prev ? { ...prev, status: msg.data.status } : prev));
        }
        if (msg.channel === 'rubaih:scan' && Array.isArray(msg.data?.pairs)) {
          setScanPairs(msg.data.pairs);
        }
        if (msg.channel === 'rubaih:log' && msg.data?.line) {
          setLogs((prev) => [{ ts: msg.data.ts, line: msg.data.line }, ...prev].slice(0, 120));
        }
      } catch (err) {
        console.error(err);
      }
    };
  }, []);

  const refreshAll = useCallback(async () => {
    if (!connRef.current.configured) return;
    await Promise.all([fetchDashboard(), fetchHistory(), fetchSettings(), fetchScan(), fetchLogs()]);
  }, [fetchDashboard, fetchHistory, fetchSettings, fetchScan, fetchLogs]);

  useEffect(() => {
    if (!ready) return;
    aliveRef.current = true;
    clearTimeout(reconnectTimer.current);
    if (wsRef.current) { try { wsRef.current.close(); } catch (_) {} }
    setConnected(false);
    if (!conn.configured) return undefined;
    refreshAll();
    const interval = setInterval(() => {
      fetchDashboard();
      fetchScan();
    }, 5000);
    connectWs();
    const sub = AppState.addEventListener('change', (state) => {
      if (state === 'active') connectWs();
    });
    return () => {
      aliveRef.current = false;
      clearInterval(interval);
      clearTimeout(reconnectTimer.current);
      sub.remove();
      if (wsRef.current) wsRef.current.close();
    };
  }, [ready, conn.apiHost, conn.apiToken, conn.configured, refreshAll, fetchDashboard, fetchScan, connectWs]);

  const saveConnection = async () => {
    setSaving(true);
    try {
      const next = buildUrls(hostInput, tokenInput);
      if (!next.apiHost || next.apiHost.includes('YOUR_VPS_IP')) {
        Alert.alert('VPS IP required', 'Example: 12.34.56.78:8080');
        return;
      }
      if (!next.apiToken || next.apiToken.includes('YOUR_RUBAIH')) {
        Alert.alert('API token required', 'Paste RUBAIH_API_TOKEN from VPS .env');
        return;
      }
      await AsyncStorage.setItem(STORAGE_KEY, JSON.stringify({ host: next.apiHost, token: next.apiToken }));
      setHostInput(next.apiHost);
      setTokenInput(next.apiToken);
      setConn(next);
      setEditOpen(false);
      setActiveTab('dashboard');
      Alert.alert('Saved', `Using ${next.apiHost}`);
    } catch (e) {
      Alert.alert('Save failed', String(e.message || e));
    } finally {
      setSaving(false);
    }
  };

  const testConnection = async () => {
    try {
      const next = buildUrls(hostInput, tokenInput);
      if (!next.apiHost || next.apiHost.includes('YOUR_VPS_IP')) {
        Alert.alert('Set host first', 'Use http://YOUR_VPS_IP:8080 (nginx port 8080)');
        return;
      }
      const url = `${next.apiHost}/api/health`;
      const res = await fetch(url);
      let data = {};
      try {
        data = await res.json();
      } catch (_) {
        data = {};
      }
      if (!res.ok) {
        Alert.alert(
          'Health failed',
          `HTTP ${res.status} from ${url}\nIf containers are down on the VPS: docker compose ps && docker compose logs --tail=50 rubaih_api`
        );
        return;
      }
      Alert.alert(
        data.status === 'ok' ? 'OK' : 'Degraded',
        `${url}\nstatus=${data.status} db=${data.db} redis=${data.redis}`
      );
    } catch (e) {
      Alert.alert(
        'Cannot reach VPS',
        `${String(e.message || e)}\n\nCheck host includes :8080 and on VPS:\ndocker compose up -d\ncurl -s http://127.0.0.1:8010/api/health`
      );
    }
  };

  const triggerKillSwitch = () => {
    Alert.alert('Kill switch', 'Halt bot and flatten?', [
      { text: 'Cancel', style: 'cancel' },
      {
        text: 'HALT',
        style: 'destructive',
        onPress: async () => {
          try {
            const data = await apiFetch('/kill-switch', { method: 'POST' });
            Alert.alert('Sent', data.message || 'Triggered');
            fetchDashboard();
          } catch (e) {
            Alert.alert('Failed', String(e.message || e));
          }
        },
      },
    ]);
  };

  const onRefresh = async () => {
    setRefreshing(true);
    await refreshAll();
    setRefreshing(false);
  };

  const fmtInr = (n) => {
    if (n === undefined || n === null) return '—';
    return '₹' + Number(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
  };
  const fmtUsdt = (n) => {
    if (n === undefined || n === null) return '—';
    return Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const fmtNum = (n, d = 4) => (n == null ? '—' : Number(n).toFixed(d));
  // Engine stores fractions (0.022 = 2.2%). Also tolerate a server sending 2.2.
  const fmtPricePct = (raw, fallbackPct = 2.2) => {
    const n = Number(raw);
    if (!Number.isFinite(n) || n === 0) return `${Number(fallbackPct).toFixed(2)}%`;
    const pct = n <= 1 ? n * 100 : n; // 0.022 → 2.2; 2.2 → 2.2
    return `${pct.toFixed(2)}%`;
  };

  const activePair = dashboard?.active_pair || settings?.active_pair || 'B-BTC_USDT';
  const pairBase = String(activePair).replace(/^B-/i, '').replace(/^I-/i, '').split('_')[0] || 'BTC';
  const posSize = dashboard?.position_size > 0 ? dashboard.position_size : Math.abs(dashboard?.delta || 0);
  const isLong = dashboard?.position_side === 'long' || (dashboard?.delta || 0) > 0.00005;
  const isShort = dashboard?.position_side === 'short' || (dashboard?.delta || 0) < -0.00005;
  const isFlat = !isLong && !isShort;

  const styles = useMemo(() => makeStyles(C), [C]);

  if (!ready) {
    return (
      <SafeAreaView style={[styles.root, { justifyContent: 'center', alignItems: 'center' }]}>
        <Text style={{ color: C.muted }}>Loading…</Text>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle={dark ? 'light-content' : 'dark-content'} backgroundColor={C.bg} />

      <View style={styles.header}>
        <View>
          <Text style={styles.brand}>Rubaih</Text>
          <Text style={styles.sub}>CoinDCX · auto buy / sell</Text>
        </View>
        <View style={styles.headerRight}>
          <TouchableOpacity onPress={toggleTheme} style={styles.themeBtn}>
            <Text style={styles.themeBtnText}>{dark ? 'Light' : 'Dark'}</Text>
          </TouchableOpacity>
          <Animated.View style={{ transform: [{ scale: pulseAnim }] }}>
            <View style={[styles.dot, { backgroundColor: connected ? C.good : C.bad }]} />
          </Animated.View>
          <Text style={{ color: connected ? C.good : C.bad, fontWeight: '700', fontSize: 11 }}>
            {connected ? 'ON' : 'OFF'}
          </Text>
        </View>
      </View>

      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        style={styles.tabScroll}
        contentContainerStyle={styles.tabRow}
      >
        {TABS.map((t) => (
          <TouchableOpacity
            key={t.id}
            style={[styles.tab, activeTab === t.id && styles.tabOn]}
            onPress={() => setActiveTab(t.id)}
          >
            <Text style={[styles.tabText, activeTab === t.id && styles.tabTextOn]}>{t.label}</Text>
          </TouchableOpacity>
        ))}
      </ScrollView>

      <ScrollView
        style={styles.body}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={C.accent} />}
        keyboardShouldPersistTaps="always"
      >
        {activeTab === 'dashboard' && (
          <>
            {!conn.configured && (
              <TouchableOpacity style={styles.warn} onPress={() => { setActiveTab('settings'); setEditOpen(true); }}>
                <Text style={styles.warnTitle}>Connect your VPS</Text>
                <Text style={styles.warnBody}>Open Setup → enter IP:8080 + token</Text>
              </TouchableOpacity>
            )}

            <TouchableOpacity style={styles.kill} onPress={triggerKillSwitch}>
              <Text style={styles.killText}>EMERGENCY STOP</Text>
            </TouchableOpacity>

            <View style={styles.card}>
              <Text style={styles.label}>{pairBase} mark</Text>
              <Text style={styles.price}>{fmtUsdt(dashboard?.spot_price)} USDT</Text>
              <Text style={styles.pairId}>{activePair}</Text>
              <View style={styles.row}>
                <Pill C={C} text={dashboard?.live_trading ? 'LIVE' : 'DRY-RUN'} tone={dashboard?.live_trading ? 'bad' : 'info'} />
                <Pill C={C} text={(dashboard?.status || '—').toUpperCase()} tone="accent" />
              </View>
            </View>

            <View style={styles.grid}>
              <Stat C={C} label="Side" value={isFlat ? 'FLAT' : isLong ? 'LONG' : 'SHORT'} color={isFlat ? C.good : isLong ? C.info : C.bad} />
              <Stat C={C} label="Size" value={`${fmtNum(posSize)}${isFlat ? '' : ` ${pairBase}`}`} color={C.accent} />
              <Stat C={C} label="Session PnL" value={fmtInr(dashboard?.session_pnl)} color={(dashboard?.session_pnl || 0) >= 0 ? C.good : C.bad} />
              <Stat C={C} label="Trades 24h" value={String(dashboard?.num_positions ?? 0)} color={C.text} />
            </View>

            <View style={styles.card}>
              <Text style={styles.cardTitle}>How exits work</Text>
              <Text style={styles.help}>
                {`TP is fixed at coin price +${fmtPricePct(settings?.take_profit_price_pct ?? settings?.take_profit_pct, 2.2)}. CoinDCX displays that as about +22% ROE @10x or +11% ROE @5x.`}
                {"\n"}
                {`SL is fixed at coin price −${fmtPricePct(settings?.stop_loss_price_pct ?? settings?.stop_loss_pct, 1.1)}, giving TP:SL ≈ 2:1.`}
                {"\n"}
                {`Bot locks those prices at buy and sells (exchange TP/SL attach is off — was 422). Size ${Math.round((settings?.margin_use_frac ?? 0.55) * 100)}–${Math.round((settings?.margin_use_max_frac ?? 0.60) * 100)}% of free ≠ TP.`}
              </Text>
            </View>
          </>
        )}

        {activeTab === 'coins' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Scanner list ({scanPairs.length})</Text>
            <Text style={styles.help}>Sorted by short momentum. Active pair highlighted.</Text>
            {scanPairs.length === 0 && <Text style={styles.empty}>Waiting for scan data…</Text>}
            {scanPairs.map((p) => (
              <View key={p.pair} style={[styles.coinRow, p.active && { backgroundColor: C.accentDim }]}>
                <View style={{ flex: 1 }}>
                  <Text style={styles.coinBase}>{p.base || p.pair}</Text>
                  <Text style={styles.coinPair}>{p.pair}</Text>
                </View>
                <Text style={styles.coinMid}>{p.mid ? fmtUsdt(p.mid) : '—'}</Text>
                <Text style={[styles.coinMove, {
                  color: p.move_pct == null ? C.faint : p.move_pct >= 0 ? C.good : C.bad,
                }]}>
                  {p.move_pct == null ? '…' : `${p.move_pct >= 0 ? '+' : ''}${p.move_pct.toFixed(2)}%`}
                </Text>
              </View>
            ))}
          </View>
        )}

        {activeTab === 'logs' && (
          <View style={[styles.card, { backgroundColor: C.logBg }]}>
            <Text style={styles.cardTitle}>Live engine logs</Text>
            {logs.length === 0 && <Text style={styles.empty}>No logs yet — wait for SCAN / CYCLE lines</Text>}
            {logs.map((l, i) => (
              <Text key={`${l.ts}-${i}`} style={styles.logLine} selectable>
                {l.line}
              </Text>
            ))}
          </View>
        )}

        {activeTab === 'trades' && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Recent buys / sells</Text>
            {history.length === 0 && <Text style={styles.empty}>No trades yet</Text>}
            {history.map((h) => {
              const base = (h.symbol || '').replace(/^B-/, '').split('_')[0] || '—';
              const buy = h.side === 'buy';
              return (
                <View key={h.id} style={styles.tradeRow}>
                  <View style={[styles.sideBadge, { backgroundColor: buy ? C.goodDim : C.badDim }]}>
                    <Text style={{ color: buy ? C.good : C.bad, fontWeight: '800', fontSize: 11 }}>
                      {h.side.toUpperCase()}
                    </Text>
                  </View>
                  <Text style={styles.tradeSize}>{fmtNum(h.size)} {base}</Text>
                  <Text style={styles.tradePx}>{fmtUsdt(h.price)}</Text>
                </View>
              );
            })}
          </View>
        )}

        {activeTab === 'settings' && (
          <>
            <View style={styles.card}>
              <Text style={styles.cardTitle}>Connection</Text>
              <Text style={styles.help}>Host example: 12.34.56.78:8080</Text>
              <Row C={C} label="Host" value={(conn.apiHost || '').replace(/^https?:\/\//, '') || 'not set'} />
              <Row C={C} label="Token" value={conn.configured ? 'set' : 'missing'} />
              <View style={styles.themeRow}>
                <Text style={{ color: C.text, fontWeight: '600' }}>Dark mode</Text>
                <Switch value={dark} onValueChange={toggleTheme} trackColor={{ true: C.accent, false: C.border }} />
              </View>
              <TouchableOpacity style={styles.primaryBtn} onPress={() => setEditOpen(true)}>
                <Text style={styles.primaryBtnText}>Edit connection</Text>
              </TouchableOpacity>
              <TouchableOpacity style={styles.secondaryBtn} onPress={testConnection}>
                <Text style={[styles.secondaryBtnText, { color: C.info }]}>Test health</Text>
              </TouchableOpacity>
            </View>
            <View style={styles.card}>
              <Text style={styles.cardTitle}>Bot (from server)</Text>
              <Row C={C} label="Free capital" value={`${settings?.free_capital_inr ?? settings?.capital_inr ?? '—'} INR`} />
              <Row C={C} label="Trade budget" value={`${settings?.trade_budget_inr ?? '—'} INR`} />
              <Row C={C} label="Take profit" value={settings?.tp_display || `Price +${fmtPricePct(settings?.take_profit_price_pct ?? settings?.take_profit_pct, 2.2)}`} />
              <Row C={C} label="Stop loss" value={settings?.sl_display || `Price −${fmtPricePct(settings?.stop_loss_price_pct ?? settings?.stop_loss_pct, 1.1)}`} />
              <Row C={C} label="Size / free" value={`${Math.round((settings?.margin_use_frac ?? 0.55) * 100)}–${Math.round((settings?.margin_use_max_frac ?? 0.60) * 100)}%`} />
              <Row C={C} label="Leverage" value={`${settings?.leverage ?? '—'}x`} />
              <Row C={C} label="Active pair" value={settings?.active_pair || '—'} />
              <Row C={C} label="Live trading" value={dashboard?.live_trading ? 'ON' : 'OFF'} />
            </View>
          </>
        )}
      </ScrollView>

      <Modal visible={editOpen} animationType="slide" onRequestClose={() => { if (conn.configured) setEditOpen(false); }}>
        <SafeAreaView style={[styles.root, { padding: 20 }]}>
          <Text style={[styles.brand, { marginBottom: 12 }]}>Edit connection</Text>
          <Text style={styles.help}>IP:8080 + RUBAIH_API_TOKEN from VPS .env</Text>
          <Text style={styles.inputLabel}>Host</Text>
          <TextInput
            style={styles.input}
            value={hostInput}
            onChangeText={setHostInput}
            autoCapitalize="none"
            autoCorrect={false}
            placeholder="12.34.56.78:8080"
            placeholderTextColor={C.faint}
          />
          <Text style={styles.inputLabel}>API token</Text>
          <TextInput
            style={styles.input}
            value={tokenInput}
            onChangeText={setTokenInput}
            autoCapitalize="none"
            autoCorrect={false}
            secureTextEntry
            placeholder="token"
            placeholderTextColor={C.faint}
          />
          <TouchableOpacity style={styles.primaryBtn} onPress={saveConnection} disabled={saving}>
            <Text style={styles.primaryBtnText}>{saving ? 'Saving…' : 'Save'}</Text>
          </TouchableOpacity>
          {conn.configured && (
            <TouchableOpacity onPress={() => setEditOpen(false)} style={{ marginTop: 16, alignItems: 'center' }}>
              <Text style={{ color: C.muted }}>Cancel</Text>
            </TouchableOpacity>
          )}
        </SafeAreaView>
      </Modal>
    </SafeAreaView>
  );
}

function Pill({ C, text, tone }) {
  const map = { bad: C.bad, good: C.good, info: C.info, accent: C.accent };
  const color = map[tone] || C.muted;
  return (
    <View style={{ borderWidth: 1, borderColor: color, borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4, marginRight: 6, marginTop: 6 }}>
      <Text style={{ color, fontSize: 10, fontWeight: '800' }}>{text}</Text>
    </View>
  );
}

function Stat({ C, label, value, color }) {
  return (
    <View style={{
      width: (width - 48) / 2, backgroundColor: C.card, borderRadius: 14,
      padding: 14, marginBottom: 12, borderWidth: 1, borderColor: C.border,
    }}>
      <Text style={{ color: C.muted, fontSize: 11, marginBottom: 6, textTransform: 'uppercase', letterSpacing: 0.8 }}>{label}</Text>
      <Text style={{ color: color || C.text, fontSize: 20, fontWeight: '700' }}>{value}</Text>
    </View>
  );
}

function Row({ C, label, value }) {
  return (
    <View style={{ flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: C.border }}>
      <Text style={{ color: C.muted, fontSize: 13 }}>{label}</Text>
      <Text style={{ color: C.text, fontSize: 13, fontWeight: '600', maxWidth: '55%', textAlign: 'right' }}>{String(value)}</Text>
    </View>
  );
}

function makeStyles(C) {
  return StyleSheet.create({
    root: { flex: 1, backgroundColor: C.bg },
    header: {
      flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center',
      paddingHorizontal: 16, paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: C.border,
    },
    brand: { fontSize: 22, fontWeight: '800', color: C.accent },
    sub: { fontSize: 11, color: C.muted, marginTop: 2 },
    headerRight: { flexDirection: 'row', alignItems: 'center', gap: 8 },
    themeBtn: { paddingHorizontal: 10, paddingVertical: 6, borderRadius: 8, backgroundColor: C.accentDim, marginRight: 4 },
    themeBtnText: { color: C.accent, fontWeight: '700', fontSize: 11 },
    dot: { width: 8, height: 8, borderRadius: 4, marginRight: 4 },
    tabScroll: { maxHeight: 52, borderBottomWidth: 1, borderBottomColor: C.border },
    tabRow: { paddingHorizontal: 10, paddingVertical: 8, gap: 6 },
    tab: { paddingHorizontal: 14, paddingVertical: 8, borderRadius: 999, backgroundColor: C.card, borderWidth: 1, borderColor: C.border },
    tabOn: { backgroundColor: C.accentDim, borderColor: C.accent },
    tabText: { color: C.muted, fontWeight: '600', fontSize: 12 },
    tabTextOn: { color: C.accent },
    body: { flex: 1, padding: 16 },
    warn: { backgroundColor: C.accentDim, borderColor: C.accent, borderWidth: 1, borderRadius: 12, padding: 14, marginBottom: 12 },
    warnTitle: { color: C.accent, fontWeight: '700', marginBottom: 4 },
    warnBody: { color: C.muted, fontSize: 12 },
    kill: { backgroundColor: C.bad, borderRadius: 12, paddingVertical: 14, alignItems: 'center', marginBottom: 14 },
    killText: { color: '#fff', fontWeight: '800', letterSpacing: 1 },
    card: { backgroundColor: C.card, borderRadius: 16, padding: 16, marginBottom: 14, borderWidth: 1, borderColor: C.border },
    cardTitle: { fontSize: 15, fontWeight: '700', color: C.text, marginBottom: 8 },
    label: { color: C.muted, fontSize: 12, textTransform: 'uppercase', letterSpacing: 1.5, textAlign: 'center' },
    price: { color: C.accent, fontSize: 34, fontWeight: '800', textAlign: 'center', marginVertical: 6 },
    pairId: { color: C.faint, fontSize: 11, textAlign: 'center', marginBottom: 8 },
    row: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'center' },
    grid: { flexDirection: 'row', flexWrap: 'wrap', justifyContent: 'space-between' },
    help: { color: C.muted, fontSize: 13, lineHeight: 20 },
    empty: { color: C.faint, textAlign: 'center', paddingVertical: 20 },
    coinRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 12, borderTopWidth: 1, borderTopColor: C.border, paddingHorizontal: 6, borderRadius: 8 },
    coinBase: { color: C.text, fontWeight: '700', fontSize: 15 },
    coinPair: { color: C.faint, fontSize: 10, marginTop: 2 },
    coinMid: { color: C.muted, fontSize: 12, width: 90, textAlign: 'right', fontVariant: ['tabular-nums'] },
    coinMove: { fontSize: 13, fontWeight: '700', width: 72, textAlign: 'right', fontVariant: ['tabular-nums'] },
    logLine: { color: C.muted, fontSize: 11, fontFamily: 'monospace', marginBottom: 6, lineHeight: 16 },
    tradeRow: { flexDirection: 'row', alignItems: 'center', gap: 10, paddingVertical: 10, borderTopWidth: 1, borderTopColor: C.border },
    sideBadge: { borderRadius: 6, paddingHorizontal: 8, paddingVertical: 4 },
    tradeSize: { color: C.text, fontSize: 13, flex: 1, fontVariant: ['tabular-nums'] },
    tradePx: { color: C.muted, fontSize: 12, fontVariant: ['tabular-nums'] },
    themeRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 12, marginBottom: 8 },
    primaryBtn: { backgroundColor: C.accent, borderRadius: 10, paddingVertical: 14, alignItems: 'center', marginTop: 8 },
    primaryBtnText: { color: '#1a1400', fontWeight: '800' },
    secondaryBtn: { borderWidth: 1, borderColor: C.info, borderRadius: 10, paddingVertical: 12, alignItems: 'center', marginTop: 10 },
    secondaryBtnText: { fontWeight: '700' },
    inputLabel: { color: C.faint, fontSize: 11, marginBottom: 6, marginTop: 8, textTransform: 'uppercase' },
    input: {
      backgroundColor: C.inputBg, borderWidth: 1, borderColor: C.accent, borderRadius: 10,
      paddingHorizontal: 12, paddingVertical: 14, color: C.text, marginBottom: 8,
    },
  });
}
