/**
 * Web channel — browser-based chat UI with WebSocket transport.
 *
 * Serves a sidebar-enabled chat UI with per-thread conversations.
 * Thread identity comes from the `?t=` URL parameter — same URL on any
 * browser opens the same conversation. Threads are listed in the sidebar
 * and queried from the central DB via GET /api/threads.
 *
 * Configure in .env:
 *   WEB_CHANNEL_PORT=3080        (default 3080)
 *   WEB_CHANNEL_TOKEN=           (optional, enables Bearer auth)
 */
import crypto from 'crypto';
import http from 'http';
import path from 'path';
import type { Duplex } from 'stream';
import Database from 'better-sqlite3';

import { log } from '../log.js';
import type { ChannelAdapter, ChannelSetup, OutboundMessage } from './adapter.js';
import { registerChannelAdapter } from './channel-registry.js';

const PLATFORM_ID = 'web';
const DEFAULT_PORT = 3080;
const DEFAULT_THREAD = 'default';
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

interface ClientInfo {
  socket: Duplex;
  threadId: string;
}
interface ThreadRow {
  thread_id: string;
  created_at: string;
}
interface MessageEntry {
  role: 'user' | 'bot';
  text: string;
  timestamp: string;
}

// ── WebSocket frame helpers ────────────────────────────────────────

function acceptKey(key: string): string {
  return crypto
    .createHash('sha1')
    .update(key + WS_MAGIC)
    .digest('base64');
}

function encodeFrame(payload: string): Buffer {
  const buf = Buffer.from(payload, 'utf-8');
  const len = buf.length;
  const head: number[] = [0x81]; // FIN + text opcode
  if (len < 126) {
    head.push(len);
  } else if (len < 65536) {
    head.push(126, (len >> 8) & 0xff, len & 0xff);
  } else {
    head.push(127);
    for (let i = 7; i >= 0; i--) {
      head.push(Number((BigInt(len) >> BigInt(i * 8)) & 0xffn));
    }
  }
  return Buffer.concat([Buffer.from(head), buf]);
}

// ── Session lookup (for status polling) ───────────────────────────

interface SessionRef {
  agentGroupId: string;
  sessionId: string;
}

function findWebSession(threadId: string): SessionRef | null {
  try {
    const db = new Database(path.join(process.cwd(), 'data', 'v2.db'), {
      readonly: true,
      fileMustExist: true,
    });
    try {
      const mg = db
        .prepare("SELECT id FROM messaging_groups WHERE channel_type='web' AND platform_id='web' LIMIT 1")
        .get() as { id: string } | undefined;
      if (!mg) return null;
      const sess = db
        .prepare('SELECT id, agent_group_id FROM sessions WHERE messaging_group_id=? AND thread_id=? LIMIT 1')
        .get(mg.id, threadId) as { id: string; agent_group_id: string } | undefined;
      if (!sess) return null;
      return { sessionId: sess.id, agentGroupId: sess.agent_group_id };
    } finally {
      db.close();
    }
  } catch {
    return null;
  }
}

// ── Thread listing from central DB ────────────────────────────────

function getThreads(): ThreadRow[] {
  try {
    const db = new Database(path.join(process.cwd(), 'data', 'v2.db'), {
      readonly: true,
      fileMustExist: true,
    });
    try {
      const mg = db
        .prepare("SELECT id FROM messaging_groups WHERE channel_type='web' AND platform_id='web' LIMIT 1")
        .get() as { id: string } | undefined;
      if (!mg) return [];
      return db
        .prepare(
          'SELECT thread_id, created_at FROM sessions' +
            ' WHERE messaging_group_id=? AND thread_id IS NOT NULL' +
            ' ORDER BY coalesce(last_active, created_at) DESC',
        )
        .all(mg.id) as ThreadRow[];
    } finally {
      db.close();
    }
  } catch {
    return [];
  }
}

// ── Message history from session DBs ──────────────────────────────

function getMessages(threadId: string, limit = 60): MessageEntry[] {
  try {
    const central = new Database(path.join(process.cwd(), 'data', 'v2.db'), {
      readonly: true,
      fileMustExist: true,
    });
    let sessionId: string;
    let agentGroupId: string;
    try {
      const mg = central
        .prepare("SELECT id FROM messaging_groups WHERE channel_type='web' AND platform_id='web' LIMIT 1")
        .get() as { id: string } | undefined;
      if (!mg) return [];
      const sess = central
        .prepare('SELECT id, agent_group_id FROM sessions WHERE messaging_group_id=? AND thread_id=? LIMIT 1')
        .get(mg.id, threadId) as { id: string; agent_group_id: string } | undefined;
      if (!sess) return [];
      sessionId = sess.id;
      agentGroupId = sess.agent_group_id;
    } finally {
      central.close();
    }

    const sessDir = path.join(process.cwd(), 'data', 'v2-sessions', agentGroupId, sessionId);
    const all: MessageEntry[] = [];

    try {
      const inDb = new Database(path.join(sessDir, 'inbound.db'), { readonly: true, fileMustExist: true });
      try {
        const rows = inDb
          .prepare(
            "SELECT content, timestamp FROM messages_in WHERE kind='chat' AND status='completed' ORDER BY rowid DESC LIMIT ?",
          )
          .all(limit) as { content: string; timestamp: string }[];
        for (const r of rows) {
          try {
            const c = JSON.parse(r.content) as Record<string, unknown>;
            if (typeof c.text === 'string') all.push({ role: 'user', text: c.text, timestamp: r.timestamp });
          } catch {
            /* skip */
          }
        }
      } finally {
        inDb.close();
      }
    } catch {
      /* no inbound DB yet */
    }

    try {
      const outDb = new Database(path.join(sessDir, 'outbound.db'), { readonly: true, fileMustExist: true });
      try {
        const rows = outDb
          .prepare("SELECT content, timestamp FROM messages_out WHERE kind='chat' ORDER BY rowid DESC LIMIT ?")
          .all(limit) as { content: string; timestamp: string }[];
        for (const r of rows) {
          try {
            const c = JSON.parse(r.content) as Record<string, unknown>;
            if (typeof c.text === 'string' && !c.operation)
              all.push({ role: 'bot', text: c.text, timestamp: r.timestamp });
          } catch {
            /* skip */
          }
        }
      } finally {
        outDb.close();
      }
    } catch {
      /* no outbound DB yet */
    }

    const toMs = (ts: string) => new Date(ts.includes('T') ? ts : ts.replace(' ', 'T') + 'Z').getTime();
    all.sort((a, b) => toMs(a.timestamp) - toMs(b.timestamp));
    return all.slice(-limit);
  } catch {
    return [];
  }
}

// ── Mnemon graph from SQLite ───────────────────────────────────────

const NODE_COLORS: Record<string, string> = {
  decision: '#e74c3c',
  fact: '#3498db',
  insight: '#9b59b6',
  preference: '#2ecc71',
  context: '#f39c12',
  general: '#95a5a6',
};
const EDGE_COLORS: Record<string, string> = {
  temporal: '#888888',
  semantic: '#3498db',
  causal: '#e74c3c',
  entity: '#2ecc71',
};

function wrapLabel(category: string, content: string): string {
  const cols = 30;
  const words = content.split(/\s+/);
  const lines: string[] = [`[${category}]`];
  let cur = '';
  for (const w of words) {
    if (lines.length >= 4) break; // header + 3 content lines
    const candidate = cur ? cur + ' ' + w : w;
    if (candidate.length <= cols) {
      cur = candidate;
    } else {
      if (cur) {
        lines.push(cur);
        cur = '';
      }
      cur = w.slice(0, cols);
    }
  }
  if (cur && lines.length < 4) lines.push(cur);
  // Append ellipsis if text was truncated
  const covered = lines.slice(1).join(' ').length;
  if (covered < content.length) {
    const last = lines[lines.length - 1];
    lines[lines.length - 1] = last.length >= cols - 1 ? last.slice(0, cols - 1) + '…' : last + '…';
  }
  return lines.join('\n');
}

function getMnemonGraph(): string | null {
  try {
    const central = new Database(path.join(process.cwd(), 'data', 'v2.db'), { readonly: true, fileMustExist: true });
    let agentGroupId: string | undefined;
    try {
      const mg = central
        .prepare("SELECT id FROM messaging_groups WHERE channel_type='web' AND platform_id='web' LIMIT 1")
        .get() as { id: string } | undefined;
      if (!mg) return null;
      const w = central
        .prepare('SELECT agent_group_id FROM messaging_group_agents WHERE messaging_group_id=? LIMIT 1')
        .get(mg.id) as { agent_group_id: string } | undefined;
      agentGroupId = w?.agent_group_id;
    } finally {
      central.close();
    }
    if (!agentGroupId) return null;

    const mnemonPath = path.join(
      process.cwd(),
      'data',
      'v2-sessions',
      agentGroupId,
      '.claude-shared',
      'mnemon',
      'data',
      'default',
      'mnemon.db',
    );
    const mdb = new Database(mnemonPath, { readonly: true, fileMustExist: true });
    let insights: { id: string; content: string; category: string }[];
    let edges: { source_id: string; target_id: string; edge_type: string }[];
    try {
      insights = mdb
        .prepare('SELECT id, content, category FROM insights WHERE deleted_at IS NULL ORDER BY created_at')
        .all() as typeof insights;
      edges = mdb.prepare('SELECT source_id, target_id, edge_type FROM edges').all() as typeof edges;
    } finally {
      mdb.close();
    }

    const j = (s: string) => s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
    const col = (cat: string) => NODE_COLORS[cat] ?? NODE_COLORS.general;
    const ecol = (t: string) => EDGE_COLORS[t] ?? '#888';

    const nodeJs = insights
      .map((n) => {
        const bg = col(n.category);
        const label = wrapLabel(n.category, n.content);
        return `{id:"${n.id}",label:"${j(label)}",title:"",category:"${n.category}",fullText:${JSON.stringify(n.content)},color:{background:"${bg}",border:"${bg}",highlight:{background:"${bg}",border:"#fff"}},font:{color:"#fff",size:11},widthConstraint:{minimum:180,maximum:220},margin:10}`;
      })
      .join(',\n');

    const edgeJs = edges
      .map((e) => {
        const ec = ecol(e.edge_type);
        return `{id:"${j(e.source_id)}|${j(e.target_id)}|${e.edge_type}",from:"${e.source_id}",to:"${e.target_id}",edgeType:"${e.edge_type}",color:{color:"${ec}",highlight:"#fff",hover:"#fff"},arrows:"to",font:{color:"${ec}",size:9,align:"middle"},label:"${e.edge_type}"}`;
      })
      .join(',\n');

    const nodeColorLegend = Object.entries(NODE_COLORS)
      .map(
        ([k, v]) =>
          `<label class="leg-item"><input type="checkbox" checked data-cat="${k}"><span class="dot" style="background:${v}"></span>${k}</label>`,
      )
      .join('');
    const edgeColorLegend = Object.entries(EDGE_COLORS)
      .map(
        ([k, v]) =>
          `<label class="leg-item"><input type="checkbox" checked data-etype="${k}"><span class="line" style="background:${v}"></span>${k}</label>`,
      )
      .join('');

    return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Mnemon — ${insights.length} insights</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{display:flex;height:100vh;overflow:hidden;background:#111827;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:13px}
#net{flex:1;height:100%}
#panel{width:260px;flex-shrink:0;display:flex;flex-direction:column;background:#1e1e2e;border-left:1px solid #2d2d3d;overflow:hidden}
.pblock{padding:12px;border-bottom:1px solid #2d2d3d}
.pblock h3{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#6b7280;margin-bottom:8px}
.leg-item{display:flex;align-items:center;gap:7px;padding:3px 0;cursor:pointer;user-select:none;color:#d1d5db}
.leg-item input{cursor:pointer;accent-color:#6366f1}
.dot{width:11px;height:11px;border-radius:50%;flex-shrink:0}
.line{width:18px;height:3px;border-radius:2px;flex-shrink:0}
#stats{color:#9ca3af;font-size:12px;line-height:1.6}
#search{width:100%;padding:6px 8px;background:#111827;border:1px solid #374151;border-radius:6px;color:#e0e0e0;font-size:12px;outline:none}
#search:focus{border-color:#6366f1}
#detail{flex:1;overflow-y:auto;padding:12px}
#detail-empty{color:#4b5563;font-size:12px;font-style:italic}
#detail-content{display:none}
#detail-cat{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;margin-bottom:8px;color:#fff}
#detail-text{color:#d1d5db;font-size:12px;line-height:1.6;white-space:pre-wrap;word-break:break-word}
#detail-id{color:#4b5563;font-size:10px;margin-top:8px;font-family:monospace}
#detail-connections{margin-top:10px}
#detail-connections h3{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#6b7280;margin-bottom:6px}
.conn-item{padding:4px 6px;border-radius:4px;margin-bottom:3px;font-size:11px;color:#d1d5db;background:#111827;cursor:pointer;border-left:3px solid transparent}
.conn-item:hover{background:#1a1a2e}
#reset-btn{margin:12px;padding:6px 10px;background:#374151;color:#d1d5db;border:none;border-radius:6px;cursor:pointer;font-size:12px;width:calc(100% - 24px)}
#reset-btn:hover{background:#4b5563}
</style>
</head><body>
<div id="net"></div>
<div id="panel">
  <div class="pblock">
    <div id="stats">${insights.length} insights &nbsp;·&nbsp; ${edges.length} edges</div>
  </div>
  <div class="pblock">
    <h3>Search</h3>
    <input id="search" placeholder="Filter nodes…" autocomplete="off">
  </div>
  <div class="pblock">
    <h3>Categories</h3>
    ${nodeColorLegend}
  </div>
  <div class="pblock">
    <h3>Edge types</h3>
    ${edgeColorLegend}
  </div>
  <button id="reset-btn">Reset view</button>
  <div id="detail">
    <div id="detail-empty">Click a node to explore</div>
    <div id="detail-content">
      <span id="detail-cat"></span>
      <div id="detail-text"></div>
      <div id="detail-id"></div>
      <div id="detail-connections">
        <h3>Connections</h3>
        <div id="conn-list"></div>
      </div>
    </div>
  </div>
</div>
<script>
var allNodes = new vis.DataSet([${nodeJs}]);
var allEdges = new vis.DataSet([${edgeJs}]);
var origColors = {};
allNodes.forEach(function(n){ origColors[n.id] = JSON.parse(JSON.stringify(n.color)); });

var container = document.getElementById("net");
var network = new vis.Network(container, {nodes: allNodes, edges: allEdges}, {
  physics: { solver:"forceAtlas2Based", forceAtlas2Based:{gravitationalConstant:-50, centralGravity:0.01, springLength:180}, stabilization:{iterations:200} },
  interaction: { hover:true, tooltipDelay:99999, hideEdgesOnDrag:false, multiselect:false },
  nodes: { shape:"box", borderRadius:4, font:{size:11,color:"#fff",face:"monospace"}, shadow:{enabled:true,color:"rgba(0,0,0,0.5)",size:8} },
  edges: { smooth:{type:"continuous",roundness:0.2}, font:{size:9,align:"middle"}, selectionWidth:3, hoverWidth:2 },
  layout: {}
});

network.once("stabilizationIterationsDone", function() {
  network.setOptions({ physics: { enabled:false } });
});

// ── highlight (batch updates) ──────────────────────────────────────
var highlighted = null;
function doHighlight(nodeId) {
  highlighted = nodeId;
  var connected = new Set(network.getConnectedNodes(nodeId));
  connected.add(nodeId);
  var connEdges = new Set(network.getConnectedEdges(nodeId));
  allNodes.update(allNodes.get().map(function(n) {
    if (n.hidden) return {id:n.id};
    return connected.has(n.id)
      ? {id:n.id, color:origColors[n.id], opacity:1}
      : {id:n.id, color:{background:"#1f2937",border:"#374151"}, opacity:0.3};
  }));
  allEdges.update(allEdges.get().map(function(e) {
    if (e.hidden) return {id:e.id};
    return {id:e.id, opacity: connEdges.has(e.id) ? 1 : 0.08};
  }));
}
function resetHighlight() {
  highlighted = null;
  allNodes.update(allNodes.get().map(function(n){ return n.hidden ? {id:n.id} : {id:n.id, color:origColors[n.id], opacity:1}; }));
  allEdges.update(allEdges.get().map(function(e){ return e.hidden ? {id:e.id} : {id:e.id, opacity:1}; }));
}

// ── detail panel ───────────────────────────────────────────────────
var nodeColors = ${JSON.stringify(NODE_COLORS)};
var edgeColors = ${JSON.stringify(EDGE_COLORS)};
function showDetail(nodeId) {
  var n = allNodes.get(nodeId);
  if (!n) return;
  document.getElementById("detail-empty").style.display = "none";
  document.getElementById("detail-content").style.display = "block";
  var catEl = document.getElementById("detail-cat");
  catEl.textContent = n.category;
  catEl.style.background = nodeColors[n.category] || "#95a5a6";
  document.getElementById("detail-text").textContent = n.fullText;
  document.getElementById("detail-id").textContent = n.id;
  var connList = document.getElementById("conn-list");
  connList.innerHTML = "";
  var connEdges = network.getConnectedEdges(nodeId);
  connEdges.forEach(function(eid) {
    var e = allEdges.get(eid);
    if (!e) return;
    var otherId = e.from === nodeId ? e.to : e.from;
    var other = allNodes.get(otherId);
    if (!other) return;
    var dir = e.from === nodeId ? "→" : "←";
    var d = document.createElement("div");
    d.className = "conn-item";
    d.style.borderLeftColor = edgeColors[e.edgeType] || "#888";
    d.textContent = dir + " [" + e.edgeType + "] " + other.fullText.slice(0,60) + (other.fullText.length > 60 ? "…" : "");
    d.onclick = function() { network.selectNodes([otherId]); showDetail(otherId); doHighlight(otherId); };
    connList.appendChild(d);
  });
}
function clearDetail() {
  document.getElementById("detail-empty").style.display = "";
  document.getElementById("detail-content").style.display = "none";
}

network.on("click", function(params) {
  if (params.nodes.length > 0) {
    doHighlight(params.nodes[0]);
    showDetail(params.nodes[0]);
  } else {
    resetHighlight();
    clearDetail();
  }
});
network.on("doubleClick", function(params) {
  if (params.nodes.length > 0) network.focus(params.nodes[0], {scale:1.4, animation:{duration:400}});
});

// ── filters (hidden property + single batch update — no remove/add) ──
var hiddenEdgeTypes = new Set();
var hiddenCats = new Set();

function applyFilters() {
  resetHighlight(); clearDetail();
  var visibleNodeIds = new Set();
  allNodes.forEach(function(n){ if (!hiddenCats.has(n.category)) visibleNodeIds.add(n.id); });
  allNodes.update(allNodes.get().map(function(n){
    return { id: n.id, hidden: hiddenCats.has(n.category) };
  }));
  allEdges.update(allEdges.get().map(function(e){
    return { id: e.id, hidden: hiddenEdgeTypes.has(e.edgeType) || !visibleNodeIds.has(e.from) || !visibleNodeIds.has(e.to) };
  }));
}

document.querySelectorAll("[data-etype]").forEach(function(cb) {
  cb.addEventListener("change", function() {
    if (this.checked) hiddenEdgeTypes.delete(this.dataset.etype); else hiddenEdgeTypes.add(this.dataset.etype);
    applyFilters();
  });
});
document.querySelectorAll("[data-cat]").forEach(function(cb) {
  cb.addEventListener("change", function() {
    if (this.checked) hiddenCats.delete(this.dataset.cat); else hiddenCats.add(this.dataset.cat);
    applyFilters();
  });
});

// ── search ─────────────────────────────────────────────────────────
var searchTimeout;
document.getElementById("search").addEventListener("input", function() {
  clearTimeout(searchTimeout);
  var q = this.value.trim().toLowerCase();
  searchTimeout = setTimeout(function() {
    if (!q) { resetHighlight(); return; }
    var matchSet = new Set();
    allNodes.forEach(function(n) {
      if (!n.hidden && (n.fullText.toLowerCase().includes(q) || n.category.toLowerCase().includes(q))) matchSet.add(n.id);
    });
    if (matchSet.size === 0) return;
    if (matchSet.size === 1) { var id = matchSet.values().next().value; doHighlight(id); showDetail(id); return; }
    allNodes.update(allNodes.get().map(function(n) {
      if (n.hidden) return {id:n.id};
      return {id:n.id, color: matchSet.has(n.id) ? origColors[n.id] : {background:"#1f2937",border:"#374151"}, opacity: matchSet.has(n.id) ? 1 : 0.2};
    }));
  }, 250);
});

document.getElementById("reset-btn").addEventListener("click", function() {
  resetHighlight(); clearDetail();
  document.getElementById("search").value = "";
  network.fit({ animation:{duration:500} });
});
</script>
</body></html>`;
  } catch {
    return null;
  }
}

// ── Status broadcasting ─────────────────────────────────────────────

const TOOL_LABELS: Record<string, string> = {
  bash: 'Running code…',
  Bash: 'Running code…',
  computer: 'Using computer…',
  Computer: 'Using computer…',
  web_search: 'Searching…',
  WebSearch: 'Searching…',
  str_replace_based_edit_tool: 'Editing files…',
  read_file: 'Reading files…',
  write_to_file: 'Writing files…',
};

function toolLabel(tool: string): string {
  return (
    TOOL_LABELS[tool] ??
    (tool.toLowerCase().includes('search')
      ? 'Searching…'
      : tool.toLowerCase().includes('file')
        ? 'Editing files…'
        : 'Working…')
  );
}

function broadcastStatus(clients: Map<string, ClientInfo>, threadId: string, phase: string, text: string): void {
  const frame = encodeFrame(JSON.stringify({ type: 'status', phase, text }));
  for (const [, client] of clients) {
    if (client.threadId !== threadId) continue;
    try {
      client.socket.write(frame);
    } catch {
      // status frames are best-effort — don't eject the client here;
      // deliver() is the authoritative cleanup path
    }
  }
}

async function pollAndBroadcastStatus(
  clients: Map<string, ClientInfo>,
  threadId: string,
  messageId: string,
): Promise<void> {
  broadcastStatus(clients, threadId, 'thinking', 'Thinking…');

  for (let i = 0; i < 90; i++) {
    await new Promise<void>((r) => setTimeout(r, 1000));

    const sess = findWebSession(threadId);
    if (!sess) continue;

    const outPath = path.join(process.cwd(), 'data', 'v2-sessions', sess.agentGroupId, sess.sessionId, 'outbound.db');

    try {
      const outDb = new Database(outPath, { readonly: true, fileMustExist: true });
      outDb.pragma('busy_timeout = 1000');
      try {
        // Router appends :agentGroupId to namespace the id across fan-out targets
        const fullMsgId = `${messageId}:${sess.agentGroupId}`;
        const ack = outDb.prepare('SELECT status FROM processing_ack WHERE message_id=? LIMIT 1').get(fullMsgId) as
          | { status: string }
          | undefined;

        if (ack?.status === 'completed' || ack?.status === 'failed') return;

        if (ack?.status === 'processing') {
          let tool: string | null = null;
          try {
            const state = outDb.prepare('SELECT current_tool FROM container_state WHERE id=1').get() as
              | { current_tool: string | null }
              | undefined;
            tool = state?.current_tool ?? null;
          } catch {
            /* container_state not present on older sessions */
          }
          broadcastStatus(clients, threadId, tool ? 'working' : 'processing', tool ? toolLabel(tool) : 'Thinking…');
        } else {
          // No processing_ack row yet — container starting or waiting for poll cycle
          broadcastStatus(clients, threadId, 'starting', 'Starting…');
        }
      } finally {
        outDb.close();
      }
    } catch {
      /* outbound DB not ready yet */
    }
  }
}

// ── Chat UI ────────────────────────────────────────────────────────

function chatUI(token: string): string {
  const tokenMeta = token ? `<meta name="token" content="${token.replace(/"/g, '&quot;')}">` : '';
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,user-scalable=no">
<title>NanoClaw</title>${tokenMeta}
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f0f0f;color:#e0e0e0;height:100dvh;display:flex;overflow:hidden}
/* ── Sidebar ── */
#sidebar{width:200px;flex-shrink:0;display:flex;flex-direction:column;border-right:1px solid #27272a;background:#111}
#sidebar-top{padding:10px}
#new-btn{width:100%;padding:8px 10px;background:#2563eb;color:#fff;border:none;border-radius:7px;cursor:pointer;font-size:13px;font-weight:500;text-align:left}
#new-btn:hover{background:#1d4ed8}
#new-form{padding:4px 10px 8px;display:none}
#new-input{width:100%;padding:6px 8px;background:#1e1e1e;border:1px solid #3f3f46;border-radius:6px;color:#e0e0e0;font-size:13px;outline:none}
#new-input:focus{border-color:#2563eb}
#thread-list{flex:1;overflow-y:auto;padding:4px 6px}
.thread{display:flex;align-items:center;gap:7px;padding:7px 8px;border-radius:6px;cursor:pointer;font-size:13px;color:#a1a1aa;overflow:hidden;white-space:nowrap;user-select:none}
.thread:hover{background:#1e1e1e;color:#d4d4d8}
.thread.active{background:#1e3a5f;color:#93c5fd}
.tdot{width:6px;height:6px;border-radius:50%;background:#3f3f46;flex-shrink:0}
.thread.active .tdot{background:#3b82f6}
.tname{overflow:hidden;text-overflow:ellipsis}
/* ── Chat ── */
#chat{flex:1;display:flex;flex-direction:column;min-width:0}
#mob-header{display:none}
#messages{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:8px}
.msg{max-width:85%;padding:10px 14px;border-radius:16px;line-height:1.45;white-space:pre-wrap;word-break:break-word;font-size:15px}
.msg.user{align-self:flex-end;background:#2563eb;color:#fff;border-bottom-right-radius:4px}
.msg.bot{align-self:flex-start;background:#1e1e1e;color:#e0e0e0;border-bottom-left-radius:4px}
.msg.info{align-self:center;color:#71717a;font-size:12px}
#input-area{display:flex;padding:10px;gap:8px;border-top:1px solid #27272a}
#input{flex:1;padding:10px 14px;border-radius:22px;border:1px solid #3f3f46;background:#1e1e1e;color:#e0e0e0;font-size:15px;outline:none;resize:none}
#input:focus{border-color:#2563eb}
#send{width:40px;height:40px;border-radius:50%;border:none;background:#2563eb;color:#fff;font-size:18px;cursor:pointer;flex-shrink:0;display:flex;align-items:center;justify-content:center}
#send:disabled{opacity:0.4;cursor:default}
#bar{text-align:center;padding:3px;font-size:11px;color:#52525b}
.si-msg{align-self:flex-start;max-width:85%;padding:10px 14px;background:#1e1e1e;border-radius:16px;border-bottom-left-radius:4px;color:#71717a;display:flex;align-items:center;gap:8px;font-size:14px}
.q-card{align-self:flex-start;max-width:85%;padding:12px 14px;background:#1e1e1e;border-radius:16px;border-bottom-left-radius:4px;color:#e0e0e0;border:1px solid #3f3f46}
.q-title{font-size:13px;font-weight:600;color:#a78bfa;margin-bottom:4px}
.q-body{font-size:13px;color:#9ca3af;white-space:pre-wrap;word-break:break-word;margin-bottom:10px}
.q-btns{display:flex;gap:8px;flex-wrap:wrap}
.q-btn{padding:6px 16px;border-radius:8px;border:1px solid #4b5563;background:transparent;color:#e0e0e0;cursor:pointer;font-size:13px;transition:background .15s,border-color .15s}
.q-btn:hover:not(:disabled){background:#374151}
.q-btn.ok{border-color:#22c55e;color:#22c55e}.q-btn.ok:hover:not(:disabled){background:#052e16}
.q-btn.no{border-color:#ef4444;color:#ef4444}.q-btn.no:hover:not(:disabled){background:#2d0000}
.q-btn:disabled{opacity:0.5;cursor:default}
.q-answered{color:#6b7280;font-size:13px;font-style:italic}
.si-dots{display:flex;gap:3px;align-items:center}
.si-dot{width:5px;height:5px;background:#4b5563;border-radius:50%;animation:si 1.2s infinite ease-in-out}
.si-dot:nth-child(2){animation-delay:.15s}
.si-dot:nth-child(3){animation-delay:.3s}
@keyframes si{0%,80%,100%{transform:scale(0.55);opacity:0.3}40%{transform:scale(1);opacity:1}}
/* ── Mobile ── */
#backdrop{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.55);z-index:99}
@media(max-width:599px){
  #sidebar{position:fixed;top:0;left:0;width:80vw;max-width:280px;height:100%;z-index:100;transform:translateX(-100%);transition:transform .2s ease;border-right:none;box-shadow:4px 0 24px rgba(0,0,0,.5)}
  #sidebar.open{transform:translateX(0)}
  #backdrop.open{display:block}
  #mob-header{display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #27272a;flex-shrink:0}
  #burger{background:none;border:none;color:#e0e0e0;font-size:22px;cursor:pointer;line-height:1;padding:2px 4px}
  #mob-title{font-size:14px;color:#a1a1aa;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
}
</style>
</head>
<body>
<div id="backdrop"></div>
<div id="sidebar">
  <div id="sidebar-top"><button id="new-btn">＋ New thread</button></div>
  <div id="new-form"><input id="new-input" placeholder="Thread name…" autocomplete="off"></div>
  <div id="thread-list"></div>
</div>
<div id="chat">
  <div id="mob-header">
    <button id="burger">&#9776;</button>
    <span id="mob-title"></span>
  </div>
  <div id="messages"><div class="msg info">Connecting…</div></div>
  <div id="input-area">
    <textarea id="input" rows="1" placeholder="Message…" autofocus></textarea>
    <button id="send" disabled>&#9654;</button>
  </div>
  <div id="bar">connecting…</div>
</div>
<script>
const token=document.querySelector("meta[name=token]")?.content||"";
const sp=new URLSearchParams(location.search);
const cur=sp.get("t")||"default";
if(!sp.has("t")){const u=new URL(location.href);u.searchParams.set("t","default");history.replaceState({},"",u)}
const proto=location.protocol==="https:"?"wss:":"ws:";
const wsUrl=proto+"//"+location.host+"/?t="+encodeURIComponent(cur)+"&token="+encodeURIComponent(token);
let ws,ok=false;
const $=id=>document.getElementById(id);
const msgs=$("messages"),inp=$("input"),btn=$("send"),bar=$("bar"),tlist=$("thread-list");
const newBtn=$("new-btn"),newForm=$("new-form"),newInp=$("new-input");
const sidebar=$("sidebar"),backdrop=$("backdrop"),mobTitle=$("mob-title");
if(mobTitle)mobTitle.textContent=cur;

function esc(s){return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}

function openSidebar(){sidebar.classList.add("open");backdrop.classList.add("open")}
function closeSidebar(){sidebar.classList.remove("open");backdrop.classList.remove("open")}
$("burger").onclick=openSidebar;
backdrop.onclick=closeSidebar;

function addMsg(text,role){
  const d=document.createElement("div");
  d.className="msg "+role;d.textContent=text;
  msgs.appendChild(d);msgs.scrollTop=msgs.scrollHeight;
}

function renderThreads(threads){
  const seen=new Set(threads.map(t=>t.thread_id));
  if(!seen.has(cur))threads=[{thread_id:cur},...threads];
  tlist.innerHTML="";
  for(const t of threads){
    const d=document.createElement("div");
    d.className="thread"+(t.thread_id===cur?" active":"");
    d.innerHTML='<div class="tdot"></div><span class="tname">'+esc(t.thread_id)+'</span>';
    d.onclick=()=>{const u=new URL(location.href);u.searchParams.set("t",t.thread_id);location.href=u};
    tlist.appendChild(d);
  }
}

async function loadThreads(){
  try{
    const q=token?"?token="+encodeURIComponent(token):"";
    const r=await fetch("/api/threads"+q);
    renderThreads(r.ok?await r.json():[]);
  }catch{renderThreads([])}
}

newBtn.onclick=()=>{
  const show=newForm.style.display!=="block";
  newForm.style.display=show?"block":"none";
  if(show){newInp.value="";newInp.focus()}
};
newInp.onkeydown=e=>{
  if(e.key==="Enter"){
    const name=newInp.value.trim().toLowerCase().replace(/[^a-z0-9]+/g,"-").replace(/^-+|-+$/g,"")||"new";
    const u=new URL(location.href);u.searchParams.set("t",name);location.href=u;
  }
  if(e.key==="Escape"){newForm.style.display="none"}
};

function showQuestion(q){
  const d=document.createElement("div");
  d.className="q-card";
  let h=q.title?'<div class="q-title">'+esc(q.title)+'</div>':"";
  if(q.question)h+='<div class="q-body">'+esc(q.question)+'</div>';
  h+='<div class="q-btns">';
  for(const o of(q.options||[])){
    const cls=o.value==="approve"?"ok":o.value==="reject"?"no":"";
    h+='<button class="q-btn '+cls+'" data-qid="'+esc(String(q.questionId))+'" data-val="'+esc(String(o.value))+'" data-label="'+esc(String(o.selectedLabel||o.label))+'">'+esc(String(o.label))+'</button>';
  }
  h+='</div>';
  d.innerHTML=h;
  d.querySelectorAll(".q-btn").forEach(function(btn){
    btn.addEventListener("click",function(){
      d.querySelector(".q-btns").innerHTML='<span class="q-answered">'+esc(btn.dataset.label)+'</span>';
      if(ws&&ws.readyState===1)ws.send(JSON.stringify({questionId:btn.dataset.qid,value:btn.dataset.val}));
    });
  });
  msgs.appendChild(d);msgs.scrollTop=msgs.scrollHeight;
}
function connect(){
  ws=new WebSocket(wsUrl);
  ws.onopen=()=>{ok=true;btn.disabled=false;bar.textContent=cur;inp.focus()};
  let siEl=null;
function showStatus(text){
  if(!siEl){siEl=document.createElement("div");siEl.className="si-msg";siEl.innerHTML='<div class="si-dots"><div class="si-dot"></div><div class="si-dot"></div><div class="si-dot"></div></div><span class="si-text"></span>';}
  siEl.querySelector(".si-text").textContent=text;
  msgs.appendChild(siEl);msgs.scrollTop=msgs.scrollHeight;
}
function clearStatus(){if(siEl&&siEl.parentNode)siEl.parentNode.removeChild(siEl);}
  ws.onmessage=e=>{
  try{
    const m=JSON.parse(e.data);
    if(m.type==="status"){if(m.text)showStatus(m.text);else clearStatus();}
    else if(m.type==="question"||m.type==="ask_question"){clearStatus();showQuestion(m);}
    else if(m.text){clearStatus();addMsg(m.text,m.role||"bot");}
  }catch{clearStatus();addMsg(e.data,"bot");}
};
  ws.onclose=()=>{ok=false;btn.disabled=true;bar.textContent="reconnecting…";setTimeout(connect,2000)};
  ws.onerror=()=>ws.close();
}

function send(){
  const text=inp.value.trim();if(!text||!ok)return;
  ws.send(JSON.stringify({text}));addMsg(text,"user");
  inp.value="";inp.style.height="auto";
}
btn.onclick=send;
inp.onkeydown=e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();send()}};
inp.oninput=()=>{inp.style.height="auto";inp.style.height=inp.scrollHeight+"px"};

async function loadHistory(){
  try{
    const q="?t="+encodeURIComponent(cur)+(token?"&token="+encodeURIComponent(token):"");
    const r=await fetch("/api/messages"+q);
    if(!r.ok)return;
    const history=await r.json();
    if(!history.length)return;
    msgs.innerHTML="";
    for(const m of history)addMsg(m.text,m.role);
    const sep=document.createElement("div");
    sep.className="msg info";sep.textContent="─────────────";
    msgs.appendChild(sep);
    msgs.scrollTop=msgs.scrollHeight;
  }catch{/* skip */}
}

loadThreads();loadHistory().then(connect);
</script>
</body>
</html>`;
}

// ── WebSocket upgrade handler ───────────────────────────────────────

function handleUpgrade(
  req: http.IncomingMessage,
  socket: Duplex,
  port: number,
  clients: Map<string, ClientInfo>,
  config: ChannelSetup,
): void {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }

  const url = new URL(req.url || '/', `http://localhost:${port}`);
  const threadId = url.searchParams.get('t') || DEFAULT_THREAD;

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' +
      acceptKey(key) +
      '\r\n\r\n',
  );

  const clientId = crypto.randomUUID();
  clients.set(clientId, { socket, threadId });
  log.info('Web client connected', { clientId, threadId, total: clients.size });

  let buffer = Buffer.alloc(0);

  socket.on('data', (chunk: Buffer) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const opcode = buffer[0] & 0x0f;

      if (opcode === 0x8) {
        socket.end();
        return;
      }

      if (opcode === 0x9) {
        const payloadLen = buffer[1] & 0x7f;
        socket.write(Buffer.concat([Buffer.from([0x8a, payloadLen]), buffer.subarray(2, 2 + payloadLen)]));
        buffer = buffer.subarray(2 + payloadLen);
        continue;
      }

      if (opcode !== 0x1) {
        buffer = Buffer.alloc(0);
        break;
      }

      let payloadLen = buffer[1] & 0x7f;
      const masked = (buffer[1] & 0x80) !== 0;
      let offset = 2;

      if (payloadLen === 126) {
        if (buffer.length < 4) break;
        payloadLen = buffer.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (buffer.length < 10) break;
        payloadLen = Number(buffer.readBigUInt64BE(2));
        offset = 10;
      }

      const maskOffset = masked ? 4 : 0;
      const totalLen = offset + maskOffset + payloadLen;
      if (buffer.length < totalLen) break;

      const maskKey = masked ? buffer.subarray(offset, offset + 4) : null;
      const payload = buffer.subarray(offset + maskOffset, totalLen);
      if (masked && maskKey) {
        for (let i = 0; i < payload.length; i++) payload[i] ^= maskKey[i % 4];
      }

      buffer = buffer.subarray(totalLen);

      try {
        const parsed = JSON.parse(payload.toString('utf-8'));
        if (parsed.questionId && String(parsed.value).trim()) {
          // Question card response (Approve / Reject button click)
          config.onAction?.(String(parsed.questionId), String(parsed.value), 'web:' + clientId);
        } else if (parsed.text && String(parsed.text).trim()) {
          const msgId = clientId + ':' + Date.now();
          void config.onInbound(PLATFORM_ID, threadId, {
            id: msgId,
            kind: 'chat',
            timestamp: new Date().toISOString(),
            content: {
              text: String(parsed.text).trim(),
              sender: 'web',
              senderId: 'web:' + clientId,
            },
          });
          void pollAndBroadcastStatus(clients, threadId, msgId);
        }
      } catch {
        /* ignore non-JSON */
      }
    }
  });

  socket.on('close', () => {
    clients.delete(clientId);
    log.info('Web client disconnected', { clientId, total: clients.size });
  });

  socket.on('error', (err: Error) => {
    log.warn('Web client socket error', { clientId, err: err.message });
    clients.delete(clientId);
  });
}

// ── Channel adapter ─────────────────────────────────────────────────

function createAdapter(): ChannelAdapter {
  let server: http.Server | null = null;
  let setupConfig: ChannelSetup | null = null;
  const clients = new Map<string, ClientInfo>();

  const adapter: ChannelAdapter = {
    name: 'web',
    channelType: 'web',
    supportsThreads: true,

    async setup(config: ChannelSetup): Promise<void> {
      setupConfig = config;
      const port = parseInt(process.env.WEB_CHANNEL_PORT || String(DEFAULT_PORT), 10);
      const token = process.env.WEB_CHANNEL_TOKEN || '';

      server = http.createServer((req, res) => {
        const url = new URL(req.url || '/', `http://localhost:${port}`);

        if (token) {
          const urlToken = url.searchParams.get('token') || '';
          const bearer = (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '');
          if (urlToken !== token && bearer !== token) {
            res.writeHead(401, { 'Content-Type': 'text/plain' });
            res.end('Unauthorized');
            return;
          }
        }

        if (url.pathname === '/api/threads') {
          const body = JSON.stringify(getThreads());
          res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
          res.end(body);
          return;
        }

        if (url.pathname === '/mnemon-graph') {
          const html = getMnemonGraph();
          if (!html) {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('Mnemon graph not available');
            return;
          }
          res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': Buffer.byteLength(html) });
          res.end(html);
          return;
        }

        if (url.pathname === '/api/messages') {
          const t = url.searchParams.get('t') || DEFAULT_THREAD;
          const body = JSON.stringify(getMessages(t));
          res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
          res.end(body);
          return;
        }

        const html = chatUI(token);
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': Buffer.byteLength(html) });
        res.end(html);
      });

      server.on('upgrade', (req, socket) => {
        if (token) {
          const url = new URL(req.url || '/', `http://localhost:${port}`);
          if ((url.searchParams.get('token') || '') !== token) {
            socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            socket.destroy();
            return;
          }
        }
        handleUpgrade(req, socket, port, clients, setupConfig!);
      });

      server.on('error', (err: Error) => {
        log.error('Web channel server error', { err: err.message });
      });

      await new Promise<void>((resolve, reject) => {
        server!.once('error', reject);
        server!.listen(port, '0.0.0.0', () => {
          log.info('Web channel listening', { port, auth: !!token, url: 'http://localhost:' + port });
          resolve();
        });
      });
    },

    async teardown(): Promise<void> {
      for (const [, { socket }] of clients) {
        try {
          socket.end();
        } catch {
          /* swallow */
        }
      }
      clients.clear();
      if (server) {
        await new Promise<void>((resolve) => server!.close(() => resolve()));
        server = null;
      }
      setupConfig = null;
    },

    isConnected(): boolean {
      return server !== null;
    },

    async deliver(_platformId: string, threadId: string | null, message: OutboundMessage): Promise<string | undefined> {
      const content = message.content as Record<string, unknown> | string | undefined;

      // Render ask_question cards as interactive button cards in the web UI.
      // Return undefined (not throw) when no clients — caller falls back to owner DM.
      if (content && typeof content === 'object' && content.type === 'ask_question') {
        const frame = encodeFrame(JSON.stringify({ type: 'question', ...content }));
        let sent = false;
        for (const [id, client] of clients) {
          if (threadId !== null && client.threadId !== threadId) continue;
          try {
            client.socket.write(frame);
            sent = true;
          } catch (err) {
            log.warn('Web client write error', { clientId: id, err: (err as Error).message });
            clients.delete(id);
          }
        }
        return sent ? PLATFORM_ID + ':' + Date.now() : undefined;
      }

      const text = extractText(message);
      if (!text) return undefined;

      const frame = encodeFrame(JSON.stringify({ text, role: 'bot' }));
      let sent = false;

      for (const [id, client] of clients) {
        if (threadId !== null && client.threadId !== threadId) continue;
        try {
          client.socket.write(frame);
          sent = true;
        } catch (err) {
          log.warn('Web client write error', { clientId: id, err: (err as Error).message });
          clients.delete(id);
        }
      }
      if (!sent) throw new Error(`no web clients connected for thread ${threadId ?? '*'}`);
      return PLATFORM_ID + ':' + Date.now();
    },
  };

  return adapter;
}

function extractText(message: OutboundMessage): string | null {
  const content = message.content as Record<string, unknown> | string | undefined;
  if (typeof content === 'string') return content;
  if (content && typeof content === 'object' && typeof content.text === 'string') return content.text;
  return null;
}

registerChannelAdapter('web', { factory: createAdapter });
