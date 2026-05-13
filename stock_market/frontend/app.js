const API_BASE = "http://localhost:8000";

const ALL_NODES = [
  { id: "orchestrator", label: "Orchestrator", group: "main" },
  { id: "research_supervisor", label: "Research Supervisor", group: "research" },
  { id: "analyst_supervisor", label: "Analyst Supervisor", group: "analysis" },
  { id: "tavily", label: "Tavily Search", group: "research" },
  { id: "reuters_marketwatch", label: "Reuters & MarketWatch", group: "research" },
  { id: "stocktwits", label: "StockTwits", group: "research" },
  { id: "edgar", label: "SEC EDGAR", group: "research" },
  { id: "tavily_analysis", label: "Tavily Analysis", group: "research" },
  { id: "reuters_analysis", label: "Reuters Analysis", group: "research" },
  { id: "stocktwits_analysis", label: "StockTwits Analysis", group: "research" },
  { id: "edgar_analysis", label: "EDGAR Analysis", group: "research" },
  { id: "research_reducer", label: "Research Reducer", group: "research" },
  { id: "yahoo", label: "Yahoo Finance", group: "analysis" },
  { id: "kpi", label: "KPI Calculator", group: "analysis" },
  { id: "chart_gen", label: "Chart Generator", group: "analysis" },
  { id: "kpi_analysis", label: "KPI Analysis", group: "analysis" },
  { id: "risk_signal", label: "Risk Signal", group: "analysis" },
  { id: "profit_loss_expectation", label: "One-Year Forecast", group: "analysis" },
  { id: "analyst_reducer", label: "Analyst Reducer", group: "analysis" },
  { id: "analyst_gate", label: "Analyst Gate", group: "main" },
  { id: "writer_gate", label: "Writer Gate", group: "main" },
  { id: "writer", label: "Deep Research Writer", group: "main" },
  { id: "image_planner", label: "Image Planner", group: "main" },
  { id: "gemini_image", label: "Image & Report Builder", group: "main" },
];

const state = {
  token: localStorage.getItem("token") || "",
  route: "dashboard",
  query: "",
  sessionId: "",
  sessionStatus: "idle",
  nodeStates: {},
  liveLogs: [],
  history: [],
  report: "",
  ticker: "",
  confidence: 0,
  riskSignal: "",
  sectionScores: {},
  activeTab: "nodes",
  ws: null,
  startedAt: null,
  elapsedTimer: null,
};

function $(selector) {
  return document.querySelector(selector);
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function api(path, options = {}) {
  return fetch(`${API_BASE}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
}

function setRoute(route) {
  state.route = route;
  render();
}

function render() {
  if (state.route === "dashboard") return renderDashboard();
  if (state.route === "login") return renderLogin();
  if (state.route === "signup") return renderSignup();
  renderChat();
}

function renderDashboard() {
  $("#app").innerHTML = `
    <main class="dashboard-hero">
      <header class="dashboard-nav">
        <div class="brand">Duma<span>X</span></div>
        <button id="dashboard-login" class="ghost compact">Login</button>
      </header>
      <section class="hero-content">
        <div class="hero-text">
          <h1><span>AI-Powered</span> Stock Analysis with Collaborative Agents</h1>
          <p>Deep stock search, financial metrics, sentiment analysis, charts, and BUY/HOLD/SELL reports in one workspace.</p>
          <div class="hero-actions">
            <button id="start-chat" class="primary hero-btn">Start Chat</button>
            <button id="demo-dashboard" class="ghost hero-btn">Dashboard</button>
          </div>
          <div class="partner-logos">
            <b>Yahoo Finance</b><i></i><b>Gemini</b><i></i><b>Tavily</b><i></i><b>SEC EDGAR</b>
          </div>
        </div>
        <div class="robot-stage">
          <div class="robot-card r1">🤖<span>Research Agent</span></div>
          <div class="robot-card r2">🧠<span>Analyst Agent</span></div>
          <div class="robot-card r3">📊<span>Report Agent</span></div>
        </div>
      </section>
    </main>
  `;
  $("#start-chat").onclick = () => setRoute("login");
  $("#dashboard-login").onclick = () => setRoute("login");
  $("#demo-dashboard").onclick = () => setRoute(state.token ? "chat" : "login");
}

function renderLogin() {
  $("#app").innerHTML = `
    <main class="auth-shell">
      <section class="auth-card">
        <div class="brand">Duma<span>X</span></div>
        <h1>Admin Login</h1>
        <p class="muted">Sign in to run institutional stock research reports.</p>
        <label>Email or username</label>
        <input id="login-email" value="admin" autocomplete="username" />
        <label>Password</label>
        <input id="login-password" type="password" value="admin@1234" autocomplete="current-password" />
        <button id="login-btn" class="primary">Sign In</button>
        <button id="signup-link" class="ghost">Create account</button>
        <p id="auth-error" class="error"></p>
      </section>
    </main>
  `;
  $("#login-btn").onclick = handleLogin;
  $("#signup-link").onclick = () => setRoute("signup");
}

function renderSignup() {
  $("#app").innerHTML = `
    <main class="auth-shell">
      <section class="auth-card">
        <div class="brand">Duma<span>X</span></div>
        <h1>Signup Disabled</h1>
        <p class="muted">User database is disabled for now. Use the local admin account.</p>
        <label>Full name</label>
        <input id="signup-name" placeholder="Customer Admin" />
        <label>Email</label>
        <input id="signup-email" placeholder="admin" />
        <label>Password</label>
        <input id="signup-password" type="password" placeholder="admin@1234" />
        <button id="signup-btn" class="primary">Check Signup Status</button>
        <button id="login-link" class="ghost">Back to login</button>
        <p id="auth-error" class="error"></p>
      </section>
    </main>
  `;
  $("#signup-btn").onclick = handleSignup;
  $("#login-link").onclick = () => setRoute("login");
}

async function handleLogin() {
  const email = $("#login-email").value.trim();
  const password = $("#login-password").value;
  const error = $("#auth-error");
  error.textContent = "";
  try {
    const res = await api("/api/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Login failed");
    localStorage.setItem("token", data.token);
    state.token = data.token;
    state.route = "chat";
    await loadHistory();
    render();
  } catch (err) {
    error.textContent = err.message;
  }
}

async function handleSignup() {
  const res = await api("/auth/signup", {
    method: "POST",
    body: JSON.stringify({
      fullName: $("#signup-name").value,
      email: $("#signup-email").value,
      password: $("#signup-password").value,
    }),
  });
  const data = await res.json();
  $("#auth-error").textContent = data.message || "Signup is disabled.";
}

async function loadHistory() {
  try {
    const res = await api("/history?limit=30");
    const data = await res.json();
    state.history = data.results || [];
  } catch {
    state.history = [];
  }
}

function renderChat() {
  const done = Object.values(state.nodeStates).filter((n) => n.status === "done").length;
  const running = ALL_NODES.filter((n) => state.nodeStates[n.id]?.status === "running");
  const progress = Math.round((done / ALL_NODES.length) * 100);
  $("#app").innerHTML = `
    <div class="app-shell">
      <aside class="sidebar">
        <div class="sidebar-head">
          <div class="brand small">Duma<span>X</span></div>
          <button id="new-chat" class="mini">New</button>
        </div>
        <div class="history-list">
          ${state.history.map(historyItemTemplate).join("") || `<p class="muted pad">No reports yet</p>`}
        </div>
        <button id="clear-db" class="logout">Clear Database</button>
        <button id="logout" class="logout">Logout</button>
      </aside>
      <main class="workspace">
        <header class="topbar">
          <div>
            <h1>Stock Market Research Agent</h1>
            <p>${state.sessionStatus === "running" ? `Running deep search of stock - ${elapsedText()}` : "Deep stock research with live agent tracking"}</p>
          </div>
          <div class="top-metrics">
            <span class="pill">${escapeHtml(state.ticker || "No ticker")}</span>
            <span class="pill ${riskClass(state.riskSignal)}">${escapeHtml(state.riskSignal || "N/A")}</span>
            <span class="pill">${Math.round((state.confidence || 0) * 100)}% confidence</span>
          </div>
        </header>

        <section class="composer">
          <input id="query" value="${escapeHtml(state.query)}" placeholder="Example: NVDA stock analysis with one year prediction" ${state.sessionStatus === "running" ? "disabled" : ""} />
          <button id="run" class="primary" ${state.sessionStatus === "running" ? "disabled" : ""}>Run Analysis</button>
        </section>

        <nav class="tabs">
          ${tabButton("nodes", `Nodes ${done}/${ALL_NODES.length}`)}
          ${tabButton("log", `Live Log ${state.liveLogs.length}`)}
          ${tabButton("report", "Report")}
          ${tabButton("metrics", "Metrics")}
          ${state.sessionStatus === "complete" ? `<button id="pdf" class="download">PDF</button><button id="md" class="download">Markdown</button>` : ""}
        </nav>

        <section class="panel">
          ${state.activeTab === "nodes" ? nodesPanel(progress, running) : ""}
          ${state.activeTab === "log" ? logPanel() : ""}
          ${state.activeTab === "report" ? reportPanel() : ""}
          ${state.activeTab === "metrics" ? metricsPanel() : ""}
        </section>
      </main>
    </div>
  `;
  bindChatEvents();
}

function historyItemTemplate(item) {
  const label = `${item.ticker ? `${item.ticker} - ` : ""}${item.query || ""}`.slice(0, 54);
  return `
    <div class="history-item" data-session="${item.session_id}">
      <button class="history-main" data-session="${item.session_id}">
        <span>${escapeHtml(label)}</span>
        <small>${escapeHtml(item.status)}</small>
      </button>
      <button class="history-delete" data-delete="${item.session_id}" title="Delete report">Delete</button>
    </div>
  `;
}

function tabButton(id, label) {
  return `<button class="tab ${state.activeTab === id ? "active" : ""}" data-tab="${id}">${label}</button>`;
}

function nodesPanel(progress, running) {
  return `
    <div class="progress">
      <div style="width:${progress}%"></div>
    </div>
    <p class="progress-label">${progress}% complete ${running.length ? `- Parallel running now: ${running.map((n) => n.label).join(", ")}` : ""}</p>
    <div class="graph-flow">
      ${GRAPH_STAGES.map(graphStageTemplate).join("")}
    </div>
  `;
}

const GRAPH_STAGES = [
  { title: "1. Start", nodes: ["orchestrator"] },
  { title: "2. Supervisors Run In Parallel", nodes: ["research_supervisor", "analyst_supervisor"] },
  { title: "3A. Research Agents In Parallel", nodes: ["tavily", "reuters_marketwatch", "stocktwits", "edgar"] },
  { title: "3B. Analyst Agents In Parallel", nodes: ["yahoo", "kpi", "chart_gen"] },
  { title: "4A. Research Analysis", nodes: ["tavily_analysis", "reuters_analysis", "stocktwits_analysis", "edgar_analysis", "research_reducer"] },
  { title: "4B. KPI, Risk, Forecast", nodes: ["kpi_analysis", "risk_signal", "profit_loss_expectation", "analyst_gate", "analyst_reducer"] },
  { title: "5. Report Assembly", nodes: ["writer_gate", "writer", "image_planner", "gemini_image"] },
];

function graphStageTemplate(stage) {
  return `
    <div class="graph-stage">
      <h3>${stage.title}</h3>
      <div class="stage-nodes">
        ${stage.nodes.map((id) => nodeTemplate(ALL_NODES.find((n) => n.id === id))).join("")}
      </div>
    </div>
  `;
}

function nodeTemplate(node) {
  if (!node) return "";
  const current = state.nodeStates[node.id] || { status: "waiting", detail: "" };
  return `
    <div class="node ${current.status}">
      <div>
        <strong>${node.label}</strong>
        <span>${escapeHtml(current.detail || "Waiting")}</span>
      </div>
      <b>${current.status}</b>
    </div>
  `;
}

function logPanel() {
  return `
    <div class="log-panel">
      ${state.liveLogs.map((log) => `
        <div class="log-line ${log.status}">
          <time>${new Date(log.ts).toLocaleTimeString()}</time>
          <strong>${escapeHtml(log.node)}</strong>
          <span>${escapeHtml(log.status)}</span>
          <p>${escapeHtml(log.detail || "")}</p>
        </div>
      `).join("") || `<p class="muted pad">Logs will appear as nodes run.</p>`}
    </div>
  `;
}

function reportPanel() {
  if (!state.report && state.sessionStatus === "running") {
    return `<div class="empty-state"><div class="spinner"></div><p>Report is being prepared...</p></div>`;
  }
  if (!state.report) return `<p class="muted pad">No report available yet.</p>`;
  return `
    <div class="report-toolbar">
      <button id="delete-current" class="danger">Delete This Report</button>
    </div>
    <article class="report">${markdownToHtml(state.report)}</article>
  `;
}

function metricsPanel() {
  const scores = Object.entries(state.sectionScores || {});
  return `
    <div class="metric-grid">
      <div class="metric-card"><span>Confidence</span><strong>${Math.round((state.confidence || 0) * 100)}%</strong></div>
      <div class="metric-card"><span>Risk Signal</span><strong>${escapeHtml(state.riskSignal || "N/A")}</strong></div>
      <div class="metric-card"><span>Ticker</span><strong>${escapeHtml(state.ticker || "N/A")}</strong></div>
      <div class="metric-card wide">
        <span>Section Scores</span>
        ${scores.map(([key, value]) => `<div class="score-row"><label>${escapeHtml(key)}</label><div><i style="width:${Number(value) || 0}%"></i></div><b>${escapeHtml(value)}</b></div>`).join("") || "<p>No section scores yet.</p>"}
      </div>
    </div>
  `;
}

function bindChatEvents() {
  $("#new-chat").onclick = resetSession;
  $("#clear-db").onclick = clearDatabase;
  $("#logout").onclick = () => {
    localStorage.removeItem("token");
    state.token = "";
    state.route = "login";
    render();
  };
  $("#query").oninput = (e) => { state.query = e.target.value; };
  $("#run").onclick = runAnalysis;
  document.querySelectorAll("[data-tab]").forEach((btn) => {
    btn.onclick = () => {
      state.activeTab = btn.dataset.tab;
      render();
    };
  });
  document.querySelectorAll(".history-main").forEach((btn) => {
    btn.onclick = () => loadSession(btn.dataset.session);
  });
  document.querySelectorAll("[data-delete]").forEach((btn) => {
    btn.onclick = (event) => {
      event.stopPropagation();
      deleteSession(btn.dataset.delete);
    };
  });
  const deleteCurrent = $("#delete-current");
  if (deleteCurrent) deleteCurrent.onclick = () => deleteSession(state.sessionId);
  const pdf = $("#pdf");
  if (pdf) pdf.onclick = () => window.open(`${API_BASE}/report/${state.sessionId}/pdf`, "_blank");
  const md = $("#md");
  if (md) md.onclick = () => window.open(`${API_BASE}/report/${state.sessionId}/markdown`, "_blank");
}

async function deleteSession(sessionId) {
  if (!sessionId) return;
  await api(`/history/${sessionId}`, { method: "DELETE" });
  await loadHistory();
  if (state.sessionId === sessionId) {
    resetSession(false);
  } else {
    render();
  }
}

async function clearDatabase() {
  await api("/history", { method: "DELETE" });
  await loadHistory();
  resetSession(false);
}

async function runAnalysis() {
  if (!state.query.trim()) return;
  resetSession(false);
  state.sessionStatus = "running";
  state.startedAt = Date.now();
  state.activeTab = "nodes";
  render();
  try {
    const res = await api("/analyze", { method: "POST", body: JSON.stringify({ query: state.query }) });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || "Analysis failed");
    state.sessionId = data.session_id;
    connectWs(data.session_id);
    startElapsedTimer();
  } catch (err) {
    state.sessionStatus = "failed";
    state.liveLogs.push({ ts: new Date().toISOString(), node: "system", status: "error", detail: err.message });
    render();
  }
}

function connectWs(sessionId) {
  if (state.ws) state.ws.close();
  state.ws = new WebSocket(`ws://localhost:8000/ws/${sessionId}`);
  state.ws.onmessage = (event) => handleWsMessage(JSON.parse(event.data));
  state.ws.onclose = () => {
    if (state.sessionStatus === "running") setTimeout(() => pollStatus(sessionId), 3000);
  };
}

function handleWsMessage(msg) {
  if (msg.type === "current_status") {
    state.nodeStates = msg.nodes || {};
  }
  if (msg.type === "node_update") {
    state.nodeStates[msg.node] = { status: msg.status, detail: msg.detail };
    state.liveLogs.push(msg);
  }
  if (msg.type === "complete") {
    state.sessionStatus = "complete";
    state.ticker = msg.ticker;
    state.confidence = msg.confidence;
    state.riskSignal = msg.risk_signal;
    state.activeTab = "report";
    stopElapsedTimer();
    fetchReport(msg.session_id);
    loadHistory();
  }
  if (msg.type === "error") {
    state.sessionStatus = "failed";
    state.liveLogs.push({ ts: msg.ts, node: "system", status: "error", detail: msg.error });
    stopElapsedTimer();
  }
  render();
}

async function pollStatus(sessionId) {
  const res = await api(`/status/${sessionId}`);
  const data = await res.json();
  if (data.status === "complete") {
    state.sessionStatus = "complete";
    await fetchReport(sessionId);
  } else if (data.status === "running") {
    setTimeout(() => pollStatus(sessionId), 3000);
  }
  render();
}

async function fetchReport(sessionId) {
  const res = await api(`/report/${sessionId}`);
  const data = await res.json();
  state.sessionId = sessionId;
  state.report = data.report_md || "";
  state.ticker = data.ticker || "";
  state.confidence = data.confidence || 0;
  state.riskSignal = data.risk_signal || "";
  state.sectionScores = data.section_scores || {};
  render();
}

async function loadSession(sessionId) {
  state.sessionId = sessionId;
  const [reportRes, nodesRes] = await Promise.all([
    api(`/report/${sessionId}`),
    api(`/nodes/${sessionId}`),
  ]);
  const reportData = await reportRes.json();
  const nodesData = await nodesRes.json();
  state.report = reportData.report_md || "";
  state.sessionStatus = reportData.status || "complete";
  state.ticker = reportData.ticker || "";
  state.confidence = reportData.confidence || 0;
  state.riskSignal = reportData.risk_signal || "";
  state.sectionScores = reportData.section_scores || {};
  state.nodeStates = {};
  state.liveLogs = [];
  (nodesData.nodes || []).forEach((n) => {
    state.nodeStates[n.node_name] = { status: n.status, detail: n.detail };
    state.liveLogs.push({ ts: n.ts, node: n.node_name, status: n.status, detail: n.detail });
  });
  state.activeTab = state.report ? "report" : "nodes";
  render();
}

function resetSession(clearQuery = true) {
  if (state.ws) state.ws.close();
  stopElapsedTimer();
  if (clearQuery) state.query = "";
  state.sessionId = "";
  state.sessionStatus = "idle";
  state.nodeStates = {};
  state.liveLogs = [];
  state.report = "";
  state.ticker = "";
  state.confidence = 0;
  state.riskSignal = "";
  state.sectionScores = {};
  state.activeTab = "nodes";
  render();
}

function startElapsedTimer() {
  stopElapsedTimer();
  state.elapsedTimer = setInterval(render, 1000);
}

function stopElapsedTimer() {
  if (state.elapsedTimer) clearInterval(state.elapsedTimer);
  state.elapsedTimer = null;
}

function elapsedText() {
  if (!state.startedAt) return "";
  const seconds = Math.floor((Date.now() - state.startedAt) / 1000);
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${seconds % 60}s`;
}

function riskClass(signal) {
  const value = String(signal || "").toLowerCase();
  if (value.includes("bull")) return "bull";
  if (value.includes("bear")) return "bear";
  return "neutral";
}

function markdownToHtml(markdown) {
  const lines = cleanMarkdown(markdown).split("\n");
  const html = [];
  let inTable = false;
  let inList = false;

  const closeList = () => {
    if (inList) html.push("</ol>");
    inList = false;
  };
  const closeTable = () => {
    if (inTable) html.push("</tbody></table>");
    inTable = false;
  };

  for (const raw of lines) {
    const line = raw.trim();
    if (!line) {
      closeList();
      closeTable();
      continue;
    }
    const image = line.match(/^!\[(.*?)\]\((.*?)\)$/);
    if (image) {
      closeList();
      closeTable();
      html.push(`<figure><img src="${image[2]}" alt="${escapeHtml(image[1])}" loading="lazy" /><figcaption>${escapeHtml(image[1])}</figcaption></figure>`);
      continue;
    }
    if (line.startsWith("|") && line.endsWith("|")) {
      closeList();
      const cells = line.split("|").slice(1, -1).map((c) => c.trim());
      if (cells.every((c) => /^-+$/.test(c.replaceAll(" ", "")))) continue;
      if (!inTable) {
        html.push("<table><tbody>");
        inTable = true;
      }
      html.push(`<tr>${cells.map((c) => `<td>${inlineMarkdown(c)}</td>`).join("")}</tr>`);
      continue;
    }
    closeTable();
    if (line.startsWith("# ")) {
      closeList();
      html.push(`<h1>${inlineMarkdown(line.slice(2))}</h1>`);
    } else if (line.startsWith("## ")) {
      closeList();
      html.push(`<h2>${inlineMarkdown(line.slice(3))}</h2>`);
    } else if (line.startsWith("### ")) {
      closeList();
      html.push(`<h3>${inlineMarkdown(line.slice(4))}</h3>`);
    } else if (/^\d+\.\s+/.test(line)) {
      if (!inList) {
        html.push("<ol>");
        inList = true;
      }
      html.push(`<li>${inlineMarkdown(line.replace(/^\d+\.\s+/, ""))}</li>`);
    } else {
      closeList();
      html.push(`<p>${inlineMarkdown(line)}</p>`);
    }
  }
  closeList();
  closeTable();
  return html.join("");
}

function cleanMarkdown(markdown) {
  return String(markdown || "")
    .split("\n")
    .filter((line) => {
      const trimmed = line.trim();
      return trimmed !== "---" && !/^[-_]{3,}$/.test(trimmed);
    })
    .join("\n");
}

function inlineMarkdown(text) {
  return escapeHtml(text)
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.*?)\*/g, "<em>$1</em>");
}

loadHistory().finally(render);
