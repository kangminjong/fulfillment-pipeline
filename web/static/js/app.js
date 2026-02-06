/**
 * web/static/js/app.js (통합 수정본)
 *
 * ✅ 이 파일의 목표
 * 1) "리스트"는 기존처럼 실시간 조회 (orders/events/alerts table)
 * 2) "차트/통계"는 metrics_window(30분 rollup)에서만 조회
 * 3) 프리셋 선택(기간/단위) 지원:
 *    - 30d_1d, 7d_1d, 24h_1h, 12h_30m
 *
 * ✅ 왜 이렇게 분리하나?
 * - 리스트는 최신성을 중요시(원천 조회)
 * - 차트/통계는 성능/안정성 중요(rollup 조회)
 */

/* -----------------------------
 * DOM 유틸
 * ----------------------------- */
const $ = (id) => document.getElementById(id);

/* -----------------------------
 * Fetch JSON 헬퍼
 * - 에러 메시지를 보기 좋게 던지기
 * ----------------------------- */
async function fetchJson(url, options) {
  const res = await fetch(url, options);
  const text = await res.text();
  if (!res.ok) throw new Error(text || `HTTP ${res.status}`);
  return text ? JSON.parse(text) : {};
}

// KST 라벨 함수
function shortLabelKst(ts) {
  const d = new Date(ts); // ts가 "....+00:00" (UTC)여도 OK

  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(d);

  const get = (t) => parts.find(p => p.type === t)?.value || "00";

  // "02-04 20:00" 형태
  return `${get("month")}-${get("day")} ${get("hour")}:${get("minute")}`;
}

/* -----------------------------
 * 날짜/숫자 포맷 유틸
 * ----------------------------- */
function fmtInt(v) {
  return Number(v ?? 0).toLocaleString();
}

function fmtPct(v) {
  return `${(Number(v ?? 0) * 100).toFixed(1)}%`;
}

function fmtSec(v) {
  const n = Number(v ?? 0);
  if (n >= 60) return `${(n / 60).toFixed(1)}m`;
  return `${n.toFixed(1)}s`;
}

function fmtTs(ts) {
  if (!ts) return "-";
  try {
    return new Date(ts).toLocaleString("ko-KR", { hour12: false });
  } catch {
    return String(ts);
  }
}

/* -----------------------------
 * Footer "updated_at" 표시
 * ----------------------------- */
function setFooterUpdatedAt() {
  const el = $("footer-updated-at");
  if (!el) return;
  el.innerText = `업데이트: ${new Date().toLocaleString("ko-KR", { hour12: false })}`;
}

/* =========================================================
 * ✅ 모달 오픈 방식
 * - base.html에 "숨김 트리거 버튼"이 있으므로
 *   => trigger 버튼 click으로 오픈
 * ========================================================= */
function openModalByTrigger(triggerId) {
  const btn = $(triggerId);
  if (!btn) return;
  btn.click();
}

function closeModalById(modalId) {
  // Tabler는 Bootstrap Modal 기반
  const modalEl = document.getElementById(modalId);
  if (!modalEl) return;
  // window.bootstrap이 없을 수도 있으니 tabler 모달 동작 방식에 의존하지 않고,
  // close는 data-bs-dismiss 버튼을 눌러도 되지만 여기선 안전하게 처리
  const dismissBtn = modalEl.querySelector("[data-bs-dismiss='modal']");
  if (dismissBtn) dismissBtn.click();
}

/* =========================================================
 * ✅ Alert action 모달(ACK/RESOLVE/RETRY)
 * ========================================================= */
function showAlertAction(alertKey, action) {
  $("alert-action-key").value = alertKey;
  $("alert-action-type").value = action;
  $("alert-note").value = "";
  // operator는 사용자 편의로 유지
  openModalByTrigger("trigger-modal-alert-action");
}

function wireAlertModal(onDone) {
  const btn = $("btn-alert-submit");
  if (!btn) return;

  btn.onclick = async () => {
    const alertKey = $("alert-action-key").value;
    const action = $("alert-action-type").value; // ack|resolve|retry
    const operator = $("alert-operator").value || "operator";
    const note = $("alert-note").value || null;

    try {
      await fetchJson(`/api/alerts/${encodeURIComponent(alertKey)}/${action}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ operator, note }),
      });
      closeModalById("modal-alert-action");
      if (onDone) onDone();
    } catch (e) {
      alert(`실패: ${e.message}`);
    }
  };
}

/* =========================================================
 * ✅ Order status 모달(상태 변경)
 * ========================================================= */
function showOrderStatus(orderId) {
  $("order-target-id").value = orderId;
  $("order-note").value = "";
  openModalByTrigger("trigger-modal-order-status");
}

function wireOrderModal(onDone) {
  const btn = $("btn-order-submit");
  if (!btn) return;

  btn.onclick = async () => {
    const orderId = $("order-target-id").value;
    const next_status = $("order-next-status").value;
    const operator = $("order-operator").value || "operator";
    const note = $("order-note").value || null;

    try {
      await fetchJson(`/api/orders/${encodeURIComponent(orderId)}/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ next_status, operator, note }),
      });
      closeModalById("modal-order-status");
      if (onDone) onDone();
    } catch (e) {
      alert(`실패: ${e.message}`);
    }
  };
}

/* =========================================================
 * ✅ Badge helpers (Tabler 스타일)
 * ========================================================= */
function badge(text, colorLt) {
  // Tabler: bg-red-lt, bg-green-lt 등
  return `<span class="badge bg-${colorLt}-lt">${text}</span>`;
}

function badgeStatus(status) {
  const s = (status || "").toUpperCase();
  if (s === "OPEN") return badge("OPEN", "red");
  if (s === "ACK") return badge("ACK", "yellow");
  if (s === "RESOLVED") return badge("RESOLVED", "green");
  if (s === "RETRY_REQUESTED") return badge("RETRY", "blue");
  return badge(s || "-", "secondary");
}

function badgeKind(kind) {
  const k = (kind || "").toUpperCase();
  if (k === "EVENT") return badge("EVENT", "orange");
  if (k === "HOLD") return badge("HOLD", "purple");
  return badge(k || "-", "secondary");
}

function badgeOrderStatus(status) {
  const s = (status || "").toUpperCase().trim();

  const META = {
    CREATED:   { label: "주문생성",          color: "secondary" },
    PAID:      { label: "결제완료",          color: "indigo" },
    PICKING:   { label: "피킹중(상품 준비중)", color: "yellow" },
    PACKED:    { label: "포장완료",          color: "teal" },
    SHIPPED:   { label: "출고/발송완료",      color: "blue" },
    DELIVERED: { label: "배송완료",          color: "green" },
    CANCELED:  { label: "취소",              color: "secondary" },
    CANCELLED: { label: "취소",              color: "secondary" }, 
    HOLD:      { label: "HOLD",     color: "red" },
  };

  const m = META[s];

  // 화면에는 한글 라벨로 표시
  if (m) return badge(m.label, m.color);

  // 모르는 상태는 그대로(혹은 "-" 처리)
  return badge(s || "-", "azure");
}

/* =========================================================
 * ✅ Charts: 페이지별 인스턴스(중복 생성 방지)
 * ========================================================= */
let chartDashboard = null;
let chartOrdersTop = null;
let chartEventsTop = null;
let chartAlertsTop = null;

function destroyChart(chart) {
  if (chart) chart.destroy();
  return null;
}

/**
 * labels를 너무 길게 만들면 보기 힘드니 적당히 축약
 * - ts가 ISO string이면 "MM-DD HH:MM" 정도만 쓰기
 */
function shortLabel(ts) {
  const d = new Date(ts); // ts가 "....+00:00" (UTC)여도 OK

  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(d);

  const get = (t) => parts.find(p => p.type === t)?.value || "00";

  // "02-04 20:00" 형태
  return `${get("month")}-${get("day")} ${get("hour")}:${get("minute")}`
}

/* -----------------------------
 * 프리셋 저장 (페이지별로 따로)
 * ----------------------------- */
function presetKey(page) {
  return `ts_preset:${page}`;
}
function getPreset(page, def) {
  return localStorage.getItem(presetKey(page)) || def;
}
function setPreset(page, v) {
  localStorage.setItem(presetKey(page), v);
}
function wirePresetSelect(page, selectId, defPreset, onChange) {
  const sel = $(selectId);
  if (!sel) return;

  sel.value = getPreset(page, defPreset);
  sel.addEventListener("change", () => {
    setPreset(page, sel.value);
    onChange();
  });
}

/* =========================================================
 * ✅ metrics_window 기반 데이터 로드
 * ========================================================= */
async function loadTimeseries(preset) {
  const qs = new URLSearchParams({ preset }).toString();
  return await fetchJson(`/api/timeseries?${qs}`);
}

/* =========================================================
 * ✅ Dashboard 로드
 * ========================================================= */
async function loadDashboard() {
  // 1) Summary KPI
  const s = await fetchJson("/api/summary");
  $("kpi-orders-today").innerText = fmtInt(s.orders_today);
  $("kpi-shipped-today").innerText = fmtInt(s.shipped_today);
  $("kpi-backlog").innerText = fmtInt(s.backlog);
  $("kpi-holds-now").innerText = fmtInt(s.holds_now);

  // $("kpi-ingest-count").innerText = fmtInt(s.ingest_count);
  // $("kpi-parse-errors").innerText = fmtInt(s.parse_errors);
  // $("kpi-schema-missing").innerText = fmtInt(s.schema_missing);
  // $("kpi-hold-rate").innerText = fmtPct(s.hold_rate);

  // 2) Timeseries chart (preset)
  const preset = getPreset("dashboard", "24h_1h");
  const ts = await loadTimeseries(preset);
  $("chart-updated-at").innerText = fmtTs(s.metrics_created_at);

  const labels = (ts.points || []).map(p => shortLabel(p.ts));
  const orders = (ts.points || []).map(p => Number(p.orders ?? 0));
  const shipped = (ts.points || []).map(p => Number(p.shipped ?? 0));
  const holds = (ts.points || []).map(p => Number(p.holds ?? 0));

  if ($("throughput-chart")) {
    chartDashboard = destroyChart(chartDashboard);
    chartDashboard = new Chart($("throughput-chart"), {
      type: "line",
      data: {
        labels,
        datasets: [
          { label: "orders", data: orders, tension: 0.25, pointRadius: 0 },
          { label: "shipped", data: shipped, tension: 0.25, pointRadius: 0 },
          { label: "holds", data: holds, tension: 0.25, pointRadius: 0 },
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "bottom" } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  // 3) Recent Orders
  const o = await fetchJson("/api/orders?limit=10");
  const tbody = $("orders-tbody");
  tbody.innerHTML = (o.items || []).map(row => {
    const hold = row.hold_reason_code ? badge("Y", "red") : badge("-", "secondary");
    return `
      <tr>
        <td><a href="/orders/${row.order_id}">${row.order_id}</a></td>
        <td>${badgeOrderStatus(row.current_status)}</td>
        <td>${hold}</td>
        <td>${row.last_event_type}</td>
        <td>${fmtTs(row.created_at)}</td>
        <td class="text-end">
          <button class="btn btn-outline-primary btn-sm" data-order="${row.order_id}">
            상태 변경
          </button>
        </td>
      </tr>
    `;
  }).join("");

  // 4) Recent Alerts
  const a = await fetchJson("/api/alerts?limit=10");
  const atbody = $("alerts-tbody");
  atbody.innerHTML = (a.items || []).map(row => {
    return `
      <tr>
        <td>${badgeStatus(row.status)}</td>
        <td><a href="/orders/${row.order_id}">${row.order_id || "-"}</a></td>
        <td>${row.reason || "-"}</td>
        <td>${fmtTs(row.occurred_at)}</td>
        <td class="text-end">
          <button class="btn btn-outline-secondary btn-sm" data-alert="${row.alert_key}" data-action="ack">ACK</button>
          <button class="btn btn-outline-success btn-sm" data-alert="${row.alert_key}" data-action="resolve">해결</button>
          <button class="btn btn-outline-primary btn-sm" data-alert="${row.alert_key}" data-action="retry">재처리</button>
        </td>
      </tr>
    `;
  }).join("");

  $("orders-updated-at").innerText = `rows: ${(o.items || []).length}`;
  setFooterUpdatedAt();

  // 버튼 wiring
  wireOrderActionButtons(tbody, loadDashboard);
  wireAlertActionButtons(atbody, loadDashboard);
}

/* =========================================================
 * ✅ Orders 페이지 로드
 * - KPI: /api/kpi/orders
 * - 차트: timeseries (preset) → 주문/출고(막대) + backlog/holdnow(선)
 * - 테이블: /api/orders/page (필터)
 * ========================================================= */
async function loadOrdersPage() {
  const selPreset = $("ts-preset-orders");
  if (selPreset && !selPreset.dataset.wired) {
    selPreset.dataset.wired = "1";
    selPreset.addEventListener("change", loadOrdersPage);
  }

  // ✅ KPI는 두 군데에서 가져오는 게 자연스러움
  // - summary: "오늘" 주문/출고 (orders_today, shipped_today)
  // - kpi/orders: backlog/holds_now/hold_rate 등 운영스냅샷
 // ✅ (1) 현재 선택된 preset 가져오기
  const preset =
    (selPreset && selPreset.value) ? selPreset.value : getPreset("orders", "24h_1h");
  console.log(preset)
  // ✅ (2) preset 기반 KPI 호출 (오늘/summary 섞지 않음)
  const k = await fetchJson(`/api/kpi/orders?preset=${encodeURIComponent(preset)}`);

  // ✅ 화면 id에 정확히 주입
  $("orders-kpi-updated-at").innerText = fmtTs(k.updated_at);

  $("orders-kpi-orders-today").innerText = fmtInt(k.orders);
  $("orders-kpi-shipped-today").innerText = fmtInt(k.shipped);

  $("orders-kpi-holds-now").innerText = fmtInt(k.holds_now);
  $("orders-kpi-hold-rate").innerText = fmtPct(k.hold_rate);

  const ts = await loadTimeseries(preset);

  const pts = (ts.points || []);
  const labels = pts.map(p => shortLabel(
    p.ts ?? p.bucket ?? p.window_end ?? p.windowEnd ?? p.window_end_ts ?? p.time ?? p.datetime
  ));

  const holds = pts.map(p => Number(p.holds ?? 0));
  const orders = pts.map(p => Number(p.orders ?? p.orders_window ?? 0));
  const shipped = pts.map(p => Number(p.shipped ?? p.shipped_window ?? 0));

  const safeLabels = labels.length ? labels : ["-"];
  const safeOrders = labels.length ? orders : [0];
  const safeShipped = labels.length ? shipped : [0];
  const safeHolds = labels.length ? holds : [0];

  if ($("orders-top-chart")) {
    window.chartOrdersTop = destroyChart(window.chartOrdersTop);

    window.chartOrdersTop = new Chart($("orders-top-chart"), {
      type: "bar",
      data: {
        labels: safeLabels,
        datasets: [
          { label: "orders", data: safeOrders, type: "bar", yAxisID: "y" },
          { label: "shipped", data: safeShipped, type: "bar", yAxisID: "y" },
          { label: "holds", data: safeHolds, type: "line", yAxisID: "y1" },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "bottom" } },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "orders/shipped" } },
          y1: { beginAtZero: true, position: "right", grid: { drawOnChartArea: false }, title: { display: true, text: "holds_now" } },
        },
      },
    });
  }

  // 3) 테이블(필터 적용)
  const status = $("filter-status").value;
  const stage = "ALL";
  const q = $("filter-q").value || "";
  const qs = new URLSearchParams({ status, stage, q, limit: "200" }).toString();
  const data = await fetchJson(`/api/orders/page?${qs}`);

  const tbody = $("orders-page-tbody");
  tbody.innerHTML = (data.items || []).map(row => {
    const hold = row.hold_reason_code ? badge(row.hold_reason_code, "red") : "-";
    return `
      <tr>
        <td><a href="/orders/${row.order_id}">${row.order_id}</a></td>
        <td>${badgeOrderStatus(row.current_status)}</td>
        <td>${hold}</td>
        <td>${row.last_event_type}</td>
        <td>${fmtTs(row.last_occurred_at)}</td>
        <td>${fmtTs(row.created_at)}</td>
        <td class="text-end">
          <button class="btn btn-outline-primary btn-sm" data-order="${row.order_id}">상태 변경</button>
        </td>
      </tr>
    `;
  }).join("");

  $("page-updated-at").innerText = `rows: ${(data.items || []).length}`;
  setFooterUpdatedAt();

  wireOrderActionButtons(tbody, loadOrdersPage);
}



/* =========================================================
 * ✅ Events 페이지 로드
 * ========================================================= */
/* =========================================================
 * ✅ Events 페이지 로드
 * - KPI: /api/kpi/events
 * - 차트: timeseries (preset) → ingest(막대) + errors/missing(선)
 * - 테이블: /api/events/page (필터)
 * ========================================================= */
async function loadEventsPage() {
  // (0) 프리셋 변경 시 즉시 다시 로드
  const selPreset = $("ts-preset-events");
  if (selPreset && !selPreset.dataset.wired) {
    selPreset.dataset.wired = "1";
    selPreset.addEventListener("change", loadEventsPage);
  }

  const preset =
    (selPreset && selPreset.value) ? selPreset.value : getPreset("orders", "24h_1h");
    
  // ✅ (2) preset 기반 KPI 호출 (오늘/summary 섞지 않음)
  const k = await fetchJson(`/api/kpi/events?preset=${encodeURIComponent(preset)}`);

  $("events-kpi-updated-at").innerText = fmtTs(k.created_at);
  $("events-kpi-total").innerText = fmtInt(k.event_total);
  $("events-kpi-errors").innerText = fmtInt(k.event_errors);
  $("events-kpi-delay-p95").innerText = 0;

  // HTML id가 events-kpi-ingest 이므로 여기로 넣어야 함
  // $("events-kpi-ingest").innerText = fmtInt(k.ingest_count);

  // error_rate KPI를 화면에 만들지 않을 거면 이 줄 삭제
  // 만들 거면 아래처럼 "존재할 때만" 세팅
  if ($("events-kpi-error-rate")) {
    $("events-kpi-error-rate").innerText = fmtPct(k.error_rate);
  }

  // 2) 상단 차트(프리셋)
  const ts = await loadTimeseries(preset);

  // (중요) 시간 키/필드 키가 다를 수 있으니 안전 처리
  const pts = (ts.points || []);
  const labels = pts.map(p => shortLabel(p.ts ?? p.bucket ?? p.window_end ?? p.windowEnd ?? p.time ?? p.datetime));

  const ingest = pts.map(p => Number(p.ingest_count ?? p.ingestCount ?? 0));
  const eventTotal = pts.map(p => Number(p.event_total ?? p.eventTotal ?? 0));
  const eventErrors = pts.map(p => Number(p.event_errors ?? p.eventErrors ?? 0));

  const safeLabels = labels.length ? labels : ["-"];
  const safeTotal = labels.length ? eventTotal : [0];
  const safeErrors = labels.length ? eventErrors : [0];

  if ($("events-top-chart")) {
    window.chartEventsTop = destroyChart(window.chartEventsTop);

    window.chartEventsTop = new Chart($("events-top-chart"), {
      type: "bar",
      data: {
        labels: safeLabels,
        datasets: [
          { label: "event_total", data: safeTotal, type: "bar", yAxisID: "y" },
          { label: "event_errors", data: safeErrors, type: "line", yAxisID: "y", tension: 0.25, pointRadius: 0 },
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "bottom" } },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "events/errors" } },
        }
      }
    });
  }

  // 3) 테이블(필터 적용)
  const order_id = $("ev-order-id").value || "";
  const event_type = $("ev-type").value || "";
  const only_error = $("ev-only-error").checked ? "true" : "false";
  const qs = new URLSearchParams({ order_id, event_type, only_error, limit: "300" }).toString();
  const data = await fetchJson(`/api/events/page?${qs}`);

  const tbody = $("events-tbody");
  tbody.innerHTML = (data.items || []).map(row => {
    const reason = row.reason_code ? badge(row.reason_code, "red") : "-";
    return `
      <tr>
        <td>${badgeOrderStatus(row.current_status)}</td>
        <td><a href="/orders/${row.order_id}">${row.order_id || "-"}</a></td>
        <td>${fmtTs(row.occurred_at)}</td>
        <td>${fmtTs(row.ingested_at)}</td>
      </tr>
    `;
  }).join("");

  $("events-updated-at").innerText = `rows: ${(data.items || []).length}`;
  setFooterUpdatedAt();
}


/* =========================================================
 * ✅ Alerts 페이지 로드
 * - KPI: /api/kpi/alerts
 * - 차트: timeseries (preset) → open/resolved
 * - 테이블: /api/alerts/page
 * ========================================================= */
async function loadAlertsPage() {
  // (0) 프리셋 변경 시 즉시 다시 로드
  const selPreset = $("ts-preset-alerts");
  if (selPreset && !selPreset.dataset.wired) {
    selPreset.dataset.wired = "1";
    selPreset.addEventListener("change", loadAlertsPage);
  }

  // 1) 상단 KPI
  const preset =
    (selPreset && selPreset.value) ? selPreset.value : getPreset("orders", "24h_1h");
    
  // ✅ (2) preset 기반 KPI 호출 (오늘/summary 섞지 않음)
  const k = await fetchJson(`/api/kpi/alerts?preset=${encodeURIComponent(preset)}`);

  $("alerts-kpi-updated-at").innerText = fmtTs(k.created_at);
  $("alerts-kpi-open").innerText = fmtInt(k.OPEN);
  $("alerts-kpi-ack").innerText = fmtInt(k.ACK);
  $("alerts-kpi-resolved").innerText = fmtInt(k.RESOLVED);
  $("alerts-kpi-retry").innerText = fmtInt(k.RETRY_REQUESTED);

  // 2) 상단 차트(프리셋)
  const ts = await loadTimeseries(preset);

  const pts = (ts.points || []);
  const labels = pts.map(p => shortLabel(p.ts ?? p.bucket ?? p.window_end ?? p.windowEnd ?? p.time ?? p.datetime));

  // 키가 조금씩 다를 수 있어서 안전 처리
  const open = pts.map(p => Number(p.alerts_open ?? p.open ?? p.OPEN ?? 0));
  const resolved = pts.map(p => Number(p.alerts_resolved ?? p.resolved ?? p.RESOLVED ?? 0));

  const safeLabels = labels.length ? labels : ["-"];
  const safeOpen = labels.length ? open : [0];
  const safeResolved = labels.length ? resolved : [0];

  if ($("alerts-top-chart")) {
    // (중요) chartAlertsTop 미선언으로 죽는 상황 방지
    window.chartAlertsTop = destroyChart(window.chartAlertsTop);

    window.chartAlertsTop = new Chart($("alerts-top-chart"), {
      type: "line",
      data: {
        labels: safeLabels,
        datasets: [
          { label: "alerts_open", data: safeOpen, tension: 0.25, pointRadius: 0 },
          { label: "alerts_resolved", data: safeResolved, tension: 0.25, pointRadius: 0 },
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "bottom" } },
        scales: { y: { beginAtZero: true } }
      }
    });
  }

  // 3) 테이블(필터 적용)
  const status = $("al-status").value;
  const kind = "ALL";
  const qs = new URLSearchParams({ status, kind, limit: "300" }).toString();
  const data = await fetchJson(`/api/alerts/page?${qs}`);

  const tbody = $("alerts-page-tbody");
  tbody.innerHTML = (data.items || []).map(row => {
    return `
      <tr>
        <td>${badgeStatus(row.status)}</td>
        <td><a href="/orders/${row.order_id}">${row.order_id || "-"}</a></td>
        <td>${row.event_id || "-"}</td>
        <td>${row.reason || "-"}</td>
        <td>${fmtTs(row.occurred_at)}</td>
        <td>${row.operator || "-"}</td>
        <td class="text-end">
          <button class="btn btn-outline-secondary btn-sm" data-alert="${row.alert_key}" data-action="ack">ACK</button>
          <button class="btn btn-outline-success btn-sm" data-alert="${row.alert_key}" data-action="resolve">해결</button>
          <button class="btn btn-outline-primary btn-sm" data-alert="${row.alert_key}" data-action="retry">재처리</button>
        </td>
      </tr>
    `;
  }).join("");

  $("alerts-updated-at").innerText = `rows: ${(data.items || []).length}`;
  setFooterUpdatedAt();

  wireAlertActionButtons(tbody, loadAlertsPage);
}


/* =========================================================
 * ✅ Order Detail 페이지 로드
 * - 상단 통계는 요구에 없어서 기존 상세만 유지
 * ========================================================= */
async function loadOrderDetailPage(orderId) {
  const data = await fetchJson(`/api/orders/${encodeURIComponent(orderId)}?events_limit=200&alerts_limit=200`);

  const o = data.order || {};
  $("od-order-id").innerText = o.order_id || "-";
  $("od-stage").innerText = o.product_name || "-";
  $("od-status").innerHTML = badgeOrderStatus(o.current_status) || "-";
  $("od-hold").innerText = o.hold_reason_code || "-";
  $("od-updated").innerText = fmtTs(o.created_at) || "-";
  $("od-tracking").innerText = o.tracking_no || "-";
  

  // related alerts
  const atbody = $("od-alerts-tbody");
  atbody.innerHTML = (data.alerts || []).map(row => `
    <tr>
      <td>${badgeStatus(row.status)}</td>
      <td>${row.event_id || "-"}</td>
      <td>${row.reason || "-"}</td>
      <td>${fmtTs(row.occurred_at)}</td>
      <td class="text-end">
        <button class="btn btn-outline-secondary btn-sm" data-alert="${row.alert_key}" data-action="ack">ACK</button>
        <button class="btn btn-outline-success btn-sm" data-alert="${row.alert_key}" data-action="resolve">해결</button>
        <button class="btn btn-outline-primary btn-sm" data-alert="${row.alert_key}" data-action="retry">재처리</button>
      </td>
    </tr>
  `).join("");

  // recent events
  const etbody = $("od-events-tbody");
  etbody.innerHTML = (data.events || []).map(row => `
    <tr>
      <td>${row.event_id}</td>
      <td>${badgeOrderStatus(row.current_status)}</td>
      <td>${row.reason_code ? badge(row.reason_code, "red") : "-"}</td>
      <td>${fmtTs(row.ingested_at)}</td>
    </tr>
  `).join("");

  // 모달 연결
  $("btn-open-status-modal")?.addEventListener("click", () => showOrderStatus(orderId));
  
  wireAlertActionButtons(atbody, () => loadOrderDetailPage(orderId));
  wireOrderModal(() => loadOrderDetailPage(orderId));
}

/* =========================================================
 * ✅ 테이블 내 버튼 wiring
 * ========================================================= */
function wireOrderActionButtons(container, reloadFn) {
  if (!container) return;
  container.querySelectorAll("button[data-order]").forEach(btn => {
    btn.addEventListener("click", () => {
      const orderId = btn.getAttribute("data-order");
      showOrderStatus(orderId);
    });
  });
  wireOrderModal(reloadFn);
}

function wireAlertActionButtons(container, reloadFn) {
  if (!container) return;
  container.querySelectorAll("button[data-alert]").forEach(btn => {
    btn.addEventListener("click", () => {
      const alertKey = btn.getAttribute("data-alert");
      const action = btn.getAttribute("data-action");
      showAlertAction(alertKey, action);
    });
  });
  wireAlertModal(reloadFn);
}

// -------------------------------------------
// orders 페이지: 프리셋(12h/24h/7d/30d) 바뀌면 timeseries 다시 호출
// -------------------------------------------
async function loadOrdersTopMetricsAndChart() {
  const preset = document.getElementById("ts-preset-orders")?.value || "24h_1h";

  // 예시: preset을 쿼리로 넘긴다고 가정
  // 실제로는 네 백엔드가 받는 파라미터로 맞춰야 함
  const res = await fetch(`/api/timeseries?preset=${encodeURIComponent(preset)}`);
  const data = await res.json();

  // data.points 가 timeseries 결과라고 가정
  renderOrdersTopChart(data.points || []);
}

// 페이지 진입 시 1회 로드 + 프리셋 변경 시 재로드
function wireOrdersTop() {
  const sel = document.getElementById("ts-preset-orders");
  if (sel) sel.addEventListener("change", loadOrdersTopMetricsAndChart);

  loadOrdersTopMetricsAndChart();
}


/* =========================================================
 * ✅ 초기 부팅(페이지별 분기)
 * ========================================================= */
document.addEventListener("DOMContentLoaded", () => {
  const page = document.body?.dataset?.page || "";
  const orderId = document.body?.dataset?.orderId || "";

  // 공통 새로고침 버튼
  $("btn-refresh")?.addEventListener("click", () => {
    if (page === "dashboard") loadDashboard().catch(e => console.warn(e));
    if (page === "orders") loadOrdersPage().catch(e => console.warn(e));
    if (page === "events") loadEventsPage().catch(e => console.warn(e));
    if (page === "alerts") loadAlertsPage().catch(e => console.warn(e));
    if (page === "order_detail") loadOrderDetailPage(orderId).catch(e => console.warn(e));
  });

  // 페이지별 프리셋 wiring + 초기 로드
  if (page === "dashboard") {
    wirePresetSelect("dashboard", "ts-preset-dashboard", "24h_1h", () => loadDashboard().catch(console.warn));
    loadDashboard().catch(console.warn);
  }

  if (page === "orders") {
    wirePresetSelect("orders", "ts-preset-orders", "24h_1h", () => loadOrdersPage().catch(console.warn));
    $("btn-search")?.addEventListener("click", () => loadOrdersPage().catch(console.warn));
    loadOrdersPage().catch(console.warn);
  }

  if (page === "events") {
    wirePresetSelect("events", "ts-preset-events", "24h_1h", () => loadEventsPage().catch(console.warn));
    $("btn-ev-search")?.addEventListener("click", () => loadEventsPage().catch(console.warn));
    loadEventsPage().catch(console.warn);
  }

  if (page === "alerts") {
    wirePresetSelect("alerts", "ts-preset-alerts", "24h_1h", () => loadAlertsPage().catch(console.warn));
    $("btn-al-search")?.addEventListener("click", () => loadAlertsPage().catch(console.warn));
    loadAlertsPage().catch(console.warn);
  }

  if (page === "order_detail") {
    loadOrderDetailPage(orderId).catch(console.warn);
  }
});
