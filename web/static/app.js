"use strict";

const state = {
  schema: null,
  head: null,
  snapshots: [],
  selectedSnapshot: null,
  mode: "visible",
  fileId: "",
  offset: 0,
  limit: 50,
  total: 0,
  conditions: [],
  ops: [],
  jobs: [],
  currentJob: null,
  gcPreview: null,
  crashPoint: "",
};

const $ = (sel) => document.querySelector(sel);
const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "class") node.className = v;
    else if (k === "dataset") Object.assign(node.dataset, v);
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else if (v !== null && v !== undefined) node.setAttribute(k, v);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined) continue;
    node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
  }
  return node;
};

async function api(path, opts = {}) {
  const init = { headers: { "Content-Type": "application/json" }, ...opts };
  if (init.body && typeof init.body !== "string") init.body = JSON.stringify(init.body);
  const res = await fetch(path, init);
  let data = null;
  try { data = await res.json(); } catch (e) { /* empty */ }
  if (!res.ok) {
    const msg = data && (data.message || data.error) || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

let toastTimer = null;
function toast(msg, kind = "") {
  const node = $("#toast");
  node.textContent = msg;
  node.className = `toast show ${kind}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { node.className = "toast"; }, 3600);
}

function fmtTime(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString("zh-CN", { hour12: false });
  } catch (e) { return iso; }
}

function fmtNum(n) {
  return typeof n === "number" ? n.toLocaleString("zh-CN") : String(n ?? "—");
}

function shortId(id) {
  return id ? id.slice(0, 12) : "—";
}

// ---------------------------------------------------------------------------
// Boot & load
// ---------------------------------------------------------------------------

async function boot() {
  $("#btn-refresh").onclick = () => refreshAll();
  $("#btn-query").onclick = () => { state.offset = 0; runQuery(); };
  $("#btn-prev").onclick = () => {
    state.offset = Math.max(0, state.offset - state.limit);
    runQuery();
  };
  $("#btn-next").onclick = () => {
    if (state.offset + state.limit < state.total) {
      state.offset += state.limit;
      runQuery();
    }
  };
  $("#query-snapshot").onchange = async (e) => {
    state.selectedSnapshot = e.target.value || null;
    state.offset = 0;
    renderSnapshots();
    await runQuery();
    await loadDeleteEvents();
  };
  $("#query-mode").onchange = (e) => {
    state.mode = e.target.value;
    state.offset = 0;
    runQuery();
  };
  $("#query-file").onchange = (e) => {
    state.fileId = e.target.value;
    state.offset = 0;
    runQuery();
  };
  $("#query-limit").onchange = (e) => {
    state.limit = parseInt(e.target.value, 10);
    state.offset = 0;
    runQuery();
  };

  $("#btn-add-condition").onclick = () => addCondition();
  const addFilter = document.querySelector("#btn-add-filter");
  if (addFilter) addFilter.onclick = () => {
    addCondition();
    document.querySelector("#conditions").scrollIntoView({ behavior: "smooth", block: "center" });
  };
  $("#btn-delete-preview").onclick = () => doDelete(true);
  $("#btn-delete-apply").onclick = () => doDelete(false);
  $("#btn-append").onclick = () => doAppend();

  $("#btn-merge-plan").onclick = () => planMerge();
  $("#btn-merge-commit").onclick = () => commitMerge();
  $("#btn-merge-abort").onclick = () => abortMerge();
  $("#crash-point").onchange = (e) => { state.crashPoint = e.target.value; };

  $("#btn-gc-preview").onclick = () => previewGc();
  $("#btn-gc-apply").onclick = () => applyGc();
  $("#btn-verify").onclick = () => verifyStore();
  $("#btn-recover").onclick = () => runRecover();

  const health = await api("/api/health");
  if (!health.exists) {
    renderEmpty(health);
    return;
  }
  await refreshAll();
}

function renderEmpty(health) {
  $("#pill-head").textContent = "未初始化";
  $("#pill-root").textContent = health.root;
  $("#snapshot-list").replaceChildren(el("p", { class: "hint" }, "湖表未初始化。运行 .venv/bin/python scripts/init_demo.py 创建示例库，然后点击“刷新”。"));
  if (health.demo_mode) {
    $("#snapshot-list").appendChild(el("button", { class: "btn btn-primary", style: "margin-top:8px", onclick: async () => {
      try { await api("/api/demo/reset", { method: "POST" }); await refreshAll(); toast("示例湖表已创建", "ok"); }
      catch (e) { toast(e.message, "err"); }
    } }, "创建示例湖表"));
  }
  $("#rows-head").replaceChildren();
  $("#rows-body").replaceChildren(el("tr", {}, el("td", { class: "row-idx" }, "还没有数据")));
}

async function refreshAll() {
  try {
    const data = await api("/api/state");
    if (!data.exists) { renderEmpty(data); return; }
    state.schema = data.schema;
    state.head = data.head;
    state.snapshots = data.snapshots;
    state.jobs = data.jobs;
    $("#pill-head").textContent = `head ${data.head}`;
    $("#pill-root").textContent = data.root;
    if (!state.selectedSnapshot) state.selectedSnapshot = data.head;

    const headFiles = await api("/api/files");
    state.headFiles = headFiles.files;
    renderSnapshots();
    renderFileList(headFiles);
    await loadOps();
    await loadDeleteEvents();
    renderJobs();
    fillSnapshotSelectors();
    fillFileSelector();
    fillMergeSelectors();
    await loadJobDetail();
    await runQuery();
  } catch (err) {
    toast(`刷新失败：${err.message}`, "err");
  }
}

async function loadOps() {
  if (state.ops.length) return;
  const data = await api("/api/condition-ops");
  state.ops = data.ops;
  renderConditions();
}

// ---------------------------------------------------------------------------
// Sidebar rendering
// ---------------------------------------------------------------------------

function renderSnapshots() {
  const list = $("#snapshot-list");
  const nodes = [];
  for (const s of [...state.snapshots].reverse()) {
    const selected = s.id === state.selectedSnapshot;
    const node = el(
      "div",
      {
        class: `snapshot ${selected ? "selected" : ""} ${s.is_head ? "is-head" : ""} ${s.expired ? "expired" : ""}`,
        onclick: async () => {
          if (s.expired) { toast("该快照已过期，不再提供查询", "err"); return; }
          state.selectedSnapshot = s.id;
          state.offset = 0;
          $("#query-snapshot").value = s.id;
          renderSnapshots();
          await runQuery();
          await loadDeleteEvents();
        },
      },
      el("div", { class: "snap-top" },
        el("span", { class: "snap-id" }, s.id),
        el("span", { class: "snap-op" }, s.op)),
      el("div", { class: "snap-summary" }, s.summary),
      el("div", { class: "snap-stats" },
        `${fmtNum(s.stats?.raw_rows)} 原始 / ${fmtNum(s.stats?.visible_rows)} 可见 · ${s.stats?.file_count ?? 0} 文件 · ${fmtTime(s.created_at)}`),
      s.is_head ? el("span", { class: "tag tag-ok" }, "HEAD") : null,
      s.expired ? el("span", { class: "tag" }, "已过期") : null,
    );
    nodes.push(node);
  }
  list.replaceChildren(...nodes);
}

async function renderFileList(filesData, isOldSnapshot = false) {
  const nodes = [];
  const currentSnap = filesData.snapshot;
  if (isOldSnapshot) nodes.push(el("p", { class: "hint" }, "正在查看历史版本；合并选择器仍以 head 的文件为准。"));
  else if (filesData.files.some((f) => (f.delete_sets || []).length)) {
    nodes.push(el("p", { class: "hint" },
      "删除来源标记：本版本行内删除（本版本新增的条件删除） vs 合并重映射（由旧文件位置换算而来）。"));
  }
  for (const f of filesData.files) {
    nodes.push(el("div", { class: "file-item" },
      el("div", null,
        el("span", { class: "fid" }, f.id),
        f.rows === 0 ? el("span", { class: "tag tag-neutral" }, "空") : null),
      el("div", { class: "fmeta" },
        `${f.rows} 行 · 删除 ${f.deleted_rows} · 可见 ${f.visible_rows}`),
      el("div", { class: "fsets" },
        ...f.delete_sets.map((d) => {
          const remapped = (d.derived_from || []).length > 0;
          const kind = remapped ? "合并重映射" : (d.source_snapshot === currentSnap ? "本版本删除" : "历史删除");
          return el("span", {
            class: `tag ${remapped ? "tag-remap" : ""}`,
            title: `${d.reason}\n位置: ${(d.position_list || []).join(", ")}\n来源快照: ${d.source_snapshot}`,
          }, `${shortId(d.id)}·${d.positions}p · ${kind}`);
        }))));
  }
  if (!nodes.length) nodes.push(el("p", { class: "hint" }, "该版本没有数据文件"));
  $("#file-list").replaceChildren(...nodes);
}

function fillSnapshotSelectors() {
  const q = $("#query-snapshot");
  const prev = state.selectedSnapshot || state.head;
  q.replaceChildren(...state.snapshots.filter((s) => !s.expired).map((s) =>
    el("option", { value: s.id, selected: s.id === prev ? "selected" : null },
      `${s.id}${s.is_head ? " (head)" : ""} · ${s.op}`)));
  const base = $("#merge-base");
  base.replaceChildren(el("option", { value: "" }, "当前 head"));
  for (const s of state.snapshots.filter((x) => !x.expired).reverse()) {
    base.appendChild(el("option", { value: s.id }, `${s.id} · ${s.op}`));
  }
}

function fillFileSelector() {
  // The file filter follows the *queried* snapshot, not head: when the user
  // time-travels to v2, v4's files must not appear in the dropdown.
  const sel = $("#query-file");
  const files = state.queryFiles || [];
  const prev = state.fileId;
  sel.replaceChildren(el("option", { value: "" }, "全部文件"));
  for (const f of files) {
    sel.appendChild(el("option", { value: f.id }, `${shortId(f.id)} · ${f.rows} 行`));
  }
  const stillPresent = files.some((f) => f.id === prev);
  sel.value = stillPresent ? prev : "";
  if (!stillPresent) state.fileId = "";
}

function fillMergeSelectors() {
  const sel = $("#merge-files");
  sel.replaceChildren();
  for (const f of state.headFiles || []) {
    sel.appendChild(el("option", { value: f.id, selected: "selected" }, `${shortId(f.id)} · ${f.visible_rows} 可见 / ${f.rows} 行`));
  }
}

// ---------------------------------------------------------------------------
// Query & table
// ---------------------------------------------------------------------------

async function runQuery() {
  try {
    const payload = {
      snapshot: state.selectedSnapshot || state.head,
      mode: state.mode,
      conditions: state.conditions.filter((c) => c.column && c.op).map(conditionPayload),
      match: $("#match-mode").value,
      file_id: state.fileId || null,
      offset: state.offset,
      limit: state.limit,
    };
    const res = await api("/api/query", { method: "POST", body: payload });
    state.total = res.total;
    state.queryFiles = res.files.map((f) => ({ ...f }));
    renderTable(res);
    renderFileList({ snapshot: res.snapshot.id, files: res.files.map((f) => ({
      ...f, deleted_rows: f.deleted, visible_rows: f.visible,
      delete_sets: f.delete_sets || [],
    })) }, res.snapshot.id !== state.head);
    fillFileSelector();
    $("#predicate-desc").textContent = `${res.predicate} · 版本 ${res.snapshot.id} · ${res.mode === "raw" ? "原始行" : "可见行"}`;
    const from = res.total === 0 ? 0 : state.offset + 1;
    const to = Math.min(state.offset + res.rows.length, res.total);
    $("#pageinfo").textContent = `${from}–${to} / ${res.total}`;
    $("#btn-prev").disabled = state.offset === 0;
    $("#btn-next").disabled = state.offset + state.limit >= res.total;
  } catch (err) {
    toast(`查询失败：${err.message}`, "err");
  }
}

function renderTable(res) {
  const head = $("#rows-head");
  const body = $("#rows-body");
  const columns = state.schema ? state.schema.fields.map((f) => f.name) : [];
  const headerCells = [
    el("th", { class: "meta-cell" }, "行"),
    el("th", { class: "meta-cell" }, "pos"),
    ...columns.map((c) => el("th", null, c)),
    el("th", { class: "meta-cell" }, "状态"),
    el("th", { class: "meta-cell" }, "文件"),
  ];
  head.replaceChildren(el("tr", null, ...headerCells));
  if (!res.rows.length) {
    body.replaceChildren(el("tr", null, el("td", { colspan: headerCells.length, class: "row-idx" }, "没有匹配的行")));
    return;
  }
  const rows = res.rows.map((row, i) => {
    const meta = row.meta;
    const cells = columns.map((c) => {
      const v = row.data[c];
      if (v === null || v === undefined) return el("td", null, el("span", { class: "cell-null" }, "null"));
      if (typeof v === "boolean") return el("td", null, el("span", { class: "cell-bool" }, v ? "true" : "false"));
      return el("td", null, String(v));
    });
    const statusCell = meta.deleted
      ? el("td", { class: "meta-cell" },
          el("span", { class: "tag", title: meta.delete ? `${meta.delete.reason}\n来源快照 ${meta.delete.source_snapshot}` : "" }, "已删除"))
      : el("td", { class: "meta-cell" }, el("span", { class: "tag tag-ok" }, "可见"));
    return el("tr", { class: meta.deleted ? "deleted" : "" },
      el("td", { class: "row-idx" }, String(state.offset + i + 1)),
      el("td", { class: "meta-cell" }, String(meta.position)),
      ...cells,
      statusCell,
      el("td", { class: "meta-cell", title: meta.file_path },
        shortId(meta.file_id),
        meta.delete ? el("div", { class: "row-idx", title: meta.delete.reason }, shortId(meta.delete.delete_id)) : null));
  });
  body.replaceChildren(...rows);
  renderPredicateChips(res.predicate);
}

function renderPredicateChips(desc) {
  const bar = $("#predicate-bar");
  if (!state.conditions.length) {
    bar.replaceChildren(el("span", { class: "hint" }, state.mode === "raw" ? "显示文件中全部原始行（含已删除）；可在下方“条件构建 / 删除”卡片添加条件" : "显示该版本全部可见行；可在下方“条件构建 / 删除”卡片添加条件"));
    return;
  }
  const nodes = state.conditions.map((c, i) =>
    el("span", { class: "pred-chip" },
      describeCondition(c),
      el("button", { onclick: () => { state.conditions.splice(i, 1); state.offset = 0; renderConditions(); runQuery(); }, title: "移除" }, "×")));
  bar.replaceChildren(el("span", { class: "hint" }, `筛选 ${desc}：`), ...nodes);
}

// ---------------------------------------------------------------------------
// Conditions builder
// ---------------------------------------------------------------------------

function defaultCondition() {
  const field = state.schema?.fields[0];
  return { column: field ? field.name : "", op: "eq", value: "" };
}

function addCondition(c = null) {
  state.conditions.push(c || defaultCondition());
  renderConditions();
}

function describeCondition(c) {
  const op = state.ops.find((o) => o.op === c.op);
  const label = op ? op.label : c.op;
  if (c.op === "is_null" || c.op === "not_null") return `${c.column} ${label}`;
  if (c.op === "between") return `${c.column} ${label} ${c.value} ~ ${c.second}`;
  return `${c.column} ${label} ${Array.isArray(c.value) ? `[${c.value.join(", ")}]` : c.value}`;
}

function conditionPayload(c) {
  const p = { column: c.column, op: c.op };
  const kind = (state.ops.find((o) => o.op === c.op) || {}).value;
  if (kind === "scalar" || kind === "text") {
    p.value = coerceValue(c.value, c.column);
  } else if (kind === "range") {
    p.value = coerceValue(c.value, c.column);
    p.to = coerceValue(c.second, c.column);
  } else if (kind === "list") {
    p.value = String(c.value).split(",").map((x) => coerceValue(x.trim(), c.column)).filter((x) => !(typeof x === "string" && x === ""));
  }
  return p;
}

function coerceValue(raw, column) {
  const field = state.schema?.fields.find((f) => f.name === column);
  const text = typeof raw === "string" ? raw.trim() : raw;
  if (text === "" || text === null || text === undefined) return null;
  if (!field) return text;
  if (field.type === "int64") {
    const n = Number(text);
    return Number.isFinite(n) ? Math.trunc(n) : text;
  }
  if (field.type === "float64") {
    const n = Number(text);
    return Number.isFinite(n) ? n : text;
  }
  if (field.type === "bool") {
    return text === true || text === "true" || text === "1";
  }
  return String(text);
}

function renderConditions() {
  const box = $("#conditions");
  if (!state.conditions.length) {
    box.replaceChildren(el("p", { class: "empty-cond" }, "尚未添加条件。未添加条件时不能执行删除。"));
    return;
  }
  const nodes = state.conditions.map((c, i) => {
    const opMeta = state.ops.find((o) => o.op === c.op) || { value: "scalar" };
    const kind = opMeta.value;
    const colSel = el("select", { class: "c-col", onchange: (e) => { c.column = e.target.value; renderConditions(); } },
      ...(state.schema?.fields || []).map((f) => el("option", { value: f.name, selected: c.column === f.name ? "selected" : null }, `${f.name} (${f.type})`)));
    const opSel = el("select", { class: "c-op", onchange: (e) => { c.op = e.target.value; c.value = ""; c.second = ""; renderConditions(); } },
      ...state.ops.map((o) => el("option", { value: o.op, selected: c.op === o.op ? "selected" : null }, o.label)));
    let valueInput = null;
    if (kind === "scalar" || kind === "text") {
      valueInput = el("input", { class: "c-val", type: "text", value: c.value ?? "", placeholder: "值", oninput: (e) => { c.value = e.target.value; } });
    } else if (kind === "range") {
      valueInput = el("span", { class: "c-val" },
        el("input", { type: "text", value: c.value ?? "", placeholder: "下限", oninput: (e) => { c.value = e.target.value; } }),
        " ~ ",
        el("input", { type: "text", value: c.second ?? "", placeholder: "上限", oninput: (e) => { c.second = e.target.value; } }));
    } else if (kind === "list") {
      valueInput = el("input", { class: "c-val", type: "text", value: c.value ?? "", placeholder: "逗号分隔值", oninput: (e) => { c.value = e.target.value; } });
    }
    return el("div", { class: "condition-row" },
      colSel, opSel, valueInput,
      el("button", { class: "rm", title: "移除", onclick: () => { state.conditions.splice(i, 1); renderConditions(); } }, "×"));
  });
  box.replaceChildren(...nodes);
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

async function doDelete(dryRun) {
  const conditions = state.conditions.filter((c) => c.column && c.op).map(conditionPayload);
  if (!conditions.length) {
    toast("请至少添加一个删除条件", "err");
    return;
  }
  const body = {
    conditions,
    match: $("#match-mode").value,
    reason: $("#delete-reason").value,
    dry_run: dryRun,
    expected_head: state.head,
  };
  const box = $("#delete-result");
  try {
    if (!dryRun && !confirm(`将在当前 head ${state.head} 上按条件删除行（写入独立位置集合）。继续？`)) return;
    const res = await api("/api/delete", { method: "POST", body });
    const preview = res.preview || {};
    const parts = [
      el("div", { class: "ok" }, `匹配 ${fmtNum(preview.would_delete ?? 0)} 行`),
      el("div", null, `条件：${preview.predicate || "—"}`),
      el("div", null, `说明：${preview.reason || "—"}`),
    ];
    if (res.changed) {
      parts.push(el("div", { class: "ok" }, `已提交快照 ${res.snapshot.id}：原始 ${fmtNum(res.snapshot.stats.raw_rows)} 行，删除 ${fmtNum(res.snapshot.stats.deleted_rows)}，可见 ${fmtNum(res.snapshot.stats.visible_rows)}`));
      box.replaceChildren(el("div", { class: "result-box" }, ...parts));
      toast(`删除已提交：${res.snapshot.id}`, "ok");
      state.selectedSnapshot = res.snapshot.id;
      // The shared condition set was just consumed by the delete; clear it so
      // the table shows the whole post-delete version instead of an empty
      // filter over rows that no longer exist.
      state.conditions = [];
      renderConditions();
      await refreshAfterMutation();
    } else {
      parts.push(el("div", null, res.message || "未生成新版本"));
      box.replaceChildren(el("div", { class: "result-box" }, ...parts));
    }
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `删除失败：${err.message}`));
    toast(`删除失败：${err.message}`, "err");
  }
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

function defaultAppendJson() {
  if (!state.schema) return "[]";
  const next = 1 + (state.schema.fields.reduce((m, f) => m, 0));
  const row = {};
  for (const f of state.schema.fields) {
    row[f.name] = f.type === "int64" ? 1000 : f.type === "float64" ? 1.5 : f.type === "bool" ? true : "示例";
  }
  return JSON.stringify([row], null, 2);
}

async function doAppend() {
  const box = $("#append-result");
  let rows;
  try {
    rows = JSON.parse($("#append-json").value);
  } catch (e) {
    box.replaceChildren(el("div", { class: "err" }, `JSON 解析失败：${e.message}`));
    return;
  }
  try {
    const res = await api("/api/append", { method: "POST", body: { rows, expected_head: state.head } });
    box.replaceChildren(el("div", { class: "result-box" },
      el("div", { class: "ok" }, `已写入新文件并提交快照 ${res.snapshot.id}`),
      el("div", null, `新增文件 ${res.file.id} · ${fmtNum(res.file.rows)} 行`),
      el("div", null, `原始 ${fmtNum(res.snapshot.stats.raw_rows)} 行 / 可见 ${fmtNum(res.snapshot.stats.visible_rows)} 行`)));
    state.selectedSnapshot = res.snapshot.id;
    toast(`追加已提交：${res.snapshot.id}`, "ok");
    $("#append-json").value = defaultAppendJson();
    await refreshAfterMutation();
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `追加失败：${err.message}`));
  }
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

function selectedMergeFiles() {
  return [...$("#merge-files").selectedOptions].map((o) => o.value);
}

async function planMerge() {
  const files = selectedMergeFiles();
  if (!files.length) { toast("请选择至少一个文件（或清空选择以合并全部）", "err"); }
  const box = $("#job-detail");
  try {
    const res = await api("/api/merge/plan", { method: "POST", body: {
      file_ids: files.length ? files : null,
      base_snapshot: $("#merge-base").value || null,
      note: $("#merge-note").value,
    }});
    state.currentJob = res.job;
    renderJobDetail(res.job);
    $("#btn-merge-commit").disabled = false;
    $("#btn-merge-abort").disabled = false;
    toast(`任务 ${res.job.job_id} 已准备（基线 ${res.job.base_snapshot}）`, "ok");
    await refreshJobs();
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `准备失败：${err.message}`));
  }
}

async function commitMerge() {
  if (!state.currentJob) return;
  const box = $("#job-detail");
  const crashAt = $("#crash-point").value || null;
  try {
    const res = await api("/api/merge/commit", { method: "POST", body: {
      job_id: state.currentJob.job_id, crash_at: crashAt,
    }});
    state.currentJob = res.job;
    renderJobDetail(res.job, res);
    $("#btn-merge-commit").disabled = true;
    $("#btn-merge-abort").disabled = true;
    // A merge rewrites every position in the lake: follow the new head so the
    // user immediately sees the merged files and their remapped delete sets.
    if (res.snapshot && res.snapshot.id) state.selectedSnapshot = res.snapshot.id;
    if (res.changed) {
      const absorbed = res.absorbed && res.absorbed.length;
      toast(`合并已提交 ${res.snapshot.id}${absorbed ? ` · 吸收 ${absorbed} 个并发删除集` : ""}`, "ok");
    } else {
      toast(res.message || "任务已提交，无需重复执行");
    }
    await refreshAfterMutation();
    await refreshJobs();
  } catch (err) {
    if (err.payload && err.payload.error === "injected_failure") {
      box.replaceChildren(el("div", { class: "result-box" },
        el("div", { class: "err" }, err.message),
        el("div", null, "模拟崩溃已注入。当前 head 未前进；点击左侧“崩溃恢复”可重放提交。")));
      toast("已注入模拟崩溃，服务仍在运行", "err");
      await refreshJobs();
      return;
    }
    box.replaceChildren(el("div", { class: "err" }, `提交失败：${err.message}`));
  }
}

async function abortMerge() {
  if (!state.currentJob) return;
  try {
    const res = await api("/api/merge/abort", { method: "POST", body: {
      job_id: state.currentJob.job_id, reason: "用户在页面中止",
    }});
    state.currentJob = res.job;
    renderJobDetail(res.job);
    $("#btn-merge-commit").disabled = true;
    $("#btn-merge-abort").disabled = true;
    toast(`任务 ${res.job.job_id} 已中止`, "ok");
    await refreshJobs();
  } catch (err) {
    toast(`中止失败：${err.message}`, "err");
  }
}

async function loadJobDetail() {
  if (!state.currentJob) return;
  try {
    state.currentJob = await api(`/api/jobs/${state.currentJob.job_id}`);
    renderJobDetail(state.currentJob);
    $("#btn-merge-commit").disabled = state.currentJob.status !== "prepared" && state.currentJob.status !== "committing" && state.currentJob.status !== "failed";
    $("#btn-merge-abort").disabled = ["committed", "aborted"].includes(state.currentJob.status);
  } catch (e) { /* gone */ }
}

function renderJobDetail(job, res = null) {
  const box = $("#job-detail");
  if (!job) { box.replaceChildren(); return; }
  const nodes = [
    el("div", { class: "result-box" },
      el("div", null, el("strong", null, `任务 ${job.job_id}`), " ", statusBadge(job.status)),
      el("div", { class: "job-meta" },
        `基线 ${job.base_snapshot} · 输入 ${job.input_file_ids.length} 文件 · 输出 ${job.output ? job.output.rows + " 行" : "0 行（全部删除）"}`),
      job.note ? el("div", { class: "job-meta" }, `说明：${job.note}`) : null,
      el("div", { class: "job-meta" }, `暂存 ${job.output ? job.output.staging_path : "—"}`),
      el("div", { class: "job-meta" }, `目标 ${job.output ? job.output.final_path : "—"}`)),
  ];
  for (const attempt of job.attempts || []) {
    const conflict = attempt.conflict || {};
    nodes.push(el("div", { class: "job-attempt" },
      `尝试 @ ${fmtTime(attempt.started_at)} · head ${attempt.head_at_start || "?"} · `,
      el("strong", null, attempt.disposition || "?"),
      conflict.reason ? ` — ${conflict.reason}` : "",
      attempt.committed_snapshot ? ` → ${attempt.committed_snapshot}` : ""));
  }
  if (res && res.absorbed && res.absorbed.length) {
    for (const a of res.absorbed) {
      nodes.push(el("div", { class: "job-attempt" },
        `重映射 ${a.from_delete_set} (${a.from_file}): 旧位置 [${(a.old_positions || []).join(", ")}] → 新位置 [${(a.new_positions || []).join(", ")}]`));
    }
  }
  box.replaceChildren(...nodes);
}

function statusBadge(status) {
  return el("span", { class: `status-badge ${status}` }, status || "?");
}

async function refreshJobs() {
  const data = await api("/api/jobs");
  state.jobs = data.jobs;
  renderJobs();
}

function renderJobs() {
  const box = $("#job-list");
  if (!state.jobs.length) {
    box.replaceChildren(el("p", { class: "hint" }, "还没有合并任务。"));
    return;
  }
  const nodes = state.jobs.slice().reverse().map((j) => {
    const attempts = j.attempts || [];
    const last = attempts[attempts.length - 1] || {};
    return el("div", { class: `job-item status-${j.status}` },
      el("div", { class: "job-top" },
        el("span", { class: "job-id" }, j.job_id),
        statusBadge(j.status),
        el("span", { class: "tag tag-neutral" }, `${j.input_file_ids.length} 文件`),
        j.result && j.result.snapshot ? el("span", { class: "tag tag-ok" }, j.result.snapshot) : null),
      el("div", { class: "job-meta" },
        `基线 ${j.base_snapshot} · ${fmtTime(j.created_at)} · 输出 ${j.output_rows ?? "—"}`),
      last.conflict ? el("div", { class: "job-attempt" }, `最近尝试：${last.disposition || "?"} — ${last.conflict.reason || ""}`) : null,
      j.error ? el("div", { class: "job-attempt" }, `错误：${j.error}`) : null,
      el("div", { class: "btn-row" },
        el("button", { class: "btn btn-small", onclick: async () => {
          state.currentJob = await api(`/api/jobs/${j.job_id}`);
          renderJobDetail(state.currentJob);
          $("#btn-merge-commit").disabled = ["committed", "aborted"].includes(state.currentJob.status);
          $("#btn-merge-abort").disabled = ["committed", "aborted"].includes(state.currentJob.status);
        } }, "查看")));
  });
  box.replaceChildren(...nodes);
}

// ---------------------------------------------------------------------------
// Delete provenance
// ---------------------------------------------------------------------------

async function loadDeleteEvents() {
  const box = $("#delete-events");
  try {
    const snap = await api(`/api/snapshot/${encodeURIComponent(state.selectedSnapshot)}`);
    const events = snap.delete_events || [];
    if (!events.length) {
      box.replaceChildren(el("p", { class: "hint" }, "该版本没有删除记录。"));
      return;
    }
    const originLabel = (e) => {
      if (e.derived_from && e.derived_from.length) {
        return `来源标记：合并重映射（上游删除集 ${e.derived_from.join(", ")}）`;
      }
      return e.source_snapshot === snap.id
        ? "来源标记：本版本行内删除"
        : `来源标记：历史删除（基线 ${e.source_snapshot}）`;
    };
    box.replaceChildren(...events.map((e) =>
      el("div", { class: "delete-event" },
        el("div", { class: "de-head" }, `${e.delete_id} → ${shortId(e.file_id)}`),
        el("div", { class: "de-body" }, `条件：${e.predicate || "—"}`),
        el("div", { class: "de-body" }, `原因：${e.reason || "—"}`),
        el("div", { class: "de-body" },
          `${e.positions} 个位置 [${e.position_list.slice(0, 12).join(", ")}${e.position_list.length > 12 ? ", …" : ""}] · 来源快照 ${e.source_snapshot}`),
        el("div", { class: "de-body" }, originLabel(e)))));
  } catch (e) { /* ignore */ }
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------

async function previewGc() {
  const box = $("#gc-result");
  try {
    const keep = parseInt($("#keep-snapshots").value || "0", 10);
    const res = await api(`/api/gc/preview?keep_snapshots=${keep}`);
    state.gcPreview = res;
    $("#btn-gc-apply").disabled = res.count === 0;
    box.replaceChildren(el("div", { class: "result-box" },
      el("div", null, el("strong", null, "可安全回收范围")),
      el("div", null, `${res.count} 个不可达文件 · ${fmtBytes(res.bytes)}`),
      res.would_expire && res.would_expire.length
        ? el("div", null, `将过期快照：${res.would_expire.join(", ")}`)
        : el("div", null, "不会过期任何快照"),
      el("div", { class: "job-meta" },
        `数据文件 ${res.files.length} · 删除集 ${res.delete_sets.length} · 清单 ${res.manifests.length}`),
      el("div", { class: "hint" }, "预览不修改任何文件。执行回收前请确认这些快照不再需要时间旅行。")));
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `预览失败：${err.message}`));
  }
}

async function applyGc() {
  if (!state.gcPreview || !state.gcPreview.count) return;
  const keep = parseInt($("#keep-snapshots").value || "0", 10);
  if (!confirm("这会永久删除上一步预览的不可达文件，并使旧快照过期。确认执行？")) return;
  try {
    const res = await api("/api/gc/apply", { method: "POST", body: { keep_snapshots: keep, confirm: true } });
    $("#gc-result").replaceChildren(el("div", { class: "result-box" },
      el("div", { class: "ok" }, `已回收 ${res.removed.length} 个文件，释放 ${fmtBytes(res.reclaimed_bytes)}`),
      el("div", null, `过期快照：${(res.expired_now || []).join(", ") || "无"}`)));
    $("#btn-gc-apply").disabled = true;
    state.gcPreview = null;
    await refreshAll();
  } catch (err) {
    toast(`回收失败：${err.message}`, "err");
  }
}

async function verifyStore() {
  const box = $("#verify-result");
  try {
    const res = await api("/api/verify?full=true");
    box.replaceChildren(el("div", { class: res.ok ? "ok" : "err" },
      res.ok ? `校验通过：${res.checked_files} 个文件，${fmtNum(res.row_count)} 原始行，${fmtNum(res.deleted_positions)} 个删除位置`
             : `发现问题（${res.issues.length}）：${res.issues.join("；")}`));
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `校验失败：${err.message}`));
  }
}

async function runRecover() {
  const box = $("#verify-result");
  try {
    const res = await api("/api/recover", { method: "POST" });
    const actions = res.actions.map((a) => a.action + (a.job_id ? `(${a.job_id})` : "") + (a.path ? `(${a.path})` : "")).join(", ") || "无需修复";
    box.replaceChildren(el("div", { class: res.ok ? "ok" : "err" },
      `恢复完成：${actions}${res.errors.length ? "；错误：" + res.errors.map((e) => e.message).join("；") : ""}`));
    toast("崩溃恢复完成", res.ok ? "ok" : "err");
    await refreshAfterMutation();
  } catch (err) {
    box.replaceChildren(el("div", { class: "err" }, `恢复失败：${err.message}`));
  }
}

function fmtBytes(n) {
  if (!n) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB"];
  let i = 0, v = n;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

async function refreshAfterMutation() {
  const data = await api("/api/state");
  state.schema = data.schema;
  state.head = data.head;
  state.snapshots = data.snapshots;
  state.jobs = data.jobs;
  $("#pill-head").textContent = `head ${data.head}`;
  if (!state.snapshots.find((s) => s.id === state.selectedSnapshot && !s.expired)) {
    state.selectedSnapshot = data.head;
  }
  const headFiles = await api("/api/files");
  state.headFiles = headFiles.files;
  renderSnapshots();
  fillSnapshotSelectors();
  fillFileSelector();
  fillMergeSelectors();
  renderJobs();
  state.offset = 0;
  await runQuery();
  await loadDeleteEvents();
  await loadJobDetail();
}

document.addEventListener("DOMContentLoaded", () => {
  boot().then(() => {
    if (state.schema) $("#append-json").value = defaultAppendJson();
  });
});
