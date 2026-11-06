  // ======================================================
  // PIPELINE RUNS VIEW (ticket #30): lineage graph, run timeline, quality trend.
  // Inlined into app.js's scope by dashboard/build.py; uses its helpers (h, s, card, chartHost...).
  // Every input is optional: a missing one renders "Not supplied" plus the command that makes it.
  // ======================================================
  var P = D.pipeline;
  var patternSeq = 0;

  function notSupplied(what, cmdKey, extra) {
    return h("div", { class: "ext", role: "note" }, [
      h("strong", { text: "Not supplied. " }),
      what + (extra ? " " + extra : ""),
      h("div", { class: "cmd-label", text: "Command that produces it:" }),
      h("code", { class: "cmd", text: P.commands[cmdKey] })
    ]);
  }
  function secs(v) {
    if (v === null || v === undefined) return "";
    if (v < 1) return Math.round(v * 1000) + " ms";
    if (v < 60) return v.toFixed(v < 10 ? 2 : 1) + " s";
    return Math.floor(v / 60) + " min " + Math.round(v % 60) + " s";
  }
  function nn(v) { return v === null || v === undefined ? "n/a" : int(v); }
  function hatchDef(svg, color) {
    var id = "hatch" + (++patternSeq);
    svg.appendChild(s("defs", null, [s("pattern", { id: id, width: 6, height: 6, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" }, [
      s("rect", { width: 6, height: 6, fill: "var(--surface)" }),
      s("line", { x1: 0, y1: 0, x2: 0, y2: 6, stroke: color, "stroke-width": 3 })
    ])]));
    return "url(#" + id + ")";
  }
  function sampleBanner() {
    return P.label ? h("p", { class: "ext sample", role: "note" }, [h("strong", { text: "Sample: " }), P.label + ". Every number below was written by the repository's own CLIs on the synthetic fixture; timestamps and durations are measured on the machine that built this page. Nothing ran on a cloud service."]) : null;
  }

  // ---------- lineage ----------
  var NODE_W = 150, NODE_H = 38, NODE_GAP = 10, LG_W = 1060, LG_PAD = 8;
  function neighbours(L) {
    var up = {}, down = {};
    L.edges.forEach(function (e) {
      (down[e.src] = down[e.src] || []).push(e.dst);
      (up[e.dst] = up[e.dst] || []).push(e.src);
    });
    return { up: up, down: down };
  }
  function nodeLabel(n) { return n.table; }
  function nodeSub(n) {
    var t = n.source === "events" ? "in events" : "declared";
    if (n.quality) t += (n.quality.status === "success" ? " · DQ pass ✓" : " · DQ fail ✕");
    return t;
  }
  function lineageGraphSvg(L) {
    var by = {}, maxN = 0;
    L.stages.forEach(function (st) { by[st] = L.nodes.filter(function (n) { return n.stage === st; }); maxN = Math.max(maxN, by[st].length); });
    var colW = (LG_W - 2 * LG_PAD) / L.stages.length, top = 26;
    var hgt = top + maxN * (NODE_H + NODE_GAP) + 8;
    var svg = svgRoot(LG_W, hgt, "Lineage graph: " + L.nodes.length + " tables across " + L.stages.length + " stages, " + L.edges.length + " edges. A table of every edge follows.");
    svg.setAttribute("class", "lineage-svg");
    var pos = {};
    L.stages.forEach(function (st, ci) {
      var x = LG_PAD + ci * colW + (colW - NODE_W) / 2;
      svg.appendChild(s("text", { x: x, y: 14, class: "lbl", text: L.stage_labels[st] || st }));
      by[st].forEach(function (n, ri) { pos[n.id] = { x: x, y: top + ri * (NODE_H + NODE_GAP) }; });
    });
    var edgeEls = [];
    var gE = s("g", { fill: "none" });
    L.edges.forEach(function (e) {
      var a = pos[e.src], b = pos[e.dst];
      if (!a || !b) return;
      var x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2, mx = (x1 + x2) / 2;
      var obs = e.kind === "observed";
      var p = s("path", { d: "M" + x1 + " " + y1 + " C" + mx + " " + y1 + " " + mx + " " + y2 + " " + x2 + " " + y2, class: "edge " + (obs ? "obs" : "decl"), "data-src": e.src, "data-dst": e.dst });
      edgeEls.push(p); gE.appendChild(p);
    });
    svg.appendChild(gE);
    var nb = neighbours(L);
    function mark(id) {
      var on = {}; on[id] = 1;
      (nb.up[id] || []).concat(nb.down[id] || []).forEach(function (x) { on[x] = 1; });
      edgeEls.forEach(function (p) {
        var hit = p.getAttribute("data-src") === id || p.getAttribute("data-dst") === id;
        p.setAttribute("class", p.getAttribute("class").replace(/ (hl|dim)/g, "") + (hit ? " hl" : " dim"));
      });
      svg.querySelectorAll("g.node").forEach(function (g) { g.classList.toggle("dim", !on[g.getAttribute("data-id")]); });
    }
    function clear() {
      edgeEls.forEach(function (p) { p.setAttribute("class", p.getAttribute("class").replace(/ (hl|dim)/g, "")); });
      svg.querySelectorAll("g.node").forEach(function (g) { g.classList.remove("dim"); });
    }
    L.nodes.forEach(function (n) {
      var p = pos[n.id];
      var ups = (nb.up[n.id] || []).join(", ") || "none", downs = (nb.down[n.id] || []).join(", ") || "none";
      var g = s("g", { class: "node " + (n.source === "events" ? "ev" : "decl"), "data-id": n.id, tabindex: 0, role: "group", "aria-label": n.id + ", " + nodeSub(n) + ". Fed by: " + ups + ". Feeds: " + downs + "." }, [
        s("rect", { x: p.x, y: p.y, width: NODE_W, height: NODE_H, rx: 6 }),
        s("text", { x: p.x + 8, y: p.y + 16, class: "lbl", text: nodeLabel(n) }),
        s("text", { x: p.x + 8, y: p.y + 30, class: "sub", text: nodeSub(n) })
      ]);
      g.addEventListener("focus", function () { mark(n.id); });
      g.addEventListener("blur", clear);
      g.addEventListener("pointerenter", function () { mark(n.id); });
      g.addEventListener("pointerleave", clear);
      svg.appendChild(g);
    });
    return svg;
  }
  function lineageList(L) {
    var nb = neighbours(L);
    return h("div", { class: "lg-list" }, L.stages.map(function (st) {
      return h("section", { "aria-label": L.stage_labels[st] || st }, [
        h("h4", { text: L.stage_labels[st] || st }),
        h("ul", null, L.nodes.filter(function (n) { return n.stage === st; }).map(function (n) {
          var ups = nb.up[n.id] || [];
          return h("li", null, [
            h("strong", { text: n.table }), h("span", { class: "sub", text: " · " + nodeSub(n) }),
            ups.length ? h("div", { class: "from", text: "← from " + ups.join(", ") }) : null
          ]);
        }))
      ]);
    }));
  }
  function columnPanel(L) {
    var tables = Object.keys(L.columns);
    var cur = tables.indexOf("fact_claim") >= 0 ? "fact_claim" : tables[0];
    var wrap = h("div", { class: "colpanel" });
    var tabs = h("div", { class: "tabs", role: "group", "aria-label": "Gold table" });
    var body = h("div", { class: "colbody" });
    var detail = h("p", { class: "col-detail", "aria-live": "polite" });
    var sel = null;
    function srcKey(x) { return x.layer + "." + x.table + "." + x.column; }
    function show(table) {
      cur = table; sel = null;
      Array.prototype.forEach.call(tabs.children, function (b) { b.setAttribute("aria-pressed", b.getAttribute("data-t") === table ? "true" : "false"); });
      body.textContent = "";
      var cols = L.columns[table], srcCols = {}, order = [];
      cols.forEach(function (c) { c.sources.concat(c.indirect).forEach(function (x) { if (!srcCols[srcKey(x)]) { srcCols[srcKey(x)] = x; order.push(srcKey(x)); } }); });
      order.sort();
      var left = h("ul", { class: "cols-left", "aria-label": "Columns of gold." + table }, cols.map(function (c) {
        var b = h("button", { type: "button", class: "colbtn", "data-c": c.name, "aria-pressed": "false" }, [c.name, c.derived ? h("span", { class: "tag", text: " derived" }) : null, c.masking ? h("span", { class: "tag", text: " masked" }) : null]);
        b.addEventListener("click", function () { pick(c.name); });
        b.addEventListener("focus", function () { pick(c.name); });
        return h("li", null, [b]);
      }));
      var groups = {}, gOrder = [];
      order.forEach(function (k) { var x = srcCols[k]; if (!groups[x.table]) { groups[x.table] = []; gOrder.push(x.table); } groups[x.table].push(x); });
      var right = h("div", { class: "cols-right", "aria-label": "Source columns" }, gOrder.length ? gOrder.map(function (t) {
        return h("section", null, [h("h4", { text: "silver." + t }), h("ul", null, groups[t].map(function (x) {
          return h("li", { class: "srccol", "data-k": srcKey(x) }, [h("span", { class: "mark", "aria-hidden": "true", text: "" }), x.column, h("span", { class: "why" })]);
        }))]);
      }) : [h("p", { class: "note", text: "This table has no source columns in the map (all derived)." })]);
      var svg = s("svg", { class: "connectors", "aria-hidden": "true", focusable: "false" });
      body.appendChild(svg); body.appendChild(left); body.appendChild(right);
      detail.textContent = "Select or focus a column to see where it comes from.";
      body._cols = cols;
    }
    function pick(name) {
      sel = name;
      var c = body._cols.filter(function (x) { return x.name === name; })[0];
      Array.prototype.forEach.call(body.querySelectorAll(".colbtn"), function (b) { b.setAttribute("aria-pressed", b.getAttribute("data-c") === name ? "true" : "false"); });
      var direct = {}, ind = {};
      c.sources.forEach(function (x) { direct[srcKey(x)] = 1; });
      c.indirect.forEach(function (x) { ind[srcKey(x)] = 1; });
      Array.prototype.forEach.call(body.querySelectorAll(".srccol"), function (li) {
        var k = li.getAttribute("data-k"), d = direct[k], i = !d && ind[k];
        li.className = "srccol" + (d ? " hl" : i ? " hl ind" : "");
        li.querySelector(".mark").textContent = d ? "◀ " : i ? "◁ " : "";
        li.querySelector(".why").textContent = d ? " (source)" : i ? " (join or filter input)" : "";
      });
      var parts = [];
      if (c.sources.length) parts.push("gold." + cur + "." + c.name + " comes from " + c.sources.map(function (x) { return "silver." + x.table + "." + x.column; }).join(", ") + " (" + c.transformation.toLowerCase() + (c.masking ? ", masked: the value is a keyed token" : "") + ").");
      else parts.push("gold." + cur + "." + c.name + " has no source column: " + (c.derived || "derived") + ".");
      if (c.indirect.length) parts.push("Also influenced by " + c.indirect.map(function (x) { return "silver." + x.table + "." + x.column; }).join(", ") + ".");
      if (c.description) parts.push(c.description + ".");
      detail.textContent = parts.join(" ");
      connect();
    }
    function connect() {
      var svg = body.querySelector(".connectors");
      svg.textContent = "";
      var narrow = body.clientWidth < 560;
      svg.style.display = narrow ? "none" : "block";
      if (narrow || !sel) return;
      var box = body.getBoundingClientRect(), btn = body.querySelector('.colbtn[data-c="' + sel + '"]').getBoundingClientRect();
      svg.setAttribute("width", box.width); svg.setAttribute("height", box.height);
      Array.prototype.forEach.call(body.querySelectorAll(".srccol.hl"), function (li) {
        var r = li.getBoundingClientRect();
        var x1 = btn.right - box.left, y1 = btn.top + btn.height / 2 - box.top, x2 = r.left - box.left, y2 = r.top + r.height / 2 - box.top, mx = (x1 + x2) / 2;
        svg.appendChild(s("path", { d: "M" + x1 + " " + y1 + " C" + mx + " " + y1 + " " + mx + " " + y2 + " " + x2 + " " + y2, class: "conn" + (li.classList.contains("ind") ? " ind" : "") }));
      });
    }
    tables.forEach(function (t) {
      var b = h("button", { type: "button", "data-t": t, "aria-pressed": "false", text: t });
      b.addEventListener("click", function () { show(t); });
      tabs.appendChild(b);
    });
    wrap.appendChild(tabs); wrap.appendChild(detail); wrap.appendChild(body);
    show(cur);
    if (typeof ResizeObserver === "function") new ResizeObserver(function () { if (sel) connect(); }).observe(body);
    redraws.push(function () { if (sel) connect(); });
    return wrap;
  }
  function renderLineage() {
    var L = P.lineage, root = h("div");
    if (!L) {
      root.appendChild(notSupplied("No OpenLineage events and no column-lineage map were passed, so there is no graph to draw.", "lineage_events"));
      root.appendChild(h("div", { class: "spacer" }));
      root.appendChild(notSupplied("Column-level drill-down needs the column map.", "column_lineage"));
      return root;
    }
    root.appendChild(h("p", { class: "lede", text: "Each box is a table. An arrow means a job read the table on the left to write the table on the right. Solid arrows were recorded in OpenLineage events from real runs. Dashed arrows are declared in files in this repository (the gold column map, the dbt models, the dashboard's SQL), because no job emitted an event for them." }));
    root.appendChild(legend([{ name: "Observed in an OpenLineage event", color: "var(--series-1)" }, { name: "Declared in a repository file", color: "var(--muted)", dash: true }]));
    if (!L.events_supplied) root.appendChild(notSupplied("No OpenLineage events were passed; only declared edges are shown.", "lineage_events"));
    var hostCard = card("Table-level lineage", L.events_supplied ? L.event_count + " events, " + L.runs.length + " runs, " + L.nodes.length + " tables, " + L.edges.length + " edges. Hover or focus (Tab) a table to highlight what feeds it and what it feeds." : null, [
      chartHost(function (w) { return w < 860 ? lineageList(L) : lineageGraphSvg(L); })
    ]);
    hostCard.appendChild(dataTable("Every edge of the lineage graph", [
      { label: "From", key: "src" }, { label: "To", key: "dst" }, { label: "Job", key: "job", cls: "l" },
      { label: "Evidence", key: "kind", cls: "l", fmt: function (v) { return v === "observed" ? "OpenLineage event" : "declared in repo file"; } },
      { label: "Run status", key: "status", cls: "l", fmt: function (v) { return v || "n/a"; } }
    ], L.edges));
    root.appendChild(hostCard);
    if (L.columns) {
      root.appendChild(h("h3", { class: "sub-h", id: "pv-columns", text: "Column lineage: where a gold column comes from" }));
      root.appendChild(h("p", { class: "lede", text: "Pick a gold table, then click or Tab to a column. Its source columns in silver light up. ◀ marks a source the value is computed from; ◁ marks a join or filter input that shapes the value without being in it. Derived columns (batch ids, SCD2 dates, calendar) have no source column. This map is hand-written and reviewed (lineage/column_lineage.yaml); it describes the code, not a Spark plan." }));
      root.appendChild(card("Column lineage", null, [columnPanel(L)]));
      var rows = [];
      Object.keys(L.columns).forEach(function (t) { L.columns[t].forEach(function (c) { rows.push({ table: t, column: c.name, sources: c.sources.map(function (x) { return x.table + "." + x.column; }).join(", ") || "(derived) " + (c.derived || ""), how: c.transformation || "", masked: c.masking ? "yes" : "", indirect: c.indirect.map(function (x) { return x.table + "." + x.column; }).join(", ") }); }); });
      root.appendChild(dataTable("Every gold column and its silver sources", [
        { label: "Gold table", key: "table" }, { label: "Column", key: "column", cls: "l" }, { label: "Silver sources", key: "sources", cls: "l" },
        { label: "Transformation", key: "how", cls: "l" }, { label: "Masked", key: "masked", cls: "l" }, { label: "Join or filter inputs", key: "indirect", cls: "l" }
      ], rows));
    } else {
      root.appendChild(h("div", { class: "spacer" }));
      root.appendChild(notSupplied("Column-level drill-down needs the column map.", "column_lineage"));
    }
    if (L.nodes.some(function (n) { return n.stage === "silver"; }) && !L.nodes.some(function (n) { return n.stage === "silver" && n.source === "events"; }))
      root.appendChild(h("p", { class: "note", text: "Silver and bronze tables appear as declared only: the fixture builds gold straight from the landing files, so no bronze or silver job ran here." }));
    return root;
  }

  // ---------- run timeline ----------
  var LAYER_LABEL = { bronze: "Bronze", silver: "Silver", gold: "Gold", streaming: "Streaming", quality: "Quality", other: "Landing and other" };
  function runTip(r) {
    return [{ value: secs(r.duration_s), label: "duration" }, { value: nn(r.rows_in) + " / " + nn(r.rows_out) + " / " + nn(r.rows_quarantined), label: "rows in / out / quarantined" }, { value: String(r.retries), label: "retries" }].concat(r.alerts.map(function (a) { return { value: a.severity, label: a.rule }; }));
  }
  function statusWord(r) { return r.status === "success" ? "Success" : "Failed"; }
  function ganttSvg(T, w) {
    var first = {};
    T.runs.forEach(function (r) { if (first[r.layer] === undefined || r.offset_s < first[r.layer]) first[r.layer] = r.offset_s; });
    var runs = T.runs.slice().sort(function (a, b) { return first[a.layer] - first[b.layer] || (a.layer < b.layer ? -1 : a.layer > b.layer ? 1 : 0) || a.offset_s - b.offset_s; });
    var left = 150, right = 150, rowH = 40, top = 22, bot = 30;
    var plotW = Math.max(120, w - left - right), span = Math.max(T.span_s, 0.001);
    var hgt = top + runs.length * rowH + bot;
    var svg = svgRoot(w, hgt, "Run timeline: " + runs.length + " runs over " + secs(T.span_s) + ". A table with every run follows.");
    var hatch = hatchDef(svg, "var(--series-2)");
    var ax = axis(span, 4);
    ticks(0, ax.max, 4).forEach(function (t) {
      var x = left + t / ax.max * plotW;
      svg.appendChild(s("line", { x1: x, x2: x, y1: top - 4, y2: hgt - bot + 4, class: "grid-line" }));
      svg.appendChild(s("text", { x: x, y: hgt - 10, "text-anchor": "middle", text: secs(t) }));
    });
    var lastLayer = null;
    runs.forEach(function (r, i) {
      var y = top + i * rowH, bw = Math.max(8, r.duration_s / ax.max * plotW), x = left + r.offset_s / ax.max * plotW, ok = r.status === "success";
      if (r.layer !== lastLayer) { svg.appendChild(s("text", { x: 4, y: y + 15, class: "lbl", text: LAYER_LABEL[r.layer] || r.layer })); lastLayer = r.layer; }
      svg.appendChild(s("text", { x: 4, y: y + 29, text: r.job + (r.batch_id ? " · " + r.batch_id : "") }));
      var g = s("g", { class: "run " + (ok ? "ok" : "bad"), tabindex: 0, role: "img", "aria-label": (LAYER_LABEL[r.layer] || r.layer) + " " + r.job + ", batch " + (r.batch_id || "none") + ": " + statusWord(r) + ", " + secs(r.duration_s) + ", rows in " + nn(r.rows_in) + ", out " + nn(r.rows_out) + ", quarantined " + nn(r.rows_quarantined) + ", retries " + r.retries + (r.alerts.length ? ", alerts: " + r.alerts.map(function (a) { return a.severity + " " + a.rule; }).join("; ") : "") });
      g.appendChild(s("rect", { x: x, y: y + 8, width: bw, height: rowH - 18, rx: 4, fill: ok ? "var(--series-1)" : hatch, stroke: ok ? "var(--series-1)" : "var(--series-2)", "stroke-width": 1.5 }));
      var glyph = ok ? "✓" : "✕";
      var lx = x + bw + 6;
      g.appendChild(s("text", { x: lx, y: y + 23, class: "lbl", text: glyph + " " + statusWord(r) + " · " + secs(r.duration_s) }));
      if (r.alerts.length) {
        var worst = r.alerts.some(function (a) { return a.severity === "critical"; });
        var ax0 = Math.min(w - 14, lx + 8 + (glyph + " " + statusWord(r) + " \u00b7 " + secs(r.duration_s)).length * 6.4 + 10);
        g.appendChild(s("path", { d: "M" + ax0 + " " + (y + 12) + " l7 7 l-7 7 l-7 -7 z", class: "alert-mark" + (worst ? " crit" : "") }));
        g.appendChild(s("text", { x: ax0, y: y + 23, "text-anchor": "middle", class: "alert-glyph", text: "!" }));
      }
      var tip = function (ev) { var b = g.getBoundingClientRect(); showTip(ev && ev.clientX ? ev.clientX : b.right, ev && ev.clientY ? ev.clientY : b.top, r.job + " · " + statusWord(r), runTip(r)); };
      g.addEventListener("pointermove", tip); g.addEventListener("focus", function () { tip(null); });
      g.addEventListener("pointerleave", hideTip); g.addEventListener("blur", hideTip);
      svg.appendChild(g);
    });
    return svg;
  }
  function runList(T) {
    var span = Math.max(T.span_s, 0.001);
    return h("ul", { class: "run-list" }, T.runs.map(function (r) {
      var ok = r.status === "success";
      var bar = h("span", { class: "mini", "aria-hidden": "true" }, [h("b", { class: ok ? "ok" : "bad", style: "margin-left:" + (r.offset_s / span * 100).toFixed(1) + "%;width:" + Math.max(2, r.duration_s / span * 100).toFixed(1) + "%" })]);
      return h("li", null, [
        h("div", { class: "rl-head" }, [h("strong", { text: (LAYER_LABEL[r.layer] || r.layer) + " · " + r.job }), statusEl(ok ? "pass" : "fail", statusWord(r))]),
        bar,
        h("div", { class: "rl-meta", text: secs(r.duration_s) + " · rows in " + nn(r.rows_in) + ", out " + nn(r.rows_out) + ", quarantined " + nn(r.rows_quarantined) + " · retries " + r.retries + (r.batch_id ? " · batch " + r.batch_id : "") }),
        r.alerts.length ? h("div", { class: "rl-alert", text: "◆ Alert: " + r.alerts.map(function (a) { return a.severity + " " + a.rule; }).join("; ") }) : null
      ]);
    }));
  }
  function renderTimeline() {
    var T = P.timeline, root = h("div");
    if (!T) { root.appendChild(notSupplied("No run ledger was passed, so there is no timeline.", "runs")); return root; }
    if (!T.runs.length) { root.appendChild(h("p", { class: "ext", text: "The run ledger was supplied but holds no runs." })); return root; }
    root.appendChild(h("p", { class: "lede", text: "Each bar is one run of one pipeline step, positioned by when it started and sized by how long it took. A tick means it succeeded; a cross and hatched bar mean it failed (never colour alone). A diamond marks a run with a freshness, volume or failure alert. Runs are grouped by layer; the table gives every number." }));
    root.appendChild(legend([{ name: "✓ Success", color: "var(--series-1)", box: true }, { name: "✕ Failed (hatched)", color: "var(--series-2)", box: true }, { name: "◆ Alert", color: "var(--crit-text)", box: true }]));
    var c = card("Run timeline", "Time since the first run started: " + secs(T.span_s) + " across " + T.runs.length + " runs in " + T.layers.length + " layer" + (T.layers.length === 1 ? "" : "s") + ". Layers with no runs in this ledger are not drawn.", [chartHost(function (w) { return w < 640 ? runList(T) : ganttSvg(T, w); })]);
    c.appendChild(dataTableOpen("Every run in the ledger (run ledger rows, in start order)", [
      { label: "Layer", key: "layer", cls: "l" }, { label: "Job", key: "job", cls: "l" }, { label: "Table", key: "table", cls: "l" }, { label: "Batch", key: "batch_id", cls: "l", fmt: function (v) { return v || ""; } },
      { label: "Status", key: "status", cls: "l", fmt: function (v) { return v === "success" ? "✓ success" : "✕ failed"; } },
      { label: "Start (s)", key: "offset_s", fmt: function (v) { return v.toFixed(2); } }, { label: "Duration (s)", key: "duration_s", fmt: function (v) { return v.toFixed(2); } },
      { label: "Rows in", key: "rows_in", fmt: nn }, { label: "Rows out", key: "rows_out", fmt: nn }, { label: "Quarantined", key: "rows_quarantined", fmt: nn },
      { label: "Retries", key: "retries" }, { label: "Alerts", key: "alerts", fmt: function (v) { return v.length; } }
    ], T.runs));
    c.appendChild(h("p", { class: "note", text: "Retries = earlier ledger rows with the same job, table and batch id (the ledger has no retry column). Start is seconds after the first run in the ledger started." }));
    root.appendChild(c);
    root.appendChild(h("h3", { class: "sub-h", text: "Alerts" }));
    if (T.alerts === null) root.appendChild(notSupplied("No alerts file was passed, so alerts are not marked on the timeline.", "alerts"));
    else if (!T.alerts.length) root.appendChild(h("p", { class: "ext", text: "No alert fired: the freshness, volume and failure rules were evaluated over this ledger and none was breached." }));
    else root.appendChild(card("Alerts evaluated over this ledger", "From alerts.jsonl. Rule meanings are in docs/observability.md.", [
      h("div", { class: "tbl-wrap" }, [h("table", null, [h("caption", { text: "Alerts" }),
        h("thead", null, [h("tr", null, ["Severity", "Rule", "Table", "Message"].map(function (t) { return h("th", { scope: "col", class: "l", text: t }); }))]),
        h("tbody", null, T.alerts.map(function (a) { return h("tr", null, [h("td", { class: "l" }, [statusEl(a.severity === "critical" ? "fail" : "na", a.severity)]), h("td", { class: "l", text: a.rule }), h("td", { class: "l", text: a.table }), h("td", { class: "wrap-cell l", text: a.message })]); }))])])
    ]));
    return root;
  }

  // ---------- quality trend ----------
  function trendSvg(rows, w) {
    var left = 34, bot = 44, top = 22, plotH = 110, hgt = top + plotH + bot;
    var maxV = Math.max.apply(null, rows.map(function (r) { return r.evaluated; })) || 1, ax = axis(maxV, 3);
    var svg = svgRoot(w, hgt, "Expectations per run: " + rows.map(function (r) { return r.label + " " + r.passed + " passed, " + (r.evaluated - r.passed) + " failed"; }).join("; ") + ". A table follows.");
    var hatch = hatchDef(svg, "var(--series-2)");
    ticks(0, ax.max, 3).forEach(function (t) {
      var y = top + plotH - t / ax.max * plotH;
      svg.appendChild(s("line", { x1: left, x2: w - 4, y1: y, y2: y, class: "grid-line" }));
      svg.appendChild(s("text", { x: left - 4, y: y + 4, "text-anchor": "end", text: int(t) }));
    });
    var slot = (w - left - 4) / rows.length, bw = Math.min(46, slot * 0.6);
    rows.forEach(function (r, i) {
      var cx = left + slot * (i + 0.5), x = cx - bw / 2, failed = r.evaluated - r.passed;
      var hp = r.passed / ax.max * plotH, hf = failed / ax.max * plotH;
      var g = s("g", { tabindex: 0, role: "img", "aria-label": r.label + ": " + r.passed + " of " + r.evaluated + " expectations passed, " + failed + " failed" + (r.tables_missing ? ", " + r.tables_missing + " required tables missing" : "") });
      g.appendChild(s("rect", { x: x, y: top + plotH - hp, width: bw, height: Math.max(hp, 0.5), fill: "var(--series-1)" }));
      if (failed) g.appendChild(s("rect", { x: x, y: top + plotH - hp - hf, width: bw, height: Math.max(hf, 2), fill: hatch, stroke: "var(--series-2)", "stroke-width": 1.5 }));
      g.appendChild(s("text", { x: cx, y: top + plotH - hp - hf - 5, "text-anchor": "middle", class: "lbl", text: failed ? "✕ " + failed + " failed" : "✓ all pass" }));
      g.appendChild(s("text", { x: cx, y: top + plotH + 14, "text-anchor": "middle", text: r.label.length > 12 ? r.label.slice(0, 11) + "…" : r.label }));
      g.appendChild(s("text", { x: cx, y: top + plotH + 28, "text-anchor": "middle", text: r.passed + " / " + r.evaluated }));
      var tip = function (ev) { var b = g.getBoundingClientRect(); showTip(ev && ev.clientX ? ev.clientX : b.right, ev && ev.clientY ? ev.clientY : b.top, r.label + " · " + r.generated_at.slice(0, 19).replace("T", " ") + " UTC", [{ value: String(r.passed), label: "passed" }, { value: String(r.failed_blocking), label: "failed, blocking" }, { value: String(r.failed_warn), label: "failed, warn" }, { value: String(r.tables_missing), label: "required tables missing" }]); };
      g.addEventListener("pointermove", tip); g.addEventListener("focus", function () { tip(null); });
      g.addEventListener("pointerleave", hideTip); g.addEventListener("blur", hideTip);
      svg.appendChild(g);
    });
    svg.appendChild(s("line", { x1: left, x2: w - 4, y1: top + plotH, y2: top + plotH, class: "axis-line" }));
    return svg;
  }
  function renderTrend() {
    var Tr = P.trend, root = h("div");
    if (!Tr) { root.appendChild(notSupplied("No Great Expectations result files were passed, so there is no trend.", "quality_history")); return root; }
    root.appendChild(h("p", { class: "lede", text: "Each bar is one run of the Great Expectations checks on one layer: expectations that passed (solid) and failed (hatched, with a cross and a count). One small chart per layer so each keeps its own scale. A layer with no result files says so rather than showing zero." }));
    root.appendChild(legend([{ name: "✓ Passed", color: "var(--series-1)", box: true }, { name: "✕ Failed (hatched)", color: "var(--series-2)", box: true }]));
    var grid = h("div", { class: "grid three" });
    Tr.order.forEach(function (layer) {
      var rows = Tr.layers[layer];
      if (!rows.length) {
        grid.appendChild(card(LAYER_LABEL[layer] + " layer", null, [notSupplied("No " + layer + " result was passed.", "quality_history")]));
        return;
      }
      var c = card(LAYER_LABEL[layer] + " layer", rows.length + " run" + (rows.length === 1 ? "" : "s") + ", oldest to newest");
      c.appendChild(chartHost(function (w) { return trendSvg(rows, w); }));
      c.appendChild(dataTable(LAYER_LABEL[layer] + " layer quality runs", [
        { label: "Run", key: "label" }, { label: "Generated (UTC)", key: "generated_at", cls: "l", fmt: function (v) { return v.slice(0, 19).replace("T", " "); } },
        { label: "Evaluated", key: "evaluated", fmt: int }, { label: "Passed", key: "passed", fmt: int }, { label: "Failed blocking", key: "failed_blocking", fmt: int },
        { label: "Failed warn", key: "failed_warn", fmt: int }, { label: "Tables missing", key: "tables_missing", fmt: int },
        { label: "Result", key: "success", cls: "l", fmt: function (v) { return v ? "✓ no blocking failure" : "✕ blocking failure"; } }
      ], rows));
      grid.appendChild(c);
    });
    root.appendChild(grid);
    root.appendChild(h("p", { class: "note", text: "A required table that is missing counts as a blocking failure in the runner but has no expectations, so it shows in the table column, not in the bars." }));
    return root;
  }

  // ---------- the view ----------
  var PIPELINE_RENDERERS = { lineage: renderLineage, timeline: renderTimeline, trend: renderTrend };
  var PIPELINE_TITLES = { lineage: "Lineage graph", timeline: "Run timeline", trend: "Quality trend" };
  function pipelineView() {
    var root = h("div");
    root.appendChild(h("p", { class: "lede", text: "How the data moved and how healthy each run was: where every table comes from, when each step ran and whether it worked, and how many data-quality checks passed over time. Everything is drawn from files the pipeline's own tools wrote; if a file was not supplied, the section says so and gives the command that makes it." }));
    var sb = sampleBanner();
    if (sb) root.appendChild(sb);
    root.appendChild(h("ul", { class: "jump", "aria-label": "Sections" }, P.views.map(function (v) {
      return h("li", null, [h("a", { href: "#pipeline/" + v, text: PIPELINE_TITLES[v] || v })]);
    })));
    P.views.forEach(function (v) {
      if (typeof PIPELINE_RENDERERS[v] !== "function") throw new Error("pipeline view '" + v + "' is declared in build.py but has no renderer");
      root.appendChild(h("section", { class: "pv", "data-view": v, "aria-labelledby": "pv-" + v }, [
        h("h2", { id: "pv-" + v, tabindex: -1, text: PIPELINE_TITLES[v] || v }),
        PIPELINE_RENDERERS[v]()
      ]));
    });
    return root;
  }

