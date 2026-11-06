"use strict";
(function () {
  var D = JSON.parse(document.getElementById("dashboard-data").textContent);
  var SVGNS = "http://www.w3.org/2000/svg";
  var MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  // ---------- helpers ----------
  function h(tag, attrs, kids) {
    var e = document.createElement(tag);
    set(e, attrs);
    add(e, kids);
    return e;
  }
  function s(tag, attrs, kids) {
    var e = document.createElementNS(SVGNS, tag);
    set(e, attrs);
    add(e, kids);
    return e;
  }
  function set(e, attrs) {
    for (var k in attrs || {}) {
      if (attrs[k] === null || attrs[k] === undefined) continue;
      if (k === "text") e.textContent = attrs[k];
      else if (k === "class") e.setAttribute("class", attrs[k]);
      else e.setAttribute(k, attrs[k]);
    }
  }
  function add(e, kids) {
    (kids || []).forEach(function (c) {
      if (c === null || c === undefined) return;
      e.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
    });
  }
  function money(v, d) {
    var a = Math.abs(v);
    if (a >= 1e6) return "$" + (v / 1e6).toFixed(d === undefined ? 2 : d) + "M";
    if (a >= 1e4) return "$" + Math.round(v / 1e3) + "K";
    if (a >= 1e3) return "$" + (v / 1e3).toFixed(1) + "K";
    return "$" + Math.round(v);
  }
  function money0(v) { return "$" + Math.round(v).toLocaleString("en-US"); }
  function int(v) { return Math.round(v).toLocaleString("en-US"); }
  function pct(v, d) { return (v * 100).toFixed(d === undefined ? 1 : d) + "%"; }
  function mlabel(iso) { var m = +iso.slice(5, 7); return MONTHS[m - 1] + " " + iso.slice(0, 4); }
  function mshort(iso) { return MONTHS[+iso.slice(5, 7) - 1]; }
  // Round axis: returns {max, ticks} with a 1/2/2.5/5 x 10^k step so tick labels stay clean.
  function axis(maxv, want) {
    var raw = maxv / (want || 4), p = Math.pow(10, Math.floor(Math.log10(raw))), f = raw / p;
    var step = (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * p;
    var max = Math.ceil(maxv / step - 1e-9) * step, t = [];
    for (var v = 0; v <= max + step / 2; v += step) t.push(v);
    return { max: max, ticks: t, step: step };
  }
  function ticks(lo, hi, n) {
    var out = [], step = (hi - lo) / n;
    for (var i = 0; i <= n; i++) out.push(lo + step * i);
    return out;
  }
  function cssvar(name) { return "var(--" + name + ")"; }

  // ---------- tooltip ----------
  var tip = document.getElementById("tip");
  function showTip(clientX, clientY, title, rows) {
    tip.textContent = "";
    if (title) tip.appendChild(h("div", { class: "t", text: title }));
    rows.forEach(function (r) {
      var row = h("div", { class: "r" });
      if (r.color) row.appendChild(h("i", { style: "border-color:" + r.color }));
      row.appendChild(h("b", { text: r.value }));
      row.appendChild(h("span", { text: r.label }));
      tip.appendChild(row);
    });
    tip.hidden = false;
    var w = tip.offsetWidth, hgt = tip.offsetHeight;
    var x = clientX + 14, y = clientY + 14;
    if (x + w > window.innerWidth - 8) x = clientX - w - 14;
    if (y + hgt > window.innerHeight - 8) y = clientY - hgt - 14;
    tip.style.left = Math.max(8, x) + "px";
    tip.style.top = Math.max(8, y) + "px";
  }
  function hideTip() { tip.hidden = true; }

  // ---------- generic pieces ----------
  function legend(items) {
    return h("ul", { class: "legend", "aria-label": "Legend" }, items.map(function (it) {
      return h("li", null, [h("i", { class: it.box ? "sw" : "", style: (it.box ? "background:" : "border-color:") + it.color + (it.dash ? ";border-top-style:dashed" : "") }), it.name]);
    }));
  }
  function dataTable(caption, cols, rows) {
    var thead = h("thead", null, [h("tr", null, cols.map(function (c, i) { return h("th", { scope: "col", class: i === 0 ? "l" : "", text: c.label }); }))]);
    var tbody = h("tbody", null, rows.map(function (r) {
      return h("tr", null, cols.map(function (c, i) {
        var v = c.fmt ? c.fmt(r[c.key], r) : r[c.key];
        return h("td", { class: c.cls || (i === 0 ? "l" : ""), text: v === null || v === undefined ? "" : String(v) });
      }));
    }));
    return h("details", null, [
      h("summary", { text: "View data as a table" }),
      h("div", { class: "tbl-wrap" }, [h("table", null, [h("caption", { text: caption }), thead, tbody])])
    ]);
  }
  function card(title, caption, body) {
    var c = h("section", { class: "card", "aria-label": title }, [h("h3", { text: title })]);
    if (caption) c.appendChild(h("p", { class: "cap", text: caption }));
    add(c, body);
    return c;
  }
  var redraws = [];
  function chartHost(draw) {
    var host = h("div", { class: "chart" });
    var lastW = 0;
    function go() {
      var w = Math.max(260, Math.floor(host.clientWidth));
      if (!host.clientWidth || w === lastW) return;
      lastW = w;
      host.textContent = "";
      host.appendChild(draw(w));
    }
    redraws.push(go);
    // Draw whenever the host gets a real width: covers a page opened in a background tab, a
    // view switched from hidden, and container resizes, without relying on window events.
    if (typeof ResizeObserver === "function") new ResizeObserver(function () { go(); }).observe(host);
    return host;
  }
  function svgRoot(w, hgt, label) {
    return s("svg", { viewBox: "0 0 " + w + " " + hgt, width: w, height: hgt, role: "img", "aria-label": label, focusable: "false" });
  }

  // ---------- line chart with crosshair ----------
  // opts: labels[], series[{name,color,values,dash,width}], fmt, tipFmt, height, yMax, label(aria), endLabel(bool)
  function lineChart(opts) {
    return chartHost(function (W) {
      var H = opts.height || 240, m = { t: 12, r: opts.endLabel ? 74 : 14, b: 26, l: 48 };
      var n = opts.labels.length, iw = W - m.l - m.r, ih = H - m.t - m.b;
      var all = [].concat.apply([], opts.series.map(function (x) { return x.values; }));
      var ax = axis(Math.max.apply(null, all) * 1.02), ymax = opts.yMax || ax.max;
      var ymin = opts.yMin || 0;
      var yt = opts.yMax ? ticks(0, opts.yMax, 4) : ax.ticks;
      var x = function (i) { return m.l + (n === 1 ? iw / 2 : (iw * i) / (n - 1)); };
      var y = function (v) { return m.t + ih - ((v - ymin) / (ymax - ymin)) * ih; };
      var svg = svgRoot(W, H, opts.label);
      yt.forEach(function (t) {
        svg.appendChild(s("line", { class: "grid-line", x1: m.l, x2: W - m.r, y1: y(t), y2: y(t) }));
        svg.appendChild(s("text", { x: m.l - 6, y: y(t) + 4, "text-anchor": "end", text: opts.fmt(t) }));
      });
      var every = W < 520 ? 2 : 1;
      opts.labels.forEach(function (l, i) {
        if (i % every === 0) svg.appendChild(s("text", { x: x(i), y: H - 8, "text-anchor": "middle", text: l }));
      });
      svg.appendChild(s("line", { class: "axis-line", x1: m.l, x2: W - m.r, y1: y(ymin), y2: y(ymin) }));
      opts.series.forEach(function (se) {
        var d = se.values.map(function (v, i) { return (i ? "L" : "M") + x(i).toFixed(1) + " " + y(v).toFixed(1); }).join("");
        svg.appendChild(s("path", { class: "series", d: d, fill: "none", stroke: se.color, "stroke-width": se.width || 2, "stroke-dasharray": se.dash ? "5 4" : null, "stroke-linejoin": "round", "stroke-linecap": "round" }));
        if (!se.dash) {
          se.values.forEach(function (v, i) {
            svg.appendChild(s("circle", { cx: x(i), cy: y(v), r: 3.5, fill: se.color, stroke: cssvar("surface"), "stroke-width": 2 }));
          });
        }
        if (opts.endLabel && !se.dash) {
          var last = se.values[n - 1];
          svg.appendChild(s("text", { class: "lbl", x: x(n - 1) + 9, y: y(last) + 4, text: se.name }));
        }
      });
      // crosshair + focus
      var cross = s("line", { x1: 0, x2: 0, y1: m.t, y2: m.t + ih, stroke: cssvar("muted"), "stroke-width": 1, visibility: "hidden" });
      svg.appendChild(cross);
      var hit = s("rect", { x: m.l, y: m.t, width: iw, height: ih, fill: "transparent", tabindex: 0, role: "group", "aria-label": opts.label + ". Use left and right arrow keys to read each month." });
      svg.appendChild(hit);
      function at(i, cx, cy) {
        i = Math.max(0, Math.min(n - 1, i));
        cross.setAttribute("x1", x(i)); cross.setAttribute("x2", x(i)); cross.setAttribute("visibility", "visible");
        var box = svg.getBoundingClientRect();
        showTip(cx === undefined ? box.left + x(i) * (box.width / W) : cx, cy === undefined ? box.top + m.t + 10 : cy, opts.longLabels ? opts.longLabels[i] : opts.labels[i],
          opts.series.filter(function (se) { return !se.noTip; }).map(function (se) { return { color: se.color, value: (opts.tipFmt || opts.fmt)(se.values[i]), label: se.name }; }));
        return i;
      }
      var cur = -1;
      hit.addEventListener("pointermove", function (e) {
        var box = svg.getBoundingClientRect(), px = (e.clientX - box.left) * (W / box.width);
        cur = at(Math.round(((px - m.l) / iw) * (n - 1)), e.clientX, e.clientY);
      });
      hit.addEventListener("pointerleave", function () { cross.setAttribute("visibility", "hidden"); hideTip(); });
      hit.addEventListener("blur", function () { cross.setAttribute("visibility", "hidden"); hideTip(); });
      hit.addEventListener("focus", function () { cur = at(cur < 0 ? n - 1 : cur); });
      hit.addEventListener("keydown", function (e) {
        if (e.key === "ArrowRight") { cur = at(cur + 1); e.preventDefault(); }
        else if (e.key === "ArrowLeft") { cur = at(cur - 1); e.preventDefault(); }
        else if (e.key === "Escape") { hideTip(); }
      });
      return svg;
    });
  }

  // ---------- stacked columns ----------
  function stackedColumns(opts) {
    return chartHost(function (W) {
      var H = 240, m = { t: 12, r: 12, b: 26, l: 48 };
      var n = opts.labels.length, iw = W - m.l - m.r, ih = H - m.t - m.b, gap = 2;
      var totals = opts.labels.map(function (_, i) { return opts.series.reduce(function (a, se) { return a + se.values[i]; }, 0); });
      var ax = axis(Math.max.apply(null, totals) * 1.02), ymax = ax.max;
      var y = function (v) { return m.t + ih - (v / ymax) * ih; };
      var bw = Math.max(6, (iw / n) * 0.62), step = iw / n;
      var svg = svgRoot(W, H, opts.label);
      ax.ticks.forEach(function (t) {
        svg.appendChild(s("line", { class: "grid-line", x1: m.l, x2: W - m.r, y1: y(t), y2: y(t) }));
        svg.appendChild(s("text", { x: m.l - 6, y: y(t) + 4, "text-anchor": "end", text: money(t, 1) }));
      });
      opts.labels.forEach(function (l, i) {
        var cx = m.l + step * i + step / 2, acc = 0;
        if (i % (W < 520 ? 2 : 1) === 0) svg.appendChild(s("text", { x: cx, y: H - 8, "text-anchor": "middle", text: l }));
        opts.series.forEach(function (se) {
          var v = se.values[i], y0 = y(acc), y1 = y(acc + v);
          acc += v;
          svg.appendChild(s("rect", { x: cx - bw / 2, y: y1, width: bw, height: Math.max(0, y0 - y1 - gap), fill: se.color, rx: 1.5 }));
        });
        var g = s("rect", { x: cx - step / 2, y: m.t, width: step, height: ih, fill: "transparent", tabindex: 0, "aria-label": opts.longLabels[i] + ": " + opts.series.map(function (se) { return se.name + " " + money(se.values[i]); }).join(", ") });
        function show(cx2, cy2) {
          var box = svg.getBoundingClientRect();
          showTip(cx2 === undefined ? box.left + cx * (box.width / W) : cx2, cy2 === undefined ? box.top + m.t : cy2, opts.longLabels[i],
            opts.series.map(function (se) { return { color: se.color, value: money0(se.values[i]), label: se.name }; }));
        }
        g.addEventListener("pointermove", function (e) { show(e.clientX, e.clientY); });
        g.addEventListener("focus", function () { show(); });
        g.addEventListener("pointerleave", hideTip);
        g.addEventListener("blur", hideTip);
        svg.appendChild(g);
      });
      svg.appendChild(s("line", { class: "axis-line", x1: m.l, x2: W - m.r, y1: y(0), y2: y(0) }));
      return svg;
    });
  }

  // ---------- horizontal bars with confidence whiskers ----------
  // rows: {label, value, lo, hi, n, k}
  function barsH(opts) {
    return chartHost(function (W) {
      var rowH = 28, m = { t: 6, r: 56, b: 22, l: Math.min(150, W * 0.38) };
      var H = m.t + m.b + rowH * opts.rows.length, iw = W - m.l - m.r;
      var ax = axis(Math.max.apply(null, opts.rows.map(function (r) { return r.hi; })) * 1.02), xmax = ax.max;
      var x = function (v) { return m.l + (v / xmax) * iw; };
      var svg = svgRoot(W, H, opts.label);
      ax.ticks.forEach(function (t) {
        svg.appendChild(s("line", { class: "grid-line", x1: x(t), x2: x(t), y1: m.t, y2: H - m.b }));
        svg.appendChild(s("text", { x: x(t), y: H - 6, "text-anchor": "middle", text: pct(t, (ax.step * 100) % 1 > 1e-6 ? 1 : 0) }));
      });
      opts.rows.forEach(function (r, i) {
        var cy = m.t + rowH * i + rowH / 2, bh = 12;
        svg.appendChild(s("text", { x: m.l - 8, y: cy + 4, "text-anchor": "end", text: r.label.length > 22 ? r.label.slice(0, 21) + "…" : r.label }));
        svg.appendChild(s("rect", { x: m.l, y: cy - bh / 2, width: Math.max(1, x(r.value) - m.l), height: bh, fill: cssvar("series-1"), rx: 2 }));
        svg.appendChild(s("line", { x1: x(r.lo), x2: x(r.hi), y1: cy, y2: cy, stroke: cssvar("text"), "stroke-width": 1.5 }));
        svg.appendChild(s("line", { x1: x(r.lo), x2: x(r.lo), y1: cy - 4, y2: cy + 4, stroke: cssvar("text"), "stroke-width": 1.5 }));
        svg.appendChild(s("line", { x1: x(r.hi), x2: x(r.hi), y1: cy - 4, y2: cy + 4, stroke: cssvar("text"), "stroke-width": 1.5 }));
        svg.appendChild(s("text", { class: "lbl", x: x(r.hi) + 6, y: cy + 4, text: pct(r.value) }));
        var g = s("rect", { x: 0, y: cy - rowH / 2, width: W, height: rowH, fill: "transparent", tabindex: 0, "aria-label": r.label + ": denial rate " + pct(r.value) + ", " + int(r.k) + " of " + int(r.n) + " claims, 95% interval " + pct(r.lo) + " to " + pct(r.hi) });
        function show(cx, cy2) {
          var box = svg.getBoundingClientRect();
          showTip(cx === undefined ? box.left + x(r.value) * (box.width / W) : cx, cy2 === undefined ? box.top + cy : cy2, r.label, [
            { value: pct(r.value, 2), label: "denial rate" },
            { value: int(r.k) + " of " + int(r.n), label: "claims denied" },
            { value: pct(r.lo) + " to " + pct(r.hi), label: "95% interval" }
          ]);
        }
        g.addEventListener("pointermove", function (e) { show(e.clientX, e.clientY); });
        g.addEventListener("focus", function () { show(); });
        g.addEventListener("pointerleave", hideTip);
        g.addEventListener("blur", hideTip);
        svg.appendChild(g);
      });
      return svg;
    });
  }

  // ---------- provider scatter ----------
  function providerScatter(P) {
    return chartHost(function (W) {
      var H = 340, m = { t: 14, r: 86, b: 38, l: 48 };
      var iw = W - m.l - m.r, ih = H - m.t - m.b;
      var pts = P.peer_index;
      var xmin = Math.log10(P.min_claims * 0.9), xmax = Math.log10(Math.max.apply(null, pts.map(function (p) { return p.claim_count; })) * 1.1);
      var ymax = Math.ceil(Math.max.apply(null, pts.map(function (p) { return p.weighted_cost_index; })) * 4) / 4 + 0.1;
      var ymin = Math.floor(Math.min.apply(null, pts.map(function (p) { return p.weighted_cost_index; })) * 4) / 4 - 0.1;
      var x = function (v) { return m.l + ((Math.log10(v) - xmin) / (xmax - xmin)) * iw; };
      var y = function (v) { return m.t + ih - ((v - ymin) / (ymax - ymin)) * ih; };
      var svg = svgRoot(W, H, "Scatter of " + pts.length + " providers: claim volume on a log axis against cost index versus specialty peers. " + P.flagged.length + " providers sit at or above " + P.flag_threshold + ".");
      for (var t = Math.ceil(ymin * 4) / 4; t <= ymax + 1e-9; t += 0.25) {
        svg.appendChild(s("line", { class: "grid-line", x1: m.l, x2: W - m.r, y1: y(t), y2: y(t) }));
        svg.appendChild(s("text", { x: m.l - 6, y: y(t) + 4, "text-anchor": "end", text: t.toFixed(2) }));
      }
      [20, 50, 100, 200, 400].forEach(function (v) {
        if (Math.log10(v) < xmin || Math.log10(v) > xmax) return;
        svg.appendChild(s("text", { x: x(v), y: H - 20, "text-anchor": "middle", text: String(v) }));
      });
      svg.appendChild(s("text", { x: m.l + iw / 2, y: H - 4, "text-anchor": "middle", text: "Claims in period (log scale)" }));
      svg.appendChild(s("line", { x1: m.l, x2: W - m.r, y1: y(1), y2: y(1), stroke: cssvar("muted"), "stroke-width": 1.25 }));
      svg.appendChild(s("text", { x: W - m.r + 6, y: y(1) + 4, text: "peer average" }));
      svg.appendChild(s("line", { x1: m.l, x2: W - m.r, y1: y(P.flag_threshold), y2: y(P.flag_threshold), stroke: cssvar("series-2"), "stroke-width": 1.25, "stroke-dasharray": "5 4" }));
      svg.appendChild(s("text", { class: "lbl", x: W - m.r + 6, y: y(P.flag_threshold) + 4, text: "flag ≥ " + P.flag_threshold.toFixed(2) }));
      var ordered = pts.slice().sort(function (a, b) { return a.weighted_cost_index - b.weighted_cost_index; });
      ordered.forEach(function (p) {
        var fl = p.weighted_cost_index >= P.flag_threshold;
        svg.appendChild(s("circle", { cx: x(p.claim_count), cy: y(p.weighted_cost_index), r: fl ? 5 : 4, fill: fl ? cssvar("series-2") : cssvar("series-1"), "fill-opacity": fl ? 1 : 0.6, stroke: cssvar("surface"), "stroke-width": 1.5 }));
      });
      var top = pts.slice().sort(function (a, b) { return b.weighted_cost_index - a.weighted_cost_index; }).slice(0, 3);
      top.forEach(function (p, i) {
        svg.appendChild(s("text", { class: "lbl", x: Math.min(x(p.claim_count) + 8, W - m.r - 4), y: y(p.weighted_cost_index) + (i ? 12 : 4), "text-anchor": x(p.claim_count) + 8 > W - m.r - 60 ? "end" : "start", text: p.provider_label }));
      });
      svg.appendChild(s("line", { class: "axis-line", x1: m.l, x2: W - m.r, y1: m.t + ih, y2: m.t + ih }));
      // nearest-point hit layer
      var cursor = s("circle", { r: 8, fill: "none", stroke: cssvar("text"), "stroke-width": 2, visibility: "hidden" });
      svg.appendChild(cursor);
      var hit = s("rect", { x: m.l, y: m.t, width: iw, height: ih, fill: "transparent" });
      svg.appendChild(hit);
      hit.addEventListener("pointermove", function (e) {
        var box = svg.getBoundingClientRect(), k = W / box.width, px = (e.clientX - box.left) * k, py = (e.clientY - box.top) * k;
        var best = null, bd = 1e9;
        pts.forEach(function (p) { var d = Math.hypot(x(p.claim_count) - px, y(p.weighted_cost_index) - py); if (d < bd) { bd = d; best = p; } });
        if (!best || bd > 28) { cursor.setAttribute("visibility", "hidden"); hideTip(); return; }
        cursor.setAttribute("cx", x(best.claim_count)); cursor.setAttribute("cy", y(best.weighted_cost_index)); cursor.setAttribute("visibility", "visible");
        showTip(e.clientX, e.clientY, best.provider_id + " · " + best.specialty, [
          { value: best.weighted_cost_index.toFixed(2) + "×", label: "cost vs specialty peers" },
          { value: int(best.claim_count), label: "claims" },
          { value: pct(best.denial_rate), label: "denial rate" },
          { value: best.cost_tier, label: "cost tier" }
        ]);
      });
      hit.addEventListener("pointerleave", function () { cursor.setAttribute("visibility", "hidden"); hideTip(); });
      return svg;
    });
  }

  // ---------- plain share bars (value 0..1, optional marker) ----------
  function shareBars(opts) {
    return chartHost(function (W) {
      var rowH = 28, m = { t: 6, r: 60, b: 22, l: Math.min(190, W * 0.46) };
      var rows = opts.rows, H = m.t + m.b + rowH * rows.length, iw = W - m.l - m.r;
      var ax = axis(Math.max.apply(null, rows.map(function (r) { return r.value; })) * 1.02), xmax = Math.min(1, ax.max);
      var x = function (v) { return m.l + (v / xmax) * iw; };
      var svg = svgRoot(W, H, opts.label);
      ax.ticks.forEach(function (t) {
        if (t > xmax + 1e-9) return;
        svg.appendChild(s("line", { class: "grid-line", x1: x(t), x2: x(t), y1: m.t, y2: H - m.b }));
        svg.appendChild(s("text", { x: x(t), y: H - 6, "text-anchor": "middle", text: pct(t, 0) }));
      });
      rows.forEach(function (r, i) {
        var cy = m.t + rowH * i + rowH / 2, bh = 12;
        svg.appendChild(s("text", { x: m.l - 8, y: cy + 4, "text-anchor": "end", text: r.label.length > 30 ? r.label.slice(0, 29) + "\u2026" : r.label }));
        svg.appendChild(s("rect", { x: m.l, y: cy - bh / 2, width: Math.max(1, x(r.value) - m.l), height: bh, fill: cssvar("series-1"), rx: 2 }));
        if (r.marker !== undefined) svg.appendChild(s("line", { x1: x(r.marker), x2: x(r.marker), y1: cy - 9, y2: cy + 9, stroke: cssvar("series-2"), "stroke-width": 2 }));
        svg.appendChild(s("text", { class: "lbl", x: x(Math.max(r.value, r.marker || 0)) + 6, y: cy + 4, text: pct(r.value) }));
        var g = s("rect", { x: 0, y: cy - rowH / 2, width: W, height: rowH, fill: "transparent", tabindex: 0, "aria-label": r.tip });
        function show(cx, cy2) {
          var box = svg.getBoundingClientRect();
          showTip(cx === undefined ? box.left + x(r.value) * (box.width / W) : cx, cy2 === undefined ? box.top + cy : cy2, r.label, [{ value: pct(r.value, 2), label: "of total paid cost" }].concat(r.marker !== undefined ? [{ value: pct(r.marker, 0), label: "if spread equally" }] : []));
        }
        g.addEventListener("pointermove", function (e) { show(e.clientX, e.clientY); });
        g.addEventListener("focus", function () { show(); });
        g.addEventListener("pointerleave", hideTip);
        g.addEventListener("blur", hideTip);
        svg.appendChild(g);
      });
      if (opts.markerName) svg.appendChild(s("text", { x: W - m.r, y: m.t - 0, "text-anchor": "end", text: "| " + opts.markerName }));
      return svg;
    });
  }

  // ---------- Lorenz ----------
  function lorenzChart(M) {
    return chartHost(function (W) {
      var H = Math.min(340, Math.max(260, W * 0.62)), m = { t: 12, r: 14, b: 38, l: 48 };
      var iw = W - m.l - m.r, ih = H - m.t - m.b;
      var x = function (v) { return m.l + v * iw; }, y = function (v) { return m.t + ih - v * ih; };
      var pts = M.lorenz;
      var svg = svgRoot(W, H, "Lorenz curve of paid cost across members. The top 10 percent of members hold " + pct(M.concentration.top10, 0) + " of paid cost; Gini " + M.concentration.gini.toFixed(2) + ".");
      ticks(0, 1, 4).forEach(function (t) {
        svg.appendChild(s("line", { class: "grid-line", x1: m.l, x2: W - m.r, y1: y(t), y2: y(t) }));
        svg.appendChild(s("text", { x: m.l - 6, y: y(t) + 4, "text-anchor": "end", text: pct(t, 0) }));
        svg.appendChild(s("text", { x: x(t), y: H - 20, "text-anchor": "middle", text: pct(t, 0) }));
      });
      svg.appendChild(s("text", { x: m.l + iw / 2, y: H - 4, "text-anchor": "middle", text: "Share of members, lowest to highest paid" }));
      var area = "M" + x(0) + " " + y(0) + pts.map(function (p) { return "L" + x(p.x).toFixed(1) + " " + y(p.y).toFixed(1); }).join("") + "L" + x(1) + " " + y(1) + "Z";
      svg.appendChild(s("path", { d: area, fill: cssvar("series-1"), "fill-opacity": 0.12 }));
      svg.appendChild(s("line", { x1: x(0), y1: y(0), x2: x(1), y2: y(1), stroke: cssvar("muted"), "stroke-width": 1.25, "stroke-dasharray": "5 4" }));
      svg.appendChild(s("text", { x: x(0.08), y: y(0.08) - 8, text: "equal spread", transform: "rotate(-" + (Math.atan2(ih, iw) * 180 / Math.PI).toFixed(1) + " " + x(0.08) + " " + (y(0.08) - 8) + ")" }));
      svg.appendChild(s("path", { class: "series", d: "M" + pts.map(function (p) { return x(p.x).toFixed(1) + " " + y(p.y).toFixed(1); }).join("L"), fill: "none", stroke: cssvar("series-1"), "stroke-width": 2.25, "stroke-linejoin": "round" }));
      // annotated point: top 10 percent
      var p90 = pts.filter(function (p) { return Math.abs(p.x - 0.9) < 1e-9; })[0];
      if (p90) {
        svg.appendChild(s("circle", { cx: x(p90.x), cy: y(p90.y), r: 5, fill: cssvar("series-2"), stroke: cssvar("surface"), "stroke-width": 2 }));
        var anchorEnd = true;
        svg.appendChild(s("text", { class: "lbl", x: x(p90.x) - 4, y: y(p90.y) + 22, "text-anchor": "end", text: "Top 10% of members = " + pct(M.concentration.top10, 0) + " of paid" }));
      }
      var cross = s("circle", { r: 5, fill: cssvar("series-1"), stroke: cssvar("surface"), "stroke-width": 2, visibility: "hidden" });
      svg.appendChild(cross);
      var hit = s("rect", { x: m.l, y: m.t, width: iw, height: ih, fill: "transparent" });
      svg.appendChild(hit);
      hit.addEventListener("pointermove", function (e) {
        var box = svg.getBoundingClientRect(), px = (e.clientX - box.left) * (W / box.width);
        var i = Math.max(0, Math.min(pts.length - 1, Math.round(((px - m.l) / iw) * (pts.length - 1))));
        var p = pts[i];
        cross.setAttribute("cx", x(p.x)); cross.setAttribute("cy", y(p.y)); cross.setAttribute("visibility", "visible");
        showTip(e.clientX, e.clientY, "Lowest-paid " + pct(p.x, 0) + " of members", [
          { value: pct(p.y), label: "of total paid cost" },
          { value: pct(1 - p.y), label: "held by the other " + pct(1 - p.x, 0) }
        ]);
      });
      hit.addEventListener("pointerleave", function () { cross.setAttribute("visibility", "hidden"); hideTip(); });
      return svg;
    });
  }

  // ---------- status ----------
  function statusEl(kind, text) {
    var icon = kind === "pass"
      ? s("svg", { viewBox: "0 0 16 16", "aria-hidden": "true" }, [s("circle", { cx: 8, cy: 8, r: 7, fill: "none", stroke: "currentColor", "stroke-width": 1.5 }), s("path", { d: "M4.8 8.3l2.2 2.2 4.2-4.6", fill: "none", stroke: "currentColor", "stroke-width": 1.8, "stroke-linecap": "round", "stroke-linejoin": "round" })])
      : kind === "fail"
        ? s("svg", { viewBox: "0 0 16 16", "aria-hidden": "true" }, [s("circle", { cx: 8, cy: 8, r: 7, fill: "none", stroke: "currentColor", "stroke-width": 1.5 }), s("path", { d: "M5.5 5.5l5 5M10.5 5.5l-5 5", stroke: "currentColor", "stroke-width": 1.8, "stroke-linecap": "round" })])
        : s("svg", { viewBox: "0 0 16 16", "aria-hidden": "true" }, [s("circle", { cx: 8, cy: 8, r: 7, fill: "none", stroke: "currentColor", "stroke-width": 1.5, "stroke-dasharray": "3 2" })]);
    return h("span", { class: "status " + kind }, [icon, text]);
  }

  // ======================================================
  // EXECUTIVE VIEW
  // ======================================================
  function executive() {
    var K = D.kpis, mo = D.monthly, root = h("div");
    var labels = mo.map(function (r) { return mshort(r.service_month); });
    var longLabels = mo.map(function (r) { return mlabel(r.service_month); });
    var trend = D.pmpm_trend;
    var kpiDefs = [
      ["Paid cost", money(K.paid), "of " + money(K.billed) + " billed"],
      ["Paid / billed", pct(K.paid_to_billed, 0), "discounts and denials"],
      ["Claims", int(K.claims), int(K.members) + " members"],
      ["Denial rate", pct(K.denial_rate), int(K.denied_claims) + " claims denied"],
      ["PMPM paid, " + mshort(mo[mo.length - 1].service_month), "$" + K.pmpm_latest.toFixed(0), "from $" + K.pmpm_first.toFixed(0) + " in " + mshort(mo[0].service_month)]
    ];
    root.appendChild(h("div", { class: "kpis", role: "list" }, kpiDefs.map(function (k) {
      return h("div", { class: "kpi", role: "listitem" }, [h("div", { class: "l", text: k[0] }), h("div", { class: "v", text: k[1] }), h("div", { class: "n", text: k[2] })]);
    })));

    // --- cost
    root.appendChild(h("h2", { id: "cost", text: "Cost" }));
    var peak = mo.reduce(function (a, r) { return r.pmpm_paid > a.pmpm_paid ? r : a; }, mo[0]);
    var trendTxt = trend.r2 < 0.2
      ? "PMPM (paid per member per month) shows no clear linear trend (R\u00b2 " + trend.r2.toFixed(2) + ", slope " + (trend.slope >= 0 ? "+" : "-") + "$" + Math.abs(trend.slope).toFixed(0) + " a month); it peaks in " + mlabel(peak.service_month) + " at $" + peak.pmpm_paid.toFixed(0) + "."
      : "PMPM (paid per member per month) moves " + (trend.slope >= 0 ? "up" : "down") + " about $" + Math.abs(trend.slope).toFixed(0) + " a month over the period (linear fit, R\u00b2 " + trend.r2.toFixed(2) + ").";
    root.appendChild(h("p", { class: "lede", text: "Plans pay a fraction of what providers bill. " + trendTxt }));
    var g1 = h("div", { class: "grid two" });
    g1.appendChild(card("Billed vs paid, by service month", "Same dollar axis. The gap is the allowed-amount discount plus denied claims.", [
      legend([{ name: "Billed", color: cssvar("series-2") }, { name: "Paid", color: cssvar("series-1") }]),
      lineChart({ label: "Line chart of monthly billed and paid cost, " + longLabels[0] + " to " + longLabels[longLabels.length - 1] + ". Paid is " + pct(K.paid_to_billed, 0) + " of billed overall.", labels: labels, longLabels: longLabels, endLabel: true, fmt: function (v) { return money(v, 1); }, tipFmt: money0,
        series: [{ name: "Billed", color: cssvar("series-2"), values: mo.map(function (r) { return r.billed_amount; }) }, { name: "Paid", color: cssvar("series-1"), values: mo.map(function (r) { return r.paid_amount; }) }] }),
      dataTable("Monthly cost", [{ key: "service_month", label: "Month", fmt: mlabel }, { key: "billed_amount", label: "Billed", fmt: money0 }, { key: "allowed_amount", label: "Allowed", fmt: money0 }, { key: "paid_amount", label: "Paid", fmt: money0 }, { key: "paid_to_billed", label: "Paid/billed", fmt: function (v) { return pct(v); } }], mo)
    ]));
    g1.appendChild(card("PMPM paid with linear trend", "Paid cost per member per month. Dashed line is the least-squares fit.", [
      legend([{ name: "PMPM paid", color: cssvar("series-1") }, { name: "Linear trend", color: cssvar("muted"), dash: true }]),
      lineChart({ label: "Line chart of PMPM paid by month with a dashed linear trend, slope " + trend.slope.toFixed(1) + " dollars per month.", labels: labels, longLabels: longLabels, endLabel: false, fmt: function (v) { return "$" + Math.round(v); }, tipFmt: function (v) { return "$" + v.toFixed(2); }, yMin: 0,
        series: [{ name: "PMPM paid", color: cssvar("series-1"), values: mo.map(function (r) { return r.pmpm_paid; }) }, { name: "Linear trend", color: cssvar("muted"), dash: true, width: 1.5, values: trend.fitted }] }),
      dataTable("PMPM", [{ key: "service_month", label: "Month", fmt: mlabel }, { key: "member_months", label: "Member-months", fmt: int }, { key: "pmpm_paid", label: "PMPM paid", fmt: function (v) { return "$" + v.toFixed(2); } }], mo),
      h("p", { class: "note", text: "PMPM uses an approximate member-month denominator (no disenrollment in the data). Recent months can still move as late claims arrive." })
    ]));
    root.appendChild(g1);
    var mix = D.claim_type_mix;
    var ctNames = ["Professional", "Institutional", "Pharmacy"], ctKeys = ["professional_share", "institutional_share", "pharmacy_share"], ctCol = ["series-1", "series-2", "series-3"];
    root.appendChild(h("div", { class: "grid", style: "margin-top:16px" }, [card("Paid cost by claim type", "Stacked monthly paid dollars.", [
      legend(ctNames.map(function (n, i) { return { name: n, color: cssvar(ctCol[i]), box: true }; })),
      stackedColumns({ label: "Stacked columns of monthly paid cost by claim type.", labels: labels, longLabels: longLabels,
        series: ctNames.map(function (n, i) { return { name: n, color: cssvar(ctCol[i]), values: mix.map(function (r) { return r.paid_amount * r[ctKeys[i]]; }) }; }) }),
      dataTable("Paid share by claim type", [{ key: "service_month", label: "Month", fmt: mlabel }, { key: "paid_amount", label: "Paid", fmt: money0 }].concat(ctNames.map(function (n, i) { return { key: ctKeys[i], label: n, fmt: function (v) { return pct(v); } }; })), mix)
    ])]));

    // --- denials
    var DN = D.denials;
    root.appendChild(h("h2", { id: "denials", text: "Denials" }));
    root.appendChild(h("p", { class: "lede", text: "Denial rate is denied claims / claims. Whiskers show a 95% Wilson interval, so thin cells do not look more certain than they are. The synthetic data has no denial reason code and no payer dimension; claim type, specialty and plan type are the cuts it supports." }));
    function denialRows(list, key) { return list.map(function (r) { return { label: r[key], value: r.denial_rate, lo: r.ci_low, hi: r.ci_high, n: r.claim_count, k: r.denied_claim_count }; }); }
    var denCols = function (k, l) { return [{ key: k, label: l }, { key: "claim_count", label: "Claims", fmt: int }, { key: "denied_claim_count", label: "Denied", fmt: int }, { key: "denial_rate", label: "Rate", fmt: function (v) { return pct(v, 2); } }, { key: "ci_low", label: "95% low", fmt: function (v) { return pct(v, 1); } }, { key: "ci_high", label: "95% high", fmt: function (v) { return pct(v, 1); } }]; };
    var g2 = h("div", { class: "grid two" });
    g2.appendChild(card("By claim type", null, [barsH({ label: "Bar chart of denial rate by claim type with 95% intervals.", rows: denialRows(DN.by_claim_type, "claim_type") }), dataTable("Denials by claim type", denCols("claim_type", "Claim type"), DN.by_claim_type)]));
    g2.appendChild(card("By plan type (payer proxy)", "Members' current plan type; the closest thing to a payer cut.", [barsH({ label: "Bar chart of denial rate by plan type with 95% intervals.", rows: denialRows(DN.by_plan_type, "plan_type") }), dataTable("Denials by plan type", denCols("plan_type", "Plan type"), DN.by_plan_type)]));
    root.appendChild(g2);
    root.appendChild(h("div", { class: "grid two", style: "margin-top:16px" }, [
      card("By provider specialty", null, [barsH({ label: "Bar chart of denial rate by provider specialty with 95% intervals.", rows: denialRows(DN.by_specialty, "specialty") }), dataTable("Denials by specialty", denCols("specialty", "Specialty"), DN.by_specialty)]),
      card("Denial rate by month", "Monthly rates are noisy: about " + int(K.claims / mo.length) + " claims per month.", [
        lineChart({ label: "Line chart of monthly denial rate.", labels: labels, longLabels: longLabels, fmt: function (v) { return pct(v, 0); }, tipFmt: function (v) { return pct(v, 2); }, yMax: Math.max(0.2, Math.ceil(Math.max.apply(null, mo.map(function (r) { return r.denial_rate; })) * 25) / 25),
          series: [{ name: "Denial rate", color: cssvar("series-1"), values: mo.map(function (r) { return r.denial_rate; }) }] }),
        dataTable("Monthly denial rate", [{ key: "service_month", label: "Month", fmt: mlabel }, { key: "claim_count", label: "Claims", fmt: int }, { key: "denied_claim_count", label: "Denied", fmt: int }, { key: "denial_rate", label: "Rate", fmt: function (v) { return pct(v, 2); } }], mo)
      ])
    ]));

    // --- providers
    var P = D.providers, tiers = P.tier_summary;
    var highElev = ["HIGH_COST", "ELEVATED"].reduce(function (a, t) { return a + (tiers[t] ? tiers[t].flagged : 0); }, 0);
    root.appendChild(h("h2", { id: "providers", text: "Provider performance" }));
    root.appendChild(h("p", { class: "lede", text: "Each provider's allowed cost per claim divided by the average for its specialty (1.00 = at peers). Providers with fewer than " + P.min_claims + " claims are left out because their ratio is noise. No case-mix adjustment: this flags cost, not quality." }));
    root.appendChild(h("p", { class: "insight", text: P.flagged.length + " of " + P.peer_index.length + " providers cost at least " + Math.round((P.flag_threshold - 1) * 100) + "% above their specialty peers; " + highElev + " of them belong to the ELEVATED or HIGH_COST tiers the generator planted." }));
    var g3 = h("div", { class: "grid two" });
    g3.appendChild(card("Cost index vs peers", "One dot per provider; orange dots are flagged.", [
      legend([{ name: "Within peer range", color: cssvar("series-1"), box: true }, { name: "Flagged", color: cssvar("series-2"), box: true }]),
      providerScatter(P),
      dataTable("Flagged providers (chart labels show the last 6 characters of the token)", [{ key: "provider_label", label: "Chart label" }, { key: "provider_id", label: "Provider token" }, { key: "specialty", label: "Specialty" }, { key: "cost_tier", label: "Tier" }, { key: "claim_count", label: "Claims", fmt: int }, { key: "weighted_cost_index", label: "Index", fmt: function (v) { return v.toFixed(2); } }], P.flagged)
    ]));
    g3.appendChild(card("Specialty benchmark", "Allowed and paid per claim, with denial rate.", [
      dataTableOpen("Specialty benchmark", [{ key: "specialty", label: "Specialty" }, { key: "claim_count", label: "Claims", fmt: int }, { key: "allowed_per_claim", label: "Allowed/claim", fmt: money0 }, { key: "paid_per_claim", label: "Paid/claim", fmt: money0 }, { key: "denial_rate", label: "Denial", fmt: function (v) { return pct(v); } }], P.specialty_benchmark)
    ]));
    root.appendChild(g3);

    // --- members: every declared view must have a renderer (see MEMBER_VIEWS in build.py)
    var M = D.members, C = M.concentration;
    root.appendChild(h("h2", { id: "members", text: "Member cost concentration" }));
    root.appendChild(h("p", { class: "lede", text: "A small share of members drives most paid cost. The Lorenz curve plots cumulative paid cost against members ranked from lowest to highest paid; the further it bows below the diagonal, the more concentrated the spend." }));
    var renderers = {
      lorenz: function () {
        return card("Lorenz curve of paid cost", "All " + int(C.members) + " members, including those with no claims.", [lorenzChart(M), dataTable("Lorenz points (every percentile)", [{ key: "x", label: "Members (lowest paid first)", fmt: function (v) { return pct(v, 0); } }, { key: "y", label: "Cumulative paid", fmt: function (v) { return pct(v, 1); } }], M.lorenz)]);
      },
      pareto: function () {
        return card("Pareto summary", "Share of total paid cost held by the top slice of members, against an equal split.", [
          shareBars({ label: "Bar chart: share of paid cost held by the top 1, 5, 10, 20 and 50 percent of members.", rows: M.pareto.map(function (p) { return { label: "Top " + p.top_pct + "% of members", value: p.share, marker: p.top_pct / 100, tip: "Top " + p.top_pct + "% of members hold " + pct(p.share) + " of paid cost (equal split: " + p.top_pct + "%)" }; }), markerName: "equal split" }),
          dataTableOpen("Share of paid cost by top slice of members", [{ key: "top_pct", label: "Top slice of members", fmt: function (v) { return "Top " + v + "%"; } }, { key: "share", label: "Share of paid cost", fmt: function (v) { return pct(v); } }, { key: "equal", label: "If spread equally", fmt: function (v) { return pct(v, 0); } }], M.pareto.map(function (p) { return { top_pct: p.top_pct, share: p.share, equal: p.top_pct / 100 }; }).concat([])),
          h("p", { class: "note", text: "Gini coefficient " + C.gini.toFixed(2) + " (0 = everyone costs the same, 1 = one member holds everything). Concentration is a generator setting (skewed utilisation), not real population behaviour." })
        ]);
      },
      risk_tiers: function () {
        var rows = M.risk_tiers.map(function (r) { return { label: r.plan_type + " \u00b7 " + r.risk_tier, value: r.share_of_total_paid, tip: r.plan_type + " " + r.risk_tier + ": " + int(r.member_count) + " members hold " + pct(r.share_of_total_paid) + " of paid cost (" + money0(r.avg_paid_per_member) + " each)" }; });
        return card("High and medium cost members by plan", "Share of total paid cost held by HIGH (top 10%) and MEDIUM risk-tier members, per current plan type. Risk tier is a cost percentile, not a clinical score.", [
          shareBars({ label: "Bar chart: share of total paid cost by plan type and risk tier.", rows: rows }),
          dataTable("High and medium cost members", [{ key: "plan_type", label: "Plan" }, { key: "risk_tier", label: "Tier" }, { key: "member_count", label: "Members", fmt: int }, { key: "paid_amount", label: "Paid", fmt: money0 }, { key: "avg_paid_per_member", label: "Per member", fmt: money0 }, { key: "share_of_total_paid", label: "Share of paid", fmt: function (v) { return pct(v); } }], M.risk_tiers)
        ]);
      }
    };
    var g4 = h("div", { class: "grid two", "data-section": "members" });
    M.views.forEach(function (id) {
      var node;
      if (!renderers[id]) throw new Error("member cost view has no renderer: " + id);
      var data = id === "lorenz" ? M.lorenz : id === "pareto" ? M.pareto : M.risk_tiers;
      if (!data || !data.length) node = card(id, "No data for this view in the current build.", []);
      else node = renderers[id]();
      node.setAttribute("data-view", id);
      if (id === "risk_tiers") node.classList.add("span2");
      g4.appendChild(node);
    });
    root.appendChild(g4);
    return root;
  }
  function dataTableOpen(caption, cols, rows) {
    var d = dataTable(caption, cols, rows);
    d.open = true;
    d.querySelector("summary").textContent = "Table";
    return d;
  }

  // ======================================================
  // DATA QUALITY VIEW
  // ======================================================
  function qualityView() {
    var Q = D.quality, root = h("div");
    var failing = Q.invariants.filter(function (i) { return i.status !== "pass"; }).length;
    root.appendChild(h("h2", { id: "scorecard", text: "Data-quality scorecard", style: "margin-top:0" }));
    root.appendChild(h("p", { class: "lede", text: "Checks run on the gold export this dashboard was built from, plus what the data generator says it injected. A defect counts as caught when it is absent from gold; this does not say which layer removed it." }));
    var tiles = [];
    tiles.push([ "Gold integrity checks", (Q.invariants.length - failing) + " / " + Q.invariants.length, failing ? statusEl("fail", failing + " failing") : statusEl("pass", "All pass")]);
    if (Q.defects) {
      var rated = Q.defects.filter(function (d) { return d.catch_rate !== null; });
      var inj = rated.reduce(function (a, d) { return a + d.injected; }, 0), left = rated.reduce(function (a, d) { return a + d.still_in_gold; }, 0);
      tiles.push(["Injected defects found in gold", int(left) + " of " + int(inj), left === 0 ? statusEl("pass", "100% caught") : statusEl("fail", "Leaks")]);
    } else tiles.push(["Defect catch rate", "n/a", statusEl("na", "No manifest supplied")]);
    if (Q.raw_to_gold) {
      var c = Q.raw_to_gold[0];
      tiles.push(["Claims quarantined or excluded", int(c.quarantined_or_excluded), statusEl("na", pct(c.quarantined_or_excluded / c.raw_distinct) + " of " + int(c.raw_distinct) + " distinct")]);
    }
    tiles.push(["Quality framework (#22)", Q.quality_framework ? Q.quality_framework.suites.reduce(function (a, x) { return a + x.passed; }, 0) + " / " + Q.quality_framework.suites.reduce(function (a, x) { return a + x.evaluated; }, 0) : "n/a", Q.quality_framework ? statusEl(Q.quality_framework.suites.some(function (x) { return x.failed || x.status === "missing"; }) ? "fail" : "pass", "expectations passed") : statusEl("na", "Not supplied")]);
    root.appendChild(h("div", { class: "tiles" }, tiles.map(function (t) {
      return h("div", { class: "kpi tile" }, [h("div", { class: "l", text: t[0] }), h("div", { class: "v", text: t[1] }), h("div", { class: "n" }, [t[2]])]);
    })));

    // lineage
    root.appendChild(h("h2", { id: "lineage", text: "Lineage: raw to dashboard" }));
    var r2g = Q.raw_to_gold, claimsRow = r2g ? r2g[0] : null, linesRow = r2g ? r2g[1] : null;
    function stage(name, desc, count) { return h("li", null, [h("strong", { text: name }), h("span", { text: desc }), h("div", { class: "c" + (count ? "" : " na"), text: count || "row count not measured in this build" })]); }
    root.appendChild(h("ol", { class: "flow", "aria-label": "Pipeline stages" }, [
      stage("Generator", "claimsgen, with seeded defects", claimsRow ? int(claimsRow.raw_rows) + " claim rows, " + int(linesRow.raw_rows) + " lines" : null),
      stage("Bronze and silver", /fixture/i.test(D.meta.gold_source) ? "not run here: fixture SQL approximates dedupe and quarantine" : "typed, deduplicated, quarantined", claimsRow ? int(claimsRow.duplicates_removed) + " duplicate claim rows removed" : null),
      stage("Gold", "star schema, SCD2 dimensions", int(D.quality.invariants[0].checked) + " claims, " + int(D.quality.invariants[1].checked) + " lines"),
      stage("dbt marts", "4 consumption tables", "mart_claims_monthly, _provider_performance, _member_risk, _denials"),
      stage("This page", "DuckDB queries, inline JSON", Object.keys(D.meta.queries).length + " queries")
    ]));

    // defects
    root.appendChild(h("h2", { id: "defects", text: "Injected defects vs gold" }));
    if (Q.defects) {
      var rows = Q.defects.map(function (d) { return d; });
      var tbl = h("table", null, [h("caption", { text: "Each defect the generator injected, and how many records of that kind are still visible in gold." }),
        h("thead", null, [h("tr", null, ["Defect", "Injected", "Still in gold", "Caught", "Note"].map(function (t, i) { return h("th", { scope: "col", class: i === 0 || i === 4 ? "l" : "", text: t }); }))]),
        h("tbody", null, rows.map(function (d) {
          var caught;
          if (d.catch_rate === null) caught = h("td", { class: "l" }, [statusEl("na", "Not applicable")]);
          else caught = h("td", null, [h("span", { class: "bar-cell", "aria-hidden": "true" }, [h("b", { style: "width:" + (d.catch_rate * 100).toFixed(0) + "%" })]), statusEl(d.catch_rate === 1 ? "pass" : "fail", pct(d.catch_rate, 0))]);
          var note = d.note || (d.shared_check ? "Shares one gold check with a sibling defect (the count is combined)." : "");
          return h("tr", null, [h("td", { class: "l", text: d.defect }), h("td", { text: int(d.injected) }), h("td", { text: d.still_in_gold === null ? "n/a" : int(d.still_in_gold) }), caught, h("td", { class: "wrap-cell l", text: note })]);
        }))]);
      root.appendChild(h("div", { class: "card" }, [h("div", { class: "tbl-wrap" }, [tbl]),
        h("p", { class: "note", text: "Counts are distinct injected records from defects_manifest.json (seed " + Q.manifest_seed + ", " + int(Q.manifest_claims) + " claims requested). Whole claims are quarantined when any of their lines is bad, so more claims leave than defects were injected." })]));
    } else root.appendChild(h("p", { class: "ext", text: Q.defects_note }));

    // invariants
    root.appendChild(h("h2", { id: "checks", text: "Gold integrity checks" }));
    var itbl = h("table", null, [h("caption", { text: "Independent DuckDB checks on the gold Parquet export. Expected violations: zero." }),
      h("thead", null, [h("tr", null, ["Check", "Rows examined", "Violations", "Result"].map(function (t, i) { return h("th", { scope: "col", class: i === 0 ? "l" : "", text: t }); }))]),
      h("tbody", null, Q.invariants.map(function (i) { return h("tr", null, [h("td", { class: "l", text: i.label }), h("td", { text: int(i.checked) }), h("td", { text: int(i.violations) }), h("td", { class: "l" }, [statusEl(i.status, i.status === "pass" ? "Pass" : "Fail")])]); }))]);
    root.appendChild(h("div", { class: "card" }, [h("div", { class: "tbl-wrap" }, [itbl])]));

    // raw -> gold counts
    if (r2g) {
      var ctbl = h("table", null, [h("caption", { text: "Row counts from the generator's raw output to gold." }),
        h("thead", null, [h("tr", null, ["Table", "Raw rows", "Re-sent duplicates", "Quarantined or excluded", "Gold rows"].map(function (t, i) { return h("th", { scope: "col", class: i === 0 ? "l" : "", text: t }); }))]),
        h("tbody", null, r2g.map(function (r) { return h("tr", null, [h("td", { class: "l", text: r.table }), h("td", { text: int(r.raw_rows) }), h("td", { text: int(r.duplicates_removed) }), h("td", { text: int(r.quarantined_or_excluded) }), h("td", { text: int(r.gold_rows) })]); }))]);
      root.appendChild(h("h2", { id: "recon", text: "Reconciliation: raw to gold" }));
      root.appendChild(h("div", { class: "card" }, [h("div", { class: "tbl-wrap" }, [ctbl]), h("p", { class: "note", text: "Identity: raw rows = duplicates + quarantined/excluded + gold rows." })]));
    }
    // spark reconciliation extension
    root.appendChild(h("h2", { id: "spark", text: "Spark gold reconciliation" }));
    if (Q.reconciliation) {
      var rtbl = h("table", null, [h("caption", { text: "gold_reconciliation as exported from the Spark gold job." }),
        h("thead", null, [h("tr", null, ["Gold table", "Batch", "Silver rows", "Gold rows", "Excluded", "Identity"].map(function (t, i) { return h("th", { scope: "col", class: i < 2 ? "l" : "", text: t }); }))]),
        h("tbody", null, Q.reconciliation.map(function (r) { return h("tr", null, [h("td", { class: "l", text: r.gold_table }), h("td", { class: "l", text: r.batch_id }), h("td", { text: int(r.silver_rows) }), h("td", { text: int(r.gold_rows) }), h("td", { text: int(r.excluded_rows) }), h("td", { class: "l" }, [statusEl(r.identity_ok ? "pass" : "fail", r.identity_ok ? "Holds" : "Broken")])]); }))]);
      root.appendChild(h("div", { class: "card" }, [h("div", { class: "tbl-wrap" }, [rtbl])]));
    } else root.appendChild(h("p", { class: "ext", text: "Not supplied. Pass an export of the Spark gold_reconciliation table with --reconciliation (JSON or Parquet) and it appears here. This build used the dbt test fixture, which has no reconciliation table." }));

    // quality framework extension
    root.appendChild(h("h2", { id: "dq", text: "Data-quality framework results" }));
    if (Q.quality_framework) {
      var qs = Q.quality_framework.suites, qMissing = qs.filter(function (x) { return x.status === "missing"; });
      var qtbl = h("table", null, [h("caption", { text: "Expectation suites reported by " + Q.quality_framework.generated_by + ". Run on this page's data source (" + D.meta.gold_source + ")." }),
        h("thead", null, [h("tr", null, ["Suite", "Layer", "Evaluated", "Passed", "Failed", "Status"].map(function (t, i) { return h("th", { scope: "col", class: i < 2 || i === 5 ? "l" : "", text: t }); }))]),
        h("tbody", null, qs.map(function (x) { return h("tr", null, [h("td", { class: "l", text: x.name }), h("td", { class: "l", text: x.layer }), h("td", { text: int(x.evaluated) }), h("td", { text: int(x.passed) }), h("td", { text: int(x.failed) }), h("td", { class: "l", text: x.status === "missing" ? "Table missing from this source: not evaluated" : x.failed ? "Failed" : "Passed" })]); }))]);
      root.appendChild(h("div", { class: "card" }, [h("div", { class: "tbl-wrap" }, [qtbl])]));
      if (qMissing.length) root.appendChild(h("p", { class: "ext", text: qMissing.length + " required table" + (qMissing.length > 1 ? "s were" : " was") + " absent from this source (" + qMissing.map(function (x) { return x.name; }).join(", ") + "), so the framework reported a blocking failure for them. The fixture is not the Spark gold output, which writes those tables." }));
    } else root.appendChild(h("p", { class: "ext", text: "Not supplied. Run python -m lakehouse.quality run --layer gold and pass its gold-result.json with --quality-json; its pass/fail counts appear here (see docs/dashboard.md)." }));

    // methodology
    root.appendChild(h("h2", { id: "method", text: "How this page was built" }));
    var ul = h("ul", { class: "note" }, Object.keys(D.meta.queries).map(function (k) { return h("li", null, [h("code", { text: D.meta.queries[k].file }), " – " + D.meta.queries[k].question]); }));
    root.appendChild(h("div", { class: "card" }, [h("p", { class: "cap", text: "Source: " + D.meta.gold_source + ". Every figure comes from one of these queries, which read only the dbt marts." }), ul]));
    return root;
  }

  /*__PIPELINE__*/

  // ---------- boot ----------
  document.getElementById("subtitle").textContent = "Gold layer · " + D.meta.gold_source + " · " + mlabel(D.meta.period[0]) + " to " + mlabel(D.meta.period[1]);
  document.getElementById("notice").textContent = D.meta.synthetic_notice;
  var VIEWS = ["executive", "quality", "pipeline"];
  var TITLES = { executive: "Executive view", quality: "Data quality", pipeline: "Pipeline runs" };
  var vEl = {};
  VIEWS.forEach(function (v) { vEl[v] = document.getElementById("view-" + v); });
  vEl.executive.appendChild(executive());
  vEl.quality.appendChild(qualityView());
  vEl.pipeline.appendChild(pipelineView());
  function route() {
    var parts = location.hash.replace(/^#/, "").split("/");
    var cur = VIEWS.indexOf(parts[0]) >= 0 ? parts[0] : "executive";
    VIEWS.forEach(function (v) {
      vEl[v].hidden = v !== cur;
      var a = document.getElementById("nav-" + v);
      if (v === cur) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
    });
    document.title = TITLES[cur] + " \u2013 Claims Lakehouse Analytics";
    hideTip();
    redraw(true);
    if (cur === "pipeline" && parts[1]) {
      var t = document.getElementById("pv-" + parts[1]);
      if (t) { t.scrollIntoView(); t.focus({ preventScroll: true }); }
    }
  }
  function redraw(force) { redraws.forEach(function (fn) { fn(force); }); }
  window.addEventListener("hashchange", route);
  var timer;
  window.addEventListener("resize", function () { clearTimeout(timer); timer = setTimeout(function () { redraw(); }, 120); });
  // charts in a hidden view have zero width; draw when shown
  function themeInit() {
    var btn = document.getElementById("theme"), root = document.documentElement;
    try { var saved = localStorage.getItem("dashboard-theme"); if (saved) root.setAttribute("data-theme", saved); } catch (e) { /* storage unavailable */ }
    btn.addEventListener("click", function () {
      var cur = root.getAttribute("data-theme") || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
      var next = cur === "dark" ? "light" : "dark";
      root.setAttribute("data-theme", next);
      try { localStorage.setItem("dashboard-theme", next); } catch (e) { /* ignore */ }
    });
  }
  themeInit();
  route();
  window.__dashboardReady = true;
})();
