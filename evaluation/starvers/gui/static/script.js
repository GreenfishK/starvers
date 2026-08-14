/* ──────────────────────────────────────────────────────────────
   script.js – Progressive loading of step details + interactivity
   ────────────────────────────────────────────────────────────── */

document.addEventListener('DOMContentLoaded', () => {

  // ── Load step details progressively ─────────────────────────
  const ts = document.body.dataset.activeTs;
  if (ts) {
    const steps = document.querySelectorAll('.step-section-body[id^="step-body-"]');
    loadStepsSequentially(ts, steps, 0);
  }

  // ── Zoom modal close handlers ───────────────────────────────
  const modal = document.getElementById('plotZoomModal');
  if (modal) {
    const close = () => modal.classList.add('hidden');
    modal.querySelector('.plot-zoom-backdrop').addEventListener('click', close);
    modal.querySelector('.plot-zoom-close').addEventListener('click', close);
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape' && !modal.classList.contains('hidden')) close();
    });
  }
});


// ── Progressive step loading ──────────────────────────────────

function loadStepsSequentially(ts, stepEls, index) {
  if (index >= stepEls.length) {
    // Auto-refresh disabled (previously reloaded every 15s while a step ran).
    // Manual refresh is available via the ↻ button.
    return;
  }

  const el = stepEls[index];
  const loadingIndicator = el.querySelector('.step-loading');

  // Skip steps that don't have a loading indicator (pending/no data)
  if (!loadingIndicator) {
    loadStepsSequentially(ts, stepEls, index + 1);
    return;
  }

  const name = el.id.replace('step-body-', '');

  const urlTemplate = document.body.dataset.stepUrlTemplate || '';
  const url = urlTemplate.replace('__TS__', encodeURIComponent(ts)).replace('__STEP__', name);

  fetch(url)
    .then(res => {
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.text();
    })
    .then(html => {
      if (html.trim()) {
        el.innerHTML = html;
      } else {
        el.innerHTML = '<div class="dim">No detail data available for this step.</div>';
      }
      // Activate interactivity on newly inserted content
      el.querySelectorAll('.query-flow-container').forEach(activateQueryFlowHover);
      el.querySelectorAll('.tsplot-card').forEach(activatePlotZoom);
    })
    .catch(err => {
      el.innerHTML = `<div class="error-msg">Could not load step details: ${err.message}</div>`;
    })
    .finally(() => {
      // Load next step
      loadStepsSequentially(ts, stepEls, index + 1);
    });
}


// ── Plot zoom ─────────────────────────────────────────────────

function activatePlotZoom(card) {
  card.style.cursor = 'zoom-in';
  card.addEventListener('click', () => {
    const modal = document.getElementById('plotZoomModal');
    if (!modal) return;
    const title = card.dataset.zoomTitle || '';
    const imgEl = card.querySelector('.tsplot-svg-wrap img');
    const avgEl = card.querySelector('.tsplot-avg-list');
    modal.querySelector('.plot-zoom-title').textContent = title;
    modal.querySelector('.plot-zoom-body').innerHTML = imgEl ? imgEl.outerHTML : '';
    modal.querySelector('.plot-zoom-avgs').innerHTML = avgEl ? avgEl.outerHTML : '';
    modal.classList.remove('hidden');
  });
}


// ── Query flow diagram hover ──────────────────────────────────

function activateQueryFlowHover(container) {
  const allNodes = [...container.querySelectorAll('.qf-node-group')];
  const allEdges = [...container.querySelectorAll('.qf-edge')];

  const adj = {};
  function ensure(id) { if (!adj[id]) adj[id] = []; }

  allEdges.forEach(edge => {
    const src = edge.dataset.src;
    const dst = edge.dataset.dst;
    ensure(src);
    ensure(dst);
    adj[src].push({ other: dst, el: edge });
    adj[dst].push({ other: src, el: edge });
  });

  function getConnected(startId) {
    const visitedNodes = new Set([startId]);
    const visitedEdges = new Set();
    const queue = [startId];
    while (queue.length) {
      const cur = queue.shift();
      (adj[cur] || []).forEach(({ other, el }) => {
        visitedEdges.add(el);
        if (!visitedNodes.has(other)) {
          visitedNodes.add(other);
          queue.push(other);
        }
      });
    }
    return { nodes: visitedNodes, edges: visitedEdges };
  }

  function highlight(nid) {
    const { nodes, edges } = getConnected(nid);
    allNodes.forEach(g => {
      const lit = nodes.has(g.dataset.nid);
      g.classList.toggle('qf-dim', !lit);
      g.classList.toggle('qf-lit', lit);
    });
    allEdges.forEach(e => e.classList.toggle('qf-edge-lit', edges.has(e)));
  }

  function clear() {
    allNodes.forEach(g => { g.classList.remove('qf-dim', 'qf-lit'); });
    allEdges.forEach(e => { e.classList.remove('qf-edge-lit'); });
  }

  allNodes.forEach(g => {
    g.addEventListener('mouseenter', () => highlight(g.dataset.nid));
    g.addEventListener('mouseleave', clear);
  });
}