/* ──────────────────────────────────────────────────────────────
   script.js  –  StarVers Evaluation Dashboard
   ────────────────────────────────────────────────────────────── */

const API_BASE = '/evaluation/starvers/api';

const ALL_STEPS = [
  'download', 'preprocess_data', 'construct_datasets',
  'ingest', 'construct_queries', 'evaluate', 'visualize',
];

const step_descriptions = {
  download: 'The datasets and query sets are fetched from the provided URLs and the number of snapshots (versions) and total snapshot size are computed from the source files and displayed below.',
  preprocess_data: 'The snapshot files of the datasets and the query sets are preprocessed in different ways. The dataset triples are skolemized and validated for RDF compliance using two different RDF validators. The queries of the SciQA dataset are parsed and validated by querying them against the three evaluated triple stores. The queries are also transformed into the timestamped-based representation and also executed against the triple stores. If a query is invalid in at least one of the triple stores in either original or timestamped form, it is excluded from the evaluation.',
  construct_datasets: 'Four different dataset variants are constructed from the snapshot files. Three of them use a certain RDF-based versioning approach and the fourth one is a simple collection of the first snapshots and the consecutive deltas/change sets, which are ingested and internally versioned by the Ostrich store.',
  ingest: 'Each dataset variant that applys versioning on RDF level is ingested into the two evaluated RDF-star triple stores, whereas the first snapshot and changesets variant is ingested into the Ostrich store. The total ingestion time is measured for one run only. The size of the ingested data is also measured and displayed below.',
  construct_queries: 'Each dataset variant has their own query form. A query is constructed from a query template for each dataset, dataset variant (versioning policy), and version. The table below shows how many queries are generated and executed in the next step.',
  evaluate: 'The evaluation loop for the query execution is shown below.',
  visualize: 'For each dataset and query set a line is plotted showing the query execution time over the versions for each dataset variant (versioning policy) and triple store combination.'
};

// ── State ─────────────────────────────────────────────────────
let runs        = [];
let activeRunTs = null;

// ── DOM refs ──────────────────────────────────────────────────
const runListEl   = document.getElementById('runList');
const runCountEl  = document.getElementById('runCount');
const placeholder = document.getElementById('placeholder');
const detailPanel = document.getElementById('detailPanel');
const runMetaEl   = document.getElementById('runMeta');
const stepsGridEl = document.getElementById('stepsGrid');
const refreshBtn  = document.getElementById('refreshBtn');

// ── Bootstrap ─────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadRuns();
  refreshBtn.addEventListener('click', loadRuns);
});

// ── Data fetching ─────────────────────────────────────────────
async function loadRuns() {
  refreshBtn.style.opacity = '0.4';
  try {
    const res = await fetch(`${API_BASE}/runs`);
    runs      = await res.json();
    renderSidebar();
    runCountEl.textContent = `${runs.length} run${runs.length !== 1 ? 's' : ''}`;
    if (activeRunTs) {
      const run = runs.find(r => r.ts === activeRunTs);
      if (run) renderDetail(run);
    }
  } catch {
    runListEl.innerHTML = `<li class="error-msg" style="margin:12px">Could not reach API — is the backend running?</li>`;
  } finally {
    refreshBtn.style.opacity = '1';
  }
}

async function loadRunDetail(ts) {
  try {
    const res = await fetch(`${API_BASE}/runs/${encodeURIComponent(ts)}`);
    return await res.json();
  } catch { return null; }
}

// ── Sidebar ───────────────────────────────────────────────────
function renderSidebar() {
  if (!runs.length) {
    runListEl.innerHTML = `<li style="padding:20px 16px;color:var(--text-faint);font-size:12px;">No evaluation runs found yet.</li>`;
    return;
  }
  runListEl.innerHTML = runs.map(run => {
    const { completed, total, overallStatus } = runStats(run);
    const pct = total ? Math.round((completed / total) * 100) : 0;
    const cls = overallStatus === 'success' ? 'complete' : overallStatus === 'failed' ? 'failed' : '';
    return `
      <li class="run-item${run.ts === activeRunTs ? ' active' : ''}" data-ts="${run.ts}">
        <span class="run-ts">${formatTs(run.ts)}</span>
        <div class="run-progress">
          <div class="progress-bar-wrap">
            <div class="progress-bar-fill ${cls}" style="width:${pct}%"></div>
          </div>
          <span class="run-steps-label">${completed}/${total}</span>
        </div>
      </li>`;
  }).join('');

  runListEl.querySelectorAll('.run-item').forEach(li => {
    li.addEventListener('click', async () => {
      activeRunTs = li.dataset.ts;
      runListEl.querySelectorAll('.run-item').forEach(x => x.classList.remove('active'));
      li.classList.add('active');
      placeholder.classList.add('hidden');
      detailPanel.classList.remove('hidden');
      stepsGridEl.innerHTML = `<div class="loading"><div class="spinner"></div>Loading…</div>`;
      const run = await loadRunDetail(activeRunTs);
      if (run) renderDetail(run);
    });
  });
}

// ── Detail view ───────────────────────────────────────────────
function renderDetail(run) {
  placeholder.classList.add('hidden');
  detailPanel.classList.remove('hidden');
  const { completed, total, overallStatus } = runStats(run);

  runMetaEl.innerHTML = `
    <div class="run-meta-label">Execution start date</div>
    <span class="run-meta-ts">${formatTs(run.ts)}</span>
    <span class="run-meta-badge ${overallStatus}">${overallStatus}</span>
    <span class="badge" style="background:var(--surface2);color:var(--text-dim);
          border:1px solid var(--border);margin-left:auto">${completed}/${total} steps</span>
  `;

  const stepMap = {};
  (run.steps || []).forEach(s => { stepMap[s.step_name] = s; });

  // Render all steps immediately as always-visible sections
  stepsGridEl.innerHTML = ALL_STEPS.map((name, i) => {
    const s = stepMap[name] || {
      step_number: i + 1, step_name: name,
      status: 'pending', start_time: '', end_time: '',
    };
    const duration  = calcDuration(s.start_time, s.end_time);
    const hasDetail = s.status === 'success' || s.status === 'failed' || s.status === 'running';

    const headerHtml = `
      <div class="step-section-header">
        <span class="step-num">${i + 1}</span>
        <span class="step-name">${name.replace(/_/g, ' ')}</span>
        <div class="step-header-right">
          ${s.start_time ? `<span class="step-time-label">start:</span><span class="step-time-val">${formatTs(s.start_time)}</span>` : ''}
          ${s.end_time   ? `<span class="step-time-label">end:</span><span class="step-time-val">${formatTs(s.end_time)}</span>` : ''}
          ${s.start_time && s.end_time ? `<span class="step-time-label">duration:</span><span class="step-time-val">${duration}</span>` : ''}
          <span class="chip ${s.status}">${s.status}</span>
        </div>
      </div>`;

    const stepDescriptionHTML = `
      <div class="step-description">
        ${step_descriptions?.[name] || 'No description available.'}
      </div>
    `;

    let bodyHtml;
    if (!hasDetail && s.status !== 'running') {
      bodyHtml = `
        <div class="step-section-body">
          <div class="dim" style="font-size:12px">
            ${s.status === 'pending' ? 'Step has not run yet.' : 'No detail data available.'}
          </div>
        </div>`;
    } else {
      // Content placeholder — will be filled after fetch (also used for running steps)
      bodyHtml = `
        <div class="step-section-body" id="body-${name}">
          <div class="loading"><div class="spinner"></div>${s.status === 'running' ? 'Step is currently running…' : 'Loading details…'}</div>
        </div>`;
    }

    return `
      <div class="step-section" id="step-${name}">
        ${headerHtml}
        ${stepDescriptionHTML}
        ${bodyHtml}
      </div>`;
  }).join('');

  // Fetch detail for all completed/failed steps in parallel
  ALL_STEPS.forEach((name) => {
    const s = stepMap[name];
    if (!s || (s.status !== 'success' && s.status !== 'failed' && s.status !== 'running')) return;
    loadStepDetail(run.ts, name);
  });
}

async function loadStepDetail(ts, name) {
  const bodyEl = document.getElementById(`body-${name}`);
  if (!bodyEl) return;
  try {
    const res  = await fetch(`${API_BASE}/step-detail/${encodeURIComponent(ts)}/${name}`);
    const info = await res.json();

    const stepEl   = document.getElementById(`step-${name}`);
    const isRunning = stepEl?.querySelector('.chip.running') !== null;

    const runningBanner = isRunning
      ? `<div class="loading" style="margin-bottom:12px">
           <div class="spinner"></div>
           <span style="font-size:12px;color:var(--text-dim)">Step is currently running — showing partial results</span>
         </div>`
      : '';

    const detailHtml = renderStepInfo(name, info);
    bodyEl.innerHTML = runningBanner + (detailHtml ||
      `<div class="dim" style="font-size:12px">No partial results available yet.</div>`);

    // Activate hover effects for query flow diagrams (if any)
    bodyEl
    .querySelectorAll('.query-flow-container')
    .forEach(activateQueryFlowHover);

    if (isRunning) {
      setTimeout(() => loadStepDetail(ts, name), 10000);
    }
  } catch (e) {
    bodyEl.innerHTML = `<div class="error-msg">Could not load step details: ${e.message}</div>`;
  }
}

function activateQueryFlowHover(container) {
    const allNodes = [...container.querySelectorAll('.qf-node-group')];
    const allEdges = [...container.querySelectorAll('.qf-edge')];

    // Build adjacency from edge data attributes
    const adj = {};
    function ensAdj(id) { if (!adj[id]) adj[id] = []; }

    allEdges.forEach(edge => {
        const src = edge.dataset.src;
        const dst = edge.dataset.dst;
        const eid = edge.dataset.eid;
        ensAdj(src);
        ensAdj(dst);
        adj[src].push({ eid, other: dst, el: edge });
        adj[dst].push({ eid, other: src, el: edge });
    });

    // BFS: find all connected nodes and edges from a starting node
    function getConnected(startId) {
        const visitedNodes = new Set([startId]);
        const visitedEdges = new Set();
        const queue = [startId];
        while (queue.length) {
            const cur = queue.shift();
            (adj[cur] || []).forEach(({ eid, other, el }) => {
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
            const isLit = nodes.has(g.dataset.nid);
            g.classList.toggle('dim', !isLit);
            g.classList.toggle('lit', isLit);
        });
        allEdges.forEach(e => {
            e.classList.toggle('lit', edges.has(e));
        });
    }

    function clearHighlight() {
        allNodes.forEach(g => { g.classList.remove('dim', 'lit'); });
        allEdges.forEach(e => { e.classList.remove('lit'); });
    }

    allNodes.forEach(g => {
        g.addEventListener('mouseenter', () => highlight(g.dataset.nid));
        g.addEventListener('mouseleave', clearHighlight);
    });
}

function renderQueryFlowDiagram(queryTable, colCounts = {}) {

  const STORES_ALL = [
    {id:'graphdb', color:'#185FA5', label:'GraphDB'},
    {id:'jena',    color:'#BA7517', label:'Jena TDB2'},
    {id:'ostrich', color:'#9B3DB8', label:'Ostrich'},
  ];
  // Fix 1: Ostrich has no timestamped form — only 2 stores for S2→S3
  const STORES_TS = STORES_ALL.filter(s => s.id !== 'ostrich');

  function validOrig(q, sid) {
    if (sid==='graphdb')  return !q.invalid_in_graphdb;
    if (sid==='jena')     return !q.invalid_in_jena;
    if (sid==='ostrich')  return !q.invalid_in_ostrich;
    return true;
  }
  function validTs(q, sid) {
    if (sid==='graphdb')  return !q.invalid_in_graphdb && !q.malformed_graphdb;
    if (sid==='jena')     return !q.invalid_in_jena    && !q.malformed_jena;
    return true;
  }

  // Fix 3: S2 partitions are non-exclusive — a query appears in VALID if ≥1
  // store says orig-valid, AND in INVALID if ≥1 store says orig-invalid.
  const s2ValidSet   = new Set(queryTable.filter(q => STORES_ALL.some(s =>  validOrig(q,s.id))).map(q=>q.query));
  const s2InvalidSet = new Set(queryTable.filter(q => STORES_ALL.some(s => !validOrig(q,s.id))).map(q=>q.query));
  const s2Valid   = queryTable.filter(q => s2ValidSet.has(q.query));
  const s2Invalid = queryTable.filter(q => s2InvalidSet.has(q.query));

  // S3: same non-exclusive logic, only for orig-valid stores
  const s3ValidSet = new Set(), s3InvalidSet = new Set();
  queryTable.forEach(q => {
    STORES_TS.forEach(s => {
      if (!validOrig(q, s.id)) return;
      if ( validTs(q, s.id))   s3ValidSet.add(q.query);
      if (!validTs(q, s.id))   s3InvalidSet.add(q.query);
    });
  });
  const s3Valid   = queryTable.filter(q => s3ValidSet.has(q.query));
  const s3Invalid = queryTable.filter(q => s3InvalidSet.has(q.query));

  // Fix 2: green only if ALL stores send an edge to this node
  function s2ValidIncoming(q)  { return STORES_ALL.filter(s =>  validOrig(q,s.id)).length; }
  function s3ValidIncoming(q)  { return STORES_TS.filter(s =>   validOrig(q,s.id) &&  validTs(q,s.id)).length; }
  function nodeColor(incoming, max) {
    return (incoming > 0 && incoming === max) ? '#C6E8CC' : '#ffffff';
  }

  /* ── layout ── */
  const BOX_W=22, BOX_H=80, PORT_R=3;
  const STAGE_Y=[60,290,520];
  const DX=30, PGAP=48, MARGIN=36;
  const ANN_W=160, LEGEND_H=72;

  function rowW(arr){ return arr.length*DX; }
  function partXs(a,b){ return {v:0, i:rowW(a)+PGAP, total:rowW(a)+PGAP+rowW(b)}; }

  const s1W=rowW(queryTable), s2ps=partXs(s2Valid,s2Invalid), s3ps=partXs(s3Valid,s3Invalid);
  const contentW=Math.max(s1W,s2ps.total,s3ps.total);
  const SVG_W=contentW+MARGIN*2+ANN_W, SVG_H=STAGE_Y[2]+BOX_H+LEGEND_H+48;

  function stageOX(w){ return MARGIN+(contentW-w)/2; }
  const s1X=stageOX(s1W), s2X=stageOX(s2ps.total), s3X=stageOX(s3ps.total);

  function portX(cx, idx, count){
    const pad=4, span=BOX_W-2*pad;
    return count<=1 ? cx : (cx-BOX_W/2)+pad+idx*(span/(count-1));
  }

  /* ── CSS (injected once) ── */
  if (!document.getElementById('qfd-css')) {
    const s=document.createElement('style'); s.id='qfd-css';
    s.textContent=`
      .qf-edge{fill:none;stroke-width:1.5;stroke-linecap:round;opacity:.25;transition:opacity .12s,stroke-width .12s}
      .qf-edge.lit{opacity:.88;stroke-width:2.5}
      .qf-node-group .qf-node-box{transition:filter .12s,opacity .12s}
      .qf-node-group.dim .qf-node-box,.qf-node-group.dim .qf-node-text,.qf-node-group.dim .qf-port-dot{opacity:.15}
      .qf-node-group.lit .qf-node-box{filter:drop-shadow(0 0 3px rgba(0,0,0,.22))}
      .qf-port-dot{stroke:#fff;stroke-width:1}`;
    document.head.appendChild(s);
  }

  const uid = 'qfd_' + Date.now();
  const nodePos={}, adj={};
  let eidN=0;

  function ensAdj(id){ if(!adj[id]) adj[id]=[]; }

  /* ── node + edge SVG strings ── */
  let bgSvg='', edgeSvg='', nodeSvg='', portSvg='', labelSvg='';

  function partBgSvg(items, ox, sy, color, label){
    if(!items.length) return '';
    const x=ox-8, w=items.length*DX+6, y=sy-22, h=BOX_H+40;
    return `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="6" fill="${color}" opacity=".11"/>
            <text x="${x+4}" y="${sy-26}" font-size="11" font-weight="600"
                  font-family="Inter,sans-serif" fill="${color}">${escHtml(label)}</text>`;
  }

  bgSvg += partBgSvg(s2Valid,   s2X+s2ps.v, STAGE_Y[1], '#1A7C3A', 'Valid (original)');
  bgSvg += partBgSvg(s2Invalid, s2X+s2ps.i, STAGE_Y[1], '#B02020', 'Invalid (original)');
  bgSvg += partBgSvg(s3Valid,   s3X+s3ps.v, STAGE_Y[2], '#1A7C3A', 'Valid (timestamped)');
  bgSvg += partBgSvg(s3Invalid, s3X+s3ps.i, STAGE_Y[2], '#B02020', 'Invalid (timestamped)');

  function stageLblSvg(text, y){
    return `<text x="${MARGIN}" y="${y}" font-size="11" font-weight="600" letter-spacing=".04em"
                  font-family="Inter,sans-serif" fill="#888">${escHtml(text)}</text>`;
  }
  labelSvg += stageLblSvg('Stage 1 — original queries',    STAGE_Y[0]-22);
  labelSvg += stageLblSvg('Stage 2 — original validity',   STAGE_Y[1]-40);
  labelSvg += stageLblSvg('Stage 3 — timestamped validity',STAGE_Y[2]-40);

  function addNodeSvg(id, cx, topY, fill, stroke, label){
    nodePos[id]={cx, topY, botY:topY+BOX_H};
    nodeSvg += `<g class="qf-node-group" data-nid="${escHtml(id)}" style="cursor:pointer">
      <rect class="qf-node-box" x="${cx-BOX_W/2}" y="${topY}" width="${BOX_W}" height="${BOX_H}" rx="3"
            fill="${fill}" stroke="${stroke}" stroke-width="1"/>
      <text class="qf-node-text" x="${cx}" y="${topY+BOX_H/2}"
            text-anchor="middle" dominant-baseline="central"
            transform="rotate(-90,${cx},${topY+BOX_H/2})"
            font-size="10" font-family="monospace" fill="#222">${escHtml(label)}</text>
    </g>`;
  }

  // Stage 1
  queryTable.forEach((q,i)=>{
    addNodeSvg('s1_'+q.query, s1X+i*DX+BOX_W/2, STAGE_Y[0], '#f5f5f5','#aaa', q.query);
  });
  // Stage 2 valid
  s2Valid.forEach((q,i)=>{
    const fill=nodeColor(s2ValidIncoming(q), STORES_ALL.length);
    addNodeSvg('s2v_'+q.query, s2X+s2ps.v+i*DX+BOX_W/2, STAGE_Y[1], fill,'#1A7C3A', q.query);
  });
  // Stage 2 invalid (always white — presence here means ≥1 store failed)
  s2Invalid.forEach((q,i)=>{
    addNodeSvg('s2i_'+q.query, s2X+s2ps.i+i*DX+BOX_W/2, STAGE_Y[1], '#ffffff','#B02020', q.query);
  });
  // Stage 3 valid
  s3Valid.forEach((q,i)=>{
    const fill=nodeColor(s3ValidIncoming(q), STORES_TS.length);
    addNodeSvg('s3v_'+q.query, s3X+s3ps.v+i*DX+BOX_W/2, STAGE_Y[2], fill,'#1A7C3A', q.query);
  });
  // Stage 3 invalid (always white)
  s3Invalid.forEach((q,i)=>{
    addNodeSvg('s3i_'+q.query, s3X+s3ps.i+i*DX+BOX_W/2, STAGE_Y[2], '#ffffff','#B02020', q.query);
  });

  function addEdgeSvg(srcId, dstId, portIdx, portCount, color){
    const src=nodePos[srcId], dst=nodePos[dstId];
    if(!src||!dst) return;
    const sx=portX(src.cx,portIdx,portCount), sy=src.botY;
    const dx=portX(dst.cx,portIdx,portCount), dy=dst.topY;
    const my=(sy+dy)/2;
    const eid='e'+(eidN++);
    edgeSvg+=`<path class="qf-edge" data-eid="${eid}" data-src="${escHtml(srcId)}" data-dst="${escHtml(dstId)}"
       stroke="${color}" fill="none"
       d="M${sx},${sy} C${sx},${my} ${dx},${my} ${dx},${dy}"/>`;
    portSvg+=`<circle class="qf-port-dot" cx="${sx}" cy="${sy}" r="${PORT_R}" fill="${color}"/>`;
    portSvg+=`<circle class="qf-port-dot" cx="${dx}" cy="${dy}" r="${PORT_R}" fill="${color}"/>`;
    ensAdj(srcId); adj[srcId].push({eid,other:dstId});
    ensAdj(dstId); adj[dstId].push({eid,other:srcId});
  }

  // Stage 1 → Stage 2: each store independently, always STORES_ALL.length ports
  queryTable.forEach(q => {
    STORES_ALL.forEach((s,si) => {
      const dstId = validOrig(q,s.id) ? 's2v_'+q.query : 's2i_'+q.query;
      addEdgeSvg('s1_'+q.query, dstId, si, STORES_ALL.length, s.color);
    });
  });

  // Stage 2 valid → Stage 3: STORES_TS only, 2 ports
  queryTable.forEach(q => {
    if (!s2ValidSet.has(q.query)) return;
    STORES_TS.forEach((s,si) => {
      if (!validOrig(q, s.id)) return; // this store was orig-invalid
      const dstId = validTs(q,s.id) ? 's3v_'+q.query : 's3i_'+q.query;
      addEdgeSvg('s2v_'+q.query, dstId, si, STORES_TS.length, s.color);
    });
  });

  /* ── side annotations ── */
  const annX=SVG_W-ANN_W+8;
  const midY12=(STAGE_Y[0]+BOX_H+STAGE_Y[1])/2-28;
  const midY23=(STAGE_Y[1]+BOX_H+STAGE_Y[2])/2-20;

  function annSvg(title, items, y){
    let s=`<text x="${annX}" y="${y}" font-size="9" font-weight="600"
                 font-family="Inter,sans-serif" fill="#888">${escHtml(title)}</text>`;
    items.forEach(({color,label,count,total},i)=>{
      const ty=y+14+i*16;
      s+=`<rect x="${annX}" y="${ty-7}" width="8" height="8" rx="2" fill="${color}"/>`;
      s+=`<text x="${annX+12}" y="${ty}" font-size="10" font-family="Inter,sans-serif"
               fill="#555">${escHtml(label)}: ${count}/${total}</text>`;
    });
    return s;
  }

  labelSvg += annSvg('valid (original)', STORES_ALL.map(s=>({
    color:s.color, label:s.label,
    count: colCounts['valid_in_'+(s.id==='jena'?'jena':s.id)] ??
           queryTable.filter(q=>validOrig(q,s.id)).length,
    total: queryTable.length,
  })), midY12);
  labelSvg += annSvg('valid (timestamped)', STORES_TS.map(s=>({
    color:s.color, label:s.label,
    count: colCounts['valid_trans_in_'+(s.id==='jena'?'jena':s.id)] ??
           queryTable.filter(q=>validTs(q,s.id)).length,
    total: queryTable.length,
  })), midY23);

  /* ── legend ── */
  const LY=SVG_H-LEGEND_H+12;
  labelSvg+=`<text x="${MARGIN}" y="${LY}" font-size="10" font-weight="600"
    font-family="Inter,sans-serif" fill="#888">Triple store color</text>`;
  STORES_ALL.forEach((s,i)=>{
    const lx=MARGIN+i*120;
    labelSvg+=`<rect x="${lx}" y="${LY+14}" width="24" height="5" rx="2" fill="${s.color}"/>`;
    labelSvg+=`<text x="${lx+28}" y="${LY+20}" font-size="10" font-family="Inter,sans-serif"
                     fill="#333">${escHtml(s.label)}</text>`;
  });
  const gfx=MARGIN+STORES_ALL.length*120;
  labelSvg+=`<rect x="${gfx}" y="${LY+10}" width="14" height="10" rx="2"
    fill="#C6E8CC" stroke="#1A7C3A" stroke-width="1"/>`;
  labelSvg+=`<text x="${gfx+18}" y="${LY+20}" font-size="10" font-family="Inter,sans-serif"
    fill="#333">Valid in all stores</text>`;
  const wfx=gfx+130;
  labelSvg+=`<rect x="${wfx}" y="${LY+10}" width="14" height="10" rx="2"
    fill="#fff" stroke="#888" stroke-width="1"/>`;
  labelSvg+=`<text x="${wfx+18}" y="${LY+20}" font-size="10" font-family="Inter,sans-serif"
    fill="#333">Partially valid</text>`;

  /* ── adjacency JSON for inline script ── */
  const adjJson = JSON.stringify(adj);

  return `
<div class="query-flow-container" style="overflow-x:auto;overflow-y:hidden;max-width:100%">
<svg id="${uid}" width="${SVG_W}" height="${SVG_H}" viewBox="0 0 ${SVG_W} ${SVG_H}"
     style="display:block;min-width:${SVG_W}px">
  <g>${bgSvg}${labelSvg}</g>
  <g>${edgeSvg}</g>
  <g>${nodeSvg}</g>
  <g>${portSvg}</g>
</svg>
</div>`;
}

// ── Step detail renderers ─────────────────────────────────────
function renderStepInfo(stepName, info) {
  if (!info || Object.keys(info).length === 0)
    return `<div class="dim" style="font-size:12px;padding:4px 0">No detail data available for this step yet.</div>`;

  const sections = [];

// ── download ────────────────────────────────────────────────
  if (stepName === 'download') {
    
    if (info.datasets?.length) {
      const cards = info.datasets.map(d => {
        // Query sets for this dataset
        const qsHtml = (d.query_sets || []).map(qs => {
          const linkItems = (qs.links || []).map(l =>
            `<a class="link" href="${escHtml(l.url)}" target="_blank"
                style="display:inline-block;margin-right:8px;font-size:11px">
               ↗ ${escHtml(l.filename)}
             </a>`
          ).join('');
          return `
            <div style="display:flex;align-items:baseline;gap:8px;margin-top:4px">
              <span style="font-size:11px;font-weight:600;color:var(--text-dim);
                           min-width:60px;font-family:var(--font-mono)">
                ${escHtml(qs.name)}
              </span>
              <span style="font-size:11px;color:var(--text-faint)">${linkItems || '—'}</span>
            </div>`;
        }).join('');

        return `
          <div style="
            background:var(--surface);
            border:1px solid var(--border);
            border-radius:var(--radius-lg);
            overflow:hidden;
            display:flex;
            flex-direction:column;
          ">
            <!-- Dataset title bar -->
            <div style="
              background:var(--c-blue);
              color:#fff;
              padding:10px 14px;
              font-family:var(--font-mono);
              font-size:13px;
              font-weight:700;
              letter-spacing:0.04em;
            ">${escHtml(d.name)}</div>

            <!-- Body -->
            <div style="padding:14px 16px;flex:1;display:flex;flex-direction:column;gap:12px">

              <!-- Description -->
              <p style="font-size:12px;color:var(--text-dim);line-height:1.6;margin:0">
                ${escHtml(d.description || '')}
              </p>

              <!-- Key facts -->
              <div class="kv-grid" style="grid-template-columns:auto 1fr">
                <span class="kv-key">Versions</span>
                <span class="kv-val">${fmt(d.versions)}</span>
                <span class="kv-key">All Snapshots Size</span>
                <span class="kv-val">${d.size_mb != null ? fmtMb(d.size_mb) : '—'}</span>
                <span class="kv-key">Source</span>
                <span class="kv-val">${d.download_link
                  ? `<a class="link" href="${escHtml(d.download_link)}" target="_blank">↗ link</a>`
                  : '—'}</span>
              </div>

              <!-- Query sets -->
              ${qsHtml ? `
                <div>
                  <div style="font-size:10px;text-transform:uppercase;letter-spacing:0.08em;
                              color:var(--c-blue);font-weight:700;margin-bottom:4px;
                              font-family:var(--font-mono)">Query Sets</div>
                  ${qsHtml}
                </div>` : ''}
            </div>
          </div>`;
      }).join('');

      sections.push(section('Datasets', `
        <div style="
          display:grid;
          grid-template-columns:repeat(2, 1fr);
          gap:16px;
        ">${cards}</div>`));
    }
  }

// ── preprocess ────────────────────────────────────────────────

  if (stepName === 'preprocess_data') {

    // ── Step 1: Skolemization and Dataset Validation ─────────
    let step1Body = '';

    if (info.validators) {
      step1Body += `
        <div style="display:flex;gap:24px;margin-bottom:14px;flex-wrap:wrap">
          <span style="font-size:12px;font-weight:600;">Parsers:</span>
          <span style="font-size:12px;color:var(--text-dim)">
            <span style="font-weight:600;color:var(--text)">RDF4J</span>
            &nbsp;<span class="mono" style="color:var(--c-blue)">${escHtml(info.validators.rdf4j || '—')}</span>
          </span>
          <span style="font-size:12px;color:var(--text-dim)">
            <span style="font-weight:600;color:var(--text)">Apache Jena</span>
            &nbsp;<span class="mono" style="color:var(--c-blue)">${escHtml(info.validators.jena || '—')}</span>
          </span>
        </div>`;
    }

    if (info.skolemization_per_dataset?.length) {
      const rows = info.skolemization_per_dataset.map(d => `
        <tr>
          <td><strong>${d.dataset}</strong></td>
          <td>${fmt(d.subject)}</td>
          <td>${fmt(d.object)}</td>
          <td>${fmt(d.invalid)}</td>
          <td>${d.invalid_avg != null ? d.invalid_avg.toLocaleString('en-US', {maximumFractionDigits:2}) : '—'}</td>
        </tr>`).join('');
      step1Body += `
        <table class="data-table">
          <thead><tr>
            <th>Dataset</th><th>Blank nodes (subject)</th>
            <th>Blank nodes (object)</th><th>Invalid triples (total)</th>
            <th>Invalid triples (avg / snapshot)</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    if (step1Body) {
      sections.push(section('Step 1: Skolemization and Dataset Validation', step1Body));
    }

    // ── Step 2: Query Parsing and Validation ─────────────────
    let step2Body = '';

    if (info.sciqa_query_table?.length) {
      const total              = info.sciqa_total ?? info.sciqa_query_table.length;
      const valid_orig_graphdb = info.sciqa_col_counts?.valid_in_graphdb ?? info.sciqa_query_table.filter(r => r.valid_in_graphdb).length;
      const valid_orig_jena    = info.sciqa_col_counts?.valid_in_jena    ?? info.sciqa_query_table.filter(r => r.valid_in_jena).length;
      const excluded           = info.sciqa_query_table.filter(r => r.excluded).length;
      const kept               = total - excluded;
      const cc                 = info.sciqa_col_counts || {};

      const flag = v => v
        ? `<span class="sciqa-flag sciqa-flag--yes">✕</span>`
        : `<span class="sciqa-flag sciqa-flag--no">✓</span>`;

      const rows = info.sciqa_query_table.map(r => {
        const rowClass = r.excluded ? 'sciqa-excluded' : 'sciqa-kept';
        return `<tr class="${rowClass}">
          <td class="mono sciqa-query">${escHtml(r.query)}</td>
          <td>${flag(r.invalid_in_graphdb)}</td>
          <td>${flag(r.malformed_graphdb)}</td>
          <td>${flag(r.invalid_in_jena)}</td>
          <td>${flag(r.malformed_jena)}</td>
          <td>${flag(r.invalid_in_ostrich)}</td>
        </tr>`;
      }).join('');

      const hdr = (label, key, query_count) =>
        `<th style="text-align:center">${label}<br><span style="font-weight:400;font-size:10px">(${cc[key]??0}/${query_count})</span></th>`;

      step2Body += renderQueryFlowDiagram(info.sciqa_query_table, info.sciqa_col_counts || {});

    }

    if (info.query_counts?.length) {
      const qcRows = info.query_counts.map(q => `
        <tr>
          <td><strong>${escHtml(q.query_set)}</strong></td>
          <td>${escHtml(q.for_dataset)}</td>
          <td>${fmt(q.count)}</td>
        </tr>`).join('');
      step2Body += `
        <div style="margin-top:16px">
          <table class="data-table">
            <thead><tr><th>Query Set</th><th>Dataset</th><th>Queries</th></tr></thead>
            <tbody>${qcRows}</tbody>
          </table>
        </div>`;
    }

    if (step2Body) {
      sections.push(section('Step 2: Query Parsing and Validation (ORKG queries)', step2Body));
    }
  }

  // ── construct_datasets ───────────────────────────────────────
  if (stepName === 'construct_datasets') {
    if (info.variants?.length) {
      const byVariant = {};
      info.variants.forEach(v => {
        if (!byVariant[v.variant])
          byVariant[v.variant] = { approach: v.versioning_approach, datasets: [] };
        byVariant[v.variant].datasets.push({ dataset: v.dataset, size_mb: v.size_mb });
      });

      const variantHtml = `
        <div class="variants-grid">
          ${Object.entries(byVariant).map(([variant, data]) => {
            const dsRows = data.datasets.map(d =>
              `<tr><td>${d.dataset}</td><td>${d.size_mb != null ? fmtMb(d.size_mb) : '—'}</td></tr>`
            ).join('');

            return `
              <div class="variant-section">
                <div class="variant-title">${variant}</div>
                <div class="variant-body">
                  <table class="data-table" style="margin-bottom:12px">
                    <thead><tr><th>Dataset</th><th>Size</th></tr></thead>
                    <tbody>${dsRows}</tbody>
                  </table>
                  <div class="variant-approach-label">RDF Versioning Model</div>
                  <div>Case: A triple with two validation periods .</div>
                  <div class="variant-approach">${escHtml(data.approach || '—')}</div>
                </div>
              </div>`;
          }).join('')}
        </div>
      `;
      sections.push(section('Dataset Variants', variantHtml));
    }
  }

  // ── ingest ───────────────────────────────────────────────────
  if (stepName === 'ingest') {
    if (info.ingestion_summary?.length) {
      sections.push(section('Ingestion Results — avg over 10 runs',
        renderIngestChart(info.ingestion_summary)));
    }
  }

  // ── Ingest dual-panel SVG chart ───────────────────────────────
function renderIngestChart(rows) {
  // One colour per triple store — maximally distinct hues
  const TS_COLORS = {
    graphdb:           '#006699',   // blue
    jenatdb2:          '#E18922',   // orange
    ostrich:           '#BA4682',   // magenta
    ostrich_aggchange: '#007E71',   // teal
  };
  const COLOR_FALLBACKS = ['#000000','#646363','#5485AB','#6AAAA5','#EEB473'];
  const tsColorMap = {};
  let fallbackIdx = 0;
  rows.forEach(r => {
    if (!tsColorMap[r.triplestore])
      tsColorMap[r.triplestore] = TS_COLORS[r.triplestore]
        || COLOR_FALLBACKS[fallbackIdx++ % COLOR_FALLBACKS.length];
  });

  // Group rows by dataset
  const byDataset = {};
  rows.forEach(r => {
    if (!byDataset[r.dataset]) byDataset[r.dataset] = [];
    byDataset[r.dataset].push(r);
  });
  const datasets = Object.keys(byDataset).sort();

  // Layout per mini-chart
  const labelW  = 160;
  const panelW  = 200;
  const gap     = 48;
  const padTop  = 32;
  const padBot  = 12;
  const barH    = 14;
  const barGap  = 8;
  const rowH    = barH + barGap;
  const chartW  = labelW + panelW + gap + panelW + 24;

  // Log scale for time — fixed domain 0.1s … 10000s
  const timeUpper = 100000;
  const timeTicks  = [1, 10, 100, 1000, 10000, 100000];
  const timeLabels = ['1s', '10s', '1m40s', '16.6m', '2.77h', '1.16d'];

  function timeX(sec) {
    return (Math.log10(Math.max(sec, 0.1)) / Math.log10(timeUpper)) * panelW;
  }

  function buildChart(dataset) {
    const dataRows = [...byDataset[dataset]]
      .sort((a, b) => a.avg_ingestion_time - b.avg_ingestion_time);
    const n      = dataRows.length;
    const chartH = padTop + n * rowH + padBot;

    // DB size scale — linear, rounded up to nice value
    const maxDb   = Math.max(...dataRows.map(r => r.avg_db_size_mib), 1);
    const dbUpper = Math.ceil(maxDb / 100) * 100 || 100;
    function dbX(mib) { return (mib / dbUpper) * panelW; }

    function rowY(i) { return padTop + i * rowH; }

    // X-axis ticks — time (log)
    const timeTickSvg = timeTicks.map((t, i) => {
      const x = timeX(t);
      return `
        <line x1="${x}" y1="${padTop - 5}" x2="${x}" y2="${padTop + n * rowH}"
              stroke="#D0D0D0" stroke-width="1" stroke-dasharray="3,2"/>
        <text x="${x}" y="${padTop - 8}" text-anchor="middle"
              font-size="8" fill="#9D9D9C" font-family="JetBrains Mono,monospace">
          ${timeLabels[i]}
        </text>`;
    }).join('');

    // X-axis ticks — DB size (linear, 4 steps)
    const dbTickSvg = [0, 1, 2, 3, 4].map(i => {
      const val = (dbUpper / 4) * i;
      const x   = dbX(val);
      const gib = val / 1024;
      const lbl = val === 0 ? '0'
        : gib < 0.1  ? Math.round(val) + 'M'
        : gib < 10   ? gib.toFixed(1) + 'G'
        : gib.toFixed(0) + 'G';
      return `
        <line x1="${x}" y1="${padTop - 5}" x2="${x}" y2="${padTop + n * rowH}"
              stroke="#D0D0D0" stroke-width="1" stroke-dasharray="3,2"/>
        <text x="${x}" y="${padTop - 8}" text-anchor="middle"
              font-size="8" fill="#9D9D9C" font-family="JetBrains Mono,monospace">
          ${lbl}
        </text>`;
    }).join('');

    // Y-axis labels: triplestore / policy
    const labelsSvg = dataRows.map((r, i) => {
      const y   = rowY(i) + barH / 2 + 4;
      const lbl = `${r.triplestore} / ${r.policy}`;
      const short = lbl.length > 28 ? lbl.slice(0, 27) + '…' : lbl;
      return `<text x="${labelW - 6}" y="${y}" text-anchor="end"
        font-size="9" fill="#646363"
        font-family="JetBrains Mono,monospace">${escHtml(short)}</text>`;
    }).join('');

    // DB size bars
    const dbBarsSvg = dataRows.map((r, i) => {
      const y   = rowY(i);
      const w   = Math.max(dbX(r.avg_db_size_mib), 2);
      const col = tsColorMap[r.triplestore];
      const gib = r.avg_db_size_mib / 1024;
      const lbl = gib < 0.01
        ? r.avg_db_size_mib.toFixed(0) + 'MiB'
        : gib < 10 ? gib.toFixed(2) + 'GiB'
        : gib.toFixed(1) + 'GiB';
      return `
        <rect x="0" y="${y}" width="${w}" height="${barH}"
              fill="${col}" rx="2" opacity="0.85"/>
        <text x="${w + 3}" y="${y + barH - 2}" font-size="8"
              fill="#646363" font-family="JetBrains Mono,monospace">${lbl}</text>`;
    }).join('');

    // Ingest time bars
    const timeBarsSvg = dataRows.map((r, i) => {
      const y   = rowY(i);
      const w   = Math.max(timeX(r.avg_ingestion_time), 2);
      const col = tsColorMap[r.triplestore];
      const t   = r.avg_ingestion_time;
      const lbl = t >= 3600 ? (t/3600).toFixed(2)+'h'
        : t >= 60 ? (t/60).toFixed(1)+'m'
        : t.toFixed(1)+'s';
      return `
        <rect x="0" y="${y}" width="${w}" height="${barH}"
              fill="${col}" rx="2" opacity="0.85"/>
        <text x="${w + 3}" y="${y + barH - 2}" font-size="8"
              fill="#646363" font-family="JetBrains Mono,monospace">${lbl}</text>`;
    }).join('');

    const timePanelX = labelW + panelW + gap;
    const sepX       = labelW + panelW + gap / 2;

    return { svg: `
      <text x="${chartW / 2}" y="13" text-anchor="middle"
            font-size="12" font-weight="700" fill="#006699"
            font-family="Inter,sans-serif">${escHtml(dataset)}</text>

      <!-- DB Size panel header -->
      <text x="${labelW + panelW/2}" y="26" text-anchor="middle"
            font-size="10" font-weight="600" fill="#646363"
            font-family="Inter,sans-serif">DB Size</text>

      <!-- Ingest Time panel header -->
      <text x="${timePanelX + panelW/2}" y="26" text-anchor="middle"
            font-size="10" font-weight="600" fill="#646363"
            font-family="Inter,sans-serif">Ingest Time (log)</text>

      <!-- Column separator -->
      <line x1="${sepX}" y1="16" x2="${sepX}" y2="${padTop + n * rowH + 16}"
            stroke="#D0D0D0" stroke-width="1.5"/>

      <g transform="translate(0, 16)">
        <g>${labelsSvg}</g>
        <g transform="translate(${labelW}, 0)">
          ${dbTickSvg}${dbBarsSvg}
        </g>
        <g transform="translate(${timePanelX}, 0)">
          ${timeTickSvg}${timeBarsSvg}
        </g>
      </g>`,
      height: chartH + 16
    };
  }

  // Legend (shared across all charts)
  const legendItems = Object.entries(tsColorMap).map(([ts, col]) =>
    `<g>
      <rect width="10" height="10" rx="2" fill="${col}" opacity="0.85"/>
      <text x="14" y="9" font-size="10" fill="#646363"
            font-family="Inter,sans-serif">${escHtml(ts)}</text>
    </g>`
  );
  const legendSpacing = 130;
  const legendSvg = legendItems.map((item, i) =>
    `<g transform="translate(${i * legendSpacing}, 0)">${item}</g>`
  ).join('');
  const legendW = legendItems.length * legendSpacing;
  const legendH = 20;

  // Build all four charts, then arrange in 2×2 grid
  const charts = datasets.map(ds => buildChart(ds));
  const colW   = chartW + 32;
  const rows2  = Math.ceil(datasets.length / 2);
  const rowHeights = [];
  for (let row = 0; row < rows2; row++) {
    const left  = charts[row * 2];
    const right = charts[row * 2 + 1];
    rowHeights.push(Math.max(left?.height ?? 0, right?.height ?? 0) + 24);
  }
  const totalH = legendH + 12 + rowHeights.reduce((a, b) => a + b, 0);
  const totalW = colW * 2 + 16;

  let chartCells = '';
  let yOff = legendH + 12;
  for (let row = 0; row < rows2; row++) {
    const rh = rowHeights[row];
    [0, 1].forEach(col => {
      const idx = row * 2 + col;
      if (idx >= datasets.length) return;
      const xOff = col * colW;
      chartCells += `
        <g transform="translate(${xOff}, ${yOff})">
          <rect width="${chartW}" height="${charts[idx].height}"
                rx="6" fill="#ffffff" stroke="#D0D0D0" stroke-width="1"/>
          <g transform="translate(8, 8)">${charts[idx].svg}</g>
        </g>`;
    });
    yOff += rh;
  }

  const fullSvg = `
  <svg xmlns="http://www.w3.org/2000/svg"
       width="${totalW}" height="${totalH}"
       style="overflow:visible;font-family:Inter,sans-serif">
    <g transform="translate(0, 0)">${legendSvg}</g>
    ${chartCells}
  </svg>`;

  return `<div style="overflow-x:auto">${fullSvg}</div>`;
}

  // ── construct_queries ────────────────────────────────────────
  if (stepName === 'construct_queries') {
    const { query_counts, totals_per_dataset, policies, datasets } = info;
    if (datasets?.length && policies?.length && query_counts) {
      // Compact matrix: datasets as rows, policies as columns
      // Since counts are the same per policy, show one column "Queries per policy" + total
      const firstPolicy = policies[0];
      const headerCols  = `<th>Dataset</th>${policies.map(p => `<th>${p}</th>`).join('')}<th>Total</th>`;
      const bodyRows = datasets.map(ds => {
        const cells = policies.map(p =>
          `<td class="mono">${fmt(query_counts[p]?.[ds] ?? 0)}</td>`).join('');
        const total = totals_per_dataset?.[ds] ?? 0;
        return `<tr><td><strong>${ds}</strong></td>${cells}<td class="mono"><strong>${fmt(total)}</strong></td></tr>`;
      }).join('');

      // Grand total row
      const grandTotal = datasets.reduce((sum, ds) => sum + (totals_per_dataset?.[ds] ?? 0), 0);
      const policyCols = policies.map(p => {
        const pTotal = datasets.reduce((sum, ds) => sum + (query_counts[p]?.[ds] ?? 0), 0);
        return `<td class="mono"><strong>${fmt(pTotal)}</strong></td>`;
      }).join('');
      const totalRow = `<tr class="total-row"><td><strong>Total</strong></td>${policyCols}<td class="mono"><strong>${fmt(grandTotal)}</strong></td></tr>`;

      sections.push(section('Queries Constructed', `
        <table class="data-table">
          <thead><tr>${headerCols}</tr></thead>
          <tbody>${bodyRows}${totalRow}</tbody>
        </table>`));
    } else {
      sections.push(section('Queries Constructed',
        `<div class="dim" style="font-size:12px">No query files found in final_queries directory.</div>`));
    }
  }

  // ── evaluate ─────────────────────────────────────────────────
  if (stepName === 'evaluate') {

    // ── 1. Evaluation algorithm ──────────────────────────────
    // center horizontally with max width 50% so it doesn't dominate the section, but can still be legible

    const algoHtml = `
    <div style="display:flex;justify-content:center;">
      <img
        src="/evaluation/starvers/static/images/evaluation_loop_horizontal.svg"
        alt="Evaluation loop activity diagram"
        style="max-width:100%;height:auto;display:flex"
      >
    </div>`;
    sections.push(section('Evaluation Algorithm', algoHtml));

    

    // ── 3. Recorded measurements ─────────────────────────────
    if (info.time_header?.length) {
      // ── Section 1: Hardware Infos ──────────────────────────────
      const hw = info.hardware || {};
      const hwRows = Object.entries(hw).map(([k, v]) => `
        <tr>
          <td class="kv-key" style="padding:7px 12px;white-space:nowrap">${escHtml(k)}</td>
          <td class="kv-val mono" style="padding:7px 12px">${escHtml(String(v))}</td>
        </tr>`).join('');

      sections.push(section('Hardware Infos', `
        <table class="data-table" style="width:100%">
          <thead><tr><th>Property</th><th>Value</th></tr></thead>
          <tbody>${hwRows}</tbody>
        </table>`));

      // ── Section 2: Recorded Measurements ──────────────────────
      const thCells = info.time_header.map(h =>
        `<th>${escHtml(h)}</th>`).join('');

      const sampleRows = (info.time_samples || []).map(row => {
        const cells = row.map((v, i) => {
          if (i === 7 && v !== '' && !isNaN(Number(v))) {
            return `<td class="mono">${Number(v).toFixed(4)}</td>`;
          }
          return `<td class="mono">${escHtml(String(v ?? ''))}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
      }).join('');

      const totalNote = info.time_total_rows > 0
        ? `<div style="font-size:11px;color:var(--text-faint);padding:8px 12px">
            Showing 5 of ${fmt(info.time_total_rows)} rows
          </div>`
        : '';

      sections.push(section('Recorded Measurements', `
        <div style="overflow-x:auto">
          <table class="data-table">
            <thead><tr>${thCells}</tr></thead>
            <tbody>${sampleRows}</tbody>
          </table>
        </div>
        ${totalNote}`));
    }
  }

  // ── visualize ────────────────────────────────────────────────
  if (stepName === 'visualize') {
    
    if (!info.plot_data?.length) {
      sections.push(section('Query Performance Plots',
        `<div class="dim" style="font-size:12px">No measurement data found. Run the evaluate step first.</div>`));
    } else {
      sections.push(section('Query Performance Plots', renderTimePlots(info.plot_data)));
    }
  }

  return sections.length
    ? sections.join('')
    : `<div class="dim" style="font-size:12px;padding:4px 0">Details will appear here once the step completes.</div>`;
}

// ── Query performance plots ───────────────────────────────────
function renderTimePlots(plotData) {
  // Colours per policy — unchanged from before
  const POLICY_COLORS = {
    ic_sr_ng:          '#006699',
    cb_sr_ng:          '#007E71',
    tb_sr_ng:          '#E18922',
    tb_sr_rs:          '#BA4682',
    tb_sr_re:          '#9D9D9C',
    ostrich:           '#5485AB',
    ostrich_aggchange: '#000000',
  };
  const COLOR_FALLBACKS = ['#646363','#6AAAA5','#EEB473','#CD81A8'];
 
  // Collect unique policies and triple stores for the legend
  const policySet     = new Set(plotData.map(s => s.policy));
  const tsSet         = new Set(plotData.map(s => s.triplestore));
  const policyList    = [...policySet].sort();
  const tsList        = [...tsSet].sort();
 
  const policyColorMap = {};
  let ci = 0;
  policyList.forEach(p => {
    policyColorMap[p] = POLICY_COLORS[p] !== undefined
      ? POLICY_COLORS[p]
      : COLOR_FALLBACKS[ci++ % COLOR_FALLBACKS.length];
  });
 
  // ── Legend (policy → solid colour swatch) ───────────────────
  const legendItems = policyList.map(p => {
    const color = policyColorMap[p];
    return `
      <div class="tsplot-legend-item">
        <span class="tsplot-legend-swatch" style="background:${color}"></span>
        <span class="tsplot-legend-label">${escHtml(p)}</span>
      </div>`;
  }).join('');
 
  const legendHtml = `
    <div class="tsplot-legend">
      <span class="tsplot-legend-heading">Policy (line colour):</span>
      ${legendItems}
    </div>`;
 
  // ── Group data: dataset → query_set → triplestore → series[] ─
  // series = { policy, points: [[version, avg_time], …] }
  const byDataset = {};
  plotData.forEach(s => {
    if (!byDataset[s.dataset])                              byDataset[s.dataset] = {};
    if (!byDataset[s.dataset][s.query_set])                 byDataset[s.dataset][s.query_set] = {};
    if (!byDataset[s.dataset][s.query_set][s.triplestore])  byDataset[s.dataset][s.query_set][s.triplestore] = [];
    byDataset[s.dataset][s.query_set][s.triplestore].push(s);
  });
 
  const datasets = Object.keys(byDataset).sort();
 
  // ── SVG chart constants ──────────────────────────────────────
  const W     = 300;
  const H     = 190;
  const padL  = 48;
  const padR  = 10;
  const padT  = 22;
  const padB  = 32;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;
 
  // Log-scale Y: 0.001s … 30s
  const Y_MIN_LOG = Math.log10(0.001);
  const Y_MAX_LOG = Math.log10(30);
  function yScale(val) {
    if (val <= 0) return padT + plotH;
    const logV = Math.log10(Math.max(val, 0.001));
    const frac = (logV - Y_MIN_LOG) / (Y_MAX_LOG - Y_MIN_LOG);
    return padT + plotH - frac * plotH;
  }
  const yTicks      = [0.001, 0.01, 0.1, 1, 10, 30];
  const yTickLabels = ['1ms', '10ms', '100ms', '1s', '10s', '30s'];
 
  // Build one SVG for a given triplestore within a dataset/query_set
  function buildPlot(seriesList, tsName) {
    const allVersions = [...new Set(
      seriesList.flatMap(s => s.points.map(p => p[0]))
    )].sort((a, b) => Number(a) - Number(b));
 
    const xMin   = allVersions[0]  ?? 0;
    const xMax   = allVersions[allVersions.length - 1] ?? 1;
    const xRange = xMax - xMin || 1;
    function xScale(v) { return padL + ((v - xMin) / xRange) * plotW; }
 
    // Y grid + tick labels
    const yGridSvg = yTicks.map((t, i) => {
      const y = yScale(t);
      return `
        <line x1="${padL}" y1="${y}" x2="${padL + plotW}" y2="${y}"
              stroke="#EDEDED" stroke-width="1"/>
        <text x="${padL - 4}" y="${y + 4}" text-anchor="end"
              font-size="7" fill="#9D9D9C" font-family="JetBrains Mono,monospace">
          ${yTickLabels[i]}
        </text>`;
    }).join('');
 
    // X ticks (up to 6)
    const xTickCount = Math.min(6, allVersions.length);
    const step = Math.max(1, Math.floor(allVersions.length / xTickCount));
    const xTickSvg = allVersions.filter((_, i) => i % step === 0).map(v => {
      const x = xScale(Number(v));
      return `
        <line x1="${x}" y1="${padT}" x2="${x}" y2="${padT + plotH}"
              stroke="#EDEDED" stroke-width="1"/>
        <text x="${x}" y="${padT + plotH + 10}" text-anchor="middle"
              font-size="7" fill="#9D9D9C" font-family="JetBrains Mono,monospace">${v}</text>`;
    }).join('');
 
    // One solid line per policy (series)
    const linesSvg = seriesList.map(s => {
      const color = policyColorMap[s.policy] || '#666';
      const pts   = s.points.filter(p => p[1] > 0);
      if (pts.length < 2) return '';
      const d = pts.map((p, i) => {
        const x = xScale(Number(p[0]));
        const y = yScale(p[1]);
        return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
      }).join(' ');
      return `<path d="${d}" fill="none" stroke="${color}" stroke-width="1.8"
                stroke-linecap="round" stroke-linejoin="round"/>`;
    }).join('');
 
    // Title: triple store name
    const titleShort = tsName.length > 22 ? tsName.slice(0, 21) + '…' : tsName;
 
    return `
      <svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg"
           style="font-family:Inter,sans-serif;overflow:visible;display:block">
        <rect x="${padL}" y="${padT}" width="${plotW}" height="${plotH}"
              fill="#fafafa" stroke="#D0D0D0" stroke-width="0.5"/>
        ${yGridSvg}
        ${xTickSvg}
        <!-- Axis labels -->
        <text x="${padL + plotW / 2}" y="${H - 2}" text-anchor="middle"
              font-size="8" fill="#646363">Version</text>
        <text transform="rotate(-90,10,${padT + plotH / 2})"
              x="10" y="${padT + plotH / 2}" text-anchor="middle"
              font-size="8" fill="#646363">Query time (s)</text>
        ${linesSvg}
        <!-- Triple store label at top -->
        <text x="${padL + plotW / 2}" y="${padT - 4}" text-anchor="middle"
              font-size="9" font-weight="700" fill="#006699"
              font-family="Inter,sans-serif">${escHtml(titleShort)}</text>
      </svg>`;
  }
 
  // ── Assemble all dataset panels ──────────────────────────────
  const datasetPanels = datasets.map(ds => {
    const querySets = Object.keys(byDataset[ds]).sort();
 
    const qsGroups = querySets.map(qs => {
      const tsMap    = byDataset[ds][qs];
      const tsNames  = Object.keys(tsMap).sort();
 
      const plotCards = tsNames.map(ts => {
        const seriesList = tsMap[ts];
        const svgHtml    = buildPlot(seriesList, ts);
        return `<div class="tsplot-card">${svgHtml}</div>`;
      }).join('');
 
      return `
        <div class="tsplot-qs-group">
          <div class="tsplot-qs-label">${escHtml(qs)}</div>
          <div class="tsplot-plots-row">
            ${plotCards}
          </div>
        </div>`;
    }).join('');
 
    return `
      <div class="tsplot-dataset-panel">
        <div class="tsplot-dataset-heading">${escHtml(ds)}</div>
        <div class="tsplot-qs-wrap">
          ${qsGroups}
        </div>
      </div>`;
  }).join('');
 
  return legendHtml + datasetPanels;
}


function section(title, body) {
  return `<div class="detail-section">
    <div class="detail-section-title">${title}</div>
    <div class="detail-section-body">${body}</div>
  </div>`;
}

function escHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ── Number formatting ─────────────────────────────────────────
function fmt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}

function fmtMb(mb) {
  if (mb == null) return '—';
  return Number(mb).toLocaleString('en-US', { maximumFractionDigits: 1 }) + ' MiB';
}

// ── Duration with hours/days ──────────────────────────────────
function calcDuration(start, end) {
  const s = parseTs(start), e = parseTs(end);
  if (!s || !e) return '—';
  const ms = e - s;
  if (ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  const totalSec = Math.round(ms / 1000);
  if (totalSec < 60) return `${totalSec}s`;
  const totalMin = Math.floor(totalSec / 60);
  const sec      = totalSec % 60;
  if (totalMin < 60) return `${totalMin}m ${sec}s`;
  const hrs  = Math.floor(totalMin / 60);
  const mins = totalMin % 60;
  if (hrs < 24) return `${hrs}h ${mins}m`;
  const days = Math.floor(hrs / 24);
  const remH = hrs % 24;
  return `${days}d ${remH}h ${mins}m`;
}

// ── Timestamp helpers ─────────────────────────────────────────
function parseTs(ts) {
  if (!ts) return null;
  const m = ts.match(/^(\d{4})(\d{2})(\d{2})T(\d{2}:\d{2}:\d{2}(?:\.\d+)?)$/);
  if (!m) return null;
  return new Date(`${m[1]}-${m[2]}-${m[3]}T${m[4]}Z`);
}

function formatTs(ts) {
  const d = parseTs(ts);
  if (!d) return ts;
  return d.toLocaleString('en-GB', {
    year: 'numeric', month: 'short', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    timeZone: 'UTC',
  }) + ' UTC';
}

function runStats(run) {
  const steps     = run.steps || [];
  const completed = steps.filter(s => s.status === 'success').length;
  const failed    = steps.filter(s => s.status === 'failed').length;
  const total     = ALL_STEPS.length;
  const overallStatus =
    failed > 0          ? 'failed'  :
    completed === total ? 'success' :
    completed > 0       ? 'running' : 'pending';
  return { completed, total, overallStatus };
}