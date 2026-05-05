/* ──────────────────────────────────────────────────────────────
   script.js  –  StarVers Evaluation Dashboard
   ────────────────────────────────────────────────────────────── */

const API_BASE = '/evaluation/starvers/api';

const ALL_STEPS = [
  'download', 'preprocess_data', 'construct_datasets',
  'ingest', 'construct_queries', 'evaluate', 'visualize',
];

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
    <span class="badge" style="background:var(--surface2);color:var(--text-dim);border:1px solid var(--border);margin-left:auto">${completed}/${total} steps</span>
  `;

  const stepMap = {};
  (run.steps || []).forEach(s => { stepMap[s.step_name] = s; });

  stepsGridEl.innerHTML = ALL_STEPS.map((name, i) => {
    const s        = stepMap[name] || { step_number: i+1, step_name: name, status: 'pending', start_time: '', end_time: '' };
    const duration = calcDuration(s.start_time, s.end_time);
    const hasDetail = s.status === 'success' || s.status === 'failed';
    return `
      <div class="step-card${hasDetail ? ' clickable' : ''}" data-step="${name}" id="step-${name}">
        <div class="step-card-header">
          <span class="step-num">${i+1}</span>
          <span class="step-name">${name.replace(/_/g, ' ')}</span>
          <span class="chip ${s.status}">${s.status}</span>
          <span class="step-duration">${duration}</span>
          ${hasDetail ? '<span class="step-chevron">›</span>' : '<span></span>'}
        </div>
        <div class="step-detail" id="detail-${name}">
          <div class="loading"><div class="spinner"></div>Loading details…</div>
        </div>
      </div>`;
  }).join('');

  stepsGridEl.querySelectorAll('.step-card.clickable').forEach(card => {
    card.addEventListener('click', () => toggleStep(card, run));
  });
}

async function toggleStep(card, run) {
  const name     = card.dataset.step;
  const detailEl = document.getElementById(`detail-${name}`);
  const isOpen   = card.classList.contains('open');
  stepsGridEl.querySelectorAll('.step-card.open').forEach(c => c.classList.remove('open'));
  if (isOpen) return;
  card.classList.add('open');
  detailEl.innerHTML = `<div class="loading"><div class="spinner"></div>Loading details…</div>`;
  try {
    const res  = await fetch(`${API_BASE}/step-detail/${encodeURIComponent(run.ts)}/${name}`);
    const info = await res.json();
    detailEl.innerHTML = renderStepInfo(name, info);
  } catch (e) {
    detailEl.innerHTML = `<div class="error-msg">Could not load step details: ${e.message}</div>`;
  }
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

      step2Body += `
        <div style="overflow-x:auto;max-height:480px;overflow-y:auto">
          <table class="data-table">
            <thead style="position:sticky;top:0">
              <tr>
                <th colspan="1" style="text-align:center;border-right:1px solid rgba(255,255,255,0.3);border-bottom:1px solid rgba(255,255,255,0.3)">Query</th>
                <th colspan="2" style="text-align:center;border-right:1px solid rgba(255,255,255,0.3);border-bottom:1px solid rgba(255,255,255,0.3)">GraphDB</th>
                <th colspan="2" style="text-align:center;border-right:1px solid rgba(255,255,255,0.3);border-bottom:1px solid rgba(255,255,255,0.3)">Jena TDB2</th>
                <th colspan="1" style="text-align:center;border-bottom:1px solid rgba(255,255,255,0.3)">Ostrich</th>
              </tr>
              <tr>
                <th></th>
                ${hdr('Valid Original',        'valid_in_graphdb',       total)}
                ${hdr('Valid timestamped query','valid_trans_in_graphdb', valid_orig_graphdb)}
                ${hdr('Valid Original',        'valid_in_jena',          total)}
                ${hdr('Valid timestamped query','valid_trans_in_jena',    valid_orig_jena)}
                ${hdr('Valid Original',        'valid_in_ostrich',       total)}
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
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
      sections.push(section('Step 2: Query Parsing and Validation', step2Body));
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
                  <div class="variant-approach-label">RDF Versioning Approach</div>
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
    const algoHtml = `
      <div style="font-family:var(--font-mono);font-size:11px;line-height:1.9;color:var(--text-dim)">
        <div><span style="color:var(--c-blue);font-weight:700">for</span>
          &nbsp;(triple_store, policy, dataset)
          &nbsp;<span style="color:var(--c-blue);font-weight:700">in</span>
          &nbsp;combinations:</div>

        <div style="padding-left:24px">
          <span style="color:var(--c-grey-mid)">// skip if combination not registered in eval_setup.toml</span>
        </div>
        <div style="padding-left:24px">
          <span style="color:var(--c-blue);font-weight:700">if not</span>
          &nbsp;eval_combi_exists(triple_store, dataset, policy):&nbsp;
          <span style="color:var(--c-blue);font-weight:700">continue</span>
        </div>

        <div style="padding-left:24px;margin-top:6px">
          <span style="color:var(--c-teal);font-weight:600">startup</span>(triple_store, policy, dataset)
          &nbsp;<span style="color:var(--c-grey-mid)">// start the triple store process</span>
        </div>

        <div style="padding-left:24px;margin-top:6px">
          <span style="color:var(--c-blue);font-weight:700">for</span>
          &nbsp;query_set
          &nbsp;<span style="color:var(--c-blue);font-weight:700">in</span>
          &nbsp;query_sets(policy, dataset):</div>
        <div style="padding-left:48px">
          <span style="color:var(--c-blue);font-weight:700">for</span>
          &nbsp;version
          &nbsp;<span style="color:var(--c-blue);font-weight:700">in</span>
          &nbsp;versions:</div>
        <div style="padding-left:72px">
          <span style="color:var(--c-blue);font-weight:700">for</span>
          &nbsp;query
          &nbsp;<span style="color:var(--c-blue);font-weight:700">in</span>
          &nbsp;queries(version):</div>

        <div style="padding-left:96px;margin-top:4px">
          response, exec_time
          &nbsp;<span style="color:var(--c-blue);font-weight:700">=</span>
          &nbsp;<span style="color:var(--c-teal);font-weight:600">execute</span
          >(query,
          &nbsp;<span style="color:var(--c-orange);font-weight:600">timeout=30s</span>)
        </div>
        <div style="padding-left:96px">
          <span style="color:var(--c-teal);font-weight:600">record</span
          >(triple_store, dataset, policy, query_set, version, exec_time, yn_timeout)
        </div>
        <div style="padding-left:96px">
          <span style="color:var(--c-teal);font-weight:600">save_result_set</span
          >(response &rarr; result_sets/…)
        </div>

        <div style="padding-left:96px;margin-top:4px;color:var(--c-grey-mid)">
          // on crash or memory pressure (&gt;85%): restart triple store, continue
        </div>

        <div style="padding-left:24px;margin-top:6px">
          <span style="color:var(--c-teal);font-weight:600">write_results</span
          >(rows &rarr; time.csv)
        </div>
        <div style="padding-left:24px">
          <span style="color:var(--c-teal);font-weight:600">shutdown</span>(triple_store)
          &nbsp;<span style="color:var(--c-grey-mid)">// shut down after each query set</span>
        </div>
      </div>`;

    sections.push(section('Evaluation Algorithm', algoHtml));

    // ── 2. Evaluation matrix ─────────────────────────────────
    if (info.combinations?.length) {
      const byTs = {};
      info.combinations.forEach(c => {
        if (!byTs[c.triplestore]) byTs[c.triplestore] = [];
        byTs[c.triplestore].push(c);
      });

      const matrixHtml = Object.entries(byTs).map(([ts, rows]) => {
        const tableRows = rows.map(r => `
          <tr>
            <td>${escHtml(r.dataset)}</td>
            <td><span class="tag" style="font-size:10px">${escHtml(r.policy)}</span></td>
            <td class="mono">${fmt(r.query_count)}</td>
          </tr>`).join('');
        return `
          <div style="margin-bottom:16px">
            <div style="font-size:11px;font-weight:700;color:var(--c-blue);
                        text-transform:uppercase;letter-spacing:0.05em;
                        padding:4px 0 6px;border-bottom:2px solid var(--c-blue-wash);
                        margin-bottom:6px;font-family:var(--font-mono)">
              ${escHtml(ts)}
            </div>
            <table class="data-table">
              <thead><tr><th>Dataset</th><th>Policy</th><th>Queries</th></tr></thead>
              <tbody>${tableRows}</tbody>
            </table>
          </div>`;
      }).join('');

      sections.push(section('Evaluation Matrix', matrixHtml));
    }

    // ── 3. Recorded measurements ─────────────────────────────
    if (info.time_header?.length) {
      const thCells = info.time_header.map(h =>
        `<th>${escHtml(h)}</th>`).join('');

      const sampleRows = (info.time_samples || []).map(row => {
        const cells = row.map((v, i) => {
          // Format exec_time column (index 7) to 4 decimal places
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

      sections.push(section('Recorded Measurements (time.csv)', `
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
    if (info.plots?.length) {
      const plotsHtml = info.plots.map(p => `
        <div class="plot-card">
          <div class="plot-title">${escHtml(p.filename.replace(/\.svg$/, '').replace(/_/g, ' '))}</div>
          <div class="plot-img">${p.data}</div>
        </div>`).join('');
      sections.push(section(`Result Plots (${info.plots.length})`, `<div class="plots-grid">${plotsHtml}</div>`));
    } else {
      sections.push(section('Visualize', `<div class="dim" style="font-size:12px">No plot files found. Run the visualize step to generate plots.</div>`));
    }
  }

  return sections.length
    ? sections.join('')
    : `<div class="dim" style="font-size:12px;padding:4px 0">Details will appear here once the step completes.</div>`;
}

// ── HTML helpers ──────────────────────────────────────────────
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