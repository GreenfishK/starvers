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
      const rows = info.datasets.map(d => `
        <tr>
          <td><strong>${d.name}</strong></td>
          <td>${fmt(d.versions)}</td>
          <td>${d.size_mb != null ? fmtMb(d.size_mb) : '—'}</td>
          <td>${d.download_link
            ? `<a class="link" href="${escHtml(d.download_link)}" target="_blank">↗ link</a>`
            : '—'}</td>
        </tr>`).join('');
      sections.push(section('Datasets', `
        <table class="data-table">
          <thead><tr><th>Dataset</th><th>Versions</th><th>All Snapshots Size</th><th>Source</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }
    if (info.query_sets?.length) {
      const qsRows = info.query_sets.map(q => {
        const linkItems = (q.links || []).map(l =>
          `<li><a class="link" href="${escHtml(l.url)}" target="_blank">${escHtml(l.filename)}</a></li>`
        ).join('');
        const linksHtml = linkItems
          ? `<details style="margin-top:4px">
               <summary style="cursor:pointer;font-size:11px;color:var(--text-faint)">
                 ${q.links.length} source file${q.links.length !== 1 ? 's' : ''} ↗
               </summary>
               <ul style="margin:4px 0 0 12px;padding:0;font-size:11px;list-style:disc">${linkItems}</ul>
             </details>`
          : '—';
        return `
          <tr>
            <td><strong>${escHtml(q.name)}</strong></td>
            <td>${escHtml(q.for_dataset || '—')}</td>
            <td class="mono">${q.count != null ? fmt(q.count) : '—'}</td>
            <td>${linksHtml}</td>
          </tr>`;
      }).join('');
      sections.push(section('Query Sets Downloaded', `
        <table class="data-table">
          <thead><tr>
            <th>Query Set</th><th>Dataset</th><th>Queries</th><th>Source Files</th>
          </tr></thead>
          <tbody>${qsRows}</tbody>
        </table>`));
    }
  }

  // ── preprocess_data ──────────────────────────────────────────
  if (stepName === 'preprocess_data') {
    // Substeps as horizontal flow
    const substeps = [
      'skolemize_subject_blanks',
      'skolemize_object_blanks',
      'validate_and_comment_invalid_triples',
      'extract_queries',
      'exclude_queries',
    ];
    const flowHtml = substeps.map((s, i) =>
      `<div class="flow-step">${s.replace(/_/g, ' ')}</div>${i < substeps.length - 1 ? '<div class="flow-arrow">→</div>' : ''}`
    ).join('');
    sections.push(section('Substeps Executed', `<div class="flow-container">${flowHtml}</div>`));

    if (info.validators) {
      sections.push(section('RDF Validators', `
        <table class="data-table">
          <thead><tr><th>Parser</th><th>Version</th></tr></thead>
          <tbody>
            <tr><td>rdf4j</td><td class="mono">${info.validators.rdf4j || '—'}</td></tr>
            <tr><td>Apache Jena</td><td class="mono">${info.validators.jena || '—'}</td></tr>
          </tbody>
        </table>`));
    }

    if (info.skolemization_per_dataset?.length) {
      const rows = info.skolemization_per_dataset.map(d => `
        <tr>
          <td><strong>${d.dataset}</strong></td>
          <td>${fmt(d.subject)}</td>
          <td>${fmt(d.object)}</td>
          <td>${fmt(d.invalid)}</td>
        </tr>`).join('');
      sections.push(section('Skolemization & Validation per Dataset', `
        <table class="data-table">
          <thead><tr>
            <th>Dataset</th><th>Blank nodes (subject)</th>
            <th>Blank nodes (object)</th><th>Invalid triples excluded</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }

    if (info.excluded_queries && Object.keys(info.excluded_queries).length) {
      const total = Object.values(info.excluded_queries).reduce((a, b) => a + b, 0);
      const rows  = Object.entries(info.excluded_queries).map(([reason, count]) =>
        `<tr><td>${reason}</td><td>${fmt(count)}</td></tr>`).join('');
      sections.push(section(`Excluded SciQA Queries (${fmt(total)} total)`, `
        <table class="data-table">
          <thead><tr><th>Reason</th><th>Count</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
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

      const variantHtml = Object.entries(byVariant).map(([variant, data]) => {
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
      }).join('');
      sections.push(section('Dataset Variants', variantHtml));
    }
  }

  // ── ingest ───────────────────────────────────────────────────
  if (stepName === 'ingest') {
    if (info.ingestion_summary?.length) {
      // Group by triplestore for grouped bar chart + table
      const byTs = {};
      info.ingestion_summary.forEach(r => {
        if (!byTs[r.triplestore]) byTs[r.triplestore] = [];
        byTs[r.triplestore].push(r);
      });

      const maxTime = Math.max(...info.ingestion_summary.map(r => r.avg_ingestion_time || 0)) || 1;

      const groupHtml = Object.entries(byTs).map(([ts, rows]) => {
        const bars = rows.map(r => {
          const pct = Math.round((r.avg_ingestion_time / maxTime) * 100);
          return `
            <div class="bar-row">
              <span class="bar-label" title="${r.policy} / ${r.dataset}">${r.policy} / ${r.dataset}</span>
              <div class="bar-wrap"><div class="bar-fill" style="width:${pct}%"></div></div>
              <span class="bar-val">${r.avg_ingestion_time.toFixed(2)}s</span>
              <span class="bar-val" style="color:var(--text-faint)">${fmtMb(r.avg_db_size_mib)}</span>
            </div>`;
        }).join('');
        return `
          <div class="ingest-group">
            <div class="ingest-group-title">${ts}</div>
            <div class="bar-chart" style="margin-bottom:4px">
              <div class="bar-row" style="font-size:10px;color:var(--text-faint)">
                <span class="bar-label">Policy / Dataset</span>
                <span></span>
                <span class="bar-val">Ingest Time</span>
                <span class="bar-val">DB Size</span>
              </div>
              ${bars}
            </div>
          </div>`;
      }).join('');
      sections.push(section('Ingestion Results — avg over 10 runs', groupHtml));
    }
  }

// ── construct_queries ────────────────────────────────────────
if (stepName === 'construct_queries') {
  const { query_counts, policies, datasets } = info;

  if (datasets?.length && policies?.length && query_counts) {

    // Extract unique query_sets
    const querySets = [...new Set(
      Object.keys(query_counts).map(k => k.split(" / ")[2])
    )].sort();

    // Header: only one value column now
    const headerCols = `
      <th>Dataset</th>
      <th>Query Set</th>
      <th>Queries</th>
    `;

    let tableRows = '';
    let totalSum = 0;

    datasets.forEach(dataset => {
      querySets.forEach((qs, idx) => {

        // Since values are identical across policies, just take first policy
        const key = `${policies[0]} / ${dataset} / ${qs}`;
        const val = query_counts[key] ?? 0;

        totalSum += val;

        // Rowspan for dataset
        const datasetCell = idx === 0
          ? `<td rowspan="${querySets.length}"><strong>${dataset}</strong></td>`
          : '';

        tableRows += `
          <tr>
            ${datasetCell}
            <td>${qs}</td>
            <td class="mono">${fmt(val)}</td>
          </tr>`;
      });
    });

    // Formula now uses number of policies
    const numPolicies = policies.length;

    const totalBlock = `
      <div style="margin-top:10px">
        <div><strong>Total Queries (formula)</strong></div>
        <div class="mono">Total = (∑ table cells) × (#policies)</div>
        <div class="mono">= ${fmt(totalSum)} × ${numPolicies} = <strong>${fmt(totalSum * numPolicies)}</strong></div>
      </div>
    `;

    sections.push(`
      <table class="data-table">
        <thead><tr>${headerCols}</tr></thead>
        <tbody>${tableRows}</tbody>
      </table>
      ${totalBlock}
    `);

  } else {
    sections.push(
      `<div class="dim" style="font-size:12px">
         No query files found in final_queries directory.
       </div>`
    );
  }
}

  // ── evaluate ─────────────────────────────────────────────────
  if (stepName === 'evaluate') {
    if (info.evaluations?.length) {
      const byTs = {};
      info.evaluations.forEach(e => {
        if (!byTs[e.triplestore]) byTs[e.triplestore] = [];
        byTs[e.triplestore].push(e);
      });
      const groupHtml = Object.entries(byTs).map(([ts, rows]) => {
        const tableRows = rows.map(r => `
          <tr>
            <td>${r.dataset}</td>
            <td>${(r.policies || []).map(p => `<span class="tag" style="font-size:10px">${p}</span>`).join(' ')}</td>
            <td class="mono">${fmt(r.query_count)}</td>
          </tr>`).join('');
        return `
          <div class="ingest-group">
            <div class="ingest-group-title">${ts}</div>
            <table class="data-table">
              <thead><tr><th>Dataset</th><th>Policies</th><th>Queries</th></tr></thead>
              <tbody>${tableRows}</tbody>
            </table>
          </div>`;
      }).join('');
      sections.push(section('Evaluation Matrix', groupHtml));
    }
  }

  // ── visualize ────────────────────────────────────────────────
  if (stepName === 'visualize') {
    if (info.plots?.length) {
      const plotsHtml = info.plots.map(p => `
        <div class="plot-card">
          <div class="plot-title">${escHtml(p.filename.replace(/\.png$/, '').replace(/_/g, ' '))}</div>
          <img class="plot-img" src="data:image/png;base64,${p.data}" alt="${escHtml(p.filename)}" />
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