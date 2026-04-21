/* ──────────────────────────────────────────────────────────────
   script.js  –  StarVers Evaluation Dashboard
   ────────────────────────────────────────────────────────────── */

const API_BASE = '/evaluation/starvers/api';

const ALL_STEPS = [
  'download', 'preprocess_data', 'construct_datasets',
  'ingest', 'construct_queries', 'evaluate', 'visualize',
];

const POLICY_TO_VARIANT = {
  ic_sr_ng: 'alldata.ICNG.trig',
  ostrich:  'first IC + change sets',
  tb_sr_ng: 'alldata.TB_computed.nq',
  tb_sr_rs: 'alldata.TB_star_hierarchical.ttl',
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
  } catch (e) {
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
    <span class="run-meta-ts">${formatTs(run.ts)}</span>
    <span class="run-meta-badge ${overallStatus}">${overallStatus}</span>
    <span class="badge" style="background:var(--surface2);color:var(--text-dim);border:1px solid var(--border)">${completed}/${total} steps</span>
    <span class="dim mono" style="font-size:11px;margin-left:auto">${run.ts}</span>
  `;

  const stepMap = {};
  (run.steps || []).forEach(s => { stepMap[s.step_name] = s; });

  stepsGridEl.innerHTML = ALL_STEPS.map((name, i) => {
    const s = stepMap[name] || { step_number: i+1, step_name: name, status: 'pending', start_time: '', end_time: '' };
    const duration   = calcDuration(s.start_time, s.end_time);
    const hasDetail  = s.status === 'success' || s.status === 'failed';
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
          <td>${d.versions}</td>
          <td>${d.size_mb != null ? d.size_mb + ' MiB' : '—'}</td>
          <td>${d.download_link ? `<a class="link" href="${escHtml(d.download_link)}" target="_blank">↗ link</a>` : '—'}</td>
        </tr>`).join('');
      sections.push(section('Datasets', `
        <table class="data-table">
          <thead><tr><th>Dataset</th><th>Versions</th><th>All Snapshots Size</th><th>Source</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }
    if (info.query_sets?.length) {
      const rows = info.query_sets.map(q =>
        `<tr><td>${q.name}</td><td>${q.count}</td></tr>`).join('');
      sections.push(section('Query Sets Downloaded', `
        <table class="data-table">
          <thead><tr><th>Query Set</th><th>Queries</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }
  }

  // ── preprocess_data ──────────────────────────────────────────
  if (stepName === 'preprocess_data') {
    sections.push(section('Substeps Executed', tagList([
      'skolemize_subject_blanks', 'skolemize_object_blanks',
      'validate_and_comment_invalid_triples', 'extract_queries', 'exclude_queries',
    ])));

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
          <td>${d.subject}</td>
          <td>${d.object}</td>
          <td>${d.invalid}</td>
        </tr>`).join('');
      sections.push(section('Skolemization & Validation per Dataset', `
        <table class="data-table">
          <thead><tr>
            <th>Dataset</th>
            <th>Blank nodes (subject)</th>
            <th>Blank nodes (object)</th>
            <th>Invalid triples excluded</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }

    if (info.excluded_queries && Object.keys(info.excluded_queries).length) {
      const rows = Object.entries(info.excluded_queries).map(([reason, count]) =>
        `<tr><td>${reason || '(no reason)'}</td><td>${count}</td></tr>`).join('');
      sections.push(section('Excluded SciQA Queries by Reason', `
        <table class="data-table">
          <thead><tr><th>Reason</th><th>Count</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`));
    }
  }

  // ── construct_datasets ───────────────────────────────────────
  if (stepName === 'construct_datasets') {
    if (info.variants?.length) {
      // Group datasets per variant
      const byVariant = {};
      info.variants.forEach(v => {
        if (!byVariant[v.variant]) byVariant[v.variant] = { approach: v.versioning_approach, datasets: [] };
        byVariant[v.variant].datasets.push({ dataset: v.dataset, size_mb: v.size_mb });
      });

      const variantHtml = Object.entries(byVariant).map(([variant, data]) => {
        const dsRows = data.datasets.map(d =>
          `<tr><td>${d.dataset}</td><td>${d.size_mb != null ? d.size_mb + ' MiB' : '—'}</td></tr>`
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
      // Group by triplestore
      const byTs = {};
      info.ingestion_summary.forEach(r => {
        if (!byTs[r.triplestore]) byTs[r.triplestore] = [];
        byTs[r.triplestore].push(r);
      });

      const groupHtml = Object.entries(byTs).map(([ts, rows]) => {
        const tableRows = rows.map(r => `
          <tr>
            <td>${r.policy}</td>
            <td>${r.dataset}</td>
            <td class="mono">${r.avg_ingestion_time != null ? r.avg_ingestion_time.toFixed(2) + 's' : '—'}</td>
            <td class="mono">${r.avg_db_size_mib != null ? r.avg_db_size_mib.toFixed(1) + ' MiB' : '—'}</td>
          </tr>`).join('');
        return `
          <div class="ingest-group">
            <div class="ingest-group-title">${ts}</div>
            <table class="data-table">
              <thead><tr><th>Policy</th><th>Dataset</th><th>Avg Ingest Time</th><th>Avg DB Size</th></tr></thead>
              <tbody>${tableRows}</tbody>
            </table>
          </div>`;
      }).join('');

      sections.push(section('Ingestion Results (avg over 10 runs)', groupHtml));
    }
  }

  // ── construct_queries ────────────────────────────────────────
  if (stepName === 'construct_queries') {
    if (info.query_counts && Object.keys(info.query_counts).length) {
      // Group by policy
      const byPolicy = {};
      Object.entries(info.query_counts).forEach(([key, count]) => {
        const [policy, dataset, query_set] = key.split(' / ');
        if (!byPolicy[policy]) byPolicy[policy] = [];
        byPolicy[policy].push({ dataset, query_set, count });
      });

      const groupHtml = Object.entries(byPolicy).map(([policy, rows]) => {
        const tableRows = rows.map(r =>
          `<tr><td>${r.dataset}</td><td>${r.query_set}</td><td class="mono">${r.count}</td></tr>`).join('');
        return `
          <div class="ingest-group">
            <div class="ingest-group-title">${policy}</div>
            <table class="data-table">
              <thead><tr><th>Dataset</th><th>Query Set</th><th>Query Count</th></tr></thead>
              <tbody>${tableRows}</tbody>
            </table>
          </div>`;
      }).join('');

      sections.push(section('Queries Constructed per Policy / Dataset / Query Set', groupHtml));
    } else {
      sections.push(section('Queries Constructed', `<div class="dim" style="font-size:12px">No query files found in final_queries directory.</div>`));
    }
  }

  // ── evaluate ─────────────────────────────────────────────────
  if (stepName === 'evaluate') {
    if (info.evaluations?.length) {
      // Group by triplestore
      const byTs = {};
      info.evaluations.forEach(e => {
        if (!byTs[e.triplestore]) byTs[e.triplestore] = [];
        byTs[e.triplestore].push(e);
      });

      const groupHtml = Object.entries(byTs).map(([ts, rows]) => {
        const tableRows = rows.map(r => `
          <tr>
            <td>${r.dataset}</td>
            <td>${r.policies.map(p => `<span class="tag" style="font-size:10px">${p}</span>`).join(' ')}</td>
            <td class="mono">${r.query_count}</td>
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
    if (info.result_table?.headers?.length) {
      const headerHtml = info.result_table.headers.map(h => `<th>${escHtml(h)}</th>`).join('');
      const rowsHtml   = info.result_table.rows.map(row =>
        `<tr>${row.map(cell => `<td>${escHtml(cell)}</td>`).join('')}</tr>`
      ).join('');
      sections.push(section('Results Table', `
        <div class="latex-table-wrap">
          <table class="data-table">
            <thead><tr>${headerHtml}</tr></thead>
            <tbody>${rowsHtml}</tbody>
          </table>
        </div>`));
    } else if (info.latex_raw) {
      sections.push(section('LaTeX Results Table (raw)', `<div class="code-block">${escHtml(info.latex_raw)}</div>`));
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

function tagList(items) {
  return `<div class="tag-list">${items.map(i => `<span class="tag">${i}</span>`).join('')}</div>`;
}

function escHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Utilities ─────────────────────────────────────────────────
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

function calcDuration(start, end) {
  const s = parseTs(start), e = parseTs(end);
  if (!s || !e) return '—';
  const ms = e - s;
  if (ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${Math.round(ms / 1000)}s`;
  const m = Math.floor(ms / 60000);
  return `${m}m ${Math.round((ms % 60000) / 1000)}s`;
}

function runStats(run) {
  const steps     = run.steps || [];
  const completed = steps.filter(s => s.status === 'success').length;
  const failed    = steps.filter(s => s.status === 'failed').length;
  const total     = ALL_STEPS.length;
  const overallStatus =
    failed > 0        ? 'failed'  :
    completed === total ? 'success' :
    completed > 0     ? 'running' : 'pending';
  return { completed, total, overallStatus };
}