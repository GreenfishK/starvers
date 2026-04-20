/* ──────────────────────────────────────────────────────────────
   script.js  –  StarVers Evaluation Dashboard
   ────────────────────────────────────────────────────────────── */

const API_BASE = '/evaluation/starvers/api';

const ALL_STEPS = [
  'download',
  'preprocess_data',
  'construct_datasets',
  'ingest',
  'construct_queries',
  'evaluate',
  'visualize',
];

// ── State ─────────────────────────────────────────────────────
let runs        = [];
let activeRunTs = null;

// ── DOM refs ──────────────────────────────────────────────────
const runListEl    = document.getElementById('runList');
const runCountEl   = document.getElementById('runCount');
const placeholder  = document.getElementById('placeholder');
const detailPanel  = document.getElementById('detailPanel');
const runMetaEl    = document.getElementById('runMeta');
const stepsGridEl  = document.getElementById('stepsGrid');
const refreshBtn   = document.getElementById('refreshBtn');

// ── Bootstrap ─────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  loadRuns();
  refreshBtn.addEventListener('click', loadRuns);
});

// ── Data fetching ─────────────────────────────────────────────

async function loadRuns() {
  refreshBtn.style.opacity = '0.4';
  try {
    const res  = await fetch(`${API_BASE}/runs`);
    runs       = await res.json();
    renderSidebar();
    runCountEl.textContent = `${runs.length} run${runs.length !== 1 ? 's' : ''}`;

    // Re-render active run if one is selected
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
  } catch {
    return null;
  }
}

// ── Sidebar ───────────────────────────────────────────────────

function renderSidebar() {
  if (!runs.length) {
    runListEl.innerHTML = `<li style="padding:20px 16px;color:var(--text-faint);font-size:12px;">No evaluation runs found yet.</li>`;
    return;
  }

  runListEl.innerHTML = runs.map(run => {
    const { completed, total, overallStatus } = runStats(run);
    const pct   = total ? Math.round((completed / total) * 100) : 0;
    const cls   = overallStatus === 'success' ? 'complete' : overallStatus === 'failed' ? 'failed' : '';
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
      // Update active class immediately for responsiveness
      runListEl.querySelectorAll('.run-item').forEach(x => x.classList.remove('active'));
      li.classList.add('active');

      // Show loading state in content
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

  // ── Run meta bar
  runMetaEl.innerHTML = `
    <span class="run-meta-ts">${formatTs(run.ts)}</span>
    <span class="run-meta-badge ${overallStatus}">${overallStatus}</span>
    <span class="badge">${completed}/${total} steps</span>
    <span class="dim" style="font-size:11px;margin-left:auto">${run.ts}</span>
  `;

  // ── Build step map
  const stepMap = {};
  (run.steps || []).forEach(s => { stepMap[s.step_name] = s; });

  // ── Render each step card
  stepsGridEl.innerHTML = ALL_STEPS.map((name, i) => {
    const s = stepMap[name] || { step_number: i + 1, step_name: name, status: 'pending', start_time: '', end_time: '' };
    const duration = calcDuration(s.start_time, s.end_time);
    const hasDetail = s.status === 'success' || s.status === 'failed';

    return `
      <div class="step-card${hasDetail ? ' clickable' : ''}" data-step="${name}" id="step-${name}">
        <div class="step-card-header">
          <span class="step-num">${i + 1}</span>
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

  // ── Attach click handlers for expandable steps
  stepsGridEl.querySelectorAll('.step-card.clickable').forEach(card => {
    card.addEventListener('click', () => toggleStep(card, run));
  });
}

async function toggleStep(card, run) {
  const name     = card.dataset.step;
  const detailEl = document.getElementById(`detail-${name}`);
  const isOpen   = card.classList.contains('open');

  // Close all others
  stepsGridEl.querySelectorAll('.step-card.open').forEach(c => c.classList.remove('open'));

  if (isOpen) return; // was open → just close

  card.classList.add('open');

  // Fetch fresh run data once, then render the specific step detail
  const freshRun = await loadRunDetail(run.ts);
  const stepData = (freshRun?.steps || run.steps || []).find(s => s.step_name === name);
  detailEl.innerHTML = await buildStepDetail(name, run.ts, stepData);
}

// ── Step detail builders ──────────────────────────────────────

async function buildStepDetail(stepName, runTs, stepData) {
  try {
    const info = await fetch(`${API_BASE}/step-detail/${encodeURIComponent(runTs)}/${stepName}`).then(r => r.json());
    return renderStepInfo(stepName, info);
  } catch {
    return `<div class="error-msg">Could not load step details.</div>`;
  }
}

function renderStepInfo(stepName, info) {
  if (!info || Object.keys(info).length === 0) {
    return `<div class="dim" style="font-size:12px;padding:4px 0">No detail data available for this step yet.</div>`;
  }

  const sections = [];

  // ── download ─────────────────────────────────────────────
  if (stepName === 'download') {
    if (info.datasets?.length) {
      sections.push(section('Datasets Downloaded', tagList(info.datasets.map(d =>
        `${d.name} (${d.versions} versions)`
      ))));
    }
    if (info.dataset_sizes?.length) {
      const max = Math.max(...info.dataset_sizes.map(d => d.avg_size_mb || 0)) || 1;
      sections.push(section('Avg Snapshot Size (MiB)', barChart(
        info.dataset_sizes.map(d => ({ label: d.name, value: d.avg_size_mb, display: `${d.avg_size_mb} MiB` })),
        max
      )));
    }
    if (info.query_sets?.length) {
      sections.push(section('Query Sets Downloaded', kvGrid(
        info.query_sets.map(q => [q.name, `${q.count} queries`])
      )));
    }
  }

  // ── preprocess_data ───────────────────────────────────────
  if (stepName === 'preprocess_data') {
    sections.push(section('Substeps Executed', tagList([
      'skolemize_subject_blanks',
      'skolemize_object_blanks',
      'validate_and_comment_invalid_triples',
      'extract_queries',
      'exclude_queries',
    ])));

    if (info.validators) {
      sections.push(section('RDF Validators', kvGrid([
        ['rdf4j',       info.validators.rdf4j       || '—'],
        ['Apache Jena', info.validators.jena         || '—'],
      ])));
    }

    if (info.skolemization) {
      sections.push(section('Skolemization Summary', kvGrid([
        ['Blank nodes in subject position', info.skolemization.subject ?? '—'],
        ['Blank nodes in object position',  info.skolemization.object  ?? '—'],
        ['Invalid triples excluded',        info.skolemization.invalid ?? '—'],
      ])));
    }

    if (info.excluded_queries) {
      const rows = Object.entries(info.excluded_queries).map(([reason, count]) =>
        `<tr><td>${reason || '(none)'}</td><td>${count}</td></tr>`
      ).join('');
      sections.push(section('Excluded SciQA Queries by Reason',
        `<table class="data-table">
          <thead><tr><th>Reason</th><th>Count</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`
      ));
    }
  }

  // ── construct_datasets ────────────────────────────────────
  if (stepName === 'construct_datasets') {
    if (info.variants?.length) {
      const rows = info.variants.map(v => `
        <tr>
          <td>${v.name}</td>
          <td>${v.size_mb != null ? v.size_mb + ' MiB' : '—'}</td>
          <td style="font-size:10px;color:var(--text-dim)">${v.versioning_approach || '—'}</td>
        </tr>`).join('');
      sections.push(section('Dataset Variants', `
        <table class="data-table">
          <thead><tr><th>Variant</th><th>Size</th><th>RDF Versioning Approach</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`
      ));
    }
  }

  // ── ingest ────────────────────────────────────────────────
  if (stepName === 'ingest') {
    if (info.ingestion_table?.length) {
      const max = Math.max(...info.ingestion_table.map(r => parseFloat(r.ingestion_time) || 0)) || 1;
      sections.push(section('Ingestion Times (avg over 10 runs)', barChart(
        info.ingestion_table.map(r => ({
          label:   `${r.triplestore} / ${r.policy} / ${r.dataset}`,
          value:   parseFloat(r.ingestion_time),
          display: `${parseFloat(r.ingestion_time).toFixed(1)}s`,
        })),
        max
      )));

      const headers = ['Triplestore', 'Policy', 'Dataset', 'Ingest Time (s)', 'Raw Size (MiB)', 'DB Size (MiB)'];
      const rows    = info.ingestion_table.map(r => `
        <tr>
          <td>${r.triplestore}</td><td>${r.policy}</td><td>${r.dataset}</td>
          <td>${r.ingestion_time}</td><td>${r.raw_file_size_mib}</td><td>${r.db_files_disk_usage_mib}</td>
        </tr>`).join('');
      sections.push(section('Full Ingestion Table',
        `<table class="data-table">
          <thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>
          <tbody>${rows}</tbody>
        </table>`
      ));
    }
  }

  // ── construct_queries ─────────────────────────────────────
  if (stepName === 'construct_queries') {
    if (info.query_counts) {
      const rows = Object.entries(info.query_counts).map(([path, count]) =>
        `<tr><td>${path}</td><td>${count}</td></tr>`
      ).join('');
      sections.push(section('Queries Constructed', `
        <table class="data-table">
          <thead><tr><th>Policy / Dataset</th><th>Query Count</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`
      ));
    }
  }

  // ── evaluate ──────────────────────────────────────────────
  if (stepName === 'evaluate') {
    if (info.evaluations?.length) {
      const rows = info.evaluations.map(e => `
        <tr><td>${e.triplestore}</td><td>${e.policy}</td><td>${e.dataset}</td><td>${e.query_count}</td></tr>
      `).join('');
      sections.push(section('Evaluation Matrix', `
        <table class="data-table">
          <thead><tr><th>Triplestore</th><th>Policy</th><th>Dataset</th><th>Queries</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`
      ));
    }
  }

  // ── visualize ─────────────────────────────────────────────
  if (stepName === 'visualize') {
    if (info.latex_table) {
      sections.push(section('LaTeX Results Table',
        `<div class="code-block">${escHtml(info.latex_table)}</div>`
      ));
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
    ${body}
  </div>`;
}

function tagList(items) {
  return `<div class="tag-list">${items.map(i => `<span class="tag">${i}</span>`).join('')}</div>`;
}

function kvGrid(pairs) {
  return `<div class="kv-grid">${pairs.map(([k, v]) =>
    `<span class="kv-key">${k}</span><span class="kv-val">${v}</span>`
  ).join('')}</div>`;
}

function barChart(items, max) {
  return `<div class="bar-chart">${items.map(item => {
    const pct = max > 0 ? Math.round((item.value / max) * 100) : 0;
    return `<div class="bar-row">
      <span class="bar-label" title="${item.label}">${item.label}</span>
      <div class="bar-wrap"><div class="bar-fill" style="width:${pct}%"></div></div>
      <span class="bar-val">${item.display}</span>
    </div>`;
  }).join('')}</div>`;
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// ── Utilities ─────────────────────────────────────────────────

function parseTs(ts) {
  if (!ts) return null;
  // Handles "20260417T15:59:19.564" → Date
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
  const s = parseTs(start);
  const e = parseTs(end);
  if (!s || !e) return '—';
  const ms = e - s;
  if (ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${Math.round(ms / 1000)}s`;
  const m = Math.floor(ms / 60000);
  const sec = Math.round((ms % 60000) / 1000);
  return `${m}m ${sec}s`;
}

function runStats(run) {
  const steps    = run.steps || [];
  const completed = steps.filter(s => s.status === 'success').length;
  const failed    = steps.filter(s => s.status === 'failed').length;
  const total     = ALL_STEPS.length;
  const overallStatus =
    failed   > 0            ? 'failed'  :
    completed === total     ? 'success' :
    completed > 0           ? 'running' : 'pending';
  return { completed, total, overallStatus };
}
