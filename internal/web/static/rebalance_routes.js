// Ausgelagert aus gui/templates/rebalance_routes.html (JS-Template-Literals).
const GRAPH_LINKS = window.GRAPH_LINKS;

function openTab(evt, name) {
  document.querySelectorAll('.tabcontent').forEach(el => el.style.display = 'none');
  document.querySelectorAll('.tablink').forEach(el => el.classList.remove('w3-blue'));
  document.getElementById(name).style.display = 'block';
  if (evt) evt.currentTarget.classList.add('w3-blue');
  localStorage.setItem('routes-tab', name);
}

function restoreTab() {
  const valid = ['Overview', 'Routes', 'Probing', 'Pairs', 'Settings'];
  const saved = localStorage.getItem('routes-tab');
  const tab = valid.includes(saved) ? saved : 'Overview';
  document.querySelectorAll('.tabcontent').forEach(el => el.style.display = 'none');
  document.querySelectorAll('.tablink').forEach(el => el.classList.remove('w3-blue'));
  document.getElementById(tab).style.display = 'block';
  document.querySelectorAll('.tablink').forEach(el => {
    if (el.textContent.trim() === tab || (tab === 'Pairs' && el.textContent.trim() === 'Node Pairs'))
      el.classList.add('w3-blue');
  });
}

function ageColor(dateStr) {
  if (!dateStr) return 'w3-text-grey';
  const hours = (Date.now() - new Date(dateStr).getTime()) / 3600000;
  if (hours < 24) return 'w3-text-green';
  if (hours < 72) return 'w3-text-orange';
  return 'w3-text-red';
}

let routesData = [];
let pairsData = [];
let routesLoaded;

async function loadRoutes() {
  const res = await GET('rebalanceroutes', {data:{}});
  if (!res.results) return;
  routesData = res.results;
  routesData.sort((a, b) => {
    const ta = (a.target_alias || a.target_pubkey).toLowerCase();
    const tb = (b.target_alias || b.target_pubkey).toLowerCase();
    if (ta < tb) return -1;
    if (ta > tb) return 1;
    return b.weighted_ratio - a.weighted_ratio;
  });
  renderRoutes();
  updateOverview();
}

function filterRoutes() {
  const q = byId('routeSearch').value.toLowerCase();
  const filtered = q ? routesData.filter(r =>
    (r.target_alias || '').toLowerCase().includes(q) ||
    r.target_pubkey.toLowerCase().includes(q)
  ) : routesData;
  renderRoutes(filtered);
}

function renderRoutes(data) {
  if (!data) data = routesData;
  const table = byId('routesTable');
  table.innerHTML = '';
  data.forEach(r => {
    const row = table.insertRow();
    row.dataset.id = r.id;
    row.insertCell().innerHTML = `<input class="w3-check route-select" value="${r.id}" type="checkbox">`;
    row.insertCell().innerText = (r.target_alias ? r.target_alias + ' - ' : '') + r.target_pubkey;
    row.insertCell().innerText = (r.outgoing_alias ? r.outgoing_alias + ' - ' : '') + r.outgoing_chan_id;
    row.insertCell().innerText = r.last_fee_ppm != null ? Math.round(r.last_fee_ppm) : '---';
    // Source badge
    const isProbed = r.success_count === 0 && r.failure_count === 0;
    const srcCell = row.insertCell();
    const badge = document.createElement('span');
    badge.className = isProbed ? 'w3-tag w3-light-blue' : 'w3-tag w3-light-green';
    badge.innerText = isProbed ? 'Probed' : 'Tested';
    srcCell.appendChild(badge);
    row.insertCell().innerText = (r.success_ratio * 100).toFixed(1) + '%';
    row.insertCell().innerText = (r.weighted_ratio * 100).toFixed(1) + '%';
    row.insertCell().innerText = r.success_count;
    row.insertCell().innerText = r.failure_count;
    // Last success with age color
    const lsCell = row.insertCell();
    lsCell.innerText = r.last_success ? formatDate(r.last_success) : '---';
    lsCell.className = ageColor(r.last_success);
    row.insertCell().innerText = r.last_failure ? formatDate(r.last_failure) : '---';
    const cell = row.insertCell();
    const view = document.createElement('a');
    view.innerText = 'View';
    view.href = `/rebalanceroute/${r.id}`;
    view.target = '_blank';
    cell.appendChild(view);
    cell.appendChild(document.createTextNode(' '));
    const del = document.createElement('button');
    del.innerText = 'Delete';
    del.className = 'w3-button w3-tiny w3-red';
    del.onclick = async () => {
      if (!confirm('Delete this route?')) return;
      await DELETE(`rebalanceroutes/${r.id}`);
      row.remove();
    };
    cell.appendChild(del);
  });
}

async function loadPairs() {
  const res = await GET('pair_stats', {data:{}});
  if (!res || res.error || !res.pairs) {
    byId('pairInfo').innerText = res && res.error ? res.error : 'No pair statistics.';
    return;
  }
  pairsData = res.pairs;
  byId('stat-total-pairs').innerText = pairsData.length;
  byId('stat-avg-pair').innerText = (res.average_prob * 100).toFixed(1) + '%';
  byId('stat-slow-pairs').innerText = pairsData.filter(p => p.slow).length;
  pairsInfo = `${pairsData.length} pairs, average chance ${(res.average_prob * 100).toFixed(1)}%, P90/hop of all pairs ${Math.round(res.hop_p90_ms)} ms`;
  renderPairs();
}

const PAIRS_SHOWN = 200;
let pairsInfo = '';
let pairSortKey = 'prob';
let pairSortAsc = true;

function sortPairs(key) {
  if (pairSortKey === key) {
    pairSortAsc = !pairSortAsc;
  } else {
    pairSortKey = key;
    pairSortAsc = key === 'prob';
  }
  renderPairs();
}

function pairNode(cell, pubkey, alias) {
  const link = document.createElement('a');
  link.href = GRAPH_LINKS + '/node/' + pubkey;
  link.target = '_blank';
  link.innerText = alias || pubkey.substring(0, 12) + '...';
  link.title = pubkey;
  cell.appendChild(link);
}

function renderPairs() {
  const q = byId('pairSearch').value.toLowerCase();
  const minAttempts = parseInt(byId('pairMin').value) || 0;
  const slowOnly = byId('pairSlowOnly').checked;
  let data = pairsData.filter(p => p.attempts >= minAttempts && (!slowOnly || p.slow));
  if (q) {
    data = data.filter(p =>
      (p.from_alias || '').toLowerCase().includes(q) || p.from.toLowerCase().includes(q) ||
      (p.to_alias || '').toLowerCase().includes(q) || p.to.toLowerCase().includes(q)
    );
  }
  data.sort((a, b) => {
    let av = a[pairSortKey], bv = b[pairSortKey];
    if (pairSortKey === 'last_used') {
      av = av ? new Date(av).getTime() : 0;
      bv = bv ? new Date(bv).getTime() : 0;
    }
    return pairSortAsc ? av - bv : bv - av;
  });
  ['attempts','successes','prob','hop_p90_ms','samples','last_used'].forEach(k => {
    const el = byId('pair-sort-' + k);
    if (el) el.innerText = pairSortKey === k ? (pairSortAsc ? '\u25B2' : '\u25BC') : '';
  });
  byId('pairInfo').innerText = pairsInfo + ` - ${data.length} match` + (data.length > PAIRS_SHOWN ? `, first ${PAIRS_SHOWN} shown` : '');
  const table = byId('pairsTable');
  table.innerHTML = '';
  data.slice(0, PAIRS_SHOWN).forEach(p => {
    const row = table.insertRow();
    if (p.slow) row.className = 'w3-pale-red';
    else if (p.prob < 0.3) row.className = 'w3-pale-yellow';
    pairNode(row.insertCell(), p.from, p.from_alias);
    pairNode(row.insertCell(), p.to, p.to_alias);
    row.insertCell().innerText = p.attempts;
    row.insertCell().innerText = p.successes;
    row.insertCell().innerText = (p.prob * 100).toFixed(1) + '%';
    row.insertCell().innerText = p.samples > 0 ? Math.round(p.hop_p90_ms) + ' ms' : '---';
    row.insertCell().innerText = p.samples;
    row.insertCell().innerText = p.last_used ? formatDate(p.last_used) : '---';
    row.insertCell().innerText = p.slow ? 'yes' : '';
  });
}

let probeSettings = {};

async function loadProbeData() {
  const res = await GET('settings', {data:{}});
  if (!res.results) return;
  res.results.forEach(s => { probeSettings[s.key] = s.value; });

  const enabled = probeSettings['QR-Enabled'] === '1';
  const lastProbeStr = probeSettings['QR-LastProbe'];
  const interval = parseFloat(probeSettings['QR-UpdateHours'] || '6');
  // Overview stat
  byId('stat-last-probe').innerText = lastProbeStr ? formatDate(lastProbeStr) : 'Never';

  // Probing tab status
  const statusEl = byId('probe-status');
  if (enabled) {
    statusEl.innerHTML = '<span class="w3-tag w3-green">Enabled</span>';
  } else {
    statusEl.innerHTML = '<span class="w3-tag w3-red">Disabled</span>';
  }
  byId('probe-last').innerText = lastProbeStr ? formatDate(lastProbeStr) : 'Never';
  byId('probe-interval').innerText = interval + 'h';
  // Next probe due
  if (lastProbeStr && enabled) {
    const lastMs = new Date(lastProbeStr).getTime();
    const nextMs = lastMs + interval * 3600000;
    const nowMs = Date.now();
    if (nextMs > nowMs) {
      const mins = Math.round((nextMs - nowMs) / 60000);
      if (mins < 60) {
        byId('probe-next').innerText = 'in ' + mins + ' min';
      } else {
        byId('probe-next').innerText = 'in ' + (mins / 60).toFixed(1) + 'h';
      }
    } else {
      byId('probe-next').innerText = 'due now';
    }
  } else {
    byId('probe-next').innerText = enabled ? 'pending' : '---';
  }

  await routesLoaded;
  updateProbeResults();
  loadProbeLogs();
}

function updateProbeResults() {
  const probed = routesData.filter(r => r.success_count === 0 && r.failure_count === 0);
  const tested = routesData.filter(r => r.success_count > 0 || r.failure_count > 0);
  byId('probe-routes').innerText = probed.length;

  const targetSet = new Set(probed.map(r => r.target_pubkey));
  byId('probe-targets').innerText = targetSet.size;

  if (tested.length > 0) {
    const withSuccess = tested.filter(r => r.success_count > 0).length;
    byId('probe-success-rate').innerText = (withSuccess / tested.length * 100).toFixed(1) + '%';
  } else {
    byId('probe-success-rate').innerText = '---';
  }
}

let probeLogData = [];

async function loadProbeLogs() {
  const res = await GET('probelogs', {data:{}});
  if (!res.results) return;
  probeLogData = res.results;
  renderProbeLogs();
}

function renderProbeLogs() {
  const table = byId('probeLogTable');
  table.innerHTML = '';
  if (probeLogData.length === 0) {
    const row = table.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 6;
    cell.innerText = 'No probe runs recorded yet';
    cell.className = 'w3-text-grey';
    return;
  }
  probeLogData.forEach((log, idx) => {
    const row = table.insertRow();
    row.style.cursor = 'pointer';
    row.onclick = () => showProbeDetail(idx);
    if (log.routes_found > 0) row.className = 'w3-pale-green';
    else if (log.errors > 0 && log.routes_found === 0) row.className = 'w3-pale-yellow';
    row.insertCell().innerText = formatDate(log.timestamp);
    row.insertCell().innerText = log.targets_scanned;
    row.insertCell().innerText = log.routes_found;
    row.insertCell().innerText = log.routes_existing;
    row.insertCell().innerText = log.errors;
    row.insertCell().innerText = log.duration_ms < 1000 ? log.duration_ms + 'ms' : (log.duration_ms / 1000).toFixed(1) + 's';
  });
}

function showProbeDetail(idx) {
  const log = probeLogData[idx];
  const panel = byId('probeDetailPanel');
  const table = byId('probeDetailTable');
  byId('probeDetailTime').innerText = formatDate(log.timestamp);
  table.innerHTML = '';
  if (!log.details || log.details.length === 0) {
    const row = table.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 9;
    cell.innerText = 'No target details available';
    cell.className = 'w3-text-grey';
  } else {
    log.details.forEach(d => {
      const row = table.insertRow();
      if (d.errors > 0 && d.new === 0) row.className = 'w3-pale-yellow';
      else if (d.new > 0) row.className = 'w3-pale-green';
      const nameCell = row.insertCell();
      const link = document.createElement('a');
      link.href = GRAPH_LINKS + '/node/' + d.pubkey;
      link.target = '_blank';
      link.innerText = d.alias || d.pubkey.substring(0, 20) + '...';
      nameCell.appendChild(link);
      const opt = v => v != null ? v : '---';
      row.insertCell().innerText = opt(d.sources);
      row.insertCell().innerText = opt(d.no_budget);
      row.insertCell().innerText = opt(d.no_route);
      row.insertCell().innerText = d.out_chans_tried;
      row.insertCell().innerText = d.new;
      row.insertCell().innerText = d.existing;
      row.insertCell().innerText = opt(d.probe_failed);
      row.insertCell().innerText = opt(d.rpc_errors);
    });
  }
  panel.style.display = 'block';
}

function updateOverview() {
  if (routesData.length > 0) {
    byId('stat-total-routes').innerText = routesData.length;
    const probed = routesData.filter(r => r.success_count === 0 && r.failure_count === 0).length;
    byId('stat-probed').innerText = probed;
    byId('stat-tested').innerText = routesData.length - probed;
    const dayAgo = Date.now() - 86400000;
    const recent = routesData.filter(r => {
      const ls = r.last_success ? new Date(r.last_success).getTime() : 0;
      const lf = r.last_failure ? new Date(r.last_failure).getTime() : 0;
      return Math.max(ls, lf) > dayAgo;
    }).length;
    byId('stat-recent').innerText = recent;
  }
}

async function deleteSelected() {
  const ids = [...document.getElementsByClassName('route-select')].filter(c => c.checked).map(c => c.value);
  if (ids.length == 0 || !confirm(`Delete ${ids.length} route(s)?`)) return;
  for (const id of ids) {
    await DELETE(`rebalanceroutes/${id}`);
    const row = document.querySelector(`tr[data-id='${id}']`);
    if (row) row.remove();
  }
}

async function probeNow() {
  const btn = byId('probeNowBtn');
  btn.disabled = true;
  btn.innerText = 'Probe triggered, waiting...';
  btn.className = 'w3-button w3-grey w3-margin-bottom';
  const token = document.getElementById('api').dataset.token;
  const body = new URLSearchParams({key: 'QR-LastProbe', value: '2000-01-01T00:00:00'});
  await fetch('/update_setting/', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': token},
    body: body,
  });
  // Poll for new probe log (jobs loop runs every ~20s)
  const prevCount = probeLogData.length;
  const prevId = probeLogData.length > 0 ? probeLogData[0].id : 0;
  let attempts = 0;
  const poll = setInterval(async () => {
    attempts++;
    const res = await GET('probelogs', {data:{}});
    if (res.results && res.results.length > 0 && res.results[0].id !== prevId) {
      clearInterval(poll);
      probeLogData = res.results;
      renderProbeLogs();
      routesLoaded = loadRoutes();
      await loadProbeData();
      btn.disabled = false;
      btn.innerText = 'Probe Now';
      btn.className = 'w3-button w3-green w3-margin-bottom';
      setTimeout(() => { btn.className = 'w3-button w3-blue w3-margin-bottom'; }, 2000);
    } else if (attempts >= 15) {
      clearInterval(poll);
      btn.disabled = false;
      btn.innerText = 'Probe Now';
      btn.className = 'w3-button w3-blue w3-margin-bottom';
    }
  }, 5000);
}

async function cleanupUntested() {
  const probed = routesData.filter(r => r.success_count === 0 && r.failure_count === 0).length;
  if (probed === 0) { alert('No untested routes to clean up.'); return; }
  if (!confirm(`Delete ${probed} untested/probed route(s)? They will be re-discovered on the next probe run with proper fee data.`)) return;
  const btn = byId('cleanupBtn');
  btn.disabled = true;
  btn.innerText = 'Cleaning up...';
  const token = document.getElementById('api').dataset.token;
  const res = await fetch('/api/rebalanceroutes/cleanup_untested/', {
    method: 'POST',
    headers: {'X-CSRFToken': token},
  });
  const data = await res.json();
  btn.disabled = false;
  btn.innerText = 'Clean Up Untested Routes';
  alert(`Deleted ${data.deleted} untested route(s).`);
  routesLoaded = loadRoutes();
  await loadProbeData();
}

document.addEventListener('DOMContentLoaded', () => {
  restoreTab();
  routesLoaded = loadRoutes();
  loadPairs();
  loadProbeData();
});
