const API_BASE = '/api';

export async function getReport() {
  const res = await fetch(`${API_BASE}/report`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

export async function processData(startDate, endDate) {
  const res = await fetch(`${API_BASE}/process`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ start_date: startDate, end_date: endDate }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || res.statusText || 'Process failed');
  }
  if (!data.success) {
    throw new Error(data.error || 'Process failed');
  }
  return { report: data.report, raw_data: data.raw_data };
}

export async function getRawData() {
  const res = await fetch(`${API_BASE}/raw-data`);
  const data = await res.json().catch(() => ({ properties: [] }));
  return data.properties || [];
}
