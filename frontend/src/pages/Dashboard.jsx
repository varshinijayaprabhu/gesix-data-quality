import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { getReport, processData, getRawData } from '../api';
import { downloadPropertiesPdf } from '../pdfExport';
import './Dashboard.css';

function DimCard({ name, score }) {
  const status = score >= 90 ? 'PASS' : score >= 70 ? 'WARN' : 'FAIL';
  const reasoning =
    status === 'PASS'
      ? 'High reliability.'
      : status === 'WARN'
        ? 'Needs attention.'
        : 'Critical issues found.';

  return (
    <div className="dim-card" id={`dim-${name.toLowerCase()}`}>
      <h3>{name}</h3>
      <p>Score: <strong>{score}%</strong></p>
      <p>Status: <span className={`status-${status}`}>{status}</span></p>
      <p><small>{reasoning}</small></p>
    </div>
  );
}

export default function Dashboard() {
  const [report, setReport] = useState(null);
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState(null);

  // Simplified to 3 input methods
  const [sourceType, setSourceType] = useState('upload');
  const [sourceUrl, setSourceUrl] = useState('');
  const [useDateRange, setUseDateRange] = useState(false);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [apiInputMode, setApiInputMode] = useState('link');
  const [apiKey, setApiKey] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);

  const isUpload = sourceType === 'upload';

  useEffect(() => {
    setReport(null);
    setRawData([]);
    setLoading(false);
  }, []);

  const handleSourceChange = (e) => {
    setSourceType(e.target.value);
    setError(null);
    setSelectedFile(null);
    setSourceUrl('');
    setUseDateRange(false);
    setStartDate('');
    setEndDate('');
    setApiInputMode('link');
    setApiKey('');
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);

    if (isUpload) {
      if (!selectedFile) {
        setError('Please select a file to upload.');
        return;
      }
    } else {
      if (!sourceUrl) {
        setError('Please provide a valid Source URL.');
        return;
      }
      if (sourceType === 'api' && apiInputMode === 'key' && !apiKey) {
        setError('Please provide a valid API Key.');
        return;
      }
      if (useDateRange && (!startDate || !endDate)) {
        setError('Please select both start and end dates.');
        return;
      }
    }

    setProcessing(true);
    setReport(null);
    setRawData([]);
    try {
      const data = await processData({
        sourceType: isUpload ? 'others_upload' : sourceType,
        sourceUrl,
        startDate: (!isUpload && useDateRange) ? startDate : '',
        endDate: (!isUpload && useDateRange) ? endDate : '',
        apiKey: apiInputMode === 'key' ? apiKey : '',
        file: isUpload ? selectedFile : null,
      });

      setReport(data.report);
      const list = data.raw_data?.data || data.raw_data?.properties || [];
      setRawData(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e.message);
      setReport(null);
      setRawData([]);
    } finally {
      setProcessing(false);
    }
  };

  const handleDownloadPdf = () => {
    downloadPropertiesPdf(rawData);
  };

  const isFormValid = () => {
    if (isUpload) return !!selectedFile;
    const hasUrl = sourceUrl.trim().length > 0;
    const hasKey = (sourceType === 'api' && apiInputMode === 'key') ? apiKey.trim().length > 0 : true;
    if (useDateRange) return hasUrl && hasKey && startDate && endDate;
    return hasUrl && hasKey;
  };

  const getValidationMessage = () => {
    if (isUpload) {
      if (!selectedFile) return 'Waiting for file upload...';
      return `${selectedFile.name} ready for analysis.`;
    }
    if (sourceType === 'api' && apiInputMode === 'key' && !apiKey) return 'Please enter an API Key.';
    if (!sourceUrl) return 'Please enter a Source URL.';
    if (useDateRange && (!startDate || !endDate)) return 'Please select a date range.';
    return 'Parameters set. Ready to process.';
  };

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Data Trustability Dashboard</h1>
        <p>AI-Ready Data Quality Framework — powered by Great Expectations</p>
        <nav>
          <Link to="/">Home</Link>
        </nav>
      </header>

      <section className="control-panel">
        <div className="panel-header">
          <h3>Run New Analysis</h3>
          <span className={`validation-badge ${isFormValid() ? 'valid' : 'invalid'}`}>
            {getValidationMessage()}
          </span>
        </div>
        <form onSubmit={handleSubmit} className="dashboard-form">
          <div className="form-row">
            <div className="form-group source-selector">
              <label htmlFor="source_type">Data Source</label>
              <select 
                id="source_type" 
                value={sourceType} 
                onChange={handleSourceChange}
                disabled={processing}
              >
                <option value="upload">📂 File Upload (Any Format)</option>
                <option value="api">🌐 Dynamic API (Custom Endpoint)</option>
                <option value="scraping">🔍 Web Scraper (Target URL)</option>
              </select>
            </div>

            {sourceType === 'api' && (
              <div className="radio-group-horizontal">
                <label className="radio-label">
                  <input
                    type="radio"
                    name="apiMode"
                    value="link"
                    checked={apiInputMode === 'link'}
                    onChange={() => setApiInputMode('link')}
                    disabled={processing}
                  />
                  Public API (Link Only)
                </label>
                <label className="radio-label">
                  <input
                    type="radio"
                    name="apiMode"
                    value="key"
                    checked={apiInputMode === 'key'}
                    onChange={() => setApiInputMode('key')}
                    disabled={processing}
                  />
                  Secured API (Key Required)
                </label>
              </div>
            )}
          </div>

          <div className="form-row">
            {!isUpload && (
              <div className="form-group url-input">
                <label htmlFor="source_url">Source URL</label>
                <input
                  type="text"
                  id="source_url"
                  placeholder={sourceType === 'api' ? "https://api.example.com/v1/data" : "https://example.com/portal"}
                  value={sourceUrl}
                  onChange={(e) => setSourceUrl(e.target.value)}
                  disabled={processing}
                />
              </div>
            )}

            {sourceType === 'api' && apiInputMode === 'key' && (
              <div className="form-group key-input">
                <label htmlFor="api_key">Authentication Key</label>
                <input
                  type="password"
                  id="api_key"
                  placeholder="Bearer token or x-api-key"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  disabled={processing}
                />
              </div>
            )}

            {isUpload && (
              <div className="file-input-group">
                <div className="form-group">
                  <label htmlFor="data_file">
                    Upload Dataset (CSV, Excel, JSON, PDF, Parquet, XML, or any format)
                  </label>
                  <input
                    type="file"
                    id="data_file"
                    accept="*"
                    onChange={(e) => setSelectedFile(e.target.files[0])}
                    disabled={processing}
                  />
                </div>
              </div>
            )}
          </div>

          {!isUpload && (
            <div className="form-row">
              <div className="form-group toggle-group">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={useDateRange}
                    onChange={(e) => setUseDateRange(e.target.checked)}
                    disabled={processing}
                  />
                  Filter by Date Range (Optional)
                </label>
                {!useDateRange && (
                  <p className="hint-text">No range selected. System will fetch baseline (last 15 records).</p>
                )}
              </div>

              {useDateRange && (
                <div className="date-inputs">
                  <div className="form-group">
                    <label htmlFor="start_date">Period Start</label>
                    <input
                      type="date"
                      id="start_date"
                      name="start_date"
                      value={startDate}
                      onChange={(e) => setStartDate(e.target.value)}
                      disabled={processing}
                    />
                  </div>
                  <div className="form-group">
                    <label htmlFor="end_date">Period End</label>
                    <input
                      type="date"
                      id="end_date"
                      name="end_date"
                      value={endDate}
                      onChange={(e) => setEndDate(e.target.value)}
                      disabled={processing}
                    />
                  </div>
                </div>
              )}
            </div>
          )}

          <div className="form-row submit-row">
            <button type="submit" className="btn-process" disabled={processing || !isFormValid()}>
              {processing ? 'Analyzing Data...' : 'Run Quality Analysis'}
            </button>
          </div>
        </form>
      </section>

      {(error || (report && report.error)) && (
        <div className="dashboard-error">
          {error || report.error}
        </div>
      )}

      {loading && !report && !error && (
        <p className="dashboard-loading">Executing Pipeline...</p>
      )}

      {report && !report.error && report.status !== 'No Data Found for this period' && (
        <>
          <section className="score-box">
            <div className="score-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <h2>Framework Trustability: <span id="overall-score">{report.overall_trustability}%</span></h2>
              <span className="status-badge" style={{ padding: '4px 12px', borderRadius: '20px', background: '#2c3e50', color: 'white', fontSize: '0.9rem', fontWeight: 'bold' }}>
                {report.status}
              </span>
            </div>
            <p>Total Records Processed: <span id="total-count">{report.total_records}</span></p>
          </section>

          <section className="dimensions-grid">
            {report.dimensions && typeof report.dimensions === 'object' &&
              Object.entries(report.dimensions).map(([name, score]) => (
                <DimCard key={name} name={name} score={score} />
              ))}
          </section>
        </>
      )}

      {report && report.status === 'No Data Found for this period' && (
        <section className="score-box no-data">
          <h2>No data found</h2>
          <p>Run an analysis with a different source or date range.</p>
        </section>
      )}

      <section className="generated-data-section">
        <h3>Raw Dataset Preview</h3>
        <p className="generated-data-hint">Showing ingested records with unified metadata layer.</p>
        {rawData.length > 0 && rawData[0] && typeof rawData[0] === 'object' ? (
          <>
            <div className="table-wrap">
              <table className="generated-data-table">
                <thead>
                  <tr>
                    {Object.keys(rawData[0]).map(key => (
                      <th key={key}>{key.replace(/_/g, ' ')}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rawData.map((row, i) => (
                    <tr key={i}>
                      {Object.keys(rawData[0]).map(key => (
                        <td key={key}>{row[key] != null ? String(row[key]) : '—'}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <button type="button" className="btn-download-pdf" onClick={handleDownloadPdf}>
              Export Quality Report (PDF)
            </button>
          </>
        ) : (
          <p className="no-generated-data">Initialize a data source to view the dataset preview.</p>
        )}
      </section>

      <footer className="dashboard-footer">
        <p>Gesix Data Quality Framework — Powered by Great Expectations</p>
      </footer>
    </div>
  );
}
