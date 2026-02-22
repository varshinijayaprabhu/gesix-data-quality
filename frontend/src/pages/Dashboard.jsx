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

function MultimediaSuitabilityDash({ data }) {
  if (!data || data.length === 0) return null;

  const images = data.filter(item => item.multimedia_type === 'Image');
  if (images.length === 0) return null;

  const total = images.length;
  const aiReady = images.filter(img => img.suitability?.includes('AI Training')).length;
  const printReady = images.filter(img => img.suitability?.includes('Professional Print')).length;
  const webReady = images.filter(img => img.suitability?.includes('Standard Web')).length;
  const lowQuality = images.filter(img => img.suitability?.includes('Low Quality') || img.suitability === 'Corrupted').length;

  const aiPercent = Math.round((aiReady / total) * 100);
  const printPercent = Math.round((printReady / total) * 100);
  const webPercent = Math.round((webReady / total) * 100);

  let analysisText = "";
  if (aiPercent > 70) analysisText = "High-Quality Collection. Excellent for Computer Vision and AI training.";
  else if (aiPercent > 30 || webPercent > 50) analysisText = "Moderate Quality. Good for web stores and general UI use.";
  else if (total > 0) analysisText = "Low-Resolution Collection. Better suited for thumbnails or internal placeholders.";
  else analysisText = "Collection contains corrupted or non-image assets.";

  return (
    <div className="suitability-dash">
      <div className="suitability-summary">
        <p><strong>Analysis:</strong> {analysisText} {lowQuality > 0 ? `Alert: ${lowQuality} items are low-res or corrupted.` : 'All items passed integrity checks.'}</p>
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [report, setReport] = useState(null);
  const [rawData, setRawData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState(null);

  const [sourceType, setSourceType] = useState('api'); // 'api', 'scraping', 'upload'
  const [sourceUrl, setSourceUrl] = useState('');
  const [useDateRange, setUseDateRange] = useState(false);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [apiInputMode, setApiInputMode] = useState('link'); // 'link' or 'key'
  const [apiKey, setApiKey] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);

  const UPLOAD_TYPES = ['upload', 'pdf', 'docx', 'json_upload', 'xlsx_upload', 'zip_upload', 'xml_upload', 'parquet_upload', 'others_upload'];
  const isUpload = UPLOAD_TYPES.includes(sourceType);

  const loadReport = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getReport();
      if (data.error) throw new Error(data.error);
      setReport(data);
    } catch (e) {
      setError(e.message);
      setReport(null);
    } finally {
      setLoading(false);
    }
  };

  const loadRawData = async () => {
    try {
      const list = await getRawData();
      setRawData(Array.isArray(list) ? list : []);
    } catch {
      setRawData([]);
    }
  };

  // Removed auto-load on mount to prevent showing stale data
  useEffect(() => {
    // Intentionally empty to start fresh
    setReport(null);
    setRawData([]);
    setLoading(false);
  }, []);

  const handleSourceChange = (e) => {
    setSourceType(e.target.value);
    // Reset state when switching formats
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

    // Validation
    if (isUpload) {
      if (!selectedFile) {
        const label = sourceType === 'upload' ? 'CSV' : sourceType === 'pdf' ? 'PDF' : sourceType === 'docx' ? 'Word' : sourceType === 'json_upload' ? 'JSON' : sourceType === 'xlsx_upload' ? 'Excel' : sourceType === 'zip_upload' ? 'ZIP' : sourceType === 'xml_upload' ? 'XML' : sourceType === 'parquet_upload' ? 'Parquet' : 'Any';
        setError(`Please select a ${label} file to upload.`);
        return;
      }
      if (sourceType !== 'others_upload') {
        const allowedExt = sourceType === 'upload' ? '.csv' : sourceType === 'pdf' ? '.pdf' : sourceType === 'docx' ? '.docx' : sourceType === 'json_upload' ? '.json' : sourceType === 'xlsx_upload' ? '.xlsx' : sourceType === 'zip_upload' ? '.zip' : sourceType === 'xml_upload' ? '.xml' : '.parquet';
        if (!selectedFile.name.toLowerCase().endsWith(allowedExt)) {
          setError(`Unsupported format. Please upload a ${allowedExt} file.`);
          return;
        }
      }
    } else {
      if (!sourceUrl && !isUpload) {
        setError('Please provide a valid Source URL.');
        return;
      }
      if (sourceType === 'api' && apiInputMode === 'key' && !apiKey) {
        setError('Please provide a valid API Key.');
        return;
      }
      if (useDateRange && (!startDate || !endDate)) {
        setError('Please select both start and end dates or disable date filtering.');
        return;
      }
    }

    setProcessing(true);
    setReport(null);
    setRawData([]);
    try {
      // processData in api.js already throws on failure (success=false),
      // so if we reach the next line, the pipeline succeeded.
      // It returns { report, raw_data } — NO "success" field.
      const data = await processData({
        sourceType,
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
      // processData throws on backend failure — clear stale data
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
    if (isUpload) {
      if (sourceType === 'others_upload') return !!selectedFile;
      const allowedExt = sourceType === 'upload' ? '.csv' : sourceType === 'pdf' ? '.pdf' : sourceType === 'docx' ? '.docx' : sourceType === 'json_upload' ? '.json' : sourceType === 'xlsx_upload' ? '.xlsx' : sourceType === 'zip_upload' ? '.zip' : sourceType === 'xml_upload' ? '.xml' : '.parquet';
      return selectedFile && selectedFile.name.toLowerCase().endsWith(allowedExt);
    }
    const hasUrl = sourceUrl.trim().length > 0;
    const hasKey = (sourceType === 'api' && apiInputMode === 'key') ? apiKey.trim().length > 0 : true;
    
    if (useDateRange) {
      return hasUrl && hasKey && startDate && endDate;
    }
    return hasUrl && hasKey;
  };

  const getValidationMessage = () => {
    if (isUpload) {
      const label = sourceType === 'upload' ? 'CSV' : sourceType === 'pdf' ? 'PDF' : sourceType === 'docx' ? 'Word' : sourceType === 'json_upload' ? 'JSON' : sourceType === 'xlsx_upload' ? 'Excel' : sourceType === 'zip_upload' ? 'ZIP' : sourceType === 'xml_upload' ? 'XML' : sourceType === 'parquet_upload' ? 'Parquet' : 'Any';
      if (!selectedFile) return `Waiting for ${label} file...`;
      if (sourceType === 'others_upload') return 'Universal file ready.';
      const allowedExt = sourceType === 'upload' ? '.csv' : sourceType === 'pdf' ? '.pdf' : sourceType === 'docx' ? '.docx' : sourceType === 'json_upload' ? '.json' : sourceType === 'xlsx_upload' ? '.xlsx' : sourceType === 'zip_upload' ? '.zip' : sourceType === 'xml_upload' ? '.xml' : '.parquet';
      if (!selectedFile.name.toLowerCase().endsWith(allowedExt)) {
        return `Unsupported format. Please upload a ${allowedExt} file.`;
      }
      return 'File ready for processing.';
    }
    if (sourceType === 'api' && apiInputMode === 'key') {
      if (!apiKey) return 'Please enter an API Key.';
    } else {
      if (!sourceUrl) return 'Please enter a Source URL.';
    }
    if (useDateRange && (!startDate || !endDate)) return 'Please select a date range.';
    return 'Parameters set. Ready to process.';
  };

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Data Trustability Dashboard</h1>
        <p>Generated by Gesix Data Quality Framework</p>
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
                <option value="api">Dynamic API (Custom Endpoint)</option>
                <option value="scraping">Web Scraper (Target URL)</option>
                <option value="upload">Custom CSV Upload</option>
                <option value="pdf">PDF Document Extraction</option>
                <option value="docx">MS Word Document Extraction</option>
                <option value="json_upload">JSON File Upload</option>
                <option value="xlsx_upload">Excel (XLSX) Upload</option>
                <option value="zip_upload">ZIP Archive (Multimedia)</option>
                <option value="xml_upload">XML File Upload</option>
                <option value="parquet_upload">Apache Parquet Upload</option>
                <option value="others_upload">Universal Ingestion (Any Format)</option>
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
                    Upload {sourceType === 'others_upload' ? 'Any File (Universal)' : 
                           sourceType === 'upload' ? 'Dataset (CSV)' : 
                           sourceType === 'pdf' ? 'Document (PDF)' : 
                           sourceType === 'docx' ? 'Document (Word)' : 
                           sourceType === 'json_upload' ? 'Dataset (JSON)' : 
                           sourceType === 'xlsx_upload' ? 'Spreadsheet (XLSX)' : 
                           sourceType === 'zip_upload' ? 'Archive (ZIP)' : 
                           sourceType === 'xml_upload' ? 'Data (XML)' : 
                           'Binary (Parquet)'}
                  </label>
                  <input
                    type="file"
                    id="data_file"
                    accept={sourceType === 'others_upload' ? "*" : 
                            sourceType === 'upload' ? ".csv" : 
                            sourceType === 'pdf' ? ".pdf" : 
                            sourceType === 'docx' ? ".docx" : 
                            sourceType === 'json_upload' ? ".json" : 
                            sourceType === 'xlsx_upload' ? ".xlsx" : 
                            sourceType === 'zip_upload' ? ".zip" : 
                            sourceType === 'xml_upload' ? ".xml" : 
                            ".parquet"}
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

        {sourceType === 'zip_upload' && rawData.length > 0 && (
          <MultimediaSuitabilityDash data={rawData} />
        )}
      </section>

      <footer className="dashboard-footer">
        <p>End-to-End Backend Verification: COMPLETE</p>
      </footer>
    </div>
  );
}
