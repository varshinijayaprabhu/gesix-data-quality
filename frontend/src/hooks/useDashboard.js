import { useState, useCallback } from "react";
import { processData, retrieveAnalysis } from "../api";
import { buildPayload } from "../services/dashboardService";

/**
 * Custom hook for managing Dashboard state and operations.
 */
export function useDashboard() {
  const [report, setReport] = useState(null);
  const [rawReport, setRawReport] = useState(null);
  const [cleanedReport, setCleanedReport] = useState(null);
  const [rawData, setRawData] = useState([]);
  const [cleanedData, setCleanedData] = useState([]);
  const [noisyData, setNoisyData] = useState([]);
  const [reportUrl, setReportUrl] = useState(null);
  const [edaUrl, setEdaUrl] = useState(null);
  const [rawEdaUrl, setRawEdaUrl] = useState(null);
  const [analysisId, setAnalysisId] = useState(null);
  const [retrievedRecord, setRetrievedRecord] = useState(null);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState(null);

  const executeAnalysis = useCallback(async (formState) => {
    setError(null);
    setProcessing(true);
    setReport(null);
    setRawReport(null);
    setCleanedReport(null);
    setRawData([]);
    setCleanedData([]);
    setNoisyData([]);
    setReportUrl(null);
    setRetrievedRecord(null);

    try {
      const payload = buildPayload(formState);
      const data = await processData(payload);

      setReport(data.report);
      setRawReport(data.raw_report);
      setCleanedReport(data.cleaned_report);
      setReportUrl(data.report_url);
      setEdaUrl(data.eda_url);
      setRawEdaUrl(data.raw_eda_url);
      setAnalysisId(data.id || data.report?.id);

      // Get raw data (before cleaning)
      const rawList = data.raw_data?.data || data.raw_data?.properties || [];
      setRawData(Array.isArray(rawList) ? rawList : []);

      // Get cleaned data (after remediation)
      const cleanedList =
        data.cleaned_data?.data || data.cleaned_data?.properties || [];
      setCleanedData(Array.isArray(cleanedList) ? cleanedList : []);

      // Get noisy data (flagged rows with quality issues)
      const noisyList =
        data.noisy_data?.data || data.noisy_data?.properties || [];
      setNoisyData(Array.isArray(noisyList) ? noisyList : []);

      return data;
    } catch (err) {
      setError(err.message);
      throw err;
    } finally {
      setProcessing(false);
    }
  }, []);

  const loadRemoteAnalysis = useCallback(async (fileId) => {
    setError(null);
    setProcessing(true);
    setRetrievedRecord(null);
    setReport(null);
    try {
      const record = await retrieveAnalysis(fileId);

      // Store as a retrieved record (metadata only, not a full analysis report)
      setRetrievedRecord(record);
      setReportUrl(record.analysis_report_url);
      setEdaUrl(record.eda_profile_url);
      setRawEdaUrl(record.raw_eda_profile_url);
      setAnalysisId(record.id);

      return record;
    } catch (err) {
      setError(err.message);
      throw err;
    } finally {
      setProcessing(false);
    }
  }, []);

  const resetDashboard = useCallback(() => {
    setReport(null);
    setRawReport(null);
    setCleanedReport(null);
    setRawData([]);
    setCleanedData([]);
    setNoisyData([]);
    setReportUrl(null);
    setEdaUrl(null);
    setRawEdaUrl(null);
    setAnalysisId(null);
    setRetrievedRecord(null);
    setError(null);
  }, []);

  return {
    report,
    rawReport,
    cleanedReport,
    rawData,
    cleanedData,
    noisyData,
    reportUrl,
    edaUrl,
    rawEdaUrl,
    analysisId,
    retrievedRecord,
    processing,
    error,
    executeAnalysis,
    resetDashboard,
    loadRemoteAnalysis,
  };
}
