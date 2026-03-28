import { useState, useEffect } from "react";
import { downloadPropertiesPdf } from "../pdfExport";
import { downloadCleanedDataXlsx } from "../lib/downloadCleanedDataXlsx";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  FileUp,
  ArrowRight,
  Download,
  Activity,
  ExternalLink,
  Calendar as CalendarIcon,
  FileText,
  Copy,
  Search,
  Lock,
  Clock,
  X,
  Check,
  Loader2,
  AlertCircle,
  Database,
  ShieldCheck,
} from "lucide-react";
import { format } from "date-fns";

import { useDashboard } from "../hooks/useDashboard";
import { INGESTION_METHODS } from "../services/dashboardService";

function StatBlock({ label, value, description, isFail }) {
  return (
    <div
      className={`card-stat group ${isFail ? "ring-2 ring-destructive/40" : ""}`}
    >
      <div>
        <p className="text-xs tracking-widest uppercase font-semibold text-[var(--stat-card-foreground)] opacity-70 mb-2">
          {label}
        </p>
        <div className="text-4xl md:text-5xl font-serif tracking-tight mb-3 text-[var(--stat-card-foreground)] transition-transform duration-500 group-hover:-translate-y-1">
          {value}
        </div>
      </div>
      {description && (
        <p className="text-sm text-[var(--stat-card-foreground)] opacity-90 leading-relaxed max-w-sm">
          {description}
        </p>
      )}
    </div>
  );
}

function DatePicker({ label, value, onChange, disabled }) {
  return (
    <div className="space-y-3 flex flex-col">
      <Label className="text-xs uppercase tracking-widest font-bold text-muted-foreground">
        {label}
      </Label>
      <Popover>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            disabled={disabled}
            className={`btn-outline w-full ${!value && "text-muted-foreground"}`}
          >
            <CalendarIcon className="mr-2 h-4 w-4 text-muted-foreground opacity-80 dark:text-foreground" />
            {value ? (
              format(value, "PPP")
            ) : (
              <span className="text-muted-foreground opacity-80">
                Pick a date
              </span>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar
            mode="single"
            selected={value}
            onSelect={onChange}
            initialFocus
            className="rounded-lg border border-border"
            captionLayout="dropdown"
          />
        </PopoverContent>
      </Popover>
    </div>
  );
}

function EmptyState({ title, message }) {
  return (
    <div className="container mx-auto px-6 py-24 text-center">
      <h2 className="text-6xl font-serif italic text-muted-foreground/30 mb-6">
        {title}
      </h2>
      <p className="text-xl text-muted-foreground max-w-md mx-auto">
        {message}
      </p>
    </div>
  );
}

export default function Dashboard() {
  const {
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
    processing,
    error,
    executeAnalysis,
    resetDashboard,
    loadRemoteAnalysis,
    retrievedRecord,
  } = useDashboard();

  const [sourceType, setSourceType] = useState("upload");
  const [sourceUrl, setSourceUrl] = useState("");
  const [useDateRange, setUseDateRange] = useState(false);
  const [startDate, setStartDate] = useState();
  const [endDate, setEndDate] = useState();
  const [apiInputMode, setApiInputMode] = useState("link");
  const [apiKey, setApiKey] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);

  const [searchId, setSearchId] = useState("");
  const [edaViewerUrl, setEdaViewerUrl] = useState(null);
  const [edaViewerTitle, setEdaViewerTitle] = useState("");

  const isUpload = sourceType === "upload";

  useEffect(() => {
    resetDashboard();
  }, [resetDashboard]);

  const handleRetrieve = async (e) => {
    if (e) e.preventDefault();
    if (!searchId.trim()) return;
    try {
      await loadRemoteAnalysis(searchId);
    } catch (err) {
      // Error handled by hook
    }
  };

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
  };

  const isValidUrl = (urlStr) => {
    try {
      const parsed = new URL(urlStr);
      // Strictly enforce HTTPS as per security requirements
      if (parsed.protocol !== "https:") return false;
      return true;
    } catch {
      return false;
    }
  };

  const isFormValid = () => {
    if (isUpload) return !!selectedFile;
    const hasUrl = sourceUrl.trim().length > 0 && isValidUrl(sourceUrl.trim());
    const hasKey =
      sourceType === "api" && apiInputMode === "key"
        ? apiKey.trim().length >= 8
        : true;
    return useDateRange
      ? hasUrl && hasKey && startDate && endDate
      : hasUrl && hasKey;
  };

  const handleSubmit = async (e) => {
    e?.preventDefault();
    await executeAnalysis({
      sourceType,
      sourceUrl,
      useDateRange,
      startDate,
      endDate,
      apiInputMode,
      apiKey,
      selectedFile,
    });
  };

  const rawTableKeys =
    rawData.length > 0 && typeof rawData[0] === "object"
      ? Object.keys(rawData[0])
      : [];
  const cleanedTableKeys =
    cleanedData.length > 0 && typeof cleanedData[0] === "object"
      ? Object.keys(cleanedData[0])
      : [];
  const noisyTableKeys =
    noisyData.length > 0 && typeof noisyData[0] === "object"
      ? Object.keys(noisyData[0])
      : [];
  const isNoData = report?.status === "No Data Found for this period";
  const showResults = report && !report.error && !isNoData;

  return (
    <div className="min-h-screen bg-background text-foreground font-sans flex flex-col selection:bg-primary/20">
      {/* ── Form ── */}
      <main className="flex-1 container mx-auto px-6 py-16 md:py-24">
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-16 lg:gap-24 items-start">
          {/* Left — sticky header + submit */}
          <div className="lg:col-span-5 lg:sticky lg:top-32 space-y-10">
            <div>
              <h1 className="text-5xl md:text-7xl font-serif font-medium tracking-tight leading-[1.1] mb-8">
                Run Quality <br />
                <span className="italic text-muted-foreground">Analysis.</span>
              </h1>
              <p className="text-lg md:text-xl text-muted-foreground leading-relaxed max-w-md">
                Configure your data pipeline ingestion source. We will extract,
                normalize, and score your dataset against the 7-dimensional
                trust framework.
              </p>
            </div>

            <button
              onClick={handleSubmit}
              disabled={processing || !isFormValid()}
              className={`group w-full md:w-auto md:min-w-[300px] px-8 py-6 text-lg flex items-center gap-3 rounded-full
                ${
                  processing || !isFormValid()
                    ? "bg-secondary text-muted-foreground cursor-not-allowed border border-border"
                    : "btn-gold"
                }`}
            >
              <span>
                {processing ? "Processing Dataset..." : "Execute Pipeline"}
              </span>
              {processing ? (
                <Activity className="w-6 h-6 animate-spin opacity-70" />
              ) : (
                <ArrowRight className="w-6 h-6 transform group-hover:translate-x-2 transition-transform" />
              )}
            </button>

            {error && (
              <p className="text-destructive font-medium bg-destructive/5 px-6 py-4 rounded-xl border border-destructive/20 animate-in fade-in">
                {error}
              </p>
            )}
          </div>

          {/* Right — form steps */}
          <div className="lg:col-span-7 space-y-20 pt-8 lg:pt-0">
            {/* Step 01 — Ingestion method */}
            <div className="space-y-8 animate-in slide-in-from-bottom-8 fade-in duration-700">
              <h3 className="text-2xl font-serif">
                01. Select Ingestion Method
              </h3>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                {INGESTION_METHODS.map(({ id, icon: Icon, label, desc }) => (
                  <button
                    key={id}
                    type="button"
                    onClick={() => setSourceType(id)}
                    className={`flex flex-col items-start p-6 rounded-3xl border transition-all duration-300 text-left
                      ${
                        sourceType === id
                          ? "border-foreground bg-foreground/5 shadow-inner"
                          : "border-border/60 hover:border-foreground/30 hover:bg-secondary/20"
                      }`}
                  >
                    <Icon
                      className={`w-6 h-6 mb-4 ${sourceType === id ? "text-foreground" : "text-muted-foreground"}`}
                    />
                    <span className="font-semibold text-lg mb-1">{label}</span>
                    <span className="text-sm text-muted-foreground">
                      {desc}
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {/* Step 02 — Configure source */}
            <div className="space-y-8 animate-in slide-in-from-bottom-8 fade-in duration-700 delay-150">
              <h3 className="text-2xl font-serif">02. Configure Source</h3>

              {isUpload ? (
                <div className="space-y-6">
                  {/* Upload Best Practices Info Box */}
                  <div className="bg-primary/5 hover:bg-primary/10 border-2 border-primary/20 rounded-2xl p-6 transition-colors duration-300">
                    <h4 className="flex items-center gap-2 text-primary font-bold tracking-wide uppercase text-sm mb-4">
                      <Activity className="w-4 h-4" />
                      Upload Best Practices
                    </h4>
                    <ul className="space-y-3 text-sm text-muted-foreground">
                      <li className="flex gap-3">
                        <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                        <span>
                          <strong>Data Formatting:</strong> Ensure your file has
                          only column headers in the first row. Remove any
                          overarching sheet labels or blank rows before the
                          data.
                        </span>
                      </li>
                      <li className="flex gap-3">
                        <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                        <span>
                          <strong>File Size Limit:</strong> The maximum allowed
                          file size is <strong>50MB</strong>.
                        </span>
                      </li>
                      <li className="flex gap-3">
                        <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                        <span>
                          <strong>Accepted Formats:</strong> .csv, .json, .xlsx,
                          .xls, .xml, .txt, .pdf.{" "}
                          <em>
                            Executable files or scripts are strictly prohibited.
                          </em>
                        </span>
                      </li>
                    </ul>
                  </div>

                  {/* Dropzone */}
                  <div className="relative group rounded-2xl border-2 border-dashed border-border/60 hover:border-gold/60 bg-secondary/10 hover:bg-secondary/20 transition-all duration-300 w-full max-w-lg overflow-hidden flex-col items-center justify-center min-h-[200px] flex">
                    <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none p-6 text-center">
                      <div
                        className={`h-16 w-16 rounded-full ${selectedFile ? "bg-gold/20" : "bg-primary/10"} flex items-center justify-center mb-6 group-hover:scale-110 group-hover:-translate-y-1 transition-all duration-500`}
                      >
                        {selectedFile ? (
                          <FileText className="h-8 w-8 text-gold" />
                        ) : (
                          <FileUp className="h-8 w-8 text-primary group-hover:animate-bounce" />
                        )}
                      </div>
                      <p className="text-base font-semibold text-foreground mb-2">
                        {selectedFile
                          ? selectedFile.name
                          : "Drag & drop or click to upload"}
                      </p>
                      <p className="text-sm font-medium text-muted-foreground/80">
                        {selectedFile
                          ? "Ready for processing"
                          : "Must follow best practices above"}
                      </p>
                    </div>
                    <Input
                      type="file"
                      accept=".csv,.json,.xml,.pdf,.xlsx,.xls,.txt"
                      onChange={(e) => setSelectedFile(e.target.files[0])}
                      disabled={processing}
                      className="absolute inset-0 w-full h-full opacity-0 cursor-pointer text-[0]"
                    />
                  </div>
                </div>
              ) : (
                <div className="space-y-12">
                  {/* API / Web Scraping Best Practices Info Box */}
                  <div className="bg-primary/5 hover:bg-primary/10 border-2 border-primary/20 rounded-2xl p-6 transition-colors duration-300">
                    <h4 className="flex items-center gap-2 text-primary font-bold tracking-wide uppercase text-sm mb-4">
                      <Activity className="w-4 h-4" />
                      {sourceType === "api"
                        ? "API Ingestion Best Practices"
                        : "Web Scraping Best Practices"}
                    </h4>
                    <ul className="space-y-3 text-sm text-muted-foreground">
                      <li className="flex gap-3">
                        <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                        <span>
                          <strong>Secure Connections Only:</strong> All target
                          URLs must use the highly secure <strong>HTTPS</strong>{" "}
                          protocol. HTTP connections will not be executed.
                        </span>
                      </li>
                      {sourceType === "api" ? (
                        <>
                          <li className="flex gap-3">
                            <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                            <span>
                              <strong>Authentication:</strong> Ensure your API
                              Key or Bearer Token is valid for the target
                              endpoint. Keys must be at least 8 characters long
                              if provided.
                            </span>
                          </li>
                          <li className="flex gap-3">
                            <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                            <span>
                              <strong>Payload Formatting:</strong> The API
                              should return structured JSON or XML arrays.
                            </span>
                          </li>
                        </>
                      ) : (
                        <>
                          <li className="flex gap-3">
                            <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                            <span>
                              <strong>Data Extraction:</strong> Ensure the
                              target webpage contains structured tabular data
                              (e.g., HTML tables or list structures) for
                              accurate scraping.
                            </span>
                          </li>
                          <li className="flex gap-3">
                            <Check className="w-4 h-4 text-primary shrink-0 mt-0.5" />
                            <span>
                              <strong>Compliance:</strong> Verify that scraping
                              the target website complies with their Terms of
                              Service.
                            </span>
                          </li>
                        </>
                      )}
                    </ul>
                  </div>

                  <div>
                    <Label className="form-label">Target URL</Label>
                    <Input
                      type="url"
                      placeholder={
                        sourceType === "api"
                          ? "https://api.example.com/v1/data"
                          : "https://example.com/portal"
                      }
                      value={sourceUrl}
                      onChange={(e) => setSourceUrl(e.target.value)}
                      disabled={processing}
                      className={`input-field max-w-lg ${sourceUrl.trim().length > 0 && !isValidUrl(sourceUrl.trim()) ? "border-destructive focus-visible:ring-destructive/20" : ""}`}
                    />
                    {sourceUrl.trim().length > 0 &&
                      !isValidUrl(sourceUrl.trim()) && (
                        <p className="text-xs text-destructive mt-2 font-medium flex items-center gap-1.5">
                          <AlertCircle className="w-3.5 h-3.5" />
                          Security Policy: Only secure HTTPS connections are
                          permitted. HTTP is blocked.
                        </p>
                      )}
                  </div>

                  {sourceType === "api" && (
                    <div className="space-y-6">
                      <Label className="form-label">
                        Security Authorization
                      </Label>
                      <RadioGroup
                        value={apiInputMode}
                        onValueChange={setApiInputMode}
                        disabled={processing}
                        className="flex gap-8"
                      >
                        {[
                          { value: "link", label: "Public Endpoint" },
                          {
                            value: "key",
                            label: "Secured Endpoint (Key required)",
                          },
                        ].map(({ value, label }) => (
                          <div
                            key={value}
                            className="flex items-center space-x-3"
                          >
                            <RadioGroupItem
                              value={value}
                              id={`api-${value}`}
                              className="w-5 h-5 border-2"
                            />
                            <Label
                              htmlFor={`api-${value}`}
                              className="text-base cursor-pointer"
                            >
                              {label}
                            </Label>
                          </div>
                        ))}
                      </RadioGroup>

                      {apiInputMode === "key" && (
                        <div className="mt-6 space-y-2 animate-in fade-in zoom-in-95">
                          <Label className="text-sm font-medium text-muted-foreground flex items-center gap-1.5">
                            API Key / Bearer Token
                            {apiKey.trim().length > 0 &&
                              apiKey.trim().length < 8 && (
                                <span className="text-[10px] text-destructive uppercase tracking-widest font-black ml-auto">
                                  Invalid
                                </span>
                              )}
                            {apiKey.trim().length >= 8 && (
                              <span className="text-[10px] text-emerald-500 uppercase tracking-widest font-black ml-auto">
                                Valid Format
                              </span>
                            )}
                          </Label>
                          <Input
                            type="password"
                            placeholder="Enter your API key or token here"
                            value={apiKey}
                            onChange={(e) => setApiKey(e.target.value)}
                            disabled={processing}
                            className={`input-field font-mono max-w-lg ${apiKey.trim().length > 0 && apiKey.trim().length < 8 ? "border-destructive focus-visible:ring-destructive/20" : ""}`}
                          />
                          <p className="text-xs text-muted-foreground flex justify-between">
                            <span>
                              Key will be sent as Authorization header and
                              x-api-key
                            </span>
                            {apiKey.trim().length > 0 &&
                              apiKey.trim().length < 8 && (
                                <span className="text-destructive font-bold">
                                  Minimum 8 characters required
                                </span>
                              )}
                          </p>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Step 03 — Timeframe (non-upload only) */}
            {!isUpload && (
              <div className="space-y-8 animate-in slide-in-from-bottom-8 fade-in duration-700 delay-300">
                <div className="flex flex-col space-y-4">
                  <Label
                    htmlFor="date_range"
                    className="text-2xl font-serif cursor-pointer"
                  >
                    03. Timeframe Filter
                  </Label>
                  <div className="flex items-center space-x-3">
                    <Checkbox
                      id="date_range"
                      checked={useDateRange}
                      onCheckedChange={setUseDateRange}
                      disabled={processing}
                      className="w-5 h-5 border-2 border-primary rounded-full data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground"
                    />
                    <Label
                      htmlFor="date_range"
                      className="form-label mb-0 cursor-pointer"
                    >
                      Enable Date Range Filtering
                    </Label>
                  </div>
                </div>

                {useDateRange && (
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-8 pl-10 border-l-2 border-border/40 mt-8">
                    <DatePicker
                      label="Period Start"
                      value={startDate}
                      onChange={setStartDate}
                      disabled={processing}
                    />
                    <DatePicker
                      label="Period End"
                      value={endDate}
                      onChange={setEndDate}
                      disabled={processing}
                    />
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </main>

      {/* ── Results ── */}
      {showResults && (
        <section className="border-t border-border mt-12 bg-secondary/5 animate-in fade-in slide-in-from-bottom-16 duration-1000 relative">
          <div className="absolute inset-0 bg-gradient-to-b from-transparent to-background/80 pointer-events-none -z-10" />
          <div className="container mx-auto px-6 py-24 md:py-32">
            {/* Quality Scores - Side by Side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12 mb-24">
              {/* Raw Data Quality Score */}
              {rawReport && (
                <div className="surface-glass p-8 md:p-10 rounded-3xl border border-border/50 relative overflow-hidden group">
                  <div className="absolute top-0 right-0 w-64 h-64 bg-amber-500/10 rounded-full blur-[80px] -z-10 group-hover:bg-amber-500/20 transition-colors duration-500" />
                  <Badge
                    variant="outline"
                    className="mb-8 bg-amber-500/10 text-amber-600 dark:text-amber-400 px-5 py-2 text-xs tracking-[0.2em] font-bold uppercase rounded-full border-amber-500/20 shadow-sm"
                  >
                    Raw Data Quality
                  </Badge>
                  <h2 className="text-5xl md:text-7xl font-serif font-black tracking-tight mb-4 text-foreground drop-shadow-sm">
                    {Math.round(rawReport.overall_trustability)}
                    <span className="text-3xl md:text-5xl text-muted-foreground/50 font-medium">
                      /100
                    </span>
                  </h2>
                  <p className="text-xl font-medium text-foreground mb-10 flex items-center gap-3">
                    {rawReport.total_records}{" "}
                    <span className="text-sm text-muted-foreground uppercase tracking-widest font-bold bg-secondary/30 px-3 py-1 rounded-md">
                      Records Analyzed
                    </span>
                  </p>

                  {/* Raw Dimensions */}
                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 mt-8">
                    {rawReport.dimensions &&
                      Object.entries(rawReport.dimensions).map(
                        ([name, data]) => {
                          const score =
                            typeof data === "number" ? data : data?.score || 0;
                          const isFail = score < 70;
                          return (
                            <div
                              key={name}
                              className={`p-4 rounded-2xl border transition-all duration-300 hover:-translate-y-1 hover:shadow-md ${isFail ? "bg-destructive/5 border-destructive/20" : "bg-card border-border/50 shadow-sm"}`}
                            >
                              <p className="text-[10px] md:text-xs font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 truncate">
                                {name}
                              </p>
                              <p
                                className={`text-2xl md:text-3xl font-black ${isFail ? "text-destructive" : "text-foreground"}`}
                              >
                                {Math.round(score)}%
                              </p>
                            </div>
                          );
                        },
                      )}
                  </div>
                </div>
              )}

              {/* Cleaned Data Quality Score */}
              {cleanedReport && (
                <div className="surface-glass p-8 md:p-10 rounded-3xl border border-border/50 relative overflow-hidden group">
                  <div className="absolute top-0 right-0 w-64 h-64 bg-emerald-500/10 rounded-full blur-[80px] -z-10 group-hover:bg-emerald-500/20 transition-colors duration-500" />
                  <Badge
                    variant="outline"
                    className="mb-8 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 px-5 py-2 text-xs tracking-[0.2em] font-bold uppercase rounded-full border-emerald-500/20 shadow-sm"
                  >
                    Cleaned Data Quality
                  </Badge>
                  <h2 className="text-5xl md:text-7xl font-serif font-black tracking-tight mb-4 text-foreground drop-shadow-sm">
                    {Math.round(cleanedReport.overall_trustability)}
                    <span className="text-3xl md:text-5xl text-muted-foreground/50 font-medium">
                      /100
                    </span>
                  </h2>
                  <p className="text-xl font-medium text-foreground mb-10 flex items-center gap-3">
                    {cleanedReport.total_records}{" "}
                    <span className="text-sm text-muted-foreground uppercase tracking-widest font-bold bg-secondary/30 px-3 py-1 rounded-md">
                      Records Preserved
                    </span>
                  </p>

                  {/* Cleaned Dimensions */}
                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 mt-8">
                    {cleanedReport.dimensions &&
                      Object.entries(cleanedReport.dimensions).map(
                        ([name, data]) => {
                          const score =
                            typeof data === "number" ? data : data?.score || 0;
                          const isFail = score < 70;
                          return (
                            <div
                              key={name}
                              className={`p-4 rounded-2xl border transition-all duration-300 hover:-translate-y-1 hover:shadow-md ${isFail ? "bg-destructive/5 border-destructive/20" : "bg-card border-border/50 shadow-sm"}`}
                            >
                              <p className="text-[10px] md:text-xs font-bold uppercase tracking-[0.15em] text-muted-foreground mb-2 truncate">
                                {name}
                              </p>
                              <p
                                className={`text-2xl md:text-3xl font-black ${isFail ? "text-destructive" : "text-foreground"}`}
                              >
                                {Math.round(score)}%
                              </p>
                            </div>
                          );
                        },
                      )}
                  </div>
                </div>
              )}
            </div>

            {/* Data Tables - Full Width */}
            {(rawTableKeys.length > 0 || cleanedTableKeys.length > 0) && (
              <div className="mt-32 space-y-24">
                {/* Raw data table */}
                {rawTableKeys.length > 0 && (
                  <div className="w-full">
                    <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-8">
                      <div>
                        <h3 className="text-3xl md:text-4xl font-serif font-bold tracking-tight mb-3 flex items-center gap-3">
                          <Database className="w-8 h-8 text-primary" />
                          Raw Data
                        </h3>
                        <p className="text-lg text-muted-foreground font-medium">
                          Original ingested data before cleaning and
                          remediation.
                        </p>
                      </div>
                    </div>
                    <div className="border border-border/60 rounded-3xl overflow-hidden surface-glass shadow-lg">
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto custom-scrollbar">
                        <Table className="w-full min-w-max">
                          <TableHeader className="bg-secondary/40 sticky top-0 z-10 backdrop-blur-md">
                            <TableRow className="border-border/60 hover:bg-transparent">
                              {rawTableKeys.map((key) => (
                                <TableHead
                                  key={key}
                                  className="font-bold text-foreground whitespace-nowrap px-6 py-5 h-auto text-xs uppercase tracking-[0.15em]"
                                >
                                  {key.replace(/_/g, " ")}
                                </TableHead>
                              ))}
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {rawData.map((row, i) => (
                              <TableRow
                                key={i}
                                className="border-border/40 hover:bg-secondary/20 transition-colors"
                              >
                                {rawTableKeys.map((key) => (
                                  <TableCell
                                    key={key}
                                    className="font-mono text-sm px-6 py-4 whitespace-nowrap text-muted-foreground"
                                  >
                                    {row[key] != null ? String(row[key]) : "—"}
                                  </TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  </div>
                )}

                {/* Cleaned data table */}
                {cleanedTableKeys.length > 0 && (
                  <div className="w-full">
                    <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 mb-8">
                      <div>
                        <h3 className="text-3xl md:text-4xl font-serif font-bold tracking-tight mb-3 flex items-center gap-3">
                          <ShieldCheck className="w-8 h-8 text-primary" />
                          Cleaned Data
                        </h3>
                        <p className="text-lg text-muted-foreground font-medium">
                          Processed data after remediation and quality
                          improvements.
                        </p>
                      </div>
                      <div className="flex flex-wrap gap-4">
                        <button
                          onClick={() => downloadCleanedDataXlsx(cleanedData)}
                          className="btn-secondary text-sm py-2.5 px-6 font-bold shadow-sm"
                        >
                          <Download className="w-4 h-4 mr-2" />
                          <span>Download XLSX</span>
                        </button>
                      </div>
                    </div>

                    <div className="border border-border/50 rounded-2xl overflow-hidden bg-background">
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <Table className="w-full min-w-max">
                          <TableHeader className="bg-secondary/30 sticky top-0 z-10">
                            <TableRow className="border-border/50 hover:bg-transparent">
                              {cleanedTableKeys.map((key) => (
                                <TableHead
                                  key={key}
                                  className="font-semibold text-foreground whitespace-nowrap px-6 py-4 h-auto text-sm uppercase tracking-widest"
                                >
                                  {key.replace(/_/g, " ")}
                                </TableHead>
                              ))}
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {cleanedData.map((row, i) => (
                              <TableRow
                                key={i}
                                className="border-border/50 hover:bg-secondary/20 transition-colors"
                              >
                                {cleanedTableKeys.map((key) => (
                                  <TableCell
                                    key={key}
                                    className="font-mono text-base px-6 py-4 whitespace-nowrap text-muted-foreground"
                                  >
                                    {row[key] != null ? String(row[key]) : "—"}
                                  </TableCell>
                                ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                    {/* Big Download Quality Check Report button below table */}
                    <div className="flex justify-center mt-10 mb-16">
                      <button
                        onClick={() =>
                          downloadPropertiesPdf(
                            cleanedData,
                            cleanedReport || report,
                            rawData,
                            rawReport,
                          )
                        }
                        className="w-full max-w-xl px-8 py-6 text-2xl font-bold rounded-2xl bg-gold text-background flex items-center justify-center gap-4 shadow-lg hover:bg-amber-500 transition-all border-2 border-gold uppercase tracking-widest"
                      >
                        <Download className="w-8 h-8 mr-2" />
                        Download Quality Check Report
                      </button>
                    </div>

                    <div className="flex flex-col items-center gap-4 mt-10 mb-16">
                      <div className="flex gap-4 w-full max-w-xl">
                        {edaUrl && (
                          <button
                            onClick={() => {
                              setEdaViewerUrl(edaUrl);
                              setEdaViewerTitle("EDA Profile (Cleaned)");
                            }}
                            className="flex-1 px-4 py-4 text-lg font-bold rounded-xl bg-secondary text-secondary-foreground flex items-center justify-center gap-2 shadow-md hover:bg-secondary/80 transition-all border-2 border-secondary uppercase tracking-tight cursor-pointer"
                          >
                            <Activity className="w-5 h-5" />
                            Cleaned EDA
                          </button>
                        )}
                        {rawEdaUrl && (
                          <button
                            onClick={() => {
                              setEdaViewerUrl(rawEdaUrl);
                              setEdaViewerTitle("EDA Profile (Raw)");
                            }}
                            className="flex-1 px-4 py-4 text-lg font-bold rounded-xl bg-muted text-muted-foreground flex items-center justify-center gap-2 shadow-md hover:bg-muted/80 transition-all border-2 border-muted uppercase tracking-tight cursor-pointer"
                          >
                            <Activity className="w-5 h-5" />
                            Raw EDA
                          </button>
                        )}
                      </div>

                      {analysisId && (
                        <div className="mt-8 p-6 bg-primary/5 rounded-3xl border-2 border-primary/20 flex flex-col items-center gap-4 w-full max-w-xl">
                          <div className="text-center w-full">
                            <div className="flex items-center justify-center gap-2 mb-2">
                              <Lock className="w-4 h-4 text-primary" />
                              <p className="text-xs uppercase tracking-[0.2em] font-black text-primary">
                                Private Analysis ID
                              </p>
                            </div>
                            <p className="text-xl font-mono font-bold text-foreground break-all bg-background/50 p-4 rounded-xl border border-border/50 select-all">
                              {analysisId}
                            </p>
                            <p className="text-[10px] text-muted-foreground mt-3 uppercase tracking-widest font-bold">
                              Copy this ID to retrieve your report later. Valid
                              for 7 days.
                            </p>
                          </div>
                          <Button
                            variant="outline"
                            onClick={() => copyToClipboard(analysisId)}
                            className="w-full h-12 rounded-xl flex items-center justify-center gap-3 hover:bg-primary hover:text-primary-foreground transition-all duration-300 group"
                          >
                            <Copy className="w-5 h-5 group-hover:scale-110 transition-transform" />
                            <span className="font-bold uppercase tracking-widest">
                              Copy Analysis ID
                            </span>
                          </Button>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Combined Preprocessing View: Raw → Cleaned + Noisy */}
                {(cleanedData.length > 0 || noisyData.length > 0) && (
                  <div className="w-full">
                    <div className="mb-8">
                      <h3 className="text-3xl md:text-4xl font-serif font-bold tracking-tight mb-3 flex items-center gap-3">
                        <ShieldCheck className="w-8 h-8 text-primary" />
                        Preprocessing Results
                      </h3>
                      <p className="text-lg text-muted-foreground font-medium">
                        Complete view of preprocessing: {cleanedData.length}{" "}
                        cleaned rows + {noisyData.length} flagged rows ={" "}
                        {cleanedData.length + noisyData.length} total processed
                      </p>
                    </div>

                    <div className="border border-border/50 rounded-2xl overflow-hidden bg-background">
                      <div className="overflow-x-auto max-h-[700px] overflow-y-auto">
                        <Table className="w-full min-w-max">
                          <TableHeader className="bg-secondary/30 sticky top-0 z-10">
                            <TableRow className="border-border/50 hover:bg-transparent">
                              <TableHead className="font-semibold text-foreground whitespace-nowrap px-6 py-4 h-auto text-sm uppercase tracking-widest w-40">
                                Status
                              </TableHead>
                              {cleanedTableKeys.map((key) => (
                                <TableHead
                                  key={key}
                                  className="font-semibold text-foreground whitespace-nowrap px-6 py-4 h-auto text-sm uppercase tracking-widest"
                                >
                                  {key.replace(/_/g, " ")}
                                </TableHead>
                              ))}
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {/* Cleaned rows */}
                            {cleanedData.map((row, i) => (
                              <TableRow
                                key={`clean-${i}`}
                                className="border-border/50 hover:bg-secondary/20 transition-colors bg-green-50/30 dark:bg-green-950/20"
                              >
                                <TableCell className="font-bold text-green-700 dark:text-green-400 px-6 py-4 whitespace-nowrap">
                                  ✓ Cleaned
                                </TableCell>
                                {cleanedTableKeys.map((key) => (
                                  <TableCell
                                    key={key}
                                    className="font-mono text-base px-6 py-4 whitespace-nowrap text-muted-foreground"
                                  >
                                    {row[key] != null ? String(row[key]) : "—"}
                                  </TableCell>
                                ))}
                              </TableRow>
                            ))}

                            {/* Noisy rows with flags */}
                            {noisyData.map((row, i) => (
                              <TableRow
                                key={`noisy-${i}`}
                                className="border-border/50 hover:bg-amber-100/20 transition-colors bg-amber-50/30 dark:bg-amber-950/20"
                              >
                                <TableCell className="font-bold text-amber-900 dark:text-amber-200 px-6 py-4 whitespace-normal max-w-xs break-words">
                                  ⚠️ Flagged
                                  <br />
                                  <span className="text-xs font-mono opacity-75">
                                    {row["remediation_notes"] != null
                                      ? String(
                                          row["remediation_notes"],
                                        ).substring(0, 50) +
                                        (String(row["remediation_notes"])
                                          .length > 50
                                          ? "..."
                                          : "")
                                      : "See notes"}
                                  </span>
                                </TableCell>
                                {noisyTableKeys
                                  .filter((k) => k !== "remediation_notes")
                                  .map((key) => (
                                    <TableCell
                                      key={key}
                                      className="font-mono text-base px-6 py-4 whitespace-nowrap text-muted-foreground"
                                    >
                                      {row[key] != null
                                        ? String(row[key])
                                        : "—"}
                                    </TableCell>
                                  ))}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>

                    <div className="mt-6 p-4 bg-blue-50 dark:bg-blue-950/30 rounded-lg border border-blue-200 dark:border-blue-800">
                      <div className="flex gap-3">
                        <AlertCircle className="w-5 h-5 text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
                        <div className="text-sm text-blue-900 dark:text-blue-200">
                          <p>
                            <strong>Data Preservation Policy:</strong> No rows
                            are removed during preprocessing. Rows with quality
                            issues are flagged with detailed reasons. You can
                            review these rows and decide whether to:
                          </p>
                          <ul className="list-disc list-inside mt-2 ml-2 space-y-1">
                            <li>Manually correct and re-upload</li>
                            <li>Remove manually if truly invalid</li>
                            <li>
                              Accept as-is if issues are acceptable for your use
                              case
                            </li>
                          </ul>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </section>
      )}

      {isNoData && (
        <EmptyState
          title="Zero Results"
          message="No records were ingested. Ensure the target source has data or expand the requested time frame."
        />
      )}

      {/* Retrieved Record Display */}
      {retrievedRecord && (
        <section className="border-t border-border mt-12 bg-secondary/5 animate-in fade-in slide-in-from-bottom-16 duration-1000 relative">
          <div className="absolute inset-0 bg-gradient-to-t from-transparent to-background/50 pointer-events-none -z-10" />
          <div className="container mx-auto px-6 py-24 md:py-32">
            <div className="max-w-4xl mx-auto surface-glass p-8 md:p-14 rounded-3xl border border-border/60 text-center relative overflow-hidden group">
              <div className="absolute top-0 right-0 w-64 h-64 bg-emerald-500/5 rounded-full blur-[80px] -z-10 group-hover:bg-emerald-500/10 transition-colors duration-500" />
              <Badge
                variant="outline"
                className="mb-8 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 px-5 py-2 text-xs tracking-[0.2em] font-bold uppercase rounded-full border-emerald-500/20 shadow-sm"
              >
                Retrieved Successfully
              </Badge>
              <h2 className="text-4xl md:text-6xl font-serif font-black tracking-tight mb-6 text-foreground">
                {retrievedRecord.file_name}
              </h2>
              <div className="flex flex-wrap items-center justify-center gap-3 text-sm md:text-base text-muted-foreground font-medium mb-12">
                <span className="bg-secondary/30 px-3 py-1 rounded-md font-mono text-foreground">
                  {(() => {
                    const mime = retrievedRecord.file_type || "";
                    const map = {
                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                        "XLSX",
                      "application/vnd.ms-excel": "XLS",
                      "text/csv": "CSV",
                      "application/json": "JSON",
                      "application/pdf": "PDF",
                      "text/plain": "TXT",
                      "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                        "DOCX",
                    };
                    return (
                      map[mime.toLowerCase()] ||
                      mime.split("/").pop()?.toUpperCase() ||
                      retrievedRecord.file_name
                        ?.split(".")
                        .pop()
                        ?.toUpperCase() ||
                      "FILE"
                    );
                  })()}
                </span>
                <span>&middot;</span>
                <span className="flex items-center gap-1.5 object-contain">
                  {(() => {
                    const src = retrievedRecord.source || "";
                    const sourceMap = {
                      others_upload: "File Upload",
                      xlsx_upload: "XLSX Upload",
                      json_upload: "JSON Upload",
                      parquet_upload: "Parquet Upload",
                      api: "API Ingestion",
                      link: "URL Ingestion",
                      upload: "File Upload",
                    };
                    return (
                      sourceMap[src.toLowerCase()] ||
                      src
                        .replace(/_/g, " ")
                        .replace(/\b\w/g, (l) => l.toUpperCase())
                    );
                  })()}
                </span>
                <span>&middot;</span>
                <span className="flex items-center gap-1.5">
                  <Clock className="w-4 h-4" />
                  Uploaded{" "}
                  {new Date(retrievedRecord.upload_date).toLocaleDateString()}
                </span>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6 mt-12">
                {reportUrl && (
                  <a
                    href={reportUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="group/card flex flex-col items-center justify-center gap-4 p-8 rounded-2xl border-2 border-border/60 bg-background/80 hover:border-primary/50 hover:bg-primary/5 transition-all duration-300 hover:shadow-lg hover:-translate-y-1"
                  >
                    <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center group-hover/card:scale-110 transition-transform">
                      <FileText className="w-8 h-8 text-primary" />
                    </div>
                    <span className="text-sm font-black uppercase tracking-widest text-foreground">
                      Quality Report
                    </span>
                    <span className="text-xs text-muted-foreground flex items-center gap-1 mt-auto">
                      Open PDF <ExternalLink className="w-3 h-3" />
                    </span>
                  </a>
                )}
                {edaUrl && (
                  <button
                    onClick={() => {
                      setEdaViewerUrl(edaUrl);
                      setEdaViewerTitle("EDA Profile (Cleaned)");
                    }}
                    className="group/card flex flex-col items-center justify-center gap-4 p-8 rounded-2xl border-2 border-border/60 bg-background/80 hover:border-primary/50 hover:bg-primary/5 transition-all duration-300 hover:shadow-lg hover:-translate-y-1 cursor-pointer w-full"
                  >
                    <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center group-hover/card:scale-110 transition-transform">
                      <Activity className="w-8 h-8 text-primary" />
                    </div>
                    <span className="text-sm font-black uppercase tracking-widest text-foreground">
                      Cleaned EDA
                    </span>
                    <span className="text-xs text-muted-foreground flex items-center gap-1 mt-auto">
                      View Profile <ExternalLink className="w-3 h-3" />
                    </span>
                  </button>
                )}
                {rawEdaUrl && (
                  <button
                    onClick={() => {
                      setEdaViewerUrl(rawEdaUrl);
                      setEdaViewerTitle("EDA Profile (Raw)");
                    }}
                    className="group/card flex flex-col items-center justify-center gap-4 p-8 rounded-2xl border-2 border-border/60 bg-background/80 hover:border-amber-500/50 hover:bg-amber-500/5 transition-all duration-300 hover:shadow-lg hover:-translate-y-1 cursor-pointer w-full"
                  >
                    <div className="w-16 h-16 rounded-full bg-amber-500/10 flex items-center justify-center group-hover/card:scale-110 transition-transform">
                      <Activity className="w-8 h-8 text-amber-500" />
                    </div>
                    <span className="text-sm font-black uppercase tracking-widest text-foreground">
                      Raw EDA
                    </span>
                    <span className="text-xs text-muted-foreground flex items-center gap-1 mt-auto">
                      View Profile <ExternalLink className="w-3 h-3" />
                    </span>
                  </button>
                )}
              </div>

              {analysisId && (
                <div className="p-8 bg-primary/5 rounded-3xl border-2 border-primary/20 flex flex-col items-center gap-5 w-full max-w-xl mx-auto mt-16 shadow-inner">
                  <div className="text-center w-full">
                    <div className="flex items-center justify-center gap-2 mb-3 mt-2">
                      <span className="relative flex h-3 w-3">
                        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75"></span>
                        <span className="relative inline-flex rounded-full h-3 w-3 bg-primary"></span>
                      </span>
                      <p className="text-xs uppercase tracking-[0.2em] font-black text-primary">
                        Analysis ID
                      </p>
                    </div>
                    <p className="text-xl md:text-2xl font-mono font-black text-foreground break-all bg-background/80 p-5 rounded-2xl border border-border/80 select-all shadow-sm">
                      {analysisId}
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => copyToClipboard(analysisId)}
                    className="w-full h-14 rounded-xl flex items-center justify-center gap-3 hover:bg-primary/10 hover:text-primary hover:border-primary/50 transition-all duration-300 group border-2"
                  >
                    <Copy className="w-5 h-5 group-hover:scale-110 transition-transform" />
                    <span className="font-bold uppercase tracking-widest text-xs">
                      Copy Analysis ID
                    </span>
                  </Button>
                </div>
              )}

              {!reportUrl && !edaUrl && !rawEdaUrl && (
                <div className="mt-12 p-8 border border-border/50 rounded-2xl bg-secondary/10 flex flex-col items-center">
                  <Database className="w-12 h-12 text-muted-foreground/30 mb-4" />
                  <p className="text-muted-foreground text-lg font-medium">
                    No cloud artifacts available for this analysis.
                  </p>
                </div>
              )}
            </div>
          </div>
        </section>
      )}

      {/* Private Retrieval Section */}
      <section className="py-24 md:py-32 border-t border-border relative overflow-hidden">
        <div className="absolute inset-0 bg-background" />
        <div className="absolute -top-40 -right-40 w-96 h-96 bg-primary/5 rounded-full blur-[100px] pointer-events-none" />
        <div className="container mx-auto px-6 max-w-4xl text-center relative z-10">
          <div className="mb-14">
            <h2 className="text-4xl md:text-5xl font-serif font-black tracking-tight mb-6 flex items-center justify-center gap-4 text-foreground">
              <Lock className="w-10 h-10 text-primary" />
              Private Report Retrieval
            </h2>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto leading-relaxed">
              Enter your unique Analysis ID to retrieve your persistent cloud
              reports.
              <span className="block mt-4 font-bold text-primary flex items-center justify-center gap-2 bg-primary/5 w-fit mx-auto px-4 py-2 rounded-full border border-primary/10 text-sm">
                <Clock className="w-4 h-4" /> Reports are purged after 7 days
                for your privacy.
              </span>
            </p>
          </div>

          <form
            onSubmit={handleRetrieve}
            className="flex flex-col sm:flex-row gap-4 max-w-3xl mx-auto"
          >
            <div className="relative flex-1">
              <Search className="absolute left-5 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
              <Input
                placeholder="Paste your Analysis ID here..."
                value={searchId}
                onChange={(e) => setSearchId(e.target.value)}
                className="border-2 border-border/80 bg-background/50 backdrop-blur-sm pl-14 h-16 rounded-2xl focus-visible:border-primary focus-visible:ring-primary/20 text-foreground font-mono text-lg shadow-sm w-full"
              />
            </div>
            <Button
              type="submit"
              disabled={processing || !searchId.trim()}
              className="btn-primary rounded-2xl h-16 px-10 font-black uppercase tracking-widest text-sm w-full sm:w-auto"
            >
              {processing ? (
                <Loader2 className="w-6 h-6 animate-spin" />
              ) : (
                "Retrieve"
              )}
            </Button>
          </form>

          {error && (
            <div className="mt-8 animate-in fade-in slide-in-from-top-4">
              <p className="inline-flex items-center gap-2 text-destructive font-bold text-sm bg-destructive/10 py-3 px-6 rounded-xl border border-destructive/20 shadow-sm">
                <AlertCircle className="w-4 h-4" /> {error}
              </p>
            </div>
          )}
        </div>
      </section>

      {/* EDA Profile Iframe Viewer Modal */}
      {edaViewerUrl && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex flex-col animate-in fade-in duration-300">
          <div className="flex items-center justify-between px-6 py-4 bg-background/95 border-b border-border shadow-lg">
            <div className="flex items-center gap-3">
              <Activity className="w-5 h-5 text-primary" />
              <h3 className="text-lg font-bold uppercase tracking-widest">
                {edaViewerTitle}
              </h3>
            </div>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => {
                setEdaViewerUrl(null);
                setEdaViewerTitle("");
              }}
              className="rounded-full hover:bg-destructive/10 hover:text-destructive transition-colors"
            >
              <X className="w-5 h-5" />
            </Button>
          </div>
          <div className="flex-1 overflow-hidden">
            <iframe
              src={`/api/eda-viewer?url=${encodeURIComponent(edaViewerUrl)}`}
              title={edaViewerTitle}
              className="w-full h-full border-0"
              sandbox="allow-scripts allow-same-origin"
            />
          </div>
        </div>
      )}
    </div>
  );
}
