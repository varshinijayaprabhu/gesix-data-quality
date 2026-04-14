import { FileUp, Globe, Link2 } from "lucide-react";
import { format } from "date-fns";

export const INGESTION_METHODS = [
  {
    id: "upload",
    icon: FileUp,
    label: "File Upload",
    desc: "CSV, JSON, Excel, PDF",
  },
  { id: "api", icon: Globe, label: "Custom API", desc: "REST endpoint" },
  { id: "scraping", icon: Link2, label: "Web Scraper", desc: "Target URL" },
];

/**
 * Builds the payload for the data processing API.
 *
 * @param {Object} options - The form options from the dashboard.
 * @returns {Object} The formatted payload for the API.
 */
export function buildPayload({
  sourceType,
  sourceUrl,
  useDateRange,
  startDate,
  endDate,
  apiInputMode,
  apiKey,
  selectedFile,
}) {
  const isUpload = sourceType === "upload";
  return {
    sourceType: isUpload ? "others_upload" : sourceType,
    sourceUrl,
    startDate:
      !isUpload && useDateRange && startDate
        ? format(startDate, "yyyy-MM-dd")
        : "",
    endDate:
      !isUpload && useDateRange && endDate ? format(endDate, "yyyy-MM-dd") : "",
    apiKey: apiInputMode === "key" ? apiKey : "",
    file: isUpload ? selectedFile : null,
  };
}
