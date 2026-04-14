import { jsPDF } from "jspdf";

// Helper to draw border on new pages
function drawPageBorder(doc, border, pageW, pageH) {
  doc.setDrawColor(0, 0, 0); // Always black
  doc.setLineWidth(0.5);
  doc.rect(border / 2, border / 2, pageW - border, pageH - border, "S");
}

export function downloadPropertiesPdf(
  cleanedData,
  cleanedReport,
  rawData,
  rawReport,
  filename = "data-quality-and-trustability-framework.pdf",
) {
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "a4" });
  doc.setFont("times", "normal");
  const pageW = doc.internal.pageSize.getWidth();
  const pageH = doc.internal.pageSize.getHeight();
  const border = 15; // 1.5cm in mm
  const margin = border + 5; // Content margin inside border
  let y = margin + 5;
  const lineHeight = 6;
  // Draw border on all four sides (1.5cm)
  drawPageBorder(doc, border, pageW, pageH);

  // Title
  doc.setFontSize(14);
  doc.setFont("times", "bold");
  doc.text("Data Quality and Trustability Framework", margin, y);
  doc.setFont("times", "normal");
  y += lineHeight * 2;

  // ...existing code...

  // Raw Data Section
  doc.setFontSize(12);
  doc.setFont("times", "bold");
  doc.text("Raw Data Quality", margin, y);
  doc.setFont("times", "normal");
  y += lineHeight * 1.5;
  if (rawReport) {
    doc.setFontSize(10);
    doc.setFont("times", "bold");
    doc.text(
      `Overall Trustability: ${rawReport.overall_trustability}%`,
      margin,
      y,
    );
    y += lineHeight;
    doc.text(`Total Records: ${rawReport.total_records}`, margin, y);
    y += lineHeight * 1.5;
    doc.setFontSize(12);
    doc.text("Quality Dimensions", margin, y);
    doc.setFont("times", "normal");
    y += lineHeight;
    doc.setDrawColor(200, 200, 200);
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;
    const dimensions = rawReport.dimensions || {};
    Object.entries(dimensions).forEach(([dim, value]) => {
      const score = typeof value === "object" ? value.score : value;
      const status = score >= 90 ? "PASS" : score >= 70 ? "WARN" : "FAIL";
      doc.setFont("times", "bold");
      doc.text(dim, margin, y);
      doc.setFont("times", "normal");
      doc.text(`${score}%  [${status}]`, margin + 40, y);
      y += lineHeight;
      // Remediation explanation paragraph
      doc.setFontSize(10);
      doc.setFont("times", "italic");
      let explanation = "";
      switch (dim.toLowerCase()) {
        case "completeness":
          explanation =
            "Completeness was improved by filling missing values and blanks using imputation strategies, ensuring all records are present.";
          break;
        case "accuracy":
          explanation =
            "Accuracy was enhanced by correcting outliers and normalizing values to expected statistical ranges.";
          break;
        case "validity":
          explanation =
            "Validity was addressed by enforcing correct data types and formats, and applying custom business rules to fix invalid entries.";
          break;
        case "consistency":
          explanation =
            "Consistency was ensured by resolving mismatches in related fields (like dates, IDs) and harmonizing duplicate or conflicting values.";
          break;
        case "uniqueness":
          explanation =
            "Uniqueness was improved by removing duplicate records and repeated values from the dataset.";
          break;
        case "integrity":
          explanation =
            "Integrity was strengthened by fixing missing or incorrect metadata and referential links, such as foreign keys.";
          break;
        case "lineage":
          explanation =
            "Lineage was maintained by ensuring continuity and completeness across the dataset, eliminating gaps and blank rows.";
          break;
        default:
          explanation = "";
      }
      if (explanation) {
        const wrapped = doc.splitTextToSize(explanation, pageW - margin * 2);
        doc.text(wrapped, margin, y);
        y += wrapped.length * lineHeight;
      }
      doc.setFontSize(10);
      doc.setFont(undefined, "normal");
      // ...existing code...
    });
    y += lineHeight;
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;
  }
  // Raw Data Table
  doc.setFontSize(13);
  doc.setFont(undefined, "bold");
  doc.text("Raw Dataset Preview", margin, y);
  y += lineHeight;
  doc.setFont(undefined, "normal");
  doc.setFontSize(10);
  doc.text(`Total properties: ${rawData.length}`, margin, y);
  y += lineHeight * 1.5;
  if (rawData.length === 0) {
    doc.setFontSize(10);
    doc.text("No dataset records to display.", margin, y);
    y += lineHeight;
  } else {
    const allKeys = Object.keys(rawData[0]);
    const headers = allKeys.slice(0, 5);
    const availableWidth = pageW - margin * 2;
    const colWidth = availableWidth / headers.length;
    doc.setFontSize(9);
    doc.setFont("times", "bold");
    // Draw table header
    headers.forEach((header, i) => {
      const cleanHeader = header.replace(/_/g, " ").toUpperCase();
      doc.text(cleanHeader.substring(0, 15), margin + i * colWidth, y);
      // Draw header cell border
      doc.rect(
        margin + i * colWidth,
        y - lineHeight + 2,
        colWidth,
        lineHeight,
        "S",
      );
    });
    doc.setFont("times", "normal");
    y += lineHeight;
    // Draw table rows
    for (const row of rawData) {
      if (y > 270) {
        doc.addPage();
        drawPageBorder(doc, border, pageW, pageH);
        y = margin + 5;
        // Redraw header
        doc.setFont("times", "bold");
        headers.forEach((header, i) => {
          const cleanHeader = header.replace(/_/g, " ").toUpperCase();
          doc.text(cleanHeader.substring(0, 15), margin + i * colWidth, y);
          doc.rect(
            margin + i * colWidth,
            y - lineHeight + 2,
            colWidth,
            lineHeight,
            "S",
          );
        });
        doc.setFont("times", "normal");
        y += lineHeight;
      }
      headers.forEach((header, i) => {
        const val = row[header] != null ? String(row[header]) : "—";
        doc.text(val.substring(0, 15), margin + i * colWidth, y);
        doc.rect(
          margin + i * colWidth,
          y - lineHeight + 2,
          colWidth,
          lineHeight,
          "S",
        );
      });
      y += lineHeight;
    }
    y += lineHeight * 2;
  }

  // Start Cleaned Data Quality Check Report on a new page
  doc.addPage();
  drawPageBorder(doc, border, pageW, pageH);
  y = margin + 5;
  doc.setFontSize(12);
  doc.setFont("times", "bold");
  doc.text("Cleaned Data Quality Check Report", margin, y);
  doc.setFont("times", "normal");
  y += lineHeight * 1.5;
  doc.setFontSize(10);
  doc.text(
    "This section summarizes the quality scores and diagnostics for your cleaned dataset after remediation.",
    margin,
    y,
  );
  y += lineHeight * 1.5;
  if (cleanedReport) {
    doc.setFontSize(10);
    doc.setFont("times", "bold");
    doc.text(
      `Overall Trustability: ${cleanedReport.overall_trustability}%`,
      margin,
      y,
    );
    y += lineHeight;
    doc.text(`Total Records: ${cleanedReport.total_records}`, margin, y);
    y += lineHeight * 1.5;
    doc.setFontSize(12);
    doc.text("Quality Dimensions", margin, y);
    doc.setFont("times", "normal");
    y += lineHeight;
    doc.setDrawColor(200, 200, 200);
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;
    const dimensions = cleanedReport.dimensions || {};
    Object.entries(dimensions).forEach(([dim, value]) => {
      const score = typeof value === "object" ? value.score : value;
      const status = score >= 90 ? "PASS" : score >= 70 ? "WARN" : "FAIL";
      const reason =
        typeof value === "object" && value.reason ? value.reason : null;
      doc.setFont("times", "bold");
      // If y is too close to bottom, add page
      if (y + lineHeight * 3 > pageH - border) {
        doc.addPage();
        drawPageBorder(doc, border, pageW, pageH);
        y = margin + 5;
      }
      doc.text(dim, margin, y);
      doc.setFont("times", "normal");
      doc.text(`${score}%  [${status}]`, margin + 40, y);
      y += lineHeight;
      // Dimension explanation paragraph
      doc.setFontSize(10);
      doc.setFont("times", "italic");
      let explanation = "";
      switch (dim.toLowerCase()) {
        case "completeness":
          explanation =
            "Completeness measures the proportion of data that is present and not missing. Factors affecting the score include missing values, blanks, or incomplete records.";
          break;
        case "accuracy":
          explanation =
            "Accuracy assesses whether numeric values fall within expected statistical ranges. Scores are affected by outliers and values outside expected bounds.";
          break;
        case "validity":
          explanation =
            "Validity checks if values match their expected data types and formats, including custom business rules. Invalid types or formats reduce the score.";
          break;
        case "consistency":
          explanation =
            "Consistency evaluates whether related data (like dates, IDs) is logically consistent across records. Inconsistencies indicate data errors.";
          break;
        case "uniqueness":
          explanation =
            "Uniqueness measures the absence of duplicate records. Duplicate rows or repeated values decrease the score.";
          break;
        case "integrity":
          explanation =
            "Integrity assesses the presence and correctness of key metadata and referential links (like foreign keys). Missing or incorrect links lower the score.";
          break;
        case "lineage":
          explanation =
            "Lineage checks for data continuity and completeness across the dataset, including blank rows or gaps. Discontinuity reduces the score.";
          break;
        default:
          explanation = "";
      }
      if (explanation) {
        const wrapped = doc.splitTextToSize(explanation, pageW - margin * 2);
        // If explanation would overflow page, add page
        if (y + wrapped.length * lineHeight > pageH - border) {
          doc.addPage();
          drawPageBorder(doc, border, pageW, pageH);
          y = margin + 5;
        }
        doc.text(wrapped, margin, y);
        y += wrapped.length * lineHeight;
      }
      doc.setFontSize(10);
      doc.setFont(undefined, "normal");
    });
    y += lineHeight;
    doc.line(margin, y - 2, pageW - margin, y - 2);
    y += 4;
  }
  // Cleaned Data Table
  doc.setFontSize(13);
  doc.setFont(undefined, "bold");
  doc.text("Cleaned Dataset Preview", margin, y);
  y += lineHeight;
  doc.setFont(undefined, "normal");
  doc.setFontSize(10);
  doc.text(`Total properties: ${cleanedData.length}`, margin, y);
  y += lineHeight * 1.5;
  if (cleanedData.length === 0) {
    doc.setFontSize(10);
    doc.text("No dataset records to display.", margin, y);
    y += lineHeight;
  } else {
    const allKeys = Object.keys(cleanedData[0]);
    const headers = allKeys.slice(0, 5);
    const availableWidth = pageW - margin * 2;
    const colWidth = availableWidth / headers.length;
    doc.setFontSize(9);
    doc.setFont("times", "bold");
    // Draw table header
    headers.forEach((header, i) => {
      const cleanHeader = header.replace(/_/g, " ").toUpperCase();
      doc.text(cleanHeader.substring(0, 15), margin + i * colWidth, y);
      // Draw header cell border
      doc.rect(
        margin + i * colWidth,
        y - lineHeight + 2,
        colWidth,
        lineHeight,
        "S",
      );
    });
    doc.setFont("times", "normal");
    y += lineHeight;
    // Draw table rows
    for (const row of cleanedData) {
      if (y > 270) {
        doc.addPage();
        drawPageBorder(doc, border, pageW, pageH);
        y = margin + 5;
        // Redraw header
        doc.setFont("times", "bold");
        headers.forEach((header, i) => {
          const cleanHeader = header.replace(/_/g, " ").toUpperCase();
          doc.text(cleanHeader.substring(0, 15), margin + i * colWidth, y);
          doc.rect(
            margin + i * colWidth,
            y - lineHeight + 2,
            colWidth,
            lineHeight,
            "S",
          );
        });
        doc.setFont("times", "normal");
        y += lineHeight;
      }
      headers.forEach((header, i) => {
        const val = row[header] != null ? String(row[header]) : "—";
        doc.text(val.substring(0, 15), margin + i * colWidth, y);
        doc.rect(
          margin + i * colWidth,
          y - lineHeight + 2,
          colWidth,
          lineHeight,
          "S",
        );
      });
      y += lineHeight;
    }
  }
  doc.save(filename);
}
