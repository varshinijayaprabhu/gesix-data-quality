import { jsPDF } from 'jspdf';

/**
 * Generate a PDF from the processed properties (API JSON data).
 * Uses jsPDF to create a downloadable report.
 */
export function downloadPropertiesPdf(properties, filename = 'gesix-data-quality-report.pdf') {
  const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
  const pageW = doc.internal.pageSize.getWidth();
  const margin = 15;
  let y = 20;
  const lineHeight = 7;

  doc.setFontSize(18);
  doc.text('Gesix Data Quality – Generated Data', margin, y);
  y += lineHeight * 2;

  doc.setFontSize(10);
  doc.text(`Total properties: ${properties.length}`, margin, y);
  y += lineHeight * 1.5;

  if (properties.length === 0) {
    doc.text('No dataset records to display.', margin, y);
    doc.save(filename);
    return;
  }

  // Dynamically extract headers from keys (limit to first 5 for better fit)
  const allKeys = Object.keys(properties[0]);
  const headers = allKeys.slice(0, 5); 
  const availableWidth = pageW - (margin * 2);
  const colWidth = availableWidth / headers.length;

  const renderHeader = (currentY) => {
    doc.setFont(undefined, 'bold');
    headers.forEach((header, i) => {
      const cleanHeader = header.replace(/_/g, ' ').toUpperCase();
      doc.text(cleanHeader.substring(0, 15), margin + (i * colWidth), currentY);
    });
    doc.setFont(undefined, 'normal');
    return currentY + lineHeight;
  };

  y = renderHeader(y);

  doc.setDrawColor(200, 200, 200);
  doc.line(margin, y - 2, pageW - margin, y - 2);
  y += 4;

  for (const row of properties) {
    if (y > 270) {
      doc.addPage();
      y = 20;
      y = renderHeader(y);
      y += 4;
    }
    
    headers.forEach((header, i) => {
      const val = row[header] != null ? String(row[header]) : '—';
      doc.text(val.substring(0, 15), margin + (i * colWidth), y);
    });
    
    y += lineHeight;
  }

  doc.save(filename);
}
