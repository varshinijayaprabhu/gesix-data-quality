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
    doc.text('No property data to display.', margin, y);
    doc.save(filename);
    return;
  }

  const colWidths = { add: 55, price: 35, listed_date: 40 };
  const headers = ['Address', 'Price', 'Listed Date'];

  doc.setFont(undefined, 'bold');
  doc.text(headers[0], margin, y);
  doc.text(headers[1], margin + colWidths.add, y);
  doc.text(headers[2], margin + colWidths.add + colWidths.price, y);
  y += lineHeight;
  doc.setFont(undefined, 'normal');

  doc.setDrawColor(200, 200, 200);
  doc.line(margin, y - 2, pageW - margin, y - 2);
  y += 4;

  for (const row of properties) {
    if (y > 270) {
      doc.addPage();
      y = 20;
      doc.setFont(undefined, 'bold');
      doc.text(headers[0], margin, y);
      doc.text(headers[1], margin + colWidths.add, y);
      doc.text(headers[2], margin + colWidths.add + colWidths.price, y);
      y += lineHeight;
      doc.setFont(undefined, 'normal');
      y += 4;
    }
    const addr = (row.add ?? row.address ?? '').toString();
    const price = row.price != null ? String(row.price) : '—';
    const date = (row.listed_date ?? '').toString();
    doc.text(addr.substring(0, 32), margin, y);
    doc.text(price, margin + colWidths.add, y);
    doc.text(date, margin + colWidths.add + colWidths.price, y);
    y += lineHeight;
  }

  doc.save(filename);
}
