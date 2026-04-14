import * as XLSX from "xlsx";

export function downloadCleanedDataXlsx(
  cleanedData,
  filename = "cleaned_data.xlsx",
) {
  if (!cleanedData || cleanedData.length === 0) {
    alert("No cleaned data to export.");
    return;
  }
  const worksheet = XLSX.utils.json_to_sheet(cleanedData);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, "Cleaned Data");
  XLSX.writeFile(workbook, filename);
}
