import os

class ReportGenerator:
    """
    Generates professional summaries of the Data Quality Pipeline results.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def generate_summary(self, scores):
        """Prints a professional dashboard to the console."""
        if not scores:
            print("[!] No quality scores available to report.")
            return

        print("\n" + "╔" + "═"*48 + "╗")
        print("║" + " "*13 + "📊 DATA QUALITY DASHBOARD" + " "*10 + "║")
        print("╠" + "═"*48 + "╣")
        print(f"║  Total Records Processed: {scores['total_records']:<25} ║")
        print(f"║  Overall Trustability:    {scores['trustability_score']:>5}%" + " "*18 + "║")
        print(f"║  Items Flagged for Review: {scores['remediation_required']:<24} ║")
        print("╚" + "═"*48 + "╝\n")

    def save_report(self, scores, filename="quality_report.txt"):
        """Saves the report to the processed data folder."""
        report_path = os.path.join(self.base_dir, "data", "processed", filename)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("DATA QUALITY REPORT\n")
            f.write("="*30 + "\n")
            f.write(f"Total Processed: {scores['total_records']}\n")
            f.write(f"Trustability: {scores['trustability_score']}%\n")
            f.write(f"Issues Found: {scores['remediation_required']}\n")
            
        print(f"[+] Detailed report saved to: {report_path}")
        return report_path
