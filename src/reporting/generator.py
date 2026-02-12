import os

class ReportGenerator:
    """
    Enhanced Reporting Layer: Generates deep-dive dashboard for the 7 quality dimensions.
    """
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    def generate_summary(self, report):
        if not report:
            print("[!] Error: No report data to display.")
            return

        print("\n" + "╔" + "═"*50 + "╗")
        print("║" + " "*12 + "📍 MASTER QUALITY DASHBOARD" + " "*11 + "║")
        print("╠" + "═"*50 + "╣")
        print(f"║  OVERALL TRUSTABILITY SCORE: {report['overall_trustability']:>6}%" + " "*13 + "║")
        print("╠" + "─"*50 + "╢")
        
        for dim, score in report['dimensions'].items():
            bar = "█" * int(score / 10) + "░" * (10 - int(score / 10))
            print(f"║  {dim:<15} [{bar}] {score:>6}%" + " "*14 + "║")
            
        print("╚" + "═"*50 + "╝\n")

    def save_report(self, report, filename="final_quality_report.txt"):
        report_path = os.path.join(self.base_dir, "data", "processed", filename)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=== DATA TRUSTABILITY REPORT ===\n")
            f.write(f"Timestamp: {os.path.getmtime(report_path) if os.path.exists(report_path) else 'New'}\n")
            f.write(f"Total Rows: {report['total_records']}\n")
            f.write(f"Overall Score: {report['overall_trustability']}%\n\n")
            f.write("--- Dimension Breakdown ---\n")
            for dim, score in report['dimensions'].items():
                f.write(f"{dim}: {score}%\n")
                
        print(f"[+] Detailed Technical Report saved: data/processed/{filename}")
