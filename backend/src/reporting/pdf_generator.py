import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.units import mm
import datetime

class PDFGenerator:
    def __init__(self, filename):
        self.filename = filename
        self.c = canvas.Canvas(self.filename, pagesize=A4)
        self.width, self.height = A4
        self.border = 15 * mm
        self.margin = 20 * mm
        self.y = self.height - self.margin
        self.line_height = 6 * mm

    def draw_page_border(self):
        self.c.setStrokeColor(colors.black)
        self.c.setLineWidth(0.5)
        self.c.rect(self.border/2, self.border/2, self.width - self.border, self.height - self.border)

    def add_text(self, text, size=12, bold=False, italic=False, x_offset=0):
        font = "Times-Bold" if bold else ("Times-Italic" if italic else "Times-Roman")
        self.c.setFont(font, size)
        self.c.drawString(self.margin + x_offset, self.y, text)

    def add_wrapped_text(self, text, size=10, italic=True):
        self.c.setFont("Times-Italic" if italic else "Times-Roman", size)
        from reportlab.lib.utils import simpleSplit
        lines = simpleSplit(text, "Times-Italic" if italic else "Times-Roman", size, self.width - self.margin * 2)
        for line in lines:
            if self.y < self.margin + self.line_height:
                self.new_page()
            self.c.drawString(self.margin, self.y, line)
            self.y -= self.line_height

    def new_page(self):
        self.c.showPage()
        self.draw_page_border()
        self.y = self.height - self.margin

    def generate(self, raw_report, cleaned_report, raw_data, cleaned_data):
        self.draw_page_border()
        
        # Title
        self.add_text("Data Quality and Trustability Framework", size=14, bold=True)
        self.y -= self.line_height * 1.5
        self.add_text(f"Report Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", size=10)
        self.y -= self.line_height * 2

        # Raw Data Section
        if raw_report:
            self.add_text("Raw Data Quality", size=12, bold=True)
            self.y -= self.line_height
            self.add_text(f"Overall Trustability: {raw_report.get('overall_trustability')}%", size=10, bold=True)
            self.y -= self.line_height
            self.add_text(f"Total Records: {raw_report.get('total_records')}", size=10, bold=True)
            self.y -= self.line_height * 1.5
            
            self.add_text("Quality Dimensions", size=11, bold=True)
            self.y -= self.line_height
            
            dimensions = raw_report.get("dimensions", {})
            for dim, value in dimensions.items():
                if self.y < self.margin + self.line_height * 2:
                    self.new_page()
                
                score = value.get("score") if isinstance(value, dict) else value
                status = "PASS" if score >= 90 else ("WARN" if score >= 70 else "FAIL")
                
                self.add_text(f"{dim}: {score}% [{status}]", size=10, bold=True)
                self.y -= self.line_height
                
                # Add descriptions similar to pdfExport.js
                desc = self.get_dimension_description(dim)
                self.add_wrapped_text(desc)
                self.y -= self.line_height / 2

        # Dataset Preview (Simple table)
        if self.y < self.margin + self.line_height * 5:
            self.new_page()
        
        self.y -= self.line_height
        self.add_text("Cleaned Data Quality Check Report", size=12, bold=True)
        self.y -= self.line_height * 1.5
        
        if cleaned_report:
            self.add_text(f"Overall Trustability: {cleaned_report.get('overall_trustability')}%", size=10, bold=True)
            self.y -= self.line_height
            self.add_text(f"Total Records: {cleaned_report.get('total_records')}", size=10, bold=True)
            self.y -= self.line_height * 1.5
            
            dimensions = cleaned_report.get("dimensions", {})
            for dim, value in dimensions.items():
                if self.y < self.margin + self.line_height * 2:
                    self.new_page()
                
                score = value.get("score") if isinstance(value, dict) else value
                status = "PASS" if score >= 90 else ("WARN" if score >= 70 else "FAIL")
                
                self.add_text(f"{dim}: {score}% [{status}]", size=10, bold=True)
                self.y -= self.line_height
                
                desc = self.get_dimension_description(dim)
                self.add_wrapped_text(desc)
                self.y -= self.line_height / 2

        self.c.save()
        return self.filename

    def get_dimension_description(self, dim):
        dim = dim.lower()
        if dim == "completeness": return "Completeness was improved by filling missing values or blanks, ensuring all records are present."
        if dim == "accuracy": return "Accuracy was enhanced by correcting outliers and normalizing values to expected statistical ranges."
        if dim == "validity": return "Validity was addressed by enforcing correct data types and formats, and applying custom business rules."
        if dim == "consistency": return "Consistency was ensured by resolving mismatches in related fields and harmonizing duplicate or conflicting values."
        if dim == "uniqueness": return "Uniqueness was improved by removing duplicate records and repeated values from the dataset."
        if dim == "integrity": return "Integrity was strengthened by fixing missing or incorrect metadata and referential links."
        if dim == "lineage": return "Lineage checks for data continuity and completeness across the dataset, eliminating gaps."
        return ""

def generate_pdf_report(raw_report, cleaned_report, raw_data, cleaned_data, output_path):
    generator = PDFGenerator(output_path)
    return generator.generate(raw_report, cleaned_report, raw_data, cleaned_data)
