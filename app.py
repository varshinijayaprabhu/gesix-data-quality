from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

@app.route('/')
def index():
    """Serves the generated Data Quality Dashboard."""
    try:
        return send_from_directory(PROCESSED_DIR, "dashboard.html")
    except Exception as e:
        return f"<h1>Error: Dashboard not found</h1><p>Please run the pipeline first using <code>python main.py</code></p><p>Technical Error: {str(e)}</p>"

from flask import request, redirect, url_for
from main import run_pipeline

@app.route('/process', methods=['POST'])
def process():
    """Triggers the data pipeline with user-provided dates."""
    start_date = request.form.get('start_date')
    end_date = request.form.get('end_date')
    
    print(f"[*] Web Server trigger: Processing data from {start_date} to {end_date}")
    try:
        run_pipeline(start_date=start_date, end_date=end_date)
        return redirect(url_for('index'))
    except Exception as e:
        return f"<h1>⚠️ Pipeline Error</h1><p>{str(e)}</p><a href='/'>Go Back</a>"

if __name__ == '__main__':
    print("\n" + "="*50)
    print("🚀 Gesix Web Server Starting...")
    print(f"🔗 Local URL: http://localhost:8080")
    print("="*50 + "\n")
    app.run(debug=True, host='localhost', port=8080)
