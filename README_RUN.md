# 🚀 How to Run the Gesix Project

To avoid **501 Unsupported Method** errors, please follow these steps to run the project correctly.

## 1. Start the Flask Server
The project requires a Flask backend to process data. Do **NOT** use `python -m http.server`.

Run this command in your terminal:
```bash
python app.py
```

## 2. Access the Dashboard
Once the server is running, open your web browser and go to:
**[http://localhost:8080](http://localhost:8080)**

## 3. Process Data
1. On the dashboard, locate the **"Run New Analysis"** section.
2. Select a **Start Date** and **End Date**.
3. Click **"🚀 Process Data"**.
4. The backend will fetch and clean the data, then automatically redirect you back to the updated dashboard.

---
**Troubleshooting:**
* **Error 501:** You are likely running `python -m http.server`. Stop that process and run `python app.py` instead.
* **Error 404:** Ensure you are in the project root directory before running `python app.py`.
