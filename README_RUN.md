# How to Run the Gesix Project

## React frontend + Flask backend (recommended)

### 1. Install dependencies

**Backend (project root):**
```bash
pip install -r requirements.txt
```

**Frontend:**
```bash
cd frontend
npm install
```

### 2. Start the backend
From the **project root**:
```bash
python app.py
```
Backend runs at **http://localhost:8080** and exposes:
- `GET /api/report` – latest quality report (JSON)
- `POST /api/process` – run pipeline (JSON body: `start_date`, `end_date`)

### 3. Start the React frontend
In a **second terminal**:
```bash
cd frontend
npm run dev
```
Open **http://localhost:5173** (or the port Vite prints). You get:
- **Landing page** (`/`) – intro and link to Dashboard
- **Dashboard** (`/dashboard`) – run new analysis (date range + Process Data) and view trustability scores

The frontend proxies `/api` to the Flask server, so no CORS issues when using the dev server.

---

## Legacy: HTML dashboard only

If you only want the static HTML dashboard:
1. Run `python main.py` once (with date prompts) to generate `data/processed/dashboard.html`.
2. Run `python app.py` and open **http://localhost:8080** to serve that HTML and use the form (POST `/process`).

---

**Troubleshooting**
- **501 on POST:** Do not use `python -m http.server`. Use `python app.py` for the backend.
- **Report not loading:** Run `python main.py` once to create `data/processed/cleaned_data.csv`, or use “Process Data” on the Dashboard with a date range.
