# Stock Market Research Agent

AI-powered stock research project with a FastAPI backend and a plain JavaScript frontend. The app runs multi-step research agents, tracks node progress live, generates a formatted stock report, and supports PDF/Markdown download.

<!-- 1. RESİM: ANA KARŞILAMA EKRANI -->
<img width="1919" height="905" alt="dashboard" src="https://github.com/user-attachments/assets/f52f748d-2b03-404a-b9f6-f2b62739e8e5" />

## Features

- Admin login
- Stock/ticker search
- Live agent node tracking
- Research, financial metrics, charts, risk signal, and one-year prediction
- Report view with PDF and Markdown download
- Local SQLite runtime storage

## Requirements

- Windows 10/11
- Python 3.10 or newer
- Git
- API keys:
  - `OPENAI_API_KEY`
  - `TAVILY_API_KEY`
  - `GOOGLE_API_KEY`

## Download From GitHub

Open PowerShell and run:

```powershell
git clone https://github.com/GulistanDuman/graduation_project-.git
cd graduation_project-
```

If you downloaded the ZIP from GitHub, extract it first, then open PowerShell inside the extracted folder.

## Create Python Environment

```powershell
python -m venv myenv
.\myenv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r stock_market\backend\requirements.txt
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\myenv\Scripts\Activate.ps1
```

## Add API Keys

Create a file named `.env` in the project root:

```env
OPENAI_API_KEY=your_openai_key_here
TAVILY_API_KEY=your_tavily_key_here
GOOGLE_API_KEY=your_google_key_here
```

Do not upload/share your `.env` file.

## Run Backend

Open PowerShell in the project root:

```powershell
.\myenv\Scripts\Activate.ps1
cd stock_market\backend
python -m uvicorn app:app --host 127.0.0.1 --port 8000
```

Backend health check:

```text
http://127.0.0.1:8000/
```

Expected response:

```json
{"status":"ok","service":"DumaX Stock Agents API"}
```

## Run Frontend

Open a second PowerShell window:

```powershell
cd path\to\graduation_project-\stock_market\frontend
python -m http.server 3000
```

Open the app:

```text
http://127.0.0.1:3000/
```

## Login

Use the local admin account:

```text
Username: admin
Password: admin@1234
```

Signup is disabled in this version.

## How To Search A Stock

In the chat input, type a ticker or a full request:

```text
AAPL stock analysis with one year prediction
```

Other examples:

```text
NVDA stock analysis
CRWV stock analysis with one year prediction
AVGO stock analysis with buy hold sell recommendation
```

## Download Report

After the analysis completes:

- Use `PDF` button to download a formatted PDF.
- Use `Markdown` button to download the report text.
- Use `Delete This Report` to remove a report from local history.

## Stop Servers

Press `Ctrl + C` in each PowerShell window:

- Backend window
- Frontend window

## Common Issues

### No module named uvicorn

You are probably using the wrong environment. Run:

```powershell
cd path\to\graduation_project-
.\myenv\Scripts\Activate.ps1
python -m pip install -r stock_market\backend\requirements.txt
```

### Failed to fetch

Backend is not running or port `8000` is blocked. Start backend again and check:

```text
http://127.0.0.1:8000/
```

### Frontend opens but analysis does not start

Make sure backend and frontend are both running:

```text
Backend:  http://127.0.0.1:8000/
Frontend: http://127.0.0.1:3000/
```

### API key error

Check that `.env` exists in the project root and contains valid keys.

## Project Structure

```text
stock_market/
  backend/
    app.py
    stock_agent_core.py
    requirements.txt
  frontend/
    index.html
    app.js
    style.css
```

## Notes

This project is for research and educational use. It does not provide financial advice. Always verify data and make investment decisions independently.
