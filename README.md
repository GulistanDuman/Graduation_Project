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
git clone [https://github.com/GulistanDuman/graduation_project-.git](https://github.com/GulistanDuman/graduation_project-.git)
cd graduation_project-

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
<img width="1919" height="900" alt="login" src="https://github.com/user-attachments/assets/b4720b01-29b6-43d3-86cb-15ebf5c0da1e" />

You can also register a new institutional admin account via the registration interface.
<img width="1916" height="895" alt="register" src="https://github.com/user-attachments/assets/7a72f54d-86ad-4958-aa37-e57d6d40d765" />

## How To Search A Stock

In the chat input, type a ticker or a full request:
<img width="1919" height="902" alt="report" src="https://github.com/user-attachments/assets/9e23e5b6-5e8a-4bb8-b26e-48a4e3b3db0f" />

<img width="1913" height="899" alt="summary" src="https://github.com/user-attachments/assets/75a4bcb4-23ce-44a0-9af9-4611c9ce527e" />

<img width="1915" height="897" alt="metrics" src="https://github.com/user-attachments/assets/31d4ca8a-0c48-4bcc-b44c-a64310cd983b" />

<img width="1919" height="905" alt="live log" src="https://github.com/user-attachments/assets/d2b4f086-971a-4649-8ec8-28013332c3d6" />

<img width="1518" height="890" alt="live_tracking" src="https://github.com/user-attachments/assets/5e63fe81-29cc-4c83-8cfd-7366021df410" />

<img width="1911" height="907" alt="nodes2" src="https://github.com/user-attachments/assets/1da503d4-4ba6-4e1c-ae7e-15acb5c3650e" />

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
