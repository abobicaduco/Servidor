# Abobi Cron Server

**Abobi Cron Server** is a professional, lightweight Python-based workflow orchestrator and cron scheduler. It is designed to manage and monitor automation scripts with ease, providing a real-time dashboard and robust process management.

![Abobi Cron Server](static_build/preview.png)

## Key Features

- **Dynamic Scheduling**: Leverages `APScheduler` for precise cron-like scheduling without high CPU overhead.
- **Workflow Orchestration**: Define sequences of scripts (workflows) that run in order.
- **Real-Time Monitoring**: A sleek React-based dashboard (Corporate Dark Mode) to track running processes, PIDs, and execution logs.
- **Priority Queueing**: Automatically handles "catch-up" for missed runs and manages a queue with priority.
- **Node-Free Deployment**: The frontend comes pre-compiled, allowing you to run the entire server using only Python.
- **Process Management**: Integrated `psutil` support for clean process termination (no zombie processes).
- **Hot-Reload**: Automatically detects changes in your automation folder or scheduling spreadsheets.

## Architecture

- **Backend**: Python / Flask
- **Frontend**: React / Tailwind CSS (Pre-compiled in `static_build/`)
- **Storage**: Excel-based (`.xlsx`) configuration for ease of use in corporate environments.
- **Concurrency**: Managed via Semaphores (Limit: 3 simultaneous processes by default).

## Quick Start

### 1. Prerequisites
- Python 3.8+
- Requirements: `pip install -r requirements.txt`

### 2. Configuration
Copy the template environment file:
```bash
cp .env.example .env
```
Edit `.env` and set the absolute paths to your:
- Automation scripts folder
- `registration_automacoes.xlsx` (Main database)
- `workflows.xlsx` (Workflow definitions)

### 3. Usage
Run the server:
```bash
python main.py
```
The dashboard will automatically open at `http://127.0.0.1:5000`.

## Advanced Deployment (No Node.js)
This project is designed for restricted environments. You can build the frontend on a machine with Node.js and commit the `static_build/` folder. On the target machine (e.g., corporate PC), you only need Python to serve the UI.

## Contributing
Feel free to fork and submit pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](LICENSE)
