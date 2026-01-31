# Repository Guidelines

## Project Structure & Module Organization

This repository hosts a small Flask web app for browsing files and loading CSV/Parquet data into DuckDB.

- `app.py`: Flask server and DuckDB integration.
- `templates/`: HTML templates for the UI.
- `static/`: Front-end assets (CSS/JS).
- `requirements.txt`: Python dependencies.

## Build, Test, and Development Commands

Create a virtual environment and install dependencies:

- `python -m venv .venv`
- `./.venv/Scripts/Activate.ps1`
- `pip install -r requirements.txt`

Run the server locally:

- `python app.py`

The app binds to `127.0.0.1` and is intended for local-only use.

## Coding Style & Naming Conventions

- Use 4-space indentation for Python.
- Keep route handlers thin; core logic lives in helper methods.
- File names use `snake_case` (e.g., `app.py`).
- Table names should be `snake_case` when possible.

## Testing Guidelines

No automated tests yet. If you add tests, place them under `tests/` and document the test command here.

## Commit & Pull Request Guidelines

No conventions are established yet. Use clear, imperative commit messages (e.g., “Add query results grid”).

## Security & Configuration Tips

The app can browse any path on the machine. Do not expose it publicly. If you add configuration, prefer environment variables and document them in `README.md`.