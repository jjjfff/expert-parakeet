from __future__ import annotations

import csv
import os
import random
import string
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from flask import Flask, jsonify, redirect, render_template, request

APP_HOST = "127.0.0.1"
APP_PORT = 5000
MAX_ROWS = 10_000

app = Flask(__name__)


@dataclass
class DuckDBManager:
    con: duckdb.DuckDBPyConnection | None = None
    mode: str | None = None
    db_path: str | None = None

    def connect(self, mode: str, db_path: str | None) -> None:
        if mode not in {"memory", "file"}:
            raise ValueError("mode must be 'memory' or 'file'")
        if mode == "file" and not db_path:
            raise ValueError("db_path required for file mode")

        if mode == "memory":
            self.con = duckdb.connect(database=":memory:")
            self.db_path = None
        else:
            self.con = duckdb.connect(database=db_path)
            self.db_path = db_path
        self.mode = mode

    def ensure_connected(self) -> duckdb.DuckDBPyConnection:
        if not self.con:
            raise RuntimeError("DuckDB is not connected")
        return self.con

    def load_file(self, path: str, table_name: str | None = None) -> str:
        con = self.ensure_connected()
        suffix = Path(path).suffix.lower()
        if not table_name:
            table_name = Path(path).stem
        table_name = sanitize_table_name(table_name)

        if suffix == ".csv":
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)",
                [path],
            )
        elif suffix == ".parquet":
            con.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
                [path],
            )
        else:
            raise ValueError("Only .csv and .parquet files are supported")
        return table_name

    def list_tables(self) -> list[str]:
        con = self.ensure_connected()
        rows = con.execute("SHOW TABLES").fetchall()
        return [row[0] for row in rows]

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        con = self.ensure_connected()
        rows = con.execute(f"DESCRIBE {sanitize_table_name(table)}").fetchall()
        return [(row[0], row[1]) for row in rows]

    def rename_table(self, old: str, new: str) -> None:
        con = self.ensure_connected()
        con.execute(
            f"ALTER TABLE {sanitize_table_name(old)} RENAME TO {sanitize_table_name(new)}"
        )

    def run_query(self, sql: str, limit: int = MAX_ROWS) -> dict[str, Any]:
        con = self.ensure_connected()
        sql = sql.strip().rstrip(";")
        if not sql:
            raise ValueError("Query is empty")

        kind = first_keyword(sql)
        if kind in {"select", "with"}:
            limited_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit}"
            cur = con.execute(limited_sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return {"columns": cols, "rows": rows, "row_count": len(rows)}

        cur = con.execute(sql)
        if cur.description:
            rows = cur.fetchmany(limit)
            cols = [desc[0] for desc in cur.description]
            return {"columns": cols, "rows": rows, "row_count": len(rows)}

        return {"columns": [], "rows": [], "row_count": 0, "message": "Query executed"}


manager = DuckDBManager()
manager.connect("memory", None)


def sanitize_table_name(name: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name.strip())
    if not clean:
        raise ValueError("Invalid table name")
    if clean[0].isdigit():
        clean = f"t_{clean}"
    return clean


def first_keyword(sql: str) -> str:
    for token in sql.split():
        if token:
            return token.lower()
    return ""




def generate_sample_csv(
    rows: int, string_cols: int, double_cols: int, distribution: str
) -> str:
    if rows <= 0 or rows > 1_000_000:
        raise ValueError("rows must be between 1 and 1,000,000")
    if string_cols < 0 or double_cols < 0:
        raise ValueError("column counts must be >= 0")
    if string_cols + double_cols == 0:
        raise ValueError("at least one column is required")
    if distribution not in {"uniform", "normal"}:
        raise ValueError("distribution must be 'uniform' or 'normal'")

    headers = [f"str_{i + 1}" for i in range(string_cols)]
    headers.extend(f"dbl_{i + 1}" for i in range(double_cols))

    def rand_string() -> str:
        return "".join(random.choices(string.ascii_lowercase, k=8))

    def rand_double() -> float:
        if distribution == "normal":
            return random.gauss(0, 1)
        return random.random()

    temp_dir = Path(tempfile.gettempdir()) / "duckdb_loader"
    temp_dir.mkdir(parents=True, exist_ok=True)
    file_path = temp_dir / f"sample_{rows}_{string_cols}_{double_cols}.csv"
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for _ in range(rows):
            row = [rand_string() for _ in range(string_cols)]
            row.extend(rand_double() for _ in range(double_cols))
            writer.writerow(row)
    return str(file_path)


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/connect", methods=["POST"])
def connect() -> Any:
    payload = request.get_json(force=True)
    mode = payload.get("mode")
    db_path = payload.get("db_path")
    try:
        manager.connect(mode, db_path)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/generate", methods=["POST"])
def generate_csv() -> Any:
    payload = request.get_json(force=True)
    try:
        rows = int(payload.get("rows", 0))
        string_cols = int(payload.get("string_cols", 0))
        double_cols = int(payload.get("double_cols", 0))
        distribution = payload.get("distribution", "uniform")
        path = generate_sample_csv(rows, string_cols, double_cols, distribution)
        return jsonify({"ok": True, "path": path})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/load", methods=["POST"])
def load_file() -> Any:
    payload = request.get_json(force=True)
    path = payload.get("path")
    table_name = payload.get("table_name")
    try:
        loaded = manager.load_file(path, table_name)
        return jsonify({"ok": True, "table": loaded})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/rename", methods=["POST"])
def rename_table() -> Any:
    payload = request.get_json(force=True)
    old = payload.get("old")
    new = payload.get("new")
    try:
        manager.rename_table(old, new)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/query/run", methods=["POST"])
def run_query() -> Any:
    payload = request.get_json(force=True)
    sql = payload.get("sql", "")
    try:
        result = manager.run_query(sql, limit=MAX_ROWS)
        return jsonify({"ok": True, **result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/tables/json")
def tables_json() -> Any:
    try:
        tables = manager.list_tables()
        schemas = {t: manager.get_schema(t) for t in tables}
        return jsonify({"ok": True, "tables": tables, "schemas": schemas})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


if __name__ == "__main__":
    print("WARNING: This app can access any path on this machine. Do not expose it on a shared network.")
    app.run(host=APP_HOST, port=APP_PORT, debug=True)
