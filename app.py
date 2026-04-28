#!/usr/bin/env python3
"""
Modern Madrasa Accounting System – Flask + SQLite
Beautiful, secure, and easy to use
"""
import os
import json
import secrets
import sqlite3
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

from flask import Flask, render_template, request, jsonify, session, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash

# Configuration
APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "jamia_data.db"
STATIC_DIR = APP_DIR / "static"
TEMPLATES_DIR = APP_DIR / "templates"

app = Flask(__name__, static_folder=str(STATIC_DIR), template_folder=str(TEMPLATES_DIR))
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SECURE"] = False  # Set True in production with HTTPS
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=2)

BRAND_NAME = "جامیہ ملیہ اسلامیہ فیصل آباد"
BRAND_NAME_EN = "JAMIA MILLIA ISLAMIA FAISALABAD"

# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_db():
    """Get database connection"""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db

def init_db():
    """Initialize database schema"""
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS accounts (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'General',
            description TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            voucher_no TEXT,
            account_code TEXT NOT NULL,
            description TEXT,
            debit REAL DEFAULT 0,
            credit REAL DEFAULT 0,
            reference TEXT,
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(account_code) REFERENCES accounts(code)
        );
        
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(entry_date);
        CREATE INDEX IF NOT EXISTS idx_entries_account ON entries(account_code);
    """)
    db.commit()
    db.close()

def seed_default_user():
    """Seed default admin user"""
    db = get_db()
    exists = db.execute("SELECT id FROM users WHERE username = 'admin'").fetchone()
    if not exists:
        password_hash = generate_password_hash("admin123")
        db.execute(
            "INSERT INTO users (username, password_hash, display_name, role) VALUES (?, ?, ?, ?)",
            ("admin", password_hash, "Administrator", "admin")
        )
        db.commit()
    db.close()

# ============================================================================
# AUTHENTICATION
# ============================================================================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def before_request():
    session.permanent = True
    app.permanent_session_lifetime = timedelta(hours=2)

# ============================================================================
# API ROUTES
# ============================================================================

@app.route("/api/login", methods=["POST"])
def login():
    """User login"""
    data = request.get_json()
    username = data.get("username", "").strip()
    password = data.get("password", "")
    
    db = get_db()
    user = db.execute(
        "SELECT id, username, display_name, role FROM users WHERE username = ? AND is_active = 1",
        (username,)
    ).fetchone()
    db.close()
    
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "نام یا پاس ورڈ غلط ہے"}), 401
    
    session["user_id"] = user["id"]
    session["username"] = user["username"]
    session["display_name"] = user["display_name"]
    session["role"] = user["role"]
    
    return jsonify({
        "success": True,
        "user": {
            "username": user["username"],
            "display_name": user["display_name"],
            "role": user["role"]
        }
    })

@app.route("/api/logout", methods=["POST"])
def logout():
    """User logout"""
    session.clear()
    return jsonify({"success": True})

@app.route("/api/me")
@login_required
def get_current_user():
    """Get current user info"""
    return jsonify({
        "username": session.get("username"),
        "display_name": session.get("display_name"),
        "role": session.get("role")
    })

# ============================================================================
# ACCOUNTS API
# ============================================================================

@app.route("/api/accounts", methods=["GET"])
@login_required
def get_accounts():
    """Get all accounts"""
    db = get_db()
    accounts = db.execute(
        "SELECT * FROM accounts WHERE is_active = 1 ORDER BY code"
    ).fetchall()
    db.close()
    return jsonify([dict(a) for a in accounts])

@app.route("/api/accounts", methods=["POST"])
@login_required
def create_account():
    """Create new account"""
    if session.get("role") != "admin":
        return jsonify({"error": "صرف ایڈمن"}), 403
    
    data = request.get_json()
    db = get_db()
    try:
        db.execute(
            "INSERT INTO accounts (code, name, type, description) VALUES (?, ?, ?, ?)",
            (data.get("code"), data.get("name"), data.get("type", "General"), data.get("description", ""))
        )
        db.commit()
        db.close()
        return jsonify({"success": True})
    except sqlite3.IntegrityError:
        db.close()
        return jsonify({"error": "یہ کوڈ پہلے سے موجود ہے"}), 400

@app.route("/api/accounts/<code>", methods=["PUT"])
@login_required
def update_account(code):
    """Update account"""
    if session.get("role") != "admin":
        return jsonify({"error": "صرف ایڈمن"}), 403
    
    data = request.get_json()
    db = get_db()
    db.execute(
        "UPDATE accounts SET name = ?, type = ?, description = ?, updated_at = CURRENT_TIMESTAMP WHERE code = ?",
        (data.get("name"), data.get("type"), data.get("description"), code)
    )
    db.commit()
    db.close()
    return jsonify({"success": True})

# ============================================================================
# ENTRIES API
# ============================================================================

@app.route("/api/entries", methods=["GET"])
@login_required
def get_entries():
    """Get entries with filters"""
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    account_code = request.args.get("account_code")
    limit = int(request.args.get("limit", 100))
    
    db = get_db()
    query = "SELECT * FROM entries WHERE 1=1"
    params = []
    
    if from_date:
        query += " AND entry_date >= ?"
        params.append(from_date)
    if to_date:
        query += " AND entry_date <= ?"
        params.append(to_date)
    if account_code:
        query += " AND account_code = ?"
        params.append(account_code)
    
    query += " ORDER BY entry_date DESC LIMIT ?"
    params.append(limit)
    
    entries = db.execute(query, params).fetchall()
    db.close()
    return jsonify([dict(e) for e in entries])

@app.route("/api/entries", methods=["POST"])
@login_required
def create_entry():
    """Create new entry"""
    data = request.get_json()
    db = get_db()
    
    try:
        db.execute(
            """INSERT INTO entries (entry_date, voucher_no, account_code, description, 
                                    debit, credit, reference, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data.get("entry_date", datetime.now().strftime("%Y-%m-%d")),
                data.get("voucher_no", ""),
                data.get("account_code"),
                data.get("description", ""),
                float(data.get("debit", 0)),
                float(data.get("credit", 0)),
                data.get("reference", ""),
                session.get("username")
            )
        )
        db.commit()
        db.close()
        return jsonify({"success": True})
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 400

@app.route("/api/entries/<int:id>", methods=["DELETE"])
@login_required
def delete_entry(id):
    """Delete entry"""
    if session.get("role") != "admin":
        return jsonify({"error": "صرف ایڈمن"}), 403
    
    db = get_db()
    db.execute("DELETE FROM entries WHERE id = ?", (id,))
    db.commit()
    db.close()
    return jsonify({"success": True})

# ============================================================================
# REPORTS API
# ============================================================================

@app.route("/api/reports/trial-balance")
@login_required
def trial_balance():
    """Generate trial balance"""
    db = get_db()
    result = db.execute("""
        SELECT account_code, 
               SUM(debit) as total_debit,
               SUM(credit) as total_credit,
               (SUM(debit) - SUM(credit)) as balance
        FROM entries
        GROUP BY account_code
        ORDER BY account_code
    """).fetchall()
    db.close()
    
    return jsonify([
        {
            "account_code": r["account_code"],
            "total_debit": r["total_debit"] or 0,
            "total_credit": r["total_credit"] or 0,
            "balance": r["balance"] or 0
        }
        for r in result
    ])

@app.route("/api/reports/ledger/<code>")
@login_required
def ledger_report(code):
    """Get ledger for specific account"""
    db = get_db()
    entries = db.execute(
        "SELECT * FROM entries WHERE account_code = ? ORDER BY entry_date",
        (code,)
    ).fetchall()
    db.close()
    
    return jsonify([dict(e) for e in entries])

# ============================================================================
# HTML PAGES
# ============================================================================

@app.route("/")
def index():
    """Main page"""
    return render_template("index.html", brand_name=BRAND_NAME, brand_name_en=BRAND_NAME_EN)

@app.route("/dashboard")
def dashboard():
    """Dashboard page"""
    return render_template("dashboard.html")

@app.route("/entries")
def entries_page():
    """Entries management page"""
    return render_template("entries.html")

@app.route("/accounts")
def accounts_page():
    """Accounts management page"""
    return render_template("accounts.html")

@app.route("/reports")
def reports_page():
    """Reports page"""
    return render_template("reports.html")

# ============================================================================
# STATIC FILES
# ============================================================================

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory(STATIC_DIR, filename)

# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "صفحہ نہیں ملا"}), 404

@app.errorhandler(500)
def server_error(error):
    return jsonify({"error": "سرور کی خرابی"}), 500

# ============================================================================
# INITIALIZATION & RUN
# ============================================================================

if __name__ == "__main__":
    STATIC_DIR.mkdir(exist_ok=True)
    TEMPLATES_DIR.mkdir(exist_ok=True)
    
    init_db()
    seed_default_user()
    
    print(f"🏫 {BRAND_NAME}")
    print("جدید حساب کتاب کا نظام")
    print("=" * 50)
    print("http://127.0.0.1:5000 پر چل رہا ہے")
    print("=" * 50)
    
    app.run(debug=True, host="127.0.0.1", port=5000)
