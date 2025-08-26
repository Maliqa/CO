import os
import uuid
import hashlib
import sqlite3
import base64
import html
import mimetypes
import json
from datetime import datetime, date, time, timedelta
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Opsional: kalender hari libur nasional
try:
    import holidays as pyholidays
except ImportError:
    pyholidays = None

# INISIALISASI SESSION STATE DI LEVEL ATAS
if "initialized" not in st.session_state:
    st.session_state.update({
        "initialized": True,
        "authenticated": False,
        "user": None
    })

# -------------------- Konfigurasi --------------------
DB_PATH = os.environ.get("HRMS_DB_PATH", "hrms.db")
UPLOAD_DIR = os.environ.get("HRMS_UPLOAD_DIR", "uploads")
BASE64_SIZE_WARN_BYTES = int(os.environ.get("HRMS_BASE64_WARN_BYTES", 5 * 1024 * 1024))  # 5 MB
TEXT_PREVIEW_MAX_BYTES = int(os.environ.get("HRMS_TEXT_PREVIEW_MAX_BYTES", 200 * 1024))  # 200 KB

# -------------------- DB Helpers --------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
    except Exception:
        pass
    return conn

def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_def: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def};")
        conn.commit()

def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()
    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('EMPLOYEE','MANAGER','HR_ADMIN')),
        manager_id INTEGER,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        division TEXT,
        FOREIGN KEY(manager_id) REFERENCES users(id)
    );
    """)
    # quotas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quotas(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        leave_total INTEGER NOT NULL DEFAULT 12,
        leave_used INTEGER NOT NULL DEFAULT 0,
        changeoff_earned INTEGER NOT NULL DEFAULT 0,
        changeoff_used INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(user_id, year),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)
    # requests
    cur.execute("""
    CREATE TABLE IF NOT EXISTS requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('LEAVE','CHANGEOFF')),
        start_date TEXT,
        end_date TEXT,
        single_date TEXT,
        hours INTEGER,
        reason TEXT,
        status TEXT NOT NULL CHECK(status IN ('PENDING_MANAGER','PENDING_HR','APPROVED','REJECTED')),
        manager_by INTEGER,
        manager_at TEXT,
        hr_by INTEGER,
        hr_at TEXT,
        timesheet_path TEXT,
        location TEXT,
        activity TEXT,
        pic TEXT,
        job_execution TEXT,
        payload_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        file_uploaded BOOLEAN DEFAULT 0,
        activity_start_time TEXT,
        activity_end_time TEXT,
        departure_date TEXT,
        return_date TEXT,
        activities_json TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(manager_by) REFERENCES users(id),
        FOREIGN KEY(hr_by) REFERENCES users(id)
    );
    """)
    conn.commit()
    add_column_if_missing(conn, "users", "division", "TEXT")
    add_column_if_missing(conn, "requests", "activities_json", "TEXT")
    add_column_if_missing(conn, "requests", "file_uploaded", "BOOLEAN DEFAULT 0")
    add_column_if_missing(conn, "requests", "activity_start_time", "TEXT")
    add_column_if_missing(conn, "requests", "activity_end_time", "TEXT")
    add_column_if_missing(conn, "requests", "departure_date", "TEXT")
    add_column_if_missing(conn, "requests", "return_date", "TEXT")
    # Seed default users jika kosong
    cur.execute("SELECT COUNT(1) AS c FROM users;")
    if cur.fetchone()["c"] == 0:
        now = datetime.utcnow().isoformat()
        def hpw(p): return hashlib.sha256(p.encode()).hexdigest()
        cur.execute("""INSERT INTO users(email,name,role,manager_id,password_hash,created_at,updated_at,division)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    ("manager@example.com", "Manager One", "MANAGER", None, hpw("password"), now, now, "Engineering"))
        manager_id = cur.lastrowid
        cur.execute("""INSERT INTO users(email,name,role,manager_id,password_hash,created_at,updated_at,division)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    ("employee@example.com", "Employee One", "EMPLOYEE", manager_id, hpw("password"), now, now, "Engineering"))
        cur.execute("""INSERT INTO users(email,name,role,manager_id,password_hash,created_at,updated_at,division)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    ("hr@example.com", "HR Admin", "HR_ADMIN", None, hpw("password"), now, now, "Human Resources"))
        conn.commit()
        cur.execute("SELECT id FROM users WHERE email=?", ("employee@example.com",))
        emp_id = cur.fetchone()["id"]
        year = datetime.utcnow().year
        cur.execute("""INSERT OR IGNORE INTO quotas(user_id,year,leave_total,leave_used,changeoff_earned,changeoff_used,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (emp_id, year, 12, 0, 0, 0, now, now))
        conn.commit()
    conn.close()

# -------------------- Auth --------------------
def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def login(email: str, password: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email=?", (email,))
    row = cur.fetchone()
    conn.close()
    if not row: return None
    if row["password_hash"] != hash_pw(password): return None
    return row

def current_year() -> int:
    return date.today().year

# -------------------- Helpers User/Manager --------------------
def get_manager_for_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT m.*
        FROM users u
        LEFT JOIN users m ON m.id = u.manager_id
        WHERE u.id = ?
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def require_manager_assigned(user: dict) -> bool:
    mgr = get_manager_for_user(int(user["id"]))
    if not mgr:
        st.error("Akun Anda belum memiliki Manager yang ditetapkan. Hubungi HR untuk mengatur Manager terlebih dahlu.")
        return False
    if mgr["role"] != "MANAGER":
        st.error(f"Manager yang ditetapkan adalah {mgr['name']} ({mgr['email']}) tetapi rolenya {mgr['role']}. HR perlu memperbaiki.")
        return False
    return True

# -------------------- Business Logic --------------------
def get_or_create_quota(user_id: int, year: int) -> sqlite3.Row:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM quotas WHERE user_id=? AND year=?", (user_id, year))
    q = cur.fetchone()
    if not q:
        now = datetime.utcnow().isoformat()
        cur.execute("""INSERT INTO quotas(user_id,year,leave_total,leave_used,changeoff_earned,changeoff_used,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (user_id, year, 12, 0, 0, 0, now, now))
        conn.commit()
        cur.execute("SELECT * FROM quotas WHERE user_id=? AND year=?", (user_id, year))
        q = cur.fetchone()
    conn.close()
    return q

def save_file(uploaded_file) -> str:
    ext = os.path.splitext(uploaded_file.name)[1]
    fname = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, fname)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path

def inclusive_days(d1: date, d2: date) -> int:
    return (d2 - d1).days + 1

def submit_changeoff(user_id: int, departure_date: date, return_date: date, 
                    activity_start_time: str, activity_end_time: str, 
                    location: str, activity: str, pic: str, job_exec: Optional[str], timesheet_path: str):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    start_time_obj = datetime.strptime(activity_start_time, '%H:%M').time()
    end_time_obj = datetime.strptime(activity_end_time, '%H:%M').time()
    start_dt = datetime.combine(date.today(), start_time_obj)
    end_dt = datetime.combine(date.today(), end_time_obj)
    if end_dt < start_dt:
        end_dt = datetime.combine(date.today() + timedelta(days=1), end_time_obj)
    hours_diff = (end_dt - start_dt).total_seconds() / 3600
    hours = int(hours_diff)
    cur.execute("""
        INSERT INTO requests(user_id,type,departure_date,return_date,activity_start_time,activity_end_time,
                            hours,reason,status,timesheet_path,location,activity,pic,job_execution,
                            created_at,updated_at,file_uploaded)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (user_id, 'CHANGEOFF', departure_date.isoformat(), return_date.isoformat(), 
          activity_start_time, activity_end_time, hours, 'CHANGEOFF', 'PENDING_MANAGER', 
          timesheet_path, location, activity, pic, job_exec, now, now, 1))
    conn.commit()
    conn.close()

def submit_leave(user_id: int, start: date, end: date, reason: str) -> Tuple[bool, str]:
    days = inclusive_days(start, end)
    year = start.year
    q = get_or_create_quota(user_id, year)
    leave_balance = q["leave_total"] - q["leave_used"]
    co_balance = q["changeoff_earned"] - q["changeoff_used"]
    if reason == 'CHANGEOFF':
        if co_balance < days:
            return False, f"Saldo Change Off tidak cukup. Tersedia {co_balance} hari, diminta {days}."
    elif reason == 'PERSONAL':
        if leave_balance < days:
            return False, f"Saldo cuti tidak cukup. Tersedia {leave_balance} hari, diminta {days}."
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO requests(user_id,type,start_date,end_date,reason,status,created_at,updated_at,file_uploaded)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, (user_id, 'LEAVE', start.isoformat(), end.isoformat(), reason, 'PENDING_MANAGER', now, now, 0))
    conn.commit()
    conn.close()
    return True, "Leave request terkirim dan menunggu persetujuan Manager."

def manager_pending(manager_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT r.*, u.name as employee_name, u.email as employee_email, u.division as employee_division
        FROM requests r
        JOIN users u ON u.id = r.user_id
        WHERE r.status='PENDING_MANAGER' AND u.manager_id = ?
        ORDER BY r.created_at DESC
    """, conn, params=(manager_id,))
    conn.close()
    return df

def hr_pending() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT r.*, u.name as employee_name, u.email as employee_email, u.division as employee_division
        FROM requests r
        JOIN users u ON u.id = r.user_id
        WHERE r.status='PENDING_HR'
        ORDER BY r.created_at DESC
    """, conn)
    conn.close()
    return df

def set_manager_decision(manager_id: int, request_id: int, approve: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT r.*, u.manager_id
                   FROM requests r JOIN users u ON u.id=r.user_id
                   WHERE r.id=?""", (request_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("Request tidak ditemukan")
    if row["manager_id"] != manager_id:
        conn.close()
        raise PermissionError("Anda bukan manager dari karyawan ini.")
    new_status = 'PENDING_HR' if approve else 'REJECTED'
    now = datetime.utcnow().isoformat()
    cur.execute("UPDATE requests SET status=?, manager_by=?, manager_at=?, updated_at=? WHERE id=?",
                (new_status, manager_id, now, now, request_id))
    conn.commit()
    conn.close()

def adjust_quota_leave(user_id: int, year: int, days: int):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("INSERT OR IGNORE INTO quotas(user_id,year,created_at,updated_at) VALUES(?,?,?,?)",
                (user_id, year, now, now))
    cur.execute("UPDATE quotas SET leave_used = leave_used + ?, updated_at=? WHERE user_id=? AND year=?",
                (days, now, user_id, year))
    conn.commit()
    conn.close()

def adjust_quota_changeoff_earned(user_id: int, year: int, days: int):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("INSERT OR IGNORE INTO quotas(user_id,year,created_at,updated_at) VALUES(?,?,?,?)",
                (user_id, year, now, now))
    cur.execute("UPDATE quotas SET changeoff_earned = changeoff_earned + ?, updated_at=? WHERE user_id=? AND year=?",
                (days, now, user_id, year))
    conn.commit()
    conn.close()

def adjust_quota_changeoff_used(user_id: int, year: int, days: int):
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("INSERT OR IGNORE INTO quotas(user_id,year,created_at,updated_at) VALUES(?,?,?,?)",
                (user_id, year, now, now))
    cur.execute("UPDATE quotas SET changeoff_used = changeoff_used + ?, updated_at=? WHERE user_id=? AND year=?",
                (days, now, user_id, year))
    conn.commit()
    conn.close()

def set_hr_decision(hr_id: int, request_id: int, approve: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE id=?", (request_id,))
    req = cur.fetchone()
    if not req:
        conn.close()
        raise ValueError("Request tidak ditemukan")
    if req["status"] != 'PENDING_HR':
        conn.close()
        raise ValueError("Request tidak menunggu HR")
    new_status = 'APPROVED' if approve else 'REJECTED'
    now = datetime.utcnow().isoformat()
    cur.execute("UPDATE requests SET status=?, hr_by=?, hr_at=?, updated_at=? WHERE id=?", (new_status, hr_id, now, now, request_id))
    conn.commit()
    conn.close()
    if approve:
        if req["type"] == 'LEAVE':
            s = date.fromisoformat(req["start_date"])
            e = date.fromisoformat(req["end_date"])
            days = inclusive_days(s, e)
            if req["reason"] == 'CHANGEOFF':
                adjust_quota_changeoff_used(req["user_id"], s.year, days)
            elif req["reason"] == 'PERSONAL':
                adjust_quota_leave(req["user_id"], s.year, days)
        elif req["type"] == 'CHANGEOFF':
            d = date.fromisoformat(req["departure_date"])
            hours = req["hours"] or 0
            credit = max(0, hours // 8)
            if credit > 0:
                adjust_quota_changeoff_earned(req["user_id"], d.year, credit)

# -------------------- Admin CRUD --------------------
def list_users() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT u.id, u.email, u.name, u.role, u.manager_id, u.division, m.name as manager_name, u.created_at
        FROM users u LEFT JOIN users m ON m.id = u.manager_id
        ORDER BY u.created_at DESC
    """, conn)
    conn.close()
    return df

def list_managers() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT id, name, email FROM users WHERE role='MANAGER' ORDER by name", conn)
    conn.close()
    return df

def create_user(email: str, name: str, role: str, password: str, manager_id: Optional[int], division: Optional[str]):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""INSERT INTO users(email,name,role,manager_id,password_hash,created_at,updated_at,division)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (email, name, role, manager_id, hash_pw(password), now, now, division))
    conn.commit()
    conn.close()

def update_user(user_id: int, email: str, name: str, role: str, manager_id: Optional[int], new_password: Optional[str], division: Optional[str]):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    if new_password:
        cur.execute("""UPDATE users SET email=?, name=?, role=?, manager_id=?, password_hash=?, division=?, updated_at=?
                       WHERE id=?""",
                    (email, name, role, manager_id, hash_pw(new_password), division, now, user_id))
    else:
        cur.execute("""UPDATE users SET email=?, name=?, role=?, manager_id=?, division=?, updated_at=?
                       WHERE id=?""",
                    (email, name, role, manager_id, division, now, user_id))
    conn.commit()
    conn.close()

def delete_user(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM requests WHERE user_id=? LIMIT 1", (user_id,))
    if cur.fetchone():
        conn.close()
        raise ValueError("Tidak bisa hapus user: masih ada request sebagai pemilik. Hapus/arsipkan dulu request-nya.")
    cur.execute("SELECT 1 FROM quotas WHERE user_id=? LIMIT 1", (user_id,))
    if cur.fetchone():
        conn.close()
        raise ValueError("Tidak bisa hapus user: masih ada kuota terkait. Hapus kuotanya dulu.")
    cur.execute("UPDATE users SET manager_id=NULL WHERE manager_id=?", (user_id,))
    cur.execute("UPDATE requests SET manager_by=NULL WHERE manager_by=?", (user_id,))
    cur.execute("UPDATE requests SET hr_by=NULL WHERE hr_by=?", (user_id,))
    cur.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()

def upsert_quota(user_id: int, year: int, leave_total: int, changeoff_earned: int, changeoff_used: int, leave_used: int):
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM quotas WHERE user_id=? AND year=?", (user_id, year))
    row = cur.fetchone()
    if row:
        cur.execute("""UPDATE quotas SET leave_total=?, leave_used=?, changeoff_earned=?, changeoff_used=?, updated_at=?
                       WHERE user_id=? AND year=?""",
                    (leave_total, leave_used, changeoff_earned, changeoff_used, now, user_id, year))
    else:
        cur.execute("""INSERT INTO quotas(user_id,year,leave_total,leave_used,changeoff_earned,changeoff_used,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (user_id, year, leave_total, leave_used, changeoff_earned, changeoff_used, now, now))
    conn.commit()
    conn.close()

def delete_quota(user_id: int, year: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM quotas WHERE user_id=? AND year=?", (user_id, year))
    conn.commit()
    conn.close()

def get_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row

def my_requests(user_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM requests WHERE user_id=? ORDER BY created_at DESC", conn, params=(user_id,))
    conn.close()
    return df

def user_quota(user_id: int, year: int) -> dict:
    q = get_or_create_quota(user_id, year)
    return {
        "year": year,
        "leave_total": int(q["leave_total"]),
        "leave_used": int(q["leave_used"]),
        "leave_balance": int(q["leave_total"] - q["leave_used"]),
        "co_earned": int(q["changeoff_earned"]),
        "co_used": int(q["changeoff_used"]),
        "co_balance": int(q["changeoff_earned"] - q["changeoff_used"]),
    }

# -------------------- File Preview (iframe + base64, PDF only) --------------------
def human_size(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"

def _open_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def preview_pdf_iframe(file_path, width="100%", height=900):
    try:
        with open(file_path, "rb") as f:
            base64_pdf = base64.b64encode(f.read()).decode('utf-8')
        pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="{width}" height="{height}" type="application/pdf" style="border: none;"></iframe>'
        st.markdown(pdf_display, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Gagal menampilkan PDF: {e}")

def preview_file(path: str, label_prefix: str = "Attachment", key_prefix: Optional[str] = None, user_role: str = "EMPLOYEE"):
    if not os.path.exists(path):
        st.error("File tidak ditemukan di server.")
        return
    if key_prefix is None:
        key_prefix = os.path.basename(path)
    size = os.path.getsize(path)
    mime, _ = mimetypes.guess_type(path)
    ext = (os.path.splitext(path)[1] or "").lower()
    st.write(f"{label_prefix}: {os.path.basename(path)} • {human_size(size)} • {mime or 'application/octet-stream'}")
    with open(path, "rb") as f:
        st.download_button("Download File", f, file_name=os.path.basename(path), mime=mime or "application/octet-stream", key=f"dl_{key_prefix}")
    if user_role not in ["MANAGER", "HR_ADMIN"]:
        st.info("Hanya Manager dan HR yang dapat melihat preview file.")
        return
    if ext == ".pdf" or (mime == "application/pdf"):
        preview_pdf_iframe(path, width="100%", height=900)
    else:
        st.warning("Preview hanya tersedia untuk file PDF. Tipe lain hanya dapat diunduh.")

# -------------------- UI --------------------
def page_login():
    st.title("HRMS - Login")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        user = login(email, password)
        if user:
            st.session_state.user = dict(user)
            st.session_state.authenticated = True
            st.success(f"Login sukses. Halo, {user['name']}!")
            st.rerun()
        else:
            st.error("Email atau password salah.")

def sidebar_menu():
    user = st.session_state.user
    if not user: 
        return None
    with st.sidebar:
        st.write(f"Logged in as: {user['name']} ({user['role']})")
        if user.get("division"):
            st.caption(f"Division: {user['division']}")
        if user["role"] == "EMPLOYEE":
            mgr = get_manager_for_user(int(user["id"]))
            if mgr:
                st.caption(f"Manager: {mgr['name']} ({mgr['email']})")
                if mgr["role"] != "MANAGER":
                    st.warning(f"Perhatian: role manager Anda adalah {mgr['role']}, seharusnya 'MANAGER'. Minta HR memperbaiki.")
            else:
                st.warning("Manager belum ditetapkan. Pengajuan tidak akan masuk ke akun Manager mana pun.")
        choice = None
        if user["role"] == "EMPLOYEE":
            choice = st.radio("Menu", ["Dashboard", "Submit Leave", "Submit Change Off", "My Requests"])
        elif user["role"] == "MANAGER":
            choice = st.radio("Menu", ["Dashboard", "Submit Leave", "Submit Change Off", "Pending (Manager)", "Team Requests"])
        elif user["role"] == "HR_ADMIN":
            choice = st.radio("Menu", ["Pending (HR)", "Quotas", "Users"])
        if st.button("Logout"):
            st.session_state.clear()
            st.rerun()
        return choice

def quota_kanban(q: dict, title_prefix: str = ""):
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.expander(f"{title_prefix} Leave • {q['leave_balance']}/{q['leave_total']} sisa", expanded=True):
            st.metric("Leave Balance", q["leave_balance"], delta=-q["leave_used"])
            st.write(f"Total: {q['leave_total']} | Used: {q['leave_used']}")
    with c2:
        with st.expander(f"{title_prefix} ChangeOff • {q['co_balance']} saldo", expanded=True):
            st.metric("CO Balance", q["co_balance"], delta=q["co_earned"] - q["co_used"])
            st.write(f"Earned: {q['co_earned']} | Used: {q['co_used']}")
    with c3:
        with st.expander(f"Tahun {q['year']}", expanded=True):
            st.write("- Leave dipotong saat HR approve")
            st.write("- ChangeOff bertambah (jam/8) saat HR approve")

def page_employee_dashboard(user):
    st.header("Dashboard")
    st.caption(f"Division: {user.get('division') or '-'}")
    year = st.number_input("Tahun", min_value=2000, max_value=2100, value=current_year(), step=1)
    q = user_quota(user["id"], year)
    quota_kanban(q)
    st.subheader("Kalender Libur Nasional (Indonesia)")
    if pyholidays:
        id_holidays = pyholidays.country_holidays("ID", years=[year])
        data = [{"date": d, "name": name} for d, name in sorted(id_holidays.items())]
        st.dataframe(pd.DataFrame(data), use_container_width=True)
    else:
        st.info("Package 'holidays' belum terinstall. Jalankan: pip install holidays")

def page_submit_leave(user):
    st.header("Submit Leave (ke Manager dulu)")
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("Tanggal Mulai", date.today())
    with col2:
        end = st.date_input("Tanggal Akhir", date.today())
    reason = st.selectbox("Alasan", ["PERSONAL", "SAKIT", "CHANGEOFF"])
    if st.button("Kirim Leave"):
        if not require_manager_assigned(user): return
        if end < start:
            st.error("Tanggal akhir harus >= tanggal mulai")
        else:
            ok, msg = submit_leave(user["id"], start, end, reason)
            st.success(msg) if ok else st.error(msg)

def page_submit_changeoff(user):
    st.header("Submit Change Off (ke Manager dulu)")
    col1, col2 = st.columns(2)
    with col1:
        departure_date = st.date_input("Tanggal Keberangkatan", date.today())
    with col2:
        return_date = st.date_input("Tanggal Kepulangan", date.today())
    total_days = (return_date - departure_date).days + 1
    if total_days <= 0:
        st.error("Tanggal kepulangan harus setelah tanggal keberangkatan.")
        return
    st.success(f"✅ Total hari aktivitas: {total_days} hari")
    location = st.text_input("Lokasi")
    pic = st.text_input("PIC")
    job_exec = st.text_input("Job Eksekusi (opsional)")
    st.subheader("Detail Aktivitas per Hari")
    activities_data = []
    for day in range(total_days):
        current_date = departure_date + timedelta(days=day)
        st.markdown(f"### Hari {day + 1} - {current_date.strftime('%A, %d %B %Y')}")
        col3, col4 = st.columns(2)
        with col3:
            start_time = st.text_input(f"Waktu Mulai (HH:MM) - Hari {day+1}", 
                                     value="08:00", key=f"start_{day}")
        with col4:
            end_time = st.text_input(f"Waktu Selesai (HH:MM) - Hari {day+1}", 
                                   value="17:00", key=f"end_{day}")
        activity_desc = st.text_area(f"Detail Aktivitas - Hari {day+1}", 
                                   placeholder="Deskripsikan aktivitas yang dilakukan", 
                                   key=f"activity_{day}")
        activities_data.append({
            "hari": day + 1,
            "tanggal": current_date.isoformat(),
            "waktu_mulai": start_time,
            "waktu_selesai": end_time,
            "aktivitas": activity_desc
        })
    if activities_data:
        st.subheader("Preview Aktivitas")
        preview_df = pd.DataFrame(activities_data)
        preview_df['tanggal'] = pd.to_datetime(preview_df['tanggal']).dt.strftime('%A, %Y-%m-%d')
        preview_df['hari'] = preview_df.index + 1
        st.dataframe(preview_df[['hari', 'tanggal', 'waktu_mulai', 'waktu_selesai', 'aktivitas']], use_container_width=True)
    file = st.file_uploader("Upload Timesheet (wajib)", type=None)
    if file:
        st.success("✅ File telah diupload")
    if st.button("Kirim Change Off"):
        if not require_manager_assigned(user): 
            return
        if not file:
            st.error("Timesheet wajib diupload.")
        elif not location or not pic:
            st.error("Harap isi Lokasi dan PIC.")
        elif departure_date > return_date:
            st.error("Tanggal kepulangan harus setelah tanggal keberangkatan.")
        else:
            valid = True
            for activity in activities_data:
                try:
                    datetime.strptime(activity['waktu_mulai'], '%H:%M')
                    datetime.strptime(activity['waktu_selesai'], '%H:%M')
                except ValueError:
                    st.error(f"Format waktu tidak valid untuk Hari {activity['hari']}. Harus HH:MM")
                    valid = False
                    break
            if not valid:
                return
            path = save_file(file)
            activities_json = json.dumps(activities_data, ensure_ascii=False)
            total_hours = 0
            for activity in activities_data:
                start_dt = datetime.strptime(activity['waktu_mulai'], '%H:%M')
                end_dt = datetime.strptime(activity['waktu_selesai'], '%H:%M')
                if end_dt < start_dt:
                    end_dt = end_dt.replace(day=end_dt.day + 1)
                hours_diff = (end_dt - start_dt).total_seconds() / 3600
                total_hours += hours_diff
            now = datetime.utcnow().isoformat()
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO requests(user_id,type,departure_date,return_date,
                            hours,reason,status,timesheet_path,location,pic,job_execution,
                            activities_json,created_at,updated_at,file_uploaded)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (user["id"], 'CHANGEOFF', departure_date.isoformat(), return_date.isoformat(), 
                  total_hours, 'CHANGEOFF', 'PENDING_MANAGER', path, location, pic, 
                  job_exec if job_exec else None, activities_json, now, now, 1))
            conn.commit()
            conn.close()
            st.success("Change Off request terkirim. Menunggu persetujuan Manager.")
            st.balloons()

def page_my_requests(user):
    st.header("My Requests")
    df = my_requests(user["id"])
    if df.empty:
        st.info("Belum ada request.")
        return
    for _, r in df.iterrows():
        status_text = f"ID: {r['id']} | {r['type']} | {r['status']}"
        if r.get('file_uploaded', 0):
            status_text += " ✅"
        with st.expander(status_text):
            st.dataframe(pd.DataFrame([r]).drop(columns=['user_id']), use_container_width=True)

def page_manager_pending(user):
    st.header("Pending Approval (Manager)")
    df = manager_pending(user["id"])
    if df.empty:
        st.info("Tidak ada request menunggu Manager.")
        return
    for _, r in df.iterrows():
        status_text = f"[{r['type']}] {r['employee_name']} • Div {r.get('employee_division','-')} • Status: {r['status']} • ID: {r['id']}"
        if r.get('file_uploaded', 0):
            status_text += " ✅"
        with st.expander(status_text):
            json_data = None
            if r.get('activities_json') and r['activities_json'] != 'null':
                try:
                    json_data = json.loads(r['activities_json'])
                except:
                    pass
            if not json_data and r.get('payload_json') and r['payload_json'] != 'null':
                try:
                    json_data = json.loads(r['payload_json'])
                except:
                    pass
            if json_data:
                try:
                    activities_df = pd.DataFrame(json_data)
                    st.subheader("Detail Aktivitas")
                    activities_df['hari'] = activities_df.index + 1
                    if 'tanggal' in activities_df.columns:
                        activities_df['tanggal_dt'] = pd.to_datetime(activities_df['tanggal'])
                        day_mapping = {
                            'Monday': 'Senin',
                            'Tuesday': 'Selasa', 
                            'Wednesday': 'Rabu',
                            'Thursday': 'Kamis',
                            'Friday': 'Jumat',
                            'Saturday': 'Sabtu',
                            'Sunday': 'Minggu'
                        }
                        activities_df['hari_nama'] = activities_df['tanggal_dt'].dt.strftime('%A').map(day_mapping)
                        activities_df['tanggal'] = activities_df['hari_nama'] + ', ' + activities_df['tanggal_dt'].dt.strftime('%Y-%m-%d')
                        activities_df = activities_df.drop(['tanggal_dt', 'hari_nama'], axis=1)
                    columns_to_show = ['hari']
                    if 'tanggal' in activities_df.columns:
                        columns_to_show.append('tanggal')
                    if 'waktu_mulai' in activities_df.columns:
                        columns_to_show.append('waktu_mulai')
                    if 'waktu_selesai' in activities_df.columns:
                        columns_to_show.append('waktu_selesai')
                    if 'aktivitas' in activities_df.columns:
                        columns_to_show.append('aktivitas')
                    st.dataframe(activities_df[columns_to_show], 
                                use_container_width=True,
                                hide_index=True)
                except Exception as e:
                    st.error(f"Error menampilkan data: {e}")
            else:
                st.warning("Tidak ada data aktivitas yang dapat ditampilkan")
            if r["timesheet_path"]:
                preview_file(r["timesheet_path"], key_prefix=f"mgr_req_{int(r['id'])}", user_role=user["role"])
            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"Approve (ID {int(r['id'])})", key=f"mgr_appr_{int(r['id'])}"):
                    try:
                        set_manager_decision(int(user["id"]), int(r["id"]), True)
                        st.success("Approved → dikirim ke HR.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            with c2:
                if st.button(f"Reject (ID {int(r['id'])})", key=f"mgr_rej_{int(r['id'])}"):
                    try:
                        set_manager_decision(int(user["id"]), int(r["id"]), False)
                        st.warning("Rejected.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

def page_manager_team(user):
    st.header("Team Requests (All)")
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT r.*, u.name as employee_name, u.division as employee_division
        FROM requests r JOIN users u ON u.id=r.user_id
        WHERE u.manager_id = ?
        ORDER BY r.created_at DESC
    """, conn, params=(user["id"],))
    conn.close()
    if df.empty:
        st.info("Belum ada request dari tim.")
        return
    for _, r in df.iterrows():
        status_text = f"ID: {r['id']} | {r['employee_name']} | {r['type']} | {r['status']}"
        if r.get('file_uploaded', 0):
            status_text += " ✅"
        with st.expander(status_text):
            st.dataframe(pd.DataFrame([r]).drop(columns=['user_id']), use_container_width=True)

def page_hr_pending(user):
    st.header("Pending Approval (HR)")
    df = hr_pending()
    if df.empty:
        st.info("Tidak ada request menunggu HR.")
        return

    for _, r in df.iterrows():
        status_text = f"[{r['type']}] {r['employee_name']} • Div {r.get('employee_division','-')} • Status: {r['status']} • ID: {r['id']}"
        if r.get('file_uploaded', 0):
            status_text += " ✅"
        with st.expander(status_text):
            # Tampilkan info utama
            if r["type"] == "CHANGEOFF":
                st.write(f"Keberangkatan: {r.get('departure_date') or '-'} | Kepulangan: {r.get('return_date') or '-'}")
                st.write(f"Waktu Aktivitas: {r.get('activity_start_time') or '-'} - {r.get('activity_end_time') or '-'} | Jam (perhitungan): {r.get('hours') or 0}")
                st.write(f"Lokasi: {r.get('location') or '-'} | Aktivitas: {r.get('activity') or '-'} | PIC: {r.get('pic') or '-'}")
            else:
                st.write(f"Leave {r['start_date']} s/d {r['end_date']} | Reason: {r['reason']}")

            # --- DETAIL AKTIVITAS DITAMPILKAN SEBELUM PDF ---
            json_data = None
            if r.get('activities_json') and r['activities_json'] != 'null':
                try:
                    json_data = json.loads(r['activities_json'])
                except Exception:
                    pass
            if not json_data and r.get('payload_json') and r['payload_json'] != 'null':
                try:
                    json_data = json.loads(r['payload_json'])
                except Exception:
                    pass
            if json_data:
                try:
                    activities_df = pd.DataFrame(json_data)
                    activities_df['hari'] = activities_df.index + 1
                    if 'tanggal' in activities_df.columns:
                        activities_df['tanggal_dt'] = pd.to_datetime(activities_df['tanggal'])
                        day_mapping = {
                            'Monday': 'Senin',
                            'Tuesday': 'Selasa', 
                            'Wednesday': 'Rabu',
                            'Thursday': 'Kamis',
                            'Friday': 'Jumat',
                            'Saturday': 'Sabtu',
                            'Sunday': 'Minggu'
                        }
                        activities_df['hari_nama'] = activities_df['tanggal_dt'].dt.strftime('%A').map(day_mapping)
                        activities_df['tanggal'] = activities_df['hari_nama'] + ', ' + activities_df['tanggal_dt'].dt.strftime('%Y-%m-%d')
                        activities_df = activities_df.drop(['tanggal_dt', 'hari_nama'], axis=1)
                    columns_to_show = ['hari']
                    if 'tanggal' in activities_df.columns:
                        columns_to_show.append('tanggal')
                    if 'waktu_mulai' in activities_df.columns:
                        columns_to_show.append('waktu_mulai')
                    if 'waktu_selesai' in activities_df.columns:
                        columns_to_show.append('waktu_selesai')
                    if 'aktivitas' in activities_df.columns:
                        columns_to_show.append('aktivitas')
                    st.dataframe(activities_df[columns_to_show], use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Error menampilkan data: {e}")
            else:
                st.warning("Tidak ada data aktivitas yang dapat ditampilkan")

            # --- PDF ATAU FILE PREVIEW SETELAH DETAIL AKTIVITAS ---
            if r["timesheet_path"]:
                preview_file(r["timesheet_path"], key_prefix=f"hr_req_{int(r['id'])}", user_role=user["role"])

            # Tombol Approve/Reject HR
            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"Approve HR (ID {int(r['id'])})", key=f"hr_appr_{int(r['id'])}"):
                    try:
                        set_hr_decision(int(user["id"]), int(r["id"]), True)
                        st.success("Approved final.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
            with c2:
                if st.button(f"Reject HR (ID {int(r['id'])})", key=f"hr_rej_{int(r['id'])}"):
                    try:
                        set_hr_decision(int(user["id"]), int(r["id"]), False)
                        st.warning("Rejected.")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
def page_hr_quotas(user):
    st.header("Quotas Management (Kanban)")
    users_df = list_users()
    if users_df.empty:
        st.info("Belum ada user.")
        return
    display = users_df.apply(lambda r: f"{r['name']} ({r['email']}) [{r['role']}] • {r.get('division','-')}", axis=1).tolist()
    idx = st.selectbox("Pilih User", options=list(range(len(display))), format_func=lambda i: display[i])
    user_id = int(users_df.iloc[int(idx)]["id"])
    year = st.number_input("Tahun", min_value=2000, max_value=2100, value=current_year(), step=1)
    q = user_quota(user_id, year)
    quota_kanban(q)
    col1, col2, col3 = st.columns(3)
    with col1:
        leave_total = st.number_input("Leave Total", min_value=0, value=int(q["leave_total"]), step=1, key="qt_leave_total")
        leave_used = st.number_input("Leave Used", min_value=0, value=int(q["leave_used"]), step=1, key="qt_leave_used")
    with col2:
        co_earned = st.number_input("ChangeOff Earned", min_value=0, value=int(q["co_earned"]), step=1, key="qt_co_earned")
    with col3:
        co_used = st.number_input("ChangeOff Used", min_value=0, value=int(q["co_used"]), step=1, key="qt_co_used")
    a, b = st.columns(2)
    with a:
        if st.button("Simpan Kuota"):
            upsert_quota(user_id, year, int(leave_total), int(co_earned), int(co_used), int(leave_used))
            st.success("Kuota tersimpan.")
            st.rerun()
    with b:
        if st.button("Hapus Kuota Tahun Ini"):
            delete_quota(user_id, year)
            st.warning("Kuota tahun ini dihapus.")
            st.rerun()

def page_hr_users(user):
    st.header("Users Management")
    st.subheader("Daftar User")
    st.dataframe(list_users(), use_container_width=True)
    st.subheader("Tambah User")
    email_new = st.text_input("Email Baru")
    name_new = st.text_input("Nama Baru")
    role_new = st.selectbox("Role Baru", ["EMPLOYEE", "MANAGER", "HR_ADMIN"], key="role_new")
    division_new = st.text_input("Division", value="")
    managers_df = list_managers()
    mgr_options = ["(None)"] + [f"{r['name']} ({r['email']})" for _, r in managers_df.iterrows()]
    mgr_sel = st.selectbox("Manager", options=list(range(len(mgr_options))), format_func=lambda i: mgr_options[i], key="mgr_new")
    manager_id_new = None if mgr_sel == 0 else int(managers_df.iloc[int(mgr_sel)-1]["id"])
    password_new = st.text_input("Password Baru", type="password")
    if st.button("Buat User"):
        if not email_new or not name_new or not password_new:
            st.error("Email/Nama/Password wajib diisi.")
        else:
            try:
                create_user(email_new, name_new, role_new, password_new, manager_id_new, division_new.strip() or None)
                st.success("User dibuat.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("Email sudah digunakan.")
    st.markdown("---")
    st.subheader("Edit / Delete User")
    users_df2 = list_users()
    if users_df2.empty:
        st.info("Belum ada user.")
        return
    display2 = users_df2.apply(lambda r: f"{r['name']} ({r['email']}) [{r['role']}] • {r.get('division','-')}", axis=1).tolist()
    idx2 = st.selectbox("Pilih User untuk Diedit", options=list(range(len(display2))), format_func=lambda i: display2[i], key="edit_user_pick")
    user_to_edit = users_df2.iloc[int(idx2)]
    edit_email = st.text_input("Email", value=user_to_edit["email"], key="edit_email")
    edit_name = st.text_input("Nama", value=user_to_edit["name"], key="edit_name")
    edit_role = st.selectbox("Role", ["EMPLOYEE", "MANAGER", "HR_ADMIN"],
                             index=int(["EMPLOYEE", "MANAGER", "HR_ADMIN"].index(user_to_edit["role"])), key="edit_role")
    edit_division = st.text_input("Division", value=user_to_edit.get("division") or "", key="edit_division")
    managers_df3 = list_managers()
    mgr_opts3 = ["(None)"] + [f"{r['name']} ({r['email']})" for _, r in managers_df3.iterrows()]
    current_mgr_id = user_to_edit["manager_id"]
    if pd.isna(current_mgr_id):
        current_mgr_id = None
    else:
        current_mgr_id = int(current_mgr_id)
    current_idx = 0
    if current_mgr_id is not None and not managers_df3.empty:
        ids_list = managers_df3["id"].astype(int).tolist()
        if current_mgr_id in ids_list:
            current_idx = 1 + ids_list.index(current_mgr_id)
    sel_mgr_idx = st.selectbox("Manager", options=list(range(len(mgr_opts3))), index=int(current_idx),
                               format_func=lambda i: mgr_opts3[i], key="edit_mgr")
    edit_manager_id = None if sel_mgr_idx == 0 else int(managers_df3.iloc[int(sel_mgr_idx)-1]["id"])
    new_pw = st.text_input("Reset Password (opsional)", type="password", key="edit_pw")
    col_save, col_del = st.columns(2)
    with col_save:
        if st.button("Simpan Perubahan"):
            try:
                update_user(int(user_to_edit["id"]), edit_email, edit_name, edit_role, edit_manager_id,
                            new_pw if new_pw else None, edit_division.strip() or None)
                st.success("Perubahan user disimpan.")
                st.rerun()
            except sqlite3.IntegrityError:
                st.error("Email sudah digunakan user lain.")
            except Exception as e:
                st.error(str(e))
    with col_del:
        if st.button("Hapus User"):
            if int(user_to_edit["id"]) == int(user["id"]):
                st.error("Tidak dapat menghapus akun yang sedang login.")
            else:
                try:
                    delete_user(int(user_to_edit["id"]))
                    st.warning("User dihapus.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

def main():
    st.set_page_config(page_title="HR-MS CISTECH", layout="wide")
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image("cistech.png", width=450) 
    if pyholidays is None:
        st.warning("Package 'holidays' tidak ditemukan. Fitur kalender libur dinonaktifkan. Install: pip install holidays")
    init_db()
    if not st.session_state.authenticated:
        page_login()
        return
    user = st.session_state.user
    if not user:
        page_login()
        return
    choice = sidebar_menu()
    if user["role"] == "EMPLOYEE":
        if choice == "Dashboard":
            page_employee_dashboard(user)
        elif choice == "Submit Leave":
            page_submit_leave(user)
        elif choice == "Submit Change Off":
            page_submit_changeoff(user)
        elif choice == "My Requests":
            page_my_requests(user)
    elif user["role"] == "MANAGER":
        if choice == "Dashboard":
            page_employee_dashboard(user)
        elif choice == "Submit Leave":
            page_submit_leave(user)
        elif choice == "Submit Change Off":
            page_submit_changeoff(user)
        elif choice == "Pending (Manager)":
            page_manager_pending(user)
        elif choice == "Team Requests":
            page_manager_team(user)
    elif user["role"] == "HR_ADMIN":
        if choice == "Pending (HR)":
            page_hr_pending(user)
        elif choice == "Quotas":
            page_hr_quotas(user)
        elif choice == "Users":
            page_hr_users(user)

if __name__ == "__main__":
    main()
