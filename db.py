"""قاعدة بيانات SQLite: الأكواد + عدّاد الصلاة + سجل المهام + إحصائيات المستخدم."""
import sqlite3
import threading
import time

_lock = threading.Lock()
_conn = None


def init(path="bot.db"):
    global _conn
    _conn = sqlite3.connect(path, check_same_thread=False)
    with _lock:
        _conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS users (
                tg_id      INTEGER PRIMARY KEY,
                tg_name    TEXT,
                username   TEXT,
                first_seen INTEGER,
                last_seen  INTEGER
            );
            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id      INTEGER,
                started    INTEGER,
                finished   INTEGER,
                status     TEXT,
                questions  INTEGER DEFAULT 0,
                activities INTEGER DEFAULT 0,
                duration   INTEGER DEFAULT 0,
                note       TEXT
            );
            INSERT OR IGNORE INTO counters(name, value) VALUES ('salawat', 0);
            INSERT OR IGNORE INTO counters(name, value) VALUES ('total_users', 0);
            INSERT OR IGNORE INTO counters(name, value) VALUES ('total_jobs', 0);
            INSERT OR IGNORE INTO counters(name, value) VALUES ('total_questions', 0);
            """
        )
        # Migration: أضف عمود duration لو مش موجود (ترقية من v5)
        try:
            _conn.execute("ALTER TABLE jobs ADD COLUMN duration INTEGER DEFAULT 0")
            _conn.commit()
        except Exception:
            pass
        _conn.commit()


# ───────────────────────── Users ─────────────────────────

def touch_user(tg_id, tg_name=""):
    now = int(time.time())
    with _lock:
        existing = _conn.execute(
            "SELECT tg_id FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        _conn.execute(
            "INSERT INTO users(tg_id, tg_name, first_seen, last_seen) VALUES (?,?,?,?) "
            "ON CONFLICT(tg_id) DO UPDATE SET tg_name=excluded.tg_name, last_seen=excluded.last_seen",
            (tg_id, tg_name, now, now),
        )
        if not existing:
            _conn.execute(
                "UPDATE counters SET value=value+1 WHERE name='total_users'"
            )
        _conn.commit()


def save_username(tg_id, username):
    with _lock:
        _conn.execute("UPDATE users SET username=? WHERE tg_id=?", (username, tg_id))
        _conn.commit()


def get_username(tg_id):
    with _lock:
        row = _conn.execute(
            "SELECT username FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
    return row[0] if row and row[0] else None


# ───────────────────────── Salawat ─────────────────────────

def add_salawat():
    with _lock:
        _conn.execute("UPDATE counters SET value=value+1 WHERE name='salawat'")
        value = _conn.execute(
            "SELECT value FROM counters WHERE name='salawat'"
        ).fetchone()[0]
        _conn.commit()
    return value


def get_salawat():
    with _lock:
        return _conn.execute(
            "SELECT value FROM counters WHERE name='salawat'"
        ).fetchone()[0]


# ───────────────────────── Jobs ─────────────────────────

def log_job(tg_id, started, status, questions, activities, note="", duration=0):
    with _lock:
        _conn.execute(
            "INSERT INTO jobs(tg_id, started, finished, status, questions, activities, duration, note) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                tg_id, int(started), int(time.time()),
                status, questions, activities or 0,
                int(duration), note[:300],
            ),
        )
        # حدّث العدادات الكلية
        _conn.execute(
            "UPDATE counters SET value=value+1 WHERE name='total_jobs'"
        )
        if questions:
            _conn.execute(
                "UPDATE counters SET value=value+? WHERE name='total_questions'",
                (questions,),
            )
        _conn.commit()


# ───────────────────────── إحصائيات الفرد ─────────────────────────

def get_user_stats(tg_id):
    """إحصائيات مستخدم واحد: عدد الجلسات، الأسئلة، الأنشطة، آخر جلسة، أطول جلسة."""
    with _lock:
        row = _conn.execute(
            """
            SELECT
                COUNT(*)                                    AS sessions,
                COALESCE(SUM(questions), 0)                 AS total_q,
                COALESCE(SUM(CASE WHEN activities IS NOT NULL
                              THEN activities ELSE 0 END), 0) AS total_act,
                MAX(finished)                               AS last_run,
                MAX(duration)                               AS longest_sec,
                COALESCE(SUM(duration), 0)                  AS total_sec,
                SUM(CASE WHEN status='finished' THEN 1 ELSE 0 END) AS finished_count
            FROM jobs
            WHERE tg_id=?
            """,
            (tg_id,),
        ).fetchone()
        user_row = _conn.execute(
            "SELECT tg_name, first_seen FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
    return {
        "sessions":      row[0] or 0,
        "total_q":       row[1] or 0,
        "total_act":     row[2] or 0,
        "last_run":      row[3],
        "longest_sec":   row[4] or 0,
        "total_sec":     row[5] or 0,
        "finished":      row[6] or 0,
        "name":          user_row[0] if user_row else "",
        "first_seen":    user_row[1] if user_row else None,
    }


# ───────────────────────── إحصائيات الكل (أدمن) ─────────────────────────

def get_global_stats():
    """إحصائيات عامة للبوت كله."""
    with _lock:
        totals = {
            r[0]: r[1]
            for r in _conn.execute("SELECT name, value FROM counters").fetchall()
        }
        active_today = _conn.execute(
            "SELECT COUNT(DISTINCT tg_id) FROM jobs WHERE started >= ?",
            (int(time.time()) - 86400,),
        ).fetchone()[0]
        top_row = _conn.execute(
            """
            SELECT tg_id, tg_name, SUM(questions) AS q
            FROM jobs JOIN users USING(tg_id)
            GROUP BY tg_id ORDER BY q DESC LIMIT 1
            """
        ).fetchone()
    return {
        "total_users":     totals.get("total_users", 0),
        "total_jobs":      totals.get("total_jobs", 0),
        "total_questions": totals.get("total_questions", 0),
        "salawat":         totals.get("salawat", 0),
        "active_today":    active_today,
        "top_user":        (top_row[1] or f"ID:{top_row[0]}", top_row[2]) if top_row else None,
    }
