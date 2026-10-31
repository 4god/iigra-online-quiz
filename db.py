"""SQLite storage. Used only from the event-loop thread, so one connection is enough."""

import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    salt BLOB NOT NULL,
    pw BLOB NOT NULL,
    created REAL NOT NULL,
    score INTEGER NOT NULL DEFAULT 0,
    time REAL NOT NULL DEFAULT 0,
    answered INTEGER NOT NULL DEFAULT 0,
    muted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    user_id TEXT NOT NULL,
    variant INTEGER NOT NULL,
    answers TEXT NOT NULL,
    score INTEGER NOT NULL DEFAULT 0,
    started REAL NOT NULL,
    code TEXT,
    completed REAL,
    PRIMARY KEY (user_id, variant)
);
CREATE TABLE IF NOT EXISTS codes (
    code TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    variant INTEGER NOT NULL,
    score INTEGER NOT NULL,
    completed REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""


class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)

    # ------------------------------------------------------------ users
    def all_users(self):
        return self.conn.execute("SELECT id, score, time, answered, muted FROM users").fetchall()

    def create_user(self, uid, salt, pw):
        self.conn.execute("INSERT INTO users (id, salt, pw, created) VALUES (?, ?, ?, ?)", (uid, salt, pw, time.time()))

    def user_secret(self, uid):
        return self.conn.execute("SELECT salt, pw FROM users WHERE id = ?", (uid,)).fetchone()

    def save_scores(self, rows):
        """rows: iterable of (score, time, answered, uid)"""
        with self.conn:
            self.conn.execute("BEGIN")
            self.conn.executemany("UPDATE users SET score = ?, time = ?, answered = ? WHERE id = ?", rows)

    def reset_scores(self):
        self.conn.execute("UPDATE users SET score = 0, time = 0, answered = 0")

    def set_muted(self, uid, muted):
        self.conn.execute("UPDATE users SET muted = ? WHERE id = ?", (1 if muted else 0, uid))

    # ------------------------------------------------------------ sessions
    def all_sessions(self):
        return self.conn.execute("SELECT token, user_id FROM sessions").fetchall()

    def create_session(self, token, uid):
        self.conn.execute("INSERT INTO sessions (token, user_id, created) VALUES (?, ?, ?)", (token, uid, time.time()))

    def delete_session(self, token):
        self.conn.execute("DELETE FROM sessions WHERE token = ?", (token,))

    # ------------------------------------------------------------ self-paced attempts
    def attempts_for(self, uid):
        return self.conn.execute(
            "SELECT variant, answers, score, code, completed FROM attempts WHERE user_id = ?", (uid,)
        ).fetchall()

    def get_attempt(self, uid, variant):
        row = self.conn.execute(
            "SELECT answers, score, code, completed FROM attempts WHERE user_id = ? AND variant = ?", (uid, variant)
        ).fetchone()
        if not row:
            return None
        return {"answers": json.loads(row["answers"]), "score": row["score"], "code": row["code"], "completed": row["completed"]}

    def create_attempt(self, uid, variant):
        self.conn.execute(
            "INSERT OR IGNORE INTO attempts (user_id, variant, answers, started) VALUES (?, ?, '{}', ?)",
            (uid, variant, time.time()),
        )

    def save_attempt(self, uid, variant, answers, score):
        self.conn.execute(
            "UPDATE attempts SET answers = ?, score = ? WHERE user_id = ? AND variant = ?",
            (json.dumps(answers), score, uid, variant),
        )

    def complete_attempt(self, uid, variant, code, score):
        now = time.time()
        with self.conn:
            self.conn.execute("BEGIN")
            self.conn.execute(
                "UPDATE attempts SET code = ?, completed = ? WHERE user_id = ? AND variant = ?", (code, now, uid, variant)
            )
            self.conn.execute(
                "INSERT INTO codes (code, user_id, variant, score, completed) VALUES (?, ?, ?, ?, ?)",
                (code, uid, variant, score, now),
            )
        return now

    def code_exists(self, code):
        return self.conn.execute("SELECT 1 FROM codes WHERE code = ?", (code,)).fetchone() is not None

    def find_code(self, code):
        return self.conn.execute(
            "SELECT code, user_id, variant, score, completed FROM codes WHERE code = ?", (code,)
        ).fetchone()

    def codes_count_for(self, uid):
        return self.conn.execute("SELECT COUNT(*) FROM codes WHERE user_id = ?", (uid,)).fetchone()[0]

    def all_codes(self):
        return self.conn.execute(
            "SELECT code, user_id, variant, score, completed FROM codes ORDER BY completed"
        ).fetchall()

    # ------------------------------------------------------------ key/value settings
    def get_kv(self, key, default=None):
        row = self.conn.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
        return json.loads(row["v"]) if row else default

    def set_kv(self, key, value):
        self.conn.execute(
            "INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", (key, json.dumps(value))
        )
