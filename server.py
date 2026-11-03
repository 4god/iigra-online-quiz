"""
ИИгра — «Своя игра» про ИИ для онлайн-трансляции.

  /        — игроки: регистрация (логин + пароль, ID выдаётся сервером), игра, чат, самостоятельные варианты
  /host    — пульт ведущего: голосования, вопросы, модерация чата, итоги
  /verify  — проверка кодов призов за самостоятельные варианты
  /screen  — экран для трансляции: поле, вопрос, результаты, лидеры, QR (только просмотр)

Рассчитано на ~1000 одновременных игроков: у каждого подключения своя очередь
исходящих сообщений (медленный телефон не тормозит остальных), а частые события —
чат, ход голосования, счётчики — рассылаются пачками по таймеру.
"""

import asyncio
import base64
import csv
import hashlib
import hmac
import io
import json
import os
import random
import re
import secrets
import sqlite3
import time
import unicodedata
from collections import deque
from urllib.parse import unquote
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import qrcode
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import DB
from questions import HARD, POINTS, SETS, QuestionsError, _question, load_single_board, read_text

BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

QUESTION_SECONDS = int(os.environ.get("QUESTION_SECONDS", 30))
# The final question: everyone bets part of their score after seeing only its theme.
WAGER_SECONDS = int(os.environ.get("WAGER_SECONDS", 30))
# Players can't join the broadcast (vote, answer, chat) before this moment.
LIVE_OPENS_AT = datetime.fromisoformat(os.environ.get("LIVE_OPENS_AT", "2026-11-12T00:00:00+03:00")).timestamp()


def live_open():
    return time.time() >= LIVE_OPENS_AT
HOST_KEY = os.environ.get("HOST_KEY") or secrets.token_hex(3)
print("=" * 50)
print(f"  КЛЮЧ ВЕДУЩЕГО: {HOST_KEY}")
print("=" * 50, flush=True)


# ---------------------------------------------------------------- questions

def load_live_board():
    """The broadcast set is kept out of the public repo: LIVE_QUESTIONS env (base64 of the CSV)
    or live_questions.csv next to the server. Without either (e.g. a local game) LIVE_VARIANT=1..N
    picks one of the public variants. Returns (board, unique, error text for the host)."""
    raw = os.environ.get("LIVE_QUESTIONS")
    csv_path, json_path = BASE_DIR / "live_questions.csv", BASE_DIR / "live_questions.json"
    try:
        if raw:
            text, source = base64.b64decode(raw).decode("utf-8-sig"), "LIVE_QUESTIONS"
        elif csv_path.exists():
            text, source = read_text(csv_path), csv_path.name
        elif json_path.exists():
            text, source = json_path.read_text("utf-8"), json_path.name
        else:
            text = None
        if text is not None:
            if text.lstrip().startswith("{"):  # older JSON format
                data = json.loads(text)
                board = [
                    {"name": name, "questions": [_question(POINTS[li], tuple(q)) for li, q in enumerate(cat)]}
                    for name, cat in zip(data["categories"], data["questions"])
                ]
            else:
                board = load_single_board(text, source)
            return board, True, None
    except (QuestionsError, ValueError, KeyError) as e:
        print("!!! Вопросы трансляции не загружены:\n", e, flush=True)
        error = str(e)
    else:
        error = None
    variant = min(max(int(os.environ.get("LIVE_VARIANT", 1)), 1), len(SETS))
    print(f"Игра идёт на варианте {variant} из questions.csv", flush=True)
    return SETS[variant - 1], False, error


LIVE_BOARD, LIVE_UNIQUE, LIVE_ERROR = load_live_board()
N_CATS = len(LIVE_BOARD)
N_LEVELS = len(POINTS)
TOTAL_TILES = N_CATS * N_LEVELS

VARIANTS = SETS + ([HARD] if HARD else [])  # self-paced; the hidden ML set goes last
ML_INDEX = len(SETS) if HARD else -1


def tile_key(ci, li):
    return f"{ci}-{li}"


def valid_tile(ci, li):
    return isinstance(ci, int) and isinstance(li, int) and 0 <= ci < N_CATS and 0 <= li < N_LEVELS


def public_question(board, ci, li):
    cat = board[ci]
    q = cat["questions"][li]
    return {"ci": ci, "li": li, "category": cat["name"], "points": q["points"], "q": q["q"], "options": q["options"]}


# ---------------------------------------------------------------- storage & in-memory state

db = DB(DATA_DIR / "iigra.db")

players = {
    r["id"]: {"name": r["login"], "score": r["score"], "time": r["time"], "answered": r["answered"],
              "streak": r["streak"], "muted": bool(r["muted"])}
    for r in db.all_users()
}
sessions = {r["token"]: r["user_id"] for r in db.all_sessions()}

settings = {"chat_enabled": True, "slow": 5, "solo_open": False}
settings.update(db.get_kv("settings", {}))

game = {
    "phase": "idle",          # idle | voting | wager | question | locked | revealed | finished
    "used": db.get_kv("used", {}),
    "current": None,          # {ci, li, opened, deadline, answers: {uid: (option, seconds)}, final}
    "vote": None,             # {votes: {uid: key}, tallies: {key: n}, level: row index or None}
    "final": None,            # {ci, li, deadline, closed, wagers: {uid: amount}} from the wager until the reveal
    "vote_winner": None,
    "last_reveal": None,
    "last_results": {},
    "finish_at": None,
}
if len(game["used"]) >= TOTAL_TILES:
    game["phase"] = "finished"

ranked = []
rank_of = {}
rank_move = {}  # uid -> places gained at the last reveal (negative: lost), "new" for a first entry


def recompute_ranks():
    global ranked, rank_of
    rows = [(uid, p) for uid, p in players.items() if p["answered"] > 0]
    rows.sort(key=lambda r: (-r[1]["score"], r[1]["time"], r[0]))
    ranked = rows
    rank_of = {uid: i + 1 for i, (uid, _) in enumerate(rows)}


def top(n):
    return [{"id": uid, "name": p["name"], "score": p["score"], "streak": p["streak"], "move": rank_move.get(uid)}
            for uid, p in ranked[:n]]


def streak_mult(streak):
    """×1.1 from the 3rd correct answer in a row, +0.1 for each one after that, at most ×1.5."""
    return round(1 + 0.1 * max(0, min(streak, 7) - 2), 1)


recompute_ranks()


def now():
    return time.time()


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------- rate limiting

class Limiter:
    def __init__(self, limit, window):
        self.limit, self.window, self.hits = limit, window, {}

    def _trim(self, key):
        q = self.hits.get(key)
        if q is None:
            return None
        t = now()
        while q and t - q[0] > self.window:
            q.popleft()
        return q

    def blocked(self, key):
        q = self._trim(key)
        return q is not None and len(q) >= self.limit

    def add(self, key):
        self.hits.setdefault(key, deque()).append(now())

    def allow(self, key):
        if self.blocked(key):
            return False
        self.add(key)
        return True

    def cleanup(self):
        for key in list(self.hits):
            if not self._trim(key):
                del self.hits[key]


register_limit = Limiter(int(os.environ.get("REGISTER_LIMIT", 60)), 60)  # per IP per minute; offices share one NAT
login_fail_ip = Limiter(50, 600)
login_fail_id = Limiter(10, 600)
host_fail_ip = Limiter(10, 600)
verify_limit = Limiter(60, 60)
chat_global = Limiter(15, 1.0)        # whole chat: at most 15 messages per second
LIMITERS = [register_limit, login_fail_ip, login_fail_id, host_fail_ip, verify_limit, chat_global]


def client_ip(headers, client):
    fwd = headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return client.host if client else "?"


# ---------------------------------------------------------------- accounts

PBKDF2_ROUNDS = 20_000  # game accounts guard nothing valuable; keeps a 1000-person signup burst cheap


def hash_pw(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ROUNDS)


def host_key_ok(key):
    # compare as bytes: hmac.compare_digest rejects non-ASCII str, and keys may be Cyrillic
    return hmac.compare_digest(str(key or "").encode("utf-8"), HOST_KEY.encode("utf-8"))


def new_uid():
    while True:
        uid = str(secrets.randbelow(900_000) + 100_000)
        if uid not in players:
            return uid


def new_session(uid):
    token = secrets.token_urlsafe(32)
    db.create_session(token, uid)
    sessions[token] = uid
    return token


def uid_from_auth(authorization):
    token = (authorization or "").removeprefix("Bearer ").strip()
    uid = sessions.get(token)
    if not uid or uid not in players:
        raise HTTPException(401, "Сессия устарела — войдите заново")
    return uid


# ---------------------------------------------------------------- connections

class Conn:
    __slots__ = ("ws", "q", "uid", "host", "alive", "ip", "msg_times")

    def __init__(self, ws, ip):
        self.ws, self.ip = ws, ip
        self.q = asyncio.Queue(maxsize=400)
        self.uid, self.host, self.alive = None, False, True
        self.msg_times = deque()

    def push(self, text):
        if not self.alive:
            return
        try:
            self.q.put_nowait(text)
        except asyncio.QueueFull:
            # The client can't keep up — drop it; it will reconnect and resync.
            self.alive = False
            asyncio.create_task(self._close())

    async def _close(self):
        try:
            await self.ws.close()
        except Exception:
            pass

    async def writer(self):
        try:
            while self.alive:
                text = await self.q.get()
                if text is None:
                    break
                await self.ws.send_text(text)
        except Exception:
            pass
        finally:
            self.alive = False

    def flooding(self):
        t = now()
        while self.msg_times and t - self.msg_times[0] > 1.0:
            self.msg_times.popleft()
        self.msg_times.append(t)
        return len(self.msg_times) > 20


conns = set()
user_conns = {}
hosts = set()
dirty = {"vote": False, "answers": False, "wagers": False, "stats": True}
last_sent = {"chat": 0.0, "vote": 0.0, "answers": 0.0, "wagers": 0.0, "stats": 0.0, "cleanup": 0.0}


def broadcast(obj):
    text = dumps(obj)
    for c in list(conns):
        c.push(text)


def send(c, obj):
    c.push(dumps(obj))


def send_user(uid, obj):
    cs = user_conns.get(uid)
    if cs:
        text = dumps(obj)
        for c in list(cs):
            c.push(text)


def send_hosts(obj):
    text = dumps(obj)
    for c in list(hosts):
        c.push(text)


def online_count():
    return len(user_conns)


# ---------------------------------------------------------------- chat

chat_history = deque(maxlen=150)
chat_out = []
chat_seq = 0
chat_user_last = {}

BAD_WORDS = re.compile(
    r"(?<![а-яёa-z])(?:х[уy][йиеяю]\w*|п[иеi]зд\w*|[её]б[аеиуы]\w*|бля\w*|сук[аи](?![а-яё])|муд[аи]\w*|"
    r"долбо\w*|пид[оа]р\w*|гандон\w*|залуп\w*)",
    re.IGNORECASE,
)
CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def clean_text(text):
    text = CONTROL.sub(" ", str(text))[:200]
    text = re.sub(r"\s+", " ", text).strip()
    return BAD_WORDS.sub("***", text)


def post_chat(uid, text, host=False):
    global chat_seq
    chat_seq += 1
    name = None if host else players[uid]["name"]
    msg = {"id": chat_seq, "uid": uid, "name": name, "text": text, "ts": round(now(), 1), "host": host}
    chat_history.append(msg)
    chat_out.append(msg)


def chat_from_player(c, text):
    p = players[c.uid]
    if not settings["chat_enabled"]:
        return send(c, {"type": "notice", "text": "Чат выключен ведущим"})
    if p["muted"]:
        return send(c, {"type": "notice", "text": "Вам ограничили отправку сообщений в чат"})
    text = clean_text(text)
    if not text:
        return
    t = now()
    wait = settings["slow"] - (t - chat_user_last.get(c.uid, 0))
    if wait > 0:
        return send(c, {"type": "notice", "text": f"Писать можно раз в {settings['slow']} с — подождите ещё {int(wait) + 1} с"})
    if not chat_global.allow("all"):
        return send(c, {"type": "notice", "text": "Чат перегружен — попробуйте через пару секунд"})
    chat_user_last[c.uid] = t
    post_chat(c.uid, text)


def delete_chat_ids(ids):
    ids = {i for i in ids if isinstance(i, int)}
    if not ids:
        return
    kept = [m for m in chat_history if m["id"] not in ids]
    chat_history.clear()
    chat_history.extend(kept)
    chat_out[:] = [m for m in chat_out if m["id"] not in ids]
    broadcast({"type": "chat_delete", "ids": sorted(ids)})


# ---------------------------------------------------------------- live game

def public_current():
    c = game["current"]
    if not c:
        return None
    d = public_question(LIVE_BOARD, c["ci"], c["li"])
    d.update({"deadline": c["deadline"], "locked": game["phase"] == "locked", "answered": len(c["answers"]),
              "final": c.get("final", False)})
    return d


def me_payload(uid):
    p = players.get(uid)
    if not p:
        return None
    return {"id": uid, "name": p["name"], "score": p["score"], "rank": rank_of.get(uid), "muted": p["muted"],
            "streak": p["streak"], "move": rank_move.get(uid)}


def muted_list():
    return [{"id": u, "name": p["name"]} for u, p in players.items() if p["muted"]]


def sync_payload(uid=None, host=False):
    phase = game["phase"]
    cur = public_current()
    if cur and uid and uid in game["current"]["answers"]:
        cur["my_answer"] = game["current"]["answers"][uid][0]
    if cur and cur["final"] and game["final"]:
        cur["my_wager"] = game["final"]["wagers"].get(uid)
    vote = None
    if game["vote"]:
        v = game["vote"]
        vote = {"tallies": v["tallies"], "total": len(v["votes"]), "my": v["votes"].get(uid), "level": v["level"]}
    payload = {
        "type": "sync",
        "phase": phase,
        "used": game["used"],
        "current": cur,
        "vote": vote,
        "vote_winner": game["vote_winner"],
        "final": final_info(uid) if phase == "wager" else None,
        "last_reveal": game["last_reveal"] if phase == "revealed" else None,
        "my_result": game["last_results"].get(uid) if phase == "revealed" else None,
        "top": top(50 if phase == "finished" else 15),
        "participants": len(ranked),
        "me": me_payload(uid) if uid else None,
        "chat": list(chat_history)[-100:],
        "settings": settings,
        "online": online_count(),
        "registered": len(players),
        "categories": [c["name"] for c in LIVE_BOARD],
        "points": POINTS,
        "question_seconds": QUESTION_SECONDS,
        "server_now": now(),
        "live_open": live_open(),
        "live_opens_at": LIVE_OPENS_AT,
    }
    if host:
        payload["live_unique"] = LIVE_UNIQUE
        payload["live_error"] = LIVE_ERROR
        payload["muted"] = muted_list()
    return payload


def save_settings():
    db.set_kv("settings", settings)
    broadcast({"type": "settings", "settings": settings})


def free_levels():
    return sorted({li for ci in range(N_CATS) for li in range(N_LEVELS) if tile_key(ci, li) not in game["used"]})


def start_vote(level=None):
    """level: a row index to vote within, "auto" for the cheapest row with open questions, anything else for all."""
    if game["phase"] not in ("idle", "revealed"):
        return
    levels = free_levels()
    if not levels:
        return
    if level == "auto":
        level = levels[0]
    elif isinstance(level, bool) or level not in levels:
        level = None
    game.update(phase="voting", vote={"votes": {}, "tallies": {}, "level": level}, vote_winner=None, last_reveal=None)
    broadcast({"type": "vote_started", "level": level})


def cast_vote(uid, ci, li):
    v = game["vote"]
    if game["phase"] != "voting" or not v or not valid_tile(ci, li):
        return
    key = tile_key(ci, li)
    if key in game["used"] or (v["level"] is not None and li != v["level"]):
        return
    prev = v["votes"].get(uid)
    if prev == key:
        return
    if prev:
        v["tallies"][prev] -= 1
        if v["tallies"][prev] <= 0:
            del v["tallies"][prev]
    v["votes"][uid] = key
    v["tallies"][key] = v["tallies"].get(key, 0) + 1
    dirty["vote"] = True
    send_user(uid, {"type": "vote_ack", "key": key})


def end_vote():
    v = game["vote"]
    if game["phase"] != "voting" or not v:
        return
    tallies = v["tallies"]
    if tallies:
        best = max(tallies.values())
        winner = random.choice([k for k, n in tallies.items() if n == best])
    else:
        free = [tile_key(ci, li) for ci in range(N_CATS) for li in range(N_LEVELS)
                if tile_key(ci, li) not in game["used"] and v["level"] in (None, li)]
        winner = random.choice(free) if free else None
    game.update(phase="idle", vote=None, vote_winner=winner)
    broadcast({"type": "vote_ended", "winner": winner, "tallies": tallies, "total": len(v["votes"])})


def open_question(ci, li, final=False):
    if game["phase"] not in ("idle", "voting", "revealed") or not valid_tile(ci, li):
        return
    if tile_key(ci, li) in game["used"]:
        return
    t = now()
    game.update(
        phase="question",
        vote=None,
        vote_winner=None,
        last_reveal=None,
        last_results={},
        current={"ci": ci, "li": li, "opened": t, "deadline": t + QUESTION_SECONDS, "answers": {}, "final": final},
    )
    broadcast({"type": "question_opened", "data": public_current(), "server_now": t})


def final_info(uid=None):
    f = game["final"]
    cat = LIVE_BOARD[f["ci"]]
    return {"category": cat["name"], "points": cat["questions"][f["li"]]["points"], "deadline": f["deadline"],
            "seconds": WAGER_SECONDS, "closed": f["closed"], "count": len(f["wagers"]),
            "my": f["wagers"].get(uid) if uid else None}


def start_final():
    """The most expensive open question becomes the final; players bet before it opens."""
    if game["phase"] not in ("idle", "revealed"):
        return
    levels = free_levels()
    if not levels:
        return
    li = levels[-1]
    ci = random.choice([c for c in range(N_CATS) if tile_key(c, li) not in game["used"]])
    t = now()
    game.update(phase="wager", vote=None, vote_winner=None, last_reveal=None, last_results={}, current=None,
                final={"ci": ci, "li": li, "deadline": t + WAGER_SECONDS, "closed": False, "wagers": {}})
    broadcast({"type": "final_started", **final_info(), "server_now": t})


def wager_cap(p, points):
    # Everyone can play the final: whoever has less than the question is worth may bet up to its worth.
    return max(p["score"], points)


def place_wager(uid, amount):
    f = game["final"]
    p = players.get(uid)
    if game["phase"] != "wager" or not f or f["closed"] or not p:
        return
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return
    points = LIVE_BOARD[f["ci"]]["questions"][f["li"]]["points"]
    f["wagers"][uid] = max(0, min(int(amount), wager_cap(p, points)))
    dirty["wagers"] = True
    send_user(uid, {"type": "wager_ack", "amount": f["wagers"][uid]})


def close_wagers():
    f = game["final"]
    if f and not f["closed"]:
        f["closed"] = True
        broadcast({"type": "wager_closed", "count": len(f["wagers"])})


def open_final():
    f = game["final"]
    if game["phase"] != "wager" or not f:
        return
    close_wagers()
    game["phase"] = "idle"
    open_question(f["ci"], f["li"], final=True)


def submit_answer(uid, option):
    c = game["current"]
    if game["phase"] != "question" or not c:
        return
    if not isinstance(option, int) or not 0 <= option <= 3:
        return
    t = now()
    if t > c["deadline"] or uid in c["answers"]:
        return
    c["answers"][uid] = (option, round(t - c["opened"], 3))
    dirty["answers"] = True
    send_user(uid, {"type": "answer_ack", "option": option})


def cancel_question():
    if not game["current"] and game["phase"] != "wager":
        return
    game.update(phase="idle", current=None, final=None)
    broadcast({"type": "cancelled"})


def reveal():
    c = game["current"]
    if not c or game["phase"] not in ("question", "locked"):
        return
    q = LIVE_BOARD[c["ci"]]["questions"][c["li"]]
    pts, correct = q["points"], q["correct"]
    final = game["final"] if c.get("final") else None
    wagers = final["wagers"] if final else {}
    dist = [0, 0, 0, 0]
    results, changed = {}, {}
    fastest = None
    prev_rank = dict(rank_of)
    for uid, (opt, dt) in c["answers"].items():
        p = players.get(uid)
        if not p:
            continue
        dist[opt] += 1
        p["answered"] += 1
        ok = opt == correct
        r = {"option": opt, "correct": ok, "delta": 0}
        if final:
            r["wager"] = wagers.get(uid, 0)
            r["delta"] = r["wager"] if ok else -min(r["wager"], p["score"])
        else:
            p["streak"] = p["streak"] + 1 if ok else 0
            r["streak"] = p["streak"]
            if ok:
                r["mult"] = streak_mult(p["streak"])
                r["delta"] = round(pts * r["mult"])
        if ok:
            p["time"] = round(p["time"] + dt, 3)
            if fastest is None or dt < fastest[1]:
                fastest = (uid, dt)
        p["score"] += r["delta"]
        results[uid] = r
        changed[uid] = p
    if final:
        for uid, w in wagers.items():  # a bet without an answer is lost as well
            p = players.get(uid)
            if uid in results or not p or not w:
                continue
            loss = min(w, p["score"])
            p["score"] -= loss
            results[uid] = {"option": None, "correct": False, "delta": -loss, "wager": w}
            changed[uid] = p
    else:
        for uid, p in players.items():  # skipping a question breaks the streak
            if p["streak"] and uid not in results:
                p["streak"] = 0
                changed[uid] = p
    db.save_scores([(p["score"], p["time"], p["answered"], p["streak"], uid) for uid, p in changed.items()])

    game["used"][tile_key(c["ci"], c["li"])] = True
    db.set_kv("used", game["used"])
    recompute_ranks()
    rank_move.clear()
    rank_move.update({uid: prev_rank[uid] - r if uid in prev_rank else "new" for uid, r in rank_of.items()})

    last = final is not None or len(game["used"]) >= TOTAL_TILES
    payload = {
        "type": "revealed",
        "question": public_question(LIVE_BOARD, c["ci"], c["li"]),
        "correct": correct,
        "dist": dist,
        "answered": sum(dist),
        "correct_count": dist[correct],
        "fastest": {"id": fastest[0], "name": players[fastest[0]]["name"], "seconds": round(fastest[1], 1)} if fastest else None,
        "fact": q.get("fact", ""),
        "final": final is not None,
        "top": top(15),
        "participants": len(ranked),
        "used": game["used"],
        "last": last,
    }
    if final:
        payload["wager_won"] = sum(1 for r in results.values() if r.get("wager") and r["correct"])
        payload["wager_lost"] = sum(1 for r in results.values() if r.get("wager") and not r["correct"])
    game.update(phase="revealed", current=None, last_reveal=payload, last_results=results, final=None)
    broadcast(payload)
    for uid in list(user_conns):
        me = me_payload(uid)
        if me:
            me["result"] = results.get(uid)
            send_user(uid, {"type": "me", **me})
    if last:
        game["finish_at"] = now() + 8


def finish():
    game.update(phase="finished", finish_at=None, vote=None, current=None, vote_winner=None)
    broadcast({"type": "finished", "top": top(50), "participants": len(ranked)})
    for uid in list(user_conns):
        send_user(uid, {"type": "me", **me_payload(uid)})


def reset_game():
    for p in players.values():
        p.update(score=0, time=0, answered=0, streak=0)
    db.reset_scores()
    rank_move.clear()
    game.update(phase="idle", used={}, current=None, vote=None, vote_winner=None, final=None,
                last_reveal=None, last_results={}, finish_at=None)
    db.set_kv("used", {})
    recompute_ranks()
    for c in list(conns):
        send(c, sync_payload(c.uid, c.host))


# ---------------------------------------------------------------- periodic fan-out

def tick():
    t = now()
    if chat_out and t - last_sent["chat"] >= 0.4:
        broadcast({"type": "chat", "messages": chat_out[-40:]})
        chat_out.clear()
        last_sent["chat"] = t
    if dirty["vote"] and game["vote"] and t - last_sent["vote"] >= 0.8:
        v = game["vote"]
        broadcast({"type": "vote_tallies", "tallies": v["tallies"], "total": len(v["votes"])})
        dirty["vote"], last_sent["vote"] = False, t
    if dirty["answers"] and game["current"] and t - last_sent["answers"] >= 0.8:
        broadcast({"type": "answer_count", "count": len(game["current"]["answers"]), "online": online_count()})
        dirty["answers"], last_sent["answers"] = False, t
    if dirty["stats"] and t - last_sent["stats"] >= 2:
        broadcast({"type": "stats", "online": online_count(), "registered": len(players)})
        dirty["stats"], last_sent["stats"] = False, t
    f = game["final"]
    if game["phase"] == "wager" and f:
        if dirty["wagers"] and t - last_sent["wagers"] >= 0.8:
            broadcast({"type": "wager_count", "count": len(f["wagers"])})
            dirty["wagers"], last_sent["wagers"] = False, t
        if not f["closed"] and t >= f["deadline"]:
            close_wagers()
    if game["phase"] == "question" and game["current"] and t >= game["current"]["deadline"]:
        game["phase"] = "locked"
        broadcast({"type": "time_up"})
    if game["finish_at"] and t >= game["finish_at"]:
        finish()
    if t - last_sent["cleanup"] > 60:
        for lim in LIMITERS:
            lim.cleanup()
        last_sent["cleanup"] = t


async def ticker():
    while True:
        await asyncio.sleep(0.25)
        try:
            tick()
        except Exception as e:
            print("tick error:", repr(e), flush=True)


@asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(ticker())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------- websocket

def handle(c, msg):
    mtype = msg.get("type")

    if mtype == "ping":
        return
    if mtype == "auth":
        uid = sessions.get(str(msg.get("token", "")))
        if not uid or uid not in players:
            return send(c, {"type": "auth_failed"})
        c.uid = uid
        user_conns.setdefault(uid, set()).add(c)
        dirty["stats"] = True
        return send(c, sync_payload(uid))
    if mtype == "watch":
        # The broadcast screen: no account, gets only what every player sees.
        return send(c, sync_payload(None))
    if mtype == "host_auth":
        if host_fail_ip.blocked(c.ip):
            return send(c, {"type": "error", "message": "Слишком много попыток, подождите 10 минут"})
        if not host_key_ok(msg.get("key")):
            host_fail_ip.add(c.ip)
            return send(c, {"type": "error", "message": "Неверный ключ"})
        c.host = True
        hosts.add(c)
        send(c, {"type": "host_ok"})
        return send(c, sync_payload(None, host=True))

    if c.uid and not c.host:
        if mtype in ("vote", "answer", "chat", "wager") and not live_open():
            return send(c, {"type": "notice", "text": "Трансляция ещё не началась"})
        if mtype == "vote":
            cast_vote(c.uid, msg.get("ci"), msg.get("li"))
        elif mtype == "answer":
            submit_answer(c.uid, msg.get("option"))
        elif mtype == "chat":
            chat_from_player(c, msg.get("text", ""))
        elif mtype == "wager":
            place_wager(c.uid, msg.get("amount"))
        return

    if not c.host:
        return
    if mtype == "start_vote":
        start_vote(msg.get("level"))
    elif mtype == "end_vote":
        end_vote()
    elif mtype == "open_question":
        open_question(msg.get("ci"), msg.get("li"))
    elif mtype == "reveal":
        reveal()
    elif mtype == "start_final":
        start_final()
    elif mtype == "open_final":
        open_final()
    elif mtype == "cancel_question":
        cancel_question()
    elif mtype == "finish":
        finish()
    elif mtype == "reset_game":
        reset_game()
    elif mtype == "chat":
        text = clean_text(msg.get("text", ""))
        if text:
            post_chat("host", text, host=True)
    elif mtype == "chat_delete":
        delete_chat_ids([msg.get("id")])
    elif mtype == "chat_mute":
        uid, muted = str(msg.get("uid", "")), bool(msg.get("muted"))
        p = players.get(uid)
        if not p:
            return
        p["muted"] = muted
        db.set_muted(uid, muted)
        if muted and msg.get("purge"):
            delete_chat_ids([m["id"] for m in chat_history if m["uid"] == uid])
        send_user(uid, {"type": "muted", "muted": muted})
        send_hosts({"type": "muted_list", "muted": muted_list()})
    elif mtype == "chat_settings":
        if "enabled" in msg:
            settings["chat_enabled"] = bool(msg["enabled"])
        if msg.get("slow") in (0, 3, 5, 10, 30, 60):
            settings["slow"] = msg["slow"]
        save_settings()
    elif mtype == "set_solo_open":
        settings["solo_open"] = bool(msg.get("open"))
        save_settings()


def drop(c):
    c.alive = False
    conns.discard(c)
    hosts.discard(c)
    if c.uid:
        cs = user_conns.get(c.uid)
        if cs:
            cs.discard(c)
            if not cs:
                del user_conns[c.uid]
        dirty["stats"] = True
    try:
        c.q.put_nowait(None)
    except asyncio.QueueFull:
        pass


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    c = Conn(ws, client_ip(ws.headers, ws.client))
    conns.add(c)
    writer = asyncio.create_task(c.writer())
    try:
        while c.alive:
            raw = await ws.receive_text()
            if len(raw) > 2000 or c.flooding():
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if isinstance(msg, dict):
                handle(c, msg)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("ws error:", repr(e), flush=True)
    finally:
        drop(c)
        writer.cancel()


# ---------------------------------------------------------------- accounts API

class RegisterBody(BaseModel):
    login: str
    password: str


class LoginBody(BaseModel):
    login: str = ""
    id: str = ""  # pages loaded before logins existed send the ID here
    password: str


LOGIN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_.\-]{3,20}")
RESERVED_LOGINS = {"ведущий", "host", "admin", "админ", "администратор", "moderator", "модератор", "иигра", "iigra"}


def login_key(login):
    """Case-insensitive form used for uniqueness: «Нейро», «нейро» and «НЕЙРО» are one login."""
    return login.casefold().replace("ё", "е")


def check_login(raw):
    login = unicodedata.normalize("NFC", str(raw)).strip()
    if not LOGIN_RE.fullmatch(login):
        raise HTTPException(400, "Логин: от 3 до 20 символов — буквы, цифры, точка, дефис или подчёркивание, без пробелов")
    if not re.search(r"[A-Za-zА-Яа-яЁё]", login):
        raise HTTPException(400, "В логине должна быть хотя бы одна буква")
    if BAD_WORDS.search(login) or login_key(login) in RESERVED_LOGINS:
        raise HTTPException(400, "Этот логин нельзя использовать — придумайте другой")
    return login


@app.post("/api/register")
async def register(body: RegisterBody, request: Request):
    if not register_limit.allow(client_ip(request.headers, request.client)):
        raise HTTPException(429, "Слишком много регистраций с вашего адреса — подождите минуту")
    login = check_login(body.login)
    key = login_key(login)
    taken = HTTPException(409, "Этот логин уже занят — придумайте другой")
    if db.login_taken(key):
        raise taken
    if not 6 <= len(body.password) <= 64:
        raise HTTPException(400, "Пароль должен быть от 6 до 64 символов")
    salt = secrets.token_bytes(16)
    pw = await asyncio.to_thread(hash_pw, body.password, salt)
    uid = new_uid()
    try:
        db.create_user(uid, login, key, salt, pw)
    except sqlite3.IntegrityError:  # someone took the same login while we were hashing
        raise taken
    players[uid] = {"name": login, "score": 0, "time": 0, "answered": 0, "streak": 0, "muted": False}
    dirty["stats"] = True
    return {"id": uid, "login": login, "token": new_session(uid), "live_open": live_open(), "live_opens_at": LIVE_OPENS_AT}


@app.post("/api/login")
async def login(body: LoginBody, request: Request):
    ip = client_ip(request.headers, request.client)
    who = unicodedata.normalize("NFC", (body.login or body.id)).strip()
    if re.fullmatch(r"[\d ]{6,8}", who):  # a 6-digit ID, possibly typed as «123 456»
        who = re.sub(r"\D", "", who)
        row = db.user_secret(who)
    else:
        who = login_key(who)
        row = db.user_secret_by_login(who)
    if login_fail_ip.blocked(ip) or login_fail_id.blocked(who):
        raise HTTPException(429, "Слишком много неудачных попыток — подождите 10 минут")
    pw = await asyncio.to_thread(hash_pw, body.password, row["salt"] if row else b"x" * 16)
    if not row or not hmac.compare_digest(pw, row["pw"]):
        login_fail_ip.add(ip)
        login_fail_id.add(who)
        raise HTTPException(401, "Неверный логин или пароль")
    return {"id": row["id"], "login": row["login"], "token": new_session(row["id"])}


@app.post("/api/logout")
async def logout(authorization: str | None = Header(None)):
    token = (authorization or "").removeprefix("Bearer ").strip()
    sessions.pop(token, None)
    db.delete_session(token)
    return {"ok": True}


# ---------------------------------------------------------------- self-paced variants API

TOTAL_SOLO = N_CATS * N_LEVELS


class SoloBody(BaseModel):
    variant: int


class SoloTileBody(SoloBody):
    ci: int
    li: int


class SoloAnswerBody(SoloTileBody):
    option: int


def variant_label(v):
    return "ML" if v == ML_INDEX else v + 1


def check_solo(variant):
    if not settings["solo_open"]:
        raise HTTPException(403, "Самостоятельные варианты откроются после трансляции")
    if not 0 <= variant < len(VARIANTS):
        raise HTTPException(400, "Нет такого варианта")


def attempt_summary(v, row):
    answers = json.loads(row["answers"]) if row else {}
    return {
        "variant": v,
        "label": variant_label(v),
        "status": "done" if row and row["code"] else ("progress" if row else "new"),
        "answered": len(answers),
        "score": row["score"] if row else 0,
        "code": row["code"] if row else None,
    }


@app.get("/api/me")
async def api_me(authorization: str | None = Header(None)):
    uid = uid_from_auth(authorization)
    rows = {r["variant"]: r for r in db.attempts_for(uid)}
    return {
        "id": uid,
        "login": players[uid]["name"],
        "solo_open": settings["solo_open"],
        "live_open": live_open(),
        "live_opens_at": LIVE_OPENS_AT,
        "total": TOTAL_SOLO,
        "variants": [attempt_summary(v, rows.get(v)) for v in range(len(SETS))],
        "ml": attempt_summary(ML_INDEX, rows[ML_INDEX]) if ML_INDEX in rows else None,
        "has_ml": ML_INDEX >= 0,
    }


@app.post("/api/solo/start")
async def solo_start(body: SoloBody, authorization: str | None = Header(None)):
    uid = uid_from_auth(authorization)
    check_solo(body.variant)
    db.create_attempt(uid, body.variant)
    a = db.get_attempt(uid, body.variant)
    return {
        "variant": body.variant,
        "label": variant_label(body.variant),
        "hard": body.variant == ML_INDEX,
        "categories": [c["name"] for c in VARIANTS[body.variant]],
        "points": POINTS,
        "used": {k: v["correct"] for k, v in a["answers"].items()},
        "score": a["score"],
        "total": TOTAL_SOLO,
        "code": a["code"],
    }


@app.post("/api/solo/open")
async def solo_open(body: SoloTileBody, authorization: str | None = Header(None)):
    uid = uid_from_auth(authorization)
    check_solo(body.variant)
    if not valid_tile(body.ci, body.li):
        raise HTTPException(400, "Нет такой клетки")
    a = db.get_attempt(uid, body.variant)
    if not a:
        raise HTTPException(404, "Сначала начните вариант")
    board = VARIANTS[body.variant]
    payload = public_question(board, body.ci, body.li)
    prior = a["answers"].get(tile_key(body.ci, body.li))
    if prior:
        payload.update(already_answered=True, chosen=prior["option"], correct=prior["correct"],
                       correct_option=board[body.ci]["questions"][body.li]["correct"])
    else:
        payload["already_answered"] = False
    return payload


@app.post("/api/solo/answer")
async def solo_answer(body: SoloAnswerBody, authorization: str | None = Header(None)):
    uid = uid_from_auth(authorization)
    check_solo(body.variant)
    if not valid_tile(body.ci, body.li) or not 0 <= body.option <= 3:
        raise HTTPException(400, "Некорректный ответ")
    a = db.get_attempt(uid, body.variant)
    if not a:
        raise HTTPException(404, "Сначала начните вариант")
    if a["code"]:
        raise HTTPException(409, "Вариант уже пройден")
    q = VARIANTS[body.variant][body.ci]["questions"][body.li]
    key = tile_key(body.ci, body.li)
    if key not in a["answers"]:
        ok = body.option == q["correct"]
        a["answers"][key] = {"option": body.option, "correct": ok}
        if ok:
            a["score"] += q["points"]
        db.save_attempt(uid, body.variant, a["answers"], a["score"])
    return {
        "correct": a["answers"][key]["correct"],
        "correct_option": q["correct"],
        "score": a["score"],
        "answered": len(a["answers"]),
        "total": TOTAL_SOLO,
    }


CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def gen_code():
    while True:
        code = "-".join("".join(secrets.choice(CODE_ALPHABET) for _ in range(4)) for _ in range(2))
        if not db.code_exists(code):
            return code


@app.post("/api/solo/finish")
async def solo_finish(body: SoloBody, authorization: str | None = Header(None)):
    uid = uid_from_auth(authorization)
    check_solo(body.variant)
    a = db.get_attempt(uid, body.variant)
    if not a:
        raise HTTPException(404, "Сначала начните вариант")
    if not a["code"]:
        if len(a["answers"]) < TOTAL_SOLO:
            raise HTTPException(409, f"Отвечено {len(a['answers'])} из {TOTAL_SOLO}")
        a["code"] = gen_code()
        a["completed"] = db.complete_attempt(uid, body.variant, a["code"], a["score"])
    return {"code": a["code"], "score": a["score"], "label": variant_label(body.variant), "completed": a["completed"]}


# ---------------------------------------------------------------- organisers

class VerifyBody(BaseModel):
    key: str
    code: str


def require_host_key(key, request):
    ip = client_ip(request.headers, request.client)
    if host_fail_ip.blocked(ip):
        raise HTTPException(429, "Слишком много попыток — подождите 10 минут")
    if not host_key_ok(key):
        host_fail_ip.add(ip)
        raise HTTPException(403, "Неверный ключ ведущего")


@app.post("/api/verify")
async def verify_code(body: VerifyBody, request: Request):
    require_host_key(body.key, request)
    if not verify_limit.allow(client_ip(request.headers, request.client)):
        raise HTTPException(429, "Слишком много проверок — подождите минуту")
    code = re.sub(r"[^A-Z0-9]", "", body.code.upper())
    if len(code) == 8:
        code = code[:4] + "-" + code[4:]
    row = db.find_code(code)
    if not row:
        return {"valid": False}
    return {
        "valid": True,
        "code": row["code"],
        "id": row["user_id"],
        "login": (players.get(row["user_id"]) or {}).get("name"),
        "variant": variant_label(row["variant"]),
        "score": row["score"],
        "completed_at": row["completed"],
        "codes_total": db.codes_count_for(row["user_id"]),
    }


def csv_response(filename, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return Response(
        "﻿" + buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


MSK = timezone(timedelta(hours=3))  # Russia has no DST; Render servers run in UTC


def iso(ts):
    return datetime.fromtimestamp(ts, MSK).strftime("%Y-%m-%d %H:%M:%S")


@app.get("/api/admin/leaderboard.csv")
async def export_leaderboard(request: Request, x_host_key: str | None = Header(None)):
    require_host_key(unquote(x_host_key or ""), request)  # header is URL-encoded: keys may be Cyrillic
    rows = [(i + 1, uid, p["name"] or "", p["score"], p["time"], p["answered"]) for i, (uid, p) in enumerate(ranked)]
    return csv_response("iigra-leaderboard.csv", ["Место", "ID", "Логин", "Очки", "Время верных ответов, с", "Ответов"], rows)


@app.get("/api/admin/codes.csv")
async def export_codes(request: Request, x_host_key: str | None = Header(None)):
    require_host_key(unquote(x_host_key or ""), request)  # header is URL-encoded: keys may be Cyrillic
    rows = [
        (r["code"], r["user_id"], (players.get(r["user_id"]) or {}).get("name") or "", variant_label(r["variant"]), r["score"], iso(r["completed"]))
        for r in db.all_codes()
    ]
    return csv_response("iigra-codes.csv", ["Код", "ID", "Логин", "Вариант", "Очки", "Пройдено"], rows)


# ---------------------------------------------------------------- pages

@app.get("/api/board")
def api_board():
    return {"categories": [c["name"] for c in LIVE_BOARD], "points": POINTS, "question_seconds": QUESTION_SECONDS}


@app.get("/qr")
def qr_code(request: Request, data: str | None = None):
    # Without ?data= the code leads to this server's player page, so /qr itself can go on a poster.
    img = qrcode.make((data or str(request.base_url))[:300], border=2)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png")


@app.get("/")
def app_page():
    return FileResponse(BASE_DIR / "public" / "app.html")


@app.get("/play")
@app.get("/solo")
def old_links():
    return RedirectResponse("/")


@app.get("/host")
def host_page():
    return FileResponse(BASE_DIR / "public" / "host.html")


@app.get("/screen")
def screen_page():
    return FileResponse(BASE_DIR / "public" / "screen.html")


@app.get("/verify")
def verify_page():
    return FileResponse(BASE_DIR / "public" / "verify.html")


app.mount("/static", StaticFiles(directory=BASE_DIR / "public"), name="static")
