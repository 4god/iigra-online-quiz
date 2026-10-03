"""
ИИгра онлайн — мультиплеерная "Своя игра" про ИИ.

Ведущий открывает вопросы на экране трансляции (/host), зрители отвечают
со своих телефонов (/play). Очки получает каждый, кто ответил верно —
без авторизации, без аккаунтов, только по ссылке/QR.
"""

import asyncio
import io
import json
import os
import secrets
import time
import uuid
from pathlib import Path

import qrcode
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from questions import BOARD, QUESTION_SECONDS as DEFAULT_QUESTION_SECONDS

QUESTION_SECONDS = int(os.environ.get("QUESTION_SECONDS", DEFAULT_QUESTION_SECONDS))

BASE_DIR = Path(__file__).parent
STATE_FILE = BASE_DIR / "game_state.json"

HOST_KEY = os.environ.get("HOST_KEY") or secrets.token_hex(3)  # e.g. "a1b2c3"
print("=" * 50)
print(f"  КЛЮЧ ВЕДУЩЕГО: {HOST_KEY}")
print("  Введите его на странице /host, чтобы управлять игрой.")
print("=" * 50)

app = FastAPI()


# ---------------------------------------------------------------- game state

def fresh_state():
    return {
        "phase": "idle",  # idle | question | locked | revealed
        "current": None,  # {ci, li, points, deadline, answers: {player_id: option}}
        "used": {},  # "ci-li": True
        "players": {},  # player_id: {"name": str, "score": int}
        "generation": 0,  # bumps each open/cancel/reveal to invalidate stale timers
    }


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            s = fresh_state()
            s["used"] = saved.get("used", {})
            s["players"] = saved.get("players", {})
            return s
        except Exception:
            pass
    return fresh_state()


state = load_state()


def persist():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"used": state["used"], "players": state["players"]}, f, ensure_ascii=False)
    except Exception as e:
        print("persist failed:", e)


def leaderboard(limit=15):
    rows = [
        {"player_id": pid, "name": p["name"], "score": p["score"]}
        for pid, p in state["players"].items()
    ]
    rows.sort(key=lambda r: r["score"], reverse=True)
    return rows[:limit]


def question_data(ci, li):
    cat = BOARD[ci]
    q = cat["questions"][li]
    return cat, q


def public_current():
    """Current question state without the answer, safe to send to players."""
    if not state["current"]:
        return None
    c = state["current"]
    cat, q = question_data(c["ci"], c["li"])
    return {
        "ci": c["ci"],
        "li": c["li"],
        "category": cat["name"],
        "points": q["points"],
        "q": q["q"],
        "options": q["options"],
        "deadline": c["deadline"],
        "locked": state["phase"] == "locked",
    }


# ---------------------------------------------------------------- connections

class Hub:
    def __init__(self):
        self.sockets: set[WebSocket] = set()
        self.by_player: dict[str, WebSocket] = {}
        self.socket_player: dict[WebSocket, str] = {}
        self.hosts: set[WebSocket] = set()

    async def register(self, ws: WebSocket):
        await ws.accept()
        self.sockets.add(ws)

    def drop(self, ws: WebSocket):
        self.sockets.discard(ws)
        self.hosts.discard(ws)
        pid = self.socket_player.pop(ws, None)
        if pid and self.by_player.get(pid) is ws:
            del self.by_player[pid]

    def bind_player(self, ws: WebSocket, pid: str):
        self.socket_player[ws] = pid
        self.by_player[pid] = ws

    async def send(self, ws: WebSocket, payload: dict):
        try:
            await ws.send_json(payload)
        except Exception:
            pass

    async def broadcast(self, payload: dict, skip: WebSocket | None = None):
        dead = []
        for ws in list(self.sockets):
            if ws is skip:
                continue
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.drop(ws)

    async def broadcast_player_count(self):
        await self.broadcast({"type": "player_count", "count": len(state["players"])})

    async def broadcast_answer_count(self):
        n = len(state["current"]["answers"]) if state["current"] else 0
        await self.broadcast({"type": "answer_count", "count": n})

    async def broadcast_leaderboard(self):
        await self.broadcast({"type": "leaderboard", "rows": leaderboard()})


hub = Hub()


# ---------------------------------------------------------------- game logic

async def sync_one(ws: WebSocket, player_id: str | None):
    you = None
    if player_id and player_id in state["players"]:
        p = state["players"][player_id]
        you = {"player_id": player_id, "name": p["name"], "score": p["score"]}
    await hub.send(ws, {
        "type": "sync",
        "phase": state["phase"],
        "used": state["used"],
        "current": public_current(),
        "leaderboard": leaderboard(),
        "player_count": len(state["players"]),
        "you": you,
    })


async def run_timer(generation: int, deadline: float):
    delay = deadline - time.time()
    if delay > 0:
        await asyncio.sleep(delay)
    if state["generation"] != generation:
        return  # question already cancelled/revealed
    if state["phase"] != "question":
        return
    state["phase"] = "locked"
    await hub.broadcast({"type": "time_up"})


async def open_question(ci: int, li: int):
    if not (0 <= ci < len(BOARD)) or not (0 <= li < len(BOARD[0]["questions"])):
        return
    key = f"{ci}-{li}"
    if state["used"].get(key):
        return
    if state["phase"] not in ("idle", "revealed"):
        return
    state["generation"] += 1
    deadline = time.time() + QUESTION_SECONDS
    state["current"] = {"ci": ci, "li": li, "answers": {}, "deadline": deadline}
    state["phase"] = "question"
    await hub.broadcast({"type": "question_opened", "data": public_current()})
    asyncio.create_task(run_timer(state["generation"], deadline))


async def cancel_question():
    if not state["current"]:
        return
    state["generation"] += 1
    state["current"] = None
    state["phase"] = "idle"
    await hub.broadcast({"type": "cancelled"})


async def reveal_question():
    if not state["current"]:
        return
    state["generation"] += 1
    c = state["current"]
    cat, q = question_data(c["ci"], c["li"])
    correct_idx = q["correct"]
    key = f"{c['ci']}-{c['li']}"
    state["used"][key] = True

    correct_count = 0
    results = {}  # player_id -> bool correct
    for pid, opt in c["answers"].items():
        ok = (opt == correct_idx)
        results[pid] = ok
        if ok:
            correct_count += 1
            if pid in state["players"]:
                state["players"][pid]["score"] += q["points"]

    state["phase"] = "revealed"
    state["current"] = None
    persist()

    full_rows = leaderboard(limit=max(len(state["players"]), 1))
    board_rows = full_rows[:15]
    await hub.broadcast({
        "type": "revealed",
        "ci": c["ci"],
        "li": c["li"],
        "category": cat["name"],
        "points": q["points"],
        "q": q["q"],
        "options": q["options"],
        "correct": correct_idx,
        "answered_count": len(c["answers"]),
        "correct_count": correct_count,
        "used": state["used"],
        "leaderboard": board_rows,
    })

    # per-player individual result + rank (computed over ALL players, not just the top-15 shown on screen)
    for pid, ok in results.items():
        ws = hub.by_player.get(pid)
        if not ws:
            continue
        p = state["players"].get(pid)
        if not p:
            continue
        rank = next((i + 1 for i, r in enumerate(full_rows) if r["player_id"] == pid), None)
        await hub.send(ws, {
            "type": "your_result",
            "correct": ok,
            "points_earned": q["points"] if ok else 0,
            "score": p["score"],
            "rank": rank,
        })


async def reset_game():
    state["generation"] += 1
    state["phase"] = "idle"
    state["current"] = None
    state["used"] = {}
    for p in state["players"].values():
        p["score"] = 0
    persist()
    await hub.broadcast({
        "type": "sync",
        "phase": state["phase"],
        "used": state["used"],
        "current": None,
        "leaderboard": leaderboard(),
        "player_count": len(state["players"]),
        "you": None,
    })


# ---------------------------------------------------------------- solo mode
#
# Self-paced, single-player version for anyone to play after the broadcast:
# no timer pressure, scoring is still checked server-side so a finished run
# is trustworthy, and completing all 25 questions mints a short code that
# staff can look up later (via /verify) to confirm someone played it through.

SOLO_FILE = BASE_DIR / "solo_state.json"
TOTAL_QUESTIONS = sum(len(c["questions"]) for c in BOARD)
CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O/1/I — easier to read aloud


def fresh_solo():
    return {"attempts": {}, "completions": {}}


def load_solo():
    if SOLO_FILE.exists():
        try:
            with open(SOLO_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "attempts" in data and "completions" in data:
                return data
        except Exception:
            pass
    return fresh_solo()


solo_db = load_solo()


def persist_solo():
    try:
        with open(SOLO_FILE, "w", encoding="utf-8") as f:
            json.dump(solo_db, f, ensure_ascii=False)
    except Exception as e:
        print("persist_solo failed:", e)


def gen_code():
    while True:
        code = "-".join("".join(secrets.choice(CODE_ALPHABET) for _ in range(4)) for _ in range(2))
        if code not in solo_db["completions"]:
            return code


class SoloStartBody(BaseModel):
    name: str


class SoloOpenBody(BaseModel):
    attempt_id: str
    ci: int
    li: int


class SoloAnswerBody(BaseModel):
    attempt_id: str
    ci: int
    li: int
    option: int


class SoloFinishBody(BaseModel):
    attempt_id: str


class VerifyBody(BaseModel):
    key: str
    code: str


def get_attempt(attempt_id: str):
    attempt = solo_db["attempts"].get(attempt_id)
    if not attempt:
        raise HTTPException(404, "attempt not found")
    return attempt


@app.post("/api/solo/start")
def solo_start(body: SoloStartBody):
    name = (body.name or "Игрок").strip()[:24] or "Игрок"
    attempt_id = uuid.uuid4().hex
    solo_db["attempts"][attempt_id] = {
        "name": name,
        "answers": {},
        "score": 0,
        "started_at": time.time(),
        "code": None,
        "completed_at": None,
    }
    persist_solo()
    return {"attempt_id": attempt_id, "name": name, "total": TOTAL_QUESTIONS}


@app.get("/api/solo/status")
def solo_status(attempt_id: str):
    a = get_attempt(attempt_id)
    return {
        "name": a["name"],
        "score": a["score"],
        "used": {k: v["correct"] for k, v in a["answers"].items()},
        "total": TOTAL_QUESTIONS,
        "completed": a["code"] is not None,
        "code": a["code"],
    }


@app.post("/api/solo/open")
def solo_open(body: SoloOpenBody):
    a = get_attempt(body.attempt_id)
    if not (0 <= body.ci < len(BOARD)) or not (0 <= body.li < len(BOARD[0]["questions"])):
        raise HTTPException(400, "bad tile")
    cat, q = question_data(body.ci, body.li)
    key = f"{body.ci}-{body.li}"
    prior = a["answers"].get(key)
    payload = {
        "category": cat["name"],
        "points": q["points"],
        "q": q["q"],
        "options": q["options"],
    }
    if prior:
        payload.update({
            "already_answered": True,
            "chosen": prior["option"],
            "correct": prior["correct"],
            "correct_option": q["correct"],
        })
    else:
        payload["already_answered"] = False
    return payload


@app.post("/api/solo/answer")
def solo_answer(body: SoloAnswerBody):
    a = get_attempt(body.attempt_id)
    if not (0 <= body.ci < len(BOARD)) or not (0 <= body.li < len(BOARD[0]["questions"])):
        raise HTTPException(400, "bad tile")
    cat, q = question_data(body.ci, body.li)
    key = f"{body.ci}-{body.li}"

    existing = a["answers"].get(key)
    if existing:
        return {
            "correct": existing["correct"],
            "correct_option": q["correct"],
            "score": a["score"],
            "answered_count": len(a["answers"]),
            "total": TOTAL_QUESTIONS,
        }

    if not (0 <= body.option <= 3):
        raise HTTPException(400, "bad option")

    is_correct = body.option == q["correct"]
    a["answers"][key] = {"option": body.option, "correct": is_correct}
    if is_correct:
        a["score"] += q["points"]
    persist_solo()

    return {
        "correct": is_correct,
        "correct_option": q["correct"],
        "score": a["score"],
        "answered_count": len(a["answers"]),
        "total": TOTAL_QUESTIONS,
    }


@app.post("/api/solo/finish")
def solo_finish(body: SoloFinishBody):
    a = get_attempt(body.attempt_id)
    if a["code"]:
        return {"code": a["code"], "score": a["score"], "name": a["name"], "completed_at": a["completed_at"]}

    if len(a["answers"]) < TOTAL_QUESTIONS:
        raise HTTPException(409, f"not complete: {len(a['answers'])}/{TOTAL_QUESTIONS}")

    code = gen_code()
    completed_at = time.time()
    a["code"] = code
    a["completed_at"] = completed_at
    solo_db["completions"][code] = {
        "name": a["name"],
        "score": a["score"],
        "completed_at": completed_at,
    }
    persist_solo()

    return {"code": code, "score": a["score"], "name": a["name"], "completed_at": completed_at}


@app.post("/api/verify")
def verify_code(body: VerifyBody):
    if body.key != HOST_KEY:
        raise HTTPException(403, "bad key")
    record = solo_db["completions"].get(body.code.strip().upper())
    if not record:
        return {"valid": False}
    return {"valid": True, **record}


# ---------------------------------------------------------------- websocket

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await hub.register(ws)
    is_host = False
    try:
        await sync_one(ws, None)
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            mtype = msg.get("type")

            if mtype == "join":
                pid = msg.get("player_id") or str(uuid.uuid4())
                name = (msg.get("name") or "Игрок").strip()[:24] or "Игрок"
                if pid not in state["players"]:
                    state["players"][pid] = {"name": name, "score": 0}
                else:
                    state["players"][pid]["name"] = name
                hub.bind_player(ws, pid)
                persist()
                await hub.send(ws, {
                    "type": "joined",
                    "player_id": pid,
                    "score": state["players"][pid]["score"],
                })
                await sync_one(ws, pid)
                await hub.broadcast_player_count()

            elif mtype == "answer":
                pid = hub.socket_player.get(ws)
                if not pid or state["phase"] != "question" or not state["current"]:
                    continue
                if pid in state["current"]["answers"]:
                    continue
                opt = msg.get("option")
                if not isinstance(opt, int) or not (0 <= opt <= 3):
                    continue
                state["current"]["answers"][pid] = opt
                await hub.send(ws, {"type": "answer_ack"})
                await hub.broadcast_answer_count()

            elif mtype == "host_auth":
                if msg.get("key") == HOST_KEY:
                    is_host = True
                    hub.hosts.add(ws)
                    await hub.send(ws, {"type": "host_ok"})
                else:
                    await hub.send(ws, {"type": "error", "message": "Неверный ключ"})

            elif mtype == "open_question" and is_host:
                ci, li = msg.get("ci"), msg.get("li")
                if isinstance(ci, int) and isinstance(li, int):
                    await open_question(ci, li)

            elif mtype == "cancel_question" and is_host:
                await cancel_question()

            elif mtype == "reveal" and is_host:
                await reveal_question()

            elif mtype == "reset_game" and is_host:
                await reset_game()

    except WebSocketDisconnect:
        pass
    finally:
        hub.drop(ws)
        await hub.broadcast_player_count()


# ---------------------------------------------------------------- http routes

@app.get("/api/board")
def api_board():
    """Category names + point values only (no answers) — used to draw the host/board grid."""
    return {
        "categories": [c["name"] for c in BOARD],
        "points": [q["points"] for q in BOARD[0]["questions"]],
        "question_seconds": QUESTION_SECONDS,
    }


@app.get("/qr")
def qr_code(data: str):
    img = qrcode.make(data, border=2)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png")


@app.get("/host")
def host_page():
    return FileResponse(BASE_DIR / "public" / "host.html")


@app.get("/solo")
def solo_page():
    return FileResponse(BASE_DIR / "public" / "solo.html")


@app.get("/verify")
def verify_page():
    return FileResponse(BASE_DIR / "public" / "verify.html")


@app.get("/")
@app.get("/play")
def play_page():
    return FileResponse(BASE_DIR / "public" / "play.html")


app.mount("/static", StaticFiles(directory=BASE_DIR / "public"), name="static")
