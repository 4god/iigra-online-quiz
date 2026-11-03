"""End-to-end smoke test against a running server: python tools/e2e_test.py http://127.0.0.1:8765 HOSTKEY"""
import asyncio, json, os, sys, urllib.parse, urllib.request
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8765"
KEY = sys.argv[2] if len(sys.argv) > 2 else "test123"
WS = BASE.replace("http", "ws", 1) + "/ws"


def http(method, path, body=None, token=None, headers=None):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    h.update({k: urllib.parse.quote(v) for k, v in (headers or {}).items()})
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode() if body is not None else None, headers=h, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            data = r.read()
            return r.status, (json.loads(data) if r.headers.get_content_type() == "application/json" else data.decode("utf-8-sig"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


async def main():
    import websockets
    run = os.urandom(3).hex()  # logins are unique for good, so every run needs fresh ones
    st, bad = http("POST", "/api/register", {"login": f"Тест_{run}", "password": "123"})
    assert st == 400, bad
    for login in ("ab", "123456", "два слова", "admin"):
        st, bad = http("POST", "/api/register", {"login": login, "password": "secret0"})
        assert st == 400, (login, bad)
    users = [http("POST", "/api/register", {"login": f"Тест{i}_{run}", "password": f"secret{i}"})[1] for i in range(3)]
    print("registered:", [(u["id"], u["login"]) for u in users])
    st, dup = http("POST", "/api/register", {"login": f"тЕСТ0_{run}", "password": "secret9"})
    assert st == 409, dup
    st, wrong = http("POST", "/api/login", {"login": users[0]["login"], "password": "nope"})
    assert st == 401, wrong
    for who in (f"ТЕСТ0_{run}", users[0]["id"]):
        st, ok = http("POST", "/api/login", {"login": who, "password": "secret0"})
        assert st == 200 and ok["id"] == users[0]["id"] and ok["login"] == users[0]["login"], (who, ok)
    st, ok = http("POST", "/api/login", {"id": users[0]["id"], "password": "secret0"})  # old page format
    assert st == 200 and ok["id"] == users[0]["id"], ok

    async def recv_until(ws, mtype, timeout=5):
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
            if m["type"] == mtype:
                return m

    host = await websockets.connect(WS)
    await host.send(json.dumps({"type": "host_auth", "key": KEY}))
    await recv_until(host, "host_ok")
    hsync = await recv_until(host, "sync")
    print("host sync: live_unique =", hsync["live_unique"], "| categories:", hsync["categories"][:2])

    # broadcast screen: no account, read-only, sees what every player sees
    screen = await websockets.connect(WS)
    await screen.send(json.dumps({"type": "watch"}))
    ssync = await recv_until(screen, "sync")
    assert ssync["me"] is None and "muted" not in ssync and len(ssync["categories"]) == 5, ssync.keys()
    await screen.send(json.dumps({"type": "vote", "ci": 0, "li": 0}))  # spectators cannot act
    await screen.send(json.dumps({"type": "start_vote"}))
    print("screen sync: ok, phase =", ssync["phase"])
    await host.send(json.dumps({"type": "reset_game"}))
    await host.send(json.dumps({"type": "chat_settings", "enabled": True, "slow": 0}))

    pws = []
    for u in users:
        w = await websockets.connect(WS)
        await w.send(json.dumps({"type": "auth", "token": u["token"]}))
        s = await recv_until(w, "sync")
        assert s["me"]["id"] == u["id"] and s["me"]["streak"] == 0, s["me"]  # fresh accounts must sync before any reset
        pws.append(w)

    # vote: 2 votes for 0-0, 1 for 1-1
    await host.send(json.dumps({"type": "start_vote"}))
    for w in pws:
        await recv_until(w, "vote_started")
    for w, key in zip(pws, [(0, 0), (0, 0), (1, 1)]):
        await w.send(json.dumps({"type": "vote", "ci": key[0], "li": key[1]}))
        await recv_until(w, "vote_ack")
    t = await recv_until(pws[0], "vote_tallies")
    print("tallies:", t["tallies"])
    await host.send(json.dumps({"type": "end_vote"}))
    ended = await recv_until(pws[0], "vote_ended")
    assert ended["winner"] == "0-0", ended
    print("vote winner:", ended["winner"])

    # question
    await host.send(json.dumps({"type": "open_question", "ci": 0, "li": 0}))
    q = (await recv_until(pws[0], "question_opened"))["data"]
    print("question:", q["q"][:60], q["options"])
    hq = (await recv_until(host, "question_opened"))["data"]
    # we need the correct index: answer differently and check dist
    for w, opt in zip(pws, [0, 1, 2]):
        await w.send(json.dumps({"type": "answer", "option": opt}))
        ack = await recv_until(w, "answer_ack")
        assert ack["option"] == opt
    await pws[0].send(json.dumps({"type": "answer", "option": 3}))  # second answer must be ignored
    cnt = await recv_until(host, "answer_count")
    while cnt["count"] < 3:  # counts are batched, so an early batch may be partial
        cnt = await recv_until(host, "answer_count")
    assert cnt["count"] == 3, cnt
    await host.send(json.dumps({"type": "reveal"}))
    rev = await recv_until(pws[0], "revealed")
    print("revealed: correct =", rev["correct"], "dist =", rev["dist"], "top =", rev["top"])
    assert {u["login"] for u in users} <= {r["name"] for r in rev["top"]}, rev["top"]
    assert rev["dist"] == [1, 1, 1, 0]
    for i, w in enumerate(pws):
        me = await recv_until(w, "me")
        print("  player", i, "score", me["score"], "rank", me["rank"], "result", me["result"])

    # chat + moderation
    await pws[1].send(json.dumps({"type": "chat", "text": "Привет, бля, всем!"}))
    chat = await recv_until(pws[0], "chat")
    print("chat:", [m["text"] for m in chat["messages"]])
    assert all(m["name"] in {u["login"] for u in users} for m in chat["messages"]), chat["messages"]
    assert "***" in chat["messages"][0]["text"]
    await host.send(json.dumps({"type": "chat_mute", "uid": users[1]["id"], "muted": True, "purge": True}))
    d = await recv_until(pws[0], "chat_delete")
    print("purged ids:", d["ids"])
    await recv_until(pws[1], "muted")
    await pws[1].send(json.dumps({"type": "chat", "text": "а теперь?"}))
    n = await recv_until(pws[1], "notice")
    print("muted notice:", n["text"])

    # finish
    await host.send(json.dumps({"type": "finish"}))
    fin = await recv_until(pws[2], "finished")
    print("finished, participants:", fin["participants"])

    # self-paced: closed by default
    st, r = http("POST", "/api/solo/start", {"variant": 0}, users[0]["token"])
    assert st == 403, r
    await host.send(json.dumps({"type": "set_solo_open", "open": True}))
    await recv_until(pws[0], "settings")
    from questions import SETS
    st, s = http("POST", "/api/solo/start", {"variant": 3}, users[0]["token"])
    assert st == 200, s
    b = SETS[3]
    for ci in range(5):
        for li in range(5):
            http("POST", "/api/solo/open", {"variant": 3, "ci": ci, "li": li}, users[0]["token"])
            st, a = http("POST", "/api/solo/answer", {"variant": 3, "ci": ci, "li": li, "option": b[ci]["questions"][li]["correct"]}, users[0]["token"])
            assert st == 200 and a["correct"], a
    st, f = http("POST", "/api/solo/finish", {"variant": 3}, users[0]["token"])
    print("solo finish:", f)
    st, me = http("GET", "/api/me", None, users[0]["token"])
    print("me variants:", [(v["label"], v["status"]) for v in me["variants"]][:5])
    st, v = http("POST", "/api/verify", {"key": KEY, "code": f["code"].replace("-", "").lower()})
    print("verify:", v)
    assert v["valid"] and v["id"] == users[0]["id"]
    st, v2 = http("POST", "/api/verify", {"key": "wrong", "code": f["code"]})
    assert st == 403
    st, csvtext = http("GET", "/api/admin/codes.csv", None, None, {"X-Host-Key": KEY})
    print("codes.csv:", csvtext.strip().splitlines()[:2])
    st, lb = http("GET", "/api/admin/leaderboard.csv", None, None, {"X-Host-Key": KEY})
    print("leaderboard.csv:", lb.strip().splitlines()[:3])
    await host.send(json.dumps({"type": "set_solo_open", "open": False}))
    await host.send(json.dumps({"type": "chat_settings", "enabled": True, "slow": 5}))
    await host.send(json.dumps({"type": "reset_game"}))
    await asyncio.sleep(0.3)
    print("ALL OK")


asyncio.run(main())
