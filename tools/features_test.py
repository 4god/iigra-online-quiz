"""Game mechanics test: voting by rows, answer streaks, the final wager, fastest answer and facts.

    python tools/features_test.py http://127.0.0.1:8765 HOSTKEY [questions.csv]

Resets the game (scores to zero) — run it on a test server or before the broadcast, never during one.
The server must have LIVE_OPENS_AT in the past. Correct answers are taken from live_questions.csv
(or from variant 1 of questions.csv if there is no broadcast file), the same way the server picks its board.
"""
import asyncio, json, os, sys, urllib.request
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import websockets
from questions import SETS, load_single_board, read_text

LIVE_FILE = sys.argv[3] if len(sys.argv) > 3 else os.path.join(ROOT, "live_questions.csv")
BOARD = load_single_board(read_text(LIVE_FILE), LIVE_FILE) if os.path.exists(LIVE_FILE) else SETS[0]
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8765"
KEY = sys.argv[2] if len(sys.argv) > 2 else "test123"
WS = BASE.replace("http", "ws", 1) + "/ws"
RUN = os.urandom(3).hex()
def reg(login):
    r = urllib.request.Request(BASE + "/api/register", data=json.dumps({"login": login, "password": "secret123"}).encode(), headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(r).read())
def correct(ci, li): return BOARD[ci]["questions"][li]["correct"]

async def until(ws, t, timeout=12):
    while True:
        m = json.loads(await asyncio.wait_for(ws.recv(), timeout))
        if m["type"] == t: return m

async def main():
    a, b, c = reg(f"streaker_{RUN}"), reg(f"wrongo_{RUN}"), reg(f"sleepy_{RUN}")
    host = await websockets.connect(WS); await host.send(json.dumps({"type": "host_auth", "key": KEY})); await until(host, "sync")
    await host.send(json.dumps({"type": "reset_game"})); await until(host, "sync")
    ws = {}
    for u in (a, b, c):
        w = await websockets.connect(WS); await w.send(json.dumps({"type": "auth", "token": u["token"]})); await until(w, "sync"); ws[u["login"].split("_")[0]] = w
    # row voting: auto -> row 0; votes outside the row are ignored
    await host.send(json.dumps({"type": "start_vote", "level": "auto"}))
    vs = await until(ws["streaker"], "vote_started"); assert vs["level"] == 0, vs
    await ws["streaker"].send(json.dumps({"type": "vote", "ci": 0, "li": 3}))
    await ws["wrongo"].send(json.dumps({"type": "vote", "ci": 2, "li": 0}))
    await asyncio.sleep(1.2)
    await host.send(json.dumps({"type": "end_vote"})); ve = await until(host, "vote_ended")
    assert ve["total"] == 1 and ve["winner"] == "2-0", ve
    print("row voting ok:", ve["winner"], ve["tallies"])
    # 4 correct answers in a row for streaker; wrongo always wrong; sleepy skips
    plan = [(2, 0), (0, 0), (1, 0), (3, 0)]
    expect = [100, 100, 110, 120]
    for i, (ci, li) in enumerate(plan):
        await host.send(json.dumps({"type": "open_question", "ci": ci, "li": li}))
        await until(ws["streaker"], "question_opened")
        k = correct(ci, li)
        await ws["streaker"].send(json.dumps({"type": "answer", "option": k}))
        await ws["wrongo"].send(json.dumps({"type": "answer", "option": (k + 1) % 4}))
        await asyncio.sleep(0.4)
        await host.send(json.dumps({"type": "reveal"}))
        rv = await until(ws["streaker"], "revealed")
        me = await until(ws["streaker"], "me")
        assert me["result"]["delta"] == expect[i], (i, me["result"])
        assert rv["fastest"]["name"] == a["login"], rv["fastest"]
        assert rv["fact"] or len(sys.argv) > 3 or not os.path.exists(LIVE_FILE), "broadcast questions should have facts"
        print(f"q{i+1}: delta {me['result']['delta']} mult {me['result']['mult']} streak {me['result']['streak']} | fastest {rv['fastest']} | top moves {[(t['name'], t['move'], t['streak']) for t in rv['top']]}")
    assert me["score"] == 430 and me["streak"] == 4, me
    # final: streaker bets 300 of 430 and is right, wrongo bets 500 (cap = 500 as he has 0) and is wrong, sleepy bets but does not answer
    await host.send(json.dumps({"type": "start_final"}))
    fs = await until(ws["streaker"], "final_started")
    assert fs["points"] == 500, fs
    await ws["streaker"].send(json.dumps({"type": "wager", "amount": 300}))
    await ws["wrongo"].send(json.dumps({"type": "wager", "amount": 99999}))
    await ws["sleepy"].send(json.dumps({"type": "wager", "amount": 50}))
    ack = await until(ws["wrongo"], "wager_ack"); assert ack["amount"] == 500, ack
    await asyncio.sleep(1)
    await host.send(json.dumps({"type": "open_final"}))
    q = await until(ws["streaker"], "question_opened"); assert q["data"]["final"], q
    k = correct(q["data"]["ci"], q["data"]["li"])
    await ws["streaker"].send(json.dumps({"type": "answer", "option": k}))
    await ws["wrongo"].send(json.dumps({"type": "answer", "option": (k + 1) % 4}))
    await asyncio.sleep(0.4)
    await host.send(json.dumps({"type": "reveal"}))
    rv = await until(ws["streaker"], "revealed"); assert rv["final"] and rv["last"], rv
    me = await until(ws["streaker"], "me"); assert me["result"]["delta"] == 300 and me["score"] == 730, me
    me_w = await until(ws["wrongo"], "me"); assert me_w["result"]["delta"] == 0 and me_w["score"] == 0, me_w
    print("final ok: streaker", me["score"], "| wrongo", me_w["score"], me_w["result"], "| won/lost", rv["wager_won"], rv["wager_lost"])
    fin = await until(ws["streaker"], "finished", timeout=15)
    print("finished:", [(t["name"], t["score"]) for t in fin["top"]])
    await host.send(json.dumps({"type": "reset_game"}))
    print("ALL FEATURES OK")
asyncio.run(main())
