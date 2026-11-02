"""
Load test: N simulated players register, connect, vote, answer and chat.

    python tools/load_test.py http://127.0.0.1:8765 HOSTKEY 1000

Measures how long it takes for every client to receive the vote result,
the question, and their personal result after the reveal.
"""
import asyncio
import json
import random
import sys
import time
from collections import Counter

import subprocess

import aiohttp

BASE = sys.argv[1]
KEY = sys.argv[2]
N = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
PID = sys.argv[4] if len(sys.argv) > 4 else None


def server_cpu():
    if not PID:
        return 0.0
    out = subprocess.run(["ps", "-o", "time=", "-p", PID], capture_output=True, text=True).stdout.strip()
    m, sec = out.split(":")
    return int(m) * 60 + float(sec)
WS = BASE.replace("http", "ws", 1) + "/ws"


class Player:
    def __init__(self, session, token):
        self.session, self.token = session, token
        self.events = {}
        self.ws = self.error = None

    async def run(self, ready, gate):
        # Over the internet one laptop can't open 1000 TLS sockets at once: connect in batches, retry, count failures.
        async with gate:
            for attempt in range(3):
                try:
                    self.ws = await self.session.ws_connect(WS, heartbeat=25, timeout=aiohttp.ClientWSTimeout(ws_close=10))
                    await self.ws.send_str(json.dumps({"type": "auth", "token": self.token}))
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                    self.ws, self.error = None, type(e).__name__ + ": " + str(e)[:80]
                    await asyncio.sleep(1 + attempt)
        ready.release()
        if self.ws is None:
            return
        async for msg in self.ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                break
            m = json.loads(msg.data)
            t = m["type"]
            self.events.setdefault(t, time.perf_counter())
            if t == "vote_started":
                asyncio.create_task(self.later(random.uniform(0, 3), {"type": "vote", "ci": random.randrange(5), "li": random.randrange(5)}))
            elif t == "question_opened":
                asyncio.create_task(self.later(random.uniform(0.5, 6), {"type": "answer", "option": random.randrange(4)}))

    async def later(self, delay, msg):
        await asyncio.sleep(delay)
        await self.ws.send_str(json.dumps(msg))


def spread(players, event, t0):
    ts = [p.events[event] - t0 for p in players if event in p.events]
    if not ts:
        return f"{event}: никто не получил"
    ts.sort()
    return f"{event}: получили {len(ts)}/{len(players)}, медиана {ts[len(ts)//2]*1000:.0f} мс, 99% {ts[int(len(ts)*0.99)-1]*1000:.0f} мс, макс {ts[-1]*1000:.0f} мс"


async def main():
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=conn) as session:
        t = time.perf_counter()
        sem = asyncio.Semaphore(50)

        async def reg(i):
            async with sem:
                async with session.post(BASE + "/api/register", json={"password": f"loadtest{i}"}) as r:
                    return (await r.json())["token"]

        tokens = await asyncio.gather(*[reg(i) for i in range(N)])
        print(f"регистрация {N}: {time.perf_counter() - t:.1f} с, CPU сервера {server_cpu():.1f} с")
        cpu0 = server_cpu()

        host = await session.ws_connect(WS, heartbeat=25)
        await host.send_str(json.dumps({"type": "host_auth", "key": KEY}))

        async def host_wait(mtype):
            async for msg in host:
                if json.loads(msg.data)["type"] == mtype:
                    return

        await host_wait("sync")
        # Keep draining the host socket, otherwise the server drops it as a slow client.
        asyncio.create_task(host_wait("never"))
        await host.send_str(json.dumps({"type": "reset_game"}))
        await host.send_str(json.dumps({"type": "chat_settings", "enabled": True, "slow": 3}))

        players = [Player(session, tok) for tok in tokens]
        ready = asyncio.Semaphore(0)
        gate = asyncio.Semaphore(100)
        t = time.perf_counter()
        tasks = [asyncio.create_task(p.run(ready, gate)) for p in players]
        for _ in players:
            await ready.acquire()
        await asyncio.sleep(2)
        failed = [p.error for p in players if p.ws is None]
        players = [p for p in players if p.ws is not None]
        print(f"подключение {N}: {time.perf_counter() - t:.1f} с, не подключились {len(failed)}, sync получили {sum('sync' in p.events for p in players)}")
        for err, n in Counter(failed).most_common(3):
            print(f"  {n} × {err}")

        for round_no in range(2):
            for p in players:
                p.events.clear()
            t0 = time.perf_counter()
            await host.send_str(json.dumps({"type": "start_vote"}))
            await asyncio.sleep(4)
            await host.send_str(json.dumps({"type": "end_vote"}))
            await asyncio.sleep(1)
            print(spread(players, "vote_started", t0))
            print(spread(players, "vote_tallies", t0))

            t1 = time.perf_counter()
            await host.send_str(json.dumps({"type": "open_question", "ci": round_no, "li": round_no}))
            # chat storm while people answer
            for p in random.sample(players, 200):
                await p.ws.send_str(json.dumps({"type": "chat", "text": f"сообщение от {id(p) % 1000}"}))
            await asyncio.sleep(8)
            print(spread(players, "question_opened", t1))
            print(spread(players, "answer_ack", t1))
            print(spread(players, "chat", t1))
            t2 = time.perf_counter()
            await host.send_str(json.dumps({"type": "reveal"}))
            await asyncio.sleep(3)
            print(spread(players, "revealed", t2))
            print(spread(players, "me", t2))
            wall = time.perf_counter() - t0
            used = server_cpu() - cpu0
            print(f"--- раунд {round_no + 1}: {wall:.1f} с, CPU сервера {used:.2f} с ({used / wall * 100:.0f}% одного ядра)")
            cpu0 = server_cpu()

        await host.send_str(json.dumps({"type": "reset_game"}))
        await asyncio.sleep(1)
        for p in players:
            await p.ws.close()
        for task in tasks:
            task.cancel()


asyncio.run(main())
