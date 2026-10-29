#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEGATA Security - Sohbet relay sunucusu (bağımlılıksız, yalnızca Python 3 stdlib).

Amaç: Kullanıcılar arası mesajları CANLI olarak iletmek. Mesajlar sunucuda KALICI
SAKLANMAZ; yalnızca anlık yönlendirilir. Alıcı çevrimdışıysa mesaj kısa süreli
(bellekte, en çok OFFLINE_MAX) tutulur ve bağlanınca iletilir, sonra silinir.

Protokol: her satır bir JSON nesnesi (newline ile ayrılır), UTF-8.
  İstemci -> Sunucu:
    {"type":"hello","id":"<telefon>"}            # kaydol (bu bağlantı = bu telefon)
    {"type":"msg","to":"<telefon>","from":"<telefon>","ts":<ms>,"mid":"<id>","body":"<sifreli>"}
    {"type":"ping"}
  Sunucu -> İstemci:
    {"type":"welcome"}
    {"type":"msg", ...}                           # gelen mesaj (yukarıdaki ile aynı)
    {"type":"ack","mid":"<id>","status":"delivered|queued"}
    {"type":"presence","id":"<telefon>","online":true|false}
    {"type":"pong"}

Kullanım:  python3 segata_relay.py [port]     (vars. 9696)
"""
import json, socket, threading, time, sys, collections

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9696
HOST = sys.argv[2] if len(sys.argv) > 2 else "0.0.0.0"
OFFLINE_MAX = 200          # çevrimdışı kullanıcı başına bellekte tutulacak en çok mesaj
OFFLINE_TTL = 7 * 24 * 3600  # saniye; bu süre sonra çevrimdışı kuyruk mesajı düşer

clients = {}               # phone -> conn (socket)
offline = collections.defaultdict(list)   # phone -> [(expire_ts, obj), ...]
lock = threading.Lock()


def send_line(conn, obj):
    try:
        conn.sendall((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
        return True
    except Exception:
        return False


def deliver_or_queue(to, obj):
    with lock:
        c = clients.get(to)
    if c is not None and send_line(c, obj):
        return "delivered"
    with lock:
        q = offline[to]
        q.append((time.time() + OFFLINE_TTL, obj))
        if len(q) > OFFLINE_MAX:
            del q[0:len(q) - OFFLINE_MAX]
    return "queued"


def flush_offline(phone, conn):
    now = time.time()
    with lock:
        q = offline.pop(phone, [])
    for exp, obj in q:
        if exp >= now:
            send_line(conn, obj)


def broadcast_presence(phone, online):
    note = {"type": "presence", "id": phone, "online": online}
    with lock:
        conns = list(clients.values())
    for c in conns:
        send_line(c, note)


def handle(conn, addr):
    phone = None
    buf = b""
    try:
        conn.settimeout(120)
        while True:
            try:
                data = conn.recv(4096)
            except socket.timeout:
                # inaktif; canlı mı diye ping
                if not send_line(conn, {"type": "ping"}):
                    break
                continue
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode("utf-8"))
                except Exception:
                    continue
                t = msg.get("type")
                if t == "hello":
                    phone = "".join(ch for ch in str(msg.get("id", "")) if ch.isdigit())
                    if not phone:
                        continue
                    with lock:
                        old = clients.get(phone)
                        clients[phone] = conn
                    if old is not None and old is not conn:
                        try: old.close()
                        except Exception: pass
                    send_line(conn, {"type": "welcome"})
                    flush_offline(phone, conn)
                    broadcast_presence(phone, True)
                elif t == "msg":
                    to = "".join(ch for ch in str(msg.get("to", "")) if ch.isdigit())
                    if not to:
                        continue
                    status = deliver_or_queue(to, msg)
                    mid = msg.get("mid")
                    if mid:
                        send_line(conn, {"type": "ack", "mid": mid, "status": status})
                elif t == "ping":
                    send_line(conn, {"type": "pong"})
                elif t == "who":
                    # çevrimiçi mi sorgusu
                    to = "".join(ch for ch in str(msg.get("id", "")) if ch.isdigit())
                    with lock:
                        on = to in clients
                    send_line(conn, {"type": "presence", "id": to, "online": on})
    except Exception:
        pass
    finally:
        if phone is not None:
            with lock:
                if clients.get(phone) is conn:
                    del clients[phone]
            broadcast_presence(phone, False)
        try: conn.close()
        except Exception: pass


def reaper():
    # süresi geçmiş çevrimdışı kuyrukları temizle
    while True:
        time.sleep(3600)
        now = time.time()
        with lock:
            for p in list(offline.keys()):
                offline[p] = [(e, o) for (e, o) in offline[p] if e >= now]
                if not offline[p]:
                    del offline[p]


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(200)
    print("SEGATA relay dinliyor: %s:%d" % (HOST, PORT), flush=True)
    threading.Thread(target=reaper, daemon=True).start()
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
