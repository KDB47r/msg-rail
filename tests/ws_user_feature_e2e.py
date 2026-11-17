#!/usr/bin/env python3
"""MsgRail 用户功能真实 WebSocket E2E 测试。

不依赖任何 WebSocket 第三方库：本脚本用 Python 标准库实现 RFC 6455
客户端（握手、掩码文本帧、ping/pong、分片），直接连接真实 Comet 9200。

前置条件：
  1. logic(9100/9101)、comet(9200)、Kafka、Redis 已启动；
  2. 三个测试账号存在，默认使用 api_user_feature_e2e.py 最近一轮创建的数据：
       A: e2e_a_46804654 / e2e-pass-46804654 (id=19)
       B: e2e_b_46804654 / e2e-pass-46804654 (id=20)
       C: e2e_c_46804654 / e2e-pass-46804654 (id=21)
     A、B 已是好友；C 与 A 不是好友。

可通过环境变量覆盖：
  WS_A_ACCOUNT / WS_A_PASSWORD / WS_B_ACCOUNT / ... / WS_ROOM_ID

脚本不删除任何数据：会新增单聊消息和聊天室消息。
"""

import base64
import hashlib
import json
import os
import socket
import struct
import time

import requests

LOGIC_BASE = os.environ.get("LOGIC_BASE", "http://127.0.0.1:9101")
WS_HOST = os.environ.get("WS_HOST", "127.0.0.1")
WS_PORT = int(os.environ.get("WS_PORT", "9200"))
ROOM_ID = int(os.environ.get("WS_ROOM_ID", "1"))

A_ACCOUNT = os.environ.get("WS_A_ACCOUNT", "e2e_a_46804654")
A_PASSWORD = os.environ.get("WS_A_PASSWORD", "e2e-pass-46804654")
B_ACCOUNT = os.environ.get("WS_B_ACCOUNT", "e2e_b_46804654")
B_PASSWORD = os.environ.get("WS_B_PASSWORD", "e2e-pass-46804654")
C_ACCOUNT = os.environ.get("WS_C_ACCOUNT", "e2e_c_46804654")
C_PASSWORD = os.environ.get("WS_C_PASSWORD", "e2e-pass-46804654")

HTTP_TIMEOUT = 15
WS_TIMEOUT = 8
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

PASS = 0
FAIL = 0
FAILED_NAMES = []


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print("[PASS] " + name)
        return True
    FAIL += 1
    FAILED_NAMES.append(name)
    print("[FAIL] " + name + ((" | " + str(detail)) if detail else ""))
    return False


class WSError(RuntimeError):
    pass


class WSClient:
    """最小 RFC 6455 客户端：只实现测试需要的文本帧与心跳。"""

    def __init__(self, token):
        self.sock = socket.create_connection((WS_HOST, WS_PORT),
                                             timeout=WS_TIMEOUT)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        path = "/ws?token=" + requests.utils.quote(token, safe="")
        request = (
            "GET %s HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: %s\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ) % (path, WS_HOST, WS_PORT, key)
        self.sock.sendall(request.encode("ascii"))

        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise WSError("handshake: connection closed")
            raw += chunk
            if len(raw) > 65536:
                raise WSError("handshake: response too large")

        head, self._buffer = raw.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        if not lines or " 101 " not in lines[0]:
            raise WSError("handshake failed: " + lines[0] if lines else "empty")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        expect = base64.b64encode(
            hashlib.sha1((key + GUID).encode("ascii")).digest()).decode("ascii")
        if headers.get("sec-websocket-accept") != expect:
            raise WSError("handshake: bad Sec-WebSocket-Accept")

        self._fragment = b""
        self._fragment_opcode = None
        # 同一条 WS 连接上可能先收到推送再收到 ack；保留全部消息，
        # 避免等待某一个类型时把另一个类型的消息丢掉。
        self.inbox = []

    def close(self):
        try:
            self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def send_json(self, obj):
        self._send_frame(0x1, json.dumps(obj, separators=(",", ":")).encode())

    def _send_frame(self, opcode, payload):
        first = 0x80 | (opcode & 0x0F)
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header = bytes([first, 0x80 | length])
        elif length < 65536:
            header = bytes([first, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([first, 0x80 | 127]) + struct.pack("!Q", length)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def _read_exact(self, n):
        while len(self._buffer) < n:
            chunk = self.sock.recv(max(4096, n - len(self._buffer)))
            if not chunk:
                raise WSError("connection closed while reading")
            self._buffer += chunk
        out, self._buffer = self._buffer[:n], self._buffer[n:]
        return out

    def _recv_frame(self, timeout):
        self.sock.settimeout(max(0.05, timeout))
        first, second = self._read_exact(2)
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else None
        payload = self._read_exact(length) if length else b""
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload

    def recv_json_until(self, predicate, timeout=WS_TIMEOUT):
        """读取到满足 predicate 的 JSON 为止；超时返回 None。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                fin, opcode, payload = self._recv_frame(deadline - time.time())
            except socket.timeout:
                return None
            except OSError:
                return None

            if opcode == 0x9:  # ping -> pong
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode == 0x8:
                return None

            if opcode == 0x1:
                self._fragment = payload
                self._fragment_opcode = 0x1
            elif opcode == 0x0:
                self._fragment += payload
            else:
                continue

            if not fin:
                continue
            data = self._fragment
            self._fragment = b""
            self._fragment_opcode = None
            try:
                obj = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            self.inbox.append(obj)
            if predicate(obj):
                return obj
        return None

    def drain(self, seconds=1.0):
        """收集一段时间内的所有 JSON 消息（用于确认某条消息没有投递）。"""
        out = []
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                fin, opcode, payload = self._recv_frame(deadline - time.time())
            except socket.timeout:
                break
            except OSError:
                break
            if opcode == 0x9:
                self._send_frame(0xA, payload)
                continue
            if opcode in (0x1, 0x0):
                if opcode == 0x1:
                    self._fragment = payload
                else:
                    self._fragment += payload
                if fin:
                    try:
                        obj = json.loads(self._fragment.decode("utf-8"))
                        out.append(obj)
                        self.inbox.append(obj)
                    except Exception:
                        pass
                    self._fragment = b""
        return out


def http_post(path, payload=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(LOGIC_BASE + path, json=payload or {},
                         headers=headers, timeout=HTTP_TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError("%s -> HTTP %s %s" %
                           (path, resp.status_code, resp.text[:200]))
    if resp.status_code != 200 or body.get("code") != 0:
        raise RuntimeError("%s -> HTTP %s code=%s message=%s" %
                           (path, resp.status_code, body.get("code"),
                            body.get("message")))
    return body.get("data") or {}


def login(account, password):
    data = http_post("/api/login", {"account": account, "password": password})
    return data["user_id"], data["token"]


def unread(token, session_id):
    return http_post("/api/session/unread", {"session_id": session_id}, token)


def mark_read(token, session_id):
    info = unread(token, session_id)
    http_post("/api/session/mark_read",
              {"session_id": session_id,
               "read_seq": int(info.get("last_msg_seq", 0))}, token)
    return unread(token, session_id)


def main():
    print("Logic HTTP:", LOGIC_BASE)
    print("Comet WS: ws://%s:%d/ws" % (WS_HOST, WS_PORT))

    uid_a, token_a = login(A_ACCOUNT, A_PASSWORD)
    uid_b, token_b = login(B_ACCOUNT, B_PASSWORD)
    uid_c, token_c = login(C_ACCOUNT, C_PASSWORD)
    check("A/B/C 登录成功", uid_a > 0 and uid_b > 0 and uid_c > 0,
          "a=%s b=%s c=%s" % (uid_a, uid_b, uid_c))

    session_id = "single:%d:%d" % (min(uid_a, uid_b), max(uid_a, uid_b))
    base_unread = int(unread(token_b, session_id).get("unread", 0))

    ws_a = ws_b = ws_c = None
    try:
        try:
            ws_a = WSClient(token_a)
            check("A WebSocket 握手成功", True)
        except Exception as e:
            check("A WebSocket 握手成功", False, str(e))
            raise
        try:
            ws_b = WSClient(token_b)
            check("B WebSocket 握手成功", True)
        except Exception as e:
            check("B WebSocket 握手成功", False, str(e))
            raise

        # 等 Comet OnOpen 把连接登记进 registry / Logic 路由。
        time.sleep(0.3)

        # ---------- 单聊：A -> B ----------
        marker1 = "ws-e2e-a2b-%d" % int(time.time() * 1000)
        client1 = "ws-client-a-%d" % int(time.time() * 1000)
        ws_a.send_json({
            "type": "single_chat",
            "to_user_id": uid_b,
            "client_msg_id": client1,
            "content": {"text": marker1},
        })

        msg_b = ws_b.recv_json_until(
            lambda o: o.get("type") == "single_chat" and
            (o.get("content") or {}).get("text") == marker1)
        check("B 实时收到 A 的单聊消息", msg_b is not None, msg_b)
        if msg_b:
            check("B 收到的消息字段完整",
                  msg_b.get("from_user_id") == uid_a and
                  bool(msg_b.get("msg_id")) and
                  bool(msg_b.get("create_time")),
                  msg_b)

        ack_a = ws_a.recv_json_until(
            lambda o: o.get("type") == "ack" and
            o.get("client_msg_id") == client1)
        check("A 收到服务端 ack", ack_a is not None, ack_a)
        if ack_a and msg_b:
            check("ack 的 msg_id 与 B 收到的一致",
                  ack_a.get("msg_id") == msg_b.get("msg_id"),
                  "ack=%s push=%s" % (ack_a.get("msg_id"),
                                      msg_b.get("msg_id")))

        info_b = unread(token_b, session_id)
        check("接收方未读增加 1",
              int(info_b.get("unread", 0)) == base_unread + 1,
              "base=%s now=%s" % (base_unread, info_b.get("unread")))

        info_b2 = mark_read(token_b, session_id)
        check("接收方标记已读后未读归零",
              int(info_b2.get("unread", 0)) == 0, info_b2)

        # ---------- 单聊：B -> A ----------
        marker2 = "ws-e2e-b2a-%d" % int(time.time() * 1000)
        client2 = "ws-client-b-%d" % int(time.time() * 1000)
        ws_b.send_json({
            "type": "single_chat",
            "to_user_id": uid_a,
            "client_msg_id": client2,
            "content": {"text": marker2},
        })
        msg_a = ws_a.recv_json_until(
            lambda o: o.get("type") == "single_chat" and
            (o.get("content") or {}).get("text") == marker2)
        check("A 实时收到 B 的回复", msg_a is not None, msg_a)
        ack_b = ws_b.recv_json_until(
            lambda o: o.get("type") == "ack" and
            o.get("client_msg_id") == client2)
        check("B 收到服务端 ack", ack_b is not None, ack_b)
        mark_read(token_a, session_id)

        # ---------- 非好友：C -> A 必须被 Logic 拒绝 ----------
        try:
            ws_c = WSClient(token_c)
            check("C WebSocket 握手成功", True)
        except Exception as e:
            check("C WebSocket 握手成功", False, str(e))
        if ws_c:
            marker3 = "ws-e2e-nonfriend-%d" % int(time.time() * 1000)
            client3 = "ws-client-c-%d" % int(time.time() * 1000)
            ws_c.send_json({
                "type": "single_chat",
                "to_user_id": uid_a,
                "client_msg_id": client3,
                "content": {"text": marker3},
            })
            err_c = ws_c.recv_json_until(
                lambda o: o.get("type") == "error" and
                o.get("client_msg_id") == client3)
            check("非好友 C 发送单聊被拒绝", err_c is not None, err_c)
            if err_c:
                check("拒绝原因包含好友校验",
                      "friend" in str(err_c.get("message", "")).lower(),
                      err_c.get("message"))
            leftovers = ws_a.drain(1.0)
            leaked = [o for o in leftovers
                      if marker3 in json.dumps(o, ensure_ascii=False)]
            check("非好友消息没有投递给 A", not leaked, leaked)

        # ---------- 聊天室广播 ----------
        if ws_a and ws_b:
            ws_a.send_json({"type": "chatroom_join", "group_id": ROOM_ID})
            ack_join_a = ws_a.recv_json_until(
                lambda o: o.get("type") == "ack" and
                o.get("op") == "chatroom_join")
            check("A 加入聊天室收到 ack", ack_join_a is not None, ack_join_a)

            ws_b.send_json({"type": "chatroom_join", "group_id": ROOM_ID})
            ack_join_b = ws_b.recv_json_until(
                lambda o: o.get("type") == "ack" and
                o.get("op") == "chatroom_join")
            check("B 加入聊天室收到 ack", ack_join_b is not None, ack_join_b)

            time.sleep(0.2)
            room_marker = "ws-e2e-room-%d" % int(time.time() * 1000)
            room_client = "ws-client-room-%d" % int(time.time() * 1000)
            ws_a.send_json({
                "type": "chatroom",
                "group_id": ROOM_ID,
                "client_msg_id": room_client,
                "content": {"text": room_marker},
            })
            room_a = ws_a.recv_json_until(
                lambda o: o.get("type") == "chatroom" and
                (o.get("content") or {}).get("text") == room_marker)
            room_b = ws_b.recv_json_until(
                lambda o: o.get("type") == "chatroom" and
                (o.get("content") or {}).get("text") == room_marker)
            check("A 实时收到聊天室广播", room_a is not None, room_a)
            check("B 实时收到聊天室广播", room_b is not None, room_b)
            ack_room = next(
                (o for o in ws_a.inbox
                 if o.get("type") == "ack" and
                 o.get("client_msg_id") == room_client), None)
            if ack_room is None:
                ack_room = ws_a.recv_json_until(
                    lambda o: o.get("type") == "ack" and
                    o.get("client_msg_id") == room_client)
            check("聊天室消息收到服务端 ack", ack_room is not None, ack_room)

    finally:
        for client in (ws_a, ws_b, ws_c):
            if client:
                client.close()

    print("\n" + "=" * 60)
    print("通过 %d 项，失败 %d 项" % (PASS, FAIL))
    if FAILED_NAMES:
        print("失败项：")
        for name in FAILED_NAMES:
            print("  - " + name)
    print("本次新增消息（未清理）：")
    print("  单聊 %s 两条（A->B、B->A）" % session_id)
    print("  聊天室 room:%d 一条" % ROOM_ID)
    print("=" * 60)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
