#!/usr/bin/env python3
"""MsgRail 用户资料 / 好友 / 昵称搜索 / 未读消息真实 HTTP API 测试。

前置条件：
  1. MySQL 已执行 sql/migrations/002_user_profile_friend.sql；
  2. scripts/start_all.sh 已启动 logic(9101)、job、media_worker、MinIO；
  3. Python 3 可导入 requests。

本脚本只调用真实 HTTP 接口，不直接写数据库。
脚本不会删除任何数据：运行后会留下测试账号、好友关系、消息和头像媒体。
"""

import base64
import os
import sys
import time

import requests

LOGIC_BASE = os.environ.get("LOGIC_BASE", "http://127.0.0.1:9101")
TIMEOUT = 15

PASS = 0
FAIL = 0
FAILED_NAMES = []

# 1x1 透明 PNG，用于测试完整头像上传管线。
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8"
    "z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


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


class ApiError(RuntimeError):
    def __init__(self, path, status, body):
        self.path = path
        self.status = status
        self.body = body
        super().__init__(
            "%s -> HTTP %s code=%s message=%s"
            % (path, status, body.get("code"), body.get("message"))
        )


def _post_raw(path, payload, token):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    resp = requests.post(
        LOGIC_BASE + path, json=payload or {}, headers=headers, timeout=TIMEOUT
    )
    try:
        body = resp.json()
    except Exception:
        raise ApiError(path, resp.status_code, {"message": resp.text[:300]})
    return resp.status_code, body


def ok(path, payload=None, token=None):
    """期望 HTTP 200 + code=0，返回 data。"""
    status, body = _post_raw(path, payload, token)
    if status != 200 or body.get("code") != 0:
        raise ApiError(path, status, body)
    return body.get("data") or {}


def expect_error(path, payload, token, http_status, code):
    """期望指定 HTTP 状态与业务 code，返回 data（可能为空）。"""
    status, body = _post_raw(path, payload, token)
    if status != http_status or body.get("code") != code:
        raise ApiError(path, status, body)
    check(
        "错误响应 %s -> HTTP %s code=%s" % (path, http_status, code),
        True,
    )
    return body.get("data") or {}


def get_json(path, token):
    headers = {"Authorization": "Bearer " + token} if token else {}
    resp = requests.get(LOGIC_BASE + path, headers=headers, timeout=TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        raise ApiError(path, resp.status_code, {"message": resp.text[:300]})
    if resp.status_code != 200 or body.get("code") != 0:
        raise ApiError(path, resp.status_code, body)
    return body.get("data") or {}


def register(account, password, name):
    data = ok("/api/register", {"account": account, "password": password,
                                "name": name})
    return data


def search(token, keyword, limit=50, offset=0):
    return ok("/api/user/search",
              {"keyword": keyword, "limit": limit, "offset": offset}, token)


def find_session(summary, session_id):
    for item in summary.get("sessions", []):
        if item.get("session_id") == session_id:
            return item
    return None


def upload_avatar(token):
    init = ok("/api/media/upload/init", {
        "kind": "image",
        "mime_type": "image/png",
        "size_bytes": len(PNG_1X1),
        "filename": "e2e-avatar.png",
    }, token)
    upload_url = init["upload_url"]
    media_id = init["media_id"]

    put = requests.put(upload_url, data=PNG_1X1,
                       headers={"Content-Type": "image/png"}, timeout=30)
    check("头像原图直传 MinIO", put.status_code in (200, 204),
          "HTTP %s" % put.status_code)

    ok("/api/media/upload/complete", {"media_id": media_id}, token)

    meta = {}
    deadline = time.time() + 30
    while time.time() < deadline:
        meta = get_json("/api/media/" + media_id, token)
        if meta.get("status") == "ready":
            break
        if meta.get("status") == "failed":
            raise RuntimeError("头像处理失败: " + str(meta.get("error")))
        time.sleep(0.5)
    check("头像异步处理 ready", meta.get("status") == "ready",
          "status=%s" % meta.get("status"))
    return media_id, meta


def wait_history(token, session_id, msg_id, timeout=10):
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = ok("/api/session/history",
                  {"session_id": session_id, "anchor_seq": 0, "limit": 50},
                  token)
        for msg in last.get("messages", []):
            if msg.get("msg_id") == msg_id:
                return last
        time.sleep(0.3)
    return last


def main():
    print("Logic HTTP:", LOGIC_BASE)

    # Logic HTTP 存活检查：未带 token 的资料接口应返回 401。
    status, body = _post_raw("/api/user/profile", {}, None)
    check("Logic HTTP 存活且未鉴权返回 401",
          status == 401 and body.get("code") == 401,
          "HTTP %s code=%s" % (status, body.get("code")))

    # 每次运行使用唯一账号，避免与历史数据冲突。
    suffix = "%06d%02d" % (int(time.time()) % 1000000, os.getpid() % 100)
    acc_a, acc_b, acc_c = (
        "e2e_a_" + suffix, "e2e_b_" + suffix, "e2e_c_" + suffix)
    pwd = "e2e-pass-" + suffix
    name_a = "E2E-A-" + suffix
    name_b = "E2E-B-" + suffix
    name_c = "E2E-C-" + suffix
    name_a2 = "E2E-A2-" + suffix

    a = register(acc_a, pwd, name_a)
    b = register(acc_b, pwd, name_b)
    c = register(acc_c, pwd, name_c)
    uid_a, uid_b, uid_c = a["user_id"], b["user_id"], c["user_id"]
    check("注册三个独立用户", len({uid_a, uid_b, uid_c}) == 3,
          "%s/%s/%s" % (uid_a, uid_b, uid_c))

    # 登录接口：重新签发 token
    login = ok("/api/login", {"account": acc_a, "password": pwd})
    check("登录返回同一 user_id", login.get("user_id") == uid_a,
          login)
    token_a = login["token"]
    token_b = b["token"]
    token_c = c["token"]

    # ---------- 用户资料 ----------
    p = ok("/api/user/profile", {}, token_a)
    check("查询自己资料", p.get("user_id") == uid_a and
          p.get("is_self") is True and p.get("relationship") == "self", p)
    check("自己资料包含 account", p.get("account") == acc_a, p)
    check("新用户默认无头像", p.get("avatar_media_id") == "", p)

    p_other = ok("/api/user/profile", {"user_id": uid_a}, token_b)
    check("他人可查公开资料", p_other.get("user_id") == uid_a and
          p_other.get("is_self") is False, p_other)
    check("他人资料不泄露 account", "account" not in p_other, p_other)
    check("初始关系为 none", p_other.get("relationship") == "none", p_other)

    expect_error("/api/user/profile", {"user_id": 999999999}, token_a, 404, 404)

    up = ok("/api/user/profile/update", {"name": name_a2}, token_a)
    check("修改昵称并返回新昵称", up.get("name") == name_a2, up)

    expect_error("/api/user/profile/update", {"name": ""}, token_a, 400, 400)
    expect_error("/api/user/profile/update", {"name": "x" * 33},
                 token_a, 400, 400)
    expect_error("/api/user/profile/update", {"avatar_media_id": "bad"},
                 token_a, 400, 400)
    expect_error("/api/user/profile/update",
                 {"avatar_media_id": "m_" + "0" * 26}, token_a, 404, 404)

    # ---------- 昵称 / UserID 搜索 ----------
    s = search(token_a, name_a2)
    check("昵称精确搜索命中", s.get("match_type") == "nickname" and
          any(u.get("user_id") == uid_a for u in s.get("users", [])), s)
    me = [u for u in s.get("users", []) if u.get("user_id") == uid_a][0]
    check("搜索结果关系为 self", me.get("relationship") == "self", me)

    s_lower = search(token_b, name_a2.lower())
    check("昵称搜索大小写不敏感", s_lower.get("match_type") == "nickname" and
          any(u.get("user_id") == uid_a for u in s_lower.get("users", [])),
          s_lower)

    s_old = search(token_b, name_a)
    check("旧昵称搜索不到", s_old.get("users") == [], s_old)

    s_id = search(token_b, str(uid_a))
    check("纯数字按 UserID 精确查", s_id.get("match_type") == "user_id" and
          len(s_id.get("users", [])) == 1 and
          s_id["users"][0].get("user_id") == uid_a, s_id)

    s_none = search(token_b, "NoSuchNickname-" + suffix)
    check("不存在的昵称返回空", s_none.get("users") == [], s_none)
    s_none_id = search(token_b, "999999999")
    check("不存在的 UserID 返回空",
          s_none_id.get("match_type") == "user_id" and
          s_none_id.get("users") == [], s_none_id)
    expect_error("/api/user/search", {"keyword": ""}, token_b, 400, 400)

    # ---------- 好友申请 / 同意 ----------
    req = ok("/api/friend/request", {"to_user_id": uid_b, "message": ""},
             token_a)
    request_id = req["request_id"]
    check("发送好友申请", request_id > 0 and req.get("status") == "pending",
          req)

    req2 = ok("/api/friend/request", {"to_user_id": uid_b, "message": ""},
              token_a)
    check("同方向重复申请幂等", req2.get("request_id") == request_id, req2)

    rev = expect_error("/api/friend/request", {"to_user_id": uid_a},
                       token_b, 409, 409)
    check("反方向申请返回 409 与原申请 ID",
          rev.get("request_id") == request_id, rev)

    requests_b = ok("/api/friend/requests", {}, token_b)
    inc = requests_b.get("incoming", [])
    check("接收方看到 pending 申请", len(inc) == 1 and
          inc[0].get("request_id") == request_id and
          inc[0].get("from_user", {}).get("user_id") == uid_a, requests_b)
    check("接收方 outgoing 为空", requests_b.get("outgoing") == [], requests_b)

    requests_a = ok("/api/friend/requests", {}, token_a)
    out = requests_a.get("outgoing", [])
    check("申请方看到 outgoing 申请", len(out) == 1 and
          out[0].get("to_user", {}).get("user_id") == uid_b, requests_a)

    pending = ok("/api/friend/pending_count", {}, token_b)
    check("待处理申请数为 1", pending.get("pending_count") == 1, pending)

    # 非好友打开会话 / 发消息
    expect_error("/api/session/open_single", {"peer_user_id": uid_a},
                 token_c, 403, 403)
    expect_error("/api/message/send",
                 {"to_user_id": uid_a, "msg_type": "text", "content": "hi"},
                 token_c, 403, 403)

    resp = ok("/api/friend/respond",
              {"request_id": request_id, "action": "accept"}, token_b)
    check("同意好友申请", resp.get("status") == "accepted", resp)

    pending = ok("/api/friend/pending_count", {}, token_b)
    check("同意后待处理数为 0", pending.get("pending_count") == 0, pending)

    friends_b = ok("/api/friend/list", {}, token_b).get("friends", [])
    check("B 好友列表包含 A",
          any(f.get("user_id") == uid_a and f.get("name") == name_a2
              for f in friends_b), friends_b)
    friends_a = ok("/api/friend/list", {}, token_a).get("friends", [])
    check("A 好友列表包含 B",
          any(f.get("user_id") == uid_b and f.get("name") == name_b
              for f in friends_a), friends_a)

    s_rel = search(token_a, name_b)
    check("好友搜索结果关系为 friend",
          s_rel.get("users") and
          s_rel["users"][0].get("relationship") == "friend", s_rel)

    expect_error("/api/friend/request", {"to_user_id": uid_a}, token_a,
                 400, 400)
    expect_error("/api/friend/request", {"to_user_id": 999999999}, token_a,
                 404, 404)

    # ---------- 打开单聊 / 发送消息 / 未读 ----------
    opened = ok("/api/session/open_single", {"peer_user_id": uid_b}, token_a)
    expected_sid = "single:%d:%d" % (min(uid_a, uid_b), max(uid_a, uid_b))
    session_id = opened.get("session_id")
    check("打开单聊返回规范 session_id", session_id == expected_sid,
          opened)
    check("open_single 返回 peer 昵称",
          opened.get("peer", {}).get("name") == name_b, opened)
    check("open_single 返回 unread/last_msg_seq",
          "unread" in opened and "last_msg_seq" in opened, opened)

    m1 = ok("/api/message/send",
            {"to_user_id": uid_b, "msg_type": "text", "content": "e2e-1"},
            token_a)
    check("HTTP 发送单聊消息", m1.get("msg_seq", 0) >= 1, m1)
    seq1 = m1["msg_seq"]

    unread_b = ok("/api/session/unread", {"session_id": session_id}, token_b)
    check("B 会话未读为 1", unread_b.get("unread") == 1 and
          unread_b.get("last_msg_seq") == seq1, unread_b)

    list_b = ok("/api/session/list_single", {}, token_b).get("sessions", [])
    sess_b = [x for x in list_b if x.get("session_id") == session_id]
    check("B 单聊列表包含未读与 peer 昵称", len(sess_b) == 1 and
          sess_b[0].get("unread") == 1 and
          sess_b[0].get("peer_user_id") == uid_a and
          sess_b[0].get("peer_name") == name_a2, list_b)

    summary_b = ok("/api/session/unread_summary", {}, token_b)
    item_b = find_session(summary_b, session_id)
    check("全局未读汇总包含该会话",
          summary_b.get("total") == 1 and item_b is not None and
          item_b.get("unread") == 1 and item_b.get("type") == 0,
          summary_b)

    hist = wait_history(token_b, session_id, m1["msg_id"])
    msg1 = [x for x in hist.get("messages", [])
            if x.get("msg_id") == m1["msg_id"]]
    check("历史消息落库", len(msg1) == 1, hist)
    if msg1:
        check("历史消息 sender 使用当前昵称",
              msg1[0].get("sender", {}).get("user_id") == uid_a and
              msg1[0].get("sender", {}).get("name") == name_a2, msg1[0])

    ok("/api/session/mark_read",
       {"session_id": session_id, "read_seq": seq1}, token_b)
    unread_b = ok("/api/session/unread", {"session_id": session_id}, token_b)
    check("标记已读后未读为 0", unread_b.get("unread") == 0, unread_b)
    check("B 全局未读归零",
          ok("/api/session/unread_summary", {}, token_b).get("total") == 0)

    m2 = ok("/api/message/send",
            {"to_user_id": uid_b, "msg_type": "text", "content": "e2e-2"},
            token_a)
    seq2 = m2["msg_seq"]
    check("第二条消息序号递增", seq2 > seq1, m2)
    check("B 再次出现 1 条未读",
          ok("/api/session/unread", {"session_id": session_id},
             token_b).get("unread") == 1)

    ok("/api/session/mark_read",
       {"session_id": session_id, "read_seq": seq2}, token_b)
    # 尝试回退读游标：服务端必须保持单调。
    ok("/api/session/mark_read",
       {"session_id": session_id, "read_seq": seq1}, token_b)
    unread_b = ok("/api/session/unread", {"session_id": session_id}, token_b)
    check("读游标不可回退", unread_b.get("unread") == 0, unread_b)

    hist = wait_history(token_b, session_id, m2["msg_id"])
    check("两条消息都可在历史中查到",
          len([x for x in hist.get("messages", [])
               if x.get("msg_id") in (m1["msg_id"], m2["msg_id"])]) == 2,
          hist)

    # ---------- 头像上传全链路 ----------
    media_id, meta = upload_avatar(token_a)
    check("媒体详情包含头像缩略图变体",
          bool(meta.get("variants")), meta)
    prof = ok("/api/user/profile/update",
              {"avatar_media_id": media_id}, token_a)
    check("绑定头像成功", prof.get("avatar_media_id") == media_id and
          bool(prof.get("avatar_url")), prof)

    prof_b_view = ok("/api/user/profile", {"user_id": uid_a}, token_b)
    check("他人可见头像签名 URL",
          prof_b_view.get("avatar_media_id") == media_id and
          bool(prof_b_view.get("avatar_url")), prof_b_view)

    avatar_resp = requests.get(prof_b_view["avatar_url"], timeout=30)
    check("头像签名 URL 可真实下载",
          avatar_resp.status_code == 200 and len(avatar_resp.content) > 0 and
          avatar_resp.headers.get("Content-Type", "").startswith("image/"),
          "HTTP %s type=%s bytes=%s" % (
              avatar_resp.status_code,
              avatar_resp.headers.get("Content-Type"),
              len(avatar_resp.content)))

    s_avatar = search(token_b, name_a2)
    entry = [u for u in s_avatar.get("users", [])
             if u.get("user_id") == uid_a]
    check("搜索结果带头像 URL", entry and bool(entry[0].get("avatar_url")),
          s_avatar)

    list_b = ok("/api/session/list_single", {}, token_b).get("sessions", [])
    sess_b = [x for x in list_b if x.get("session_id") == session_id]
    check("单聊列表带 peer 头像 URL",
          sess_b and bool(sess_b[0].get("peer_avatar_url")), list_b)

    # ---------- 聊天室未读基线 ----------
    before = ok("/api/session/unread_summary", {}, token_a)
    check("加入前全局汇总无聊天室",
          not any(x.get("type") == 2 for x in before.get("sessions", [])),
          before)

    ok("/api/chatroom/join", {"room_id": 1}, token_a)
    rooms = ok("/api/chatroom/list", {}, token_a).get("rooms", [])
    room = [x for x in rooms if x.get("room_id") == 1]
    check("聊天室列表包含 room 1", len(room) == 1, rooms)
    if room:
        check("加入时已读基线避免历史未读",
              room[0].get("unread") == 0, room[0])

    after = ok("/api/session/unread_summary", {}, token_a)
    room_item = [x for x in after.get("sessions", [])
                 if x.get("type") == 2 and x.get("room_id") == 1]
    check("全局汇总包含已加入聊天室", len(room_item) == 1 and
          room_item[0].get("unread") == 0, after)

    # ---------- 汇总 ----------
    print()
    print("=" * 60)
    print("通过 %d 项，失败 %d 项" % (PASS, FAIL))
    if FAILED_NAMES:
        print("失败项:")
        for name in FAILED_NAMES:
            print("  - " + name)
    print("测试数据（未清理）:")
    print("  A: id=%s account=%s name=%s" % (uid_a, acc_a, name_a2))
    print("  B: id=%s account=%s name=%s" % (uid_b, acc_b, name_b))
    print("  C: id=%s account=%s name=%s" % (uid_c, acc_c, name_c))
    print("  好友申请 id=%s, session=%s" % (request_id, session_id))
    print("  msg1=%s seq=%s, msg2=%s seq=%s"
          % (m1.get("msg_id"), seq1, m2.get("msg_id"), seq2))
    print("  avatar media_id=%s" % media_id)
    print("=" * 60)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("[ERROR] " + repr(exc))
        sys.exit(2)
