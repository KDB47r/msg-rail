#include "ai_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

#include "logging.h"
#include "proto_enums.h"

namespace sparkpush {

namespace {

std::atomic<uint64_t> g_ai_job_counter{0};

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

const char* StatusName(int status) {
    switch (status) {
        case 0:
            return "pending";
        case 1:
            return "active";
        case 2:
            return "rejected";
        case 3:
            return "removed";
        default:
            return "none";
    }
}

}  // namespace

AiService::AiService(const Config& cfg, AiDao* ai_dao, UserDao* user_dao,
                     ConversationStore* store, RedisStore* redis_store,
                     KafkaProducer* push_producer,
                     KafkaProducer* trigger_producer)
    : cfg_(cfg),
      ai_dao_(ai_dao),
      user_dao_(user_dao),
      store_(store),
      redis_store_(redis_store),
      push_producer_(push_producer),
      trigger_producer_(trigger_producer) {}

bool AiService::IsPrivateBot(int64_t user_id) const {
    return user_id > 0 && user_id == cfg_.ai_private_bot_user_id;
}

bool AiService::IsSharedBot(int64_t user_id) const {
    return user_id > 0 && user_id == cfg_.ai_shared_bot_user_id;
}

bool AiService::IsBot(int64_t user_id) const {
    return IsPrivateBot(user_id) || IsSharedBot(user_id);
}

bool AiService::ParseSingleUsers(const std::string& session_id, int64_t* user1,
                                 int64_t* user2) {
    static const std::string kPrefix = "single:";
    if (!user1 || !user2 ||
        session_id.compare(0, kPrefix.size(), kPrefix) != 0) {
        return false;
    }
    const std::string rest = session_id.substr(kPrefix.size());
    const auto sep = rest.find(':');
    if (sep == std::string::npos) return false;
    try {
        *user1 = std::stoll(rest.substr(0, sep));
        *user2 = std::stoll(rest.substr(sep + 1));
    } catch (...) {
        return false;
    }
    return *user1 > 0 && *user2 > 0;
}

std::string AiService::ExtractText(const std::string& content_json) {
    try {
        const auto j = nlohmann::json::parse(content_json);
        if (j.is_object()) {
            if (j.contains("content") && j["content"].is_string()) {
                return j["content"].get<std::string>();
            }
            if (j.contains("text") && j["text"].is_string()) {
                return j["text"].get<std::string>();
            }
        }
        if (j.is_string()) return j.get<std::string>();
    } catch (...) {
    }
    return "";
}

bool AiService::HasMention(const std::string& content_json,
                           int64_t bot_user_id, const std::string& text) {
    try {
        const auto j = nlohmann::json::parse(content_json);
        if (j.is_object() && j.contains("mentions") &&
            j["mentions"].is_array()) {
            for (const auto& item : j["mentions"]) {
                if (!item.is_object() || !item.contains("user_id")) continue;
                int64_t mentioned = 0;
                if (item["user_id"].is_number_integer()) {
                    mentioned = item["user_id"].get<int64_t>();
                } else if (item["user_id"].is_string()) {
                    try {
                        mentioned = std::stoll(
                            item["user_id"].get<std::string>());
                    } catch (...) {
                        continue;
                    }
                }
                if (mentioned == bot_user_id) return true;
            }
        }
    } catch (...) {
    }
    // 兼容旧客户端：没有结构化 mentions 时退回文本判断。
    return text.find("@小桁") != std::string::npos;
}

std::string AiService::SenderName(int64_t user_id) const {
    if (!user_dao_ || user_id <= 0) return "";
    User user;
    std::string err;
    if (!user_dao_->GetUserById(user_id, &user, &err)) return "";
    return user.name;
}

AiJob AiService::BuildJob(const std::string& mode,
                          const std::string& session_id, int64_t bot_user_id,
                          const std::string& trigger_msg_id,
                          int64_t trigger_sender_id,
                          const std::string& trigger_sender_name,
                          const std::string& trigger_content,
                          int64_t activated_seq) const {
    AiJob job;
    job.job_id = "job_" + std::to_string(NowMs()) + "_" +
                 std::to_string(g_ai_job_counter.fetch_add(1));
    job.session_id = session_id;
    job.bot_user_id = bot_user_id;
    job.mode = mode;
    job.trigger_message_id = trigger_msg_id;
    job.trigger_sender_id = trigger_sender_id;
    job.trigger_sender_name = trigger_sender_name;
    job.trigger_content = trigger_content;
    job.activated_seq = activated_seq;
    job.status = "queued";
    return job;
}

bool AiService::ResolveTargets(const AiJob& job,
                               std::vector<int64_t>* targets,
                               std::string* err_msg) const {
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(job.session_id, &a, &b)) {
        if (err_msg) *err_msg = "invalid single session_id";
        return false;
    }
    targets->clear();
    if (job.mode == "private") {
        if (IsPrivateBot(a)) {
            targets->push_back(b);
        } else if (IsPrivateBot(b)) {
            targets->push_back(a);
        } else {
            if (err_msg) *err_msg = "bot not in private session";
            return false;
        }
        return true;
    }
    if (IsBot(a) || IsBot(b)) {
        if (err_msg) *err_msg = "shared bot must not be a session member";
        return false;
    }
    targets->push_back(a);
    targets->push_back(b);
    return true;
}

bool AiService::GetSessionBotActive(const std::string& session_id,
                                    int64_t bot_user_id, bool* active,
                                    SessionBot* row) {
    if (!active) return false;
    bool cached = false;
    if (redis_store_ &&
        redis_store_->GetSessionBotActive(session_id, bot_user_id, &cached)) {
        *active = cached;
        // 缓存只存 active 布尔值；触发 AI 时还需要 activated_seq，
        // 因此 active=true 且调用方要 row 时仍回源一次 MySQL。
        if (!cached || row == nullptr) return true;
    }
    if (!ai_dao_) return false;
    SessionBot db_row;
    std::string err;
    if (!ai_dao_->GetSessionBot(session_id, bot_user_id, &db_row, &err)) {
        if (!cached) {
            *active = false;
            return true;
        }
        // 缓存说 active，但拿不到 activated_seq 时不能凭 0 当截断点，
        // 否则可能把加入前的历史喂给气氛组。
        return false;
    }
    *active = (db_row.status == 1);
    if (row) *row = db_row;
    if (redis_store_) {
        redis_store_->SetSessionBotActive(session_id, bot_user_id, *active);
    }
    return true;
}

bool AiService::SendTrigger(const AiJob& job) {
    if (!trigger_producer_) {
        LogError("AI trigger producer not initialized");
        return false;
    }
    std::vector<int64_t> targets;
    std::string err;
    if (!ResolveTargets(job, &targets, &err)) {
        LogError("AI trigger target resolve failed: " + err);
        return false;
    }
    nlohmann::json j;
    j["type"] = "ai_trigger";
    j["job_id"] = job.job_id;
    j["mode"] = job.mode;
    j["session_id"] = job.session_id;
    j["bot_user_id"] = job.bot_user_id;
    j["trigger_message_id"] = job.trigger_message_id;
    j["trigger_sender_id"] = job.trigger_sender_id;
    j["trigger_sender_name"] = job.trigger_sender_name;
    j["trigger_content"] = job.trigger_content;
    j["activated_seq"] = job.activated_seq;
    j["target_user_ids"] = targets;
    j["created_at_ms"] = NowMs();
    if (!trigger_producer_->Send(job.job_id, j.dump())) {
        LogError("AI trigger kafka send failed job_id=" + job.job_id);
        return false;
    }
    return true;
}

bool AiService::CreateAndQueueJob(const AiJob& job, bool* created) {
    if (!ai_dao_) {
        if (created) *created = false;
        return false;
    }
    std::string err;
    if (!ai_dao_->CreateJob(job, created, &err)) {
        LogError("create ai_job failed: " + err);
        return false;
    }
    if (!*created) return true;
    if (!SendTrigger(job)) {
        std::string fail_err;
        Fail(job.job_id, "AI_TRIGGER_FAILED", "AI 触发消息投递失败", &fail_err);
        return false;
    }
    return true;
}

bool AiService::TriggerPrivate(const std::string& session_id,
                               int64_t from_user, int64_t to_user,
                               const std::string& msg_id,
                               const std::string& content_json) {
    if (!IsPrivateBot(to_user)) return true;
    const std::string text = ExtractText(content_json);
    if (text.empty()) return true;
    const AiJob job = BuildJob("private", session_id, to_user, msg_id, from_user,
                               SenderName(from_user), text, 0);
    bool created = false;
    if (!CreateAndQueueJob(job, &created)) {
        LogError("private AI trigger failed session=" + session_id);
        return false;
    }
    return true;
}

bool AiService::MaybeTriggerShared(const std::string& session_id,
                                   int64_t from_user, int64_t to_user,
                                   const std::string& msg_id,
                                   const std::string& content_json) {
    const std::string text = ExtractText(content_json);
    if (!HasMention(content_json, cfg_.ai_shared_bot_user_id, text)) {
        return true;
    }
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(session_id, &a, &b)) return true;
    // 气氛组只存在于 A-B 单聊；bot 本身不是会话成员。
    if (IsBot(a) || IsBot(b)) return true;
    if (!(from_user == a || from_user == b)) return true;
    if (!(to_user == a || to_user == b)) return true;

    bool active = false;
    SessionBot row;
    if (!GetSessionBotActive(session_id, cfg_.ai_shared_bot_user_id, &active,
                             &row)) {
        return true;
    }
    if (!active) return true;

    const AiJob job = BuildJob("shared", session_id,
                               cfg_.ai_shared_bot_user_id, msg_id, from_user,
                               SenderName(from_user), text, row.activated_seq);
    bool created = false;
    if (!CreateAndQueueJob(job, &created)) {
        LogError("shared AI trigger failed session=" + session_id);
        return false;
    }
    return true;
}

bool AiService::SendChatMessage(const std::string& session_id,
                                int64_t sender_id,
                                const std::string& msg_type,
                                const std::string& content_json,
                                int64_t msg_seq, const std::string& msg_id,
                                const std::vector<int64_t>& targets,
                                bool need_persist, std::string* err_msg) {
    if (!push_producer_) {
        if (err_msg) *err_msg = "push producer not initialized";
        return false;
    }
    if (targets.empty()) {
        if (err_msg) *err_msg = "no targets";
        return false;
    }

    const int64_t now_ms = NowMs();
    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    if (redis_store_) {
        for (int64_t uid : targets) {
            std::vector<std::string> comets;
            if (!redis_store_->GetUserRoutes(uid, &comets)) continue;
            for (const auto& cid : comets) {
                auto& users = comet_to_users[cid];
                if (std::find(users.begin(), users.end(), uid) == users.end()) {
                    users.push_back(uid);
                }
            }
        }
    }

    auto build_message = [&](ChatMessage* cm) {
        cm->set_msg_id(msg_id);
        cm->set_session_id(session_id);
        cm->set_msg_seq(msg_seq);
        cm->set_sender_id(sender_id);
        cm->set_timestamp_ms(now_ms);
        cm->set_msg_type(MsgTypeFromString(msg_type));
        cm->set_content_json(content_json);
    };

    bool all_ok = true;
    if (comet_to_users.empty()) {
        if (!need_persist) return true;
        PushToCometRequest req;
        req.set_persist_only(true);
        req.set_scene(SCENE_SINGLE);
        req.set_to_user_id(targets.size() == 1 ? targets[0] : 0);
        req.set_need_persist(true);
        build_message(req.mutable_message());
        std::string payload;
        if (!req.SerializeToString(&payload) ||
            !push_producer_->Send(session_id, payload)) {
            if (err_msg) *err_msg = "kafka persist send failed";
            all_ok = false;
        }
        return all_ok;
    }

    for (const auto& [comet_id, users] : comet_to_users) {
        PushToCometRequest req;
        req.set_comet_id(comet_id);
        req.set_scene(SCENE_SINGLE);
        req.set_to_user_id(users.size() == 1 ? users[0] : 0);
        req.set_need_persist(need_persist);
        for (int64_t uid : users) {
            req.add_targets()->set_user_id(uid);
        }
        build_message(req.mutable_message());
        std::string payload;
        if (!req.SerializeToString(&payload)) {
            all_ok = false;
            continue;
        }
        if (!push_producer_->Send(session_id + "|" + comet_id, payload)) {
            all_ok = false;
        }
    }
    if (!all_ok && err_msg) *err_msg = "kafka push send failed";
    return all_ok;
}

bool AiService::SendSystemMessage(const std::string& session_id,
                                  int64_t bot_user_id,
                                  const std::string& content_json,
                                  const std::vector<int64_t>& targets,
                                  int64_t msg_seq, std::string* err_msg) {
    const std::string msg_id =
        session_id + "-" + std::to_string(msg_seq);
    return SendChatMessage(session_id, bot_user_id, "system", content_json,
                           msg_seq, msg_id, targets, true, err_msg);
}

bool AiService::Invite(const std::string& session_id, int64_t inviter,
                       std::string* err_msg) {
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(session_id, &a, &b) || IsBot(a) || IsBot(b)) {
        if (err_msg) *err_msg = "only A-B single chats can invite the bot";
        return false;
    }
    if (inviter != a && inviter != b) {
        if (err_msg) *err_msg = "not a session member";
        return false;
    }
    if (!ai_dao_ || !store_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }

    SessionBot existing;
    std::string db_err;
    if (ai_dao_->GetSessionBot(session_id, cfg_.ai_shared_bot_user_id,
                               &existing, &db_err)) {
        if (existing.status == 0) {
            if (err_msg) *err_msg = "invite already pending";
            return false;
        }
        if (existing.status == 1) {
            if (err_msg) *err_msg = "bot already active";
            return false;
        }
    }

    if (!ai_dao_->UpsertPendingInvite(session_id, cfg_.ai_shared_bot_user_id,
                                      inviter, err_msg)) {
        return false;
    }
    if (redis_store_) {
        redis_store_->SetSessionBotActive(session_id, cfg_.ai_shared_bot_user_id,
                                          false);
    }

    int64_t seq = 0;
    if (!store_->AllocateMessageSeq(session_id, &seq, err_msg)) return false;
    nlohmann::json content;
    content["type"] = "system";
    content["session_id"] = session_id;
    content["event"] = "bot_invite";
    content["bot_id"] = cfg_.ai_shared_bot_user_id;
    content["bot_name"] = "小桁";
    content["inviter_id"] = inviter;
    content["inviter_name"] = SenderName(inviter);
    content["status"] = "pending";
    content["text"] = "邀请小桁加入会话，等待对方同意";
    std::vector<int64_t> targets{a, b};
    return SendSystemMessage(session_id, cfg_.ai_shared_bot_user_id,
                             content.dump(), targets, seq, err_msg);
}

bool AiService::Respond(const std::string& session_id, int64_t responder,
                        bool accept, std::string* err_msg) {
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(session_id, &a, &b)) {
        if (err_msg) *err_msg = "invalid session_id";
        return false;
    }
    if (responder != a && responder != b) {
        if (err_msg) *err_msg = "not a session member";
        return false;
    }
    if (!ai_dao_ || !store_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }

    SessionBot row;
    if (!ai_dao_->GetSessionBot(session_id, cfg_.ai_shared_bot_user_id, &row,
                                err_msg)) {
        return false;
    }
    if (row.status != 0) {
        if (err_msg) *err_msg = "invite is not pending";
        return false;
    }
    if (responder == row.invited_by) {
        if (err_msg) *err_msg = "inviter cannot accept their own invite";
        return false;
    }

    int64_t seq = 0;
    if (!store_->AllocateMessageSeq(session_id, &seq, err_msg)) return false;

    nlohmann::json content;
    content["type"] = "system";
    content["session_id"] = session_id;
    content["bot_id"] = cfg_.ai_shared_bot_user_id;
    content["bot_name"] = "小桁";
    content["responder_id"] = responder;
    content["responder_name"] = SenderName(responder);
    std::vector<int64_t> targets{a, b};

    if (!accept) {
        if (!ai_dao_->Reject(session_id, cfg_.ai_shared_bot_user_id,
                             err_msg)) {
            return false;
        }
        if (redis_store_) {
            redis_store_->SetSessionBotActive(
                session_id, cfg_.ai_shared_bot_user_id, false);
        }
        content["event"] = "bot_invite_rejected";
        content["text"] = "已拒绝小桁加入";
        return SendSystemMessage(session_id, cfg_.ai_shared_bot_user_id,
                                 content.dump(), targets, seq, err_msg);
    }

    if (!ai_dao_->Activate(session_id, cfg_.ai_shared_bot_user_id, seq,
                           err_msg)) {
        return false;
    }
    if (redis_store_) {
        redis_store_->SetSessionBotActive(session_id, cfg_.ai_shared_bot_user_id,
                                          true);
    }
    content["event"] = "bot_joined";
    content["text"] = "小桁已加入，@小桁 可以叫他说话";
    const bool sent = SendSystemMessage(session_id, cfg_.ai_shared_bot_user_id,
                                        content.dump(), targets, seq, err_msg);
    if (!sent) {
        LogError("bot joined system message send failed session=" +
                 session_id);
    }
    return true;
}

bool AiService::Remove(const std::string& session_id, int64_t user_id,
                       std::string* err_msg) {
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(session_id, &a, &b)) {
        if (err_msg) *err_msg = "invalid session_id";
        return false;
    }
    if (user_id != a && user_id != b) {
        if (err_msg) *err_msg = "not a session member";
        return false;
    }
    if (!ai_dao_ || !store_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    SessionBot row;
    if (!ai_dao_->GetSessionBot(session_id, cfg_.ai_shared_bot_user_id, &row,
                                err_msg)) {
        return false;
    }
    if (row.status != 0 && row.status != 1) {
        if (err_msg) *err_msg = "bot is not in this session";
        return false;
    }

    int64_t seq = 0;
    if (!store_->AllocateMessageSeq(session_id, &seq, err_msg)) return false;
    if (!ai_dao_->Remove(session_id, cfg_.ai_shared_bot_user_id, err_msg)) {
        return false;
    }
    if (redis_store_) {
        redis_store_->SetSessionBotActive(session_id, cfg_.ai_shared_bot_user_id,
                                          false);
    }
    nlohmann::json content;
    content["type"] = "system";
    content["session_id"] = session_id;
    content["event"] = "bot_removed";
    content["bot_id"] = cfg_.ai_shared_bot_user_id;
    content["bot_name"] = "小桁";
    content["remover_id"] = user_id;
    content["remover_name"] = SenderName(user_id);
    content["text"] = "小桁已退出会话";
    std::vector<int64_t> targets{a, b};
    return SendSystemMessage(session_id, cfg_.ai_shared_bot_user_id,
                             content.dump(), targets, seq, err_msg);
}

bool AiService::GetStatus(const std::string& session_id, int64_t requester,
                          nlohmann::json* out, std::string* err_msg) {
    int64_t a = 0;
    int64_t b = 0;
    if (!ParseSingleUsers(session_id, &a, &b)) {
        if (err_msg) *err_msg = "invalid session_id";
        return false;
    }
    if (requester != a && requester != b) {
        if (err_msg) *err_msg = "not a session member";
        return false;
    }
    if (!ai_dao_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    SessionBot row;
    std::string db_err;
    nlohmann::json data;
    data["session_id"] = session_id;
    data["bot_user_id"] = cfg_.ai_shared_bot_user_id;
    data["bot_name"] = "小桁";
    if (ai_dao_->GetSessionBot(session_id, cfg_.ai_shared_bot_user_id, &row,
                               &db_err)) {
        data["status"] = StatusName(row.status);
        data["invited_by"] = row.invited_by;
        data["activated_seq"] = row.activated_seq;
    } else {
        data["status"] = "none";
        data["invited_by"] = 0;
        data["activated_seq"] = 0;
    }
    *out = std::move(data);
    return true;
}

bool AiService::GetContext(const std::string& job_id, nlohmann::json* out,
                           std::string* err_msg) {
    if (!ai_dao_ || !store_ || !out) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    AiJob job;
    if (!ai_dao_->GetJob(job_id, &job, err_msg)) return false;
    if (job.status == "completed" || job.status == "failed" ||
        job.status == "aborted") {
        if (err_msg) *err_msg = "job is not active: " + job.status;
        return false;
    }
    std::vector<int64_t> targets;
    if (!ResolveTargets(job, &targets, err_msg)) return false;

    std::string run_err;
    ai_dao_->MarkJobRunning(job_id, &run_err);

    // 不能把触发之后才到达的消息喂给模型。trigger_message_id 形如
    // <session_id>-<seq>，取最后一段作为上界。
    int64_t trigger_seq = 0;
    const auto dash = job.trigger_message_id.rfind('-');
    if (dash != std::string::npos) {
        try {
            trigger_seq = std::stoll(job.trigger_message_id.substr(dash + 1));
        } catch (...) {
            trigger_seq = 0;
        }
    }

    std::vector<Message> messages;
    const int limit = cfg_.ai_context_message_limit > 0
                          ? cfg_.ai_context_message_limit
                          : 30;
    const int64_t after_seq = (job.mode == "shared") ? job.activated_seq : 0;
    if (!store_->GetRecentMessages(job.session_id, after_seq, trigger_seq,
                                   limit, &messages, err_msg)) {
        return false;
    }

    std::vector<int64_t> sender_ids;
    std::unordered_set<int64_t> seen;
    for (const auto& m : messages) {
        if (m.sender_id > 0 && seen.insert(m.sender_id).second) {
            sender_ids.push_back(m.sender_id);
        }
    }
    if (seen.insert(job.trigger_sender_id).second) {
        sender_ids.push_back(job.trigger_sender_id);
    }
    std::unordered_map<int64_t, std::string> names;
    if (!sender_ids.empty() && user_dao_) {
        std::vector<User> users;
        std::string user_err;
        if (user_dao_->ListUsersByIds(sender_ids, &users, &user_err)) {
            for (const auto& user : users) names[user.id] = user.name;
        }
    }

    nlohmann::json list = nlohmann::json::array();
    bool trigger_seen = false;
    for (const auto& m : messages) {
        if (trigger_seq > 0 && m.msg_seq > trigger_seq) continue;
        if (m.msg_type == "system" || m.msg_type == "danmaku") continue;
        std::string content = ExtractText(m.content_json);
        if (content.empty()) {
            if (m.msg_type == "image") content = "[图片]";
            else if (m.msg_type == "video") content = "[视频]";
            else if (m.msg_type == "file") content = "[文件]";
            else continue;
        }
        nlohmann::json item;
        item["message_id"] = m.msg_id;
        item["sender_id"] = m.sender_id;
        item["sender_name"] = names.count(m.sender_id)
                                  ? names[m.sender_id]
                                  : ("User " + std::to_string(m.sender_id));
        item["sender_type"] =
            (m.sender_id == job.bot_user_id || IsBot(m.sender_id)) ? "bot"
                                                                   : "human";
        item["content"] = content;
        item["timestamp_ms"] = m.timestamp_ms;
        list.push_back(std::move(item));
        if (m.msg_id == job.trigger_message_id) trigger_seen = true;
    }

    // 用户消息可能还在持久化队列里；触发消息以 ai_trigger 携带的原文为准，
    // 不能因为读库稍慢而让 AI 看不到本次问题。
    if (!trigger_seen) {
        nlohmann::json item;
        item["message_id"] = job.trigger_message_id;
        item["sender_id"] = job.trigger_sender_id;
        item["sender_name"] =
            job.trigger_sender_name.empty()
                ? ("User " + std::to_string(job.trigger_sender_id))
                : job.trigger_sender_name;
        item["sender_type"] = "human";
        item["content"] = job.trigger_content;
        item["timestamp_ms"] = NowMs();
        list.push_back(std::move(item));
    }

    nlohmann::json data;
    data["job_id"] = job.job_id;
    data["mode"] = job.mode;
    data["session_id"] = job.session_id;
    data["bot_user_id"] = job.bot_user_id;
    data["bot_name"] = "小桁";
    data["trigger_message_id"] = job.trigger_message_id;
    data["trigger_sender_id"] = job.trigger_sender_id;
    data["trigger_sender_name"] = job.trigger_sender_name;
    data["trigger_content"] = job.trigger_content;
    data["activated_seq"] = job.activated_seq;
    data["target_user_ids"] = targets;
    data["messages"] = std::move(list);
    *out = std::move(data);
    return true;
}

bool AiService::PushDelta(const std::string& job_id, const std::string& delta,
                          std::string* err_msg) {
    if (delta.empty() || !ai_dao_) return true;
    AiJob job;
    if (!ai_dao_->GetJob(job_id, &job, err_msg)) return false;
    if (job.status == "completed" || job.status == "failed" ||
        job.status == "aborted") {
        return true;
    }
    std::vector<int64_t> targets;
    if (!ResolveTargets(job, &targets, err_msg)) return false;
    nlohmann::json content;
    content["type"] = "ai_stream";
    content["job_id"] = job.job_id;
    content["session_id"] = job.session_id;
    content["bot_user_id"] = job.bot_user_id;
    content["sender_name"] = "小桁";
    content["mentioner_id"] = job.trigger_sender_id;
    content["delta"] = delta;
    return SendChatMessage(job.session_id, job.bot_user_id, "text",
                           content.dump(), 0, job_id + ":stream", targets,
                           false, err_msg);
}

bool AiService::Finalize(const std::string& job_id,
                         const std::string& content, std::string* err_msg) {
    if (!ai_dao_ || !store_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    AiJob job;
    if (!ai_dao_->GetJob(job_id, &job, err_msg)) return false;
    if (job.status == "completed") return true;
    if (job.status == "failed" || job.status == "aborted") {
        if (err_msg) *err_msg = "job is not active: " + job.status;
        return false;
    }
    if (content.empty()) {
        return Fail(job_id, "EMPTY_REPLY", "AI 返回了空内容", err_msg);
    }
    std::vector<int64_t> targets;
    if (!ResolveTargets(job, &targets, err_msg)) return false;

    int64_t seq = 0;
    if (!store_->AllocateMessageSeq(job.session_id, &seq, err_msg)) return false;
    const std::string msg_id = job.session_id + "-" + std::to_string(seq);
    nlohmann::json body;
    body["type"] = "text";
    body["session_id"] = job.session_id;
    body["content"] = content;
    body["text"] = content;
    body["ai_job_id"] = job.job_id;
    body["sender_name"] = "小桁";
    body["from_user_id"] = job.bot_user_id;
    body["mentioner_id"] = job.trigger_sender_id;
    body["streamed"] = true;
    if (!SendChatMessage(job.session_id, job.bot_user_id, "text",
                         body.dump(), seq, msg_id, targets, true, err_msg)) {
        std::string fail_err;
        Fail(job_id, "SEND_FAILED", "AI 回复落库/推送失败", &fail_err);
        return false;
    }
    return ai_dao_->CompleteJob(job_id, msg_id, seq, err_msg);
}

bool AiService::Fail(const std::string& job_id, const std::string& code,
                     const std::string& message, std::string* err_msg) {
    if (!ai_dao_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    AiJob job;
    if (!ai_dao_->GetJob(job_id, &job, err_msg)) return false;
    const std::string safe_code = code.size() > 60 ? code.substr(0, 60) : code;
    const std::string safe_message =
        message.size() > 500 ? message.substr(0, 500) : message;
    ai_dao_->FailJob(job_id, safe_code, safe_message, err_msg);
    std::vector<int64_t> targets;
    std::string target_err;
    if (!ResolveTargets(job, &targets, &target_err)) return true;
    nlohmann::json content;
    content["type"] = "ai_stream";
    content["job_id"] = job.job_id;
    content["session_id"] = job.session_id;
    content["bot_user_id"] = job.bot_user_id;
    content["sender_name"] = "小桁";
    content["mentioner_id"] = job.trigger_sender_id;
    content["error"] = {{"code", safe_code}, {"message", safe_message}};
    std::string send_err;
    SendChatMessage(job.session_id, job.bot_user_id, "text", content.dump(), 0,
                    job_id + ":error", targets, false, &send_err);
    return true;
}

bool AiService::Stop(const std::string& job_id, int64_t user_id,
                     std::string* err_msg) {
    if (!ai_dao_ || !trigger_producer_) {
        if (err_msg) *err_msg = "ai service not initialized";
        return false;
    }
    AiJob job;
    if (!ai_dao_->GetJob(job_id, &job, err_msg)) return false;
    if (job.trigger_sender_id != user_id) {
        if (err_msg) *err_msg = "only the mentioner can stop this reply";
        return false;
    }
    if (job.status == "completed" || job.status == "failed" ||
        job.status == "aborted") {
        return true;
    }
    nlohmann::json stop;
    stop["type"] = "ai_stop";
    stop["job_id"] = job.job_id;
    stop["session_id"] = job.session_id;
    stop["requested_by"] = user_id;
    stop["created_at_ms"] = NowMs();
    if (!trigger_producer_->Send(job_id, stop.dump())) {
        if (err_msg) *err_msg = "send ai_stop failed";
        return false;
    }
    ai_dao_->AbortJob(job_id, err_msg);

    // 让 A/B 双方都结束占位气泡；stop 本身不落历史消息。
    std::vector<int64_t> targets;
    std::string target_err;
    if (ResolveTargets(job, &targets, &target_err)) {
        nlohmann::json content;
        content["type"] = "ai_stream";
        content["job_id"] = job.job_id;
        content["session_id"] = job.session_id;
        content["bot_user_id"] = job.bot_user_id;
        content["sender_name"] = "小桁";
        content["mentioner_id"] = job.trigger_sender_id;
        content["stopped"] = true;
        std::string send_err;
        SendChatMessage(job.session_id, job.bot_user_id, "text",
                        content.dump(), 0, job_id + ":stop", targets, false,
                        &send_err);
    }
    return true;
}

}  // namespace sparkpush
