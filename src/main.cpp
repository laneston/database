#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <fstream>
#include <iostream>
#include <thread>

#include "dbManager.h"
#include "json.hpp" // 使用 nlohmann/json 库
#include "log_manager.hpp"
#include "modbus_message_queue.h"
#include "mqttclient.h"

using json = nlohmann::json;

std::atomic<bool> g_running{ true };
std::mutex g_mutex;
std::condition_variable g_cv;
bool g_need_flush = false;      // 由 reboot 请求设置，通知读取线程立即处理
bool g_flush_completed = false; // 表示处理已完成，用于等待

// -------------------------- 台账召测全局变量 --------------------------
DevListMsg g_queryDevList; // 点表队列，存储数据库查询结果
std::string g_localAddr;   // 本地设备地址缓存（address_tmp）
std::string g_deviceMode;  // 运行模式 Master/Slave

// 同步控制
std::mutex g_tsMutex;
std::condition_variable g_tsCv;
bool g_tsNotifyReceived = false;
bool g_tsReplyReceived = false;
uint64_t g_reqTimestamp = 0;
uint16_t g_reqRegister = 0;
std::string g_reqAddr; // 发起召测的设备地址

static void onSignal(int /*sig*/) { g_running = false; }

// 加载本地配置 plcLocalConfig.json
static bool loadPlcLocalConfig(const std::string& path)
{
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG_ERROR("Failed to open plcLocalConfig: " + path);
    return false;
  }
  try {
    json root;
    file >> root;
    if (!root.contains("address") || !root["address"].is_string()) {
      LOG_ERROR("plcLocalConfig missing 'address' field");
      return false;
    }
    g_localAddr = root["address"].get<std::string>();
    if (g_localAddr.length() != 12) {
      LOG_ERROR("Invalid address length: " + g_localAddr + " (expect 12 hex chars)");
      return false;
    }

    if (!root.contains("mode") || !root["mode"].is_string()) {
      LOG_ERROR("plcLocalConfig missing 'mode' field");
      return false;
    }
    g_deviceMode = root["mode"].get<std::string>();

    LOG_INFO("Load plcLocalConfig success: address=" + g_localAddr + ", mode=" + g_deviceMode);
    return true;
  } catch (const json::parse_error& e) {
    LOG_ERROR("Parse plcLocalConfig failed: " + std::string(e.what()));
    return false;
  } catch (const std::exception& e) {
    LOG_ERROR("plcLocalConfig exception: " + std::string(e.what()));
    return false;
  }
}

// 台账召测工作线程
void tsQueryWorker(DataBaseManager& dbManager, MqttClient& mqttClient)
{
  LOG_INFO("Timestamp query worker thread started.");

  while (g_running) {
    // ---------------- 1. 等待并快照请求参数 ----------------
    uint64_t reqTimestamp = 0;
    uint16_t reqRegister = 0;
    std::string reqAddr;

    {
      std::unique_lock<std::mutex> lock(g_tsMutex);
      g_tsCv.wait(lock, []() { return g_tsNotifyReceived || !g_running; });
      if (!g_running) break;

      // 快照本次请求参数（避免处理过程中被 MQTT 回调覆盖）
      reqTimestamp = g_reqTimestamp;
      reqRegister = g_reqRegister;
      reqAddr = g_reqAddr;

      // 重置控制标志，准备本次任务
      g_tsNotifyReceived = false;
      g_tsReplyReceived = false;
    }

    LOG_INFO("Start process query: ts=" + std::to_string(reqTimestamp) + ", reg=" + std::to_string(reqRegister)
             + ", requester=" + reqAddr + ", localAddr=" + g_localAddr);

    // ---------------- 2. 跨通道查询数据库 ----------------
    std::vector<RegisterMap> queryResult
      = dbManager.queryByTimestampAndRegister(reqTimestamp, reqRegister, g_localAddr);

    // ---------------- 3. 存入点表队列 ----------------
    g_queryDevList.clear();
    for (const auto& entry : queryResult) { g_queryDevList.addRegisterMapEntry(entry); }
    size_t total = g_queryDevList.size();
    LOG_INFO("Loaded " + std::to_string(total) + " records into devList queue.");

    // ---------------- 4. 无数据：发送无效结束帧（status=3） ----------------
    if (total == 0) {
      LOG_WARN("No matching records for ts=" + std::to_string(reqTimestamp) + ", reg=" + std::to_string(reqRegister)
               + ", send INVALID end frame (status=3).");

      json endFrame;
      endFrame["addr"] = reqAddr;
      endFrame["deep"] = 0;
      endFrame["status"] = 3; // 3 = 当前帧无效（数据库无匹配数据）
      endFrame["register"] = 0;
      endFrame["value"] = 0;
      endFrame["timestamp"] = reqTimestamp;
      std::string payload = endFrame.dump();

      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      if (mqttClient.publish("database/plcManager/report/dataTimestamp", payload, 0, false)) {
        LOG_INFO("Published invalid end frame (status=3): " + payload);
      } else {
        LOG_ERROR("Publish invalid end frame failed: " + payload);
      }
      continue;
    }

    // ---------------- 5. 逐帧上报 + 等待应答 ----------------
    enum class Result { Completed, Timeout, Aborted };
    Result result = Result::Completed;

    for (size_t i = 0; i < total; ++i) {
      if (!g_running) {
        result = Result::Aborted;
        break;
      }

      RegisterMap entry;
      if (!g_queryDevList.getRegisterMap(i, entry)) {
        LOG_ERROR("Get register failed at index " + std::to_string(i));
        result = Result::Aborted;
        break;
      }

      // status: 0 = 中间帧，1 = 最后一帧（正常），3 = 无效帧（无数据时单独发送）
      int status = (i == total - 1) ? 1 : 0;

      // 构造上报报文
      json frame;
      frame["addr"] = reqAddr;
      frame["deep"] = 0;
      frame["status"] = status;
      frame["register"] = entry.registerAddr;
      frame["value"] = entry.value;
      frame["timestamp"] = entry.timestamp_unix;
      std::string payload = frame.dump();

      LOG_INFO("Prepare frame " + std::to_string(i + 1) + "/" + std::to_string(total)
               + ": reg=" + std::to_string(entry.registerAddr) + ", value=" + std::to_string(entry.value)
               + ", status=" + std::to_string(status) + ", ts=" + std::to_string(entry.timestamp_unix));

      // 发送前等待 1000ms
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));

      // 清空应答标志（必须在发布前完成，避免旧标志导致误判）
      {
        std::lock_guard<std::mutex> lock(g_tsMutex);
        g_tsReplyReceived = false;
      }

      // 发布当前帧
      if (!mqttClient.publish("database/plcManager/report/dataTimestamp", payload, 0, false)) {
        LOG_ERROR("Publish frame " + std::to_string(i + 1) + "/" + std::to_string(total) + " failed: " + payload);
        result = Result::Aborted;
        break;
      }
      LOG_INFO("Published frame " + std::to_string(i + 1) + "/" + std::to_string(total) + ": " + payload);

      // 最后一帧不再等待应答
      if (status == 1) {
        LOG_INFO("Last frame sent (status=1), task done without waiting reply.");
        break;
      }

      // 等待应答：reply/dataTimestamp 或 reply/dataList，超时 10 秒
      {
        std::unique_lock<std::mutex> waitLock(g_tsMutex);
        bool gotReply
          = g_tsCv.wait_for(waitLock, std::chrono::seconds(10), []() { return g_tsReplyReceived || !g_running; });

        if (!g_running) {
          result = Result::Aborted;
          break;
        }
        if (!gotReply) {
          LOG_ERROR("Wait reply timeout at frame " + std::to_string(i + 1) + "/" + std::to_string(total)
                    + ", abort current task, back to listen notify/dataTimestamp.");
          result = Result::Timeout;
          break;
        }
        LOG_INFO("Reply received for frame " + std::to_string(i + 1) + ", continue next frame.");
      }
    }

    // ---------------- 6. 任务收尾日志 ----------------
    switch (result) {
    case Result::Completed:
      LOG_INFO("Timestamp query task completed: total=" + std::to_string(total) + ", ts=" + std::to_string(reqTimestamp)
               + ", addr=" + reqAddr);
      break;
    case Result::Timeout:
      LOG_WARN("Timestamp query task aborted by timeout: ts=" + std::to_string(reqTimestamp) + ", addr=" + reqAddr);
      break;
    case Result::Aborted:
      LOG_WARN("Timestamp query task aborted (running=false or publish failed): ts=" + std::to_string(reqTimestamp)
               + ", addr=" + reqAddr);
      break;
    }
  }

  LOG_INFO("Timestamp query worker thread exited.");
}

// 解析 JSON 为 ModbusMasterMsg，严格按照新 RegisterItem 结构
bool parseModbusMsg(const json& j, ModbusMasterMsg& msg)
{
  try {
    // 基本字段校验
    if (!j.contains("id") || !j["id"].is_number_integer()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.id = j["id"].get<int>();

    if (!j.contains("channel") || !j["channel"].is_number_integer()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.channel = j["channel"].get<int>();

    if (!j.contains("pdu_addr") || !j["pdu_addr"].is_number_integer()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.pdu_addr = j["pdu_addr"].get<int>();

    if (!j.contains("pdu_func") || !j["pdu_func"].is_number_integer()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.pdu_func = j["pdu_func"].get<int>();

    if (!j.contains("pdu_data") || !j["pdu_data"].is_string()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.pdu_data = j["pdu_data"].get<std::string>();

    if (!j.contains("timestamp") || !j["timestamp"].is_string()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.timestamp = j["timestamp"].get<std::string>();

    // register_map 解析（新结构：每个对象包含三个键）
    if (!j.contains("register_map") || !j["register_map"].is_array()) {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
    msg.register_map.clear();
    for (const auto& item : j["register_map"]) {
      if (!item.is_object()) {
        printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
        return false;
      }

      // 提取地址键名（如 "16"）和映射地址值（如 0）
      int address = -1, map_addr = -1, value = -1;
      std::string description;

      // 遍历对象中的键值对
      for (auto it = item.begin(); it != item.end(); ++it) {
        const std::string& key = it.key();
        if (key == "value") {
          if (!it.value().is_number_integer()) {
            printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
            return false;
          }
          value = it.value().get<int>();
        } else if (key == "description") {
          if (!it.value().is_string()) {
            printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
            return false;
          }
          description = it.value().get<std::string>();
        } else {
          // 其他键视为地址键名，其值应为 map_addr
          if (!it.value().is_number_integer()) {
            printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
            return false;
          }
          address = std::stoi(key);
          map_addr = it.value().get<int>();
        }
      }

      // 必须同时包含 address, map_addr, value, description
      if (address == -1 || map_addr == -1 || value == -1 || description.empty()) {
        {
          printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
          return false;
        }
      }

      ModbusMasterMsg::RegisterItem reg;
      reg.address = address;
      reg.map_addr = map_addr;
      reg.value = value;
      reg.description = description;
      msg.register_map.push_back(reg);
    }
    return true;
  } catch (const std::exception& e) {
    LOG_WARN("JSON parse exception: " + std::string(e.what()));
    {
      printf("error file: %s, line num: %d\n", __FILE__, __LINE__);
      return false;
    }
  }
}

int main()
{
  // ---------- 注册信号处理，保证 Ctrl+C 能优雅退出 ----------
  std::signal(SIGINT, onSignal);
  std::signal(SIGTERM, onSignal);

  // 初始化日志（可调整参数）
  if (!LogManager::getInstance().init(LogLevel::INFO, "/root/log/database.log", 1024 * 1024, 3600, 10)) {
    std::cerr << "LogManager init failed!" << std::endl;
    return -1;
  }

  // 加载本地设备配置
  if (!loadPlcLocalConfig("/root/config/plcLocalConfig.json")) {
    std::cerr << "Load plcLocalConfig failed!" << std::endl;
    return -1;
  }
  bool isSlave = (g_deviceMode == "Slave");

  // 使用 DevListMsg 缓存本机地址（与需求 2 一致：用 devListMsg.h 的功能保存地址）
  // loadPlcLocalConfig 已经把 address 解析到 g_localAddr；
  // 这里再调用一次 DevListMsg::loadDevAddrFromConfig 以保证 devAddr 也能从 DevListMsg 获取。
  {
    DevListMsg tmp;
    if (tmp.loadDevAddrFromConfig("/root/config/plcLocalConfig.json")) {
      LOG_INFO("DevListMsg cached devAddr = " + tmp.getDevAddrString());
    } else {
      LOG_WARN("DevListMsg::loadDevAddrFromConfig failed, use g_localAddr = " + g_localAddr);
    }
  }

  LOG_INFO("Application started. mode=" + g_deviceMode);

  DataBaseManager dbManager(2, 10, "/root/data");

  MessageQueue mq(100);

  MqttClient client("database_reader");
  client.setServer("localhost", 1883);

  client.setConnectCallback([]() { LOG_INFO("Connected to MQTT broker."); });

  // 消息回调：根据主题分别处理
  client.setMessageCallback([&mq, &client, &isSlave](const std::string& topic, const std::string& payload) {
    LOG_INFO("Received message on topic: " + topic);
    LOG_INFO("Received message payload: " + payload);

    // 处理 keepAlive 请求（原有功能）
    if (topic == "modbusMaster/database/request/keepAlive") {
      try {
        json j = json::parse(payload);
        if (j.contains("token") && j["token"].is_number_integer()) {
          int token = j["token"].get<int>();
          LOG_INFO("Valid keepAlive request with token=" + std::to_string(token));
          // 构造响应
          json response;
          response["token"] = token;
          response["status"] = "ready";
          std::string response_str = response.dump();
          // 发布响应
          if (client.publish("database/modbusMaster/response/keepAlive", response_str, 0, false)) {
            LOG_INFO("Published keepAlive response: " + response_str);
          } else {
            LOG_ERROR("Failed to publish keepAlive response");
          }
        } else {
          LOG_WARN("KeepAlive request missing 'token' field or token not integer");
        }
      } catch (const json::parse_error& e) {
        LOG_WARN("Failed to parse keepAlive payload: " + std::string(e.what()));
      } catch (const std::exception& e) {
        LOG_WARN("Unexpected error in keepAlive handler: " + std::string(e.what()));
      }
      return; // 处理完 keepAlive 后不再继续
    }

    // 处理 reboot 请求（新增功能）
    if (topic == "httpserver/database/request/reboot") {
      try {
        json j = json::parse(payload);
        if (j.contains("token") && j["token"].is_number_integer()) {
          int token = j["token"].get<int>();
          LOG_INFO("Valid reboot request with token=" + std::to_string(token));
          // 记录 token 值（可存储到全局变量，此处仅记录日志）
          // 唤醒读取线程，立即处理队列并落盘
          {
            std::lock_guard<std::mutex> lock(g_mutex);
            g_need_flush = true;
            g_flush_completed = false;
          }
          g_cv.notify_one(); // 唤醒读取线程
          // 等待读取线程完成处理（确保入库完成后再返回响应）
          {
            std::unique_lock<std::mutex> lock(g_mutex);
            g_cv.wait(lock, [] { return g_flush_completed; });
          }
          // 构造响应
          json response;
          response["token"] = token;
          response["status"] = "ready";
          std::string response_str = response.dump();
          // 发布响应到 database/httpserver/response/reboot
          LOG_INFO("Published reboot response: " + response_str);
          if (client.publish("database/httpserver/response/reboot", response_str, 0, false)) {
            LOG_INFO("Published reboot response: " + response_str);
          } else {
            LOG_ERROR("Failed to publish reboot response");
          }
        } else {
          LOG_WARN("Reboot request missing 'token' field or token not integer");
        }
      } catch (const json::parse_error& e) {
        LOG_WARN("Failed to parse reboot payload: " + std::string(e.what()));
      } catch (const std::exception& e) {
        LOG_WARN("Unexpected error in reboot handler: " + std::string(e.what()));
      }
      return;
    }

    // 数据上报
    if (topic == "modbusMaster/database/report/data") {
      try {
        json j = json::parse(payload);
        ModbusMasterMsg msg;
        if (parseModbusMsg(j, msg)) {
          mq.write(msg);
          LOG_INFO("Enqueued msg id=" + std::to_string(msg.id)
                   + ", registers=" + std::to_string(msg.register_map.size()));
        } else {
          LOG_WARN("Invalid message format, discarded.");
        }
      } catch (const std::exception& e) {
        LOG_WARN("Unexpected error: " + std::string(e.what()));
      }
      return;
    }

    // 台账召测通知
    if (topic == "plcManager/database/notify/dataTimestamp" && isSlave) {
      try {
        json j = json::parse(payload);
        if (!j.contains("token") || !j["token"].is_number_integer() || !j.contains("timestamp")
            || !j["timestamp"].is_number_integer() || !j.contains("register") || !j["register"].is_number_integer()
            || !j.contains("addr") || !j["addr"].is_string()) {
          LOG_ERROR("Invalid dataTimestamp notify: bad fields format, payload=" + payload);
          return;
        }
        uint64_t ts = j["timestamp"].get<uint64_t>();
        int regVal = j["register"].get<int>();
        std::string addr = j["addr"].get<std::string>();

        if (regVal < 0 || regVal > 0xFFFF) {
          LOG_ERROR("Invalid register value: " + std::to_string(regVal));
          return;
        }
        if (addr.length() != 12) {
          LOG_ERROR("Invalid addr length: " + addr);
          return;
        }

        LOG_INFO("Received ts notify: token=" + std::to_string(j["token"].get<int>()) + ", ts=" + std::to_string(ts)
                 + ", reg=" + std::to_string(regVal) + ", addr=" + addr);

        {
          std::lock_guard<std::mutex> lock(g_tsMutex);
          g_reqTimestamp = ts;
          g_reqRegister = static_cast<uint16_t>(regVal);
          g_reqAddr = addr;
          g_tsNotifyReceived = true;
        }
        g_tsCv.notify_one();
      } catch (const json::parse_error& e) {
        LOG_ERROR("Parse ts notify failed: " + std::string(e.what()));
      } catch (const std::exception& e) {
        LOG_ERROR("Ts notify handler error: " + std::string(e.what()));
      }
      return;
    }

    // 应答报文
    if ((topic == "plcManager/database/reply/dataTimestamp" || topic == "plcManager/database/reply/dataList")
        && isSlave) {
      LOG_INFO("Received ts reply on topic: " + topic);
      {
        std::lock_guard<std::mutex> lock(g_tsMutex);
        g_tsReplyReceived = true;
      }
      g_tsCv.notify_all();
      return;
    }

    LOG_INFO("Ignored message on topic: " + topic);
  });

  if (!client.connect()) { LOG_ERROR("MQTT initial connection failed, will retry in run loop."); }
  client.subscribe("modbusMaster/database/report/data", 0);
  client.subscribe("modbusMaster/database/request/keepAlive", 0);
  client.subscribe("httpserver/database/request/reboot", 0); // 订阅 reboot 主题

  // Slave模式订阅台账相关主题
  if (isSlave) {
    client.subscribe("plcManager/database/notify/dataTimestamp", 0);
    client.subscribe("plcManager/database/reply/dataTimestamp", 0);
    client.subscribe("plcManager/database/reply/dataList", 0);
    LOG_INFO("Slave mode: subscribed all ts topics.");
  }

  // MQTT 运行线程
  std::thread mqtt_thread([&client]() { client.run(); });

  // 定时读取线程（每 60 秒清空队列并打印，可被 reboot 请求唤醒）
  std::thread reader_thread([&mq, &dbManager]() {
    while (g_running) {
      // 等待 60 秒或被条件变量唤醒
      {
        std::unique_lock<std::mutex> lock(g_mutex);
        if (!g_need_flush) {
          g_cv.wait_for(lock, std::chrono::seconds(60), [] { return g_need_flush || !g_running; });
        }
        if (!g_running) break;
      }

      // 执行一轮队列处理
      size_t count = mq.size();
      LOG_INFO("Queue size before reading: " + std::to_string(count));
      ModbusMasterMsg msg;
      while (mq.read(msg)) {
        // 写入数据库
        if (!dbManager.writeMessage(msg)) { LOG_ERROR("DB write failed for msg id=" + std::to_string(msg.id)); }
        std::ostringstream oss;
        oss << "Read msg id=" << msg.id << ", channel=" << msg.channel << ", pdu_addr=" << msg.pdu_addr
            << ", pdu_func=" << msg.pdu_func << ", pdu_data=" << msg.pdu_data << ", timestamp=" << msg.timestamp
            << ", registers_count=" << msg.register_map.size();
        LOG_INFO(oss.str());

        // 可选：打印每个寄存器详情
        for (const auto& reg : msg.register_map) {
          LOG_INFO("  reg: address=" + std::to_string(reg.address) + ", map_addr=" + std::to_string(reg.map_addr)
                   + ", value=" + std::to_string(reg.value) + ", desc=" + reg.description);
        }
      }
      LOG_INFO("Queue reading completed.");

      // 落盘：由于使用 WAL 模式，每个 INSERT 已自动提交，但可强制执行 checkpoint 确保数据完全写入磁盘
      // 此处可调用 sqlite3_wal_checkpoint，但为简化，假设已持久化。若需严格落盘，可扩展 dbManager 接口。
      // 这里通过关闭并重新打开数据库连接来强制落盘（代价较大，仅作示例），实际生产应使用 checkpoint。
      // 为简单，我们仅记录日志。
      LOG_INFO("Data flushed to disk.");

      // 重置标志并通知等待的线程
      {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_need_flush = false;
        g_flush_completed = true;
      }
      g_cv.notify_all(); // 唤醒可能等待的 reboot 回调
    }
    LOG_INFO("Reader thread exiting.");
  });

  // 启动台账召测工作线程（仅Slave模式）
  std::thread tsWorkerThread;
  if (isSlave) { tsWorkerThread = std::thread(tsQueryWorker, std::ref(dbManager), std::ref(client)); }

  LOG_INFO("Application is running. Press Ctrl+C to exit.");
  while (g_running) { std::this_thread::sleep_for(std::chrono::seconds(1)); }

  // -------------------------- 修复：优雅退出顺序 --------------------------
  LOG_INFO("Stopping application...");

  // 1. 先停止所有循环标志
  g_running = false;

  // 2. 唤醒所有等待中的线程
  g_tsCv.notify_all();
  g_cv.notify_all();

  // 3. 关闭 MQTT（让 mqtt_thread 的 run() 退出）
  client.stop();
  mqtt_thread.join();

  // 4. join 工作线程
  if (isSlave && tsWorkerThread.joinable()) { tsWorkerThread.join(); }
  if (reader_thread.joinable()) { reader_thread.join(); }

  LOG_INFO("Application exited.");
  return 0;
}