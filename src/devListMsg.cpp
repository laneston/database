

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "devListMsg.h"
#include "json.hpp"
#include "log_manager.hpp"

using json = nlohmann::json;

// ---------- 辅助函数：字节数组转十六进制字符串 ----------
static std::string bytesToHexString(const std::vector<uint8_t>& bytes)
{
  std::string hex;
  hex.reserve(bytes.size() * 2);
  char buf[3];
  for (uint8_t b : bytes) {
    snprintf(buf, sizeof(buf), "%02X", b);
    hex += buf;
  }
  return hex;
}

// ---------- 读写锁实现 ----------
void DevListMsg::lockRead() const
{
  std::unique_lock<std::mutex> lock(mtx_);
  cv_.wait(lock, [this] { return !writerWaiting_; });
  ++readers_;
}

void DevListMsg::unlockRead() const
{
  std::lock_guard<std::mutex> lock(mtx_);
  if (--readers_ == 0) { cv_.notify_all(); }
}

void DevListMsg::lockWrite()
{
  std::unique_lock<std::mutex> lock(mtx_);
  cv_.wait(lock, [this] { return readers_ == 0 && !writerWaiting_; });
  writerWaiting_ = true;
}

void DevListMsg::unlockWrite()
{
  std::lock_guard<std::mutex> lock(mtx_);
  writerWaiting_ = false;
  cv_.notify_all();
}

// ---------- 构造函数/析构函数 ----------
DevListMsg::DevListMsg() : readers_(0), writerWaiting_(false) { memset(cachedDevAddr_, 0, sizeof(cachedDevAddr_)); }

DevListMsg::~DevListMsg() {}

std::string DevListMsg::getDevAddrString() const
{
  static const char* HEX = "0123456789ABCDEF";
  std::string s;
  s.reserve(12);
  for (int i = 0; i < 6; ++i) {
    s.push_back(HEX[(cachedDevAddr_[i] >> 4) & 0x0F]);
    s.push_back(HEX[cachedDevAddr_[i] & 0x0F]);
  }
  return s;
}

// ---------- 加载设备地址 ----------
bool DevListMsg::loadDevAddrFromConfig(const std::string& path)
{
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG_ERROR("Failed to open config file: " + path);
    return false;
  }

  try {
    json root;
    file >> root;

    if (!root.contains("address") || !root["address"].is_string()) {
      LOG_ERROR("Config missing 'address' field");
      return false;
    }

    std::string addrStr = root["address"].get<std::string>();
    if (addrStr.length() != 12) {
      LOG_ERROR("Invalid address length: " + addrStr);
      return false;
    }

    // 转换为 6 字节数组
    uint8_t addr[6];
    for (size_t i = 0; i < 6; ++i) {
      std::string byteStr = addrStr.substr(i * 2, 2);
      addr[i] = static_cast<uint8_t>(std::stoi(byteStr, nullptr, 16));
    }
    memcpy(cachedDevAddr_, addr, 6);
    LOG_INFO("Loaded device address: " + addrStr);
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception: " + std::string(e.what()));
    return false;
  }
}

// ---------- 获取缓存的设备地址 ----------
const uint8_t* DevListMsg::getDevAddr() const { return cachedDevAddr_; }

// ---------- 加载点表信息 ----------
bool DevListMsg::loadFromModbusMap(const std::string& path)
{
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG_ERROR("Failed to open modbusmap file: " + path);
    return false;
  }

  // 从机模式下 address 可为空，用全零占位（此时 devAddr 为 "000000000000"）
  bool hasAddr = false;
  for (int i = 0; i < 6; ++i) {
    if (cachedDevAddr_[i] != 0) {
      hasAddr = true;
      break;
    }
  }
  if (!hasAddr) {
    LOG_WARN(
      "Device address not loaded; devAddr will be all-zero placeholder "
      "(OK in Slave mode, must NOT happen in Master mode).");
  }

  try {
    json root;
    file >> root;

    if (!root.contains("collection_channels") || !root["collection_channels"].is_object()) {
      LOG_ERROR("Invalid modbusmap: missing 'collection_channels'");
      return false;
    }

    // 统一使用缓存的设备地址（12 位十六进制字符串）
    std::string addrHex = getDevAddrString();

    std::vector<RegisterMap> newMap;

    const auto& channels = root["collection_channels"];
    // 遍历所有通道，不再硬编码 CH01/CH02
    for (auto it = channels.begin(); it != channels.end(); ++it) {
      const std::string& chName = it.key();
      const auto& channelData = it.value();
      if (!channelData.is_array()) {
        LOG_WARN("Channel " + chName + " is not an array, skipping");
        continue;
      }

      LOG_INFO("Processing channel: " + chName);

      for (const auto& device : channelData) {
        if (!device.contains("register_mapping") || !device["register_mapping"].is_array()) {
          LOG_WARN("Device in " + chName + " missing 'register_mapping', skipping");
          continue;
        }
        for (const auto& reg : device["register_mapping"]) {
          if (!reg.contains("local_point_table_addr") || !reg["local_point_table_addr"].is_number_integer()
              || !reg.contains("description") || !reg["description"].is_string()) {
            LOG_WARN("Invalid register mapping entry, skipping");
            continue;
          }

          RegisterMap entry;
          entry.devAddr = addrHex;
          entry.registerAddr = static_cast<uint16_t>(reg["local_point_table_addr"].get<int>());
          entry.value = 0;
          entry.timestamp_unix = 0; // 初始化时间戳为 0
          entry.describe = reg["description"].get<std::string>();
          newMap.push_back(std::move(entry));
        }
      }
    }

    lockWrite();
    map_ = std::move(newMap);
    size_t n = map_.size();
    unlockWrite();

    LOG_INFO("Loaded " + std::to_string(n) + " register mappings from modbusmap");
    return true;
  } catch (const json::parse_error& e) {
    LOG_ERROR("JSON parse error: " + std::string(e.what()));
    return false;
  } catch (const std::exception& e) {
    LOG_ERROR("Exception: " + std::string(e.what()));
    return false;
  }
}

bool DevListMsg::updateRegister(uint16_t registerAddr, uint16_t value, uint64_t timestamp_unix,
                                const std::string& describe)
{
  lockWrite();
  for (auto& entry : map_) {
    if (entry.registerAddr == registerAddr) {
      entry.value = value;
      if (timestamp_unix > 0) entry.timestamp_unix = timestamp_unix;
      if (!describe.empty()) entry.describe = describe;
      unlockWrite();
      return true;
    }
  }
  unlockWrite();
  return false;
}

// ---------- 获取点表数量 ----------
size_t DevListMsg::size() const
{
  lockRead();
  size_t s = map_.size();
  unlockRead();
  return s;
}

// ---------- 获取指定索引的点表信息 ----------
bool DevListMsg::getRegisterMap(size_t index, RegisterMap& out) const
{
  lockRead();
  if (index >= map_.size()) {
    unlockRead();
    return false;
  }
  out = map_[index];
  unlockRead();
  return true;
}

// ---------- 更新寄存器值 ----------
bool DevListMsg::updateValue(uint16_t registerAddr, uint16_t value)
{
  lockWrite();
  bool found = false;
  for (auto& entry : map_) {
    if (entry.registerAddr == registerAddr) {
      entry.value = value;
      found = true;
      LOG_DEBUG("Updated register 0x" + std::to_string(registerAddr) + " to " + std::to_string(value));
      break;
    }
  }
  unlockWrite();
  return found;
}

void DevListMsg::addRegisterMap(const std::string& devAddr, uint16_t registerAddr, const std::string& describe)
{
  lockWrite();
  RegisterMap entry;
  entry.devAddr = devAddr;
  entry.registerAddr = registerAddr;
  entry.value = 0;
  entry.describe = describe;
  entry.timestamp_unix = 0; // 新增
  map_.push_back(std::move(entry));
  unlockWrite();
  LOG_DEBUG("Added register map: addr=" + devAddr + ", reg=" + std::to_string(registerAddr) + ", desc=" + describe);
}

// ---------- 清空指定设备的所有映射 ----------
void DevListMsg::clearByAddr(const std::string& devAddr)
{
  lockWrite();
  auto newEnd = std::remove_if(map_.begin(), map_.end(),
                               [&devAddr](const RegisterMap& entry) { return entry.devAddr == devAddr; });
  map_.erase(newEnd, map_.end());
  unlockWrite();
  LOG_DEBUG("DevListMsg cleared entries for addr: " + devAddr);
}

// ---------- 清空所有数据 ----------
void DevListMsg::clear()
{
  lockWrite();
  map_.clear();
  unlockWrite();
  LOG_DEBUG("DevListMsg cleared");
}

void DevListMsg::addRegisterMapEntry(const RegisterMap& entry)
{
  lockWrite();
  map_.push_back(entry);
  unlockWrite();
  LOG_DEBUG("Add full register entry: devAddr=" + entry.devAddr + ", regAddr=" + std::to_string(entry.registerAddr)
            + ", value=" + std::to_string(entry.value) + ", ts_unix=" + std::to_string(entry.timestamp_unix));
}

void DevListMsg::setRegisterMaps(const std::vector<RegisterMap>& maps)
{
  lockWrite();
  map_ = maps;
  unlockWrite();
}

void DevListMsg::setRegisterMaps(std::vector<RegisterMap>&& maps)
{
  lockWrite();
  map_ = std::move(maps);
  unlockWrite();
}