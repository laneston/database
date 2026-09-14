
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <sqlite3.h>
#include <sstream>
#include <sys/stat.h>

#include "dbManager.h"
#include "log_manager.hpp" // 假设已有日志宏 LOG_INFO/LOG_ERROR

// Unix秒级时间戳 -> 数据库timestamp格式字符串（YYYYMMDDHHMMSS）
static std::string unixToDbTs(uint64_t unixSec)
{
  time_t t = static_cast<time_t>(unixSec);
  struct tm tmBuf;
  if (localtime_r(&t, &tmBuf) == nullptr) { return ""; }
  char buf[32] = { 0 };
  strftime(buf, sizeof(buf), "%Y%m%d%H%M%S", &tmBuf);
  return std::string(buf);
}

// 辅助：确保目录存在
static bool ensureDir(const std::string& dir)
{
  struct stat st;
  if (stat(dir.c_str(), &st) == 0) { return S_ISDIR(st.st_mode); }
  // 递归创建父目录
  size_t pos = dir.find_last_of('/');
  if (pos != std::string::npos) {
    std::string parent = dir.substr(0, pos);
    if (!ensureDir(parent)) return false;
  }
  return (mkdir(dir.c_str(), 0755) == 0);
}

DataBaseManager::DataBaseManager(int maxChannels, int maxTablesPerDb, const std::string& dbDir)
  : maxChannels_(maxChannels), maxTablesPerDb_(maxTablesPerDb), dbDir_(dbDir)
{
  // 确保目录存在
  if (!dbDir_.empty() && dbDir_ != "." && !ensureDir(dbDir_)) {
    LOG_ERROR("Failed to create database directory: " + dbDir_);
  }

  // 预分配句柄数组，初始化为 nullptr
  dbHandles_.resize(maxChannels_, nullptr);
}

DataBaseManager::~DataBaseManager()
{
  std::lock_guard<std::mutex> lock(mutex_);
  for (sqlite3* db : dbHandles_) {
    if (db) { sqlite3_close(db); }
  }
}

sqlite3* DataBaseManager::getDbHandle(int channel)
{
  if (channel < 1 || channel > maxChannels_) {
    LOG_ERROR("Invalid channel: " + std::to_string(channel) + ", max channels: " + std::to_string(maxChannels_));
    return nullptr;
  }

  int idx = channel - 1;
  std::lock_guard<std::mutex> lock(mutex_);
  if (dbHandles_[idx] != nullptr) { return dbHandles_[idx]; }

  // 打开数据库文件
  std::string dbPath = getDbPath(channel);
  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(dbPath.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Failed to open database " + dbPath + ": " + sqlite3_errmsg(db));
    if (db) sqlite3_close(db);
    return nullptr;
  }

  // 设置WAL模式以提高并发性（可选）
  char* errMsg = nullptr;
  rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errMsg);
  if (rc != SQLITE_OK) {
    LOG_WARN("Set WAL mode failed: " + std::string(errMsg));
    sqlite3_free(errMsg);
  }

  dbHandles_[idx] = db;
  LOG_INFO("Opened database: " + dbPath);
  return db;
}

std::string DataBaseManager::getDateFromTimestamp(const std::string& timestamp) const
{
  // timestamp 格式为 YYYYMMDDHHMMSS，取前8位
  if (timestamp.size() >= 8) { return timestamp.substr(0, 8); }
  LOG_WARN("Invalid timestamp format: " + timestamp);
  return "";
}

std::vector<std::string> DataBaseManager::getAllTableNames(sqlite3* db)
{
  std::vector<std::string> tables;
  const char* sql
    = "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB '20[0-9][0-9][0-1][0-9][0-3][0-9]' ORDER BY "
      "name ASC;";
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Failed to prepare table list: " + std::string(sqlite3_errmsg(db)));
    return tables;
  }

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const unsigned char* name = sqlite3_column_text(stmt, 0);
    if (name) { tables.emplace_back(reinterpret_cast<const char*>(name)); }
  }
  sqlite3_finalize(stmt);
  return tables;
}

void DataBaseManager::cleanOldTables(sqlite3* db)
{
  std::vector<std::string> tables = getAllTableNames(db);
  if (tables.size() <= static_cast<size_t>(maxTablesPerDb_)) { return; }

  // tables 已按名称升序，最早的表在前面
  int toDelete = tables.size() - maxTablesPerDb_;
  for (int i = 0; i < toDelete; ++i) {
    std::string dropSql = "DROP TABLE IF EXISTS \"" + tables[i] + "\";";
    char* errMsg = nullptr;
    int rc = sqlite3_exec(db, dropSql.c_str(), nullptr, nullptr, &errMsg);
    if (rc != SQLITE_OK) {
      LOG_ERROR("Failed to drop table " + tables[i] + ": " + std::string(errMsg));
      sqlite3_free(errMsg);
    } else {
      LOG_INFO("Dropped old table: " + tables[i]);
    }
  }
}

bool DataBaseManager::createTable(sqlite3* db, const std::string& date)
{
  std::string sql = "CREATE TABLE IF NOT EXISTS \"" + date + "\" ("
                      "id INTEGER, "
                      "pdu_addr INTEGER, "
                      "pdu_func INTEGER, "
                      "pdu_data TEXT, "
                      "timestamp TEXT, "
                      "address INTEGER, "
                      "map_addr INTEGER, "
                      "value INTEGER, "
                      "description TEXT"
                      ");";
  char* errMsg = nullptr;
  int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Failed to create table " + date + ": " + std::string(errMsg));
    sqlite3_free(errMsg);
    return false;
  }
  return true;
}

bool DataBaseManager::ensureTableExists(sqlite3* db, const std::string& date)
{
  // 检查表是否存在
  std::string checkSql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, checkSql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Prepare check table failed: " + std::string(sqlite3_errmsg(db)));
    return false;
  }
  rc = sqlite3_bind_text(stmt, 1, date.c_str(), -1, SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(stmt);
    return false;
  }

  bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
  sqlite3_finalize(stmt);

  if (!exists) {
    if (!createTable(db, date)) { return false; }
    // 创建新表后，检查并清理超出数量的旧表
    cleanOldTables(db);
  }
  return true;
}

bool DataBaseManager::insertRow(sqlite3* db, const std::string& table, const ModbusMasterMsg& msg,
                                const ModbusMasterMsg::RegisterItem& reg)
{
  std::string sql = "INSERT INTO \"" + table + "\" ("
                      "id, pdu_addr, pdu_func, pdu_data, timestamp, "
                      "address, map_addr, value, description"
                      ") VALUES (?,?,?,?,?,?,?,?,?);";
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Prepare insert failed: " + std::string(sqlite3_errmsg(db)));
    return false;
  }

  sqlite3_bind_int(stmt, 1, msg.id);
  sqlite3_bind_int(stmt, 2, msg.pdu_addr);
  sqlite3_bind_int(stmt, 3, msg.pdu_func);
  sqlite3_bind_text(stmt, 4, msg.pdu_data.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 5, msg.timestamp.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 6, reg.address);
  sqlite3_bind_int(stmt, 7, reg.map_addr);
  sqlite3_bind_int(stmt, 8, reg.value);
  sqlite3_bind_text(stmt, 9, reg.description.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);
  bool success = (rc == SQLITE_DONE);
  if (!success) { LOG_ERROR("Insert row failed: " + std::string(sqlite3_errmsg(db))); }
  sqlite3_finalize(stmt);
  return success;
}

bool DataBaseManager::writeMessage(const ModbusMasterMsg& msg)
{
  sqlite3* db = getDbHandle(msg.channel);
  if (!db) {
    LOG_ERROR("No database for channel " + std::to_string(msg.channel));
    return false;
  }

  std::string date = getDateFromTimestamp(msg.timestamp);
  if (date.empty()) {
    LOG_ERROR("Invalid timestamp in message id=" + std::to_string(msg.id));
    return false;
  }

  if (!ensureTableExists(db, date)) { return false; }

  // 为每个 register_map 条目插入一行
  bool allSuccess = true;
  for (const auto& reg : msg.register_map) {
    if (!insertRow(db, date, msg, reg)) {
      allSuccess = false;
      // 继续尝试插入其他行，记录错误
    }
  }
  if (!allSuccess) { LOG_ERROR("Failed to write all rows for message id=" + std::to_string(msg.id)); }
  return allSuccess;
}

bool DataBaseManager::dropTable(int channel, const std::string& tableName)
{
  sqlite3* db = getDbHandle(channel);
  if (!db) return false;

  std::string sql = "DROP TABLE IF EXISTS \"" + tableName + "\";";
  char* errMsg = nullptr;
  int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
  if (rc != SQLITE_OK) {
    LOG_ERROR("Drop table " + tableName + " failed: " + std::string(errMsg));
    sqlite3_free(errMsg);
    return false;
  }
  LOG_INFO("Dropped table " + tableName + " from channel " + std::to_string(channel));
  return true;
}

bool DataBaseManager::selectByIdAndChannel(int channel, int id, ModbusMasterMsg& outMsg)
{
  sqlite3* db = getDbHandle(channel);
  if (!db) return false;

  // 先获取所有表名
  std::vector<std::string> tables = getAllTableNames(db);
  if (tables.empty()) return false;

  // 我们需要在所有表中查找指定 id 的记录。由于 id 可能分布在多天的表中，我们依次查询。
  // 这里简化为依次查询每个表，并聚合结果。可以优化为一条 UNION 语句，但为了简单，循环处理。
  bool found = false;
  outMsg = ModbusMasterMsg(); // 清空
  outMsg.register_map.clear();

  for (const auto& table : tables) {
    std::string sql
      = "SELECT id, pdu_addr, pdu_func, pdu_data, timestamp, "
        "address, map_addr, value, description FROM \""
        + table + "\" WHERE id=?;";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
      LOG_ERROR("Prepare select failed: " + std::string(sqlite3_errmsg(db)));
      continue;
    }
    rc = sqlite3_bind_int(stmt, 1, id);
    if (rc != SQLITE_OK) {
      sqlite3_finalize(stmt);
      continue;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
      if (!found) {
        // 第一次找到记录，填充公共字段
        outMsg.id = sqlite3_column_int(stmt, 0);
        outMsg.channel = channel; // 已知
        outMsg.pdu_addr = sqlite3_column_int(stmt, 1);
        outMsg.pdu_func = sqlite3_column_int(stmt, 2);
        const char* pdu_data = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
        if (pdu_data) outMsg.pdu_data = pdu_data;
        const char* timestamp = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
        if (timestamp) outMsg.timestamp = timestamp;
        found = true;
      }
      // 填充 register_map 条目
      ModbusMasterMsg::RegisterItem reg;
      reg.address = sqlite3_column_int(stmt, 5);
      reg.map_addr = sqlite3_column_int(stmt, 6);
      reg.value = sqlite3_column_int(stmt, 7);
      const char* desc = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 8));
      if (desc) reg.description = desc;
      outMsg.register_map.push_back(reg);
    }
    sqlite3_finalize(stmt);
    if (found) break; // 一旦找到，可以停止搜索后续表（假设每个 id 只属于一张表）
  }

  return found;
}

std::string DataBaseManager::getDbPath(int channel) const
{
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/CHANNEL%02d.db", dbDir_.c_str(), channel);
  return std::string(buf);
}

std::vector<RegisterMap> DataBaseManager::queryByTimestampAndRegister(uint64_t timestampUnix, uint16_t registerFilter,
                                                                      const std::string& devAddr)
{
  std::vector<RegisterMap> result;
  std::string tsStr = unixToDbTs(timestampUnix);
  if (tsStr.empty()) {
    LOG_ERROR("Invalid unix timestamp: " + std::to_string(timestampUnix));
    return result;
  }
  std::string dateStr = tsStr.substr(0, 8); // 按天分表，表名YYYYMMDD
  LOG_INFO("Query db: ts=" + tsStr + ", regFilter=" + std::to_string(registerFilter));

  // 依次查询CHANNEL01、CHANNEL02...所有通道
  for (int ch = 1; ch <= maxChannels_; ++ch) {
    sqlite3* db = getDbHandle(ch);
    if (!db) {
      LOG_WARN("Skip channel " + std::to_string(ch) + ": db open failed");
      continue;
    }

    // 检查表是否存在，不存在则跳过
    std::string checkSql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";
    sqlite3_stmt* checkStmt = nullptr;
    int rc = sqlite3_prepare_v2(db, checkSql.c_str(), -1, &checkStmt, nullptr);
    if (rc != SQLITE_OK) {
      LOG_ERROR("Prepare check table failed, ch" + std::to_string(ch) + ": " + sqlite3_errmsg(db));
      continue;
    }
    sqlite3_bind_text(checkStmt, 1, dateStr.c_str(), -1, SQLITE_STATIC);
    bool tableExist = (sqlite3_step(checkStmt) == SQLITE_ROW);
    sqlite3_finalize(checkStmt);
    if (!tableExist) {
      LOG_INFO("Table " + dateStr + " not exist in CHANNEL" + std::to_string(ch) + ".db, skip");
      continue;
    }

    // 构造SQL：65535查全部，否则查指定寄存器
    std::string sql = "SELECT address, value, description FROM \"" + dateStr + "\" WHERE timestamp = ?";
    if (registerFilter != 0xFFFF) { sql += " AND address = ?"; }
    sql += " ORDER BY address ASC;";

    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
      LOG_ERROR("Prepare query failed, ch" + std::to_string(ch) + ": " + sqlite3_errmsg(db));
      continue;
    }

    // 绑定参数
    sqlite3_bind_text(stmt, 1, tsStr.c_str(), -1, SQLITE_STATIC);
    if (registerFilter != 0xFFFF) { sqlite3_bind_int(stmt, 2, registerFilter); }

    // 读取结果并填充RegisterMap
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      RegisterMap entry;
      entry.devAddr = devAddr;
      entry.registerAddr = static_cast<uint16_t>(sqlite3_column_int(stmt, 0));
      entry.value = static_cast<uint16_t>(sqlite3_column_int(stmt, 1));
      const unsigned char* desc = sqlite3_column_text(stmt, 2);
      entry.describe = desc ? reinterpret_cast<const char*>(desc) : "";
      entry.timestamp_unix = timestampUnix;
      result.push_back(std::move(entry));
    }
    sqlite3_finalize(stmt);
    LOG_INFO("CHANNEL" + std::to_string(ch) + ".db found records, total now: " + std::to_string(result.size()));
  }

  LOG_INFO("Query finished, total " + std::to_string(result.size()) + " records");
  return result;
}
