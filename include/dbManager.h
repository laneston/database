#ifndef DBMANAGER_H
#define DBMANAGER_H

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "devListMsg.h"
#include "modbus_message_queue.h" // 引入 ModbusMasterMsg 定义

struct sqlite3; // 前向声明

class DataBaseManager {
public:
  /**
   * 构造函数
   * @param maxChannels  需要管理的通道数量（通道编号从1开始）
   * @param maxTablesPerDb  每个数据库最多允许的表数量（按日期分表，超过则删除最旧的表）
   * @param dbDir        数据库文件存放目录，默认为当前目录（"./")
   */
  DataBaseManager(int maxChannels, int maxTablesPerDb, const std::string& dbDir = "./");
  ~DataBaseManager();

  // 禁止拷贝和赋值
  DataBaseManager(const DataBaseManager&) = delete;
  DataBaseManager& operator=(const DataBaseManager&) = delete;

  /**
   * @brief 按时间戳+寄存器地址跨通道查询历史数据
   * @param timestampUnix 秒级Unix时间戳
   * @param registerFilter 寄存器地址；65535(0xFFFF)表示查询该时刻所有寄存器
   * @param devAddr 填入结果的设备地址
   * @return 匹配的RegisterMap结果集
   */
  std::vector<RegisterMap> queryByTimestampAndRegister(uint64_t timestampUnix, uint16_t registerFilter,
                                                       const std::string& devAddr);

  /**
   * 写入一条 ModbusMasterMsg 消息到对应的数据库
   * @param msg  消息对象
   * @return 成功返回 true，失败返回 false
   */
  bool writeMessage(const ModbusMasterMsg& msg);

  /**
   * 删除指定通道的某张表
   * @param channel   通道号
   * @param tableName 表名（格式为 YYYYMMDD）
   * @return 成功返回 true，失败返回 false
   */
  bool dropTable(int channel, const std::string& tableName);

  /**
   * 根据通道号和消息ID查询消息（支持按register_map分行的聚合）
   * @param channel   通道号
   * @param id        消息ID
   * @param outMsg    输出的消息对象（若找到，其register_map会被填充）
   * @return 成功返回 true，未找到返回 false，出错返回 false
   */
  bool selectByIdAndChannel(int channel, int id, ModbusMasterMsg& outMsg);

private:
  // 获取指定通道的数据库句柄（若未打开则打开）
  sqlite3* getDbHandle(int channel);

  // 从时间戳字符串中提取日期部分（YYYYMMDD）
  std::string getDateFromTimestamp(const std::string& timestamp) const;

  // 确保指定数据库中存在指定日期的表，若不存在则创建，并清理超出的旧表
  bool ensureTableExists(sqlite3* db, const std::string& date);

  // 清理指定数据库中超出最大表数限制的最旧表
  void cleanOldTables(sqlite3* db);

  // 获取指定数据库中所有以数字命名的表（按名称升序）
  std::vector<std::string> getAllTableNames(sqlite3* db);

  // 创建一张新表（表名为 date）
  bool createTable(sqlite3* db, const std::string& date);

  // 插入一行记录到指定表
  bool insertRow(sqlite3* db, const std::string& table, const ModbusMasterMsg& msg,
                 const ModbusMasterMsg::RegisterItem& reg);

  // 将数据库文件路径拼接好
  std::string getDbPath(int channel) const;

private:
  int maxChannels_;                 // 最大通道数（通道编号从1开始）
  int maxTablesPerDb_;              // 每个数据库最多表数量
  std::string dbDir_;               // 数据库文件存放目录
  std::vector<sqlite3*> dbHandles_; // 每个通道对应一个数据库句柄，索引为 channel-1
  mutable std::mutex mutex_;        // 保护多线程访问（写操作可能在定时线程中）
};

#endif // DBMANAGER_H