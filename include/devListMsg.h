#ifndef DEVLISTMSG_H
#define DEVLISTMSG_H

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

/**
 * @brief 本地点表映射结构体
 *
 * 一条记录描述某个从机的单个寄存器：
 *  - devAddr        12 字节十六进制设备地址（来自 plcLocalConfig.json 的 address）
 *  - registerAddr   本地点表地址
 *  - value          当前值
 *  - describe       寄存器属性描述
 *  - timestamp_unix 秒级 Unix 时间戳（新增）
 */
struct RegisterMap {
  std::string devAddr;
  uint16_t registerAddr;
  uint16_t value;
  std::string describe;
  uint64_t timestamp_unix; // 新增

  RegisterMap() : registerAddr(0), value(0), timestamp_unix(0) {}
};

/**
 * @brief 设备点表管理类
 *
 * 存储所有点表映射，支持多线程读写；读并发、写独占。
 */
class DevListMsg {
public:
  DevListMsg();
  ~DevListMsg();

  /**
   * @brief 添加完整的寄存器映射条目（写锁）
   * @param entry 完整RegisterMap结构体，包含所有字段
   */
  void addRegisterMapEntry(const RegisterMap& entry);

  /**
   * @brief 从 plcLocalConfig.json 加载设备地址并缓存
   */
  bool loadDevAddrFromConfig(const std::string& path);

  /**
   * @brief 获取缓存的设备地址（6 字节）
   */
  const uint8_t* getDevAddr() const;

  /**
   * @brief 获取缓存的设备地址（12 位十六进制字符串，未加载时返回空串）
   */
  std::string getDevAddrString() const;

  /**
   * @brief 从 modbusmap.json 加载所有通道的点表信息
   *        （遍历 collection_channels 下所有通道，不再限制 CH01/CH02）
   */
  bool loadFromModbusMap(const std::string& path);

  /**
   * @brief 点表数量（读锁）
   */
  size_t size() const;

  /**
   * @brief 获取指定索引的点表信息副本（读锁）
   */
  bool getRegisterMap(size_t index, RegisterMap& out) const;

  /**
   * @brief 更新指定寄存器地址的值（写锁，保留兼容）
   */
  bool updateValue(uint16_t registerAddr, uint16_t value);

  /**
   * @brief 从 modbus 上报更新寄存器
   *        匹配 registerAddr 成功后，更新 value / timestamp / describe
   * @return 匹配并更新成功返回 true，否则 false
   */
  bool updateRegister(uint16_t registerAddr, uint16_t value, uint64_t timestamp_unix, const std::string& describe);

  /**
   * @brief 添加一个映射（写锁，devList 主动上报流程使用）
   */
  void addRegisterMap(const std::string& devAddr, uint16_t registerAddr, const std::string& describe);

  /**
   * @brief 清空指定设备地址的所有映射（写锁）
   */
  void clearByAddr(const std::string& devAddr);

  /**
   * @brief 清空所有数据（写锁）
   */
  void clear();

  void setRegisterMaps(const std::vector<RegisterMap>& maps);
  void setRegisterMaps(std::vector<RegisterMap>&& maps);

private:
  std::vector<RegisterMap> map_;
  uint8_t cachedDevAddr_[6];

  // 读写锁（C++11 兼容）
  mutable std::mutex mtx_;
  mutable std::condition_variable cv_;
  mutable int readers_;
  bool writerWaiting_;

  void lockRead() const;
  void unlockRead() const;
  void lockWrite();
  void unlockWrite();
};

#endif // DEVLISTMSG_H