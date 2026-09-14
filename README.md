# SQLite 交叉编译


# 指定交叉编译器

## 设置环境变量（关键）

export CC=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-gcc
export CXX=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-g++
export AR=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-ar
export AS=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-as
export LD=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-ld
export RANLIB=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-ranlib
export STRIP=/home/lanceli/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin/arm-buildroot-linux-gnueabihf-strip

## 配置编译参数

mkdir -p ../sqlite3-arm-install

## 运行 configure 配置（针对 cortex-a7 优化）：

./configure \
  --host=arm-linux-gnueabihf \
  --prefix=$(pwd)/../sqlite3-arm-install \
  --enable-static \
  --enable-shared \
  --disable-tcl \
  --disable-readline \
  CFLAGS="-Os -mcpu=cortex-a7 -mfpu=neon-vfpv4 -mfloat-abi=hard -fPIC" \
  LDFLAGS="-Wl,-O1 -Wl,--hash-style=gnu"


- *-Os: 优化代码大小；-mcpu=cortex-a7: 针对cortex-a7优化；-mfpu=neon-vfpv4: 启用NEON和VFPv4浮点；-mfloat-abi=hard: 硬浮点ABI；*
- *-fPIC: 生成位置无关代码*

## 编译与安装

make
make install

# APP 编译步骤


mkdir build && cd build
cmake -DCMAKE_TOOLCHAIN_FILE=../aarch32-toolchain.cmake ..
make

## 需求

设计用于存储 JSON 格式的报文内容的消息结构，并编写相应的交互函数，报文内容格式参考文件 modbusMasterMsg.json ，需求如下：
1. 程序编译语言版本为C++11，运行环境为 aarch32 linux；
2. 初始化过程中指定消息队列的大小；
3. 消息队列为环形结构，如果写入的内容数量超出队列剩余空间，则覆盖掉最早的存储节点；
4. 交互函数包含读函数、写函数；
5. 交互函数支持查询空闲队列大小，即没用用于存储的节点数量；
6. 读函数返回值即为单个报文内容消息结构，读出后将此节点置为空闲状态；
7. 写函数形参包括单个报文内容消息结构，写入后将此节点置为忙碌状态；


## 需求++

修改 main.cpp 文件，实现将从 MQTT 总线接收到的 JSON 报文存入 MessageQueue 消息结构当中，并定期读取打印到日志当中，具体需求如下：

1. 订阅主题 modbusMaster/database/data 监测 modbusMaster 发布的消息内容；
2. 将 modbusMaster/database/data 主体的 payload 进行校验，判定是否符合 MessageQueue  消息结构的规范；
3. 如果符合 MessageQueue 消息结构的规范，则存入队列当中，如果不符合则丢弃不处理；
4. 另起一个线程，每隔60秒查询当前队列中的报文数量，并将队列中所有报文打印出来，直至队列为空；



## 需求++
根据上传的 modbus_message_queue.h 文件对 modbus_message_queue.cpp 文件内容进行修改，并对main.cpp文件进行修改以适应新的功能调整，具体需求如下：
1. 在 modbus_message_queue.h 文件的寄存器映射条目 RegisterItem 中，变量 address 存储的是  modbusMasterMsg.json 中的 数组对象 register_map 里的寄存器地址键名，譬如 "16"、"17"、"18"...
2. 在 modbus_message_queue.h 文件的寄存器映射条目 RegisterItem 中，变量 map_addr 存储的是 modbusMasterMsg.json 中的 数组对象 register_map 里的寄存器地址键值，譬如 0、1、2...
3. 在 modbus_message_queue.h 文件的寄存器映射条目 RegisterItem 中，变量 value 存储的是 modbusMasterMsg.json 中的 数组对象 register_map 里的寄存器值，譬如 220、456...



## 需求（SQLite）

编写一个 SQLite 应用程序，实现将消息结构 ModbusMasterMsg 中的内容写入到数据库中，具体需求如下：
1. SQLite 数据库交互操作类的声明放在 dbManager.h 当中，类函数的定义放在 dbManager.cpp 当中，交互函数至少包含增（写入）、删（删除数据表）、查（根据 id + channel 读取信息）；
   -  在初始化过程中，根据初始化参数的值对数据库进行创建，例如 DataBaseManager db(2) 即为创建 CHANNEL01 和 CHANNEL02两个数据库；
   -  每个channel一个数据路，即判断 channel 的值后，将 channel 值相同的内容写入到对应channel的数据库，譬如有 channel 值有 1和2 两个通道，channel为1的 ModbusMasterMsg 消息结构的 id、pdu_addr、pdu_func、pdu_data、timestamp、register_map 内容写入到名为 CHANNEL01的数据库；
   -  一个数据中包含多张数据表，每张数据表以日期作为区分，单位为天，例如 20260401；
   -  一个数据库中最大的表格数目在初始化过程中可设置，如果超出最大数量的阈值，则创建新表后将最旧一张表格删除，以控制数据库占用的磁盘空间；
   -  数据表的表头分别为：id(ModbusMasterMsg中的id)、pdu_addr(ModbusMasterMsg中的pdu_addr)、pdu_func(ModbusMasterMsg中的pdu_func)、pdu_data(ModbusMasterMsg中的pdu_data)、timestamp(ModbusMasterMsg中的timestamp)、address(ModbusMasterMsg中的register_map.address)、map_addr(ModbusMasterMsg中的register_map.map_addr)、value(ModbusMasterMsg中的register_map.value)、description(ModbusMasterMsg中的register_map.description)；
   -  同一帧报文中的 register_map 数组如果有多个对象，则以每个对象一行的方式分多行写入到数据表当中，这种情况下，register_map 中的 address、map_addr、value、description 可能有不同，其他参数（id、channel、pdu_addr、pdu_func、pdu_data、timestamp）则共用同一个值；
2. 程序编译语言版本为C++11，运行环境为 aarch32 linux；
3. 在 main.cpp 文件中的 main 函数进行数据库初始化操作；
4. 在 main.cpp 文件 main 函数 的定时读取线程中，加入数据库写入函数，将从 ModbusMasterMsg 消息队列中读取的内容写入到 SQLite 数据库；


## 初始化需求

修改 main.cpp 文件，在保留原有功能的前提下，实现 keepAlive 功能的 MQTT 消息的交互，具体需求如下：
1. 订阅 modbusMaster 发送的请求报文，主题是 modbusMaster/database/request/keepAlive；
2. 判定 MQTT 接收消息的主题是否为 modbusMaster/database/request/keepAlive ，如果是，则判定 payload 内容是否为  "{"token":123456}" ,如果不是，则忽略此次请求操作；
3. 当 MQTT 接收消息的主题是 modbusMaster/database/request/keepAlive ，且 payload 内容是 "{"token":123456}" ，则记录 "token" 值（整型数值），并返回 MQTT 消息，消息主题为 database/modbusMaster/response/keepAlive ，payload 内容为 "{"token":123456,"status":"ready"}" ，"token" 值必须与请求报文中的 "token" 值一致；
4. 判定 MQTT 接收消息的主题是否为 "modbusMaster/database/data" ，如果是，判定报文格式是否合法后，存入 MessageQueue 消息队列中；
5. 如果接收到的消息主题内容不包含以上内容，则不做处理；


## 重启落盘功能

修改 main.cpp 文件，在保留原有功能的前提下，增加 reboot 功能的 MQTT 消息的交互，具体需求如下：
1. 订阅 httpserver 发送的请求报文，主题是 httpserver/database/request/reboot ，如果是，则判定 payload 格式是否为  "{"token":123456}", "token" 值不需要固定，如果不是，则忽略此次请求操作；
2. 当 MQTT 接收消息的主题是 httpserver/database/request/reboot ，且 payload 内容格式正确 ，则记录 "token" 值（整型数值）;
3. 记录 "token" 值后，需要立即唤醒 定时读取线程 reader_thread ,确保消息队列 MessageQueue 中的数据已入库并且落盘保存；
4. 完成步骤 3 的操作后，返回 MQTT 消息，消息主题为 database/httpserver/response/reboot ，payload 内容为 "{"token":123456,"status":"ready"}" ，"token" 值必须与请求报文中的 "token" 值一致；




修改附件中的 .h/cpp 文件，实现以下需求：
1. 在初始化过程中，使用 devListMsg.h 文件中的功能定义 1 个点表队列用于存储从数据库中读出的点表信息；
   - devAddr        12 字节十六进制设备地址（来自 plcLocalConfig.json 的 address）
   - registerAddr   本地点表地址
   - value          当前值
   - describe       寄存器属性描述
   - timestamp_unix 秒级 Unix 时间戳
2. 在初始化过程中，读取 /root/config/plcLocalConfig.json 文件中的 "address" 键值，譬如 "7BE158680053"，保存到缓存中待使用；
3. 在初始化过程中，读取 /root/config/plcLocalConfig.json 文件中的 "mode" 键值，如果是 "Slave" ，则订阅并监听 MQTT 主题报文 plcManager/database/notify/dataTimestamp 
   1. 报文的 payload 为 JSON 格式 {"token": 4521, "timestamp": 1774411200, "register": 65535, "addr": "5FE158680053"}
   2. "token" 键值为标记当前报文的 ID，无需处理；
   3. "timestamp" 键值为 unix 时间戳；
   4. "register" 键值为点表寄存器地址，为 uint16_t 格式的数值，保存到缓存中待使用；
   5. "addr" 键值是发起台账召测设备的地址，保存到缓存中待使用；
4. 如果监听到 MQTT 主题报文 plcManager/database/notify/dataTimestamp 需判断 payload 格式是否符合规格：
5. 如果 payload 格式不符合规格，则将错误信息打印到日志当中；
6. 如果 payload 格式符合规格，则根据 MQTT 报文的信息，对数据库 CHANNEL01.db 和 CHANNEL02.db 的数据进行读出处理：
   1. 判断 register 的值是否为 65535，如果是，则从数据库中读取与缓存中的 timestamp 值一致的 adress 值和 value 值，并存入队列当中，以上述 JSON 报文举例：
      1. 譬如当前的 register 的值为 65535，表示当前的动作为查询所有符合条件的寄存器，与对应寄存器的 value 值；
      2. 首先将数据库 CHANNEL01.db 中 timestamp 列的 "20260325120000" 时刻的 adress 和 value 值读出，并写入到 std::vector<RegisterMap> map_ 队列当中；
      3. 譬如 map_[0].devAddr="7BE158680053"; map_[0].registerAddr=16; map_[0].value=200; map_[0].timestamp_unix=1774411200
      4. 如果还有下 1 节点，则继续读取并写入，譬如 map_[1].devAddr="7BE158680053"; map_[1].registerAddr=17; map_[1].value=100; map_[1].timestamp_unix=1774411200
      5. 如果当前 CHANNEL01.db 数据库中所有符合 timestamp 值的信息都已写入到队列当中，则继续读取 CHANNEL02.db 数据库的数据，如此类推；
   2. 如果 register 的值是不是 65535，则从数据库中读取 "timestamp" 和 "register" 的值都符合报文键值条件的信息，存入 std::vector<RegisterMap> map_ 队列当中；
      1. 譬如 register 的值为 16，timestamp 的值为 "20260325120000"
      2. 首先将数据库 CHANNEL01.db 中满足 timestamp 列值为 "20260325120000" 和 address 列值为 16 的 value 值读出，并写入到 std::vector<RegisterMap> map_ 队列当中；
      3. 譬如 map_[0].devAddr="7BE158680053"; map_[0].registerAddr=16; map_[0].value=200; map_[0].timestamp_unix=1774411200
      4. 如果数据库 CHANNEL01.db 中没有满足条件的寄存器点表值，则继续在数据库 CHANNEL02.db 中进行查找；
      5. 即 register 的值不为 65535 时，表示指定时间和指点寄存器点表的值，如果 register 的值为 65535(0xFFFF) 表示满足指定时间的所有寄存器点表的值；
7. 根据上述操作可得知，MQTT 主题报文的 payload 内容中 "timestamp" 键值为 秒级 Unix 时间戳，但是数据库 "timestamp" 列存储的时间戳格式为 "YYYYMMDDHHMMSS"，在进行时间戳匹配前，需转换成 "YYYYMMDDHHMMSS" 格式的 std:string 类型，保存到缓存中待查找使用，譬如 "20260325120000"，但是 RegisterMap.timestamp_unix 类型为 uint64_t 的 unix 时间戳，以上格式转换需要注意；
8. 按照步骤 6 中对 2 个数据库 CHANNEL01.db 和 CHANNEL02.db 处理完毕后，则将 std::vector<RegisterMap> map_ 队列中的内容读出，并组成 MQTT 主题为 database/plcManager/report/dataTimestamp 的报文，payload 内容为：{"addr": "5FE158680053", "deep": 0, "status": 0, "register": 0, "value": 200, "timestamp": 1774411200} 具体描述如下：
   1. "addr" 键值是发起台账召测设备的地址，从上述变量缓存中获取；
   2. "deep" 为中继深度，默认为0；
   3. "register" 为寄存器点表地址，对应 map_[0].registerAddr
   4. "value" 为当前寄存器点表地址所对应的数值，对应 map_[0].value
   5. "status" 值为当前报文的状态值，如果数组 map_ 还没遍历完毕，此状态值为 0 ，如果遍历完毕，状态值为 1
   6. timestamp 是当前报文的 unix 时间戳，对应 map_[0].timestamp_unix
9. 如果 registers 对象还没遍历完毕，则进入阻塞状态，仅监听和处理主题报文 plcManager/database/reply/dataTimestamp
10. 如果 10 秒内没有监听到应答报文 plcManager/database/reply/dataTimestamp ，则视为超时，打印错误日志，退出当前监听动作，重新进入步骤 4 中监听主题 plcManager/database/notify/dataTimestamp
11. 如果监听到有应答报文 plcManager/database/reply/dataList ，无需判断 payload 内容，即发送第二帧内容报文 {"addr": "5FE158680053", "deep": 0, "status": 0, "register": 1, "value": 201, "timestamp": 1774411200}
12. 主题报文 database/plcManager/report/dataTimestamp 发送前需等待 1000ms 后，才发布到 MQTT 总线；
13. 代码中需包括以上操作的过程日志打印信息，用于调试使用；
14. 如果 "mode" 为 Master，则无需进行以上操作；
15. 实现上述需求的完整功能，如果篇幅受限，仅需输出修改部分的函数完整代码即可；

mosquitto_pub -h localhost -p 1883 -t "plcManager/database/notify/dataTimestamp" -m  "{\"addr\":\"7BE158680053\",\"register\":65535,\"timestamp\":1789353030,\"token\":31005}"
