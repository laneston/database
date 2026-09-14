# 目标系统及架构
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

# 交叉编译工具链路径及前缀
set(TOOLCHAIN_PATH /home/lancelipc/arm-buildroot-linux-gnueabihf_sdk-buildroot/bin)
set(TOOLCHAIN_PREFIX arm-buildroot-linux-gnueabihf-)

# 指定编译器
set(CMAKE_C_COMPILER   ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}gcc)
set(CMAKE_CXX_COMPILER ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}g++)
set(CMAKE_AR           ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}ar)
set(CMAKE_RANLIB       ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}ranlib)
set(CMAKE_STRIP        ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}strip)
set(CMAKE_NM           ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}nm)
set(CMAKE_OBJCOPY      ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}objcopy)
set(CMAKE_OBJDUMP      ${TOOLCHAIN_PATH}/${TOOLCHAIN_PREFIX}objdump)

# 调整查找策略：不搜索宿主机的程序/库/头文件
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# 如果工具链包含 sysroot，可取消下一行注释并设置正确路径
set(CMAKE_SYSROOT /home/lancelipc/arm-buildroot-linux-gnueabihf_sdk-buildroot/arm-buildroot-linux-gnueabihf/sysroot)

add_definitions(-D_AARCH32_)