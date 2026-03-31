#!/bin/bash
#
# 单机快速测试脚本
# 用法: ./quick_test.sh /mnt/shared/test.dat [大小] [线程数] [持续时间]

FILEPATH=${1:-/tmp/distfs_test.dat}
SIZE=${2:-100M}
THREADS=${3:-4}
DURATION=${4:-10}

echo "=========================================="
echo "分布式文件系统 - 单机快速测试"
echo "=========================================="
echo "文件:     $FILEPATH"
echo "大小:     $SIZE"
echo "线程数:   $THREADS"
echo "持续时间: ${DURATION}s"
echo "=========================================="

# 检查并编译
if [ ! -f ./distfs_test ]; then
    echo "编译测试程序..."
    make clean && make
    if [ $? -ne 0 ]; then
        echo "编译失败!"
        exit 1
    fi
fi

# 清理旧测试文件
echo "清理旧测试文件..."
rm -f "$FILEPATH"

# 运行测试
echo ""
echo "启动测试..."
./distfs_test \
    -f "$FILEPATH" \
    -s "$SIZE" \
    -p "$THREADS" \
    -d "$DURATION" \
    -b 4K \
    -w 50 \
    -v

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "测试通过 ✓"
else
    echo "测试失败 ✗ (退出码: $EXIT_CODE)"
fi
echo "=========================================="

exit $EXIT_CODE
