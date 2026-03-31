#!/bin/bash
#
# 手动多机测试启动脚本
# 当 distfs_launcher.py 的 SSH 连接有问题时使用
#
# 用法:
#   1. 先编译并复制 distfs_test 到各节点
#   2. 修改下面的 HOSTS 数组
#   3. 运行此脚本

# ==================== 配置 ====================
# 节点列表（格式: 主机名:起始offset:结束offset）
HOSTS=(
    "stonode4:0:3579139413"          # 第一台: 0-3.33GB
    "stonode5:3579139413:7158278826" # 第二台: 3.33-6.66GB
    "stonode6:7158278826:10737418240" # 第三台: 6.66-10GB
)

# 测试参数
FILE="/mnt/shared/test.dat"
SIZE="10G"
BLOCK="4K"
THREADS=8
DURATION=60
WRITE_RATIO=50
# ==============================================

echo "========================================"
echo "手动多机测试启动"
echo "========================================"
echo ""

# 检查本地是否有二进制
if [ ! -f ./distfs_test ]; then
    echo "错误: 找不到 ./distfs_test，请先编译: make"
    exit 1
fi

# 在本地启动所有实例（使用后台进程）
for host_config in "${HOSTS[@]}"; do
    IFS=':' read -r host start_offset end_offset <<< "$host_config"
    
    echo "[$host] 准备启动..."
    echo "  区域: [$start_offset - $end_offset]"
    
    # 构建命令
    CMD="./distfs_test \\
        -f $FILE \\
        -s $SIZE \\
        -b $BLOCK \\
        -p $THREADS \\
        -d $DURATION \\
        -w $WRITE_RATIO \\
        --start $start_offset \\
        --end $end_offset \\
        --hostname $host"
    
    echo "  命令: $CMD"
    echo ""
done

echo "========================================"
echo "请复制以下命令到各节点手动执行:"
echo "========================================"
echo ""

for host_config in "${HOSTS[@]}"; do
    IFS=':' read -r host start_offset end_offset <<< "$host_config"
    
    echo "--- $host ---"
    echo "mkdir -p /tmp/distfs_test && cd /tmp/distfs_test"
    echo "# 复制 distfs_test 到该节点"
    echo "./distfs_test -f $FILE -s $SIZE -b $BLOCK -p $THREADS -d $DURATION -w $WRITE_RATIO --start $start_offset --end $end_offset --hostname $host"
    echo ""
done

echo "========================================"
echo "或者使用 SSH 一次性启动所有节点:"
echo "========================================"
echo ""

# 生成并行 SSH 启动命令
for host_config in "${HOSTS[@]}"; do
    IFS=':' read -r host start_offset end_offset <<< "$host_config"
    
    echo "ssh $host 'cd /tmp/distfs_test && nohup ./distfs_test \\"
    echo "  -f $FILE -s $SIZE -b $BLOCK -p $THREADS -d $DURATION -w $WRITE_RATIO \\"
    echo "  --start $start_offset --end $end_offset --hostname $host \\"
    echo "  > /tmp/distfs_test/\$HOSTNAME.log 2>&1 &' &"
    echo ""
done

echo "wait  # 等待所有后台 SSH 完成"
echo ""
echo "========================================"
echo "提示:"
echo "========================================"
echo "1. 使用 GNU parallel 简化并行执行:"
echo "   parallel -j0 'ssh {} ...' ::: stonode4 stonode5 stonode6"
echo ""
echo "2. 查看各节点日志:"
echo "   ssh stonode4 'tail -f /tmp/distfs_test/stonode4.log'"
echo ""
echo "3. 杀死所有测试进程:"
echo "   for h in stonode4 stonode5 stonode6; do ssh \$h 'pkill -f distfs_test'; done"
