#!/bin/bash
#
# 分布式文件系统并发测试 - 多机 Shell 启动脚本
# 
# 用法:
#   ./launch.sh --hosts stonode4,stonode5,stonode6 --file /mnt/shared/test.dat --size 1G
#

set -e

# ==================== 默认配置 ====================
HOSTS=""
FILE=""
SIZE="1G"
BLOCK="4K"
THREADS=4
DURATION=60
WRITE_RATIO=50
REMOTE_DIR="/tmp/distfs_test"
BINARY="./distfs_test"
VERBOSE=0

# ==================== 帮助 ====================
show_help() {
    cat << EOF
分布式文件系统并发测试 - 多机启动脚本

用法: $0 [选项]

必选:
  --hosts HOSTS         逗号分隔的主机列表，如: node1,node2,node3
  --file PATH           分布式存储上的测试文件路径

可选:
  --size SIZE           测试文件大小 (默认: 1G)
  --block SIZE          IO 块大小 (默认: 4K)
  --threads N           每主机线程数 (默认: 4)
  --duration SEC        测试持续时间秒数 (默认: 60)
  --write-ratio N       写操作比例 0-100 (默认: 50)
  --remote-dir PATH     远程工作目录 (默认: /tmp/distfs_test)
  --binary PATH         本地二进制文件路径 (默认: ./distfs_test)
  -v, --verbose         详细输出
  -h, --help            显示帮助

示例:
  # 3 台机器，每机 8 线程，共测试 10G 文件
  $0 --hosts stonode4,stonode5,stonode6 --file /mnt/lustre/test.dat --size 10G --threads 8

  # 指定 SSH 别名（主机名:SSH别名）
  $0 --hosts "node1:prod-node1,node2:prod-node2" --file /mnt/gpfs/test.dat
EOF
}

# ==================== 解析参数 ====================
while [[ $# -gt 0 ]]; do
    case $1 in
        --hosts)
            HOSTS="$2"
            shift 2
            ;;
        --file)
            FILE="$2"
            shift 2
            ;;
        --size)
            SIZE="$2"
            shift 2
            ;;
        --block)
            BLOCK="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --write-ratio)
            WRITE_RATIO="$2"
            shift 2
            ;;
        --remote-dir)
            REMOTE_DIR="$2"
            shift 2
            ;;
        --binary)
            BINARY="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
done

# ==================== 检查参数 ====================
if [[ -z "$HOSTS" ]] || [[ -z "$FILE" ]]; then
    echo "错误: --hosts 和 --file 是必需参数"
    show_help
    exit 1
fi

if [[ ! -f "$BINARY" ]]; then
    echo "错误: 找不到二进制文件: $BINARY"
    echo "请先编译: make"
    exit 1
fi

# ==================== 工具函数 ====================
log_info() {
    echo "[$(date '+%H:%M:%S')] INFO: $1"
}

log_error() {
    echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2
}

# 解析大小（支持 K/M/G/T）
parse_size() {
    local size_str=$1
    local val=${size_str%[KkMmGgTt]*}
    local unit=${size_str#$val}
    
    case ${unit^^} in
        K) echo $((val * 1024)) ;;
        M) echo $((val * 1024 * 1024)) ;;
        G) echo $((val * 1024 * 1024 * 1024)) ;;
        T) echo $((val * 1024 * 1024 * 1024 * 1024)) ;;
        *) echo $val ;;
    esac
}

# ==================== 主程序 ====================
TOTAL_SIZE=$(parse_size "$SIZE")
BLOCK_SIZE=$(parse_size "$BLOCK")
NUM_HOSTS=$(echo "$HOSTS" | tr ',' '\n' | wc -l)

log_info "========================================"
log_info "分布式文件系统并发测试"
log_info "========================================"
log_info "文件: $FILE"
log_info "大小: $SIZE ($TOTAL_SIZE 字节)"
log_info "块大小: $BLOCK ($BLOCK_SIZE 字节)"
log_info "主机数: $NUM_HOSTS"
log_info "每主机线程: $THREADS"
log_info "持续时间: ${DURATION}秒"
log_info "写比例: ${WRITE_RATIO}%"
log_info "========================================"

# 计算每个主机的 offset 范围
BYTES_PER_HOST=$((TOTAL_SIZE / NUM_HOSTS))
BYTES_PER_HOST=$(( (BYTES_PER_HOST / BLOCK_SIZE) * BLOCK_SIZE ))  # 对齐到块大小

# 解析主机列表并计算范围
declare -a HOST_ARRAY
declare -a SSH_HOST_ARRAY
declare -a START_OFFSET_ARRAY
declare -a END_OFFSET_ARRAY

idx=0
for host_entry in $(echo "$HOSTS" | tr ',' ' '); do
    # 解析主机名和 SSH 别名
    if [[ "$host_entry" == *":"* ]]; then
        host=${host_entry%%:*}
        ssh_host=${host_entry#*:}
    else
        host=$host_entry
        ssh_host=$host_entry
    fi
    
    # 计算 offset 范围
    start=$((idx * BYTES_PER_HOST))
    if [[ $idx -eq $((NUM_HOSTS - 1)) ]]; then
        end=$TOTAL_SIZE  # 最后一个主机处理剩余所有
    else
        end=$((start + BYTES_PER_HOST))
    fi
    
    HOST_ARRAY[$idx]=$host
    SSH_HOST_ARRAY[$idx]=$ssh_host
    START_OFFSET_ARRAY[$idx]=$start
    END_OFFSET_ARRAY[$idx]=$end
    
    log_info "[$host] 区域: [$start - $end] (通过 $ssh_host 连接)"
    
    idx=$((idx + 1))
done

log_info "========================================"

# ==================== 阶段 1: 部署 ====================
log_info "阶段 1: 部署到远程主机"

for i in "${!HOST_ARRAY[@]}"; do
    host=${HOST_ARRAY[$i]}
    ssh_host=${SSH_HOST_ARRAY[$i]}
    
    log_info "[$host] 部署中..."
    
    # 创建远程目录
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$ssh_host" "mkdir -p $REMOTE_DIR" || {
        log_error "[$host] 无法创建远程目录"
        exit 1
    }
    
    # 复制二进制
    scp -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$BINARY" "$ssh_host:$REMOTE_DIR/distfs_test" || {
        log_error "[$host] 复制二进制失败"
        exit 1
    }
    
    # 添加执行权限
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$ssh_host" "chmod +x $REMOTE_DIR/distfs_test" || {
        log_error "[$host] 设置执行权限失败"
        exit 1
    }
    
    log_info "[$host] 部署完成"
done

log_info "========================================"

# ==================== 阶段 2: 预创建文件 ====================
log_info "阶段 2: 预创建测试文件"

first_host=${HOST_ARRAY[0]}
first_ssh=${SSH_HOST_ARRAY[0]}

log_info "[$first_host] 预创建文件到 $SIZE..."
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$first_ssh" \
    "$REMOTE_DIR/distfs_test -f $FILE -s $SIZE -b $BLOCK -p 1 -d 1" || {
    log_error "[$first_host] 预创建文件失败"
    exit 1
}
log_info "[$first_host] 预创建完成"

log_info "========================================"

# ==================== 阶段 3: 启动测试 ====================
log_info "阶段 3: 启动测试 (按任意键开始，或等待 5 秒自动开始...)"

# 倒计时
for i in 5 4 3 2 1; do
    echo -ne "\r开始倒计时: $i 秒..."
    sleep 1
done
echo ""

# 启动所有主机（并行）
declare -A PIDS
declare -A LOG_FILES

for i in "${!HOST_ARRAY[@]}"; do
    host=${HOST_ARRAY[$i]}
    ssh_host=${SSH_HOST_ARRAY[$i]}
    start=${START_OFFSET_ARRAY[$i]}
    end=${END_OFFSET_ARRAY[$i]}
    
    LOG_FILE="/tmp/distfs_test_${host}.log"
    LOG_FILES[$host]=$LOG_FILE
    
    log_info "[$host] 启动测试..."
    
    # 构建命令
    CMD="$REMOTE_DIR/distfs_test"
    CMD="$CMD -f $FILE"
    CMD="$CMD -s $SIZE"
    CMD="$CMD -b $BLOCK"
    CMD="$CMD -p $THREADS"
    CMD="$CMD -d $DURATION"
    CMD="$CMD -w $WRITE_RATIO"
    CMD="$CMD --start $start"
    CMD="$CMD --end $end"
    CMD="$CMD --hostname $host"
    [[ $VERBOSE -eq 1 ]] && CMD="$CMD -v"
    
    # 在后台通过 SSH 启动，捕获输出到本地日志
    (
        ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 \
            "$ssh_host" "$CMD" > "$LOG_FILE" 2>&1
        echo "EXIT_CODE:$?" >> "$LOG_FILE"
    ) &
    
    PIDS[$host]=$!
    log_info "[$host] 后台启动 (PID: ${PIDS[$host]}, 日志: $LOG_FILE)"
done

log_info "========================================"
log_info "阶段 4: 等待测试完成 (${DURATION}秒 + 缓冲)..."

# 计算等待时间（测试时间 + 启动缓冲）
WAIT_TIME=$((DURATION + 10))
log_info "等待 $WAIT_TIME 秒..."

# 等待并显示进度
for ((i=0; i<WAIT_TIME; i++)); do
    sleep 1
    
    # 每 10 秒显示一次进度
    if [[ $((i % 10)) -eq 0 ]] && [[ $i -gt 0 ]]; then
        log_info "已等待 ${i}秒 / ${WAIT_TIME}秒"
        
        # 显示各主机最新日志
        for host in "${!HOST_ARRAY[@]}"; do
            host_name=${HOST_ARRAY[$host]}
            log_file=${LOG_FILES[$host_name]}
            if [[ -f "$log_file" ]]; then
                tail -1 "$log_file" 2>/dev/null | head -c 80
                echo ""
            fi
        done
    fi
done

log_info "========================================"

# ==================== 阶段 5: 收集结果 ====================
log_info "阶段 5: 收集结果"

TOTAL_OPS=0
TOTAL_BYTES=0
TOTAL_ERRORS=0

for i in "${!HOST_ARRAY[@]}"; do
    host=${HOST_ARRAY[$i]}
    log_file=${LOG_FILES[$host]}
    
    echo ""
    echo "--- $host ---"
    
    if [[ -f "$log_file" ]]; then
        cat "$log_file"
        
        # 解析统计
        ops=$(grep '总操作数:' "$log_file" 2>/dev/null | tail -1 | awk -F':' '{print $2}' | tr -d ' ' || echo 0)
        bytes=$(grep '总字节数:' "$log_file" 2>/dev/null | tail -1 | awk -F':' '{print $2}' | awk '{print $1}' || echo 0)
        errors=$(grep '错误数:' "$log_file" 2>/dev/null | tail -1 | awk -F':' '{print $2}' | tr -d ' ' || echo 0)
        
        TOTAL_OPS=$((TOTAL_OPS + ops))
        TOTAL_BYTES=$((TOTAL_BYTES + bytes))
        TOTAL_ERRORS=$((TOTAL_ERRORS + errors))
    else
        echo "错误: 找不到日志文件"
    fi
done

echo ""
echo "========================================"
echo "集群汇总"
echo "========================================"
printf "集群总操作:   %d\n" $TOTAL_OPS
printf "集群总字节:   %.2f GB\n" $(echo "scale=2; $TOTAL_BYTES / 1024 / 1024 / 1024" | bc 2>/dev/null || echo 0)
printf "集群总错误:   %d\n" $TOTAL_ERRORS
printf "集群 IOPS:    %.0f\n" $(echo "scale=0; $TOTAL_OPS / $DURATION" | bc 2>/dev/null || echo 0)
printf "集群吞吐:     %.2f MB/s\n" $(echo "scale=2; $TOTAL_BYTES / 1024 / 1024 / $DURATION" | bc 2>/dev/null || echo 0)
echo "========================================"

# 清理
log_info "清理本地日志文件..."
for host in "${!LOG_FILES[@]}"; do
    rm -f "${LOG_FILES[$host]}"
done

log_info "完成！"
