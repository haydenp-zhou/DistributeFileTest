#!/bin/bash
#
# SSH 连接诊断脚本
# 用法: ./check_ssh.sh stonode4,stonode5,stonode6

HOSTS=$1

if [ -z "$HOSTS" ]; then
    echo "用法: $0 host1,host2,host3"
    exit 1
fi

echo "========================================"
echo "SSH 连接诊断"
echo "========================================"
echo ""

for host in $(echo $HOSTS | tr ',' ' '); do
    echo "--- 检查 $host ---"
    
    # 1. 基础连通性
    echo -n "  网络连通性: "
    if ping -c 1 -W 2 $host > /dev/null 2>&1; then
        echo "OK"
    else
        echo "FAIL (ping 不通)"
        continue
    fi
    
    # 2. SSH 端口检查
    echo -n "  SSH 端口 (22): "
    if nc -z -w 5 $host 22 > /dev/null 2>&1; then
        echo "OK"
    else
        echo "FAIL (端口不通)"
        continue
    fi
    
    # 3. SSH 免密登录测试
    echo -n "  免密登录: "
    if ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes -o PasswordAuthentication=no $host "echo OK" > /dev/null 2>&1; then
        echo "OK"
    else
        echo "FAIL (需要密码或密钥不匹配)"
        echo ""
        echo "  调试信息:"
        ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -v $host "echo OK" 2>&1 | grep -E "(debug|Authenticated|password|Permission)" | head -10
    fi
    
    echo ""
done

echo "========================================"
echo "诊断建议:"
echo "========================================"
echo ""
echo "1. 如果 '网络连通性' 失败:"
echo "   - 检查目标机器是否开机"
echo "   - 检查防火墙设置"
echo ""
echo "2. 如果 'SSH 端口' 失败:"
echo "   - 检查目标机器的 sshd 服务是否运行"
echo "   - 检查防火墙是否放行 22 端口"
echo ""
echo "3. 如果 '免密登录' 失败:"
echo "   - 先手动执行: ssh-copy-id $host"
echo "   - 或使用脚本批量配置:"
echo ""
echo "     for h in stonode4 stonode5 stonode6; do"
echo "         ssh-copy-id -o StrictHostKeyChecking=no \$h"
echo "     done"
echo ""
echo "4. 如果 SSH 使用非标准端口，修改 distfs_launcher.py 中的 SSH 配置"
echo ""
