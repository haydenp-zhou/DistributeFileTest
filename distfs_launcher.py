#!/usr/bin/env python3
"""
分布式文件系统并发测试 - 多机启动脚本

功能：
- 将测试程序分发到多台机器
- 自动划分每个机器的操作区域（offset 范围）
- 同步启动测试
- 收集结果

用法：
    python3 distfs_launcher.py --hosts host1,host2,host3 --file /mnt/shared/test.dat --size 10G

依赖：
    - Python 3.6+
    - 目标机器配置了 SSH 免密登录
    - 目标机器安装了 g++ 或已编译好的二进制
"""

import argparse
import subprocess
import sys
import os
import time
import json
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional
import signal


@dataclass
class HostConfig:
    """单个主机的配置"""
    hostname: str
    ssh_host: str
    offset_start: int
    offset_end: int
    threads: int
    work_dir: str = "/tmp/distfs_test"


class DistFSLauncher:
    def __init__(self, args):
        self.args = args
        self.hosts: List[HostConfig] = []
        self.remote_bin_path = f"{args.remote_dir}/distfs_test"
        self.results: Dict[str, dict] = {}
        self._stop_requested = False
        
        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print(f"\n[信号] 收到信号 {signum}，正在停止所有远程进程...")
        self._stop_requested = True
        self.stop_all()
        sys.exit(1)
    
    def _parse_hosts(self) -> List[Tuple[str, str]]:
        """解析主机列表，支持 host 或 host:ssh_name 格式"""
        hosts = []
        for h in self.args.hosts.split(','):
            h = h.strip()
            if ':' in h:
                hostname, ssh_host = h.split(':', 1)
            else:
                hostname = ssh_host = h
            hosts.append((hostname, ssh_host))
        return hosts
    
    def _calculate_ranges(self, hosts: List[Tuple[str, str]]) -> List[HostConfig]:
        """计算每个主机的 offset 范围"""
        total_size = self._parse_size(self.args.size)
        num_hosts = len(hosts)
        
        # 按主机数均分
        bytes_per_host = total_size // num_hosts
        block_size = self._parse_size(self.args.block)
        
        # 对齐到 block_size
        blocks_per_host = bytes_per_host // block_size
        bytes_per_host = blocks_per_host * block_size
        
        configs = []
        for i, (hostname, ssh_host) in enumerate(hosts):
            start = i * bytes_per_host
            if i == num_hosts - 1:
                end = total_size  # 最后一个主机处理剩余所有
            else:
                end = start + bytes_per_host
            
            configs.append(HostConfig(
                hostname=hostname,
                ssh_host=ssh_host,
                offset_start=start,
                offset_end=end,
                threads=self.args.threads,
                work_dir=self.args.remote_dir
            ))
        
        return configs
    
    def _parse_size(self, size_str: str) -> int:
        """解析大小字符串，支持 K/M/G/T 后缀"""
        size_str = size_str.upper()
        multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
        
        for suffix, mult in multipliers.items():
            if size_str.endswith(suffix):
                return int(float(size_str[:-1]) * mult)
        
        return int(size_str)
    
    def _ssh_cmd(self, host: str, cmd: str, timeout: int = 60) -> Tuple[int, str, str]:
        """执行 SSH 命令，返回 (returncode, stdout, stderr)"""
        ssh_cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
            host,
            cmd
        ]
        
        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "SSH command timed out"
        except Exception as e:
            return -1, "", str(e)
    
    def _scp_file(self, local_path: str, remote_host: str, remote_path: str) -> bool:
        """复制文件到远程主机"""
        cmd = [
            "scp",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            "-o", "BatchMode=yes",
            local_path,
            f"{remote_host}:{remote_path}"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                print(f"[错误] SCP 到 {remote_host} 失败: {result.stderr}")
                return False
            return True
        except Exception as e:
            print(f"[错误] SCP 到 {remote_host} 异常: {e}")
            return False
    
    def setup_hosts(self):
        """设置所有主机：创建目录、复制文件、编译（如果需要）"""
        print("=" * 60)
        print("阶段 1: 设置远程主机")
        print("=" * 60)
        
        # 编译本地二进制
        if self.args.compile:
            print("[本地] 编译测试程序...")
            if not self._compile_local():
                print("[错误] 本地编译失败")
                sys.exit(1)
        
        local_bin = self.args.binary or "./distfs_test"
        if not os.path.exists(local_bin):
            print(f"[错误] 找不到二进制文件: {local_bin}")
            print("请使用 --binary 指定，或添加 --compile 让脚本自动编译")
            sys.exit(1)
        
        for cfg in self.hosts:
            print(f"\n[{cfg.hostname}] 设置中...")
            
            # 创建远程目录
            rc, _, err = self._ssh_cmd(cfg.ssh_host, f"mkdir -p {cfg.work_dir}")
            if rc != 0:
                print(f"[错误] 无法创建远程目录: {err}")
                continue
            
            # 复制二进制
            print(f"[{cfg.hostname}] 复制二进制文件...")
            remote_bin = f"{cfg.work_dir}/distfs_test"
            if not self._scp_file(local_bin, cfg.ssh_host, remote_bin):
                continue
            
            # 添加执行权限
            self._ssh_cmd(cfg.ssh_host, f"chmod +x {remote_bin}")
            
            # 测试远程文件系统访问
            test_dir = os.path.dirname(self.args.file)
            rc, _, _ = self._ssh_cmd(cfg.ssh_host, f"test -d {test_dir}")
            if rc != 0:
                print(f"[警告] {cfg.hostname} 可能无法访问 {test_dir}")
            else:
                print(f"[{cfg.hostname}] 设置完成")
    
    def _compile_local(self) -> bool:
        """在本地编译程序"""
        cmd = [
            "g++", "-std=c++17", "-O2", "-o", "distfs_test",
            "distfs_test.cpp", "-pthread"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"编译错误:\n{result.stderr}")
            return False
        print("编译成功: ./distfs_test")
        return True
    
    def start_test(self):
        """在所有主机上启动测试"""
        print("\n" + "=" * 60)
        print("阶段 2: 启动测试")
        print("=" * 60)
        
        # 先在所有主机上预创建文件（避免并发创建冲突）
        if self.args.prefill:
            print("\n预创建测试文件...")
            first_host = self.hosts[0]
            prefill_cmd = (
                f"{self.remote_bin_path} -f {self.args.file} "
                f"-s {self.args.size} -b {self.args.block} "
                f"-p 1 -d 1 --no-prefill"
            )
            rc, stdout, stderr = self._ssh_cmd(first_host.ssh_host, prefill_cmd, timeout=300)
            if rc != 0:
                print(f"[警告] 预创建可能失败: {stderr}")
            else:
                print("预创建完成")
        
        # 构建所有启动命令
        commands = []
        for cfg in self.hosts:
            cmd = self._build_command(cfg)
            commands.append((cfg, cmd))
            print(f"\n[{cfg.hostname}] 启动命令:")
            print(f"  {cmd}")
        
        # 确认启动
        if not self.args.yes:
            print("\n" + "=" * 60)
            response = input("确认启动测试? [y/N]: ")
            if response.lower() != 'y':
                print("已取消")
                return
        
        # 同步启动所有测试
        print("\n同步启动所有主机测试...")
        self.start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=len(self.hosts)) as executor:
            futures = {}
            for cfg, cmd in commands:
                future = executor.submit(self._run_on_host, cfg, cmd)
                futures[future] = cfg
            
            # 收集结果
            for future in as_completed(futures):
                cfg = futures[future]
                try:
                    result = future.result()
                    self.results[cfg.hostname] = result
                except Exception as e:
                    print(f"[{cfg.hostname}] 异常: {e}")
                    self.results[cfg.hostname] = {"error": str(e)}
    
    def _build_command(self, cfg: HostConfig) -> str:
        """构建远程执行命令"""
        cmd_parts = [
            f"{self.remote_bin_path}",
            f"-f {self.args.file}",
            f"-s {self.args.size}",
            f"-b {self.args.block}",
            f"-p {cfg.threads}",
            f"-d {self.args.duration}",
            f"-w {self.args.write_ratio}",
            f"--start {cfg.offset_start}",
            f"--end {cfg.offset_end}",
            f"--hostname {cfg.hostname}",
        ]
        
        if self.args.verbose:
            cmd_parts.append("-v")
        
        if not self.args.no_verify:
            cmd_parts.append("--no-verify")
        
        # 使用 nohup 和重定向输出
        cmd = " ".join(cmd_parts)
        log_file = f"{cfg.work_dir}/test.log"
        
        # 使用 nohup 确保 SSH 断开后继续运行
        full_cmd = f"cd {cfg.work_dir} && nohup {cmd} > {log_file} 2>&1 &"
        full_cmd += f" echo $! > {cfg.work_dir}/test.pid"
        
        return full_cmd
    
    def _run_on_host(self, cfg: HostConfig, cmd: str) -> dict:
        """在单个主机上运行测试"""
        print(f"[{cfg.hostname}] 启动测试...")
        
        # 启动进程
        rc, stdout, stderr = self._ssh_cmd(cfg.ssh_host, cmd, timeout=10)
        if rc != 0:
            return {"error": f"启动失败: {stderr}"}
        
        pid = stdout.strip()
        print(f"[{cfg.hostname}] PID: {pid}")
        
        # 等待测试完成
        return self._wait_for_completion(cfg, pid)
    
    def _wait_for_completion(self, cfg: HostConfig, pid: str) -> dict:
        """等待测试完成"""
        log_file = f"{cfg.work_dir}/test.log"
        
        while not self._stop_requested:
            # 检查进程是否还在运行
            rc, _, _ = self._ssh_cmd(cfg.ssh_host, f"kill -0 {pid} 2>/dev/null")
            if rc != 0:
                # 进程已结束，读取日志
                rc, stdout, _ = self._ssh_cmd(cfg.ssh_host, f"cat {log_file}")
                return {
                    "hostname": cfg.hostname,
                    "pid": pid,
                    "log": stdout,
                    "offset_range": [cfg.offset_start, cfg.offset_end]
                }
            
            # 打印进度
            if self.args.verbose:
                rc, stdout, _ = self._ssh_cmd(cfg.ssh_host, f"tail -5 {log_file} 2>/dev/null")
                if stdout:
                    print(f"\n[{cfg.hostname}] 进度:\n{stdout}")
            
            time.sleep(5)
        
        return {"hostname": cfg.hostname, "status": "stopped"}
    
    def stop_all(self):
        """停止所有远程测试进程"""
        print("\n停止所有远程测试...")
        for cfg in self.hosts:
            # 尝试读取 PID 并 kill
            cmd = f"if [ -f {cfg.work_dir}/test.pid ]; then kill $(cat {cfg.work_dir}/test.pid) 2>/dev/null; fi"
            self._ssh_cmd(cfg.ssh_host, cmd)
    
    def collect_results(self):
        """收集并汇总结果"""
        print("\n" + "=" * 60)
        print("阶段 3: 测试结果汇总")
        print("=" * 60)
        
        total_ops = 0
        total_bytes = 0
        total_errors = 0
        
        for hostname, result in self.results.items():
            print(f"\n--- {hostname} ---")
            if "error" in result:
                print(f"错误: {result['error']}")
                continue
            
            if "log" in result:
                # 提取关键信息
                log = result["log"]
                print(log)  # 打印完整日志
                
                # 解析统计数据
                for line in log.split('\n'):
                    if '总操作数' in line or '总字节数' in line or '错误数' in line:
                        try:
                            if '总操作数' in line:
                                total_ops += int(line.split(':')[1].strip())
                            elif '总字节数' in line and 'GB' not in line:
                                total_bytes += int(line.split(':')[1].strip().split()[0])
                            elif '错误数' in line:
                                total_errors += int(line.split(':')[1].strip())
                        except:
                            pass
        
        # 汇总
        duration = time.time() - self.start_time
        print("\n" + "=" * 60)
        print("集群汇总")
        print("=" * 60)
        print(f"集群总耗时:   {duration:.1f} 秒")
        print(f"集群总操作:   {total_ops}")
        print(f"集群总字节:   {total_bytes / (1024**3):.2f} GB")
        print(f"集群总错误:   {total_errors}")
        print(f"集群 IOPS:    {total_ops / duration:.0f}")
        print(f"集群吞吐:     {total_bytes / (1024**2) / duration:.2f} MB/s")
        print("=" * 60)
        
        # 保存详细结果
        if self.args.output:
            with open(self.args.output, 'w') as f:
                json.dump({
                    "config": vars(self.args),
                    "hosts": [asdict(h) for h in self.hosts],
                    "results": self.results,
                    "summary": {
                        "duration": duration,
                        "total_ops": total_ops,
                        "total_bytes": total_bytes,
                        "total_errors": total_errors
                    }
                }, f, indent=2)
            print(f"\n详细结果已保存到: {self.args.output}")
    
    def run(self):
        """主流程"""
        # 解析主机和计算范围
        hosts = self._parse_hosts()
        self.hosts = self._calculate_ranges(hosts)
        
        print("测试计划:")
        print(f"  文件: {self.args.file}")
        print(f"  大小: {self.args.size}")
        print(f"  主机数: {len(self.hosts)}")
        for cfg in self.hosts:
            print(f"    - {cfg.hostname}: offset [{cfg.offset_start} - {cfg.offset_end}), "
                  f"线程 {cfg.threads}")
        
        # 设置主机
        self.setup_hosts()
        
        # 启动测试
        self.start_time = time.time()
        self.start_test()
        
        # 收集结果
        self.collect_results()


def main():
    parser = argparse.ArgumentParser(
        description="分布式文件系统并发测试 - 多机启动器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 3 台机器，每机 8 线程，共测试 10G 文件
  python3 distfs_launcher.py --hosts node1,node2,node3 --file /mnt/lustre/test.dat --size 10G -p 8

  # 指定 SSH 别名（主机名:SSH别名）
  python3 distfs_launcher.py --hosts node1:prod-node1,node2:prod-node2 --file /mnt/gpfs/test.dat

  # 自动编译并分发
  python3 distfs_launcher.py --hosts node1,node2 --file /mnt/nfs/test.dat --compile

  # 使用已编译好的二进制
  python3 distfs_launcher.py --hosts node1,node2 --file /mnt/nfs/test.dat --binary ./distfs_test
        """
    )
    
    # 必需参数
    parser.add_argument("--hosts", required=True, help="逗号分隔的主机列表，如: node1,node2,node3")
    parser.add_argument("--file", "-f", required=True, help="分布式存储上的测试文件路径")
    
    # 测试参数
    parser.add_argument("--size", "-s", default="1G", help="测试文件大小 (默认: 1G)")
    parser.add_argument("--block", "-b", default="4K", help="IO 块大小 (默认: 4K)")
    parser.add_argument("--threads", "-p", type=int, default=4, help="每主机线程数 (默认: 4)")
    parser.add_argument("--duration", "-d", type=int, default=60, help="测试持续时间秒数 (默认: 60)")
    parser.add_argument("--write-ratio", "-w", type=int, default=50, help="写操作比例 0-100 (默认: 50)")
    parser.add_argument("--no-verify", action="store_true", help="跳过数据验证")
    parser.add_argument("--no-prefill", dest="prefill", action="store_false", help="跳过预填充")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    
    # 部署参数
    parser.add_argument("--binary", help="本地二进制文件路径（默认: ./distfs_test）")
    parser.add_argument("--compile", action="store_true", help="自动编译并分发")
    parser.add_argument("--remote-dir", default="/tmp/distfs_test", help="远程工作目录 (默认: /tmp/distfs_test)")
    
    # 其他
    parser.add_argument("--output", "-o", help="结果输出 JSON 文件")
    parser.add_argument("--yes", "-y", action="store_true", help="自动确认，不询问")
    
    args = parser.parse_args()
    
    launcher = DistFSLauncher(args)
    launcher.run()


if __name__ == "__main__":
    main()
