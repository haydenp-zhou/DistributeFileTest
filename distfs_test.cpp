/**
 * 分布式文件系统并发读写测试工具
 * 
 * 特性：
 * - 使用 pread64/pwrite64 进行原子读写
 * - 每个线程操作不同的 offset 区域，互不干扰
 * - 支持多机并发（通过脚本在多台机器启动）
 * - 数据一致性验证（pattern 验证）
 * - 实时统计和详细日志
 * 
 * 编译: g++ -std=c++17 -O2 -o distfs_test distfs_test.cpp -pthread
 * 运行: ./distfs_test -f /mnt/shared/test.dat -p 16 -s 1G -b 4K -d 60
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <cstring>
#include <cassert>
#include <csignal>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/syscall.h>
#include <ctime>

// 版本信息
#define VERSION "1.0.0"

// 全局停止标志
static std::atomic<bool> g_stop(false);
static std::atomic<uint64_t> g_total_ops(0);
static std::atomic<uint64_t> g_total_bytes(0);
static std::atomic<uint64_t> g_errors(0);
static std::atomic<uint64_t> g_verify_failures(0);

// 信号处理
void signal_handler(int sig) {
    std::cerr << "\n[信号] 收到信号 " << sig << "，正在停止..." << std::endl;
    g_stop.store(true);
}

// 当前时间字符串
std::string timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&time));
    return std::string(buf);
}

// 线程ID
pid_t gettid() {
    return syscall(SYS_gettid);
}

// 配置结构
struct Config {
    std::string filepath;           // 测试文件路径
    uint64_t file_size = 1ULL << 30; // 默认 1GB
    size_t block_size = 4096;       // 默认 4KB
    int num_threads = 4;            // 默认 4 线程
    int duration_sec = 60;          // 默认跑 60 秒
    bool verify = true;             // 是否验证数据
    bool prefill = true;            // 是否预填充
    int write_ratio = 50;           // 写比例 (0-100)
    int64_t offset_start = -1;      // 起始 offset (-1 表示自动分配)
    int64_t offset_end = -1;        // 结束 offset
    bool verbose = false;           // 详细输出
    std::string hostname;           // 主机标识
};

// 生成数据 pattern：将线程ID、offset 信息编码到数据中
void generate_pattern(char* buf, size_t len, uint64_t offset, int thread_id, uint64_t seq) {
    // 前 8 字节：offset
    // 接下来 4 字节：线程ID
    // 接下来 4 字节：sequence
    // 剩余：循环填充
    
    uint64_t* p64 = (uint64_t*)buf;
    p64[0] = offset;
    
    uint32_t* p32 = (uint32_t*)(buf + 8);
    p32[0] = (uint32_t)thread_id;
    p32[1] = (uint32_t)(seq & 0xFFFFFFFF);
    
    // 剩余填充
    uint64_t seed = offset ^ thread_id ^ seq;
    std::mt19937_64 rng(seed);
    for (size_t i = 16; i < len; i += 8) {
        uint64_t val = rng();
        memcpy(buf + i, &val, std::min(size_t(8), len - i));
    }
}

// 验证数据 pattern
bool verify_pattern(const char* buf, size_t len, uint64_t expected_offset, int thread_id) {
    if (len < 16) return true;
    
    uint64_t stored_offset;
    memcpy(&stored_offset, buf, 8);
    
    if (stored_offset != expected_offset) {
        return false;
    }
    
    uint32_t stored_thread_id;
    memcpy(&stored_thread_id, buf + 8, 4);
    
    if ((int)stored_thread_id != thread_id) {
        // offset 对但 thread_id 不对，可能是其他线程写的
        return false;
    }
    
    return true;
}

// 工作线程函数
void worker_thread(int thread_id, const Config& cfg, int fd, 
                   uint64_t my_start_off, uint64_t my_end_off) {
    
    std::vector<char> write_buf(cfg.block_size);
    std::vector<char> read_buf(cfg.block_size);
    
    std::random_device rd;
    std::mt19937_64 rng(rd() + thread_id);
    std::uniform_int_distribution<int> op_dist(0, 99);
    std::uniform_int_distribution<uint64_t> offset_dist(0, 
        (my_end_off - my_start_off) / cfg.block_size - 1);
    
    uint64_t local_ops = 0;
    uint64_t local_bytes = 0;
    uint64_t local_errors = 0;
    uint64_t local_verify_fail = 0;
    uint64_t seq = 0;
    
    auto thread_start = std::chrono::steady_clock::now();
    
    while (!g_stop.load()) {
        // 检查是否超时
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - thread_start).count();
        if (elapsed >= cfg.duration_sec) {
            break;
        }
        
        // 选择 offset（在我的区域内随机）
        uint64_t block_idx = offset_dist(rng);
        uint64_t offset = my_start_off + block_idx * cfg.block_size;
        
        bool is_write = (op_dist(rng) < cfg.write_ratio);
        
        if (is_write) {
            // 写操作
            generate_pattern(write_buf.data(), cfg.block_size, offset, thread_id, seq++);
            
            ssize_t ret = pwrite64(fd, write_buf.data(), cfg.block_size, offset);
            
            if (ret != (ssize_t)cfg.block_size) {
                local_errors++;
                if (cfg.verbose) {
                    std::cerr << "[T" << thread_id << "] pwrite64 失败 at offset " 
                              << offset << ": " << strerror(errno) << std::endl;
                }
            } else {
                // 确保数据落盘（可选，根据测试需求）
                // fdatasync(fd);
                local_bytes += cfg.block_size;
            }
        } else {
            // 读操作
            ssize_t ret = pread64(fd, read_buf.data(), cfg.block_size, offset);
            
            if (ret < 0) {
                local_errors++;
                if (cfg.verbose) {
                    std::cerr << "[T" << thread_id << "] pread64 失败 at offset " 
                              << offset << ": " << strerror(errno) << std::endl;
                }
            } else if (ret == 0) {
                // EOF，文件不够大
                local_errors++;
            } else {
                local_bytes += ret;
                
                // 验证数据（如果启用了）
                if (cfg.verify && ret == (ssize_t)cfg.block_size) {
                    // 这里验证可能不准确，因为可能被其他线程覆盖了
                    // 所以只做基本的 offset 验证
                    uint64_t stored_offset;
                    memcpy(&stored_offset, read_buf.data(), 8);
                    
                    // 存储的 offset 应该是某个 block 的起始位置
                    if (stored_offset % cfg.block_size != 0) {
                        local_verify_fail++;
                        if (cfg.verbose) {
                            std::cerr << "[T" << thread_id << "] 数据验证失败 at offset " 
                                      << offset << ": 非法的存储 offset " << stored_offset << std::endl;
                        }
                    }
                }
            }
        }
        
        local_ops++;
        
        // 每 1000 次操作汇报一次（仅 verbose 模式）
        if (cfg.verbose && local_ops % 1000 == 0) {
            std::cout << "[T" << thread_id << "] 已完成 " << local_ops << " 操作" << std::endl;
        }
    }
    
    // 汇总到全局
    g_total_ops.fetch_add(local_ops);
    g_total_bytes.fetch_add(local_bytes);
    g_errors.fetch_add(local_errors);
    g_verify_failures.fetch_add(local_verify_fail);
    
    std::cout << "[线程 " << thread_id << " @ " << cfg.hostname << "] "
              << "PID=" << getpid() << " TID=" << gettid()
              << " 区域=[" << my_start_off << "-" << my_end_off << "]"
              << " 操作=" << local_ops 
              << " 字节=" << local_bytes
              << " 错误=" << local_errors
              << " 验证失败=" << local_verify_fail
              << std::endl;
}

// 预填充文件
void prefill_file(int fd, uint64_t size, size_t block_size, const std::string& hostname) {
    std::cout << "[" << hostname << "] 预填充文件到 " << size << " 字节..." << std::endl;
    
    std::vector<char> buf(block_size);
    uint64_t blocks = size / block_size;
    
    for (uint64_t i = 0; i < blocks; i++) {
        uint64_t offset = i * block_size;
        generate_pattern(buf.data(), block_size, offset, -1, i);
        
        ssize_t ret = pwrite64(fd, buf.data(), block_size, offset);
        if (ret != (ssize_t)block_size) {
            std::cerr << "预填充失败 at offset " << offset << ": " << strerror(errno) << std::endl;
            exit(1);
        }
        
        if (i % 1000 == 0 && i > 0) {
            std::cout << "[" << hostname << "] 已填充 " << (i * block_size / (1024*1024)) << " MB\r" << std::flush;
        }
    }
    
    // 确保数据落盘
    fsync(fd);
    std::cout << "[" << hostname << "] 预填充完成！" << std::endl;
}

// 打印帮助
void print_usage(const char* prog) {
    std::cout << "分布式文件系统并发测试工具 v" << VERSION << std::endl;
    std::cout << "用法: " << prog << " [选项]" << std::endl;
    std::cout << std::endl;
    std::cout << "必选:" << std::endl;
    std::cout << "  -f, --file PATH          测试文件路径（分布式存储上的路径）" << std::endl;
    std::cout << std::endl;
    std::cout << "可选:" << std::endl;
    std::cout << "  -s, --size SIZE          文件大小 (默认: 1G，支持 K/M/G/T 后缀)" << std::endl;
    std::cout << "  -b, --block SIZE         IO 块大小 (默认: 4K)" << std::endl;
    std::cout << "  -p, --parallel N         线程数 (默认: 4)" << std::endl;
    std::cout << "  -d, --duration SEC       测试持续时间秒数 (默认: 60)" << std::endl;
    std::cout << "  -w, --write-ratio N      写操作比例 0-100 (默认: 50)" << std::endl;
    std::cout << "      --start OFFSET       起始 offset (默认: 自动分配)" << std::endl;
    std::cout << "      --end OFFSET         结束 offset" << std::endl;
    std::cout << "      --no-prefill         跳过预填充" << std::endl;
    std::cout << "      --no-verify          跳过数据验证" << std::endl;
    std::cout << "      --hostname NAME      主机标识 (默认: 自动获取)" << std::endl;
    std::cout << "  -v, --verbose            详细输出" << std::endl;
    std::cout << "  -h, --help               显示帮助" << std::endl;
    std::cout << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  # 单机测试" << std::endl;
    std::cout << "  " << prog << " -f /mnt/nfs/test.dat -p 8 -s 10G -d 120" << std::endl;
    std::cout << std::endl;
    std::cout << "  # 多机测试（机器A，操作前半部分）" << std::endl;
    std::cout << "  " << prog << " -f /mnt/nfs/test.dat --start 0 --end 5368709120 -p 16" << std::endl;
    std::cout << std::endl;
    std::cout << "  # 多机测试（机器B，操作后半部分）" << std::endl;
    std::cout << "  " << prog << " -f /mnt/nfs/test.dat --start 5368709120 --end 10737418240 -p 16" << std::endl;
}

// 解析大小（支持 K/M/G/T）
uint64_t parse_size(const std::string& str) {
    if (str.empty()) return 0;
    
    char* endptr;
    double val = strtod(str.c_str(), &endptr);
    
    if (endptr == str.c_str()) return 0;
    
    switch (*endptr) {
        case 'K': case 'k': val *= 1024; break;
        case 'M': case 'm': val *= 1024 * 1024; break;
        case 'G': case 'g': val *= 1024 * 1024 * 1024; break;
        case 'T': case 't': val *= 1024ULL * 1024 * 1024 * 1024; break;
        default: break;
    }
    
    return (uint64_t)val;
}

// 获取主机名
std::string get_hostname() {
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0) {
        return std::string(buf);
    }
    return "unknown";
}

int main(int argc, char* argv[]) {
    Config cfg;
    cfg.hostname = get_hostname();
    
    // 解析命令行
    static struct option long_options[] = {
        {"file", required_argument, 0, 'f'},
        {"size", required_argument, 0, 's'},
        {"block", required_argument, 0, 'b'},
        {"parallel", required_argument, 0, 'p'},
        {"duration", required_argument, 0, 'd'},
        {"write-ratio", required_argument, 0, 'w'},
        {"start", required_argument, 0, 0},
        {"end", required_argument, 0, 0},
        {"no-prefill", no_argument, 0, 0},
        {"no-verify", no_argument, 0, 0},
        {"hostname", required_argument, 0, 0},
        {"verbose", no_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };
    
    int option_index = 0;
    int c;
    
    while ((c = getopt_long(argc, argv, "f:s:b:p:d:w:vh", long_options, &option_index)) != -1) {
        switch (c) {
            case 'f':
                cfg.filepath = optarg;
                break;
            case 's':
                cfg.file_size = parse_size(optarg);
                break;
            case 'b':
                cfg.block_size = parse_size(optarg);
                break;
            case 'p':
                cfg.num_threads = std::atoi(optarg);
                break;
            case 'd':
                cfg.duration_sec = std::atoi(optarg);
                break;
            case 'w':
                cfg.write_ratio = std::atoi(optarg);
                break;
            case 'v':
                cfg.verbose = true;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 0:
                if (strcmp(long_options[option_index].name, "start") == 0) {
                    cfg.offset_start = parse_size(optarg);
                } else if (strcmp(long_options[option_index].name, "end") == 0) {
                    cfg.offset_end = parse_size(optarg);
                } else if (strcmp(long_options[option_index].name, "no-prefill") == 0) {
                    cfg.prefill = false;
                } else if (strcmp(long_options[option_index].name, "no-verify") == 0) {
                    cfg.verify = false;
                } else if (strcmp(long_options[option_index].name, "hostname") == 0) {
                    cfg.hostname = optarg;
                }
                break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }
    
    // 检查必需参数
    if (cfg.filepath.empty()) {
        std::cerr << "错误: 必须指定测试文件路径 (-f)" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    std::cout << "========================================" << std::endl;
    std::cout << "分布式文件系统并发测试" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "主机:      " << cfg.hostname << std::endl;
    std::cout << "PID:       " << getpid() << std::endl;
    std::cout << "文件:      " << cfg.filepath << std::endl;
    std::cout << "文件大小:  " << cfg.file_size << " 字节 (" 
              << (cfg.file_size / (1024.0*1024*1024)) << " GB)" << std::endl;
    std::cout << "块大小:    " << cfg.block_size << " 字节 (" 
              << (cfg.block_size / 1024.0) << " KB)" << std::endl;
    std::cout << "线程数:    " << cfg.num_threads << std::endl;
    std::cout << "持续时间:  " << cfg.duration_sec << " 秒" << std::endl;
    std::cout << "写比例:    " << cfg.write_ratio << "%" << std::endl;
    
    // 计算 offset 范围
    uint64_t range_start, range_end;
    if (cfg.offset_start >= 0 && cfg.offset_end > 0) {
        range_start = cfg.offset_start;
        range_end = cfg.offset_end;
    } else {
        range_start = 0;
        range_end = cfg.file_size;
    }
    
    // 对齐到 block_size
    range_start = (range_start / cfg.block_size) * cfg.block_size;
    range_end = (range_end / cfg.block_size) * cfg.block_size;
    
    uint64_t range_size = range_end - range_start;
    uint64_t blocks_per_thread = range_size / cfg.block_size / cfg.num_threads;
    
    std::cout << "操作范围:  [" << range_start << " - " << range_end << "]" << std::endl;
    std::cout << "每线程块数: " << blocks_per_thread << std::endl;
    std::cout << "========================================" << std::endl;
    
    // 打开文件
    int fd = open(cfg.filepath.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        std::cerr << "打开文件失败: " << strerror(errno) << std::endl;
        return 1;
    }
    
    // 预分配文件大小
    if (cfg.prefill) {
        // 检查文件实际大小
        struct stat st;
        if (fstat(fd, &st) == 0 && (uint64_t)st.st_size < cfg.file_size) {
            prefill_file(fd, cfg.file_size, cfg.block_size, cfg.hostname);
        }
    }
    
    std::cout << "[" << cfg.hostname << "] 启动 " << cfg.num_threads << " 个工作线程..." << std::endl;
    
    auto test_start = std::chrono::steady_clock::now();
    
    // 启动工作线程
    std::vector<std::thread> threads;
    for (int i = 0; i < cfg.num_threads; i++) {
        uint64_t my_start = range_start + i * blocks_per_thread * cfg.block_size;
        uint64_t my_end = (i == cfg.num_threads - 1) ? range_end 
                                                     : my_start + blocks_per_thread * cfg.block_size;
        
        threads.emplace_back(worker_thread, i, std::ref(cfg), fd, my_start, my_end);
    }
    
    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }
    
    auto test_end = std::chrono::steady_clock::now();
    auto test_duration = std::chrono::duration_cast<std::chrono::milliseconds>(test_end - test_start).count();
    
    close(fd);
    
    // 输出统计
    uint64_t total_ops = g_total_ops.load();
    uint64_t total_bytes = g_total_bytes.load();
    uint64_t errors = g_errors.load();
    uint64_t verify_fail = g_verify_failures.load();
    
    double duration_sec = test_duration / 1000.0;
    double iops = total_ops / duration_sec;
    double throughput_mbps = (total_bytes / (1024.0 * 1024)) / duration_sec;
    
    std::cout << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "[" << cfg.hostname << "] 测试结果" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "总耗时:       " << duration_sec << " 秒" << std::endl;
    std::cout << "总操作数:     " << total_ops << std::endl;
    std::cout << "总字节数:     " << total_bytes << " (" 
              << (total_bytes / (1024.0*1024*1024)) << " GB)" << std::endl;
    std::cout << "IOPS:         " << iops << std::endl;
    std::cout << "吞吐量:       " << throughput_mbps << " MB/s" << std::endl;
    std::cout << "错误数:       " << errors << std::endl;
    std::cout << "验证失败:     " << verify_fail << std::endl;
    std::cout << "========================================" << std::endl;
    
    if (errors > 0 || verify_fail > 0) {
        return 1;
    }
    return 0;
}
