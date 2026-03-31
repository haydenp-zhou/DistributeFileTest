# 分布式文件系统并发测试工具 Makefile

CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -pthread
DEBUG_FLAGS = -std=c++17 -g -O0 -Wall -pthread -DDEBUG

TARGET = distfs_test
SRC = distfs_test.cpp

.PHONY: all clean debug run-test

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< -pthread

debug: $(SRC)
	$(CXX) $(DEBUG_FLAGS) -o $(TARGET)_debug $< -pthread

clean:
	rm -f $(TARGET) $(TARGET)_debug *.o

# 快速测试目标
run-test: $(TARGET)
	./$(TARGET) -f /tmp/test_distfs.dat -s 100M -b 4K -p 4 -d 10 -v

# 使用 Docker 测试（如果有）
docker-test:
	docker run --rm -v $(PWD):/workspace -w /workspace gcc:latest \
		bash -c "g++ -std=c++17 -O2 -o distfs_test distfs_test.cpp -pthread && ./distfs_test --help"

help:
	@echo "目标:"
	@echo "  make          - 编译测试程序"
	@echo "  make debug    - 编译调试版本"
	@echo "  make clean    - 清理编译产物"
	@echo "  make run-test - 编译并运行快速测试"
	@echo ""
	@echo "用法示例:"
	@echo "  # 单机测试"
	@echo "  ./distfs_test -f /mnt/nfs/test.dat -s 1G -p 8 -d 60"
	@echo ""
	@echo "  # 多机测试（机器A）"
	@echo "  ./distfs_test -f /mnt/nfs/test.dat --start 0 --end 5368709120 -p 16"
	@echo ""
	@echo "  # 多机测试（机器B）"
	@echo "  ./distfs_test -f /mnt/nfs/test.dat --start 5368709120 --end 10737418240 -p 16"
