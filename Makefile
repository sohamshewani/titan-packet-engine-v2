CXX ?= g++
CXXFLAGS = -std=c++17 -O3 -Wall -Wextra -pthread
INCLUDES = -Iinclude

TARGET = titan-packet-engine
SRC_DIR = src
TEST_DIR = tests
BENCH_DIR = benchmarks
BUILD_DIR = build

SOURCES = $(wildcard $(SRC_DIR)/*.cpp)
OBJECTS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SOURCES))

TEST_SOURCES = $(wildcard $(TEST_DIR)/*.cpp)
BENCH_SOURCES = $(wildcard $(BENCH_DIR)/*.cpp)

.PHONY: all build test clean benchmark directories

all: directories $(BUILD_DIR)/$(TARGET)

directories:
	@mkdir -p $(BUILD_DIR)

build: all

$(BUILD_DIR)/$(TARGET): $(OBJECTS)
	@if [ -n "$(OBJECTS)" ]; then \
		$(CXX) $(CXXFLAGS) $(INCLUDES) $(OBJECTS) -o $@; \
	else \
		echo "No source files found in $(SRC_DIR). Creating dummy executable."; \
		echo "int main() {}" > $(BUILD_DIR)/dummy.cpp; \
		$(CXX) $(CXXFLAGS) $(BUILD_DIR)/dummy.cpp -o $@; \
		rm -f $(BUILD_DIR)/dummy.cpp; \
	fi

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: directories
	@if [ -d "$(TEST_DIR)" ] && [ -n "$(TEST_SOURCES)" ]; then \
		echo "Building and running tests..."; \
		$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_SOURCES) -o $(BUILD_DIR)/runner_test -lgtest -lgtest_main -lpthread; \
		$(BUILD_DIR)/runner_test; \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: directories
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$(BENCH_SOURCES)" ]; then \
		echo "Building and running benchmarks..."; \
		$(CXX) $(CXXFLAGS) $(INCLUDES) $(BENCH_SOURCES) -o $(BUILD_DIR)/runner_bench -lbenchmark -lpthread; \
		$(BUILD_DIR)/runner_bench; \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts."