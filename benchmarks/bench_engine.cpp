#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <cstdint>

// Mocking the 'titan-packet-engine' API for the sake of a self-contained, compilable microbenchmark.
// In a real integration, this would include the actual Titan packet engine headers.
namespace titan {
    namespace engine {
        struct PacketBuffer {
            uint8_t data[1500];
            size_t length;
            uint32_t flags;
        };

        class PacketEngine {
        public:
            bool initialize() { return true; }
            inline bool process_packet(PacketBuffer& pkt) {
                // Simulate processing overhead of the packet engine
                pkt.flags |= 0x1;
                return true;
            }
        };
    }
}

int main() {
    // Configuration
    const size_t WARMUP_ITERATIONS = 50000;
    const size_t BENCHMARK_ITERATIONS = 1000000;

    titan::engine::PacketEngine engine;
    if (!engine.initialize()) {
        std::cerr << "Failed to initialize titan-packet-engine" << std::endl;
        return 1;
    }

    titan::engine::PacketBuffer pkt;
    pkt.length = 64; // Standard minimum Ethernet frame size for throughput stress
    std::fill(std::begin(pkt.data), std::end(pkt.data), 0xAB);
    pkt.flags = 0;

    // Allocate latency record storage pre-benchmark to avoid allocation overhead during measurement
    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(BENCHMARK_ITERATIONS);

    // 1. Warmup phase
    for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        engine.process_packet(pkt);
    }

    // 2. Benchmark execution phase (Measuring individual packet processing latency via rdtsc/high_resolution_clock)
    auto total_start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; ++i) {
        auto start_ts = std::chrono::high_resolution_clock::now();
        
        // Core operation being benchmarked
        engine.process_packet(pkt);

        auto end_ts = std::chrono::high_resolution_clock::now();
        auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
        
        latencies_ns.push_back(static_cast<uint64_t>(duration_ns));
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration_sec = std::chrono::duration<double>(total_end - total_start).count();

    // 3. Post-processing metrics
    std::sort(latencies_ns.begin(), latencies_ns.end());

    double ops_sec = static_cast<double>(BENCHMARK_ITERATIONS) / total_duration_sec;
    uint64_t p50_ns = latencies_ns[BENCHMARK_ITERATIONS * 50 / 100];
    uint64_t p99_ns = latencies_ns[BENCHMARK_ITERATIONS * 99 / 100];
    uint64_t p999_ns = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 99.9 / 100)];
    
    uint64_t min_ns = latencies_ns.front();
    uint64_t max_ns = latencies_ns.back();

    // 4. Formatted tabular output to stdout
    std::cout << "\n";
    std::cout << "=================================================================\n";
    std::cout << "      TITAN-PACKET-ENGINE THROUGHPUT & LATENCY MICROBENCHMARK    \n";
    std::cout << "=================================================================\n";
    std::cout << std::left << std::setw(30) << "Metric" 
              << std::right << std::setw(15) << "Value" 
              << std::setw(15) << "Unit" << "\n";
    std::cout << "-----------------------------------------------------------------\n";
    
    std::cout << std::left << std::setw(30) << "Total Iterations" 
              << std::right << std::setw(15) << BENCHMARK_ITERATIONS 
              << std::setw(15) << "ops" << "\n";
              
    std::cout << std::left << std::setw(30) << "Total Duration" 
              << std::right << std::setw(15) << std::fixed << std::setprecision(4) << total_duration_sec 
              << std::setw(15) << "s" << "\n";

    std::cout << std::left << std::setw(30) << "Throughput (Ops/Sec)" 
              << std::right << std::setw(15) << static_cast<uint64_t>(ops_sec) 
              << std::setw(15) << "ops/sec" << "\n";

    std::cout << std::left << std::setw(30) << "Min Latency" 
              << std::right << std::setw(15) << min_ns 
              << std::setw(15) << "ns" << "\n";

    std::cout << std::left << std::setw(30) << "Median Latency (p50)" 
              << std::right << std::setw(15) << p50_ns 
              << std::setw(15) << "ns" << "\n";

    std::cout << std::left << std::setw(30) << "Tail Latency (p99)" 
              << std::right << std::setw(15) << p99_ns 
              << std::setw(15) << "ns" << "\n";

    std::cout << std::left << std::setw(30) << "High Tail Latency (p99.9)" 
              << std::right << std::setw(15) << p999_ns 
              << std::setw(15) << "ns" << "\n";

    std::cout << std::left << std::setw(30) << "Max Latency" 
              << std::right << std::setw(15) << max_ns 
              << std::setw(15) << "ns" << "\n";
              
    std::cout << "=================================================================\n";
    std::cout << std::endl;

    return 0;
}