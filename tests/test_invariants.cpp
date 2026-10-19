#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <algorithm>

namespace TitanEngine {

    constexpr size_t MAX_PACKET_SIZE = 65535;
    constexpr size_t MIN_PACKET_SIZE = 64;
    constexpr uint32_t MAGIC_HEADER = 0x54495441; // "TITA"

    enum class PacketStatus : uint8_t {
        OK = 0,
        MALFORMED,
        OVERFLOW,
        UNDERFLOW,
        CRC_ERROR,
        DROPPED
    };

    struct PacketHeader {
        uint32_t magic;
        uint16_t length;
        uint16_t sequence;
        uint32_t crc32;
    };

    class Packet {
    private:
        std::unique_ptr<uint8_t[]> data_;
        size_t size_;
        PacketStatus status_;

    public:
        Packet() : size_(0), status_(PacketStatus::OK) {}

        Packet(const uint8_t* src, size_t size) : size_(size), status_(PacketStatus::OK) {
            if (size > MAX_PACKET_SIZE) {
                status_ = PacketStatus::OVERFLOW;
                size_ = 0;
                return;
            }
            if (size < MIN_PACKET_SIZE) {
                status_ = PacketStatus::UNDERFLOW;
            }
            data_ = std::make_unique<uint8_t[]>(size);
            std::memcpy(data_.get(), src, size);
        }

        Packet(const Packet& other) : size_(other.size_), status_(other.status_) {
            if (other.data_) {
                data_ = std::make_unique<uint8_t[]>(size_);
                std::memcpy(data_.get(), other.data_.get(), size_);
            }
        }

        Packet& operator=(const Packet& other) {
            if (this == &other) return *this;
            size_ = other.size_;
            status_ = other.status_;
            if (other.data_) {
                data_ = std::make_unique<uint8_t[]>(size_);
                std::memcpy(data_.get(), other.data_.get(), size_);
            } else {
                data_.reset();
            }
            return *this;
        }

        Packet(Packet&&) noexcept = default;
        Packet& operator=(Packet&&) noexcept = default;

        const uint8_t* data() const { return data_.get(); }
        size_t size() const { return size_; }
        PacketStatus status() const { return status_; }
        void set_status(PacketStatus s) { status_ = s; }
    };

    uint32_t calculate_crc32(const uint8_t* data, size_t length) {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (uint8_t j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ (-(int32_t)(crc & 1) & 0xEDB88320);
            }
        }
        return ~crc;
    }

    class PacketEngine {
    private:
        std::queue<Packet> queue_;
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::atomic<bool> running_;
        std::atomic<uint64_t> processed_count_;
        std::atomic<uint64_t> dropped_count_;
        size_t capacity_;
        uint16_t expected_sequence_;

    public:
        explicit PacketEngine(size_t capacity) 
            : running_(true), processed_count_(0), dropped_count_(0), 
              capacity_(capacity), expected_sequence_(0) {}

        ~PacketEngine() {
            stop();
        }

        void stop() {
            running_ = false;
            cv_.notify_all();
        }

        bool ingest(const uint8_t* raw_data, size_t size) {
            if (!running_) return false;

            Packet pkt(raw_data, size);
            if (pkt.status() == PacketStatus::OVERFLOW) {
                dropped_count_++;
                return false;
            }

            if (size >= sizeof(PacketHeader)) {
                PacketHeader header;
                std::memcpy(&header, raw_data, sizeof(PacketHeader));
                if (header.magic != MAGIC_HEADER) {
                    pkt.set_status(PacketStatus::MALFORMED);
                } else {
                    uint32_t computed_crc = calculate_crc32(raw_data + sizeof(PacketHeader), size - sizeof(PacketHeader));
                    if (computed_crc != header.crc32) {
                        pkt.set_status(PacketStatus::CRC_ERROR);
                    }
                }
            } else {
                pkt.set_status(PacketStatus::MALFORMED);
            }

            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.size() >= capacity_) {
                dropped_count_++;
                return false;
            }

            queue_.push(std::move(pkt));
            cv_.notify_one();
            return true;
        }

        bool fetch(Packet& pkt) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !queue_.empty() || !running_; });

            if (queue_.empty()) return false;

            pkt = std::move(queue_.front());
            queue_.pop();
            processed_count_++;
            return true;
        }

        size_t queue_size() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return queue_.size();
        }

        uint64_t processed_count() const { return processed_count_; }
        uint64_t dropped_count() const { return dropped_count_; }

        void reset_state() {
            std::lock_guard<std::mutex> lock(mutex_);
            while (!queue_.empty()) queue_.pop();
            processed_count_ = 0;
            dropped_count_ = 0;
            running_ = true;
            expected_sequence_ = 0;
        }
    };
}

void test_boundaries() {
    std::cout << "[RUN] test_boundaries..." << std::endl;
    TitanEngine::PacketEngine engine(10);

    // 1. Underflow packet (< 64 bytes)
    std::vector<uint8_t> small_packet(30, 0xAB);
    assert(engine.ingest(small_packet.data(), small_packet.size()) == true);
    
    TitanEngine::Packet p1;
    assert(engine.fetch(p1) == true);
    assert(p1.status() == TitanEngine::PacketStatus::UNDERFLOW);

    // 2. Overflow packet (> 65535 bytes)
    std::vector<uint8_t> large_packet(TitanEngine::MAX_PACKET_SIZE + 1, 0xCD);
    assert(engine.ingest(large_packet.data(), large_packet.size()) == false);
    assert(engine.dropped_count() == 1);

    // 3. Exact Max Boundary
    std::vector<uint8_t> exact_max(TitanEngine::MIN_PACKET_SIZE, 0xEE);
    TitanEngine::PacketHeader header;
    header.magic = TitanEngine::MAGIC_HEADER;
    header.length = TitanEngine::MIN_PACKET_SIZE;
    header.sequence = 1;
    header.crc32 = TitanEngine::calculate_crc32(exact_max.data() + sizeof(TitanEngine::PacketHeader), TitanEngine::MIN_PACKET_SIZE - sizeof(TitanEngine::PacketHeader));
    std::memcpy(exact_max.data(), &header, sizeof(TitanEngine::PacketHeader));

    assert(engine.ingest(exact_max.data(), exact_max.size()) == true);
    TitanEngine::Packet p2;
    assert(engine.fetch(p2) == true);
    assert(p2.status() == TitanEngine::PacketStatus::OK);

    std::cout << "[PASS] test_boundaries passed." << std::endl;
}

void test_invariants() {
    std::cout << "[RUN] test_invariants..." << std::endl;
    TitanEngine::PacketEngine engine(5);

    // CRC Corruption Invariant
    std::vector<uint8_t> pkt_data(TitanEngine::MIN_PACKET_SIZE, 0x00);
    TitanEngine::PacketHeader header;
    header.magic = TitanEngine::MAGIC_HEADER;
    header.length = TitanEngine::MIN_PACKET_SIZE;
    header.sequence = 42;
    header.crc32 = 0xDEADBEEF; // Invalid CRC
    std::memcpy(pkt_data.data(), &header, sizeof(TitanEngine::PacketHeader));

    assert(engine.ingest(pkt_data.data(), pkt_data.size()) == true);
    TitanEngine::Packet p;
    assert(engine.fetch(p) == true);
    assert(p.status() == TitanEngine::PacketStatus::CRC_ERROR);

    // Magic Header Malformation Invariant
    header.magic = 0x11111111; // Bad Magic
    header.crc32 = TitanEngine::calculate_crc32(pkt_data.data() + sizeof(TitanEngine::PacketHeader), TitanEngine::MIN_PACKET_SIZE - sizeof(TitanEngine::PacketHeader));
    std::memcpy(pkt_data.data(), &header, sizeof(TitanEngine::PacketHeader));

    assert(engine.ingest(pkt_data.data(), pkt_data.size()) == true);
    assert(engine.fetch(p) == true);
    assert(p.status() == TitanEngine::PacketStatus::MALFORMED);

    std::cout << "[PASS] test_invariants passed." << std::endl;
}

void test_concurrent_stress() {
    std::cout << "[RUN] test_concurrent_stress..." << std::endl;
    constexpr size_t capacity = 1000;
    TitanEngine::PacketEngine engine(capacity);

    std::atomic<bool> start_signal{false};
    std::atomic<uint64_t> total_ingested{0};
    std::atomic<uint64_t> total_fetched{0};

    const int producers_count = 4;
    const int consumers_count = 4;
    const int iterations = 5000;

    auto producer = [&](int id) {
        while (!start_signal) {
            std::this_thread::yield();
        }
        std::vector<uint8_t> payload(TitanEngine::MIN_PACKET_SIZE, static_cast<uint8_t>(id));
        TitanEngine::PacketHeader header;
        header.magic = TitanEngine::MAGIC_HEADER;
        header.length = TitanEngine::MIN_PACKET_SIZE;
        header.sequence = static_cast<uint16_t>(id);
        header.crc32 = TitanEngine::calculate_crc32(payload.data() + sizeof(TitanEngine::PacketHeader), TitanEngine::MIN_PACKET_SIZE - sizeof(TitanEngine::PacketHeader));
        std::memcpy(payload.data(), &header, sizeof(TitanEngine::PacketHeader));

        for (int i = 0; i < iterations; ++i) {
            if (engine.ingest(payload.data(), payload.size())) {
                total_ingested++;
            }
        }
    };

    auto consumer = [&]() {
        while (!start_signal) {
            std::this_thread::yield();
        }
        TitanEngine::Packet p;
        while (true) {
            if (engine.fetch(p)) {
                total_fetched++;
                if (total_fetched >= (producers_count * iterations)) {
                    break;
                }
            } else if (total_fetched >= (producers_count * iterations)) {
                break;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0;