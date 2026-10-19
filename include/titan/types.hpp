#ifndef TITAN_TYPES_HPP
#ifndef TITAN_TYPES_HPP
#define TITAN_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>
#include <system_error>
#include <chrono>
#include <sys/mman.h>
#include <unistd.h>
#include <immintrin.h>

namespace titan {

// Alignment requirements for cache lines and huge pages
constexpr std::size_t kCacheLineSize = 64;
constexpr std::size_t kHugePageSize2MB = 2 * 1024 * 1024;

// 32-byte Fixed-Size Metadata Descriptor
struct alignas(32) PacketDescriptor {
    uint32_t arena_offset;     // Offset into the ArenaAllocator memory block
    uint32_t length;           // Packet length in bytes
    uint64_t timestamp;        // Hardware/Software nanosecond timestamp
    uint64_t flow_id;          // Flow identifier for sliding-window rate limiting
    uint32_t sequence_number;  // Monotonic sequence number for lag/drop detection
    uint32_t generation_id;    // Arena generation ID to prevent use-after-free
};

static_assert(sizeof(PacketDescriptor) == 32, "PacketDescriptor must be exactly 32 bytes.");

enum class DropPolicy : uint8_t {
    DropTail = 0,
    DropHead = 1
};

struct TelemetryCounters {
    std::atomic<uint64_t> total_allocated_bytes{0};
    std::atomic<uint64_t> dropped_packets{0};
    std::atomic<uint64_t> ring_overflows{0};
    std::atomic<uint64_t> allocation_failures{0};
};

class ArenaAllocator {
public:
    explicit ArenaAllocator(std::size_t capacity = kHugePageSize2MB)
        : capacity_(capacity), offset_(0), generation_id_(1), base_ptr_(nullptr) {
        
        // Round up capacity to huge page boundary if needed
        if (capacity_ % kHugePageSize2MB != 0) {
            capacity_ = ((capacity_ + kHugePageSize2MB - 1) / kHugePageSize2MB) * kHugePageSize2MB;
        }

        void* ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        
        if (ptr == MAP_FAILED) {
            // Fallback to standard anonymous mapping if huge pages are unavailable
            ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (ptr == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "ArenaAllocator mmap failed");
            }
        }

        base_ptr_ = static_cast<uint8_t*>(ptr);
    }

    ~ArenaAllocator() {
        if (base_ptr_ && base_ptr_ != MAP_FAILED) {
            munmap(base_ptr_, capacity_);
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&& other) noexcept
        : capacity_(other.capacity_),
          offset_(other.offset_.load(std::memory_order_relaxed)),
          generation_id_(other.generation_id_.load(std::memory_order_relaxed)),
          base_ptr_(other.base_ptr_),
          telemetry_(other.telemetry_) {
        other.base_ptr_ = nullptr;
        other.capacity_ = 0;
        other.offset_.store(0, std::memory_order_relaxed);
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (base_ptr_ && base_ptr_ != MAP_FAILED) {
                munmap(base_ptr_, capacity_);
            }
            capacity_ = other.capacity_;
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            generation_id_.store(other.generation_id_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            base_ptr_ = other.base_ptr_;
            telemetry_ = other.telemetry_;
            other.base_ptr_ = nullptr;
            other.capacity_ = 0;
            other.offset_.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    // Thread-safe bump pointer allocation maintaining 64-byte alignment
    bool allocate(std::size_t size, uint32_t& out_offset, uint32_t& out_gen_id) {
        // Align size to 64-byte cache line boundary
        std::size_t aligned_size = (size + kCacheLineSize - 1) & ~(kCacheLineSize - 1);
        
        std::size_t current_offset = offset_.load(std::memory_order_relaxed);
        while (true) {
            if (current_offset + aligned_size > capacity_) {
                telemetry_.allocation_failures.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            if (offset_.compare_exchange_weak(current_offset, current_offset + aligned_size,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                out_offset = static_cast<uint32_t>(current_offset);
                out_gen_id = generation_id_.load(std::memory_order_relaxed);
                telemetry_.total_allocated_bytes.fetch_add(aligned_size, std::memory_order_relaxed);
                return true;
            }
        }
    }

    void reset() {
        offset_.store(0, std::memory_order_release);
        generation_id_.fetch_add(1, std::memory_order_relaxed);
    }

    [[nodiscard]] uint8_t* get_address(uint32_t offset) const noexcept {
        return base_ptr_ + offset;
    }

    [[nodiscard]] uint32_t get_generation_id() const noexcept {
        return generation_id_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] TelemetryCounters& telemetry() noexcept {
        return telemetry_;
    }

private:
    std::size_t capacity_;
    std::atomic<std::size_t> offset_;
    std::atomic<uint32_t> generation_id_;
    uint8_t* base_ptr_;
    TelemetryCounters telemetry_;
};

template <typename T, std::size_t Capacity>
class MpscFifoRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Ring capacity must be a power of two.");
public:
    explicit MpscFifoRing(DropPolicy policy = DropPolicy::DropTail)
        : head_(0), tail_(0), drop_policy_(policy) {}

    ~MpscFifoRing() = default;

    MpscFifoRing(const MpscFifoRing&) = delete;
    MpscFifoRing& operator=(const MpscFifoRing&) = delete;

    bool push(const T& item, TelemetryCounters& telemetry) {
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        while (true) {
            std::size_t current_head = head_.load(std::memory_order_acquire);
            if ((current_tail - current_head) >= Capacity) {
                telemetry.ring_overflows.fetch_add(1, std::memory_order_relaxed);
                if (drop_policy_ == DropPolicy::DropTail) {
                    telemetry.dropped_packets.fetch_add(1, std::memory_order_relaxed);
                    return false; // Drop incoming packet
                } else {
                    // Drop Head policy: Force advance head to make room
                    if (head_.compare_exchange_weak(current_head, current_head + 1,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed)) {
                        telemetry.dropped_packets.fetch_add(1, std::memory_order_relaxed);
                        break; // Room made, proceed to push
                    }
                    continue;
                }
            }
            if (tail_.compare_exchange_weak(current_tail, current_tail + 1,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                buffer_[current_tail & kMask] = item;
                return true;
            }
        }

        // Fallback for DropHead execution path
        current_tail = tail_.load(std::memory_order_relaxed);
        while (true) {
            std::size_t current_head = head_.load(std::memory_order_acquire);
            if ((current_tail - current_head) >= Capacity) {
                continue; // Re-check if race condition occurred
            }
            if (tail_.compare_exchange_weak(current_tail, current_tail + 1,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                buffer_[current_tail & kMask] = item;
                return true;
            }
        }
    }

    bool pop(T& item) {
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Ring is empty
        }

        item = buffer_[current_head & kMask];
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        return (current_tail >= current_head) ? (current_tail - current_head) : 0;
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;
    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    T buffer_[Capacity];
    DropPolicy drop_policy_;
};

template <typename T, std::size_t Capacity>
class SpmcFifoRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Ring capacity must be a power of two.");
public:
    SpmcFifoRing() : head_(0), tail_(0) {}

    ~SpmcFifoRing() = default;

    SpmcFifoRing(const SpmcFifoRing&) = delete;
    SpmcFifoRing& operator=(const SpmcFifoRing&) = delete;

    bool push(const T& item, TelemetryCounters& telemetry) {
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            telemetry.ring_overflows.fetch_add(1, std::memory_order_relaxed);
            telemetry.dropped_packets.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        buffer_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        while (true) {
            std::size_t current_tail = tail_.load(std::memory_order_acquire);
            if (current_head == current_tail) {
                return false; // Ring empty
            }

            if (head_.compare_exchange_weak(current_head, current_head + 1,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                item = buffer_[current_head & kMask];
                return true;
            }
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        return (current_tail >= current_head) ? (current_tail - current_head) : 0;
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;
    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    T buffer_[Capacity];
};

class TokenBucketRateLimiter {
public:
    TokenBucketRateLimiter(uint64_t max_tokens, uint64_t refill_rate_per_sec)
        : max_tokens_(max_tokens),
          tokens_(max_tokens),
          refill_rate_(refill_rate_per_sec),
          last_refill_timestamp_(get_current_nanoseconds()) {}

    bool consume(uint64_t tokens_to_consume) noexcept {
        refill();
        uint64_t current_tokens = tokens_.load(std::memory_order_relaxed);
        while (true) {
            if (current_tokens < tokens_to_consume) {
                return false; // Rate limit exceeded
            }
            if (tokens_.compare_exchange_weak(current_tokens, current_tokens - tokens_to_consume,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return true;
            }
        }
    }

private:
    void refill() noexcept {
        uint64_t now = get_current_nanoseconds();
        uint64_t last = last_refill_timestamp_.load(std::memory_order_relaxed);
        if (now <= last) {
            return;
        }

        if (last_refill_timestamp_.compare_exchange_strong(last, now,
                                                           std::memory_order_relaxed,
                                                           std::memory_order_relaxed)) {
            uint64_t elapsed_ns = now - last;
            uint64_t generated_tokens = (elapsed_ns * refill_rate_) / 1'000'000'000ULL;
            if (generated_tokens > 0) {
                uint64_t current = tokens_.load(std::memory_order_relaxed);
                uint64_t updated;
                do {
                    updated = (current + generated_tokens > max_tokens_) ? max_tokens_ : (current + generated_tokens);
                } while (!tokens_.compare_exchange_weak(current, updated,
                                                        std::memory_order_release,
                                                        std::memory_order_relaxed));
            }
        }
    }

    [[nodiscard]] static uint64_t get_current_nanoseconds() noexcept {
        auto now = std::chrono::steady_clock::now();
        auto duration = now.time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
    }

    uint64_t max_tokens_;
    std::atomic<uint64_t> tokens_;
    uint64_t refill_rate_;
    std::atomic<uint64_t> last_refill_timestamp_;
};

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M