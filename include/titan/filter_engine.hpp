#ifndef TITAN_FILTER_ENGINE_HPP
#define TITAN_FILTER_ENGINE_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>
#include <system_error>
#include <chrono>
#include <cstring>
#include <sys/mman.h>

namespace titan {

#ifdef __x86_64__
inline void cpu_relax() noexcept {
    _mm_pause();
}
#else
inline void cpu_relax() noexcept {
    asm volatile("yield" ::: "memory");
}
#endif

enum class DropPolicy : uint8_t {
    DropNewest = 0,
    DropOldest = 1
};

struct PacketDescriptor {
    uint64_t arena_offset;
    uint32_t length;
    uint32_t flow_id;
    uint64_t timestamp;
    uint64_t sequence_number;
} __attribute__((aligned(32)));

static_assert(sizeof(PacketDescriptor) == 32, "PacketDescriptor must be exactly 32 bytes.");

class ArenaAllocator {
public:
    explicit ArenaAllocator(size_t capacity_bytes)
        : capacity_(capacity_bytes), current_offset_(0), generation_id_(1) {
        
        void* ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr == MAP_FAILED) {
            ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (ptr == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "ArenaAllocator mmap failed");
            }
        }
        buffer_ = static_cast<uint8_t*>(ptr);
    }

    ~ArenaAllocator() {
        if (buffer_ && buffer_ != MAP_FAILED) {
            munmap(buffer_, capacity_);
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&& other) noexcept
        : buffer_(other.buffer_), capacity_(other.capacity_),
          current_offset_(other.current_offset_.load(std::memory_order_relaxed)),
          generation_id_(other.generation_id_.load(std::memory_order_relaxed)) {
        other.buffer_ = nullptr;
        other.capacity_ = 0;
    }
    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (buffer_ && buffer_ != MAP_FAILED) {
                munmap(buffer_, capacity_);
            }
            buffer_ = other.buffer_;
            capacity_ = other.capacity_;
            current_offset_.store(other.current_offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            generation_id_.store(other.generation_id_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.buffer_ = nullptr;
            other.capacity_ = 0;
        }
        return this;
    }

    uint8_t* allocate(size_t size, uint64_t& out_offset, uint64_t& out_generation) noexcept {
        size_t aligned_size = (size + 63) & ~size_t(63);
        size_t old_offset = current_offset_.load(std::memory_order_relaxed);
        
        while (true) {
            if (old_offset + aligned_size > capacity_) {
                return nullptr;
            }
            if (current_offset_.compare_exchange_weak(old_offset, old_offset + aligned_size,
                                                      std::memory_order_acquire,
                                                      std::memory_order_relaxed)) {
                out_offset = old_offset;
                out_generation = generation_id_.load(std::memory_order_relaxed);
                return buffer_ + old_offset;
            }
        }
    }

    void reset() noexcept {
        current_offset_.store(0, std::memory_order_release);
        generation_id_.fetch_add(1, std::memory_order_release);
    }

    bool validate(uint64_t offset, uint64_t gen) const noexcept {
        return gen == generation_id_.load(std::memory_order_acquire) && offset < capacity_;
    }

    uint8_t* get_base() const noexcept { return buffer_; }
    size_t get_capacity() const noexcept { return capacity_; }
    uint64_t get_generation() const noexcept { return generation_id_.load(std::memory_order_relaxed); }

private:
    uint8_t* buffer_;
    size_t capacity_;
    alignas(64) std::atomic<size_t> current_offset_;
    alignas(64) std::atomic<uint64_t> generation_id_;
};

template <typename T, size_t Capacity>
class MpscFifoRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    MpscFifoRing() : head_(0), tail_(0), dropped_count_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            nodes_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~MpscFifoRing() = default;

    MpscFifoRing(const MpscFifoRing&) = delete;
    MpscFifoRing& operator=(const MpscFifoRing&) = delete;

    bool push(const T& item, DropPolicy policy = DropPolicy::DropNewest) noexcept {
        Node* node;
        size_t pos = tail_.load(std::memory_order_relaxed);
        while (true) {
            node = &nodes_[pos & Mask];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                if (policy == DropPolicy::DropNewest) {
                    dropped_count_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                } else {
                    size_t current_head = head_.load(std::memory_order_relaxed);
                    if (pos - current_head >= Capacity) {
                        dropped_count_.fetch_add(1, std::memory_order_relaxed);
                        return false;
                    }
                    pos = tail_.load(std::memory_order_relaxed);
                }
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
        node->data = item;
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        Node* node;
        size_t pos = head_.load(std::memory_order_relaxed);
        while (true) {
            node = &nodes_[pos & Mask];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (dif == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
        item = node->data;
        node->sequence.store(pos + Mask + 1, std::memory_order_release);
        return true;
    }

    size_t size() const noexcept {
        size_t head_val = head_.load(std::memory_order_acquire);
        size_t tail_val = tail_.load(std::memory_order_acquire);
        if (tail_val >= head_val) {
            return tail_val - head_val;
        }
        return 0;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    uint64_t get_dropped_count() const noexcept {
        return dropped_count_.load(std::memory_order_relaxed);
    }

private:
    struct Node {
        T data;
        std::atomic<size_t> sequence;
    };

    static constexpr size_t Mask = Capacity - 1;
    Node nodes_[Capacity];
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    alignas(64) std::atomic<uint64_t> dropped_count_;
};

class TokenBucketRateLimiter {
public:
    TokenBucketRateLimiter(uint64_t capacity, uint64_t refill_rate_per_sec)
        : capacity_(capacity),
          tokens_(capacity),
          refill_rate_(refill_rate_per_sec),
          last_refill_timestamp_(get_time_ns()) {}

    bool consume(uint64_t tokens_to_consume) noexcept {
        uint64_t now = get_time_ns();
        uint64_t last = last_refill_timestamp_.load(std::memory_order_relaxed);
        
        if (now > last) {
            uint64_t delta_ns = now - last;
            uint64_t new_tokens = (delta_ns * refill_rate_) / 1000000000ULL;
            if (new_tokens > 0) {
                if (last_refill_timestamp_.compare_exchange_weak(last, now, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    uint64_t current_tokens = tokens_.load(std::memory_order_relaxed);
                    uint64_t updated_tokens = std::min(capacity_, current_tokens + new_tokens);
                    tokens_.store(updated_tokens, std::memory_order_relaxed);
                }
            }
        }

        uint64_t current = tokens_.load(std::memory_order_relaxed);
        while (true) {
            if (current < tokens_to_consume) {
                return false;
            }
            if (tokens_.compare_exchange_weak(current, current - tokens_to_consume,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    void set_rate(uint64_t new_capacity, uint64_t new_refill_rate) noexcept {
        capacity_ = new_capacity;
        refill_rate_ = new_refill_rate;
        tokens_.store(new_capacity, std::memory_order_relaxed);
        last_refill_timestamp_.store(get_time_ns(), std::memory_order_relaxed);
    }

private:
    static uint64_t get_time_ns() noexcept {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_init_time(ts);
    }

    static constexpr uint64_t static_init_time(const struct timespec& ts) noexcept {
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    }

    uint64_t capacity_;
    alignas(64) std::atomic<uint64_t> tokens_;
    uint64_t refill_rate_;
    alignas(64) std::atomic<uint64_t> last_refill_timestamp_;
};

class FilterEngine {
public:
    FilterEngine(size_t arena_size, uint64_t token_capacity, uint64_t refill_rate)
        : arena_(arena_size),
          limiter_(token_capacity, refill_rate),
          sequence_counter_(0),
          processed_packets_(0),
          rate_limited_drops_(0) {}

    ~FilterEngine() = default;

    FilterEngine(const FilterEngine&) = delete;
    FilterEngine& operator=(const FilterEngine&) = delete;

    bool ingest_packet(const uint8_t* raw_packet, uint32_t length, uint32_t flow_id, PacketDescriptor& out_descriptor) noexcept {
        if (!raw_packet || length == 0) {
            return false;
        }

        if (!limiter_.consume(1)) {
            rate_limited_drops_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        uint64_t arena_offset = 0;
        uint64_t generation = 0;
        uint8_t* dest = arena_.allocate(length, arena_offset, generation);
        if (!dest) {
            return false;
        }

        std::memcpy(dest, raw_packet, length);

        out_descriptor.arena_offset = arena_offset;
        out_descriptor.length = length;
        out_descriptor.flow_id = flow_id;
        out_descriptor.timestamp = get_time_ns();
        out_descriptor.sequence_number = sequence_counter_.fetch_add(1, std::memory_order_relaxed);

        processed_packets_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool validate_descriptor(const PacketDescriptor& desc) const noexcept {
        return arena_.validate(desc.arena_offset, arena_.get_generation()) &&
               (desc.arena_offset + desc.length <= arena_.get_capacity());
    }

    const uint8_t* resolve_payload(const PacketDescriptor& desc) const noexcept {
        if (!validate_descriptor(desc)) {
            return nullptr;
        }
        return arena_.get_base() + desc.arena_offset;
    }

    void reset_arena() noexcept {
        arena_.reset();
    }

    uint64_t get_processed_count() const noexcept {
        return processed_packets_.load(std::memory_order_relaxed);
    }

    uint64_t get_rate_limited_drops() const noexcept {
        return rate_limited_drops_.load(std::memory_order_relaxed);
    }

    ArenaAllocator& get_arena() noexcept { return arena_; }
    TokenBucketRateLimiter& get_limiter() noexcept { return limiter_; }

private:
    static uint64_t get_time_ns() noexcept {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    }

    ArenaAllocator arena_;
    TokenBucketRateLimiter limiter_;
    alignas(64) std::atomic<uint64_t> sequence_counter_;
    alignas(64) std::atomic<uint64_t> processed_packets_;
    alignas(64) std::atomic<uint64_t> rate_limited_drops_;
};

} // namespace titan

#endif // TITAN_FILTER_ENGINE_HPP