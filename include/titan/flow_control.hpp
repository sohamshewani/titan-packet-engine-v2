#ifndef TITAN_FLOW_CONTROL_HPP
#ifndef TITAN_FLOW_CONTROL_HPP
#define TITAN_FLOW_CONTROL_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <new>
#include <chrono>
#include <system_error>
#include <immintrin.h>
#include <sys/mman.h>
#include <unistd.h>

namespace titan {

struct alignas(64) PacketDescriptor {
    uint64_t arena_offset;
    uint32_t length;
    uint32_t flow_id;
    uint64_t timestamp_ns;
    uint64_t sequence_number;
};

static_assert(sizeof(PacketDescriptor) == 32, "PacketDescriptor must be exactly 32 bytes.");

class ArenaAllocator {
public:
    explicit ArenaAllocator(size_t capacity_bytes)
        : capacity_(capacity_bytes), offset_(0) {
        // Round up to huge page alignment (2MB)
        constexpr size_t huge_page_size = 2 * 1024 * 1024;
        capacity_ = (capacity_ + huge_page_size - 1) & ~(huge_page_size - 1);

        int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
        buffer_ = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (buffer_ == MAP_FAILED) {
            // Fallback to standard mmap if huge pages are unavailable
            flags = MAP_PRIVATE | MAP_ANONYMOUS;
            buffer_ = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, flags, -1, 0);
            if (buffer_ == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "ArenaAllocator mmap failed");
            }
        }
    }

    ~ArenaAllocator() {
        if (buffer_ && buffer_ != MAP_FAILED) {
            munmap(buffer_, capacity_);
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&& other) noexcept
        : buffer_(other.buffer_), capacity_(other.capacity_), offset_(other.offset_.load()) {
        other.buffer_ = nullptr;
        other.capacity_ = 0;
        other.offset_ = 0;
    }
    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (buffer_ && buffer_ != MAP_FAILED) {
                munmap(buffer_, capacity_);
            }
            buffer_ = other.buffer_;
            capacity_ = other.capacity_;
            offset_ = other.offset_.load();
            other.buffer_ = nullptr;
            other.capacity_ = 0;
            other.offset_ = 0;
        }
        return *this;
    }

    void* allocate(size_t size, size_t alignment = 64) {
        size_t current_offset = offset_.load(std::memory_order_relaxed);
        while (true) {
            size_t aligned_offset = (current_offset + alignment - 1) & ~(alignment - 1);
            size_t next_offset = aligned_offset + size;
            if (next_offset > capacity_) {
                return nullptr; // Arena exhausted
            }
            if (offset_.compare_exchange_weak(current_offset, next_offset,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                return static_cast<char*>(buffer_) + aligned_offset;
            }
        }
    }

    void reset() {
        offset_.store(0, std::memory_order_release);
    }

    uint64_t offset_of(const void* ptr) const {
        return static_cast<uint64_t>(static_cast<const char*>(ptr) - static_cast<const char*>(buffer_));
    }

    void* address_at(uint64_t offset) const {
        return static_cast<char*>(buffer_) + offset;
    }

    size_t capacity() const { return capacity_; }
    size_t allocated_bytes() const { return offset_.load(std::memory_order_relaxed); }

private:
    void* buffer_{nullptr};
    size_t capacity_{0};
    std::atomic<size_t> offset_{0};
};

template <typename T, size_t Capacity>
class MPSC_RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    MPSC_RingBuffer() : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            sequence_[i].store(i, std::memory_order_relaxed);
        }
    }

    ~MPSC_RingBuffer() = default;

    MPSC_RingBuffer(const MPSC_RingBuffer&) = delete;
    MPSC_RingBuffer& operator=(const MPSC_RingBuffer&) = delete;

    bool push(const T& item) {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        while (true) {
            size_t idx = current_tail & mask_;
            size_t seq = sequence_[idx].load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(current_tail);

            if (dif == 0) {
                if (tail_.compare_exchange_weak(current_tail, current_tail + 1,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed)) {
                    buffer_[idx] = item;
                    sequence_[idx].store(current_tail + 1, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false; // Ring full
            } else {
                current_tail = tail_.load(std::memory_order_relaxed);
            }
        }
    }

    bool pop(T& item) {
        size_t current_head = head_.load(std::memory_order_relaxed);
        size_t idx = current_head & mask_;
        size_t seq = sequence_[idx].load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(current_head + 1);

        if (dif == 0) {
            item = buffer_[idx];
            head_.store(current_head + 1, std::memory_order_release);
            sequence_[idx].store(current_head + Capacity, std::memory_order_release);
            return true;
        }
        return false; // Ring empty
    }

    bool empty() const {
        size_t current_head = head_.load(std::memory_order_relaxed);
        size_t idx = current_head & mask_;
        size_t seq = sequence_[idx].load(std::memory_order_acquire);
        return static_cast<intptr_t>(seq) - static_cast<intptr_t>(current_head + 1) < 0;
    }

private:
    static constexpr size_t mask_ = Capacity - 1;
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    alignas(64) std::atomic<size_t> sequence_[Capacity];
    T buffer_[Capacity];
};

class TokenBucketRateLimiter {
public:
    TokenBucketRateLimiter(uint64_t capacity, uint64_t refill_rate_per_sec)
        : capacity_(capacity),
          tokens_(capacity),
          refill_rate_(refill_rate_per_sec),
          last_refill_timestamp_(now_ns()) {}

    bool consume(uint64_t tokens = 1) {
        refill();
        uint64_t current_tokens = tokens_.load(std::memory_order_relaxed);
        while (true) {
            if (current_tokens < tokens) {
                return false; // Rate limit exceeded
            }
            if (tokens_.compare_exchange_weak(current_tokens, current_tokens - tokens,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
                return true;
            }
        }
    }

private:
    uint64_t now_ns() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    }

    void refill() {
        uint64_t current_time = now_ns();
        uint64_t last_time = last_refill_timestamp_.load(std::memory_order_relaxed);
        if (current_time <= last_time) {
            return;
        }

        if (last_refill_timestamp_.compare_exchange_weak(last_time, current_time,
                                                         std::memory_order_relaxed,
                                                         std::memory_order_relaxed)) {
            uint64_t elapsed_ns = current_time - last_time;
            uint64_t generated_tokens = (elapsed_ns * refill_rate_) / 1000000000ULL;
            if (generated_tokens > 0) {
                uint64_t current_tokens = tokens_.load(std::memory_order_relaxed);
                while (true) {
                    uint64_t new_tokens = std::min(capacity_, current_tokens + generated_tokens);
                    if (tokens_.compare_exchange_weak(current_tokens, new_tokens,
                                                      std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
                        break;
                    }
                }
            }
        }
    }

    uint64_t capacity_;
    std::atomic<uint64_t> tokens_;
    uint64_t refill_rate_;
    std::atomic<uint64_t> last_refill_timestamp_;
};

class FlowController {
public:
    FlowController(size_t ring_capacity, uint64_t token_capacity, uint64_t refill_rate)
        : ring_(), limiter_(token_capacity, refill_rate), dropped_frames_counter_(0), sequence_tracker_(0) {}

    bool admit_packet(const PacketDescriptor& desc, ArenaAllocator& arena, const void* payload, size_t payload_len) {
        if (!limiter_.consume(1)) {
            dropped_frames_counter_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        void* dest = arena.allocate(payload_len, 64);
        if (!dest) {
            dropped_frames_counter_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        // Copy raw payload into zero-copy arena buffer
        __builtin_memcpy(dest, payload, payload_len);

        PacketDescriptor updated_desc = desc;
        updated_desc.arena_offset = arena.offset_of(dest);
        updated_desc.length = static_cast<uint32_t>(payload_len);
        updated_desc.sequence_number = sequence_tracker_.fetch_add(1, std::memory_order_relaxed);

        while (!ring_.push(updated_desc)) {
            _mm_pause();
            // Drop-tail implementation on ring saturation
            dropped_frames_counter_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        return true;
    }

    bool poll_packet(PacketDescriptor& desc) {
        return ring_.pop(desc);
    }

    uint64_t get_dropped_frames() const {
        return dropped_frames_counter_.load(std::memory_order_relaxed);
    }

private:
    MPSC_RingBuffer<PacketDescriptor, 4096> ring_;
    TokenBucketRateLimiter limiter_;
    std::atomic<uint64_t> dropped_frames_counter_;
    std::atomic<uint64_t> sequence_tracker_;
};

} // namespace titan

#endif // TITAN_FLOW_CONTROL_HPP
#endif // TITAN_FLOW_CONTROL_HPP