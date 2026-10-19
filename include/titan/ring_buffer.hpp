#ifndef TITAN_RING_BUFFER_HPP
#define TITAN_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <new>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>
#include <immintrin.h>

namespace titan {

#ifdef __cpp_lib_hardware_interference_size
    using std::hardware_destructive_interference_size;
#else
    constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

enum class DropPolicy : uint8_t {
    DropNewest = 0,
    DropOldest = 1
};

struct alignas(32) PacketDescriptor {
    uint64_t arena_offset;
    uint32_t length;
    uint32_t flow_id;
    uint64_t timestamp;
    uint64_t sequence;
};

static_assert(sizeof(PacketDescriptor) == 32, "PacketDescriptor must be exactly 32 bytes.");

class ArenaAllocator {
public:
    explicit ArenaAllocator(std::size_t capacity)
        : capacity_(capacity), offset_(0), generation_(1) {
        // Align capacity to page size
        std::size_t page_size = sysconf(_SC_PAGESIZE);
        if (capacity_ % page_size != 0) {
            capacity_ = ((capacity_ + page_size - 1) / page_size) * page_size;
        }

        // Attempt allocation with huge pages, fallback to standard anonymous mmap
        void* ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr == MAP_FAILED) {
            ptr = mmap(nullptr, capacity_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (ptr == MAP_FAILED) {
                throw std::bad_alloc();
            }
        }
        buffer_ = static_cast<char*>(ptr);
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
          offset_(other.offset_.load(std::memory_order_relaxed)),
          generation_(other.generation_.load(std::memory_order_relaxed)) {
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
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            generation_.store(other.generation_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.buffer_ = nullptr;
            other.capacity_ = 0;
        }
        return *this;
    }

    uint64_t allocate(const void* data, uint32_t length) {
        // 64-byte alignment
        std::size_t aligned_len = (length + 63) & ~std::size_t(63);
        std::size_t current_offset = offset_.load(std::memory_order_relaxed);

        while (true) {
            if (current_offset + aligned_len > capacity_) {
                // Reset arena or handle exhaustion; for monotonic bump allocator, wrap around with generation increment
                std::size_t old_offset = current_offset;
                if (offset_.compare_exchange_weak(old_offset, aligned_len, std::memory_order_release, std::memory_order_relaxed)) {
                    generation_.fetch_add(1, std::memory_order_release);
                    std::memcpy(buffer_, data, length);
                    return 0;
                }
                current_offset = offset_.load(std::memory_order_relaxed);
                continue;
            }

            if (offset_.compare_exchange_weak(current_offset, current_offset + aligned_len, 
                                              std::memory_order_release, std::memory_order_relaxed)) {
                std::memcpy(buffer_ + current_offset, data, length);
                return static_cast<uint64_t>(current_offset);
            }
        }
    }

    const char* get_address(uint64_t offset) const {
        return buffer_ + offset;
    }

    uint64_t get_generation() const {
        return generation_.load(std::memory_order_acquire);
    }

private:
    char* buffer_;
    std::size_t capacity_;
    alignas(hardware_destructive_interference_size) std::atomic<std::size_t> offset_;
    alignas(hardware_destructive_interference_size) std::atomic<uint64_t> generation_;
};

template <typename T, std::size_t Capacity>
class MPSCAsyncRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    MPSCAsyncRing() : head_(0), tail_(0), dropped_packets_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            sequence_[i].store(i, std::memory_order_relaxed);
        }
    }

    ~MPSCAsyncRing() = default;

    MPSCAsyncRing(const MPSCAsyncRing&) = delete;
    MPSCAsyncRing& operator=(const MPSCAsyncRing&) = delete;

    bool enqueue(const T& item, DropPolicy policy = DropPolicy::DropNewest) {
        std::size_t dif;
        std::size_t head = head_.load(std::memory_order_relaxed);

        while (true) {
            std::size_t seq = sequence_[head & Mask].load(std::memory_order_acquire);
            dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head);

            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Ring is full
                if (policy == DropPolicy::DropNewest) {
                    dropped_packets_.fetch_add(1, std::memory_order_relaxed);
                    return false;
                } else {
                    // Drop oldest: advance tail to make room
                    std::size_t tail = tail_.load(std::memory_order_relaxed);
                    if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_release, std::memory_order_relaxed)) {
                        dropped_packets_.fetch_add(1, std::memory_order_relaxed);
                        // Retry push after forcing space
                        head = head_.load(std::memory_order_relaxed);
                        continue;
                    }
                }
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        buffer_[head & Mask] = item;
        sequence_[head & Mask].store(head + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) {
        std::size_t dif;
        std::size_t tail = tail_.load(std::memory_order_relaxed);

        while (true) {
            std::size_t seq = sequence_[tail & Mask].load(std::memory_order_acquire);
            dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1);

            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Ring is empty
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        item = buffer_[tail & Mask];
        sequence_[tail & Mask].store(tail + Capacity, std::memory_order_release);
        return true;
    }

    uint64_t get_dropped_count() const {
        return dropped_packets_.load(std::memory_order_relaxed);
    }

    bool is_empty() const {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t seq = sequence_[tail & Mask].load(std::memory_order_acquire);
        return static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1) < 0;
    }

private:
    static constexpr std::size_t Mask = Capacity - 1;

    T buffer_[Capacity];
    std::atomic<std::size_t> sequence_[Capacity];
    
    alignas(hardware_destructive_interference_size) std::atomic<std::size_t> head_;
    alignas(hardware_destructive_interference_size) std::atomic<std::size_t> tail_;
    alignas(hardware_destructive_interference_size) std::atomic<uint64_t> dropped_packets_;
};

template <typename T, std::size_t Capacity>
class SPMCAsyncRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    SPMCAsyncRing() : head_(0), tail_(0), dropped_packets_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            sequence_[i].store(i, std::memory_order_relaxed);
        }
    }

    ~SPMCAsyncRing() = default;

    SPMCAsyncRing(const SPMCAsyncRing&) = delete;
    SPMCAsyncRing& operator=(const SPMCAsyncRing&) = delete;

    bool enqueue(const T& item, DropPolicy policy = DropPolicy::DropNewest) {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t seq = sequence_[head & Mask].load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head);

        if (dif < 0) {
            // Ring full
            dropped_packets_.fetch_add(1, std::memory_order_relaxed);
            if (policy == DropPolicy::DropOldest) {
                std::size_t tail = tail_.load(std::memory_order_relaxed);
                tail_.compare_exchange_strong(tail, tail + 1, std::memory_order_release, std::memory_order_relaxed);
            }
            return false;
        }

        buffer_[head & Mask] = item;
        sequence_[head & Mask].store(head + 1, std::memory_order_release);
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);

        while (true) {
            std::size_t seq = sequence_[tail & Mask].load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1);

            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Ring empty
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        item = buffer_[tail & Mask];
        sequence_[tail & Mask].store(tail + Capacity, std::memory_order_release);
        return true;
    }

    uint64_t get_dropped_count() const {
        return dropped_packets_.load(std::memory_order_relaxed);
    }

    bool is_empty() const {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        std::size_t seq = sequence_[tail & Mask].load(std::memory_order_acquire);
        return static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1) < 0;
    }

private:
    static constexpr std::size_t Mask = Capacity - 1;

    T buffer_[Capacity];
    std::atomic<std::size_t> sequence_[Capacity];

    alignas(hardware_destructive_interference_size) std::atomic<std::size_t> head_;
    alignas(hardware_destructive_interference_size) std::atomic<std::size_t> tail_;
    alignas(hardware_destructive_interference_size) std::atomic<uint64_t> dropped_packets_;
};

} // namespace titan

#endif // TITAN_RING_BUFFER_HPP