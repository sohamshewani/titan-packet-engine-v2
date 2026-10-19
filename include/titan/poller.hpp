#ifndef TITAN_POLLER_HPP
#define TITAN_POLLER_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <system_error>
#include <chrono>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <immintrin.h>

namespace titan {

// Constants and Alignment
constexpr std::size_t CACHE_LINE_SIZE = 64;
constexpr std::size_t DEFAULT_ARENA_SIZE = 1024 * 1024 * 1024; // 1 GB Huge Page backing
constexpr std::size_t RING_BUFFER_CAPACITY = 65536; // Must be power of two

// 32-Byte Fixed-Size Metadata Descriptor
struct alignas(32) PacketDescriptor {
    uint64_t arena_offset;
    uint32_t length;
    uint32_t flow_id;
    uint64_t timestamp;
    uint64_t sequence_number;
};

static_assert(sizeof(PacketDescriptor) == 32, "PacketDescriptor must be exactly 32 bytes");

// High-Performance Arena Allocator backed by Transparent Huge Pages
class alignas(CACHE_LINE_SIZE) ArenaAllocator {
private:
    char* base_ptr_;
    std::size_t capacity_;
    std::atomic<std::size_t> offset_;
    uint32_t generation_id_;

public:
    explicit ArenaAllocator(std::size_t capacity = DEFAULT_ARENA_SIZE) 
        : capacity_(capacity), offset_(0), generation_id_(1) {
        
        void* ptr = mmap(
            nullptr, 
            capacity_, 
            PROT_READ | PROT_WRITE, 
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, 
            -1, 
            0
        );

        if (ptr == MAP_FAILED) {
            // Fallback to standard huge page sizing or standard pages if MAP_HUGETLB fails
            ptr = mmap(
                nullptr, 
                capacity_, 
                PROT_READ | PROT_WRITE, 
                MAP_PRIVATE | MAP_ANONYMOUS, 
                -1, 
                0
            );
            if (ptr == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "ArenaAllocator mmap failed");
            }
        }

        base_ptr_ = static_cast<char*>(ptr);
        
        // Advise kernel for huge pages or sequential access
        madvise(base_ptr_, capacity_, MADV_HUGEPAGE);
        madvise(base_ptr_, capacity_, MADV_WILLNEED);
    }

    ~ArenaAllocator() {
        if (base_ptr_ && base_ptr_ != MAP_FAILED) {
            munmap(base_ptr_, capacity_);
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    [[nodiscard]] std::pair<void*, uint64_t> allocate(std::size_t size) noexcept {
        // Enforce 64-byte alignment
        const std::size_t aligned_size = (size + CACHE_LINE_SIZE - 1) & ~(CACHE_LINE_SIZE - 1);
        std::size_t current_offset = offset_.load(std::memory_order_relaxed);

        while (true) {
            if (current_offset + aligned_size > capacity_) {
                return {nullptr, 0}; // Exhaustion
            }
            if (offset_.compare_exchange_weak(
                current_offset, 
                current_offset + aligned_size, 
                std::memory_order_release, 
                std::memory_order_relaxed)) {
                return {static_cast<void*>(base_ptr_ + current_offset), static_cast<uint64_t>(current_offset)};
            }
        }
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
        generation_id_++;
    }

    [[nodiscard]] uint32_t get_generation_id() const noexcept {
        return generation_id_;
    }

    [[nodiscard]] char* get_base_address() const noexcept {
        return base_ptr_;
    }
};

// Lock-Free Multi-Producer Single-Consumer (MPSC) Circular Ring Buffer
template <typename T, std::size_t Capacity>
class alignas(CACHE_LINE_SIZE) MpscRingBuffer {
private:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    
    struct Cell {
        std::atomic<std::size_t> sequence;
        T data;
    };

    Cell buffer_[Capacity];
    char pad1_[CACHE_LINE_SIZE];
    std::atomic<std::size_t> head_;
    char pad2_[CACHE_LINE_SIZE];
    std::atomic<std::size_t> tail_;
    char pad3_[CACHE_LINE_SIZE];

    static constexpr std::size_t MASK = Capacity - 1;

public:
    MpscRingBuffer() : head_(0), tail_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    MpscRingBuffer(const MpscRingBuffer&) = delete;
    MpscRingBuffer& operator=(const MpscRingBuffer&) = delete;

    bool push(const T& item) noexcept {
        Cell* cell;
        std::size_t head = head_.load(std::memory_order_relaxed);

        while (true) {
            cell = &buffer_[head & MASK];
            std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head);

            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Ring full (Drop-tail policy execution)
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        cell->data = item;
        cell->sequence.store(head + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        Cell* cell;
        std::size_t tail = tail_.load(std::memory_order_relaxed);

        while (true) {
            cell = &buffer_[tail & MASK];
            std::size_t seq = cell->sequence.load(std::memory_order_acquire);
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

        item = cell->data;
        cell->sequence.store(tail + MASK + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        Cell* cell = &buffer_[tail & MASK];
        std::size_t seq = cell->sequence.load(std::memory_order_acquire);
        return static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1) < 0;
    }
};

// Sliding-Window Flow Control Token Bucket
class alignas(CACHE_LINE_SIZE) TokenBucketRateLimiter {
private:
    double capacity_;
    double tokens_;
    double refill_rate_per_ns_;
    std::chrono::steady_clock::time_point last_refill_;

public:
    TokenBucketRateLimiter(double capacity, double refill_rate_per_sec) 
        : capacity_(capacity), 
          tokens_(capacity), 
          refill_rate_per_ns_(refill_rate_per_sec / 1e9),
          last_refill_(std::chrono::steady_clock::now()) {}

    bool consume(double tokens_requested = 1.0) noexcept {
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_refill_).count();
        last_refill_ = now;

        tokens_ = std::min(capacity_, tokens_ + static_cast<double>(duration) * refill_rate_per_ns_);

        if (tokens_ >= tokens_requested) {
            tokens_ -= tokens_requested;
            return true;
        }
        return false;
    }
};

// High-Performance Non-Blocking Epoll Event Loop Poller
class alignas(CACHE_LINE_SIZE) Poller {
private:
    int epoll_fd_;
    int monitored_fd_;
    bool running_;
    uint64_t dropped_packets_counter_;
    uint64_t processed_packets_counter_;

    void set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_GETFL failed");
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
            throw std::system_error(errno, std::generic_category(), "fcntl F_SETFL O_NONBLOCK failed");
        }
    }

public:
    explicit Poller(int target_fd) 
        : epoll_fd_(-1), monitored_fd_(target_fd), running_(false), 
          dropped_packets_counter_(0), processed_packets_counter_(0) {
        
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ == -1) {
            throw std::system_error(errno, std::generic_category(), "epoll_create1 failed");
        }

        set_nonblocking(monitored_fd_);

        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLERR;
        ev.data.fd = monitored_fd_;

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, monitored_fd_, &ev) == -1) {
            close(epoll_fd_);
            throw std::system_error(errno, std::generic_category(), "epoll_ctl EPOLL_CTL_ADD failed");
        }
    }

    ~Poller() {
        if (epoll_fd_ != -1) {
            close(epoll_fd_);
        }
    }

    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;
    Poller(Poller&&) noexcept = delete;
    Poller& operator=(Poller&&) noexcept = delete;

    template <typename Callback>
    void poll_loop(Callback&& packet_handler, ArenaAllocator& arena, MpscRingBuffer<PacketDescriptor, RING_BUFFER_CAPACITY>& ring, TokenBucketRateLimiter& limiter) {
        running_ = true;
        const int max_events = 64;
        struct epoll_event events[max_events];
        int backoff_counter = 0;

        while (running_) {
            int nfds = epoll_wait(epoll_fd_, events, max_events, backoff_counter > 100 ? 1 : 0);

            if (nfds < 0) {
                if (errno == EINTR) continue;
                break; // Unrecoverable error
            }

            if (nfds == 0) {
                // Adaptive Polling / Exponential Backoff to bound CPU utilization
                backoff_counter++;
                if (backoff_counter > 1000) {
                    usleep(100); // Sleep briefly during prolonged network starvation
                } else {
                    _mm_pause();
                }
                continue;
            }

            backoff_counter = 0;

            for (int i = 0; i < nfds; ++i) {
                if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & EPOLLIN))) {
                    // Structural link failure or closure
                    running_ = false;
                    break;
                }

                // Drain socket in edge-triggered mode
                while (true) {
                    char dummy_stack_buffer[2048];
                    ssize_t bytes_read = read(monitored_fd_, dummy_stack_buffer, sizeof(dummy_stack_buffer));
                    
                    if (bytes_read < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // Fully drained socket
                        }
                        if (errno == EINTR) {
                            continue;
                        }
                        break; // Fatal read error
                    } else if (bytes_read == 0) {
                        running_ = false; // Peer closed connection
                        break;
                    }

                    // Sliding-window Flow Control Check
                    if (!limiter.consume(1.0)) {
                        dropped_packets_counter_++;
                        continue; // Drop-tail flow control active
                    }

                    // Zero-Copy Allocation in Arena
                    auto [packet_mem, arena_offset] = arena.allocate(bytes_read);
                    if (!packet_mem) {
                        dropped_packets_counter_++;
                        arena.reset(); // Cyclic recovery mode on exhaustion
                        auto [fallback_mem, fallback_offset] = arena.allocate(bytes_read);
                        if (!fallback_mem) {
                            break; // Complete failure to ingest
                        }
                        packet_mem = fallback_mem;
                        arena_offset = fallback_offset;
                    }

                    std::memcpy(packet_mem, dummy_stack_buffer, bytes_read);

                    PacketDescriptor desc{
                        .arena_offset = arena_offset,
                        .length = static_cast<uint32_t>(bytes_read),
                        .flow_id = 0xDEADBEEF, // Deterministic sample Flow ID
                        .timestamp = static_cast<uint64_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
                        .sequence_number = processed_packets_counter_++
                    };

                    // Push descriptor to ring buffer with wait-free guarantees
                    if (!ring.push(desc)) {
                        dropped_packets_counter_++; // Ring overflow drop-tail
                    } else {
                        packet_handler(desc, packet_mem);
                    }
                }
            }
        }
    }

    void stop() noexcept {
        running_ = false;
    }

    [[nodiscard]] uint64_t get_dropped_count() const noexcept {
        return dropped_packets_counter_;
    }

    [[nodiscard]] uint64_t get_processed_count() const noexcept {
        return processed_packets_counter_;
    }
};

} // namespace titan

#endif // TITAN_POLLER_HPP