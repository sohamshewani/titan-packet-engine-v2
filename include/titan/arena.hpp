#ifndef TITAN_ARENA_HPP
#define TITAN_ARENA_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace titan {

struct ArenaConfig {
    std::size_t capacity_bytes{1024 * 1024 * 1024}; // Default 1GB
    int numa_node{-1};
    bool use_huge_pages{true};
};

class ArenaAllocator {
private:
    char* m_buffer{nullptr};
    std::size_t m_capacity{0};
    std::atomic<std::size_t> m_offset{0};
    std::atomic<uint64_t> m_generation{1};
    bool m_huge_pages_active{false};

    static constexpr std::size_t kCacheLineAlignment = 64;

public:
    explicit ArenaAllocator(const ArenaConfig& config)
        : m_capacity(config.capacity_bytes) {
        
        // Ensure capacity is a multiple of cache line size
        if (m_capacity % kCacheLineAlignment != 0) {
            m_capacity = ((m_capacity + kCacheLineAlignment - 1) / kCacheLineAlignment) * kCacheLineAlignment;
        }

        int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
        if (config.use_huge_pages) {
#ifdef MAP_HUGETLB
            // Request 2MB huge pages via MAP_HUGETLB
            mmap_flags |= MAP_HUGETLB;
            m_huge_pages_active = true;
#endif
        }

        void* ptr = mmap(nullptr, m_capacity, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
        if (ptr == MAP_FAILED) {
            if (config.use_huge_pages && m_huge_pages_active) {
                // Fallback to standard pages if hugepages allocation fails
                mmap_flags &= ~MAP_HUGETLB;
                m_huge_pages_active = false;
                ptr = mmap(nullptr, m_capacity, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
            }
            if (ptr == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "ArenaAllocator mmap failed");
            }
        }

        m_buffer = static_cast<char*>(ptr);

#ifdef MPOL_PREFERRED
        if (config.numa_node >= 0) {
            unsigned long nodemask = (1UL << config.numa_node);
            // Ignore failure if NUMA is not fully supported in the environment
            (void)set_mempolicy(MPOL_PREFERRED, &nodemask, sizeof(nodemask) * 8);
        }
#endif
    }

    ~ArenaAllocator() {
        if (m_buffer && m_buffer != MAP_FAILED) {
            munmap(m_buffer, m_capacity);
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    [[nodiscard]] void* allocate(std::size_t size) noexcept {
        // Align request size to 64 bytes
        std::size_t aligned_size = (size + kCacheLineAlignment - 1) & ~(kCacheLineAlignment - 1);
        
        std::size_t current_offset = m_offset.load(std::memory_order_relaxed);
        while (true) {
            if (current_offset + aligned_size > m_capacity) {
                return nullptr; // Arena exhausted
            }
            if (m_offset.compare_exchange_weak(current_offset, current_offset + aligned_size,
                                               std::memory_order_relaxed,
                                               std::memory_order_relaxed)) {
                return static_cast<void*>(m_buffer + current_offset);
            }
        }
    }

    void reset() noexcept {
        m_offset.store(0, std::memory_order_relaxed);
        m_generation.fetch_add(1, std::memory_order_release);
    }

    [[nodiscard]] uint64_t current_generation() const noexcept {
        return m_generation.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return m_capacity;
    }

    [[nodiscard]] std::size_t used_bytes() const noexcept {
        return m_offset.load(std::memory_order_relaxed);
    }

    [[nodiscard]] char* base_address() const noexcept {
        return m_buffer;
    }

    [[nodiscard]] bool is_huge_pages_active() const noexcept {
        return m_huge_pages_active;
    }
};

} // namespace titan

#endif // TITAN_ARENA_HPP