#ifndef __MEMPOOL_FACTORY_HPP__
#define __MEMPOOL_FACTORY_HPP__

#include <map>

#include "address_space_manager.hpp"

// A class that wraps a provided pool, or arena, of memory
// and handles allocation/dellocation requests for that pool of memory.
class MemoryPool {
public:
    typedef void* pointer_t;
    // Constructor.
    MemoryPool(pointer_t pool_addr, std::size_t pool_size) :
        pool_addr(pool_addr), pool_size(pool_size), unused_past(pool_addr) {}

    // Disable the copy constructor and copy assignment operators for now.
    // We'll leave the destructor intact. I don't actually want this pool
    // object to own the resource (the memory it's managing), I just want it to
    // be a tool for managing it (although it is strongly coupled).
    // We'll re-enable operators as necessary for rdma-receiving containers.
    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;

    pointer_t allocate(std::size_t bytes) noexcept {
        std::cerr << "allocate(bytes = " << bytes
                  << ") called on " << *this << std::endl;
        // For now, we'll use the next free address.
        pointer_t result = unused_past;
        // But before we return, update the next free address.
        unused_past = static_cast<pointer_t>(
            static_cast<char*>(unused_past) + bytes);

        return result;
    }

    friend std::ostream& operator<<(std::ostream&, const MemoryPool&);

    void deallocate(pointer_t addr, std::size_t bytes) noexcept {
        std::cerr << "deallocate(addr = " << addr
                  << ", bytes = " << bytes << ") called on "
                  << *this << " (no-op)" << std::endl;
    }

private:
    pointer_t pool_addr;
    std::size_t pool_size;
    // We may (and will) be given pool sizes far larger than needed.
    // Due to this, we should track how much space we actually use,
    // so if we ever need to rdma the memory, we know how much we need to move.
    // This is a pointer to the first unused address.
    pointer_t unused_past;
};


class MempoolFactory {
    typedef void* pointer_t;

private:
    AddressSpaceManager& coordinator;
    // A map from pool addresses to their MemoryPool objects.
    // Note that we (the MempoolFactory) owns the MemoryPool objects
    // that are given out, and is responsible for deallocating them.
    // So we track these just to be safe.
    std::map<pointer_t, MemoryPool*> pools_table;

public:
    // Constructor.
    MempoolFactory(AddressSpaceManager& coordinator) :
            coordinator(coordinator) {
        pools_table = std::map<pointer_t, MemoryPool*>();
    }

    static const size_t DEFAULT_POOL_SIZE = (size_t)1024 * 1024 * 1024 * 4;  // 4 GB
    MemoryPool* new_mempool(size_t size = DEFAULT_POOL_SIZE) {
        // Get an address we can use for this pool from the coordinator.
        pointer_t addr = coordinator.coordinated_mmap(size);
        // Make the pool.
        MemoryPool* pool = new MemoryPool(addr, size);
        // Record it in our map.
        pools_table[addr] = pool;

        return pool;
    }

    // MemoryPool* get_mempool_by_addr(pointer_t addr);
    // We should never have to re-retrieve the reference to a memory pool,
    // but in case we do need to, we can implement this method.

    void delete_mempool(pointer_t addr) {
        // Called by the user when they are no longer using the pool.
        // Postcondition: pool's memory will be GLOBALLY FREED
        // (i.e. freed to the coordinator).

        pools_table.erase(addr);
        // No-op for now.
    }
};


#endif // __MEMPOOL_FACTORY_HPP__