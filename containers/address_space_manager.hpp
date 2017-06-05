#ifndef __ADDRESS_SPACE_MANAGER_HPP__
#define __ADDRESS_SPACE_MANAGER_HPP__

#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cstddef>
#include <iostream>

class AddressSpaceManager {
    // This class handles requests to allocate moveable blocks of memory.
    // Obviously currently it doesn't do much other than track mmaps,
    // but in the future this will need to interface with central coordination
    // (or is this just the client side interface FOR central coordination?)
    // Also, other open design questions are
    // - how do we track ownership of regions, and
    // - should we track contents of regions here, or delegate that to the
    // users? (At the very least, another level of indirection seems
    // appropriate; there is no reason the memory coordinator should have to
    // know about the contents.)
    // TODO: eventually make this a singleton?

    typedef void* pointer_t;

    // Linked list of used memory blocks for coordinating memory.
    // Note; this is a nondistributed implementation just for testing.
    // In any real setup, almost all of the methods will probably be calls
    // to a centralized coordination server.
    class MemoryNode {
    public:
        MemoryNode* next;
            // If this is null, then this is the FIXED ENDING BLOCK.
            // Do not allocate memory after this block.
        pointer_t addr;
        size_t size;

        MemoryNode(MemoryNode* next, pointer_t addr, size_t size) :
            next(next), addr(addr), size(size) {}

        // Within this linked list of memory nodes,
        // place a node for the specified amount of memory
        // at the specified address.
        // If addr is 0, place memory anywhere.
        // Returns the address at which memory was placed,
        // or 0 if we failed to place the address
        // (which happens if memory was full or address not available).
        pointer_t place_address(size_t req_size, pointer_t req_addr) {
            // First, errors.
            // Note we recursively call this on suffixes of the linked list.
            bool end_of_memory = next == NULL;
            bool negative_req_addr = req_addr < 0;
            bool req_addr_too_small = (req_addr > 0) and (req_addr < addr);
            if (end_of_memory or negative_req_addr or req_addr_too_small) {
                return (pointer_t) 0;
            }
            // We call this on the head (recursively), so the question is:
            // Is the memory to allocate between this node and the next node?
            // (This is vacuously true if we did not specify an address.)
            bool req_addr_goes_here = (
                req_addr > 0 and req_addr >= addr and req_addr < next->addr);
            bool no_addr_specified = req_addr == 0;

            if (req_addr_goes_here or no_addr_specified) {
                // Okay, let's try allocating here.
                // Is there enough space?
                // Calculate where the free memory after this block is.
                pointer_t free_mem_starts_at = (pointer_t)((char*)addr + size);
                // We know the request address goes here,
                // but now check if the request address is actually free.
                if (not no_addr_specified and req_addr < free_mem_starts_at) {
                    return (pointer_t) 0;
                }
                pointer_t allocation_addr = \
                    req_addr == 0 ? free_mem_starts_at : req_addr;
                pointer_t allocation_end = (pointer_t)(
                    (char*)allocation_addr + req_size);
                // Now make sure we have enough space.
                bool enough_space_here = allocation_end <= next->addr;

                if (enough_space_here) {
                    // If we've gotten here, then the address is free, and we have
                    // enough space. So let's allocate it.
                    // To do that, make a Node for it, and tuck it between this
                    // node and the next.
                    MemoryNode* new_node = new MemoryNode(next, allocation_addr, req_size);
                    next = new_node;
                    // And return our allocation address.
                    return allocation_addr;
                } else if (req_addr_goes_here) {
                    // If we don't have enough space here BUT our address
                    // has to go here, then we can't serve the request,
                    // so return 0.
                    return (pointer_t) 0;
                    // (If our address does not have to go here,
                    // it will continue searching recursively, as required.)
                }
            }

            // Otherwise, "after this node" is not the correct place to
            // allocate the memory. Move on to the next.
            return next->place_address(req_size, req_addr);
        }

        // NB: we don't have a free memory function,
        // but also this isn't our final address space management algo by far.
    };

public:
    AddressSpaceManager() {
        // Set up the linked list of memory allocations.
        pointer_t FIRST_ALLOCATABLE_ADDRESS = (pointer_t)((intptr_t)1 << 44);
        pointer_t DO_NOT_ALLOC_PAST = (pointer_t)((intptr_t)1 << 46);
        MemoryNode* tail = new MemoryNode(NULL, DO_NOT_ALLOC_PAST, 0);
        memory_list = new MemoryNode(tail, FIRST_ALLOCATABLE_ADDRESS, 0);
    }
    ~AddressSpaceManager() {
        // TODO
    }

    // Request to register a chunk of memory.
    // If addr is specified, attempts to use that exact address.
    // Returns the address registered. If the request fails, returns 0.
    pointer_t coordinated_mmap(size_t size, pointer_t addr = 0) {
        // Implementation notes:
        // There's a lot of uncertainty about how we want to implement atomic
        // centralized mmapping.
        // I think, given what we currently know, the best way is to constrain
        // the region of addresses where coordinated chunks of memory can be
        // put. In other words, we must always consciously pick, from some
        // fixed range, an address that looks free, and pass it into mmap.
        // The reason for this is that there is a problem with bare (addr=0)
        // calls to mmap (EVEN IF we mmap coordinated allocations across all
        // machines); the address returned will almost always be from the
        // mmap heap (near the bottom of the address space) and thus will
        // almost certainly not be free on other machines.
        // However, this is still not perfect; it is always possible that
        // a noncoordinated allocation somehow ended up intruding into our
        // coordinated allocation space. So we must still check to see if
        // mmapping this address on all other machines succeeds before
        // declaring a successful map.

        // Anyway, our current implementation will just be traversing a simple
        // linked memory list.
        // Acquire a free address from our address space allocator.
        pointer_t acquired_addr = memory_list->place_address(size, addr);
        std::cerr << "place_address(" << size << ", " << addr << ") ";
        std::cerr << "returned " << acquired_addr << std::endl;
        if (acquired_addr == 0) {
            return NULL;
        }
        // mmap this address.
        int prot = PROT_READ | PROT_WRITE;
        int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
        int fd = -1;
        off_t offset = 0;
        pointer_t res = (pointer_t) mmap(
            (void*)acquired_addr, size, prot, flags, fd, offset);
        std::cerr << "Did an mmap, returning address " << res;
        std::cerr << " and error " << strerror(errno) << std::endl;

        return res;
    }

    // pointer_t coordinated_munmap() {};
    // We'll have to think about this before we implement it.
    // Specifically, you can only unmap regions that are currently owned
    // by you. This will change until we finalize how we do ownwership,
    // so since we don't really need to free blocks right now let's
    // leave this unimplemented.

private:
    // The linked list tracking our memory allocation.
    MemoryNode* memory_list;

};

#endif // __ADDRESS_SPACE_MANAGER_H__