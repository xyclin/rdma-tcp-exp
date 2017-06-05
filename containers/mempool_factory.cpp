#include "mempool_factory.hpp"


std::ostream& operator<<(std::ostream& os, const MemoryPool& pool) {
    os << "MemoryPool(pool_addr = " << pool.pool_addr
       << ", pool_size = " << pool.pool_size
       << ", unused_past = " << pool.unused_past << ")";
    return os;
}
