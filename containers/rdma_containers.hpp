#ifndef __RDMA_CONTAINERS_HPP__
#define __RDMA_CONTAINERS_HPP__

#include <scoped_allocator>

#include <vector>

#include "fixed_addr_allocator.hpp"
#include "mempool_factory.hpp"

template < class T >
class rdma_vector {
    // Typedefs for our custom allocator templated classes.
    typedef std::scoped_allocator_adaptor<FixedAddrAllocator<T>> alloc_t;
    typedef std::vector<T, alloc_t> vec_t;
private:
    vec_t* vec;
    alloc_t alloc;

    // Initialize the allocator from a provided mempool factory.
    // For use in the constructor.
    void allocator_from_mempool_factory(MempoolFactory& mempool_factory) {
        // Grab a mempool.
        MemoryPool* mempool = mempool_factory.new_mempool();
        // Make an allocator out of it.
        // (Note: This is implicitly made into a scoped_allocator_adaptor.)
        alloc = FixedAddrAllocator<T>(mempool);
    }

public:
    // Constructors:
    // I'd like to accept variadic arguments so I can transparently support all
    // of the vector constructors, but this seems highly nontrivial,

    // Default constructor.
    // template <class ...Args> TODO test this after u get basic one working
    explicit rdma_vector(MempoolFactory& mempool_factory) {
        // Initialize the allocator.
        this->allocator_from_mempool_factory(mempool_factory);
        // Allocate space for the vector from the allocator.
        using rebound_alloc_t = typename alloc_t::template rebind<vec_t>::other;
        auto rebound = rebound_alloc_t(alloc);
        vec = static_cast<vec_t*>(rebound.allocate(1));
        rebound.construct(vec);
    }

    // Special constructors: copy constructor, etc.
    // Plus swap and other non-member functions?
    // TODO.

    // Methods (mostly just forwarding):
    typename vec_t::iterator begin() noexcept { return vec->begin(); }
    typename vec_t::const_iterator begin() const noexcept { return vec->begin(); }
    typename vec_t::iterator end() noexcept { return vec->end(); }
    typename vec_t::const_iterator end() const noexcept { return vec->end(); }
    typename vec_t::size_type size() const noexcept { return vec->size(); }
    bool empty() const noexcept { return vec->empty(); }
    typename vec_t::reference operator[](typename vec_t::size_type n) { return vec->operator[](n); }
    typename vec_t::reference at(typename vec_t::size_type n) { return vec->at(n); }
    typename vec_t::const_reference at(typename vec_t::size_type n) const { return vec->at(n); }
    typename vec_t::reference front(typename vec_t::size_type n) { return vec->front(n); }
    typename vec_t::const_reference front(typename vec_t::size_type n) const { return vec->front(n); }
    typename vec_t::reference back(typename vec_t::size_type n) { return vec->back(n); }
    typename vec_t::const_reference back(typename vec_t::size_type n) const { return vec->back(n); }
    void push_back(const typename vec_t::value_type& val) { return vec->push_back(val); }
    void pop_back() { return vec->pop_back(); }
};

#endif // __RDMA_CONTAINERS_HPP__