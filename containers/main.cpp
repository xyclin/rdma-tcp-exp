#include <stdint.h>

#include <cstddef>
#include <iostream>
#include <string>

#include "address_space_manager.hpp"
#include "mempool_factory.hpp"
#include "rdma_containers.hpp"




// This only works with containers that take exactly two template arguments
// e.g. not map, set, etc.
// There is no way to generalize the number of template arguments in C++98,
// so the only solution for the other types is to make their own functions,
// overloading-style (and at that point it's probably best to just get rid of
// the template entirely). Since this is unwieldly, we'll leave it like this
// and move on to the C++11 implementation, where we bring in scoped
// allocators.
// template <
//     typename T,
//     template<typename type_arg, typename alloc_arg> class container_tm >
// container_tm< T, std::scoped_allocator_adaptor<FixedAddrAllocator<T>> >* new_fixed_addr_container(
//     void* pool_addr, size_t pool_size
// ) {
//     // These type names are really messy.
//     // We'll use cont_t for the container type which we wish to allocate,
//     // which is template-constructed from our container_tm argument.
//     typedef container_tm< T, std::scoped_allocator_adaptor<FixedAddrAllocator<T>> > cont_t;

//     // We'll place the object itself on the given address,
//     // so we'll just return the given address.
//     cont_t* res = static_cast<cont_t*>(pool_addr);
//     // Since we've used up some of our pool space with our object,
//     // we must tell the allocator it can't use this space.
//     void* next = (void*)((char*)pool_addr + sizeof(cont_t));
//     // Now construct the object, with the allocator, and place it
//     // at the address given.
//     new((void*)pool_addr) cont_t(
//         std::scoped_allocator_adaptor<FixedAddrAllocator<T>>(
//             FixedAddrAllocator<T>(pool_addr, pool_size, next)));

//     return res;
// }

// void* addr = (void*)((intptr_t)1<<44 - (intptr_t)1<<5);
// size_t length = (size_t)1<<5;
// int prot = PROT_READ | PROT_WRITE;
// int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
// int fd = -1;
// off_t offset = 0;
// void* map = mmap(addr, length, prot, flags, fd, offset);
// std::cerr << "mmap returned address " << map;
// std::cerr << " and error " << strerror(errno) << std::endl;

int main(int argc, char* argv[]) {

    AddressSpaceManager vaddr_manager = AddressSpaceManager();
    MempoolFactory mempool_factory = MempoolFactory(vaddr_manager);

    const std::string stars = "********************************";

    std::cerr << stars << std::endl;
    std::cerr << "Testing custom scoped-allocated std::vector:" << std::endl;
    std::cerr << stars << std::endl;

    using String = std::basic_string<char, std::char_traits<char>, FixedAddrAllocator<char>>;

    rdma_vector<String> vec(mempool_factory);
    vec.push_back("a");
    vec.push_back("hello world! wow");

    std::cerr << "Contents: ";
    for (const auto& i : vec) {
        std::cerr << i << ", ";
    }
    std::cerr << std::endl;

    // void* addr;
    // size_t size = (size_t)1024*1024;
    // addr = (void*)(vaddr_manager.coordinated_mmap(size));
    // typedef std::vector<int, std::scoped_allocator_adaptor<FixedAddrAllocator<int>>> int_vector_t;
    // int_vector_t* vec_ptr;
    // vec_ptr = new_fixed_addr_container<int, std::vector>(addr, size);

    // // const FixedAddrAllocator<int> myalloc = FixedAddrAllocator<int>();
    // // std::vector<int, FixedAddrAllocator<int> > vec = std::vector<int, FixedAddrAlloator<int> >(myalloc);

    // for (int i = 0; i < 30; i++) {
    //     vec_ptr->push_back(i);
    // }

    // std::cerr << "Printing contents of vec_ptr: ";
    // for (
    //     int_vector_t::iterator i = vec_ptr->begin();
    //     i != vec_ptr->end();
    // ) {
    //     std::cerr << *i;
    //     if (++i != vec_ptr->end()) {
    //         std::cerr << ", ";
    //     } else {
    //         std::cerr << std::endl;
    //     }
    // }

    // typedef std::vector<
    //     int_vector_t,
    //     std::scoped_allocator_adaptor<FixedAddrAllocator<int_vector_t>>
    // > int_vec2_t;
    // addr = (void*)(vaddr_manager.coordinated_mmap(size));
    // int_vec2_t* vec2_ptr;
    // vec2_ptr = new_fixed_addr_container<int_vector_t, std::vector>(addr, size);
    // vec2_ptr->push_back(int_vector_t(FixedAddrAllocator<int>(0, 0, 0)));
    // vec2_ptr->at(0).push_back(0);
    // vec2_ptr->push_back(int_vector_t(FixedAddrAllocator<int>(0, 0, 0)));
    // vec2_ptr->at(1).push_back(10);
    // vec2_ptr->at(0).push_back(1);
    // for (const auto& vec : *vec2_ptr) {
    //     for (const auto& i : vec) {
    //         std::cerr << i << ", ";
    //     }
    //     std::cerr << std::endl;
    // }

    // std::cerr << stars << std::endl;
    // std::cerr << "Testing custom heap allocated std::list:" << std::endl;
    // std::cerr << stars << std::endl;

    // addr = (void*)(vaddr_manager.coordinated_mmap(size));
    // typedef std::list<int, FixedAddrAllocator<int> > custom_list_t;
    // custom_list_t* list_ptr;
    // list_ptr = new_fixed_addr_container<int, std::list>(addr, size);
    // // Testing: Fill it up with garbage from both sides.
    // for (int i = 0; i < 10; i++) {
    //     list_ptr->push_front(100);
    //     list_ptr->push_back(100);
    // }
    // // Dump all but two items of garbage.
    // for (int i = 0; i < 9; i++) {
    //     list_ptr->pop_back();
    //     list_ptr->pop_back();
    // }
    // // Push sequential numbers onto both sides.
    // for (int i = 1; i < 10; i++) {
    //     list_ptr->push_front(-i);
    //     list_ptr->push_back(i);
    // }

    // std::cerr << "Printing contents of list_ptr: ";
    // for (
    //     custom_list_t::iterator i = list_ptr->begin();
    //     i != list_ptr->end();
    // ) {
    //     std::cerr << *i;
    //     if (++i != list_ptr->end()) {
    //         std::cerr << ", ";
    //     } else {
    //         std::cerr << std::endl;
    //     }
    // }




    // int pagesize = getpagesize();
    // void* map = mmap(addr, length, prot, flags, fd, offset);

    // std::cout << addr << std::endl;
    // std::cout << length << std::endl;
    // std::cout << pagesize << std::endl;
    // std::cout << map << std::endl;
    // std::cout << strerror(errno) << std::endl;

    // addr = (void*)0;
    // for (i = 0; i < 10; i++) {
    //     void* map2 = mmap(addr, length, prot, flags, fd, offset);
    //     std::cout << map2 << std::endl;
    //     std::cout << strerror(errno) << std::endl;
    // }


    while(true) {
        sleep(5);
    }
    return 0;
}

