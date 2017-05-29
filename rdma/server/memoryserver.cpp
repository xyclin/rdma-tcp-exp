#include "memoryserver.hpp"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <limits>
#include <vector>


using namespace Draco;


template <class T>
class MyAlloc {
public:
    // type definitions
    typedef T        value_type;
    typedef T*       pointer;
    typedef const T* const_pointer;
    typedef T&       reference;
    typedef const T& const_reference;
    typedef std::size_t    size_type;
    typedef std::ptrdiff_t difference_type;
    // rebind allocator to type U
    template <class U>
    struct rebind {
        typedef MyAlloc<U> other;
    };
    // return address of values
    pointer address (reference value) const {
        return &value;
    }
    const_pointer address (const_reference value) const {
        return &value;
    }
    /* constructors and destructor
     * - nothing to do because the allocator has no state
     */

    MyAlloc() throw() {
        std::cerr << "NOARGS MYALLOC CALLED" << std::endl;
    }
    // MyAlloc() throw() {
    MyAlloc(void* pool_addr, size_t pool_size, void* next) throw() {
        // (Note: This is just for testing memory access patterns.
        // When we actually make this, we should have the constructor
        // access some pool structure, which determines the specific
        // addresses.)
        std::cerr << "Creating custom allocator using ";
        std::cerr << "addr " << pool_addr << ", ";
        std::cerr << "size " << pool_size << ", ";
        std::cerr << "free memory beginning at " << next << std::endl;

        this->pool_addr = pool_addr;
        this->pool_size = pool_size;
        this->next = next;
    }

    MyAlloc(const MyAlloc& other) throw() {
        pool_addr = other.pool_addr;
        pool_size = other.pool_size;
        next = other.next;
    }

    template <class U> MyAlloc (const MyAlloc<U>&) throw() {}

    ~MyAlloc() throw() {}

    // return maximum number of elements that can be allocated
    size_type max_size () const throw() {
        return pool_size / sizeof(T);
    }

    // allocate but don't initialize num elements of type T
    pointer allocate (size_type num, const void* = 0) {
        // print message and allocate memory with global new
        std::cerr << "Allocating " << num << " element(s) "
                  << " of size " << sizeof(T) << std::endl;
        std::cerr << "pool_addr: " << pool_addr << std::endl;
        std::cerr << "pool_size: " << pool_size << std::endl;
        std::cerr << "next is currently " << next << std::endl;
        pointer ret = (pointer)(next);
        size_t total_size = (size_t)(num * sizeof(T));
        next = (void*)((char*)next + total_size);

        std::cerr << "Items allocated at: " << (void*)ret << std::endl;
        return ret;
    }
    // initialize elements of allocated storage p with value value
    void construct (pointer p, const T& value) {
        // initialize memory with placement new
        new((void*)p) T(value);
    }

    // destroy elements of initialized storage p
    void destroy (pointer p) {
        // destroy objects by calling their destructor
        p->~T();
    }
    // deallocate storage p of deleted elements
    void deallocate (pointer p, size_type num) {
        // print message and deallocate memory with global delete
        std::cerr << "Deallocating " << num << " element(s)"
                  << " of size " << sizeof(T)
                  << " at " << (void*)p << std::endl;
        std::cerr << "(This is currently a no-op.)" << std::endl;
    }

private:
    void* pool_addr;
    size_t pool_size;
    void* next;
};

// Disable equality comparisons for now.
template <class T1, class T2>
bool operator== (const MyAlloc<T1>&,
                 const MyAlloc<T2>&) throw() {
    return false;
}
template <class T1, class T2>
bool operator!= (const MyAlloc<T1>&,
                 const MyAlloc<T2>&) throw() {
    return true;
}


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

std::vector<int, MyAlloc<int> >* new_fixed_addr_vector(
    void* pool_addr, size_t pool_size
) {
    std::vector<int, MyAlloc<int> >* res = \
        (std::vector<int, MyAlloc<int> >*) pool_addr;
    void* next = (void*)((char*)pool_addr + sizeof(std::vector<int, MyAlloc<int> >));
    new((void*)pool_addr) std::vector<int, MyAlloc<int> >(
        MyAlloc<int>(pool_addr, pool_size, next));
    return res;
}


int main(int argc, char** argv) {
	if (argc < 2) {
		printf("usage: ./server port\n");
		return -1;
	}
    AddressSpaceManager vaddr_manager = AddressSpaceManager();

    void* addr;
    size_t size = (size_t)1024*1024*200 + 12;
    addr = (void*)(vaddr_manager.coordinated_mmap(size));
    std::vector<int, MyAlloc<int> >* vec_ptr = new_fixed_addr_vector(addr, size);

    for (int i = 0; i < 50; i++) {
    	vec_ptr->push_back(i);
    }

	MemoryServer srv((const char*) argv[1], addr);// = new MemoryServer();// = new MemoryServer();
	srv.CreateConnection();
	srv.callJoin();
	return 0;
}



namespace Draco {


MemoryServer::MemoryServer() {

}

MemoryServer::MemoryServer(const char * port_, void* test_addr) : test_addr(test_addr) {
	uint16_t port = atoi(port_);
	admin_server->port = port;
	log_server->port = port + 1000;
}

// MemoryServer::MemoryServer(const char* port) {

// }

void MemoryServer::CreateConnection() {
	// set up connection and listen for each region
	if (debug_level >= 1) {
		printf("starting memory server for log on port %d and for admin on port %d", log_server->port, admin_server->port);
	}

	if (debug_level >= 2) {
		printf("declaring the log server variables\n");
	}

	memset(&(log_server->addr), 0, sizeof((log_server->addr)));
	(log_server->addr).sin6_family = AF_INET;
	(log_server->addr).sin6_port = htons(log_server->port);

	if (debug_level >= 2)
		printf("declaring log region rdma variables\n");

	TEST_Z((log_server->ec) = rdma_create_event_channel());
	TEST_NZ(rdma_create_id((log_server->ec), &(log_server->listener), NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr((log_server->listener), (struct sockaddr *)&(log_server->addr)));
	TEST_NZ(rdma_listen((log_server->listener), back_log));

	log_server->port = ntohs(rdma_get_src_port(log_server->listener));

	if (debug_level >= 2) {
		printf("listening for log region on port %d.\n", (log_server->port));
		printf("log server is initialized\n");
	}

	if (debug_level >= 2)
		printf("declaring admin region server variables\n");

	memset(&(admin_server->addr), 0, sizeof((admin_server->addr)));
	(admin_server->addr).sin6_family = AF_INET;
	(admin_server->addr).sin6_port = htons(admin_server->port);

	if (debug_level >= 2)
		printf("declaring admin region rdma variables\n");

	TEST_Z((admin_server->ec) = rdma_create_event_channel());
	TEST_NZ(rdma_create_id((admin_server->ec), &(admin_server->listener), NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr((admin_server->listener), (struct sockaddr *)&(admin_server->addr)));
	TEST_NZ(rdma_listen((admin_server->listener), back_log));

	admin_server->port = ntohs(rdma_get_src_port(admin_server->listener));

	if (debug_level >= 1) {
			printf("listening on port %d.\n", (admin_server->port));
			printf("admin server is initialized\n");
	}

	// set up poller threads
	//	pthread_create();
	if (debug_level >= 1)
		printf("setting up poller threads\n");

	if (debug_level >= 2)
		printf("setting up poller thread for log region\n");

	t_log = new std::thread(&MemoryServer::event_loop_log, this, (void*)NULL);

	if (debug_level >= 2)
		printf("setting up poller thread for admin region\n");

	t_admin = new std::thread(&MemoryServer::event_loop_admin, this, (void*)NULL);
}

/*
	must call join after creation of connection
*/
void MemoryServer::callJoin() {
	t_log->join();
	t_admin->join();
}




void* MemoryServer::event_loop_log (void* param) {
	while (rdma_get_cm_event(log_server->ec, &(log_server->event)) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, log_server->event, sizeof(*(log_server->event)));
		rdma_ack_cm_event(log_server->event);

		if (on_event(&event_copy, false))
			break;

		}
	return NULL;
}

void* MemoryServer::event_loop_admin (void* param ) {
	while (rdma_get_cm_event(admin_server->ec, &(admin_server->event)) == 0) {
	    struct rdma_cm_event event_copy;

	    memcpy(&event_copy, admin_server->event, sizeof(*(admin_server->event)));
	    rdma_ack_cm_event(admin_server->event);

	    if (on_event(&event_copy, true))
	    	break;

	  }
    return NULL;
}


int MemoryServer::on_event(struct rdma_cm_event *event, bool isAdmin) {
	int r = 0;

	if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
		r = on_connect_request(event->id, isAdmin);
	else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
		r = on_connection(event->id);
	else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
		r = on_disconnect(event->id);
	else if (event->event == RDMA_CM_EVENT_UNREACHABLE)
		printf("unreachable server, possible network partition\n");
	else if (event->event == RDMA_CM_EVENT_REJECTED) {
		printf("rejected, port closed\n");
		// printf("%d\n", event->status);
	}
	else if (event-> event == RDMA_CM_EVENT_ADDR_ERROR)
		printf("RDMA_CM_EVENT_ADDR_ERROR\n");
	else if (event-> event == RDMA_CM_EVENT_CONNECT_RESPONSE)
		printf("RDMA_CM_EVENT_CONNECT_RESPONSE\n");
	else if (event-> event == RDMA_CM_EVENT_CONNECT_ERROR)
		printf("RDMA_CM_EVENT_CONNECT_ERROR\n");
	else if (event-> event == RDMA_CM_EVENT_DEVICE_REMOVAL)
		printf("RDMA_CM_EVENT_DEVICE_REMOVAL\n");
	else if (event-> event == RDMA_CM_EVENT_MULTICAST_JOIN)
		printf("RDMA_CM_EVENT_MULTICAST_JOIN\n");
	else if (event-> event == RDMA_CM_EVENT_MULTICAST_ERROR)
		printf("RDMA_CM_EVENT_MULTICAST_ERROR\n");
	else if (event-> event == RDMA_CM_EVENT_ROUTE_ERROR)
		printf("RDMA_CM_EVENT_ROUTE_ERROR\n");
	else if (event-> event == RDMA_CM_EVENT_ADDR_CHANGE)
		printf("RDMA_CM_EVENT_ADDR_CHANGE\n");
	else if (event-> event == RDMA_CM_EVENT_TIMEWAIT_EXIT)
		printf("RDMA_CM_EVENT_TIMEWAIT_EXIT\n");

	else
		printf("weird thing happened\n");
		// die("on_event: unknown event.");

	return r;
}


int MemoryServer::on_connect_request(struct rdma_cm_id *id, bool isAdmin) {
	/*logic to add more connections on requests, essentially a way
	to ensure that the server isnt getting connected in a rougue manner*/

	if (debug_level >= 1)
		printf("on connect request called, received connection request\n");

	struct rdma_conn_param cm_params;

	build_connection(id, isAdmin);
	build_params(&cm_params);

	TEST_NZ(rdma_accept(id, &cm_params));

	return 0;
}

int MemoryServer::on_connection(struct rdma_cm_id *id) {
	if (debug_level >= 1)
		printf("connection established\n");

	on_connect(id->context);
	return 0;
}

int MemoryServer::on_disconnect(struct rdma_cm_id *id) {
	if (debug_level >= 1) {
		printf("peer disconnected.\n");
	}

	rdma_disconnect(id);
	// destroy_connection(id->context);

	return 0;
}


void MemoryServer::build_connection(struct rdma_cm_id *id, bool isAdmin) {
		// read from files and register and pin memory
		struct connection *conn;
		struct ibv_qp_init_attr qp_attr;
		Server *server = NULL;

		if (debug_level >= 1)
			printf("building connection\n");

		if (!isAdmin){
			server = log_server;
			if (debug_level >= 2)
				printf("got connection to log region\n");
		} else {
			server = admin_server;
			if (debug_level >= 2)
				printf("got the connection to admin region\n");
		}

		if (debug_level >= 1)
			printf("building context\n");

		build_context(server, id->verbs);

		if (debug_level >= 1)
			printf("building queue pair attributes\n");

		build_qp_attr(server,&qp_attr);


		if (debug_level >= 1)
			printf("building QP\n");

		TEST_NZ(rdma_create_qp(id, server->s_ctx->pd, &qp_attr));


		id->context = conn = (struct connection *)malloc(sizeof(struct connection));

		conn->id = id;
		conn->qp = id->qp;
		conn->ss = SS_INIT;
		conn->connected = 0;
		conn->s_ctx = server->s_ctx;

		if (debug_level >= 1)
			printf("registering memory\n");

		register_memory(server, conn);
}


void MemoryServer::build_context(Server* server, struct ibv_context *verbs) {
	/*do only once to build server vals.,
	it has completion queue decleration, of only 10 size, this is arbitrary,
	refactor later to include config variable*/

	if (server->s_ctx) {
		if (server->s_ctx->ctx != verbs)
			die("cannot handle events in more than one context.");

		return;
	}

	server->s_ctx = (struct context *)malloc(sizeof(struct context));

	server->s_ctx->ctx = verbs;

	TEST_Z(server->s_ctx->pd = ibv_alloc_pd(server->s_ctx->ctx));
	TEST_Z(server->s_ctx->comp_channel = ibv_create_comp_channel(server->s_ctx->ctx));
	TEST_Z(server->s_ctx->cq = ibv_create_cq(server->s_ctx->ctx, QUEUE_SIZE, NULL, server->s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary, to be updated using the queue size (refactor this code) */
	TEST_NZ(ibv_req_notify_cq(server->s_ctx->cq, 0));

	server->s_ctx->cq_poller_thread = new std::thread(&MemoryServer::poll_cq, this, (server->s_ctx) );
}


void* MemoryServer::poll_cq(struct context* s_ctx) {
	struct ibv_cq *cq;
	struct ibv_wc wc;

	while (1) {
		void* x = static_cast<void*> (s_ctx->ctx);
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &x));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (ibv_poll_cq(cq, 1, &wc)) {
			on_completion(&wc);
		}
		// break;
	}

	return NULL;
}

void MemoryServer::build_qp_attr(Server* server, struct ibv_qp_init_attr *qp_attr) {
	// for passive server we might not actually need many things here
	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = server->s_ctx->cq;
	qp_attr->recv_cq = server->s_ctx->cq;

	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = QUEUE_SIZE;
	qp_attr->cap.max_recv_wr = QUEUE_SIZE;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

void MemoryServer::register_memory(Server* server, struct connection *conn) {
// be careful with the read write accesses

	if (debug_level >= 1)
		printf("registering memory\n");

	if (!server->connected_once) {
		if (debug_level >= 2) {
			printf("pinning memory\n");
		}

		server->connected_once = true;

		conn->send_msg = static_cast<struct message*>(malloc(sizeof(struct message)));

		conn->rdma_local_region = (char*)test_addr;

		TEST_Z(conn->send_mr = ibv_reg_mr(
			server->s_ctx->pd,
			conn->send_msg,
			sizeof(struct message),
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));

		TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
			server->s_ctx->pd,
			conn->rdma_local_region,
			RDMA_BUFFER_SIZE,
			IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));


		send_msg = conn->send_msg;
		rdma_local_region = conn->rdma_local_region;
		send_mr = conn->send_mr;
		rdma_local_mr = conn->rdma_local_mr;
	} else {
		if (debug_level >= 2) {
			printf("linking memory\n");
		}

		conn->send_mr = send_mr;
		conn->rdma_local_mr = rdma_local_mr;
		conn->send_msg = send_msg;
		conn->rdma_local_region = rdma_local_region;
	}

}


void MemoryServer::build_params(struct rdma_conn_param *params) { // this basically controls number of simultaneous read requests
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

void MemoryServer::on_connect(void *context) {
	if(debug_level >= 1)
    	printf("on_connect");

	((struct connection *)context)->connected = 1;

	if (debug_level >= 1)
		printf("got connection\n");

	send_mr_message(((struct connection *)context));
}


void MemoryServer::on_completion(struct ibv_wc *wc){
	struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

	if (wc->status != IBV_WC_SUCCESS)
		die("on_completion: status is not IBV_WC_SUCCESS.");

	if (wc->opcode & IBV_WC_SEND) {
		if (conn->ss == SS_INIT) {
			conn->ss = SS_MR_SENT;
		}else if (conn->ss == SS_MR_SENT) {
			conn->ss = SS_DONE_SENT;
		}

		if (debug_level >= 1)
			printf("send completed successfully.\n");
	}

	/*
		to do, add disconnect and revoke action, server, should come up and connect
		and carry on working

		the above logic was pushed to server code in DRACO
	*/

}

void MemoryServer::send_mr_message(void *context) {

	if (debug_level >= 1)
		printf("sending mr message to client\n");

	struct connection *conn = (struct connection *)context;

	conn->send_msg->type = message::MSG_MR;
	memcpy(&conn->send_msg->data.mr, conn->rdma_local_mr, sizeof(struct ibv_mr));

	send_message(conn);
}

void MemoryServer::send_message(struct connection *conn) {
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)conn;
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t)conn->send_msg;
	sge.length = sizeof(struct message);
	sge.lkey = conn->send_mr->lkey;

	while (!conn->connected);

	if (debug_level >= 1)
		printf("posting send\n");

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}


void MemoryServer::set_debug_level(int level) {
	debug_level = level;
}

void MemoryServer::die(const char *reason) {
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

} // namespace decleration