#ifndef MEMORYSERVER_HPP
#define MEMORYSERVER_HPP

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <thread>
#include <rdma/rdma_cma.h>

namespace Draco{

// method declerations
#define TEST_NZ(x) do { if ( (x)) { printf("%s\n", strerror(errno)); die("error: " #x " failed (returned non-zero)." );} } while (0) // crashes at non-zero
#define TEST_Z(x)  do { if (!(x)) {printf("%s\n", strerror(errno)); die("error: " #x " failed (returned zero/null)."); }} while (0) // crashes at zero

#define DEBUG_LEVEL 2

#define DEBUG(x) do { if (DEBUG_LEVEL) {printf("%s\n", x);} } while(0)

enum send_state {
    SS_INIT,
    SS_MR_SENT,
    SS_DONE_SENT
};

struct message {
  enum {
    MSG_MR,
    MSG_DONE
  } type;

  union {
    struct ibv_mr mr;
  } data;
};

struct context {
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *comp_channel;

	std::thread* cq_poller_thread;
};

struct connection {
 	struct rdma_cm_id *id;
  	struct ibv_qp *qp;

  	int connected;

  	struct ibv_mr *send_mr;
  	struct ibv_mr *rdma_local_mr;
  	struct message *send_msg;

  	char *rdma_local_region;
  	send_state ss;

  	struct context *s_ctx = NULL;
};

class Server {
	// memory server for the log
	public:
	struct sockaddr_in6 addr;
	struct rdma_cm_event *event = NULL;
	struct rdma_cm_id *listener = NULL;
	struct rdma_event_channel *ec = NULL;
	uint16_t port = 5000;

	struct context *s_ctx = NULL;

	bool connected_once = false;

	Server(int port) {
		this->port = port;
	}
};

class MemoryServer{
private:
	// default variables
	int debug_level = 2; // higher the level the more debug output is given
	uint32_t back_log = 10;
	const int QUEUE_SIZE = 10;
	const int RDMA_BUFFER_SIZE = 1024*1024*200 + 12;
    // The address from which to serve remote memory requests.
    const void* test_addr;

	// server variables
	int port_ = 5001; // ignore, gets overwritten
	Server *admin_server = new Server(port_);
	Server *log_server = new Server(port_ + 1000);

	// poller threads
	std::thread* t_admin;
	std::thread* t_log;

	// global registered memory
	/*local variables for globally registred memory*/
	struct ibv_mr *send_mr;
	struct ibv_mr *rdma_local_mr;
	struct message *send_msg;
	char *rdma_local_region;

public:
	// common functions to be used
	void die(const char *reason);

	// common functions to build rdma connection
	MemoryServer();
	// MemoryServer::MemoryServer(uint16_t port) {
	// MemoryServer(const char* port);
    MemoryServer(const char* port, void* test_addr);

	void CreateConnection();
	void callJoin();

	//event loops and other required functions for event based calls
	void* event_loop_admin(void* param);
	void* event_loop_log(void* param);

	int on_event(struct rdma_cm_event *event, bool isAdmin);
	int on_connect_request(struct rdma_cm_id *id, bool isAdmin);

	int on_connection(struct rdma_cm_id *id);
	int on_disconnect(struct rdma_cm_id *id);


	// common functions
	void build_connection(struct rdma_cm_id *id, bool isAdmin);
	void on_connect(void *context);
	void send_mr_message(void *context);
	void destroy_connection(void *context);
	void build_params(struct rdma_conn_param *params);

	void build_context(Server* server, struct ibv_context *verbs);
	void build_qp_attr(Server* sever, struct ibv_qp_init_attr *qp_attr);
	void register_memory(Server* server, struct connection *conn);
	void* poll_cq(struct context * s_ctx);

	void on_completion(struct ibv_wc *wc);
	void send_message(struct connection *conn);


	// miscellaneous
	void set_debug_level(int level);
};


} // namespace decleration

#endif