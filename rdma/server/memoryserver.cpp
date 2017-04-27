#include "memoryserver.hpp"

using namespace Draco;

int main(int argc, char** argv) {
	if (argc < 2) {
		printf("usage: ./server port\n");
		return -1;
	}
	MemoryServer srv((const char*) argv[1] );// = new MemoryServer();// = new MemoryServer();
	srv.CreateConnection();
	srv.callJoin();
	return 0;
}


namespace Draco {


MemoryServer::MemoryServer() {

}

MemoryServer::MemoryServer(const char * port_) {
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
		printf("setting up poller threads");

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

		conn->rdma_local_region = static_cast<char*>(calloc(1,RDMA_BUFFER_SIZE));

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