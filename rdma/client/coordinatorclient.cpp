#include "coordinatorclient.hpp"

#include <sys/time.h>

#include <map>

using namespace Draco;
using namespace std;

namespace Draco {

Coordinator::Coordinator(char *address, char *port, int node_id) : node_id(node_id) {
	c = new client();
	c->port = port;
	c->address = address;
	
	if (debug_level >= 1) {
		printf("address is : %s\n", c->address);
		printf("port is : %s\n", c->port);	
	}
	
	if (debug_level >= 2)
		printf("setting up client variables\n");


	TEST_NZ(getaddrinfo((c->address), c->port, NULL, &(c->addr)));
	TEST_Z((c->ec) = rdma_create_event_channel());
	TEST_NZ(rdma_create_id((c->ec), &(c->conn), NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_resolve_addr((c->conn), NULL, (c->addr)->ai_addr, TIMEOUT_IN_MS));

	if (debug_level >= 2)
		printf("declaring variables and connected int\n");

	pthread_mutex_init(&mutex_conn_new, NULL);
	pthread_cond_init (&cv_conn_new, NULL);

	c->mutex_conn = &mutex_conn_new;

	c->cv_conn = &cv_conn_new;
	
	//initialize you cond vars and mutex
	// int temp = 0;
	
	c->connected = new int();
	*(c->connected) = 0;
	freeaddrinfo(c->addr);

}

Coordinator::~Coordinator() {
	if (debug_level >=1)
//		printf("destroying the coordinator\n");
	pthread_mutex_destroy(c->mutex_conn );
	pthread_cond_destroy( c->cv_conn );
	rdma_destroy_event_channel(c->ec);
	delete c;
}

int Coordinator::RdmaRead(int offset, int size, char* buffer) {
	// if (debug_level >= 1)
		// printf("RDMA READ %d\n", op_done);

    struct connection * conn = ((struct connection *)c->conn->context);
    return rdma_read(conn, offset, size, buffer);
}
    
int Coordinator::RdmaCompSwap(int offset, uint64_t compare, uint64_t swap, char* buffer){
    if (debug_level >= 1)
		printf("RDMA CompSwap\n");

    struct connection * conn = ((struct connection *)c->conn->context);
    return rdma_compswap(conn, offset, compare, swap, buffer);
}

int Coordinator::RdmaWrite(int offset, int size, char* buffer){
    // if (debug_level >= 1)
		// printf("RDMA Write\n");
    struct connection * conn = ((struct connection *)c->conn->context);
    return rdma_write(conn, offset, size, buffer);
}


/* new API use struct enqueue wait for reply pop/memcpy function increment*/

// int Coordinator::RdmaRead(WorkRequest * workReq) {
// 	workReq->setWorkRequestCommand(READ_);

//     std::unique_lock<std::mutex> mlock(mutex_);
//     queue_->push(workReq);
    
//     RdmaRead(workReq->getOffset(), workReq->getSize(), workReq->getValue());

//     mlock.unlock();
//     cond_.notify_one();

//     return 1;
// }
    
// int Coordinator::RdmaCompSwap(WorkRequest * workReq){
// 	workReq->setWorkRequestCommand(ACS);

//     std::unique_lock<std::mutex> mlock(mutex_);
//     queue_->push(workReq);
    
//     RdmaCompSwap(workReq->getOffset(), workReq->getCompare(), workReq->getSwap(), workReq->getValue());

//     mlock.unlock();
//     cond_.notify_one();

//     return 1;
// }

// int Coordinator::RdmaWrite(WorkRequest * workReq){
//     workReq->setWorkRequestCommand(WRITE);

//     std::unique_lock<std::mutex> mlock(mutex_);
//     queue_->push(workReq);
    
//     RdmaWrite(workReq->getOffset(), workReq->getSize(), workReq->getValue());

//     mlock.unlock();
//     cond_.notify_one();
// }


int Coordinator::CreateConnection() {
    event_thread = new std::thread(&Coordinator::event_loop_client, this, (void*)NULL );
    
    pthread_mutex_lock( c->mutex_conn );
    
    while (  *(c->connected) == 0) {
    	if (this->connected_ == 1) {
    		return 1;
    	}
        pthread_cond_wait((c->cv_conn), ( c->mutex_conn ));
    }
    pthread_mutex_unlock( ( c->mutex_conn ));
    
//    printf("Connection established and ready to do rdma operations\n");
    
    return (this->connected_);
}

void Coordinator::callJoin() {
    event_thread->join();
    s_ctx->cq_poller_thread->join();
}


void* Coordinator::event_loop_client(void *param) {
//	DEBUG("Started event loop\n");
//	printf("event loop created\n");
	while (rdma_get_cm_event((c->ec), &(c->event)) == 0) {
		struct rdma_cm_event event_copy;
		memcpy(&event_copy, c->event, sizeof(*(c->event)));
		rdma_ack_cm_event(c->event);

		if (Coordinator::on_event(&event_copy))
			break;
	}
	return NULL;
}

int Coordinator::on_event(struct rdma_cm_event *event) {
	int r = 0;
//	printf("on event\n");

	if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
		r = on_addr_resolved(event->id);
	else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
		r = on_route_resolved(event->id);
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
	else if (event-> event == RDMA_CM_EVENT_CONNECT_REQUEST)
		printf("RDMA_CM_EVENT_CONNECT_REQUEST\n");
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
	else {
		fprintf(stderr, "ERROR on_event: %d\n", event->event);
		//        die("on_event: unknown event, fault was not handled.");
		return -1;
	}

	return r;
}

int Coordinator::on_addr_resolved(struct rdma_cm_id *id) {
//	DEBUG("address resolved.\n");
//	printf("address resolved.\n");

	build_connection(id, c);
	TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

	return 0;
}

int Coordinator::on_connection(struct rdma_cm_id *id) {
//	DEBUG("on connection\n");
	// on_connect(id->context); // called after MR is received, ignore this since we are not sending any MR to server to do ops
	return 0;
}


int Coordinator::on_disconnect(struct rdma_cm_id *id) {
	// printf("disconnected.\n");
	destroy_connection(id->context);
	return 1;
}


int Coordinator::on_route_resolved(struct rdma_cm_id *id) {
	
	struct rdma_conn_param cm_params;
	// printf("route resolving.\n");
	build_params(&cm_params);
	TEST_NZ(rdma_connect(id, &cm_params));
//	DEBUG("route resolved\n");
	
	return 0;
}

void Coordinator::die(const char *reason) {
	fprintf(stderr, "%s\n", reason);
	exit(EXIT_FAILURE);
}


void Coordinator::build_connection(struct rdma_cm_id *id, client *c) {
	struct connection *conn;

	struct ibv_qp_init_attr qp_attr;

	build_context(id->verbs);
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
	conn = (struct connection *)malloc(sizeof(struct connection));
	
	id->context = conn;
	conn->id = id;
	conn->qp = id->qp;
	conn->rs = RS_INIT;
	conn->connected = (c->connected);
	conn->mutex_conn =  (c->mutex_conn);
	conn->cv_conn = (c->cv_conn);

	register_memory(conn);
	post_receives(conn);

//	DEBUG("connection built\n");
}

void Coordinator::build_context(struct ibv_context *verbs) {
	if (s_ctx) {
		if (s_ctx->ctx != verbs)
			die("cannot handle events in more than one context.");

//		DEBUG("context was already there\n");
		return;
	}

//	DEBUG("building context\n");

	s_ctx = (struct context *)malloc(sizeof(struct context));

	s_ctx->ctx = verbs;

//	DEBUG("building pd, comp channel and setting up poller thread");

	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
	TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, CQ_QUEUE_SIZE, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
	s_ctx->cq_poller_thread = new std::thread(&Coordinator::poll_cq, this, (void*)NULL);
	// s_ctx->cq_poller_thread.detach();
}

void Coordinator::build_qp_attr(struct ibv_qp_init_attr *qp_attr) {
	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = s_ctx->cq;
	qp_attr->recv_cq = s_ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	// confirm what these arrtibutes do and make them generic to caller functions on top
	qp_attr->cap.max_send_wr = CQ_QUEUE_SIZE;
	qp_attr->cap.max_recv_wr = CQ_QUEUE_SIZE;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

void Coordinator::build_params(struct rdma_conn_param *params) {
	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
}



void * Coordinator::poll_cq(void * ctx) { // check this for void* ctx
	struct ibv_cq *cq;
	struct ibv_wc wc;

	void* x = static_cast<void*> (s_ctx->ctx);
//	DEBUG("poll_cq");
	while (1) {
		
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &x));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		
		while (ibv_poll_cq(cq, 1, &wc)) {
			on_completion(&wc);
		}
	}

	return NULL;
}

void Coordinator::post_receives(struct connection *conn) {
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	wr.wr_id = (uintptr_t)conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)conn->recv_msg;
	sge.length = sizeof(struct message);
	sge.lkey = conn->recv_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void Coordinator::register_memory(struct connection *conn)
{
	conn->recv_msg = static_cast<struct message*>(malloc(sizeof(struct message)));

	conn->rdma_remote_region = static_cast<char*>(calloc(1, RDMA_BUFFER_SIZE));
	conn->rdma_local_region  = static_cast<char*>(calloc(1, RDMA_BUFFER_SIZE));

	TEST_Z(conn->recv_mr = ibv_reg_mr(
	  s_ctx->pd, 
	  conn->recv_msg, 
	  sizeof(struct message), 
	  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));


	TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
	  s_ctx->pd, 
	  conn->rdma_remote_region, 
	  RDMA_BUFFER_SIZE, 
	  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));

	TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
	  s_ctx->pd, 
	  conn->rdma_local_region, 
	  RDMA_BUFFER_SIZE, 
	  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
}

void Coordinator::on_connect(void *context)
{
	struct connection * conn = ((struct connection *)context);
	pthread_mutex_lock((conn->mutex_conn));
	
	*(conn->connected) = 1;
	
	pthread_cond_signal((conn->cv_conn));
	pthread_mutex_unlock((conn->mutex_conn));
}
/*
  To do: add remaining possible errors codes and handling, instead of just dying
  check if the QP does not go into a bad state and if it does, inform the client and ask him to recreate a message, or establish a valid contractr
*/

/* new API use struct enqueue wait for reply pop/memcpy function increment*/
  void Coordinator::on_completion(struct ibv_wc *wc)
  {

//  	DEBUG("in on completion\n");

    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
    if (wc->status ==  IBV_WC_REM_ACCESS_ERR) {
      printf("PROTECTED DOMAIN ERROR\n");
      return;
    }

    if (wc->status != IBV_WC_SUCCESS) {
//      die("on_completion: status is not IBV_WC_SUCCESS.");
      op_done  = -1;
      return;
    }

    if (wc->opcode & IBV_WC_RECV) {
//         printf("op code was receive\n");

        if (conn->rs == RS_INIT) {
//           printf("RS_MR_RECV\n");
        	conn->rs = RS_MR_RECV;
        } else {
        	conn->rs = RS_DONE_RECV;
//        	 printf("received memory region from server of size: (add size)\n");
        }
      if (conn->recv_msg->tp == MSG_MR) {
        memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
        on_connect(conn->id->context);
        ops_started = true;
//        printf("should start ops now\n");
        // post_receives(conn); // no need to post receives since no other message is expected by contract
      }
    }

    if (ops_started) {
// //    	printf("op received\n");
// 		std::unique_lock<std::mutex> mlock(mutex_);
// 		if (queue_->empty()) {
// 			// cond_.wait(mlock);
// 			return;
// 		}
		
// 		WorkRequest* item = queue_->front();
// 		queue_->pop();

// 		if (item->getWorkRequestCommand() == READ_) {
// 			item->copyValue(node_id, conn->rdma_local_region);
// 			item->vote(node_id);
// 		} else if (item->getWorkRequestCommand() == WRITE) {
// 			// item->copyValue(conn->rdma_local_region);
// 			item->vote(node_id);
// 		} else if (item->getWorkRequestCommand() == ACS) {
// 			item->copyValueACS(node_id, conn->rdma_local_region);
// 			item->vote(node_id);
// 		}

//     }
   	// {
    	// std::unique_lock<std::mutex> mlock(mutex_);
    	// memcpy(buffer, conn->rdma_local_region, 1024);
    	op_done=1; // change to accomodate multiple ops together	
    	// mlock.unlock();
    }
  }



void Coordinator::destroy_connection(void *context)
{
	struct connection *conn = (struct connection *)context;

	rdma_destroy_qp(conn->id);

	ibv_dereg_mr(conn->recv_mr);
	ibv_dereg_mr(conn->rdma_remote_mr);

	free(conn->recv_msg);
	free(conn->rdma_remote_region);

	rdma_destroy_id(conn->id);

	free(conn);
}



//rdma stuff
  int Coordinator::rdma_read(connection *conn, int offset, int size, char* buffer) {
    op_done = 0;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

   	memset(&wr, 0, sizeof(wr));
      
    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr + offset; 
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_local_region;
    sge.length = size;
    sge.lkey = conn->rdma_local_mr->lkey;

    ibv_post_send(conn->qp, &wr, &bad_wr);
    
    while(op_done == 0) {}
    
    if (op_done == 1) {
      // do memcpy
      // printf("error was not null\n");
      // printf("read op completed, buffer value being copied is (as a string) : %s\n", conn->rdma_local_region);
      // memcpy(buffer, conn->rdma_local_region, size);
      return 0;
    }

    return 1;

  }

  int Coordinator::rdma_write(connection *conn, int offset, int size, char* buffer) {
  	memcpy(conn->rdma_remote_region, buffer,  size); // copy to registered memory
    op_done = 0;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

   	memset(&wr, 0, sizeof(wr));
      
    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr + offset;
    wr.wr.rdma.rkey = conn->peer_mr.rkey;

    sge.addr = (uintptr_t)conn->rdma_remote_region;
    sge.length = size;
    sge.lkey = conn->rdma_remote_mr->lkey;
    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));	

    while(op_done == 0) {
    }

    if (op_done == 1) {
      // printf("write op was completed, written value as (as a sting) : %s\n", conn->rdma_remote_region);
      // memcpy(buffer, conn->rdma_remote_region, size);
      return 0;
    }

    return 1;
  }

  // Compare and swap data over this RdmaConnection
  int Coordinator::rdma_compswap(connection *conn, int offset, uint64_t compare, uint64_t swap, char* buffer)
  {
    // op_done = 0;

    /*
        debug for CAS ops, buffer always returns current value at the location specified
    */

    // printf("this is compare %d\n", compare);
    // printf("this is swap %d\n", swap);
    // printf("this is offset %d\n", offset);

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)conn;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    
    wr.wr.atomic.remote_addr = (uintptr_t)conn->peer_mr.addr + offset;
    wr.wr.atomic.rkey = conn->peer_mr.rkey;
    wr.wr.atomic.compare_add = compare;
    wr.wr.atomic.swap = swap;

    sge.addr = (uintptr_t)conn->rdma_local_region;
    sge.length = sizeof(uint64_t);
    sge.lkey = conn->rdma_local_mr->lkey;

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

    /*
      busy wait for 1 seconds
    */

    // while(op_done == 0) {
    // }

    // if (op_done == 1) {
    //   // uint64_t x;
    //   memcpy(&buffer, conn->rdma_local_region, sizeof(uint64_t));
    //   printf("CAS op completed, the existing value is/was: : %ld\n", buffer);
    //   return 0;
    // }

    return 1;
  }


	// WorkRequest* Coordinator::peek() {
 //        return (queue_->front());
 //    }

 //    bool Coordinator::isEmpty() {
 //        return (queue_->empty());
 //    }	
} // namespace


int main(int argc, char **argv) {

    if (argc < 4) {
        printf("Usage: address port num_of_ops read/write/cas\n");
        return -1;
    }
    printf("staring test\n");

    int num_ops = atoi(argv[3]);
    int command = atoi(argv[4]); //0 read, 1 write, 2 CAS

    Coordinator *c1 = new Coordinator(argv[1], argv[2], 0);
    c1->CreateConnection();
    // printf("command was %d\n", atoi(argv[4]));

    struct timeval start,end;
 	double t1,t2;

	if(gettimeofday(&start,NULL)) {
	   printf("time failed\n");
	   exit(1);
	}
	
	t1=(start.tv_sec * 1000000.0)+(start.tv_usec);
	

    for (int i=0; i<num_ops; i++) {
    	char* readinto = (char*)malloc(1024);
    	if (command == 0) {
			c1->RdmaRead(0, 10, readinto);    		
    	} else if (command == 1) {
    		c1->RdmaWrite(0, 10, readinto);
    	}
    }

    // while (c1->op_done < (num_ops) ) {}
    
    if(gettimeofday(&end,NULL)) {
	    printf("time failed\n");
	    exit(1);
   }

    t2=(end.tv_sec * 1000000.0)+(end.tv_usec);
	printf("%f\n", (t2-t1)/num_ops );
	// arr2[ii] = t2;
	// ii++;


    printf("%d\n", c1->op_done);

    c1->callJoin();
    return 0;
}