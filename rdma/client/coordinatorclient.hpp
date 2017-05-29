#ifndef COORDINATORCLIENT_H
#define COORDINATORCLIENT_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <queue>
#include <rdma/rdma_cma.h>
#include <mutex>

namespace Draco {

#define TEST_NZ(x) do { if ( (x)) { printf("%s\n", strerror(errno)); die("error: " #x " failed (returned non-zero)." );}  } while (0)
#define TEST_Z(x)  do { if (!(x)) { printf("%s\n", strerror(errno)); die("error: " #x " failed (returned zero/null)."); } } while (0)

#define DEBUG_LEVEL 2

#define DEBUG(x) do { if (DEBUG_LEVEL) { std::cout << x << endl; } } while(0)

struct client {
    struct addrinfo *addr;
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;

    char *address = NULL;
    char *port = NULL;

    std::thread event_loop_thread;
    pthread_mutex_t *mutex_conn;
    pthread_cond_t *cv_conn;
    int *connected;
};

enum type {
    MSG_MR,
    MSG_DONE
};

struct message {
    type tp;

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

enum recv_state{
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV
};


struct connection {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;
    int rand;
    int *connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *rdma_remote_mr;
    struct ibv_mr *rdma_local_mr;

    struct ibv_mr peer_mr;

    struct message *recv_msg;

    char *rdma_remote_region;
    char *rdma_local_region;
    recv_state rs;

    pthread_mutex_t *mutex_conn;
    pthread_cond_t *cv_conn;
};


class Coordinator {

/*
    You should only be concerned with the public section of this class, details on calling and return is in the HPP file
    NOTE:
    All calls a Synchronous for the time being via waiting for op to be complete, only one op per QP is tested and implemented

*/
    public:

        /*
            the code works for all general cases, testing has not been extensive, please report any bugs discovered

            1.  Coordinator(char *address, char *port);

                initializes required client and connection structs

            2.  ~Coordinator();

                called by C++ on disconnection, destroys related structs

            3.  int RdmaRead(int offset, int size, char* buffer);

                offset: in bytes the distance from top of the registered memory region
                size: size of read, please ensure it does not exceed the max value, no checks are in place to do so
                buffer: malloced buffer to which rdma copies read data into

            4. int RdmaWrite(int offset, int size, char* buffer);

                offset: in bytes the distance from top of the registered memory region
                size: size of write, please ensure it does not exceed the max value, no checks are in place to do so
                buffer: malloced buffer from which RDMA reads and writes on remote address specified

            5. int RdmaCompSwap(int offset, uint64_t compare, uint64_t swap);

                offset: bytes from top addr from where the read begins
                compare: uint_64 integer that we are comparing against, if this is equal value will be swapped
                swap: if condition is met by compare, this uint64_t will be replaced atomically
                buffer: value of existing value will always be copied in this buffer, please malloc

                untested cases, trying to overwrite not a valid int, will probably fail


                TO DO:
                    add revoke mechanism and implement easier to use API with some caching and soft state code pushed to these API, will need some sort of message passing
                    and/or timeouts to ensure revokes

            6. void callJoin();

                needs to be called to prevent connection from disconnecting if no work will be done and all threads will exit // primary use case is for testing

        */


        Coordinator(char *address, char *port, int node_id, void* test_addr);
        ~Coordinator();
        int CreateConnection();
        int RdmaRead(int offset, int size, char* buffer);
        int RdmaWrite(int offset, int size, char* buffer);
        int RdmaCompSwap(int offset, uint64_t compare, uint64_t swap, char* buffer);
        // int RdmaRead(WorkRequest* workReq);
        // int RdmaWrite(WorkRequest* workReq);
        // int RdmaCompSwap(WorkRequest* workReq);
        void callJoin();


        // common functions
        void die(const char *reason);
        void build_connection(struct rdma_cm_id *id, client *c);
        void build_params(struct rdma_conn_param *params);
        void on_connect(void *context);
        void destroy_connection(void *context);
        int rdma_read(connection *conn, int offset, int size, char* buffer);
        int rdma_write(connection *conn, int offset, int size, char* buffer);
        int rdma_compswap(connection *conn, int offset, uint64_t compare, uint64_t swap, char* buffer);

        //connection funcitons
        void* event_loop_client(void *param);
        int on_event(struct rdma_cm_event *event);
        int on_addr_resolved(struct rdma_cm_id *id);
        int on_connection(struct rdma_cm_id *id);
        int on_disconnect(struct rdma_cm_id *id);
        int on_route_resolved(struct rdma_cm_id *id);

        void build_context(struct ibv_context *verbs);
        void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
        void post_receives(struct connection *conn);
        void register_memory(struct connection *conn);
        void on_completion(struct ibv_wc *);
        void * poll_cq(void*);


        // WorkRequest* peek();
        bool isEmpty();

    int op_done = 0;

private:
    // default values
    int debug_level = 2;
    int TIMEOUT_IN_MS = 500; /* ms */
    const int CQ_QUEUE_SIZE = 1024;
    const int RDMA_BUFFER_SIZE = 1024*1024*200 + 12;
    const void* test_addr;
    int node_id;
    //class variables
    client *c;

    pthread_mutex_t mutex_conn_new;
    pthread_cond_t cv_conn_new;
    std::thread* event_thread;

    int connected_ = 0;

    struct context* s_ctx = NULL;


    bool ops_started = false;

    // std::queue<WorkRequest*> *queue_ = new std::queue<WorkRequest*>();
    std::mutex mutex_;

    char* buffer = (char*)malloc(1024);
    // std::condition_variable cond_;
};

}// namespace

#endif