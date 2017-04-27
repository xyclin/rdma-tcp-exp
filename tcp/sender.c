#include <string.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>

#define MYPORT 3490
#define SIZE_TO_SEND 10
#define MY_IP "172.19.18.72"

int main(int argc, char *argv[]) {

 int sockfd,sockfd2;
 char tosend = 's'; //a char (1byte) to send to receivers
 char toSendArr[SIZE_TO_SEND] = {0};
 char ack;
 struct sockaddr_in my_addr,rcvr_addr;
 struct timeval start,end;
 int sin_size = sizeof(my_addr),i,k,num_packet_sent,optval;
 double t1,t2;

 //open TCP socket,bind and accept RECEIVERS connections 
 if( (sockfd = socket(PF_INET, SOCK_STREAM, 0)) ==-1){
  perror("socket error");
  exit(1);
 }
 my_addr.sin_family = AF_INET;
 my_addr.sin_port = htons(MYPORT);
 my_addr.sin_addr.s_addr = inet_addr(MY_IP);
 memset(my_addr.sin_zero, '\0', sizeof(my_addr.sin_zero));
 //allow reuse of port
 optval = 1;
 if (setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,&optval,sizeof(int)) == -1) {
  perror("setsockopt");
  exit(1);
 }
 //bind(socketfd, struct about my address,sizeofmy address);
 if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof my_addr) == -1) {
  perror("bind");
  exit(1);
 }
 listen(sockfd,10);

 sockfd2 = accept(sockfd, (struct sockaddr *)&rcvr_addr, &sin_size);

 //connections OK
 //send 100 packet of size 1 byte and for each send wait for ack
 t1=0.0; t2=0.0;
 printf("Sending 100 messages 1 kbyte each and wait for ack.\n");
 for(num_packet_sent=0;num_packet_sent<100;num_packet_sent++){
  if(gettimeofday(&start,NULL)) {
   printf("time failed\n");
   exit(1);
  }
  send(sockfd2,toSendArr,sizeof(char) * SIZE_TO_SEND,0);
  optval=recv(sockfd2,&ack,sizeof(char),0);
  if(optval==-1) {
   perror("Receive error");
   exit(1);
  }
  else{
   if(gettimeofday(&end,NULL)) {
    printf("time failed\n");
    exit(1);
   }
             //t1+=start.tv_sec+(start.tv_usec/1000000.0);
             //t2+=end.tv_sec+(end.tv_usec/1000000.0);
             t1+=(start.tv_sec * 1000000.0)+(start.tv_usec);
             t2+=(end.tv_sec * 1000000.0)+(end.tv_usec);
  }
 }
 //calculate and print mean rtt
 printf("RTT = %g us\n",(t2-t1)/100);
 printf("close sockets and exit\n");
 shutdown(sockfd2,2);
 shutdown(sockfd,2);
 exit(0);
}
