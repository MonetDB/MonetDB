#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include <stream.h>

#include "comm.h"

int client(char *host, int port ){
    struct sockaddr_in server;
    struct hostent * host_info;
    int sock;
    
    if ((sock = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
      fprintf (stderr, "client: could not open socket");
      exit (2);
    }
    
    if (!(host_info = gethostbyname (host))) {
      fprintf (stderr, "client: unknown host %s", host);
      exit (3);
    }
    
    server.sin_family = host_info -> h_addrtype;
    memcpy ((char *) &server.sin_addr,
            host_info -> h_addr,
            host_info -> h_length);
    server.sin_port = htons (port);
    
    printf("connecting\n");
    if (connect (sock, (struct sockaddr *) &server, sizeof server) < 0) {
      fprintf (stderr, "client: could not connect to server");
      exit (4);
    }
    printf("connected\n");
 
    return sock;
}

