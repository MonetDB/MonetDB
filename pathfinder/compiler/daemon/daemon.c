/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file daemon.c
 * Pathfinder daemon and TCP/IP support.
 *
 * $Id$
 */

#include "daemon.h"

/* exit () */
#include <stdlib.h>

/**
 * Try to clean up if we are terminated via kill(2).
 */
void
terminate (int sig) 
{
    PFoops (OOPS_FATAL,
            "terminated (received signal %d)", sig);
}

/** 
 * Turn ourselves into a daemon. 
 * @return return code OK if daemonizing went allright, otherwise
 * an indication of the failure
 */
static PFrc_t
daemonize (void)
{
    int child;
    int fd;

    /*
     * if we were started by init (pid 1) from the /etc/inittab file
     * there is no need to detach;
     * this test is unreliable due to an unavoidable ambiguity
     * if the process is started by some other process and orphaned
     * (i.e, if the parent process terminates before we are started)
     */
    if (getppid () == 1)
        goto in_background;

    /*
     * if we were not started in the background, fork and
     * let the parent exit; this also guarantees the first child
     * is not a process group leader.
     */	
    if ((child = fork ()) < 0)
        PFoops (OOPS_FATAL, "cannot fork first child");
    else if (child > 0)
        /* parent: return to caller (probably the shell) */
        exit (0); 

    /*
     * disassociate from controlling terminal and process group;
     * ensure the process can't reacquire a new controlling terminal
     */
    if (setpgrp () == -1)
        PFoops (OOPS_FATAL, "cannot diassociate from controlling tty");

    /* be immune from pgrp leader death */
    signal (SIGHUP, SIG_IGN); 

    if ((child = fork ()) < 0)
        PFoops (OOPS_FATAL, "cannot fork second child");
    else if (child > 0)
        /* death of the first child */
        exit (0);

in_background:
    /* close all file descs but stderr (log output) */
    for (fd = 0; fd < FOPEN_MAX; fd++)
        if (fd != fileno (stderr))
            close (fd);

    /* clear a possible EBADF from a close */
    errno = 0; 

    /*
     * move cwd to root, to make sure we 
     * aren't on any mounted filesystem
     */	
    chdir ("/");

    /*
     * clear any inherited file mode creation mask 
     */
    umask (0);

    /*
     * ignore any signals sent because of dying childs 
     * (prevents zombies)
     */
    signal (SIGCLD, SIG_IGN);

    /* ignore signals sent when writing to unconnected sockets */
    signal (SIGPIPE, SIG_IGN);

    /* trap forced termination */
    signal (SIGTERM, terminate);

    return OOPS_OK;
}


/**
 * Establish a TCP socket on the specified port @a port and listen for
 * incoming connections (= queries).
 * This function creates a new compiler instance for each incoming
 * connection (so do _not_ loop around it!).
 *
 * @param port TCP port to listen on
 * @retval client socket file descriptor associated with new client connection
 * @return error code
 */
static PFrc_t
new_instance (int port, int *client)
{
    struct sockaddr_in localhost;

    /* client address information */
    struct sockaddr_in client_addr;
    socklen_t          client_addrlen = sizeof (client_addr);

    /* rebind to already bound TCP ports */
    int reuse = 1;

    int incoming;

    incoming = socket (AF_INET, SOCK_STREAM, 0);
    if (incoming < 0) 
        PFoops (OOPS_FATAL,
                "cannot create TCP socket: %s", strerror (errno));

    setsockopt (incoming, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof (reuse));

    /* setup internet address information */
    memset((char *) &localhost, 0, sizeof(localhost));
    localhost.sin_family = AF_INET;
    localhost.sin_port = htons (port);
    localhost.sin_addr.s_addr = htonl (INADDR_ANY);

    if (bind (incoming, (struct sockaddr *) &localhost, sizeof (localhost)) < 0)
        PFoops (OOPS_FATAL, "cannot bind to TCP socket: %s", strerror (errno));

    /* queue up to 5 incoming connections before the kernel
     * rejects them on our behalf
     */
    if (listen (incoming, 5) < 0)
        PFoops (OOPS_FATAL,
                "cannot listen on TCP socket: %s", strerror (errno));

    while (true) {
        *client = accept (incoming, 
                          (struct sockaddr *) &client_addr, &client_addrlen);

        if (*client < 0) {
            /*
             * either a real error occured, or blocking was interrupted for
             * some reason;  only abort execution if a real error occured
             */
            if (errno == EINTR) 
                continue;

            close (incoming);
            PFoops (OOPS_FATAL, "accept on incoming TCP socket failed: %s",
                                strerror (errno));
        }

        /* client is connected, fork off a new compiler instance */
        switch (fork ()) {
            case 0:
                /* this is the new Pathfinder service instance */
                close (incoming);
                (void) PFoops (OOPS_NOTICE, "query was sent from @ %s", 
                               inet_ntoa (client_addr.sin_addr));

                return OOPS_OK;

            default:
                /* this is the parent: continue to listen */
                close (*client);
        }
    }
}

/** 
 * Turn this process into a daemon, listening on TCP port @a port
 * for incoming XQuery queries.  For each query, fork a copy of this
 * process to compile this single query.  Everytime we successfully
 * return from this function, stdin and stdout of the compiler are
 * bound to the incoming query client's TCP socket.  
 *
 * @param port TCP port to listen on for incoming queries 
 * @return return code indicating successful creation of 
 * a new compiler instance or failure
 */
PFrc_t PFdaemonize (int port)
{
    PFrc_t rc;
    int client;

    /* check for unprivileged TCP port */
    if (port < 1024) 
        return OOPS_NOSERVICE;

    /* try to enter daemon state */
    if ((rc = daemonize ()))
        return rc;

    /* wait for incoming query and create new compiler instance */
    if ((rc = new_instance (port, &client)))
        return rc;

    /* bind stdin/stdout to incoming socket fd */
    if (dup2 (client, fileno (stdin)) < 0) 
        PFoops (OOPS_FATAL, "dæmon cannot establish its input: %s",
                strerror (errno));

    if (dup2 (client, fileno (stdout)) < 0) 
        PFoops (OOPS_FATAL, "dæmon cannot establish its output: %s",
                strerror (errno));

    return OOPS_OK;
}


/* vim:set shiftwidth=4 expandtab: */
