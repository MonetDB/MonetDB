/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file daemon.c
 * Pathfinder daemon and TCP/IP support.
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

#include "pathfinder.h"
#include "daemon.h"

#include "oops.h"

#if HAVE_SOCKLEN_T
#define SOCKLEN socklen_t
#else
#define SOCKLEN size_t
#endif

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
 */
/* HACK to pacify picky Intel compiler on Linux64 */
#if (defined(__INTEL_COMPILER) && (SIZEOF_VOID_P > SIZEOF_INT))  
#undef  SIG_IGN /*((__sighandler_t)1 )*/
#define SIG_IGN   ((__sighandler_t)1L)
#endif
void
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
}


/**
 * Establish a TCP socket on the specified port @a port and listen for
 * incoming connections (= queries).
 * This function creates a new compiler instance for each incoming
 * connection (so do _not_ loop around it!).
 *
 * @param port TCP port to listen on
 * @retval client socket file descriptor associated with new client connection
 */
void
new_instance (int port, int *client)
{
    struct sockaddr_in localhost;

    /* client address information */
    struct sockaddr_in client_addr;
    SOCKLEN            client_addrlen = sizeof (client_addr);

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
                          (struct sockaddr *) &client_addr,
                          (socklen_t *) &client_addrlen);

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

                break;

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
 */
void
PFdaemonize (int port)
{
    int client;

    /* check for unprivileged TCP port */
    if (port < 1024) 
        PFoops (OOPS_NOSERVICE, "port %d is unusable for us", port);

    /* try to enter daemon state */
    daemonize ();

    /* wait for incoming query and create new compiler instance */
    new_instance (port, &client);

    /* bind stdin/stdout to incoming socket fd */
    if (dup2 (client, fileno (stdin)) < 0) 
        PFoops (OOPS_FATAL, "dæmon cannot establish its input: %s",
                strerror (errno));

    if (dup2 (client, fileno (stdout)) < 0) 
        PFoops (OOPS_FATAL, "dæmon cannot establish its output: %s",
                strerror (errno));
}


/* vim:set shiftwidth=4 expandtab: */
