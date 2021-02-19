================
MONETDBE_OPTIONS
================

NAME
====

monetdbe_options --- struct that holds options for a MonetDBe connection

SYNOPSIS
========
.. code-block:: c

 #include <monetdbe.h>

    typedef struct {
        int memorylimit;  
        int querytimeout;|
        int sessiontimeout;  
        int nr_threads;  
        monetdbe_remote* remote;
        monetdbe_mapi_server* mapi_server;

    } monetdbe_options;

DESCRIPTION
===========
MonetDBe options struct. Object can be passed to a monetdbe_open() function. Can also be null, if there are no options. This struct holds these fields:

(1) int memorylimit. Top off the amount of RAM to be used, in MB.
(2) int querytimeout. Gracefully terminate query after a few seconds.
(3) int sessiontimeout. Graceful terminate the session after a few seconds.
(4)	int nr_threads. Maximum number of worker treads, limits level of parallelism.
(5)	monetdbe_remote* remote. Pointer to a monetdbe_remote object.
(6)	monetdbe_mapi_server* mapi_server. Pointer to a monetdbe_mapi_server object.

EXAMPLES
========

.. code-block:: c
    
    monetdbe_options *opts = malloc(sizeof(monetdbe_options));
    opts->memorylimit = 1024;
    opts->querytimeout = 10;
    opts->sessiontimeout = 10;
    opts->nrthreads = 2;
    opts->remote = remote; // somewhere else defined monetdbe_remote*

    monetdbe_open(db, "mapi:monetdb://localhost:50000/test", opts);

SEE ALSO
========
*monetdbe_remote*\ (1) *monetdbe_mapi_server*\ 
