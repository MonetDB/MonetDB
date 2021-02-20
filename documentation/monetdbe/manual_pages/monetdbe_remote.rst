================
MONETDBE_REMOTE
================

NAME
====

monetdbe_remote --- struct that holds options for a MonetDBe remote connection

SYNOPSIS
========
.. code-block:: c

 #include <monetdbe.h>

    typedef struct {
        const char *host;
        int port;
        const char *username;
        const char *password;
        const char *lang;
    } monetdbe_remote;


DESCRIPTION
===========
MonetDBe remote struct. Object can be passed to a monetdbe_options object. Can also be null, if there are no remote options. This struct holds these fields:

(1) const char \*host. String that holds the server ip (e.g. "localhost" or "192.168.178.10").
(2) int port. Server port number on which the MonetDB database is hosted.
(3) const char \*username. MonetDB username.
(4) const char \*password. MonetDB password.
(5) const char \*lang. Language, currently not (yet) used.

EXAMPLES
========

.. code-block:: c
   
    monetdbe_remote *remote = malloc(sizeof(monetdbe_remote));

    remote->host = "localhost";
    remote->port = 50000;
    remote->username = "monetdb";
    remote->password = "monetdb";
    remote->lang = NULL; // not currently used.
