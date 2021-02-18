=============
MONETDBE_OPEN
=============

NAME
====

monetdbe_open --- Open a MonetDBe connection

SYNOPSIS
========
#include <monetdbe.h>

monetdbe_open(monetdbe_database \*db, char \*url, monetdbe_options \*opts);

DESCRIPTION
===========
The monetdbe_open() function opens a connection to a MonetDB database.
This function takes 3 arguments:

(1) monetdbe_database* database.
(2) char* url. NULL for in-memory database.
(3) monetdbe_options* opts. NULL if no options.

RETURN VALUE
============
0 for success, else errno.

EXAMPLES
========

.. code-block:: c

    // Usage of an in-memory database
    monetdbe_database mdbe = NULL;
    if (monetdbe_open(&mdbe, NULL, NULL))
        fprintf(stderr, "Failed to open database")


.. code-block:: c

    // Usage of an remote database
    monetdbe_database remote = NULL;
    monetdbe_open(&remote, "monetdb://localhost:5000/sf1?user=monetdb&password=monetdb", NULL); 

.. code-block:: c

    // Usage of opts
    monetdbe_options *opts = malloc(sizeof(monetdbe_options));
    monetdbe_remote *remote = malloc(sizeof(monetdbe_remote));

    remote->host = "localhost";
    remote->port = 50000;
    remote->username = "monetdb";
    remote->password = "monetdb";
    remote->lang = NULL; // NOT USED
    opts->remote = remote;

    monetdbe_open(db, "mapi:monetdb://localhost:50000/test", opts);


SEE ALSO
========
*monetdbe_database*\ (1) *monetdbe_options*\ (1) *monetdbe_remote*\ (1)
