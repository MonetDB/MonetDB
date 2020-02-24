*******************
The MonetDB clients
*******************

This chapter discusses the various clients that can connect to a MonetDB Server.

``mclient``
===========


``msqldump``
============

Miscellaneous
=============

The ``.monetdb`` file
---------------------

Various options to the above clients can be configured by a file. The clients
search for this file in the following locations:

#. The file specified in the ``DOTMONETDBFILE`` environment variable.
#. The file ``.monetdb`` in the current working directory.
#. The file ``monetdb`` in the ``XDG_CONFIG_HOME/monetdb/`` directory.
#. The file ``.monetdb`` in the user's home directory.

The options that can be specified in this file are the following:

  ``user``
    The username used to connect to the server.

  ``password``
    The password used to connect to the server.

  ``database``
    The database the client will connect to.

  ``host``
    The hostname where the server is running.

  ``port``
    The port where the server is listening on.

  ``language``
    What language this connection will use. Valid values are ``sql`` and ``mal``.

  ``save_history``
    This option allows the command history to persist across different client
    sessions. Valid options are ``true``, ``on``, ``false`` and ``off``. This
    option is relevant for interactive clients (only ``mclient`` currently).

  ``format``
    The format of the client output. Valid options are ``csv``, ``tab``,
    ``raw``, ``sql``, ``xml``, ``trash``, ``rowcount``.

  ``width``
    The width in characters of each row in the paginated output that the client
    produces.
