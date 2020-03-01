/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _MAL_CLIENT_H_
#define _MAL_CLIENT_H_

#include "mal.h"

#include "mal_resolve.h"

#define SCENARIO_PROPERTIES 8

enum clientmode {
	FREECLIENT,
	FINISHCLIENT,
	RUNCLIENT,
	BLOCKCLIENT
};

/*
 * The prompt structure is designed to simplify recognition of the
 * language framework for interaction. For access through an API we 
 * assume the prompt is an ASCII string surrounded by a \001 character. This
 * simplifies recognition.  The information between the prompt brackets
 * can be used to pass the mode to the front-end. Moreover, the prompt
 * can be dropped if a single stream of information is expected from the
 * server (see mal_profiler.c).
 *
 * The user can request server-side compilation as part of the
 * initialization string. See the documentation on Scenarios.
 */
typedef struct CLIENT_INPUT {
	bstream             *fdin;
	size_t              yycur;
	int                 listing;
	char                *prompt;
	struct CLIENT_INPUT *next;    
} ClientInput;

typedef struct CLIENT {
	int idx;        /* entry in mal_clients */
	oid user;       /* user id in the auth administration */
	str username;	/* for event processor */
	/*
	 * The actions for a client is separated into several stages:
	 * parsing, strategic optimization, tactical optimization, and
	 * execution.  The routines to handle them are obtained once the
	 * scenario is chosen.  Each stage carries a state descriptor, but
	 * they share the IO state description. A backup structure is
	 * provided to temporarily switch to another scenario.
	 */
	str     scenario;  /* scenario management references */
	str     oldscenario;
	void    *state[SCENARIO_PROPERTIES], *oldstate[SCENARIO_PROPERTIES];
	MALfcn  phase[SCENARIO_PROPERTIES], oldphase[SCENARIO_PROPERTIES];
	char    itrace;    /* trace execution using interactive mdb */
						/* if set to 'S' it will put the process to sleep */
	bit		sqlprofiler;		/* control off-line sql performance monitoring */
	/*
	 * Each session comes with resource limitations and predefined settings.
	 */
	char	optimizer[IDLENGTH];/* The optimizer pipe preferred for this session */
	int 	workerlimit;		/* maximum number of workthreads processing a query */
	int		memorylimit;		/* Memory claim highwater mark, 0 = no limit */
	lng 	querytimeout;		/* query abort after x usec, 0 = no limit*/
	lng	    sessiontimeout;		/* session abort after x usec, 0 = no limit */

	time_t  login;  	/* Time when this session started */
	lng 	session;	/* usec since start of server */
	time_t  idle;		/* Time when the session became idle */  

	/*
	 * For program debugging and performance trace we keep the actual resource claims.
	 */
	time_t  lastcmd;	/* set when query is received */

	/* The user can request a TRACE SQL statement, calling for collecting the events locally */
	BAT *profticks;				
	BAT *profstmt;

	ATOMIC_TYPE	lastprint;	/* when we last printed the query, to be depricated */
	/*
	 * Communication channels for the interconnect are stored here.
	 * It is perfectly legal to have a client without input stream.
	 * It will simply terminate after consuming the input buffer.
	 */
	str       srcFile;  /* NULL for stdin, or file name */
	bstream  *fdin;
	size_t    yycur;    /* the scanners current position */
	/*
	 * Keeping track of instructions executed is a valuable tool for
	 * script processing and debugging.  It can be changed at runtime
	 * for individual clients using the operation clients.listing(mask).
	 * A listing bit controls the level of detail to be generated during
	 * program execution tracing.  The lowest level (1) simply dumps the
	 * input, (2) also demonstrates the MAL internal structure, (4) adds
	 * the type information.
	 */
	int  listing;        
	str prompt;         /* acknowledge prompt */
	size_t promptlength;
	ClientInput *bak;   /* used for recursive script and string execution */

	stream   *fdout;    /* streams from and to user. */
	/*
	 * In interactive mode, reading one line at a time, we should be
	 * aware of parsing compound structures, such as functions and
	 * barrier blocks. The level of nesting is maintained in blkmode,
	 * which is reset to zero upon encountering an end instruction, or
	 * the closing bracket has been detected. Once the complete
	 * structure has been parsed the program can be checked and
	 * executed.  Nesting is indicated using a '+' before the prompt.
	 */
	int blkmode;        /* control block parsing */
	/*
	 * The MAL debugger uses the client record to keep track of any
	 * pervasive debugger command. For detailed information on the
	 * debugger features.
	 */
	int debug;
	enum clientmode mode;  /* FREECLIENT..BLOCKED */
	/*
	 * Client records are organized into a two-level dependency tree,
	 * where children may be created to deal with parallel processing
	 * activities. Each client runs in its own process thread. Its
	 * identity is retained here for access by others (=father).
	 */
	MT_Sema 	s;	    /* sema to (de)activate thread */
	Thread      	mythread;
	str     	errbuf;     /* location of GDK exceptions */
	struct CLIENT   *father;    
	/*
	 * Each client has a private entry point into the namespace and
	 * object space (the global variables).  Moreover, the parser needs
	 * some administration variables to keep track of critical elements.
	 */
	Module      usermodule;     /* private user scope */
	Module		curmodule;		/* where to deliver the symbol, used by parser , only freed globally */
	Symbol      curprg;     /* container for the malparser */
	Symbol      backup;     /* saving the parser context for functions,commands/patterns */
	MalStkPtr   glb;        /* global variable stack */
	/*
	 * Some statistics on client behavior becomes relevant for server
	 * maintenance. The scenario loop is used as a frame of reference.
	 * We measure the elapsed time after a request has been received and
	 * we have to wait for the next one.
	 */
	int		actions;

	/*
	 * Here are pointers to scenario backends contexts.  For the time
	 * being just SQL.  We need a pointer for each of them, since they
	 * have to be able to interoperate with each other, e.g.  both
	 * contexts at the same time are in use.
	 */
	void *sqlcontext;

	/*
	 * The workload for replication/replay is saved initially as a MAL block.
	 * It is split into the capturing part (wlc) and the replay part (wlr).
	 * This allows a single server to act as both a master and a replica.
	 */
	int wlc_kind;	// used by master to characterise the compound transaction
	MalBlkPtr wlc;

	/*	
	 *	Errors during copy into are collected in a user specific column set
	 */
	BAT *error_row;
	BAT *error_fld;
	BAT *error_msg;
	BAT *error_input;

	size_t blocksize;
	protocol_version protocol;
	bool filetrans;			/* whether the client can read files for us */
	const char *(*getquery)(struct CLIENT *);
} *Client, ClientRec;

mal_export int MAL_MAXCLIENTS;
mal_export ClientRec *mal_clients;

mal_export Client  MCgetClient(int id);
mal_export Client  MCinitClient(oid user, bstream *fin, stream *fout);
mal_export Client  MCforkClient(Client father);
mal_export void	   MCstopClients(Client c);
mal_export int	   MCactiveClients(void);
mal_export void    MCcloseClient(Client c);
mal_export str     MCsuspendClient(int id);
mal_export str     MCawakeClient(int id);
mal_export int     MCpushClientInput(Client c, bstream *new_input, int listing, char *prompt);
mal_export int	   MCvalid(Client c);

#endif /* _MAL_CLIENT_H_ */
