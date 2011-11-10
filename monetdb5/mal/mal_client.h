/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_CLIENT_H_
#define _MAL_CLIENT_H_
#define bitset int

/*#define MAL_CLIENT_DEBUG */

#include "mal_resolve.h"
#include "mal_profiler.h"
#include "mal.h"

#define CONSOLE     0
#define isAdministrator(X) (X==mal_clients)

#define FREECLIENT  0
#define FINISHING   1   
#define CLAIMED     2
#define AWAITING    4   /* not used, see bug #1939 */

#define TIMEOUT     (5*60)  /* seconds */
#define PROCESSTIMEOUT  2   /* seconds */

#ifdef HAVE_SYS_RESOURCE_H
# include <sys/resource.h>
#endif

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif
#include <setjmp.h>

/*
 * @-
 * The prompt structure is designed to simplify recognition
 * of the language framework for interaction. For direct console
 * access it is a short printable ascii string. For access through
 * an API we assume the prompt is an ascii string surrounded by a \001
 * character. This simplifies recognition.
 * The information between the prompt brackets can be used to
 * pass the mode to the front-end. Moreover, the prompt can be
 * dropped if a single stream of information is expected from the
 * server(See mal_profiler.mx).
 * @-
 * The user can request server-side compilation as part of the
 * initialization string. See the documentation on Scenarios.
 */
typedef struct CLIENT_INPUT {
	bstream             *fdin;
	int                 yycur;		
	int                 listing;
	char                *prompt;
	struct CLIENT_INPUT *next;    
} ClientInput;

typedef struct CLIENT {
	int idx;        /* entry in mal_clients */
	oid user;       /* user id in the auth administration */
	/*
	 * @-
	 * The actions for a client is separated into several stages: parsing,
	 * strategic optimization, tactial optimization, and execution.
	 * The routines to handle them are obtained once the scenario is choosen.
	 * Each stage carries a state descriptor, but they share the IO state
	 * description. A backup structure is provided
	 * to temporarily switch to another scenario. Propagation of the state
	 * information should be dealt with separately.[TODO]
	 */
	str     scenario;  /* scenario management references */
	str     oldscenario;
	void    *state[7], *oldstate[7];
	MALfcn  phase[7], oldphase[7];
	sht	stage;	   /* keep track of the phase being ran */
	char    itrace;    /* trace execution using interactive mdb */
						/* if set to 'S' it will put the process to sleep */
	short   debugOptimizer,debugScheduler;
	/*
	 * @-
	 * For program debugging we need information on the timer and memory
	 * usage patterns.
	 */
	sht	flags;	 /* resource tracing flags */
	lng     timer;   /* trace time in usec */
	lng	bigfoot; /* maximum virtual memory use */
	lng	vmfoot;  /* virtual memory use */
	lng memory;	/* memory claimed for keeping BATs */
	BUN	cnt;	/* bat count */

#define timerFlag	1
#define memoryFlag	2
#define ioFlag		4
#define flowFlag	8
#define bigfootFlag	16
#define cntFlag		32
#define threadFlag	64
#define bbpFlag		128
	/*
	 * @-
	 * @-
	 * Session structures are currently not saved over network failures.
	 * Future releases may support a re-connect facility.  [TODO]
	 */
	time_t      login;  
	time_t      lastcmd;	/* set when input is received */
	unsigned int  delay;	/* not yet used */
	int 	    qtimeout;	/* query abort after x seconds */
	int	    stimeout;	/* session abort after x seconds */
	/*
	 * @-
	 * Communication channels for the interconnect are stored here.
	 * It is perfectly legal to have a client without input stream.
	 * It will simple terminate after consuming the input buffer.
	 */
	str       srcFile;  /* NULL for stdin, or file name */
	bstream  *fdin;
	int       yycur;    /* the scanners current position */
	/*
	 * @-
	 * Keeping track of instructions executed is a valueable tool for
	 * script processing and debugging.
	 * Its default value is defined in the MonetDB configuration file.
	 * It can be changed at runtime for individual clients using
	 * the operation @sc{clients.listing}(@emph{mask}).
	 * A listing bit controls the level of detail to be generated during
	 * program execution tracing. The lowest level (1) simply dumps the input,
	 * (2) also demonstrates the MAL internal structur (4) adds the
	 * type information
	 */
	bitset  listing;        
	str prompt;         /* acknowledge prompt */
	size_t promptlength;
	ClientInput *bak;   /* used for recursive script and string execution */

	stream   *fdout;    /* streams from and to user. */
	/*
	 * @-
	 * In interactive mode, reading one line at a time, we should be
	 * aware of parsing compound structures, such as functions and
	 * barrier blocks. The level of nesting is maintained in blkmode,
	 * which is reset to zero upon encountering an end instruction,
	 * or the closing bracket has been detected. Once the complete
	 * structure has been parsed the program can be checked and executed.
	 * Nesting is indicated using a '+' before the prompt.
	 */
	int blkmode;        /* control block parsing */
	/*
	 * @-
	 * The MAL debugger uses the client record to keep track of
	 * any pervasive debugger command. For detailed information
	 * on the debugger features.
	 */
	bitset debug;
	void  *mdb;            /* context upon suspend */
	str    history;	       /* where to keep console history */
	short  mode;           /* FREECLIENT..BLOCKED */
	/*
	 * @-
	 * Client records are organized into a two-level dependency
	 * tree, where children may be created to deal with parallel processing
	 * activities. Each client runs in its own process thread. Its identity
	 * is retained here for access by others (=father).
	 */
	MT_Sema 	s;	    /* sema to (de)activate thread */ 
	Thread      	mythread;
	MT_Id		mypid;
	str     	errbuf;     /* location of GDK exceptions */
	struct CLIENT   *father;    
	/*
	 * @-
	 * Each client has a private entry point into the namespace and
	 * object space (the global variables).
	 * Moreover, the parser needs some administration variables
	 * to keep track of critical elements.
	 */
	Module      nspace;     /* private scope resolution list */
	Symbol      curprg;     /* focus of parser */
	Symbol      backup;     /* save parsing context */
	MalStkPtr   glb;        /* global variable stack */
	/*
	 * @-
	 * Some statistics on client behavior becomes relevant
	 * for server maintenance. The scenario loop is used as
	 * a frame of reference. We measure the elapsed time after
	 * a request has been received and we have to wait for
	 * the next one.
	 */
	int		actions;
	lng		totaltime;	/* sum of elapsed processing times */
	struct RECSTAT *rcc;	/* recycling stat */
#ifdef HAVE_TIMES
	struct tms	workload;
#endif
	jmp_buf	exception_buf;
	int exception_buf_initialized;
} *Client, ClientRec;

mal_export void    MCinit(void);

mal_export int MAL_MAXCLIENTS;
mal_export ClientRec *mal_clients;
mal_export int MCdefault;

mal_export Client  MCgetClient(int id);
mal_export Client  MCinitClient(oid user, bstream *fin, stream *fout);
mal_export int     MCinitClientThread(Client c);
mal_export void    MCcloseClient    (Client c);
mal_export Client  MCforkClient     (Client c);
mal_export int	   MCcountClients(void);
mal_export int     MCreadClient  (Client c);
mal_export str	   MCsuspendClient(int id, unsigned int timeout);
mal_export str	   MCawakeClient(int id);
mal_export void    MCcleanupClients(void);
mal_export void    MCtraceAllClients(int flag);
mal_export void    MCtraceClient(oid which, int flag);
mal_export int     MCpushClientInput(Client c, bstream *new_input, int listing, char *prompt);
mal_export void    MCpopClientInput(Client c);

#endif /* _MAL_CLIENT_H_ */
